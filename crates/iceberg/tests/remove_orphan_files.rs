// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! End-to-end tests for `RemoveOrphanFilesAction`: a real table (memory
//! catalog + parquet writer chain) with extra unreferenced files dropped in.
//!
//! The dry-run report is the safety-critical surface: it must contain EXACTLY
//! the garbage files and never a referenced one (data files, manifests,
//! manifest lists, metadata). Deleting a referenced file is unrecoverable.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use iceberg::maintenance::RemoveOrphanFilesAction;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    DataFile, DataFileFormat, FormatVersion, NestedField, PrimitiveType, Schema, Type,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

async fn write_one_data_file(table: &Table, prefix: &str, batch: RecordBatch) -> Vec<DataFile> {
    let schema = table.metadata().current_schema().clone();
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(
        ParquetWriterBuilder::new(WriterProperties::builder().build(), schema),
        table.file_io().clone(),
        DefaultLocationGenerator::new(table.metadata()).unwrap(),
        // Unique per-write prefix: the generator's counter resets per instance,
        // so a shared prefix collides consecutive writes onto one path.
        DefaultFileNameGenerator::new(prefix.to_string(), None, DataFileFormat::Parquet),
    );
    let mut writer = DataFileWriterBuilder::new(rolling)
        .build(None)
        .await
        .unwrap();
    writer.write(batch).await.unwrap();
    writer.close().await.unwrap()
}

async fn live_row_count(table: &Table) -> usize {
    let mut n = 0;
    let mut stream = table
        .scan()
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();
    while let Some(batch) = stream.try_next().await.unwrap() {
        n += batch.num_rows();
    }
    n
}

/// Build a table with two appended data files (ids 1-4, 5-8).
async fn seed_table(warehouse: &TempDir) -> (impl Catalog, Table) {
    let catalog = MemoryCatalogBuilder::default()
        .load(
            "memory",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                warehouse.path().to_str().unwrap().to_string(),
            )]),
        )
        .await
        .unwrap();
    let ns = NamespaceIdent::new("db".to_string());
    catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .unwrap();
    let mut table = catalog
        .create_table(
            &ns,
            TableCreation::builder()
                .name("t".to_string())
                .schema(schema)
                .format_version(FormatVersion::V3)
                .build(),
        )
        .await
        .unwrap();

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "1".to_string(),
        )])),
    ]));
    for (i, ids) in [vec![1, 2, 3, 4], vec![5, 6, 7, 8]].into_iter().enumerate() {
        let batch =
            RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(Int32Array::from(ids))])
                .unwrap();
        let files = write_one_data_file(&table, &format!("data-{i}"), batch).await;
        let tx = Transaction::new(&table);
        table = tx
            .fast_append()
            .add_data_files(files)
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();
    }
    assert_eq!(live_row_count(&table).await, 8);
    (catalog, table)
}

/// Write an unreferenced file under the table's data dir; returns its path.
async fn plant_garbage(table: &Table, name: &str) -> String {
    let path = format!("{}/data/{name}", table.metadata().location());
    table
        .file_io()
        .new_output(&path)
        .unwrap()
        .write(bytes::Bytes::from_static(b"garbage"))
        .await
        .unwrap();
    path
}

/// Dry run: the report contains exactly the planted garbage — never a
/// referenced file — and nothing is deleted.
#[tokio::test]
async fn orphan_dry_run_finds_only_garbage() {
    let warehouse = TempDir::new().unwrap();
    let (_catalog, table) = seed_table(&warehouse).await;
    let garbage = plant_garbage(&table, "zz-garbage.parquet").await;

    let result = RemoveOrphanFilesAction::new(table.clone())
        .older_than_ms(now_ms() + 60_000) // everything qualifies by age
        .dry_run(true)
        .execute()
        .await
        .unwrap();

    assert_eq!(result.orphan_files, vec![garbage.clone()]);
    assert!(result.deleted_files.is_empty(), "dry run must not delete");
    assert!(result.failed_deletes.is_empty());
    assert!(
        result.listed_count > result.orphan_files.len(),
        "listing must cover referenced files too (listed {})",
        result.listed_count
    );
    // Nothing was deleted — the garbage file still exists, table intact.
    assert!(table.file_io().exists(&garbage).await.unwrap());
    assert_eq!(live_row_count(&table).await, 8);
}

/// The age guard: a freshly-written garbage file is NOT eligible when the
/// cutoff is in the past.
#[tokio::test]
async fn orphan_age_guard_skips_recent_files() {
    let warehouse = TempDir::new().unwrap();
    let (_catalog, table) = seed_table(&warehouse).await;
    let garbage = plant_garbage(&table, "zz-recent.parquet").await;

    let result = RemoveOrphanFilesAction::new(table.clone())
        .older_than_ms(now_ms() - 3_600_000) // cutoff 1h ago — nothing is that old
        .dry_run(true)
        .execute()
        .await
        .unwrap();

    assert!(
        result.orphan_files.is_empty(),
        "recent files must be protected: {:?}",
        result.orphan_files
    );
    assert_eq!(result.skipped_recent, 1, "the garbage file was age-skipped");
    assert!(table.file_io().exists(&garbage).await.unwrap());
}

/// Delete mode removes the garbage and ONLY the garbage: the table remains
/// fully readable afterwards, and a second run reports zero orphans.
#[tokio::test]
async fn orphan_delete_removes_garbage_and_preserves_table() {
    let warehouse = TempDir::new().unwrap();
    let (_catalog, table) = seed_table(&warehouse).await;
    let g1 = plant_garbage(&table, "zz-garbage-1.parquet").await;
    let g2 = plant_garbage(&table, "zz-garbage-2.parquet").await;

    let result = RemoveOrphanFilesAction::new(table.clone())
        .older_than_ms(now_ms() + 60_000)
        .execute()
        .await
        .unwrap();

    let mut deleted = result.deleted_files.clone();
    deleted.sort();
    let mut expected = vec![g1.clone(), g2.clone()];
    expected.sort();
    assert_eq!(deleted, expected);
    assert!(result.failed_deletes.is_empty());
    assert!(!table.file_io().exists(&g1).await.unwrap());
    assert!(!table.file_io().exists(&g2).await.unwrap());

    // The table is untouched: full scan still returns every row.
    assert_eq!(live_row_count(&table).await, 8);

    // Convergence: a second pass finds nothing.
    let again = RemoveOrphanFilesAction::new(table.clone())
        .older_than_ms(now_ms() + 60_000)
        .dry_run(true)
        .execute()
        .await
        .unwrap();
    assert!(again.orphan_files.is_empty());
}
