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

//! Integration tests for rest catalog.

use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use futures::FutureExt;
use iceberg::io::FileIO;
use iceberg::spec::{
    DataFile, ManifestEntry, ManifestFile, ManifestStatus, ManifestWriterBuilder, SnapshotRef,
};
use iceberg::table::Table;
use iceberg::transaction::{
    Transaction, CREATED_MANIFESTS_COUNT, KEPT_MANIFESTS_COUNT, PROCESSED_ENTRY_COUNT,
    REPLACED_MANIFESTS_COUNT,
};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, TableCreation};
use iceberg_catalog_rest::RestCatalog;
use parquet::file::properties::WriterProperties;

use crate::get_shared_containers;
use crate::shared_tests::{random_ns, test_schema};

async fn generate_data_file(table: &Table, name: &str) -> Vec<DataFile> {
    let schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        name.to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(
        parquet_writer_builder,
        None,
        table.metadata().default_partition_spec_id(),
    );
    let mut data_file_writer = data_file_writer_builder.build().await.unwrap();
    let col1 = StringArray::from(vec![Some("foo"), Some("bar"), None, Some("baz")]);
    let col2 = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
    let col3 = BooleanArray::from(vec![Some(true), Some(false), None, Some(false)]);
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(col1) as ArrayRef,
        Arc::new(col2) as ArrayRef,
        Arc::new(col3) as ArrayRef,
    ])
    .unwrap();
    data_file_writer.write(batch.clone()).await.unwrap();
    data_file_writer.close().await.unwrap()
}

async fn write_v2_data_manifest_file(
    table: &Table,
    file_name: &str,
    entry: impl IntoIterator<Item = ManifestEntry>,
) -> ManifestFile {
    let manifest_file_writer = ManifestWriterBuilder::new(
        table
            .file_io()
            .new_output(format!(
                "{}/meta/{}",
                table.metadata().location(),
                file_name
            ))
            .unwrap(),
        None,
        vec![],
        table.metadata().current_schema().clone(),
        table.metadata().default_partition_spec().as_ref().clone(),
    );
    let mut writer = manifest_file_writer.build_v2_data();
    for entry in entry {
        writer
            .add_existing_file(
                entry.data_file().clone(),
                entry.snapshot_id().unwrap(),
                entry.sequence_number().unwrap(),
                entry.file_sequence_number(),
            )
            .unwrap();
    }
    writer.write_manifest_file().await.unwrap()
}

fn validate_summary(
    snapshot: &SnapshotRef,
    replaced: usize,
    kept: usize,
    created: usize,
    entry_count: usize,
) {
    let summary = snapshot.summary();
    assert_eq!(
        summary
            .additional_properties
            .get(REPLACED_MANIFESTS_COUNT)
            .unwrap(),
        &replaced.to_string()
    );
    assert_eq!(
        summary
            .additional_properties
            .get(KEPT_MANIFESTS_COUNT)
            .unwrap(),
        &kept.to_string()
    );
    assert_eq!(
        summary
            .additional_properties
            .get(CREATED_MANIFESTS_COUNT)
            .unwrap(),
        &created.to_string()
    );
    assert_eq!(
        summary
            .additional_properties
            .get(PROCESSED_ENTRY_COUNT)
            .unwrap(),
        &entry_count.to_string()
    );
}

async fn validate_manifest_entry(
    manifest: &ManifestFile,
    file_io: &FileIO,
    snapshot_id: impl IntoIterator<Item = i64>,
    expected_files: impl IntoIterator<Item = DataFile>,
    expected_statuses: impl IntoIterator<Item = ManifestStatus>,
) {
    let mut expect_snapshot_id = snapshot_id.into_iter();
    let mut expected_files = expected_files.into_iter();
    let mut expected_statuses = expected_statuses.into_iter();
    let manifest = manifest.load_manifest(file_io).await.unwrap();
    for e in manifest.entries() {
        assert_eq!(e.file_path(), expected_files.next().unwrap().file_path());
        assert_eq!(e.snapshot_id().unwrap(), expect_snapshot_id.next().unwrap());
        assert_eq!(e.status(), expected_statuses.next().unwrap());
    }
}

#[tokio::test]
async fn test_rewrite_manifest_append_directly() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalog::new(fixture.catalog_config.clone());
    let ns = random_ns().await;
    let schema = test_schema();

    // Create table
    let table_creation = TableCreation::builder()
        .name("t1".to_string())
        .schema(schema.clone())
        .build();
    let table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    // Append a data file
    let data_file = generate_data_file(&table, "d1").await;
    let tx = Transaction::new(&table);
    let mut append_action = tx.fast_append(None, vec![]).unwrap();
    append_action.add_data_files(data_file.clone()).unwrap();
    let table = append_action
        .apply()
        .await
        .unwrap()
        .commit(&rest_catalog)
        .await
        .unwrap();
    let snapshot_id = table.metadata().current_snapshot_id().unwrap();

    // Rewrite manifest diretcly
    let tx = Transaction::new(&table);
    let rewrite_action = tx
        .rewrite_manifest(
            Some(Box::new(|_data_file| "".to_string())),
            None,
            None,
            None,
            vec![],
        )
        .unwrap();
    let table = rewrite_action
        .apply()
        .await
        .unwrap()
        .commit(&rest_catalog)
        .await
        .unwrap();

    // Check result
    assert!(table.metadata().current_snapshot_id().unwrap() != snapshot_id);
    let manifest_list = table
        .metadata()
        .current_snapshot()
        .unwrap()
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert!(manifest_list.entries().len() == 1);
    validate_manifest_entry(
        &manifest_list.entries()[0],
        table.file_io(),
        vec![snapshot_id],
        data_file,
        vec![ManifestStatus::Existing],
    )
    .await;
}

#[tokio::test]
async fn test_rewrite_manifest_append_directly_combine() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalog::new(fixture.catalog_config.clone());
    let ns = random_ns().await;
    let schema = test_schema();

    // Create table
    let table_creation = TableCreation::builder()
        .name("t1".to_string())
        .schema(schema.clone())
        .build();
    let mut table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    // Append data file twice to generate two manifest file
    for i in 0..2 {
        let data_file = generate_data_file(&table, &i.to_string()).await;
        let tx = Transaction::new(&table);
        let mut append_action = tx.fast_append(None, vec![]).unwrap();
        append_action.add_data_files(data_file.clone()).unwrap();
        table = append_action
            .apply()
            .await
            .unwrap()
            .commit(&rest_catalog)
            .await
            .unwrap();
    }
    let manifest_list = table
        .metadata()
        .current_snapshot()
        .unwrap()
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert_eq!(manifest_list.entries().len(), 2);
    let expect_entries = {
        let mut entries = vec![];
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            assert!(manifest.entries().len() == 1);
            entries.push(manifest.entries()[0].clone());
        }
        entries
    };

    // Rewrite manifest diretcly
    let tx = Transaction::new(&table);
    let rewrite_action = tx
        .rewrite_manifest(
            Some(Box::new(|_data_file| "".to_string())),
            None,
            None,
            None,
            vec![],
        )
        .unwrap();
    let table = rewrite_action
        .apply()
        .await
        .unwrap()
        .commit(&rest_catalog)
        .await
        .unwrap();

    // Check result
    let manifest_list = table
        .metadata()
        .current_snapshot()
        .unwrap()
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert!(manifest_list.entries().len() == 1);
    let manifest_file = manifest_list.entries()[0]
        .load_manifest(table.file_io())
        .await
        .unwrap();
    assert!(manifest_file.entries().len() == 2);
    for entry in manifest_file.entries() {
        expect_entries
            .iter()
            .find(|e| e.file_path() == entry.file_path())
            .unwrap();
        expect_entries
            .iter()
            .find(|e| e.snapshot_id() == entry.snapshot_id())
            .unwrap();
        assert_eq!(entry.status(), ManifestStatus::Existing);
    }
}

#[tokio::test]
async fn test_rewrite_manifest_separate() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalog::new(fixture.catalog_config.clone());
    let ns = random_ns().await;
    let schema = test_schema();

    // Create table
    let table_creation = TableCreation::builder()
        .name("t1".to_string())
        .schema(schema.clone())
        .build();
    let mut table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    // Append two data file
    let data_file1 = generate_data_file(&table, "d1").await;
    let data_file2 = generate_data_file(&table, "d2").await;
    let tx = Transaction::new(&table);
    let mut append_action = tx.fast_append(None, vec![]).unwrap();
    append_action
        .add_data_files(data_file1.into_iter().chain(data_file2.into_iter()))
        .unwrap();
    table = append_action
        .apply()
        .await
        .unwrap()
        .commit(&rest_catalog)
        .await
        .unwrap();
    let manifest_list = table
        .metadata()
        .current_snapshot()
        .unwrap()
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert_eq!(manifest_list.entries().len(), 1);
    let manifest = manifest_list.entries()[0]
        .load_manifest(table.file_io())
        .await
        .unwrap();
    assert_eq!(manifest.entries().len(), 2);
    let (expect_entries, _) = manifest.into_parts();

    // Rewrite manifest to separate two data file
    let tx = Transaction::new(&table);
    let rewrite_action = tx
        .rewrite_manifest(
            Some(Box::new(|data_file| data_file.file_path().to_string())),
            None,
            None,
            None,
            vec![],
        )
        .unwrap();
    let table = rewrite_action
        .apply()
        .await
        .unwrap()
        .commit(&rest_catalog)
        .await
        .unwrap();

    // Check result
    let manifest_list = table
        .metadata()
        .current_snapshot()
        .unwrap()
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert_eq!(manifest_list.entries().len(), 2);
    for manifest_file in manifest_list.entries() {
        let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
        assert!(manifest.entries().len() == 1);
        let entry = manifest.entries()[0].clone();
        expect_entries
            .iter()
            .find(|e| e.file_path() == entry.file_path())
            .unwrap();
        expect_entries
            .iter()
            .find(|e| e.snapshot_id() == entry.snapshot_id())
            .unwrap();
        assert_eq!(entry.status(), ManifestStatus::Existing);
    }
}

#[tokio::test]
async fn test_rewrite_manifest_filter() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalog::new(fixture.catalog_config.clone());
    let ns = random_ns().await;
    let schema = test_schema();

    // Create table
    let table_creation = TableCreation::builder()
        .name("t1".to_string())
        .schema(schema.clone())
        .build();
    let mut table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    for i in 1..4 {
        let data_file = generate_data_file(&table, &format!("d{}", i)).await;
        let tx = Transaction::new(&table);
        let mut append_action = tx.fast_append(None, vec![]).unwrap();
        append_action.add_data_files(data_file.clone()).unwrap();
        table = append_action
            .apply()
            .await
            .unwrap()
            .commit(&rest_catalog)
            .await
            .unwrap();
    }
    let manifest_list = table
        .metadata()
        .current_snapshot()
        .unwrap()
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert_eq!(manifest_list.entries().len(), 3);
    let mut expect_entries = vec![];
    let mut expect_entry3 = vec![];
    for manifest_file in manifest_list.entries() {
        let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
        assert!(manifest.entries().len() == 1);
        if manifest.entries()[0].file_path().contains("d3") {
            expect_entry3.push(manifest.entries()[0].clone());
        } else {
            expect_entries.push(manifest.entries()[0].clone());
        }
    }
    assert_eq!(expect_entry3.len(), 1);
    assert_eq!(expect_entries.len(), 2);

    // Rewrite manifest to separate two data file
    let tx = Transaction::new(&table);
    let file_io_clone = table.file_io().clone();
    let rewrite_action = tx
        .rewrite_manifest(
            Some(Box::new(|_data_file| "file".to_string())),
            Some(Box::new(move |entry| {
                let entry_clone = entry.clone();
                let file_io = file_io_clone.clone();
                async move {
                    let manifest = entry_clone.load_manifest(&file_io).await.unwrap();
                    !manifest
                        .entries()
                        .iter()
                        .any(|e| e.file_path().contains("d3"))
                }
                .boxed()
            })),
            None,
            None,
            vec![],
        )
        .unwrap();
    let table = rewrite_action
        .apply()
        .await
        .unwrap()
        .commit(&rest_catalog)
        .await
        .unwrap();

    // Check result
    let manifest_list = table
        .metadata()
        .current_snapshot()
        .unwrap()
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert_eq!(manifest_list.entries().len(), 2);
    for manifest_file in manifest_list.entries() {
        let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
        assert!(manifest.entries().len() == 1 || manifest.entries().len() == 2);
        if manifest.entries().len() == 1 {
            let entry = manifest.entries()[0].clone();
            assert_eq!(entry.file_path(), expect_entry3[0].file_path());
            assert_eq!(entry.snapshot_id(), expect_entry3[0].snapshot_id());
            assert_eq!(entry.status(), ManifestStatus::Added);
        } else {
            for entry in manifest.entries() {
                expect_entries
                    .iter()
                    .find(|e| e.file_path() == entry.file_path())
                    .unwrap();
                expect_entries
                    .iter()
                    .find(|e| e.snapshot_id() == entry.snapshot_id())
                    .unwrap();
                assert_eq!(entry.status(), ManifestStatus::Existing);
            }
        }
    }
}

// # TODO
// Test rewrite manifest target size
// # TODO
// Test concurrent append with rewrite manifest

#[tokio::test]
async fn test_basic_manifest_replacement() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalog::new(fixture.catalog_config.clone());
    let ns = random_ns().await;
    let schema = test_schema();

    // Create table
    let table_creation = TableCreation::builder()
        .name("t1".to_string())
        .schema(schema.clone())
        .build();
    let mut table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    // Append twice and each time append two data file
    let data_file1 = generate_data_file(&table, "d1-1").await;
    let data_file2 = generate_data_file(&table, "d1-2").await;
    let tx = Transaction::new(&table);
    let mut append_action = tx.fast_append(None, vec![]).unwrap();
    append_action
        .add_data_files(data_file1.iter().chain(data_file2.iter()).cloned())
        .unwrap();
    table = append_action
        .apply()
        .await
        .unwrap()
        .commit(&rest_catalog)
        .await
        .unwrap();
    let data_file3 = generate_data_file(&table, "d2-1").await;
    let data_file4 = generate_data_file(&table, "d2-2").await;
    let tx = Transaction::new(&table);
    let mut append_action = tx.fast_append(None, vec![]).unwrap();
    append_action
        .add_data_files(data_file3.iter().chain(data_file4.iter()).cloned())
        .unwrap();
    table = append_action
        .apply()
        .await
        .unwrap()
        .commit(&rest_catalog)
        .await
        .unwrap();
    let manifest_list = table
        .metadata()
        .current_snapshot()
        .unwrap()
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert_eq!(manifest_list.entries().len(), 2);
    assert_eq!(manifest_list.entries()[0].added_files_count, Some(2));
    assert_eq!(manifest_list.entries()[1].added_files_count, Some(2));

    // Separate the first manifest file into two manifest files manually
    let first_snapshot_id = manifest_list.entries()[1].added_snapshot_id;
    let second_snapshot_id = manifest_list.entries()[0].added_snapshot_id;
    let delete_manifest = manifest_list.entries()[1].clone();
    let added_manifest1 =
        write_v2_data_manifest_file(&table, "m1-1", vec![ManifestEntry::builder()
            .status(ManifestStatus::Existing)
            .snapshot_id(first_snapshot_id)
            .sequence_number(0)
            .file_sequence_number(0)
            .data_file(data_file1[0].clone())
            .build()])
        .await;
    let added_manifest2 =
        write_v2_data_manifest_file(&table, "m1-2", vec![ManifestEntry::builder()
            .status(ManifestStatus::Existing)
            .snapshot_id(first_snapshot_id)
            .sequence_number(0)
            .file_sequence_number(0)
            .data_file(data_file2[0].clone())
            .build()])
        .await;
    let mut rewrite_manifest = Transaction::new(&table)
        .rewrite_manifest::<()>(None, None, None, None, vec![])
        .unwrap();
    rewrite_manifest.delete_manifest(delete_manifest).unwrap();
    rewrite_manifest.add_manifest(added_manifest1).unwrap();
    rewrite_manifest.add_manifest(added_manifest2).unwrap();
    let table = rewrite_manifest
        .apply()
        .await
        .unwrap()
        .commit(&rest_catalog)
        .await
        .unwrap();

    // Check result
    let manifest_list = table
        .metadata()
        .current_snapshot()
        .unwrap()
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert_eq!(manifest_list.entries().len(), 3);
    assert!(manifest_list.entries()[0].manifest_path.contains("m1-1"));
    assert!(manifest_list.entries()[1].manifest_path.contains("m1-2"));
    validate_summary(table.metadata().current_snapshot().unwrap(), 1, 1, 2, 0);
    println!("{:?}", manifest_list.entries());
    validate_manifest_entry(
        &manifest_list.entries()[0],
        table.file_io(),
        vec![first_snapshot_id],
        data_file1,
        vec![ManifestStatus::Existing],
    )
    .await;
    validate_manifest_entry(
        &manifest_list.entries()[1],
        table.file_io(),
        vec![first_snapshot_id],
        data_file2,
        vec![ManifestStatus::Existing],
    )
    .await;
    validate_manifest_entry(
        &manifest_list.entries()[2],
        table.file_io(),
        vec![second_snapshot_id, second_snapshot_id],
        data_file3.iter().chain(data_file4.iter()).cloned(),
        vec![ManifestStatus::Added; 2],
    )
    .await;
}
