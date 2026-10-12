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

//! Integration tests for scans of tables whose schema changed after their last
//! write, so their current snapshot was written with an older schema.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use futures::TryStreamExt;
use iceberg::expr::Reference;
use iceberg::scan::TableScanBuilder;
use iceberg::spec::Datum;
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_integration_tests::get_test_fixture;
use iceberg_storage_opendal::OpenDalStorageFactory;

async fn load_table(name: &str) -> Table {
    let fixture = get_test_fixture();
    let rest_catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    rest_catalog
        .load_table(&TableIdent::from_strs(["default", name]).unwrap())
        .await
        .unwrap()
}

async fn read(scan: TableScanBuilder<'_>) -> Vec<RecordBatch> {
    let batch_stream = scan.build().unwrap().to_arrow().await.unwrap();
    batch_stream.try_collect().await.unwrap()
}

fn column_names(batches: &[RecordBatch]) -> Vec<String> {
    let schema = batches[0].schema();
    schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}

/// The `id` and string column `name` of each row, ordered by `id`, since the
/// data files are read in no fixed order.
fn string_rows(batches: &[RecordBatch], name: &str) -> Vec<(i32, Option<String>)> {
    let mut rows: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_primitive::<Int32Type>();
            let values = batch.column_by_name(name).unwrap().as_string::<i32>();
            ids.values()
                .iter()
                .copied()
                .zip(values.iter().map(|value| value.map(str::to_string)))
                .collect::<Vec<_>>()
        })
        .collect();
    rows.sort();
    rows
}

#[tokio::test]
async fn test_scan_reads_column_added_after_last_write() {
    let table = load_table("test_add_column_after_last_write").await;

    // Null, as the data files predate the column.
    let current = read(table.scan()).await;
    assert_eq!(column_names(&current), ["id", "added"]);
    assert_eq!(string_rows(&current, "added"), [(1, None), (2, None)]);

    // A scan pinned to the snapshot uses the schema it was written with.
    let snapshot_id = table.metadata().current_snapshot_id().unwrap();
    let error = table
        .scan()
        .snapshot_id(snapshot_id)
        .select(["added"])
        .build()
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        concat!(
            "DataInvalid => Column added not found in table. Schema: table {\n",
            "  1: id: optional int \n",
            "}\n",
        )
    );
}

#[tokio::test]
async fn test_scan_filters_on_column_added_after_last_write() {
    let table = load_table("test_add_column_after_last_write").await;

    let is_null = Reference::new("added").is_null();
    let filtered = read(table.scan().with_filter(is_null)).await;
    assert_eq!(string_rows(&filtered, "added"), [(1, None), (2, None)]);

    let equal_to_a = Reference::new("added").equal_to(Datum::string("a"));
    let filtered = read(table.scan().with_filter(equal_to_a)).await;
    assert_eq!(string_rows(&filtered, "added"), []);
}

#[tokio::test]
async fn test_scan_excludes_column_dropped_after_last_write() {
    let table = load_table("test_drop_column_after_last_write").await;

    let current = read(table.scan()).await;
    assert_eq!(column_names(&current), ["id"]);
    let error = table.scan().select(["val"]).build().unwrap_err();
    assert_eq!(
        error.to_string(),
        concat!(
            "DataInvalid => Column val not found in table. Schema: table {\n",
            "  1: id: optional int \n",
            "}\n",
        )
    );

    // A scan pinned to the snapshot still reads the column.
    let snapshot_id = table.metadata().current_snapshot_id().unwrap();
    let pinned = read(table.scan().snapshot_id(snapshot_id)).await;
    assert_eq!(column_names(&pinned), ["id", "val"]);
    assert_eq!(string_rows(&pinned, "val"), [
        (1, Some("a".to_string())),
        (2, Some("b".to_string()))
    ]);
}

/// Columns are matched to data files by field id, so a column dropped and
/// added back under the same name reads as null, not as the old column's
/// values.
#[tokio::test]
async fn test_scan_reads_null_for_column_readded_after_last_write() {
    let table = load_table("test_readd_column_after_last_write").await;

    let current = read(table.scan()).await;
    assert_eq!(column_names(&current), ["id", "val"]);
    assert_eq!(string_rows(&current, "val"), [(1, None), (2, None)]);

    let snapshot_id = table.metadata().current_snapshot_id().unwrap();
    let pinned = read(table.scan().snapshot_id(snapshot_id)).await;
    assert_eq!(column_names(&pinned), ["id", "val"]);
    assert_eq!(string_rows(&pinned, "val"), [
        (1, Some("a".to_string())),
        (2, Some("b".to_string()))
    ]);
}

#[tokio::test]
async fn test_scan_reads_column_renamed_after_last_write() {
    let table = load_table("test_rename_column_after_last_write").await;

    let current = read(table.scan()).await;
    assert_eq!(column_names(&current), ["id", "renamed"]);
    assert_eq!(string_rows(&current, "renamed"), [
        (1, Some("a".to_string())),
        (2, Some("b".to_string()))
    ]);
}
