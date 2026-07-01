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

//! Integration tests for predicate evaluation with Reference and Transform terms.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use futures::TryStreamExt;
use iceberg::expr::{Predicate, Reference, TransformTerm};
use iceberg::spec::Datum;
use iceberg::{Catalog, CatalogBuilder, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_integration_tests::get_test_fixture;
use iceberg_storage_opendal::OpenDalStorageFactory;

async fn load_table(table_name: &str) -> iceberg::table::Table {
    let fixture = get_test_fixture();
    let rest_catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    rest_catalog
        .load_table(&TableIdent::from_strs(["default", table_name]).unwrap())
        .await
        .unwrap()
}

async fn scan_i64_values(table_name: &str, predicate: Predicate, column_name: &str) -> Vec<i64> {
    let table = load_table(table_name).await;
    let scan = table.scan().with_filter(predicate).build();
    let batch_stream = scan.unwrap().to_arrow().await.unwrap();
    let batches: Vec<_> = batch_stream.try_collect().await.unwrap();
    collect_i64_values(&batches, column_name)
}

fn collect_i64_values(batches: &[RecordBatch], column_name: &str) -> Vec<i64> {
    let mut values = batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column_by_name(column_name)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..array.len())
                .map(|index| array.value(index))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    values.sort();
    values
}

#[tokio::test]
async fn test_predicate_with_reference_term() {
    let actual = scan_i64_values(
        "test_promote_column",
        Reference::new("foo").not_equal_to(Datum::int(22)),
        "foo",
    )
    .await;

    assert_eq!(actual, vec![19, 25]);
}

#[tokio::test]
async fn test_bucket_transform_predicate_prunes_partitions() {
    let actual = scan_i64_values(
        "test_transform_bucket_filter",
        TransformTerm::bucket("id", 10).equal_to(Datum::int(9)),
        "id",
    )
    .await;

    assert_eq!(actual, vec![34]);
}

#[tokio::test]
async fn test_year_transform_predicate_prunes_partitions() {
    let actual = scan_i64_values(
        "test_transform_year_filter",
        TransformTerm::year("event_ts").equal_to(Datum::int(54)),
        "id",
    )
    .await;

    assert_eq!(actual, vec![202406, 202412]);
}
