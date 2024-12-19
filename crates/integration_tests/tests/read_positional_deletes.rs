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

use iceberg::ErrorKind::FeatureUnsupported;
use iceberg::{Catalog, TableIdent};
use iceberg_integration_tests::set_test_fixture;

#[tokio::test]
async fn test_read_table_with_positional_deletes() {
    let fixture = set_test_fixture("read_table_with_positional_deletes").await;

    let catalog = fixture.rest_catalog;

    let table = catalog
        .load_table(
            &TableIdent::from_strs(["default", "test_positional_merge_on_read_double_deletes"])
                .unwrap(),
        )
        .await
        .unwrap();

    let scan = table.scan().build().unwrap();
    println!("{:?}", scan);

    assert!(scan
        .to_arrow()
        .await
        .is_err_and(|e| e.kind() == FeatureUnsupported));

    // 😱 If we don't support positional deletes, we should fail when we try to read a table that
    // has positional deletes. The table has 12 rows, and 2 are deleted, see provision.py

    // When we get support for it:
    // let batch_stream = scan.to_arrow().await.unwrap();
    // let batches: Vec<_> = batch_stream.try_collect().await.is_err();
    // let num_rows: usize = batches.iter().map(|v| v.num_rows()).sum();
    // assert_eq!(num_rows, 10);
}
