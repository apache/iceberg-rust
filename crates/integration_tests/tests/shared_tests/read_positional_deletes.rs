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

use futures::TryStreamExt;
use iceberg::ErrorKind::FeatureUnsupported;
use iceberg::{Catalog, TableIdent};
use iceberg_catalog_rest::RestCatalog;

use crate::get_shared_containers;

#[tokio::test]
async fn test_read_table_with_positional_deletes_with_delete_support_disabled() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalog::new(fixture.catalog_config.clone());

    let table = rest_catalog
        .load_table(
            &TableIdent::from_strs(["default", "test_positional_merge_on_read_double_deletes"])
                .unwrap(),
        )
        .await
        .unwrap();

    let scan = table
        .scan()
        .with_delete_file_processing_enabled(false)
        .build()
        .unwrap();
    println!("{:?}", scan);

    let plan: Vec<_> = scan
        .plan_files()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    println!("{:?}", plan);

    // Scan plan phase stills include delete files in file plan
    // when with_delete_file_processing_enabled == false. We instead
    // fail at the read phase after this.
    assert_eq!(plan[0].deletes.len(), 2);

    // with delete_file_processing_enabled == false, we should fail when we
    // try to read a table that has positional deletes.
    let result = scan.to_arrow().await.unwrap().try_collect::<Vec<_>>().await;

    assert!(result.is_err_and(|e| e.kind() == FeatureUnsupported));
}
