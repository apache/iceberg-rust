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

use arrow_array::{Int64Array, StringArray};
use futures::TryStreamExt;
use iceberg::expr::Reference;
use iceberg::spec::Datum;
use iceberg::{Catalog, TableIdent};
use iceberg_catalog_rest::RestCatalog;

use crate::get_shared_containers;

#[tokio::test]
async fn test_evolved_schema() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalog::new(fixture.catalog_config.clone());

    let table = rest_catalog
        .load_table(&TableIdent::from_strs(["default", "test_rename_column"]).unwrap())
        .await
        .unwrap();

    let predicate = Reference::new("language").not_equal_to(Datum::string("Rust"));

    let scan = table.scan().with_filter(predicate).build();
    let batch_stream = scan.unwrap().to_arrow().await.unwrap();

    let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

    let mut actual = vec![
        batches[0]
            .column_by_name("language")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        batches[1]
            .column_by_name("language")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
    ];

    actual.sort();

    assert_eq!(actual, vec!["Java", "Python"]);

    // Evolve column
    let table = rest_catalog
        .load_table(&TableIdent::from_strs(["default", "test_promote_column"]).unwrap())
        .await
        .unwrap();

    // Does not yet work, somewhere in the reader
    // let predicate =
    //     Reference::new("foo").not_equal_to(Datum::int(22));

    let scan = table.scan().build();
    let batch_stream = scan.unwrap().to_arrow().await.unwrap();

    let batches: Vec<_> = batch_stream.try_collect().await.unwrap();
    let mut actual = vec![
        batches[0]
            .column_by_name("foo")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        batches[1]
            .column_by_name("foo")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
    ];

    actual.sort();

    assert_eq!(actual, vec![19, 25]);
}
