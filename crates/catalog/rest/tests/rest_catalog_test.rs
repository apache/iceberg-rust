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
//!
//! These tests assume Docker containers are started externally via `make docker-up`.
//! Each test uses unique namespaces based on module path to avoid conflicts.

use std::collections::HashMap;

use iceberg::spec::Schema;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, RestCatalog, RestCatalogBuilder};
use iceberg_test_utils::{
    cleanup_namespace, get_rest_catalog_endpoint, normalize_test_name_with_parts, set_up,
};
use tokio::time::sleep;
use tracing::info;

async fn get_catalog() -> RestCatalog {
    set_up();

    let rest_endpoint = get_rest_catalog_endpoint();

    // Wait for catalog to be ready
    let client = reqwest::Client::new();
    let mut retries = 0;
    while retries < 30 {
        match client
            .get(format!("{rest_endpoint}/v1/config"))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                info!("REST catalog is ready at {}", rest_endpoint);
                break;
            }
            _ => {
                info!(
                    "Waiting for REST catalog to be ready... (attempt {})",
                    retries + 1
                );
                sleep(std::time::Duration::from_millis(1000)).await;
                retries += 1;
            }
        }
    }

    RestCatalogBuilder::default()
        .load(
            "rest",
            HashMap::from([(REST_CATALOG_PROP_URI.to_string(), rest_endpoint)]),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn test_register_table() {
    let catalog = get_catalog().await;

    // Create unique namespace to avoid conflicts
    let ns = NamespaceIdent::new(normalize_test_name_with_parts!("test_register_table"));

    // Clean up from any previous test runs
    cleanup_namespace(&catalog, &ns).await;

    catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

    // Create the table, store the metadata location, drop the table
    let empty_schema = Schema::builder().build().unwrap();
    let table_creation = TableCreation::builder()
        .name("t1".to_string())
        .schema(empty_schema)
        .build();

    let table = catalog.create_table(&ns, table_creation).await.unwrap();

    let metadata_location = table.metadata_location().unwrap();
    catalog.drop_table(table.identifier()).await.unwrap();

    let new_table_identifier = TableIdent::new(ns.clone(), "t2".to_string());
    let table_registered = catalog
        .register_table(&new_table_identifier, metadata_location.to_string())
        .await
        .unwrap();

    assert_eq!(
        table.metadata_location(),
        table_registered.metadata_location()
    );
    assert_ne!(
        table.identifier().to_string(),
        table_registered.identifier().to_string()
    );
}
