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

use iceberg::{Catalog, NamespaceIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    // ANCHOR: create_catalog
    // Create catalog
    let config = RestCatalogConfig::builder()
        .uri("http://localhost:8080".to_string())
        .build();

    let catalog = RestCatalog::new(config).await.unwrap();
    // ANCHOR_END: create_catalog

    // ANCHOR: list_all_namespace
    // List all namespaces
    let all_namespaces = catalog.list_namespaces(None).await.unwrap();
    println!("Namespaces in current catalog: {:?}", all_namespaces);
    // ANCHOR_END: list_all_namespace

    // ANCHOR: create_namespace
    let namespace_id =
        NamespaceIdent::from_vec(vec!["ns1".to_string(), "ns11".to_string()]).unwrap();
    // Create namespace
    let ns = catalog
        .create_namespace(
            &namespace_id,
            HashMap::from([("key1".to_string(), "value1".to_string())]),
        )
        .await
        .unwrap();

    println!("Namespace created: {:?}", ns);
    // ANCHOR_END: create_namespace
}
