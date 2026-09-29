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

//! REST pagination against the Docker fixture started by `make docker-up`.

mod common;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use common::{cleanup_namespace_dyn, table_creation};
use iceberg::io::LocalFsStorageFactory;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, Result};
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, RestCatalogBuilder};
use iceberg_test_utils::{get_rest_catalog_endpoint, normalize_test_name_with_parts, set_up};

#[tokio::test]
async fn test_rest_lists_all_namespaces_and_tables_with_page_size() -> Result<()> {
    set_up();
    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .with_page_size(1)
        .load(
            "rest",
            HashMap::from([(
                REST_CATALOG_PROP_URI.to_string(),
                get_rest_catalog_endpoint(),
            )]),
        )
        .await?;
    let parent_name = normalize_test_name_with_parts!("pagination");
    let parent = NamespaceIdent::new(parent_name.clone());
    let expected_namespaces = ["child1", "child2", "child3"]
        .into_iter()
        .map(|name| NamespaceIdent::from_strs([parent_name.as_str(), name]))
        .collect::<Result<Vec<_>>>()?;

    // Isolate the listing from other suites and clean up remnants of failed runs.
    for child in &expected_namespaces {
        cleanup_namespace_dyn(&catalog, child).await;
    }
    cleanup_namespace_dyn(&catalog, &parent).await;
    catalog.create_namespace(&parent, HashMap::new()).await?;
    for child in &expected_namespaces {
        catalog.create_namespace(child, HashMap::new()).await?;
    }
    let mut expected_tables = Vec::new();
    for name in ["table1", "table2", "table3"] {
        let table = catalog.create_table(&parent, table_creation(name)).await?;
        expected_tables.push(table.identifier().clone());
    }

    let namespaces = catalog.list_namespaces(Some(&parent)).await;
    let tables = catalog.list_tables(&parent).await;

    // Clean up before assertions so a pagination regression does not leave data behind.
    for table in &expected_tables {
        catalog.purge_table(table).await?;
    }
    for child in &expected_namespaces {
        catalog.drop_namespace(child).await?;
    }
    catalog.drop_namespace(&parent).await?;

    let namespaces = namespaces?;
    let tables = tables?;

    assert_eq!(namespaces.len(), expected_namespaces.len());
    assert_eq!(tables.len(), expected_tables.len());
    assert_eq!(
        namespaces.into_iter().collect::<HashSet<_>>(),
        expected_namespaces.into_iter().collect::<HashSet<_>>()
    );
    assert_eq!(
        tables.into_iter().collect::<HashSet<_>>(),
        expected_tables.into_iter().collect::<HashSet<_>>()
    );
    Ok(())
}
