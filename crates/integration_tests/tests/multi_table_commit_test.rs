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

//! Integration tests for atomic multi-table commits in the REST catalog.

mod common;

use std::sync::Arc;

use common::{random_ns, test_schema};
use iceberg::spec::NullOrder;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, ErrorKind, NamespaceIdent, TableCommit, TableCreation};
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder};
use iceberg_integration_tests::get_test_fixture;
use iceberg_storage_opendal::OpenDalStorageFactory;

async fn catalog() -> RestCatalog {
    RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
        .load("rest", get_test_fixture().catalog_config.clone())
        .await
        .unwrap()
}

async fn create_table(catalog: &RestCatalog, ns: &NamespaceIdent, name: &str) -> Table {
    let creation = TableCreation::builder()
        .name(name.to_string())
        .schema(test_schema())
        .build();
    catalog.create_table(ns, creation).await.unwrap()
}

async fn set_property(table: &Table) -> TableCommit {
    let tx = Transaction::new(table);
    let mut tx = tx
        .update_table_properties()
        .set("k".to_string(), "v".to_string())
        .apply(tx)
        .unwrap();
    tx.prepare_commit().await.unwrap()
}

async fn property(catalog: &RestCatalog, table: &Table) -> Option<String> {
    let table = catalog.load_table(table.identifier()).await.unwrap();
    table.metadata().properties().get("k").cloned()
}

#[tokio::test]
async fn test_commit_transaction_updates_all_tables() {
    let catalog = catalog().await;
    let ns = random_ns().await;
    let a = create_table(&catalog, ns.name(), "a").await;
    let b = create_table(&catalog, ns.name(), "b").await;

    catalog
        .commit_transaction(vec![set_property(&a).await, set_property(&b).await])
        .await
        .unwrap();

    assert_eq!(property(&catalog, &a).await.as_deref(), Some("v"));
    assert_eq!(property(&catalog, &b).await.as_deref(), Some("v"));
}

#[tokio::test]
async fn test_commit_transaction_conflict_updates_no_table() {
    let catalog = catalog().await;
    let ns = random_ns().await;
    let a = create_table(&catalog, ns.name(), "a").await;
    let b = create_table(&catalog, ns.name(), "b").await;

    let tx = Transaction::new(&a);
    let mut stale_a = tx
        .replace_sort_order()
        .asc("foo", NullOrder::First)
        .apply(tx)
        .unwrap();
    let tx = Transaction::new(&a);
    tx.replace_sort_order()
        .asc("bar", NullOrder::First)
        .apply(tx)
        .unwrap()
        .commit(&catalog)
        .await
        .unwrap();

    let err = catalog
        .commit_transaction(vec![
            stale_a.prepare_commit().await.unwrap(),
            set_property(&b).await,
        ])
        .await
        .unwrap_err();

    assert_eq!(err.kind(), ErrorKind::CatalogCommitConflicts);
    assert_eq!(property(&catalog, &b).await, None);
}
