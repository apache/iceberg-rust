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

//! Common unregister-table behavior across catalogs.
//!
//! These tests assume Docker containers are started externally via `make docker-up`.
//! The pinned REST fixture does not advertise the optional unregister endpoint;
//! REST behavior is covered by mock-server tests in `iceberg-catalog-rest`.

mod common;

use std::collections::HashMap;

use common::{CatalogKind, cleanup_namespace_dyn, load_catalog, table_creation};
use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{ErrorKind, NamespaceIdent, Result, TableIdent};
use iceberg_test_utils::normalize_test_name_with_parts;
use rstest::rstest;

#[rstest]
#[case::sql_catalog(CatalogKind::Sql)]
#[case::memory_catalog(CatalogKind::Memory)]
#[tokio::test]
async fn test_catalog_unregister_table_roundtrip(#[case] kind: CatalogKind) -> Result<()> {
    let Some(harness) = load_catalog(kind).await else {
        return Ok(());
    };
    let catalog = harness.catalog;
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!(
        "catalog_unregister_table_roundtrip",
        harness.label
    ));
    cleanup_namespace_dyn(catalog.as_ref(), &namespace).await;
    catalog.create_namespace(&namespace, HashMap::new()).await?;

    let table = catalog
        .create_table(
            &namespace,
            table_creation(normalize_test_name_with_parts!(
                "catalog_unregister_table_roundtrip",
                harness.label,
                "source"
            )),
        )
        .await?;
    let source_ident = table.identifier().clone();
    let data_file_path = format!("{}/data/00000.parquet", table.metadata().location());
    table
        .file_io()
        .new_output(&data_file_path)?
        .write(vec![0; 16].into())
        .await?;
    let data_file = DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(data_file_path.clone())
        .file_format(DataFileFormat::Parquet)
        .partition(Struct::empty())
        .record_count(1)
        .file_size_in_bytes(16)
        .partition_spec_id(table.metadata().default_partition_spec_id())
        .build()
        .unwrap();
    let tx = Transaction::new(&table);
    tx.fast_append()
        .add_data_files(vec![data_file])
        .apply(tx)?
        .commit(catalog.as_ref())
        .await?;
    let committed = catalog.load_table(&source_ident).await?;

    let unregistered = catalog.unregister_table(&source_ident).await?;
    assert_eq!(unregistered.identifier(), &source_ident);
    assert_eq!(
        unregistered.metadata_location(),
        committed.metadata_location()
    );
    assert_eq!(
        unregistered.metadata().current_snapshot(),
        committed.metadata().current_snapshot()
    );
    assert!(unregistered.readonly());
    assert!(!catalog.table_exists(&source_ident).await?);
    assert!(unregistered.file_io().exists(&data_file_path).await?);

    let destination_ident =
        TableIdent::new(namespace, format!("{}_registered", source_ident.name()));
    let registered = catalog
        .register_table(
            &destination_ident,
            unregistered.metadata_location().unwrap().to_string(),
        )
        .await?;
    assert_eq!(registered.identifier(), &destination_ident);
    assert_eq!(
        registered.metadata().current_snapshot(),
        committed.metadata().current_snapshot()
    );
    Ok(())
}

#[rstest]
#[case::sql_catalog(CatalogKind::Sql)]
#[case::memory_catalog(CatalogKind::Memory)]
#[tokio::test]
async fn test_catalog_unregister_missing_table(#[case] kind: CatalogKind) -> Result<()> {
    let Some(harness) = load_catalog(kind).await else {
        return Ok(());
    };
    let catalog = harness.catalog;
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!(
        "catalog_unregister_missing_table",
        harness.label
    ));
    cleanup_namespace_dyn(catalog.as_ref(), &namespace).await;
    catalog.create_namespace(&namespace, HashMap::new()).await?;

    let ident = TableIdent::new(namespace, "missing".to_string());
    let err = catalog.unregister_table(&ident).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::TableNotFound);
    Ok(())
}

#[rstest]
#[case::hms_catalog(CatalogKind::Hms)]
#[case::s3tables_catalog(CatalogKind::S3Tables)]
#[case::glue_catalog(CatalogKind::Glue)]
#[tokio::test]
async fn test_catalog_unregister_unsupported(#[case] kind: CatalogKind) -> Result<()> {
    let Some(harness) = load_catalog(kind).await else {
        return Ok(());
    };
    let ident = TableIdent::new(NamespaceIdent::new("any".to_string()), "table".to_string());
    let err = harness.catalog.unregister_table(&ident).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);
    Ok(())
}
