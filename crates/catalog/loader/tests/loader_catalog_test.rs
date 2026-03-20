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

//! Catalog tests for schema evolution with `MemoryCatalog`.

use std::collections::HashMap;

use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{AddColumn, ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, ErrorKind, NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

fn base_schema() -> Schema {
    Schema::builder()
        .with_fields(vec![
            NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
        ])
        .with_identifier_field_ids(vec![2])
        .build()
        .unwrap()
}

async fn new_catalog() -> (iceberg::MemoryCatalog, TempDir) {
    let warehouse = TempDir::new().unwrap();
    let catalog = MemoryCatalogBuilder::default()
        .load(
            "memory",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                warehouse.path().to_string_lossy().to_string(),
            )]),
        )
        .await
        .unwrap();

    (catalog, warehouse)
}

async fn create_table(catalog: &iceberg::MemoryCatalog, table_name: &str) -> TableIdent {
    let ns = NamespaceIdent::new("schema_evolution".to_string());
    if catalog.get_namespace(&ns).await.is_err() {
        catalog.create_namespace(&ns, HashMap::new()).await.unwrap();
    }

    let table_ident = TableIdent::new(ns.clone(), table_name.to_string());
    let _ = catalog.drop_table(&table_ident).await;

    catalog
        .create_table(
            &ns,
            TableCreation::builder()
                .name(table_name.to_string())
                .schema(base_schema())
                .build(),
        )
        .await
        .unwrap();

    table_ident
}

#[tokio::test]
async fn test_add_field_with_memory_catalog() {
    let (catalog, _warehouse) = new_catalog().await;
    let table_ident = create_table(&catalog, "t_add_field").await;
    let table = catalog.load_table(&table_ident).await.unwrap();

    let tx = Transaction::new(&table);
    let tx = tx
        .update_schema()
        .add_column(AddColumn::optional("a", Type::Primitive(PrimitiveType::Int)))
        .apply(tx)
        .unwrap();

    let updated_table = tx.commit(&catalog).await.unwrap();
    let schema = updated_table.metadata().current_schema();

    let field_a = schema.field_by_name("a").expect("a should exist");
    assert_eq!(field_a.id, 4);
    assert_eq!(*field_a.field_type, Type::Primitive(PrimitiveType::Int));
}

#[tokio::test]
async fn test_add_nested_and_delete_field_with_memory_catalog() {
    let (catalog, _warehouse) = new_catalog().await;
    let table_ident = create_table(&catalog, "t_add_nested_delete").await;
    let table = catalog.load_table(&table_ident).await.unwrap();

    let tx = Transaction::new(&table);
    let tx = tx
        .update_schema()
        .add_column(AddColumn::optional(
            "info",
            Type::Struct(iceberg::spec::StructType::new(vec![
                NestedField::optional(
                    0,
                    "city",
                    Type::Primitive(PrimitiveType::String),
                )
                .into(),
            ])),
        ))
        .apply(tx)
        .unwrap();
    let table = tx.commit(&catalog).await.unwrap();

    let tx = Transaction::new(&table);
    let tx = tx
        .update_schema()
        .add_column(
            AddColumn::builder()
                .name("zip")
                .field_type(Type::Primitive(PrimitiveType::String))
                .parent("info")
                .build(),
        )
        .delete_column("baz")
        .apply(tx)
        .unwrap();
    let table = tx.commit(&catalog).await.unwrap();

    let schema = table.metadata().current_schema();
    assert!(schema.field_by_name("info").is_some());
    assert!(schema.field_by_name("info.city").is_some());
    assert!(schema.field_by_name("info.zip").is_some());
    assert!(schema.field_by_name("baz").is_none());
}

#[tokio::test]
async fn test_delete_identifier_and_missing_field_fail_with_memory_catalog() {
    let (catalog, _warehouse) = new_catalog().await;
    let table_ident = create_table(&catalog, "t_delete_failures").await;
    let table = catalog.load_table(&table_ident).await.unwrap();

    let tx = Transaction::new(&table);
    let tx = tx.update_schema().delete_column("bar").apply(tx).unwrap();
    let err = tx.commit(&catalog).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::PreconditionFailed);

    let tx = Transaction::new(&table);
    let tx = tx
        .update_schema()
        .delete_column("nonexistent")
        .apply(tx)
        .unwrap();
    let err = tx.commit(&catalog).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
}
