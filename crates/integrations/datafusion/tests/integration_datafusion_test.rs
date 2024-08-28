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

//! Integration tests for Iceberg Datafusion with Hive Metastore.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::execution::context::SessionContext;
use iceberg::io::FileIOBuilder;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, NamespaceIdent, Result, TableCreation};
use iceberg_catalog_memory::MemoryCatalog;
use iceberg_datafusion::IcebergCatalogProvider;
use tempfile::TempDir;

fn temp_path() -> String {
    let temp_dir = TempDir::new().unwrap();
    temp_dir.path().to_str().unwrap().to_string()
}

fn get_iceberg_catalog() -> MemoryCatalog {
    let file_io = FileIOBuilder::new_fs_io().build().unwrap();
    MemoryCatalog::new(file_io, Some(temp_path()))
}

async fn set_test_namespace(catalog: &MemoryCatalog, namespace: &NamespaceIdent) -> Result<()> {
    let properties = HashMap::new();

    catalog.create_namespace(namespace, properties).await?;

    Ok(())
}

fn set_table_creation(location: impl ToString, name: impl ToString) -> Result<TableCreation> {
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "foo", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "bar", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;

    let creation = TableCreation::builder()
        .location(location.to_string())
        .name(name.to_string())
        .properties(HashMap::new())
        .schema(schema)
        .build();

    Ok(creation)
}

#[tokio::test]
async fn test_provider_get_table_schema() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog();
    let namespace = NamespaceIdent::new("test_provider_get_table_schema".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let creation = set_table_creation(temp_path(), "my_table")?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    let provider = ctx.catalog("catalog").unwrap();
    let schema = provider.schema("test_provider_get_table_schema").unwrap();

    let table = schema.table("my_table").await.unwrap().unwrap();
    let table_schema = table.schema();

    let expected = [("foo", &DataType::Int32), ("bar", &DataType::Utf8)];

    for (field, exp) in table_schema.fields().iter().zip(expected.iter()) {
        assert_eq!(field.name(), exp.0);
        assert_eq!(field.data_type(), exp.1);
        assert!(!field.is_nullable())
    }

    Ok(())
}

#[tokio::test]
async fn test_provider_list_table_names() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog();
    let namespace = NamespaceIdent::new("test_provider_list_table_names".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let creation = set_table_creation(temp_path(), "my_table")?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    let provider = ctx.catalog("catalog").unwrap();
    let schema = provider.schema("test_provider_list_table_names").unwrap();

    let expected = vec!["my_table"];
    let result = schema.table_names();

    assert_eq!(result, expected);

    Ok(())
}

#[tokio::test]
async fn test_provider_list_schema_names() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog();
    let namespace = NamespaceIdent::new("test_provider_list_schema_names".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    set_table_creation("test_provider_list_schema_names", "my_table")?;
    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    let provider = ctx.catalog("catalog").unwrap();

    let expected = ["test_provider_list_schema_names"];
    let result = provider.schema_names();

    assert!(expected
        .iter()
        .all(|item| result.contains(&item.to_string())));
    Ok(())
}
