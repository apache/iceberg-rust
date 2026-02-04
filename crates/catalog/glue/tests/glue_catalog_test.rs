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

//! Integration tests for glue catalog.
//!
//! These tests assume Docker containers are started externally via `make docker-up`.
//! Each test uses unique namespaces based on module path to avoid conflicts.

use std::collections::HashMap;

use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, Result, TableCreation, TableIdent};
use iceberg_catalog_glue::{
    AWS_ACCESS_KEY_ID, AWS_REGION_NAME, AWS_SECRET_ACCESS_KEY, GLUE_CATALOG_PROP_URI,
    GLUE_CATALOG_PROP_WAREHOUSE, GlueCatalog, GlueCatalogBuilder,
};
use iceberg_test_utils::{
    cleanup_namespace, get_glue_endpoint, get_minio_endpoint, normalize_test_name_with_parts,
    set_up,
};
use tokio::time::sleep;
use tracing::info;

async fn get_catalog() -> GlueCatalog {
    set_up();

    let glue_endpoint = get_glue_endpoint();
    let minio_endpoint = get_minio_endpoint();

    let props = HashMap::from([
        (AWS_ACCESS_KEY_ID.to_string(), "my_access_id".to_string()),
        (
            AWS_SECRET_ACCESS_KEY.to_string(),
            "my_secret_key".to_string(),
        ),
        (AWS_REGION_NAME.to_string(), "us-east-1".to_string()),
        (S3_ENDPOINT.to_string(), minio_endpoint),
        (S3_ACCESS_KEY_ID.to_string(), "admin".to_string()),
        (S3_SECRET_ACCESS_KEY.to_string(), "password".to_string()),
        (S3_REGION.to_string(), "us-east-1".to_string()),
    ]);

    // Wait for bucket to actually exist
    let file_io = iceberg::io::FileIO::from_path("s3a://")
        .unwrap()
        .with_props(props.clone())
        .build()
        .unwrap();

    let mut retries = 0;
    while retries < 30 {
        if file_io.exists("s3a://warehouse/").await.unwrap_or(false) {
            info!("S3 bucket 'warehouse' is ready");
            break;
        }
        info!("Waiting for bucket creation... (attempt {})", retries + 1);
        sleep(std::time::Duration::from_millis(1000)).await;
        retries += 1;
    }

    let mut glue_props = HashMap::from([
        (GLUE_CATALOG_PROP_URI.to_string(), glue_endpoint),
        (
            GLUE_CATALOG_PROP_WAREHOUSE.to_string(),
            "s3a://warehouse/hive".to_string(),
        ),
    ]);
    glue_props.extend(props.clone());

    GlueCatalogBuilder::default()
        .load("glue", glue_props)
        .await
        .unwrap()
}

async fn set_test_namespace(catalog: &GlueCatalog, namespace: &NamespaceIdent) -> Result<()> {
    let properties = HashMap::new();
    catalog.create_namespace(namespace, properties).await?;

    Ok(())
}

fn set_table_creation(location: Option<String>, name: impl ToString) -> Result<TableCreation> {
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "foo", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "bar", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;

    let builder = TableCreation::builder()
        .name(name.to_string())
        .properties(HashMap::new())
        .location_opt(location)
        .schema(schema);

    Ok(builder.build())
}

#[tokio::test]
async fn test_register_table() -> Result<()> {
    let catalog = get_catalog().await;
    // Use unique namespace to avoid conflicts
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!("test_register_table"));
    cleanup_namespace(&catalog, &namespace).await;
    set_test_namespace(&catalog, &namespace).await?;

    let location = format!("s3a://warehouse/hive/{namespace}");
    let creation = set_table_creation(Some(location), "my_table")?;
    let table = catalog.create_table(&namespace, creation).await?;
    let metadata_location = table
        .metadata_location()
        .expect("Expected metadata location to be set")
        .to_string();

    catalog.drop_table(table.identifier()).await?;
    let ident = TableIdent::new(namespace.clone(), "my_table".to_string());

    let registered = catalog
        .register_table(&ident, metadata_location.clone())
        .await?;

    assert_eq!(registered.identifier(), &ident);
    assert_eq!(
        registered.metadata_location(),
        Some(metadata_location.as_str())
    );

    Ok(())
}
