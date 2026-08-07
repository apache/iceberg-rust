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

//! Integration tests for hms catalog.
//!
//! These tests assume Docker containers are started externally via `make docker-up`.
//! Each test uses unique namespaces based on module path to avoid conflicts.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::io::{
    FileIOBuilder, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION,
    S3_SECRET_ACCESS_KEY,
};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, Namespace, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_hms::{
    HMS_CATALOG_PROP_THRIFT_TRANSPORT, HMS_CATALOG_PROP_URI, HMS_CATALOG_PROP_WAREHOUSE,
    HmsCatalog, HmsCatalogBuilder, THRIFT_TRANSPORT_BUFFERED,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use iceberg_test_utils::{get_hms_endpoint, get_minio_endpoint, set_up};
use tokio::time::sleep;
use tracing::info;

type Result<T> = std::result::Result<T, iceberg::Error>;

async fn get_catalog() -> HmsCatalog {
    get_catalog_with_props(HashMap::new()).await
}

async fn get_catalog_with_optimistic_locking() -> HmsCatalog {
    use iceberg_catalog_hms::HMS_HIVE_LOCKS_DISABLED;
    let mut extra_props = HashMap::new();
    extra_props.insert(HMS_HIVE_LOCKS_DISABLED.to_string(), "true".to_string());
    get_catalog_with_props(extra_props).await
}

async fn get_catalog_with_props(extra_props: HashMap<String, String>) -> HmsCatalog {
    set_up();

    let hms_endpoint = get_hms_endpoint();
    let minio_endpoint = get_minio_endpoint();

    let mut props = HashMap::from([
        (HMS_CATALOG_PROP_URI.to_string(), hms_endpoint),
        (
            HMS_CATALOG_PROP_THRIFT_TRANSPORT.to_string(),
            THRIFT_TRANSPORT_BUFFERED.to_string(),
        ),
        (
            HMS_CATALOG_PROP_WAREHOUSE.to_string(),
            "s3a://warehouse/hive".to_string(),
        ),
        (S3_ENDPOINT.to_string(), minio_endpoint),
        (S3_ACCESS_KEY_ID.to_string(), "admin".to_string()),
        (S3_SECRET_ACCESS_KEY.to_string(), "password".to_string()),
        (S3_REGION.to_string(), "us-east-1".to_string()),
        (S3_PATH_STYLE_ACCESS.to_string(), "true".to_string()),
    ]);

    // Merge in extra properties
    props.extend(extra_props);

    // Wait for bucket to actually exist
    let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::S3 {
        customized_credential_load: None,
    }))
    .with_props(props.clone())
    .build();

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

    HmsCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
        .load("hms", props)
        .await
        .unwrap()
}

async fn set_test_namespace(catalog: &HmsCatalog, namespace: &NamespaceIdent) -> Result<()> {
    catalog.create_namespace(namespace, HashMap::new()).await?;

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
async fn test_get_default_namespace() -> Result<()> {
    let catalog = get_catalog().await;

    let ns = Namespace::new(NamespaceIdent::new("default".into()));
    let properties = HashMap::from([
        ("location".to_string(), "s3a://warehouse/hive".to_string()),
        (
            "hive.metastore.database.owner-type".to_string(),
            "Role".to_string(),
        ),
        ("comment".to_string(), "Default Hive database".to_string()),
        (
            "hive.metastore.database.owner".to_string(),
            "public".to_string(),
        ),
    ]);

    let expected = Namespace::with_properties(NamespaceIdent::new("default".into()), properties);

    let result = catalog.get_namespace(ns.name()).await?;

    assert_eq!(expected, result);

    Ok(())
}

#[tokio::test]
async fn test_namespace_exists() -> Result<()> {
    let catalog = get_catalog().await;

    let ns_exists = Namespace::new(NamespaceIdent::new("default".into()));
    let ns_not_exists = Namespace::new(NamespaceIdent::new("test_namespace_exists".into()));

    let result_exists = catalog.namespace_exists(ns_exists.name()).await?;
    let result_not_exists = catalog.namespace_exists(ns_not_exists.name()).await?;

    assert!(result_exists);
    assert!(!result_not_exists);

    Ok(())
}

#[tokio::test]
async fn test_update_namespace() -> Result<()> {
    let catalog = get_catalog().await;

    let ns = NamespaceIdent::new("test_update_namespace".into());
    set_test_namespace(&catalog, &ns).await?;
    let properties = HashMap::from([("comment".to_string(), "my_update".to_string())]);

    catalog.update_namespace(&ns, properties).await?;

    let db = catalog.get_namespace(&ns).await?;

    assert_eq!(
        db.properties().get("comment"),
        Some(&"my_update".to_string())
    );

    Ok(())
}

#[tokio::test]
async fn test_drop_namespace() -> Result<()> {
    let catalog = get_catalog().await;

    let ns = Namespace::new(NamespaceIdent::new("delete_me".into()));

    catalog.create_namespace(ns.name(), HashMap::new()).await?;

    let result = catalog.namespace_exists(ns.name()).await?;
    assert!(result);

    catalog.drop_namespace(ns.name()).await?;

    let result = catalog.namespace_exists(ns.name()).await?;
    assert!(!result);

    Ok(())
}

#[tokio::test]
async fn test_update_table() -> Result<()> {
    let catalog = get_catalog().await;
    let creation = set_table_creation(None, "my_table")?;
    let namespace = Namespace::new(NamespaceIdent::new("test_update_table".into()));
    set_test_namespace(&catalog, namespace.name()).await?;

    let expected = catalog.create_table(namespace.name(), creation).await?;

    let table = catalog
        .load_table(&TableIdent::new(
            namespace.name().clone(),
            "my_table".to_string(),
        ))
        .await?;

    assert_eq!(table.identifier(), expected.identifier());
    assert_eq!(table.metadata_location(), expected.metadata_location());
    assert_eq!(table.metadata(), expected.metadata());
    let original_metadata_location = table.metadata_location();
    let tx = Transaction::new(&table);
    let tx = tx
        .update_table_properties()
        .set("test_property".to_string(), "test_value".to_string())
        .apply(tx)?;

    let updated_table = tx.commit(&catalog).await?;

    assert_eq!(
        updated_table.metadata().properties().get("test_property"),
        Some(&"test_value".to_string())
    );
    assert_ne!(
        updated_table.metadata_location(),
        original_metadata_location,
        "Metadata location should be updated after commit"
    );

    let reloaded_table = catalog.load_table(table.identifier()).await?;

    assert_eq!(
        reloaded_table.metadata().properties().get("test_property"),
        Some(&"test_value".to_string())
    );
    assert_eq!(
        reloaded_table.metadata_location(),
        updated_table.metadata_location(),
        "Reloaded table should have the same metadata location as the updated table"
    );

    Ok(())
}

#[tokio::test]
async fn test_update_table_with_optimistic_locking() -> Result<()> {
    let catalog = get_catalog_with_optimistic_locking().await;
    let creation = set_table_creation(None, "my_table")?;
    let namespace = Namespace::new(NamespaceIdent::new("test_update_table_optimistic".into()));
    set_test_namespace(&catalog, namespace.name()).await?;

    let expected = catalog.create_table(namespace.name(), creation).await?;

    let table = catalog
        .load_table(&TableIdent::new(
            namespace.name().clone(),
            "my_table".to_string(),
        ))
        .await?;

    assert_eq!(table.identifier(), expected.identifier());
    assert_eq!(table.metadata_location(), expected.metadata_location());
    assert_eq!(table.metadata(), expected.metadata());
    let original_metadata_location = table.metadata_location();

    let tx = Transaction::new(&table);
    let tx = tx
        .update_table_properties()
        .set(
            "test_property_optimistic".to_string(),
            "test_value_optimistic".to_string(),
        )
        .apply(tx)?;

    let updated_table = tx.commit(&catalog).await?;

    assert_eq!(
        updated_table
            .metadata()
            .properties()
            .get("test_property_optimistic"),
        Some(&"test_value_optimistic".to_string())
    );

    assert_ne!(
        updated_table.metadata_location(),
        original_metadata_location,
        "Metadata location should be updated after commit with optimistic locking"
    );

    let reloaded_table = catalog.load_table(table.identifier()).await?;
    assert_eq!(
        reloaded_table
            .metadata()
            .properties()
            .get("test_property_optimistic"),
        Some(&"test_value_optimistic".to_string())
    );
    assert_eq!(
        reloaded_table.metadata_location(),
        updated_table.metadata_location(),
        "Reloaded table should have the same metadata location as the updated table"
    );

    Ok(())
}
