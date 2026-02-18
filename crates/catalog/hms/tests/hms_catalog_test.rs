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

use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::{Catalog, CatalogBuilder, Namespace, NamespaceIdent};
use iceberg_catalog_hms::{
    HMS_CATALOG_PROP_THRIFT_TRANSPORT, HMS_CATALOG_PROP_URI, HMS_CATALOG_PROP_WAREHOUSE,
    HmsCatalog, HmsCatalogBuilder, THRIFT_TRANSPORT_BUFFERED,
};
use iceberg_test_utils::{get_hms_endpoint, get_minio_endpoint, set_up};
use tokio::time::sleep;
use tracing::info;

type Result<T> = std::result::Result<T, iceberg::Error>;

async fn get_catalog() -> HmsCatalog {
    set_up();

    let hms_endpoint = get_hms_endpoint();
    let minio_endpoint = get_minio_endpoint();

    let props = HashMap::from([
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

    HmsCatalogBuilder::default()
        .load("hms", props)
        .await
        .unwrap()
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
