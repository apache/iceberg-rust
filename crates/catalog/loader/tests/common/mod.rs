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

//! Shared helpers for catalog integration suites.

#![allow(dead_code)]

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};
use iceberg_catalog_glue::{
    AWS_ACCESS_KEY_ID, AWS_REGION_NAME, AWS_SECRET_ACCESS_KEY, GLUE_CATALOG_PROP_URI,
    GLUE_CATALOG_PROP_WAREHOUSE, GlueCatalog, GlueCatalogBuilder,
};
use iceberg_catalog_hms::{
    HMS_CATALOG_PROP_THRIFT_TRANSPORT, HMS_CATALOG_PROP_URI, HMS_CATALOG_PROP_WAREHOUSE,
    HmsCatalog, HmsCatalogBuilder, THRIFT_TRANSPORT_BUFFERED,
};
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, RestCatalog, RestCatalogBuilder};
use iceberg_catalog_s3tables::{
    S3TABLES_CATALOG_PROP_ENDPOINT_URL, S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN,
    S3TablesCatalogBuilder,
};
use iceberg_catalog_sql::{
    SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlBindStyle,
    SqlCatalogBuilder,
};
use iceberg_test_utils::{
    get_glue_endpoint, get_hms_endpoint, get_minio_endpoint, get_rest_catalog_endpoint, set_up,
};
use sqlx::migrate::MigrateDatabase;
use tempfile::TempDir;
use tokio::time::sleep;

#[derive(Debug, Clone, Copy)]
pub enum CatalogKind {
    Rest,
    Glue,
    Hms,
    Sql,
    S3Tables,
    Memory,
}

impl fmt::Display for CatalogKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            CatalogKind::Rest => "rest_catalog",
            CatalogKind::Glue => "glue_catalog",
            CatalogKind::Hms => "hms_catalog",
            CatalogKind::Sql => "sql_catalog",
            CatalogKind::S3Tables => "s3tables_catalog",
            CatalogKind::Memory => "memory_catalog",
        };
        f.write_str(name)
    }
}

pub struct CatalogHarness {
    pub catalog: Arc<dyn Catalog>,
    pub label: &'static str,
    _tempdirs: Vec<TempDir>,
}

// Shared setup for each catalog implementation so the suites exercise
// the same behavior against all backends.
pub async fn load_catalog(kind: CatalogKind) -> Option<CatalogHarness> {
    set_up();
    match kind {
        CatalogKind::Rest => Some(CatalogHarness {
            catalog: Arc::new(rest_catalog().await) as Arc<dyn Catalog>,
            label: "rest",
            _tempdirs: Vec::new(),
        }),
        CatalogKind::Glue => Some(CatalogHarness {
            catalog: Arc::new(glue_catalog().await) as Arc<dyn Catalog>,
            label: "glue",
            _tempdirs: Vec::new(),
        }),
        CatalogKind::Hms => Some(CatalogHarness {
            catalog: Arc::new(hms_catalog().await) as Arc<dyn Catalog>,
            label: "hms",
            _tempdirs: Vec::new(),
        }),
        CatalogKind::Sql => {
            let warehouse_dir = TempDir::new().unwrap();
            let db_dir = TempDir::new().unwrap();
            let db_path = db_dir.path().join("catalog.db");
            let db_uri = format!("sqlite:{}", db_path.to_str().unwrap());
            sqlx::Sqlite::create_database(&db_uri).await.unwrap();

            let catalog = SqlCatalogBuilder::default()
                .load(
                    "sql",
                    HashMap::from([
                        (SQL_CATALOG_PROP_URI.to_string(), db_uri),
                        (
                            SQL_CATALOG_PROP_WAREHOUSE.to_string(),
                            warehouse_dir.path().to_str().unwrap().to_string(),
                        ),
                        (
                            SQL_CATALOG_PROP_BIND_STYLE.to_string(),
                            SqlBindStyle::QMark.to_string(),
                        ),
                    ]),
                )
                .await
                .unwrap();

            Some(CatalogHarness {
                catalog: Arc::new(catalog) as Arc<dyn Catalog>,
                label: "sql",
                _tempdirs: vec![warehouse_dir, db_dir],
            })
        }
        CatalogKind::S3Tables => {
            let table_bucket_arn = match std::env::var("TABLE_BUCKET_ARN").ok() {
                Some(value) => value,
                None => return None,
            };

            let mut props = HashMap::from([(
                S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN.to_string(),
                table_bucket_arn,
            )]);

            if let Ok(endpoint_url) = std::env::var("S3TABLES_ENDPOINT_URL") {
                props.insert(S3TABLES_CATALOG_PROP_ENDPOINT_URL.to_string(), endpoint_url);
            }

            let catalog = S3TablesCatalogBuilder::default()
                .load("s3tables", props)
                .await
                .unwrap();

            Some(CatalogHarness {
                catalog: Arc::new(catalog) as Arc<dyn Catalog>,
                label: "s3tables",
                _tempdirs: Vec::new(),
            })
        }
        CatalogKind::Memory => {
            let warehouse_dir = TempDir::new().unwrap();
            let props = HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                warehouse_dir.path().to_str().unwrap().to_string(),
            )]);
            let catalog = MemoryCatalogBuilder::default()
                .load("memory", props)
                .await
                .unwrap();

            Some(CatalogHarness {
                catalog: Arc::new(catalog) as Arc<dyn Catalog>,
                label: "memory",
                _tempdirs: vec![warehouse_dir],
            })
        }
    }
}

// Catalog-specific setup is intentionally isolated here so the suites
// remain implementation-agnostic.
async fn rest_catalog() -> RestCatalog {
    let rest_endpoint = get_rest_catalog_endpoint();

    let client = reqwest::Client::new();
    let mut retries = 0;
    while retries < 30 {
        if client
            .get(format!("{rest_endpoint}/v1/config"))
            .send()
            .await
            .map(|resp| resp.status().is_success())
            .unwrap_or(false)
        {
            break;
        }
        sleep(std::time::Duration::from_millis(1000)).await;
        retries += 1;
    }

    RestCatalogBuilder::default()
        .load(
            "rest",
            HashMap::from([(REST_CATALOG_PROP_URI.to_string(), rest_endpoint)]),
        )
        .await
        .unwrap()
}

async fn glue_catalog() -> GlueCatalog {
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

    let file_io = iceberg::io::FileIO::from_path("s3a://")
        .unwrap()
        .with_props(props.clone())
        .build()
        .unwrap();

    let mut retries = 0;
    while retries < 30 {
        if file_io.exists("s3a://warehouse/").await.unwrap_or(false) {
            break;
        }
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
    glue_props.extend(props);

    GlueCatalogBuilder::default()
        .load("glue", glue_props)
        .await
        .unwrap()
}

async fn hms_catalog() -> HmsCatalog {
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

    let file_io = iceberg::io::FileIO::from_path("s3a://")
        .unwrap()
        .with_props(props.clone())
        .build()
        .unwrap();

    let mut retries = 0;
    while retries < 30 {
        if file_io.exists("s3a://warehouse/").await.unwrap_or(false) {
            break;
        }
        sleep(std::time::Duration::from_millis(1000)).await;
        retries += 1;
    }

    HmsCatalogBuilder::default()
        .load("hms", props)
        .await
        .unwrap()
}

// Common table schema used across suites to validate shared behavior.
pub fn table_creation(name: impl ToString) -> TableCreation {
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "foo", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "bar", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .unwrap();

    TableCreation::builder()
        .name(name.to_string())
        .properties(HashMap::new())
        .schema(schema)
        .build()
}

pub fn assert_map_contains(expected: &HashMap<String, String>, actual: &HashMap<String, String>) {
    for (key, value) in expected {
        assert_eq!(actual.get(key), Some(value));
    }
}

pub async fn cleanup_namespace_dyn(catalog: &dyn Catalog, namespace: &NamespaceIdent) {
    if let Ok(tables) = catalog.list_tables(namespace).await {
        for table in tables {
            let _ = catalog.drop_table(&table).await;
        }
    }
    let _ = catalog.drop_namespace(namespace).await;
}
