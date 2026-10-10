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

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::encryption::kms::KmsClientFactory;
use iceberg::io::StorageFactory;
use iceberg::{Catalog, CatalogBuilder, Error, ErrorKind, Result, Runtime};
use iceberg_catalog_glue::GlueCatalogBuilder;
use iceberg_catalog_hms::HmsCatalogBuilder;
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_catalog_s3tables::S3TablesCatalogBuilder;
use iceberg_catalog_sql::SqlCatalogBuilder;

/// A CatalogBuilderFactory creating a new catalog builder.
type CatalogBuilderFactory = fn() -> Box<dyn BoxedCatalogBuilder>;

/// A registry of catalog builders.
static CATALOG_REGISTRY: &[(&str, CatalogBuilderFactory)] = &[
    ("rest", || Box::new(RestCatalogBuilder::default())),
    ("glue", || Box::new(GlueCatalogBuilder::default())),
    ("s3tables", || Box::new(S3TablesCatalogBuilder::default())),
    ("hms", || Box::new(HmsCatalogBuilder::default())),
    ("sql", || Box::new(SqlCatalogBuilder::default())),
];

/// Return the list of supported catalog types.
pub fn supported_types() -> Vec<&'static str> {
    CATALOG_REGISTRY.iter().map(|(k, _)| *k).collect()
}

#[async_trait]
pub trait BoxedCatalogBuilder: Send {
    /// Sets the storage factory used to build the catalog's `FileIO`; see
    /// [`CatalogBuilder::with_storage_factory`].
    fn with_storage_factory(
        self: Box<Self>,
        storage_factory: Arc<dyn StorageFactory>,
    ) -> Box<dyn BoxedCatalogBuilder>;

    /// Sets the KMS client factory used to enable table encryption; see
    /// [`CatalogBuilder::with_kms_client_factory`].
    fn with_kms_client_factory(
        self: Box<Self>,
        kms_client_factory: Arc<dyn KmsClientFactory>,
    ) -> Box<dyn BoxedCatalogBuilder>;

    /// Sets the runtime the catalog, and the tables it creates, spawn their
    /// tasks on; see [`CatalogBuilder::with_runtime`].
    fn with_runtime(self: Box<Self>, runtime: Runtime) -> Box<dyn BoxedCatalogBuilder>;

    async fn load(
        self: Box<Self>,
        name: String,
        props: HashMap<String, String>,
    ) -> Result<Arc<dyn Catalog>>;
}

#[async_trait]
impl<T: CatalogBuilder + 'static> BoxedCatalogBuilder for T {
    fn with_storage_factory(
        self: Box<Self>,
        storage_factory: Arc<dyn StorageFactory>,
    ) -> Box<dyn BoxedCatalogBuilder> {
        Box::new(CatalogBuilder::with_storage_factory(*self, storage_factory))
    }

    fn with_kms_client_factory(
        self: Box<Self>,
        kms_client_factory: Arc<dyn KmsClientFactory>,
    ) -> Box<dyn BoxedCatalogBuilder> {
        Box::new(CatalogBuilder::with_kms_client_factory(
            *self,
            kms_client_factory,
        ))
    }

    fn with_runtime(self: Box<Self>, runtime: Runtime) -> Box<dyn BoxedCatalogBuilder> {
        Box::new(CatalogBuilder::with_runtime(*self, runtime))
    }

    async fn load(
        self: Box<Self>,
        name: String,
        props: HashMap<String, String>,
    ) -> Result<Arc<dyn Catalog>> {
        let builder = *self;
        Ok(Arc::new(builder.load(name, props).await?) as Arc<dyn Catalog>)
    }
}

/// Load a catalog from a string.
pub fn load(r#type: &str) -> Result<Box<dyn BoxedCatalogBuilder>> {
    let key = r#type.trim();
    if let Some((_, factory)) = CATALOG_REGISTRY
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(key))
    {
        Ok(factory())
    } else {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            format!(
                "Unsupported catalog type: {}. Supported types: {}",
                r#type,
                supported_types().join(", ")
            ),
        ))
    }
}

/// Ergonomic catalog loader builder pattern.
pub struct CatalogLoader<'a> {
    catalog_type: &'a str,
}

impl<'a> From<&'a str> for CatalogLoader<'a> {
    fn from(s: &'a str) -> Self {
        Self { catalog_type: s }
    }
}

impl CatalogLoader<'_> {
    pub async fn load(
        self,
        name: String,
        props: HashMap<String, String>,
    ) -> Result<Arc<dyn Catalog>> {
        let builder = load(self.catalog_type)?;
        builder.load(name, props).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use iceberg::encryption::kms::KmsClientFactory;
    use iceberg::io::{LocalFsStorageFactory, StorageFactory};
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalog, MemoryCatalogBuilder};
    use iceberg::{CatalogBuilder, Result, Runtime};
    use sqlx::migrate::MigrateDatabase;
    use tempfile::TempDir;

    use crate::{BoxedCatalogBuilder, CatalogLoader, load};

    #[tokio::test]
    async fn test_load_unsupported_catalog() {
        let result = load("unsupported");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_catalog_loader_pattern() {
        use iceberg_catalog_rest::REST_CATALOG_PROP_URI;

        let catalog = CatalogLoader::from("rest")
            .load(
                "rest".to_string(),
                HashMap::from([
                    (
                        REST_CATALOG_PROP_URI.to_string(),
                        "http://localhost:8080".to_string(),
                    ),
                    ("key".to_string(), "value".to_string()),
                ]),
            )
            .await;

        assert!(catalog.is_ok());
    }

    #[tokio::test]
    async fn test_catalog_loader_pattern_rest_catalog() {
        use iceberg_catalog_rest::REST_CATALOG_PROP_URI;

        let catalog_loader = load("rest").unwrap();
        let catalog = catalog_loader
            .load(
                "rest".to_string(),
                HashMap::from([
                    (
                        REST_CATALOG_PROP_URI.to_string(),
                        "http://localhost:8080".to_string(),
                    ),
                    ("key".to_string(), "value".to_string()),
                ]),
            )
            .await;

        assert!(catalog.is_ok());
    }

    #[tokio::test]
    async fn test_catalog_loader_pattern_glue_catalog() {
        use iceberg_catalog_glue::GLUE_CATALOG_PROP_WAREHOUSE;

        let catalog_loader = load("glue").unwrap();
        let catalog = catalog_loader
            .load(
                "glue".to_string(),
                HashMap::from([
                    (
                        GLUE_CATALOG_PROP_WAREHOUSE.to_string(),
                        "s3://test".to_string(),
                    ),
                    ("key".to_string(), "value".to_string()),
                ]),
            )
            .await;

        assert!(catalog.is_ok());
    }

    #[tokio::test]
    async fn test_catalog_loader_pattern_s3tables() {
        use iceberg_catalog_s3tables::S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN;

        let catalog = CatalogLoader::from("s3tables")
            .load(
                "s3tables".to_string(),
                HashMap::from([
                    (
                        S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN.to_string(),
                        "arn:aws:s3tables:us-east-1:123456789012:bucket/test".to_string(),
                    ),
                    ("key".to_string(), "value".to_string()),
                ]),
            )
            .await;

        assert!(catalog.is_ok());
    }

    #[tokio::test]
    async fn test_catalog_loader_pattern_hms_catalog() {
        use iceberg_catalog_hms::{HMS_CATALOG_PROP_URI, HMS_CATALOG_PROP_WAREHOUSE};

        let catalog_loader = load("hms").unwrap();
        let catalog = catalog_loader
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "hms".to_string(),
                HashMap::from([
                    (HMS_CATALOG_PROP_URI.to_string(), "127.0.0.1:1".to_string()),
                    (
                        HMS_CATALOG_PROP_WAREHOUSE.to_string(),
                        "s3://warehouse".to_string(),
                    ),
                    ("key".to_string(), "value".to_string()),
                ]),
            )
            .await;

        assert!(catalog.is_ok());
    }

    fn temp_path() -> String {
        let temp_dir = TempDir::new().unwrap();
        temp_dir.path().to_str().unwrap().to_string()
    }

    #[tokio::test]
    async fn test_catalog_loader_pattern_sql_catalog() {
        use iceberg_catalog_sql::{SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE};

        let uri = format!("sqlite:{}", temp_path());
        sqlx::Sqlite::create_database(&uri).await.unwrap();

        let catalog_loader = load("sql").unwrap();
        let catalog = catalog_loader
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "sql".to_string(),
                HashMap::from([
                    (SQL_CATALOG_PROP_URI.to_string(), uri),
                    (
                        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
                        "s3://warehouse".to_string(),
                    ),
                ]),
            )
            .await;

        assert!(catalog.is_ok());
    }

    /// A memory catalog builder that records the runtime it is given.
    #[derive(Debug, Default)]
    struct RuntimeRecordingBuilder {
        runtime: Arc<Mutex<Option<Runtime>>>,
    }

    impl CatalogBuilder for RuntimeRecordingBuilder {
        type C = MemoryCatalog;

        fn with_storage_factory(self, _storage_factory: Arc<dyn StorageFactory>) -> Self {
            self
        }

        fn with_kms_client_factory(self, _kms_client_factory: Arc<dyn KmsClientFactory>) -> Self {
            self
        }

        fn with_runtime(self, runtime: Runtime) -> Self {
            *self.runtime.lock().unwrap() = Some(runtime);
            self
        }

        fn load(
            self,
            name: impl Into<String>,
            props: HashMap<String, String>,
        ) -> impl Future<Output = Result<MemoryCatalog>> + Send {
            MemoryCatalogBuilder::default().load(name, props)
        }
    }

    #[test]
    fn test_with_runtime_reaches_the_catalog_builder() {
        let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("loader-test-runtime")
            .enable_all()
            .build()
            .unwrap();
        let recorded = Arc::new(Mutex::new(None));
        let builder: Box<dyn BoxedCatalogBuilder> = Box::new(RuntimeRecordingBuilder {
            runtime: recorded.clone(),
        });

        tokio_runtime.block_on(async {
            builder
                .with_runtime(Runtime::new(&tokio_runtime))
                .load(
                    "memory".to_string(),
                    HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), temp_path())]),
                )
                .await
                .unwrap();

            // The builder got the runtime passed to the boxed builder: its
            // tasks run on that runtime's threads.
            let runtime = recorded.lock().unwrap().take().unwrap();
            let thread = runtime
                .io()
                .spawn(async { std::thread::current().name().map(str::to_string) })
                .await
                .unwrap();
            assert!(
                thread
                    .as_deref()
                    .is_some_and(|n| n.starts_with("loader-test-runtime")),
                "got: {thread:?}"
            );
        });
    }

    #[tokio::test]
    async fn test_error_message_includes_supported_types() {
        let err = match load("does-not-exist") {
            Ok(_) => panic!("expected error for unsupported type"),
            Err(e) => e,
        };
        let msg = err.message().to_string();
        assert!(msg.contains("Supported types:"));
        // Should include at least the built-in type
        assert!(msg.contains("rest"));
    }
}
