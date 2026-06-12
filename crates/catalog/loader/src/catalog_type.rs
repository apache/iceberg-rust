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

//! Typed selection of the built-in catalog implementations.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::io::StorageFactory;
use iceberg::memory::MemoryCatalogBuilder;
use iceberg::{Catalog, Result};
use iceberg_catalog_glue::GlueCatalogBuilder;
use iceberg_catalog_hms::HmsCatalogBuilder;
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_catalog_s3tables::S3TablesCatalogBuilder;
use iceberg_catalog_sql::SqlCatalogBuilder;

use crate::BoxedCatalogBuilder;

/// A declared selector for one of the built-in catalog implementations.
///
/// Use this over a bare type string when the catalog is known ahead of
/// time: the compiler then checks the selection exhaustively, and a typo
/// cannot silently fall through to an "unsupported type" error at runtime.
//
// Derive `Clone` but not `Copy`: a future revision may add a string-carrying
// escape-hatch variant for out-of-tree catalogs, which would not be `Copy`.
// Omitting `Copy` now keeps that addition from removing a derive that downstream
// code may have come to rely on. `load` takes `self` by value, so it works
// regardless.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CatalogType {
    /// REST catalog.
    Rest,
    /// AWS Glue catalog.
    Glue,
    /// Amazon S3 Tables catalog.
    S3Tables,
    /// Hive Metastore catalog.
    Hms,
    /// SQL catalog.
    Sql,
    /// In-memory catalog.
    Memory,
}

impl CatalogType {
    /// Construct the selected catalog and load it from `name` and `props`.
    ///
    /// `storage_factory` controls how the catalog builds
    /// [`FileIO`](iceberg::io::FileIO) for data and metadata files:
    ///
    /// - `Some(factory)` is forwarded to whichever catalog is selected, so the
    ///   same call site can pin one storage backend across every variant.
    /// - `None` defers to the catalog's own backend. [`Glue`](CatalogType::Glue)
    ///   and [`S3Tables`](CatalogType::S3Tables) are AWS-native and default to
    ///   S3; [`Memory`](CatalogType::Memory) defaults to in-memory storage.
    ///   Each of those has one unambiguous backend, so it loads. The
    ///   storage-agnostic catalogs ([`Rest`](CatalogType::Rest),
    ///   [`Sql`](CatalogType::Sql), and [`Hms`](CatalogType::Hms)) have no
    ///   inherent backend; rather than silently writing to some fabricated
    ///   default location, they require a factory and return an error when one
    ///   is not supplied.
    ///
    /// This method never substitutes a stand-in storage location: a load either
    /// uses a backend the caller can name, or it fails.
    pub async fn load(
        self,
        name: impl Into<String>,
        props: HashMap<String, String>,
        storage_factory: Option<Arc<dyn StorageFactory>>,
    ) -> Result<Arc<dyn Catalog>> {
        // Exhaustive match: adding a variant without an arm is a compile error,
        // so the set of loadable built-ins cannot drift out of sync with the enum.
        let builder: Box<dyn BoxedCatalogBuilder> = match self {
            CatalogType::Rest => Box::new(RestCatalogBuilder::default()),
            CatalogType::Glue => Box::new(GlueCatalogBuilder::default()),
            CatalogType::S3Tables => Box::new(S3TablesCatalogBuilder::default()),
            CatalogType::Hms => Box::new(HmsCatalogBuilder::default()),
            CatalogType::Sql => Box::new(SqlCatalogBuilder::default()),
            CatalogType::Memory => Box::new(MemoryCatalogBuilder::default()),
        };

        // Forward the caller's factory when present and otherwise leave the
        // builder untouched. No fallback factory is injected here: catalogs with
        // one unambiguous backend (Glue/S3Tables on S3, Memory in-memory) apply
        // it themselves, while the storage-agnostic catalogs (Sql/Hms) surface
        // an error rather than papering over a missing backend with a silent
        // default location.
        let builder = match storage_factory {
            Some(factory) => builder.with_storage_factory(factory),
            None => builder,
        };
        builder.load(name.into(), props).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use iceberg::ErrorKind;
    use iceberg::io::MemoryStorageFactory;
    use iceberg::memory::MEMORY_CATALOG_WAREHOUSE;
    use iceberg_catalog_rest::REST_CATALOG_PROP_URI;
    use tempfile::TempDir;

    use super::CatalogType;

    // Building a REST catalog is lazy: the builder validates props and stores
    // them without contacting the server or constructing FileIO, so this needs
    // no running service and no storage factory. (REST resolves its backend on
    // first file access, which is its own upstream design, not a default this
    // selector injects.)
    #[tokio::test]
    async fn test_rest_loads_without_service() {
        CatalogType::Rest
            .load(
                "rest",
                HashMap::from([(
                    REST_CATALOG_PROP_URI.to_string(),
                    "http://localhost:8080".to_string(),
                )]),
                None,
            )
            .await
            .expect("REST should build without a storage factory");
    }

    // The memory catalog was previously absent from the string registry; this
    // exercises that it is now reachable through the typed selector.
    #[tokio::test]
    async fn test_memory_loads_via_typed_selector() {
        let warehouse = TempDir::new().expect("temp dir should be creatable");
        CatalogType::Memory
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    warehouse
                        .path()
                        .to_str()
                        .expect("temp dir path should be valid UTF-8")
                        .to_string(),
                )]),
                None,
            )
            .await
            .expect("memory catalog should load via the typed selector");
    }

    // SQL is storage-agnostic: it has no inherent backend, so loading it without
    // a factory must fail rather than silently fall back to a fabricated
    // location. The missing-factory check in `SqlCatalog::new` fires before any
    // database connection, so no sqlite setup is needed to observe it.
    #[tokio::test]
    async fn test_sql_errors_without_storage_factory() {
        let err = CatalogType::Sql
            .load("sql", HashMap::new(), None)
            .await
            .expect_err("SQL must refuse to load without a storage factory");

        assert_eq!(err.kind(), ErrorKind::Unexpected);
    }

    // A caller-supplied factory must be threaded through to a catalog that has
    // its own default (here Memory, whose default is in-memory storage). The
    // override is accepted rather than rejected or ignored.
    #[tokio::test]
    async fn test_explicit_factory_overrides_default() {
        let warehouse = TempDir::new().expect("temp dir should be creatable");
        CatalogType::Memory
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    warehouse
                        .path()
                        .to_str()
                        .expect("temp dir path should be valid UTF-8")
                        .to_string(),
                )]),
                Some(Arc::new(MemoryStorageFactory)),
            )
            .await
            .expect("memory catalog should accept an explicit storage factory");
    }
}
