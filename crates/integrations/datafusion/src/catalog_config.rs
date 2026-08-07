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

/// A serializable description of the catalog (and storage) that backs an
/// [`IcebergTableProvider`](crate::table::IcebergTableProvider).
///
/// This is the minimal, self-contained handle needed to *reconstruct* a catalog
/// and its associated `FileIO` on a remote node. It deliberately holds only
/// plain data (no live connections) so that distributed query engines such as
/// Ballista can serialize it, ship it to executors, and rebuild the catalog
/// there via a catalog loader (e.g. `iceberg-catalog-loader`) and the storage
/// via `FileIOBuilder::with_props`.
///
/// The `props` map carries both the catalog connection properties (e.g. the
/// REST catalog URI) and the storage/`FileIO` properties (e.g. S3 endpoint and
/// credentials); in practice these live together in a single map.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcebergCatalogConfig {
    /// The catalog type, e.g. `"rest"`, `"sql"`, `"glue"`.
    pub r#type: String,
    /// The catalog name it is registered under (used to look it up when
    /// reconstructing the catalog on a remote node).
    pub name: String,
    /// Catalog connection and storage properties.
    pub props: HashMap<String, String>,
}

impl IcebergCatalogConfig {
    pub fn new(
        r#type: impl Into<String>,
        name: impl Into<String>,
        props: HashMap<String, String>,
    ) -> Self {
        Self {
            r#type: r#type.into(),
            name: name.into(),
            props,
        }
    }
}
