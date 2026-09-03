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

/// A plain-data description of the catalog and storage that backs a table.
///
/// Holds the inputs a catalog loader takes — `type` selects the builder, `name`
/// and `props` are what [`CatalogBuilder::load`](iceberg::CatalogBuilder::load)
/// receives — and no live connections, so it can be serialized and shipped to a
/// remote worker that rebuilds the catalog and its `FileIO`. Nothing in this
/// crate connects using these values. See
/// [`IcebergStaticTableProvider::with_catalog_config`](crate::IcebergStaticTableProvider::with_catalog_config).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcebergCatalogConfig {
    /// Catalog type, e.g. `"rest"`, `"sql"`, `"glue"`.
    pub r#type: String,
    /// Catalog name, as it would be passed to a catalog loader.
    pub name: String,
    /// Catalog connection properties and storage/`FileIO` properties, which in
    /// practice live together in a single map.
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
