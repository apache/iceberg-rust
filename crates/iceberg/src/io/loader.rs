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

// todo move this to a new crate
/// todo doc: mimic storage builder
use std::sync::Arc;

use async_trait::async_trait;
use crate::{Error, ErrorKind, Result};
use crate::io::{FileIOBuilder, Storage, StorageBuilder};
use crate::io::storage::OpenDALStorageBuilder;

/// A StorageBuilderFactory creating a new storage builder.
type StorageBuilderFactory = fn() -> Box<dyn BoxedStorageBuilder>;

/// A registry of storage builders.
static STORAGE_REGISTRY: &[(&str, StorageBuilderFactory)] = &[
    ("opendal", || Box::new(OpenDALStorageBuilder::default())),
];

/// Return the list of supported storage types.
pub fn supported_types() -> Vec<&'static str> {
    STORAGE_REGISTRY.iter().map(|(k, _)| *k).collect()
}

#[async_trait]
pub trait BoxedStorageBuilder {
    fn build(
        self: Box<Self>,
        file_io_builder: FileIOBuilder
    ) -> Result<Arc<dyn Storage>>;
}

#[async_trait]
impl<T: StorageBuilder + 'static> BoxedStorageBuilder for T {
    fn build(
        self: Box<Self>,
        props: HashMap<String, String>,
    ) -> Result<Arc<dyn Storage>> {
        let builder = *self;
        Ok(Arc::new(builder.build(props)?) as Arc<dyn Storage>)
    }
}

/// Load a storage from a string.
pub fn load(r#type: &str) -> Result<Box<dyn BoxedStorageBuilder>> {
    let key = r#type.trim();
    if let Some((_, factory)) = STORAGE_REGISTRY
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(key))
    {
        Ok(factory())
    } else {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            format!(
                "Unsupported storage type: {}. Supported types: {}",
                r#type,
                supported_types().join(", ")
            ),
        ))
    }
}

/// Ergonomic storage loader builder pattern.
pub struct StorageLoader<'a> {
    storage_type: &'a str,
}

impl<'a> From<&'a str> for StorageLoader<'a> {
    fn from(s: &'a str) -> Self {
        Self { storage_type: s }
    }
}

impl StorageLoader<'_> {
    pub fn load(
        self,
        file_io_builder: FileIOBuilder
    ) -> Result<Arc<dyn Storage>> {
        let builder = load(self.storage_type)?;
        builder.build(file_io_builder)
    }
}
