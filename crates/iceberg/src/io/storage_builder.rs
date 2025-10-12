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
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

#[cfg(feature = "storage-azdls")]
use crate::io::storage_azdls::OpenDALAzdlsStorageBuilder;
#[cfg(feature = "storage-fs")]
use crate::io::storage_fs::OpenDALFsStorageBuilder;
#[cfg(feature = "storage-gcs")]
use crate::io::storage_gcs::OpenDALGcsStorageBuilder;
#[cfg(feature = "storage-memory")]
use crate::io::storage_memory::OpenDALMemoryStorageBuilder;
#[cfg(feature = "storage-oss")]
use crate::io::storage_oss::OpenDALOssStorageBuilder;
#[cfg(feature = "storage-s3")]
use crate::io::storage_s3::OpenDALS3StorageBuilder;
use crate::io::{Extensions, Storage, StorageBuilder};
use crate::{Error, ErrorKind, Result};

/// A StorageBuilderFactory creating a new storage builder.
type StorageBuilderFactory = fn() -> Box<dyn BoxedStorageBuilder>;

/// A registry of storage builders.
static STORAGE_REGISTRY: &[(&str, StorageBuilderFactory)] = &[
    #[cfg(feature = "storage-memory")]
    (
        "memory",
        || Box::new(OpenDALMemoryStorageBuilder::default()),
    ),
    #[cfg(feature = "storage-fs")]
    ("file", || Box::new(OpenDALFsStorageBuilder::default())),
    #[cfg(feature = "storage-fs")]
    ("", || Box::new(OpenDALFsStorageBuilder::default())),
    #[cfg(feature = "storage-s3")]
    ("s3", || Box::new(OpenDALS3StorageBuilder::default())),
    #[cfg(feature = "storage-s3")]
    ("s3a", || Box::new(OpenDALS3StorageBuilder::default())),
    #[cfg(feature = "storage-gcs")]
    ("gs", || Box::new(OpenDALGcsStorageBuilder::default())),
    #[cfg(feature = "storage-gcs")]
    ("gcs", || Box::new(OpenDALGcsStorageBuilder::default())),
    #[cfg(feature = "storage-oss")]
    ("oss", || Box::new(OpenDALOssStorageBuilder::default())),
    #[cfg(feature = "storage-azdls")]
    ("abfs", || Box::new(OpenDALAzdlsStorageBuilder::default())),
    #[cfg(feature = "storage-azdls")]
    ("abfss", || Box::new(OpenDALAzdlsStorageBuilder::default())),
    #[cfg(feature = "storage-azdls")]
    ("wasb", || Box::new(OpenDALAzdlsStorageBuilder::default())),
    #[cfg(feature = "storage-azdls")]
    ("wasbs", || Box::new(OpenDALAzdlsStorageBuilder::default())),
];

/// Return the list of supported storage types.
pub fn supported_types() -> Vec<&'static str> {
    STORAGE_REGISTRY.iter().map(|(k, _)| *k).collect()
}

#[async_trait]
pub trait BoxedStorageBuilder {
    fn build(
        self: Box<Self>,
        props: HashMap<String, String>,
    ) -> Result<Arc<dyn Storage>>;

    fn build_with_extensions(
        self: Box<Self>,
        props: HashMap<String, String>,
        extensions: Extensions,
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

    fn build_with_extensions(self: Box<Self>, props: HashMap<String, String>, extensions: Extensions) -> Result<Arc<dyn Storage>> {
        let builder = *self;
        Ok(Arc::new(builder.with_extensions(extensions).build(props)?) as Arc<dyn Storage>)
    }
}

/// Load a storage builder by storage type..
pub fn load_storage_builder(r#type: &str) -> Result<Box<dyn BoxedStorageBuilder>> {
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
