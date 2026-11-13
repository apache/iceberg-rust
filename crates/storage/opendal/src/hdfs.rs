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

//! HDFS storage backend via OpenDAL's `services-hdfs` (libhdfs over JNI).
//! Hadoop's Java client reads its own configuration from `CLASSPATH`.

use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};

use iceberg::{Error, ErrorKind, Result};
use opendal::Operator;
use opendal::services::HdfsConfig;
use serde::{Deserialize, Serialize};

use crate::OpenDalClientConfig;
use crate::utils::from_opendal_error;

/// State of [`OpenDalStorage::Hdfs`](crate::OpenDalStorage::Hdfs): an operator
/// per NameNode, shared by clones, because building one connects.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HdfsStorage {
    #[serde(skip)]
    pub(crate) operators: Arc<RwLock<HashMap<String, Operator>>>,
    #[serde(default)]
    pub(crate) client_config: OpenDalClientConfig,
}

impl HdfsStorage {
    pub(crate) fn new(client_config: OpenDalClientConfig) -> Self {
        Self {
            operators: Arc::default(),
            client_config,
        }
    }
}

/// The NameNode URI for a path, as Hadoop's `FileSystem.get` takes it, and
/// the path relative to it. `hdfs:///` resolves to `fs.defaultFS`.
pub(crate) fn hdfs_name_node(path: &str) -> Result<(String, &str)> {
    let rest = path.strip_prefix("hdfs://").ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid hdfs path: {path}, expected scheme `hdfs://`"),
        )
    })?;
    let (authority, relative_path) = rest.split_once('/').unwrap_or((rest, ""));
    // OpenDAL paths must not start with `/`.
    Ok((
        format!("hdfs://{authority}/"),
        relative_path.trim_start_matches('/'),
    ))
}

/// Creates an operator for the path, reusing the cached one for its NameNode.
pub(crate) fn hdfs_create_operator<'a>(
    path: &'a str,
    operators: &RwLock<HashMap<String, Operator>>,
) -> Result<(Operator, &'a str)> {
    let (name_node, relative_path) = hdfs_name_node(path)?;
    if let Some(op) = operators
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .get(&name_node)
    {
        return Ok((op.clone(), relative_path));
    }

    // Built outside the lock, since connecting may start a JVM. A racing
    // caller keeps the operator inserted first.
    let mut config = HdfsConfig::default();
    config.name_node = Some(name_node.clone());
    let op = Operator::from_config(config).map_err(from_opendal_error)?;
    let op = operators
        .write()
        .unwrap_or_else(PoisonError::into_inner)
        .entry(name_node)
        .or_insert(op)
        .clone();
    Ok((op, relative_path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OpenDalStorage;

    #[test]
    fn test_hdfs_name_node() {
        for (path, name_node, relative_path) in [
            ("hdfs://nn:8020/a/b", "hdfs://nn:8020/", "a/b"),
            ("hdfs://ns1//a", "hdfs://ns1/", "a"),
            ("hdfs:///a/b", "hdfs:///", "a/b"),
            ("hdfs://nn:8020", "hdfs://nn:8020/", ""),
        ] {
            assert_eq!(
                hdfs_name_node(path).unwrap(),
                (name_node.to_string(), relative_path),
                "{path}"
            );
        }
        assert!(hdfs_name_node("s3://bucket/a").is_err());
    }

    #[test]
    fn test_hdfs_delete_stream_keys() {
        let storage = OpenDalStorage::Hdfs(HdfsStorage::new(OpenDalClientConfig::default()));

        // NameNodes that differ only by port must not share a deleter.
        assert_ne!(
            storage.batch_key_for_path("hdfs://nn:8020/a"),
            storage.batch_key_for_path("hdfs://nn:9000/a")
        );
        assert_eq!(
            storage.relativize_path("hdfs://nn:8020/a/b").unwrap(),
            "a/b"
        );
    }
}
