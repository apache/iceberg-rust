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

//! HDFS storage configuration.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use super::StorageConfig;
use crate::Result;

/// HDFS NameNode RPC endpoint(s), e.g. `hdfs://namenode:8020`; a
/// comma-separated list enables HA failover. When unset, the NameNode is
/// derived from the path authority.
pub const HDFS_NAME_NODE: &str = "hdfs.name-node";
/// Prefix for properties forwarded to the HDFS client configuration, e.g.
/// `hadoop.dfs.client.failover.random.order`. Forwarded values (prefix
/// stripped) override those loaded from `$HADOOP_CONF_DIR`.
pub const HDFS_HADOOP_CONF_PREFIX: &str = "hadoop.";

/// HDFS storage configuration.
///
/// This struct contains all the configuration options for connecting to HDFS.
/// Use the builder pattern via `HdfsConfig::builder()` to construct instances.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, TypedBuilder)]
pub struct HdfsConfig {
    /// NameNode endpoint(s); comma-separated for HA failover.
    #[builder(default, setter(strip_option, into))]
    pub name_node: Option<String>,
    /// Extra HDFS client configuration (the `hadoop.` prefix stripped).
    #[builder(default)]
    pub options: HashMap<String, String>,
}

impl TryFrom<&StorageConfig> for HdfsConfig {
    type Error = crate::Error;

    fn try_from(config: &StorageConfig) -> Result<Self> {
        let props = config.props();

        let mut cfg = HdfsConfig::default();

        if let Some(name_node) = props.get(HDFS_NAME_NODE) {
            cfg.name_node = Some(name_node.clone());
        }
        for (key, value) in props {
            if let Some(stripped) = key.strip_prefix(HDFS_HADOOP_CONF_PREFIX) {
                cfg.options.insert(stripped.to_string(), value.clone());
            }
        }

        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hdfs_config_builder() {
        let cfg = HdfsConfig::builder()
            .name_node("hdfs://nn1:8020,hdfs://nn2:8020")
            .build();
        assert_eq!(
            cfg.name_node.as_deref(),
            Some("hdfs://nn1:8020,hdfs://nn2:8020")
        );
        assert!(cfg.options.is_empty());
    }

    #[test]
    fn test_hdfs_config_from_storage_config() {
        let storage_config = StorageConfig::new()
            .with_prop(HDFS_NAME_NODE, "hdfs://namenode:8020")
            .with_prop("hadoop.dfs.client.failover.random.order", "true")
            .with_prop("unrelated.key", "ignored");

        let cfg = HdfsConfig::try_from(&storage_config).unwrap();
        assert_eq!(cfg.name_node.as_deref(), Some("hdfs://namenode:8020"));
        assert_eq!(
            cfg.options.get("dfs.client.failover.random.order"),
            Some(&"true".to_string())
        );
        assert!(!cfg.options.contains_key("unrelated.key"));
    }

    #[test]
    fn test_hdfs_config_empty() {
        let cfg = HdfsConfig::try_from(&StorageConfig::new()).unwrap();
        assert_eq!(cfg.name_node, None);
        assert!(cfg.options.is_empty());
    }
}
