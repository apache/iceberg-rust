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

use iceberg_property_macro::Properties;

/// HDFS NameNode RPC endpoint(s), e.g. `hdfs://namenode:8020`; a
/// comma-separated list enables HA failover. When unset, the NameNode is
/// derived from the path authority.
pub const HDFS_NAME_NODE: &str = "hdfs.name-node";
/// Prefix for properties forwarded to the HDFS client configuration, e.g.
/// `hadoop.dfs.client.failover.random.order`. Forwarded values (prefix
/// stripped) override those loaded from `$HADOOP_CONF_DIR`.
pub const HDFS_HADOOP_CONF_PREFIX: &str = "hadoop.";

/// HDFS storage configuration.
// No in-crate consumer yet: `iceberg-storage-opendal` parses the raw
// properties itself and only shares the key constants above.
#[allow(dead_code)]
#[derive(Debug, Properties)]
pub(crate) struct HdfsConfig {
    /// NameNode endpoint(s); comma-separated for HA failover.
    #[property(key = HDFS_NAME_NODE, default = None, getter)]
    name_node: Option<String>,
    /// Extra HDFS client configuration (the `hadoop.` prefix stripped).
    #[property(prefix = HDFS_HADOOP_CONF_PREFIX, getter)]
    options: HashMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hdfs_config_from_properties() {
        let props = HashMap::from([
            (
                HDFS_NAME_NODE.to_string(),
                "hdfs://namenode:8020".to_string(),
            ),
            (
                "hadoop.dfs.client.failover.random.order".to_string(),
                "true".to_string(),
            ),
            ("unrelated.key".to_string(), "ignored".to_string()),
        ]);

        let cfg = HdfsConfig::from_properties(&props).unwrap();
        assert_eq!(cfg.name_node().as_deref(), Some("hdfs://namenode:8020"));
        assert_eq!(
            cfg.options().get("dfs.client.failover.random.order"),
            Some(&"true".to_string())
        );
        assert!(!cfg.options().contains_key("unrelated.key"));
    }

    #[test]
    fn test_hdfs_config_empty() {
        let cfg = HdfsConfig::from_properties(&HashMap::new()).unwrap();
        assert_eq!(cfg.name_node().as_deref(), None);
        assert!(cfg.options().is_empty());
    }
}
