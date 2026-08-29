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

//! HDFS storage backend via OpenDAL's `services-hdfs-native` (pure Rust, no JNI).

use std::collections::HashMap;
use std::sync::RwLock;

use iceberg::io::{HDFS_HADOOP_CONF_PREFIX, HDFS_NAME_NODE};
use iceberg::{Error, ErrorKind, Result};
use opendal::Operator;
use opendal::services::HdfsNativeConfig;
use url::Url;

use crate::utils::from_opendal_error;

/// Parse iceberg properties to [`HdfsNativeConfig`].
pub(crate) fn hdfs_config_parse(mut m: HashMap<String, String>) -> Result<HdfsNativeConfig> {
    let mut cfg = HdfsNativeConfig::default();

    if let Some(name_node) = m.remove(HDFS_NAME_NODE) {
        cfg.name_node = Some(name_node);
    }

    let options: HashMap<String, String> = m
        .into_iter()
        .filter_map(|(key, value)| {
            key.strip_prefix(HDFS_HADOOP_CONF_PREFIX)
                .map(|stripped| (stripped.to_string(), value))
        })
        .collect();
    if !options.is_empty() {
        cfg.options = Some(options);
    }

    Ok(cfg)
}

/// Parse an HDFS path into `Some("hdfs://<authority>")` (`None` when
/// authority-less) and the relative path (no leading `/`, opendal style).
pub(crate) fn parse_hdfs_path(path: &str) -> Result<(Option<String>, &str)> {
    let url = Url::parse(path).map_err(|e| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid hdfs path: {path}: {e}"),
        )
    })?;
    if url.scheme() != "hdfs" {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid hdfs path: {path}, expected scheme `hdfs://`"),
        ));
    }

    let name_node = url.host_str().filter(|h| !h.is_empty()).map(|host| {
        url.port()
            .map(|port| format!("hdfs://{host}:{port}"))
            .unwrap_or_else(|| format!("hdfs://{host}"))
    });

    // `url.path()` borrows from `url` and can't be returned with the input's
    // lifetime. Slice the path component out of the original input instead;
    // it starts after the first `/` following the `hdfs://` prefix. Opendal
    // paths must not start with `/` (`Deleter::delete` rejects them).
    let after_scheme = &path["hdfs://".len()..];
    let rel = match after_scheme.find('/') {
        Some(i) => after_scheme[i..].trim_start_matches('/'),
        None => "",
    };

    Ok((name_node, rel))
}

/// Creates an operator for the path, cached per effective NameNode (the
/// configured `hdfs.name-node`, else the path authority) — each operator
/// holds an HDFS client with live RPC connections.
pub(crate) fn hdfs_create_operator<'a>(
    path: &'a str,
    config: &HdfsNativeConfig,
    operators: &RwLock<HashMap<String, Operator>>,
) -> Result<(Operator, &'a str)> {
    let (authority_name_node, relative_path) = parse_hdfs_path(path)?;

    let name_node = match config.name_node.clone().or(authority_name_node) {
        Some(name_node) => name_node,
        None => {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Invalid hdfs path: {path}, authority-less paths require the `{HDFS_NAME_NODE}` property"
                ),
            ));
        }
    };

    // Fast path: check read lock first.
    {
        let cache = operators
            .read()
            .map_err(|_| Error::new(ErrorKind::Unexpected, "HDFS operator cache lock poisoned"))?;
        if let Some(op) = cache.get(&name_node) {
            return Ok((op.clone(), relative_path));
        }
    }

    // Slow path: build and insert under write lock, re-checking for a
    // concurrent insert.
    let mut cache = operators
        .write()
        .map_err(|_| Error::new(ErrorKind::Unexpected, "HDFS operator cache lock poisoned"))?;
    let op = match cache.get(&name_node) {
        Some(op) => op.clone(),
        None => {
            let op = hdfs_operator_build(config, &name_node)?;
            cache.insert(name_node, op.clone());
            op
        }
    };

    Ok((op, relative_path))
}

/// Build a new OpenDAL [`Operator`]: OpenDAL splits `name_node` on commas
/// into a synthetic HA name service; `$HADOOP_CONF_DIR` XML still merges in.
fn hdfs_operator_build(config: &HdfsNativeConfig, name_node: &str) -> Result<Operator> {
    let mut cfg = config.clone();
    cfg.name_node = Some(name_node.to_string());
    Operator::from_config(cfg).map_err(from_opendal_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hdfs_config_parse_name_node_and_options() {
        let props = HashMap::from([
            (
                HDFS_NAME_NODE.to_string(),
                "hdfs://nn1:8020,hdfs://nn2:8020".to_string(),
            ),
            (
                "hadoop.dfs.client.failover.random.order".to_string(),
                "true".to_string(),
            ),
            ("unrelated.key".to_string(), "ignored".to_string()),
        ]);

        let cfg = hdfs_config_parse(props).unwrap();

        assert_eq!(
            cfg.name_node.as_deref(),
            Some("hdfs://nn1:8020,hdfs://nn2:8020")
        );
        let options = cfg.options.unwrap();
        assert_eq!(
            options.get("dfs.client.failover.random.order"),
            Some(&"true".to_string())
        );
        assert!(!options.contains_key("unrelated.key"));
    }

    #[test]
    fn test_hdfs_config_parse_empty() {
        let cfg = hdfs_config_parse(HashMap::new()).unwrap();

        assert_eq!(cfg.name_node, None);
        assert_eq!(cfg.options, None);
    }

    #[test]
    fn test_parse_hdfs_path_with_authority_and_rel() {
        let (nn, rel) = parse_hdfs_path("hdfs://nameservice1/a/b").unwrap();

        assert_eq!(nn.as_deref(), Some("hdfs://nameservice1"));
        assert_eq!(rel, "a/b");
    }

    #[test]
    fn test_parse_hdfs_path_with_authority_and_port() {
        let (nn, rel) = parse_hdfs_path("hdfs://nn:8020/foo").unwrap();

        assert_eq!(nn.as_deref(), Some("hdfs://nn:8020"));
        assert_eq!(rel, "foo");
    }

    #[test]
    fn test_parse_hdfs_path_with_authority_no_path() {
        let (nn, rel) = parse_hdfs_path("hdfs://nameservice1").unwrap();

        assert_eq!(nn.as_deref(), Some("hdfs://nameservice1"));
        assert_eq!(rel, "");
    }

    #[test]
    fn test_parse_hdfs_path_with_authority_trailing_slash() {
        let (nn, rel) = parse_hdfs_path("hdfs://nameservice1/").unwrap();

        assert_eq!(nn.as_deref(), Some("hdfs://nameservice1"));
        assert_eq!(rel, "");
    }

    #[test]
    fn test_parse_hdfs_path_authority_less_returns_none() {
        let (nn, rel) = parse_hdfs_path("hdfs:///a/b").unwrap();

        assert_eq!(nn, None);
        assert_eq!(rel, "a/b");
    }

    #[test]
    fn test_parse_hdfs_path_wrong_scheme_errors() {
        let err = parse_hdfs_path("file:///tmp/x").unwrap_err();

        assert!(err.to_string().contains("expected scheme `hdfs://`"));
    }

    #[test]
    fn test_parse_hdfs_path_invalid_url_errors() {
        let err = parse_hdfs_path("not-a-url").unwrap_err();

        assert!(err.to_string().contains("Invalid hdfs path"));
    }

    #[test]
    fn test_hdfs_create_operator_configured_name_node_wins() {
        let config = hdfs_config_parse(HashMap::from([(
            HDFS_NAME_NODE.to_string(),
            "hdfs://configured:8020".to_string(),
        )]))
        .unwrap();
        let operators = RwLock::new(HashMap::new());

        let (_, rel) =
            hdfs_create_operator("hdfs://from-path:9000/a/b", &config, &operators).unwrap();

        assert_eq!(rel, "a/b");
        let cache = operators.read().unwrap();
        assert!(cache.contains_key("hdfs://configured:8020"));
        assert!(!cache.contains_key("hdfs://from-path:9000"));
    }

    #[test]
    fn test_hdfs_create_operator_uses_path_authority() {
        let config = HdfsNativeConfig::default();
        let operators = RwLock::new(HashMap::new());

        let (_, rel) = hdfs_create_operator("hdfs://nn:8020/a/b", &config, &operators).unwrap();

        assert_eq!(rel, "a/b");
        assert!(operators.read().unwrap().contains_key("hdfs://nn:8020"));
    }

    #[test]
    fn test_hdfs_create_operator_caches_per_name_node() {
        let config = HdfsNativeConfig::default();
        let operators = RwLock::new(HashMap::new());

        hdfs_create_operator("hdfs://nn1:8020/a", &config, &operators).unwrap();
        hdfs_create_operator("hdfs://nn1:8020/b", &config, &operators).unwrap();
        hdfs_create_operator("hdfs://nn2:8020/c", &config, &operators).unwrap();

        assert_eq!(operators.read().unwrap().len(), 2);
    }

    #[test]
    fn test_hdfs_create_operator_authority_less_without_config_errors() {
        let config = HdfsNativeConfig::default();
        let operators = RwLock::new(HashMap::new());

        let err = hdfs_create_operator("hdfs:///a/b", &config, &operators).unwrap_err();

        assert!(err.to_string().contains(HDFS_NAME_NODE));
    }
}
