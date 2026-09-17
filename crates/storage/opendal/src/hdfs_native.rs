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
use std::sync::{Arc, RwLock};

use iceberg::io::{HDFS_HADOOP_CONF_PREFIX, HDFS_NAME_NODE};
use iceberg::{Error, ErrorKind, Result};
use opendal::Operator;
use opendal::services::HdfsNativeConfig;
use url::Url;

use crate::utils::from_opendal_error;

/// Parse iceberg properties to [`HdfsNativeConfig`].
pub(crate) fn hdfs_native_config_parse(mut m: HashMap<String, String>) -> Result<HdfsNativeConfig> {
    let mut cfg = HdfsNativeConfig::default();

    // `Operator::from_config` bypasses the builder's empty-string guard, and
    // `Some("")` would shadow the path-authority fallback below.
    if let Some(name_node) = m
        .remove(HDFS_NAME_NODE)
        .map(|s| s.trim().trim_end_matches('/').to_string())
        .filter(|s| !s.is_empty())
    {
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
pub(crate) fn hdfs_native_parse_path(path: &str) -> Result<(Option<String>, &str)> {
    let url = Url::parse(path).map_err(|e| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid hdfs path: {path}: {e}"),
        )
    })?;
    // Non-special schemes parse even without `//` (e.g. `hdfs:x` is a valid
    // non-hierarchical URL), so require the literal prefix before slicing.
    let (Some(after_scheme), "hdfs") = (path.strip_prefix("hdfs://"), url.scheme()) else {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid hdfs path: {path}, expected scheme `hdfs://`"),
        ));
    };

    let name_node = url.host_str().filter(|h| !h.is_empty()).map(|host| {
        url.port()
            .map(|port| format!("hdfs://{host}:{port}"))
            .unwrap_or_else(|| format!("hdfs://{host}"))
    });

    // `url.path()` borrows from `url` and can't be returned with the input's
    // lifetime. Slice the path component out of the original input instead;
    // it starts after the first `/` following the `hdfs://` prefix. Opendal
    // paths must not start with `/` (`Deleter::delete` rejects them).
    let rel = match after_scheme.find('/') {
        Some(i) => after_scheme[i..].trim_start_matches('/'),
        None => "",
    };

    Ok((name_node, rel))
}

/// Resolves the effective NameNode for a path — the configured
/// `hdfs.name-node` when set, else the path authority — plus the relative
/// path. Both the operator cache and `delete_stream` batching key on this,
/// so they cannot drift apart.
pub(crate) fn hdfs_native_effective_name_node<'a>(
    config: &HdfsNativeConfig,
    path: &'a str,
) -> Result<(String, &'a str)> {
    let (authority_name_node, relative_path) = hdfs_native_parse_path(path)?;
    let name_node = config
        .name_node
        .clone()
        .or(authority_name_node)
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Invalid hdfs path: {path}, authority-less paths require the `{HDFS_NAME_NODE}` property"
                ),
            )
        })?;
    Ok((name_node, relative_path))
}

/// Operators cached per effective NameNode: each holds an `hdfs-native`
/// client with live RPC connections, whose tasks run on the tokio runtime
/// current when it was built.
#[derive(Clone, Debug, Default)]
pub struct HdfsNativeOperatorCache(Arc<RwLock<HashMap<String, Operator>>>);

impl HdfsNativeOperatorCache {
    fn get(&self, name_node: &str) -> Result<Option<Operator>> {
        Ok(self.0.read().map_err(poisoned)?.get(name_node).cloned())
    }

    /// Inserts `op` unless a concurrent caller got there first, returning
    /// whichever operator the cache now holds.
    fn insert(&self, name_node: String, op: Operator) -> Result<Operator> {
        Ok(self
            .0
            .write()
            .map_err(poisoned)?
            .entry(name_node)
            .or_insert(op)
            .clone())
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.0.read().unwrap().len()
    }
}

fn poisoned<T>(_: T) -> Error {
    Error::new(ErrorKind::Unexpected, "HDFS operator cache lock poisoned")
}

/// Creates an operator for the path, reusing the cached one for its
/// effective NameNode.
pub(crate) fn hdfs_native_create_operator<'a>(
    path: &'a str,
    config: &HdfsNativeConfig,
    operators: &HdfsNativeOperatorCache,
) -> Result<(Operator, &'a str)> {
    let (name_node, relative_path) = hdfs_native_effective_name_node(config, path)?;

    if let Some(op) = operators.get(&name_node)? {
        return Ok((op, relative_path));
    }

    // Built outside the lock: the build reads the Hadoop XML config
    // synchronously. A racing first caller may build too; the loser is
    // dropped before opening any connection.
    let op = hdfs_native_operator_build(config, &name_node)?;
    Ok((operators.insert(name_node, op)?, relative_path))
}

/// Returns the `delete_stream` grouping key for a path: the effective
/// NameNode, so paths that resolve to different operators never share a
/// deleter. Unresolvable paths key on themselves (as `hf_batch_key` does);
/// `create_operator` then reports the real error.
pub(crate) fn hdfs_native_batch_key(config: &HdfsNativeConfig, path: &str) -> String {
    hdfs_native_effective_name_node(config, path)
        .map(|(name_node, _)| name_node)
        .unwrap_or_else(|_| path.to_string())
}

/// Build a new OpenDAL [`Operator`]: OpenDAL splits `name_node` on commas
/// into a synthetic HA name service; `$HADOOP_CONF_DIR` XML still merges in.
fn hdfs_native_operator_build(config: &HdfsNativeConfig, name_node: &str) -> Result<Operator> {
    let mut cfg = config.clone();
    cfg.name_node = Some(name_node.to_string());
    Operator::from_config(cfg).map_err(from_opendal_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hdfs_native_config_parse_name_node_and_options() {
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

        let cfg = hdfs_native_config_parse(props).unwrap();

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
    fn test_hdfs_native_config_parse_empty() {
        let cfg = hdfs_native_config_parse(HashMap::new()).unwrap();

        assert_eq!(cfg.name_node, None);
        assert_eq!(cfg.options, None);
    }

    #[test]
    fn test_hdfs_native_config_parse_normalizes_name_node() {
        let parse = |value: &str| {
            hdfs_native_config_parse(HashMap::from([(
                HDFS_NAME_NODE.to_string(),
                value.to_string(),
            )]))
            .unwrap()
            .name_node
        };

        // Empty must not shadow the path-authority fallback.
        assert_eq!(parse(""), None);
        assert_eq!(parse("  "), None);
        // Trailing `/` would otherwise yield a second cache entry for one cluster.
        assert_eq!(
            parse(" hdfs://nn:8020/ ").as_deref(),
            Some("hdfs://nn:8020")
        );
    }

    #[test]
    fn test_hdfs_native_effective_name_node_precedence() {
        let configured = hdfs_native_config_parse(HashMap::from([(
            HDFS_NAME_NODE.to_string(),
            "hdfs://nn1:8020,hdfs://nn2:8020".to_string(),
        )]))
        .unwrap();
        let unconfigured = HdfsNativeConfig::default();

        // Configured wins over the authority, including for authority-less paths.
        for path in ["hdfs://ns-a/x", "hdfs:///y"] {
            let (nn, _) = hdfs_native_effective_name_node(&configured, path).unwrap();
            assert_eq!(nn, "hdfs://nn1:8020,hdfs://nn2:8020");
        }
        // Otherwise the authority, including its port.
        let (nn, rel) =
            hdfs_native_effective_name_node(&unconfigured, "hdfs://nn:9000/a/b").unwrap();
        assert_eq!((nn.as_str(), rel), ("hdfs://nn:9000", "a/b"));
        // Neither: a pointed error.
        let err = hdfs_native_effective_name_node(&unconfigured, "hdfs:///a").unwrap_err();
        assert!(err.to_string().contains(HDFS_NAME_NODE));
    }

    #[test]
    fn test_hdfs_native_parse_path_with_authority_and_rel() {
        let (nn, rel) = hdfs_native_parse_path("hdfs://nameservice1/a/b").unwrap();

        assert_eq!(nn.as_deref(), Some("hdfs://nameservice1"));
        assert_eq!(rel, "a/b");
    }

    #[test]
    fn test_hdfs_native_parse_path_with_authority_and_port() {
        let (nn, rel) = hdfs_native_parse_path("hdfs://nn:8020/foo").unwrap();

        assert_eq!(nn.as_deref(), Some("hdfs://nn:8020"));
        assert_eq!(rel, "foo");
    }

    #[test]
    fn test_hdfs_native_parse_path_with_authority_no_path() {
        let (nn, rel) = hdfs_native_parse_path("hdfs://nameservice1").unwrap();

        assert_eq!(nn.as_deref(), Some("hdfs://nameservice1"));
        assert_eq!(rel, "");
    }

    #[test]
    fn test_hdfs_native_parse_path_with_authority_trailing_slash() {
        let (nn, rel) = hdfs_native_parse_path("hdfs://nameservice1/").unwrap();

        assert_eq!(nn.as_deref(), Some("hdfs://nameservice1"));
        assert_eq!(rel, "");
    }

    #[test]
    fn test_hdfs_native_parse_path_authority_less_returns_none() {
        let (nn, rel) = hdfs_native_parse_path("hdfs:///a/b").unwrap();

        assert_eq!(nn, None);
        assert_eq!(rel, "a/b");
    }

    #[test]
    fn test_hdfs_native_parse_path_wrong_scheme_errors() {
        let err = hdfs_native_parse_path("file:///tmp/x").unwrap_err();

        assert!(err.to_string().contains("expected scheme `hdfs://`"));
    }

    #[test]
    fn test_hdfs_native_parse_path_invalid_url_errors() {
        let err = hdfs_native_parse_path("not-a-url").unwrap_err();

        assert!(err.to_string().contains("Invalid hdfs path"));
    }

    #[test]
    fn test_hdfs_native_parse_path_non_hierarchical_errors() {
        // `hdfs:x` parses as a valid non-hierarchical URL; it must be
        // rejected rather than panic on slicing.
        for path in ["hdfs:x", "hdfs:/x", "hdfs:"] {
            let err = hdfs_native_parse_path(path).unwrap_err();
            assert!(err.to_string().contains("expected scheme `hdfs://`"));
        }
    }

    #[test]
    fn test_hdfs_native_batch_key_distinguishes_ports() {
        let config = HdfsNativeConfig::default();

        assert_eq!(
            hdfs_native_batch_key(&config, "hdfs://namenode:8020/a"),
            "hdfs://namenode:8020"
        );
        assert_eq!(
            hdfs_native_batch_key(&config, "hdfs://namenode:9000/b"),
            "hdfs://namenode:9000"
        );
    }

    #[test]
    fn test_hdfs_native_batch_key_invalid_path_keys_on_itself() {
        let config = HdfsNativeConfig::default();

        // Unresolvable paths must not collapse onto a shared "" key.
        assert_eq!(hdfs_native_batch_key(&config, "not-a-url"), "not-a-url");
        assert_eq!(hdfs_native_batch_key(&config, "hdfs:///a"), "hdfs:///a");
    }

    #[test]
    fn test_hdfs_native_create_operator_configured_name_node_wins() {
        let config = hdfs_native_config_parse(HashMap::from([(
            HDFS_NAME_NODE.to_string(),
            "hdfs://configured:8020".to_string(),
        )]))
        .unwrap();
        let operators = HdfsNativeOperatorCache::default();

        let (_, rel) =
            hdfs_native_create_operator("hdfs://from-path:9000/a/b", &config, &operators).unwrap();

        assert_eq!(rel, "a/b");
        assert!(operators.get("hdfs://configured:8020").unwrap().is_some());
        assert!(operators.get("hdfs://from-path:9000").unwrap().is_none());
    }

    #[test]
    fn test_hdfs_native_create_operator_uses_path_authority() {
        let config = HdfsNativeConfig::default();
        let operators = HdfsNativeOperatorCache::default();

        let (_, rel) =
            hdfs_native_create_operator("hdfs://nn:8020/a/b", &config, &operators).unwrap();

        assert_eq!(rel, "a/b");
        assert!(operators.get("hdfs://nn:8020").unwrap().is_some());
    }

    #[test]
    fn test_hdfs_native_create_operator_caches_per_name_node() {
        let config = HdfsNativeConfig::default();
        let operators = HdfsNativeOperatorCache::default();

        hdfs_native_create_operator("hdfs://nn1:8020/a", &config, &operators).unwrap();
        hdfs_native_create_operator("hdfs://nn1:8020/b", &config, &operators).unwrap();
        hdfs_native_create_operator("hdfs://nn2:8020/c", &config, &operators).unwrap();

        assert_eq!(operators.len(), 2);
    }

    #[test]
    fn test_hdfs_native_create_operator_authority_less_without_config_errors() {
        let config = HdfsNativeConfig::default();
        let operators = HdfsNativeOperatorCache::default();

        let err = hdfs_native_create_operator("hdfs:///a/b", &config, &operators).unwrap_err();

        assert!(err.to_string().contains(HDFS_NAME_NODE));
    }
}
