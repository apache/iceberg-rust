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
use std::collections::hash_map::Entry;
use std::sync::{Arc, RwLock, Weak};

use iceberg::io::{HDFS_HADOOP_CONF_PREFIX, HDFS_HOST, HDFS_NAME_NODE, HDFS_PORT};
use iceberg::{Error, ErrorKind, Result};
use opendal::Operator;
use opendal::services::HdfsNativeConfig;
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use url::Url;

use crate::OpenDalClientConfig;
use crate::utils::from_opendal_error;

/// Hadoop's default filesystem, which serves authority-less paths.
const FS_DEFAULT_FS: &str = "fs.defaultFS";
const HDFS_DEFAULT_PORT: u16 = 8020;
/// PyIceberg keys with no equivalent in opendal's config.
const HDFS_UNSUPPORTED_KEYS: [&str; 2] = ["hdfs.user", "hdfs.kerberos_ticket"];
/// Hadoop's keys declaring an HA nameservice: `hdfs.name-node.<nameservice>`
/// expands to them, and they are honored as well when passed through `hadoop.`.
const HA_NAMENODES_PREFIX: &str = "dfs.ha.namenodes";
const HA_NAMENODE_RPC_ADDRESS_PREFIX: &str = "dfs.namenode.rpc-address";

/// Normalizes one NameNode spelling to `hdfs://host:port`, the form path
/// authorities take, so every source shares cache keys. `hdfs-native` dials
/// a socket address and has no default port, so anything else is `None`:
/// another scheme, a logical name, port 0, userinfo, a path.
fn hdfs_native_name_node(entry: &str) -> Option<String> {
    let rest = entry.trim().trim_end_matches('/');
    let rest = rest.strip_prefix("hdfs://").unwrap_or(rest);
    if rest.is_empty() || rest.contains("://") {
        return None;
    }
    let url = Url::parse(&format!("hdfs://{rest}")).ok()?;
    let (host, port) = (url.host_str()?, url.port()?);
    let plain = host.is_empty()
        || port == 0
        || !url.username().is_empty()
        || url.password().is_some()
        || !matches!(url.path(), "" | "/")
        || url.query().is_some()
        || url.fragment().is_some();
    (!plain).then(|| format!("hdfs://{host}:{port}"))
}

/// Parses a comma-separated NameNode list, each entry normalized; an empty
/// list is `Ok` and empty.
fn hdfs_native_name_node_list(property: &str, value: &str) -> Result<Vec<String>> {
    value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            hdfs_native_name_node(entry).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Invalid `{property}` entry: {entry}, expected host:port (hdfs:// optional)"
                    ),
                )
            })
        })
        .collect()
}

/// Parse iceberg properties to [`HdfsNativeConfig`].
pub(crate) fn hdfs_native_config_parse(mut m: HashMap<String, String>) -> Result<HdfsNativeConfig> {
    let mut cfg = HdfsNativeConfig::default();

    // Entries are trimmed one by one: opendal splits the list on `,` as is,
    // so a space after a comma would break failover to that NameNode. An
    // empty result is dropped because `Operator::from_config` bypasses the
    // builder's empty-string guard and `Some("")` would shadow the
    // path-authority fallback below.
    if let Some(name_node) = m.remove(HDFS_NAME_NODE) {
        let entries = hdfs_native_name_node_list(HDFS_NAME_NODE, &name_node)?;
        if !entries.is_empty() {
            cfg.name_node = Some(entries.join(","));
        }
    }

    // `hdfs.name-node.<nameservice>` is sugar for Hadoop's own declaration of
    // an HA nameservice, which the resolver reads back from the options.
    let nameservice_prefix = format!("{HDFS_NAME_NODE}.");
    let declared_keys: Vec<String> = m
        .keys()
        .filter(|key| key.starts_with(&nameservice_prefix))
        .cloned()
        .collect();
    let mut declared = Vec::new();
    for key in declared_keys {
        let value = m.remove(&key).unwrap_or_default();
        let nameservice = key[nameservice_prefix.len()..].to_string();
        if nameservice.is_empty() || nameservice.chars().any(char::is_whitespace) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Invalid property `{key}`: a nameservice name must follow `{nameservice_prefix}`"
                ),
            ));
        }
        let entries = hdfs_native_name_node_list(&key, &value)?;
        if entries.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Invalid property `{key}`: no NameNodes"),
            ));
        }
        declared.push((nameservice, entries));
    }

    // A config carried over from PyIceberg would otherwise change identity
    // silently; the client reads `HADOOP_USER_NAME` and the Kerberos cache.
    for key in HDFS_UNSUPPORTED_KEYS {
        if m.remove(key).is_some() {
            tracing::warn!("`{key}` is not supported by the hdfs-native backend and is ignored");
        }
    }
    if m.contains_key(HDFS_HADOOP_CONF_PREFIX) {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Invalid property `{HDFS_HADOOP_CONF_PREFIX}`: a Hadoop key must follow the prefix"
            ),
        ));
    }

    let host = m
        .remove(HDFS_HOST)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let port = m
        .remove(HDFS_PORT)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .map(|port| {
            port.parse::<u16>().map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Invalid `{HDFS_PORT}`: {port}: {e}"),
                )
            })
        })
        .transpose()?;

    let mut options: HashMap<String, String> = m
        .into_iter()
        .filter_map(|(key, value)| {
            key.strip_prefix(HDFS_HADOOP_CONF_PREFIX)
                .map(|stripped| (stripped.to_string(), value))
        })
        .collect();
    // Explicit `hadoop.` keys win over the sugar.
    for (nameservice, entries) in declared {
        let ids: Vec<String> = (0..entries.len()).map(|i| format!("nn{i}")).collect();
        options
            .entry(format!("{HA_NAMENODES_PREFIX}.{nameservice}"))
            .or_insert_with(|| ids.join(","));
        for (id, entry) in ids.iter().zip(&entries) {
            options
                .entry(format!(
                    "{HA_NAMENODE_RPC_ADDRESS_PREFIX}.{nameservice}.{id}"
                ))
                .or_insert_with(|| entry.trim_start_matches("hdfs://").to_string());
        }
    }
    // PyIceberg's `hdfs.host`/`hdfs.port` name the filesystem for
    // authority-less paths, which is what Hadoop's `fs.defaultFS` means; an
    // explicit `hadoop.fs.defaultFS` wins.
    match host {
        Some(host) => {
            // An IPv6 literal needs brackets in a URI authority.
            let host = if host.contains(':') && !host.starts_with('[') {
                format!("[{host}]")
            } else {
                host
            };
            let port = port.unwrap_or(HDFS_DEFAULT_PORT);
            options
                .entry(FS_DEFAULT_FS.to_string())
                .or_insert_with(|| format!("hdfs://{host}:{port}"));
        }
        None if port.is_some() => {
            tracing::warn!("`{HDFS_PORT}` has no effect without `{HDFS_HOST}` and is ignored");
        }
        None => {}
    }
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

/// Resolves the effective NameNode for a path, plus the relative path. As in
/// Hadoop, an authority with a port is used as is; a logical nameservice
/// authority (no port) resolves through its declaration, and an
/// authority-less path through `hdfs.name-node`, else `fs.defaultFS`. The
/// operator cache, `delete_stream` batching and `relativize_path` all go
/// through this, so they cannot drift apart.
pub(crate) fn hdfs_native_effective_name_node<'a>(
    config: &HdfsNativeConfig,
    path: &'a str,
) -> Result<(String, &'a str)> {
    let (authority, relative_path) = hdfs_native_parse_path(path)?;
    let invalid = |reason: String| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid hdfs path: {path}, {reason}"),
        )
    };
    let name_node = match authority {
        Some(authority) if hdfs_native_name_node(&authority).is_some() => authority,
        Some(logical) => {
            let nameservice = logical.trim_start_matches("hdfs://");
            hdfs_native_nameservice(config, nameservice)?.ok_or_else(|| {
                invalid(format!(
                    "logical nameservice `{nameservice}` is not declared; set `{HDFS_NAME_NODE}.{nameservice}`"
                ))
            })?
        }
        None => match (&config.name_node, hdfs_native_default_fs(config)) {
            (Some(name_node), _) => name_node.clone(),
            (None, Some(default_fs)) => hdfs_native_name_node(default_fs).ok_or_else(|| {
                invalid(format!(
                    "`{FS_DEFAULT_FS}` {default_fs} is not an HDFS host:port, a logical nameservice requires `{HDFS_NAME_NODE}`"
                ))
            })?,
            (None, None) => {
                return Err(invalid(format!(
                    "authority-less paths require `{HDFS_NAME_NODE}` or `{HDFS_HOST}`"
                )));
            }
        },
    };
    Ok((name_node, relative_path))
}

/// `fs.defaultFS` from the forwarded options, as written.
fn hdfs_native_default_fs(config: &HdfsNativeConfig) -> Option<&str> {
    config
        .options
        .as_ref()?
        .get(FS_DEFAULT_FS)
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
}

/// The NameNodes that Hadoop's keys in the forwarded options declare for a
/// nameservice, if any. A declaration with a missing or malformed address is
/// an error rather than a silent fallback.
fn hdfs_native_nameservice(config: &HdfsNativeConfig, nameservice: &str) -> Result<Option<String>> {
    let Some(options) = config.options.as_ref() else {
        return Ok(None);
    };
    let Some(ids) = options.get(&format!("{HA_NAMENODES_PREFIX}.{nameservice}")) else {
        return Ok(None);
    };
    let name_nodes = ids
        .split(',')
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(|id| {
            let key = format!("{HA_NAMENODE_RPC_ADDRESS_PREFIX}.{nameservice}.{id}");
            options
                .get(&key)
                .and_then(|value| hdfs_native_name_node(value))
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Nameservice `{nameservice}` declares NameNode `{id}` but `{key}` is missing or not host:port"
                        ),
                    )
                })
        })
        .collect::<Result<Vec<_>>>()?;
    if name_nodes.is_empty() {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Nameservice `{nameservice}` declares no NameNodes"),
        ));
    }
    Ok(Some(name_nodes.join(",")))
}

/// State of [`OpenDalStorage::HdfsNative`](crate::OpenDalStorage::HdfsNative):
/// the parsed configuration and the per-NameNode operator cache. Only the
/// storage factories build it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HdfsNativeStorage {
    pub(crate) config: Arc<HdfsNativeConfig>,
    #[serde(skip, default)]
    pub(crate) operators: HdfsNativeOperatorCache,
    #[serde(default)]
    pub(crate) client_config: OpenDalClientConfig,
}

impl HdfsNativeStorage {
    pub(crate) fn new(config: HdfsNativeConfig, client_config: OpenDalClientConfig) -> Self {
        Self {
            config: Arc::new(config),
            operators: HdfsNativeOperatorCache::default(),
            client_config,
        }
    }
}

/// Operators cached per effective NameNode: each holds an `hdfs-native`
/// client with live RPC connections, whose tasks run on the tokio runtime
/// current when it was built. An entry is rebuilt once that runtime is
/// gone, as `hdfs-native` panics when
/// it spawns onto a dead one. The cache lives as long as the storage that
/// owns it (clones share it).
#[derive(Clone, Debug, Default)]
pub(crate) struct HdfsNativeOperatorCache(Arc<RwLock<HashMap<String, CachedOperator>>>);

#[derive(Debug)]
struct CachedOperator {
    operator: Operator,
    sentinel: RuntimeSentinel,
}

/// A task parked on the building runtime that owns the token, so the token
/// outlives it only while that runtime is alive. Aborted on drop so entries
/// do not leave parked tasks behind.
#[derive(Debug)]
struct RuntimeSentinel {
    alive: Weak<()>,
    task: JoinHandle<()>,
}

impl RuntimeSentinel {
    fn spawn(handle: &Handle) -> Self {
        let token = Arc::new(());
        let alive = Arc::downgrade(&token);
        let task = handle.spawn(async move {
            let _token = token;
            std::future::pending::<()>().await
        });
        Self { alive, task }
    }
}

impl Drop for RuntimeSentinel {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl CachedOperator {
    fn new(operator: Operator, handle: &Handle) -> Self {
        Self {
            operator,
            sentinel: RuntimeSentinel::spawn(handle),
        }
    }

    fn runtime_alive(&self) -> bool {
        self.sentinel.alive.strong_count() > 0
    }
}

impl HdfsNativeOperatorCache {
    pub(crate) fn get(&self, name_node: &str) -> Result<Option<Operator>> {
        Ok(self
            .0
            .read()
            .map_err(poisoned)?
            .get(name_node)
            .filter(|cached| cached.runtime_alive())
            .map(|cached| cached.operator.clone()))
    }

    /// Inserts `op` unless a concurrent caller got there first, returning
    /// whichever operator the cache now holds; a stale entry is replaced.
    fn insert(&self, name_node: String, op: Operator, handle: &Handle) -> Result<Operator> {
        let mut operators = self.0.write().map_err(poisoned)?;
        match operators.entry(name_node) {
            Entry::Occupied(entry) if entry.get().runtime_alive() => {
                Ok(entry.get().operator.clone())
            }
            Entry::Occupied(mut entry) => {
                entry.insert(CachedOperator::new(op.clone(), handle));
                Ok(op)
            }
            Entry::Vacant(entry) => {
                entry.insert(CachedOperator::new(op.clone(), handle));
                Ok(op)
            }
        }
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
pub(crate) async fn hdfs_native_create_operator<'a>(
    path: &'a str,
    config: &Arc<HdfsNativeConfig>,
    operators: &HdfsNativeOperatorCache,
) -> Result<(Operator, &'a str)> {
    let (name_node, relative_path) = hdfs_native_effective_name_node(config, path)?;

    if let Some(op) = operators.get(&name_node)? {
        return Ok((op, relative_path));
    }

    // Every operator in this crate needs a tokio runtime for its I/O (the
    // timeout layer), so say so instead of panicking in `spawn_blocking`.
    let handle = Handle::try_current().map_err(|_| {
        Error::new(
            ErrorKind::FeatureUnsupported,
            "HDFS storage requires a tokio runtime",
        )
    })?;

    // The build reads the Hadoop XML config synchronously, so it runs on a
    // blocking thread and outside the lock. A racing first caller may build
    // too; the loser is dropped before opening any connection.
    let build_config = Arc::clone(config);
    let build_name_node = name_node.clone();
    let op = handle
        .spawn_blocking(move || hdfs_native_operator_build(&build_config, &build_name_node))
        .await
        .map_err(|e| {
            Error::new(ErrorKind::Unexpected, "HDFS operator build task failed").with_source(e)
        })??;
    Ok((operators.insert(name_node, op, &handle)?, relative_path))
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
        assert_eq!(parse(" , "), None);
        assert_eq!(
            parse(" hdfs://nn:8020/ ").as_deref(),
            Some("hdfs://nn:8020")
        );
        // Per entry: a space after the comma would break failover to nn2.
        assert_eq!(
            parse("hdfs://nn1:8020/, hdfs://nn2:8020/,").as_deref(),
            Some("hdfs://nn1:8020,hdfs://nn2:8020")
        );
    }

    #[test]
    fn test_hdfs_native_config_parse_host_port_as_default_fs() {
        let parse = |props: &[(&str, &str)]| {
            let props = props
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            hdfs_native_config_parse(props)
                .map(|cfg| cfg.options.and_then(|o| o.get(FS_DEFAULT_FS).cloned()))
        };

        assert_eq!(
            parse(&[(HDFS_HOST, "nn"), (HDFS_PORT, " 9000 ")])
                .unwrap()
                .as_deref(),
            Some("hdfs://nn:9000")
        );
        assert_eq!(
            parse(&[(HDFS_HOST, " nn ")]).unwrap().as_deref(),
            Some("hdfs://nn:8020")
        );
        assert_eq!(
            parse(&[(HDFS_HOST, "::1")]).unwrap().as_deref(),
            Some("hdfs://[::1]:8020")
        );
        // An explicit `hadoop.fs.defaultFS` wins.
        assert_eq!(
            parse(&[
                (HDFS_HOST, "nn"),
                ("hadoop.fs.defaultFS", "hdfs://explicit:8020")
            ])
            .unwrap()
            .as_deref(),
            Some("hdfs://explicit:8020")
        );
        // An empty host is unset; a port without a host has nothing to apply to.
        assert_eq!(
            parse(&[(HDFS_HOST, " "), (HDFS_PORT, "9000")]).unwrap(),
            None
        );
        assert_eq!(parse(&[(HDFS_PORT, "9000")]).unwrap(), None);
        // A bad port is rejected whether or not a host accompanies it.
        for props in [
            &[(HDFS_HOST, "nn"), (HDFS_PORT, "x")][..],
            &[(HDFS_PORT, "x")][..],
            &[(HDFS_PORT, "70000")][..],
        ] {
            let err = parse(props).unwrap_err();
            assert!(err.to_string().contains(HDFS_PORT), "{props:?}: {err}");
        }
    }

    #[test]
    fn test_hdfs_native_config_parse_rejects_bare_prefix_and_drops_unsupported_keys() {
        let err = hdfs_native_config_parse(HashMap::from([(
            HDFS_HADOOP_CONF_PREFIX.to_string(),
            "x".to_string(),
        )]))
        .unwrap_err();
        assert!(err.to_string().contains(HDFS_HADOOP_CONF_PREFIX), "{err}");

        // PyIceberg's identity keys are ignored (with a warning), never
        // forwarded as Hadoop options.
        let cfg = hdfs_native_config_parse(HashMap::from([
            ("hdfs.user".to_string(), "alice".to_string()),
            (
                "hdfs.kerberos_ticket".to_string(),
                "/tmp/krb5cc".to_string(),
            ),
        ]))
        .unwrap();
        assert_eq!(cfg.options, None);
    }

    #[test]
    fn test_hdfs_native_config_parse_name_node_requires_port() {
        let parse = |value: &str| {
            hdfs_native_config_parse(HashMap::from([(
                HDFS_NAME_NODE.to_string(),
                value.to_string(),
            )]))
        };

        // `hdfs://` is optional (Hadoop's own rpc-address format is bare) and
        // every entry is normalized to it, so it keys the cache like a path
        // authority does.
        for (value, normalized) in [
            ("nn:8020", "hdfs://nn:8020"),
            ("hdfs://nn:8020/", "hdfs://nn:8020"),
            ("[::1]:8020", "hdfs://[::1]:8020"),
            (
                "hdfs://nn1:8020, nn2:8020",
                "hdfs://nn1:8020,hdfs://nn2:8020",
            ),
        ] {
            assert_eq!(parse(value).unwrap().name_node.as_deref(), Some(normalized));
        }
        for value in [
            "hdfs://ns1",
            "nn",
            "nn:x",
            "nn:0",
            "nn:+80",
            "::1",
            "hdfs://[::1]",
            "viewfs://nn:8020",
            "foo://nn:8020",
            "user@nn:8020",
            "nn:8020/path",
            "hdfs://nn1:8020,nn2",
        ] {
            let err = parse(value).unwrap_err().to_string();
            assert!(err.contains(HDFS_NAME_NODE), "{value}: {err}");
        }
    }

    #[test]
    fn test_hdfs_native_default_fs_must_be_hdfs() {
        let parse = |default_fs: &str| {
            hdfs_native_config_parse(HashMap::from([(
                "hadoop.fs.defaultFS".to_string(),
                default_fs.to_string(),
            )]))
            .unwrap()
        };

        let (nn, _) =
            hdfs_native_effective_name_node(&parse("hdfs://nn:8020/"), "hdfs:///a").unwrap();
        assert_eq!(nn, "hdfs://nn:8020");
        for default_fs in ["viewfs://cluster/", "hdfs://", ""] {
            assert!(hdfs_native_effective_name_node(&parse(default_fs), "hdfs:///a").is_err());
        }
        // Portless: a logical nameservice, which only `hdfs.name-node` resolves.
        let err = hdfs_native_effective_name_node(&parse("hdfs://ns1"), "hdfs:///a").unwrap_err();
        assert!(
            err.to_string().contains(FS_DEFAULT_FS) && err.to_string().contains(HDFS_NAME_NODE),
            "{err}"
        );
        // Normalized like every other source.
        let (nn, _) = hdfs_native_effective_name_node(&parse("nn:9000"), "hdfs:///a").unwrap();
        assert_eq!(nn, "hdfs://nn:9000");
    }

    /// A bare configured NameNode and the same NameNode as a path authority
    /// share one cache entry.
    #[tokio::test]
    async fn test_hdfs_native_create_operator_dedupes_spellings() {
        let config = Arc::new(
            hdfs_native_config_parse(HashMap::from([(
                HDFS_NAME_NODE.to_string(),
                "nn:8020".to_string(),
            )]))
            .unwrap(),
        );
        let operators = HdfsNativeOperatorCache::default();

        for path in ["hdfs:///a", "hdfs://nn:8020/b"] {
            hdfs_native_create_operator(path, &config, &operators)
                .await
                .unwrap();
        }

        assert_eq!(operators.len(), 1);
        assert!(operators.get("hdfs://nn:8020").unwrap().is_some());
    }

    #[test]
    fn test_hdfs_native_config_parse_declares_nameservices() {
        let cfg = hdfs_native_config_parse(HashMap::from([
            (
                format!("{HDFS_NAME_NODE}.ns-a"),
                "nn1:8020, hdfs://nn2:8020/".to_string(),
            ),
            ("hadoop.dfs.ha.namenodes.ns-b".to_string(), "x".to_string()),
            (
                "hadoop.dfs.namenode.rpc-address.ns-b.x".to_string(),
                "nn3:8020".to_string(),
            ),
        ]))
        .unwrap();
        let options = cfg.options.clone().unwrap();

        // The sugar expands to Hadoop's keys...
        assert_eq!(
            options.get("dfs.ha.namenodes.ns-a").map(String::as_str),
            Some("nn0,nn1")
        );
        assert_eq!(
            options
                .get("dfs.namenode.rpc-address.ns-a.nn0")
                .map(String::as_str),
            Some("nn1:8020")
        );
        assert_eq!(
            options
                .get("dfs.namenode.rpc-address.ns-a.nn1")
                .map(String::as_str),
            Some("nn2:8020")
        );
        // ...and the resolver reads both forms back.
        assert_eq!(
            hdfs_native_nameservice(&cfg, "ns-a").unwrap().as_deref(),
            Some("hdfs://nn1:8020,hdfs://nn2:8020")
        );
        assert_eq!(
            hdfs_native_nameservice(&cfg, "ns-b").unwrap().as_deref(),
            Some("hdfs://nn3:8020")
        );
        assert_eq!(hdfs_native_nameservice(&cfg, "ns-c").unwrap(), None);

        for (key, value) in [
            (format!("{HDFS_NAME_NODE}."), "nn:8020"),
            (format!("{HDFS_NAME_NODE}.ns"), "nn"),
            (format!("{HDFS_NAME_NODE}.ns"), " "),
        ] {
            let err = hdfs_native_config_parse(HashMap::from([(key.clone(), value.to_string())]))
                .unwrap_err();
            assert!(err.to_string().contains(&key), "{key}={value}: {err}");
        }
        // A declaration with a broken address is an error, not a silent fallback.
        let broken = hdfs_native_config_parse(HashMap::from([(
            "hadoop.dfs.ha.namenodes.ns-d".to_string(),
            "a".to_string(),
        )]))
        .unwrap();
        assert!(hdfs_native_nameservice(&broken, "ns-d").is_err());
    }

    #[test]
    fn test_hdfs_native_effective_name_node_declared_nameservices() {
        let parse = |props: &[(String, &str)]| {
            hdfs_native_config_parse(
                props
                    .iter()
                    .map(|(k, v)| (k.clone(), v.to_string()))
                    .collect(),
            )
            .unwrap()
        };
        let ns_a = (
            format!("{HDFS_NAME_NODE}.ns-a"),
            "hdfs://a1:8020,hdfs://a2:8020",
        );
        let declared_only = parse(std::slice::from_ref(&ns_a));
        let with_default = parse(&[ns_a.clone(), (HDFS_NAME_NODE.to_string(), "hdfs://d:8020")]);

        // A declared nameservice resolves to its own list, ahead of the plain key.
        for config in [&declared_only, &with_default] {
            let (nn, rel) = hdfs_native_effective_name_node(config, "hdfs://ns-a/x").unwrap();
            assert_eq!((nn.as_str(), rel), ("hdfs://a1:8020,hdfs://a2:8020", "x"));
        }
        // An undeclared one is an error, with or without the plain key.
        for config in [&declared_only, &with_default] {
            let err = hdfs_native_effective_name_node(config, "hdfs://ns-b/x").unwrap_err();
            assert!(err.to_string().contains("`hdfs.name-node.ns-b`"), "{err}");
        }
        // The plain key serves authority-less paths only.
        let (nn, _) = hdfs_native_effective_name_node(&with_default, "hdfs:///x").unwrap();
        assert_eq!(nn, "hdfs://d:8020");
        // A concrete authority is never redirected.
        let (nn, _) =
            hdfs_native_effective_name_node(&with_default, "hdfs://other:9000/x").unwrap();
        assert_eq!(nn, "hdfs://other:9000");
    }

    #[tokio::test]
    async fn test_hdfs_native_create_operator_per_nameservice() {
        let config = Arc::new(
            hdfs_native_config_parse(HashMap::from([
                (
                    format!("{HDFS_NAME_NODE}.ns-a"),
                    "hdfs://a:8020".to_string(),
                ),
                (
                    format!("{HDFS_NAME_NODE}.ns-b"),
                    "hdfs://b:8020".to_string(),
                ),
                (HDFS_NAME_NODE.to_string(), "hdfs://d:8020".to_string()),
            ]))
            .unwrap(),
        );
        let operators = HdfsNativeOperatorCache::default();

        for path in ["hdfs://ns-a/x", "hdfs://ns-b/y", "hdfs:///w"] {
            hdfs_native_create_operator(path, &config, &operators)
                .await
                .unwrap();
        }
        // ns-a, ns-b, and the plain key for the authority-less path; an
        // undeclared nameservice never reaches an operator.
        assert_eq!(operators.len(), 3);
        assert_eq!(
            hdfs_native_batch_key(&config, "hdfs://ns-b/y"),
            "hdfs://b:8020"
        );
        let err = hdfs_native_create_operator("hdfs://ns-c/z", &config, &operators)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("`hdfs.name-node.ns-c`"), "{err}");
        assert_eq!(operators.len(), 3);
    }

    #[test]
    fn test_hdfs_native_effective_name_node_precedence() {
        let configured = hdfs_native_config_parse(HashMap::from([
            (
                HDFS_NAME_NODE.to_string(),
                "hdfs://nn1:8020,hdfs://nn2:8020".to_string(),
            ),
            (HDFS_HOST.to_string(), "ignored".to_string()),
        ]))
        .unwrap();
        let default_fs =
            hdfs_native_config_parse(HashMap::from([(HDFS_HOST.to_string(), "nn".to_string())]))
                .unwrap();
        let unconfigured = HdfsNativeConfig::default();

        // An authority with a port is used as is, whatever is configured.
        for config in [&configured, &default_fs, &unconfigured] {
            let (nn, rel) =
                hdfs_native_effective_name_node(config, "hdfs://other:9000/a/b").unwrap();
            assert_eq!((nn.as_str(), rel), ("hdfs://other:9000", "a/b"));
        }
        // An authority-less path uses the configured list, ahead of `hdfs.host`.
        let (nn, _) = hdfs_native_effective_name_node(&configured, "hdfs:///y").unwrap();
        assert_eq!(nn, "hdfs://nn1:8020,hdfs://nn2:8020");
        // A logical nameservice never falls back to it: it must be declared.
        let err = hdfs_native_effective_name_node(&configured, "hdfs://ns-a/x").unwrap_err();
        assert!(err.to_string().contains("`hdfs.name-node.ns-a`"), "{err}");
        // Only an authority-less path falls back to `fs.defaultFS`.
        let (nn, rel) = hdfs_native_effective_name_node(&default_fs, "hdfs:///y").unwrap();
        assert_eq!((nn.as_str(), rel), ("hdfs://nn:8020", "y"));
        let err = hdfs_native_effective_name_node(&default_fs, "hdfs://ns-a/x").unwrap_err();
        assert!(err.to_string().contains("logical nameservice `ns-a`"));
        // Nothing applicable: pointed errors.
        for path in ["hdfs:///a", "hdfs://ns-a/x"] {
            let err = hdfs_native_effective_name_node(&unconfigured, path).unwrap_err();
            assert!(err.to_string().contains(HDFS_NAME_NODE), "{err}");
        }
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
    fn test_hdfs_native_parse_path_ipv6_authority_keeps_brackets() {
        let (nn, rel) = hdfs_native_parse_path("hdfs://[::1]:8020/a/b").unwrap();
        assert_eq!(nn.as_deref(), Some("hdfs://[::1]:8020"));
        assert_eq!(rel, "a/b");
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

    // Creating an operator offloads the build to a blocking thread, so these
    // run under tokio; `test_hdfs_native_operator_build_needs_no_runtime`
    // pins that the build itself does not.
    #[tokio::test]
    async fn test_hdfs_native_create_operator_plain_key_serves_authority_less_paths() {
        let config = Arc::new(
            hdfs_native_config_parse(HashMap::from([(
                HDFS_NAME_NODE.to_string(),
                "hdfs://nn1:8020/,hdfs://nn2:8020/".to_string(),
            )]))
            .unwrap(),
        );
        let operators = HdfsNativeOperatorCache::default();

        // Authority-less paths share the configured list's operator.
        let (_, rel) = hdfs_native_create_operator("hdfs:///a/b", &config, &operators)
            .await
            .unwrap();
        assert_eq!(rel, "a/b");
        assert_eq!(operators.len(), 1);
        assert!(
            operators
                .get("hdfs://nn1:8020,hdfs://nn2:8020")
                .unwrap()
                .is_some()
        );

        // A logical nameservice must be declared; a concrete authority is
        // another cluster, never the configured one.
        let err = hdfs_native_create_operator("hdfs://ns-a/c", &config, &operators)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("`hdfs.name-node.ns-a`"), "{err}");
        hdfs_native_create_operator("hdfs://other:9000/d", &config, &operators)
            .await
            .unwrap();
        assert_eq!(operators.len(), 2);
        assert!(operators.get("hdfs://other:9000").unwrap().is_some());
    }

    #[tokio::test]
    async fn test_hdfs_native_create_operator_uses_path_authority() {
        let config = Arc::new(HdfsNativeConfig::default());
        let operators = HdfsNativeOperatorCache::default();

        let (_, rel) = hdfs_native_create_operator("hdfs://nn:8020/a/b", &config, &operators)
            .await
            .unwrap();

        assert_eq!(rel, "a/b");
        assert!(operators.get("hdfs://nn:8020").unwrap().is_some());
    }

    #[tokio::test]
    async fn test_hdfs_native_create_operator_caches_per_name_node() {
        let config = Arc::new(HdfsNativeConfig::default());
        let operators = HdfsNativeOperatorCache::default();

        for path in [
            "hdfs://nn1:8020/a",
            "hdfs://nn1:8020/b",
            "hdfs://nn2:8020/c",
        ] {
            hdfs_native_create_operator(path, &config, &operators)
                .await
                .unwrap();
        }

        assert_eq!(operators.len(), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_hdfs_native_create_operator_concurrent_callers_share_one_entry() {
        let config = Arc::new(HdfsNativeConfig::default());
        let operators = HdfsNativeOperatorCache::default();

        let callers: Vec<_> = (0..8)
            .map(|_| {
                let (config, operators) = (config.clone(), operators.clone());
                tokio::spawn(async move {
                    hdfs_native_create_operator("hdfs://nn:8020/a", &config, &operators)
                        .await
                        .unwrap();
                })
            })
            .collect();
        for caller in callers {
            caller.await.unwrap();
        }

        assert_eq!(operators.len(), 1);
    }

    /// The build itself needs no runtime; only creating a cached operator does.
    #[test]
    fn test_hdfs_native_operator_build_needs_no_runtime() {
        let config = HdfsNativeConfig::default();
        hdfs_native_operator_build(&config, "hdfs://nn:8020").unwrap();
    }

    #[test]
    fn test_hdfs_native_create_operator_without_runtime_errors() {
        let config = Arc::new(HdfsNativeConfig::default());
        let operators = HdfsNativeOperatorCache::default();

        let err = futures::executor::block_on(hdfs_native_create_operator(
            "hdfs://nn:8020/a",
            &config,
            &operators,
        ))
        .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);
        assert_eq!(operators.len(), 0);
    }

    #[tokio::test]
    async fn test_hdfs_native_operator_cache_keeps_first_insert() {
        let operators = HdfsNativeOperatorCache::default();
        let handle = Handle::current();
        let build = |root: &str| {
            let mut config = hdfs_native_config_parse(HashMap::new()).unwrap();
            config.root = Some(root.to_string());
            hdfs_native_operator_build(&config, "hdfs://nn:8020").unwrap()
        };

        // Two callers racing for one NameNode: the first insert wins and both
        // get the cached operator.
        let first = operators
            .insert("hdfs://nn:8020".to_string(), build("/first"), &handle)
            .unwrap();
        let second = operators
            .insert("hdfs://nn:8020".to_string(), build("/second"), &handle)
            .unwrap();
        assert_eq!(first.info().root(), "/first/");
        assert_eq!(second.info().root(), "/first/");
        assert_eq!(operators.len(), 1);
    }

    #[test]
    fn test_hdfs_native_operator_cache_rebuilds_after_runtime_shutdown() {
        let flavors: [fn() -> tokio::runtime::Runtime; 2] = [
            || {
                tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap()
            },
            || tokio::runtime::Builder::new_multi_thread().build().unwrap(),
        ];
        for build_runtime in flavors {
            let operators = HdfsNativeOperatorCache::default();
            let config = Arc::new(HdfsNativeConfig::default());

            let runtime = build_runtime();
            runtime.block_on(async {
                hdfs_native_create_operator("hdfs://nn:8020/a", &config, &operators)
                    .await
                    .unwrap();
            });
            assert!(operators.get("hdfs://nn:8020").unwrap().is_some());

            // The building runtime is gone: the entry is stale and the next
            // runtime to use it rebuilds it.
            drop(runtime);
            assert!(operators.get("hdfs://nn:8020").unwrap().is_none());
            let next = build_runtime();
            next.block_on(async {
                hdfs_native_create_operator("hdfs://nn:8020/a", &config, &operators)
                    .await
                    .unwrap();
            });
            assert!(operators.get("hdfs://nn:8020").unwrap().is_some());
            assert_eq!(operators.len(), 1);
        }
    }

    #[test]
    fn test_hdfs_native_operator_cache_drops_its_sentinel_task() {
        let flavors: [fn() -> tokio::runtime::Runtime; 2] = [
            || {
                tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap()
            },
            || tokio::runtime::Builder::new_multi_thread().build().unwrap(),
        ];
        for build_runtime in flavors {
            build_runtime().block_on(async {
                let metrics = Handle::current().metrics();
                let baseline = metrics.num_alive_tasks();

                let operators = HdfsNativeOperatorCache::default();
                let config = Arc::new(HdfsNativeConfig::default());
                hdfs_native_create_operator("hdfs://nn:8020/a", &config, &operators)
                    .await
                    .unwrap();
                assert_eq!(metrics.num_alive_tasks(), baseline + 1);

                // Dropping the cache aborts the sentinel; the abort lands once
                // the runtime schedules the task.
                drop(operators);
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                while metrics.num_alive_tasks() != baseline {
                    assert!(std::time::Instant::now() < deadline, "sentinel task leaked");
                    tokio::task::yield_now().await;
                }
            });
        }
    }

    #[tokio::test]
    async fn test_hdfs_native_create_operator_authority_less_without_config_errors() {
        let config = Arc::new(HdfsNativeConfig::default());
        let operators = HdfsNativeOperatorCache::default();

        let err = hdfs_native_create_operator("hdfs:///a/b", &config, &operators)
            .await
            .unwrap_err();

        assert!(err.to_string().contains(HDFS_NAME_NODE));
    }
}
