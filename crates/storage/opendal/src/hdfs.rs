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

//! HDFS storage support via OpenDAL's `services-hdfs-native` backend.
//!
//! Cluster topology (HA name services, namenode RPC addresses) and Kerberos
//! authentication are entirely delegated to `hdfs-native` and its
//! environment. `hdfs-native` reads `core-site.xml` / `hdfs-site.xml` from
//! `$HADOOP_CONF_DIR` (or `$HADOOP_HOME/etc/hadoop`); Kerberos goes through
//! `libgssapi_krb5` via the standard `KRB5CCNAME` / `KRB5_CONFIG` env. The
//! caller's `libgssapi_krb5` must be installed on the host (e.g.
//! `brew install krb5` on macOS, `apt install libgssapi-krb5-2` on Debian)
//! for HDFS calls to link at runtime.
//!
//! No iceberg-level HDFS configuration is exposed - mirroring the Java
//! HadoopFileIO, which has no iceberg-side HDFS knobs and defers everything
//! to Hadoop's `Configuration`. Paths with an authority (`hdfs://ns/foo`)
//! route to that name node; authority-less paths (`hdfs:///foo`) are passed
//! to `hdfs-native` without an explicit name node, so it picks up
//! `fs.defaultFS` from the loaded Hadoop config.

use iceberg::{Error, ErrorKind, Result};
use opendal::Operator;
use opendal::services::HdfsNative;
use url::Url;

use crate::utils::from_opendal_error;

/// Parse an HDFS path into its name node (when an authority is present) and
/// the relative path beginning with `/`.
///
/// The returned `Option<String>` is `Some("hdfs://<authority>")` when the
/// input has an authority and `None` when it does not. `None` causes the
/// operator to be built without an explicit name node, so that `hdfs-native`
/// resolves `fs.defaultFS` from the loaded Hadoop config.
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
    // it always starts at the first `/` after the `hdfs://` prefix, or is
    // implicitly `/` when only an authority is given.
    let after_scheme = &path["hdfs://".len()..];
    let rel = match after_scheme.find('/') {
        Some(i) => &after_scheme[i..],
        None => "/",
    };

    Ok((name_node, rel))
}

/// Build a new OpenDAL [`Operator`] for the given name node, or - when
/// `name_node` is `None` - one that defers to `fs.defaultFS` from the
/// `hdfs-native`-loaded Hadoop config.
pub(crate) fn hdfs_operator_build(name_node: Option<&str>) -> Result<Operator> {
    let mut builder = HdfsNative::default().root("/");
    if let Some(nn) = name_node {
        builder = builder.name_node(nn);
    }
    Ok(Operator::new(builder).map_err(from_opendal_error)?.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_hdfs_path_with_authority_and_rel() {
        let (nn, rel) = parse_hdfs_path("hdfs://nameservice1/a/b").unwrap();

        assert_eq!(nn.as_deref(), Some("hdfs://nameservice1"));
        assert_eq!(rel, "/a/b");
    }

    #[test]
    fn test_parse_hdfs_path_with_authority_and_port() {
        let (nn, rel) = parse_hdfs_path("hdfs://nn:8020/foo").unwrap();

        assert_eq!(nn.as_deref(), Some("hdfs://nn:8020"));
        assert_eq!(rel, "/foo");
    }

    #[test]
    fn test_parse_hdfs_path_with_authority_no_path() {
        let (nn, rel) = parse_hdfs_path("hdfs://nameservice1").unwrap();

        assert_eq!(nn.as_deref(), Some("hdfs://nameservice1"));
        assert_eq!(rel, "/");
    }

    #[test]
    fn test_parse_hdfs_path_with_authority_trailing_slash() {
        let (nn, rel) = parse_hdfs_path("hdfs://nameservice1/").unwrap();

        assert_eq!(nn.as_deref(), Some("hdfs://nameservice1"));
        assert_eq!(rel, "/");
    }

    #[test]
    fn test_parse_hdfs_path_authority_less_returns_none() {
        let (nn, rel) = parse_hdfs_path("hdfs:///a/b").unwrap();

        assert_eq!(nn, None);
        assert_eq!(rel, "/a/b");
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
}
