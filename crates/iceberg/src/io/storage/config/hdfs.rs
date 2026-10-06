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

/// HDFS NameNode RPC endpoint(s) as `host:port`, e.g. `hdfs://namenode:8020`;
/// a comma-separated list enables HA failover. Resolves authority-less paths
/// and, as the single-cluster default, every logical nameservice authority
/// (no port) that is not declared on its own; as in Hadoop, an authority with
/// a port is always used as is.
///
/// A catalog spanning several HDFS clusters must declare each nameservice:
/// `hdfs.name-node.<nameservice>` takes the same list and is sugar for
/// Hadoop's `dfs.ha.namenodes.<nameservice>` and
/// `dfs.namenode.rpc-address.<nameservice>.<id>`, which are honored as well
/// when passed through [`HDFS_HADOOP_CONF_PREFIX`]. With only the plain key
/// set, an unknown or mistyped nameservice routes to that one cluster.
pub const HDFS_NAME_NODE: &str = "hdfs.name-node";
/// NameNode host for authority-less paths, as in PyIceberg; paths that carry
/// an authority ignore it, as they do there. Combined with [`HDFS_PORT`] into
/// Hadoop's `fs.defaultFS`. PyIceberg's `hdfs.user` and `hdfs.kerberos_ticket`
/// have no equivalent: the client reads `HADOOP_USER_NAME` and the default
/// Kerberos credential cache.
pub const HDFS_HOST: &str = "hdfs.host";
/// NameNode port for [`HDFS_HOST`]; defaults to `8020` and has no effect
/// without the host.
pub const HDFS_PORT: &str = "hdfs.port";
/// Prefix for properties forwarded to the HDFS client configuration with the
/// prefix stripped: `hadoop.dfs.client.failover.random.order` is forwarded as
/// `dfs.client.failover.random.order`. Forwarded values override those loaded
/// from `$HADOOP_CONF_DIR`; `hadoop.fs.defaultFS` also serves authority-less
/// paths.
pub const HDFS_HADOOP_CONF_PREFIX: &str = "hadoop.";
