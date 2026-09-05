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

/// HDFS NameNode RPC endpoint(s), e.g. `hdfs://namenode:8020`; a
/// comma-separated list enables HA failover. When unset, the NameNode is
/// derived from the path authority.
pub const HDFS_NAME_NODE: &str = "hdfs.name-node";
/// Prefix for properties forwarded to the HDFS client configuration, e.g.
/// `hadoop.dfs.client.failover.random.order`. Forwarded values (prefix
/// stripped) override those loaded from `$HADOOP_CONF_DIR`.
pub const HDFS_HADOOP_CONF_PREFIX: &str = "hadoop.";
