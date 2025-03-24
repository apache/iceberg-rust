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

use std::collections::HashMap;

use opendal::services::WebhdfsConfig;
use opendal::{Configurator, Operator};

use crate::io::is_truthy;
use crate::{Error, ErrorKind, Result};

/// Configure the HDFS host to connect to
pub const HDSF_HOST: &str = "hdfs.host";
/// Configure the HDFS part to connect to
pub const HDSF_PORT: &str = "hdfs.port";
/// Configure the HDFS username used for connection.
pub const HDFS_USER: &str = "hdfs.user";
/// Configure the hdfs to not use the list batch api.
pub const HDFS_DISABLE_LIST_BATCH: &str = "hdfs.disable_list_batch";

/// Parse iceberg props to hdfs config.
pub(crate) fn webhdfs_config_parse(mut m: HashMap<String, String>) -> Result<WebhdfsConfig> {
    let mut cfg = WebhdfsConfig::default();

    let host = if let Some(host) = m.remove(HDSF_HOST) {
        host
    } else {
        return Err(
            Error::new(ErrorKind::Unexpected, "hdfs.host is required to use HDFS")
                .with_context("config", format!("{m:?}")),
        );
    };

    let port = if let Some(port) = m.remove(HDSF_PORT) {
        port
    } else {
        return Err(
            Error::new(ErrorKind::Unexpected, "hdfs.port is required to use HDFS")
                .with_context("config", format!("{m:?}")),
        );
    };

    cfg.endpoint = Some(format!("{}:{}", host, port));

    if let Some(user) = m.remove(HDFS_USER) {
        cfg.user_name = Some(user);
    };
    if let Some(disable_list_batch) = m.remove(HDFS_DISABLE_LIST_BATCH) {
        cfg.disable_list_batch = is_truthy(&disable_list_batch);
    };

    Ok(cfg)
}

/// Build new opendal operator from give path.
pub(crate) fn webhdfs_config_build(cfg: &WebhdfsConfig) -> Result<Operator> {
    let builder = cfg.clone().into_builder();
    Ok(Operator::new(builder)?.finish())
}
