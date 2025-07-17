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

use opendal::services::HdfsNativeConfig;
use opendal::{Configurator, Operator};

use crate::Result;

/// The configuration key for the default filesystem in core-site.xml.
/// This key is typically used to specify the HDFS namenode address.
pub const FS_DEFAULTFS: &str = "fs.defaultFS";

pub(crate) fn hdfs_native_config_parse(mut m: HashMap<String, String>) -> Result<HdfsNativeConfig> {
    let mut cfg = HdfsNativeConfig::default();
    cfg.root = Some("/".to_string());

    if let Some(default_fs) = m.remove(FS_DEFAULTFS) {
        cfg.name_node = Some(default_fs);
    }

    Ok(cfg)
}

/// Build new opendal operator from give path.
pub(crate) fn hdfs_native_config_build(cfg: &HdfsNativeConfig) -> Result<Operator> {
    let builder = cfg.clone().into_builder();

    Ok(Operator::new(builder)?.finish())
}
