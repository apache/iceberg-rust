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

use opendal::services::GcsConfig;
use opendal::Operator;
use url::Url;

use crate::{Error, ErrorKind, Result};

// [Google Cloud Storage](https://py.iceberg.apache.org/configuration/#google-cloud-storage)
// configuration parameters

/// Google Project ID
pub const GOOGLE_PROJECT_ID: &str = "gcs.project-id";
/// Google Cloud OAuth token
pub const GCS_ENDPOINT: &str = "gcs.endpoint";
pub const GCS_OAUTH: &str = "gcs.oauth2.token";
/// Google Cloud default location
pub const GCS_DEFAULT_LOCATION: &str = "gcs.default-location";

/// Parse iceberg properties to Google Cloud config.
pub(crate) fn gcs_config_parse(mut m: HashMap<String, String>) -> Result<GcsConfig> {
    let mut cfg = GcsConfig::default();
    if let Some(endpoint) = m.remove(GCS_ENDPOINT) {
        cfg.endpoint = Some(endpoint);
    };

    Ok(cfg)
}

/// Build a new OpenDAL [`Operator`] based on a known [`GcsConfig`].
pub(crate) fn gcs_config_build(_cfg: &GcsConfig) -> Result<Operator> {
    todo!();
}
