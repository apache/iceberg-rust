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
//! Google Cloud Storage properties

use std::collections::HashMap;

use opendal::services::GcsConfig;
use opendal::Operator;
use url::Url;

use crate::{Error, ErrorKind, Result};

// Reference: https://github.com/apache/iceberg/blob/main/gcp/src/main/java/org/apache/iceberg/gcp/GCPProperties.java

/// Google Cloud Project ID
pub const GCS_PROJECT_ID: &str = "gcs.project-id";
/// Google Cloud Storage endpoint
pub const GCS_SERVICE_PATH: &str = "gcs.service.path";
/// Google Cloud user project
pub const GCS_USER_PROJECT: &str = "gcs.user-project";

/// Parse iceberg properties to [`GcsConfig`].
pub(crate) fn gcs_config_parse(mut m: HashMap<String, String>) -> Result<GcsConfig> {
    let mut cfg = GcsConfig::default();

    if let Some(endpoint) = m.remove(GCS_SERVICE_PATH) {
        cfg.endpoint = Some(endpoint);
    }

    Ok(cfg)
}

/// Build a new OpenDAL [`Operator`] based on a provided [`GcsConfig`].
pub(crate) fn gcs_config_build(cfg: &GcsConfig, path: &str) -> Result<Operator> {
    let url = Url::parse(path)?;
    let bucket = url.host_str().ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid gcs url: {}, bucket is required", path),
        )
    })?;

    let mut cfg = cfg.clone();
    cfg.bucket = bucket.to_string();
    Ok(Operator::from_config(cfg)?.finish())
}
