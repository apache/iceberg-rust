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

use crate::{Error, ErrorKind, Result};

// Reference: https://github.com/apache/iceberg/blob/main/gcp/src/main/java/org/apache/iceberg/gcp/GCPProperties.java

/// Google Cloud Storage bucket name
pub const GCS_BUCKET: &str = "gcs.bucket";
/// Google Project ID
pub const GCS_PROJECT_ID: &str = "gcs.project-id";
/// Google Cloud Storage endpoint
pub const GCS_ENDPOINT: &str = "gcs.endpoint";
/// Google Cloud Storage OAuth token
pub const GCS_OAUTH2_TOKEN: &str = "gcs.oauth2.token";
/// Google Cloud Storage working (root) directory
pub const GCS_ROOT: &str = "gcs.root";

/// Parse iceberg properties to [`GcsConfig`].
pub(crate) fn gcs_config_parse(mut m: HashMap<String, String>) -> Result<GcsConfig> {
    let mut cfg = GcsConfig::default();
    if let Some(endpoint) = m.remove(GCS_ENDPOINT) {
        cfg.endpoint = Some(endpoint);
    };

    if let Some(bucket) = m.remove(GCS_BUCKET) {
        cfg.bucket = bucket;
    }

    if let Some(root) = m.remove(GCS_ROOT) {
        cfg.root = Some(root)
    }

    if let Some(credential) = m.remove(GCS_OAUTH2_TOKEN) {
        cfg.credential = Some(credential)
    }

    Ok(cfg)
}

/// Build a new OpenDAL [`Operator`] based on a known [`GcsConfig`].
pub(crate) fn gcs_config_build(cfg: &GcsConfig) -> Result<Operator> {
    Ok(Operator::from_config(cfg.clone())?.finish())
}
