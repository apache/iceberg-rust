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

use opendal::services::S3Config;
use opendal::Operator;
use url::Url;

use crate::{Error, ErrorKind, Result};

/// Following are arguments for [s3 file io](https://py.iceberg.apache.org/configuration/#s3).
/// S3 endpoint.
pub const S3_ENDPOINT: &str = "s3.endpoint";
/// S3 access key id.
pub const S3_ACCESS_KEY_ID: &str = "s3.access-key-id";
/// S3 secret access key.
pub const S3_SECRET_ACCESS_KEY: &str = "s3.secret-access-key";
/// S3 region.
pub const S3_REGION: &str = "s3.region";

/// Parse iceberg props to s3 config.
pub(crate) fn s3_config_parse(m: HashMap<String, String>) -> S3Config {
    let mut cfg = S3Config::default();
    if let Some(endpoint) = m.get(S3_ENDPOINT) {
        cfg.endpoint = Some(endpoint.clone());
    };
    if let Some(access_key_id) = m.get(S3_ACCESS_KEY_ID) {
        cfg.access_key_id = Some(access_key_id.clone());
    };
    if let Some(secret_access_key) = m.get(S3_SECRET_ACCESS_KEY) {
        cfg.secret_access_key = Some(secret_access_key.clone());
    };
    if let Some(region) = m.get(S3_REGION) {
        cfg.region = Some(region.clone());
    };

    cfg
}

/// Build new opendal operator from give path.
pub(crate) fn s3_config_build(cfg: &S3Config, path: &str) -> Result<Operator> {
    let url = Url::parse(path)?;
    let bucket = url.host_str().ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid s3 url: {}, missing bucket", path),
        )
    })?;

    let mut cfg = cfg.clone();
    cfg.bucket = bucket.to_string();
    Ok(Operator::from_config(cfg)?.finish())
}
