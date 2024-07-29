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
use std::fmt::{Debug, Formatter};

use opendal::{Operator, Scheme};
use url::Url;

use crate::io::storage::redact_secret;
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

/// # TODO
///
/// opendal has a plan to introduce native config support.
/// We manually parse the config here and those code will be finally removed.
#[derive(Default, Clone)]
pub(crate) struct S3Config {
    pub endpoint: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub region: String,
}

impl Debug for S3Config {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("access_key_id", &redact_secret(&self.access_key_id))
            .field("secret_access_key", &redact_secret(&self.secret_access_key))
            .finish()
    }
}

impl S3Config {
    /// Decode from iceberg props.
    pub fn new(m: HashMap<String, String>) -> Self {
        let mut cfg = Self::default();
        if let Some(endpoint) = m.get(S3_ENDPOINT) {
            cfg.endpoint = endpoint.clone();
        };
        if let Some(access_key_id) = m.get(S3_ACCESS_KEY_ID) {
            cfg.access_key_id = access_key_id.clone();
        };
        if let Some(secret_access_key) = m.get(S3_SECRET_ACCESS_KEY) {
            cfg.secret_access_key = secret_access_key.clone();
        };
        if let Some(region) = m.get(S3_REGION) {
            cfg.region = region.clone();
        };

        cfg
    }

    /// Build new opendal operator from give path.
    pub fn build(&self, path: &str) -> Result<Operator> {
        let url = Url::parse(path)?;
        let bucket = url.host_str().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Invalid s3 url: {}, missing bucket", path),
            )
        })?;

        let mut m = HashMap::with_capacity(5);
        m.insert("bucket".to_string(), bucket.to_string());
        m.insert("endpoint".to_string(), self.endpoint.clone());
        m.insert("access_key_id".to_string(), self.access_key_id.clone());
        m.insert(
            "secret_access_key".to_string(),
            self.secret_access_key.clone(),
        );
        m.insert("region".to_string(), self.region.clone());

        Ok(Operator::via_iter(Scheme::S3, m)?)
    }
}
