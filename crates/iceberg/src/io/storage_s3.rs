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
use std::str::FromStr;

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
/// S3 Path Style Access.
pub const S3_PATH_STYLE_ACCESS: &str = "s3.path-style-access";
/// S3 Server Side Encryption Type.
pub const S3_SSE_TYPE: &str = "s3.sse.type";
/// S3 Server Side Encryption Key.
/// If S3 encryption type is kms, input is a KMS Key ID.
/// In case this property is not set, default key "aws/s3" is used.
/// If encryption type is custom, input is a custom base-64 AES256 symmetric key.
pub const S3_SSE_KEY: &str = "s3.sse.key";
/// S3 Server Side Encryption MD5.
pub const S3_SSE_MD5: &str = "s3.sse.md5";

/// S3 Server Side Encryption types
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum S3SSEType {
    /// S3 SSE-C, using customer managed keys. https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html
    Custom,
    /// S3 SSE KMS, either using default or custom KMS key. https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingKMSEncryption.html
    KMS,
    /// S3 SSE-S3 encryption (S3 managed keys). https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingServerSideEncryption.html
    S3,
    /// No Server Side Encryption
    None,
}

impl FromStr for S3SSEType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "custom" => Ok(Self::Custom),
            "kms" => Ok(Self::KMS),
            "s3" => Ok(Self::S3),
            "none" => Ok(Self::None),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Invalid {}: {}. Expected one of (custom, kms, s3, none)",
                    S3_SSE_TYPE, s
                ),
            )),
        }
    }
}

/// Parse iceberg props to s3 config.
pub(crate) fn s3_config_parse(mut m: HashMap<String, String>) -> Result<S3Config> {
    let mut cfg = S3Config::default();
    if let Some(endpoint) = m.remove(S3_ENDPOINT) {
        cfg.endpoint = Some(endpoint);
    };
    if let Some(access_key_id) = m.remove(S3_ACCESS_KEY_ID) {
        cfg.access_key_id = Some(access_key_id);
    };
    if let Some(secret_access_key) = m.remove(S3_SECRET_ACCESS_KEY) {
        cfg.secret_access_key = Some(secret_access_key);
    };
    if let Some(region) = m.remove(S3_REGION) {
        cfg.region = Some(region);
    };
    if let Some(path_style_access) = m.remove(S3_PATH_STYLE_ACCESS) {
        if ["true", "True", "1"].contains(&path_style_access.as_str()) {
            cfg.enable_virtual_host_style = true;
        }
    };
    let s3_sse_key = m.remove(S3_SSE_KEY);
    if let Some(sse_type) = m.remove(S3_SSE_TYPE) {
        let sse_type = sse_type.parse()?;
        match sse_type {
            S3SSEType::None => {}
            S3SSEType::S3 => {
                cfg.server_side_encryption = Some("AES256".to_string());
            }
            S3SSEType::KMS => {
                cfg.server_side_encryption = Some("aws:kms".to_string());
                cfg.server_side_encryption_aws_kms_key_id = s3_sse_key;
            }
            S3SSEType::Custom => {
                cfg.server_side_encryption_customer_algorithm = Some("AES256".to_string());
                cfg.server_side_encryption_customer_key = s3_sse_key;
                cfg.server_side_encryption_customer_key_md5 = m.remove(S3_SSE_MD5);
            }
        }
    };

    Ok(cfg)
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
