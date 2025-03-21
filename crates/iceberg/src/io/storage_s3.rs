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

use opendal::raw::HttpClient;
use opendal::services::S3Config;
use opendal::{Configurator, Operator};
use url::Url;

use crate::io::is_truthy;
use crate::{Error, ErrorKind, Result};

/// Following are arguments for [s3 file io](https://py.iceberg.apache.org/configuration/#s3).
/// S3 endpoint.
pub const S3_ENDPOINT: &str = "s3.endpoint";
/// S3 access key id.
pub const S3_ACCESS_KEY_ID: &str = "s3.access-key-id";
/// S3 secret access key.
pub const S3_SECRET_ACCESS_KEY: &str = "s3.secret-access-key";
/// S3 session token.
/// This is required when using temporary credentials.
pub const S3_SESSION_TOKEN: &str = "s3.session-token";
/// S3 region.
pub const S3_REGION: &str = "s3.region";
/// Region to use for the S3 client.
///
/// This takes precedence over [`S3_REGION`].
pub const CLIENT_REGION: &str = "client.region";
/// S3 Path Style Access.
pub const S3_PATH_STYLE_ACCESS: &str = "s3.path-style-access";
/// S3 Server Side Encryption Type.
pub const S3_SSE_TYPE: &str = "s3.sse.type";
/// S3 Server Side Encryption Key.
///
/// If S3 encryption type is kms, input is a KMS Key ID.
/// In case this property is not set, default key "aws/s3" is used.
/// If encryption type is custom, input is a custom base-64 AES256 symmetric key.
pub const S3_SSE_KEY: &str = "s3.sse.key";
/// S3 Server Side Encryption MD5.
pub const S3_SSE_MD5: &str = "s3.sse.md5";
/// If set, all AWS clients will assume a role of the given ARN, instead of using the default
/// credential chain.
pub const S3_ASSUME_ROLE_ARN: &str = "client.assume-role.arn";
/// Optional external ID used to assume an IAM role.
pub const S3_ASSUME_ROLE_EXTERNAL_ID: &str = "client.assume-role.external-id";
/// Optional session name used to assume an IAM role.
pub const S3_ASSUME_ROLE_SESSION_NAME: &str = "client.assume-role.session-name";
/// Option to skip signing requests (e.g. for public buckets/folders).
pub const S3_ALLOW_ANONYMOUS: &str = "s3.allow-anonymous";
/// Option to skip loading the credential from EC2 metadata (typically used in conjunction with
/// `S3_ALLOW_ANONYMOUS`).
pub const S3_DISABLE_EC2_METADATA: &str = "s3.disable-ec2-metadata";
/// Option to skip loading configuration from config file and the env.
pub const S3_DISABLE_CONFIG_LOAD: &str = "s3.disable-config-load";

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
    if let Some(session_token) = m.remove(S3_SESSION_TOKEN) {
        cfg.session_token = Some(session_token);
    };
    if let Some(region) = m.remove(S3_REGION) {
        cfg.region = Some(region);
    };
    if let Some(region) = m.remove(CLIENT_REGION) {
        cfg.region = Some(region);
    };
    if let Some(path_style_access) = m.remove(S3_PATH_STYLE_ACCESS) {
        cfg.enable_virtual_host_style = !is_truthy(path_style_access.to_lowercase().as_str());
    };
    if let Some(arn) = m.remove(S3_ASSUME_ROLE_ARN) {
        cfg.role_arn = Some(arn);
    }
    if let Some(external_id) = m.remove(S3_ASSUME_ROLE_EXTERNAL_ID) {
        cfg.external_id = Some(external_id);
    };
    if let Some(session_name) = m.remove(S3_ASSUME_ROLE_SESSION_NAME) {
        cfg.role_session_name = Some(session_name);
    };
    let s3_sse_key = m.remove(S3_SSE_KEY);
    if let Some(sse_type) = m.remove(S3_SSE_TYPE) {
        match sse_type.to_lowercase().as_str() {
            // No Server Side Encryption
            "none" => {}
            // S3 SSE-S3 encryption (S3 managed keys). https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingServerSideEncryption.html
            "s3" => {
                cfg.server_side_encryption = Some("AES256".to_string());
            }
            // S3 SSE KMS, either using default or custom KMS key. https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingKMSEncryption.html
            "kms" => {
                cfg.server_side_encryption = Some("aws:kms".to_string());
                cfg.server_side_encryption_aws_kms_key_id = s3_sse_key;
            }
            // S3 SSE-C, using customer managed keys. https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html
            "custom" => {
                cfg.server_side_encryption_customer_algorithm = Some("AES256".to_string());
                cfg.server_side_encryption_customer_key = s3_sse_key;
                cfg.server_side_encryption_customer_key_md5 = m.remove(S3_SSE_MD5);
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Invalid {}: {}. Expected one of (custom, kms, s3, none)",
                        S3_SSE_TYPE, sse_type
                    ),
                ));
            }
        }
    };

    if let Some(allow_anonymous) = m.remove(S3_ALLOW_ANONYMOUS) {
        if is_truthy(allow_anonymous.to_lowercase().as_str()) {
            cfg.allow_anonymous = true;
        }
    }
    if let Some(disable_ec2_metadata) = m.remove(S3_DISABLE_EC2_METADATA) {
        if is_truthy(disable_ec2_metadata.to_lowercase().as_str()) {
            cfg.disable_ec2_metadata = true;
        }
    };
    if let Some(disable_config_load) = m.remove(S3_DISABLE_CONFIG_LOAD) {
        if is_truthy(disable_config_load.to_lowercase().as_str()) {
            cfg.disable_config_load = true;
        }
    };

    Ok(cfg)
}

/// Build new opendal operator from give path.
pub(crate) fn s3_config_build(
    client: &reqwest::Client,
    cfg: &S3Config,
    path: &str,
) -> Result<Operator> {
    let url = Url::parse(path)?;
    let bucket = url.host_str().ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid s3 url: {}, missing bucket", path),
        )
    })?;

    let builder = cfg
        .clone()
        .into_builder()
        // Set bucket name.
        .bucket(bucket)
        // Set http client we want to use.
        .http_client(HttpClient::with(client.clone()));

    Ok(Operator::new(builder)?.finish())
}
