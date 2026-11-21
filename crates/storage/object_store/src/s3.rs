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

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use iceberg::io::{
    S3_RETRY_MAX_WAIT_MS, S3_RETRY_MIN_WAIT_MS, S3_RETRY_NUM_RETRIES, S3Config, StorageConfig,
};
use iceberg::{Error, ErrorKind, Result};
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::{ObjectStore, RetryConfig};
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
use url::Url;

/// Parsed components of an S3 URL.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ParsedS3Url {
    pub(crate) scheme: String,
    pub(crate) bucket: String,
    pub(crate) relative: String,
}

/// Parse an absolute S3 URL into [`ParsedS3Url`].
///
/// Accepts `s3://`, `s3a://`, and `s3n://` schemes.
pub(crate) fn parse_s3_url(path: &str) -> Result<ParsedS3Url> {
    let url = Url::parse(path).map_err(|e| {
        Error::new(ErrorKind::DataInvalid, format!("Invalid URL: {path}")).with_source(e)
    })?;

    let scheme = url.scheme();
    match scheme {
        "s3" | "s3a" | "s3n" => {}
        _ => {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!("Unsupported S3 scheme: {scheme} in url: {path}"),
            ));
        }
    }

    let bucket = url.host_str().ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid s3 url: {path}, missing bucket"),
        )
    })?;

    if bucket.is_empty() {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Empty s3 url: {path}, missing bucket"),
        ));
    }

    let bucket = percent_decode_str(bucket).decode_utf8().map_err(|e| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid percent-encoded bucket in s3 url: {path}"),
        )
        .with_source(e)
    })?;

    let relative = match path.find("://") {
        Some(scheme_end) => {
            let after_scheme = &path[scheme_end + 3..];
            match after_scheme.find('/') {
                Some(idx) => after_scheme[idx..].strip_prefix('/').unwrap_or(""),
                None => "",
            }
        }
        None => url.path().strip_prefix('/').unwrap_or(url.path()),
    };

    Ok(ParsedS3Url {
        scheme: scheme.to_string(),
        bucket: bucket.to_string(),
        relative: relative.to_string(),
    })
}

/// Parse a string into an [`AmazonS3ConfigKey`].
fn parse_s3_config_key(key: &str) -> Result<AmazonS3ConfigKey> {
    AmazonS3ConfigKey::from_str(key).map_err(|e| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Failed to parse S3 config key: {key}"),
        )
        .with_source(e)
    })
}

/// Configure Server-Side Encryption on `AmazonS3Builder` from `S3Config`.
///
/// Uses string-based `with_config(parse_s3_config_key(...), ...)` because
/// `S3EncryptionConfigKey` is not re-exported from `object_store::aws` in 0.13.x.
/// The string keys (`"aws_server_side_encryption"`) are stable and used in
/// object_store's own test suite.
fn configure_sse(mut builder: AmazonS3Builder, config: &S3Config) -> Result<AmazonS3Builder> {
    if let Some(ref sse) = config.server_side_encryption {
        match sse.as_str() {
            "aws:kms" => match &config.server_side_encryption_aws_kms_key_id {
                Some(key) => {
                    builder = builder.with_sse_kms_encryption(key);
                }
                None => {
                    builder = builder.with_config(
                        parse_s3_config_key("aws_server_side_encryption")?,
                        "aws:kms",
                    );
                }
            },
            "AES256" => {
                builder = builder
                    .with_config(parse_s3_config_key("aws_server_side_encryption")?, "AES256");
            }
            other => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    format!("Unsupported server side encryption type: {other}"),
                ));
            }
        }
    }

    if let Some(ref custom_key) = config.server_side_encryption_customer_key {
        // Note: object_store's with_ssec_encryption automatically computes and sets
        // the x-amz-server-side-encryption-customer-key-MD5 header from the decoded key.
        builder = builder.with_ssec_encryption(custom_key);
    }

    Ok(builder)
}

/// Resolve the endpoint URL for `AmazonS3Builder::with_endpoint`.
///
/// When virtual hosted style is enabled, `object_store` requires the bucket to be present
/// in the endpoint URL (otherwise it uses the endpoint as-is and drops the bucket from requests).
/// If not already present, we prepend the bucket as a subdomain: `http://{bucket}.{host}`.
pub(crate) fn resolve_bucket_endpoint(
    endpoint: &str,
    bucket: &str,
    enable_virtual_host_style: bool,
) -> String {
    if enable_virtual_host_style {
        if endpoint.contains(&format!("//{bucket}.")) {
            endpoint.to_string()
        } else {
            endpoint.replace("//", &format!("//{bucket}."))
        }
    } else {
        endpoint.to_string()
    }
}

/// Validate S3 configuration for unsupported options.
pub(crate) fn validate_s3_config(config: &S3Config) -> Result<()> {
    if config.role_arn.is_some() {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "S3 assume-role (role_arn) is not supported by object_store backend",
        ));
    }
    if config.disable_ec2_metadata {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "S3 disable_ec2_metadata is not supported by object_store backend",
        ));
    }
    Ok(())
}

/// Parsed S3 retry policy configuration.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct S3RetrySettings {
    pub(crate) num_retries: Option<usize>,
    pub(crate) min_wait_ms: Option<u64>,
    pub(crate) max_wait_ms: Option<u64>,
}

impl S3RetrySettings {
    pub(crate) fn parse(config: &StorageConfig) -> Result<Self> {
        let num_retries = if let Some(val) = config.get(S3_RETRY_NUM_RETRIES) {
            Some(val.parse::<usize>().map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Invalid {S3_RETRY_NUM_RETRIES}: {val}: {e}"),
                )
            })?)
        } else {
            None
        };

        let min_wait_ms = if let Some(val) = config.get(S3_RETRY_MIN_WAIT_MS) {
            let ms = val.parse::<u64>().map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Invalid {S3_RETRY_MIN_WAIT_MS}: {val}: {e}"),
                )
            })?;
            if ms == 0 {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Invalid {S3_RETRY_MIN_WAIT_MS}: {val}: wait must be positive"),
                ));
            }
            Some(ms)
        } else {
            None
        };

        let max_wait_ms = if let Some(val) = config.get(S3_RETRY_MAX_WAIT_MS) {
            let ms = val.parse::<u64>().map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Invalid {S3_RETRY_MAX_WAIT_MS}: {val}: {e}"),
                )
            })?;
            if ms == 0 {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Invalid {S3_RETRY_MAX_WAIT_MS}: {val}: wait must be positive"),
                ));
            }
            Some(ms)
        } else {
            None
        };

        Ok(Self {
            num_retries,
            min_wait_ms,
            max_wait_ms,
        })
    }

    pub(crate) fn to_retry_config(&self) -> Option<RetryConfig> {
        if self.num_retries.is_none() && self.min_wait_ms.is_none() && self.max_wait_ms.is_none() {
            return None;
        }

        let mut retry = RetryConfig::default();
        if let Some(n) = self.num_retries {
            retry.max_retries = n;
        }
        if let Some(min) = self.min_wait_ms {
            retry.backoff.init_backoff = Duration::from_millis(min);
        }
        if let Some(max) = self.max_wait_ms {
            retry.backoff.max_backoff = Duration::from_millis(max);
        }
        Some(retry)
    }
}

/// Build an `AmazonS3` store from iceberg's `S3Config` for a given bucket.
pub(crate) fn build_s3_store(
    config: &S3Config,
    retry_settings: &S3RetrySettings,
    bucket: &str,
) -> Result<Arc<dyn ObjectStore>> {
    validate_s3_config(config)?;

    let mut builder = if config.disable_config_load {
        AmazonS3Builder::new()
    } else {
        AmazonS3Builder::from_env()
    }
    .with_bucket_name(bucket);

    if let Some(ref endpoint) = config.endpoint {
        let endpoint_url =
            resolve_bucket_endpoint(endpoint, bucket, config.enable_virtual_host_style);
        builder = builder.with_endpoint(endpoint_url);
        if endpoint.starts_with("http://") {
            builder = builder.with_allow_http(true);
        }
    }
    if let Some(ref access_key_id) = config.access_key_id {
        builder = builder.with_access_key_id(access_key_id);
    }
    if let Some(ref secret_access_key) = config.secret_access_key {
        builder = builder.with_secret_access_key(secret_access_key);
    }
    if let Some(ref session_token) = config.session_token {
        builder = builder.with_token(session_token);
    }
    if let Some(ref region) = config.region {
        builder = builder.with_region(region);
    }
    builder = builder.with_virtual_hosted_style_request(config.enable_virtual_host_style);
    if config.allow_anonymous {
        builder = builder.with_skip_signature(true);
    }

    if let Some(retry) = retry_settings.to_retry_config() {
        builder = builder.with_retry(retry);
    }

    builder = configure_sse(builder, config)?;

    let store = builder.build().map_err(|e| {
        Error::new(ErrorKind::Unexpected, "Failed to build S3 object store").with_source(e)
    })?;
    Ok(Arc::new(store))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s3_url() {
        let parsed = parse_s3_url("s3://my-bucket/path/to/file.parquet").unwrap();
        assert_eq!(parsed.scheme, "s3");
        assert_eq!(parsed.bucket, "my-bucket");
        assert_eq!(parsed.relative, "path/to/file.parquet");
    }

    #[test]
    fn test_parse_s3a_url() {
        let parsed = parse_s3_url("s3a://my-bucket/path/to/file.parquet").unwrap();
        assert_eq!(parsed.scheme, "s3a");
        assert_eq!(parsed.bucket, "my-bucket");
        assert_eq!(parsed.relative, "path/to/file.parquet");
    }

    #[test]
    fn test_parse_s3_url_unsupported_scheme() {
        let err = parse_s3_url("gs://my-bucket/file.parquet").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);
    }

    #[test]
    fn test_parse_s3_url_bucket_only() {
        let parsed = parse_s3_url("s3://my-bucket/").unwrap();
        assert_eq!(parsed.scheme, "s3");
        assert_eq!(parsed.bucket, "my-bucket");
        assert_eq!(parsed.relative, "");
    }

    #[test]
    fn test_parse_s3n_url() {
        let parsed = parse_s3_url("s3n://my-bucket/path/to/file.parquet").unwrap();
        assert_eq!(parsed.scheme, "s3n");
        assert_eq!(parsed.bucket, "my-bucket");
        assert_eq!(parsed.relative, "path/to/file.parquet");
    }

    #[test]
    fn test_parse_s3_url_uppercase_scheme() {
        let parsed = parse_s3_url("S3://my-bucket/path/to/file.parquet").unwrap();
        assert_eq!(parsed.scheme, "s3");
        assert_eq!(parsed.bucket, "my-bucket");
        assert_eq!(parsed.relative, "path/to/file.parquet");
    }

    #[test]
    fn test_parse_s3_url_percent_encoded_bucket() {
        let parsed = parse_s3_url("s3://my%2Dbucket/path/to/file.parquet").unwrap();
        assert_eq!(parsed.scheme, "s3");
        assert_eq!(parsed.bucket, "my-bucket");
        assert_eq!(parsed.relative, "path/to/file.parquet");
    }

    #[test]
    fn test_parse_s3_url_percent_encoded_path() {
        let parsed = parse_s3_url("s3://my-bucket/path%20with%20spaces/file.parquet").unwrap();
        assert_eq!(parsed.scheme, "s3");
        assert_eq!(parsed.bucket, "my-bucket");
        assert_eq!(parsed.relative, "path%20with%20spaces/file.parquet");
    }

    #[test]
    fn test_parse_s3_url_double_slash_path() {
        let parsed = parse_s3_url("s3://my-bucket//path/to/file.parquet").unwrap();
        assert_eq!(parsed.scheme, "s3");
        assert_eq!(parsed.bucket, "my-bucket");
        assert_eq!(parsed.relative, "/path/to/file.parquet");
    }

    #[test]
    fn test_parse_s3_url_empty_bucket() {
        assert!(parse_s3_url("s3:///path/to/file.parquet").is_err());
        assert!(parse_s3_url("s3://").is_err());
    }

    #[test]
    fn test_parse_s3_config_key() {
        assert!(parse_s3_config_key("aws_server_side_encryption").is_ok());
        let err = parse_s3_config_key("invalid_config_key_foo_bar").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn test_configure_sse_kms_default_key() {
        let config = S3Config::builder()
            .region("us-east-1")
            .server_side_encryption("aws:kms")
            .build();
        assert!(configure_sse(AmazonS3Builder::new(), &config).is_ok());
    }

    #[test]
    fn test_configure_sse_kms_custom_key() {
        let config = S3Config::builder()
            .region("us-east-1")
            .server_side_encryption("aws:kms")
            .server_side_encryption_aws_kms_key_id("arn:aws:kms:us-east-1:123456789012:key/test")
            .build();
        assert!(configure_sse(AmazonS3Builder::new(), &config).is_ok());
    }

    #[test]
    fn test_configure_sse_aes256() {
        let config = S3Config::builder()
            .region("us-east-1")
            .server_side_encryption("AES256")
            .build();
        assert!(configure_sse(AmazonS3Builder::new(), &config).is_ok());
    }

    #[test]
    fn test_configure_sse_ssec() {
        let config = S3Config::builder()
            .region("us-east-1")
            .server_side_encryption_customer_key("MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDE=")
            .build();
        assert!(configure_sse(AmazonS3Builder::new(), &config).is_ok());
    }

    #[test]
    fn test_configure_sse_unsupported() {
        let config = S3Config::builder()
            .region("us-east-1")
            .server_side_encryption("unsupported_sse")
            .build();
        let err = configure_sse(AmazonS3Builder::new(), &config).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);
    }

    #[test]
    fn test_resolve_bucket_endpoint_virtual_hosted() {
        assert_eq!(
            resolve_bucket_endpoint("http://localhost:9000", "my-bucket", true),
            "http://my-bucket.localhost:9000"
        );
        assert_eq!(
            resolve_bucket_endpoint("https://s3.amazonaws.com", "my-bucket", true),
            "https://my-bucket.s3.amazonaws.com"
        );
        // Already contains bucket prefix
        assert_eq!(
            resolve_bucket_endpoint("https://my-bucket.s3.amazonaws.com", "my-bucket", true),
            "https://my-bucket.s3.amazonaws.com"
        );
    }

    #[test]
    fn test_resolve_bucket_endpoint_path_style() {
        assert_eq!(
            resolve_bucket_endpoint("http://localhost:9000", "my-bucket", false),
            "http://localhost:9000"
        );
    }

    #[test]
    fn test_build_s3_store_kms_encryption() {
        let config = S3Config::builder()
            .region("us-east-1")
            .server_side_encryption("aws:kms")
            .server_side_encryption_aws_kms_key_id("arn:aws:kms:us-east-1:123456789012:key/test")
            .build();
        assert!(build_s3_store(&config, &S3RetrySettings::default(), "my-bucket").is_ok());
    }

    #[test]
    fn test_build_s3_store_ssec_encryption() {
        let config = S3Config::builder()
            .region("us-east-1")
            .server_side_encryption_customer_key("MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDE=")
            .build();
        assert!(build_s3_store(&config, &S3RetrySettings::default(), "my-bucket").is_ok());
    }

    #[test]
    fn test_build_s3_store_unsupported_role_arn() {
        let config = S3Config::builder()
            .region("us-east-1")
            .role_arn("arn:aws:iam::123456789012:role/test-role")
            .build();
        let err = build_s3_store(&config, &S3RetrySettings::default(), "my-bucket").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);
    }

    #[test]
    fn test_build_s3_store_unsupported_disable_ec2_metadata() {
        let config = S3Config::builder()
            .region("us-east-1")
            .disable_ec2_metadata(true)
            .build();
        let err = build_s3_store(&config, &S3RetrySettings::default(), "my-bucket").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);
    }

    #[test]
    fn test_build_s3_store_disable_config_load() {
        let config = S3Config::builder()
            .region("us-east-1")
            .disable_config_load(true)
            .build();
        assert!(build_s3_store(&config, &S3RetrySettings::default(), "my-bucket").is_ok());
    }

    #[test]
    fn test_s3_retry_settings_parse() {
        let mut props = std::collections::HashMap::new();
        props.insert(S3_RETRY_NUM_RETRIES.to_string(), "5".to_string());
        props.insert(S3_RETRY_MIN_WAIT_MS.to_string(), "1000".to_string());
        props.insert(S3_RETRY_MAX_WAIT_MS.to_string(), "10000".to_string());

        let settings = S3RetrySettings::parse(&StorageConfig::from_props(props)).unwrap();
        assert_eq!(settings.num_retries, Some(5));
        assert_eq!(settings.min_wait_ms, Some(1000));
        assert_eq!(settings.max_wait_ms, Some(10000));

        let retry = settings.to_retry_config().unwrap();
        assert_eq!(retry.max_retries, 5);
        assert_eq!(retry.backoff.init_backoff, Duration::from_millis(1000));
        assert_eq!(retry.backoff.max_backoff, Duration::from_millis(10000));
    }

    #[test]
    fn test_s3_retry_settings_zero_retries() {
        let mut props = std::collections::HashMap::new();
        props.insert(S3_RETRY_NUM_RETRIES.to_string(), "0".to_string());
        let settings = S3RetrySettings::parse(&StorageConfig::from_props(props)).unwrap();
        assert_eq!(settings.num_retries, Some(0));
        let retry = settings.to_retry_config().unwrap();
        assert_eq!(retry.max_retries, 0);
    }

    #[test]
    fn test_s3_retry_settings_invalid_values() {
        let mut props_invalid_num = std::collections::HashMap::new();
        props_invalid_num.insert(S3_RETRY_NUM_RETRIES.to_string(), "-1".to_string());
        assert_eq!(
            S3RetrySettings::parse(&StorageConfig::from_props(props_invalid_num))
                .unwrap_err()
                .kind(),
            ErrorKind::DataInvalid
        );

        let mut props_zero_wait = std::collections::HashMap::new();
        props_zero_wait.insert(S3_RETRY_MIN_WAIT_MS.to_string(), "0".to_string());
        assert_eq!(
            S3RetrySettings::parse(&StorageConfig::from_props(props_zero_wait))
                .unwrap_err()
                .kind(),
            ErrorKind::DataInvalid
        );
    }
}
