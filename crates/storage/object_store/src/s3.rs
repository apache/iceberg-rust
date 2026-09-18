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

use iceberg::io::S3Config;
use iceberg::{Error, ErrorKind, Result};
use object_store::ObjectStore;
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use percent_encoding::percent_decode_str;
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
                ErrorKind::DataInvalid,
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

    let relative = url.path().trim_start_matches('/');

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
            ErrorKind::Unexpected,
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

/// Build an `AmazonS3` store from iceberg's `S3Config` for a given bucket.
pub(crate) fn build_s3_store(config: &S3Config, bucket: &str) -> Result<Arc<dyn ObjectStore>> {
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
    if config.disable_config_load {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "S3 disable_config_load is not supported by object_store backend",
        ));
    }

    let mut builder = AmazonS3Builder::new().with_bucket_name(bucket);

    if let Some(ref endpoint) = config.endpoint {
        builder = builder.with_endpoint(endpoint);
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
        assert!(parse_s3_url("gs://my-bucket/file.parquet").is_err());
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
    fn test_parse_s3_url_empty_bucket() {
        assert!(parse_s3_url("s3:///path/to/file.parquet").is_err());
        assert!(parse_s3_url("s3://").is_err());
    }

    #[test]
    fn test_parse_s3_config_key() {
        assert!(parse_s3_config_key("aws_server_side_encryption").is_ok());
        let err = parse_s3_config_key("invalid_config_key_foo_bar").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
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
    fn test_build_s3_store_kms_encryption() {
        let config = S3Config::builder()
            .region("us-east-1")
            .server_side_encryption("aws:kms")
            .server_side_encryption_aws_kms_key_id("arn:aws:kms:us-east-1:123456789012:key/test")
            .build();
        assert!(build_s3_store(&config, "my-bucket").is_ok());
    }

    #[test]
    fn test_build_s3_store_ssec_encryption() {
        let config = S3Config::builder()
            .region("us-east-1")
            .server_side_encryption_customer_key("MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDE=")
            .build();
        assert!(build_s3_store(&config, "my-bucket").is_ok());
    }

    #[test]
    fn test_build_s3_store_unsupported_role_arn() {
        let config = S3Config::builder()
            .region("us-east-1")
            .role_arn("arn:aws:iam::123456789012:role/test-role")
            .build();
        let err = build_s3_store(&config, "my-bucket").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);
    }

    #[test]
    fn test_build_s3_store_unsupported_disable_ec2_metadata() {
        let config = S3Config::builder()
            .region("us-east-1")
            .disable_ec2_metadata(true)
            .build();
        let err = build_s3_store(&config, "my-bucket").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);
    }

    #[test]
    fn test_build_s3_store_unsupported_disable_config_load() {
        let config = S3Config::builder()
            .region("us-east-1")
            .disable_config_load(true)
            .build();
        let err = build_s3_store(&config, "my-bucket").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);
    }
}
