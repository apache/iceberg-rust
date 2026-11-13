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
use std::num::NonZeroU64;
use std::sync::Arc;

use iceberg::io::{
    CLIENT_REGION, S3_ACCESS_KEY_ID, S3_ALLOW_ANONYMOUS, S3_ASSUME_ROLE_ARN,
    S3_ASSUME_ROLE_EXTERNAL_ID, S3_ASSUME_ROLE_SESSION_NAME, S3_DISABLE_CONFIG_LOAD,
    S3_DISABLE_EC2_METADATA, S3_ENDPOINT, S3_MULTIPART_PART_SIZE_BYTES, S3_PATH_STYLE_ACCESS,
    S3_REGION, S3_RETRY_MAX_WAIT_MS, S3_RETRY_MIN_WAIT_MS, S3_RETRY_NUM_RETRIES,
    S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN, S3_SSE_KEY, S3_SSE_MD5, S3_SSE_TYPE,
};
use iceberg::{Error, ErrorKind, Result};
use iceberg_property_macro::Properties;
use opendal::services::S3Config;
use opendal::{Configurator, Operator};
/// AWS credentials: access key ID, secret access key, and optional session token.
pub use reqsign_aws_v4::Credential as AwsCredential;
/// Trait for types that can asynchronously supply [`AwsCredential`] to a [`CustomAwsCredentialLoader`].
pub use reqsign_core::ProvideCredential;
use reqsign_core::{ProvideCredentialChain, ProvideCredentialDyn};
use url::Url;

use crate::utils::{from_opendal_error, is_truthy};
use crate::{OpenDalClientConfig, parse_positive_ms};

#[derive(Properties)]
struct S3RetryConfig {
    #[property(key = S3_RETRY_NUM_RETRIES, default = None)]
    num_retries: Option<usize>,
    #[property(key = S3_RETRY_MIN_WAIT_MS, default = None, parse_with = parse_positive_ms)]
    min_wait_ms: Option<NonZeroU64>,
    #[property(key = S3_RETRY_MAX_WAIT_MS, default = None, parse_with = parse_positive_ms)]
    max_wait_ms: Option<NonZeroU64>,
}

/// Overlays the `s3.retry.*` properties onto the `opendal.retry.*` settings of `client_config`, per key.
pub(crate) fn s3_client_config(
    props: &HashMap<String, String>,
    mut client_config: OpenDalClientConfig,
) -> Result<OpenDalClientConfig> {
    let retry = S3RetryConfig::from_properties(props)?;
    client_config.retry_num_retries = retry.num_retries.or(client_config.retry_num_retries);
    client_config.retry_min_wait_ms = retry.min_wait_ms.or(client_config.retry_min_wait_ms);
    client_config.retry_max_wait_ms = retry.max_wait_ms.or(client_config.retry_max_wait_ms);
    Ok(client_config)
}

/// S3 rejects a non-final part smaller than this.
const MULTIPART_PART_SIZE_MIN: u64 = 5 * 1024 * 1024;

/// Java reads this property as an `int`, so a larger value fails there.
const MULTIPART_PART_SIZE_MAX: u64 = i32::MAX as u64;

/// Matches Java `S3FileIOProperties.MULTIPART_SIZE_DEFAULT`.
const MULTIPART_PART_SIZE_DEFAULT: u64 = 32 * 1024 * 1024;

pub(crate) fn default_multipart_part_size() -> u64 {
    MULTIPART_PART_SIZE_DEFAULT
}

/// Parse iceberg props to s3 multipart upload part size.
pub(crate) fn s3_multipart_part_size_parse(m: &HashMap<String, String>) -> Result<u64> {
    let Some(value) = m.get(S3_MULTIPART_PART_SIZE_BYTES) else {
        return Ok(default_multipart_part_size());
    };
    let part_size = value.parse::<u64>().map_err(|e| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid {S3_MULTIPART_PART_SIZE_BYTES}: {value}: {e}"),
        )
    })?;
    if part_size < MULTIPART_PART_SIZE_MIN {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Invalid {S3_MULTIPART_PART_SIZE_BYTES}: {part_size} bytes is below the S3 minimum part size of {} MiB",
                MULTIPART_PART_SIZE_MIN / (1024 * 1024)
            ),
        ));
    }
    if part_size > MULTIPART_PART_SIZE_MAX {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Invalid {S3_MULTIPART_PART_SIZE_BYTES}: {part_size} bytes is above {MULTIPART_PART_SIZE_MAX} bytes, the largest value Java accepts"
            ),
        ));
    }
    Ok(part_size)
}

/// Parse iceberg props to s3 config.
pub(crate) fn s3_config_parse(mut m: HashMap<String, String>) -> Result<S3Config> {
    let mut cfg = S3Config::default();
    // Match Iceberg `S3FileIOProperties.PATH_STYLE_ACCESS_DEFAULT = false`:
    // virtual-host-style addressing is the spec default. opendal's own
    // default is path-style, which disagrees with the Java SDK and breaks
    // S3-compatible stores that only accept virtual-hosted-style URLs.
    // Any explicit `s3.path-style-access` property below overrides this.
    cfg.enable_virtual_host_style = true;
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
                        "Invalid {S3_SSE_TYPE}: {sse_type}. Expected one of (custom, kms, s3, none)"
                    ),
                ));
            }
        }
    };

    if let Some(allow_anonymous) = m.remove(S3_ALLOW_ANONYMOUS)
        && is_truthy(allow_anonymous.to_lowercase().as_str())
    {
        cfg.skip_signature = true;
    }
    if let Some(disable_ec2_metadata) = m.remove(S3_DISABLE_EC2_METADATA)
        && is_truthy(disable_ec2_metadata.to_lowercase().as_str())
    {
        cfg.disable_ec2_metadata = true;
    };
    if let Some(disable_config_load) = m.remove(S3_DISABLE_CONFIG_LOAD)
        && is_truthy(disable_config_load.to_lowercase().as_str())
    {
        cfg.disable_config_load = true;
    };

    Ok(cfg)
}

/// Build new opendal operator from give path.
pub(crate) fn s3_config_build(
    cfg: &S3Config,
    customized_credential_load: &Option<CustomAwsCredentialLoader>,
    path: &str,
) -> Result<Operator> {
    let url = Url::parse(path)?;
    let bucket = url.host_str().ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid s3 url: {path}, missing bucket"),
        )
    })?;

    let mut builder = cfg
        .clone()
        .into_builder()
        // Set bucket name.
        .bucket(bucket);

    if let Some(loader) = customized_credential_load {
        let chain = ProvideCredentialChain::new().push(Arc::clone(&loader.0));
        builder = builder.credential_provider_chain(chain);
    }

    Operator::new(builder).map_err(from_opendal_error)
}

/// Custom AWS credential loader.
///
/// Wraps any [`ProvideCredential`] implementation for use with the S3 storage backend.
/// Use [`CustomAwsCredentialLoader::new`] to create one, then pass it to
/// [`OpenDalStorageFactory::S3`](crate::OpenDalStorageFactory).
pub struct CustomAwsCredentialLoader(Arc<dyn ProvideCredentialDyn<Credential = AwsCredential>>);

impl Clone for CustomAwsCredentialLoader {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl std::fmt::Debug for CustomAwsCredentialLoader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CustomAwsCredentialLoader")
            .finish_non_exhaustive()
    }
}

impl CustomAwsCredentialLoader {
    /// Create a new custom AWS credential loader from any [`ProvideCredential`] implementation.
    pub fn new(provider: impl ProvideCredential<Credential = AwsCredential> + 'static) -> Self {
        Self(Arc::new(provider) as Arc<dyn ProvideCredentialDyn<Credential = AwsCredential>>)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use iceberg::Result;
    use iceberg::io::{
        S3_MULTIPART_PART_SIZE_BYTES, S3_PATH_STYLE_ACCESS, S3_RETRY_MAX_WAIT_MS,
        S3_RETRY_MIN_WAIT_MS, S3_RETRY_NUM_RETRIES,
    };
    use opendal::layers::RetryLayer;
    use opendal::services::S3Config;

    use super::{
        AwsCredential, CustomAwsCredentialLoader, MULTIPART_PART_SIZE_DEFAULT,
        MULTIPART_PART_SIZE_MAX, MULTIPART_PART_SIZE_MIN, ProvideCredential,
        default_multipart_part_size, s3_client_config, s3_config_parse,
        s3_multipart_part_size_parse,
    };
    use crate::{
        OPENDAL_RETRY_MAX_WAIT_MS, OPENDAL_RETRY_MIN_WAIT_MS, OPENDAL_RETRY_NUM_RETRIES,
        OpenDalClientConfig, OpenDalStorage, OpenDalStorageFactory,
    };

    fn parse_with(prop: Option<&str>) -> bool {
        let mut props = HashMap::new();
        if let Some(v) = prop {
            props.insert(S3_PATH_STYLE_ACCESS.to_string(), v.to_string());
        }
        s3_config_parse(props).unwrap().enable_virtual_host_style
    }

    #[test]
    fn s3_config_parse_path_style_access() {
        // Match Iceberg S3FileIOProperties.PATH_STYLE_ACCESS_DEFAULT = false.
        assert!(parse_with(None));
        assert!(parse_with(Some("false")));
        assert!(!parse_with(Some("true")));
    }

    fn parse_part_size(prop: Option<&str>) -> Result<u64> {
        let mut props = HashMap::new();
        if let Some(v) = prop {
            props.insert(S3_MULTIPART_PART_SIZE_BYTES.to_string(), v.to_string());
        }
        s3_multipart_part_size_parse(&props)
    }

    #[test]
    fn s3_multipart_part_size_parse_defaults_and_overrides() {
        // Match Iceberg S3FileIOProperties.MULTIPART_SIZE_DEFAULT = 32 MiB.
        assert_eq!(MULTIPART_PART_SIZE_DEFAULT, 32 * 1024 * 1024);
        assert_eq!(parse_part_size(None).unwrap(), MULTIPART_PART_SIZE_DEFAULT);
        assert_eq!(parse_part_size(Some("8388608")).unwrap(), 8 * 1024 * 1024);
    }

    #[test]
    fn s3_multipart_part_size_parse_rejects_invalid() {
        // Match Iceberg S3FileIOProperties.MULTIPART_SIZE_MIN = 5 MiB.
        assert_eq!(MULTIPART_PART_SIZE_MIN, 5 * 1024 * 1024);
        assert!(parse_part_size(Some(&MULTIPART_PART_SIZE_MIN.to_string())).is_ok());
        assert!(parse_part_size(Some(&(MULTIPART_PART_SIZE_MIN - 1).to_string())).is_err());
        assert!(parse_part_size(Some(&MULTIPART_PART_SIZE_MAX.to_string())).is_ok());
        assert!(parse_part_size(Some(&(MULTIPART_PART_SIZE_MAX + 1).to_string())).is_err());
        assert!(parse_part_size(Some("not-a-number")).is_err());
    }

    fn s3_retry_layer(props: &[(&str, &str)]) -> Result<String> {
        let props = props
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<HashMap<_, _>>();
        let client_config =
            s3_client_config(&props, OpenDalClientConfig::from_properties(&props)?)?;
        Ok(format!("{:?}", client_config.retry_layer()))
    }

    #[test]
    fn test_s3_retry_properties_reach_retry_layer() {
        let expected = RetryLayer::new()
            .with_max_times(10)
            .with_min_delay(Duration::from_secs(2))
            .with_max_delay(Duration::from_secs(20))
            .with_jitter();
        assert_eq!(
            s3_retry_layer(&[
                (S3_RETRY_NUM_RETRIES, "10"),
                (S3_RETRY_MIN_WAIT_MS, "2000"),
                (S3_RETRY_MAX_WAIT_MS, "20000"),
            ])
            .unwrap(),
            format!("{expected:?}")
        );
        assert_eq!(
            s3_retry_layer(&[(S3_RETRY_NUM_RETRIES, "0")]).unwrap(),
            format!("{:?}", RetryLayer::new().with_max_times(0).with_jitter())
        );
        assert_eq!(
            s3_retry_layer(&[]).unwrap(),
            format!("{:?}", RetryLayer::new())
        );
    }

    #[test]
    fn test_s3_retry_properties_win_over_the_generic_ones_per_key() {
        let expected = RetryLayer::new()
            .with_max_times(10)
            .with_min_delay(Duration::from_secs(2))
            .with_max_delay(Duration::from_secs(20))
            .with_jitter();
        assert_eq!(
            s3_retry_layer(&[
                (OPENDAL_RETRY_NUM_RETRIES, "5"),
                (OPENDAL_RETRY_MIN_WAIT_MS, "100"),
                (OPENDAL_RETRY_MAX_WAIT_MS, "200"),
                (S3_RETRY_NUM_RETRIES, "10"),
                (S3_RETRY_MIN_WAIT_MS, "2000"),
                (S3_RETRY_MAX_WAIT_MS, "20000"),
            ])
            .unwrap(),
            format!("{expected:?}")
        );

        // a key set at one layer only is taken from that layer
        let mixed = RetryLayer::new()
            .with_max_times(10)
            .with_min_delay(Duration::from_millis(100))
            .with_max_delay(Duration::from_secs(20))
            .with_jitter();
        assert_eq!(
            s3_retry_layer(&[
                (S3_RETRY_NUM_RETRIES, "10"),
                (OPENDAL_RETRY_MIN_WAIT_MS, "100"),
                (OPENDAL_RETRY_MAX_WAIT_MS, "200"),
                (S3_RETRY_MAX_WAIT_MS, "20000"),
            ])
            .unwrap(),
            format!("{mixed:?}")
        );
    }

    #[test]
    fn test_s3_retry_property_parsing_rejects_invalid_values() {
        for (key, invalid) in [
            (S3_RETRY_NUM_RETRIES, "-1"),
            (S3_RETRY_NUM_RETRIES, "abc"),
            (S3_RETRY_NUM_RETRIES, ""),
            (S3_RETRY_MIN_WAIT_MS, "0"),
            (S3_RETRY_MIN_WAIT_MS, "abc"),
            (S3_RETRY_MAX_WAIT_MS, "0"),
            (S3_RETRY_MAX_WAIT_MS, "12.5"),
        ] {
            let err = s3_retry_layer(&[(key, invalid)]).unwrap_err().to_string();
            assert!(err.contains(key), "{key}={invalid}: {err}");
        }
    }

    #[cfg(feature = "opendal-memory")]
    #[test]
    fn test_other_backends_ignore_the_s3_retry_properties() {
        use iceberg::io::{StorageConfig, StorageFactory};

        let config = StorageConfig::new().with_props([
            (S3_RETRY_NUM_RETRIES.to_string(), "abc".to_string()),
            (OPENDAL_RETRY_NUM_RETRIES.to_string(), "5".to_string()),
        ]);
        let storage = OpenDalStorageFactory::Memory.build(&config).unwrap();
        let json = serde_json::to_string(&storage).unwrap();
        assert!(json.contains(r#""retry_num_retries":5"#), "{json}");
    }

    #[test]
    fn test_relativize_path_s3() {
        let storage = OpenDalStorage::S3 {
            config: Arc::new(S3Config::default()),
            multipart_part_size: default_multipart_part_size(),
            customized_credential_load: None,
            client_config: OpenDalClientConfig::default(),
        };

        // All S3-family schemes are accepted by the same storage instance.
        // Custom schemes for S3-compatible stores (e.g., `minio://`) are also
        // accepted because the path's scheme is used as-is for prefix matching.
        for scheme in ["s3", "s3a", "s3n", "minio"] {
            assert_eq!(
                storage
                    .relativize_path(&format!("{scheme}://my-bucket/path/to/file.parquet"))
                    .unwrap(),
                "path/to/file.parquet"
            );
        }
    }

    #[derive(Debug)]
    struct EmptyCredentialLoader;

    impl ProvideCredential for EmptyCredentialLoader {
        type Credential = AwsCredential;

        async fn provide_credential(
            &self,
            _ctx: &reqsign_core::Context,
        ) -> reqsign_core::Result<Option<AwsCredential>> {
            Ok(None)
        }
    }

    #[test]
    fn test_s3_factory_custom_credential_loader_serialization_fails() {
        let file_io = iceberg::io::FileIOBuilder::new(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: Some(CustomAwsCredentialLoader::new(EmptyCredentialLoader)),
        }))
        .build();

        let err = file_io.serialize_all().unwrap_err();
        assert!(
            err.to_string()
                .contains("custom AWS credential loaders cannot be serialized")
        );
    }
}
