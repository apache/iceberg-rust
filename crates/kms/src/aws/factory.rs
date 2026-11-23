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
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use aws_config::{BehaviorVersion, ConfigLoader, Region, SdkConfig};
use iceberg::encryption::kms::{KeyManagementClient, KmsClientFactory};
use iceberg::io::S3_ASSUME_ROLE_ARN;
use iceberg::{Error, ErrorKind, Result};

use super::client::AwsKeyManagementClient;
use super::config::{AwsKmsConfig, AwsSdkProperties};

/// Factory for AWS KMS-backed Iceberg key management clients.
///
/// By default, AWS credentials and region are resolved through the AWS SDK's
/// standard provider chains. Catalog properties can override the region,
/// profile, or static credentials. Applications with custom credential or HTTP
/// providers can supply a complete [`SdkConfig`] through
/// [`with_sdk_config`](Self::with_sdk_config).
#[derive(Clone, Default)]
pub struct AwsKmsClientFactory {
    sdk_config: Option<SdkConfig>,
}

impl AwsKmsClientFactory {
    /// Create a factory using catalog properties and the AWS SDK default chains.
    pub fn new() -> Self {
        Self::default()
    }

    /// Use an application-provided AWS SDK configuration.
    ///
    /// When set, the region, profile, and credential catalog properties are not
    /// read. KMS-specific properties such as `kms.endpoint` continue to be
    /// applied.
    pub fn with_sdk_config(mut self, sdk_config: SdkConfig) -> Self {
        self.sdk_config = Some(sdk_config);
        self
    }
}

impl fmt::Debug for AwsKmsClientFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AwsKmsClientFactory")
            .field("has_sdk_config", &self.sdk_config.is_some())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl KmsClientFactory for AwsKmsClientFactory {
    async fn create_kms_client(
        &self,
        properties: &HashMap<String, String>,
    ) -> Result<Arc<dyn KeyManagementClient>> {
        let kms_config = AwsKmsConfig::from_properties(properties)?;
        let sdk_config = match &self.sdk_config {
            Some(config) => config.clone(),
            None => {
                let sdk_properties = AwsSdkProperties::from_properties(properties)?;
                create_sdk_config(
                    &sdk_properties,
                    aws_config::defaults(BehaviorVersion::latest()),
                )
                .await?
            }
        };

        let mut builder = aws_sdk_kms::config::Builder::from(&sdk_config);
        if sdk_config.behavior_version().is_none() {
            builder = builder.behavior_version(BehaviorVersion::latest());
        }
        if let Some(endpoint) = &kms_config.endpoint {
            builder = builder.endpoint_url(endpoint);
        }

        Ok(Arc::new(AwsKeyManagementClient::new(
            aws_sdk_kms::Client::from_conf(builder.build()),
            kms_config.encryption_algorithm,
            kms_config.data_key_spec,
        )))
    }
}

async fn create_sdk_config(
    properties: &AwsSdkProperties,
    mut loader: ConfigLoader,
) -> Result<SdkConfig> {
    if properties.assume_role_arn.is_some() {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            format!(
                "'{S3_ASSUME_ROLE_ARN}' is not supported by the AWS KMS client yet; supply an SdkConfig with the desired credentials through AwsKmsClientFactory::with_sdk_config"
            ),
        ));
    }
    if let Some(credentials) = properties.static_credentials()? {
        loader = loader.credentials_provider(credentials);
    }
    if let Some(profile) = &properties.profile_name {
        loader = loader.profile_name(profile);
    }
    if let Some(region) = &properties.region {
        loader = loader.region(Region::new(region.clone()));
    }
    Ok(loader.load().await)
}

#[cfg(test)]
mod tests {
    use aws_sdk_kms::config::{Credentials, ProvideCredentials};
    use aws_smithy_http_client::test_util::{CaptureRequestReceiver, capture_request};
    use base64::Engine as _;
    use base64::prelude::BASE64_STANDARD;
    use iceberg::io::CLIENT_REGION;

    use super::*;
    use crate::aws::config::{
        AWS_ACCESS_KEY_ID, AWS_REGION_NAME, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN,
    };
    use crate::aws::{KMS_DATA_KEY_SPEC, KMS_ENCRYPTION_ALGORITHM_SPEC, KMS_ENDPOINT};

    const ROLE_ARN: &str = "arn:aws:iam::123456789012:role/kms";

    /// A config loader that ignores the host's `AWS_*` environment variables
    /// and shared configuration files.
    fn test_loader() -> ConfigLoader {
        aws_config::defaults(BehaviorVersion::latest()).empty_test_environment()
    }

    /// A fully loaded SDK configuration whose HTTP client captures the first
    /// request instead of sending it.
    async fn capturing_sdk_config() -> (SdkConfig, CaptureRequestReceiver) {
        let (http_client, request) = capture_request(None);
        let sdk_config = test_loader()
            .region(Region::new("eu-west-2"))
            .credentials_provider(Credentials::new("access", "secret", None, None, "test"))
            .http_client(http_client)
            .load()
            .await;
        (sdk_config, request)
    }

    async fn capturing_factory() -> (AwsKmsClientFactory, CaptureRequestReceiver) {
        let (sdk_config, request) = capturing_sdk_config().await;
        (
            AwsKmsClientFactory::new().with_sdk_config(sdk_config),
            request,
        )
    }

    fn request_body(request: CaptureRequestReceiver) -> String {
        let request = request.expect_request();
        String::from_utf8(request.body().bytes().unwrap().to_vec()).unwrap()
    }

    fn sdk_properties(properties: &[(&str, &str)]) -> AwsSdkProperties {
        let properties = properties
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        AwsSdkProperties::from_properties(&properties).unwrap()
    }

    #[tokio::test]
    async fn test_kms_endpoint_overrides_injected_sdk_config() {
        let (factory, request) = capturing_factory().await;
        let properties = HashMap::from([(
            KMS_ENDPOINT.to_string(),
            "http://localhost:4566".to_string(),
        )]);

        let client = factory.create_kms_client(&properties).await.unwrap();
        // The captured response is empty, so only the outgoing request matters here.
        let _ = client.unwrap_key(b"wrapped", "test-key").await;

        assert_eq!(request.expect_request().uri(), "http://localhost:4566/");
    }

    #[tokio::test]
    async fn test_reject_invalid_kms_endpoint() {
        let factory = AwsKmsClientFactory::new().with_sdk_config(capturing_sdk_config().await.0);
        let properties = HashMap::from([(KMS_ENDPOINT.to_string(), "localhost:4566".to_string())]);

        let Err(error) = factory.create_kms_client(&properties).await else {
            panic!("endpoints without a scheme must be rejected");
        };

        assert_eq!(error.kind(), ErrorKind::DataInvalid);
    }

    #[tokio::test]
    async fn test_factory_applies_kms_data_key_spec() {
        let (factory, request) = capturing_factory().await;
        let properties = HashMap::from([(KMS_DATA_KEY_SPEC.to_string(), "AES_128".to_string())]);

        let client = factory.create_kms_client(&properties).await.unwrap();
        let _ = client.generate_key("test-key").await;

        assert!(request_body(request).contains(r#""KeySpec":"AES_128""#));
    }

    #[tokio::test]
    async fn test_factory_applies_kms_encryption_algorithm_spec() {
        let (factory, request) = capturing_factory().await;
        let properties = HashMap::from([(
            KMS_ENCRYPTION_ALGORITHM_SPEC.to_string(),
            "RSAES_OAEP_SHA_256".to_string(),
        )]);

        let client = factory.create_kms_client(&properties).await.unwrap();
        let _ = client.wrap_key(b"key", "test-key").await;

        assert!(request_body(request).contains(r#""EncryptionAlgorithm":"RSAES_OAEP_SHA_256""#));
    }

    #[tokio::test]
    async fn test_create_client_adds_missing_behavior_version() {
        let factory = AwsKmsClientFactory::new().with_sdk_config(SdkConfig::builder().build());

        // `aws_sdk_kms::Client::from_conf` panics if no behavior version is set.
        factory.create_kms_client(&HashMap::new()).await.unwrap();
    }

    #[tokio::test]
    async fn test_static_credentials_and_region_are_applied() {
        let properties = sdk_properties(&[
            (AWS_ACCESS_KEY_ID, "access"),
            (AWS_SECRET_ACCESS_KEY, "secret"),
            (AWS_SESSION_TOKEN, "token"),
            (AWS_REGION_NAME, "eu-west-2"),
        ]);

        let config = create_sdk_config(&properties, test_loader()).await.unwrap();
        let credentials = config
            .credentials_provider()
            .expect("static credentials must install a credentials provider")
            .provide_credentials()
            .await
            .unwrap();

        assert_eq!(config.region().map(Region::as_ref), Some("eu-west-2"));
        assert_eq!(credentials.access_key_id(), "access");
        assert_eq!(credentials.secret_access_key(), "secret");
        assert_eq!(credentials.session_token(), Some("token"));
    }

    #[tokio::test]
    async fn test_client_region_takes_precedence_over_region_name() {
        let properties =
            sdk_properties(&[(CLIENT_REGION, "eu-west-1"), (AWS_REGION_NAME, "eu-west-2")]);

        let config = create_sdk_config(&properties, test_loader()).await.unwrap();

        assert_eq!(config.region().map(Region::as_ref), Some("eu-west-1"));
    }

    #[tokio::test]
    async fn test_reject_assume_role() {
        // Rejected before the AWS environment is read.
        let properties = HashMap::from([(S3_ASSUME_ROLE_ARN.to_string(), ROLE_ARN.to_string())]);

        let Err(error) = AwsKmsClientFactory::new()
            .create_kms_client(&properties)
            .await
        else {
            panic!("assume-role properties must be rejected");
        };

        assert_eq!(error.kind(), ErrorKind::FeatureUnsupported);
    }

    #[tokio::test]
    async fn test_reject_incomplete_static_credentials() {
        // Rejected before the AWS environment is read.
        let properties = HashMap::from([(AWS_ACCESS_KEY_ID.to_string(), "access".to_string())]);

        let Err(error) = AwsKmsClientFactory::new()
            .create_kms_client(&properties)
            .await
        else {
            panic!("incomplete static credentials must be rejected");
        };

        assert_eq!(error.kind(), ErrorKind::DataInvalid);
    }

    #[tokio::test]
    async fn test_injected_sdk_config_ignores_sdk_properties() {
        let factory = AwsKmsClientFactory::new().with_sdk_config(capturing_sdk_config().await.0);
        let properties = HashMap::from([
            (AWS_ACCESS_KEY_ID.to_string(), "access".to_string()),
            (S3_ASSUME_ROLE_ARN.to_string(), ROLE_ARN.to_string()),
        ]);

        assert!(factory.create_kms_client(&properties).await.is_ok());
    }

    #[tokio::test]
    async fn test_invalid_sdk_endpoint_does_not_leak_plaintext_key() {
        // The SDK describes the whole request, body included, when it cannot
        // apply an endpoint URL. An injected `SdkConfig` is not validated.
        let (http_client, _request) = capture_request(None);
        let sdk_config = test_loader()
            .region(Region::new("eu-west-2"))
            .credentials_provider(Credentials::new("access", "secret", None, None, "test"))
            .endpoint_url("http://%zz")
            .http_client(http_client)
            .load()
            .await;
        let factory = AwsKmsClientFactory::new().with_sdk_config(sdk_config);
        let client = factory.create_kms_client(&HashMap::new()).await.unwrap();
        let plaintext = b"0123456789abcdef";
        let encoded = BASE64_STANDARD.encode(plaintext);

        let error = client.wrap_key(plaintext, "test-key").await.unwrap_err();

        // Guards against this passing without the SDK describing the request.
        assert!(error.message().contains("<redacted>"), "{error}");

        for output in [
            error.to_string(),
            format!("{error:?}"),
            format!("{error:#?}"),
        ] {
            assert!(!output.contains(&encoded), "{output}");
        }
    }
}
