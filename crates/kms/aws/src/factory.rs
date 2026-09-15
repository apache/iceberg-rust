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
use aws_config::{BehaviorVersion, SdkConfig};
use aws_sdk_kms::Client as KmsClient;
use aws_sdk_kms::config::{Builder as KmsConfigBuilder, Credentials, Region};
use iceberg::encryption::kms::{KeyManagementClient, KmsClientFactory};
use iceberg::{Error, ErrorKind, Result};

use crate::client::AwsKeyManagementClient;
use crate::config::{
    AWS_ACCESS_KEY_ID, AWS_PROFILE_NAME, AWS_REGION_NAME, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN,
    AwsKmsConfig,
};

/// Factory for AWS KMS-backed Iceberg key management clients.
///
/// By default, AWS credentials and region are resolved through the AWS SDK's
/// standard provider chains. Catalog properties can override the profile,
/// region, or static credentials. Applications with custom credential or HTTP
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
    /// When set, SDK-wide credential, region, retry, and HTTP configuration
    /// properties are not rebuilt from catalog properties. KMS-specific
    /// properties such as `kms.endpoint` continue to be applied.
    pub fn with_sdk_config(mut self, sdk_config: SdkConfig) -> Self {
        self.sdk_config = Some(sdk_config);
        self
    }
}

impl fmt::Debug for AwsKmsClientFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AwsKmsClientFactory")
            .field("has_sdk_config", &self.sdk_config.is_some())
            .finish()
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
            None => create_sdk_config(properties).await?,
        };

        let mut builder = KmsConfigBuilder::from(&sdk_config);
        if sdk_config.behavior_version().is_none() {
            builder = builder.behavior_version(BehaviorVersion::latest());
        }
        if let Some(endpoint) = &kms_config.endpoint {
            builder = builder.endpoint_url(endpoint);
        }

        Ok(Arc::new(AwsKeyManagementClient::new(
            KmsClient::from_conf(builder.build()),
            kms_config.encryption_algorithm,
            kms_config.data_key_spec,
        )))
    }
}

async fn create_sdk_config(properties: &HashMap<String, String>) -> Result<SdkConfig> {
    let mut loader = aws_config::defaults(BehaviorVersion::latest());

    match (
        properties.get(AWS_ACCESS_KEY_ID),
        properties.get(AWS_SECRET_ACCESS_KEY),
    ) {
        (Some(access_key), Some(secret_key)) => {
            let credentials = Credentials::new(
                access_key,
                secret_key,
                properties.get(AWS_SESSION_TOKEN).cloned(),
                None,
                "catalog-properties",
            );
            loader = loader.credentials_provider(credentials);
        }
        (None, None) => {
            if properties.contains_key(AWS_SESSION_TOKEN) {
                return Err(incomplete_credentials_error());
            }
        }
        _ => return Err(incomplete_credentials_error()),
    }

    if let Some(profile) = properties.get(AWS_PROFILE_NAME) {
        loader = loader.profile_name(profile);
    }
    if let Some(region) = properties.get(AWS_REGION_NAME) {
        loader = loader.region(Region::new(region.clone()));
    }

    Ok(loader.load().await)
}

fn incomplete_credentials_error() -> Error {
    Error::new(
        ErrorKind::DataInvalid,
        format!(
            "AWS static credentials require both '{AWS_ACCESS_KEY_ID}' and '{AWS_SECRET_ACCESS_KEY}'"
        ),
    )
}

#[cfg(test)]
mod tests {
    use aws_config::SdkConfig;
    use aws_sdk_kms::config::ProvideCredentials;
    use iceberg::encryption::kms::KmsClientFactory;

    use super::*;
    use crate::KMS_ENDPOINT;

    #[tokio::test]
    async fn test_create_client_with_injected_sdk_config() {
        let factory = AwsKmsClientFactory::new().with_sdk_config(test_sdk_config());
        let properties = HashMap::from([(
            KMS_ENDPOINT.to_string(),
            "http://localhost:4566".to_string(),
        )]);

        let client = factory.create_kms_client(&properties).await.unwrap();
        assert!(client.supports_key_generation());
    }

    #[tokio::test]
    async fn test_create_client_adds_missing_behavior_version() {
        let factory = AwsKmsClientFactory::new().with_sdk_config(SdkConfig::builder().build());

        let client = factory.create_kms_client(&HashMap::new()).await.unwrap();

        assert!(client.supports_key_generation());
    }

    #[tokio::test]
    async fn test_reject_incomplete_static_credentials() {
        let only_access_key =
            HashMap::from([(AWS_ACCESS_KEY_ID.to_string(), "access".to_string())]);
        let error = create_sdk_config(&only_access_key).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DataInvalid);

        let only_session_token =
            HashMap::from([(AWS_SESSION_TOKEN.to_string(), "token".to_string())]);
        let error = create_sdk_config(&only_session_token).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
    }

    #[tokio::test]
    async fn test_static_credentials_and_region_override_default_chains() {
        let properties = HashMap::from([
            (AWS_ACCESS_KEY_ID.to_string(), "access".to_string()),
            (AWS_SECRET_ACCESS_KEY.to_string(), "secret".to_string()),
            (AWS_SESSION_TOKEN.to_string(), "token".to_string()),
            (AWS_REGION_NAME.to_string(), "eu-west-2".to_string()),
        ]);

        let config = create_sdk_config(&properties).await.unwrap();
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

    fn test_sdk_config() -> SdkConfig {
        SdkConfig::builder()
            .behavior_version(BehaviorVersion::latest())
            .build()
    }
}
