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
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::encryption::kms::{KeyManagementClient, KmsClientFactory};
use iceberg::{Error, ErrorKind, Result};

#[cfg(feature = "aws")]
use crate::aws::AwsKmsClientFactory;

/// Catalog property selecting the key management service implementation.
pub const ENCRYPTION_KMS_TYPE: &str = "encryption.kms-type";
/// [`ENCRYPTION_KMS_TYPE`] value selecting AWS Key Management Service.
pub const ENCRYPTION_KMS_TYPE_AWS: &str = "aws";
/// [`ENCRYPTION_KMS_TYPE`] value selecting Azure Key Vault.
pub const ENCRYPTION_KMS_TYPE_AZURE: &str = "azure";
/// [`ENCRYPTION_KMS_TYPE`] value selecting Google Cloud Key Management Service.
pub const ENCRYPTION_KMS_TYPE_GCP: &str = "gcp";

const ENCRYPTION_KMS_IMPL: &str = "encryption.kms-impl";

/// A KMS client factory that resolves an implementation from catalog properties.
///
/// The factory reads [`ENCRYPTION_KMS_TYPE`] and delegates client creation to the
/// corresponding provider factory. These built-in mappings are fixed: custom
/// implementations should instead be supplied directly through
/// [`CatalogBuilder::with_kms_client_factory`](iceberg::CatalogBuilder::with_kms_client_factory).
///
/// Provider implementations must be enabled through their corresponding crate
/// features. The `aws` feature currently provides the `aws` KMS type.
///
/// Unlike Iceberg Java, this factory does not load class names from
/// `encryption.kms-impl`. Rust applications use the `KmsClientFactory` trait as
/// the custom implementation extension point. Configuring `encryption.kms-impl`
/// is rejected with guidance to supply a custom factory directly.
///
/// Catalog properties returned later by a REST catalog server are not currently
/// available when the KMS client is created. Until REST initialization is
/// updated, `encryption.kms-type` must be supplied by the application.
///
/// # Example
///
/// This example requires the `aws` feature.
///
/// ```rust,no_run
/// use std::collections::HashMap;
/// use std::sync::Arc;
///
/// use iceberg::CatalogBuilder;
/// use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, RestCatalogBuilder};
/// use iceberg_kms::{ENCRYPTION_KMS_TYPE, ResolvingKmsClientFactory};
///
/// # async fn example() -> iceberg::Result<()> {
/// let properties = HashMap::from([
///     (
///         REST_CATALOG_PROP_URI.to_string(),
///         "https://catalog.example.com".to_string(),
///     ),
///     (ENCRYPTION_KMS_TYPE.to_string(), "aws".to_string()),
/// ]);
/// let catalog = RestCatalogBuilder::default()
///     .with_kms_client_factory(Arc::new(ResolvingKmsClientFactory::new()))
///     .load("rest", properties)
///     .await?;
/// # let _ = catalog;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct ResolvingKmsClientFactory {
    #[cfg(feature = "aws")]
    aws_factory: AwsKmsClientFactory,
}

impl Default for ResolvingKmsClientFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl ResolvingKmsClientFactory {
    /// Create a factory containing all enabled built-in KMS implementations.
    pub fn new() -> Self {
        Self {
            #[cfg(feature = "aws")]
            aws_factory: AwsKmsClientFactory::new(),
        }
    }

    /// Use a configured AWS KMS client factory for the `aws` KMS type.
    #[cfg(feature = "aws")]
    pub fn with_aws_factory(mut self, factory: AwsKmsClientFactory) -> Self {
        self.aws_factory = factory;
        self
    }
}

#[async_trait]
impl KmsClientFactory for ResolvingKmsClientFactory {
    async fn create_kms_client(
        &self,
        properties: &HashMap<String, String>,
    ) -> Result<Arc<dyn KeyManagementClient>> {
        let kms_type = properties.get(ENCRYPTION_KMS_TYPE);
        let kms_impl = properties.get(ENCRYPTION_KMS_IMPL);

        if let Some(kms_impl) = kms_impl {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!(
                    "KMS implementation class '{kms_impl}' is not supported; provide a custom KmsClientFactory instead"
                ),
            ));
        }

        let kms_type = kms_type
            .filter(|kms_type| !kms_type.is_empty())
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Missing required catalog property '{ENCRYPTION_KMS_TYPE}'"),
                )
            })?;

        match kms_type.to_ascii_lowercase().as_str() {
            ENCRYPTION_KMS_TYPE_AWS => {
                #[cfg(feature = "aws")]
                {
                    self.aws_factory.create_kms_client(properties).await
                }
                #[cfg(not(feature = "aws"))]
                {
                    Err(Error::new(
                        ErrorKind::FeatureUnsupported,
                        "KMS type 'aws' requires the 'aws' feature of iceberg-kms",
                    ))
                }
            }
            ENCRYPTION_KMS_TYPE_AZURE | ENCRYPTION_KMS_TYPE_GCP => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!("KMS type '{kms_type}' is not implemented by this version of iceberg-kms"),
            )),
            _ => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!("Unsupported KMS type: {kms_type}"),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "aws")]
    use aws_config::{BehaviorVersion, SdkConfig};

    use super::*;

    #[cfg(feature = "aws")]
    #[tokio::test]
    async fn test_resolves_aws_case_insensitively() {
        let factory = ResolvingKmsClientFactory::new().with_aws_factory(
            AwsKmsClientFactory::new().with_sdk_config(
                SdkConfig::builder()
                    .behavior_version(BehaviorVersion::latest())
                    .build(),
            ),
        );
        let properties = HashMap::from([(ENCRYPTION_KMS_TYPE.to_string(), "AWS".to_string())]);

        let client = factory.create_kms_client(&properties).await.unwrap();

        assert!(client.supports_key_generation());
    }

    #[tokio::test]
    async fn test_rejects_missing_kms_type() {
        let error = ResolvingKmsClientFactory::new()
            .create_kms_client(&HashMap::new())
            .await
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(error.to_string().contains(ENCRYPTION_KMS_TYPE));
    }

    #[tokio::test]
    async fn test_rejects_unsupported_kms_type() {
        let properties =
            HashMap::from([(ENCRYPTION_KMS_TYPE.to_string(), "unsupported".to_string())]);
        let error = ResolvingKmsClientFactory::new()
            .create_kms_client(&properties)
            .await
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::FeatureUnsupported);
        assert!(error.to_string().contains("unsupported"));
    }

    #[tokio::test]
    async fn test_reports_unimplemented_built_in_kms_types() {
        for kms_type in [ENCRYPTION_KMS_TYPE_AZURE, ENCRYPTION_KMS_TYPE_GCP] {
            let properties =
                HashMap::from([(ENCRYPTION_KMS_TYPE.to_string(), kms_type.to_string())]);
            let error = ResolvingKmsClientFactory::new()
                .create_kms_client(&properties)
                .await
                .unwrap_err();

            assert_eq!(error.kind(), ErrorKind::FeatureUnsupported);
            assert!(error.to_string().contains(kms_type));
        }
    }

    #[tokio::test]
    async fn test_rejects_java_kms_impl() {
        let properties = HashMap::from([(
            ENCRYPTION_KMS_IMPL.to_string(),
            "com.example.CustomKmsClient".to_string(),
        )]);
        let error = ResolvingKmsClientFactory::new()
            .create_kms_client(&properties)
            .await
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::FeatureUnsupported);
        assert!(error.to_string().contains("custom KmsClientFactory"));
    }

    #[cfg(not(feature = "aws"))]
    #[tokio::test]
    async fn test_reports_disabled_built_in_kms_type() {
        let properties = HashMap::from([(
            ENCRYPTION_KMS_TYPE.to_string(),
            ENCRYPTION_KMS_TYPE_AWS.to_string(),
        )]);
        let error = ResolvingKmsClientFactory::new()
            .create_kms_client(&properties)
            .await
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::FeatureUnsupported);
        assert!(error.to_string().contains(ENCRYPTION_KMS_TYPE_AWS));
    }
}
