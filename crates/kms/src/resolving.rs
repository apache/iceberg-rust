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
const ENCRYPTION_KMS_TYPE_AZURE: &str = "azure";
const ENCRYPTION_KMS_TYPE_GCP: &str = "gcp";

const ENCRYPTION_KMS_IMPL: &str = "encryption.kms-impl";
// Iceberg Java's built-in implementations, accepted so that catalog properties
// can be shared with Java.
const ENCRYPTION_KMS_IMPL_AWS: &str = "org.apache.iceberg.aws.AwsKeyManagementClient";
const ENCRYPTION_KMS_IMPL_AZURE: &str =
    "org.apache.iceberg.azure.keymanagement.AzureKeyManagementClient";
const ENCRYPTION_KMS_IMPL_GCP: &str = "org.apache.iceberg.gcp.GcpKeyManagementClient";

/// A KMS client factory that selects a built-in implementation from catalog
/// properties.
///
/// The implementation is selected by [`ENCRYPTION_KMS_TYPE`], using Iceberg
/// Java's type names, or by `encryption.kms-impl` naming one of Iceberg Java's
/// built-in client classes. Provider implementations must be enabled through
/// their crate features; the `aws` feature provides the `aws` type. Custom
/// implementations are supplied directly through
/// [`CatalogBuilder::with_kms_client_factory`](iceberg::CatalogBuilder::with_kms_client_factory).
///
/// Catalog properties returned later by a REST catalog server are not
/// available when the KMS client is created, so the selecting property must be
/// supplied by the application.
///
/// # Errors
///
/// [`create_kms_client`](KmsClientFactory::create_kms_client) returns:
///
/// - [`ErrorKind::DataInvalid`] if neither property is set, or both are.
/// - [`ErrorKind::FeatureUnsupported`] if the selected implementation is
///   unknown, not implemented, or not enabled through its crate feature.
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
/// use iceberg::io::CLIENT_REGION;
/// use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
/// use iceberg_kms::{ENCRYPTION_KMS_TYPE, ENCRYPTION_KMS_TYPE_AWS, ResolvingKmsClientFactory};
///
/// # async fn example() -> iceberg::Result<()> {
/// let properties = HashMap::from([
///     (
///         MEMORY_CATALOG_WAREHOUSE.to_string(),
///         "s3://bucket/warehouse".to_string(),
///     ),
///     (
///         ENCRYPTION_KMS_TYPE.to_string(),
///         ENCRYPTION_KMS_TYPE_AWS.to_string(),
///     ),
///     (CLIENT_REGION.to_string(), "eu-west-2".to_string()),
/// ]);
/// let catalog = MemoryCatalogBuilder::default()
///     .with_kms_client_factory(Arc::new(ResolvingKmsClientFactory::new()))
///     .load("catalog", properties)
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
        match kms_type(properties)?.as_str() {
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
            kms_type @ (ENCRYPTION_KMS_TYPE_AZURE | ENCRYPTION_KMS_TYPE_GCP) => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!("KMS type '{kms_type}' is not implemented by this version of iceberg-kms"),
            )),
            kms_type => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!("Unsupported KMS type: {kms_type}"),
            )),
        }
    }
}

/// Returns the lowercase KMS type selected by the catalog properties.
fn kms_type(properties: &HashMap<String, String>) -> Result<String> {
    let kms_type = properties
        .get(ENCRYPTION_KMS_TYPE)
        .filter(|kms_type| !kms_type.is_empty());
    let kms_impl = properties
        .get(ENCRYPTION_KMS_IMPL)
        .filter(|kms_impl| !kms_impl.is_empty());

    match (kms_type, kms_impl) {
        (Some(kms_type), None) => Ok(kms_type.to_ascii_lowercase()),
        (None, Some(kms_impl)) => match kms_impl.as_str() {
            ENCRYPTION_KMS_IMPL_AWS => Ok(ENCRYPTION_KMS_TYPE_AWS.to_string()),
            ENCRYPTION_KMS_IMPL_AZURE => Ok(ENCRYPTION_KMS_TYPE_AZURE.to_string()),
            ENCRYPTION_KMS_IMPL_GCP => Ok(ENCRYPTION_KMS_TYPE_GCP.to_string()),
            _ => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!(
                    "KMS implementation class '{kms_impl}' is not supported; provide a custom KmsClientFactory instead"
                ),
            )),
        },
        (Some(kms_type), Some(kms_impl)) => Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Cannot set both '{ENCRYPTION_KMS_TYPE}' ({kms_type}) and '{ENCRYPTION_KMS_IMPL}' ({kms_impl})"
            ),
        )),
        (None, None) => Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Missing required catalog property '{ENCRYPTION_KMS_TYPE}'"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn properties(properties: &[(&str, &str)]) -> HashMap<String, String> {
        properties
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn test_kms_type_resolution() {
        for (input, expected) in [
            (vec![(ENCRYPTION_KMS_TYPE, "aws")], "aws"),
            (vec![(ENCRYPTION_KMS_TYPE, "AWS")], "aws"),
            (vec![(ENCRYPTION_KMS_IMPL, ENCRYPTION_KMS_IMPL_AWS)], "aws"),
            (
                vec![(ENCRYPTION_KMS_IMPL, ENCRYPTION_KMS_IMPL_AZURE)],
                "azure",
            ),
            (vec![(ENCRYPTION_KMS_IMPL, ENCRYPTION_KMS_IMPL_GCP)], "gcp"),
        ] {
            assert_eq!(
                kms_type(&properties(&input)).unwrap(),
                expected,
                "{input:?}"
            );
        }
    }

    #[test]
    fn test_kms_type_resolution_errors() {
        for (input, expected) in [
            (vec![], ErrorKind::DataInvalid),
            (vec![(ENCRYPTION_KMS_TYPE, "")], ErrorKind::DataInvalid),
            (
                vec![
                    (ENCRYPTION_KMS_TYPE, "aws"),
                    (ENCRYPTION_KMS_IMPL, ENCRYPTION_KMS_IMPL_AWS),
                ],
                ErrorKind::DataInvalid,
            ),
            (
                vec![(ENCRYPTION_KMS_IMPL, "com.example.CustomKmsClient")],
                ErrorKind::FeatureUnsupported,
            ),
        ] {
            assert_eq!(
                kms_type(&properties(&input)).unwrap_err().kind(),
                expected,
                "{input:?}"
            );
        }
    }

    #[cfg(feature = "aws")]
    #[tokio::test]
    async fn test_aws_type_delegates_properties_to_aws_factory() {
        use aws_config::{BehaviorVersion, SdkConfig};

        use crate::aws::KMS_ENCRYPTION_ALGORITHM_SPEC;

        let factory = ResolvingKmsClientFactory::new().with_aws_factory(
            AwsKmsClientFactory::new().with_sdk_config(
                SdkConfig::builder()
                    .behavior_version(BehaviorVersion::latest())
                    .build(),
            ),
        );
        let properties = properties(&[
            (ENCRYPTION_KMS_TYPE, ENCRYPTION_KMS_TYPE_AWS),
            (KMS_ENCRYPTION_ALGORITHM_SPEC, "RSAES_OAEP_SHA_256"),
        ]);

        let client = factory.create_kms_client(&properties).await.unwrap();

        assert!(!client.supports_key_generation());
    }

    #[tokio::test]
    async fn test_reject_unsupported_and_unimplemented_kms_types() {
        for kms_type in [
            "unsupported",
            ENCRYPTION_KMS_TYPE_AZURE,
            ENCRYPTION_KMS_TYPE_GCP,
        ] {
            let Err(error) = ResolvingKmsClientFactory::new()
                .create_kms_client(&properties(&[(ENCRYPTION_KMS_TYPE, kms_type)]))
                .await
            else {
                panic!("KMS type '{kms_type}' must be rejected");
            };

            assert_eq!(error.kind(), ErrorKind::FeatureUnsupported);
            assert!(error.message().contains(kms_type), "{error}");
        }
    }

    #[cfg(not(feature = "aws"))]
    #[tokio::test]
    async fn test_reject_aws_type_without_aws_feature() {
        let Err(error) = ResolvingKmsClientFactory::new()
            .create_kms_client(&properties(&[(
                ENCRYPTION_KMS_TYPE,
                ENCRYPTION_KMS_TYPE_AWS,
            )]))
            .await
        else {
            panic!("the aws type must require the aws feature");
        };

        assert_eq!(error.kind(), ErrorKind::FeatureUnsupported);
    }
}
