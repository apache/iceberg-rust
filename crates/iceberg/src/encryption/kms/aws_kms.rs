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

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_kms::Client;
use aws_sdk_kms::primitives::Blob;
use aws_sdk_kms::types::DataKeySpec;

use crate::encryption::{AesKeySize, GeneratedKey, KeyManagementClient, SensitiveBytes};
use crate::{Error, ErrorKind};

/// AWS KMS implementation of [`KeyManagementClient`].
///
/// ```no_run
/// use iceberg::encryption::KeyManagementClient;
/// use iceberg::encryption::kms::AwsKeyManagementClient;
///
/// async fn example() -> iceberg::Result<()> {
///     let kms = AwsKeyManagementClient::new().await;
///     let dek = vec![0u8; 32];
///     const KMS_KEY_ID: &str =
///         "arn:aws:kms:us-west-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab";
///     let wrapped = kms.wrap_key(&dek, KMS_KEY_ID).await?;
///     let unwrapped = kms.unwrap_key(&wrapped, KMS_KEY_ID).await?;
///     assert_eq!(dek.as_slice(), unwrapped.as_bytes());
///     Ok(())
/// }
/// ```
#[derive(Clone, Debug)]
pub struct AwsKeyManagementClient {
    kms_client: Client,
}

impl AwsKeyManagementClient {
    /// Creates an `AwsKeyManagementClient` using AWS SDK default credential
    /// and region resolution.
    ///
    /// AWS SDK config resolution (env vars, profiles, IMDS) runs once on construction;
    /// callers should reuse a single instance rather than building per request.
    pub async fn new() -> Self {
        let region_provider = RegionProviderChain::default_provider();

        let config = aws_config::defaults(BehaviorVersion::v2026_01_12())
            .region(region_provider)
            .load()
            .await;

        Self {
            kms_client: Client::new(&config),
        }
    }

    /// Creates an `AwsKeyManagementClient` from a pre-configured AWS KMS client.
    pub fn new_with_client(kms_client: Client) -> Self {
        Self { kms_client }
    }
}

#[async_trait]
impl KeyManagementClient for AwsKeyManagementClient {
    async fn wrap_key(&self, key: &[u8], wrapping_key_id: &str) -> crate::Result<Vec<u8>> {
        let blob = Blob::new(key);

        let resp = self
            .kms_client
            .encrypt()
            .key_id(wrapping_key_id)
            .plaintext(blob)
            .send()
            .await
            .map_err(|e| {
                Error::new(ErrorKind::Unexpected, "Failed to encrypt key via AWS KMS")
                    .with_source(e)
            })?;

        let ciphertext = resp.ciphertext_blob.ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "AWS KMS encrypt response missing ciphertext_blob",
            )
        })?;

        Ok(ciphertext.into_inner())
    }

    async fn unwrap_key(
        &self,
        wrapped_key: &[u8],
        wrapping_key_id: &str,
    ) -> crate::Result<SensitiveBytes> {
        let blob = Blob::new(wrapped_key);

        let resp = self
            .kms_client
            .decrypt()
            .key_id(wrapping_key_id)
            .ciphertext_blob(blob)
            .send()
            .await
            .map_err(|e| {
                Error::new(ErrorKind::Unexpected, "Failed to decrypt key via AWS KMS")
                    .with_source(e)
            })?;

        let plaintext = resp.plaintext.ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "AWS KMS decrypt response missing plaintext",
            )
        })?;

        Ok(SensitiveBytes::new(plaintext.into_inner()))
    }

    fn supports_key_generation(&self) -> bool {
        true
    }

    async fn generate_key(
        &self,
        wrapping_key_id: &str,
        aes_key_size: AesKeySize,
    ) -> crate::Result<GeneratedKey> {
        let data_key_spec = match aes_key_size {
            AesKeySize::Bits128 => Ok(DataKeySpec::Aes128),
            AesKeySize::Bits192 => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "AWS KMS DataKeySpec doesn't support AES-192",
            )),
            AesKeySize::Bits256 => Ok(DataKeySpec::Aes256),
        }?;

        let resp = self
            .kms_client
            .generate_data_key()
            .key_id(wrapping_key_id)
            .key_spec(data_key_spec)
            .send()
            .await
            .map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to generate data key via AWS KMS",
                )
                .with_source(e)
            })?;

        let wrapped_key = resp
            .ciphertext_blob
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "AWS KMS generate_data_key response missing ciphertext_blob",
                )
            })?
            .into_inner();

        let plaintext = resp
            .plaintext
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "AWS KMS generate_data_key response missing plaintext",
                )
            })?
            .into_inner();

        Ok(GeneratedKey::new(
            SensitiveBytes::new(plaintext),
            wrapped_key,
        ))
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_kms::operation::decrypt::DecryptOutput;
    use aws_sdk_kms::operation::encrypt::EncryptOutput;
    use aws_smithy_mocks::{mock, mock_client};

    use super::*;

    #[tokio::test]
    async fn test_wrap_key() {
        let encrypt_rule = mock!(Client::encrypt)
            .match_requests(|req| req.key_id.as_deref() == Some("master-key-id"))
            .then_output(|| {
                EncryptOutput::builder()
                    .ciphertext_blob(Blob::from(b"hello world".to_vec()))
                    .build()
            });

        let client = mock_client!(aws_sdk_kms, [&encrypt_rule]);

        let kms = AwsKeyManagementClient::new_with_client(client);

        assert_eq!(
            kms.wrap_key(b"123", "master-key-id").await.unwrap(),
            b"hello world".to_vec()
        );
    }

    #[tokio::test]
    async fn test_unwrap_key() {
        let decrypt_rule = mock!(Client::decrypt)
            .match_requests(|req| req.key_id.as_deref() == Some("master-key-id"))
            .then_output(|| {
                DecryptOutput::builder()
                    .plaintext(Blob::from(b"hello world".to_vec()))
                    .build()
            });

        let client = mock_client!(aws_sdk_kms, [&decrypt_rule]);

        let kms = AwsKeyManagementClient::new_with_client(client);

        assert_eq!(
            kms.unwrap_key(b"encrypted-blob", "master-key-id")
                .await
                .unwrap(),
            SensitiveBytes::new(b"hello world".to_vec())
        );
    }
}
