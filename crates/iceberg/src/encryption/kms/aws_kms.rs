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

use std::fmt;

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_kms::Client;
use aws_sdk_kms::primitives::Blob;
use aws_sdk_kms::types::DataKeySpec;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;

use crate::encryption::{GeneratedKey, KeyManagementClient, SensitiveBytes};
use crate::{Error, ErrorKind};

/// AWS KMS implementation
///
/// ```
/// use aws_sdk_kms::types::DataKeySpec;
/// use iceberg::encryption::KeyManagementClient;
/// use iceberg::encryption::kms::AwsKeyManagementClient;
///
/// async fn example() -> iceberg::Result<()> {
///     let kms = AwsKeyManagementClient::new(DataKeySpec::Aes128).await;
///     let dek = vec![0u8; 32];
///     let wrapped = kms.wrap_key(&dek, "arn:aws:kms:us-west-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab").await?;
///     let unwrapped = kms.unwrap_key(&wrapped, "arn:aws:kms:us-west-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab").await?;
///     assert_eq!(dek.as_slice(), unwrapped.as_bytes());
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct AwsKeyManagementClient {
    kms_client: Client,
    data_key_spec: DataKeySpec,
}

impl fmt::Debug for AwsKeyManagementClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AwsKeyManagementClient").finish()
    }
}

impl AwsKeyManagementClient {
    /// Creates an AwsKeyManagementClient with a default AWS KMS client and DataKeySpec
    pub async fn new(data_key_spec: DataKeySpec) -> Self {
        let region_provider = RegionProviderChain::default_provider().or_else("us-west-2");

        // This line uses the default credential provider chain
        let config = aws_config::defaults(BehaviorVersion::v2026_01_12())
            // region can also be loaded from AWS_DEFAULT_REGION, just remove this line.
            .region(region_provider)
            .load()
            .await;

        Self {
            kms_client: Client::new(&config),
            data_key_spec,
        }
    }

    /// Creates an AwsKeyManagementClient with a provided AWS KMS client and DataKeySpec
    pub fn new_with_client(kms_client: Client, data_key_spec: DataKeySpec) -> Self {
        Self {
            kms_client,
            data_key_spec,
        }
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
            .await;
        let blob = resp
            .map(|r| r.ciphertext_blob.expect("Could not get encrypted text"))
            .map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Error encrypting wrapped key: {e}"),
                )
            })?;

        Ok(blob.into_inner())
    }

    async fn unwrap_key(
        &self,
        wrapped_key: &[u8],
        wrapping_key_id: &str,
    ) -> crate::Result<SensitiveBytes> {
        let wrapped_key = BASE64.decode(wrapped_key).map(Blob::new).map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Error base64 decoding wrapped key: {e}"),
            )
        })?;

        let resp = self
            .kms_client
            .decrypt()
            .key_id(wrapping_key_id)
            .ciphertext_blob(wrapped_key)
            .send()
            .await;

        let inner = resp.map(|r| r.plaintext.unwrap()).map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Error decrypting wrapped key: {e}"),
            )
        })?;

        Ok(SensitiveBytes::new(inner.into_inner()))
    }

    fn supports_key_generation(&self) -> bool {
        true
    }

    async fn generate_key(&self, wrapping_key_id: &str) -> crate::Result<GeneratedKey> {
        let resp = self
            .kms_client
            .generate_data_key()
            .key_id(wrapping_key_id)
            .key_spec(self.data_key_spec.clone())
            .send()
            .await;

        let key = resp
            .map(|r| {
                let wrapped_key = r
                    .ciphertext_blob
                    .expect("Could not get encrypted text")
                    .into_inner();
                let plaintext = r.plaintext.expect("Could not get plaintext");
                GeneratedKey::new(SensitiveBytes::new(plaintext.into_inner()), wrapped_key)
            })
            .map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Error generating wrapped key: {e}"),
                )
            })?;

        Ok(key)
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
            .match_requests(|req| req.key_id == Some(String::from("master-key-id")))
            .then_output(|| {
                EncryptOutput::builder()
                    .ciphertext_blob(Blob::from(b"hello world".to_vec()))
                    .build()
            });

        let client = mock_client!(aws_sdk_kms, [&encrypt_rule]);

        let kms = AwsKeyManagementClient::new_with_client(client, DataKeySpec::Aes256);

        assert_eq!(
            kms.wrap_key(b"123", "master-key-id").await.unwrap(),
            b"hello world".to_vec()
        );
    }

    #[tokio::test]
    async fn test_unwrap_key() {
        let decrypt_rule = mock!(Client::decrypt)
            .match_requests(|req| req.key_id == Some(String::from("master-key-id")))
            .then_output(|| {
                DecryptOutput::builder()
                    .plaintext(Blob::from(b"hello world".to_vec()))
                    .build()
            });

        let client = mock_client!(aws_sdk_kms, [&decrypt_rule]);

        let kms = AwsKeyManagementClient::new_with_client(client, DataKeySpec::Aes128);

        assert_eq!(
            kms.unwrap_key(BASE64.encode(b"123").as_bytes(), "master-key-id")
                .await
                .unwrap(),
            SensitiveBytes::new(b"hello world".to_vec())
        );
    }
}
