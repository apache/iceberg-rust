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
use aws_sdk_kms::Client;
use aws_sdk_kms::primitives::Blob;
use aws_sdk_kms::types::{DataKeySpec, EncryptionAlgorithmSpec};
use iceberg::encryption::{GeneratedKey, KeyManagementClient, SensitiveBytes};
use iceberg::{Error, ErrorKind, Result};

pub(crate) struct AwsKeyManagementClient {
    client: Client,
    encryption_algorithm: EncryptionAlgorithmSpec,
    data_key_spec: DataKeySpec,
}

impl AwsKeyManagementClient {
    pub(crate) fn new(
        client: Client,
        encryption_algorithm: EncryptionAlgorithmSpec,
        data_key_spec: DataKeySpec,
    ) -> Self {
        Self {
            client,
            encryption_algorithm,
            data_key_spec,
        }
    }

    fn missing_response_field(operation: &str, field: &str) -> Error {
        Error::new(
            ErrorKind::Unexpected,
            format!("AWS KMS {operation} response did not contain {field}"),
        )
    }
}

impl fmt::Debug for AwsKeyManagementClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AwsKeyManagementClient")
            .field("encryption_algorithm", &self.encryption_algorithm)
            .field("data_key_spec", &self.data_key_spec)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl KeyManagementClient for AwsKeyManagementClient {
    async fn wrap_key(&self, key: &[u8], wrapping_key_id: &str) -> Result<Vec<u8>> {
        let response = self
            .client
            .encrypt()
            .key_id(wrapping_key_id)
            .encryption_algorithm(self.encryption_algorithm.clone())
            .plaintext(Blob::new(key))
            .send()
            .await
            .map_err(|source| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("AWS KMS Encrypt failed for key '{wrapping_key_id}'"),
                )
                .with_source(source)
            })?;

        response
            .ciphertext_blob
            .map(Blob::into_inner)
            .ok_or_else(|| Self::missing_response_field("Encrypt", "ciphertext_blob"))
    }

    async fn unwrap_key(
        &self,
        wrapped_key: &[u8],
        wrapping_key_id: &str,
    ) -> Result<SensitiveBytes> {
        let response = self
            .client
            .decrypt()
            .key_id(wrapping_key_id)
            .encryption_algorithm(self.encryption_algorithm.clone())
            .ciphertext_blob(Blob::new(wrapped_key))
            .send()
            .await
            .map_err(|source| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("AWS KMS Decrypt failed for key '{wrapping_key_id}'"),
                )
                .with_source(source)
            })?;

        response
            .plaintext
            .map(|blob| SensitiveBytes::new(blob.into_inner()))
            .ok_or_else(|| Self::missing_response_field("Decrypt", "plaintext"))
    }

    fn supports_key_generation(&self) -> bool {
        self.encryption_algorithm == EncryptionAlgorithmSpec::SymmetricDefault
    }

    async fn generate_key(&self, wrapping_key_id: &str) -> Result<GeneratedKey> {
        let response = self
            .client
            .generate_data_key()
            .key_id(wrapping_key_id)
            .key_spec(self.data_key_spec.clone())
            .send()
            .await
            .map_err(|source| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("AWS KMS GenerateDataKey failed for key '{wrapping_key_id}'"),
                )
                .with_source(source)
            })?;

        let plaintext = response
            .plaintext
            .map(|blob| SensitiveBytes::new(blob.into_inner()))
            .ok_or_else(|| Self::missing_response_field("GenerateDataKey", "plaintext"))?;
        let wrapped_key = response
            .ciphertext_blob
            .map(Blob::into_inner)
            .ok_or_else(|| Self::missing_response_field("GenerateDataKey", "ciphertext_blob"))?;

        Ok(GeneratedKey::new(plaintext, wrapped_key))
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;
    use std::sync::{Arc, Mutex};

    use aws_sdk_kms::Client;
    use aws_sdk_kms::operation::decrypt::DecryptOutput;
    use aws_sdk_kms::operation::encrypt::{EncryptError, EncryptOutput};
    use aws_sdk_kms::operation::generate_data_key::GenerateDataKeyOutput;
    use aws_sdk_kms::types::error::NotFoundException;
    use aws_smithy_mocks::{RuleMode, mock, mock_client};
    use iceberg::encryption::{EncryptionManager, StandardKeyMetadata};

    use super::*;

    const KEY_ID: &str = "arn:aws:kms:eu-west-2:123456789012:key/test-key";

    #[tokio::test]
    async fn test_wrap_key() {
        let plaintext = b"0123456789abcdef";
        let ciphertext = b"wrapped-key";
        let rule = mock!(Client::encrypt)
            .match_requests(|request| {
                request.key_id() == Some(KEY_ID)
                    && request.encryption_algorithm()
                        == Some(&EncryptionAlgorithmSpec::SymmetricDefault)
                    && request.plaintext().map(AsRef::as_ref) == Some(plaintext.as_slice())
            })
            .then_output(move || {
                EncryptOutput::builder()
                    .ciphertext_blob(Blob::new(ciphertext))
                    .build()
            });
        let client = mock_client!(aws_sdk_kms, &[&rule]);
        let kms = test_client(client);

        assert_eq!(kms.wrap_key(plaintext, KEY_ID).await.unwrap(), ciphertext);
        assert_eq!(rule.num_calls(), 1);
    }

    #[tokio::test]
    async fn test_unwrap_key() {
        let plaintext = b"0123456789abcdef";
        let ciphertext = b"wrapped-key";
        let rule = mock!(Client::decrypt)
            .match_requests(|request| {
                request.key_id() == Some(KEY_ID)
                    && request.encryption_algorithm()
                        == Some(&EncryptionAlgorithmSpec::SymmetricDefault)
                    && request.ciphertext_blob().map(AsRef::as_ref) == Some(ciphertext.as_slice())
            })
            .then_output(move || {
                DecryptOutput::builder()
                    .plaintext(Blob::new(plaintext))
                    .build()
            });
        let client = mock_client!(aws_sdk_kms, &[&rule]);
        let kms = test_client(client);

        assert_eq!(
            kms.unwrap_key(ciphertext, KEY_ID).await.unwrap().as_bytes(),
            plaintext
        );
        assert_eq!(rule.num_calls(), 1);
    }

    #[tokio::test]
    async fn test_generate_key() {
        let plaintext = b"0123456789abcdef0123456789abcdef";
        let ciphertext = b"generated-wrapped-key";
        let rule = mock!(Client::generate_data_key)
            .match_requests(|request| {
                request.key_id() == Some(KEY_ID) && request.key_spec() == Some(&DataKeySpec::Aes256)
            })
            .then_output(move || {
                GenerateDataKeyOutput::builder()
                    .plaintext(Blob::new(plaintext))
                    .ciphertext_blob(Blob::new(ciphertext))
                    .build()
            });
        let client = mock_client!(aws_sdk_kms, &[&rule]);
        let kms = test_client(client);

        let generated = kms.generate_key(KEY_ID).await.unwrap();
        assert_eq!(generated.key().as_bytes(), plaintext);
        assert_eq!(generated.wrapped_key(), ciphertext);
        assert_eq!(rule.num_calls(), 1);
    }

    #[test]
    fn test_key_generation_support_depends_on_encryption_algorithm() {
        let client = mock_client!(aws_sdk_kms, &[]);

        assert!(
            AwsKeyManagementClient::new(
                client.clone(),
                EncryptionAlgorithmSpec::SymmetricDefault,
                DataKeySpec::Aes256,
            )
            .supports_key_generation()
        );
        assert!(
            !AwsKeyManagementClient::new(
                client,
                EncryptionAlgorithmSpec::RsaesOaepSha256,
                DataKeySpec::Aes256,
            )
            .supports_key_generation()
        );
    }

    #[tokio::test]
    async fn test_asymmetric_manager_roundtrip_uses_encrypt_instead_of_generate_data_key() {
        let plaintext_kek = Arc::new(Mutex::new(Vec::new()));
        let plaintext_kek_from_encrypt = Arc::clone(&plaintext_kek);
        let encrypt_rule = mock!(Client::encrypt)
            .match_requests(move |request| {
                if request.key_id() != Some(KEY_ID)
                    || request.encryption_algorithm()
                        != Some(&EncryptionAlgorithmSpec::RsaesOaepSha256)
                {
                    return false;
                }

                *plaintext_kek_from_encrypt.lock().unwrap() = request
                    .plaintext()
                    .expect("EncryptionManager must provide a plaintext KEK")
                    .as_ref()
                    .to_vec();
                true
            })
            .then_output(|| {
                EncryptOutput::builder()
                    .ciphertext_blob(Blob::new(b"wrapped-asymmetric-kek"))
                    .build()
            });
        let plaintext_kek_for_decrypt = Arc::clone(&plaintext_kek);
        let decrypt_rule = mock!(Client::decrypt).then_output(move || {
            DecryptOutput::builder()
                .plaintext(Blob::new(plaintext_kek_for_decrypt.lock().unwrap().clone()))
                .build()
        });
        let client = mock_client!(aws_sdk_kms, &[&encrypt_rule, &decrypt_rule]);
        let kms: Arc<dyn KeyManagementClient> = Arc::new(AwsKeyManagementClient::new(
            client,
            EncryptionAlgorithmSpec::RsaesOaepSha256,
            DataKeySpec::Aes256,
        ));
        let manager = EncryptionManager::builder()
            .kms_client(Arc::clone(&kms))
            .table_key_id(KEY_ID)
            .build();
        let key_metadata = StandardKeyMetadata::try_new(b"0123456789abcdef")
            .unwrap()
            .with_aad_prefix(b"test-aad-prefix!");

        let encrypted_key_id = manager
            .encrypt_manifest_list_key_metadata(&key_metadata)
            .await
            .unwrap();
        let encryption_keys = manager.with_encryption_keys(Clone::clone);
        let reader = EncryptionManager::builder()
            .kms_client(kms)
            .table_key_id(KEY_ID)
            .encryption_keys(encryption_keys)
            .build();
        let decrypted = reader
            .decrypt_manifest_list_key_metadata(&encrypted_key_id)
            .await
            .unwrap();

        assert_eq!(decrypted, key_metadata);
        assert_eq!(encrypt_rule.num_calls(), 1);
        assert_eq!(decrypt_rule.num_calls(), 1);
    }

    #[tokio::test]
    async fn test_aws_service_error_is_preserved_as_source() {
        let rule = mock!(Client::encrypt).then_error(|| {
            EncryptError::NotFoundException(
                NotFoundException::builder()
                    .message("test key not found")
                    .build(),
            )
        });
        let client = mock_client!(aws_sdk_kms, &[&rule]);
        let kms = test_client(client);

        let error = kms.wrap_key(b"key", KEY_ID).await.unwrap_err();

        assert_eq!(error.kind(), ErrorKind::Unexpected);
        assert!(error.source().is_some());
    }

    #[tokio::test]
    async fn test_missing_response_fields() {
        let encrypt_rule = mock!(Client::encrypt).then_output(|| EncryptOutput::builder().build());
        let decrypt_rule = mock!(Client::decrypt).then_output(|| DecryptOutput::builder().build());
        let generate_rule = mock!(Client::generate_data_key)
            .then_output(|| GenerateDataKeyOutput::builder().build());
        let client = mock_client!(aws_sdk_kms, RuleMode::Sequential, &[
            &encrypt_rule,
            &decrypt_rule,
            &generate_rule
        ]);
        let kms = test_client(client);

        let error = kms.wrap_key(b"key", KEY_ID).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unexpected);
        assert!(error.message().contains("ciphertext_blob"));

        let error = kms.unwrap_key(b"wrapped", KEY_ID).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unexpected);
        assert!(error.message().contains("plaintext"));

        let error = kms.generate_key(KEY_ID).await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::Unexpected);
        assert!(error.message().contains("plaintext"));
    }

    #[tokio::test]
    async fn test_generate_key_requires_wrapped_key() {
        let rule = mock!(Client::generate_data_key).then_output(|| {
            GenerateDataKeyOutput::builder()
                .plaintext(Blob::new([0; 32]))
                .build()
        });
        let client = mock_client!(aws_sdk_kms, &[&rule]);
        let kms = test_client(client);

        let error = kms.generate_key(KEY_ID).await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::Unexpected);
        assert!(error.message().contains("ciphertext_blob"));
    }

    fn test_client(client: Client) -> AwsKeyManagementClient {
        AwsKeyManagementClient::new(
            client,
            EncryptionAlgorithmSpec::SymmetricDefault,
            DataKeySpec::Aes256,
        )
    }
}
