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
use aws_sdk_kms::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_kms::primitives::Blob;
use aws_sdk_kms::types::{DataKeySpec, EncryptionAlgorithmSpec};
use base64::Engine as _;
use base64::prelude::BASE64_STANDARD;
use iceberg::encryption::SensitiveBytes;
use iceberg::encryption::kms::{GeneratedKey, KeyManagementClient};
use iceberg::{Error, ErrorKind, Result};

pub(crate) struct AwsKeyManagementClient {
    client: aws_sdk_kms::Client,
    encryption_algorithm: EncryptionAlgorithmSpec,
    data_key_spec: DataKeySpec,
}

impl AwsKeyManagementClient {
    pub(crate) fn new(
        client: aws_sdk_kms::Client,
        encryption_algorithm: EncryptionAlgorithmSpec,
        data_key_spec: DataKeySpec,
    ) -> Self {
        Self {
            client,
            encryption_algorithm,
            data_key_spec,
        }
    }

    /// Converts a failed KMS request into an Iceberg error.
    ///
    /// `plaintext` is the key sent in the request, if any. The SDK can describe
    /// the request, including its body, in transport errors (for example when
    /// an endpoint URL cannot be applied), so its base64 encoding is redacted
    /// from the message. The raw HTTP response, which holds the plaintext key
    /// when a Decrypt or GenerateDataKey response fails to parse, is dropped.
    fn request_failed<E, R>(
        operation: &str,
        wrapping_key_id: &str,
        plaintext: Option<&[u8]>,
        error: SdkError<E, R>,
    ) -> Error
    where
        E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
        R: fmt::Debug,
    {
        let message = |cause: String| {
            format!("AWS KMS {operation} failed for key '{wrapping_key_id}': {cause}")
        };
        match error {
            SdkError::ServiceError(context) => {
                let service_error = context.into_err();
                let description = service_error.to_string();
                let cause = match service_error.message() {
                    Some(aws_message) if !description.contains(aws_message) => {
                        format!("{description}: {aws_message}")
                    }
                    _ => description,
                };
                Error::new(ErrorKind::Unexpected, message(cause)).with_source(service_error)
            }
            error => {
                let mut causes =
                    std::iter::successors(Some(&error as &dyn std::error::Error), |error| {
                        (*error).source()
                    })
                    .map(ToString::to_string)
                    .collect::<Vec<_>>();
                causes.dedup();
                let cause = causes.join(": ");
                let cause = match plaintext.filter(|plaintext| !plaintext.is_empty()) {
                    Some(plaintext) => {
                        cause.replace(&BASE64_STANDARD.encode(plaintext), "<redacted>")
                    }
                    None => cause,
                };
                // The cause chain is not attached as a source, since only the
                // message is redacted.
                Error::new(ErrorKind::Unexpected, message(cause))
            }
        }
    }

    fn missing_response_field(operation: &str, field: &str) -> Error {
        Error::new(
            ErrorKind::Unexpected,
            format!("AWS KMS {operation} response did not contain {field}"),
        )
    }

    fn ciphertext_blob(operation: &str, blob: Option<Blob>) -> Result<Vec<u8>> {
        blob.map(Blob::into_inner)
            .filter(|ciphertext| !ciphertext.is_empty())
            .ok_or_else(|| Self::missing_response_field(operation, "ciphertext_blob"))
    }

    fn generated_key_length(&self) -> Option<usize> {
        match self.data_key_spec {
            DataKeySpec::Aes128 => Some(16),
            DataKeySpec::Aes256 => Some(32),
            _ => None,
        }
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
                Self::request_failed("Encrypt", wrapping_key_id, Some(key), source)
            })?;

        Self::ciphertext_blob("Encrypt", response.ciphertext_blob)
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
            .map_err(|source| Self::request_failed("Decrypt", wrapping_key_id, None, source))?;

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
                Self::request_failed("GenerateDataKey", wrapping_key_id, None, source)
            })?;

        let plaintext = response
            .plaintext
            .map(|blob| SensitiveBytes::new(blob.into_inner()))
            .ok_or_else(|| Self::missing_response_field("GenerateDataKey", "plaintext"))?;
        if let Some(expected) = self.generated_key_length()
            && plaintext.len() != expected
        {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!(
                    "AWS KMS GenerateDataKey returned a {}-byte key for key spec {}, expected {expected} bytes",
                    plaintext.len(),
                    self.data_key_spec.as_str()
                ),
            ));
        }
        let wrapped_key = Self::ciphertext_blob("GenerateDataKey", response.ciphertext_blob)?;

        Ok(GeneratedKey::new(plaintext, wrapped_key))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use aws_sdk_kms::error::{ConnectorError, ErrorMetadata};
    use aws_sdk_kms::operation::decrypt::{DecryptError, DecryptOutput};
    use aws_sdk_kms::operation::encrypt::{EncryptError, EncryptOutput};
    use aws_sdk_kms::operation::generate_data_key::{GenerateDataKeyError, GenerateDataKeyOutput};
    use aws_sdk_kms::types::error::{DisabledException, NotFoundException};
    use aws_smithy_mocks::{RuleMode, mock, mock_client};
    use iceberg::encryption::{EncryptionManager, StandardKeyMetadata};

    use super::*;

    const KEY_ID: &str = "arn:aws:kms:eu-west-2:123456789012:key/test-key";

    #[tokio::test]
    async fn test_wrap_key() {
        let plaintext = b"0123456789abcdef";
        let ciphertext = b"wrapped-key";
        let rule = mock!(aws_sdk_kms::Client::encrypt)
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
        let rule = mock!(aws_sdk_kms::Client::decrypt)
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
        let rule = mock!(aws_sdk_kms::Client::generate_data_key)
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
        let encrypt_rule = mock!(aws_sdk_kms::Client::encrypt)
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
        let decrypt_rule = mock!(aws_sdk_kms::Client::decrypt)
            .match_requests(|request| {
                request.key_id() == Some(KEY_ID)
                    && request.encryption_algorithm()
                        == Some(&EncryptionAlgorithmSpec::RsaesOaepSha256)
                    && request.ciphertext_blob().map(AsRef::as_ref)
                        == Some(b"wrapped-asymmetric-kek".as_slice())
            })
            .then_output(move || {
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
    async fn test_aws_service_error_messages_name_operation_and_key() {
        let not_found = || {
            NotFoundException::builder()
                .message("test key not found")
                .build()
        };
        let encrypt_rule = mock!(aws_sdk_kms::Client::encrypt)
            .then_error(move || EncryptError::NotFoundException(not_found()));
        let decrypt_rule = mock!(aws_sdk_kms::Client::decrypt).then_error(|| {
            DecryptError::DisabledException(
                DisabledException::builder()
                    .message("test key disabled")
                    .build(),
            )
        });
        let generate_rule = mock!(aws_sdk_kms::Client::generate_data_key)
            .then_error(move || GenerateDataKeyError::NotFoundException(not_found()));
        let client = mock_client!(aws_sdk_kms, RuleMode::Sequential, &[
            &encrypt_rule,
            &decrypt_rule,
            &generate_rule
        ]);
        let kms = test_client(client);

        let error = kms.wrap_key(b"key", KEY_ID).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unexpected);
        assert!(std::error::Error::source(&error).is_some());
        assert_eq!(
            error.message(),
            format!(
                "AWS KMS Encrypt failed for key '{KEY_ID}': NotFoundException: test key not found"
            )
        );

        let error = kms.unwrap_key(b"wrapped", KEY_ID).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unexpected);
        assert!(
            error
                .message()
                .starts_with(&format!("AWS KMS Decrypt failed for key '{KEY_ID}': ")),
            "{error}"
        );
        assert!(error.message().contains("test key disabled"), "{error}");

        let error = kms.generate_key(KEY_ID).await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::Unexpected);
        assert!(
            error.message().starts_with(&format!(
                "AWS KMS GenerateDataKey failed for key '{KEY_ID}': "
            )),
            "{error}"
        );
        assert!(error.message().contains("test key not found"), "{error}");
    }

    #[test]
    fn test_request_failure_describes_transport_errors() {
        let error = SdkError::<DecryptError, String>::dispatch_failure(ConnectorError::other(
            "an error occurred while loading credentials".into(),
            None,
        ));

        let error = AwsKeyManagementClient::request_failed("Decrypt", KEY_ID, None, error);

        // Nested transport errors are part of the message.
        assert!(
            error
                .message()
                .ends_with(": an error occurred while loading credentials"),
            "{error}"
        );
    }

    #[test]
    fn test_request_failure_redacts_plaintext_key() {
        let plaintext = b"0123456789abcdef";
        let encoded = BASE64_STANDARD.encode(plaintext);
        let error = SdkError::<EncryptError, String>::dispatch_failure(ConnectorError::other(
            format!(r#"failed to apply endpoint to request {{"Plaintext":"{encoded}"}}"#).into(),
            None,
        ));

        let error =
            AwsKeyManagementClient::request_failed("Encrypt", KEY_ID, Some(plaintext), error);

        assert!(error.message().contains("<redacted>"), "{error}");
        for output in [
            error.to_string(),
            format!("{error:?}"),
            format!("{error:#?}"),
        ] {
            assert!(!output.contains(&encoded), "{output}");
        }
    }

    #[test]
    fn test_request_failure_includes_unmodeled_error_code_and_message() {
        let error = SdkError::<DecryptError, String>::service_error(
            DecryptError::generic(
                ErrorMetadata::builder()
                    .code("AccessDeniedException")
                    .message("User is not authorized to perform: kms:Decrypt")
                    .build(),
            ),
            String::new(),
        );

        let error = AwsKeyManagementClient::request_failed("Decrypt", KEY_ID, None, error);

        assert!(error.message().contains("AccessDeniedException"), "{error}");
        assert!(
            error
                .message()
                .ends_with(": User is not authorized to perform: kms:Decrypt"),
            "{error}"
        );
    }

    #[test]
    fn test_request_failure_drops_raw_response() {
        let error = SdkError::service_error(
            DecryptError::NotFoundException(NotFoundException::builder().build()),
            r#"{"Plaintext":"SECRET"}"#.to_string(),
        );

        let error = AwsKeyManagementClient::request_failed("Decrypt", KEY_ID, None, error);

        let debug = format!("{error:#?}");
        assert!(debug.contains("NotFoundException"), "{debug}");
        assert!(!debug.contains("SECRET"), "{debug}");
    }

    #[tokio::test]
    async fn test_reject_missing_or_empty_response_fields() {
        let encrypt_missing =
            mock!(aws_sdk_kms::Client::encrypt).then_output(|| EncryptOutput::builder().build());
        let encrypt_empty = mock!(aws_sdk_kms::Client::encrypt).then_output(|| {
            EncryptOutput::builder()
                .ciphertext_blob(Blob::new(Vec::new()))
                .build()
        });
        let decrypt_missing =
            mock!(aws_sdk_kms::Client::decrypt).then_output(|| DecryptOutput::builder().build());
        let generate_missing_plaintext =
            mock!(aws_sdk_kms::Client::generate_data_key).then_output(|| {
                GenerateDataKeyOutput::builder()
                    .ciphertext_blob(Blob::new(b"wrapped"))
                    .build()
            });
        let generate_missing_ciphertext = mock!(aws_sdk_kms::Client::generate_data_key)
            .then_output(|| {
                GenerateDataKeyOutput::builder()
                    .plaintext(Blob::new([0; 32]))
                    .build()
            });
        let generate_empty_ciphertext =
            mock!(aws_sdk_kms::Client::generate_data_key).then_output(|| {
                GenerateDataKeyOutput::builder()
                    .plaintext(Blob::new([0; 32]))
                    .ciphertext_blob(Blob::new(Vec::new()))
                    .build()
            });
        let client = mock_client!(aws_sdk_kms, RuleMode::Sequential, &[
            &encrypt_missing,
            &encrypt_empty,
            &decrypt_missing,
            &generate_missing_plaintext,
            &generate_missing_ciphertext,
            &generate_empty_ciphertext
        ]);
        let kms = test_client(client);

        let errors = [
            kms.wrap_key(b"key", KEY_ID).await.unwrap_err(),
            kms.wrap_key(b"key", KEY_ID).await.unwrap_err(),
            kms.unwrap_key(b"wrapped", KEY_ID).await.unwrap_err(),
            kms.generate_key(KEY_ID).await.err().unwrap(),
            kms.generate_key(KEY_ID).await.err().unwrap(),
            kms.generate_key(KEY_ID).await.err().unwrap(),
        ];
        let expected = [
            "AWS KMS Encrypt response did not contain ciphertext_blob",
            "AWS KMS Encrypt response did not contain ciphertext_blob",
            "AWS KMS Decrypt response did not contain plaintext",
            "AWS KMS GenerateDataKey response did not contain plaintext",
            "AWS KMS GenerateDataKey response did not contain ciphertext_blob",
            "AWS KMS GenerateDataKey response did not contain ciphertext_blob",
        ];
        for (error, expected) in errors.iter().zip(expected) {
            assert_eq!(error.kind(), ErrorKind::Unexpected);
            assert_eq!(error.message(), expected);
        }
    }

    #[tokio::test]
    async fn test_reject_generated_key_not_matching_key_spec() {
        for (data_key_spec, length, expected) in [
            (
                DataKeySpec::Aes256,
                24,
                "a 24-byte key for key spec AES_256, expected 32 bytes",
            ),
            (
                DataKeySpec::Aes128,
                32,
                "a 32-byte key for key spec AES_128, expected 16 bytes",
            ),
        ] {
            let rule = mock!(aws_sdk_kms::Client::generate_data_key).then_output(move || {
                GenerateDataKeyOutput::builder()
                    .plaintext(Blob::new(vec![0; length]))
                    .ciphertext_blob(Blob::new(b"wrapped"))
                    .build()
            });
            let client = mock_client!(aws_sdk_kms, &[&rule]);
            let kms = AwsKeyManagementClient::new(
                client,
                EncryptionAlgorithmSpec::SymmetricDefault,
                data_key_spec,
            );

            let error = kms.generate_key(KEY_ID).await.err().unwrap();

            assert_eq!(error.kind(), ErrorKind::Unexpected);
            assert_eq!(
                error.message(),
                format!("AWS KMS GenerateDataKey returned {expected}")
            );
        }
    }

    #[test]
    fn test_request_failure_does_not_repeat_aws_message() {
        // Errors parsed from AWS responses carry the message in their metadata
        // as well as in the modeled exception.
        let error = SdkError::<EncryptError, String>::service_error(
            EncryptError::NotFoundException(
                NotFoundException::builder()
                    .message("test key not found")
                    .meta(
                        ErrorMetadata::builder()
                            .code("NotFoundException")
                            .message("test key not found")
                            .build(),
                    )
                    .build(),
            ),
            String::new(),
        );

        let error = AwsKeyManagementClient::request_failed("Encrypt", KEY_ID, None, error);

        assert_eq!(
            error.message().matches("test key not found").count(),
            1,
            "{error}"
        );
    }

    #[tokio::test]
    async fn test_symmetric_manager_roundtrip_uses_generate_data_key() {
        let plaintext_kek = [7; 32];
        let wrapped_kek = b"wrapped-symmetric-kek";
        let generate_rule = mock!(aws_sdk_kms::Client::generate_data_key)
            .match_requests(|request| request.key_id() == Some(KEY_ID))
            .then_output(move || {
                GenerateDataKeyOutput::builder()
                    .plaintext(Blob::new(plaintext_kek))
                    .ciphertext_blob(Blob::new(wrapped_kek))
                    .build()
            });
        let decrypt_rule = mock!(aws_sdk_kms::Client::decrypt)
            .match_requests(move |request| {
                request.key_id() == Some(KEY_ID)
                    && request.ciphertext_blob().map(AsRef::as_ref) == Some(wrapped_kek.as_slice())
            })
            .then_output(move || {
                DecryptOutput::builder()
                    .plaintext(Blob::new(plaintext_kek))
                    .build()
            });
        let client = mock_client!(aws_sdk_kms, &[&generate_rule, &decrypt_rule]);
        let kms: Arc<dyn KeyManagementClient> = Arc::new(test_client(client));
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
        assert_eq!(generate_rule.num_calls(), 1);
        assert_eq!(decrypt_rule.num_calls(), 1);
    }

    fn test_client(client: aws_sdk_kms::Client) -> AwsKeyManagementClient {
        AwsKeyManagementClient::new(
            client,
            EncryptionAlgorithmSpec::SymmetricDefault,
            DataKeySpec::Aes256,
        )
    }
}
