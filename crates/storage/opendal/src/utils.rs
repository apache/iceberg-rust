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

use cfg_if::cfg_if;
use iceberg::io::StorageCredential;

cfg_if! {
    if #[cfg(any(feature = "opendal-s3", feature = "opendal-gcs"))] {
        use std::collections::HashMap;
        use std::sync::Arc;

        use iceberg::io::StorageCredentialProvider;
        use reqsign_core::time::Timestamp;
        use reqsign_core::{Error as ReqsignError, Result as ReqsignResult};
    }
}

#[cfg(any(feature = "opendal-s3", feature = "opendal-gcs"))]
pub(crate) fn is_truthy(value: &str) -> bool {
    ["true", "t", "1", "on"].contains(&value.to_lowercase().as_str())
}

/// Convert an opendal error into an iceberg error.
pub(crate) fn from_opendal_error(e: opendal::Error) -> iceberg::Error {
    iceberg::Error::new(
        iceberg::ErrorKind::Unexpected,
        "Failure in doing io operation",
    )
    .with_source(e)
}

/// The non-empty value of `key` in a vended credential's config.
#[cfg(any(feature = "opendal-s3", feature = "opendal-gcs"))]
pub(crate) fn required_credential_property<'a>(
    config: &'a HashMap<String, String>,
    key: &str,
) -> ReqsignResult<&'a str> {
    config
        .get(key)
        .map(String::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ReqsignError::unexpected(format!("vended credential is missing {key}")))
}

/// The latest epoch millisecond `reqsign` timestamps can represent,
/// `9999-12-30T22:00:00Z`.
#[cfg(any(feature = "opendal-s3", feature = "opendal-gcs"))]
const MAX_TIMESTAMP_MILLIS: i64 = 253_402_207_200_000;

/// The epoch-millisecond expiry under `key` in a vended credential's config,
/// if present, as the `reqsign` timestamp backend credentials use. Later
/// expiries, such as `Long.MAX_VALUE` for a credential that never expires,
/// are clamped to the latest representable timestamp.
#[cfg(any(feature = "opendal-s3", feature = "opendal-gcs"))]
pub(crate) fn credential_expiry(
    config: &HashMap<String, String>,
    key: &str,
) -> ReqsignResult<Option<Timestamp>> {
    config
        .get(key)
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .parse::<i64>()
                .ok()
                .and_then(|millis| {
                    Timestamp::from_millisecond(millis.min(MAX_TIMESTAMP_MILLIS)).ok()
                })
                .ok_or_else(|| {
                    ReqsignError::unexpected(format!("vended credential has an invalid {key}"))
                })
        })
        .transpose()
}

/// Validate that a provider's credential covers the location for which the
/// backend requested it.
#[cfg(any(feature = "opendal-s3", feature = "opendal-gcs"))]
pub(crate) fn validate_credential_prefix(
    location: &str,
    credential: &StorageCredential,
) -> ReqsignResult<()> {
    if credential.covers(location) {
        Ok(())
    } else {
        Err(ReqsignError::unexpected(uncovered_location_message(
            location, credential,
        )))
    }
}

/// The error message for a vended credential that does not cover `location`.
pub(crate) fn uncovered_location_message(location: &str, credential: &StorageCredential) -> String {
    format!(
        "vended credential prefix {:?} does not cover storage location {location:?}",
        credential.prefix()
    )
}

/// The root of the storage location containing `path`, e.g. `s3://bucket/`.
pub(crate) fn storage_root(path: &str) -> iceberg::Result<String> {
    let url = url::Url::parse(path)?;
    Ok(format!("{}://{}/", url.scheme(), url.authority()))
}

/// Loads vended credentials for one storage location on behalf of a backend's
/// `reqsign` credential provider.
#[cfg(any(feature = "opendal-s3", feature = "opendal-gcs"))]
pub(crate) struct VendedCredentialSource {
    provider: Arc<dyn StorageCredentialProvider>,
    /// Location handed to the provider: the path an operator serves, or the
    /// scope shared by a bulk-delete batch.
    location: String,
}

#[cfg(any(feature = "opendal-s3", feature = "opendal-gcs"))]
impl VendedCredentialSource {
    pub(crate) fn new(provider: Arc<dyn StorageCredentialProvider>, location: String) -> Self {
        Self { provider, location }
    }

    /// Load a credential covering the location and convert its config with
    /// `extract` into the backend's credential.
    ///
    /// Errors are logged here: reqsign's credential chain replaces them with an
    /// error that only says no credential could be loaded, and reports the
    /// cause only through the `log` crate.
    pub(crate) async fn load<T>(
        &self,
        backend: &str,
        extract: impl FnOnce(&HashMap<String, String>) -> ReqsignResult<T>,
    ) -> ReqsignResult<T> {
        self.try_load(backend, extract).await.inspect_err(|error| {
            tracing::warn!(
                "cannot use vended {backend} credentials for {}: {error}",
                self.location
            )
        })
    }

    async fn try_load<T>(
        &self,
        backend: &str,
        extract: impl FnOnce(&HashMap<String, String>) -> ReqsignResult<T>,
    ) -> ReqsignResult<T> {
        let credential = self
            .provider
            .load_credential(&self.location)
            .await
            .map_err(|e| {
                ReqsignError::unexpected(format!(
                    "failed to load vended {backend} credential for {}: {e}",
                    self.location
                ))
                .with_source(e)
            })?;
        validate_credential_prefix(&self.location, &credential)?;
        extract(credential.config())
    }
}

#[cfg(any(feature = "opendal-s3", feature = "opendal-gcs"))]
impl std::fmt::Debug for VendedCredentialSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VendedCredentialSource")
            .field("location", &self.location)
            .finish_non_exhaustive()
    }
}

#[cfg(all(test, any(feature = "opendal-s3", feature = "opendal-gcs")))]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use iceberg::io::{GCS_TOKEN, GCS_TOKEN_EXPIRES_AT};

    use super::*;

    /// Returns a credential scoped to `prefix` and records requested locations.
    #[derive(Debug)]
    struct RecordingProvider {
        prefix: &'static str,
        requested: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl StorageCredentialProvider for RecordingProvider {
        fn supports_path(&self, _path: &str) -> bool {
            true
        }

        async fn load_credential(&self, path: &str) -> iceberg::Result<StorageCredential> {
            self.requested.lock().unwrap().push(path.to_string());
            Ok(StorageCredential::new(
                self.prefix,
                HashMap::from([(GCS_TOKEN.to_string(), "token".to_string())]),
            ))
        }
    }

    async fn load(prefix: &'static str, location: &str) -> ReqsignResult<()> {
        let provider = Arc::new(RecordingProvider {
            prefix,
            requested: Mutex::new(Vec::new()),
        });
        let source = VendedCredentialSource::new(provider.clone(), location.to_string());
        let result = source
            .load("GCS", |config| {
                required_credential_property(config, GCS_TOKEN).map(|_| ())
            })
            .await;
        assert_eq!(*provider.requested.lock().unwrap(), vec![location]);
        result
    }

    #[tokio::test]
    async fn vended_source_requires_a_covering_credential() {
        let location = "gs://bucket/table/data/file.parquet";
        assert!(load("gs", location).await.is_ok());
        assert!(load("gs://bucket/table", location).await.is_ok());
        assert!(load("gs://bucket/tab", location).await.is_ok());
        assert!(
            load("gs://bucket/table/data/other", location)
                .await
                .is_err()
        );
        assert!(load("gcs://bucket", location).await.is_err());
        assert!(load("", location).await.is_err());
    }

    #[test]
    fn credential_properties_are_parsed() {
        let config = HashMap::from([
            (GCS_TOKEN.to_string(), "token".to_string()),
            (GCS_TOKEN_EXPIRES_AT.to_string(), "1500".to_string()),
        ]);
        assert_eq!(
            required_credential_property(&config, GCS_TOKEN).unwrap(),
            "token"
        );
        assert!(required_credential_property(&config, "missing").is_err());
        assert_eq!(
            credential_expiry(&config, GCS_TOKEN_EXPIRES_AT).unwrap(),
            Some(Timestamp::from_millisecond(1500).unwrap())
        );
        assert_eq!(credential_expiry(&config, "missing").unwrap(), None);
        // An expiry beyond year 9999, such as Java's `Long.MAX_VALUE`, is
        // clamped rather than rejected.
        let never = HashMap::from([(GCS_TOKEN_EXPIRES_AT.to_string(), i64::MAX.to_string())]);
        assert_eq!(
            credential_expiry(&never, GCS_TOKEN_EXPIRES_AT).unwrap(),
            Some(Timestamp::from_millisecond(MAX_TIMESTAMP_MILLIS).unwrap())
        );
        assert!(Timestamp::from_millisecond(MAX_TIMESTAMP_MILLIS + 1).is_err());
        // The value is never quoted in the error.
        let invalid = HashMap::from([(GCS_TOKEN_EXPIRES_AT.to_string(), "secret".to_string())]);
        let error = credential_expiry(&invalid, GCS_TOKEN_EXPIRES_AT)
            .unwrap_err()
            .to_string();
        assert!(!error.contains("secret"), "{error}");
    }

    #[test]
    fn storage_root_keeps_scheme_and_authority() {
        assert_eq!(
            storage_root("s3a://bucket/table/file.parquet").unwrap(),
            "s3a://bucket/"
        );
        assert_eq!(
            storage_root("abfss://fs@account.dfs.core.windows.net/table/file.parquet").unwrap(),
            "abfss://fs@account.dfs.core.windows.net/"
        );
        assert!(storage_root("not a url").is_err());
    }

    #[derive(Debug)]
    struct FailingProvider;

    #[async_trait]
    impl StorageCredentialProvider for FailingProvider {
        fn supports_path(&self, _path: &str) -> bool {
            true
        }

        async fn load_credential(&self, _path: &str) -> iceberg::Result<StorageCredential> {
            Err(iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "catalog returned 503",
            ))
        }
    }

    #[tokio::test]
    async fn vended_source_errors_carry_the_cause() {
        let source = VendedCredentialSource::new(
            Arc::new(FailingProvider),
            "gs://bucket/file.parquet".to_string(),
        );
        let error = source
            .load("GCS", |_| Ok(()))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("catalog returned 503"), "{error}");
    }
}
