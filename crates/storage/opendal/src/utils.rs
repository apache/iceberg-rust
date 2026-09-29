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

/// Convert a [`SystemTime`](std::time::SystemTime) credential expiry into the
/// `reqsign` [`Timestamp`](reqsign_core::time::Timestamp) used on backend
/// credential types (e.g. `AwsCredential::expires_in`, `google::Token::expires_at`).
#[cfg(any(
    feature = "opendal-s3",
    feature = "opendal-gcs",
    feature = "opendal-azdls"
))]
pub(crate) fn system_time_to_timestamp(
    time: std::time::SystemTime,
) -> reqsign_core::Result<reqsign_core::time::Timestamp> {
    let millis = time
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| {
            reqsign_core::Error::unexpected(format!(
                "credential expiry precedes the UNIX epoch: {e}"
            ))
        })?
        .as_millis();
    let millis = i64::try_from(millis).map_err(|_| {
        reqsign_core::Error::unexpected("credential expiry overflows i64 milliseconds")
    })?;
    reqsign_core::time::Timestamp::from_millisecond(millis)
        .map_err(|e| reqsign_core::Error::unexpected(format!("invalid credential expiry: {e}")))
}

/// Validate that a provider's credential covers the location for which the
/// backend requested it.
#[cfg(any(
    feature = "opendal-s3",
    feature = "opendal-gcs",
    feature = "opendal-azdls"
))]
pub(crate) fn validate_credential_prefix(
    location: &str,
    credential: &iceberg::io::StorageCredential,
) -> reqsign_core::Result<()> {
    if credential.covers(location) {
        Ok(())
    } else {
        Err(reqsign_core::Error::unexpected(format!(
            "vended credential prefix {:?} does not cover storage location {location:?}",
            credential.prefix()
        )))
    }
}

/// The root of the storage location containing `path`, e.g. `s3://bucket/`.
pub(crate) fn storage_root(path: &str) -> iceberg::Result<String> {
    let url = url::Url::parse(path)?;
    Ok(format!("{}://{}/", url.scheme(), url.authority()))
}

/// Loads vended credentials for one storage location on behalf of a backend's
/// `reqsign` credential provider.
#[cfg(any(
    feature = "opendal-s3",
    feature = "opendal-gcs",
    feature = "opendal-azdls"
))]
pub(crate) struct VendedCredentialSource {
    provider: std::sync::Arc<dyn iceberg::io::StorageCredentialProvider>,
    /// Location handed to the provider: the path an operator serves, or the
    /// scope shared by a bulk-delete batch.
    location: String,
}

#[cfg(any(
    feature = "opendal-s3",
    feature = "opendal-gcs",
    feature = "opendal-azdls"
))]
impl VendedCredentialSource {
    pub(crate) fn new(
        provider: std::sync::Arc<dyn iceberg::io::StorageCredentialProvider>,
        location: String,
    ) -> Self {
        Self { provider, location }
    }

    /// Load a credential covering the location, returning its backend-specific
    /// material and expiry.
    pub(crate) async fn load(
        &self,
        backend: &str,
    ) -> reqsign_core::Result<(
        iceberg::io::StorageCredentialKind,
        Option<reqsign_core::time::Timestamp>,
    )> {
        let credential = self
            .provider
            .load_credential(&self.location)
            .await
            .map_err(|e| {
                reqsign_core::Error::unexpected(format!(
                    "failed to load vended {backend} credential for {}",
                    self.location
                ))
                .with_source(e)
            })?;
        validate_credential_prefix(&self.location, &credential)?;
        let expires_at = credential
            .expires_at()
            .map(system_time_to_timestamp)
            .transpose()?;
        Ok((credential.into_kind(), expires_at))
    }
}

#[cfg(any(
    feature = "opendal-s3",
    feature = "opendal-gcs",
    feature = "opendal-azdls"
))]
impl std::fmt::Debug for VendedCredentialSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VendedCredentialSource")
            .field("location", &self.location)
            .finish_non_exhaustive()
    }
}

#[cfg(all(
    test,
    any(
        feature = "opendal-s3",
        feature = "opendal-gcs",
        feature = "opendal-azdls"
    )
))]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use iceberg::io::{
        GcsCredential, StorageCredential, StorageCredentialKind, StorageCredentialProvider,
    };

    use super::*;

    /// Returns a credential scoped to `prefix` and records requested locations.
    #[derive(Debug)]
    struct RecordingProvider {
        prefix: Option<&'static str>,
        requested: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl StorageCredentialProvider for RecordingProvider {
        async fn load_credential(&self, path: &str) -> iceberg::Result<StorageCredential> {
            self.requested.lock().unwrap().push(path.to_string());
            let credential =
                StorageCredential::new(StorageCredentialKind::Gcs(GcsCredential::new("token")));
            Ok(match self.prefix {
                Some(prefix) => credential.with_prefix(prefix),
                None => credential,
            })
        }
    }

    async fn load(prefix: Option<&'static str>, location: &str) -> reqsign_core::Result<()> {
        let provider = Arc::new(RecordingProvider {
            prefix,
            requested: Mutex::new(Vec::new()),
        });
        let source = VendedCredentialSource::new(provider.clone(), location.to_string());
        let result = source.load("GCS").await.map(|_| ());
        assert_eq!(*provider.requested.lock().unwrap(), vec![location]);
        result
    }

    #[tokio::test]
    async fn vended_source_requires_a_covering_credential() {
        let location = "gs://bucket/table/data/file.parquet";
        assert!(load(None, location).await.is_ok());
        assert!(load(Some("gs://bucket/table"), location).await.is_ok());
        assert!(load(Some("gcs://bucket"), location).await.is_ok());
        assert!(
            load(Some("gs://bucket/table/data/other"), location)
                .await
                .is_err()
        );
        assert!(load(Some("gs://bucket/tab"), location).await.is_err());
        assert!(load(Some(""), location).await.is_err());
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
}
