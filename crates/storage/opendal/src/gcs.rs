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
//! Google Cloud Storage properties

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::io::{
    GCS_ALLOW_ANONYMOUS, GCS_CREDENTIALS_JSON, GCS_DISABLE_CONFIG_LOAD, GCS_DISABLE_VM_METADATA,
    GCS_NO_AUTH, GCS_SERVICE_HOST, GCS_TOKEN, GCS_TOKEN_EXPIRES_AT, StorageCredentialProvider,
};
use iceberg::{Error, ErrorKind, Result};
use opendal::services::GcsConfig;
use opendal::{Configurator, Operator};
use reqsign_core::{Context, ProvideCredential, Result as ReqsignResult};
use reqsign_google::{Credential as GoogleCredential, Token as GoogleToken};
use url::Url;

use crate::utils::{
    VendedCredentialSource, credential_expiry, from_opendal_error, is_truthy,
    required_credential_property,
};

/// Parse iceberg properties to [`GcsConfig`].
pub(crate) fn gcs_config_parse(mut m: HashMap<String, String>) -> Result<GcsConfig> {
    let mut cfg = GcsConfig::default();

    if let Some(cred) = m.remove(GCS_CREDENTIALS_JSON) {
        cfg.credential = Some(cred);
    }

    if let Some(token) = m.remove(GCS_TOKEN) {
        cfg.token = Some(token);
    }

    if let Some(endpoint) = m.remove(GCS_SERVICE_HOST) {
        cfg.endpoint = Some(endpoint);
    }

    if m.remove(GCS_NO_AUTH).is_some() {
        cfg.skip_signature = true;
        cfg.disable_vm_metadata = true;
        cfg.disable_config_load = true;
    }

    if let Some(allow_anonymous) = m.remove(GCS_ALLOW_ANONYMOUS)
        && is_truthy(allow_anonymous.to_lowercase().as_str())
    {
        cfg.skip_signature = true;
    }
    if let Some(disable_ec2_metadata) = m.remove(GCS_DISABLE_VM_METADATA)
        && is_truthy(disable_ec2_metadata.to_lowercase().as_str())
    {
        cfg.disable_vm_metadata = true;
    };
    if let Some(disable_config_load) = m.remove(GCS_DISABLE_CONFIG_LOAD)
        && is_truthy(disable_config_load.to_lowercase().as_str())
    {
        cfg.disable_config_load = true;
    };

    Ok(cfg)
}

/// Build a new OpenDAL [`Operator`] based on a provided [`GcsConfig`].
pub(crate) fn gcs_config_build(
    cfg: &GcsConfig,
    credential_provider: &Option<Arc<dyn StorageCredentialProvider>>,
    path: &str,
    credential_location: Option<&str>,
) -> Result<Operator> {
    let url = Url::parse(path)?;
    let bucket = url.host_str().ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid gcs url: {path}, bucket is required"),
        )
    })?;

    let mut cfg = cfg.clone();
    cfg.bucket = bucket.to_string();

    let credential_provider = credential_provider
        .as_ref()
        .filter(|provider| provider.supports_path(path));
    if credential_provider.is_some() {
        if cfg.skip_signature {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Invalid GCS auth settings: anonymous access cannot be combined with refreshable credentials",
            ));
        }
        disable_other_credential_sources(&mut cfg);
    }

    let mut builder = cfg.into_builder();

    // The provider re-fetches the vended OAuth2 token as it nears expiry.
    if let Some(provider) = credential_provider {
        builder =
            builder.credential_provider(VendedGcsCredentialProvider(VendedCredentialSource::new(
                Arc::clone(provider),
                credential_location.unwrap_or(path).to_string(),
            )));
    }

    Operator::new(builder).map_err(from_opendal_error)
}

/// Disable every configured and ambient credential source but a vended one.
///
/// `reqsign_google` continues to the next provider even when a provider returns
/// an error, and OpenDAL prepends custom providers to its default chain. With
/// the other sources disabled, the vended provider is effectively the sole
/// source, and refresh failures cannot silently fall back.
fn disable_other_credential_sources(cfg: &mut GcsConfig) {
    cfg.token = None;
    cfg.credential = None;
    cfg.credential_path = None;
    cfg.service_account = None;
    cfg.disable_vm_metadata = true;
    cfg.disable_config_load = true;
}

/// Adapts a generic [`StorageCredentialProvider`] into a `reqsign`
/// [`ProvideCredential`], so the GCS signer can obtain and refresh vended OAuth2
/// tokens.
#[derive(Debug)]
struct VendedGcsCredentialProvider(VendedCredentialSource);

impl ProvideCredential for VendedGcsCredentialProvider {
    type Credential = GoogleCredential;

    async fn provide_credential(&self, _ctx: &Context) -> ReqsignResult<Option<GoogleCredential>> {
        let token = self
            .0
            .load("GCS", |config| {
                Ok(GoogleToken {
                    access_token: required_credential_property(config, GCS_TOKEN)?.to_string(),
                    expires_at: credential_expiry(config, GCS_TOKEN_EXPIRES_AT)?,
                })
            })
            .await?;
        Ok(Some(GoogleCredential::with_token(token)))
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use iceberg::io::{S3_ACCESS_KEY_ID, StorageCredential};
    use reqsign_core::time::Timestamp;

    use super::*;

    #[derive(Debug)]
    struct FixedProvider(StorageCredential);

    #[async_trait]
    impl StorageCredentialProvider for FixedProvider {
        fn supports_path(&self, _path: &str) -> bool {
            true
        }

        async fn load_credential(&self, _path: &str) -> Result<StorageCredential> {
            Ok(self.0.clone())
        }
    }

    fn adapter(credential: StorageCredential) -> VendedGcsCredentialProvider {
        VendedGcsCredentialProvider(VendedCredentialSource::new(
            Arc::new(FixedProvider(credential)),
            "gs://bucket/table/file.parquet".to_string(),
        ))
    }

    fn credential(config: &[(&str, &str)]) -> StorageCredential {
        StorageCredential::new(
            "gs://bucket/table",
            config
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
        )
    }

    #[tokio::test]
    async fn test_vended_adapter_returns_expiring_oauth2_token() {
        let credential = credential(&[(GCS_TOKEN, "ya29.token"), (GCS_TOKEN_EXPIRES_AT, "1500")]);

        let google = adapter(credential)
            .provide_credential(&Context::new())
            .await
            .unwrap()
            .unwrap();
        let token = google.token.expect("a token credential");
        assert_eq!(token.access_token, "ya29.token");
        assert_eq!(
            token.expires_at,
            Some(Timestamp::from_millisecond(1500).unwrap())
        );
    }

    #[tokio::test]
    async fn test_vended_adapter_rejects_other_credentials() {
        assert!(
            adapter(credential(&[(S3_ACCESS_KEY_ID, "AK")]))
                .provide_credential(&Context::new())
                .await
                .is_err()
        );
    }

    #[test]
    fn test_vended_credentials_disable_every_other_source() {
        let mut cfg = gcs_config_parse(HashMap::from([
            (GCS_TOKEN.to_string(), "static-token".to_string()),
            (GCS_CREDENTIALS_JSON.to_string(), "e30=".to_string()),
        ]))
        .unwrap();
        cfg.credential_path = Some("/path/to/credentials.json".to_string());
        cfg.service_account = Some("account@example.com".to_string());

        disable_other_credential_sources(&mut cfg);

        assert_eq!(cfg.token, None);
        assert_eq!(cfg.credential, None);
        assert_eq!(cfg.credential_path, None);
        assert_eq!(cfg.service_account, None);
        assert!(cfg.disable_vm_metadata);
        assert!(cfg.disable_config_load);
    }
}
