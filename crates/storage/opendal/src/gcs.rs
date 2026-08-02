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
    GCS_NO_AUTH, GCS_SERVICE_PATH, GCS_TOKEN, StorageCredentialKind, StorageCredentialProvider,
};
use iceberg::{Error, ErrorKind, Result};
use opendal::services::GcsConfig;
use opendal::{Configurator, Operator};
use reqsign_core::{Context, Error as ReqsignError, ProvideCredential, Result as ReqsignResult};
use reqsign_google::{Credential as GoogleCredential, Token as GoogleToken};
use url::Url;

use crate::utils::{
    from_opendal_error, is_truthy, system_time_to_timestamp, validate_credential_prefix,
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

    if let Some(endpoint) = m.remove(GCS_SERVICE_PATH) {
        cfg.endpoint = Some(endpoint);
    }

    if let Some(no_auth) = m.remove(GCS_NO_AUTH)
        && is_truthy(no_auth.to_lowercase().as_str())
    {
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
) -> Result<Operator> {
    let url = Url::parse(path)?;
    if !matches!(url.scheme(), "gs" | "gcs") {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid gcs url: {path}, expected gs:// or gcs://"),
        ));
    }
    let bucket = url.host_str().ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid gcs url: {path}, bucket is required"),
        )
    })?;

    let mut cfg = cfg.clone();
    cfg.bucket = bucket.to_string();

    // When a catalog-supplied provider is present, make it the sole credential
    // source. `reqsign_google` only prepends a custom provider (unlike S3, which
    // replaces the chain) and its chain continues to the next provider on error,
    // so without this a failed refresh would silently fall back to the stale seed
    // token or ambient GCP credentials.
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
        cfg.token = None;
        cfg.credential = None;
        cfg.disable_vm_metadata = true;
        cfg.disable_config_load = true;
    }

    let mut builder = cfg.into_builder();

    // A catalog-supplied provider re-fetches the vended OAuth2 token as it nears expiry
    if let Some(provider) = credential_provider {
        builder = builder.credential_provider(VendedGcsCredentialProvider::new(
            Arc::clone(provider),
            path.to_string(),
        ));
    }

    Ok(Operator::new(builder).map_err(from_opendal_error)?.finish())
}

/// Adapts a generic [`StorageCredentialProvider`] into a `reqsign`
/// [`ProvideCredential`], so the GCS signer can obtain and refresh vended OAuth2
/// tokens.
struct VendedGcsCredentialProvider {
    provider: Arc<dyn StorageCredentialProvider>,
    /// Absolute path this operator serves: handed back to the provider so it can
    /// select the vended credential whose prefix best matches the location.
    path: String,
}

impl std::fmt::Debug for VendedGcsCredentialProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VendedGcsCredentialProvider")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl VendedGcsCredentialProvider {
    fn new(provider: Arc<dyn StorageCredentialProvider>, path: String) -> Self {
        Self { provider, path }
    }
}

impl ProvideCredential for VendedGcsCredentialProvider {
    type Credential = GoogleCredential;

    async fn provide_credential(&self, _ctx: &Context) -> ReqsignResult<Option<GoogleCredential>> {
        let credential = self
            .provider
            .load_credential(&self.path)
            .await
            .map_err(|e| {
                ReqsignError::unexpected(format!(
                    "failed to load vended GCS credential for {}",
                    self.path
                ))
                .with_source(e)
            })?;

        validate_credential_prefix(&self.path, credential.prefix.as_deref())?;

        let expires_at = credential
            .expires_at
            .map(system_time_to_timestamp)
            .transpose()?;
        match credential.kind {
            StorageCredentialKind::Gcs(gcs) => {
                Ok(Some(GoogleCredential::with_token(GoogleToken {
                    access_token: gcs.token,
                    expires_at,
                })))
            }
            _ => Err(ReqsignError::unexpected(
                "GCS storage received a non-GCS credential from the provider",
            )),
        }
    }
}
