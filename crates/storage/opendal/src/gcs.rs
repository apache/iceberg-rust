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
    GCS_NO_AUTH, GCS_SERVICE_HOST, GCS_TOKEN, StorageCredentialKind, StorageCredentialProvider,
};
use iceberg::{Error, ErrorKind, Result};
use opendal::services::GcsConfig;
use opendal::{Configurator, Operator};
use reqsign_core::{Context, Error as ReqsignError, ProvideCredential, Result as ReqsignResult};
use reqsign_google::{Credential as GoogleCredential, Token as GoogleToken};
use url::Url;

use crate::utils::{VendedCredentialSource, from_opendal_error, is_truthy};

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

    if let Some(no_auth) = m.remove(GCS_NO_AUTH)
        && is_truthy(&no_auth)
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
    credential_location: Option<&str>,
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

    // `reqsign_google` continues to the next provider even when a provider returns
    // an error, and OpenDAL prepends custom providers to its default chain. Disable
    // every other configured and ambient source so the catalog provider is
    // effectively the sole source and refresh failures cannot silently fall back.
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
        cfg.credential_path = None;
        cfg.service_account = None;
        cfg.disable_vm_metadata = true;
        cfg.disable_config_load = true;
    }

    let mut builder = cfg.into_builder();

    // A catalog-supplied provider re-fetches the vended OAuth2 token as it nears expiry
    if let Some(provider) = credential_provider {
        builder =
            builder.credential_provider(VendedGcsCredentialProvider(VendedCredentialSource::new(
                Arc::clone(provider),
                credential_location.unwrap_or(path).to_string(),
            )));
    }

    Operator::new(builder).map_err(from_opendal_error)
}

/// Adapts a generic [`StorageCredentialProvider`] into a `reqsign`
/// [`ProvideCredential`], so the GCS signer can obtain and refresh vended OAuth2
/// tokens.
#[derive(Debug)]
struct VendedGcsCredentialProvider(VendedCredentialSource);

impl ProvideCredential for VendedGcsCredentialProvider {
    type Credential = GoogleCredential;

    async fn provide_credential(&self, _ctx: &Context) -> ReqsignResult<Option<GoogleCredential>> {
        match self.0.load("GCS").await? {
            (StorageCredentialKind::Gcs(gcs), expires_at) => {
                Ok(Some(GoogleCredential::with_token(GoogleToken {
                    access_token: gcs.into_token(),
                    expires_at,
                })))
            }
            _ => Err(ReqsignError::unexpected(
                "GCS storage received a non-GCS credential from the provider",
            )),
        }
    }
}
