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

use iceberg::io::{
    CLIENT_REGION, S3_ACCESS_KEY_ID, S3_ALLOW_ANONYMOUS, S3_ASSUME_ROLE_ARN,
    S3_ASSUME_ROLE_EXTERNAL_ID, S3_ASSUME_ROLE_SESSION_NAME, S3_DISABLE_CONFIG_LOAD,
    S3_DISABLE_EC2_METADATA, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY,
    S3_SESSION_TOKEN, S3_SSE_KEY, S3_SSE_MD5, S3_SSE_TYPE, StorageCredentialKind,
    StorageCredentialProvider,
};
use iceberg::{Error, ErrorKind, Result};
use opendal::services::S3Config;
use opendal::{Configurator, Operator};
/// AWS credentials: access key ID, secret access key, and optional session token.
pub use reqsign_aws_v4::Credential as AwsCredential;
/// Trait for types that can asynchronously supply [`AwsCredential`] to a [`CustomAwsCredentialLoader`].
pub use reqsign_core::ProvideCredential;
use reqsign_core::{
    Context, Error as ReqsignError, ProvideCredentialChain, ProvideCredentialDyn,
    Result as ReqsignResult,
};
use url::Url;

use crate::utils::{
    from_opendal_error, is_truthy, system_time_to_timestamp, validate_credential_prefix,
};

/// Parse iceberg props to s3 config.
pub(crate) fn s3_config_parse(mut m: HashMap<String, String>) -> Result<S3Config> {
    let mut cfg = S3Config::default();
    // Match Iceberg `S3FileIOProperties.PATH_STYLE_ACCESS_DEFAULT = false`:
    // virtual-host-style addressing is the spec default. opendal's own
    // default is path-style, which disagrees with the Java SDK and breaks
    // S3-compatible stores that only accept virtual-hosted-style URLs.
    // Any explicit `s3.path-style-access` property below overrides this.
    cfg.enable_virtual_host_style = true;
    if let Some(endpoint) = m.remove(S3_ENDPOINT) {
        cfg.endpoint = Some(endpoint);
    };
    if let Some(access_key_id) = m.remove(S3_ACCESS_KEY_ID) {
        cfg.access_key_id = Some(access_key_id);
    };
    if let Some(secret_access_key) = m.remove(S3_SECRET_ACCESS_KEY) {
        cfg.secret_access_key = Some(secret_access_key);
    };
    if let Some(session_token) = m.remove(S3_SESSION_TOKEN) {
        cfg.session_token = Some(session_token);
    };
    if let Some(region) = m.remove(S3_REGION) {
        cfg.region = Some(region);
    };
    if let Some(region) = m.remove(CLIENT_REGION) {
        cfg.region = Some(region);
    };
    if let Some(path_style_access) = m.remove(S3_PATH_STYLE_ACCESS) {
        cfg.enable_virtual_host_style = !is_truthy(path_style_access.to_lowercase().as_str());
    };
    if let Some(arn) = m.remove(S3_ASSUME_ROLE_ARN) {
        cfg.role_arn = Some(arn);
    }
    if let Some(external_id) = m.remove(S3_ASSUME_ROLE_EXTERNAL_ID) {
        cfg.external_id = Some(external_id);
    };
    if let Some(session_name) = m.remove(S3_ASSUME_ROLE_SESSION_NAME) {
        cfg.role_session_name = Some(session_name);
    };
    let s3_sse_key = m.remove(S3_SSE_KEY);
    if let Some(sse_type) = m.remove(S3_SSE_TYPE) {
        match sse_type.to_lowercase().as_str() {
            // No Server Side Encryption
            "none" => {}
            // S3 SSE-S3 encryption (S3 managed keys). https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingServerSideEncryption.html
            "s3" => {
                cfg.server_side_encryption = Some("AES256".to_string());
            }
            // S3 SSE KMS, either using default or custom KMS key. https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingKMSEncryption.html
            "kms" => {
                cfg.server_side_encryption = Some("aws:kms".to_string());
                cfg.server_side_encryption_aws_kms_key_id = s3_sse_key;
            }
            // S3 SSE-C, using customer managed keys. https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html
            "custom" => {
                cfg.server_side_encryption_customer_algorithm = Some("AES256".to_string());
                cfg.server_side_encryption_customer_key = s3_sse_key;
                cfg.server_side_encryption_customer_key_md5 = m.remove(S3_SSE_MD5);
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Invalid {S3_SSE_TYPE}: {sse_type}. Expected one of (custom, kms, s3, none)"
                    ),
                ));
            }
        }
    };

    if let Some(allow_anonymous) = m.remove(S3_ALLOW_ANONYMOUS)
        && is_truthy(allow_anonymous.to_lowercase().as_str())
    {
        cfg.skip_signature = true;
    }
    if let Some(disable_ec2_metadata) = m.remove(S3_DISABLE_EC2_METADATA)
        && is_truthy(disable_ec2_metadata.to_lowercase().as_str())
    {
        cfg.disable_ec2_metadata = true;
    };
    if let Some(disable_config_load) = m.remove(S3_DISABLE_CONFIG_LOAD)
        && is_truthy(disable_config_load.to_lowercase().as_str())
    {
        cfg.disable_config_load = true;
    };

    Ok(cfg)
}

/// Build new opendal operator from give path.
pub(crate) fn s3_config_build(
    cfg: &S3Config,
    customized_credential_load: &Option<CustomAwsCredentialLoader>,
    credential_provider: &Option<Arc<dyn StorageCredentialProvider>>,
    path: &str,
) -> Result<Operator> {
    let url = Url::parse(path)?;
    let bucket = url.host_str().ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid s3 url: {path}, missing bucket"),
        )
    })?;

    // Preserve the existing custom-loader precedence: an explicitly configured loader
    // is the sole source, otherwise install the catalog provider as a replacement chain so
    // refresh failures cannot fall through to broader ambient AWS credentials.
    let credential_provider = credential_provider
        .as_ref()
        .filter(|provider| provider.supports_path(path));
    if customized_credential_load.is_none() && credential_provider.is_some() && cfg.skip_signature {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "Invalid S3 auth settings: anonymous access cannot be combined with refreshable credentials",
        ));
    }

    let mut builder = cfg
        .clone()
        .into_builder()
        // Set bucket name.
        .bucket(bucket);

    if let Some(loader) = customized_credential_load {
        let chain = ProvideCredentialChain::new().push(Arc::clone(&loader.0));
        builder = builder.credential_provider_chain(chain);
    } else if let Some(provider) = credential_provider {
        let chain = ProvideCredentialChain::new().push(VendedS3CredentialProvider::new(
            Arc::clone(provider),
            path.to_string(),
        ));
        builder = builder.credential_provider_chain(chain);
    }

    Ok(Operator::new(builder).map_err(from_opendal_error)?.finish())
}

/// Adapts a generic [`StorageCredentialProvider`] into a reqsign
/// [`ProvideCredential`], so the S3 signer can obtain and refresh vended
/// credentials.
struct VendedS3CredentialProvider {
    provider: Arc<dyn StorageCredentialProvider>,
    /// Absolute path this operator serves; handed back to the provider so it can
    /// select the vended credential whose prefix best matches the location.
    path: String,
}

impl std::fmt::Debug for VendedS3CredentialProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VendedS3CredentialProvider")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl VendedS3CredentialProvider {
    fn new(provider: Arc<dyn StorageCredentialProvider>, path: String) -> Self {
        Self { provider, path }
    }
}

impl ProvideCredential for VendedS3CredentialProvider {
    type Credential = AwsCredential;

    async fn provide_credential(&self, _ctx: &Context) -> ReqsignResult<Option<AwsCredential>> {
        let credential = self
            .provider
            .load_credential(&self.path)
            .await
            .map_err(|e| {
                ReqsignError::unexpected(format!(
                    "failed to load vended S3 credential for {}",
                    self.path
                ))
                .with_source(e)
            })?;

        validate_credential_prefix(&self.path, credential.prefix.as_deref())?;

        let expires_in = credential
            .expires_at
            .map(system_time_to_timestamp)
            .transpose()?;
        match credential.kind {
            StorageCredentialKind::S3(s3) => Ok(Some(AwsCredential {
                access_key_id: s3.access_key_id,
                secret_access_key: s3.secret_access_key,
                session_token: s3.session_token,
                expires_in,
            })),
            _ => Err(ReqsignError::unexpected(
                "S3 storage received a non-S3 credential from the provider",
            )),
        }
    }
}

/// Custom AWS credential loader.
///
/// Wraps any [`ProvideCredential`] implementation for use with the S3 storage backend.
/// Use [`CustomAwsCredentialLoader::new`] to create one, then pass it to
/// [`OpenDalStorageFactory::S3`](crate::OpenDalStorageFactory).
pub struct CustomAwsCredentialLoader(Arc<dyn ProvideCredentialDyn<Credential = AwsCredential>>);

impl Clone for CustomAwsCredentialLoader {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl std::fmt::Debug for CustomAwsCredentialLoader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CustomAwsCredentialLoader")
            .finish_non_exhaustive()
    }
}

impl CustomAwsCredentialLoader {
    /// Create a new custom AWS credential loader from any [`ProvideCredential`] implementation.
    pub fn new(provider: impl ProvideCredential<Credential = AwsCredential> + 'static) -> Self {
        Self(Arc::new(provider))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::io::S3_PATH_STYLE_ACCESS;

    use super::s3_config_parse;

    fn parse_with(prop: Option<&str>) -> bool {
        let mut props = HashMap::new();
        if let Some(v) = prop {
            props.insert(S3_PATH_STYLE_ACCESS.to_string(), v.to_string());
        }
        s3_config_parse(props).unwrap().enable_virtual_host_style
    }

    #[test]
    fn s3_config_parse_path_style_access() {
        // Match Iceberg S3FileIOProperties.PATH_STYLE_ACCESS_DEFAULT = false.
        assert!(parse_with(None));
        assert!(parse_with(Some("false")));
        assert!(!parse_with(Some("true")));
    }
}
