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
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;

use iceberg::io::{
    ADLS_ACCOUNT_KEY, ADLS_ACCOUNT_NAME, ADLS_AUTHORITY_HOST, ADLS_CLIENT_ID, ADLS_CLIENT_SECRET,
    ADLS_CONNECTION_STRING, ADLS_SAS_TOKEN, ADLS_SAS_TOKEN_PREFIX, ADLS_TENANT_ID,
    StorageCredentialKind, StorageCredentialProvider,
};
use iceberg::{Error, ErrorKind, Result};
use opendal::Configurator;
use opendal::services::AzdlsConfig;
use reqsign_azure_storage::Credential as AzureCredential;
use reqsign_core::{
    Context, Error as ReqsignError, ProvideCredential, ProvideCredentialChain,
    Result as ReqsignResult,
};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::DynamicCredentialScope;
use crate::utils::{from_opendal_error, system_time_to_timestamp, validate_credential_prefix};

/// Local version of `ensure_data_valid` macro since the iceberg crate's macro
/// uses `$crate::error::Error` paths that don't resolve from external crates
/// (the `error` module is private).
macro_rules! ensure_data_valid {
    ($cond:expr, $fmt:literal, $($arg:tt)*) => {
        if !$cond {
            return Err(Error::new(ErrorKind::DataInvalid, format!($fmt, $($arg)*)));
        }
    };
}

/// Parses adls.* prefixed configuration properties.
pub(crate) fn azdls_config_parse(mut properties: HashMap<String, String>) -> Result<AzdlsConfig> {
    let mut config = AzdlsConfig::default();

    if let Some(_conn_str) = properties.remove(ADLS_CONNECTION_STRING) {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Azdls: connection string currently not supported",
        ));
    }

    if let Some(account_name) = properties.remove(ADLS_ACCOUNT_NAME) {
        config.account_name = Some(account_name);
    }

    if let Some(account_key) = properties.remove(ADLS_ACCOUNT_KEY) {
        config.account_key = Some(account_key);
    }

    if let Some(sas_token) = properties.remove(ADLS_SAS_TOKEN) {
        config.sas_token = Some(sas_token);
    }

    if let Some(tenant_id) = properties.remove(ADLS_TENANT_ID) {
        config.tenant_id = Some(tenant_id);
    }

    if let Some(client_id) = properties.remove(ADLS_CLIENT_ID) {
        config.client_id = Some(client_id);
    }

    if let Some(client_secret) = properties.remove(ADLS_CLIENT_SECRET) {
        config.client_secret = Some(client_secret);
    }

    if let Some(authority_host) = properties.remove(ADLS_AUTHORITY_HOST) {
        config.authority_host = Some(authority_host);
    }

    Ok(config)
}

/// Account-specific ADLS SAS tokens supplied through Java-compatible storage
/// properties.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct AzdlsSasTokens(HashMap<String, String>);

impl AzdlsSasTokens {
    pub(crate) fn from_properties(properties: &HashMap<String, String>) -> Self {
        Self(
            properties
                .iter()
                .filter_map(|(key, value)| {
                    let account = key.strip_prefix(ADLS_SAS_TOKEN_PREFIX)?;
                    (!account.is_empty() && !value.is_empty())
                        .then(|| (account.to_string(), value.clone()))
                })
                .collect(),
        )
    }

    fn for_path(&self, path: &AzureStoragePath) -> Option<&str> {
        self.0.get(&path.account_name).map(String::as_str)
    }
}

impl std::fmt::Debug for AzdlsSasTokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzdlsSasTokens")
            .field("account_count", &self.0.len())
            .finish_non_exhaustive()
    }
}

/// Builds an OpenDAL operator from the AzdlsConfig and path.
///
/// The path is expected to include the scheme in a format like:
/// `abfss://<myfs>@<myaccount>.dfs.core.windows.net/mydir/myfile.parquet`.
pub(crate) fn azdls_create_operator<'a>(
    absolute_path: &'a str,
    config: &AzdlsConfig,
    sas_tokens: &AzdlsSasTokens,
    credential_provider: &Option<Arc<dyn StorageCredentialProvider>>,
    credential_scope: Option<&DynamicCredentialScope>,
) -> Result<(opendal::Operator, &'a str)> {
    let path = absolute_path.parse::<AzureStoragePath>()?;
    match_path_with_config(&path, config)?;

    let op = azdls_config_build(
        config,
        &path,
        sas_tokens,
        credential_provider,
        absolute_path,
        credential_scope,
    )?;

    // Paths to files in ADLS tend to be written in fully qualified form,
    // including their filesystem and account name.
    // OpenDAL's operator methods expect only the relative path, so we split it
    // off and save it for later use.
    let relative_path_len = path.path.len();
    let (_, relative_path) = absolute_path.split_at(absolute_path.len() - relative_path_len);

    Ok((op, relative_path))
}

/// Note that `abf[s]` and `wasb[s]` variants have different implications:
/// - `abfs[s]` is used to refer to files in ADLS Gen2, backed by blob storage;
///   paths are expected to contain the `dfs` storage service.
/// - `wasb[s]` is accepted for compatibility with Blob Storage locations;
///   paths contain the `blob` storage service, but operations still use the
///   ADLS Gen2 `dfs` endpoint, matching Iceberg Java.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AzureStorageScheme {
    Abfs,
    Abfss,
    Wasb,
    Wasbs,
}

impl AzureStorageScheme {
    // Iceberg Java accepts the non-secure aliases for compatibility but still
    // connects over TLS. SAS tokens are query parameters and must not be sent
    // over a plaintext connection.
    pub fn as_http_scheme(&self) -> &str {
        "https"
    }
}

impl Display for AzureStorageScheme {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AzureStorageScheme::Abfs => write!(f, "abfs"),
            AzureStorageScheme::Abfss => write!(f, "abfss"),
            AzureStorageScheme::Wasb => write!(f, "wasb"),
            AzureStorageScheme::Wasbs => write!(f, "wasbs"),
        }
    }
}

impl FromStr for AzureStorageScheme {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "abfs" => Ok(AzureStorageScheme::Abfs),
            "abfss" => Ok(AzureStorageScheme::Abfss),
            "wasb" => Ok(AzureStorageScheme::Wasb),
            "wasbs" => Ok(AzureStorageScheme::Wasbs),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Unexpected Azure Storage scheme: {s}"),
            )),
        }
    }
}

/// Validates whether the given path matches what's configured for the backend.
pub(crate) fn match_path_with_config(path: &AzureStoragePath, config: &AzdlsConfig) -> Result<()> {
    if let Some(ref configured_account_name) = config.account_name {
        ensure_data_valid!(
            &path.account_name == configured_account_name,
            "Storage::Azdls: Account name mismatch: configured {}, path {}",
            configured_account_name,
            path.account_name
        );
    }

    if let Some(ref configured_endpoint) = config.endpoint {
        let passed_http_scheme = path.scheme.as_http_scheme();
        ensure_data_valid!(
            configured_endpoint.starts_with(passed_http_scheme),
            "Storage::Azdls: Endpoint {} does not use the expected http scheme {}.",
            configured_endpoint,
            passed_http_scheme
        );

        let ends_with_expected_suffix = configured_endpoint
            .trim_end_matches('/')
            .ends_with(&path.endpoint_suffix);
        ensure_data_valid!(
            ends_with_expected_suffix,
            "Storage::Azdls: Endpoint suffix {} used with configured endpoint {}.",
            path.endpoint_suffix,
            configured_endpoint,
        );
    }

    Ok(())
}

fn azdls_config_build(
    config: &AzdlsConfig,
    path: &AzureStoragePath,
    sas_tokens: &AzdlsSasTokens,
    credential_provider: &Option<Arc<dyn StorageCredentialProvider>>,
    absolute_path: &str,
    credential_scope: Option<&DynamicCredentialScope>,
) -> Result<opendal::Operator> {
    let mut builder = config.clone().into_builder();

    if config.endpoint.is_none() {
        // If no endpoint is provided, we construct it from the fully-qualified path.
        builder = builder.endpoint(&path.as_endpoint());
    }
    builder = builder.filesystem(&path.filesystem);

    let credential_provider = credential_provider
        .as_ref()
        .filter(|provider| provider.supports_path(absolute_path));
    if let Some(provider) = credential_provider {
        let chain = ProvideCredentialChain::new().push(VendedAzdlsCredentialProvider::new(
            Arc::clone(provider),
            absolute_path.to_string(),
            credential_scope.cloned(),
        ));
        builder = builder.credential_provider_chain(chain);
    } else if let Some(sas_token) = sas_tokens.for_path(path) {
        builder = builder.sas_token(sas_token);
    }

    opendal::Operator::new(builder).map_err(from_opendal_error)
}

/// Adapts a generic [`StorageCredentialProvider`] into a reqsign
/// [`ProvideCredential`] for expiring Azure SAS tokens.
struct VendedAzdlsCredentialProvider {
    provider: Arc<dyn StorageCredentialProvider>,
    path: String,
    credential_scope: Option<DynamicCredentialScope>,
}

impl VendedAzdlsCredentialProvider {
    fn new(
        provider: Arc<dyn StorageCredentialProvider>,
        path: String,
        credential_scope: Option<DynamicCredentialScope>,
    ) -> Self {
        Self {
            provider,
            path,
            credential_scope,
        }
    }
}

impl std::fmt::Debug for VendedAzdlsCredentialProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VendedAzdlsCredentialProvider")
            .field("path", &self.path)
            .field("credential_scope", &self.credential_scope)
            .finish_non_exhaustive()
    }
}

impl ProvideCredential for VendedAzdlsCredentialProvider {
    type Credential = AzureCredential;

    async fn provide_credential(&self, _ctx: &Context) -> ReqsignResult<Option<AzureCredential>> {
        let credential = self
            .provider
            .load_credential(&self.path)
            .await
            .map_err(|error| {
                ReqsignError::unexpected("failed to load vended ADLS credential").with_source(error)
            })?;
        validate_credential_prefix(
            &self.path,
            credential.prefix(),
            self.credential_scope.as_ref(),
        )?;

        let expires_at = credential
            .expires_at()
            .map(system_time_to_timestamp)
            .transpose()?;
        match credential.into_kind() {
            StorageCredentialKind::Azdls(azdls) => {
                let sas_token = azdls.into_sas_token();
                Ok(Some(match expires_at {
                    Some(expires_at) => {
                        AzureCredential::with_sas_token_expires_at(&sas_token, expires_at)
                    }
                    None => AzureCredential::with_sas_token(&sas_token),
                }))
            }
            _ => Err(ReqsignError::unexpected(
                "ADLS storage received a non-ADLS credential from the provider",
            )),
        }
    }
}

/// Represents a fully qualified path to blob/ file in Azure Storage.
#[derive(Debug, PartialEq)]
pub(crate) struct AzureStoragePath {
    /// The scheme of the URL, e.g., `abfss`, `abfs`, `wasbs`, or `wasb`.
    scheme: AzureStorageScheme,

    /// Under Blob Storage, this is considered the _container_.
    filesystem: String,

    account_name: String,

    /// The endpoint suffix, e.g., `core.windows.net` for the public cloud
    /// endpoint.
    endpoint_suffix: String,

    /// Path to the file.
    ///
    /// It is relative to the `root` of the `AzdlsConfig`.
    pub(crate) path: String,
}

impl AzureStoragePath {
    /// Converts the AzureStoragePath into a full endpoint URL.
    ///
    /// This is possible because the path is fully qualified.
    fn as_endpoint(&self) -> String {
        format!(
            "{}://{}.dfs.{}",
            self.scheme.as_http_scheme(),
            self.account_name,
            self.endpoint_suffix
        )
    }
}

impl FromStr for AzureStoragePath {
    type Err = Error;

    fn from_str(path: &str) -> Result<Self> {
        let url = Url::parse(path)?;

        let filesystem = url.username();
        ensure_data_valid!(
            !filesystem.is_empty(),
            "AzureStoragePath: No container or filesystem name in path: {}",
            path
        );

        let (account_name, storage_service, endpoint_suffix) = parse_azure_storage_endpoint(&url)?;
        let scheme = validate_storage_and_scheme(storage_service, url.scheme())?;

        Ok(AzureStoragePath {
            scheme,
            filesystem: filesystem.to_string(),
            account_name: account_name.to_string(),
            endpoint_suffix: endpoint_suffix.to_string(),
            path: url.path().to_string(),
        })
    }
}

pub(crate) fn azdls_batch_key(absolute_path: &str) -> Option<String> {
    absolute_path.parse::<AzureStoragePath>().ok().map(|path| {
        format!(
            "{}://{}@{}.{}",
            path.scheme, path.filesystem, path.account_name, path.endpoint_suffix
        )
    })
}

fn parse_azure_storage_endpoint(url: &Url) -> Result<(&str, &str, &str)> {
    let host = url.host_str().ok_or(Error::new(
        ErrorKind::DataInvalid,
        "AzureStoragePath: No host",
    ))?;

    let (account_name, endpoint) = host.split_once('.').ok_or(Error::new(
        ErrorKind::DataInvalid,
        "AzureStoragePath: No account name",
    ))?;
    if account_name.is_empty() {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "AzureStoragePath: No account name",
        ));
    }

    let (storage, endpoint_suffix) = endpoint.split_once('.').ok_or(Error::new(
        ErrorKind::DataInvalid,
        "AzureStoragePath: No storage service",
    ))?;

    Ok((account_name, storage, endpoint_suffix))
}

fn validate_storage_and_scheme(
    storage_service: &str,
    scheme_str: &str,
) -> Result<AzureStorageScheme> {
    let scheme = scheme_str.parse::<AzureStorageScheme>()?;
    match scheme {
        AzureStorageScheme::Abfss | AzureStorageScheme::Abfs => {
            ensure_data_valid!(
                storage_service == "dfs",
                "AzureStoragePath: Unexpected storage service for abfs[s]: {}",
                storage_service
            );
            Ok(scheme)
        }
        AzureStorageScheme::Wasbs | AzureStorageScheme::Wasb => {
            ensure_data_valid!(
                storage_service == "blob",
                "AzureStoragePath: Unexpected storage service for wasb[s]: {}",
                storage_service
            );
            Ok(scheme)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use async_trait::async_trait;
    use iceberg::Result;
    use iceberg::io::{
        ADLS_SAS_TOKEN_PREFIX, AzdlsCredential, StorageCredential, StorageCredentialKind,
        StorageCredentialProvider,
    };
    use opendal::services::AzdlsConfig;
    use reqsign_azure_storage::Credential as AzureCredential;
    use reqsign_core::{Context, ProvideCredential};

    use super::{
        AzdlsSasTokens, AzureStoragePath, AzureStorageScheme, VendedAzdlsCredentialProvider,
        azdls_batch_key, azdls_config_parse, azdls_create_operator,
    };

    #[derive(Debug)]
    struct FixedCredentialProvider(StorageCredential);

    #[async_trait]
    impl StorageCredentialProvider for FixedCredentialProvider {
        async fn load_credential(&self, _path: &str) -> Result<StorageCredential> {
            Ok(self.0.clone())
        }
    }

    #[test]
    fn test_azdls_config_parse() {
        let test_cases = vec![
            (
                "account name and key",
                HashMap::from([
                    (super::ADLS_ACCOUNT_NAME.to_string(), "test".to_string()),
                    (super::ADLS_ACCOUNT_KEY.to_string(), "secret".to_string()),
                ]),
                Some(AzdlsConfig {
                    account_name: Some("test".to_string()),
                    account_key: Some("secret".to_string()),
                    ..Default::default()
                }),
            ),
            (
                "account name and SAS token",
                HashMap::from([
                    (super::ADLS_ACCOUNT_NAME.to_string(), "test".to_string()),
                    (super::ADLS_SAS_TOKEN.to_string(), "token".to_string()),
                ]),
                Some(AzdlsConfig {
                    account_name: Some("test".to_string()),
                    sas_token: Some("token".to_string()),
                    ..Default::default()
                }),
            ),
            (
                "account name and ADD credentials",
                HashMap::from([
                    (super::ADLS_ACCOUNT_NAME.to_string(), "test".to_string()),
                    (super::ADLS_CLIENT_ID.to_string(), "abcdef".to_string()),
                    (super::ADLS_CLIENT_SECRET.to_string(), "secret".to_string()),
                    (super::ADLS_TENANT_ID.to_string(), "12345".to_string()),
                ]),
                Some(AzdlsConfig {
                    account_name: Some("test".to_string()),
                    client_id: Some("abcdef".to_string()),
                    client_secret: Some("secret".to_string()),
                    tenant_id: Some("12345".to_string()),
                    ..Default::default()
                }),
            ),
        ];

        for (name, properties, expected) in test_cases {
            let config = azdls_config_parse(properties);
            match expected {
                Some(expected_config) => {
                    assert!(config.is_ok(), "Test case {name} failed: {config:?}");
                    assert_eq!(config.unwrap(), expected_config, "Test case: {name}");
                }
                None => {
                    assert!(config.is_err(), "Test case {name} expected error.");
                }
            }
        }
    }

    #[test]
    fn test_azdls_create_operator() {
        let test_cases = vec![
            (
                "basic",
                (
                    "abfss://myfs@myaccount.dfs.core.windows.net/path/to/file.parquet",
                    AzdlsConfig {
                        account_name: Some("myaccount".to_string()),
                        endpoint: Some("https://myaccount.dfs.core.windows.net".to_string()),
                        ..Default::default()
                    },
                ),
                Some(("myfs", "/path/to/file.parquet")),
            ),
            (
                "different account",
                (
                    "abfss://myfs@anotheraccount.dfs.core.windows.net/path/to/file.parquet",
                    AzdlsConfig {
                        account_name: Some("myaccount".to_string()),
                        endpoint: Some("https://myaccount.dfs.core.windows.net".to_string()),
                        ..Default::default()
                    },
                ),
                None,
            ),
            (
                "incompatible scheme for endpoint",
                (
                    // `abfss` implies https; configured endpoint is plain http.
                    "abfss://myfs@myaccount.dfs.core.windows.net/path/to/file.parquet",
                    AzdlsConfig {
                        account_name: Some("myaccount".to_string()),
                        endpoint: Some("http://myaccount.dfs.core.windows.net".to_string()),
                        ..Default::default()
                    },
                ),
                None,
            ),
            (
                "different endpoint suffix",
                (
                    "abfss://somefs@myaccount.dfs.core.windows.net/path/to/file.parquet",
                    AzdlsConfig {
                        account_name: Some("myaccount".to_string()),
                        endpoint: Some("https://myaccount.dfs.core.chinacloudapi.cn".to_string()),
                        ..Default::default()
                    },
                ),
                None,
            ),
            (
                "endpoint inferred from fully qualified path",
                (
                    "abfs://myfs@myaccount.dfs.core.windows.net/path/to/file.parquet",
                    AzdlsConfig {
                        filesystem: "myfs".to_string(),
                        account_name: Some("myaccount".to_string()),
                        endpoint: None,
                        ..Default::default()
                    },
                ),
                Some(("myfs", "/path/to/file.parquet")),
            ),
            (
                "scheme differs from a previously-configured one is accepted",
                (
                    // No configured scheme exists anymore; both abfss and wasbs
                    // should be accepted by the same storage.
                    "wasbs://myfs@myaccount.blob.core.windows.net/path/to/file.parquet",
                    AzdlsConfig {
                        account_name: Some("myaccount".to_string()),
                        endpoint: Some("https://myaccount.blob.core.windows.net".to_string()),
                        ..Default::default()
                    },
                ),
                Some(("myfs", "/path/to/file.parquet")),
            ),
        ];

        for (name, input, expected) in test_cases {
            let result =
                azdls_create_operator(input.0, &input.1, &AzdlsSasTokens::default(), &None, None);
            match expected {
                Some((expected_filesystem, expected_path)) => {
                    assert!(result.is_ok(), "Test case {name} failed: {result:?}");

                    let (op, relative_path) = result.unwrap();
                    assert_eq!(op.info().name(), expected_filesystem);
                    assert_eq!(relative_path, expected_path);
                }
                None => {
                    assert!(result.is_err(), "Test case {name} expected error.");
                }
            }
        }
    }

    #[tokio::test]
    async fn vended_provider_returns_expiring_sas_credential() {
        let path = "abfss://container@account.dfs.core.windows.net/table/data.parquet";
        let expires_at = SystemTime::now() + Duration::from_secs(3600);
        let credential = StorageCredential::new(StorageCredentialKind::Azdls(
            AzdlsCredential::new("sv=2026&sig=secret"),
        ))
        .with_prefix("abfss://container@account.dfs.core.windows.net/table")
        .with_expiration(expires_at);
        let provider = VendedAzdlsCredentialProvider::new(
            Arc::new(FixedCredentialProvider(credential)),
            path.to_string(),
            None,
        );

        let credential = provider
            .provide_credential(&Context::new())
            .await
            .unwrap()
            .unwrap();
        match credential {
            AzureCredential::SasToken {
                token,
                expires_at: actual_expires_at,
            } => {
                assert_eq!(token, "sv=2026&sig=secret");
                assert_eq!(
                    actual_expires_at,
                    Some(super::system_time_to_timestamp(expires_at).unwrap())
                );
            }
            other => panic!("expected SAS token, got {other:?}"),
        }
    }

    #[test]
    fn account_specific_sas_tokens_are_selected_by_storage_account() {
        let properties = HashMap::from([
            (
                format!("{ADLS_SAS_TOKEN_PREFIX}first"),
                "sv=2026&sig=first".to_string(),
            ),
            (
                format!("{ADLS_SAS_TOKEN_PREFIX}second"),
                "sv=2026&sig=second".to_string(),
            ),
        ]);
        let sas_tokens = AzdlsSasTokens::from_properties(&properties);
        let path = "abfss://container@second.dfs.core.windows.net/table/data.parquet"
            .parse::<AzureStoragePath>()
            .unwrap();

        assert_eq!(sas_tokens.for_path(&path), Some("sv=2026&sig=second"));
    }

    #[tokio::test]
    async fn vended_provider_rejects_mismatched_prefix() {
        let credential = StorageCredential::new(StorageCredentialKind::Azdls(
            AzdlsCredential::new("sv=2026&sig=secret"),
        ))
        .with_prefix("abfss://other@account.dfs.core.windows.net/table")
        .with_expiration(SystemTime::now() + Duration::from_secs(3600));
        let provider = VendedAzdlsCredentialProvider::new(
            Arc::new(FixedCredentialProvider(credential)),
            "abfss://container@account.dfs.core.windows.net/table/data.parquet".to_string(),
            None,
        );

        assert!(provider.provide_credential(&Context::new()).await.is_err());
    }

    #[test]
    fn batch_key_distinguishes_azure_filesystems_and_schemes() {
        let first = azdls_batch_key("abfss://first@account.dfs.core.windows.net/table/a.parquet");
        let second = azdls_batch_key("abfss://second@account.dfs.core.windows.net/table/b.parquet");
        let blob = azdls_batch_key("wasbs://first@account.blob.core.windows.net/table/c.parquet");

        assert_ne!(first, second);
        assert_ne!(first, blob);
    }

    #[test]
    fn test_azure_storage_path_parse() {
        let test_cases = vec![
            (
                "succeeds",
                "abfss://somefs@myaccount.dfs.core.windows.net/path/to/file.parquet",
                Some(AzureStoragePath {
                    scheme: AzureStorageScheme::Abfss,
                    filesystem: "somefs".to_string(),
                    account_name: "myaccount".to_string(),
                    endpoint_suffix: "core.windows.net".to_string(),
                    path: "/path/to/file.parquet".to_string(),
                }),
            ),
            (
                "unexpected scheme",
                "adls://somefs@myaccount.dfs.core.windows.net/path/to/file.parquet",
                None,
            ),
            (
                "no filesystem",
                "abfss://myaccount.dfs.core.windows.net/path/to/file.parquet",
                None,
            ),
            (
                "no account name",
                "abfs://myfs@dfs.core.windows.net/path/to/file.parquet",
                None,
            ),
        ];

        for (name, input, expected) in test_cases {
            let result = input.parse::<AzureStoragePath>();
            match expected {
                Some(expected_path) => {
                    assert!(result.is_ok(), "Test case {name} failed: {result:?}");
                    assert_eq!(result.unwrap(), expected_path, "Test case: {name}");
                }
                None => {
                    assert!(result.is_err(), "Test case {name} expected error.");
                }
            }
        }
    }

    #[test]
    fn test_azure_storage_path_endpoint() {
        let test_cases = vec![
            (
                "abfss uses https",
                AzureStoragePath {
                    scheme: AzureStorageScheme::Abfss,
                    filesystem: "myfs".to_string(),
                    account_name: "myaccount".to_string(),
                    endpoint_suffix: "core.windows.net".to_string(),
                    path: "/path/to/file.parquet".to_string(),
                },
                "https://myaccount.dfs.core.windows.net",
            ),
            (
                "abfs uses https for Java compatibility and SAS security",
                AzureStoragePath {
                    scheme: AzureStorageScheme::Abfs,
                    filesystem: "myfs".to_string(),
                    account_name: "myaccount".to_string(),
                    endpoint_suffix: "core.windows.net".to_string(),
                    path: "/path/to/file.parquet".to_string(),
                },
                "https://myaccount.dfs.core.windows.net",
            ),
            (
                "wasbs uses https and dfs",
                AzureStoragePath {
                    scheme: AzureStorageScheme::Wasbs,
                    filesystem: "myfs".to_string(),
                    account_name: "myaccount".to_string(),
                    endpoint_suffix: "core.windows.net".to_string(),
                    path: "/path/to/file.parquet".to_string(),
                },
                "https://myaccount.dfs.core.windows.net",
            ),
        ];

        for (name, path, expected) in test_cases {
            let endpoint = path.as_endpoint();
            assert_eq!(endpoint, expected, "Test case: {name}");
        }
    }
}
