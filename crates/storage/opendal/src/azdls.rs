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
    ADLS_CONNECTION_STRING, ADLS_SAS_TOKEN, ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX,
    ADLS_SAS_TOKEN_PREFIX, ADLS_TENANT_ID, StorageCredentialProvider,
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

use crate::utils::{
    VendedCredentialSource, credential_expiry, from_opendal_error, required_credential_property,
};

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
///
/// Public only because [`OpenDalStorage::Azdls`](crate::OpenDalStorage::Azdls)
/// holds it; it is built from the storage configuration.
#[doc(hidden)]
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct AzdlsSasTokens(HashMap<String, String>);

impl AzdlsSasTokens {
    /// Collect `adls.sas-token.<host>` properties by key suffix.
    pub(crate) fn from_properties(properties: &HashMap<String, String>) -> Self {
        Self(
            properties
                .iter()
                .filter_map(|(key, value)| {
                    let suffix = key.strip_prefix(ADLS_SAS_TOKEN_PREFIX)?;
                    (!suffix.is_empty() && !value.is_empty())
                        .then(|| (suffix.to_string(), value.clone()))
                })
                .collect(),
        )
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The token for `path`, selected as by [`sas_token_suffixes`].
    fn for_path(&self, path: &AzureStoragePath) -> Option<&str> {
        sas_token_suffixes(path)
            .iter()
            .find_map(|suffix| self.0.get(suffix))
            .map(String::as_str)
    }
}

/// Suffixes of the `adls.sas-token.<suffix>` properties that may hold the SAS
/// token for `path`, most specific first: the exact host, as in Java, then the
/// account alone, as sent for older Java versions and PyIceberg. A key that
/// names only the account matches every host of an account with that name,
/// including one in another cloud; a host-keyed token matches only its host.
fn sas_token_suffixes(path: &AzureStoragePath) -> [String; 2] {
    [path.host(), path.account_name.clone()]
}

impl std::fmt::Debug for AzdlsSasTokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzdlsSasTokens")
            .field("token_count", &self.0.len())
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
    credential_location: Option<&str>,
) -> Result<(opendal::Operator, &'a str)> {
    let path = absolute_path.parse::<AzureStoragePath>()?;
    match_path_with_config(&path, config)?;

    let op = azdls_config_build(
        config,
        &path,
        sas_tokens,
        credential_provider,
        absolute_path,
        credential_location,
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
    /// Whether an explicitly configured endpoint must use TLS.
    fn requires_tls(&self) -> bool {
        matches!(self, AzureStorageScheme::Abfss | AzureStorageScheme::Wasbs)
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
        // An explicit plaintext endpoint, such as a local emulator, remains
        // valid for the non-secure schemes.
        ensure_data_valid!(
            !path.scheme.requires_tls() || configured_endpoint.starts_with("https://"),
            "Storage::Azdls: Endpoint {} does not use https, which the {} scheme requires.",
            configured_endpoint,
            path.scheme
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
    credential_location: Option<&str>,
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
        let chain = ProvideCredentialChain::new().push(VendedAzdlsCredentialProvider {
            source: VendedCredentialSource::new(
                Arc::clone(provider),
                credential_location.unwrap_or(absolute_path).to_string(),
            ),
            sas_token_suffixes: sas_token_suffixes(path),
        });
        builder = builder.credential_provider_chain(chain);
    } else if let Some(sas_token) = sas_tokens.for_path(path) {
        builder = builder.sas_token(sas_token);
    }

    opendal::Operator::new(builder).map_err(from_opendal_error)
}

/// Adapts a generic [`StorageCredentialProvider`] into a reqsign
/// [`ProvideCredential`] for expiring Azure SAS tokens.
#[derive(Debug)]
struct VendedAzdlsCredentialProvider {
    source: VendedCredentialSource,
    /// Suffixes of the SAS token properties for the operator's path.
    sas_token_suffixes: [String; 2],
}

impl ProvideCredential for VendedAzdlsCredentialProvider {
    type Credential = AzureCredential;

    async fn provide_credential(&self, _ctx: &Context) -> ReqsignResult<Option<AzureCredential>> {
        let credential = self
            .source
            .load("ADLS", |config| {
                let suffix = self
                    .sas_token_suffixes
                    .iter()
                    .find(|suffix| {
                        config
                            .get(&format!("{ADLS_SAS_TOKEN_PREFIX}{suffix}"))
                            .is_some_and(|token| !token.is_empty())
                    })
                    .ok_or_else(|| {
                        ReqsignError::unexpected(format!(
                            "vended credential is missing {ADLS_SAS_TOKEN_PREFIX}{}",
                            self.sas_token_suffixes[0]
                        ))
                    })?;
                let sas_token = required_credential_property(
                    config,
                    &format!("{ADLS_SAS_TOKEN_PREFIX}{suffix}"),
                )?;
                Ok(
                    match credential_expiry(
                        config,
                        &format!("{ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX}{suffix}"),
                    )? {
                        Some(expires_at) => {
                            AzureCredential::with_sas_token_expires_at(sas_token, expires_at)
                        }
                        None => AzureCredential::with_sas_token(sas_token),
                    },
                )
            })
            .await?;
        Ok(Some(credential))
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
    /// The host of the path, e.g. `account.dfs.core.windows.net`.
    fn host(&self) -> String {
        let service = match self.scheme {
            AzureStorageScheme::Abfs | AzureStorageScheme::Abfss => "dfs",
            AzureStorageScheme::Wasb | AzureStorageScheme::Wasbs => "blob",
        };
        format!("{}.{service}.{}", self.account_name, self.endpoint_suffix)
    }

    /// Converts the AzureStoragePath into a full endpoint URL.
    ///
    /// This is possible because the path is fully qualified.
    ///
    /// Like Iceberg Java, the endpoint always uses TLS, also for the
    /// non-secure schemes: SAS tokens are query parameters and must not be
    /// sent over a plaintext connection unless the user configures one.
    fn as_endpoint(&self) -> String {
        format!("https://{}.dfs.{}", self.account_name, self.endpoint_suffix)
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

pub(crate) fn azdls_batch_key(absolute_path: &str) -> Result<String> {
    let path = absolute_path.parse::<AzureStoragePath>()?;
    Ok(format!(
        "{}://{}@{}.{}",
        path.scheme, path.filesystem, path.account_name, path.endpoint_suffix
    ))
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

    use async_trait::async_trait;
    use iceberg::Result;
    use iceberg::io::{
        ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX, ADLS_SAS_TOKEN_PREFIX, StorageCredential,
        StorageCredentialProvider,
    };
    use opendal::services::AzdlsConfig;
    use reqsign_azure_storage::Credential as AzureCredential;
    use reqsign_core::{Context, ProvideCredential};

    use super::{
        AzdlsSasTokens, AzureStoragePath, AzureStorageScheme, VendedAzdlsCredentialProvider,
        VendedCredentialSource, azdls_batch_key, azdls_config_parse, azdls_create_operator,
        sas_token_suffixes,
    };

    fn adapter(credential: StorageCredential, path: &str) -> VendedAzdlsCredentialProvider {
        VendedAzdlsCredentialProvider {
            source: VendedCredentialSource::new(
                Arc::new(FixedCredentialProvider(credential)),
                path.to_string(),
            ),
            sas_token_suffixes: sas_token_suffixes(&path.parse().unwrap()),
        }
    }

    #[derive(Debug)]
    struct FixedCredentialProvider(StorageCredential);

    #[async_trait]
    impl StorageCredentialProvider for FixedCredentialProvider {
        fn supports_path(&self, _path: &str) -> bool {
            true
        }

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
                "plaintext endpoint for a non-secure scheme",
                (
                    "abfs://myfs@myaccount.dfs.core.windows.net/path/to/file.parquet",
                    AzdlsConfig {
                        account_name: Some("myaccount".to_string()),
                        endpoint: Some("http://myaccount.dfs.core.windows.net".to_string()),
                        ..Default::default()
                    },
                ),
                Some(("myfs", "/path/to/file.parquet")),
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
        let host = "account.dfs.core.windows.net";
        let credential = StorageCredential::new(
            "abfss://container@account.dfs.core.windows.net/table",
            HashMap::from([
                (
                    format!("{ADLS_SAS_TOKEN_PREFIX}{host}"),
                    "sv=2026&sig=host".to_string(),
                ),
                (
                    format!("{ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX}{host}"),
                    "1500".to_string(),
                ),
                // The host-keyed token wins over the account-keyed one.
                (
                    format!("{ADLS_SAS_TOKEN_PREFIX}account"),
                    "sv=2026&sig=account".to_string(),
                ),
            ]),
        );

        let credential = adapter(credential, path)
            .provide_credential(&Context::new())
            .await
            .unwrap()
            .unwrap();
        match credential {
            AzureCredential::SasToken { token, expires_at } => {
                assert_eq!(token, "sv=2026&sig=host");
                assert_eq!(
                    expires_at,
                    Some(reqsign_core::time::Timestamp::from_millisecond(1500).unwrap())
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

    #[test]
    fn host_keyed_sas_tokens_match_java() {
        let properties = HashMap::from([
            (
                format!("{ADLS_SAS_TOKEN_PREFIX}account.dfs.core.windows.net"),
                "sv=2026&sig=host".to_string(),
            ),
            (
                format!("{ADLS_SAS_TOKEN_PREFIX}account"),
                "sv=2026&sig=account".to_string(),
            ),
        ]);
        let sas_tokens = AzdlsSasTokens::from_properties(&properties);
        let token_for = |location: &str| {
            sas_tokens
                .for_path(&location.parse::<AzureStoragePath>().unwrap())
                .map(str::to_owned)
        };

        // The exact host wins over the account.
        assert_eq!(
            token_for("abfss://container@account.dfs.core.windows.net/table/data.parquet")
                .as_deref(),
            Some("sv=2026&sig=host")
        );
        // Other hosts of the account fall back to the account-keyed token.
        assert_eq!(
            token_for("wasbs://container@account.blob.core.windows.net/table/data.parquet")
                .as_deref(),
            Some("sv=2026&sig=account")
        );

        // A host-keyed token is never sent to the same account name in another
        // cloud.
        let host_only = AzdlsSasTokens::from_properties(&HashMap::from([(
            format!("{ADLS_SAS_TOKEN_PREFIX}account.dfs.core.windows.net"),
            "sv=2026&sig=host".to_string(),
        )]));
        let other_cloud = "abfss://container@account.dfs.core.usgovcloudapi.net/data.parquet"
            .parse::<AzureStoragePath>()
            .unwrap();
        assert_eq!(host_only.for_path(&other_cloud), None);
    }

    #[tokio::test]
    async fn vended_provider_falls_back_to_the_account_token_when_the_host_token_is_empty() {
        let path = "abfss://container@account.dfs.core.windows.net/table/data.parquet";
        let credential = StorageCredential::new(
            "abfss://container@account.dfs.core.windows.net",
            HashMap::from([
                (
                    format!("{ADLS_SAS_TOKEN_PREFIX}account.dfs.core.windows.net"),
                    String::new(),
                ),
                (
                    format!("{ADLS_SAS_TOKEN_PREFIX}account"),
                    "sv=2026&sig=account".to_string(),
                ),
            ]),
        );

        match adapter(credential, path)
            .provide_credential(&Context::new())
            .await
            .unwrap()
            .unwrap()
        {
            AzureCredential::SasToken { token, .. } => assert_eq!(token, "sv=2026&sig=account"),
            other => panic!("expected SAS token, got {other:?}"),
        }
    }

    /// Builds an ADLS operator for `path` that sends its requests to `server`.
    fn operator_for(
        server: &mockito::Server,
        path: &str,
        sas_tokens: &AzdlsSasTokens,
        credential_provider: Option<Arc<dyn StorageCredentialProvider>>,
    ) -> opendal::Operator {
        let config = AzdlsConfig {
            endpoint: Some(server.url()),
            ..Default::default()
        };
        super::azdls_config_build(
            &config,
            &path.parse().unwrap(),
            sas_tokens,
            &credential_provider,
            path,
            None,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn operator_signs_with_the_vended_sas_token() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("HEAD", mockito::Matcher::Any)
            .match_query(mockito::Matcher::Regex("sig=vended".to_string()))
            .expect_at_least(1)
            .with_status(404)
            .create_async()
            .await;
        let path = "abfss://container@account.dfs.core.windows.net/table/data.parquet";
        let credential = StorageCredential::new(
            "abfss://container@account.dfs.core.windows.net",
            HashMap::from([(
                format!("{ADLS_SAS_TOKEN_PREFIX}account.dfs.core.windows.net"),
                "sv=2026&sig=vended".to_string(),
            )]),
        );
        // A static token is not used when a provider supplies credentials.
        let static_tokens = AzdlsSasTokens::from_properties(&HashMap::from([(
            format!("{ADLS_SAS_TOKEN_PREFIX}account.dfs.core.windows.net"),
            "sv=2026&sig=static".to_string(),
        )]));

        let operator = operator_for(
            &server,
            path,
            &static_tokens,
            Some(Arc::new(FixedCredentialProvider(credential))),
        );
        assert!(operator.stat("table/data.parquet").await.is_err());
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn operator_signs_with_the_static_account_sas_token() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("HEAD", mockito::Matcher::Any)
            .match_query(mockito::Matcher::Regex("sig=static".to_string()))
            .expect_at_least(1)
            .with_status(404)
            .create_async()
            .await;
        let static_tokens = AzdlsSasTokens::from_properties(&HashMap::from([(
            format!("{ADLS_SAS_TOKEN_PREFIX}account.dfs.core.windows.net"),
            "sv=2026&sig=static".to_string(),
        )]));

        let operator = operator_for(
            &server,
            "abfss://container@account.dfs.core.windows.net/table/data.parquet",
            &static_tokens,
            None,
        );
        assert!(operator.stat("table/data.parquet").await.is_err());
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn vended_provider_rejects_mismatched_prefix() {
        let credential = StorageCredential::new(
            "abfss://other@account.dfs.core.windows.net/table",
            HashMap::from([(
                format!("{ADLS_SAS_TOKEN_PREFIX}account.dfs.core.windows.net"),
                "sv=2026&sig=secret".to_string(),
            )]),
        );
        let provider = adapter(
            credential,
            "abfss://container@account.dfs.core.windows.net/table/data.parquet",
        );

        assert!(provider.provide_credential(&Context::new()).await.is_err());
    }

    #[test]
    fn batch_key_distinguishes_azure_filesystems_and_schemes() {
        let first = azdls_batch_key("abfss://first@account.dfs.core.windows.net/table/a.parquet");
        let second = azdls_batch_key("abfss://second@account.dfs.core.windows.net/table/b.parquet");
        let blob = azdls_batch_key("wasbs://first@account.blob.core.windows.net/table/c.parquet");

        assert_ne!(first.unwrap(), second.unwrap());
        assert_ne!(
            azdls_batch_key("abfss://first@account.dfs.core.windows.net/table/a.parquet").unwrap(),
            blob.unwrap()
        );
        assert!(azdls_batch_key("abfss:///no-account.parquet").is_err());
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
