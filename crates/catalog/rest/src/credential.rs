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

//! Vended storage credentials from a REST catalog.
//!
//! A REST catalog can vend short-lived storage credentials whose lifetime the
//! client does not control. [`RestVendedCredentialProvider`] implements the
//! core [`StorageCredentialProvider`] trait so storage backends re-fetch those
//! credentials from the catalog's table credentials endpoint before they
//! expire, keeping long-running jobs authenticated instead of failing with a
//! `403` once the initial token's TTL elapses.
//!
//! Unlike the Java client, which has one provider per cloud SDK, this is a
//! single backend-agnostic provider with an independent endpoint and cache for
//! each configured cloud. The path being accessed selects the cloud cache. Like
//! Java's `StorageCredential`, the returned [`StorageCredential`] holds the
//! backend's storage properties, which the storage adapter turns into its own
//! credential. This preserves Java's per-cloud credential selection and
//! prefetch policies while supporting mixed-cloud tables through a resolving
//! FileIO. Unlike Java's scheduled refresh, which permanently stops
//! after a failed fetch, transient failures are retried here with jittered
//! exponential backoff while an unexpired credential remains available.
//!
//! Like Java's `VendedCredentialsProvider`, a provider is rebuilt from the
//! FileIO properties after [`FileIO`](iceberg::io::FileIO) serialization and
//! connects to the catalog lazily in the receiving process. This requires
//! catalog authentication that can be rebuilt from `rest.auth.type`.
//!
//! # Adding a cloud
//!
//! The refresh policy for each cloud lives in one [`CloudRefresh`] constant. To
//! add a backend, teach its storage adapter to read its credential properties
//! from a [`StorageCredential`]. Then write its `parse_*` function, add a
//! `CloudRefresh` constant, and list it in [`CloudRefresh::SUPPORTED`].

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use iceberg::io::{
    ADLS_REFRESH_CREDENTIALS_ENABLED, ADLS_REFRESH_CREDENTIALS_ENDPOINT,
    ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX, ADLS_SAS_TOKEN_PREFIX, AWS_REFRESH_CREDENTIALS_ENABLED,
    AWS_REFRESH_CREDENTIALS_ENDPOINT, GCS_REFRESH_CREDENTIALS_ENABLED,
    GCS_REFRESH_CREDENTIALS_ENDPOINT, GCS_TOKEN, GCS_TOKEN_EXPIRES_AT, S3_ACCESS_KEY_ID,
    S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN, S3_SESSION_TOKEN_EXPIRES_AT_MS, StorageConfig,
    StorageCredential, StorageCredentialProvider, StorageCredentialProviderFactory,
};
use iceberg::{Error, ErrorKind, Result, TableIdent};
use rand::Rng;
use reqwest::{Method, Url};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, OnceCell};

use crate::auth::{AUTH_TYPE_NONE, AuthManager, load_auth_manager, static_token_session};
use crate::catalog::{
    REST_CATALOG_PROP_AUTH_TYPE, REST_CATALOG_PROP_REFERENCED_BY, REST_CATALOG_PROP_SCAN_PLAN_ID,
    RestCatalogConfig,
};
use crate::client::{HttpClient, unexpected_catalog_error_without_body};
use crate::request::HttpRequest;
use crate::types::LoadCredentialsResponse;

type CredentialParser =
    fn(config: &HashMap<String, String>, prefix: String) -> Result<VendedCredential>;
type KeyedSeedCredentialParser =
    fn(config: &HashMap<String, String>) -> HashMap<String, VendedCredential>;
type KeyedSeedPathResolver = fn(path: &str) -> Result<KeyedSeedPath>;

/// A credential vended by the catalog, with the expiry its config declares.
#[derive(Clone)]
struct VendedCredential {
    credential: StorageCredential,
    expires_at: SystemTime,
}

enum SeedStrategy {
    /// One credential stored in the backend's flat properties, for every
    /// location with one of the scheme prefixes, like Java's root clients.
    Flat { prefixes: &'static [&'static str] },
    /// Credentials selected by a backend-specific key derived from each path.
    Keyed(KeyedSeedStrategy),
}

struct KeyedSeedStrategy {
    parse_credentials: KeyedSeedCredentialParser,
    resolve_path: KeyedSeedPathResolver,
}

struct KeyedSeedPath {
    /// Seed keys for the path, most specific first.
    keys: Vec<String>,
    scope: String,
}

/// Cloud-specific details regarding vended-credential refresh.
///
/// It contains the location schemes it backs, the property keys it is configured
/// with, and how to parse its credential. The generic provider stays free of
/// any per-cloud knowledge.
struct CloudRefresh {
    /// Location URL schemes this backend serves.
    schemes: &'static [&'static str],
    /// Table property naming the refresh endpoint (absolute or catalog-relative).
    endpoint_key: &'static str,
    /// Table property controlling refresh; only missing or case-insensitive `"true"` enables it.
    enabled_key: &'static str,
    /// Table property that must be present for refresh to apply, as in Java,
    /// where it decides whether the backend uses vended credentials at all.
    required_key: Option<&'static str>,
    /// Whether to jitter successful prefetch times like AWS `CachedSupplier`.
    jitter_prefetch: bool,
    /// Parse a complete credential from catalog-supplied properties.
    parse_credential: CredentialParser,
    /// How initial credentials in the table properties are selected.
    seed_strategy: SeedStrategy,
}

impl CloudRefresh {
    /// S3 / AWS
    const AWS: Self = Self {
        schemes: &["s3", "s3a", "s3n"],
        endpoint_key: AWS_REFRESH_CREDENTIALS_ENDPOINT,
        enabled_key: AWS_REFRESH_CREDENTIALS_ENABLED,
        required_key: None,
        jitter_prefetch: true,
        parse_credential: parse_s3_credential,
        // `s3` also covers `s3a` and `s3n` locations.
        seed_strategy: SeedStrategy::Flat { prefixes: &["s3"] },
    };
    /// Google Cloud Storage
    const GCP: Self = Self {
        schemes: &["gs", "gcs"],
        endpoint_key: GCS_REFRESH_CREDENTIALS_ENDPOINT,
        enabled_key: GCS_REFRESH_CREDENTIALS_ENABLED,
        // Java's GCS client only refreshes a vended OAuth2 token; without
        // one it uses Google's default credentials.
        required_key: Some(GCS_TOKEN),
        jitter_prefetch: false,
        parse_credential: parse_gcs_credential,
        seed_strategy: SeedStrategy::Flat {
            prefixes: &["gs", "gcs"],
        },
    };
    /// Azure Data Lake Storage
    const AZURE: Self = Self {
        schemes: &["abfs", "abfss", "wasb", "wasbs"],
        endpoint_key: ADLS_REFRESH_CREDENTIALS_ENDPOINT,
        enabled_key: ADLS_REFRESH_CREDENTIALS_ENABLED,
        required_key: None,
        jitter_prefetch: false,
        parse_credential: parse_azdls_credential,
        seed_strategy: SeedStrategy::Keyed(KeyedSeedStrategy {
            parse_credentials: parse_azdls_account_seeds,
            resolve_path: resolve_azdls_seed_path,
        }),
    };

    /// Backends with refresh support
    const SUPPORTED: &[Self] = &[Self::AWS, Self::GCP, Self::AZURE];

    fn matches_location(&self, location: &str) -> bool {
        self.schemes
            .iter()
            .any(|scheme| location.eq_ignore_ascii_case(scheme))
            || scheme_of(location).is_some_and(|scheme| self.schemes.contains(&scheme.as_str()))
    }
}

/// Re-fetch a credential once it is within this window of expiry, so a fresh
/// token is in hand before the object store would reject the old one.
const REFRESH_BUFFER: Duration = Duration::from_mins(5);

/// AWS keeps at least one minute between its jittered prefetch time and expiry.
const MIN_REFRESH_BUFFER: Duration = Duration::from_mins(1);

/// Initial ceiling for failure backoff. Equal jitter chooses from half this
/// value through the full value.
const INITIAL_FAILURE_BACKOFF: Duration = Duration::from_secs(1);

/// Maximum delay between failed refresh attempts.
const MAX_FAILURE_BACKOFF: Duration = Duration::from_secs(30);

/// A cached vended credential and its refresh schedule.
#[derive(Clone)]
struct CachedEntry {
    credential: StorageCredential,
    expires_at: SystemTime,
    /// When this entry becomes eligible for prefetch.
    refresh_at: SystemTime,
}

impl CachedEntry {
    fn new(vended: VendedCredential, jitter_prefetch: bool) -> Self {
        let VendedCredential {
            credential,
            expires_at,
        } = vended;
        Self {
            credential,
            expires_at,
            refresh_at: prefetch_time(SystemTime::now(), expires_at, jitter_prefetch),
        }
    }

    /// Seed entries that are already inside the nominal five-minute window are
    /// immediately due. Otherwise AWS applies the same jitter as it does to a
    /// freshly fetched value.
    fn seed(vended: VendedCredential, jitter_prefetch: bool) -> Self {
        let due = SystemTime::now()
            .checked_add(REFRESH_BUFFER)
            .is_none_or(|refresh_boundary| refresh_boundary >= vended.expires_at);
        let mut entry = Self::new(vended, jitter_prefetch);
        if due {
            entry.refresh_at = UNIX_EPOCH;
        }
        entry
    }

    fn is_fresh(&self, now: SystemTime) -> bool {
        now < self.refresh_at
    }

    fn is_unexpired(&self, now: SystemTime) -> bool {
        now < self.expires_at
    }
}

/// Parsed entries from one successful credentials response.
struct ParsedCredentials {
    entries: Vec<CachedEntry>,
    errors: Vec<CredentialError>,
}

struct CredentialError {
    prefix: String,
    error: Error,
}

/// Cached credentials plus failure-backoff state.
///
/// A fetch returns the credentials for every prefix of the table, so a path
/// the last response did not cover would not be covered by an immediate
/// re-fetch either. Backoff is therefore shared by the whole cloud.
struct CacheState {
    entries: Vec<CachedEntry>,
    /// Prefixes whose credential in the last successful response was invalid.
    /// Paths under them must not use a broader credential: the catalog scoped
    /// them more tightly.
    failed_prefixes: HashSet<String>,
    consecutive_failures: u32,
    retry_not_before: Option<Instant>,
    /// Why the last refresh failed, reported while refresh is backed off.
    last_failure: Option<String>,
}

impl CacheState {
    fn new(entries: Vec<CachedEntry>) -> Self {
        Self {
            entries,
            failed_prefixes: HashSet::new(),
            consecutive_failures: 0,
            retry_not_before: None,
            last_failure: None,
        }
    }

    /// The index of the unexpired credential with the longest prefix covering
    /// `path`.
    fn unexpired_match(&self, path: &str, now: SystemTime) -> Option<usize> {
        self.entries
            .iter()
            .enumerate()
            .filter(|(_, entry)| entry.is_unexpired(now) && entry.credential.covers(path))
            .max_by_key(|(_, entry)| entry.credential.prefix().len())
            .map(|(index, _)| index)
    }

    /// Whether a failed prefix covering `path` is narrower than `credential`'s.
    fn masks(&self, path: &str, credential: Option<&StorageCredential>) -> bool {
        let prefix_len = credential.map(|credential| credential.prefix().len());
        self.failed_prefixes.iter().any(|failed| {
            prefix_covers(failed, path) && prefix_len.is_none_or(|len| failed.len() > len)
        })
    }

    /// Cache `entry`, replacing any entry with the same prefix, and return its
    /// index.
    fn insert(&mut self, entry: CachedEntry) -> usize {
        let prefix = entry.credential.prefix().to_owned();
        self.entries
            .retain(|cached| cached.credential.prefix() != prefix);
        self.entries.push(entry);
        self.entries.len() - 1
    }

    fn record_success(&mut self) {
        self.consecutive_failures = 0;
        self.retry_not_before = None;
        self.last_failure = None;
    }

    fn record_failure(&mut self, error: &Error) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.retry_not_before =
            Instant::now().checked_add(failure_backoff(self.consecutive_failures));
        self.last_failure = Some(error.to_string());
    }

    /// Replace cached credentials with the fetched ones, prefix by prefix,
    /// and record the prefixes whose credential was invalid in this response.
    ///
    /// An unexpired cached credential survives when the response carries no
    /// valid replacement for its prefix, so an absent or malformed entry never
    /// evicts a usable credential. Returns the prefixes the response replaced.
    fn merge(&mut self, fetched: Vec<CachedEntry>, errors: &[CredentialError]) -> HashSet<String> {
        let fetched_prefixes = fetched
            .iter()
            .map(|entry| entry.credential.prefix().to_owned())
            .collect::<HashSet<_>>();
        // A response names every prefix of the table, so a prefix it omits is
        // no longer scoped separately: a fetched credential that covers it
        // replaces its cached one. A prefix whose credential is invalid keeps
        // its cached one.
        let failed_prefixes = errors
            .iter()
            .map(|error| error.prefix.clone())
            .collect::<HashSet<_>>();
        self.entries.retain(|entry| {
            let prefix = entry.credential.prefix();
            failed_prefixes.contains(prefix)
                || !fetched
                    .iter()
                    .any(|fetched| fetched.credential.covers(prefix))
        });
        self.entries.extend(fetched);
        self.failed_prefixes = failed_prefixes;
        fetched_prefixes
    }
}

struct ConfiguredCloud {
    cloud: &'static CloudRefresh,
    endpoint: String,
    cache: Mutex<CacheState>,
    /// Initial property credentials whose scope cannot be represented by a
    /// single leading URI prefix, keyed according to the cloud strategy.
    keyed_seeds: HashMap<String, CachedEntry>,
    /// Only one caller fetches at a time. The cache lock is deliberately
    /// separate so other callers can keep using an unexpired credential while
    /// the refresh is in flight.
    refresh: Mutex<()>,
}

impl ConfiguredCloud {
    fn new(
        cloud: &'static CloudRefresh,
        endpoint: String,
        entries: Vec<CachedEntry>,
        keyed_seeds: HashMap<String, CachedEntry>,
    ) -> Self {
        Self {
            cloud,
            endpoint,
            cache: Mutex::new(CacheState::new(entries)),
            keyed_seeds,
            refresh: Mutex::new(()),
        }
    }

    /// Configure `cloud` from the FileIO properties, or `None` when they
    /// advertise no enabled refresh endpoint for it.
    fn configure(
        cloud: &'static CloudRefresh,
        base_uri: &str,
        props: &HashMap<String, String>,
    ) -> Option<Self> {
        let enabled = props
            .get(cloud.enabled_key)
            .is_none_or(|value| value.eq_ignore_ascii_case("true"))
            && cloud
                .required_key
                .is_none_or(|key| props.get(key).is_some_and(|value| !value.is_empty()));
        let endpoint = props
            .get(cloud.endpoint_key)
            .filter(|endpoint| enabled && !endpoint.is_empty())
            .map(|endpoint| resolve_endpoint(base_uri, endpoint))?;

        let mut entries = Vec::new();
        let mut keyed_seeds = HashMap::new();
        match &cloud.seed_strategy {
            SeedStrategy::Flat { prefixes } => {
                entries.extend(prefixes.iter().filter_map(|prefix| {
                    (cloud.parse_credential)(props, prefix.to_string())
                        .ok()
                        .map(|vended| CachedEntry::seed(vended, cloud.jitter_prefetch))
                }));
            }
            SeedStrategy::Keyed(strategy) => {
                keyed_seeds = (strategy.parse_credentials)(props)
                    .into_iter()
                    .map(|(key, credential)| {
                        (key, CachedEntry::seed(credential, cloud.jitter_prefetch))
                    })
                    .collect();
            }
        }
        Some(Self::new(cloud, endpoint, entries, keyed_seeds))
    }

    fn keyed_seed_for_path(&self, path: &str) -> Result<Option<CachedEntry>> {
        let SeedStrategy::Keyed(strategy) = &self.cloud.seed_strategy else {
            return Ok(None);
        };
        let resolved = (strategy.resolve_path)(path)?;
        let Some(seed) = resolved
            .keys
            .iter()
            .find_map(|key| self.keyed_seeds.get(key))
        else {
            return Ok(None);
        };
        let credential = StorageCredential::new(resolved.scope, seed.credential.config().clone());
        // The scope is normalized, e.g. with a lowercase scheme, so it may not
        // cover the path as written; the path then fetches its credential.
        Ok(credential.covers(path).then_some(CachedEntry {
            credential,
            expires_at: seed.expires_at,
            refresh_at: seed.refresh_at,
        }))
    }
}

/// Serializable recipe for rebuilding a [`RestVendedCredentialProvider`] from
/// the FileIO properties, mirroring how Java rebuilds its provider on workers.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct RestVendedCredentialProviderFactory {
    /// Catalog URI, used to resolve relative refresh endpoints.
    catalog_uri: String,
    table: TableIdent,
    /// The `rest.auth.type` the catalog resolved, so a rebuilt provider uses
    /// the same auth manager even when the merged FileIO properties would
    /// infer another one.
    auth_type: String,
    /// Whether `auth_type` was inferred rather than configured. Like Java,
    /// a table `token` then authenticates a catalog without auth.
    auth_type_inferred: bool,
    /// The auth-related part of the unmerged config returned by the table
    /// endpoint, from which the auth manager derives a table session. Keeping
    /// it separate prevents local FileIO overrides from masking table auth.
    table_auth: HashMap<String, String>,
}

/// Table config keys that may override authentication in a table session.
const TABLE_AUTH_KEYS: &[&str] = &["token"];

impl RestVendedCredentialProviderFactory {
    pub(crate) fn new(
        catalog_uri: impl Into<String>,
        table: TableIdent,
        auth_type: impl Into<String>,
        auth_type_inferred: bool,
        table_config: &HashMap<String, String>,
    ) -> Self {
        Self {
            catalog_uri: catalog_uri.into(),
            table,
            auth_type: auth_type.into(),
            auth_type_inferred,
            table_auth: table_config
                .iter()
                .filter(|(key, _)| TABLE_AUTH_KEYS.contains(&key.as_str()))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        }
    }

    /// The table token that authenticates credential requests of a catalog
    /// without auth. Java's credential providers infer OAuth2 from a `token`
    /// unless `rest.auth.type` is configured.
    fn bearer_table_token(&self) -> Option<&str> {
        (self.auth_type == AUTH_TYPE_NONE && self.auth_type_inferred)
            .then(|| self.table_auth.get("token").map(String::as_str))
            .flatten()
    }

    /// Connect to the catalog from the FileIO properties, as the catalog would.
    async fn connect(&self, props: &HashMap<String, String>) -> Result<HttpClient> {
        let mut config_props = props.clone();
        config_props.insert(
            REST_CATALOG_PROP_AUTH_TYPE.to_string(),
            self.auth_type.clone(),
        );
        let config = RestCatalogConfig::builder()
            .uri(self.catalog_uri.clone())
            .props(config_props)
            .build();
        let auth_manager = load_auth_manager(&config)?;
        let client = HttpClient::new(&config)?;
        let session = auth_manager
            .catalog_session(&client.without_auth_session(), &config.auth_props())
            .await?;
        table_client(
            &client.with_auth_session(session),
            auth_manager.as_ref(),
            &self.table,
            props,
            &self.table_auth,
            self.bearer_table_token(),
        )
        .await
    }
}

impl std::fmt::Debug for RestVendedCredentialProviderFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestVendedCredentialProviderFactory")
            .field("catalog_uri", &self.catalog_uri)
            .field("table", &self.table)
            .finish_non_exhaustive()
    }
}

#[typetag::serde(name = "RestVendedCredentialProviderFactory")]
impl StorageCredentialProviderFactory for RestVendedCredentialProviderFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn StorageCredentialProvider>> {
        let provider =
            RestVendedCredentialProvider::configure(self, config.props()).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "FileIO configuration no longer configures vended credentials",
                )
            })?;
        Ok(Arc::new(RestVendedCredentialProvider {
            factory: Some(self.clone()),
            ..provider
        }))
    }
}

/// Derive the table-scoped client used for credential requests.
///
/// A `bearer_table_token` authenticates instead of a session from the auth
/// manager.
async fn table_client(
    catalog_client: &HttpClient,
    auth_manager: &dyn AuthManager,
    table: &TableIdent,
    props: &HashMap<String, String>,
    table_config: &HashMap<String, String>,
    bearer_table_token: Option<&str>,
) -> Result<HttpClient> {
    let session = match bearer_table_token {
        Some(token) => static_token_session(token),
        None => {
            auth_manager
                .table_session(
                    &catalog_client.without_auth_session(),
                    table,
                    table_config,
                    catalog_client.auth_session(),
                )
                .await?
        }
    };
    catalog_client.for_table(props, session)
}

/// Serves vended credentials for a table, refreshing them from the REST
/// catalog's table credentials endpoint.
///
/// Each cloud cache is seeded with the credentials from the initial
/// table properties (when complete) and re-fetched from its endpoint as they
/// near expiry.
pub(crate) struct RestVendedCredentialProvider {
    /// Table-scoped catalog client. Set when the catalog builds the provider,
    /// and connected on first refresh after deserialization.
    client: OnceCell<HttpClient>,
    /// Rebuilds this provider in another process. `None` when the catalog
    /// authentication cannot be rebuilt from properties.
    factory: Option<RestVendedCredentialProviderFactory>,
    /// Effective FileIO properties, which supply catalog connection settings.
    props: HashMap<String, String>,
    /// Query parameters sent with every credentials request.
    query_params: Vec<(&'static str, String)>,
    /// Independently configured endpoint and cache for each backing cloud.
    clouds: Vec<ConfiguredCloud>,
}

impl RestVendedCredentialProvider {
    /// Configure a provider without a catalog connection, or `None` when no
    /// supported cloud advertises an enabled refresh endpoint.
    fn configure(
        factory: &RestVendedCredentialProviderFactory,
        props: &HashMap<String, String>,
    ) -> Option<Self> {
        let clouds = CloudRefresh::SUPPORTED
            .iter()
            .filter_map(|cloud| ConfiguredCloud::configure(cloud, &factory.catalog_uri, props))
            .collect::<Vec<_>>();
        (!clouds.is_empty()).then(|| Self {
            client: OnceCell::new(),
            factory: None,
            props: props.clone(),
            query_params: credentials_query_params(props),
            clouds,
        })
    }

    fn configured_cloud_for_location(&self, location: &str) -> Option<&ConfiguredCloud> {
        self.clouds
            .iter()
            .find(|configured| configured.cloud.matches_location(location))
    }

    async fn client(&self) -> Result<&HttpClient> {
        self.client
            .get_or_try_init(|| async {
                let factory = self.factory.as_ref().ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "vended credential provider has no catalog connection",
                    )
                })?;
                factory.connect(&self.props).await
            })
            .await
    }

    /// Fetch fresh credentials from the catalog's credentials endpoint.
    async fn fetch(&self, configured: &ConfiguredCloud) -> Result<ParsedCredentials> {
        let cloud = configured.cloud;
        let client = self.client().await?;
        let url = credentials_url(&configured.endpoint, &self.query_params)?;
        let request = HttpRequest::build(client.request(Method::GET, url))?;
        let response = client.query_catalog(request).await?;

        if !response.status().is_success() {
            return Err(unexpected_catalog_error_without_body(
                response,
                client.disable_header_redaction(),
            ));
        }

        // Credential responses contain secrets, and serde's errors quote the
        // values they reject, so only the error's category and position are
        // reported.
        let parsed: LoadCredentialsResponse =
            serde_json::from_slice(response.body()).map_err(|error| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "failed to parse vended credential response: {:?} error at line {}, \
                         column {}",
                        error.classify(),
                        error.line(),
                        error.column()
                    ),
                )
            })?;
        let now = SystemTime::now();
        let mut entries = Vec::new();
        let mut errors = Vec::new();
        for credential in parsed
            .storage_credentials
            .into_iter()
            .filter(|credential| cloud.matches_location(&credential.prefix))
        {
            let prefix = credential.prefix;
            let parsed = (cloud.parse_credential)(&credential.config, prefix.clone())
                .map(|vended| CachedEntry::new(vended, cloud.jitter_prefetch))
                .and_then(|entry| {
                    if entry.is_unexpired(now) {
                        Ok(entry)
                    } else {
                        Err(Error::new(
                            ErrorKind::DataInvalid,
                            "invalid vended credential: credential is already expired",
                        ))
                    }
                });
            match parsed {
                Ok(entry) => entries.push(entry),
                Err(error) => errors.push(CredentialError { prefix, error }),
            }
        }
        Ok(ParsedCredentials { entries, errors })
    }

    async fn refresh_credential(
        &self,
        configured: &ConfiguredCloud,
        path: &str,
        fallback: Option<CachedEntry>,
    ) -> Result<StorageCredential> {
        let fetched = self.fetch(configured).await;
        let mut cache = configured.cache.lock().await;
        let now = SystemTime::now();
        cache.entries.retain(|entry| entry.is_unexpired(now));

        let (fetched_prefixes, failure) = match fetched {
            Ok(ParsedCredentials { entries, errors }) => {
                let fetched_prefixes = cache.merge(entries, &errors);
                // The most specific error for `path`.
                let failure = errors
                    .into_iter()
                    .filter(|error| prefix_covers(&error.prefix, path))
                    .max_by_key(|error| error.prefix.len())
                    .map(|error| error.error);
                (Some(fetched_prefixes), failure)
            }
            Err(error) => (None, Some(error)),
        };

        let selected = cache.unexpired_match(path, now);
        // A keyed seed is only cached once served as the fallback.
        let seed = selected
            .is_none()
            .then(|| {
                fallback
                    .clone()
                    .filter(|fallback| fallback.is_unexpired(now))
            })
            .flatten();
        let candidate = selected
            .map(|index| &cache.entries[index].credential)
            .or(seed.as_ref().map(|seed| &seed.credential));
        if cache.masks(path, candidate) {
            let error = failure.unwrap_or_else(|| masked_credential_error(path));
            cache.record_failure(&error);
            return Err(error);
        }

        let refreshed = selected.filter(|&index| {
            fetched_prefixes
                .as_ref()
                .is_some_and(|fetched| fetched.contains(cache.entries[index].credential.prefix()))
        });
        if let Some(index) = refreshed {
            let entry = &mut cache.entries[index];
            // A catalog may return the same credential until shortly before
            // it expires. Treat one that is no newer like no answer, so the
            // checks stay few as expiry approaches.
            if fallback
                .as_ref()
                .is_some_and(|previous| entry.expires_at <= previous.expires_at)
            {
                entry.refresh_at = recheck_time(now, entry.expires_at);
            }
            cache.record_success();
            return Ok(cache.entries[index].credential.clone());
        }

        let selected = selected.or_else(|| seed.map(|seed| cache.insert(seed)));
        let Some(index) = selected else {
            let error = failure.unwrap_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("no unexpired vended credential matches storage location: {path}"),
                )
            });
            cache.record_failure(&error);
            return Err(error);
        };
        // A failed fetch always reports its error as the failure.
        match failure {
            None => {
                // The catalog answered without a newer credential for this
                // path, for example when it replaced a scheme-wide credential
                // with scoped ones. Keep the current credential and check
                // again later instead of on every access.
                let entry = &mut cache.entries[index];
                entry.refresh_at = recheck_time(now, entry.expires_at);
                cache.record_success();
            }
            // Graceful degradation: while a credential for this path remains
            // usable, serve it and retry after jittered backoff. Expired
            // credentials are never served.
            Some(error) => cache.record_failure(&error),
        }
        Ok(cache.entries[index].credential.clone())
    }
}

impl std::fmt::Debug for RestVendedCredentialProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestVendedCredentialProvider")
            .field("configured_clouds", &self.clouds.len())
            .finish_non_exhaustive()
    }
}

enum CacheDecision {
    Use(StorageCredential),
    Refresh(Option<CachedEntry>),
    /// Refresh is backed off after the failure described, if known.
    Backoff(Option<String>),
}

fn masked_credential_error(path: &str) -> Error {
    Error::new(
        ErrorKind::DataInvalid,
        format!("the catalog vended an invalid credential for storage location: {path}"),
    )
}

fn refresh_backoff_error(path: &str, last_failure: Option<String>) -> Error {
    let error = Error::new(
        ErrorKind::Unexpected,
        format!("vended credential refresh is temporarily backed off for storage location: {path}"),
    );
    match last_failure {
        Some(last_failure) => error.with_context("last_failure", last_failure),
        None => error,
    }
}

async fn cache_decision(configured: &ConfiguredCloud, path: &str) -> Result<CacheDecision> {
    let keyed_seed = configured.keyed_seed_for_path(path)?;
    let cache = configured.cache.lock().await;
    let now = SystemTime::now();
    let current = cache
        .unexpired_match(path, now)
        .map(|index| cache.entries[index].clone())
        .or(keyed_seed)
        .filter(|entry| !cache.masks(path, Some(&entry.credential)));

    if let Some(entry) = current.as_ref().filter(|entry| entry.is_fresh(now)) {
        return Ok(CacheDecision::Use(entry.credential.clone()));
    }

    if cache
        .retry_not_before
        .is_some_and(|retry_at| Instant::now() < retry_at)
    {
        return Ok(current
            .filter(|entry| entry.is_unexpired(now))
            .map(|entry| CacheDecision::Use(entry.credential))
            .unwrap_or_else(|| CacheDecision::Backoff(cache.last_failure.clone())));
    }

    Ok(CacheDecision::Refresh(current))
}

#[async_trait]
impl StorageCredentialProvider for RestVendedCredentialProvider {
    fn supports_path(&self, path: &str) -> bool {
        self.configured_cloud_for_location(path).is_some()
    }

    async fn load_credential(&self, path: &str) -> Result<StorageCredential> {
        let configured = self.configured_cloud_for_location(path).ok_or_else(|| {
            Error::new(
                ErrorKind::FeatureUnsupported,
                format!("vended credentials are not configured for storage location: {path}"),
            )
        })?;

        let current = match cache_decision(configured, path).await? {
            CacheDecision::Use(credential) => return Ok(credential),
            CacheDecision::Refresh(current) => current,
            CacheDecision::Backoff(last_failure) => {
                return Err(refresh_backoff_error(path, last_failure));
            }
        };

        // One caller refreshes, while concurrent callers immediately keep using the
        // unexpired cached credential. With no usable credential, callers wait
        // for the in-flight refresh instead.
        let usable = current
            .as_ref()
            .filter(|entry| entry.is_unexpired(SystemTime::now()));
        let _refresh_guard = if let Some(entry) = usable {
            match configured.refresh.try_lock() {
                Ok(guard) => guard,
                Err(_) => return Ok(entry.credential.clone()),
            }
        } else {
            configured.refresh.lock().await
        };

        // Another caller may have completed a refresh between our cache check
        // and acquiring the single-flight guard.
        let current = match cache_decision(configured, path).await? {
            CacheDecision::Use(credential) => return Ok(credential),
            CacheDecision::Refresh(current) => current,
            CacheDecision::Backoff(last_failure) => {
                return Err(refresh_backoff_error(path, last_failure));
            }
        };

        self.refresh_credential(configured, path, current).await
    }

    fn factory(&self) -> Result<Arc<dyn StorageCredentialProviderFactory>> {
        match &self.factory {
            Some(factory) => Ok(Arc::new(factory.clone())),
            None => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "the vended credential provider cannot be serialized because the REST catalog \
                 uses an injected AuthManager, which cannot be rebuilt in another process",
            )),
        }
    }
}

/// Compute the prefetch time of a credential obtained at `now`.
///
/// Like Java, a credential is refreshed [`REFRESH_BUFFER`] before it expires.
/// Unlike Java, the buffer is capped at half the remaining lifetime: a
/// credential vended with a shorter lifetime would otherwise be due on
/// arrival, and every file operation would fetch again.
fn prefetch_time(now: SystemTime, expires_at: SystemTime, jitter: bool) -> SystemTime {
    let lifetime = expires_at.duration_since(now).unwrap_or_default();
    let buffer = REFRESH_BUFFER.min(lifetime / 2);
    let base = expires_at.checked_sub(buffer).unwrap_or(UNIX_EPOCH);
    if !jitter {
        return base;
    }

    // The minimum distance from expiry shrinks with a capped buffer.
    let min_buffer =
        buffer.mul_f64(MIN_REFRESH_BUFFER.as_secs_f64() / REFRESH_BUFFER.as_secs_f64());
    let jitter_millis = buffer.saturating_sub(min_buffer).as_millis() as u64;
    if jitter_millis == 0 {
        return base;
    }
    base.checked_add(Duration::from_millis(
        rand::rng().random_range(0..jitter_millis),
    ))
    .unwrap_or(base)
}

/// When to ask the catalog again for a newer credential after it answered
/// that none exists: at the regular prefetch time, which is halfway to expiry
/// within ten minutes of it, or not at all once less than twice
/// [`MIN_REFRESH_BUFFER`] remains, so the checks stay few as expiry approaches.
fn recheck_time(now: SystemTime, expires_at: SystemTime) -> SystemTime {
    let remaining = expires_at.duration_since(now).unwrap_or_default();
    if remaining <= MIN_REFRESH_BUFFER * 2 {
        expires_at
    } else {
        prefetch_time(now, expires_at, false)
    }
}

/// Equal-jitter exponential backoff. The random lower half avoids both hot
/// retry loops and synchronized retries across clients.
fn failure_backoff(consecutive_failures: u32) -> Duration {
    let exponent = consecutive_failures.saturating_sub(1).min(5);
    let ceiling = INITIAL_FAILURE_BACKOFF
        .checked_mul(1 << exponent)
        .unwrap_or(MAX_FAILURE_BACKOFF)
        .min(MAX_FAILURE_BACKOFF);
    let ceiling_millis = ceiling.as_millis() as u64;
    let floor_millis = ceiling_millis / 2;
    Duration::from_millis(rand::rng().random_range(floor_millis..=ceiling_millis))
}

/// Build a credential provider for a table, or `None` when no supported cloud
/// advertises an enabled refresh endpoint.
///
/// `catalog_client` carries the catalog session, from which `auth_manager`
/// derives a table session when the table config overrides authentication.
/// `props` contains the effective FileIO properties after applying local
/// overrides, and therefore supplies headers and client policy. `portable`
/// states whether the catalog authentication can be rebuilt from `props` in
/// another process, which makes the provider serializable.
pub(crate) async fn build_vended_credential_provider(
    catalog_client: &HttpClient,
    auth_manager: &dyn AuthManager,
    factory: RestVendedCredentialProviderFactory,
    props: &HashMap<String, String>,
    table_config: &HashMap<String, String>,
    portable: bool,
) -> Result<Option<Arc<dyn StorageCredentialProvider>>> {
    let Some(provider) = RestVendedCredentialProvider::configure(&factory, props) else {
        return Ok(None);
    };

    // An injected auth manager decides on table sessions itself, given the
    // whole table config.
    let client = table_client(
        catalog_client,
        auth_manager,
        &factory.table,
        props,
        table_config,
        portable.then(|| factory.bearer_table_token()).flatten(),
    )
    .await?;
    Ok(Some(Arc::new(RestVendedCredentialProvider {
        client: OnceCell::new_with(Some(client)),
        factory: portable.then_some(factory),
        ..provider
    })))
}

/// The `referenced-by` query parameter. Its value is already percent-encoded.
const REFERENCED_BY_QUERY_PARAMETER: &str = "referenced-by";

/// Query parameters for credentials requests, like Java's
/// `RESTUtil.credentialsQueryParams`.
fn credentials_query_params(props: &HashMap<String, String>) -> Vec<(&'static str, String)> {
    [
        (REST_CATALOG_PROP_SCAN_PLAN_ID, "planId"),
        (
            REST_CATALOG_PROP_REFERENCED_BY,
            REFERENCED_BY_QUERY_PARAMETER,
        ),
    ]
    .into_iter()
    .filter_map(|(property, parameter)| props.get(property).map(|value| (parameter, value.clone())))
    .collect()
}

/// The credentials request URL. Like Java's `HTTPRequest.requestUri`, the
/// already encoded `referenced-by` value is appended verbatim instead of being
/// encoded again.
fn credentials_url(endpoint: &str, query_params: &[(&str, String)]) -> Result<Url> {
    let mut url = Url::parse(endpoint).map_err(|error| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("invalid credentials endpoint: {endpoint}"),
        )
        .with_source(error)
    })?;
    for (parameter, value) in query_params {
        if *parameter != REFERENCED_BY_QUERY_PARAMETER {
            url.query_pairs_mut().append_pair(parameter, value);
        }
    }
    if let Some((_, referenced_by)) = query_params
        .iter()
        .find(|(parameter, _)| *parameter == REFERENCED_BY_QUERY_PARAMETER)
    {
        let query = match url.query() {
            Some(query) => format!("{query}&{REFERENCED_BY_QUERY_PARAMETER}={referenced_by}"),
            None => format!("{REFERENCED_BY_QUERY_PARAMETER}={referenced_by}"),
        };
        url.set_query(Some(&query));
    }
    Ok(url)
}

/// Resolve a possibly-relative refresh endpoint against the catalog base URI.
///
/// Absolute endpoints are used as-is and receive the catalog credentials, as
/// in Java. The catalog is trusted to advertise only its own endpoints.
fn resolve_endpoint(base_uri: &str, endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        return endpoint.to_string();
    }

    let base = base_uri.trim_end_matches('/');
    let separator = if endpoint.starts_with('/') { "" } else { "/" };
    format!("{base}{separator}{endpoint}")
}

/// Whether the credential `prefix` covers `location`, as in
/// [`StorageCredential::covers`].
fn prefix_covers(prefix: &str, location: &str) -> bool {
    !prefix.is_empty() && location.starts_with(prefix)
}

/// The URL scheme of `location`, lowercased (e.g. `"s3"` for `s3://bucket/k`).
fn scheme_of(location: &str) -> Option<String> {
    Url::parse(location)
        .ok()
        .map(|url| url.scheme().to_string())
}

/// Parse a complete S3 credential supplied by the catalog.
fn parse_s3_credential(
    config: &HashMap<String, String>,
    prefix: String,
) -> Result<VendedCredential> {
    let credential_config = copy_required(config, &[
        S3_ACCESS_KEY_ID,
        S3_SECRET_ACCESS_KEY,
        S3_SESSION_TOKEN,
        S3_SESSION_TOKEN_EXPIRES_AT_MS,
    ])?;
    Ok(VendedCredential {
        expires_at: required_epoch_millis(config, S3_SESSION_TOKEN_EXPIRES_AT_MS)?,
        credential: StorageCredential::new(prefix, credential_config),
    })
}

/// Parse a complete GCS credential supplied by the catalog.
fn parse_gcs_credential(
    config: &HashMap<String, String>,
    prefix: String,
) -> Result<VendedCredential> {
    let credential_config = copy_required(config, &[GCS_TOKEN, GCS_TOKEN_EXPIRES_AT])?;
    Ok(VendedCredential {
        expires_at: required_epoch_millis(config, GCS_TOKEN_EXPIRES_AT)?,
        credential: StorageCredential::new(prefix, credential_config),
    })
}

/// The non-empty values of `keys` in `config`, which must all be present.
fn copy_required(
    config: &HashMap<String, String>,
    keys: &[&str],
) -> Result<HashMap<String, String>> {
    keys.iter()
        .map(|key| Ok((key.to_string(), required_nonempty(config, key)?)))
        .collect()
}

/// Parse a complete account-specific ADLS SAS credential supplied by the catalog.
fn parse_azdls_credential(
    config: &HashMap<String, String>,
    prefix: String,
) -> Result<VendedCredential> {
    let location = AzdlsLocation::parse(&prefix)?;
    let (suffix, sas_token) = location
        .token_keys()
        .into_iter()
        .find_map(|key| azdls_sas_tokens(config).find(|(suffix, _)| *suffix == key))
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "invalid vended credential: no {ADLS_SAS_TOKEN_PREFIX}* token for {}",
                    location.host
                ),
            )
        })?;
    azdls_vended_credential(config, prefix, suffix, sas_token)
}

/// The vended credential for the SAS token under `suffix`, which must have an
/// expiry under the same suffix.
fn azdls_vended_credential(
    config: &HashMap<String, String>,
    prefix: String,
    suffix: &str,
    sas_token: &str,
) -> Result<VendedCredential> {
    let expiry_key = format!("{ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX}{suffix}");
    let expires_at = required_epoch_millis(config, &expiry_key)?;
    let expires_at_ms = required_nonempty(config, &expiry_key)?;
    let credential_config = HashMap::from([
        (
            format!("{ADLS_SAS_TOKEN_PREFIX}{suffix}"),
            sas_token.to_string(),
        ),
        (expiry_key, expires_at_ms),
    ]);
    Ok(VendedCredential {
        credential: StorageCredential::new(prefix, credential_config),
        expires_at,
    })
}

/// Parse every complete account-specific ADLS credential from the initial
/// table properties, keyed by their key suffix. Unlike a URI prefix, the host
/// of an Azure location occurs after the filesystem, so these seeds are
/// selected by host, or by account for keys that name only one. Their prefix
/// is set once a location selects them.
fn parse_azdls_account_seeds(
    config: &HashMap<String, String>,
) -> HashMap<String, VendedCredential> {
    azdls_sas_tokens(config)
        .filter_map(|(suffix, token)| {
            let vended = azdls_vended_credential(config, String::new(), suffix, token).ok()?;
            Some((suffix.to_string(), vended))
        })
        .collect()
}

/// Account-specific SAS tokens as `(key suffix, token)`. A suffix names a host
/// such as `account.dfs.core.windows.net`, as in Java, or only the account.
fn azdls_sas_tokens(config: &HashMap<String, String>) -> impl Iterator<Item = (&str, &str)> {
    config.iter().filter_map(|(key, token)| {
        let suffix = key.strip_prefix(ADLS_SAS_TOKEN_PREFIX)?;
        (!suffix.is_empty() && !token.is_empty()).then_some((suffix, token.as_str()))
    })
}

fn resolve_azdls_seed_path(location: &str) -> Result<KeyedSeedPath> {
    let location = AzdlsLocation::parse(location)?;
    let keys = location.token_keys();
    let mut url = location.url;
    // Without a trailing slash, the scope also covers the container root.
    url.set_path("");
    url.set_query(None);
    url.set_fragment(None);
    Ok(KeyedSeedPath {
        keys,
        scope: url.to_string(),
    })
}

/// An ADLS location, with the host and account that select its SAS token.
struct AzdlsLocation {
    url: Url,
    /// Host, e.g. `account.dfs.core.windows.net`.
    host: String,
    /// Storage account.
    account: String,
}

impl AzdlsLocation {
    fn parse(location: &str) -> Result<Self> {
        let url = Url::parse(location).map_err(|error| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("invalid ADLS storage location: {location}"),
            )
            .with_source(error)
        })?;
        if !CloudRefresh::AZURE.schemes.contains(&url.scheme()) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("invalid ADLS storage location scheme: {}", url.scheme()),
            ));
        }
        let host = url.host_str().unwrap_or_default().to_string();
        let account = host
            .split('.')
            .next()
            .filter(|account| !account.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("ADLS storage location has no account name: {location}"),
                )
            })?;
        Ok(Self { url, host, account })
    }

    /// SAS token key suffixes for this location, most specific first: the
    /// exact host, as in Java, then the account alone, as sent for older Java
    /// versions and PyIceberg. A key that names only the account matches every
    /// host of an account with that name, including one in another cloud; a
    /// host-keyed token matches only its host.
    fn token_keys(&self) -> Vec<String> {
        vec![self.host.clone(), self.account.clone()]
    }
}

fn required_nonempty(config: &HashMap<String, String>, key: &str) -> Result<String> {
    config
        .get(key)
        .filter(|value| !value.is_empty())
        .cloned()
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("invalid vended credential: {key} is missing or empty"),
            )
        })
}

fn required_epoch_millis(config: &HashMap<String, String>, key: &str) -> Result<SystemTime> {
    let value = required_nonempty(config, key)?;
    parse_epoch_millis(&value).ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("invalid vended credential: {key} is not a valid epoch-millisecond timestamp"),
        )
    })
}

/// Parse an epoch-millisecond timestamp into a [`SystemTime`].
fn parse_epoch_millis(millis: &str) -> Option<SystemTime> {
    millis
        .parse()
        .ok()
        .and_then(|millis| UNIX_EPOCH.checked_add(Duration::from_millis(millis)))
}

#[cfg(test)]
mod tests {
    use std::sync::{Barrier, mpsc};

    use mockito::{Matcher, Server};

    use super::*;
    use crate::auth::{AUTH_TYPE_NONE, AUTH_TYPE_OAUTH2, NoopAuthManager, OAuth2Manager};

    fn epoch_millis(time: SystemTime) -> String {
        time.duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string()
    }

    fn s3_vended(prefix: &str, access_key_id: &str, expires_at: SystemTime) -> VendedCredential {
        VendedCredential {
            credential: StorageCredential::new(
                prefix,
                HashMap::from([
                    (S3_ACCESS_KEY_ID.to_string(), access_key_id.to_string()),
                    (S3_SECRET_ACCESS_KEY.to_string(), "secret".to_string()),
                ]),
            ),
            expires_at,
        }
    }

    fn s3_access_key_id(credential: &StorageCredential) -> &str {
        credential
            .config()
            .get(S3_ACCESS_KEY_ID)
            .expect("an S3 credential")
    }

    fn gcs_token(credential: &StorageCredential) -> &str {
        credential
            .config()
            .get(GCS_TOKEN)
            .expect("a GCS credential")
    }

    fn sas_token(credential: &StorageCredential) -> &str {
        credential
            .config()
            .iter()
            .find(|(key, _)| key.starts_with(ADLS_SAS_TOKEN_PREFIX))
            .map(|(_, token)| token.as_str())
            .expect("an ADLS credential")
    }

    /// A previously cached entry: like a seed, its age is unknown, so it is due
    /// once inside the nominal refresh window.
    fn cached_s3(prefix: &str, access_key_id: &str, expires_at: SystemTime) -> CachedEntry {
        CachedEntry::seed(s3_vended(prefix, access_key_id, expires_at), false)
    }

    fn test_client(base_uri: &str) -> HttpClient {
        let config = RestCatalogConfig::builder()
            .uri(base_uri.to_string())
            .build();
        HttpClient::new(&config).unwrap()
    }

    fn test_factory(
        base_uri: &str,
        table_config: Option<&HashMap<String, String>>,
    ) -> RestVendedCredentialProviderFactory {
        RestVendedCredentialProviderFactory::new(
            base_uri,
            test_table(),
            AUTH_TYPE_OAUTH2,
            true,
            &table_config.cloned().unwrap_or_default(),
        )
    }

    /// A provider whose AWS cache starts with `entries`.
    fn provider_with_cached_s3(
        base_uri: &str,
        entries: Vec<CachedEntry>,
    ) -> RestVendedCredentialProvider {
        RestVendedCredentialProvider {
            client: OnceCell::new_with(Some(test_client(base_uri))),
            factory: None,
            props: HashMap::new(),
            query_params: Vec::new(),
            clouds: vec![ConfiguredCloud::new(
                &CloudRefresh::AWS,
                format!("{base_uri}/v1/credentials"),
                entries,
                HashMap::new(),
            )],
        }
    }

    fn aws_refresh_props(endpoint: &str) -> HashMap<String, String> {
        HashMap::from([(
            CloudRefresh::AWS.endpoint_key.to_string(),
            endpoint.to_string(),
        )])
    }

    fn with_s3_seed(
        mut props: HashMap<String, String>,
        access_key_id: &str,
        expires_at: SystemTime,
    ) -> HashMap<String, String> {
        props.extend([
            (S3_ACCESS_KEY_ID.to_string(), access_key_id.to_string()),
            (S3_SECRET_ACCESS_KEY.to_string(), "SEED_SK".to_string()),
            (S3_SESSION_TOKEN.to_string(), "SEED_TOK".to_string()),
            (
                S3_SESSION_TOKEN_EXPIRES_AT_MS.to_string(),
                epoch_millis(expires_at),
            ),
        ]);
        props
    }

    fn test_table() -> TableIdent {
        TableIdent::from_strs(["namespace", "table"]).unwrap()
    }

    async fn build_test_provider(
        client: HttpClient,
        base_uri: &str,
        props: &HashMap<String, String>,
        table_auth_props: Option<&HashMap<String, String>>,
    ) -> Result<Option<Arc<dyn StorageCredentialProvider>>> {
        build_vended_credential_provider(
            &client,
            &NoopAuthManager,
            test_factory(base_uri, table_auth_props),
            props,
            table_auth_props.unwrap_or(&HashMap::new()),
            true,
        )
        .await
    }

    async fn test_provider(
        base_uri: &str,
        props: &HashMap<String, String>,
    ) -> Arc<dyn StorageCredentialProvider> {
        build_test_provider(test_client(base_uri), base_uri, props, None)
            .await
            .expect("provider construction should succeed")
            .expect("provider should be built")
    }

    fn s3_response(prefix: &str, access_key_id: &str, expires_at: SystemTime) -> String {
        let expires_at = epoch_millis(expires_at);
        format!(
            r#"{{"storage-credentials":[{{"prefix":"{prefix}","config":{{"s3.access-key-id":"{access_key_id}","s3.secret-access-key":"SK","s3.session-token":"TOK","s3.session-token-expires-at-ms":"{expires_at}"}}}}]}}"#
        )
    }

    #[test]
    fn resolve_endpoint_matches_java_semantics() {
        assert_eq!(
            resolve_endpoint("https://catalog/", "https://other/creds"),
            "https://other/creds"
        );
        assert_eq!(
            resolve_endpoint("https://catalog", "http://other/creds"),
            "http://other/creds"
        );
        assert_eq!(
            resolve_endpoint("https://catalog", "v1/creds"),
            "https://catalog/v1/creds"
        );
        assert_eq!(
            resolve_endpoint("https://catalog/", "v1/creds"),
            "https://catalog/v1/creds"
        );
        // All trailing slashes stripped from the base (Java stripTrailingSlash).
        assert_eq!(
            resolve_endpoint("https://catalog///", "/v1/creds"),
            "https://catalog/v1/creds"
        );
        // Existing leading slashes on the endpoint are preserved, not collapsed
        // (matches Java's resolveEndpoint, which only prepends when absent).
        assert_eq!(
            resolve_endpoint("https://catalog/", "//v1/creds"),
            "https://catalog//v1/creds"
        );
    }

    #[test]
    fn cloud_selection_accepts_root_prefixes_and_url_schemes() {
        let supported = |location: &str| {
            CloudRefresh::SUPPORTED
                .iter()
                .any(|cloud| cloud.matches_location(location))
        };
        // Java uses these root prefixes for its fallback clients and accepts
        // credentials scoped directly to them.
        assert!(supported("s3"));
        assert!(supported("gs"));
        assert!(supported("s3://b/k"));
        assert!(supported("S3://b/k"));
        assert!(supported("s3a://b/k"));
        assert!(supported("s3n://b/k"));
        assert!(supported("gs://b/k"));
        assert!(supported("gcs://b/k"));
        assert!(supported("abfs"));
        assert!(supported("abfss://fs@acct.dfs.core.windows.net/k"));
        assert!(supported("wasb://fs@acct.blob.core.windows.net/k"));
        assert!(supported("wasbs://fs@acct.blob.core.windows.net/k"));
        assert!(!supported("not a url"));
        assert!(CloudRefresh::AWS.matches_location("s3://bucket/path"));
        assert!(!CloudRefresh::AWS.matches_location("s3evil://bucket/path"));
    }

    #[test]
    fn parses_s3_credential() {
        let prefix = || "s3://bucket".to_string();
        let mut config = HashMap::new();
        assert!(parse_s3_credential(&config, prefix()).is_err());
        config.insert(S3_ACCESS_KEY_ID.to_string(), "AK".to_string());
        assert!(parse_s3_credential(&config, prefix()).is_err());
        config.insert(S3_SECRET_ACCESS_KEY.to_string(), "SK".to_string());
        assert!(parse_s3_credential(&config, prefix()).is_err());
        config.insert(S3_SESSION_TOKEN.to_string(), "TOK".to_string());
        config.insert(
            S3_SESSION_TOKEN_EXPIRES_AT_MS.to_string(),
            "not-a-timestamp".to_string(),
        );
        assert!(parse_s3_credential(&config, prefix()).is_err());

        config.insert(
            S3_SESSION_TOKEN_EXPIRES_AT_MS.to_string(),
            "1500".to_string(),
        );
        config.insert("unrelated".to_string(), "value".to_string());
        let vended = parse_s3_credential(&config, prefix()).unwrap();
        assert_eq!(vended.credential.prefix(), "s3://bucket");
        assert_eq!(vended.expires_at, UNIX_EPOCH + Duration::from_millis(1500));
        // Only the credential's own properties are kept.
        config.remove("unrelated");
        assert_eq!(vended.credential.config(), &config);
    }

    #[test]
    fn parse_gcs_requires_token_and_expiry() {
        let prefix = || "gs://bucket".to_string();
        let mut config = HashMap::new();
        assert!(parse_gcs_credential(&config, prefix()).is_err());
        config.insert(GCS_TOKEN.to_string(), "ya29.token".to_string());
        assert!(parse_gcs_credential(&config, prefix()).is_err());

        config.insert(GCS_TOKEN_EXPIRES_AT.to_string(), "2000".to_string());
        let vended = parse_gcs_credential(&config, prefix()).unwrap();
        assert_eq!(vended.credential.prefix(), "gs://bucket");
        assert_eq!(gcs_token(&vended.credential), "ya29.token");
        assert_eq!(vended.expires_at, UNIX_EPOCH + Duration::from_millis(2000));
    }

    #[test]
    fn parse_azdls_requires_account_specific_token_and_expiry() {
        let prefix = "abfss://container@account1.dfs.core.windows.net/table";
        let token_key = format!("{ADLS_SAS_TOKEN_PREFIX}account1");
        let expiry_key = format!("{ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX}account1");
        let mut config = HashMap::new();

        assert!(parse_azdls_credential(&config, prefix.to_string()).is_err());
        config.insert(token_key, "sv=2026&sig=secret".to_string());
        assert!(parse_azdls_credential(&config, prefix.to_string()).is_err());
        config.insert(expiry_key, "2500".to_string());

        let vended = parse_azdls_credential(&config, prefix.to_string()).unwrap();
        assert_eq!(vended.credential.prefix(), prefix);
        assert_eq!(vended.expires_at, UNIX_EPOCH + Duration::from_millis(2500));
        assert_eq!(sas_token(&vended.credential), "sv=2026&sig=secret");
        assert_eq!(vended.credential.config(), &config);
    }

    #[test]
    fn azdls_tokens_match_the_exact_host_or_the_account() {
        let expires = epoch_millis(SystemTime::now() + Duration::from_secs(3600));
        let config = HashMap::from([
            (
                format!("{ADLS_SAS_TOKEN_PREFIX}acct.dfs.core.windows.net"),
                "host".to_string(),
            ),
            (
                format!("{ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX}acct.dfs.core.windows.net"),
                expires.clone(),
            ),
        ]);
        let token = |prefix: &str, config: &HashMap<String, String>| {
            parse_azdls_credential(config, prefix.to_string())
                .map(|vended| sas_token(&vended.credential).to_string())
        };

        assert_eq!(
            token("abfss://c@acct.dfs.core.windows.net/t", &config).unwrap(),
            "host"
        );
        // The same account name in another cloud is a different account.
        assert!(token("abfss://c@acct.dfs.core.usgovcloudapi.net/t", &config).is_err());

        // A key naming only the account matches any host of it, after the
        // exact host.
        let mut config = config;
        config.extend([
            (
                format!("{ADLS_SAS_TOKEN_PREFIX}acct"),
                "account".to_string(),
            ),
            (
                format!("{ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX}acct"),
                expires,
            ),
        ]);
        assert_eq!(
            token("abfss://c@acct.dfs.core.windows.net/t", &config).unwrap(),
            "host"
        );
        assert_eq!(
            token("abfss://c@acct.dfs.core.usgovcloudapi.net/t", &config).unwrap(),
            "account"
        );
    }

    #[test]
    fn cached_entry_freshness() {
        let far = cached_s3(
            "s3",
            "a",
            SystemTime::now() + REFRESH_BUFFER + Duration::from_secs(60),
        );
        // Within the buffer but not yet expired: stale for a fast-path read, but
        // still usable for graceful degradation.
        let soon = cached_s3("s3", "a", SystemTime::now() + Duration::from_secs(60));
        let past = cached_s3("s3", "a", SystemTime::now() - Duration::from_secs(60));

        let now = SystemTime::now();
        assert!(far.is_fresh(now));
        assert!(!soon.is_fresh(now));
        assert!(soon.is_unexpired(now));
        assert!(!past.is_unexpired(now));
    }

    #[test]
    fn prefetch_time_matches_cloud_policy() {
        let now = SystemTime::now();
        let expires_at = now + Duration::from_secs(3600);
        assert_eq!(
            prefetch_time(now, expires_at, false),
            expires_at - REFRESH_BUFFER
        );

        for _ in 0..16 {
            let refresh_at = prefetch_time(now, expires_at, true);
            assert!(refresh_at >= expires_at - REFRESH_BUFFER);
            assert!(refresh_at < expires_at - MIN_REFRESH_BUFFER);
        }
    }

    #[test]
    fn prefetch_buffer_is_capped_at_half_the_lifetime() {
        let now = SystemTime::now();
        let expires_at = now + Duration::from_secs(120);
        assert_eq!(
            prefetch_time(now, expires_at, false),
            now + Duration::from_secs(60)
        );

        for _ in 0..16 {
            let refresh_at = prefetch_time(now, expires_at, true);
            assert!(refresh_at >= now + Duration::from_secs(60));
            assert!(refresh_at < expires_at - Duration::from_secs(12));
        }

        // An already expired credential is due immediately.
        assert_eq!(prefetch_time(now, now, true), now);
    }

    #[test]
    fn seed_inside_nominal_window_is_immediately_due() {
        let credential = s3_vended(
            "s3",
            "a",
            SystemTime::now() + REFRESH_BUFFER - Duration::from_secs(1),
        );
        let entry = CachedEntry::seed(credential, true);
        let now = SystemTime::now();
        assert!(!entry.is_fresh(now));
        assert!(entry.is_unexpired(now));
    }

    #[test]
    fn failure_backoff_is_jittered_and_capped() {
        for (failures, ceiling) in [
            (1, Duration::from_secs(1)),
            (2, Duration::from_secs(2)),
            (5, Duration::from_secs(16)),
            (6, MAX_FAILURE_BACKOFF),
            (u32::MAX, MAX_FAILURE_BACKOFF),
        ] {
            for _ in 0..16 {
                let backoff = failure_backoff(failures);
                assert!(backoff >= ceiling / 2);
                assert!(backoff <= ceiling);
            }
        }
    }

    #[test]
    fn unexpired_match_selects_longest_unexpired_prefix() {
        let now = SystemTime::now();
        let far = now + REFRESH_BUFFER + Duration::from_secs(3600);
        let cache = CacheState::new(vec![
            cached_s3("s3://bucket", "wide", far),
            cached_s3("s3://bucket/warehouse/db", "narrow", far),
        ]);
        let got = &cache.entries[cache
            .unexpired_match("s3://bucket/warehouse/db/t/f", now)
            .unwrap()];
        assert_eq!(s3_access_key_id(&got.credential), "narrow");
        assert!(cache.unexpired_match("s3://other/x", now).is_none());

        // Freshness does not matter, but an expired narrower entry is skipped.
        let cache = CacheState::new(vec![
            cached_s3("s3://bucket", "wide", far),
            cached_s3("s3://bucket/due", "due", now + Duration::from_secs(60)),
            cached_s3(
                "s3://bucket/table",
                "expired",
                now - Duration::from_secs(60),
            ),
        ]);
        let due = &cache.entries[cache.unexpired_match("s3://bucket/due/f", now).unwrap()];
        assert_eq!(s3_access_key_id(&due.credential), "due");
        assert!(!due.is_fresh(now));
        let wide = &cache.entries[cache.unexpired_match("s3://bucket/table/f", now).unwrap()];
        assert_eq!(s3_access_key_id(&wide.credential), "wide");
    }

    #[tokio::test]
    async fn provider_support_tracks_configured_cloud_endpoints() {
        let client = test_client("http://cat");
        let props = aws_refresh_props("/v1/creds");

        // The provider is configured independently of the table metadata scheme,
        // but advertises support only for clouds whose endpoint is present.
        let provider = build_test_provider(client.clone(), "http://cat", &props, None)
            .await
            .unwrap()
            .unwrap();
        assert!(provider.supports_path("s3://b/k"));
        assert!(!provider.supports_path("abfss://fs@acct.dfs.core.windows.net/k"));
        let enabled = HashMap::from([
            (
                CloudRefresh::AWS.endpoint_key.to_string(),
                "/v1/creds".to_string(),
            ),
            (
                CloudRefresh::AWS.enabled_key.to_string(),
                "True".to_string(),
            ),
        ]);
        assert!(
            build_test_provider(client.clone(), "http://cat", &enabled, None)
                .await
                .unwrap()
                .is_some()
        );
        // No endpoint advertised.
        assert!(
            build_test_provider(client.clone(), "http://cat", &HashMap::new(), None)
                .await
                .unwrap()
                .is_none()
        );
        // Explicitly disabled.
        let disabled = HashMap::from([
            (
                CloudRefresh::AWS.endpoint_key.to_string(),
                "/v1/creds".to_string(),
            ),
            (
                CloudRefresh::AWS.enabled_key.to_string(),
                "false".to_string(),
            ),
        ]);
        assert!(
            build_test_provider(client, "http://cat", &disabled, None)
                .await
                .unwrap()
                .is_none()
        );
        // Java's `Strings.isNullOrEmpty` check treats an empty endpoint as absent.
        let empty = HashMap::from([(CloudRefresh::AWS.endpoint_key.to_string(), String::new())]);
        let client = test_client("http://cat");
        assert!(
            build_test_provider(client, "http://cat", &empty, None)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn unchanged_refreshed_credential_is_not_refetched_near_expiry() {
        let mut server = Server::new_async().await;
        // Millisecond precision, as the catalog sends it.
        let expires_at = UNIX_EPOCH
            + Duration::from_millis(
                (SystemTime::now() + Duration::from_secs(90))
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
            );
        let mock = server
            .mock("GET", "/v1/credentials")
            .expect(1)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(s3_response("s3://bucket", "AK", expires_at))
            .create_async()
            .await;
        // The catalog returns the credential the cache already holds.
        let provider = provider_with_cached_s3(&server.url(), vec![cached_s3(
            "s3://bucket",
            "AK",
            expires_at,
        )]);

        provider.load_credential("s3://bucket/f").await.unwrap();
        let cache = provider.clouds[0].cache.lock().await;
        // With under two minutes left, there is no further check.
        assert_eq!(cache.entries[0].refresh_at, expires_at);
        drop(cache);
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn malformed_narrower_credential_is_not_masked_by_broader_one() {
        let mut server = Server::new_async().await;
        let expires = epoch_millis(SystemTime::now() + Duration::from_secs(3600));
        let body = format!(
            r#"{{"storage-credentials":[
                {{"prefix":"s3://bucket","config":{{"s3.access-key-id":"BROAD_AK","s3.secret-access-key":"SK","s3.session-token":"TOK","s3.session-token-expires-at-ms":"{expires}"}}}},
                {{"prefix":"s3://bucket/table","config":{{"s3.access-key-id":"BAD"}}}}
            ]}}"#
        );
        let mock = server
            .mock("GET", "/v1/credentials")
            .expect(1)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .create_async()
            .await;
        let provider = test_provider(&server.url(), &aws_refresh_props("/v1/credentials")).await;

        let error = provider
            .load_credential("s3://bucket/table/f")
            .await
            .unwrap_err();
        assert!(error.message().contains("is missing or empty"), "{error}");
        // The cached broader credential does not mask the failure later either.
        assert!(
            provider
                .load_credential("s3://bucket/table/other")
                .await
                .is_err()
        );
        // Paths only the broader credential covers still use it.
        assert_eq!(
            s3_access_key_id(
                &provider
                    .load_credential("s3://bucket/other/f")
                    .await
                    .unwrap()
            ),
            "BROAD_AK"
        );
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn failed_refreshes_do_not_duplicate_the_account_seed() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v1/credentials")
            .expect(2)
            .with_status(503)
            .create_async()
            .await;
        let host = "acct.dfs.core.windows.net";
        let azdls = |prefix: String, token: &str, expires_at: SystemTime| VendedCredential {
            credential: StorageCredential::new(
                prefix,
                HashMap::from([(format!("{ADLS_SAS_TOKEN_PREFIX}{host}"), token.to_string())]),
            ),
            expires_at,
        };
        let seed = CachedEntry::seed(
            azdls(
                String::new(),
                "seed",
                SystemTime::now() + Duration::from_secs(60),
            ),
            false,
        );
        let expired = CachedEntry::new(
            azdls(
                format!("abfss://c@{host}/data"),
                "old",
                SystemTime::now() - Duration::from_secs(1),
            ),
            false,
        );
        let configured = ConfiguredCloud::new(
            &CloudRefresh::AZURE,
            format!("{}/v1/credentials", server.url()),
            vec![expired],
            HashMap::from([(host.to_string(), seed)]),
        );
        let provider = RestVendedCredentialProvider {
            client: OnceCell::new_with(Some(test_client(&server.url()))),
            factory: None,
            props: HashMap::new(),
            query_params: Vec::new(),
            clouds: vec![configured],
        };
        let path = format!("abfss://c@{host}/data/f.parquet");

        for _ in 0..2 {
            let fallback = configured_seed(&provider, &path);
            let credential = provider
                .refresh_credential(&provider.clouds[0], &path, fallback)
                .await
                .unwrap();
            assert_eq!(sas_token(&credential), "seed");
        }
        // The expired entry is gone and the seed is cached once.
        assert_eq!(provider.clouds[0].cache.lock().await.entries.len(), 1);
        mock.assert_async().await;
    }

    fn configured_seed(provider: &RestVendedCredentialProvider, path: &str) -> Option<CachedEntry> {
        provider.clouds[0].keyed_seed_for_path(path).unwrap()
    }

    #[tokio::test]
    async fn expired_narrower_credential_falls_back_to_scheme_wide_seed() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v1/credentials")
            .expect(1)
            .with_status(503)
            .create_async()
            .await;
        let seed = CachedEntry::seed(
            s3_vended("s3", "SEED_AK", SystemTime::now() + Duration::from_secs(60)),
            false,
        );
        let expired = CachedEntry::new(
            s3_vended(
                "s3://bucket/table",
                "OLD_AK",
                SystemTime::now() - Duration::from_secs(1),
            ),
            false,
        );
        let provider = provider_with_cached_s3(&server.url(), vec![seed, expired]);

        // As for ADLS account seeds, the expired narrower entry is ignored.
        for _ in 0..2 {
            assert_eq!(
                s3_access_key_id(
                    &provider
                        .load_credential("s3://bucket/table/f")
                        .await
                        .unwrap()
                ),
                "SEED_AK"
            );
        }
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn failed_prefix_clears_once_the_catalog_stops_naming_it() {
        let mut server = Server::new_async().await;
        let expires = epoch_millis(SystemTime::now() + Duration::from_secs(3600));
        let broad = format!(
            r#"{{"prefix":"s3://bucket","config":{{"s3.access-key-id":"BROAD_AK","s3.secret-access-key":"SK","s3.session-token":"TOK","s3.session-token-expires-at-ms":"{expires}"}}}}"#
        );
        let responses = [
            format!(
                r#"{{"storage-credentials":[{broad},{{"prefix":"s3://bucket/table","config":{{"s3.access-key-id":"BAD"}}}}]}}"#
            ),
            // The catalog fixes the table by vending only the broader credential.
            format!(r#"{{"storage-credentials":[{broad}]}}"#),
        ];
        let fetches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = Arc::clone(&fetches);
        let mock = server
            .mock("GET", "/v1/credentials")
            .expect(2)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body_from_request(move |_| {
                let fetch = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                responses[fetch.min(1)].clone().into_bytes()
            })
            .create_async()
            .await;
        let provider = provider_with_cached_s3(&server.url(), Vec::new());

        assert!(
            provider
                .load_credential("s3://bucket/table/f")
                .await
                .is_err()
        );
        // Skip the backoff, then a later fetch no longer names the prefix.
        provider.clouds[0].cache.lock().await.retry_not_before = None;
        provider.clouds[0].cache.lock().await.entries[0].refresh_at = UNIX_EPOCH;
        assert_eq!(
            s3_access_key_id(
                &provider
                    .load_credential("s3://bucket/table/f")
                    .await
                    .unwrap()
            ),
            "BROAD_AK"
        );
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn azdls_seed_is_served_consistently_when_a_broader_credential_fails() {
        let mut server = Server::new_async().await;
        let host = "acct.dfs.core.windows.net";
        // The catalog's credential for the whole container has no expiry.
        let body = format!(
            r#"{{"storage-credentials":[{{"prefix":"abfss://c@{host}","config":{{"adls.sas-token.{host}":"sv=2026&sig=bad"}}}}]}}"#
        );
        let mock = server
            .mock("GET", "/v1/credentials")
            .expect(1)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .create_async()
            .await;
        let props = HashMap::from([
            (
                CloudRefresh::AZURE.endpoint_key.to_string(),
                "/v1/credentials".to_string(),
            ),
            (
                format!("{ADLS_SAS_TOKEN_PREFIX}{host}"),
                "sv=2026&sig=seed".to_string(),
            ),
            (
                format!("{ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX}{host}"),
                epoch_millis(SystemTime::now() + Duration::from_secs(120)),
            ),
        ]);
        let provider = test_provider(&server.url(), &props).await;

        // The failed prefix is not narrower than the seed, so the seed is
        // served on the refresh and during the backoff that follows.
        for _ in 0..2 {
            let credential = provider
                .load_credential(&format!("abfss://c@{host}/data.parquet"))
                .await
                .unwrap();
            assert_eq!(sas_token(&credential), "sv=2026&sig=seed");
        }
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn backoff_error_reports_the_last_failure() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v1/credentials")
            .expect(1)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(s3_response(
                "s3://bucket/table-a",
                "AK",
                SystemTime::now() + Duration::from_secs(3600),
            ))
            .create_async()
            .await;
        let provider = test_provider(&server.url(), &aws_refresh_props("/v1/credentials")).await;

        assert!(
            provider
                .load_credential("s3://bucket/table-b/f")
                .await
                .is_err()
        );
        let error = provider
            .load_credential("s3://bucket/table-b/f")
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("backed off"), "{error}");
        assert!(
            error.contains("no unexpired vended credential matches"),
            "{error}"
        );
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn azdls_container_root_is_not_refetched_on_every_access() {
        let mut server = Server::new_async().await;
        let host = "acct.dfs.core.windows.net";
        let expires = epoch_millis(SystemTime::now() + Duration::from_secs(3600));
        let body = format!(
            r#"{{"storage-credentials":[{{"prefix":"abfss://c@{host}/table","config":{{"adls.sas-token.{host}":"sv=2026&sig=table","adls.sas-token-expires-at-ms.{host}":"{expires}"}}}}]}}"#
        );
        let mock = server
            .mock("GET", "/v1/credentials")
            .expect(1)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .create_async()
            .await;
        // The account seed is inside its refresh window.
        let props = HashMap::from([
            (
                CloudRefresh::AZURE.endpoint_key.to_string(),
                "/v1/credentials".to_string(),
            ),
            (
                format!("{ADLS_SAS_TOKEN_PREFIX}{host}"),
                "sv=2026&sig=seed".to_string(),
            ),
            (
                format!("{ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX}{host}"),
                epoch_millis(SystemTime::now() + Duration::from_secs(240)),
            ),
        ]);
        let provider = test_provider(&server.url(), &props).await;

        for _ in 0..5 {
            let credential = provider
                .load_credential(&format!("abfss://c@{host}"))
                .await
                .unwrap();
            assert_eq!(sas_token(&credential), "sv=2026&sig=seed");
        }
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn malformed_response_errors_do_not_quote_secrets() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v1/credentials")
            .expect(1)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"storage-credentials":[{"prefix":"s3://b","config":"s3.secret-access-key=TOPSECRET"}]}"#,
            )
            .create_async()
            .await;
        let provider = test_provider(&server.url(), &aws_refresh_props("/v1/credentials")).await;

        for _ in 0..2 {
            let error = provider
                .load_credential("s3://b/f")
                .await
                .unwrap_err()
                .to_string();
            assert!(!error.contains("TOPSECRET"), "{error}");
        }
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn catalog_error_messages_are_reported() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v1/credentials")
            .expect(1)
            .with_status(403)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"error":{"message":"not allowed to access the table","type":"ForbiddenException","code":403}}"#,
            )
            .create_async()
            .await;
        let provider = test_provider(&server.url(), &aws_refresh_props("/v1/credentials")).await;

        let error = provider
            .load_credential("s3://b/f")
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("not allowed to access the table"), "{error}");
        assert!(error.contains("ForbiddenException"), "{error}");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn widened_scope_replaces_the_narrower_credential() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v1/credentials")
            .expect(1)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(s3_response(
                "s3://b",
                "WIDE_AK",
                SystemTime::now() + Duration::from_secs(3600),
            ))
            .create_async()
            .await;
        // The narrower credential is inside its refresh window.
        let provider = provider_with_cached_s3(&server.url(), vec![cached_s3(
            "s3://b/t",
            "NARROW_AK",
            SystemTime::now() + Duration::from_secs(240),
        )]);

        for _ in 0..2 {
            assert_eq!(
                s3_access_key_id(&provider.load_credential("s3://b/t/f").await.unwrap()),
                "WIDE_AK"
            );
        }
        assert_eq!(provider.clouds[0].cache.lock().await.entries.len(), 1);
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn azdls_seed_is_not_served_for_a_path_it_does_not_cover() {
        let mut server = Server::new_async().await;
        let host = "acct.dfs.core.windows.net";
        let expires = epoch_millis(SystemTime::now() + Duration::from_secs(3600));
        let body = format!(
            r#"{{"storage-credentials":[{{"prefix":"ABFSS://c@{host}","config":{{"adls.sas-token.{host}":"sv=2026&sig=fetched","adls.sas-token-expires-at-ms.{host}":"{expires}"}}}}]}}"#
        );
        let mock = server
            .mock("GET", "/v1/credentials")
            .expect(1)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .create_async()
            .await;
        let props = HashMap::from([
            (
                CloudRefresh::AZURE.endpoint_key.to_string(),
                "/v1/credentials".to_string(),
            ),
            (
                format!("{ADLS_SAS_TOKEN_PREFIX}{host}"),
                "sv=2026&sig=seed".to_string(),
            ),
            (
                format!("{ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX}{host}"),
                expires.clone(),
            ),
        ]);
        let provider = test_provider(&server.url(), &props).await;

        // The seed's scope has a lowercase scheme, so it does not cover this
        // path, which fetches a credential that does.
        let path = format!("ABFSS://c@{host}/data.parquet");
        let credential = provider.load_credential(&path).await.unwrap();
        assert!(credential.covers(&path));
        assert_eq!(sas_token(&credential), "sv=2026&sig=fetched");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn concurrent_loads_without_a_usable_credential_fetch_once() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v1/credentials")
            .expect(1)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(s3_response(
                "s3://bucket",
                "AK",
                SystemTime::now() + Duration::from_secs(3600),
            ))
            .create_async()
            .await;
        let provider = test_provider(&server.url(), &aws_refresh_props("/v1/credentials")).await;

        // Waiters for the in-flight refresh use its result instead of fetching.
        let loads = (0..8).map(|_| {
            let provider = Arc::clone(&provider);
            tokio::spawn(async move { provider.load_credential("s3://bucket/x/f").await })
        });
        for load in futures::future::join_all(loads).await {
            assert_eq!(s3_access_key_id(&load.unwrap().unwrap()), "AK");
        }
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn failed_credential_requests_do_not_report_the_body() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v1/credentials")
            .expect(1)
            .with_status(500)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"error":{"message":"internal error","type":"ServerError","code":500},"storage-credentials":[{"prefix":"s3://b","config":{"s3.secret-access-key":"TOPSECRET"}}]}"#,
            )
            .create_async()
            .await;
        let provider = test_provider(&server.url(), &aws_refresh_props("/v1/credentials")).await;

        let error = provider
            .load_credential("s3://b/f")
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("internal error"), "{error}");
        assert!(!error.contains("TOPSECRET"), "{error}");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn rebuilt_provider_does_not_infer_another_auth_type() {
        let mut server = Server::new_async().await;
        // The merged FileIO properties hold a `credential`, which would infer
        // oauth2 and exchange it; the catalog resolved no auth.
        let token_mock = server
            .mock("POST", "/v1/oauth/tokens")
            .expect(0)
            .create_async()
            .await;
        let credentials_mock = server
            .mock("GET", "/v1/credentials")
            .match_header("authorization", Matcher::Missing)
            .expect(1)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(s3_response(
                "s3://bucket",
                "AK",
                SystemTime::now() + Duration::from_secs(3600),
            ))
            .create_async()
            .await;
        let mut props = aws_refresh_props("/v1/credentials");
        props.insert("credential".to_string(), "client:secret".to_string());
        let rebuilt = RestVendedCredentialProviderFactory::new(
            server.url(),
            test_table(),
            AUTH_TYPE_NONE,
            false,
            &HashMap::new(),
        )
        .build(&StorageConfig::new().with_props(props))
        .unwrap();

        rebuilt.load_credential("s3://bucket/x/f").await.unwrap();
        token_mock.assert_async().await;
        credentials_mock.assert_async().await;
    }

    #[tokio::test]
    async fn gcs_refresh_requires_a_vended_token() {
        let mut props = HashMap::from([(
            CloudRefresh::GCP.endpoint_key.to_string(),
            "/v1/credentials".to_string(),
        )]);
        // Without a vended token, like Java, GCS keeps its default credentials.
        assert!(
            build_test_provider(test_client("http://cat"), "http://cat", &props, None)
                .await
                .unwrap()
                .is_none()
        );

        props.insert(GCS_TOKEN.to_string(), "TOKEN".to_string());
        let provider = test_provider("http://cat", &props).await;
        assert!(provider.supports_path("gs://bucket/x"));
    }

    #[tokio::test]
    async fn refresh_includes_java_query_parameters() {
        let mut server = Server::new_async().await;
        let body = s3_response(
            "s3://bucket",
            "AK",
            SystemTime::now() + Duration::from_secs(3600),
        );
        let mock = server
            .mock("GET", "/v1/credentials")
            // `referenced-by` is sent verbatim, without encoding `%` again.
            .match_query(Matcher::Exact(
                "planId=scan-plan-1&referenced-by=ns%1Fview,ns%1Fother".to_string(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .create_async()
            .await;

        // No static creds -> no seed -> first load fetches from the endpoint.
        let mut props = aws_refresh_props("/v1/credentials");
        props.extend([
            (
                REST_CATALOG_PROP_SCAN_PLAN_ID.to_string(),
                "scan-plan-1".to_string(),
            ),
            (
                REST_CATALOG_PROP_REFERENCED_BY.to_string(),
                "ns%1Fview,ns%1Fother".to_string(),
            ),
        ]);

        let provider = test_provider(&server.url(), &props).await;
        let credential = provider
            .load_credential("s3://bucket/warehouse/f")
            .await
            .unwrap();
        assert_eq!(credential.prefix(), "s3://bucket");
        assert_eq!(s3_access_key_id(&credential), "AK");
        assert_eq!(
            credential
                .config()
                .get(S3_SESSION_TOKEN)
                .map(String::as_str),
            Some("TOK")
        );
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn successful_refresh_caches_entries_before_selecting_path() {
        let mut server = Server::new_async().await;
        let body = s3_response(
            "s3://bucket/table-a",
            "AK",
            SystemTime::now() + Duration::from_secs(3600),
        );
        let mock = server
            .mock("GET", "/v1/credentials")
            .expect(1)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .create_async()
            .await;

        let props = aws_refresh_props("/v1/credentials");
        let provider = test_provider(&server.url(), &props).await;

        assert!(
            provider
                .load_credential("s3://bucket/table-b/file")
                .await
                .is_err()
        );
        // A successful response with no matching credential is negatively
        // cached instead of immediately hitting the endpoint again.
        assert!(
            provider
                .load_credential("s3://bucket/table-b/file")
                .await
                .is_err()
        );
        assert_eq!(
            s3_access_key_id(
                &provider
                    .load_credential("s3://bucket/table-a/file")
                    .await
                    .unwrap()
            ),
            "AK"
        );
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn malformed_entry_does_not_discard_valid_entries() {
        let mut server = Server::new_async().await;
        let expires = epoch_millis(SystemTime::now() + Duration::from_secs(3600));
        let body = format!(
            r#"{{"storage-credentials":[
                {{"prefix":"s3://bucket/invalid","config":{{"s3.access-key-id":"BAD"}}}},
                {{"prefix":"s3://bucket/valid","config":{{"s3.access-key-id":"AK","s3.secret-access-key":"SK","s3.session-token":"TOK","s3.session-token-expires-at-ms":"{expires}"}}}}
            ]}}"#
        );
        let mock = server
            .mock("GET", "/v1/credentials")
            .expect(1)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .create_async()
            .await;

        let props = aws_refresh_props("/v1/credentials");
        let provider = test_provider(&server.url(), &props).await;

        assert!(
            provider
                .load_credential("s3://bucket/invalid/file")
                .await
                .is_err()
        );
        assert!(
            provider
                .load_credential("s3://bucket/invalid/file")
                .await
                .is_err()
        );
        assert_eq!(
            s3_access_key_id(
                &provider
                    .load_credential("s3://bucket/valid/file")
                    .await
                    .unwrap()
            ),
            "AK"
        );
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn fresh_seed_is_served_without_fetching() {
        let mut server = Server::new_async().await;
        // Any call to the server is a failure: the fresh seed must be reused.
        let mock = server
            .mock("GET", Matcher::Any)
            .expect(0)
            .create_async()
            .await;

        let props = with_s3_seed(
            aws_refresh_props("/v1/credentials"),
            "SEED_AK",
            SystemTime::now() + Duration::from_secs(3600),
        );

        let provider = test_provider(&server.url(), &props).await;
        let credential = provider.load_credential("s3://bucket/x/f").await.unwrap();

        assert_eq!(credential.prefix(), "s3");
        assert_eq!(s3_access_key_id(&credential), "SEED_AK");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn fresh_azdls_seeds_are_scoped_and_served_without_fetching() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", Matcher::Any)
            .expect(0)
            .create_async()
            .await;
        let props = HashMap::from([
            (
                CloudRefresh::AZURE.endpoint_key.to_string(),
                "/v1/credentials".to_string(),
            ),
            (
                format!("{ADLS_SAS_TOKEN_PREFIX}account1"),
                "sv=2026&sig=seed".to_string(),
            ),
            (
                format!("{ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX}account1"),
                epoch_millis(SystemTime::now() + Duration::from_secs(3600)),
            ),
            (
                format!("{ADLS_SAS_TOKEN_PREFIX}account2"),
                "sv=2026&sig=other-seed".to_string(),
            ),
            (
                format!("{ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX}account2"),
                epoch_millis(SystemTime::now() + Duration::from_secs(3600)),
            ),
        ]);
        let provider = test_provider(&server.url(), &props).await;

        let credential = provider
            .load_credential("abfss://container@account1.dfs.core.windows.net/table/data/a.parquet")
            .await
            .unwrap();
        assert_eq!(
            credential.prefix(),
            "abfss://container@account1.dfs.core.windows.net"
        );
        assert_eq!(sas_token(&credential), "sv=2026&sig=seed");

        let credential = provider
            .load_credential("abfss://other-container@account2.dfs.core.windows.net/data/b.parquet")
            .await
            .unwrap();
        assert_eq!(
            credential.prefix(),
            "abfss://other-container@account2.dfs.core.windows.net"
        );
        assert_eq!(sas_token(&credential), "sv=2026&sig=other-seed");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn invalid_azdls_path_is_rejected_before_refresh() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", Matcher::Any)
            .expect(0)
            .create_async()
            .await;
        let props = HashMap::from([(
            CloudRefresh::AZURE.endpoint_key.to_string(),
            "/v1/credentials".to_string(),
        )]);
        let provider = test_provider(&server.url(), &props).await;

        let error = provider
            .load_credential("abfss:///data.parquet")
            .await
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(error.message().contains("no account name"));
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn failed_refresh_is_backed_off_while_credential_is_unexpired() {
        let mut server = Server::new_async().await;
        // A refresh is due (the seed is within the buffer) but the catalog errors.
        let mock = server
            .mock("GET", Matcher::Any)
            .expect(1)
            .with_status(500)
            .create_async()
            .await;

        // Seeded credential is within the refresh buffer but not yet expired.
        let props = with_s3_seed(
            aws_refresh_props("/v1/credentials"),
            "SEED_AK",
            SystemTime::now() + Duration::from_secs(60),
        );

        let provider = test_provider(&server.url(), &props).await;
        // The first refresh fails, but the still-valid seed is served. Immediate
        // follow-up operations stay inside the first jittered backoff window.
        for _ in 0..2 {
            let credential = provider.load_credential("s3://bucket/x/f").await.unwrap();
            assert_eq!(s3_access_key_id(&credential), "SEED_AK");
        }
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn empty_refresh_preserves_unexpired_seed() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v1/credentials")
            .expect(1)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"storage-credentials":[]}"#)
            .create_async()
            .await;

        let props = with_s3_seed(
            aws_refresh_props("/v1/credentials"),
            "SEED_AK",
            SystemTime::now() + Duration::from_secs(60),
        );
        let provider = test_provider(&server.url(), &props).await;

        for _ in 0..2 {
            let credential = provider.load_credential("s3://bucket/x/f").await.unwrap();
            assert_eq!(s3_access_key_id(&credential), "SEED_AK");
        }
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn malformed_specific_refresh_prefers_fallback_over_valid_broader_entry() {
        let mut server = Server::new_async().await;
        let refreshed_expires = epoch_millis(SystemTime::now() + Duration::from_secs(3600));
        let body = format!(
            r#"{{"storage-credentials":[
                {{"prefix":"s3://bucket/requested","config":{{"s3.access-key-id":"BAD"}}}},
                {{"prefix":"s3://bucket","config":{{"s3.access-key-id":"NEW_AK","s3.secret-access-key":"SK","s3.session-token":"TOK","s3.session-token-expires-at-ms":"{refreshed_expires}"}}}}
            ]}}"#
        );
        let mock = server
            .mock("GET", "/v1/credentials")
            .expect(1)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .create_async()
            .await;

        let provider = provider_with_cached_s3(&server.url(), vec![cached_s3(
            "s3://bucket/requested",
            "SEED_AK",
            SystemTime::now() + Duration::from_secs(60),
        )]);

        let fallback = provider
            .load_credential("s3://bucket/requested/file")
            .await
            .unwrap();
        assert_eq!(s3_access_key_id(&fallback), "SEED_AK");

        // The malformed specific replacement is a failed refresh for this path.
        // Its still-valid fallback wins over the broader fetched credential and
        // is backed off, so an immediate retry does not fetch again.
        let fallback = provider
            .load_credential("s3://bucket/requested/other-file")
            .await
            .unwrap();
        assert_eq!(s3_access_key_id(&fallback), "SEED_AK");

        let refreshed = provider
            .load_credential("s3://bucket/other/file")
            .await
            .unwrap();
        assert_eq!(s3_access_key_id(&refreshed), "NEW_AK");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn invalid_refresh_preserves_other_prefix_fallbacks() {
        let mut server = Server::new_async().await;
        let refreshed_expires = epoch_millis(SystemTime::now() + Duration::from_secs(3600));
        let expired = epoch_millis(SystemTime::now() - Duration::from_secs(60));
        let body = format!(
            r#"{{"storage-credentials":[
                {{"prefix":"s3://bucket/table-a","config":{{"s3.access-key-id":"NEW_AK","s3.secret-access-key":"SK","s3.session-token":"TOK","s3.session-token-expires-at-ms":"{refreshed_expires}"}}}},
                {{"prefix":"s3://bucket/table-b","config":{{"s3.access-key-id":"BAD"}}}},
                {{"prefix":"s3://bucket/table-c","config":{{"s3.access-key-id":"EXPIRED_CK","s3.secret-access-key":"SK","s3.session-token":"TOK","s3.session-token-expires-at-ms":"{expired}"}}}}
            ]}}"#
        );
        let mock = server
            .mock("GET", "/v1/credentials")
            .expect(1)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .create_async()
            .await;

        let provider = provider_with_cached_s3(&server.url(), vec![
            cached_s3(
                "s3://bucket/table-a",
                "OLD_AK",
                SystemTime::now() + Duration::from_secs(60),
            ),
            cached_s3(
                "s3://bucket/table-b",
                "OLDER_BK",
                SystemTime::now() + Duration::from_secs(3600),
            ),
            cached_s3(
                "s3://bucket/table-b",
                "LATEST_BK",
                SystemTime::now() + Duration::from_secs(3600),
            ),
            cached_s3(
                "s3://bucket/table-c",
                "VALID_CK",
                SystemTime::now() + Duration::from_secs(3600),
            ),
        ]);

        let refreshed = provider
            .load_credential("s3://bucket/table-a/file")
            .await
            .unwrap();
        assert_eq!(s3_access_key_id(&refreshed), "NEW_AK");

        let fallback = provider
            .load_credential("s3://bucket/table-b/file")
            .await
            .unwrap();
        assert_eq!(s3_access_key_id(&fallback), "LATEST_BK");

        let fallback = provider
            .load_credential("s3://bucket/table-c/file")
            .await
            .unwrap();
        assert_eq!(s3_access_key_id(&fallback), "VALID_CK");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn scoped_refresh_after_scheme_wide_seed_does_not_back_off() {
        let mut server = Server::new_async().await;
        let body = s3_response(
            "s3://bucket/table",
            "SCOPED_AK",
            SystemTime::now() + Duration::from_secs(3600),
        );
        let mock = server
            .mock("GET", "/v1/credentials")
            .expect(2)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .create_async()
            .await;

        // The scheme-wide seed is inside its refresh window.
        let seed = CachedEntry::seed(
            s3_vended("s3", "SEED_AK", SystemTime::now() + Duration::from_secs(60)),
            false,
        );
        let provider = provider_with_cached_s3(&server.url(), vec![seed]);

        assert_eq!(
            s3_access_key_id(
                &provider
                    .load_credential("s3://bucket/table/data/f.parquet")
                    .await
                    .unwrap()
            ),
            "SCOPED_AK"
        );
        // A location only the seed covers refreshes once, finds no newer
        // credential, and keeps using the seed. This is not a failure, so the
        // cloud is not backed off.
        for location in ["s3://bucket/", "s3://bucket/", "s3://bucket/other/f"] {
            assert_eq!(
                s3_access_key_id(&provider.load_credential(location).await.unwrap()),
                "SEED_AK"
            );
        }
        let cache = provider.clouds[0].cache.lock().await;
        assert_eq!(cache.consecutive_failures, 0);
        assert!(cache.retry_not_before.is_none());
        drop(cache);
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn scoped_refresh_after_azdls_account_seed_does_not_back_off() {
        let mut server = Server::new_async().await;
        let host = "account1.dfs.core.windows.net";
        let expires = epoch_millis(SystemTime::now() + Duration::from_secs(3600));
        let body = format!(
            r#"{{"storage-credentials":[{{"prefix":"abfss://container@{host}/table","config":{{"adls.sas-token.{host}":"sv=2026&sig=scoped","adls.sas-token-expires-at-ms.{host}":"{expires}"}}}}]}}"#
        );
        // One fetch for the container root, which finds nothing newer, and one
        // for an account without credentials. A backed-off cloud would skip
        // the second.
        let mock = server
            .mock("GET", "/v1/credentials")
            .expect(2)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .create_async()
            .await;
        // The account seed is inside its refresh window.
        let props = HashMap::from([
            (
                CloudRefresh::AZURE.endpoint_key.to_string(),
                "/v1/credentials".to_string(),
            ),
            (
                format!("{ADLS_SAS_TOKEN_PREFIX}{host}"),
                "sv=2026&sig=seed".to_string(),
            ),
            (
                format!("{ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX}{host}"),
                epoch_millis(SystemTime::now() + Duration::from_secs(240)),
            ),
        ]);
        let provider = test_provider(&server.url(), &props).await;
        let sas_token = |credential: StorageCredential| sas_token(&credential).to_string();

        let root = format!("abfss://container@{host}/");
        for _ in 0..2 {
            assert_eq!(
                sas_token(provider.load_credential(&root).await.unwrap()),
                "sv=2026&sig=seed"
            );
        }
        assert_eq!(
            sas_token(
                provider
                    .load_credential(&format!("{root}table/data.parquet"))
                    .await
                    .unwrap()
            ),
            "sv=2026&sig=scoped"
        );
        assert!(
            provider
                .load_credential("abfss://container@account2.dfs.core.windows.net/a")
                .await
                .is_err()
        );
        mock.assert_async().await;
    }

    #[test]
    fn recheck_time_stops_near_expiry() {
        let now = SystemTime::now();
        assert_eq!(
            recheck_time(now, now + Duration::from_secs(240)),
            now + Duration::from_secs(120)
        );
        let soon = now + MIN_REFRESH_BUFFER * 2;
        assert_eq!(recheck_time(now, soon), soon);
        // Longer lifetimes keep the regular prefetch time.
        let later = now + Duration::from_secs(3600);
        assert_eq!(recheck_time(now, later), later - REFRESH_BUFFER);
    }

    #[tokio::test]
    async fn concurrent_prefetch_serves_unexpired_credential_without_waiting() {
        let mut server = Server::new_async().await;
        let body = s3_response(
            "s3://bucket",
            "NEW_AK",
            SystemTime::now() + Duration::from_secs(3600),
        );
        let (started_tx, started_rx) = mpsc::channel();
        let release = Arc::new(Barrier::new(2));
        let callback_release = Arc::clone(&release);
        let mock = server
            .mock("GET", "/v1/credentials")
            .expect(1)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_chunked_body(move |writer| {
                started_tx.send(()).unwrap();
                callback_release.wait();
                writer.write_all(body.as_bytes())
            })
            .create_async()
            .await;

        let props = with_s3_seed(
            aws_refresh_props("/v1/credentials"),
            "SEED_AK",
            SystemTime::now() + Duration::from_secs(60),
        );
        let provider = test_provider(&server.url(), &props).await;

        let first_provider = Arc::clone(&provider);
        let first =
            tokio::spawn(async move { first_provider.load_credential("s3://bucket/x/f").await });
        tokio::task::spawn_blocking(move || {
            started_rx.recv_timeout(Duration::from_secs(5)).unwrap()
        })
        .await
        .unwrap();

        let second_provider = Arc::clone(&provider);
        let second =
            tokio::spawn(async move { second_provider.load_credential("s3://bucket/x/f").await });
        for _ in 0..100 {
            if second.is_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
        let completed_without_waiting = second.is_finished();
        release.wait();

        assert!(
            completed_without_waiting,
            "a concurrent prefetch waited instead of using the unexpired credential"
        );
        assert_eq!(s3_access_key_id(&second.await.unwrap().unwrap()), "SEED_AK");
        assert_eq!(s3_access_key_id(&first.await.unwrap().unwrap()), "NEW_AK");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn refresh_uses_table_scoped_token() {
        let mut server = Server::new_async().await;
        let body = s3_response(
            "s3://bucket",
            "AK",
            SystemTime::now() + Duration::from_secs(3600),
        );
        let mock = server
            .mock("GET", "/v1/credentials")
            .match_header("authorization", "Bearer table-token")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .create_async()
            .await;

        let config = RestCatalogConfig::builder()
            .uri(server.url())
            .props(HashMap::from([(
                "token".to_string(),
                "catalog-token".to_string(),
            )]))
            .build();
        let client = HttpClient::new(&config).unwrap();
        let props = HashMap::from([
            (
                CloudRefresh::AWS.endpoint_key.to_string(),
                "/v1/credentials".to_string(),
            ),
            // Local properties win in the FileIO configuration merge, but must
            // not mask auth returned by the table endpoint.
            ("token".to_string(), "user-token".to_string()),
        ]);
        let table_auth = HashMap::from([("token".to_string(), "table-token".to_string())]);
        let auth_manager = OAuth2Manager::new(format!("{}/v1/oauth/tokens", server.url()));
        let provider = build_vended_credential_provider(
            &client,
            &auth_manager,
            test_factory(&server.url(), Some(&table_auth)),
            &props,
            &table_auth,
            true,
        )
        .await
        .unwrap()
        .unwrap();

        provider.load_credential("s3://bucket/x/f").await.unwrap();
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn effective_headers_override_raw_table_headers() {
        let mut server = Server::new_async().await;
        let body = s3_response(
            "s3://bucket",
            "AK",
            SystemTime::now() + Duration::from_secs(3600),
        );
        let mock = server
            .mock("GET", "/v1/credentials")
            .match_header("authorization", "Bearer local-header")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .create_async()
            .await;

        let client = test_client(&server.url());
        let props = HashMap::from([
            (
                CloudRefresh::AWS.endpoint_key.to_string(),
                "/v1/credentials".to_string(),
            ),
            (
                "header.Authorization".to_string(),
                "Bearer local-header".to_string(),
            ),
        ]);
        let table_auth = HashMap::from([
            ("token".to_string(), "table-token".to_string()),
            (
                "header.Authorization".to_string(),
                "Bearer table-header".to_string(),
            ),
        ]);
        let auth_manager = OAuth2Manager::new(format!("{}/v1/oauth/tokens", server.url()));
        let provider = build_vended_credential_provider(
            &client,
            &auth_manager,
            test_factory(&server.url(), Some(&table_auth)),
            &props,
            &table_auth,
            true,
        )
        .await
        .unwrap()
        .unwrap();

        provider.load_credential("s3://bucket/x/f").await.unwrap();
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn one_provider_refreshes_multiple_clouds() {
        let mut server = Server::new_async().await;
        let expires = epoch_millis(SystemTime::now() + Duration::from_secs(3600));
        let aws_body = s3_response(
            "s3://bucket",
            "AK",
            SystemTime::now() + Duration::from_secs(3600),
        );
        let gcp_body = format!(
            r#"{{"storage-credentials":[{{"prefix":"gs://bucket","config":{{"gcs.oauth2.token":"GCS","gcs.oauth2.token-expires-at":"{expires}"}}}}]}}"#
        );
        let aws_mock = server
            .mock("GET", "/v1/aws-credentials")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(aws_body)
            .create_async()
            .await;
        let gcp_mock = server
            .mock("GET", "/v1/gcp-credentials")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(gcp_body)
            .create_async()
            .await;

        let props = HashMap::from([
            (
                CloudRefresh::AWS.endpoint_key.to_string(),
                "/v1/aws-credentials".to_string(),
            ),
            (
                CloudRefresh::GCP.endpoint_key.to_string(),
                "/v1/gcp-credentials".to_string(),
            ),
            // GCS refreshes only a vended token; this one already expired.
            (GCS_TOKEN.to_string(), "EXPIRED".to_string()),
            (
                GCS_TOKEN_EXPIRES_AT.to_string(),
                epoch_millis(SystemTime::now() - Duration::from_secs(60)),
            ),
        ]);
        let provider = test_provider(&server.url(), &props).await;

        assert!(provider.supports_path("s3://bucket/x"));
        assert!(provider.supports_path("gs://bucket/x"));
        assert_eq!(
            s3_access_key_id(&provider.load_credential("s3://bucket/x").await.unwrap()),
            "AK"
        );
        assert_eq!(
            gcs_token(&provider.load_credential("gs://bucket/x").await.unwrap()),
            "GCS"
        );
        aws_mock.assert_async().await;
        gcp_mock.assert_async().await;
    }

    #[tokio::test]
    async fn host_keyed_azdls_credentials_match_java() {
        let mut server = Server::new_async().await;
        let host = "account1.dfs.core.windows.net";
        let expires = epoch_millis(SystemTime::now() + Duration::from_secs(3600));
        let prefix = format!("abfss://container@{host}/table");
        let body = format!(
            r#"{{"storage-credentials":[{{"prefix":"{prefix}","config":{{"adls.sas-token.{host}":"sv=2026&sig=refreshed","adls.sas-token-expires-at-ms.{host}":"{expires}"}}}}]}}"#
        );
        let mock = server
            .mock("GET", "/v1/azure-credentials")
            .expect(1)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .create_async()
            .await;
        let props = HashMap::from([
            (
                CloudRefresh::AZURE.endpoint_key.to_string(),
                "/v1/azure-credentials".to_string(),
            ),
            (
                format!("{ADLS_SAS_TOKEN_PREFIX}{host}"),
                "sv=2026&sig=seed".to_string(),
            ),
            (
                format!("{ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX}{host}"),
                epoch_millis(SystemTime::now() + Duration::from_secs(3600)),
            ),
        ]);
        let provider = test_provider(&server.url(), &props).await;
        let sas_token = |credential: StorageCredential| sas_token(&credential).to_string();

        // The host-keyed seed serves the account without fetching.
        let seeded = provider
            .load_credential(&format!("abfss://other@{host}/data.parquet"))
            .await
            .unwrap();
        assert_eq!(sas_token(seeded), "sv=2026&sig=seed");

        // An account without a seed refreshes. The response is parsed with
        // host-keyed tokens, and its entry then wins over the broader seed.
        let uncovered = provider
            .load_credential("abfss://container@account2.dfs.core.windows.net/table/a")
            .await;
        assert!(uncovered.is_err());
        let refreshed = provider
            .load_credential(&format!("{prefix}/data.parquet"))
            .await
            .unwrap();
        assert_eq!(sas_token(refreshed), "sv=2026&sig=refreshed");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn refreshes_azdls_credentials() {
        let mut server = Server::new_async().await;
        let expires = epoch_millis(SystemTime::now() + Duration::from_secs(3600));
        let prefix = "abfss://container@account1.dfs.core.windows.net/table";
        let body = format!(
            r#"{{"storage-credentials":[{{"prefix":"{prefix}","config":{{"adls.sas-token.account1":"sv=2026&sig=secret","adls.sas-token-expires-at-ms.account1":"{expires}"}}}}]}}"#
        );
        let mock = server
            .mock("GET", "/v1/azure-credentials")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .create_async()
            .await;
        let props = HashMap::from([(
            CloudRefresh::AZURE.endpoint_key.to_string(),
            "/v1/azure-credentials".to_string(),
        )]);
        let provider = test_provider(&server.url(), &props).await;

        let credential = provider
            .load_credential(&format!("{prefix}/data.parquet"))
            .await
            .unwrap();
        assert_eq!(sas_token(&credential), "sv=2026&sig=secret");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn refresh_failure_never_serves_expired_credential() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", Matcher::Any)
            .expect(1)
            .with_status(500)
            .create_async()
            .await;

        let props = with_s3_seed(
            aws_refresh_props("/v1/credentials"),
            "EXPIRED_AK",
            SystemTime::now() - Duration::from_secs(60),
        );

        let provider = test_provider(&server.url(), &props).await;

        assert!(provider.load_credential("s3://bucket/x/f").await.is_err());
        assert!(provider.load_credential("s3://bucket/x/f").await.is_err());
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn failed_azdls_refresh_uses_account_seed_until_expiry() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v1/credentials")
            .expect(1)
            .with_status(503)
            .with_header("content-type", "application/json")
            .with_body(r#"{"error":{"message":"unavailable","type":"test","code":503}}"#)
            .create_async()
            .await;
        let props = HashMap::from([
            (
                CloudRefresh::AZURE.endpoint_key.to_string(),
                "/v1/credentials".to_string(),
            ),
            (
                format!("{ADLS_SAS_TOKEN_PREFIX}account2"),
                "sv=2026&sig=seed".to_string(),
            ),
            (
                format!("{ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX}account2"),
                epoch_millis(SystemTime::now() + Duration::from_secs(60)),
            ),
        ]);
        let provider = test_provider(&server.url(), &props).await;
        let path = "abfss://container@account2.dfs.core.windows.net/data/a.parquet";

        for _ in 0..2 {
            let credential = provider.load_credential(path).await.unwrap();
            assert_eq!(
                credential.prefix(),
                "abfss://container@account2.dfs.core.windows.net"
            );
            assert_eq!(sas_token(&credential), "sv=2026&sig=seed");
        }
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn sub_buffer_ttl_is_reused_until_half_its_lifetime() {
        let mut server = Server::new_async().await;
        // The vended TTL (60s) is shorter than REFRESH_BUFFER. Java would treat
        // it as due on arrival and fetch on every operation; the capped buffer
        // keeps it for half its lifetime.
        let body = s3_response(
            "s3://bucket",
            "AK",
            SystemTime::now() + Duration::from_secs(60),
        );
        let mock = server
            .mock("GET", "/v1/credentials")
            .expect(1)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .create_async()
            .await;

        let provider = test_provider(&server.url(), &aws_refresh_props("/v1/credentials")).await;

        for _ in 0..2 {
            let credential = provider.load_credential("s3://bucket/x/f").await.unwrap();
            assert_eq!(s3_access_key_id(&credential), "AK");
        }
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn refreshed_credentials_must_be_complete() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v1/credentials")
            .expect(1)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"storage-credentials":[{"prefix":"s3://bucket","config":{"s3.access-key-id":"AK","s3.secret-access-key":"SK"}}]}"#,
            )
            .create_async()
            .await;

        let provider = test_provider(&server.url(), &aws_refresh_props("/v1/credentials")).await;
        let error = provider
            .load_credential("s3://bucket/x/f")
            .await
            .unwrap_err();
        assert!(error.message().contains("is missing or empty"), "{error}");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn prefixes_match_locations_like_java() {
        let mut server = Server::new_async().await;
        let body = s3_response(
            "s3://bucket/table",
            "AK",
            SystemTime::now() + Duration::from_secs(3600),
        );
        let mock = server
            .mock("GET", "/v1/credentials")
            .expect(2)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .create_async()
            .await;

        let provider = test_provider(&server.url(), &aws_refresh_props("/v1/credentials")).await;
        // As in Java, prefixes are compared as plain strings.
        for path in ["s3://bucket/table/f", "s3://bucket/table2/f"] {
            assert_eq!(
                s3_access_key_id(&provider.load_credential(path).await.unwrap()),
                "AK"
            );
        }
        // `s3a` is another scheme, so this path is not covered and refreshes.
        assert!(
            provider
                .load_credential("s3a://bucket/table/f")
                .await
                .is_err()
        );
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn serialized_provider_reconnects_from_properties() {
        let mut server = Server::new_async().await;
        let body = s3_response(
            "s3://bucket",
            "AK",
            SystemTime::now() + Duration::from_secs(3600),
        );
        let mock = server
            .mock("GET", "/v1/credentials")
            .match_header("authorization", "Bearer table-token")
            .match_header("x-custom", "value")
            .expect(2)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .create_async()
            .await;

        let mut props = aws_refresh_props("/v1/credentials");
        props.extend([
            ("token".to_string(), "catalog-token".to_string()),
            ("header.x-custom".to_string(), "value".to_string()),
        ]);
        let table_auth = HashMap::from([("token".to_string(), "table-token".to_string())]);
        let provider = build_vended_credential_provider(
            &test_client(&server.url()),
            &OAuth2Manager::new(format!("{}/v1/oauth/tokens", server.url())),
            test_factory(&server.url(), Some(&table_auth)),
            &props,
            &table_auth,
            true,
        )
        .await
        .unwrap()
        .unwrap();
        provider.load_credential("s3://bucket/x/f").await.unwrap();

        let factory: Arc<dyn StorageCredentialProviderFactory> =
            serde_json::from_str(&serde_json::to_string(&provider.factory().unwrap()).unwrap())
                .unwrap();
        let config = StorageConfig::new().with_props(props);
        let rebuilt = factory.build(&config).unwrap();

        // The rebuilt provider authenticates like the original one.
        assert_eq!(
            s3_access_key_id(&rebuilt.load_credential("s3://bucket/x/f").await.unwrap()),
            "AK"
        );
        // The rebuilt provider is itself serializable.
        assert!(rebuilt.factory().is_ok());
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn table_token_authenticates_without_catalog_auth() {
        let mut server = Server::new_async().await;
        let body = s3_response(
            "s3://bucket",
            "AK",
            SystemTime::now() + Duration::from_secs(3600),
        );
        // As in Java, a table token authenticates credential requests of a
        // catalog without auth, in process and after the provider is rebuilt.
        let mock = server
            .mock("GET", "/v1/credentials")
            .match_header("authorization", "Bearer table-token")
            .expect(2)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .create_async()
            .await;

        let props = aws_refresh_props("/v1/credentials");
        let factory = RestVendedCredentialProviderFactory::new(
            server.url(),
            test_table(),
            AUTH_TYPE_NONE,
            true,
            &HashMap::from([("token".to_string(), "table-token".to_string())]),
        );
        let provider = build_vended_credential_provider(
            &test_client(&server.url()),
            &NoopAuthManager,
            factory.clone(),
            &props,
            &HashMap::from([("token".to_string(), "table-token".to_string())]),
            true,
        )
        .await
        .unwrap()
        .unwrap();
        provider.load_credential("s3://bucket/x/f").await.unwrap();

        let rebuilt = factory
            .build(&StorageConfig::new().with_props(props))
            .unwrap();
        rebuilt.load_credential("s3://bucket/x/f").await.unwrap();
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn explicit_no_auth_ignores_the_table_token() {
        let mut server = Server::new_async().await;
        // As in Java, an explicit `rest.auth.type=none` is not overridden by a
        // table token, in process and after the provider is rebuilt.
        let mock = server
            .mock("GET", "/v1/credentials")
            .match_header("authorization", Matcher::Missing)
            .expect(2)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(s3_response(
                "s3://bucket",
                "AK",
                SystemTime::now() + Duration::from_secs(3600),
            ))
            .create_async()
            .await;
        let table_config = HashMap::from([("token".to_string(), "table-token".to_string())]);
        let props = aws_refresh_props("/v1/credentials");
        let factory = RestVendedCredentialProviderFactory::new(
            server.url(),
            test_table(),
            AUTH_TYPE_NONE,
            false,
            &table_config,
        );
        let provider = build_vended_credential_provider(
            &test_client(&server.url()),
            &NoopAuthManager,
            factory.clone(),
            &props,
            &table_config,
            true,
        )
        .await
        .unwrap()
        .unwrap();
        provider.load_credential("s3://bucket/x/f").await.unwrap();
        let rebuilt = factory
            .build(&StorageConfig::new().with_props(props))
            .unwrap();
        rebuilt.load_credential("s3://bucket/x/f").await.unwrap();
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn injected_auth_manager_receives_the_whole_table_config() {
        #[derive(Debug, Default)]
        struct RecordingAuthManager(std::sync::Mutex<Vec<String>>);

        #[async_trait]
        impl AuthManager for RecordingAuthManager {
            async fn init_session(
                &self,
                _client: &HttpClient,
                _props: &HashMap<String, String>,
            ) -> Result<Box<dyn crate::auth::AuthSession>> {
                unreachable!("only table sessions are created")
            }

            async fn catalog_session(
                &self,
                _client: &HttpClient,
                _props: &HashMap<String, String>,
            ) -> Result<Arc<dyn crate::auth::AuthSession>> {
                unreachable!("only table sessions are created")
            }

            async fn table_session(
                &self,
                _client: &HttpClient,
                _table: &TableIdent,
                props: &HashMap<String, String>,
                parent: Arc<dyn crate::auth::AuthSession>,
            ) -> Result<Arc<dyn crate::auth::AuthSession>> {
                let mut keys = props.keys().cloned().collect::<Vec<_>>();
                keys.sort();
                *self.0.lock().unwrap() = keys;
                Ok(parent)
            }
        }

        let manager = RecordingAuthManager::default();
        let table_config = HashMap::from([
            ("token".to_string(), "table-token".to_string()),
            ("custom.auth".to_string(), "value".to_string()),
        ]);
        build_vended_credential_provider(
            &test_client("http://cat"),
            &manager,
            test_factory("http://cat", Some(&table_config)),
            &aws_refresh_props("/v1/creds"),
            &table_config,
            false,
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(*manager.0.lock().unwrap(), vec![
            "custom.auth".to_string(),
            "token".to_string()
        ]);
    }

    #[tokio::test]
    async fn injected_auth_manager_decides_on_the_table_token() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v1/credentials")
            .match_header("authorization", Matcher::Missing)
            .expect(1)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(s3_response(
                "s3://bucket",
                "AK",
                SystemTime::now() + Duration::from_secs(3600),
            ))
            .create_async()
            .await;
        let provider = build_vended_credential_provider(
            &test_client(&server.url()),
            &NoopAuthManager,
            RestVendedCredentialProviderFactory::new(
                server.url(),
                test_table(),
                AUTH_TYPE_NONE,
                true,
                &HashMap::from([("token".to_string(), "table-token".to_string())]),
            ),
            &aws_refresh_props("/v1/credentials"),
            &HashMap::from([("token".to_string(), "table-token".to_string())]),
            false,
        )
        .await
        .unwrap()
        .unwrap();

        provider.load_credential("s3://bucket/x/f").await.unwrap();
        mock.assert_async().await;
    }

    #[test]
    fn factory_keeps_only_table_auth_config() {
        let factory = RestVendedCredentialProviderFactory::new(
            "http://cat",
            test_table(),
            AUTH_TYPE_OAUTH2,
            true,
            &HashMap::from([
                ("token".to_string(), "table-token".to_string()),
                (S3_SECRET_ACCESS_KEY.to_string(), "SECRET".to_string()),
            ]),
        );
        assert_eq!(
            factory.table_auth,
            HashMap::from([("token".to_string(), "table-token".to_string())])
        );
    }

    #[tokio::test]
    async fn provider_with_injected_auth_manager_is_not_serializable() {
        let provider = build_vended_credential_provider(
            &test_client("http://cat"),
            &NoopAuthManager,
            test_factory("http://cat", None),
            &aws_refresh_props("/v1/creds"),
            &HashMap::new(),
            false,
        )
        .await
        .unwrap()
        .unwrap();

        let error = provider.factory().unwrap_err();
        assert_eq!(error.kind(), ErrorKind::FeatureUnsupported);
        assert!(error.message().contains("injected AuthManager"), "{error}");
    }

    #[test]
    fn factory_debug_omits_table_auth() {
        let factory = test_factory(
            "http://cat",
            Some(&HashMap::from([(
                "token".to_string(),
                "secret-token".to_string(),
            )])),
        );
        let debug = format!("{factory:?}");
        assert!(!debug.contains("secret-token"), "{debug}");
    }
}
