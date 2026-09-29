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
//! each configured cloud. The path being accessed selects the cloud cache, and
//! the returned [`StorageCredential`] enum lets the storage adapter enforce the
//! expected backend-specific type. This preserves Java's per-cloud credential
//! selection and prefetch policies while supporting mixed-cloud tables through
//! a resolving FileIO. Unlike Java's scheduled refresh, which permanently stops
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
//! add a backend, first add its credential type to Iceberg's storage API and
//! teach the storage adapter to consume it. Then write its `parse_*` function,
//! add a `CloudRefresh` constant, and list it in [`CloudRefresh::SUPPORTED`].

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use iceberg::io::{
    ADLS_REFRESH_CREDENTIALS_ENABLED, ADLS_REFRESH_CREDENTIALS_ENDPOINT,
    ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX, ADLS_SAS_TOKEN_PREFIX, AWS_REFRESH_CREDENTIALS_ENABLED,
    AWS_REFRESH_CREDENTIALS_ENDPOINT, AzdlsCredential, GCS_REFRESH_CREDENTIALS_ENABLED,
    GCS_REFRESH_CREDENTIALS_ENDPOINT, GCS_TOKEN, GCS_TOKEN_EXPIRES_AT, GcsCredential,
    S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN, S3_SESSION_TOKEN_EXPIRES_AT_MS,
    S3Credential, StorageConfig, StorageCredential, StorageCredentialKind,
    StorageCredentialProvider, StorageCredentialProviderFactory, storage_prefix_covers,
};
use iceberg::{Error, ErrorKind, Result, TableIdent};
use rand::Rng;
use reqwest::{Method, StatusCode, Url};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, OnceCell};

use crate::auth::{AuthManager, load_auth_manager};
use crate::catalog::{REST_CATALOG_PROP_SCAN_PLAN_ID, RestCatalogConfig};
use crate::client::{HttpClient, unexpected_catalog_error_without_body};
use crate::request::HttpRequest;
use crate::types::LoadCredentialsResponse;

type CredentialParser =
    fn(config: &HashMap<String, String>, prefix: Option<String>) -> Result<StorageCredential>;
type KeyedSeedCredentialParser =
    fn(config: &HashMap<String, String>) -> HashMap<String, StorageCredential>;
type KeyedSeedPathResolver = fn(path: &str) -> Result<KeyedSeedPath>;

enum SeedStrategy {
    /// One credential stored in the backend's flat properties.
    Flat,
    /// Credentials selected by a backend-specific key derived from each path.
    Keyed(KeyedSeedStrategy),
}

struct KeyedSeedStrategy {
    parse_credentials: KeyedSeedCredentialParser,
    resolve_path: KeyedSeedPathResolver,
}

struct KeyedSeedPath {
    key: String,
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
        jitter_prefetch: true,
        parse_credential: parse_s3_credential,
        seed_strategy: SeedStrategy::Flat,
    };
    /// Google Cloud Storage
    const GCP: Self = Self {
        schemes: &["gs", "gcs"],
        endpoint_key: GCS_REFRESH_CREDENTIALS_ENDPOINT,
        enabled_key: GCS_REFRESH_CREDENTIALS_ENABLED,
        jitter_prefetch: false,
        parse_credential: parse_gcs_credential,
        seed_strategy: SeedStrategy::Flat,
    };
    /// Azure Data Lake Storage
    const AZURE: Self = Self {
        schemes: &["abfs", "abfss", "wasb", "wasbs"],
        endpoint_key: ADLS_REFRESH_CREDENTIALS_ENDPOINT,
        enabled_key: ADLS_REFRESH_CREDENTIALS_ENABLED,
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
    /// When this entry becomes eligible for prefetch. `None` means it does not
    /// expire and therefore never needs proactive refresh.
    refresh_at: Option<SystemTime>,
}

impl CachedEntry {
    fn new(credential: StorageCredential, jitter_prefetch: bool) -> Self {
        let refresh_at = credential
            .expires_at()
            .map(|expires_at| prefetch_time(SystemTime::now(), expires_at, jitter_prefetch));
        Self {
            credential,
            refresh_at,
        }
    }

    /// Seed entries that are already inside the nominal five-minute window are
    /// immediately due. Otherwise AWS applies the same jitter as it does to a
    /// freshly fetched value.
    fn seed(credential: StorageCredential, jitter_prefetch: bool) -> Self {
        let due = credential.expires_at().is_some_and(|expires_at| {
            SystemTime::now()
                .checked_add(REFRESH_BUFFER)
                .is_none_or(|refresh_boundary| refresh_boundary >= expires_at)
        });
        let mut entry = Self::new(credential, jitter_prefetch);
        if due {
            entry.refresh_at = Some(UNIX_EPOCH);
        }
        entry
    }

    fn is_fresh(&self, now: SystemTime) -> bool {
        self.refresh_at.is_none_or(|refresh_at| now < refresh_at)
    }

    fn is_unexpired(&self, now: SystemTime) -> bool {
        self.credential
            .expires_at()
            .is_none_or(|expires_at| now < expires_at)
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
    consecutive_failures: u32,
    retry_not_before: Option<Instant>,
}

impl CacheState {
    fn new(entries: Vec<CachedEntry>) -> Self {
        Self {
            entries,
            consecutive_failures: 0,
            retry_not_before: None,
        }
    }

    fn record_success(&mut self) {
        self.consecutive_failures = 0;
        self.retry_not_before = None;
    }

    fn record_failure(&mut self) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.retry_not_before =
            Instant::now().checked_add(failure_backoff(self.consecutive_failures));
    }

    /// Replace cached credentials with the fetched ones, prefix by prefix.
    ///
    /// An unexpired cached credential survives when the response carries no
    /// valid replacement for its prefix, so an absent or malformed entry never
    /// evicts a usable credential. Returns the prefixes the response replaced.
    fn merge(&mut self, fetched: Vec<CachedEntry>, now: SystemTime) -> HashSet<String> {
        let fetched_prefixes = fetched
            .iter()
            .filter_map(|entry| entry.credential.prefix().map(str::to_owned))
            .collect::<HashSet<_>>();
        self.entries.retain(|entry| {
            entry.is_unexpired(now)
                && entry
                    .credential
                    .prefix()
                    .is_none_or(|prefix| !fetched_prefixes.contains(prefix))
        });
        self.entries.extend(fetched);
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
            .is_none_or(|value| value.eq_ignore_ascii_case("true"));
        let endpoint = props
            .get(cloud.endpoint_key)
            .filter(|endpoint| enabled && !endpoint.is_empty())
            .map(|endpoint| resolve_endpoint(base_uri, endpoint))?;

        let mut entries = Vec::new();
        let mut keyed_seeds = HashMap::new();
        match &cloud.seed_strategy {
            SeedStrategy::Flat => {
                if let Ok(credential) = (cloud.parse_credential)(props, None) {
                    entries.push(CachedEntry::seed(credential, cloud.jitter_prefetch));
                }
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
        let Some(seed) = self.keyed_seeds.get(&resolved.key) else {
            return Ok(None);
        };
        Ok(Some(CachedEntry {
            credential: seed.credential.clone().with_prefix(resolved.scope),
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
    /// The unmerged config returned by the table endpoint, from which the
    /// auth manager derives a table session. Keeping it separate prevents
    /// local FileIO overrides from masking table auth.
    table_config: HashMap<String, String>,
}

impl RestVendedCredentialProviderFactory {
    pub(crate) fn new(
        catalog_uri: impl Into<String>,
        table: TableIdent,
        table_config: HashMap<String, String>,
    ) -> Self {
        Self {
            catalog_uri: catalog_uri.into(),
            table,
            table_config,
        }
    }

    /// Connect to the catalog from the FileIO properties, as the catalog would.
    async fn connect(&self, props: &HashMap<String, String>) -> Result<HttpClient> {
        let config = RestCatalogConfig::builder()
            .uri(self.catalog_uri.clone())
            .props(props.clone())
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
            &self.table_config,
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
async fn table_client(
    catalog_client: &HttpClient,
    auth_manager: &dyn AuthManager,
    table: &TableIdent,
    props: &HashMap<String, String>,
    table_config: &HashMap<String, String>,
) -> Result<HttpClient> {
    let session = auth_manager
        .table_session(
            &catalog_client.without_auth_session(),
            table,
            table_config,
            catalog_client.auth_session(),
        )
        .await?;
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
    /// Optional scan-plan identifier.
    plan_id: Option<String>,
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
            plan_id: props.get(REST_CATALOG_PROP_SCAN_PLAN_ID).cloned(),
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
        let mut request = client.request(Method::GET, &configured.endpoint);
        if let Some(plan_id) = &self.plan_id {
            request = request.query(&[("planId", plan_id)]);
        }
        let request = HttpRequest::build(request)?;
        let response = client.query_catalog(request).await?;

        if response.status() != StatusCode::OK {
            return Err(unexpected_catalog_error_without_body(
                response,
                client.disable_header_redaction(),
            ));
        }

        // Credential responses contain secrets. Do not include the response
        // body in a deserialization error.
        let parsed: LoadCredentialsResponse =
            serde_json::from_slice(response.body()).map_err(|error| {
                Error::new(
                    ErrorKind::Unexpected,
                    "failed to parse vended credential response",
                )
                .with_source(error)
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
            let parsed = (cloud.parse_credential)(&credential.config, Some(prefix.clone()))
                .map(|credential| CachedEntry::new(credential, cloud.jitter_prefetch))
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

        let (fetched_prefixes, failure) = match fetched {
            Ok(ParsedCredentials { entries, errors }) => {
                let failure = errors
                    .into_iter()
                    .filter(|error| storage_prefix_covers(&error.prefix, path))
                    .max_by_key(|error| error.prefix.len())
                    .map(|error| error.error);
                (cache.merge(entries, now), failure)
            }
            Err(error) => (HashSet::new(), Some(error)),
        };

        let selected = longest_prefix_match(&cache.entries, path)
            .filter(|entry| entry.is_unexpired(now))
            .cloned();
        let refreshed = selected.as_ref().is_some_and(|entry| {
            entry
                .credential
                .prefix()
                .is_some_and(|prefix| fetched_prefixes.contains(prefix))
        });
        if refreshed {
            cache.record_success();
        } else {
            // Graceful degradation: while a credential for this path remains
            // usable, serve it and retry after jittered backoff. Expired
            // credentials are never served.
            cache.record_failure();
        }

        selected
            .or_else(|| fallback.filter(|fallback| fallback.is_unexpired(now)))
            .map(|entry| entry.credential)
            .ok_or_else(|| {
                failure.unwrap_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        format!("no unexpired vended credential matches storage location: {path}"),
                    )
                })
            })
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
    Backoff,
}

fn refresh_backoff_error(path: &str) -> Error {
    Error::new(
        ErrorKind::Unexpected,
        format!("vended credential refresh is temporarily backed off for storage location: {path}"),
    )
}

async fn cache_decision(configured: &ConfiguredCloud, path: &str) -> Result<CacheDecision> {
    let keyed_seed = configured.keyed_seed_for_path(path)?;
    let cache = configured.cache.lock().await;
    let now = SystemTime::now();
    let current = match longest_prefix_match(&cache.entries, path).cloned() {
        Some(cached) if cached.is_unexpired(now) => Some(cached),
        Some(expired) => keyed_seed.or(Some(expired)),
        None => keyed_seed,
    };

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
            .unwrap_or(CacheDecision::Backoff));
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
            CacheDecision::Backoff => return Err(refresh_backoff_error(path)),
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
            CacheDecision::Backoff => return Err(refresh_backoff_error(path)),
        };

        self.refresh_credential(configured, path, current).await
    }

    fn factory(&self) -> Result<Arc<dyn StorageCredentialProviderFactory>> {
        match &self.factory {
            Some(factory) => Ok(Arc::new(factory.clone())),
            None => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "the vended credential provider cannot be serialized because the REST catalog \
                 uses an injected AuthManager, which cannot be rebuilt in another process; use \
                 FileIO::without_credential_provider to serialize without credential refresh",
            )),
        }
    }
}

/// Select the credential whose prefix is the longest match for `path`.
fn longest_prefix_match<'a>(entries: &'a [CachedEntry], path: &str) -> Option<&'a CachedEntry> {
    entries
        .iter()
        .filter(|entry| entry.credential.covers(path))
        .max_by_key(|entry| entry.credential.prefix().map_or(0, str::len))
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
    portable: bool,
) -> Result<Option<Arc<dyn StorageCredentialProvider>>> {
    let Some(provider) = RestVendedCredentialProvider::configure(&factory, props) else {
        return Ok(None);
    };

    let client = table_client(
        catalog_client,
        auth_manager,
        &factory.table,
        props,
        &factory.table_config,
    )
    .await?;
    Ok(Some(Arc::new(RestVendedCredentialProvider {
        client: OnceCell::new_with(Some(client)),
        factory: portable.then_some(factory),
        ..provider
    })))
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

/// The URL scheme of `location`, lowercased (e.g. `"s3"` for `s3://bucket/k`).
fn scheme_of(location: &str) -> Option<String> {
    Url::parse(location)
        .ok()
        .map(|url| url.scheme().to_string())
}

/// Parse a complete S3 credential supplied by the catalog.
fn parse_s3_credential(
    config: &HashMap<String, String>,
    prefix: Option<String>,
) -> Result<StorageCredential> {
    let access_key_id = required_nonempty(config, S3_ACCESS_KEY_ID)?;
    let secret_access_key = required_nonempty(config, S3_SECRET_ACCESS_KEY)?;
    let session_token = required_nonempty(config, S3_SESSION_TOKEN)?;
    let expires_at = required_epoch_millis(config, S3_SESSION_TOKEN_EXPIRES_AT_MS)?;
    Ok(with_prefix(
        StorageCredential::new(StorageCredentialKind::S3(S3Credential::new(
            access_key_id,
            secret_access_key,
            Some(session_token),
        )))
        .with_expiration(expires_at),
        prefix,
    ))
}

/// Parse a complete GCS credential supplied by the catalog.
fn parse_gcs_credential(
    config: &HashMap<String, String>,
    prefix: Option<String>,
) -> Result<StorageCredential> {
    let token = required_nonempty(config, GCS_TOKEN)?;
    let expires_at = required_epoch_millis(config, GCS_TOKEN_EXPIRES_AT)?;
    Ok(with_prefix(
        StorageCredential::new(StorageCredentialKind::Gcs(GcsCredential::new(token)))
            .with_expiration(expires_at),
        prefix,
    ))
}

fn with_prefix(credential: StorageCredential, prefix: Option<String>) -> StorageCredential {
    match prefix {
        Some(prefix) => credential.with_prefix(prefix),
        None => credential,
    }
}

/// Parse a complete account-specific ADLS SAS credential supplied by the catalog.
fn parse_azdls_credential(
    config: &HashMap<String, String>,
    prefix: Option<String>,
) -> Result<StorageCredential> {
    let prefix = prefix.ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            "invalid vended ADLS credential: storage prefix is missing",
        )
    })?;
    let account = azdls_account_name(&prefix)?;
    let (suffix, sas_token) = azdls_sas_tokens(config)
        .filter(|(_, token_account, _)| *token_account == account)
        .map(|(suffix, _, token)| (suffix, token))
        // Prefer the host-keyed token when both forms name the account, as
        // the storage backend does.
        .max()
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("invalid vended credential: no {ADLS_SAS_TOKEN_PREFIX}* token for account {account}"),
            )
        })?;
    let expires_at = required_epoch_millis(
        config,
        &format!("{ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX}{suffix}"),
    )?;
    Ok(
        StorageCredential::new(StorageCredentialKind::Azdls(AzdlsCredential::new(
            sas_token,
        )))
        .with_prefix(prefix)
        .with_expiration(expires_at),
    )
}

/// Parse every complete account-qualified ADLS credential from the initial
/// table properties. Unlike a URI prefix, an Azure account occurs after the
/// filesystem in a location, so these seeds are selected by account name.
fn parse_azdls_account_seeds(
    config: &HashMap<String, String>,
) -> HashMap<String, StorageCredential> {
    let mut seeds = azdls_sas_tokens(config)
        .filter_map(|(suffix, account, token)| {
            let expires_at = required_epoch_millis(
                config,
                &format!("{ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX}{suffix}"),
            )
            .ok()?;
            Some((suffix, account, token, expires_at))
        })
        .collect::<Vec<_>>();
    // Deterministic choice when several keys name the same account: the last,
    // host-keyed one wins, as in the storage backend.
    seeds.sort_by(|left, right| left.0.cmp(right.0));
    seeds
        .into_iter()
        .map(|(_, account, token, expires_at)| {
            (
                account.to_string(),
                StorageCredential::new(StorageCredentialKind::Azdls(AzdlsCredential::new(token)))
                    .with_expiration(expires_at),
            )
        })
        .collect()
}

/// Account-specific SAS tokens as `(key suffix, account, token)`. Like Java,
/// keys may name the host (`account.dfs.core.windows.net`) or only the account.
fn azdls_sas_tokens(config: &HashMap<String, String>) -> impl Iterator<Item = (&str, &str, &str)> {
    config.iter().filter_map(|(key, token)| {
        let suffix = key.strip_prefix(ADLS_SAS_TOKEN_PREFIX)?;
        let account = suffix.split('.').next().unwrap_or(suffix);
        (!account.is_empty() && !token.is_empty()).then_some((suffix, account, token.as_str()))
    })
}

fn resolve_azdls_seed_path(location: &str) -> Result<KeyedSeedPath> {
    let (mut url, account) = parse_azdls_location(location)?;
    url.set_path("/");
    url.set_query(None);
    url.set_fragment(None);
    Ok(KeyedSeedPath {
        key: account,
        scope: url.to_string(),
    })
}

fn azdls_account_name(location: &str) -> Result<String> {
    parse_azdls_location(location).map(|(_, account)| account)
}

fn parse_azdls_location(location: &str) -> Result<(Url, String)> {
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
    let account = url
        .host_str()
        .and_then(|host| host.split('.').next())
        .filter(|account| !account.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("ADLS storage location has no account name: {location}"),
            )
        })?;
    Ok((url, account))
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
    use crate::auth::{NoopAuthManager, OAuth2Manager};

    fn epoch_millis(time: SystemTime) -> String {
        time.duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string()
    }

    fn s3_cred(
        prefix: Option<&str>,
        access_key_id: &str,
        expires_at: Option<SystemTime>,
    ) -> StorageCredential {
        let mut credential = StorageCredential::new(StorageCredentialKind::S3(S3Credential::new(
            access_key_id,
            "secret",
            None,
        )));
        if let Some(prefix) = prefix {
            credential = credential.with_prefix(prefix);
        }
        if let Some(expires_at) = expires_at {
            credential = credential.with_expiration(expires_at);
        }
        credential
    }

    fn s3_access_key_id(credential: &StorageCredential) -> &str {
        match credential.kind() {
            StorageCredentialKind::S3(s3) => s3.access_key_id(),
            other => panic!("expected S3 credential, got {other:?}"),
        }
    }

    /// A previously cached entry: like a seed, its age is unknown, so it is due
    /// once inside the nominal refresh window.
    fn cached_s3(prefix: &str, access_key_id: &str, expires_at: Option<SystemTime>) -> CachedEntry {
        CachedEntry::seed(s3_cred(Some(prefix), access_key_id, expires_at), false)
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
            table_config.cloned().unwrap_or_default(),
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
            plan_id: None,
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
        let mut config = HashMap::new();
        assert!(parse_s3_credential(&config, None).is_err());
        config.insert(S3_ACCESS_KEY_ID.to_string(), "AK".to_string());
        assert!(parse_s3_credential(&config, None).is_err());
        config.insert(S3_SECRET_ACCESS_KEY.to_string(), "SK".to_string());
        assert!(parse_s3_credential(&config, None).is_err());
        config.insert(S3_SESSION_TOKEN.to_string(), "TOK".to_string());
        config.insert(
            S3_SESSION_TOKEN_EXPIRES_AT_MS.to_string(),
            "not-a-timestamp".to_string(),
        );
        assert!(parse_s3_credential(&config, None).is_err());

        config.insert(
            S3_SESSION_TOKEN_EXPIRES_AT_MS.to_string(),
            "1500".to_string(),
        );
        let credential = parse_s3_credential(&config, Some("s3://bucket".to_string())).unwrap();
        assert_eq!(credential.prefix(), Some("s3://bucket"));
        assert_eq!(
            credential.expires_at(),
            Some(UNIX_EPOCH + Duration::from_millis(1500))
        );
        match credential.kind() {
            StorageCredentialKind::S3(s3) => assert_eq!(s3.session_token(), Some("TOK")),
            other => panic!("expected S3, got {other:?}"),
        }
    }

    #[test]
    fn parse_gcs_requires_token_and_expiry() {
        let mut config = HashMap::new();
        assert!(parse_gcs_credential(&config, None).is_err());
        config.insert(GCS_TOKEN.to_string(), "ya29.token".to_string());
        assert!(parse_gcs_credential(&config, None).is_err());

        config.insert(GCS_TOKEN_EXPIRES_AT.to_string(), "2000".to_string());
        let credential = parse_gcs_credential(&config, Some("gs://bucket".to_string())).unwrap();
        assert_eq!(credential.prefix(), Some("gs://bucket"));
        match credential.kind() {
            StorageCredentialKind::Gcs(gcs) => assert_eq!(gcs.token(), "ya29.token"),
            other => panic!("expected GCS, got {other:?}"),
        }
        assert_eq!(
            credential.expires_at(),
            Some(UNIX_EPOCH + Duration::from_millis(2000))
        );
    }

    #[test]
    fn parse_azdls_requires_account_specific_token_and_expiry() {
        let prefix = "abfss://container@account1.dfs.core.windows.net/table";
        let token_key = format!("{ADLS_SAS_TOKEN_PREFIX}account1");
        let expiry_key = format!("{ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX}account1");
        let mut config = HashMap::new();

        assert!(parse_azdls_credential(&config, Some(prefix.to_string())).is_err());
        config.insert(token_key, "sv=2026&sig=secret".to_string());
        assert!(parse_azdls_credential(&config, Some(prefix.to_string())).is_err());
        config.insert(expiry_key, "2500".to_string());

        let credential = parse_azdls_credential(&config, Some(prefix.to_string())).unwrap();
        assert_eq!(credential.prefix(), Some(prefix));
        assert_eq!(
            credential.expires_at(),
            Some(UNIX_EPOCH + Duration::from_millis(2500))
        );
        match credential.kind() {
            StorageCredentialKind::Azdls(azdls) => {
                assert_eq!(azdls.sas_token(), "sv=2026&sig=secret")
            }
            other => panic!("expected ADLS, got {other:?}"),
        }
    }

    #[test]
    fn cached_entry_freshness() {
        let no_expiry = cached_s3("", "a", None);
        let far = cached_s3(
            "",
            "a",
            Some(SystemTime::now() + REFRESH_BUFFER + Duration::from_secs(60)),
        );
        // Within the buffer but not yet expired: stale for a fast-path read, but
        // still usable for graceful degradation.
        let soon = cached_s3("", "a", Some(SystemTime::now() + Duration::from_secs(60)));
        let past = cached_s3("", "a", Some(SystemTime::now() - Duration::from_secs(60)));

        let now = SystemTime::now();
        assert!(no_expiry.is_fresh(now));
        assert!(no_expiry.is_unexpired(now));
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
        let credential = s3_cred(
            None,
            "a",
            Some(SystemTime::now() + REFRESH_BUFFER - Duration::from_secs(1)),
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
    fn longest_prefix_match_ignores_freshness() {
        let far = Some(SystemTime::now() + REFRESH_BUFFER + Duration::from_secs(3600));
        let entries = vec![
            cached_s3("s3://bucket", "wide", far),
            cached_s3("s3://bucket/warehouse/db", "narrow", far),
        ];
        let got = longest_prefix_match(&entries, "s3://bucket/warehouse/db/t/f").unwrap();
        assert_eq!(s3_access_key_id(&got.credential), "narrow");
        assert_eq!(got.credential.prefix(), Some("s3://bucket/warehouse/db"));
        assert!(longest_prefix_match(&entries, "s3://other/x").is_none());

        let fresh = Some(SystemTime::now() + REFRESH_BUFFER + Duration::from_secs(60));
        let stale = Some(SystemTime::now() - Duration::from_secs(60));
        let entries = vec![
            cached_s3("s3://bucket", "wide", fresh),
            cached_s3("s3://bucket/table", "narrow-stale", stale),
        ];
        let selected = longest_prefix_match(&entries, "s3://bucket/table/f").unwrap();
        assert_eq!(s3_access_key_id(&selected.credential), "narrow-stale");
        assert!(!selected.is_fresh(SystemTime::now()));
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
    async fn refresh_includes_scan_plan_id() {
        let mut server = Server::new_async().await;
        let body = s3_response(
            "s3://bucket",
            "AK",
            SystemTime::now() + Duration::from_secs(3600),
        );
        let mock = server
            .mock("GET", "/v1/credentials")
            .match_query(Matcher::UrlEncoded(
                "planId".to_string(),
                "scan-plan-1".to_string(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .create_async()
            .await;

        // No static creds -> no seed -> first load fetches from the endpoint.
        let mut props = aws_refresh_props("/v1/credentials");
        props.insert(
            REST_CATALOG_PROP_SCAN_PLAN_ID.to_string(),
            "scan-plan-1".to_string(),
        );

        let provider = test_provider(&server.url(), &props).await;
        let credential = provider
            .load_credential("s3://bucket/warehouse/f")
            .await
            .unwrap();
        assert_eq!(credential.prefix(), Some("s3://bucket"));

        match credential.kind() {
            StorageCredentialKind::S3(s3) => {
                assert_eq!(s3.access_key_id(), "AK");
                assert_eq!(s3.session_token(), Some("TOK"));
            }
            other => panic!("expected S3, got {other:?}"),
        }
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

        assert_eq!(credential.prefix(), None);
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
            Some("abfss://container@account1.dfs.core.windows.net/")
        );
        match credential.kind() {
            StorageCredentialKind::Azdls(azdls) => {
                assert_eq!(azdls.sas_token(), "sv=2026&sig=seed")
            }
            other => panic!("expected ADLS credential, got {other:?}"),
        }

        let credential = provider
            .load_credential("abfss://other-container@account2.dfs.core.windows.net/data/b.parquet")
            .await
            .unwrap();
        assert_eq!(
            credential.prefix(),
            Some("abfss://other-container@account2.dfs.core.windows.net/")
        );
        match credential.kind() {
            StorageCredentialKind::Azdls(azdls) => {
                assert_eq!(azdls.sas_token(), "sv=2026&sig=other-seed")
            }
            other => panic!("expected ADLS credential, got {other:?}"),
        }
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
            Some(SystemTime::now() + Duration::from_secs(60)),
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
                Some(SystemTime::now() + Duration::from_secs(60)),
            ),
            cached_s3(
                "s3://bucket/table-b",
                "OLDER_BK",
                Some(SystemTime::now() + Duration::from_secs(3600)),
            ),
            cached_s3(
                "s3://bucket/table-b",
                "LATEST_BK",
                Some(SystemTime::now() + Duration::from_secs(3600)),
            ),
            cached_s3(
                "s3://bucket/table-c",
                "VALID_CK",
                Some(SystemTime::now() + Duration::from_secs(3600)),
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
        ]);
        let provider = test_provider(&server.url(), &props).await;

        assert!(provider.supports_path("s3://bucket/x"));
        assert!(provider.supports_path("gs://bucket/x"));
        assert_eq!(
            s3_access_key_id(&provider.load_credential("s3://bucket/x").await.unwrap()),
            "AK"
        );
        match provider
            .load_credential("gs://bucket/x")
            .await
            .unwrap()
            .kind()
        {
            StorageCredentialKind::Gcs(gcs) => assert_eq!(gcs.token(), "GCS"),
            other => panic!("expected GCS credential, got {other:?}"),
        }
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
        let sas_token = |credential: StorageCredential| match credential.into_kind() {
            StorageCredentialKind::Azdls(azdls) => azdls.into_sas_token(),
            other => panic!("expected ADLS credential, got {other:?}"),
        };

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
        match credential.kind() {
            StorageCredentialKind::Azdls(azdls) => {
                assert_eq!(azdls.sas_token(), "sv=2026&sig=secret")
            }
            other => panic!("expected ADLS credential, got {other:?}"),
        }
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
                Some("abfss://container@account2.dfs.core.windows.net/")
            );
            match credential.kind() {
                StorageCredentialKind::Azdls(azdls) => {
                    assert_eq!(azdls.sas_token(), "sv=2026&sig=seed")
                }
                other => panic!("expected ADLS credential, got {other:?}"),
            }
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
    async fn scheme_aliases_share_prefix_scoped_credentials() {
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
        for path in ["s3a://bucket/table/f", "s3://bucket/table/f"] {
            assert_eq!(
                s3_access_key_id(&provider.load_credential(path).await.unwrap()),
                "AK"
            );
        }
        // A prefix only covers whole path segments, so this path refreshes.
        assert!(
            provider
                .load_credential("s3://bucket/table2/f")
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
            .expect(1)
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
            &NoopAuthManager,
            test_factory(&server.url(), Some(&table_auth)),
            &props,
            true,
        )
        .await
        .unwrap()
        .unwrap();

        let factory: Arc<dyn StorageCredentialProviderFactory> =
            serde_json::from_str(&serde_json::to_string(&provider.factory().unwrap()).unwrap())
                .unwrap();
        let config = StorageConfig::new().with_props(props);
        let rebuilt = factory.build(&config).unwrap();

        assert_eq!(
            s3_access_key_id(&rebuilt.load_credential("s3://bucket/x/f").await.unwrap()),
            "AK"
        );
        // The rebuilt provider is itself serializable.
        assert!(rebuilt.factory().is_ok());
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn provider_with_injected_auth_manager_is_not_serializable() {
        let provider = build_vended_credential_provider(
            &test_client("http://cat"),
            &NoopAuthManager,
            test_factory("http://cat", None),
            &aws_refresh_props("/v1/creds"),
            false,
        )
        .await
        .unwrap()
        .unwrap();

        let error = provider.factory().unwrap_err();
        assert_eq!(error.kind(), ErrorKind::FeatureUnsupported);
        assert!(
            error.message().contains("without_credential_provider"),
            "{error}"
        );
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
