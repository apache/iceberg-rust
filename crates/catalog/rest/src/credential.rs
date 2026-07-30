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

//! Refresh of vended storage credentials against a REST catalog.
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
//! expected backend-specific type. This preserves Java's per-cloud refresh
//! policy while supporting mixed-cloud tables through a resolving FileIO.
//!
//! # Adding a cloud
//!
//! The refresh policy for each cloud lives in one [`CloudRefresh`] constant. To
//! add a backend, first add its credential type to Iceberg's storage API and
//! teach the storage adapter to consume it. Then write its `parse_*` function,
//! add a `CloudRefresh` constant, and list it in [`CloudRefresh::SUPPORTED`].

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use iceberg::io::{
    AWS_REFRESH_CREDENTIALS_ENABLED, AWS_REFRESH_CREDENTIALS_ENDPOINT,
    GCS_REFRESH_CREDENTIALS_ENABLED, GCS_REFRESH_CREDENTIALS_ENDPOINT, GCS_TOKEN,
    GCS_TOKEN_EXPIRES_AT, GcsCredential, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN,
    S3_SESSION_TOKEN_EXPIRES_AT_MS, S3Credential, StorageCredential, StorageCredentialKind,
    StorageCredentialProvider,
};
use iceberg::{Error, ErrorKind, Result};
use rand::Rng;
use reqwest::{Method, StatusCode, Url};
use tokio::sync::Mutex;

use crate::REST_CATALOG_PROP_SCAN_PLAN_ID;
use crate::client::{HttpClient, deserialize_unexpected_catalog_error};
use crate::types::LoadCredentialsResponse;

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
    /// Table property to opt out; refresh is enabled unless this is `"false"`.
    enabled_key: &'static str,
    /// Whether to jitter successful prefetch times like AWS `CachedSupplier`.
    jitter_prefetch: bool,
    /// Parse a complete credential from catalog-supplied properties.
    parse_credential: fn(&HashMap<String, String>) -> Result<StorageCredential>,
}

impl CloudRefresh {
    /// S3 / AWS
    const AWS: Self = Self {
        schemes: &["s3", "s3a", "s3n"],
        endpoint_key: AWS_REFRESH_CREDENTIALS_ENDPOINT,
        enabled_key: AWS_REFRESH_CREDENTIALS_ENABLED,
        jitter_prefetch: true,
        parse_credential: parse_s3_credential,
    };
    /// Google Cloud Storage
    const GCP: Self = Self {
        schemes: &["gs", "gcs"],
        endpoint_key: GCS_REFRESH_CREDENTIALS_ENDPOINT,
        enabled_key: GCS_REFRESH_CREDENTIALS_ENABLED,
        jitter_prefetch: false,
        parse_credential: parse_gcs_credential,
    };
    // TODO: Azure (ADLS) is not yet supported: opendal 0.57's Azdls builder exposes no
    // custom credential-provider hook, and reqsign's SAS-token credential has no
    // expiry, so reqsign-based refresh isn't possible.

    /// Backends with refresh support
    const SUPPORTED: &[Self] = &[Self::AWS, Self::GCP];

    /// The backend that serves `location`, by its URL scheme, or `None` if no
    /// supported backend matches (in which case static credentials are used
    /// as-is, as before).
    fn for_location(location: &str) -> Option<&'static Self> {
        let scheme = scheme_of(location)?;
        Self::SUPPORTED
            .iter()
            .find(|cloud| cloud.schemes.contains(&scheme.as_str()))
    }

    fn matches_credential_prefix(&self, prefix: &str) -> bool {
        scheme_of(prefix).is_some_and(|scheme| self.schemes.contains(&scheme.as_str()))
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

/// Maximum failure backoff while a cached credential remains usable.
const MAX_FAILURE_BACKOFF: Duration = Duration::from_secs(30);

/// A vended credential paired with the storage-location prefix it applies to.
#[derive(Clone)]
struct CachedEntry {
    prefix: String,
    credential: StorageCredential,
    /// When this entry becomes eligible for prefetch. `None` means it does not
    /// expire and therefore never needs proactive refresh.
    refresh_at: Option<SystemTime>,
}

impl CachedEntry {
    fn new(prefix: String, credential: StorageCredential, jitter_prefetch: bool) -> Self {
        let refresh_at = credential
            .expires_at
            .map(|expires_at| prefetch_time(expires_at, jitter_prefetch));
        Self {
            prefix,
            credential,
            refresh_at,
        }
    }

    /// Seed entries that are already inside the nominal five-minute window are
    /// immediately due. Otherwise AWS applies the same jitter as it does to a
    /// freshly fetched value.
    fn seed(prefix: String, credential: StorageCredential, jitter_prefetch: bool) -> Self {
        let due = credential.expires_at.is_some_and(|expires_at| {
            SystemTime::now()
                .checked_add(REFRESH_BUFFER)
                .is_none_or(|refresh_boundary| refresh_boundary >= expires_at)
        });
        let mut entry = Self::new(prefix, credential, jitter_prefetch);
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
            .expires_at
            .is_none_or(|expires_at| now < expires_at)
    }
}

/// Cached credentials plus failure-backoff state.
struct CacheState {
    entries: Vec<CachedEntry>,
    consecutive_failures: u32,
    retry_not_before: Option<Instant>,
}

struct ConfiguredCloud {
    cloud: &'static CloudRefresh,
    endpoint: String,
    cache: Mutex<CacheState>,
    /// Only one caller fetches at a time. The cache lock is deliberately
    /// separate so other callers can keep using an unexpired credential while
    /// the refresh is in flight.
    refresh: Mutex<()>,
}

/// Fetches and refreshes vended credentials from a REST catalog's table
/// credentials endpoint.
///
/// Each cloud cache is seeded with the credential from the initial table
/// properties (when complete) and re-fetched from its endpoint as it
/// nears expiry.
pub(crate) struct RestVendedCredentialProvider {
    client: Arc<HttpClient>,
    /// Optional scan-plan identifier.
    plan_id: Option<String>,
    /// Independently configured endpoint and cache for each backing cloud.
    clouds: Vec<ConfiguredCloud>,
}

impl RestVendedCredentialProvider {
    fn new(client: Arc<HttpClient>, plan_id: Option<String>, clouds: Vec<ConfiguredCloud>) -> Self {
        Self {
            client,
            plan_id,
            clouds,
        }
    }

    /// Fetch fresh credentials from the catalog's credentials endpoint.
    async fn fetch(&self, configured: &ConfiguredCloud) -> Result<Vec<CachedEntry>> {
        let mut request = self.client.request(Method::GET, &configured.endpoint);
        if let Some(plan_id) = &self.plan_id {
            request = request.query(&[("planId", plan_id)]);
        }
        let request = request.build()?;
        let response = self.client.query_catalog(request).await?;

        match response.status() {
            StatusCode::OK => {
                let parsed: LoadCredentialsResponse = response.json().await?;
                parsed
                    .storage_credentials
                    .into_iter()
                    .filter(|sc| configured.cloud.matches_credential_prefix(&sc.prefix))
                    .map(|sc| {
                        (configured.cloud.parse_credential)(&sc.config).map(|credential| {
                            CachedEntry::new(
                                sc.prefix,
                                credential,
                                configured.cloud.jitter_prefetch,
                            )
                        })
                    })
                    .collect()
            }
            _ => Err(deserialize_unexpected_catalog_error(
                response,
                self.client.disable_header_redaction(),
            )
            .await),
        }
    }

    async fn refresh_credential(
        &self,
        configured: &ConfiguredCloud,
        path: &str,
        fallback: Option<CachedEntry>,
    ) -> Result<StorageCredential> {
        let refreshed = self.fetch(configured).await.and_then(|entries| {
            let credential = longest_prefix_match(&entries, path)
                .filter(|entry| entry.is_unexpired(SystemTime::now()))
                .map(|entry| entry.credential.clone())
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        format!("no unexpired vended credential matches storage location: {path}"),
                    )
                })?;
            Ok((entries, credential))
        });

        match refreshed {
            Ok((entries, credential)) => {
                let mut cache = configured.cache.lock().await;
                cache.entries = entries;
                cache.consecutive_failures = 0;
                cache.retry_not_before = None;
                Ok(credential)
            }
            Err(fetch_error) => {
                let mut cache = configured.cache.lock().await;
                cache.consecutive_failures = cache.consecutive_failures.saturating_add(1);
                cache.retry_not_before =
                    Instant::now().checked_add(failure_backoff(cache.consecutive_failures));

                // Graceful degradation: while the cached credential remains
                // usable, serve it and retry after jittered backoff. Expired
                // credentials are never served.
                fallback
                    .filter(|entry| entry.is_unexpired(SystemTime::now()))
                    .map(|entry| entry.credential)
                    .ok_or(fetch_error)
            }
        }
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
}

async fn cache_decision(configured: &ConfiguredCloud, path: &str) -> CacheDecision {
    let cache = configured.cache.lock().await;
    let current = longest_prefix_match(&cache.entries, path).cloned();
    let now = SystemTime::now();

    if let Some(entry) = current.as_ref().filter(|entry| entry.is_fresh(now)) {
        return CacheDecision::Use(entry.credential.clone());
    }

    if cache
        .retry_not_before
        .is_some_and(|retry_at| Instant::now() < retry_at)
        && let Some(entry) = current.as_ref().filter(|entry| entry.is_unexpired(now))
    {
        return CacheDecision::Use(entry.credential.clone());
    }

    CacheDecision::Refresh(current)
}

#[async_trait]
impl StorageCredentialProvider for RestVendedCredentialProvider {
    fn supports_path(&self, path: &str) -> bool {
        CloudRefresh::for_location(path).is_some_and(|cloud| {
            self.clouds
                .iter()
                .any(|configured| configured.cloud.endpoint_key == cloud.endpoint_key)
        })
    }

    async fn load_credential(&self, path: &str) -> Result<StorageCredential> {
        let cloud = CloudRefresh::for_location(path).ok_or_else(|| {
            Error::new(
                ErrorKind::FeatureUnsupported,
                format!("no credential refresh implementation for storage location: {path}"),
            )
        })?;
        let configured = self
            .clouds
            .iter()
            .find(|configured| configured.cloud.endpoint_key == cloud.endpoint_key)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::FeatureUnsupported,
                    format!("credential refresh is not configured for storage location: {path}"),
                )
            })?;

        let current = match cache_decision(configured, path).await {
            CacheDecision::Use(credential) => return Ok(credential),
            CacheDecision::Refresh(current) => current,
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
        let current = match cache_decision(configured, path).await {
            CacheDecision::Use(credential) => return Ok(credential),
            CacheDecision::Refresh(current) => current,
        };

        self.refresh_credential(configured, path, current).await
    }
}

/// Select the credential whose prefix is the longest match for `path`.
fn longest_prefix_match<'a>(entries: &'a [CachedEntry], path: &str) -> Option<&'a CachedEntry> {
    entries
        .iter()
        .filter(|entry| path.starts_with(&entry.prefix))
        .max_by_key(|entry| entry.prefix.len())
}

/// Compute a successful credential's prefetch time.
fn prefetch_time(expires_at: SystemTime, jitter: bool) -> SystemTime {
    let base = expires_at.checked_sub(REFRESH_BUFFER).unwrap_or(UNIX_EPOCH);
    if !jitter {
        return base;
    }

    let jitter_window = REFRESH_BUFFER.saturating_sub(MIN_REFRESH_BUFFER);
    let jitter_millis = rand::rng().random_range(0..jitter_window.as_millis() as u64);
    base.checked_add(Duration::from_millis(jitter_millis))
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

/// Build a credential provider from a table's properties,
/// or `None` when no supported cloud advertises an enabled refresh endpoint.
///
/// `base_uri` is the catalog URI, used to resolve a relative endpoint.
pub(crate) fn build_vended_credential_provider(
    client: Arc<HttpClient>,
    base_uri: &str,
    props: &HashMap<String, String>,
) -> Result<Option<Arc<dyn StorageCredentialProvider>>> {
    let clouds = CloudRefresh::SUPPORTED
        .iter()
        .filter_map(|cloud| {
            // Refresh is enabled by default and invalid booleans disable refresh.
            let enabled = props
                .get(cloud.enabled_key)
                .is_none_or(|value| value.parse().unwrap_or(false));
            if !enabled {
                return None;
            }

            let endpoint = resolve_endpoint(
                base_uri,
                props
                    .get(cloud.endpoint_key)
                    .filter(|value| !value.is_empty())?,
            );
            let entries = (cloud.parse_credential)(props)
                .ok()
                .map(|credential| {
                    vec![CachedEntry::seed(
                        // Flat table properties carry no prefix. This cache is
                        // already cloud-specific, so an empty prefix is safe.
                        String::new(),
                        credential,
                        cloud.jitter_prefetch,
                    )]
                })
                .unwrap_or_default();

            Some(ConfiguredCloud {
                cloud,
                endpoint,
                cache: Mutex::new(CacheState {
                    entries,
                    consecutive_failures: 0,
                    retry_not_before: None,
                }),
                refresh: Mutex::new(()),
            })
        })
        .collect::<Vec<_>>();

    if clouds.is_empty() {
        return Ok(None);
    }

    let table_client = Arc::new(client.for_table(base_uri, props.clone())?);
    let plan_id = props.get(REST_CATALOG_PROP_SCAN_PLAN_ID).cloned();
    Ok(Some(Arc::new(RestVendedCredentialProvider::new(
        table_client,
        plan_id,
        clouds,
    ))))
}

/// Resolve a possibly-relative refresh endpoint against the catalog base URI.
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

/// Parse a complete S3 credential returned by the credentials endpoint.
fn parse_s3_credential(config: &HashMap<String, String>) -> Result<StorageCredential> {
    let access_key_id = required_nonempty(config, S3_ACCESS_KEY_ID)?;
    let secret_access_key = required_nonempty(config, S3_SECRET_ACCESS_KEY)?;
    let session_token = required_nonempty(config, S3_SESSION_TOKEN)?;
    let expires_at = required_epoch_millis(config, S3_SESSION_TOKEN_EXPIRES_AT_MS)?;
    Ok(StorageCredential {
        kind: StorageCredentialKind::S3(S3Credential {
            access_key_id,
            secret_access_key,
            session_token: Some(session_token),
        }),
        expires_at: Some(expires_at),
    })
}

/// Parse a complete GCS credential returned by the credentials endpoint.
fn parse_gcs_credential(config: &HashMap<String, String>) -> Result<StorageCredential> {
    let token = required_nonempty(config, GCS_TOKEN)?;
    let expires_at = required_epoch_millis(config, GCS_TOKEN_EXPIRES_AT)?;
    Ok(StorageCredential {
        kind: StorageCredentialKind::Gcs(GcsCredential { token }),
        expires_at: Some(expires_at),
    })
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
    use crate::RestCatalogConfig;

    fn epoch_millis(time: SystemTime) -> String {
        time.duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string()
    }

    fn s3_cred(access_key_id: &str, expires_at: Option<SystemTime>) -> StorageCredential {
        StorageCredential {
            kind: StorageCredentialKind::S3(S3Credential {
                access_key_id: access_key_id.to_string(),
                secret_access_key: "secret".to_string(),
                session_token: None,
            }),
            expires_at,
        }
    }

    fn s3_access_key_id(credential: &StorageCredential) -> &str {
        match &credential.kind {
            StorageCredentialKind::S3(s3) => &s3.access_key_id,
            other => panic!("expected S3 credential, got {other:?}"),
        }
    }

    fn cached_s3(prefix: &str, access_key_id: &str, expires_at: Option<SystemTime>) -> CachedEntry {
        CachedEntry::new(
            prefix.to_string(),
            s3_cred(access_key_id, expires_at),
            false,
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
    fn cloud_selection_uses_url_scheme() {
        assert!(CloudRefresh::for_location("s3://b/k").is_some());
        assert!(CloudRefresh::for_location("S3://b/k").is_some());
        assert!(CloudRefresh::for_location("s3a://b/k").is_some());
        assert!(CloudRefresh::for_location("s3n://b/k").is_some());
        assert!(CloudRefresh::for_location("gs://b/k").is_some());
        assert!(CloudRefresh::for_location("gcs://b/k").is_some());
        assert!(CloudRefresh::for_location("not a url").is_none());
        // Azure not supported yet -> no provider, static creds used as-is.
        assert!(CloudRefresh::for_location("abfss://fs@acct.dfs.core.windows.net/k").is_none());
        assert!(CloudRefresh::AWS.matches_credential_prefix("s3://bucket/path"));
        assert!(!CloudRefresh::AWS.matches_credential_prefix("s3evil://bucket/path"));
    }

    #[test]
    fn parses_s3_credential() {
        let mut config = HashMap::new();
        assert!(parse_s3_credential(&config).is_err());
        config.insert(S3_ACCESS_KEY_ID.to_string(), "AK".to_string());
        assert!(parse_s3_credential(&config).is_err());
        config.insert(S3_SECRET_ACCESS_KEY.to_string(), "SK".to_string());
        assert!(parse_s3_credential(&config).is_err());

        config.insert(S3_SESSION_TOKEN.to_string(), "TOK".to_string());
        config.insert(
            S3_SESSION_TOKEN_EXPIRES_AT_MS.to_string(),
            "1500".to_string(),
        );
        let credential = parse_s3_credential(&config).unwrap();
        assert_eq!(
            credential.expires_at,
            Some(UNIX_EPOCH + Duration::from_millis(1500))
        );
        match credential.kind {
            StorageCredentialKind::S3(s3) => assert_eq!(s3.session_token.as_deref(), Some("TOK")),
            other => panic!("expected S3, got {other:?}"),
        }
    }

    #[test]
    fn parse_gcs_requires_token_and_expiry() {
        let mut config = HashMap::new();
        assert!(parse_gcs_credential(&config).is_err());
        config.insert(GCS_TOKEN.to_string(), "ya29.token".to_string());
        assert!(parse_gcs_credential(&config).is_err());

        config.insert(GCS_TOKEN_EXPIRES_AT.to_string(), "2000".to_string());
        let credential = parse_gcs_credential(&config).unwrap();
        match &credential.kind {
            StorageCredentialKind::Gcs(gcs) => assert_eq!(gcs.token, "ya29.token"),
            other => panic!("expected GCS, got {other:?}"),
        }
        assert_eq!(
            credential.expires_at,
            Some(UNIX_EPOCH + Duration::from_millis(2000))
        );
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
        let expires_at = SystemTime::now() + Duration::from_secs(3600);
        assert_eq!(
            prefetch_time(expires_at, false),
            expires_at - REFRESH_BUFFER
        );

        for _ in 0..100 {
            let refresh_at = prefetch_time(expires_at, true);
            assert!(refresh_at >= expires_at - REFRESH_BUFFER);
            assert!(refresh_at < expires_at - MIN_REFRESH_BUFFER);
        }
    }

    #[test]
    fn seed_inside_nominal_window_is_immediately_due() {
        let credential = s3_cred(
            "a",
            Some(SystemTime::now() + REFRESH_BUFFER - Duration::from_secs(1)),
        );
        let entry = CachedEntry::seed(String::new(), credential, true);
        let now = SystemTime::now();
        assert!(!entry.is_fresh(now));
        assert!(entry.is_unexpired(now));
    }

    #[test]
    fn failure_backoff_is_jittered_and_capped() {
        for failures in 1_u32..=40 {
            let ceiling = INITIAL_FAILURE_BACKOFF
                .checked_mul(1_u32 << failures.saturating_sub(1).min(5))
                .unwrap_or(MAX_FAILURE_BACKOFF)
                .min(MAX_FAILURE_BACKOFF);
            for _ in 0..100 {
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

    #[test]
    fn provider_support_tracks_configured_cloud_endpoints() {
        let config = RestCatalogConfig::builder()
            .uri("http://cat".to_string())
            .build();
        let client = Arc::new(HttpClient::new(&config).unwrap());
        let props = HashMap::from([(
            CloudRefresh::AWS.endpoint_key.to_string(),
            "/v1/creds".to_string(),
        )]);

        // The provider is configured independently of the table metadata scheme,
        // but advertises support only for clouds whose endpoint is present.
        let provider = build_vended_credential_provider(client.clone(), "http://cat", &props)
            .unwrap()
            .unwrap();
        assert!(provider.supports_path("s3://b/k"));
        assert!(!provider.supports_path("abfss://fs@acct.dfs.core.windows.net/k"));
        // No endpoint advertised.
        assert!(
            build_vended_credential_provider(client.clone(), "http://cat", &HashMap::new())
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
            build_vended_credential_provider(client, "http://cat", &disabled)
                .unwrap()
                .is_none()
        );
        // Java's `Strings.isNullOrEmpty` check treats an empty endpoint as absent.
        let empty = HashMap::from([(CloudRefresh::AWS.endpoint_key.to_string(), String::new())]);
        let config = RestCatalogConfig::builder()
            .uri("http://cat".to_string())
            .build();
        let client = Arc::new(HttpClient::new(&config).unwrap());
        assert!(
            build_vended_credential_provider(client, "http://cat", &empty)
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn fetches_and_selects_vended_credential() {
        let mut server = Server::new_async().await;
        let expires = epoch_millis(SystemTime::now() + Duration::from_secs(3600));
        let body = format!(
            r#"{{"storage-credentials":[{{"prefix":"s3://bucket","config":{{"s3.access-key-id":"AK","s3.secret-access-key":"SK","s3.session-token":"TOK","s3.session-token-expires-at-ms":"{expires}"}}}}]}}"#
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

        let config = RestCatalogConfig::builder().uri(server.url()).build();
        let client = Arc::new(HttpClient::new(&config).unwrap());
        // No static creds -> no seed -> first load fetches from the endpoint.
        let props = HashMap::from([
            (
                CloudRefresh::AWS.endpoint_key.to_string(),
                "/v1/credentials".to_string(),
            ),
            (
                REST_CATALOG_PROP_SCAN_PLAN_ID.to_string(),
                "scan-plan-1".to_string(),
            ),
        ]);

        let provider = build_vended_credential_provider(client, &server.url(), &props)
            .expect("provider construction should succeed")
            .expect("provider should be built");
        let credential = provider
            .load_credential("s3://bucket/warehouse/f")
            .await
            .unwrap();

        match credential.kind {
            StorageCredentialKind::S3(s3) => {
                assert_eq!(s3.access_key_id, "AK");
                assert_eq!(s3.session_token.as_deref(), Some("TOK"));
            }
            other => panic!("expected S3, got {other:?}"),
        }
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

        let config = RestCatalogConfig::builder().uri(server.url()).build();
        let client = Arc::new(HttpClient::new(&config).unwrap());
        let expires = epoch_millis(SystemTime::now() + Duration::from_secs(3600));
        let props = HashMap::from([
            (
                CloudRefresh::AWS.endpoint_key.to_string(),
                "/v1/credentials".to_string(),
            ),
            (S3_ACCESS_KEY_ID.to_string(), "SEED_AK".to_string()),
            (S3_SECRET_ACCESS_KEY.to_string(), "SEED_SK".to_string()),
            (S3_SESSION_TOKEN.to_string(), "SEED_TOK".to_string()),
            (S3_SESSION_TOKEN_EXPIRES_AT_MS.to_string(), expires),
        ]);

        let provider = build_vended_credential_provider(client, &server.url(), &props)
            .expect("provider construction should succeed")
            .expect("provider should be built");
        let credential = provider.load_credential("s3://bucket/x/f").await.unwrap();

        assert_eq!(s3_access_key_id(&credential), "SEED_AK");
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

        let config = RestCatalogConfig::builder().uri(server.url()).build();
        let client = Arc::new(HttpClient::new(&config).unwrap());
        // Seeded credential is within the refresh buffer but not yet expired.
        let expires = epoch_millis(SystemTime::now() + Duration::from_secs(60));
        let props = HashMap::from([
            (
                CloudRefresh::AWS.endpoint_key.to_string(),
                "/v1/credentials".to_string(),
            ),
            (S3_ACCESS_KEY_ID.to_string(), "SEED_AK".to_string()),
            (S3_SECRET_ACCESS_KEY.to_string(), "SEED_SK".to_string()),
            (S3_SESSION_TOKEN.to_string(), "SEED_TOK".to_string()),
            (S3_SESSION_TOKEN_EXPIRES_AT_MS.to_string(), expires),
        ]);

        let provider = build_vended_credential_provider(client, &server.url(), &props)
            .expect("provider construction should succeed")
            .expect("provider should be built");
        // The first refresh fails, but the still-valid seed is served. Immediate
        // follow-up operations stay inside the first jittered backoff window.
        for _ in 0..3 {
            let credential = provider.load_credential("s3://bucket/x/f").await.unwrap();
            assert_eq!(s3_access_key_id(&credential), "SEED_AK");
        }
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn concurrent_prefetch_serves_unexpired_credential_without_waiting() {
        let mut server = Server::new_async().await;
        let expires = epoch_millis(SystemTime::now() + Duration::from_secs(3600));
        let body = format!(
            r#"{{"storage-credentials":[{{"prefix":"s3://bucket","config":{{"s3.access-key-id":"NEW_AK","s3.secret-access-key":"SK","s3.session-token":"TOK","s3.session-token-expires-at-ms":"{expires}"}}}}]}}"#
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

        let config = RestCatalogConfig::builder().uri(server.url()).build();
        let client = Arc::new(HttpClient::new(&config).unwrap());
        let seed_expires = epoch_millis(SystemTime::now() + Duration::from_secs(60));
        let props = HashMap::from([
            (
                CloudRefresh::AWS.endpoint_key.to_string(),
                "/v1/credentials".to_string(),
            ),
            (S3_ACCESS_KEY_ID.to_string(), "SEED_AK".to_string()),
            (S3_SECRET_ACCESS_KEY.to_string(), "SEED_SK".to_string()),
            (S3_SESSION_TOKEN.to_string(), "SEED_TOK".to_string()),
            (S3_SESSION_TOKEN_EXPIRES_AT_MS.to_string(), seed_expires),
        ]);
        let provider = build_vended_credential_provider(client, &server.url(), &props)
            .unwrap()
            .unwrap();

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
        let expires = epoch_millis(SystemTime::now() + Duration::from_secs(3600));
        let body = format!(
            r#"{{"storage-credentials":[{{"prefix":"s3://bucket","config":{{"s3.access-key-id":"AK","s3.secret-access-key":"SK","s3.session-token":"TOK","s3.session-token-expires-at-ms":"{expires}"}}}}]}}"#
        );
        let mock = server
            .mock("GET", "/v1/credentials")
            .match_header("authorization", "Bearer table-token")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .create_async()
            .await;

        let config = RestCatalogConfig::builder().uri(server.url()).build();
        let client = Arc::new(HttpClient::new(&config).unwrap());
        let props = HashMap::from([
            (
                CloudRefresh::AWS.endpoint_key.to_string(),
                "/v1/credentials".to_string(),
            ),
            ("token".to_string(), "table-token".to_string()),
        ]);
        let provider = build_vended_credential_provider(client, &server.url(), &props)
            .unwrap()
            .unwrap();

        provider.load_credential("s3://bucket/x/f").await.unwrap();
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn one_provider_refreshes_multiple_clouds() {
        let mut server = Server::new_async().await;
        let expires = epoch_millis(SystemTime::now() + Duration::from_secs(3600));
        let aws_body = format!(
            r#"{{"storage-credentials":[{{"prefix":"s3://bucket","config":{{"s3.access-key-id":"AK","s3.secret-access-key":"SK","s3.session-token":"TOK","s3.session-token-expires-at-ms":"{expires}"}}}}]}}"#
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

        let config = RestCatalogConfig::builder().uri(server.url()).build();
        let client = Arc::new(HttpClient::new(&config).unwrap());
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
        let provider = build_vended_credential_provider(client, &server.url(), &props)
            .unwrap()
            .unwrap();

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
            .kind
        {
            StorageCredentialKind::Gcs(gcs) => assert_eq!(gcs.token, "GCS"),
            other => panic!("expected GCS credential, got {other:?}"),
        }
        aws_mock.assert_async().await;
        gcp_mock.assert_async().await;
    }

    #[tokio::test]
    async fn refresh_failure_never_serves_expired_credential() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", Matcher::Any)
            .with_status(500)
            .create_async()
            .await;

        let config = RestCatalogConfig::builder().uri(server.url()).build();
        let client = Arc::new(HttpClient::new(&config).unwrap());
        let expired = epoch_millis(SystemTime::now() - Duration::from_secs(60));
        let props = HashMap::from([
            (
                CloudRefresh::AWS.endpoint_key.to_string(),
                "/v1/credentials".to_string(),
            ),
            (S3_ACCESS_KEY_ID.to_string(), "EXPIRED_AK".to_string()),
            (S3_SECRET_ACCESS_KEY.to_string(), "EXPIRED_SK".to_string()),
            (S3_SESSION_TOKEN.to_string(), "EXPIRED_TOK".to_string()),
            (S3_SESSION_TOKEN_EXPIRES_AT_MS.to_string(), expired),
        ]);

        let provider = build_vended_credential_provider(client, &server.url(), &props)
            .expect("provider construction should succeed")
            .expect("provider should be built");

        assert!(provider.load_credential("s3://bucket/x/f").await.is_err());
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn sub_buffer_ttl_is_refetched_on_each_sequential_operation() {
        let mut server = Server::new_async().await;
        // The vended TTL (60s) is shorter than REFRESH_BUFFER, so every operation
        // is eligible for refresh. This matches AWS CachedSupplier: successful
        // results do not receive failure backoff.
        let expires = epoch_millis(SystemTime::now() + Duration::from_secs(60));
        let body = format!(
            r#"{{"storage-credentials":[{{"prefix":"s3://bucket","config":{{"s3.access-key-id":"AK","s3.secret-access-key":"SK","s3.session-token":"TOK","s3.session-token-expires-at-ms":"{expires}"}}}}]}}"#
        );
        let mock = server
            .mock("GET", "/v1/credentials")
            .expect(3)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .create_async()
            .await;

        let config = RestCatalogConfig::builder().uri(server.url()).build();
        let client = Arc::new(HttpClient::new(&config).unwrap());
        // No static creds -> no seed -> each sequential load fetches because the
        // returned credential is already inside its prefetch window.
        let props = HashMap::from([(
            CloudRefresh::AWS.endpoint_key.to_string(),
            "/v1/credentials".to_string(),
        )]);

        let provider = build_vended_credential_provider(client, &server.url(), &props)
            .expect("provider construction should succeed")
            .expect("provider should be built");

        for _ in 0..3 {
            let credential = provider.load_credential("s3://bucket/x/f").await.unwrap();
            assert_eq!(s3_access_key_id(&credential), "AK");
        }
        mock.assert_async().await;
    }
}
