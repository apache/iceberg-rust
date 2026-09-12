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
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use http::StatusCode;
use iceberg::sensitive::SensitiveString;
use iceberg::{Error, ErrorKind, Result, SessionContext};
use moka::sync::Cache;
use reqwest::header::HeaderMap;
use tokio::sync::Mutex;

use super::{AuthManager, AuthSession, HttpRequest};
use crate::catalog::{
    REST_CATALOG_PROP_URI, RestCatalogConfig, credential_from_props, default_token_endpoint,
    explicit_headers_from_props,
};
use crate::client::HttpClient;
use crate::types::{ErrorResponse, TokenResponse};

/// OAuth2 configuration supplied when the manager is constructed. Properties
/// from each authentication handshake are merged onto this configuration.
struct OAuth2Config {
    extra_headers: HeaderMap,
    token_endpoint: String,
    credential: Option<(Option<String>, SensitiveString)>,
    extra_oauth_params: HashMap<String, String>,
}

/// [`AuthManager`] implementing the OAuth2 client-credentials flow used by
/// Iceberg REST catalogs.
///
/// A configured `token` is used directly; otherwise `credential` is exchanged
/// for a token at the token endpoint and cached. Client-credential tokens are
/// refreshed on demand after the server-provided `expires_in` duration;
/// responses without `expires_in` remain cached. The init and catalog sessions
/// share that cached token so it survives the config handshake; contextual
/// sessions keep their tokens isolated from it.
pub struct OAuth2Manager {
    token: Arc<Mutex<Option<CachedToken>>>,
    initial_config: OAuth2Config,
    /// True when the token endpoint was derived from the catalog URI (not
    /// explicitly configured): it is then recomputed from the merged URI in
    /// [`Self::catalog_session`], since `/v1/config` may override the URI.
    endpoint_is_default: bool,
    /// Installed by `catalog_session`; replacing it atomically invalidates
    /// contextual sessions derived from the previous catalog configuration.
    contextual_state: OnceLock<ContextualAuthState>,
}

/// Catalog-resolved configuration and the contextual sessions derived from it.
struct ContextualAuthState {
    token_exchange: TokenExchangeConfig,
    session_cache: Cache<String, Arc<OAuth2Session>>,
}

/// Everything needed to perform a client-credentials token exchange, except
/// for the credential itself.
#[derive(Clone)]
struct TokenExchangeConfig {
    client: HttpClient,
    extra_headers: HeaderMap,
    token_endpoint: String,
    extra_oauth_params: HashMap<String, String>,
}

impl OAuth2Manager {
    /// Creates a manager exchanging credentials at `token_endpoint`, with no
    /// token or credential configured. Combine with the `with_*` methods:
    ///
    /// ```rust,ignore
    /// let manager = OAuth2Manager::new("https://auth.example.com/v1/oauth/tokens")
    ///     .with_credential(Some("client-id".into()), "client-secret".into());
    /// ```
    pub fn new(token_endpoint: impl Into<String>) -> Self {
        Self {
            token: Arc::new(Mutex::new(None)),
            initial_config: OAuth2Config {
                extra_headers: HeaderMap::new(),
                token_endpoint: token_endpoint.into(),
                credential: None,
                // Same default as the configuration path: the catalog scope.
                extra_oauth_params: HashMap::from([("scope".to_string(), "catalog".to_string())]),
            },
            endpoint_is_default: false,
            contextual_state: OnceLock::new(),
        }
    }

    /// Sets a bearer token used directly (takes precedence over `credential`).
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Arc::new(Mutex::new(Some(CachedToken::static_token(
            SensitiveString::from(token.into()),
        ))));
        self
    }

    /// Sets the client credential exchanged for a token at the token endpoint.
    pub fn with_credential(mut self, client_id: Option<String>, client_secret: String) -> Self {
        self.initial_config.credential = Some((client_id, client_secret.into()));
        self
    }

    /// Sets extra headers sent with token requests.
    pub fn with_extra_headers(mut self, headers: HeaderMap) -> Self {
        self.initial_config.extra_headers = headers;
        self
    }

    /// Adds extra OAuth2 form parameters (e.g. `scope`, `audience`), merged
    /// onto the defaults: provide a `scope` entry to replace the default
    /// `catalog` scope.
    pub fn with_extra_oauth_params(mut self, params: HashMap<String, String>) -> Self {
        self.initial_config.extra_oauth_params.extend(params);
        self
    }

    pub(crate) fn from_config(cfg: &RestCatalogConfig) -> Result<Self> {
        Ok(Self {
            token: Arc::new(Mutex::new(
                cfg.token()
                    .map(SensitiveString::from)
                    .map(CachedToken::static_token),
            )),
            initial_config: OAuth2Config {
                extra_headers: cfg.extra_headers()?,
                token_endpoint: cfg.get_token_endpoint(),
                credential: cfg.credential().map(|(id, secret)| (id, secret.into())),
                extra_oauth_params: cfg.extra_oauth_params(),
            },
            endpoint_is_default: cfg.explicit_oauth2_server_uri().is_none(),
            contextual_state: OnceLock::new(),
        })
    }
}

impl Debug for OAuth2Manager {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuth2Manager")
            .field("token_endpoint", &self.initial_config.token_endpoint)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl AuthManager for OAuth2Manager {
    async fn init_session(
        &self,
        client: &HttpClient,
        props: &HashMap<String, String>,
    ) -> Result<Box<dyn AuthSession>> {
        let (session, _) = self.build_session_from_properties(client, props).await?;
        Ok(Box::new(session))
    }

    async fn catalog_session(
        &self,
        client: &HttpClient,
        props: &HashMap<String, String>,
    ) -> Result<Arc<dyn AuthSession>> {
        if self.contextual_state.get().is_some() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "OAuth2Manager catalog session already initialized",
            ));
        }

        let (session, token_exchange) = self.build_session_from_properties(client, props).await?;

        self.contextual_state
            .set(ContextualAuthState {
                token_exchange,
                session_cache: Cache::builder()
                    .time_to_idle(session_idle_timeout(props)?)
                    .build(),
            })
            .map_err(|_| {
                Error::new(
                    ErrorKind::PreconditionFailed,
                    "OAuth2Manager catalog session already initialized concurrently",
                )
            })?;

        Ok(Arc::new(session))
    }

    async fn contextual_session(
        &self,
        context: &SessionContext,
        catalog_session: Arc<dyn AuthSession>,
    ) -> Result<Arc<dyn AuthSession>> {
        let credential = if let Some(token) = context.credentials().get("token") {
            ContextualCredential::Token(token.clone())
        } else if let Some(credential) = context.credentials().get("credential") {
            ContextualCredential::ClientCredentials(parse_contextual_credential(credential))
        } else {
            return Ok(catalog_session);
        };

        let contextual_state = self.contextual_state.get().ok_or_else(|| {
            Error::new(
                ErrorKind::PreconditionFailed,
                "OAuth2 catalog session must be initialized before contextual sessions",
            )
        })?;

        if let Some(session) = contextual_state.session_cache.get(context.session_id()) {
            return Ok(session.clone());
        }

        let token_exchange = contextual_state.token_exchange.clone();

        let candidate_session = Arc::new(match credential {
            ContextualCredential::Token(token) => OAuth2Session {
                token: Arc::new(Mutex::new(Some(CachedToken::static_token(token)))),
                token_source: TokenSource::StaticToken,
                parent: Some(catalog_session),
            },
            ContextualCredential::ClientCredentials(credential) => OAuth2Session {
                token: Arc::new(Mutex::new(None)),
                token_source: TokenSource::ClientCredentials(Box::new(ClientCredentialsConfig {
                    token_exchange: TokenExchangeConfig {
                        client: token_exchange
                            .client
                            .with_auth_session(catalog_session.clone()),
                        ..token_exchange
                    },
                    credential,
                })),
                parent: Some(catalog_session),
            },
        });

        let session = contextual_state
            .session_cache
            .entry(context.session_id().to_string())
            .or_insert(candidate_session) // If a concurrent insert won, we'll use it instead because it may already be in use.
            .value()
            .clone();

        Ok(session)
    }
}

enum ContextualCredential {
    Token(SensitiveString),
    ClientCredentials((Option<String>, SensitiveString)),
}

fn parse_contextual_credential(credential: &SensitiveString) -> (Option<String>, SensitiveString) {
    match credential.expose().split_once(':') {
        Some((client_id, client_secret)) => (
            Some(client_id.to_string()),
            SensitiveString::from(client_secret.to_string()),
        ),
        None => (None, credential.clone()),
    }
}

impl OAuth2Manager {
    /// Builds a session from the manager's options with `props` merged onto
    /// them, so an injected manager keeps whatever a property doesn't
    /// override. The manager's token cell is shared with every session it
    /// builds, so a token cached during the handshake survives it.
    async fn build_session_from_properties(
        &self,
        client: &HttpClient,
        props: &HashMap<String, String>,
    ) -> Result<(OAuth2Session, TokenExchangeConfig)> {
        // The properties may carry a new token (or restate the user's).
        if let Some(token) = props.get("token") {
            *self.token.lock().await = Some(CachedToken::static_token(SensitiveString::from(
                token.clone(),
            )));
        }

        let mut extra_headers = self.initial_config.extra_headers.clone();
        extra_headers.extend(explicit_headers_from_props(props)?);

        let mut extra_oauth_params = self.initial_config.extra_oauth_params.clone();
        for key in ["scope", "audience", "resource"] {
            if let Some(value) = props.get(key) {
                extra_oauth_params.insert(key.to_string(), value.to_string());
            }
        }

        let token_endpoint = match props.get("oauth2-server-uri") {
            Some(uri) if !uri.is_empty() => uri.clone(),
            // A default endpoint follows the merged catalog URI (which
            // `/v1/config` may have overridden); explicit ones are kept.
            _ if self.endpoint_is_default => props
                .get(REST_CATALOG_PROP_URI)
                .map(|uri| default_token_endpoint(uri))
                .unwrap_or_else(|| self.initial_config.token_endpoint.clone()),
            _ => self.initial_config.token_endpoint.clone(),
        };

        let credential = credential_from_props(props)
            .map(|(id, secret)| (id, secret.into()))
            .or_else(|| self.initial_config.credential.clone());

        let token_exchange = TokenExchangeConfig {
            client: client.clone(),
            extra_headers,
            token_endpoint,
            extra_oauth_params,
        };

        let session = OAuth2Session {
            token: self.token.clone(),
            // A configured token takes precedence over the credential: the
            // token cell is pre-seeded, and the credential only comes into
            // play once that token is gone.
            token_source: match credential {
                Some(credential) => {
                    TokenSource::ClientCredentials(Box::new(ClientCredentialsConfig {
                        token_exchange: token_exchange.clone(),
                        credential,
                    }))
                }
                None => TokenSource::StaticToken,
            },
            parent: None,
        };
        Ok((session, token_exchange))
    }
}

const AUTH_SESSION_TIMEOUT_MS_PROP: &str = "auth.session-timeout-ms";
const DEFAULT_CONTEXTUAL_SESSION_IDLE_TIMEOUT: Duration = Duration::from_hours(1);
// Moka's cache builder panics for expiration durations longer than 1,000
// years, so we validate the property before constructing the cache.
const MAX_CONTEXTUAL_SESSION_IDLE_TIMEOUT: Duration =
    Duration::from_secs(1_000 * 365 * 24 * 60 * 60);

fn session_idle_timeout(props: &HashMap<String, String>) -> Result<Duration> {
    let Some(timeout_prop) = props.get(AUTH_SESSION_TIMEOUT_MS_PROP) else {
        return Ok(DEFAULT_CONTEXTUAL_SESSION_IDLE_TIMEOUT);
    };

    let timeout_ms = timeout_prop.parse::<u64>().map_err(|e| {
        Error::new(
            ErrorKind::PreconditionFailed,
            format!("Property {} not an integer", AUTH_SESSION_TIMEOUT_MS_PROP),
        )
        .with_source(e)
    })?;

    let timeout = Duration::from_millis(timeout_ms);

    if timeout > MAX_CONTEXTUAL_SESSION_IDLE_TIMEOUT {
        return Err(Error::new(
            ErrorKind::PreconditionFailed,
            format!(
                "Property {AUTH_SESSION_TIMEOUT_MS_PROP} must not exceed {} ms, got {timeout_ms}",
                MAX_CONTEXTUAL_SESSION_IDLE_TIMEOUT.as_millis()
            ),
        ));
    }

    Ok(timeout)
}

/// Attaches `token` as a `Authorization: Bearer <token>` header, marked
/// sensitive so `Debug`-formatted requests redact it.
fn attach_bearer(req: &mut HttpRequest, token: &SensitiveString) -> Result<()> {
    let mut value: http::HeaderValue =
        format!("Bearer {}", token.expose()).parse().map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                "Invalid token received from catalog server!",
            )
            .with_source(e)
        })?;
    value.set_sensitive(true);
    req.headers_mut().insert(http::header::AUTHORIZATION, value);
    Ok(())
}

/// A cached bearer token and the instant when it must be refreshed.
struct CachedToken {
    value: SensitiveString,
    expires_at: Option<Instant>,
}

impl CachedToken {
    fn static_token(value: SensitiveString) -> Self {
        Self {
            value,
            expires_at: None,
        }
    }

    fn from_response(response: TokenResponse) -> Result<Self> {
        let expires_at = response
            .expires_in
            .map(|seconds| {
                Instant::now()
                    .checked_add(Duration::from_secs(seconds))
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "OAuth2 token expiration exceeds the supported time range",
                        )
                    })
            })
            .transpose()?;
        Ok(Self {
            value: SensitiveString::from(response.access_token),
            expires_at,
        })
    }

    fn is_expired(&self) -> bool {
        self.expires_at
            .is_some_and(|expires_at| Instant::now() >= expires_at)
    }
}

/// [`AuthSession`] attaching an OAuth2 bearer token.
///
/// The token is a configured one (which replaces whatever the cell holds), a
/// token cached by an earlier init or catalog session (their cell is shared
/// with the owning [`OAuth2Manager`]), or — with
/// [`TokenSource::ClientCredentials`] — one exchanged for the credential on
/// demand. Tokens obtained through client credentials are refreshed on demand
/// after their `expires_in` duration. Contextual sessions have their own token
/// cell.
struct OAuth2Session {
    token: Arc<Mutex<Option<CachedToken>>>,
    token_source: TokenSource,
    /// A contextual session inherits the parent's authentication and then
    /// replaces its bearer token with the contextual one.
    parent: Option<Arc<dyn AuthSession>>,
}

/// How an [`OAuth2Session`] obtains a token once none is cached.
enum TokenSource {
    /// Nothing to obtain: the session attaches the configured token, or no
    /// authentication at all when there is none.
    StaticToken,
    /// The credential is exchanged for a token at the token endpoint.
    ClientCredentials(Box<ClientCredentialsConfig>),
}

struct ClientCredentialsConfig {
    token_exchange: TokenExchangeConfig,
    credential: (Option<String>, SensitiveString),
}

impl Debug for OAuth2Session {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut out = f.debug_struct("OAuth2Session");
        if let TokenSource::ClientCredentials(config) = &self.token_source {
            out.field("token_endpoint", &config.token_exchange.token_endpoint);
        }
        out.finish_non_exhaustive()
    }
}

impl ClientCredentialsConfig {
    async fn exchange_credential_for_token(&self) -> Result<TokenResponse> {
        let (client_id, client_secret) = &self.credential;

        let mut params = HashMap::with_capacity(4);
        params.insert("grant_type", "client_credentials");
        if let Some(client_id) = client_id {
            params.insert("client_id", client_id);
        }
        params.insert("client_secret", client_secret.expose());
        params.extend(
            self.token_exchange
                .extra_oauth_params
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str())),
        );

        let response = self
            .token_exchange
            .client
            .post_form(
                &self.token_exchange.token_endpoint,
                &self.token_exchange.extra_headers,
                &params,
            )
            .await?;
        let status = response.status();
        let body = response.body();

        let auth_res: TokenResponse = if status == StatusCode::OK {
            Ok(serde_json::from_slice(body).map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to parse response from rest catalog server!",
                )
                .with_context("operation", "auth")
                .with_context("url", self.token_exchange.token_endpoint.clone())
                .with_context("json", String::from_utf8_lossy(body))
                .with_source(e)
            })?)
        } else {
            let e: ErrorResponse = serde_json::from_slice(body).map_err(|e| {
                Error::new(ErrorKind::Unexpected, "Received unexpected response")
                    .with_context("code", status.to_string())
                    .with_context("operation", "auth")
                    .with_context("url", self.token_exchange.token_endpoint.clone())
                    .with_context("json", String::from_utf8_lossy(body))
                    .with_source(e)
            })?;
            Err(Error::from(e))
        }?;
        Ok(auth_res)
    }
}

#[async_trait]
impl AuthSession for OAuth2Session {
    /// Uses the cached token when present; otherwise exchanges the credential
    /// for one, caches it, then uses it. Without a credential and without a
    /// token, no authentication is attached.
    async fn authenticate(&self, req: &mut HttpRequest) -> Result<()> {
        if let Some(parent) = &self.parent {
            parent.authenticate(req).await?;
        }

        // The lock is held across the exchange: waiters reuse a successful
        // result, and retry themselves after a failure.
        let token = {
            let mut cached_token = self.token.lock().await;
            match &self.token_source {
                TokenSource::StaticToken => cached_token
                    .as_ref()
                    .map(|cached_token| cached_token.value.clone()),
                TokenSource::ClientCredentials(config) => {
                    if cached_token.as_ref().is_none_or(CachedToken::is_expired) {
                        *cached_token = Some(CachedToken::from_response(
                            config.exchange_credential_for_token().await?,
                        )?);
                    }

                    cached_token
                        .as_ref()
                        .map(|cached_token| cached_token.value.clone())
                }
            }
        };

        match token {
            Some(token) => attach_bearer(req, &token),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use mockito::{Matcher, Server};
    use reqwest::Client;

    use super::*;
    use crate::RestCatalogConfig;

    fn test_client() -> HttpClient {
        HttpClient::new(
            &RestCatalogConfig::builder()
                .uri("http://localhost".to_string())
                .build(),
        )
        .unwrap()
    }

    fn request() -> HttpRequest {
        HttpRequest::new(
            Client::new()
                .get("https://rest.example.com/v1/namespaces")
                .build()
                .unwrap(),
        )
    }

    fn context_with_token(session_id: &str, token: &str) -> SessionContext {
        SessionContext::builder()
            .session_id(session_id.to_string())
            .credentials(HashMap::from([(
                "token".to_string(),
                SensitiveString::from(token.to_string()),
            )]))
            .build()
    }

    async fn bearer_token(session: &Arc<dyn AuthSession>) -> Option<String> {
        let mut request = request();
        session.authenticate(&mut request).await.unwrap();
        request
            .headers()
            .get(http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .map(str::to_string)
    }

    #[test]
    fn test_session_idle_timeout_property() {
        assert_eq!(
            session_idle_timeout(&HashMap::new()).unwrap(),
            DEFAULT_CONTEXTUAL_SESSION_IDLE_TIMEOUT
        );

        let custom_timeout = Duration::from_millis(1234);
        let props = HashMap::from([(
            AUTH_SESSION_TIMEOUT_MS_PROP.to_string(),
            custom_timeout.as_millis().to_string(),
        )]);
        assert_eq!(session_idle_timeout(&props).unwrap(), custom_timeout);

        let props = HashMap::from([(
            AUTH_SESSION_TIMEOUT_MS_PROP.to_string(),
            "not-an-integer".to_string(),
        )]);
        let error = session_idle_timeout(&props).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(
            error.message(),
            format!("Property {AUTH_SESSION_TIMEOUT_MS_PROP} not an integer")
        );

        let max_timeout_ms =
            u64::try_from(MAX_CONTEXTUAL_SESSION_IDLE_TIMEOUT.as_millis()).unwrap();
        let props = HashMap::from([(
            AUTH_SESSION_TIMEOUT_MS_PROP.to_string(),
            max_timeout_ms.to_string(),
        )]);

        assert_eq!(
            session_idle_timeout(&props).unwrap(),
            MAX_CONTEXTUAL_SESSION_IDLE_TIMEOUT
        );

        let props = HashMap::from([(
            AUTH_SESSION_TIMEOUT_MS_PROP.to_string(),
            (max_timeout_ms + 1).to_string(),
        )]);
        let error = session_idle_timeout(&props).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(
            error.message(),
            format!(
                "Property {AUTH_SESSION_TIMEOUT_MS_PROP} must not exceed {max_timeout_ms} ms, got {}",
                max_timeout_ms + 1
            )
        );
    }

    #[tokio::test]
    async fn test_contextual_session_requires_catalog_session_initialization() {
        let manager = OAuth2Manager::new("http://localhost/unused").with_token("parent-token");
        let parent: Arc<dyn AuthSession> = Arc::from(
            manager
                .init_session(&test_client(), &HashMap::new())
                .await
                .unwrap(),
        );
        let context = SessionContext::builder()
            .credentials(HashMap::from([(
                "token".to_string(),
                SensitiveString::from("context-token".to_string()),
            )]))
            .build();

        let error = manager
            .contextual_session(&context, parent)
            .await
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(
            error.message(),
            "OAuth2 catalog session must be initialized before contextual sessions"
        );
    }

    #[tokio::test]
    async fn test_static_token_session_attaches_token() {
        // Token-only config: the token is attached as-is.
        let manager = OAuth2Manager::new("http://localhost/unused").with_token("tok-static");
        let session = manager
            .init_session(&test_client(), &HashMap::new())
            .await
            .unwrap();

        let mut req = HttpRequest::new(
            Client::new()
                .get("https://rest.example.com/v1/config")
                .build()
                .unwrap(),
        );
        session.authenticate(&mut req).await.unwrap();
        assert_eq!(
            req.headers().get("authorization").unwrap(),
            "Bearer tok-static"
        );
    }

    #[tokio::test]
    async fn test_context_without_oauth_credentials_reuses_parent() {
        let manager = OAuth2Manager::new("http://localhost/unused").with_token("parent-token");
        let parent = manager
            .catalog_session(&test_client(), &HashMap::new())
            .await
            .unwrap();
        let context = SessionContext::builder()
            .credentials(HashMap::from([(
                "unsupported".to_string(),
                SensitiveString::from("value".to_string()),
            )]))
            .build();

        let session = manager
            .contextual_session(&context, parent.clone())
            .await
            .unwrap();

        assert!(Arc::ptr_eq(&session, &parent));
    }

    #[tokio::test]
    async fn test_zero_idle_timeout_disables_contextual_session_reuse() {
        let manager = OAuth2Manager::new("http://localhost/unused").with_token("parent-token");
        let parent = manager
            .catalog_session(
                &test_client(),
                &HashMap::from([(AUTH_SESSION_TIMEOUT_MS_PROP.to_string(), "0".to_string())]),
            )
            .await
            .unwrap();
        let first_context = context_with_token("session-1", "first-token");
        let replacement_context = context_with_token("session-1", "replacement-token");

        let first_session = manager
            .contextual_session(&first_context, parent.clone())
            .await
            .unwrap();
        let replacement_session = manager
            .contextual_session(&replacement_context, parent)
            .await
            .unwrap();

        assert!(!Arc::ptr_eq(&first_session, &replacement_session));
        assert_eq!(
            bearer_token(&replacement_session).await.as_deref(),
            Some("replacement-token")
        );
    }

    #[tokio::test]
    async fn test_context_token_takes_precedence_and_is_cached_by_session_id() {
        let manager = OAuth2Manager::new("http://localhost/unused").with_token("parent-token");
        let parent = manager
            .catalog_session(&test_client(), &HashMap::new())
            .await
            .unwrap();
        let context = SessionContext::builder()
            .session_id("session-1".to_string())
            .credentials(HashMap::from([
                (
                    "token".to_string(),
                    SensitiveString::from("context-token".to_string()),
                ),
                (
                    "credential".to_string(),
                    SensitiveString::from("client:secret".to_string()),
                ),
            ]))
            .build();

        let session = manager
            .contextual_session(&context, parent.clone())
            .await
            .unwrap();
        assert_eq!(
            bearer_token(&session).await.as_deref(),
            Some("context-token")
        );
        assert_eq!(bearer_token(&parent).await.as_deref(), Some("parent-token"));

        let same_id_with_new_token = context_with_token("session-1", "replacement-token");
        let cached = manager
            .contextual_session(&same_id_with_new_token, parent)
            .await
            .unwrap();

        assert!(Arc::ptr_eq(&session, &cached));
        assert_eq!(
            bearer_token(&cached).await.as_deref(),
            Some("context-token")
        );
    }

    #[tokio::test]
    async fn test_context_tokens_are_isolated_by_session_id() {
        let manager = OAuth2Manager::new("http://localhost/unused").with_token("parent-token");
        let parent = manager
            .catalog_session(&test_client(), &HashMap::new())
            .await
            .unwrap();
        let first_context = context_with_token("session-1", "first-token");
        let second_context = context_with_token("session-2", "second-token");

        let first_session = manager
            .contextual_session(&first_context, parent.clone())
            .await
            .unwrap();
        let second_session = manager
            .contextual_session(&second_context, parent)
            .await
            .unwrap();

        assert!(!Arc::ptr_eq(&first_session, &second_session));
        assert_eq!(
            bearer_token(&first_session).await.as_deref(),
            Some("first-token")
        );
        assert_eq!(
            bearer_token(&second_session).await.as_deref(),
            Some("second-token")
        );
    }

    #[tokio::test]
    async fn test_context_credential_is_exchanged_once_using_parent_auth() {
        let mut server = Server::new_async().await;
        let token_mock = server
            .mock("POST", "/tokens")
            .match_header("authorization", "Bearer parent-token")
            .match_body(Matcher::Regex("grant_type=client_credentials".to_string()))
            .match_body(Matcher::Regex("client_id=context-client".to_string()))
            .match_body(Matcher::Regex("client_secret=context-secret".to_string()))
            .match_body(Matcher::Regex("scope=catalog".to_string()))
            .with_status(200)
            .with_body(r#"{"access_token":"context-token","token_type":"Bearer"}"#)
            .expect(1)
            .create_async()
            .await;
        let manager =
            OAuth2Manager::new(format!("{}/tokens", server.url())).with_token("parent-token");
        let parent = manager
            .catalog_session(&test_client(), &HashMap::new())
            .await
            .unwrap();
        let context = SessionContext::builder()
            .session_id("session-1".to_string())
            .credentials(HashMap::from([(
                "credential".to_string(),
                SensitiveString::from("context-client:context-secret".to_string()),
            )]))
            .build();

        let session = manager
            .contextual_session(&context, parent.clone())
            .await
            .unwrap();
        assert_eq!(
            bearer_token(&session).await.as_deref(),
            Some("context-token")
        );
        assert_eq!(
            bearer_token(&session).await.as_deref(),
            Some("context-token")
        );

        let cached = manager.contextual_session(&context, parent).await.unwrap();
        assert!(Arc::ptr_eq(&session, &cached));
        token_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_context_credential_is_refreshed_after_expiration() {
        let mut server = Server::new_async().await;
        let token_mock = server
            .mock("POST", "/tokens")
            .match_header("authorization", "Bearer parent-token")
            .match_body(Matcher::Regex("grant_type=client_credentials".to_string()))
            .match_body(Matcher::Regex("client_id=context-client".to_string()))
            .match_body(Matcher::Regex("client_secret=context-secret".to_string()))
            .with_status(200)
            .with_body(r#"{"access_token":"context-token","token_type":"Bearer","expires_in":0}"#)
            .expect(2)
            .create_async()
            .await;
        let manager =
            OAuth2Manager::new(format!("{}/tokens", server.url())).with_token("parent-token");
        let parent = manager
            .catalog_session(&test_client(), &HashMap::new())
            .await
            .unwrap();
        let context = SessionContext::builder()
            .session_id("session-1".to_string())
            .credentials(HashMap::from([(
                "credential".to_string(),
                SensitiveString::from("context-client:context-secret".to_string()),
            )]))
            .build();
        let session = manager.contextual_session(&context, parent).await.unwrap();

        assert_eq!(
            bearer_token(&session).await.as_deref(),
            Some("context-token")
        );
        assert_eq!(
            bearer_token(&session).await.as_deref(),
            Some("context-token")
        );
        token_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_expired_credential_token_is_replaced() {
        let mut server = Server::new_async().await;
        let first_token_mock = server
            .mock("POST", "/tokens")
            .match_body(Matcher::Regex("grant_type=client_credentials".to_string()))
            .match_body(Matcher::Regex("client_id=client".to_string()))
            .match_body(Matcher::Regex("client_secret=secret".to_string()))
            .with_status(200)
            .with_body(r#"{"access_token":"token-1","token_type":"Bearer","expires_in":0}"#)
            .expect(1)
            .create_async()
            .await;
        let manager = OAuth2Manager::new(format!("{}/tokens", server.url()))
            .with_credential(Some("client".to_string()), "secret".to_string());
        let session: Arc<dyn AuthSession> = Arc::from(
            manager
                .init_session(&test_client(), &HashMap::new())
                .await
                .unwrap(),
        );

        assert_eq!(bearer_token(&session).await.as_deref(), Some("token-1"));
        first_token_mock.assert_async().await;
        first_token_mock.remove_async().await;

        let second_token_mock = server
            .mock("POST", "/tokens")
            .with_status(200)
            .with_body(r#"{"access_token":"token-2","token_type":"Bearer","expires_in":3600}"#)
            .expect(1)
            .create_async()
            .await;

        assert_eq!(bearer_token(&session).await.as_deref(), Some("token-2"));
        second_token_mock.assert_async().await;
    }

    #[test]
    fn test_token_without_expiration_has_no_expiry() {
        let response = TokenResponse {
            access_token: "token".to_string(),
            token_type: "Bearer".to_string(),
            expires_in: None,
            issued_token_type: None,
        };

        let cached_token = CachedToken::from_response(response).unwrap();

        assert!(cached_token.expires_at.is_none());
    }

    #[test]
    fn test_token_expiration_out_of_range() {
        let response = TokenResponse {
            access_token: "token".to_string(),
            token_type: "Bearer".to_string(),
            expires_in: Some(u64::MAX),
            issued_token_type: None,
        };

        let error = match CachedToken::from_response(response) {
            Ok(_) => panic!("expected token expiration to exceed Instant's range"),
            Err(error) => error,
        };

        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            error.message(),
            "OAuth2 token expiration exceeds the supported time range"
        );
    }
}
