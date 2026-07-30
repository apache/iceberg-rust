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
use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use iceberg::{Error, ErrorKind, Result};
use reqwest::header::HeaderMap;
use reqwest::{Client, Method};
use tokio::sync::Mutex;

use super::{AuthManager, AuthRequest, AuthSession, SensitiveString};
use crate::catalog::{
    REST_CATALOG_PROP_URI, RestCatalogConfig, credential_from_props, default_token_endpoint,
    explicit_headers_from_props,
};
use crate::types::{ErrorResponse, TokenResponse};

/// Per-phase OAuth2 parameters (init vs. post-handshake catalog phase).
#[derive(Clone)]
struct OAuth2Params {
    extra_headers: HeaderMap,
    token_endpoint: String,
    credential: Option<(Option<String>, SensitiveString)>,
    extra_oauth_params: HashMap<String, String>,
}

/// [`AuthManager`] implementing the OAuth2 client-credentials flow used by
/// Iceberg REST catalogs.
///
/// A configured `token` is used directly; otherwise `credential` is exchanged
/// for a token at the token endpoint and cached. The cached token is shared
/// across sessions so it survives the config handshake.
pub struct OAuth2Manager {
    client: Client,
    token: Arc<Mutex<Option<SensitiveString>>>,
    init_params: OAuth2Params,
    /// True when the token endpoint was derived from the catalog URI (not
    /// explicitly configured): it is then recomputed from the merged URI in
    /// [`Self::catalog_session`], since `/v1/config` may override the URI.
    endpoint_is_default: bool,
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
            client: Client::default(),
            token: Arc::new(Mutex::new(None)),
            init_params: OAuth2Params {
                extra_headers: HeaderMap::new(),
                token_endpoint: token_endpoint.into(),
                credential: None,
                // Same default as the configuration path: the catalog scope.
                extra_oauth_params: HashMap::from([("scope".to_string(), "catalog".to_string())]),
            },
            endpoint_is_default: false,
        }
    }

    /// Sets a bearer token used directly (takes precedence over `credential`).
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Arc::new(Mutex::new(Some(SensitiveString::from(token.into()))));
        self
    }

    /// Sets the client credential exchanged for a token at the token endpoint.
    pub fn with_credential(mut self, client_id: Option<String>, client_secret: String) -> Self {
        self.init_params.credential = Some((client_id, client_secret.into()));
        self
    }

    /// Sets the HTTP client used for token requests.
    pub fn with_client(mut self, client: Client) -> Self {
        self.client = client;
        self
    }

    /// Sets extra headers sent with token requests.
    pub fn with_extra_headers(mut self, headers: HeaderMap) -> Self {
        self.init_params.extra_headers = headers;
        self
    }

    /// Adds extra OAuth2 form parameters (e.g. `scope`, `audience`), merged
    /// onto the defaults: provide a `scope` entry to replace the default
    /// `catalog` scope.
    pub fn with_extra_oauth_params(mut self, params: HashMap<String, String>) -> Self {
        self.init_params.extra_oauth_params.extend(params);
        self
    }

    pub(crate) fn from_config(cfg: &RestCatalogConfig) -> Result<Self> {
        Ok(Self {
            client: cfg.client(),
            token: Arc::new(Mutex::new(cfg.token().map(SensitiveString::from))),
            init_params: OAuth2Params {
                extra_headers: cfg.extra_headers()?,
                token_endpoint: cfg.get_token_endpoint(),
                credential: cfg.credential().map(|(id, secret)| (id, secret.into())),
                extra_oauth_params: cfg.extra_oauth_params(),
            },
            endpoint_is_default: cfg.explicit_oauth2_server_uri().is_none(),
        })
    }
}

impl Debug for OAuth2Manager {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuth2Manager")
            .field("token_endpoint", &self.init_params.token_endpoint)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl AuthManager for OAuth2Manager {
    async fn init_session(&self) -> Result<Box<dyn AuthSession>> {
        Ok(self.build_session(self.init_params.clone()))
    }

    async fn catalog_session(
        &self,
        props: &HashMap<String, String>,
    ) -> Result<Arc<dyn AuthSession>> {
        // The server config may carry a new token (or restate the user's).
        if let Some(token) = props.get("token") {
            *self.token.lock().await = Some(SensitiveString::from(token.clone()));
        }

        // Explicit property overrides merge ONTO the manager's options, so an
        // injected manager keeps whatever a property doesn't override.
        let mut extra_headers = self.init_params.extra_headers.clone();
        extra_headers.extend(explicit_headers_from_props(props)?);

        let mut extra_oauth_params = self.init_params.extra_oauth_params.clone();
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
                .unwrap_or_else(|| self.init_params.token_endpoint.clone()),
            _ => self.init_params.token_endpoint.clone(),
        };

        Ok(Arc::from(
            self.build_session(OAuth2Params {
                extra_headers,
                token_endpoint,
                credential: credential_from_props(props)
                    .map(|(id, secret)| (id, secret.into()))
                    .or_else(|| self.init_params.credential.clone()),
                extra_oauth_params,
            }),
        ))
    }
}

impl OAuth2Manager {
    /// Builds the session matching the configured mode:
    ///
    /// - a `credential` yields a [`ClientCredentialsSession`] (its token cache
    ///   pre-seeded when a `token` is also set, and the token then takes
    ///   precedence until invalidated);
    /// - otherwise a [`StaticTokenSession`], which attaches the configured
    ///   token as-is — or nothing when none is set.
    ///
    /// Both share the manager's token cell, so a cached token survives the
    /// config handshake and `invalidate` is observed by later sessions.
    fn build_session(&self, params: OAuth2Params) -> Box<dyn AuthSession> {
        match params.credential {
            Some(credential) => Box::new(ClientCredentialsSession {
                client: self.client.clone(),
                token: self.token.clone(),
                credential,
                token_endpoint: params.token_endpoint,
                extra_headers: params.extra_headers,
                extra_oauth_params: params.extra_oauth_params,
            }),
            None => Box::new(StaticTokenSession {
                token: self.token.clone(),
            }),
        }
    }
}

/// Attaches `token` as a `Authorization: Bearer <token>` header, marked
/// sensitive so `Debug`-formatted requests redact it.
fn attach_bearer(req: &mut AuthRequest<'_>, token: &SensitiveString) -> Result<()> {
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

/// [`AuthSession`] for a pre-configured bearer token: attaches it as-is and
/// cannot obtain a new one (there is no credential to exchange).
#[derive(Debug)]
struct StaticTokenSession {
    /// Shared with the owning [`OAuth2Manager`].
    token: Arc<Mutex<Option<SensitiveString>>>,
}

#[async_trait]
impl AuthSession for StaticTokenSession {
    async fn authenticate(&self, req: &mut AuthRequest<'_>) -> Result<()> {
        // After `invalidate` there is nothing to fall back to: no auth is sent.
        match self.token.lock().await.clone() {
            Some(token) => attach_bearer(req, &token),
            None => Ok(()),
        }
    }

    async fn invalidate(&self) -> Result<()> {
        *self.token.lock().await = None;
        Ok(())
    }

    async fn refresh(&self) -> Result<()> {
        Err(Error::new(
            ErrorKind::DataInvalid,
            "Credential must be provided for authentication",
        ))
    }
}

/// [`AuthSession`] implementing the OAuth2 client-credentials flow: exchanges
/// the credential for a token at the token endpoint and caches it.
///
/// # TODO: Support automatic token refreshing.
struct ClientCredentialsSession {
    client: Client,
    /// Cached bearer token, shared with the owning [`OAuth2Manager`].
    token: Arc<Mutex<Option<SensitiveString>>>,
    credential: (Option<String>, SensitiveString),
    token_endpoint: String,
    extra_headers: HeaderMap,
    extra_oauth_params: HashMap<String, String>,
}

impl Debug for ClientCredentialsSession {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientCredentialsSession")
            .field("token_endpoint", &self.token_endpoint)
            .finish_non_exhaustive()
    }
}

impl ClientCredentialsSession {
    async fn exchange_credential_for_token(&self) -> Result<String> {
        let (client_id, client_secret) = &self.credential;

        let mut params = HashMap::with_capacity(4);
        params.insert("grant_type", "client_credentials");
        if let Some(client_id) = client_id {
            params.insert("client_id", client_id);
        }
        params.insert("client_secret", client_secret.expose());
        params.extend(
            self.extra_oauth_params
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str())),
        );

        let mut auth_req = self
            .client
            .request(Method::POST, &self.token_endpoint)
            .headers(self.extra_headers.clone())
            .form(&params)
            .build()?;
        // extra headers add content-type application/json header it's necessary to override it with proper type
        // note that form call doesn't add content-type header if already present
        auth_req.headers_mut().insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/x-www-form-urlencoded"),
        );
        let auth_url = auth_req.url().clone();
        let auth_resp = self.client.execute(auth_req).await?;

        let auth_res: TokenResponse = if auth_resp.status() == StatusCode::OK {
            let text = auth_resp
                .bytes()
                .await
                .map_err(|err| err.with_url(auth_url.clone()))?;
            Ok(serde_json::from_slice(&text).map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to parse response from rest catalog server!",
                )
                .with_context("operation", "auth")
                .with_context("url", auth_url.to_string())
                .with_context("json", String::from_utf8_lossy(&text))
                .with_source(e)
            })?)
        } else {
            let code = auth_resp.status();
            let text = auth_resp
                .bytes()
                .await
                .map_err(|err| err.with_url(auth_url.clone()))?;
            let e: ErrorResponse = serde_json::from_slice(&text).map_err(|e| {
                Error::new(ErrorKind::Unexpected, "Received unexpected response")
                    .with_context("code", code.to_string())
                    .with_context("operation", "auth")
                    .with_context("url", auth_url.to_string())
                    .with_context("json", String::from_utf8_lossy(&text))
                    .with_source(e)
            })?;
            Err(Error::from(e))
        }?;
        Ok(auth_res.access_token)
    }
}

#[async_trait]
impl AuthSession for ClientCredentialsSession {
    /// Uses the cached token when present (a configured `token` takes
    /// precedence over the credential until invalidated); otherwise exchanges
    /// the credential for a token, caches it, then uses it.
    async fn authenticate(&self, req: &mut AuthRequest<'_>) -> Result<()> {
        // The lock is held across the exchange: waiters reuse a successful
        // result, and retry themselves after a failure.
        let token = {
            let mut token = self.token.lock().await;
            match &*token {
                Some(token) => token.clone(),
                None => {
                    let new_token =
                        SensitiveString::from(self.exchange_credential_for_token().await?);
                    *token = Some(new_token.clone());
                    new_token
                }
            }
        };

        attach_bearer(req, &token)
    }

    /// Invalidate the current token without generating a new one. On the next
    /// request, the session will attempt to generate a new token.
    async fn invalidate(&self) -> Result<()> {
        *self.token.lock().await = None;
        Ok(())
    }

    /// Invalidate the current token and set a new one. Generates a new token
    /// before invalidating the current one, meaning the old token will be used
    /// until this function acquires the lock and overwrites the token.
    ///
    /// If credential is invalid, or the request fails, this method will return
    /// an error and leave the current token unchanged.
    async fn refresh(&self) -> Result<()> {
        let new_token = self.exchange_credential_for_token().await?;
        *self.token.lock().await = Some(new_token.into());
        Ok(())
    }
}
