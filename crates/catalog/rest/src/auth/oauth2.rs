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

use super::{AuthManager, AuthRequest, AuthSession};
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
    credential: Option<(Option<String>, String)>,
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
    token: Arc<Mutex<Option<String>>>,
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
                // Same default as the configuration path (and the
                // pre-AuthManager client): the Iceberg catalog scope.
                extra_oauth_params: HashMap::from([("scope".to_string(), "catalog".to_string())]),
            },
            endpoint_is_default: false,
        }
    }

    /// Sets a bearer token used directly (takes precedence over `credential`).
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Arc::new(Mutex::new(Some(token.into())));
        self
    }

    /// Sets the client credential exchanged for a token at the token endpoint.
    pub fn with_credential(mut self, client_id: Option<String>, client_secret: String) -> Self {
        self.init_params.credential = Some((client_id, client_secret));
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
            token: Arc::new(Mutex::new(cfg.token())),
            init_params: OAuth2Params {
                extra_headers: cfg.extra_headers()?,
                token_endpoint: cfg.get_token_endpoint(),
                credential: cfg.credential(),
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
    async fn init_session(&self) -> Result<Arc<dyn AuthSession>> {
        Ok(Arc::new(OAuth2Session {
            client: self.client.clone(),
            token: self.token.clone(),
            params: self.init_params.clone(),
        }))
    }

    async fn catalog_session(
        &self,
        props: &HashMap<String, String>,
    ) -> Result<Arc<dyn AuthSession>> {
        // The server config may carry a new token (or restate the user's).
        if let Some(token) = props.get("token") {
            *self.token.lock().await = Some(token.clone());
        }

        // Explicit property overrides are merged ONTO the manager's configured
        // options: an injected manager keeps its token endpoint, extra headers
        // and OAuth params unless a property explicitly overrides them.
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
            // The built-in manager's default endpoint follows the merged
            // catalog URI (a `/v1/config` override may have changed it);
            // injected managers keep their explicitly configured endpoint.
            _ if self.endpoint_is_default => props
                .get(REST_CATALOG_PROP_URI)
                .map(|uri| default_token_endpoint(uri))
                .unwrap_or_else(|| self.init_params.token_endpoint.clone()),
            _ => self.init_params.token_endpoint.clone(),
        };

        Ok(Arc::new(OAuth2Session {
            client: self.client.clone(),
            token: self.token.clone(),
            params: OAuth2Params {
                extra_headers,
                token_endpoint,
                credential: credential_from_props(props)
                    .or_else(|| self.init_params.credential.clone()),
                extra_oauth_params,
            },
        }))
    }
}

/// [`AuthSession`] adding a `Authorization: Bearer <token>` header.
struct OAuth2Session {
    client: Client,
    /// Cached bearer token, shared with the owning [`OAuth2Manager`].
    token: Arc<Mutex<Option<String>>>,
    params: OAuth2Params,
}

impl Debug for OAuth2Session {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuth2Session")
            .field("token_endpoint", &self.params.token_endpoint)
            .finish_non_exhaustive()
    }
}

impl OAuth2Session {
    async fn exchange_credential_for_token(&self) -> Result<String> {
        // Credential must exist here.
        let (client_id, client_secret) = self.params.credential.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Credential must be provided for authentication",
            )
        })?;

        let mut params = HashMap::with_capacity(4);
        params.insert("grant_type", "client_credentials");
        if let Some(client_id) = client_id {
            params.insert("client_id", client_id);
        }
        params.insert("client_secret", client_secret);
        params.extend(
            self.params
                .extra_oauth_params
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str())),
        );

        let mut auth_req = self
            .client
            .request(Method::POST, &self.params.token_endpoint)
            .headers(self.params.extra_headers.clone())
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
impl AuthSession for OAuth2Session {
    /// Adds a bearer token to the authorization header.
    ///
    /// Three modes:
    ///
    /// 1. **No authentication** - Skip when both `credential` and `token` are missing.
    /// 2. **Token authentication** - Use the provided `token` directly.
    /// 3. **OAuth authentication** - Exchange `credential` for a token, cache it, then use it.
    ///
    /// When both `credential` and `token` are present, `token` takes precedence.
    ///
    /// # TODO: Support automatic token refreshing.
    async fn authenticate(&self, req: &mut AuthRequest<'_>) -> Result<()> {
        // Clone the token from lock without holding the lock for entire function.
        let token = self.token.lock().await.clone();

        if self.params.credential.is_none() && token.is_none() {
            return Ok(());
        }

        // Either use the provided token or exchange credential for token, cache and use that
        let token = match token {
            Some(token) => token,
            None => {
                let token = self.exchange_credential_for_token().await?;
                // Update token so that we use it for next request instead of
                // exchanging credential for token from the server again
                *self.token.lock().await = Some(token.clone());
                token
            }
        };

        // Insert token in request.
        req.headers_mut().insert(
            http::header::AUTHORIZATION,
            format!("Bearer {token}").parse().map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Invalid token received from catalog server!",
                )
                .with_source(e)
            })?,
        );

        Ok(())
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
        *self.token.lock().await = Some(new_token);
        Ok(())
    }

    #[cfg(test)]
    async fn bearer_token(&self) -> Option<String> {
        self.token.lock().await.clone()
    }
}
