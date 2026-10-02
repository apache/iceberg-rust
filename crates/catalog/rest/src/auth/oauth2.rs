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
use iceberg::sensitive::SensitiveString;
use iceberg::{Error, ErrorKind, Result};
use reqwest::header::HeaderMap;
use tokio::sync::Mutex;

use super::{AuthManager, AuthSession, HttpRequest};
use crate::catalog::{
    REST_CATALOG_PROP_URI, RestCatalogConfig, credential_from_props, default_token_endpoint,
    explicit_headers_from_props,
};
use crate::client::HttpClient;
use crate::types::{ErrorResponse, TokenResponse};

/// The manager's own OAuth2 options, which properties are merged onto.
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
    async fn init_session(
        &self,
        client: &HttpClient,
        props: &HashMap<String, String>,
    ) -> Result<Box<dyn AuthSession>> {
        Ok(Box::new(self.session_from(client, props).await?))
    }

    async fn catalog_session(
        &self,
        client: &HttpClient,
        props: &HashMap<String, String>,
    ) -> Result<Arc<dyn AuthSession>> {
        Ok(Arc::new(self.session_from(client, props).await?))
    }
}

impl OAuth2Manager {
    /// Builds a session from the manager's options with `props` merged onto
    /// them, so an injected manager keeps whatever a property doesn't
    /// override. The manager's token cell is shared with every session it
    /// builds, so a token cached during the handshake survives it.
    async fn session_from(
        &self,
        client: &HttpClient,
        props: &HashMap<String, String>,
    ) -> Result<OAuth2Session> {
        // The properties may carry a new token (or restate the user's).
        if let Some(token) = props.get("token") {
            *self.token.lock().await = Some(SensitiveString::from(token.clone()));
        }

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

        let credential = credential_from_props(props)
            .map(|(id, secret)| (id, secret.into()))
            .or_else(|| self.init_params.credential.clone());

        Ok(OAuth2Session {
            token: self.token.clone(),
            // A configured token takes precedence over the credential: the
            // token cell is pre-seeded, and the credential only comes into
            // play once that token is gone.
            token_source: match credential {
                Some(credential) => {
                    TokenSource::ClientCredentials(Box::new(ClientCredentialsConfig {
                        client: client.clone(),
                        credential,
                        token_endpoint,
                        extra_headers,
                        extra_oauth_params,
                    }))
                }
                None => TokenSource::StaticToken,
            },
        })
    }
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
    // Java's `OAuth2Util.AuthSession` puts the header only if absent, leaving a
    // configured `header.authorization` in place.
    if !req.headers().contains_key(http::header::AUTHORIZATION) {
        req.headers_mut().insert(http::header::AUTHORIZATION, value);
    }
    Ok(())
}

/// [`AuthSession`] attaching an OAuth2 bearer token.
///
/// The token is a configured one (which replaces whatever the cell holds), a
/// token cached by an earlier session (the cell is shared with the owning
/// [`OAuth2Manager`]), or — with [`TokenSource::ClientCredentials`] — one
/// exchanged for the credential on demand.
///
/// # TODO: Support automatic token refreshing.
struct OAuth2Session {
    token: Arc<Mutex<Option<SensitiveString>>>,
    token_source: TokenSource,
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
    client: HttpClient,
    credential: (Option<String>, SensitiveString),
    token_endpoint: String,
    extra_headers: HeaderMap,
    extra_oauth_params: HashMap<String, String>,
}

impl Debug for OAuth2Session {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut out = f.debug_struct("OAuth2Session");
        if let TokenSource::ClientCredentials(config) = &self.token_source {
            out.field("token_endpoint", &config.token_endpoint);
        }
        out.finish_non_exhaustive()
    }
}

impl ClientCredentialsConfig {
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

        let response = self
            .client
            .post_form(&self.token_endpoint, &self.extra_headers, &params)
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
                .with_context("url", self.token_endpoint.clone())
                .with_context("json", String::from_utf8_lossy(body))
                .with_source(e)
            })?)
        } else {
            let e: ErrorResponse = serde_json::from_slice(body).map_err(|e| {
                Error::new(ErrorKind::Unexpected, "Received unexpected response")
                    .with_context("code", status.to_string())
                    .with_context("operation", "auth")
                    .with_context("url", self.token_endpoint.clone())
                    .with_context("json", String::from_utf8_lossy(body))
                    .with_source(e)
            })?;
            Err(Error::from(e))
        }?;
        Ok(auth_res.access_token)
    }
}

#[async_trait]
impl AuthSession for OAuth2Session {
    /// Uses the cached token when present; otherwise exchanges the credential
    /// for one, caches it, then uses it. Without a credential and without a
    /// token, no authentication is attached.
    async fn authenticate(&self, req: &mut HttpRequest) -> Result<()> {
        // The lock is held across the exchange: waiters reuse a successful
        // result, and retry themselves after a failure.
        let token = {
            let mut token = self.token.lock().await;
            match (&*token, &self.token_source) {
                (Some(token), _) => Some(token.clone()),
                (None, TokenSource::StaticToken) => None,
                (None, TokenSource::ClientCredentials(config)) => {
                    let new_token =
                        SensitiveString::from(config.exchange_credential_for_token().await?);
                    *token = Some(new_token.clone());
                    Some(new_token)
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
}
