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

//! Pluggable authentication for the REST catalog client.
//!
//! Most generic trait: [`RequestAuthenticator`]
//!  - Allows arbitrary mutations to the outgoing [`reqwest::Request`].
//!
//! Less generic trait: [`TokenProvider`]
//!  - Produces a bearer token. e.g. OAuth
//!  - Use [`BearerTokenAuthenticator`] to wrap a [`TokenProvider`] into a [`RequestAuthenticator`].

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use http::{Method, StatusCode};
use iceberg::{Error, ErrorKind, Result};
use reqwest::header::HeaderMap;
use reqwest::{Client, Request};
use tokio::sync::Mutex;

use crate::{ErrorResponse, TokenResponse};

/// Authenticates outgoing REST-catalog requests by mutating them in place.
///
/// Implementations may add headers, rewrite the URL, or do anything else they
/// need before the request is sent. This is the lowest-level auth hook;
/// callers that just need to attach a bearer token should typically implement
/// [`TokenProvider`] and rely on the [`BearerTokenAuthenticator`] adapter.
#[async_trait]
pub trait RequestAuthenticator: Send + Sync + Debug {
    /// Apply authentication to the outgoing request.
    async fn authenticate_request(&self, req: &mut Request) -> Result<()>;

    /// Discard any cached auth state (tokens, signatures, credentials, etc).
    async fn invalidate_cache(&self) -> Result<()>;

    /// Refresh the cached auth state (e.g. a bearer token).
    ///
    /// Expected flow:
    ///   1.  Attempt to regenerate the auth state (e.g. fetch a new bearer token).
    ///   2a. If regeneration succeeded, use the new state and discard the old state. (e.g. replace the token)
    ///   2b. If regeneration failed, keep the old state but surface the error. (e.g. keep the old token)
    async fn regenerate_cache(&self) -> Result<()>;
}

/// Produces bearer tokens for authenticated REST catalog requests.
/// Caching, expiry, and refresh are up to the implementer.
#[async_trait]
pub trait TokenProvider: Send + Sync + Debug {
    /// Get a token for the next request.
    async fn token(&self) -> Result<String>;

    /// Discard any cached token if it exists.
    async fn invalidate(&self) -> Result<()>;

    /// Try generating a new cached token.
    async fn regenerate(&self) -> Result<()>;
}

/// Adapts a [`TokenProvider`] into a [`RequestAuthenticator`] by inserting
/// `Authorization: Bearer <token>` on the outgoing request.
#[derive(Debug, Clone)]
pub struct BearerTokenAuthenticator {
    provider: Arc<dyn TokenProvider>,
}

impl BearerTokenAuthenticator {
    /// Wrap the given token provider so it can be used as a `RequestAuthenticator`.
    pub fn new(provider: Arc<dyn TokenProvider>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl RequestAuthenticator for BearerTokenAuthenticator {
    async fn authenticate_request(&self, req: &mut Request) -> Result<()> {
        let token = self.provider.token().await?;
        req.headers_mut().insert(
            http::header::AUTHORIZATION,
            format!("Bearer {token}").parse().map_err(|e| {
                Error::new(ErrorKind::DataInvalid, "Invalid authorization token!").with_source(e)
            })?,
        );
        Ok(())
    }

    async fn invalidate_cache(&self) -> Result<()> {
        self.provider.invalidate().await
    }

    async fn regenerate_cache(&self) -> Result<()> {
        self.provider.regenerate().await
    }
}

/// Always produce the same bearer token.
pub struct StaticTokenProvider {
    token: String,
}

impl Debug for StaticTokenProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticTokenProvider")
            .finish_non_exhaustive()
    }
}

impl StaticTokenProvider {
    /// We'll provide the same bearer token for every request.
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }
}

#[async_trait]
impl TokenProvider for StaticTokenProvider {
    async fn token(&self) -> Result<String> {
        Ok(self.token.clone())
    }

    async fn invalidate(&self) -> Result<()> {
        // No-op since we only have the one token.
        Ok(())
    }

    async fn regenerate(&self) -> Result<()> {
        // No-op since we only have the one token.
        Ok(())
    }
}

/// Use the standard OAuth2 flow to obtain bearer tokens.
pub struct OAuth2TokenProvider {
    client: Client,

    /// Authenticate as this entity.
    client_id: Option<String>,
    /// Authenticate using this secret.
    client_secret: String,

    /// Authenticate here.
    token_endpoint: String,

    /// Request headers.
    extra_headers: HeaderMap,

    /// Form params.
    extra_oauth_params: HashMap<String, String>,

    /// Most recently fetched token.
    cached_token: Mutex<Option<String>>,
}

impl Debug for OAuth2TokenProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Omit `client_secret` and `cached_token`: they're secrets. For
        // `extra_headers`/`extra_oauth_params` show only the keys, since the
        // values may carry secrets (e.g. a Basic `Authorization` header).
        f.debug_struct("OAuth2TokenProvider")
            .field("client_id", &self.client_id)
            .field("token_endpoint", &self.token_endpoint)
            .field(
                "extra_headers",
                &self.extra_headers.keys().collect::<Vec<_>>(),
            )
            .field(
                "extra_oauth_params",
                &self.extra_oauth_params.keys().collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

impl OAuth2TokenProvider {
    /// Construct a new token provider.
    pub fn new(
        client: Client,
        client_id: Option<String>,
        client_secret: String,
        token_endpoint: String,
        extra_headers: HeaderMap,
        extra_oauth_params: HashMap<String, String>,
        cached_token: Option<String>,
    ) -> Self {
        Self {
            client,
            client_id,
            client_secret,
            token_endpoint,
            extra_headers,
            extra_oauth_params,
            cached_token: Mutex::new(cached_token),
        }
    }

    /// Just fetch a token. Don't store it or do anything else.
    async fn exchange_credential_for_token(&self) -> Result<String> {
        let mut params = HashMap::with_capacity(4);
        params.insert("grant_type", "client_credentials");
        if let Some(client_id) = &self.client_id {
            params.insert("client_id", client_id);
        }
        params.insert("client_secret", &self.client_secret);
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
                // We omit the response text from the error context
                // because an OK response could include an auth token (even if we can't parse it).
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to parse response from OAuth2 token provider!",
                )
                .with_context("operation", "auth")
                .with_context("url", auth_url.to_string())
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
impl TokenProvider for OAuth2TokenProvider {
    /// Fetch a new token if we don't already have one cached.
    async fn token(&self) -> Result<String> {
        let mut cached = self.cached_token.lock().await;

        if let Some(token) = cached.clone() {
            return Ok(token);
        }

        let token = self.exchange_credential_for_token().await?;
        *cached = Some(token.clone());
        Ok(token)
    }

    async fn invalidate(&self) -> Result<()> {
        *self.cached_token.lock().await = None;
        Ok(())
    }

    /// Try fetching and caching a new token. If that fails, keep the old token.
    async fn regenerate(&self) -> Result<()> {
        let mut cached = self.cached_token.lock().await;
        *cached = Some(self.exchange_credential_for_token().await?);
        Ok(())
    }
}
