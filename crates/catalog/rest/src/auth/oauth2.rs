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

//! OAuth2 authentication for the REST catalog.

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

use async_trait::async_trait;
use http::StatusCode;
use iceberg::{Error, ErrorKind, Result};
use reqwest::{Client, Method, Request};
use tokio::sync::Mutex;

use super::Authenticator;
use crate::types::{ErrorResponse, TokenResponse};

/// OAuth2 authenticator for REST catalog.
///
/// This authenticator supports two modes:
/// 1. **Token mode**: Use a pre-provided bearer token directly
/// 2. **Credential mode**: Exchange client credentials for a token using OAuth2 flow
///
/// When both token and credentials are provided, the token takes precedence.
pub struct OAuth2Authenticator {
    client: Client,
    /// Cached bearer token
    token: Mutex<Option<String>>,
    /// OAuth2 token endpoint URL
    token_endpoint: String,
    /// Client credentials: (client_id, client_secret)
    credential: Option<(Option<String>, String)>,
    /// Additional OAuth2 parameters (scope, audience, resource)
    extra_params: HashMap<String, String>,
}

impl Debug for OAuth2Authenticator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuth2Authenticator")
            .field("token_endpoint", &self.token_endpoint)
            .field("has_credential", &self.credential.is_some())
            .field("extra_params", &self.extra_params)
            .finish_non_exhaustive()
    }
}

impl OAuth2Authenticator {
    /// Creates a new OAuth2 authenticator with a pre-provided token.
    ///
    /// # Arguments
    ///
    /// * `token` - The bearer token to use for authentication
    pub fn with_token(token: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            token: Mutex::new(Some(token.into())),
            token_endpoint: String::new(),
            credential: None,
            extra_params: HashMap::new(),
        }
    }

    /// Creates a new OAuth2 authenticator with client credentials.
    ///
    /// The authenticator will exchange these credentials for a bearer token
    /// using the OAuth2 client credentials flow.
    ///
    /// # Arguments
    ///
    /// * `client_id` - The OAuth2 client ID (optional)
    /// * `client_secret` - The OAuth2 client secret
    /// * `token_endpoint` - The URL of the OAuth2 token endpoint
    pub fn with_credentials(
        client_id: Option<impl Into<String>>,
        client_secret: impl Into<String>,
        token_endpoint: impl Into<String>,
    ) -> Self {
        Self {
            client: Client::new(),
            token: Mutex::new(None),
            token_endpoint: token_endpoint.into(),
            credential: Some((client_id.map(Into::into), client_secret.into())),
            extra_params: HashMap::new(),
        }
    }

    /// Creates a new OAuth2 authenticator with both token and credentials.
    ///
    /// The token will be used initially, and credentials will be used to
    /// refresh the token when needed.
    ///
    /// # Arguments
    ///
    /// * `token` - Initial bearer token (optional)
    /// * `client_id` - The OAuth2 client ID (optional)
    /// * `client_secret` - The OAuth2 client secret
    /// * `token_endpoint` - The URL of the OAuth2 token endpoint
    pub fn new(
        token: Option<impl Into<String>>,
        client_id: Option<impl Into<String>>,
        client_secret: impl Into<String>,
        token_endpoint: impl Into<String>,
    ) -> Self {
        Self {
            client: Client::new(),
            token: Mutex::new(token.map(Into::into)),
            token_endpoint: token_endpoint.into(),
            credential: Some((client_id.map(Into::into), client_secret.into())),
            extra_params: HashMap::new(),
        }
    }

    /// Sets additional OAuth2 parameters to include in token requests.
    ///
    /// Common parameters include:
    /// - `scope`: The OAuth2 scope (default: "catalog")
    /// - `audience`: The intended audience for the token
    /// - `resource`: The resource being accessed
    pub fn with_extra_params(mut self, params: HashMap<String, String>) -> Self {
        self.extra_params = params;
        self
    }

    /// Sets a custom HTTP client for token requests.
    pub fn with_client(mut self, client: Client) -> Self {
        self.client = client;
        self
    }

    /// Exchange credentials for a new token.
    async fn exchange_credential_for_token(&self) -> Result<String> {
        let (client_id, client_secret) = self.credential.as_ref().ok_or_else(|| {
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
            self.extra_params
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str())),
        );

        let mut auth_req = self
            .client
            .request(Method::POST, &self.token_endpoint)
            .form(&params)
            .build()?;

        // Ensure correct content-type for form data
        auth_req.headers_mut().insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/x-www-form-urlencoded"),
        );

        let auth_url = auth_req.url().clone();
        let auth_resp = self.client.execute(auth_req).await?;

        if auth_resp.status() == StatusCode::OK {
            let text = auth_resp
                .bytes()
                .await
                .map_err(|err| err.with_url(auth_url.clone()))?;

            let token_response: TokenResponse = serde_json::from_slice(&text).map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to parse response from rest catalog server!",
                )
                .with_context("operation", "auth")
                .with_context("url", auth_url.to_string())
                .with_context("json", String::from_utf8_lossy(&text))
                .with_source(e)
            })?;

            Ok(token_response.access_token)
        } else {
            let code = auth_resp.status();
            let text = auth_resp
                .bytes()
                .await
                .map_err(|err| err.with_url(auth_url.clone()))?;

            let error_response: ErrorResponse = serde_json::from_slice(&text).map_err(|e| {
                Error::new(ErrorKind::Unexpected, "Received unexpected response")
                    .with_context("code", code.to_string())
                    .with_context("operation", "auth")
                    .with_context("url", auth_url.to_string())
                    .with_context("json", String::from_utf8_lossy(&text))
                    .with_source(e)
            })?;

            Err(Error::from(error_response))
        }
    }
}

#[async_trait]
impl Authenticator for OAuth2Authenticator {
    async fn authenticate(&self, request: &mut Request) -> Result<()> {
        // Clone the token from lock without holding the lock for entire function
        let token = self.token.lock().await.clone();

        if self.credential.is_none() && token.is_none() {
            return Ok(());
        }

        // Either use the provided token or exchange credential for token
        let token = match token {
            Some(token) => token,
            None => {
                let new_token = self.exchange_credential_for_token().await?;
                *self.token.lock().await = Some(new_token.clone());
                new_token
            }
        };

        // Insert bearer token in request
        request.headers_mut().insert(
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

    async fn invalidate(&self) -> Result<()> {
        *self.token.lock().await = None;
        Ok(())
    }

    async fn regenerate(&self) -> Result<()> {
        if self.credential.is_some() {
            let new_token = self.exchange_credential_for_token().await?;
            *self.token.lock().await = Some(new_token);
        }
        Ok(())
    }

    fn scheme_name(&self) -> &'static str {
        "oauth2"
    }

    async fn get_token(&self) -> Option<String> {
        self.token.lock().await.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oauth2_with_token() {
        let auth = OAuth2Authenticator::with_token("test-token");
        assert_eq!(auth.scheme_name(), "oauth2");
    }

    #[test]
    fn test_oauth2_with_credentials() {
        let auth = OAuth2Authenticator::with_credentials(
            Some("client_id"),
            "client_secret",
            "https://auth.example.com/token",
        );
        assert!(auth.credential.is_some());
        assert_eq!(auth.token_endpoint, "https://auth.example.com/token");
    }

    #[test]
    fn test_oauth2_debug_does_not_leak_secrets() {
        let auth = OAuth2Authenticator::with_credentials(
            Some("client_id"),
            "super-secret",
            "https://auth.example.com/token",
        );
        let debug_str = format!("{auth:?}");
        assert!(!debug_str.contains("super-secret"));
    }

    #[tokio::test]
    async fn test_oauth2_with_token_adds_bearer_header() {
        let auth = OAuth2Authenticator::with_token("test-token-123");
        let client = reqwest::Client::new();
        let mut request = client
            .get("http://example.com/test")
            .build()
            .expect("Failed to build request");

        auth.authenticate(&mut request).await.unwrap();

        let auth_header = request.headers().get(http::header::AUTHORIZATION);
        assert!(auth_header.is_some());
        assert_eq!(auth_header.unwrap(), "Bearer test-token-123");
    }

    #[tokio::test]
    async fn test_oauth2_invalidate_clears_token() {
        let auth = OAuth2Authenticator::with_token("test-token");

        // Token should be set initially
        assert!(auth.token.lock().await.is_some());

        // Invalidate should clear the token
        auth.invalidate().await.unwrap();
        assert!(auth.token.lock().await.is_none());
    }

    #[tokio::test]
    async fn test_oauth2_no_credentials_no_token_skips_auth() {
        let auth = OAuth2Authenticator {
            client: Client::new(),
            token: Mutex::new(None),
            token_endpoint: String::new(),
            credential: None,
            extra_params: HashMap::new(),
        };

        let client = reqwest::Client::new();
        let mut request = client
            .get("http://example.com/test")
            .build()
            .expect("Failed to build request");

        auth.authenticate(&mut request).await.unwrap();

        // No Authorization header should be added
        assert!(request.headers().get(http::header::AUTHORIZATION).is_none());
    }

    #[test]
    fn test_oauth2_with_extra_params() {
        let mut params = HashMap::new();
        params.insert("scope".to_string(), "custom-scope".to_string());
        params.insert("audience".to_string(), "custom-audience".to_string());

        let auth = OAuth2Authenticator::with_credentials(
            Some("client_id"),
            "client_secret",
            "https://auth.example.com/token",
        )
        .with_extra_params(params);

        assert_eq!(auth.extra_params.get("scope").unwrap(), "custom-scope");
        assert_eq!(
            auth.extra_params.get("audience").unwrap(),
            "custom-audience"
        );
    }
}
