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

use iceberg::Result;
use reqwest::header::HeaderMap;
use reqwest::{Client, IntoUrl, Method, Request, RequestBuilder, Response};
use serde::de::DeserializeOwned;

use crate::RestCatalogConfig;
use crate::auth::{Authenticator, NoAuth, OAuth2Authenticator};

pub(crate) struct HttpClient {
    client: Client,
    /// The authenticator to use for requests
    authenticator: Arc<dyn Authenticator>,
    /// Extra headers to be added to each request.
    extra_headers: HeaderMap,
    /// Extra oauth parameters to be added to each authentication request.
    extra_oauth_params: HashMap<String, String>,
    /// Whether to disable header redaction in error logs (defaults to false for security).
    disable_header_redaction: bool,
}

impl Debug for HttpClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpClient")
            .field("client", &self.client)
            .field("authenticator", &self.authenticator.scheme_name())
            .field("extra_headers", &self.extra_headers)
            .finish_non_exhaustive()
    }
}

impl HttpClient {
    /// Create a new http client from configuration.
    ///
    /// This method automatically creates the appropriate authenticator based on
    /// the configuration properties.
    pub fn new(cfg: &RestCatalogConfig) -> Result<Self> {
        let extra_headers = cfg.extra_headers()?;
        let authenticator = Self::create_authenticator(cfg);

        Ok(HttpClient {
            client: cfg.client().unwrap_or_default(),
            authenticator,
            extra_headers,
            extra_oauth_params: cfg.extra_oauth_params(),
            disable_header_redaction: cfg.disable_header_redaction(),
        })
    }

    /// Create the appropriate authenticator based on configuration.
    fn create_authenticator(cfg: &RestCatalogConfig) -> Arc<dyn Authenticator> {
        // Check for SigV4 authentication
        if cfg.auth_type() == Some("sigv4")
            && let Some(auth) = cfg.create_sigv4_authenticator()
        {
            return Arc::new(auth);
        }

        // Default to OAuth2 authentication
        let token = cfg.token();
        let credential = cfg.credential();

        if token.is_none() && credential.is_none() {
            return Arc::new(NoAuth);
        }

        let token_endpoint = cfg.get_token_endpoint();
        let extra_params = cfg.extra_oauth_params();

        if let Some(token) = token {
            if let Some((client_id, client_secret)) = credential {
                // Both token and credentials provided
                Arc::new(
                    OAuth2Authenticator::new(Some(token), client_id, client_secret, token_endpoint)
                        .with_extra_params(extra_params),
                )
            } else {
                // Token only
                Arc::new(OAuth2Authenticator::with_token(token))
            }
        } else if let Some((client_id, client_secret)) = credential {
            // Credentials only
            Arc::new(
                OAuth2Authenticator::with_credentials(client_id, client_secret, token_endpoint)
                    .with_extra_params(extra_params),
            )
        } else {
            Arc::new(NoAuth)
        }
    }

    /// Update the http client with new configuration.
    ///
    /// If cfg carries new value, we will use cfg instead.
    /// Otherwise, we will keep the old value.
    ///
    /// Note: The existing authenticator is preserved to maintain cached tokens.
    /// The token was already obtained during the initial config fetch.
    pub fn update_with(self, cfg: &RestCatalogConfig) -> Result<Self> {
        let extra_headers = (!cfg.extra_headers()?.is_empty())
            .then(|| cfg.extra_headers())
            .transpose()?
            .unwrap_or(self.extra_headers);

        // Keep the existing authenticator - it has the cached token from the
        // initial config fetch. The merged config will have the same credentials.
        Ok(HttpClient {
            client: cfg.client().unwrap_or(self.client),
            authenticator: self.authenticator,
            extra_headers,
            extra_oauth_params: if !cfg.extra_oauth_params().is_empty() {
                cfg.extra_oauth_params()
            } else {
                self.extra_oauth_params
            },
            disable_header_redaction: cfg.disable_header_redaction(),
        })
    }

    /// This API is testing only to assert the token.
    ///
    /// Returns the current cached token from the authenticator.
    /// For OAuth2, this returns the cached bearer token.
    /// For non-token-based auth (SigV4, NoAuth), this returns None.
    ///
    /// If no token is cached, this will trigger authentication to obtain one.
    #[cfg(test)]
    pub(crate) async fn token(&self) -> Option<String> {
        // If no token is cached, trigger authentication to obtain one
        if self.authenticator.get_token().await.is_none() {
            // Create a dummy request to trigger authentication
            if let Ok(mut req) = self.request(Method::GET, "http://localhost/test").build() {
                // Ignore authentication errors, just try to populate the cache
                let _ = self.authenticator.authenticate(&mut req).await;
            }
        }
        self.authenticator.get_token().await
    }

    /// Invalidate the current token without generating a new one. On the next request, the client
    /// will attempt to generate a new token.
    pub(crate) async fn invalidate_token(&self) -> Result<()> {
        self.authenticator.invalidate().await
    }

    /// Invalidate the current token and set a new one. Generates a new token before invalidating
    /// the current token, meaning the old token will be used until this function acquires the lock
    /// and overwrites the token.
    ///
    /// If credential is invalid, or the request fails, this method will return an error and leave
    /// the current token unchanged.
    pub(crate) async fn regenerate_token(&self) -> Result<()> {
        self.authenticator.regenerate().await
    }

    #[inline]
    pub fn request<U: IntoUrl>(&self, method: Method, url: U) -> RequestBuilder {
        self.client
            .request(method, url)
            .headers(self.extra_headers.clone())
    }

    /// Executes the given `Request` and returns a `Response`.
    pub async fn execute(&self, mut request: Request) -> Result<Response> {
        request.headers_mut().extend(self.extra_headers.clone());
        Ok(self.client.execute(request).await?)
    }

    // Queries the Iceberg REST catalog after authentication with the given `Request` and
    // returns a `Response`.
    pub async fn query_catalog(&self, mut request: Request) -> Result<Response> {
        self.authenticator.authenticate(&mut request).await?;
        self.execute(request).await
    }

    /// Returns whether header redaction is disabled for this client.
    pub(crate) fn disable_header_redaction(&self) -> bool {
        self.disable_header_redaction
    }
}

/// Deserializes a catalog response into the given [`DeserializedOwned`] type.
///
/// Returns an error if unable to parse the response bytes.
pub(crate) async fn deserialize_catalog_response<R: DeserializeOwned>(
    response: Response,
) -> Result<R> {
    let bytes = response.bytes().await?;

    serde_json::from_slice::<R>(&bytes).map_err(|e| {
        iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "Failed to parse response from rest catalog server",
        )
        .with_context("json", String::from_utf8_lossy(&bytes))
        .with_source(e)
    })
}

/// Headers that contain sensitive information and should be excluded from logs.
const SENSITIVE_HEADERS: &[&str] = &[
    "authorization",
    "proxy-authorization",
    "set-cookie",
    "cookie",
    "x-api-key",
    "x-auth-token",
];

/// Returns true if the header name is considered sensitive.
fn is_sensitive_header(name: &str) -> bool {
    let name_lower = name.to_lowercase();
    SENSITIVE_HEADERS.iter().any(|h| name_lower == *h)
}

/// Redacts sensitive headers and returns a debug-formatted string.
///
/// If `disable_redaction` is true, returns all headers without redaction.
/// Otherwise, replaces sensitive header values with "[REDACTED]".
fn format_headers_redacted(headers: &HeaderMap, disable_redaction: bool) -> String {
    if disable_redaction {
        // Return all headers as-is without redaction
        let all: HashMap<&str, &str> = headers
            .iter()
            .filter_map(|(name, value)| value.to_str().ok().map(|v| (name.as_str(), v)))
            .collect();
        return format!("{all:?}");
    }

    // Redact sensitive headers by replacing their values with "[REDACTED]"
    let redacted: HashMap<&str, &str> = headers
        .iter()
        .filter_map(|(name, value)| {
            if is_sensitive_header(name.as_str()) {
                Some((name.as_str(), "[REDACTED]"))
            } else {
                value.to_str().ok().map(|v| (name.as_str(), v))
            }
        })
        .collect();
    format!("{redacted:?}")
}

/// Deserializes a unexpected catalog response into an error.
pub(crate) async fn deserialize_unexpected_catalog_error(
    response: Response,
    disable_header_redaction: bool,
) -> iceberg::Error {
    let err = iceberg::Error::new(
        iceberg::ErrorKind::Unexpected,
        "Received response with unexpected status code",
    )
    .with_context("status", response.status().to_string())
    .with_context(
        "headers",
        format_headers_redacted(response.headers(), disable_header_redaction),
    );

    let bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(err) => return err.into(),
    };

    if bytes.is_empty() {
        return err;
    }
    err.with_context("json", String::from_utf8_lossy(&bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_headers_redacted_empty() {
        let headers = HeaderMap::new();
        let result = format_headers_redacted(&headers, false);
        assert_eq!(result, "{}");
    }

    #[test]
    fn test_format_headers_redacted_non_sensitive() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());
        headers.insert("x-request-id", "abc123".parse().unwrap());

        let result = format_headers_redacted(&headers, false);

        assert!(result.contains("content-type"));
        assert!(result.contains("application/json"));
        assert!(result.contains("x-request-id"));
        assert!(result.contains("abc123"));
    }

    #[test]
    fn test_format_headers_redacted_filters_sensitive() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer secret-token".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());

        let result = format_headers_redacted(&headers, false);

        // Sensitive header should be present but with redacted value
        assert!(result.contains("authorization"));
        assert!(result.contains("[REDACTED]"));
        // Sensitive value should NOT be present
        assert!(!result.contains("secret-token"));
        // Non-sensitive header should be present with actual value
        assert!(result.contains("content-type"));
        assert!(result.contains("application/json"));
    }

    #[test]
    fn test_format_headers_redacted_filters_set_cookie() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "set-cookie",
            "CF_Authorization=sensitive-session-token; Path=/; Secure;"
                .parse()
                .unwrap(),
        );
        headers.insert("server", "cloudflare".parse().unwrap());

        let result = format_headers_redacted(&headers, false);

        // Sensitive header should be present but with redacted value
        assert!(result.contains("set-cookie"));
        assert!(result.contains("[REDACTED]"));
        // Sensitive value should NOT be present
        assert!(!result.contains("sensitive-session-token"));
        // Non-sensitive header should be present with actual value
        assert!(result.contains("server"));
        assert!(result.contains("cloudflare"));
    }

    #[test]
    fn test_format_headers_redacted_filters_all_sensitive() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer token".parse().unwrap());
        headers.insert("proxy-authorization", "Basic creds".parse().unwrap());
        headers.insert("set-cookie", "session=abc".parse().unwrap());
        headers.insert("cookie", "session=abc".parse().unwrap());
        headers.insert("x-api-key", "api-key-123".parse().unwrap());
        headers.insert("x-auth-token", "auth-token-456".parse().unwrap());
        headers.insert("x-request-id", "req-123".parse().unwrap());

        let result = format_headers_redacted(&headers, false);

        // All sensitive headers should be present but with redacted values
        assert!(result.contains("authorization"));
        assert!(result.contains("proxy-authorization"));
        assert!(result.contains("set-cookie"));
        assert!(result.contains("cookie"));
        assert!(result.contains("x-api-key"));
        assert!(result.contains("x-auth-token"));
        assert!(result.contains("[REDACTED]"));

        // Ensure no sensitive values leaked
        assert!(!result.contains("Bearer token"));
        assert!(!result.contains("Basic creds"));
        assert!(!result.contains("session=abc"));
        assert!(!result.contains("api-key-123"));
        assert!(!result.contains("auth-token-456"));

        // Non-sensitive header should be present with actual value
        assert!(result.contains("x-request-id"));
        assert!(result.contains("req-123"));
    }

    #[test]
    fn test_format_headers_with_redaction_disabled() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer secret-token".parse().unwrap());
        headers.insert("x-api-key", "api-key-123".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());

        let result = format_headers_redacted(&headers, true);

        // When redaction is disabled, all headers and values should be present
        assert!(result.contains("authorization"));
        assert!(result.contains("Bearer secret-token"));
        assert!(result.contains("x-api-key"));
        assert!(result.contains("api-key-123"));
        assert!(result.contains("content-type"));
        assert!(result.contains("application/json"));
        // [REDACTED] should NOT be present when redaction is disabled
        assert!(!result.contains("[REDACTED]"));
    }
}
