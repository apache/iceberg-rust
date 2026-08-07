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

use http::StatusCode;
use iceberg::{Error, ErrorKind, Result};
use reqwest::header::HeaderMap;
use reqwest::{Client, IntoUrl, Method, Request, RequestBuilder, Response};
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;

use crate::RestCatalogConfig;
use crate::types::{ErrorResponse, TokenResponse};

pub(crate) struct HttpClient {
    client: Client,

    /// The token to be used for authentication.
    ///
    /// It's possible to fetch the token from the server while needed.
    token: Arc<Mutex<Option<String>>>,
    /// The token endpoint to be used for authentication.
    token_endpoint: String,
    /// The credential to be used for authentication.
    credential: Option<(Option<String>, String)>,
    /// Extra headers to be added to each request.
    extra_headers: HeaderMap,
    /// Extra oauth parameters to be added to each authentication request.
    extra_oauth_params: HashMap<String, String>,
    /// Whether to disable header redaction in error logs (defaults to false for security).
    disable_header_redaction: bool,
}

impl Debug for HttpClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // Omit the reqwest client: injected clients may carry secret default
        // headers. Explicit headers use the same redaction policy as errors.
        f.debug_struct("HttpClient")
            .field(
                "extra_headers",
                &format_headers_redacted(&self.extra_headers, self.disable_header_redaction),
            )
            .finish_non_exhaustive()
    }
}

impl HttpClient {
    /// Create a new http client.
    pub fn new(cfg: &RestCatalogConfig) -> Result<Self> {
        let extra_headers = cfg.extra_headers()?;
        Ok(HttpClient {
            client: cfg.client().unwrap_or_default(),
            token: Arc::new(Mutex::new(cfg.token())),
            token_endpoint: cfg.get_token_endpoint(),
            credential: cfg.credential(),
            extra_headers,
            extra_oauth_params: cfg.extra_oauth_params(),
            disable_header_redaction: cfg.disable_header_redaction(),
        })
    }

    /// Create a client for table-scoped resources while reusing this client's
    /// underlying connection pool.
    ///
    /// A load-table response may supply a table token or `header.*` values that
    /// must be used for subsequent table requests such as credential refresh.
    pub(crate) fn for_table(
        &self,
        catalog_uri: &str,
        props: &HashMap<String, String>,
    ) -> Result<Self> {
        let cfg = RestCatalogConfig::builder()
            .uri(catalog_uri.to_string())
            .props(props.clone())
            .client(Some(self.client.clone()))
            .build();
        let table_token = cfg.token();
        let configured_credential = cfg.credential();
        let has_table_token = props.contains_key("token");
        let has_table_credential = props.contains_key("credential");
        let has_table_auth_header = props.keys().any(|key| {
            key.strip_prefix("header.")
                .is_some_and(|name| name.eq_ignore_ascii_case("authorization"))
        });
        let has_oauth_params = ["scope", "audience", "resource"]
            .iter()
            .any(|key| props.contains_key(*key));
        let has_oauth_config = props.contains_key("oauth2-server-uri") || has_oauth_params;
        let has_oauth_override =
            has_table_credential || (has_oauth_config && self.credential.is_some());
        let has_table_auth = has_table_token || has_table_auth_header || has_oauth_override;
        let mut extra_headers = self.extra_headers.clone();
        if has_table_auth && !has_table_auth_header {
            // Authentication is applied after request headers are initially built, but
            // `execute` reapplies `extra_headers`. Do not let an inherited catalog
            // Authorization header overwrite table-scoped authentication at that point.
            extra_headers.remove(http::header::AUTHORIZATION);
        }
        extra_headers.extend(cfg.extra_headers()?);

        Ok(Self {
            client: self.client.clone(),
            token: if has_table_auth {
                Arc::new(Mutex::new(table_token))
            } else {
                Arc::clone(&self.token)
            },
            token_endpoint: if has_oauth_override && props.contains_key("oauth2-server-uri") {
                cfg.get_token_endpoint()
            } else {
                self.token_endpoint.clone()
            },
            credential: if has_table_token || has_table_auth_header {
                None
            } else if has_table_credential {
                configured_credential
            } else {
                self.credential.clone()
            },
            extra_headers,
            extra_oauth_params: if has_oauth_override && has_oauth_params {
                cfg.extra_oauth_params()
            } else {
                self.extra_oauth_params.clone()
            },
            disable_header_redaction: if props
                .contains_key(crate::REST_CATALOG_PROP_DISABLE_HEADER_REDACTION)
            {
                cfg.disable_header_redaction()
            } else {
                self.disable_header_redaction
            },
        })
    }

    /// Update the http client with new configuration.
    ///
    /// If cfg carries new value, we will use cfg instead.
    /// Otherwise, we will keep the old value.
    pub fn update_with(self, cfg: &RestCatalogConfig) -> Result<Self> {
        let extra_headers = (!cfg.extra_headers()?.is_empty())
            .then(|| cfg.extra_headers())
            .transpose()?
            .unwrap_or(self.extra_headers);
        Ok(HttpClient {
            client: cfg.client().unwrap_or(self.client),
            token: match cfg.token() {
                Some(token) => Arc::new(Mutex::new(Some(token))),
                None => self.token,
            },
            token_endpoint: if !cfg.get_token_endpoint().is_empty() {
                cfg.get_token_endpoint()
            } else {
                self.token_endpoint
            },
            credential: cfg.credential().or(self.credential),
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
    #[cfg(test)]
    pub(crate) async fn token(&self) -> Option<String> {
        let mut req = self
            .request(Method::GET, &self.token_endpoint)
            .build()
            .unwrap();
        self.authenticate(&mut req).await.ok();
        self.token.lock().await.clone()
    }

    async fn exchange_credential_for_token(&self) -> Result<String> {
        // Credential must exist here.
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
            self.extra_oauth_params
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str())),
        );

        let mut auth_req = self
            .request(Method::POST, &self.token_endpoint)
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
                    .with_source(e)
            })?;
            Err(Error::from(e))
        }?;
        Ok(auth_res.access_token)
    }

    /// Invalidate the current token without generating a new one. On the next request, the client
    /// will attempt to generate a new token.
    pub(crate) async fn invalidate_token(&self) -> Result<()> {
        *self.token.lock().await = None;
        Ok(())
    }

    /// Invalidate the current token and set a new one. Generates a new token before invalidating
    /// the current token, meaning the old token will be used until this function acquires the lock
    /// and overwrites the token.
    ///
    /// If credential is invalid, or the request fails, this method will return an error and leave
    /// the current token unchanged.
    pub(crate) async fn regenerate_token(&self) -> Result<()> {
        let new_token = self.exchange_credential_for_token().await?;
        *self.token.lock().await = Some(new_token.clone());
        Ok(())
    }

    /// Authenticates the request by adding a bearer token to the authorization header.
    ///
    /// This method supports three authentication modes:
    ///
    /// 1. **No authentication** - Skip authentication when both `credential` and `token` are missing.
    /// 2. **Token authentication** - Use the provided `token` directly for authentication.
    /// 3. **OAuth authentication** - Exchange `credential` for a token, cache it, then use it for authentication.
    ///
    /// When both `credential` and `token` are present, `token` takes precedence.
    ///
    /// # TODO: Support automatic token refreshing.
    async fn authenticate(&self, req: &mut Request) -> Result<()> {
        // Clone the token from lock without holding the lock for entire function.
        let token = self.token.lock().await.clone();

        if self.credential.is_none() && token.is_none() {
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
        self.authenticate(&mut request).await?;
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
        // Successful REST responses can contain OAuth tokens and delegated
        // storage credentials. Never copy an unparsable response into an error.
        Error::new(
            ErrorKind::Unexpected,
            "Failed to parse response from rest catalog server",
        )
        .with_source(e)
    })
}

/// Redacts header values and returns a debug-formatted string.
///
/// If `disable_redaction` is true, returns all headers without redaction.
/// Otherwise, redacts every header value.
fn format_headers_redacted(headers: &HeaderMap, disable_redaction: bool) -> String {
    if disable_redaction {
        // Return all headers as-is without redaction
        let all: HashMap<&str, &str> = headers
            .iter()
            .filter_map(|(name, value)| value.to_str().ok().map(|v| (name.as_str(), v)))
            .collect();
        return format!("{all:?}");
    }

    // Retain names for diagnostics but redact every value
    let redacted: HashMap<&str, &str> = headers
        .iter()
        .map(|(name, _)| (name.as_str(), "[REDACTED]"))
        .collect();
    format!("{redacted:?}")
}

/// Deserializes a unexpected catalog response into an error.
pub(crate) async fn deserialize_unexpected_catalog_error(
    response: Response,
    disable_header_redaction: bool,
) -> Error {
    let err = Error::new(
        ErrorKind::Unexpected,
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
    fn test_format_headers_redacts_all_values() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer secret-token".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        headers.insert("x-request-id", "abc123".parse().unwrap());

        let result = format_headers_redacted(&headers, false);

        assert!(result.contains("authorization"));
        assert!(result.contains("content-type"));
        assert!(result.contains("x-request-id"));
        assert!(result.contains("[REDACTED]"));
        assert!(!result.contains("secret-token"));
        assert!(!result.contains("application/json"));
        assert!(!result.contains("abc123"));
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

    #[tokio::test]
    async fn table_client_reuses_parent_token_unless_overridden() {
        let inherited_props = HashMap::from([(
            "credential".to_string(),
            "client-id:client-secret".to_string(),
        )]);
        let config = RestCatalogConfig::builder()
            .uri("https://catalog.example".to_string())
            .props(inherited_props.clone())
            .build();
        let client = HttpClient::new(&config).unwrap();
        *client.token.lock().await = Some("catalog-token".to_string());

        let inherited = client
            .for_table("https://catalog.example", &HashMap::new())
            .unwrap();
        assert!(Arc::ptr_eq(&client.token, &inherited.token));
        assert_eq!(
            inherited.token.lock().await.as_deref(),
            Some("catalog-token")
        );

        let overridden = client
            .for_table(
                "https://catalog.example",
                &HashMap::from([("token".to_string(), "table-token".to_string())]),
            )
            .unwrap();
        assert!(!Arc::ptr_eq(&client.token, &overridden.token));
        assert_eq!(
            overridden.token.lock().await.as_deref(),
            Some("table-token")
        );
    }

    #[tokio::test]
    async fn table_auth_removes_inherited_authorization_header() {
        let config = RestCatalogConfig::builder()
            .uri("https://catalog.example".to_string())
            .props(HashMap::from([
                ("token".to_string(), "catalog-token".to_string()),
                (
                    "header.Authorization".to_string(),
                    "Bearer catalog-header-token".to_string(),
                ),
            ]))
            .build();
        let client = HttpClient::new(&config).unwrap();

        let table_client = client
            .for_table(
                "https://catalog.example",
                &HashMap::from([("token".to_string(), "table-token".to_string())]),
            )
            .unwrap();

        assert!(
            !table_client
                .extra_headers
                .contains_key(http::header::AUTHORIZATION)
        );
        assert_eq!(
            table_client.token.lock().await.as_deref(),
            Some("table-token")
        );
    }

    #[tokio::test]
    async fn table_oauth_options_without_credential_reuse_parent_token() {
        let config = RestCatalogConfig::builder()
            .uri("https://catalog.example".to_string())
            .props(HashMap::from([(
                "token".to_string(),
                "catalog-token".to_string(),
            )]))
            .build();
        let client = HttpClient::new(&config).unwrap();
        let table_props = HashMap::from([
            ("scope".to_string(), "table-scope".to_string()),
            (
                "oauth2-server-uri".to_string(),
                "https://table-auth.example/token".to_string(),
            ),
        ]);

        let table_client = client
            .for_table("https://catalog.example", &table_props)
            .unwrap();

        assert!(Arc::ptr_eq(&client.token, &table_client.token));
        assert_eq!(
            table_client.token.lock().await.as_deref(),
            Some("catalog-token")
        );
        assert_eq!(table_client.token_endpoint, client.token_endpoint);
        assert_eq!(table_client.extra_oauth_params, client.extra_oauth_params);
    }
}
