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

use http::StatusCode;
use iceberg::{Error, ErrorKind, Result};
use reqwest::header::HeaderMap;
use reqwest::{Client, IntoUrl, Method, Request, RequestBuilder, Response};
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;

use crate::RestCatalogConfig;
use crate::error_handlers::{ErrorHandler, OAuthErrorHandler};
use crate::types::{ErrorModel, ErrorResponse, OAuthError, TokenResponse};

pub(crate) struct HttpClient {
    client: Client,

    /// The token to be used for authentication.
    ///
    /// It's possible to fetch the token from the server while needed.
    token: Mutex<Option<String>>,
    /// The token endpoint to be used for authentication.
    token_endpoint: String,
    /// The credential to be used for authentication.
    credential: Option<(Option<String>, String)>,
    /// Extra headers to be added to each request.
    extra_headers: HeaderMap,
    /// Extra oauth parameters to be added to each authentication request.
    extra_oauth_params: HashMap<String, String>,
}

impl Debug for HttpClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpClient")
            .field("client", &self.client)
            .field("extra_headers", &self.extra_headers)
            .finish_non_exhaustive()
    }
}

impl HttpClient {
    /// Create a new http client.
    pub fn new(cfg: &RestCatalogConfig) -> Result<Self> {
        let extra_headers = cfg.extra_headers()?;
        Ok(HttpClient {
            client: cfg.client().unwrap_or_default(),
            token: Mutex::new(cfg.token()),
            token_endpoint: cfg.get_token_endpoint(),
            credential: cfg.credential(),
            extra_headers,
            extra_oauth_params: cfg.extra_oauth_params(),
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
            token: Mutex::new(cfg.token().or_else(|| self.token.into_inner())),
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
                .with_context("json", String::from_utf8_lossy(&text))
                .with_source(e)
            })?)
        } else {
            let status = auth_resp.status();
            let text = auth_resp
                .bytes()
                .await
                .map_err(|err| err.with_url(auth_url.clone()))?;
            let error_response = if text.is_empty() {
                // use default error when response body is empty
                ErrorResponse::build_default_response(status)
            } else {
                match serde_json::from_slice::<OAuthError>(&text) {
                    Ok(oauth_error) => {
                        // Convert OAuth error format to ErrorResponse format
                        // OAuth "error" field becomes ErrorResponse "type"
                        // OAuth "error_description" becomes ErrorResponse "message"
                        ErrorResponse {
                            error: ErrorModel {
                                message: oauth_error
                                    .error_description
                                    .clone()
                                    .unwrap_or_else(|| oauth_error.error.clone()),
                                r#type: oauth_error.error, // OAuth error type
                                code: status.as_u16(),
                                stack: None,
                            },
                        }
                    }
                    Err(_parse_err) => {
                        // use default error when parsing failed
                        ErrorResponse::build_default_response(status)
                    }
                }
            };
            Err(OAuthErrorHandler.handle(status, &error_response))
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
}

/// Deserializes a catalog response into the given [`DeserializedOwned`] type.
///
/// Returns an error if unable to parse the response bytes.
pub(crate) async fn deserialize_catalog_response<R: DeserializeOwned>(
    response: Response,
) -> Result<R> {
    let bytes = response.bytes().await?;

    serde_json::from_slice::<R>(&bytes).map_err(|e| {
        Error::new(
            ErrorKind::Unexpected,
            "Failed to parse response from rest catalog server",
        )
        .with_context("json", String::from_utf8_lossy(&bytes))
        .with_source(e)
    })
}

/// Handle an error response using the provided error handler.
///
/// Returns an `iceberg::Error` with appropriate error kind and context.
pub(crate) async fn handle_error_response(response: Response, handler: &dyn ErrorHandler) -> Error {
    let status = response.status();

    let bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(err) => {
            return Error::new(
                ErrorKind::Unexpected,
                format!(
                    "Failed to read error response body for HTTP {}",
                    status.as_u16()
                ),
            )
            .with_context("status", status.to_string())
            .with_source(err);
        }
    };

    let error_response = if bytes.is_empty() {
        // use default error when response body is empty
        ErrorResponse::build_default_response(status)
    } else {
        match serde_json::from_slice::<ErrorResponse>(&bytes) {
            Ok(response) => response,
            Err(_parse_err) => {
                // use default error when parsing failed
                ErrorResponse::build_default_response(status)
            }
        }
    };

    handler.handle(status, &error_response)
}
