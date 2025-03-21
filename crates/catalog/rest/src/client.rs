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
use std::sync::Mutex;

use http::StatusCode;
use iceberg::{Error, ErrorKind, Result};
use reqwest::header::HeaderMap;
use reqwest::{Client, IntoUrl, Method, Request, RequestBuilder, Response};
use serde::de::DeserializeOwned;

use crate::types::{ErrorResponse, TokenResponse};
use crate::RestCatalogConfig;

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
            client: Client::builder()
                .default_headers(extra_headers.clone())
                .build()?,
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
            client: Client::builder()
                .default_headers(extra_headers.clone())
                .build()?,
            token: Mutex::new(
                cfg.token()
                    .or_else(|| self.token.into_inner().ok().flatten()),
            ),
            token_endpoint: if !cfg.get_token_endpoint().is_empty() {
                cfg.get_token_endpoint()
            } else {
                self.token_endpoint
            },
            credential: cfg.credential().or(self.credential),
            extra_headers,
            extra_oauth_params: (!cfg.extra_oauth_params().is_empty())
                .then(|| cfg.extra_oauth_params())
                .unwrap_or(self.extra_oauth_params),
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
        self.token.lock().unwrap().clone()
    }

    /// Authenticate the request by filling token.
    ///
    /// - If neither token nor credential is provided, this method will do nothing.
    /// - If only credential is provided, this method will try to fetch token from the server.
    /// - If token is provided, this method will use the token directly.
    ///
    /// # TODO
    ///
    /// Support refreshing token while needed.
    async fn authenticate(&self, req: &mut Request) -> Result<()> {
        // Clone the token from lock without holding the lock for entire function.
        let token = { self.token.lock().expect("lock poison").clone() };

        if self.credential.is_none() && token.is_none() {
            return Ok(());
        }

        // Use token if provided.
        if let Some(token) = &token {
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
            return Ok(());
        }

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

        let auth_req = self
            .client
            .request(Method::POST, &self.token_endpoint)
            .form(&params)
            .build()?;
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
        let token = auth_res.access_token;
        // Update token.
        *self.token.lock().expect("lock poison") = Some(token.clone());
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
        self.client.request(method, url)
    }

    // Queries the Iceberg REST catalog after authentication with the given `Request` and
    // returns a `Response`.
    pub async fn query_catalog(&self, mut request: Request) -> Result<Response> {
        self.authenticate(&mut request).await?;
        Ok(self.client.execute(request).await?)
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

/// Deserializes a unexpected catalog response into an error.
///
/// TODO: Eventually, this function should return an error response that is custom to the error
/// codes that all endpoints share (400, 404, etc.).
pub(crate) async fn deserialize_unexpected_catalog_error(response: Response) -> Error {
    let bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(err) => return err.into(),
    };

    Error::new(ErrorKind::Unexpected, "Received unexpected response")
        .with_context("json", String::from_utf8_lossy(&bytes))
}
