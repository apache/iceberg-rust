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

//! Pluggable authentication for the REST catalog, mirroring Iceberg Java's
//! `AuthManager`/`AuthSession` API.

mod oauth2;

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use http::{HeaderMap, Method};
use iceberg::Result;
pub use oauth2::OAuth2Manager;
use reqwest::Request;

/// `rest.auth.type` value disabling authentication.
pub const AUTH_TYPE_NONE: &str = "none";
/// `rest.auth.type` value selecting OAuth2 token authentication.
pub const AUTH_TYPE_OAUTH2: &str = "oauth2";

/// Creates the [`AuthSession`]s used to authenticate REST catalog requests.
///
/// A manager is created once per catalog, either from the `rest.auth.type`
/// property or injected through `RestCatalogBuilder::with_auth_manager`, and
/// lives for the lifetime of the catalog.
#[async_trait]
pub trait AuthManager: Debug + Send + Sync {
    /// Session used for the initial `/v1/config` handshake, built from the
    /// user-supplied configuration.
    ///
    /// Returns a [`Box`]: an init session is used once and released, unlike
    /// the shared [`AuthManager::catalog_session`].
    async fn init_session(&self) -> Result<Box<dyn AuthSession>>;

    /// Session used for all subsequent catalog requests, given the properties
    /// merged from the user configuration and the server's config response.
    ///
    /// Returns an [`Arc`]: this session is shared by concurrent requests for
    /// the rest of the catalog's lifetime. Implementations may carry state
    /// (e.g. a cached token) over from the init session.
    async fn catalog_session(
        &self,
        props: &HashMap<String, String>,
    ) -> Result<Arc<dyn AuthSession>>;
}

/// An outgoing REST request being authenticated by an [`AuthSession`].
///
/// Wraps the request so authentication implementations depend only on the
/// stable `http` crate and standard types, not on the concrete HTTP client the
/// REST catalog uses internally.
pub struct AuthRequest<'a> {
    inner: &'a mut Request,
}

impl<'a> AuthRequest<'a> {
    /// Wraps a request, e.g. to unit-test a custom [`AuthSession`].
    pub fn new(inner: &'a mut Request) -> Self {
        Self { inner }
    }

    /// The request method.
    pub fn method(&self) -> &Method {
        self.inner.method()
    }

    /// The request URL, as a string (scheme, host, path and query).
    pub fn url_str(&self) -> &str {
        self.inner.url().as_str()
    }

    /// The request headers.
    pub fn headers(&self) -> &HeaderMap {
        self.inner.headers()
    }

    /// The mutable request headers, e.g. to add an `Authorization` header.
    pub fn headers_mut(&mut self) -> &mut HeaderMap {
        self.inner.headers_mut()
    }

    /// The request body, distinguishing an absent body from a streaming one:
    /// signers can sign [`AuthRequestBody::Empty`] (empty-payload hash) and
    /// [`AuthRequestBody::Buffered`], but not [`AuthRequestBody::Streaming`].
    pub fn body(&self) -> AuthRequestBody<'_> {
        match self.inner.body() {
            None => AuthRequestBody::Empty,
            Some(body) => match body.as_bytes() {
                Some(bytes) => AuthRequestBody::Buffered(bytes),
                None => AuthRequestBody::Streaming,
            },
        }
    }
}

/// The body of an [`AuthRequest`], as seen by authentication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthRequestBody<'a> {
    /// No body is set.
    Empty,
    /// An in-memory body.
    Buffered(&'a [u8]),
    /// A streaming body, whose bytes are not available for e.g. signing.
    Streaming,
}

impl<'a> AuthRequestBody<'a> {
    /// The signable bytes: empty for [`Self::Empty`], the buffer for
    /// [`Self::Buffered`], and `None` for [`Self::Streaming`].
    pub fn as_bytes(&self) -> Option<&'a [u8]> {
        match self {
            AuthRequestBody::Empty => Some(&[]),
            AuthRequestBody::Buffered(bytes) => Some(bytes),
            AuthRequestBody::Streaming => None,
        }
    }
}

/// Authenticates outgoing REST catalog requests.
#[async_trait]
pub trait AuthSession: Debug + Send + Sync {
    /// Applies authentication to the request (adds headers, signs, ...).
    async fn authenticate(&self, request: &mut AuthRequest<'_>) -> Result<()>;
}

/// [`AuthManager`] that performs no authentication.
#[derive(Debug)]
pub struct NoopAuthManager;

/// [`AuthSession`] that performs no authentication.
#[derive(Debug)]
struct NoopSession;

#[async_trait]
impl AuthManager for NoopAuthManager {
    async fn init_session(&self) -> Result<Box<dyn AuthSession>> {
        Ok(Box::new(NoopSession))
    }

    async fn catalog_session(
        &self,
        _props: &HashMap<String, String>,
    ) -> Result<Arc<dyn AuthSession>> {
        Ok(Arc::new(NoopSession))
    }
}

#[async_trait]
impl AuthSession for NoopSession {
    async fn authenticate(&self, _request: &mut AuthRequest<'_>) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use reqwest::Client;

    use super::*;

    #[tokio::test]
    async fn test_static_token_session_attaches_token() {
        // Token-only config: the token is attached as-is.
        let manager = OAuth2Manager::new("http://localhost/unused").with_token("tok-static");
        let session = manager.init_session().await.unwrap();

        let mut req = Client::new()
            .get("https://rest.example.com/v1/config")
            .build()
            .unwrap();
        session
            .authenticate(&mut AuthRequest::new(&mut req))
            .await
            .unwrap();
        assert_eq!(
            req.headers().get("authorization").unwrap(),
            "Bearer tok-static"
        );
    }

    #[test]
    fn test_auth_request_body_states() {
        let client = Client::new();

        // No body at all.
        let mut req = client
            .get("https://rest.example.com/v1/config")
            .build()
            .unwrap();
        let auth_req = AuthRequest::new(&mut req);
        let body = auth_req.body();
        assert_eq!(body, AuthRequestBody::Empty);
        assert_eq!(body.as_bytes(), Some(&[] as &[u8]));

        // An in-memory body.
        let mut req = client
            .post("https://rest.example.com/v1/namespaces")
            .body("{}")
            .build()
            .unwrap();
        let auth_req = AuthRequest::new(&mut req);
        let body = auth_req.body();
        assert_eq!(body, AuthRequestBody::Buffered(b"{}"));
        assert_eq!(body.as_bytes(), Some(b"{}" as &[u8]));

        // A streaming body: bytes are unavailable, so it must not sign as empty.
        let mut req = client
            .post("https://rest.example.com/v1/namespaces")
            .body(reqwest::Body::wrap_stream(futures::stream::once(async {
                Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"chunk"))
            })))
            .build()
            .unwrap();
        let auth_req = AuthRequest::new(&mut req);
        let body = auth_req.body();
        assert_eq!(body, AuthRequestBody::Streaming);
        assert_eq!(body.as_bytes(), None);
    }
}
