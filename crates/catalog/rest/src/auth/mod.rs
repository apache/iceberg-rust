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
    async fn init_session(&self) -> Result<Arc<dyn AuthSession>>;

    /// Session used for all subsequent catalog requests, given the properties
    /// merged from the user configuration and the server's config response.
    ///
    /// Implementations may carry state (e.g. a cached token) over from the
    /// init session.
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
    pub(crate) fn new(inner: &'a mut Request) -> Self {
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

    /// The in-memory request body, or `None` for an empty or streaming body.
    pub fn body(&self) -> Option<&[u8]> {
        self.inner.body().and_then(|body| body.as_bytes())
    }
}

/// Authenticates outgoing REST catalog requests.
#[async_trait]
pub trait AuthSession: Debug + Send + Sync {
    /// Applies authentication to the request (adds headers, signs, ...).
    async fn authenticate(&self, request: &mut AuthRequest<'_>) -> Result<()>;

    /// Drops any cached credentials so the next request re-authenticates.
    async fn invalidate(&self) -> Result<()> {
        Ok(())
    }

    /// Proactively refreshes cached credentials (e.g. re-exchanges an OAuth2
    /// client credential for a new token), leaving them intact on failure.
    async fn refresh(&self) -> Result<()> {
        Ok(())
    }

    /// The bearer token this session would attach, if any. Test-only: lets
    /// tests observe the cached token without issuing a request.
    #[cfg(test)]
    async fn bearer_token(&self) -> Option<String> {
        None
    }
}

/// [`AuthManager`] that performs no authentication.
#[derive(Debug)]
pub struct NoopAuthManager;

/// [`AuthSession`] that performs no authentication.
#[derive(Debug)]
struct NoopSession;

#[async_trait]
impl AuthManager for NoopAuthManager {
    async fn init_session(&self) -> Result<Arc<dyn AuthSession>> {
        Ok(Arc::new(NoopSession))
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
