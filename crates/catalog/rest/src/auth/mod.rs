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
use iceberg::{Result, SessionContext};
pub use oauth2::OAuth2Manager;

use crate::client::HttpClient;
use crate::request::HttpRequest;

/// `rest.auth.type` value disabling authentication.
pub const AUTH_TYPE_NONE: &str = "none";
/// `rest.auth.type` value selecting OAuth2 token authentication.
pub const AUTH_TYPE_OAUTH2: &str = "oauth2";

/// Creates the [`AuthSession`]s used to authenticate REST catalog requests.
///
/// A manager is exclusively scoped to one catalog and must not be reused by
/// other catalogs. It is either created from the `rest.auth.type` property or
/// injected through
/// [`RestCatalogBuilder::with_auth_manager`](crate::RestCatalogBuilder::with_auth_manager) or
/// [`RestSessionCatalogBuilder::with_auth_manager`](crate::RestSessionCatalogBuilder::with_auth_manager).
/// Catalog initialization calls [`AuthManager::catalog_session`] exactly once;
/// the context-specific sessions that [`AuthManager::contextual_session`]
/// derives from it may rely on the state established by that call.
///
/// [`Self::init_session`] and [`Self::catalog_session`] are handed the
/// catalog's [`HttpClient`], which an implementation may reuse for its own
/// requests (e.g. a token exchange) so that they share the catalog's
/// connection pool and configuration.
#[async_trait]
pub trait AuthManager: Debug + Send + Sync {
    /// Session used for the initial `/v1/config` handshake, given the
    /// user-supplied properties.
    ///
    /// Returns a [`Box`]: an init session is used once and released, unlike
    /// the shared [`AuthManager::catalog_session`].
    async fn init_session(
        &self,
        client: &HttpClient,
        props: &HashMap<String, String>,
    ) -> Result<Box<dyn AuthSession>>;

    /// Session used for all subsequent catalog requests, given the properties
    /// merged from the user configuration and the server's config response.
    ///
    /// Returns an [`Arc`]: this session is shared by concurrent requests for
    /// the rest of the catalog's lifetime. Implementations may carry state
    /// (e.g. a cached token) over from the init session.
    async fn catalog_session(
        &self,
        client: &HttpClient,
        props: &HashMap<String, String>,
    ) -> Result<Arc<dyn AuthSession>>;

    /// Returns the authentication session for a specific context.
    ///
    /// The catalog calls this method only after [`Self::catalog_session`] has
    /// succeeded. `catalog_session` is the catalog session returned by this
    /// manager. If the context does not require different authentication,
    /// implementations should return `catalog_session` unchanged.
    ///
    /// The catalog does not cache the returned session. Implementations should
    /// cache context-specific sessions internally using
    /// [`SessionContext::session_id`] and are responsible for eviction and
    /// releasing any associated resources. Reusing a session ID with different
    /// context may therefore return the previously cached session.
    async fn contextual_session(
        &self,
        _context: &SessionContext,
        catalog_session: Arc<dyn AuthSession>,
    ) -> Result<Arc<dyn AuthSession>> {
        Ok(catalog_session)
    }
}

/// Authenticates outgoing REST catalog requests.
#[async_trait]
pub trait AuthSession: Debug + Send + Sync {
    /// Applies authentication to the request (adds headers, signs, ...).
    async fn authenticate(&self, request: &mut HttpRequest) -> Result<()>;
}

/// [`AuthManager`] that performs no authentication.
#[derive(Debug)]
pub struct NoopAuthManager;

/// [`AuthSession`] that performs no authentication.
#[derive(Debug)]
pub(crate) struct NoopSession;

#[async_trait]
impl AuthManager for NoopAuthManager {
    async fn init_session(
        &self,
        _client: &HttpClient,
        _props: &HashMap<String, String>,
    ) -> Result<Box<dyn AuthSession>> {
        Ok(Box::new(NoopSession))
    }

    async fn catalog_session(
        &self,
        _client: &HttpClient,
        _props: &HashMap<String, String>,
    ) -> Result<Arc<dyn AuthSession>> {
        Ok(Arc::new(NoopSession))
    }
}

#[async_trait]
impl AuthSession for NoopSession {
    async fn authenticate(&self, _request: &mut HttpRequest) -> Result<()> {
        Ok(())
    }
}
