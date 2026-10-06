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
use iceberg::{Error, ErrorKind, Result, TableIdent};
pub use oauth2::OAuth2Manager;
pub(crate) use oauth2::static_token_session;

use crate::catalog::{REST_CATALOG_PROP_AUTH_TYPE, RestCatalogConfig};
use crate::client::HttpClient;
use crate::request::HttpRequest;

/// `rest.auth.type` value disabling authentication.
pub const AUTH_TYPE_NONE: &str = "none";
/// `rest.auth.type` value selecting OAuth2 token authentication.
pub const AUTH_TYPE_OAUTH2: &str = "oauth2";

/// Builds the auth manager selected by the `rest.auth.type` configuration,
/// like Java's `AuthManagers.loadAuthManager`.
pub(crate) fn load_auth_manager(config: &RestCatalogConfig) -> Result<Arc<dyn AuthManager>> {
    let auth_type = config.auth_type();
    // Java parity (`AuthManagers`): make the inference visible so users
    // configure the type explicitly.
    if auth_type == AUTH_TYPE_OAUTH2 && !config.has_explicit_auth_type() {
        tracing::warn!(
            "Inferring {REST_CATALOG_PROP_AUTH_TYPE}={AUTH_TYPE_OAUTH2} from the configured \
             OAuth properties; set it explicitly to avoid this warning"
        );
    }
    match auth_type.as_str() {
        AUTH_TYPE_NONE => Ok(Arc::new(NoopAuthManager)),
        AUTH_TYPE_OAUTH2 => Ok(Arc::new(OAuth2Manager::from_config(config)?)),
        other => Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "unknown '{REST_CATALOG_PROP_AUTH_TYPE}': {other}; use \
                 `RestSessionCatalogBuilder::with_auth_manager` or \
                 `RestCatalogBuilder::with_auth_manager` to inject a custom auth manager"
            ),
        )),
    }
}

/// Creates the [`AuthSession`]s used to authenticate REST catalog requests.
///
/// A manager is exclusively scoped to one catalog and must not be reused by
/// other catalogs. It is either created from the `rest.auth.type` property or
/// injected through
/// [`RestCatalogBuilder::with_auth_manager`](crate::RestCatalogBuilder::with_auth_manager) or
/// [`RestSessionCatalogBuilder::with_auth_manager`](crate::RestSessionCatalogBuilder::with_auth_manager).
/// Catalog initialization calls [`AuthManager::catalog_session`] exactly once;
/// later sessions may rely on the state established by that call.
///
/// Session-construction methods are handed the catalog's [`HttpClient`], which
/// an implementation may reuse for its own requests (e.g. a token exchange)
/// so that they share the catalog's connection pool and configuration.
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

    /// Returns a session for requests associated with `table`.
    ///
    /// Currently only requests for the table's vended storage credentials use
    /// it; other table operations use the catalog session.
    ///
    /// `props` are the unmerged properties returned by the table endpoint.
    /// The default preserves the catalog session; managers should return a
    /// child session only when the table properties contain an authentication
    /// override.
    async fn table_session(
        &self,
        _client: &HttpClient,
        _table: &TableIdent,
        _props: &HashMap<String, String>,
        parent: Arc<dyn AuthSession>,
    ) -> Result<Arc<dyn AuthSession>> {
        Ok(parent)
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
