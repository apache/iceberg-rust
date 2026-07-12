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
mod sigv4;

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::Result;
pub use oauth2::OAuth2Manager;
use reqwest::Request;
pub use sigv4::SigV4AuthManager;

/// `rest.auth.type` value disabling authentication.
pub const AUTH_TYPE_NONE: &str = "none";
/// `rest.auth.type` value selecting OAuth2 token authentication.
pub const AUTH_TYPE_OAUTH2: &str = "oauth2";
/// `rest.auth.type` value selecting AWS SigV4 request signing.
pub const AUTH_TYPE_SIGV4: &str = "sigv4";

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

/// Authenticates outgoing REST catalog requests.
#[async_trait]
pub trait AuthSession: Debug + Send + Sync {
    /// Applies authentication to the request (adds headers, signs, ...).
    async fn authenticate(&self, request: &mut Request) -> Result<()>;

    /// Drops any cached credentials so the next request re-authenticates.
    async fn invalidate(&self) -> Result<()> {
        Ok(())
    }

    /// Proactively refreshes cached credentials (e.g. re-exchanges an OAuth2
    /// client credential for a new token), leaving them intact on failure.
    async fn refresh(&self) -> Result<()> {
        Ok(())
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
    async fn authenticate(&self, _request: &mut Request) -> Result<()> {
        Ok(())
    }
}
