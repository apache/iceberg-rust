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
use iceberg::Result;
pub use oauth2::OAuth2Manager;

use crate::client::HttpClient;
use crate::request::HttpRequest;

/// `rest.auth.type` value disabling authentication.
pub const AUTH_TYPE_NONE: &str = "none";
/// `rest.auth.type` value selecting OAuth2 token authentication.
pub const AUTH_TYPE_OAUTH2: &str = "oauth2";

/// Creates the [`AuthSession`]s used to authenticate REST catalog requests.
///
/// A manager is created once per catalog, either from the `rest.auth.type`
/// property or injected through `RestCatalogBuilder::with_auth_manager`. It
/// builds the sessions the catalog then keeps.
///
/// Both methods are handed the catalog's [`HttpClient`], which an
/// implementation may reuse for its own requests (e.g. a token exchange) so
/// that they share the catalog's connection pool and configuration.
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

#[cfg(test)]
mod tests {
    use reqwest::Client;

    use super::*;
    use crate::RestCatalogConfig;

    fn test_client() -> HttpClient {
        HttpClient::new(
            &RestCatalogConfig::builder()
                .uri("http://localhost".to_string())
                .build(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_static_token_session_attaches_token() {
        // Token-only config: the token is attached as-is.
        let manager = OAuth2Manager::new("http://localhost/unused").with_token("tok-static");
        let session = manager
            .init_session(&test_client(), &HashMap::new())
            .await
            .unwrap();

        let mut req = HttpRequest::new(
            Client::new()
                .get("https://rest.example.com/v1/config")
                .build()
                .unwrap(),
        );
        session.authenticate(&mut req).await.unwrap();
        assert_eq!(
            req.headers().get("authorization").unwrap(),
            "Bearer tok-static"
        );
    }
}
