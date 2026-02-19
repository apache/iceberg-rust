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

//! Authentication module for the REST catalog.
//!
//! This module provides pluggable authentication mechanisms for the Iceberg REST catalog.
//! Currently supported authentication methods:
//!
//! - **OAuth2**: Bearer token authentication using OAuth2 client credentials flow
//! - **SigV4**: AWS Signature Version 4 authentication for AWS-compatible services
//! - **None**: No authentication (for development/testing)
//!
//! # Configuration
//!
//! Authentication is configured via catalog properties:
//!
//! ## OAuth2 (default)
//! ```text
//! token = "your-bearer-token"
//! // or
//! credential = "client_id:client_secret"
//! oauth2-server-uri = "https://auth.example.com/oauth/tokens"
//! scope = "catalog"
//! ```
//!
//! ## SigV4
//! ```text
//! auth-type = "sigv4"
//! s3.access-key-id = "AKIAIOSFODNN7EXAMPLE"
//! s3.secret-access-key = "wJalrXUtnFEMI/K7MDENG..."
//! s3.session-token = "..."  // optional
//! s3.region = "us-east-1"
//! sigv4.service = "s3"  // optional, defaults to "s3"
//! ```

mod oauth2;
mod sigv4;

use std::fmt::Debug;

use async_trait::async_trait;
use iceberg::Result;
pub use oauth2::OAuth2Authenticator;
use reqwest::Request;
pub use sigv4::{SigV4Authenticator, SigV4Credentials};

/// Authentication provider trait for REST catalog requests.
///
/// Implementors of this trait provide a mechanism to authenticate HTTP requests
/// before they are sent to the REST catalog server.
///
/// # Thread Safety
///
/// Authenticators must be `Send + Sync` to allow concurrent request authentication.
///
/// # Example
///
/// ```ignore
/// use iceberg_catalog_rest::auth::{Authenticator, SigV4Authenticator, SigV4Credentials};
///
/// let credentials = SigV4Credentials::new("access_key", "secret_key", None);
/// let auth = SigV4Authenticator::new(credentials, "us-east-1", "s3");
///
/// // The authenticator will sign requests with AWS SigV4
/// auth.authenticate(&mut request).await?;
/// ```
#[async_trait]
pub trait Authenticator: Send + Sync + Debug {
    /// Authenticate a request by modifying its headers.
    ///
    /// This method is called before each request is sent to the catalog server.
    /// Implementations should add appropriate authentication headers to the request.
    ///
    /// # Arguments
    ///
    /// * `request` - The mutable request to authenticate
    ///
    /// # Returns
    ///
    /// * `Ok(())` if authentication was successful
    /// * `Err(...)` if authentication failed (e.g., invalid credentials)
    async fn authenticate(&self, request: &mut Request) -> Result<()>;

    /// Invalidate any cached credentials or tokens.
    ///
    /// This method is called when authentication fails and the client needs to
    /// refresh credentials. Implementations should clear any cached tokens.
    ///
    /// The default implementation does nothing.
    async fn invalidate(&self) -> Result<()> {
        Ok(())
    }

    /// Regenerate credentials or tokens.
    ///
    /// This method is called to proactively refresh credentials before they expire.
    /// Implementations should fetch new tokens or refresh credentials as needed.
    ///
    /// The default implementation does nothing.
    async fn regenerate(&self) -> Result<()> {
        Ok(())
    }

    /// Returns the authentication scheme name for logging and debugging.
    fn scheme_name(&self) -> &'static str;

    /// Returns the current cached token, if any.
    ///
    /// This is primarily used for testing OAuth2 authentication flows.
    /// For non-token-based authentication (SigV4, NoAuth), this returns None.
    async fn get_token(&self) -> Option<String> {
        None
    }
}

/// No authentication provider.
///
/// This authenticator does nothing and is used when no authentication is required.
#[derive(Debug, Clone, Default)]
pub struct NoAuth;

#[async_trait]
impl Authenticator for NoAuth {
    async fn authenticate(&self, _request: &mut Request) -> Result<()> {
        Ok(())
    }

    fn scheme_name(&self) -> &'static str {
        "none"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_no_auth_does_nothing() {
        let auth = NoAuth;
        let client = reqwest::Client::new();
        let mut request = client
            .get("http://example.com/test")
            .build()
            .expect("Failed to build request");

        let header_count_before = request.headers().len();
        auth.authenticate(&mut request).await.unwrap();
        let header_count_after = request.headers().len();

        assert_eq!(header_count_before, header_count_after);
    }

    #[test]
    fn test_no_auth_scheme_name() {
        let auth = NoAuth;
        assert_eq!(auth.scheme_name(), "none");
    }

    #[tokio::test]
    async fn test_no_auth_invalidate() {
        let auth = NoAuth;
        auth.invalidate().await.unwrap();
    }

    #[tokio::test]
    async fn test_no_auth_regenerate() {
        let auth = NoAuth;
        auth.regenerate().await.unwrap();
    }
}
