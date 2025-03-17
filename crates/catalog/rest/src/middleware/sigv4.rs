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

//! Middleware that signs requests using the AWS SigV4 signing process.

use std::time::SystemTime;

use anyhow::anyhow;
use aws_config::{BehaviorVersion, Region};
use aws_credential_types::provider::ProvideCredentials;
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{sign, SignableBody, SignableRequest, SigningSettings};
use aws_sigv4::sign::v4;
use http::Extensions;
use reqwest::{Request, Response};
use reqwest_middleware::{Middleware, Next, Result};
use tokio::sync::OnceCell;

pub(crate) struct SigV4Middleware {
    catalog_uri: String,
    signing_name: String,
    signing_region: Option<String>,

    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    session_token: Option<String>,
    config: OnceCell<aws_config::SdkConfig>,
}

impl SigV4Middleware {
    pub(crate) fn new(catalog_uri: &str, signing_name: &str, signing_region: Option<&str>) -> Self {
        Self {
            catalog_uri: catalog_uri.to_string(),
            signing_name: signing_name.to_string(),
            signing_region: signing_region.map(|s| s.to_string()),
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            config: OnceCell::new(),
        }
    }

    pub(crate) fn with_credentials(
        mut self,
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
    ) -> Self {
        self.access_key_id = Some(access_key_id);
        self.secret_access_key = Some(secret_access_key);
        self.session_token = session_token;
        self
    }
}

#[async_trait::async_trait]
impl Middleware for SigV4Middleware {
    async fn handle(
        &self,
        mut req: Request,
        extensions: &mut Extensions,
        next: Next<'_>,
    ) -> Result<Response> {
        // Skip requests not matching the catalog URI prefix
        if !req.url().as_str().starts_with(&self.catalog_uri) {
            return next.run(req, extensions).await;
        }

        let signing_region = self.signing_region.clone();
        let config = self
            .config
            .get_or_init(|| async {
                let mut config_loader = aws_config::defaults(BehaviorVersion::v2024_03_28());
                if let Some(signing_region) = signing_region {
                    config_loader = config_loader.region(Region::new(signing_region));
                }
                if let (Some(access_key_id), Some(secret_access_key)) =
                    (&self.access_key_id, &self.secret_access_key)
                {
                    config_loader = config_loader.credentials_provider(Credentials::new(
                        access_key_id,
                        secret_access_key,
                        self.session_token.clone(),
                        None,
                        "iceberg-rest-catalog",
                    ));
                }
                config_loader.load().await
            })
            .await;

        let credential_provider = config.credentials_provider().ok_or_else(|| {
            reqwest_middleware::Error::Middleware(anyhow!("No credentials provider found"))
        })?;
        let credentials = credential_provider
            .provide_credentials()
            .await
            .map_err(|e| reqwest_middleware::Error::Middleware(e.into()))?
            .into();

        let region: &str = config.region().map(|r| r.as_ref()).unwrap_or("us-east-1");

        // Prepare signing parameters
        let signing_params = v4::SigningParams::builder()
            .identity(&credentials)
            .region(region)
            .name(&self.signing_name)
            .time(SystemTime::now())
            .settings(SigningSettings::default())
            .build()
            .map_err(|e| reqwest_middleware::Error::Middleware(e.into()))?;

        // In order to sign the request, we need to read the body into bytes.
        let body = match req.body() {
            Some(body) => SignableBody::Bytes(body.as_bytes().ok_or_else(|| {
                reqwest_middleware::Error::Middleware(anyhow!("Unable to read body as bytes"))
            })?),
            None => SignableBody::Bytes(&[]),
        };
        let signable_request = SignableRequest::new(
            req.method().as_str(),
            req.url().as_str(),
            req.headers()
                .iter()
                .map(|(k, v)| (k.as_str(), v.to_str().expect("Invalid header value"))),
            body,
        )
        .map_err(|e| reqwest_middleware::Error::Middleware(e.into()))?;

        // Sign the request
        let signed_request = sign(signable_request, &signing_params.into())
            .map_err(|e| reqwest_middleware::Error::Middleware(e.into()))?;

        // Rebuild the reqwest request with signed headers
        let (signed_parts, _signature) = signed_request.into_parts();

        let (new_headers, _) = signed_parts.into_parts();
        for header in new_headers.into_iter() {
            let mut value = http::HeaderValue::from_str(header.value()).map_err(|e| {
                reqwest_middleware::Error::Middleware(anyhow!("Invalid header value: {e}"))
            })?;
            value.set_sensitive(header.sensitive());
            req.headers_mut().insert(header.name(), value);
        }

        next.run(req, extensions).await
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::sync::{LazyLock, Mutex};

    use reqwest::Client;
    use reqwest_middleware::ClientBuilder;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    static TEST_MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    fn set_test_credentials() {
        env::set_var("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
        env::set_var(
            "AWS_SECRET_ACCESS_KEY",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        );
    }

    fn unset_test_credentials() {
        env::remove_var("AWS_ACCESS_KEY_ID");
        env::remove_var("AWS_SECRET_ACCESS_KEY");
    }

    #[tokio::test]
    async fn test_sigv4_middleware_handles_missing_credentials() {
        let _guard = TEST_MUTEX.lock();

        // Start a mock server
        let mock_server = MockServer::start().await;
        let catalog_uri = mock_server.uri();

        // Create middleware
        let middleware = SigV4Middleware::new(&catalog_uri, "s3", Some("us-east-1"));

        // Create a client with the middleware
        let client = ClientBuilder::new(Client::new()).with(middleware).build();

        // Make request (should fail due to no credentials)
        let resp = client.get(&catalog_uri).send().await;
        assert!(resp.is_err());
        match resp.unwrap_err() {
            reqwest_middleware::Error::Middleware(_e) => {}
            _ => panic!("Unexpected error"),
        }
    }

    #[tokio::test]
    async fn test_sigv4_middleware_signs_matching_requests() {
        let _guard = TEST_MUTEX.lock();

        // Start a mock server
        let mock_server = MockServer::start().await;
        let catalog_uri = mock_server.uri();

        // Create middleware with test credentials
        set_test_credentials();
        let middleware = SigV4Middleware::new(&catalog_uri, "s3", Some("us-east-1"));

        // Create a client with the middleware
        let client = ClientBuilder::new(Client::new()).with(middleware).build();

        // Set up mock to check for AWS auth header
        Mock::given(method("GET"))
            .and(path("/"))
            .and(HeaderStartsWith("Authorization", "AWS4-HMAC-SHA256"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&mock_server)
            .await;

        // Make request
        let resp = client.get(&catalog_uri).send().await;
        assert!(resp.is_ok());

        // Verify all mocks were called as expected
        mock_server.verify().await;
        unset_test_credentials();
    }

    #[tokio::test]
    async fn test_sigv4_middleware_skips_non_matching_requests() {
        let _guard = TEST_MUTEX.lock();

        // Start a mock server
        let mock_server = MockServer::start().await;

        // Create middleware with different URI and test credentials
        set_test_credentials();
        let middleware = SigV4Middleware::new("http://different-uri", "s3", Some("us-east-1"));

        // Create a client with the middleware
        let client = ClientBuilder::new(Client::new()).with(middleware).build();

        // Set up mock that expects no AWS auth header
        Mock::given(method("GET"))
            .and(path("/"))
            .and(HeaderMissing("Authorization"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&mock_server)
            .await;

        // Make request
        let resp = client.get(mock_server.uri()).send().await;
        assert!(resp.is_ok());

        // Verify all mocks were called as expected
        mock_server.verify().await;
        unset_test_credentials();
    }

    #[tokio::test]
    async fn test_sigv4_middleware_with_explicit_credentials() {
        let _guard = TEST_MUTEX.lock();

        // Start a mock server
        let mock_server = MockServer::start().await;
        let catalog_uri = mock_server.uri();

        // Make sure environment credentials are not set
        unset_test_credentials();

        // Create middleware with explicit credentials
        let middleware = SigV4Middleware::new(&catalog_uri, "s3", Some("us-east-1"))
            .with_credentials(
                "EXPLICIT_KEY_ID".to_string(),
                "EXPLICIT_SECRET_KEY".to_string(),
                Some("EXPLICIT_SESSION_TOKEN".to_string()),
            );

        // Create a client with the middleware
        let client = ClientBuilder::new(Client::new()).with(middleware).build();

        // Set up mock to check for AWS auth header
        Mock::given(method("GET"))
            .and(path("/"))
            .and(HeaderStartsWith("Authorization", "AWS4-HMAC-SHA256"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&mock_server)
            .await;

        // Make request
        let resp = client.get(&catalog_uri).send().await;
        assert!(resp.is_ok());

        // Verify all mocks were called as expected
        mock_server.verify().await;
    }

    #[tokio::test]
    async fn test_sigv4_middleware_explicit_credentials_override_env() {
        let _guard = TEST_MUTEX.lock();

        // Start a mock server
        let mock_server = MockServer::start().await;
        let catalog_uri = mock_server.uri();

        // Set environment credentials that should be ignored
        set_test_credentials();

        // Create middleware with explicit credentials that should take precedence
        let middleware = SigV4Middleware::new(&catalog_uri, "s3", Some("us-east-1"))
            .with_credentials(
                "EXPLICIT_OVERRIDE_KEY".to_string(),
                "EXPLICIT_OVERRIDE_SECRET".to_string(),
                None,
            );

        // Create a client with the middleware
        let client = ClientBuilder::new(Client::new()).with(middleware).build();

        // Set up mock to check for AWS auth header
        Mock::given(method("GET"))
            .and(path("/"))
            .and(HeaderStartsWith("Authorization", "AWS4-HMAC-SHA256"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&mock_server)
            .await;

        // Make request
        let resp = client.get(&catalog_uri).send().await;
        assert!(resp.is_ok());

        // Verify all mocks were called as expected
        mock_server.verify().await;
        unset_test_credentials();
    }

    struct HeaderMissing(&'static str);

    impl wiremock::Match for HeaderMissing {
        fn matches(&self, request: &wiremock::Request) -> bool {
            !request.headers.contains_key(self.0)
        }
    }

    struct HeaderStartsWith(&'static str, &'static str);

    impl wiremock::Match for HeaderStartsWith {
        fn matches(&self, request: &wiremock::Request) -> bool {
            request
                .headers
                .get(self.0)
                .and_then(|h| h.to_str().ok())
                .map(|v| v.starts_with(self.1))
                .unwrap_or(false)
        }
    }
}
