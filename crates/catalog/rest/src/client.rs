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

use iceberg::{Error, ErrorKind, Result};
use reqwest::header::HeaderMap;
use reqwest::{Client, IntoUrl, Method, RequestBuilder};
use serde::de::DeserializeOwned;

use crate::RestCatalogConfig;
use crate::auth::AuthSession;
use crate::catalog::{disable_header_redaction_from_props, explicit_headers_from_props};
use crate::request::HttpRequest;
use crate::response::HttpResponse;

/// The catalog's HTTP client, handed to an [`AuthManager`] so its own
/// requests share the catalog's connection pool and configuration.
///
/// [`AuthManager`]: crate::auth::AuthManager
#[derive(Clone)]
pub struct HttpClient {
    client: Client,

    /// Extra headers to be added to each request.
    extra_headers: HeaderMap,
    /// Whether to disable header redaction in error logs (defaults to false for security).
    disable_header_redaction: bool,
}

impl Debug for HttpClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // The inner client is omitted: an injected one may carry secrets in
        // its default headers.
        f.debug_struct("HttpClient")
            .field(
                // `header.*` values may hold secrets (e.g. `authorization`).
                "extra_headers",
                &format_headers_redacted(&self.extra_headers, self.disable_header_redaction),
            )
            .finish_non_exhaustive()
    }
}

impl HttpClient {
    /// Sends an unauthenticated form-encoded POST and returns the response,
    /// which is what an [`AuthManager`] needs to exchange a credential for a
    /// token.
    ///
    /// Only `headers` are sent: neither the catalog's extra headers nor
    /// any session's authentication is applied, so a token request carries
    /// exactly the credentials its caller puts into `headers` or `form`.
    ///
    /// [`AuthManager`]: crate::auth::AuthManager
    pub async fn post_form(
        &self,
        url: &str,
        headers: &HeaderMap,
        form: &HashMap<&str, &str>,
    ) -> Result<HttpResponse> {
        let mut request = HttpRequest::build(
            self.client
                .request(Method::POST, url)
                .headers(headers.clone())
                .form(form),
        )?;
        // `headers` may carry a `content-type: application/json` that `form`
        // leaves in place.
        request.headers_mut().insert(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("application/x-www-form-urlencoded"),
        );

        let response = self.client.execute(request.into_inner()).await?;
        HttpResponse::read(response).await
    }

    /// Create a new http client.
    pub(crate) fn new(cfg: &RestCatalogConfig) -> Result<Self> {
        Ok(HttpClient {
            client: cfg.client(),
            extra_headers: cfg.extra_headers()?,
            disable_header_redaction: cfg.disable_header_redaction(),
        })
    }

    /// Update the http client with new configuration.
    ///
    /// If cfg carries new value, we will use cfg instead.
    /// Otherwise, we will keep the old value.
    pub(crate) fn update_with(self, cfg: &RestCatalogConfig) -> Result<Self> {
        let extra_headers = (!cfg.extra_headers()?.is_empty())
            .then(|| cfg.extra_headers())
            .transpose()?
            .unwrap_or(self.extra_headers);
        Ok(HttpClient {
            // `cfg.client()` returns the same shared client.
            client: cfg.client(),
            extra_headers,
            disable_header_redaction: cfg.disable_header_redaction(),
        })
    }

    /// Derives a client for table-scoped requests.
    ///
    /// The connection pool and catalog headers are inherited, and headers in
    /// the effective table properties override them. As in Java, an explicit
    /// `header.Authorization` takes precedence over the session that
    /// authenticates a request.
    pub(crate) fn for_table(&self, props: &HashMap<String, String>) -> Result<Self> {
        let mut extra_headers = self.extra_headers.clone();
        extra_headers.extend(explicit_headers_from_props(props)?);

        Ok(Self {
            client: self.client.clone(),
            extra_headers,
            disable_header_redaction: disable_header_redaction_from_props(props)
                .unwrap_or(self.disable_header_redaction),
        })
    }

    /// Testing only: the bearer token `session` would attach.
    ///
    /// Authenticates a throwaway request (never sent) and reads the header
    /// back, so it works for any [`AuthSession`].
    #[cfg(test)]
    pub(crate) async fn token(&self, auth_session: &dyn AuthSession) -> Option<String> {
        let mut request = HttpRequest::build(
            self.client
                .request(Method::GET, "http://localhost/token-probe"),
        )
        .ok()?;
        auth_session.authenticate(&mut request).await.ok()?;
        let request = request.into_inner();
        request
            .headers()
            .get(reqwest::header::AUTHORIZATION)?
            .to_str()
            .ok()?
            .strip_prefix("Bearer ")
            .map(str::to_string)
    }

    #[inline]
    pub(crate) fn request<U: IntoUrl>(&self, method: Method, url: U) -> RequestBuilder {
        self.client
            .request(method, url)
            .headers(self.extra_headers.clone())
    }

    /// Sends `request` to the Iceberg REST catalog, authenticated by this
    /// client's session.
    pub(crate) async fn query_catalog(
        &self,
        auth_session: &dyn AuthSession,
        mut request: HttpRequest,
    ) -> Result<HttpResponse> {
        // Authenticate first, then apply extra headers, so a configured
        // `header.authorization` keeps overriding a token (unchanged behavior).
        auth_session.authenticate(&mut request).await?;

        let mut request = request.into_inner();
        request.headers_mut().extend(self.extra_headers.clone());
        HttpResponse::read(self.client.execute(request).await?).await
    }

    /// Returns whether header redaction is disabled for this client.
    pub(crate) fn disable_header_redaction(&self) -> bool {
        self.disable_header_redaction
    }
}

/// Deserializes a catalog response into the given [`DeserializeOwned`] type.
///
/// Returns an error if unable to parse the response bytes.
pub(crate) fn deserialize_catalog_response<R: DeserializeOwned>(
    response: HttpResponse,
) -> Result<R> {
    let bytes = response.body();

    serde_json::from_slice::<R>(bytes).map_err(|e| {
        Error::new(
            ErrorKind::Unexpected,
            "Failed to parse response from rest catalog server",
        )
        .with_context("json", String::from_utf8_lossy(bytes))
        .with_source(e)
    })
}

/// Formats headers for errors and `Debug` output.
///
/// Like Java, every header value is redacted, as any header may carry a
/// secret. With `disable_redaction`, values are shown as they are.
pub(crate) fn format_headers_redacted(headers: &HeaderMap, disable_redaction: bool) -> String {
    let headers: HashMap<&str, &str> = headers
        .iter()
        .filter_map(|(name, value)| {
            let value = if disable_redaction {
                value.to_str().ok()?
            } else {
                "[REDACTED]"
            };
            Some((name.as_str(), value))
        })
        .collect();
    format!("{headers:?}")
}

/// Deserializes a unexpected catalog response into an error.
pub(crate) fn deserialize_unexpected_catalog_error(
    response: HttpResponse,
    disable_header_redaction: bool,
) -> Error {
    unexpected_catalog_error(response, disable_header_redaction, true)
}

/// Builds an unexpected catalog error without retaining the response body.
///
/// Credential endpoints use this because even an unsuccessful response may
/// contain credential material that must not be surfaced through an error.
pub(crate) fn unexpected_catalog_error_without_body(
    response: HttpResponse,
    disable_header_redaction: bool,
) -> Error {
    unexpected_catalog_error(response, disable_header_redaction, false)
}

fn unexpected_catalog_error(
    response: HttpResponse,
    disable_header_redaction: bool,
    include_body: bool,
) -> Error {
    let err = Error::new(
        ErrorKind::Unexpected,
        "Received response with unexpected status code",
    )
    .with_context("status", response.status().to_string())
    .with_context(
        "headers",
        format_headers_redacted(response.headers(), disable_header_redaction),
    );

    let bytes = response.body();
    if bytes.is_empty() {
        return err;
    }
    if include_body {
        return err.with_context("json", String::from_utf8_lossy(bytes));
    }
    // Without the body, keep only the catalog's error description.
    match serde_json::from_slice::<CatalogErrorResponse>(bytes) {
        Ok(CatalogErrorResponse { error }) => err
            .with_context("type", error.r#type)
            .with_context("code", error.code.to_string())
            .with_context("message", error.message),
        Err(_) => err,
    }
}

/// The parts of a REST catalog error response that describe the error.
#[derive(serde::Deserialize)]
struct CatalogErrorResponse {
    error: CatalogError,
}

#[derive(serde::Deserialize)]
struct CatalogError {
    message: String,
    r#type: String,
    code: u16,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::REST_CATALOG_PROP_DISABLE_HEADER_REDACTION;

    #[derive(Debug)]
    struct TableSession;

    #[async_trait::async_trait]
    impl AuthSession for TableSession {
        async fn authenticate(&self, request: &mut HttpRequest) -> Result<()> {
            request
                .headers_mut()
                .insert("authorization", "Bearer table-token".parse().unwrap());
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_a_truncated_body_error_names_the_url() {
        // `bytes()` builds its error without a URL, so `read` attaches the one
        // the response came from; otherwise a failure can't be attributed to a
        // catalog. Needs a raw socket: the body has to be cut short.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0u8; 1024];
            let _ = std::io::Read::read(&mut stream, &mut buf);
            // Promise more than is sent, then hang up.
            let _ = std::io::Write::write_all(
                &mut stream,
                b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nshort",
            );
        });

        let url = format!("http://{addr}/token");
        let err = HttpClient::new(&RestCatalogConfig::builder().uri(url.clone()).build())
            .unwrap()
            .post_form(&url, &HeaderMap::new(), &HashMap::new())
            .await
            .unwrap_err();

        assert!(format!("{err:?}").contains(&url), "{err:?}");
    }

    #[tokio::test]
    async fn test_reading_a_response_keeps_status_headers_and_body() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/token")
            .with_status(418)
            .with_header("x-request-id", "abc123")
            .with_body("brewing")
            .create_async()
            .await;

        let response = HttpClient::new(&RestCatalogConfig::builder().uri(server.url()).build())
            .unwrap()
            .post_form(
                &format!("{}/token", server.url()),
                &HeaderMap::new(),
                &HashMap::new(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), 418);
        assert_eq!(response.headers().get("x-request-id").unwrap(), "abc123");
        assert_eq!(response.body(), b"brewing");
        mock.assert_async().await;
    }

    #[test]
    fn test_unexpected_error_carries_status_headers_and_body() {
        // Everything a user needs to diagnose an unexpected status, with the
        // header values held back.
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer leaked".parse().unwrap());
        headers.insert("x-request-id", "abc123".parse().unwrap());
        let response = HttpResponse::new(
            http::StatusCode::IM_A_TEAPOT,
            headers,
            br#"{"error": "nope"}"#.to_vec(),
        );

        let err = format!(
            "{:?}",
            deserialize_unexpected_catalog_error(response, false)
        );

        assert!(err.contains("418"), "{err}");
        assert!(err.contains("x-request-id"), "{err}");
        assert!(err.contains("nope"), "{err}");
        assert!(!err.contains("leaked"), "{err}");
        assert!(!err.contains("abc123"), "{err}");
    }

    #[test]
    fn test_unexpected_error_can_omit_a_secret_body() {
        let response = HttpResponse::new(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            HeaderMap::new(),
            br#"{"secret": "do-not-expose"}"#.to_vec(),
        );

        let err = format!(
            "{:?}",
            unexpected_catalog_error_without_body(response, false)
        );

        assert!(err.contains("500"), "{err}");
        assert!(!err.contains("do-not-expose"), "{err}");
    }

    #[tokio::test]
    async fn test_post_form_is_never_authenticated() {
        // A token request carries no authentication, not even a
        // user-configured `header.authorization` meant for the catalog.
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/token")
            .match_header("authorization", mockito::Matcher::Missing)
            .with_status(200)
            .create_async()
            .await;

        HttpClient::new(
            &RestCatalogConfig::builder()
                .uri(server.url())
                .props(HashMap::from([(
                    "header.authorization".to_string(),
                    "Basic xyz".to_string(),
                )]))
                .build(),
        )
        .unwrap()
        .post_form(
            &format!("{}/token", server.url()),
            &HeaderMap::new(),
            &HashMap::new(),
        )
        .await
        .unwrap();

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_table_client_uses_table_session_and_explicit_headers() {
        let mut server = mockito::Server::new_async().await;
        let table_session = server
            .mock("GET", "/session")
            .match_header("authorization", "Bearer table-token")
            .match_header("x-catalog", "catalog")
            .with_status(200)
            .create_async()
            .await;
        let explicit_header = server
            .mock("GET", "/header")
            .match_header("authorization", "Bearer explicit-header")
            .with_status(200)
            .create_async()
            .await;

        let config = RestCatalogConfig::builder()
            .uri(server.url())
            .props(HashMap::from([(
                "header.x-catalog".to_string(),
                "catalog".to_string(),
            )]))
            .build();
        let catalog_client = HttpClient::new(&config).unwrap();
        let get = |client: &HttpClient, path: &str| {
            HttpRequest::build(client.request(Method::GET, format!("{}{path}", server.url())))
                .unwrap()
        };

        // The table session authenticates, and catalog headers are inherited.
        let session_client = catalog_client.for_table(&HashMap::new()).unwrap();
        session_client
            .query_catalog(&TableSession, get(&session_client, "/session"))
            .await
            .unwrap();
        table_session.assert_async().await;

        // An explicit Authorization header in the effective properties wins
        // over the session, as in Java.
        let header_client = catalog_client
            .for_table(&HashMap::from([(
                "header.Authorization".to_string(),
                "Bearer explicit-header".to_string(),
            )]))
            .unwrap();
        header_client
            .query_catalog(&TableSession, get(&header_client, "/header"))
            .await
            .unwrap();
        explicit_header.assert_async().await;
    }

    #[test]
    fn test_table_client_redaction_follows_the_table_properties() {
        let catalog_client = HttpClient::new(
            &RestCatalogConfig::builder()
                .uri("http://localhost".to_string())
                .build(),
        )
        .unwrap();
        assert!(!catalog_client.disable_header_redaction());

        let inherited = catalog_client.for_table(&HashMap::new()).unwrap();
        assert!(!inherited.disable_header_redaction());
        let disabled = catalog_client
            .for_table(&HashMap::from([(
                REST_CATALOG_PROP_DISABLE_HEADER_REDACTION.to_string(),
                "true".to_string(),
            )]))
            .unwrap();
        assert!(disabled.disable_header_redaction());
    }

    #[test]
    fn test_format_headers_redacted_empty() {
        let headers = HeaderMap::new();
        let result = format_headers_redacted(&headers, false);
        assert_eq!(result, "{}");
    }

    #[test]
    fn test_format_headers_redacted_redacts_every_value() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer secret-token".parse().unwrap());
        headers.insert(
            "set-cookie",
            "CF_Authorization=sensitive-session-token; Path=/; Secure;"
                .parse()
                .unwrap(),
        );
        headers.insert("x-custom-signature", "unmatched-secret".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());

        let result = format_headers_redacted(&headers, false);

        // Every header name is shown, with its value redacted.
        for name in [
            "authorization",
            "set-cookie",
            "x-custom-signature",
            "content-type",
        ] {
            assert!(result.contains(name), "{result}");
        }
        for value in [
            "secret-token",
            "sensitive-session-token",
            "unmatched-secret",
            "application/json",
        ] {
            assert!(!result.contains(value), "{result}");
        }
        assert!(result.contains("[REDACTED]"));
    }

    #[tokio::test]
    async fn test_http_client_debug_redacts_headers() {
        let config = RestCatalogConfig::builder()
            .uri("http://localhost".to_string())
            .props(HashMap::from([
                ("header.authorization".to_string(), "Basic xyz".to_string()),
                (
                    "header.x-client-secret".to_string(),
                    "shh-secret".to_string(),
                ),
                (
                    "header.x-client-credential".to_string(),
                    "cred-value".to_string(),
                ),
            ]))
            .build();
        let client = HttpClient::new(&config).unwrap();

        let out = format!("{client:?}");
        assert!(!out.contains("Basic xyz"));
        assert!(!out.contains("shh-secret"));
        assert!(!out.contains("cred-value"));
        assert!(out.contains("[REDACTED]"));
    }

    #[test]
    fn test_format_headers_with_redaction_disabled() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer secret-token".parse().unwrap());
        headers.insert("x-api-key", "api-key-123".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());

        let result = format_headers_redacted(&headers, true);

        // When redaction is disabled, all headers and values should be present
        assert!(result.contains("authorization"));
        assert!(result.contains("Bearer secret-token"));
        assert!(result.contains("x-api-key"));
        assert!(result.contains("api-key-123"));
        assert!(result.contains("content-type"));
        assert!(result.contains("application/json"));
        // [REDACTED] should NOT be present when redaction is disabled
        assert!(!result.contains("[REDACTED]"));
    }
}
