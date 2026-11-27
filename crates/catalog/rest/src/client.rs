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
use crate::auth::{AuthSession, UnauthorizedAction};
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

    /// Sends `request` to the Iceberg REST catalog, authenticated by
    /// `auth_session`.
    ///
    /// When the catalog answers `401` or `419`, the session's
    /// [`AuthSession::on_unauthorized`] decides whether the request is
    /// authenticated again and sent once more.
    pub(crate) async fn query_catalog(
        &self,
        auth_session: &dyn AuthSession,
        request: HttpRequest,
    ) -> Result<HttpResponse> {
        // Sending consumes the request, so the copies a rejection needs are
        // made up front. A copy shares the body's buffer and copies only the
        // URL and headers. `None` for a streaming body, which cannot be sent
        // twice.
        let unauthenticated = request.try_clone();
        let request = self.authenticate(auth_session, request).await?;
        // Paired with the request as sent, which the session inspects.
        let retryable = unauthenticated
            .and_then(|unauthenticated| Some((unauthenticated, request.try_clone()?)));
        let response = self.send(request).await?;

        let Some((unauthenticated, sent)) = retryable else {
            return Ok(response);
        };
        if !rejects_authentication(response.status()) {
            return Ok(response);
        }
        let action = auth_session
            .on_unauthorized(&sent, &response)
            .await
            .map_err(|err| err.with_context("status", response.status().to_string()))?;
        match action {
            UnauthorizedAction::Fail => Ok(response),
            UnauthorizedAction::Retry => {
                let request = self.authenticate(auth_session, unauthenticated).await?;
                self.send(request).await
            }
        }
    }

    /// Authenticates `request` with `auth_session` and applies the extra
    /// headers.
    async fn authenticate(
        &self,
        auth_session: &dyn AuthSession,
        mut request: HttpRequest,
    ) -> Result<HttpRequest> {
        // Authenticate first, then apply extra headers, so a configured
        // `header.authorization` keeps overriding a token.
        auth_session.authenticate(&mut request).await?;
        request.headers_mut().extend(self.extra_headers.clone());
        Ok(request)
    }

    async fn send(&self, request: HttpRequest) -> Result<HttpResponse> {
        HttpResponse::read(self.client.execute(request.into_inner()).await?).await
    }

    /// Returns whether header redaction is disabled for this client.
    pub(crate) fn disable_header_redaction(&self) -> bool {
        self.disable_header_redaction
    }
}

/// Whether `status` rejects a request's authentication: `401`, or `419`, which
/// the Iceberg REST spec allows for an expired token (`http` names no constant
/// for it).
fn rejects_authentication(status: http::StatusCode) -> bool {
    status == http::StatusCode::UNAUTHORIZED || status.as_u16() == 419
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

/// Returns true if the header may carry a secret (matched by substring, so
/// e.g. `x-client-secret` is covered along with `authorization`).
fn is_sensitive_header(name: &str) -> bool {
    let name_lower = name.to_lowercase();
    [
        "auth",
        "token",
        "secret",
        "key",
        "password",
        "cookie",
        "credential",
    ]
    .iter()
    .any(|pattern| name_lower.contains(pattern))
}

/// Redacts sensitive headers and returns a debug-formatted string.
///
/// If `disable_redaction` is true, returns all headers without redaction.
/// Otherwise, replaces sensitive header values with `[REDACTED]`.
pub(crate) fn format_headers_redacted(headers: &HeaderMap, disable_redaction: bool) -> String {
    if disable_redaction {
        // Return all headers as-is without redaction
        let all: HashMap<&str, &str> = headers
            .iter()
            .filter_map(|(name, value)| value.to_str().ok().map(|v| (name.as_str(), v)))
            .collect();
        return format!("{all:?}");
    }

    // Redact sensitive headers by replacing their values with "[REDACTED]"
    let redacted: HashMap<&str, &str> = headers
        .iter()
        .filter_map(|(name, value)| {
            if is_sensitive_header(name.as_str()) {
                Some((name.as_str(), "[REDACTED]"))
            } else {
                value.to_str().ok().map(|v| (name.as_str(), v))
            }
        })
        .collect();
    format!("{redacted:?}")
}

/// Deserializes a unexpected catalog response into an error.
pub(crate) fn deserialize_unexpected_catalog_error(
    response: HttpResponse,
    disable_header_redaction: bool,
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
    err.with_context("json", String::from_utf8_lossy(bytes))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;

    use super::*;

    /// Appends `x-attempt: <n>` on its n-th authentication and answers each
    /// rejection with `answer`, recording the `x-attempt` values of the
    /// requests it is handed.
    #[derive(Debug)]
    struct RecordingSession {
        attempts: AtomicUsize,
        answer: fn() -> Result<UnauthorizedAction>,
        rejected: Mutex<Vec<Vec<String>>>,
    }

    impl RecordingSession {
        fn answering(answer: fn() -> Result<UnauthorizedAction>) -> Self {
            Self {
                attempts: AtomicUsize::new(0),
                answer,
                rejected: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl AuthSession for RecordingSession {
        async fn authenticate(&self, request: &mut HttpRequest) -> Result<()> {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
            request.headers_mut().append("x-attempt", attempt.into());
            Ok(())
        }

        async fn on_unauthorized(
            &self,
            request: &HttpRequest,
            _response: &HttpResponse,
        ) -> Result<UnauthorizedAction> {
            self.rejected.lock().unwrap().push(
                request
                    .headers()
                    .get_all("x-attempt")
                    .iter()
                    .map(|value| value.to_str().unwrap().to_string())
                    .collect(),
            );
            (self.answer)()
        }
    }

    fn test_client(server: &mockito::ServerGuard) -> HttpClient {
        HttpClient::new(&RestCatalogConfig::builder().uri(server.url()).build()).unwrap()
    }

    /// `GET /v1/namespaces` answering with `status`, expecting `hits` requests.
    async fn rejecting_mock(
        server: &mut mockito::ServerGuard,
        status: usize,
        hits: usize,
    ) -> mockito::Mock {
        server
            .mock("GET", "/v1/namespaces")
            .with_status(status)
            .expect(hits)
            .create_async()
            .await
    }

    fn list_namespaces(client: &HttpClient, server: &mockito::ServerGuard) -> HttpRequest {
        HttpRequest::build(client.request(Method::GET, format!("{}/v1/namespaces", server.url())))
            .unwrap()
    }

    /// The catalog rejects the first attempt with `status`: the session is
    /// asked, and the request is authenticated again and sent once more.
    async fn assert_rejected_request_is_sent_once_more(status: usize) {
        let mut server = mockito::Server::new_async().await;
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_by_server = seen.clone();
        let mock = server
            .mock("POST", "/v1/namespaces")
            .match_body(r#"{"namespace": ["ns"]}"#)
            .with_status_code_from_request(move |request| {
                let attempts: Vec<String> = request
                    .header("x-attempt")
                    .iter()
                    .map(|value| value.to_str().unwrap().to_string())
                    .collect();
                let status = if attempts == ["2"] { 200 } else { status };
                seen_by_server.lock().unwrap().push(attempts);
                status
            })
            .expect(2)
            .create_async()
            .await;
        let client = test_client(&server);
        let session = RecordingSession::answering(|| Ok(UnauthorizedAction::Retry));

        let request = HttpRequest::build(
            client
                .request(Method::POST, format!("{}/v1/namespaces", server.url()))
                .body(r#"{"namespace": ["ns"]}"#),
        )
        .unwrap();
        let response = client.query_catalog(&session, request).await.unwrap();

        assert_eq!(response.status(), http::StatusCode::OK);
        // The second attempt is authenticated from the request as it was
        // before the first authentication.
        assert_eq!(*seen.lock().unwrap(), vec![vec!["1"], vec!["2"]]);
        assert_eq!(*session.rejected.lock().unwrap(), vec![vec!["1"]]);
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_unauthorized_request_is_authenticated_again_and_sent_once_more() {
        assert_rejected_request_is_sent_once_more(401).await;
    }

    #[tokio::test]
    async fn test_authentication_timeout_request_is_authenticated_again_and_sent_once_more() {
        assert_rejected_request_is_sent_once_more(419).await;
    }

    #[tokio::test]
    async fn test_second_rejection_is_returned_without_asking_again() {
        let mut server = mockito::Server::new_async().await;
        let mock = rejecting_mock(&mut server, 401, 2).await;
        let client = test_client(&server);
        let session = RecordingSession::answering(|| Ok(UnauthorizedAction::Retry));

        let response = client
            .query_catalog(&session, list_namespaces(&client, &server))
            .await
            .unwrap();

        assert_eq!(response.status(), http::StatusCode::UNAUTHORIZED);
        assert_eq!(*session.rejected.lock().unwrap(), vec![vec!["1"]]);
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_rejected_request_is_returned_by_a_session_without_hook() {
        let mut server = mockito::Server::new_async().await;
        let mock = rejecting_mock(&mut server, 401, 1).await;
        let client = test_client(&server);

        let response = client
            .query_catalog(&crate::auth::NoopSession, list_namespaces(&client, &server))
            .await
            .unwrap();

        assert_eq!(response.status(), http::StatusCode::UNAUTHORIZED);
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_forbidden_request_is_returned_without_hook() {
        let mut server = mockito::Server::new_async().await;
        let mock = rejecting_mock(&mut server, 403, 1).await;
        let client = test_client(&server);
        let session = RecordingSession::answering(|| Ok(UnauthorizedAction::Retry));

        let response = client
            .query_catalog(&session, list_namespaces(&client, &server))
            .await
            .unwrap();

        assert_eq!(response.status(), http::StatusCode::FORBIDDEN);
        assert_eq!(*session.rejected.lock().unwrap(), Vec::<Vec<String>>::new());
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_hook_error_is_returned_in_place_of_the_response() {
        let mut server = mockito::Server::new_async().await;
        let mock = rejecting_mock(&mut server, 401, 1).await;
        let client = test_client(&server);
        let session =
            RecordingSession::answering(|| Err(Error::new(ErrorKind::Unexpected, "hook failed")));

        let err = client
            .query_catalog(&session, list_namespaces(&client, &server))
            .await
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "Unexpected, context: { status: 401 Unauthorized } => hook failed"
        );
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_rejected_streaming_request_is_returned_without_hook() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/v1/namespaces")
            .with_status(401)
            .expect(1)
            .create_async()
            .await;
        let client = test_client(&server);
        let session = RecordingSession::answering(|| Ok(UnauthorizedAction::Retry));

        let request = HttpRequest::build(
            client
                .request(Method::POST, format!("{}/v1/namespaces", server.url()))
                .body(reqwest::Body::wrap_stream(futures::stream::once(async {
                    Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"{}"))
                }))),
        )
        .unwrap();
        let response = client.query_catalog(&session, request).await.unwrap();

        assert_eq!(response.status(), http::StatusCode::UNAUTHORIZED);
        assert_eq!(*session.rejected.lock().unwrap(), Vec::<Vec<String>>::new());
        mock.assert_async().await;
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
        // sensitive headers held back.
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
        assert!(err.contains("abc123"), "{err}");
        assert!(err.contains("nope"), "{err}");
        assert!(!err.contains("leaked"), "{err}");
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

    #[test]
    fn test_format_headers_redacted_empty() {
        let headers = HeaderMap::new();
        let result = format_headers_redacted(&headers, false);
        assert_eq!(result, "{}");
    }

    #[test]
    fn test_format_headers_redacted_non_sensitive() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());
        headers.insert("x-request-id", "abc123".parse().unwrap());

        let result = format_headers_redacted(&headers, false);

        assert!(result.contains("content-type"));
        assert!(result.contains("application/json"));
        assert!(result.contains("x-request-id"));
        assert!(result.contains("abc123"));
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
    fn test_format_headers_redacted_filters_sensitive() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer secret-token".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());

        let result = format_headers_redacted(&headers, false);

        // Sensitive header should be present but with redacted value
        assert!(result.contains("authorization"));
        assert!(result.contains("[REDACTED]"));
        // Sensitive value should NOT be present
        assert!(!result.contains("secret-token"));
        // Non-sensitive header should be present with actual value
        assert!(result.contains("content-type"));
        assert!(result.contains("application/json"));
    }

    #[test]
    fn test_format_headers_redacted_filters_set_cookie() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "set-cookie",
            "CF_Authorization=sensitive-session-token; Path=/; Secure;"
                .parse()
                .unwrap(),
        );
        headers.insert("server", "cloudflare".parse().unwrap());

        let result = format_headers_redacted(&headers, false);

        // Sensitive header should be present but with redacted value
        assert!(result.contains("set-cookie"));
        assert!(result.contains("[REDACTED]"));
        // Sensitive value should NOT be present
        assert!(!result.contains("sensitive-session-token"));
        // Non-sensitive header should be present with actual value
        assert!(result.contains("server"));
        assert!(result.contains("cloudflare"));
    }

    #[test]
    fn test_format_headers_redacted_filters_all_sensitive() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer token".parse().unwrap());
        headers.insert("proxy-authorization", "Basic creds".parse().unwrap());
        headers.insert("set-cookie", "session=abc".parse().unwrap());
        headers.insert("cookie", "session=abc".parse().unwrap());
        headers.insert("x-api-key", "api-key-123".parse().unwrap());
        headers.insert("x-auth-token", "auth-token-456".parse().unwrap());
        headers.insert("x-request-id", "req-123".parse().unwrap());

        let result = format_headers_redacted(&headers, false);

        // All sensitive headers should be present but with redacted values
        assert!(result.contains("authorization"));
        assert!(result.contains("proxy-authorization"));
        assert!(result.contains("set-cookie"));
        assert!(result.contains("cookie"));
        assert!(result.contains("x-api-key"));
        assert!(result.contains("x-auth-token"));
        assert!(result.contains("[REDACTED]"));

        // Ensure no sensitive values leaked
        assert!(!result.contains("Bearer token"));
        assert!(!result.contains("Basic creds"));
        assert!(!result.contains("session=abc"));
        assert!(!result.contains("api-key-123"));
        assert!(!result.contains("auth-token-456"));

        // Non-sensitive header should be present with actual value
        assert!(result.contains("x-request-id"));
        assert!(result.contains("req-123"));
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
