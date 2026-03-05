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

//! AWS Signature Version 4 authentication for the REST catalog.
//!
//! This module provides SigV4 authentication for AWS-compatible services,
//! enabling the REST catalog to authenticate against MinIO, AWS S3 Tables,
//! and other AWS-compatible services.

use std::fmt::{Debug, Formatter};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use iceberg::{Error, ErrorKind, Result};
use reqwest::Request;
use tokio::sync::RwLock;

use super::Authenticator;

/// AWS credentials for SigV4 authentication.
#[derive(Clone)]
pub struct SigV4Credentials {
    /// AWS access key ID
    pub access_key_id: String,
    /// AWS secret access key
    pub secret_access_key: String,
    /// Optional session token for temporary credentials
    pub session_token: Option<String>,
}

impl Debug for SigV4Credentials {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SigV4Credentials")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"[REDACTED]")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

impl SigV4Credentials {
    /// Creates new SigV4 credentials.
    ///
    /// # Arguments
    ///
    /// * `access_key_id` - AWS access key ID
    /// * `secret_access_key` - AWS secret access key
    /// * `session_token` - Optional session token for temporary credentials
    pub fn new(
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        session_token: Option<impl Into<String>>,
    ) -> Self {
        Self {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            session_token: session_token.map(Into::into),
        }
    }
}

/// Cached signing key for performance optimization.
///
/// AWS SigV4 derives a signing key from the secret key, date, region, and service.
/// This key is valid for an entire day, so caching it significantly improves performance.
struct SigningKeyCache {
    key: [u8; 32],
    date_stamp: String,
    region: String,
    service: String,
}

impl SigningKeyCache {
    /// Check if the cached key is valid for the given parameters.
    fn is_valid(&self, date_stamp: &str, region: &str, service: &str) -> bool {
        self.date_stamp == date_stamp && self.region == region && self.service == service
    }
}

/// AWS Signature Version 4 authenticator.
///
/// This authenticator signs HTTP requests using AWS Signature Version 4,
/// which is required for AWS services and AWS-compatible services like MinIO.
///
/// # Example
///
/// ```ignore
/// use iceberg_catalog_rest::auth::{SigV4Authenticator, SigV4Credentials};
///
/// let credentials = SigV4Credentials::new(
///     "AKIAIOSFODNN7EXAMPLE",
///     "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
///     None,
/// );
///
/// let auth = SigV4Authenticator::new(credentials, "us-east-1", "s3");
/// ```
pub struct SigV4Authenticator {
    credentials: SigV4Credentials,
    region: String,
    service: String,
    /// Cached signing key for performance
    signing_key_cache: RwLock<Option<SigningKeyCache>>,
}

impl Debug for SigV4Authenticator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SigV4Authenticator")
            .field("credentials", &self.credentials)
            .field("region", &self.region)
            .field("service", &self.service)
            .finish()
    }
}

impl SigV4Authenticator {
    /// Creates a new SigV4 authenticator.
    ///
    /// # Arguments
    ///
    /// * `credentials` - AWS credentials
    /// * `region` - AWS region (e.g., "us-east-1")
    /// * `service` - AWS service name (e.g., "s3", "s3tables")
    pub fn new(
        credentials: SigV4Credentials,
        region: impl Into<String>,
        service: impl Into<String>,
    ) -> Self {
        Self {
            credentials,
            region: region.into(),
            service: service.into(),
            signing_key_cache: RwLock::new(None),
        }
    }

    /// Creates a SigV4 authenticator for S3 Tables service.
    pub fn for_s3tables(credentials: SigV4Credentials, region: impl Into<String>) -> Self {
        Self::new(credentials, region, "s3tables")
    }

    /// Creates a SigV4 authenticator for S3 service.
    pub fn for_s3(credentials: SigV4Credentials, region: impl Into<String>) -> Self {
        Self::new(credentials, region, "s3")
    }

    /// Get or compute the signing key.
    async fn get_signing_key(&self, date_stamp: &str) -> [u8; 32] {
        // Check cache first
        {
            let cache = self.signing_key_cache.read().await;
            if let Some(ref cached) = *cache
                && cached.is_valid(date_stamp, &self.region, &self.service)
            {
                return cached.key;
            }
        }

        // Compute new signing key
        let key = derive_signing_key(
            &self.credentials.secret_access_key,
            date_stamp,
            &self.region,
            &self.service,
        );

        // Cache the new key
        {
            let mut cache = self.signing_key_cache.write().await;
            *cache = Some(SigningKeyCache {
                key,
                date_stamp: date_stamp.to_string(),
                region: self.region.clone(),
                service: self.service.clone(),
            });
        }

        key
    }

    /// Sign the request with AWS SigV4.
    async fn sign_request(&self, request: &mut Request, now: DateTime<Utc>) -> Result<()> {
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date_stamp = now.format("%Y%m%d").to_string();

        // Get host from URL
        let host = request
            .url()
            .host_str()
            .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "Request URL has no host"))?
            .to_string();

        // Add port if non-standard
        let host_header = if let Some(port) = request.url().port() {
            format!("{host}:{port}")
        } else {
            host
        };

        // Add required headers
        request
            .headers_mut()
            .insert("x-amz-date", amz_date.parse().unwrap());
        request
            .headers_mut()
            .insert("host", host_header.parse().unwrap());

        // Add session token if present
        if let Some(ref token) = self.credentials.session_token {
            request.headers_mut().insert(
                "x-amz-security-token",
                token.parse().map_err(|e| {
                    Error::new(ErrorKind::DataInvalid, "Invalid session token").with_source(e)
                })?,
            );
        }

        // Compute payload hash
        let payload_hash = if let Some(body) = request.body() {
            if let Some(bytes) = body.as_bytes() {
                sha256_hex(bytes)
            } else {
                sha256_hex(b"")
            }
        } else {
            sha256_hex(b"")
        };

        request
            .headers_mut()
            .insert("x-amz-content-sha256", payload_hash.parse().unwrap());

        // Build canonical request
        let (canonical_request, signed_headers) = build_canonical_request(request, &payload_hash)?;

        // Create string to sign
        let credential_scope =
            format!("{date_stamp}/{}/{}/aws4_request", self.region, self.service);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{}",
            sha256_hex(canonical_request.as_bytes())
        );

        // Get signing key and compute signature
        let signing_key = self.get_signing_key(&date_stamp).await;
        let signature = hmac_sha256_hex(&signing_key, string_to_sign.as_bytes());

        // Build Authorization header
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            self.credentials.access_key_id, credential_scope, signed_headers, signature
        );

        request.headers_mut().insert(
            http::header::AUTHORIZATION,
            authorization.parse().map_err(|e| {
                Error::new(ErrorKind::DataInvalid, "Invalid authorization header").with_source(e)
            })?,
        );

        Ok(())
    }
}

#[async_trait]
impl Authenticator for SigV4Authenticator {
    async fn authenticate(&self, request: &mut Request) -> Result<()> {
        self.sign_request(request, Utc::now()).await
    }

    async fn invalidate(&self) -> Result<()> {
        // Clear the signing key cache
        let mut cache = self.signing_key_cache.write().await;
        *cache = None;
        Ok(())
    }

    fn scheme_name(&self) -> &'static str {
        "sigv4"
    }
}

// =============================================================================
// Cryptographic helper functions
// =============================================================================

/// Compute SHA-256 hash and return as lowercase hex string.
fn sha256_hex(data: &[u8]) -> String {
    use std::fmt::Write;

    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();

    let mut hex = String::with_capacity(64);
    for byte in result {
        write!(&mut hex, "{byte:02x}").unwrap();
    }
    hex
}

/// Compute HMAC-SHA256 and return as bytes.
fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    use hmac::{Hmac, Mac};
    type HmacSha256 = Hmac<Sha256>;

    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

/// Compute HMAC-SHA256 and return as lowercase hex string.
fn hmac_sha256_hex(key: &[u8], data: &[u8]) -> String {
    use std::fmt::Write;

    let result = hmac_sha256(key, data);
    let mut hex = String::with_capacity(64);
    for byte in result {
        write!(&mut hex, "{byte:02x}").unwrap();
    }
    hex
}

/// Derive the signing key for AWS SigV4.
fn derive_signing_key(secret_key: &str, date_stamp: &str, region: &str, service: &str) -> [u8; 32] {
    let k_date = hmac_sha256(
        format!("AWS4{secret_key}").as_bytes(),
        date_stamp.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

/// URL-encode a string for use in canonical requests.
fn uri_encode(input: &str, encode_slash: bool) -> String {
    let mut encoded = String::with_capacity(input.len() * 3);
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            b'/' if !encode_slash => {
                encoded.push('/');
            }
            _ => {
                encoded.push_str(&format!("%{byte:02X}"));
            }
        }
    }
    encoded
}

/// Build the canonical request string and return (canonical_request, signed_headers).
fn build_canonical_request(request: &Request, payload_hash: &str) -> Result<(String, String)> {
    let method = request.method().as_str();

    // Canonical URI (path)
    let path = request.url().path();
    let canonical_uri = if path.is_empty() { "/" } else { path };
    let canonical_uri = uri_encode(canonical_uri, false);

    // Canonical query string
    let canonical_query_string = build_canonical_query_string(request.url().query());

    // Canonical headers and signed headers
    let (canonical_headers, signed_headers) = build_canonical_headers(request)?;

    let canonical_request = format!(
        "{method}\n{canonical_uri}\n{canonical_query_string}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    );

    Ok((canonical_request, signed_headers))
}

/// Build canonical query string from URL query parameters.
fn build_canonical_query_string(query: Option<&str>) -> String {
    let Some(query) = query else {
        return String::new();
    };

    let mut params: Vec<(String, String)> = query
        .split('&')
        .filter(|s| !s.is_empty())
        .map(|param| {
            let mut parts = param.splitn(2, '=');
            let key = parts.next().unwrap_or("");
            let value = parts.next().unwrap_or("");
            (uri_encode(key, true), uri_encode(value, true))
        })
        .collect();

    params.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

    params
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// Build canonical headers and signed headers list.
fn build_canonical_headers(request: &Request) -> Result<(String, String)> {
    let mut headers: Vec<(String, String)> = request
        .headers()
        .iter()
        .map(|(name, value)| {
            let name = name.as_str().to_lowercase();
            let value = value.to_str().unwrap_or("").trim().to_string();
            (name, value)
        })
        .collect();

    // Sort by header name
    headers.sort_by(|a, b| a.0.cmp(&b.0));

    // Build canonical headers string
    let canonical_headers: String = headers
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect();

    // Build signed headers list
    let signed_headers: String = headers
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(";");

    Ok((canonical_headers, signed_headers))
}

// Import SHA-256 from the sha2 crate
use sha2::{Digest, Sha256};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_credentials_debug_redacts_secrets() {
        let creds = SigV4Credentials::new(
            "AKIAIOSFODNN7EXAMPLE",
            "my-secret-key",
            Some("my-token-value"),
        );
        let debug_str = format!("{creds:?}");
        assert!(debug_str.contains("AKIAIOSFODNN7EXAMPLE"));
        assert!(debug_str.contains("[REDACTED]"));
        // Verify actual secrets are not in the output
        assert!(!debug_str.contains("my-secret-key"));
        assert!(!debug_str.contains("my-token-value"));
    }

    #[test]
    fn test_sha256_hex() {
        // Test vector from AWS documentation
        let hash = sha256_hex(b"");
        assert_eq!(
            hash,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );

        let hash = sha256_hex(b"hello");
        assert_eq!(
            hash,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn test_derive_signing_key() {
        // AWS SigV4 test vector
        let key = derive_signing_key(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20150830",
            "us-east-1",
            "iam",
        );

        // The signing key should be 32 bytes
        assert_eq!(key.len(), 32);

        // Verify against known test vector (from AWS docs)
        let expected_hex = "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9";
        let actual_hex = key.iter().map(|b| format!("{b:02x}")).collect::<String>();
        assert_eq!(actual_hex, expected_hex);
    }

    #[test]
    fn test_uri_encode() {
        assert_eq!(uri_encode("hello", true), "hello");
        assert_eq!(uri_encode("hello world", true), "hello%20world");
        assert_eq!(uri_encode("hello/world", true), "hello%2Fworld");
        assert_eq!(uri_encode("hello/world", false), "hello/world");
        assert_eq!(uri_encode("a=b&c=d", true), "a%3Db%26c%3Dd");
    }

    #[test]
    fn test_canonical_query_string() {
        // Empty query
        assert_eq!(build_canonical_query_string(None), "");
        assert_eq!(build_canonical_query_string(Some("")), "");

        // Single parameter
        assert_eq!(build_canonical_query_string(Some("foo=bar")), "foo=bar");

        // Multiple parameters (should be sorted)
        assert_eq!(build_canonical_query_string(Some("b=2&a=1")), "a=1&b=2");

        // Parameters with encoding needed
        assert_eq!(
            build_canonical_query_string(Some("foo=bar baz")),
            "foo=bar%20baz"
        );
    }

    #[tokio::test]
    async fn test_sigv4_authenticator_adds_headers() {
        let creds = SigV4Credentials::new("AKIAIOSFODNN7EXAMPLE", "secret-key", None::<String>);
        let auth = SigV4Authenticator::new(creds, "us-east-1", "s3");

        let client = reqwest::Client::new();
        let mut request = client
            .get("http://example.com/test")
            .build()
            .expect("Failed to build request");

        auth.authenticate(&mut request).await.unwrap();

        // Check that required headers were added
        assert!(request.headers().contains_key("x-amz-date"));
        assert!(request.headers().contains_key("x-amz-content-sha256"));
        assert!(request.headers().contains_key("host"));
        assert!(request.headers().contains_key(http::header::AUTHORIZATION));

        // Check Authorization header format
        let auth_header = request
            .headers()
            .get(http::header::AUTHORIZATION)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(auth_header.starts_with("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/"));
        assert!(auth_header.contains("SignedHeaders="));
        assert!(auth_header.contains("Signature="));
    }

    #[tokio::test]
    async fn test_sigv4_with_session_token() {
        let creds =
            SigV4Credentials::new("AKIAIOSFODNN7EXAMPLE", "secret-key", Some("session-token"));
        let auth = SigV4Authenticator::new(creds, "us-east-1", "s3");

        let client = reqwest::Client::new();
        let mut request = client
            .get("http://example.com/test")
            .build()
            .expect("Failed to build request");

        auth.authenticate(&mut request).await.unwrap();

        // Check that session token header was added
        let token_header = request.headers().get("x-amz-security-token");
        assert!(token_header.is_some());
        assert_eq!(token_header.unwrap().to_str().unwrap(), "session-token");
    }

    #[tokio::test]
    async fn test_sigv4_signing_key_caching() {
        let creds = SigV4Credentials::new("AKIAIOSFODNN7EXAMPLE", "secret-key", None::<String>);
        let auth = SigV4Authenticator::new(creds, "us-east-1", "s3");

        // Cache should be empty initially
        assert!(auth.signing_key_cache.read().await.is_none());

        let client = reqwest::Client::new();
        let mut request = client
            .get("http://example.com/test")
            .build()
            .expect("Failed to build request");

        auth.authenticate(&mut request).await.unwrap();

        // Cache should be populated after signing
        assert!(auth.signing_key_cache.read().await.is_some());
    }

    #[tokio::test]
    async fn test_sigv4_invalidate_clears_cache() {
        let creds = SigV4Credentials::new("AKIAIOSFODNN7EXAMPLE", "secret-key", None::<String>);
        let auth = SigV4Authenticator::new(creds, "us-east-1", "s3");

        let client = reqwest::Client::new();
        let mut request = client
            .get("http://example.com/test")
            .build()
            .expect("Failed to build request");

        auth.authenticate(&mut request).await.unwrap();
        assert!(auth.signing_key_cache.read().await.is_some());

        auth.invalidate().await.unwrap();
        assert!(auth.signing_key_cache.read().await.is_none());
    }

    #[test]
    fn test_sigv4_scheme_name() {
        let creds = SigV4Credentials::new("key", "secret", None::<String>);
        let auth = SigV4Authenticator::new(creds, "us-east-1", "s3");
        assert_eq!(auth.scheme_name(), "sigv4");
    }

    #[test]
    fn test_sigv4_for_s3tables() {
        let creds = SigV4Credentials::new("key", "secret", None::<String>);
        let auth = SigV4Authenticator::for_s3tables(creds, "us-east-1");
        assert_eq!(auth.service, "s3tables");
    }

    #[test]
    fn test_sigv4_for_s3() {
        let creds = SigV4Credentials::new("key", "secret", None::<String>);
        let auth = SigV4Authenticator::for_s3(creds, "us-east-1");
        assert_eq!(auth.service, "s3");
    }

    #[tokio::test]
    async fn test_sigv4_with_query_parameters() {
        let creds = SigV4Credentials::new("AKIAIOSFODNN7EXAMPLE", "secret-key", None::<String>);
        let auth = SigV4Authenticator::new(creds, "us-east-1", "s3");

        let client = reqwest::Client::new();
        let mut request = client
            .get("http://example.com/test?foo=bar&baz=qux")
            .build()
            .expect("Failed to build request");

        // Should not panic and should add auth headers
        auth.authenticate(&mut request).await.unwrap();
        assert!(request.headers().contains_key(http::header::AUTHORIZATION));
    }

    #[tokio::test]
    async fn test_sigv4_with_post_body() {
        let creds = SigV4Credentials::new("AKIAIOSFODNN7EXAMPLE", "secret-key", None::<String>);
        let auth = SigV4Authenticator::new(creds, "us-east-1", "s3");

        let client = reqwest::Client::new();
        let mut request = client
            .post("http://example.com/test")
            .body(r#"{"key": "value"}"#)
            .build()
            .expect("Failed to build request");

        // Should hash the body and include in signature
        auth.authenticate(&mut request).await.unwrap();

        let content_hash = request
            .headers()
            .get("x-amz-content-sha256")
            .unwrap()
            .to_str()
            .unwrap();
        // The hash should not be the empty string hash
        assert_ne!(
            content_hash,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[tokio::test]
    async fn test_sigv4_with_custom_port() {
        let creds = SigV4Credentials::new("AKIAIOSFODNN7EXAMPLE", "secret-key", None::<String>);
        let auth = SigV4Authenticator::new(creds, "us-east-1", "s3");

        let client = reqwest::Client::new();
        let mut request = client
            .get("http://localhost:9000/test")
            .build()
            .expect("Failed to build request");

        auth.authenticate(&mut request).await.unwrap();

        // Host header should include port
        let host_header = request.headers().get("host").unwrap().to_str().unwrap();
        assert_eq!(host_header, "localhost:9000");
    }
}
