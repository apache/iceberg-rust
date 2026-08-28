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

//! AWS SigV4 request signing for the REST catalog.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use chrono::{DateTime, Utc};
use iceberg::{Error, ErrorKind, Result};
use sha2::{Digest, Sha256};

use super::{AuthManager, AuthSession};
use crate::client::HttpClient;
use crate::request::{HttpRequest, HttpRequestBody};

/// Hex SHA-256 of the empty string.
const EMPTY_BODY_HEX_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// How the payload hash is encoded in the `x-amz-content-sha256` header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PayloadHashMode {
    /// Iceberg Java's style: a base64 header when there is a body, hex when
    /// there is none, and hex in the canonical request. The base64 comes from
    /// how Java configures the AWS SDK, not from SigV4, so a verifier that
    /// trusts the header instead of hashing the body rejects it.
    IcebergRest,
    /// Standard SigV4: hex everywhere.
    StandardAws,
}

fn hex_sha256(data: &[u8]) -> String {
    encode_hex(&Sha256::digest(data))
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn base64_encode(bytes: &[u8]) -> String {
    base64::engine::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes)
}

/// The `x-amz-content-sha256` value; `None` is no body at all.
fn content_sha256_header(body: Option<&[u8]>, mode: PayloadHashMode) -> String {
    match mode {
        PayloadHashMode::StandardAws => hex_sha256(body.unwrap_or_default()),
        PayloadHashMode::IcebergRest => match body {
            None => EMPTY_BODY_HEX_SHA256.to_string(),
            Some(body) => base64_encode(&Sha256::digest(body)),
        },
    }
}

/// Signs REST catalog requests the way Iceberg Java's `RESTSigV4AuthSession`
/// does. Carries no credentials, so one signer serves every session.
#[derive(Clone)]
pub struct SigV4Signer {
    region: String,
    service: String,
    mode: PayloadHashMode,
}

impl SigV4Signer {
    /// Creates a new SigV4 signer.
    pub fn new(
        region: impl Into<String>,
        service: impl Into<String>,
        mode: PayloadHashMode,
    ) -> Self {
        Self {
            region: region.into(),
            service: service.into(),
            mode,
        }
    }

    /// Signs `request` in place. An existing `Authorization` moves to
    /// `Original-Authorization`, and a caller's `x-amz-date`,
    /// `x-amz-content-sha256` or `x-amz-security-token` that the signer
    /// overwrites moves to `Original-<name>` (Java keeps a caller's content
    /// hash when there is a body). Userinfo leaves the URL, and a `+` in the
    /// query becomes `%20`, so write a literal plus as `%2B`.
    ///
    /// Uses `credentials` as given and never refreshes them: resolve temporary
    /// ones from their provider before each call.
    ///
    /// Fails on a streaming body or a non-UTF-8 header, which cannot be
    /// canonicalized faithfully.
    ///
    /// Send the result through a client that does not follow redirects: a
    /// redirect replays the signature, and across hosts reqwest drops
    /// `Authorization` but keeps `Original-Authorization`.
    ///
    /// `aws_sigv4` traces requests without redacting `Original-Authorization`.
    /// A `tracing` subscriber is muted for the call, but the `log` bridge (no
    /// subscriber, or `log-always`) still forwards those events, so keep
    /// `aws_sigv4` below trace level there.
    pub fn sign(
        &self,
        request: &mut HttpRequest,
        credentials: &aws_credential_types::Credentials,
    ) -> Result<()> {
        self.sign_at(request, credentials, Utc::now())
    }

    fn sign_at(
        &self,
        request: &mut HttpRequest,
        credentials: &aws_credential_types::Credentials,
        now: DateTime<Utc>,
    ) -> Result<()> {
        use aws_sigv4::http_request::{SignableBody, SignableRequest, sign};
        use aws_sigv4::sign::v4;
        use tracing::level_filters::LevelFilter;
        use tracing::subscriber::NoSubscriber;

        let body = signable_body(request)?;
        let content_header = content_sha256_header(body.as_deref(), self.mode);

        convert_headers(request);

        // Relocated after signing, so the `Original-` copy is not signed.
        let displaced_content_hash: Vec<_> = request
            .headers()
            .get_all(CONTENT_SHA256)
            .iter()
            .filter(|v| v.as_bytes() != content_header.as_bytes())
            .cloned()
            .collect();
        let content_value = content_header.parse().map_err(|e| {
            Error::new(ErrorKind::Unexpected, "invalid x-amz-content-sha256 value").with_source(e)
        })?;
        request.headers_mut().insert(CONTENT_SHA256, content_value);

        rewrite_url_for_signing(request);

        let identity = credentials.clone().into();
        let params = v4::SigningParams::builder()
            .identity(&identity)
            .region(&self.region)
            .name(&self.service)
            .time(now.into())
            .settings(signing_settings())
            .build()
            .map_err(|e| {
                Error::new(ErrorKind::Unexpected, "failed to build SigV4 params").with_source(e)
            })?
            .into();

        let headers = signable_headers(request)?;
        let signable = SignableRequest::new(
            request.method().as_str(),
            request.url_str(),
            headers.into_iter(),
            SignableBody::Bytes(body.as_deref().unwrap_or_default()),
        )
        .map_err(|e| {
            Error::new(ErrorKind::DataInvalid, "request is not signable").with_source(e)
        })?;

        // `aws_sigv4` traces the request, `Original-Authorization` included.
        // Mute it only when a subscriber could record that (the max level is
        // `OFF` until one is registered): `with_default` marks tracing as in
        // use for good, which turns off its `log` fallback process-wide.
        let signed = if LevelFilter::current() == LevelFilter::TRACE {
            tracing::subscriber::with_default(NoSubscriber::default(), || sign(signable, &params))
        } else {
            sign(signable, &params)
        };
        let (instructions, _signature) = signed
            .map_err(|e| Error::new(ErrorKind::Unexpected, "SigV4 signing failed").with_source(e))?
            .into_parts();

        update_request_headers(request, instructions, displaced_content_hash)
    }
}

/// The body to sign; as in Java, an absent body and an empty one differ.
fn signable_body(request: &HttpRequest) -> Result<Option<Vec<u8>>> {
    match request.body() {
        HttpRequestBody::Empty => Ok(None),
        HttpRequestBody::Buffered(bytes) => Ok(Some(bytes.to_vec())),
        HttpRequestBody::Streaming => Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "cannot sign a streaming request body",
        )),
    }
}

/// The headers to sign. A non-UTF-8 one is an error: skipping it would send
/// it unsigned.
fn signable_headers(request: &HttpRequest) -> Result<Vec<(&str, &str)>> {
    request
        .headers()
        .iter()
        .map(|(n, v)| {
            let v = v.to_str().map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("cannot sign non-UTF-8 header value for `{n}`"),
                )
                .with_source(e)
            })?;
            Ok((n.as_str(), v))
        })
        .collect()
}

/// Drops userinfo, which the wire `Host` never carries, and rewrites `+` in the
/// query as `%20`: a space to AWS and Java either way, but unambiguous to any
/// verifier.
fn rewrite_url_for_signing(request: &mut HttpRequest) {
    if !request.url().username().is_empty() || request.url().password().is_some() {
        let url = request.url_mut();
        let _ = url.set_username("");
        let _ = url.set_password(None);
    }
    if let Some(query) = request.url().query().filter(|q| q.contains('+')) {
        let unambiguous = query.replace('+', "%20");
        request.url_mut().set_query(Some(&unambiguous));
    }
}

/// `Aws4Signer`'s settings: normalized double-encoded path, and Java's ignore
/// list.
fn signing_settings() -> aws_sigv4::http_request::SigningSettings {
    use aws_sigv4::http_request::{
        PayloadChecksumKind, PercentEncodingMode, SigningSettings, UriPathNormalizationMode,
    };

    let mut settings = SigningSettings::default();
    settings.percent_encoding_mode = PercentEncodingMode::Double;
    settings.uri_path_normalization_mode = UriPathNormalizationMode::Enabled;
    // We set the header ourselves: base64 for IcebergRest.
    settings.payload_checksum_kind = PayloadChecksumKind::NoHeader;
    let mut excluded = settings.excluded_headers.take().unwrap_or_default();
    excluded.extend([
        // Java's list, spelled out rather than left to the crate's defaults.
        "connection".into(),
        "expect".into(),
        "transfer-encoding".into(),
        "user-agent".into(),
        "x-amzn-trace-id".into(),
        // Not Java's, but proxies append to it too.
        "x-forwarded-for".into(),
        // Relocation appends to these after signing.
        "original-x-amz-date".into(),
        "original-x-amz-content-sha256".into(),
        "original-x-amz-security-token".into(),
    ]);
    settings.excluded_headers = Some(excluded);
    settings
}

/// Java's `convertHeaders`: moves `Authorization` aside before signing, so the
/// moved copy is signed.
fn convert_headers(request: &mut HttpRequest) {
    let displaced: Vec<_> = request
        .headers()
        .get_all(reqwest::header::AUTHORIZATION)
        .iter()
        .cloned()
        .collect();
    if displaced.is_empty() {
        return;
    }
    request.headers_mut().remove(reqwest::header::AUTHORIZATION);
    for mut value in displaced {
        value.set_sensitive(true);
        request.headers_mut().append(RELOCATED_AUTHORIZATION, value);
    }
}

/// Java's `updateRequestHeaders`: installs the signed headers, moving
/// conflicting caller values aside.
fn update_request_headers(
    request: &mut HttpRequest,
    instructions: aws_sigv4::http_request::SigningInstructions,
    displaced_content_hash: Vec<reqwest::header::HeaderValue>,
) -> Result<()> {
    let (signed_headers, _params) = instructions.into_parts();
    let h = request.headers_mut();
    for mut value in displaced_content_hash {
        // The original may carry a credential.
        value.set_sensitive(true);
        h.append(RELOCATED_CONTENT_SHA256, value);
    }
    for header in signed_headers {
        let name: reqwest::header::HeaderName = header.name().parse().map_err(|e| {
            Error::new(ErrorKind::Unexpected, "invalid signed header name").with_source(e)
        })?;
        if let Some(relocated) = relocated_name(name.as_str()) {
            relocate_conflicting(h, name.as_str(), header.value(), relocated);
        }
        let mut value: reqwest::header::HeaderValue = header.value().parse().map_err(|e| {
            Error::new(ErrorKind::Unexpected, "invalid signed header value").with_source(e)
        })?;
        if name == reqwest::header::AUTHORIZATION || name == SECURITY_TOKEN {
            value.set_sensitive(true);
        }
        h.insert(name, value);
    }
    Ok(())
}

const CONTENT_SHA256: reqwest::header::HeaderName =
    reqwest::header::HeaderName::from_static("x-amz-content-sha256");
const AMZ_DATE: reqwest::header::HeaderName =
    reqwest::header::HeaderName::from_static("x-amz-date");
const SECURITY_TOKEN: reqwest::header::HeaderName =
    reqwest::header::HeaderName::from_static("x-amz-security-token");

const RELOCATED_AUTHORIZATION: reqwest::header::HeaderName =
    reqwest::header::HeaderName::from_static("original-authorization");
const RELOCATED_AMZ_DATE: reqwest::header::HeaderName =
    reqwest::header::HeaderName::from_static("original-x-amz-date");
const RELOCATED_CONTENT_SHA256: reqwest::header::HeaderName =
    reqwest::header::HeaderName::from_static("original-x-amz-content-sha256");
const RELOCATED_SECURITY_TOKEN: reqwest::header::HeaderName =
    reqwest::header::HeaderName::from_static("original-x-amz-security-token");

/// The `Original-<name>` counterpart of a header the signer generates.
fn relocated_name(name: &str) -> Option<reqwest::header::HeaderName> {
    match name {
        n if n == AMZ_DATE => Some(RELOCATED_AMZ_DATE),
        n if n == CONTENT_SHA256 => Some(RELOCATED_CONTENT_SHA256),
        n if n == SECURITY_TOKEN => Some(RELOCATED_SECURITY_TOKEN),
        _ => None,
    }
}

/// Moves `name`'s values that differ from `signed` to `relocated`.
fn relocate_conflicting(
    headers: &mut reqwest::header::HeaderMap,
    name: &str,
    signed: &str,
    relocated: reqwest::header::HeaderName,
) {
    let conflicting: Vec<_> = headers
        .get_all(name)
        .iter()
        .filter(|value| value.as_bytes() != signed.as_bytes())
        .cloned()
        .collect();
    for mut value in conflicting {
        // It may carry a credential, e.g. a session token.
        value.set_sensitive(true);
        headers.append(relocated.clone(), value);
    }
}

impl std::fmt::Debug for SigV4Signer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SigV4Signer")
            .field("region", &self.region)
            .field("service", &self.service)
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

/// Property naming Iceberg Java's `AwsProperties` uses for REST SigV4.
pub const REST_CATALOG_PROP_SIGNING_REGION: &str = "rest.signing-region";
/// SigV4 signing name; defaults to [`SIGNING_NAME_DEFAULT`].
pub const REST_CATALOG_PROP_SIGNING_NAME: &str = "rest.signing-name";
/// Static SigV4 access key id. When absent, credentials come from the AWS
/// default provider chain.
pub const REST_CATALOG_PROP_ACCESS_KEY_ID: &str = "rest.access-key-id";
/// Static SigV4 secret access key (see [`REST_CATALOG_PROP_ACCESS_KEY_ID`]).
pub const REST_CATALOG_PROP_SECRET_ACCESS_KEY: &str = "rest.secret-access-key";
/// Static SigV4 session token (see [`REST_CATALOG_PROP_ACCESS_KEY_ID`]).
pub const REST_CATALOG_PROP_SESSION_TOKEN: &str = "rest.session-token";

/// Java's `REST_CATALOG_PROP_SIGNING_NAME_DEFAULT`: API Gateway and Lambda.
pub const SIGNING_NAME_DEFAULT: &str = "execute-api";

/// Trims a property, treating blank as absent.
fn non_blank(props: &HashMap<String, String>, key: &str) -> Option<String> {
    props
        .get(key)
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .map(str::to_string)
}

/// Builds a signer and credentials from catalog properties, using the names
/// Java's `AwsProperties` does. Only static credentials come from properties;
/// for anything else pass a provider to [`SigV4AuthManager::new`].
async fn from_props(
    props: &HashMap<String, String>,
) -> Result<(SigV4Signer, SharedCredentialsProvider)> {
    let region = non_blank(props, REST_CATALOG_PROP_SIGNING_REGION).ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("'{REST_CATALOG_PROP_SIGNING_REGION}' is required for SigV4 signing"),
        )
    })?;
    let name = non_blank(props, REST_CATALOG_PROP_SIGNING_NAME)
        .unwrap_or_else(|| SIGNING_NAME_DEFAULT.into());

    let credentials = credentials_from_props(props).await?;
    Ok((
        SigV4Signer::new(region, name, PayloadHashMode::IcebergRest),
        credentials,
    ))
}

/// Java branches on the access key id alone, so a lone secret is a
/// misconfiguration rather than a silent fall through to the default chain.
async fn credentials_from_props(
    props: &HashMap<String, String>,
) -> Result<SharedCredentialsProvider> {
    let Some(access_key_id) = non_blank(props, REST_CATALOG_PROP_ACCESS_KEY_ID) else {
        if non_blank(props, REST_CATALOG_PROP_SECRET_ACCESS_KEY).is_some() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "'{REST_CATALOG_PROP_SECRET_ACCESS_KEY}' is set without '{REST_CATALOG_PROP_ACCESS_KEY_ID}'"
                ),
            ));
        }
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "no SigV4 credentials: set '{REST_CATALOG_PROP_ACCESS_KEY_ID}' and \
                 '{REST_CATALOG_PROP_SECRET_ACCESS_KEY}', or build the catalog with a \
                 `SigV4AuthManager` carrying your own credentials provider"
            ),
        ));
    };
    let secret_access_key = non_blank(props, REST_CATALOG_PROP_SECRET_ACCESS_KEY).ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("'{REST_CATALOG_PROP_ACCESS_KEY_ID}' is set without '{REST_CATALOG_PROP_SECRET_ACCESS_KEY}'"),
        )
    })?;
    Ok(SharedCredentialsProvider::new(
        aws_credential_types::Credentials::new(
            access_key_id,
            secret_access_key,
            non_blank(props, REST_CATALOG_PROP_SESSION_TOKEN),
            None,
            "iceberg-rest-properties",
        ),
    ))
}

/// [`AuthManager`] that SigV4-signs every request on top of a delegate whose
/// own `Authorization` is relocated and signed over.
///
/// Mirrors Java's `RESTSigV4AuthManager`, which holds one signer for every
/// session and leaves credentials to the session.
#[derive(Debug)]
pub struct SigV4AuthManager {
    delegate: Arc<dyn AuthManager>,
    signer: SigV4Signer,
    credentials: SharedCredentialsProvider,
    /// Set when both came from catalog properties, so
    /// [`AuthManager::catalog_session`] rebuilds them from the merged ones.
    from_config: bool,
}

impl SigV4AuthManager {
    /// Signs with `signer`, taking credentials from `credentials` before each
    /// request. Both are kept as-is, whatever the catalog properties say.
    pub fn new(
        delegate: Arc<dyn AuthManager>,
        signer: SigV4Signer,
        credentials: SharedCredentialsProvider,
    ) -> Self {
        Self {
            delegate,
            signer,
            credentials,
            from_config: false,
        }
    }

    /// Signs as the catalog properties describe, with the static credentials
    /// they carry. The merged `/v1/config` properties rebuild both, as Java's
    /// `catalogSession` does.
    pub async fn from_properties(
        delegate: Arc<dyn AuthManager>,
        props: &HashMap<String, String>,
    ) -> Result<Self> {
        let (signer, credentials) = from_props(props).await?;
        Ok(Self {
            delegate,
            signer,
            credentials,
            from_config: true,
        })
    }
}

#[async_trait]
impl AuthManager for SigV4AuthManager {
    async fn init_session(
        &self,
        client: &HttpClient,
        props: &HashMap<String, String>,
    ) -> Result<Box<dyn AuthSession>> {
        Ok(Box::new(SigV4Session {
            delegate: Arc::from(self.delegate.init_session(client, props).await?),
            signer: self.signer.clone(),
            credentials: self.credentials.clone(),
        }))
    }

    async fn catalog_session(
        &self,
        client: &HttpClient,
        props: &HashMap<String, String>,
    ) -> Result<Arc<dyn AuthSession>> {
        let (signer, credentials) = if self.from_config {
            from_props(props).await?
        } else {
            (self.signer.clone(), self.credentials.clone())
        };
        Ok(Arc::new(SigV4Session {
            delegate: self.delegate.catalog_session(client, props).await?,
            signer,
            credentials,
        }))
    }
}

/// [`AuthSession`] applying the delegate's auth, then SigV4-signing.
#[derive(Debug)]
struct SigV4Session {
    delegate: Arc<dyn AuthSession>,
    signer: SigV4Signer,
    credentials: SharedCredentialsProvider,
}

#[async_trait]
impl AuthSession for SigV4Session {
    async fn authenticate(&self, request: &mut HttpRequest) -> Result<()> {
        self.delegate.authenticate(request).await?;
        // Java resolves per request too, leaving any caching to the provider.
        let credentials = self.credentials.provide_credentials().await.map_err(|e| {
            Error::new(ErrorKind::Unexpected, "failed to resolve AWS credentials").with_source(e)
        })?;
        self.signer.sign(request, &credentials)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::HttpRequest;

    const EMPTY_HEX: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    #[test]
    fn signing_rewrites_an_ambiguous_plus_out_of_the_query() {
        use chrono::TimeZone;

        // reqwest writes a space as `+`; signing makes it `%20`.
        let mut request = HttpRequest::new(
            reqwest::Client::new()
                .get("https://rest.example.com/v1/namespaces")
                .query(&[("parent", "my ns")])
                .build()
                .unwrap(),
        );
        assert!(request.url().query().unwrap().contains("my+ns"));

        let signer = SigV4Signer::new("us-east-1", "execute-api", PayloadHashMode::StandardAws);
        let now = Utc.with_ymd_and_hms(2015, 8, 30, 12, 36, 0).unwrap();
        signer
            .sign_at(&mut request, &test_credentials(), now)
            .unwrap();

        let query = request.url().query().unwrap();
        assert!(!query.contains('+'), "{query}");
        assert!(query.contains("my%20ns"), "{query}");
        assert_signature_is(
            &request,
            "b7bb5a323a1ce0ace18454171084deef2dac44c933c3949771fc70179d3cce2b",
        );
    }

    #[test]
    fn content_sha256_header_iceberg_mode() {
        let v = content_sha256_header(Some(b"hello"), PayloadHashMode::IcebergRest);
        assert_eq!(v, "LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=");
        let e = content_sha256_header(None, PayloadHashMode::IcebergRest);
        assert_eq!(e, EMPTY_HEX);
    }

    /// As in Java, an empty body is hashed, unlike an absent one.
    #[test]
    fn content_sha256_header_separates_an_empty_body_from_an_absent_one() {
        let empty = content_sha256_header(Some(b""), PayloadHashMode::IcebergRest);
        assert_eq!(empty, "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=");
        assert_ne!(
            empty,
            content_sha256_header(None, PayloadHashMode::IcebergRest)
        );
    }

    /// The same, through `sign_at`.
    #[test]
    fn signing_separates_an_empty_body_from_an_absent_one() {
        use chrono::TimeZone;

        let signer = test_signer(PayloadHashMode::IcebergRest);
        let now = Utc.with_ymd_and_hms(2015, 8, 30, 12, 36, 0).unwrap();
        let hash_of = |builder: reqwest::RequestBuilder| {
            let mut req = HttpRequest::new(builder.build().unwrap());
            signer.sign_at(&mut req, &test_credentials(), now).unwrap();
            req.headers()
                .get("x-amz-content-sha256")
                .unwrap()
                .to_str()
                .unwrap()
                .to_string()
        };

        let client = reqwest::Client::new();
        let url = "https://rest.example.com/v1/namespaces";
        assert_eq!(
            hash_of(client.post(url).body("")),
            "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="
        );
        assert_eq!(hash_of(client.post(url)), EMPTY_HEX);
    }

    #[test]
    fn content_sha256_header_standard_mode() {
        let v = content_sha256_header(Some(b"hello"), PayloadHashMode::StandardAws);
        assert_eq!(
            v,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    /// Pins a signature this crate produced; `signatures_match_iceberg_java`
    /// checks against Java.
    fn assert_signature_is(req: &HttpRequest, expected: &str) {
        let auth = req
            .headers()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(auth.ends_with(&format!("Signature={expected}")), "{auth}");
    }

    fn test_signer(mode: PayloadHashMode) -> SigV4Signer {
        SigV4Signer::new("us-east-1", "execute-api", mode)
    }

    fn test_credentials() -> aws_credential_types::Credentials {
        aws_credential_types::Credentials::new("ak", "sk", None::<String>, None, "test")
    }

    /// Records every event field.
    #[derive(Clone, Default)]
    struct CapturedLog(Arc<Mutex<String>>);

    impl tracing::field::Visit for CapturedLog {
        fn record_debug(&mut self, _: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0.lock().unwrap().push_str(&format!("{value:?}"));
        }
    }

    impl tracing::Subscriber for CapturedLog {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::Id {
            tracing::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::Id, _: &tracing::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            event.record(&mut self.clone());
        }
        fn enter(&self, _: &tracing::Id) {}
        fn exit(&self, _: &tracing::Id) {}
    }

    /// `aws_sigv4` does not redact `Original-Authorization` itself.
    #[test]
    fn signing_does_not_trace_a_relocated_bearer_token() {
        use chrono::TimeZone;

        const TOKEN: &str = "Bearer topsecretdelegatetoken";
        let signer = test_signer(PayloadHashMode::IcebergRest);
        let mut req = HttpRequest::new(
            reqwest::Client::new()
                .get("https://rest.example.com/v1/config")
                .header(reqwest::header::AUTHORIZATION, TOKEN)
                .build()
                .unwrap(),
        );

        let log = CapturedLog::default();
        tracing::subscriber::with_default(log.clone(), || {
            // `sign_at` mutes only at this level, which is what is under test.
            assert_eq!(
                tracing::level_filters::LevelFilter::current(),
                tracing::level_filters::LevelFilter::TRACE
            );
            signer
                .sign_at(
                    &mut req,
                    &test_credentials(),
                    Utc.with_ymd_and_hms(2015, 8, 30, 12, 36, 0).unwrap(),
                )
                .unwrap();
            // Proves the capture works.
            tracing::trace!(canary = "subscriber-is-live");
        });

        let captured = log.0.lock().unwrap().clone();
        assert!(captured.contains("subscriber-is-live"), "captured nothing");
        assert!(!captured.contains(TOKEN), "{captured}");
        // The token still travels, it is just not logged.
        assert_eq!(req.headers().get(RELOCATED_AUTHORIZATION).unwrap(), TOKEN);
    }

    #[test]
    fn a_non_utf8_header_value_is_rejected_rather_than_left_unsigned() {
        use chrono::TimeZone;

        let signer = test_signer(PayloadHashMode::IcebergRest);
        let mut req = HttpRequest::new(
            reqwest::Client::new()
                .get("https://rest.example.com/v1/config")
                .header(
                    "x-amz-meta-tenant",
                    reqwest::header::HeaderValue::from_bytes(b"acme\xfa").unwrap(),
                )
                .build()
                .unwrap(),
        );

        let err = signer
            .sign_at(
                &mut req,
                &test_credentials(),
                Utc.with_ymd_and_hms(2015, 8, 30, 12, 36, 0).unwrap(),
            )
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("x-amz-meta-tenant"), "{err}");
    }

    /// Relocation appends to a caller's `Original-x-amz-*` after signing, so it
    /// must stay unsigned.
    #[test]
    fn a_caller_supplied_relocation_header_is_not_signed() {
        use chrono::TimeZone;

        let signer = test_signer(PayloadHashMode::StandardAws);
        let mut req = HttpRequest::new(
            reqwest::Client::new()
                .get("https://rest.example.com/v1/config")
                .header("x-amz-content-sha256", "caller-hash")
                .header("original-x-amz-content-sha256", "previous")
                .build()
                .unwrap(),
        );

        signer
            .sign_at(
                &mut req,
                &test_credentials(),
                Utc.with_ymd_and_hms(2015, 8, 30, 12, 36, 0).unwrap(),
            )
            .unwrap();

        let auth = req
            .headers()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        let signed = auth
            .split("SignedHeaders=")
            .nth(1)
            .unwrap()
            .split(',')
            .next()
            .unwrap();
        assert!(
            !signed
                .split(';')
                .any(|h| h == "original-x-amz-content-sha256"),
            "{signed}"
        );
        // Both values still travel, they are just outside the signature.
        let relocated: Vec<_> = req
            .headers()
            .get_all("original-x-amz-content-sha256")
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect();
        assert_eq!(relocated, ["previous", "caller-hash"]);
    }

    #[test]
    fn userinfo_is_stripped_before_signing() {
        use chrono::TimeZone;

        // A hand-built request can carry userinfo; the wire `Host` never does.
        let signer = test_signer(PayloadHashMode::StandardAws);
        let mut req = HttpRequest::new(reqwest::Request::new(
            reqwest::Method::GET,
            "https://user:pw@rest.example.com/v1/config"
                .parse()
                .unwrap(),
        ));
        assert_eq!(req.url().username(), "user");
        let now = Utc.with_ymd_and_hms(2015, 8, 30, 12, 36, 0).unwrap();

        signer.sign_at(&mut req, &test_credentials(), now).unwrap();

        assert_eq!(req.url().username(), "");
        assert_eq!(req.url().password(), None);
        assert_signature_is(
            &req,
            "0f4a3487bcff9dd16bf0a42d06c24dc49b2366e8a928dc9666f8424cf5b306b3",
        );
    }

    #[test]
    fn a_doubled_slash_in_the_path_is_normalized() {
        use chrono::TimeZone;

        // A trailing slash on the catalog URI gives `//v1/...`, which
        // `Aws4Signer` collapses.
        let signer = test_signer(PayloadHashMode::StandardAws);
        let mut req = HttpRequest::new(reqwest::Request::new(
            reqwest::Method::GET,
            "https://rest.example.com//v1//config".parse().unwrap(),
        ));
        let now = Utc.with_ymd_and_hms(2015, 8, 30, 12, 36, 0).unwrap();

        signer.sign_at(&mut req, &test_credentials(), now).unwrap();

        assert_signature_is(
            &req,
            "0f4a3487bcff9dd16bf0a42d06c24dc49b2366e8a928dc9666f8424cf5b306b3",
        );
    }

    #[test]
    fn caller_headers_the_signer_overwrites_are_relocated() {
        use chrono::TimeZone;

        // As in Java, conflicting caller values move to `Original-<name>`.
        let creds = aws_credential_types::Credentials::new(
            "ak".to_string(),
            "sk".to_string(),
            Some("signer-token".to_string()),
            None,
            "test",
        );
        let signer = SigV4Signer::new("us-east-1", "execute-api", PayloadHashMode::StandardAws);
        let mut req = HttpRequest::new(
            reqwest::Client::new()
                .get("https://rest.example.com/v1/config")
                .header("authorization", "Bearer caller-token")
                .header("x-amz-date", "19700101T000000Z")
                .header("x-amz-security-token", "caller-session")
                .header("x-amz-content-sha256", "caller-hash")
                .build()
                .unwrap(),
        );

        signer
            .sign_at(
                &mut req,
                &creds,
                Utc.with_ymd_and_hms(2015, 8, 30, 12, 36, 0).unwrap(),
            )
            .unwrap();

        let h = req.headers();
        assert_eq!(
            h.get("original-authorization").unwrap(),
            "Bearer caller-token"
        );
        assert_eq!(h.get("original-x-amz-date").unwrap(), "19700101T000000Z");
        assert_eq!(
            h.get("original-x-amz-content-sha256").unwrap(),
            "caller-hash"
        );
        let token = h.get("original-x-amz-security-token").unwrap();
        assert_eq!(token, "caller-session");
        // Relocated originals may be credentials themselves.
        assert!(token.is_sensitive());
        assert!(
            h.get("original-x-amz-content-sha256")
                .unwrap()
                .is_sensitive()
        );
        // And the signer's own values took their place.
        assert!(
            h.get("authorization")
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("AWS4-HMAC-SHA256 ")
        );
        assert_eq!(h.get("x-amz-security-token").unwrap(), "signer-token");
    }

    #[test]
    fn an_existing_authorization_is_never_signed() {
        use chrono::TimeZone;

        // The signer replaces `authorization`, and a proxy may rewrite
        // `user-agent`, so neither is signed.
        let signer = test_signer(PayloadHashMode::StandardAws);
        let mut req = HttpRequest::new(
            reqwest::Client::new()
                .get("https://rest.example.com/v1/config")
                .header("authorization", "Bearer caller-token")
                .header("user-agent", "example/1.0")
                .build()
                .unwrap(),
        );

        signer
            .sign_at(
                &mut req,
                &test_credentials(),
                Utc.with_ymd_and_hms(2015, 8, 30, 12, 36, 0).unwrap(),
            )
            .unwrap();

        let auth = req
            .headers()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        let signed = auth
            .split("SignedHeaders=")
            .nth(1)
            .unwrap()
            .split(',')
            .next()
            .unwrap();
        for excluded in ["authorization", "user-agent"] {
            assert!(!signed.split(';').any(|h| h == excluded), "{signed}");
        }
        // The relocated copy is signed: it is renamed before signing.
        assert!(
            signed.split(';').any(|h| h == "original-authorization"),
            "{signed}"
        );
    }

    /// As in Java, every `Authorization` value is relocated, and stays redacted.
    #[test]
    fn every_repeated_authorization_is_relocated_and_kept_sensitive() {
        use chrono::TimeZone;

        let signer = test_signer(PayloadHashMode::StandardAws);
        let mut req = HttpRequest::new(
            reqwest::Client::new()
                .get("https://rest.example.com/v1/config")
                .header("authorization", "Bearer first")
                .header("authorization", "Bearer second")
                .build()
                .unwrap(),
        );

        signer
            .sign_at(
                &mut req,
                &test_credentials(),
                Utc.with_ymd_and_hms(2015, 8, 30, 12, 36, 0).unwrap(),
            )
            .unwrap();

        let relocated: Vec<_> = req
            .headers()
            .get_all(RELOCATED_AUTHORIZATION)
            .iter()
            .collect();
        assert_eq!(relocated.len(), 2, "{relocated:?}");
        assert_eq!(relocated[0], "Bearer first");
        assert_eq!(relocated[1], "Bearer second");
        assert!(relocated.iter().all(|v| v.is_sensitive()), "{relocated:?}");
    }

    #[test]
    fn hop_by_hop_headers_are_not_signed() {
        use chrono::TimeZone;

        // A proxy may drop or rewrite these, so they are not signed.
        let signer = test_signer(PayloadHashMode::StandardAws);
        let mut req = HttpRequest::new(
            reqwest::Client::new()
                .get("https://rest.example.com/v1/config")
                .header("expect", "100-continue")
                .header("connection", "keep-alive")
                .header("x-forwarded-for", "203.0.113.7")
                .header("x-tenant", "acme")
                .build()
                .unwrap(),
        );
        let now = Utc.with_ymd_and_hms(2015, 8, 30, 12, 36, 0).unwrap();

        signer.sign_at(&mut req, &test_credentials(), now).unwrap();

        let auth = req
            .headers()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        let signed = auth
            .split("SignedHeaders=")
            .nth(1)
            .unwrap()
            .split(',')
            .next()
            .unwrap();
        for skipped in ["expect", "connection", "x-forwarded-for"] {
            assert!(!signed.split(';').any(|h| h == skipped), "{signed}");
        }
        // An ordinary caller header is still signed.
        assert!(signed.split(';').any(|h| h == "x-tenant"), "{signed}");
        assert_signature_is(
            &req,
            "f938221412ed6b55cf3db380ce6ded476419ad3d7db4c76d932031c98465ce79",
        );
    }

    #[test]
    fn signed_credentials_are_marked_sensitive() {
        use chrono::TimeZone;

        // Both carry a credential, so `Debug` must not print them.
        let creds = aws_credential_types::Credentials::new(
            "ak".to_string(),
            "sk".to_string(),
            Some("session-token".to_string()),
            None,
            "test",
        );
        let signer = SigV4Signer::new("us-east-1", "execute-api", PayloadHashMode::StandardAws);
        let mut req = HttpRequest::new(
            reqwest::Client::new()
                .get("https://rest.example.com/v1/config")
                .build()
                .unwrap(),
        );

        signer
            .sign_at(
                &mut req,
                &creds,
                Utc.with_ymd_and_hms(2015, 8, 30, 12, 36, 0).unwrap(),
            )
            .unwrap();

        assert!(req.headers().get("authorization").unwrap().is_sensitive());
        assert!(
            req.headers()
                .get("x-amz-security-token")
                .unwrap()
                .is_sensitive()
        );
        let debug = format!("{:?}", req.headers());
        assert!(!debug.contains("session-token"), "{debug}");
    }

    #[test]
    fn a_caller_content_hash_is_relocated_not_dropped() {
        use chrono::TimeZone;

        // The caller's value moves aside rather than vanishing.
        let signer = test_signer(PayloadHashMode::StandardAws);
        let mut req = HttpRequest::new(
            reqwest::Client::new()
                .get("https://rest.example.com/v1/config")
                .header("x-amz-content-sha256", "caller-supplied")
                .build()
                .unwrap(),
        );

        signer
            .sign_at(
                &mut req,
                &test_credentials(),
                Utc.with_ymd_and_hms(2015, 8, 30, 12, 36, 0).unwrap(),
            )
            .unwrap();

        assert_eq!(
            req.headers().get("original-x-amz-content-sha256").unwrap(),
            "caller-supplied"
        );
        assert_eq!(
            req.headers().get("x-amz-content-sha256").unwrap(),
            EMPTY_HEX
        );
    }

    #[test]
    fn signs_with_a_non_default_service_and_session_token() {
        use chrono::TimeZone;

        // As a non-AWS catalog might vend: its own signing name, and STS
        // credentials.
        let creds = aws_credential_types::Credentials::new(
            "STS.EXAMPLEACCESSKEYID",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            Some("example-session-token".to_string()),
            None,
            "test",
        );
        let signer = SigV4Signer::new("us-east-1", "custom-service", PayloadHashMode::IcebergRest);
        let mut req = HttpRequest::new(
            reqwest::Client::new()
                .get("https://catalog.example.com/v1/config?warehouse=my-catalog")
                .build()
                .unwrap(),
        );
        let now = Utc.with_ymd_and_hms(2026, 8, 26, 12, 0, 0).unwrap();

        signer.sign_at(&mut req, &creds, now).unwrap();

        assert_signature_is(
            &req,
            "6b7065e5f44da4f5c3654126b8d5fe29599905afb6b98f4de65b6ec6e1be783f",
        );
        let auth = req
            .headers()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(
            auth.contains("/us-east-1/custom-service/aws4_request"),
            "{auth}"
        );
        assert_eq!(
            req.headers().get("x-amz-security-token").unwrap(),
            "example-session-token"
        );
    }

    #[test]
    fn signs_request_iceberg_mode() {
        use chrono::TimeZone;

        let creds = aws_credential_types::Credentials::new(
            "AKIDEXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            Some("SESSIONTOKEN".to_string()),
            None,
            "test",
        );
        let signer = SigV4Signer::new("us-east-1", "glue", PayloadHashMode::IcebergRest);
        let client = reqwest::Client::new();
        let mut req = HttpRequest::new(
            client
                .post("https://rest.example.com/v1/namespaces")
                .body("{}")
                .build()
                .unwrap(),
        );

        signer
            .sign_at(
                &mut req,
                &creds,
                Utc.with_ymd_and_hms(2015, 8, 30, 12, 36, 0).unwrap(),
            )
            .unwrap();

        let h = req.headers();
        assert!(
            h.get("authorization")
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/")
        );
        assert_eq!(h.get("x-amz-date").unwrap(), "20150830T123600Z");
        assert_eq!(h.get("x-amz-security-token").unwrap(), "SESSIONTOKEN");
        let csha = h.get("x-amz-content-sha256").unwrap().to_str().unwrap();
        assert_eq!(csha, "RBNvo1WzZ4oRRq0W9+hknpT7T8If536DEMBg9hyq/4o=");
        assert_signature_is(
            &req,
            "effad6acde583dd14ba7aff52b2b83776a54421c010fe82067f166819057cb32",
        );
    }

    /// An empty body hashes to the hex constant, and caller headers are signed
    /// (Java's `authenticateWithoutBody`).
    #[test]
    fn signs_empty_body_and_all_headers() {
        use chrono::TimeZone;

        let creds = aws_credential_types::Credentials::new(
            "AKIDEXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            None::<String>,
            None,
            "test",
        );
        let signer = SigV4Signer::new("us-east-1", "glue", PayloadHashMode::IcebergRest);
        let client = reqwest::Client::new();
        let mut req = HttpRequest::new(
            client
                .get("https://rest.example.com/v1/config")
                .header("content-type", "application/json")
                .header("content-encoding", "gzip")
                .build()
                .unwrap(),
        );

        signer
            .sign_at(
                &mut req,
                &creds,
                Utc.with_ymd_and_hms(2015, 8, 30, 12, 36, 0).unwrap(),
            )
            .unwrap();

        let h = req.headers();
        assert_eq!(h.get("x-amz-content-sha256").unwrap(), EMPTY_HEX);
        assert!(!h.contains_key("x-amz-security-token"));
        let auth = h.get("authorization").unwrap().to_str().unwrap();
        assert!(auth.starts_with("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/"));
        assert!(auth.contains(
            "SignedHeaders=content-encoding;content-type;host;x-amz-content-sha256;x-amz-date"
        ));
        assert_signature_is(
            &req,
            "eaef7eb88d9cd810031684748671d8a3c9394ea5168622a212ed041b670b9777",
        );
    }

    #[test]
    fn iceberg_mode_signs_the_hex_payload_hash_not_the_base64_header() {
        // Needs a body: without one the header is the hex constant too.
        use chrono::TimeZone;

        let creds = aws_credential_types::Credentials::new(
            "AKIDEXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            None::<String>,
            None,
            "test",
        );
        let signer = SigV4Signer::new("us-east-1", "execute-api", PayloadHashMode::IcebergRest);
        let body = br#"{"namespace":["ns"]}"#;
        let mut req = HttpRequest::new(
            reqwest::Client::new()
                .post("https://rest.example.com/v1/namespaces")
                .body(body.to_vec())
                .build()
                .unwrap(),
        );
        let now = Utc.with_ymd_and_hms(2015, 8, 30, 12, 36, 0).unwrap();

        signer.sign_at(&mut req, &creds, now).unwrap();

        // The header carries base64 while the pinned signature covers hex.
        assert_eq!(
            req.headers().get("x-amz-content-sha256").unwrap(),
            content_sha256_header(Some(body), PayloadHashMode::IcebergRest).as_str()
        );
        assert_ne!(
            req.headers().get("x-amz-content-sha256").unwrap(),
            hex_sha256(body).as_str()
        );
        assert_signature_is(
            &req,
            "c68682c26cab6a781256f83b0076f50014f4922c3907f4ff09c204a74d61fc1d",
        );
    }

    /// What Iceberg Java's `RESTSigV4AuthSession` (iceberg-aws 1.10.1) sent for
    /// these requests, given these credentials as `rest.*` properties.
    #[test]
    fn signatures_match_iceberg_java() {
        use chrono::TimeZone;

        let creds = aws_credential_types::Credentials::new(
            "AKIDEXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            Some("example-session-token".to_string()),
            None,
            "test",
        );
        let signer = test_signer(PayloadHashMode::IcebergRest);
        let now = Utc.with_ymd_and_hms(2026, 10, 2, 11, 40, 30).unwrap();
        let client = reqwest::Client::new();

        // A body, so the header carries base64, and a token to relocate.
        let mut post = HttpRequest::new(
            client
                .post("https://rest.example.com/v1/namespaces")
                .header("content-type", "application/json")
                .header("authorization", "Bearer delegate-token")
                .body(r#"{"namespace":["a","b"],"properties":{}}"#)
                .build()
                .unwrap(),
        );
        signer.sign_at(&mut post, &creds, now).unwrap();
        assert_eq!(
            post.headers().get("authorization").unwrap(),
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20261002/us-east-1/execute-api/aws4_request, \
             SignedHeaders=content-type;host;original-authorization;\
             x-amz-content-sha256;x-amz-date;x-amz-security-token, \
             Signature=c5d6bfcecd19c5421e8696c67465f6909b5e09398608d8e79a9eb391e04de556"
        );
        assert_eq!(
            post.headers().get("x-amz-content-sha256").unwrap(),
            "n1CB8Yl4L77sBwHTc5qTpUEhlsHHOXFzg0dV0fbOwXs="
        );

        // No body, a multi-level namespace and an encoded query.
        let mut get = HttpRequest::new(
            client
                .get("https://rest.example.com/v1/namespaces/a%1Fb/tables/x,y?pageToken=a%20b%2Fc")
                .build()
                .unwrap(),
        );
        signer.sign_at(&mut get, &creds, now).unwrap();
        assert_eq!(
            get.headers().get("authorization").unwrap(),
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20261002/us-east-1/execute-api/aws4_request, \
             SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token, \
             Signature=5f6832ec82fc821b08cd3eac3a86ffc3e0c333571fd0a5796e43f2534f23329f"
        );
    }

    /// The signed `host` keeps a non-default port, as on the wire.
    #[test]
    fn signs_host_with_non_default_port() {
        use chrono::TimeZone;

        let creds = aws_credential_types::Credentials::new(
            "AKIDEXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            None::<String>,
            None,
            "test",
        );
        let signer = SigV4Signer::new("us-east-1", "glue", PayloadHashMode::IcebergRest);
        let client = reqwest::Client::new();
        let mut req = HttpRequest::new(
            client
                .get("https://rest.example.com:8181/v1/config")
                .build()
                .unwrap(),
        );
        let now = Utc.with_ymd_and_hms(2015, 8, 30, 12, 36, 0).unwrap();

        signer.sign_at(&mut req, &creds, now).unwrap();
        assert_signature_is(
            &req,
            "f7801a4ecac5fe6dcc4ee385223428ec4833f21d0c2fc10d2fb00694b2c0def7",
        );
    }

    /// Like `Aws4Signer`, the canonical path is encoded again: `,` becomes `%2C`
    /// and `%2C` becomes `%252C`.
    #[test]
    fn canonical_uri_is_aws_double_encoded() {
        use chrono::TimeZone;

        let creds = aws_credential_types::Credentials::new(
            "AKIDEXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            None::<String>,
            None,
            "test",
        );
        let signer = SigV4Signer::new("us-east-1", "glue", PayloadHashMode::IcebergRest);
        let client = reqwest::Client::new();
        let mut req = HttpRequest::new(
            client
                .get("https://rest.example.com/v1/namespaces/a%2Cb/tables/x,y")
                .build()
                .unwrap(),
        );
        let now = Utc.with_ymd_and_hms(2015, 8, 30, 12, 36, 0).unwrap();

        signer.sign_at(&mut req, &creds, now).unwrap();
        assert_signature_is(
            &req,
            "4d966b6fc2dfb62be5a603e4e07e5dc85b2af1c7e6a181c5646bfbf38ddfa543",
        );
    }

    /// The AWS SigV4 test suite's `post-x-www-form-urlencoded`, which signs a
    /// hex `x-amz-content-sha256` like this mode.
    #[test]
    fn standard_mode_matches_the_aws_test_suite() {
        use chrono::TimeZone;

        let creds = aws_credential_types::Credentials::new(
            "AKIDEXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            None::<String>,
            None,
            "test",
        );
        let signer = SigV4Signer::new("us-east-1", "service", PayloadHashMode::StandardAws);
        let mut req = HttpRequest::new(
            reqwest::Client::new()
                .post("https://example.amazonaws.com/")
                .header("content-type", "application/x-www-form-urlencoded")
                .header("content-length", "13")
                .body("Param1=value1")
                .build()
                .unwrap(),
        );

        signer
            .sign_at(
                &mut req,
                &creds,
                Utc.with_ymd_and_hms(2015, 8, 30, 12, 36, 0).unwrap(),
            )
            .unwrap();

        assert_eq!(
            req.headers().get("x-amz-content-sha256").unwrap(),
            "9095672bbd1f56dfc5b65f3e153adc8731a4a654192329106275f4c7b24d0b6e"
        );
        assert_eq!(
            req.headers().get("authorization").unwrap(),
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, \
             SignedHeaders=content-length;content-type;host;x-amz-content-sha256;x-amz-date, \
             Signature=d3875051da38690788ef43de4db0d8f280229d82040bfac253562e56c3f20e0b"
        );
    }

    /// The one test on the live clock: everything else pins the time.
    #[test]
    fn sign_stamps_the_current_time() {
        use chrono::SubsecRound;

        let signer = test_signer(PayloadHashMode::IcebergRest);
        let mut req = HttpRequest::new(
            reqwest::Client::new()
                .get("https://rest.example.com/v1/config")
                .build()
                .unwrap(),
        );
        let before = Utc::now();
        signer.sign(&mut req, &test_credentials()).unwrap();
        let after = Utc::now();

        let date = req.headers().get("x-amz-date").unwrap().to_str().unwrap();
        let stamped = chrono::NaiveDateTime::parse_from_str(date, "%Y%m%dT%H%M%SZ")
            .unwrap()
            .and_utc();
        assert_eq!(date.len(), "YYYYMMDDTHHMMSSZ".len(), "{date}");
        // The header drops sub-seconds, so compare against a truncated start.
        assert!(
            before.trunc_subsecs(0) <= stamped && stamped <= after,
            "{date}"
        );
    }

    fn test_credentials_provider() -> SharedCredentialsProvider {
        SharedCredentialsProvider::new(test_credentials())
    }

    fn test_session(session_token: Option<&str>) -> SigV4Session {
        SigV4Session {
            delegate: Arc::new(crate::auth::NoopSession),
            signer: test_signer(PayloadHashMode::IcebergRest),
            credentials: SharedCredentialsProvider::new(aws_credential_types::Credentials::new(
                "AKIDEXAMPLE",
                "secret",
                session_token.map(str::to_string),
                None,
                "test",
            )),
        }
    }

    fn request_with(headers: &[(&'static str, &str)]) -> HttpRequest {
        let mut builder = reqwest::Client::new().get("https://rest.example.com/v1/config");
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        HttpRequest::new(builder.build().unwrap())
    }

    #[tokio::test]
    async fn relocation_keeps_an_existing_original_authorization() {
        // Java collects both values under `Original-Authorization` instead of
        // replacing one with the other.
        let mut request = request_with(&[
            ("original-authorization", "credential-A"),
            ("authorization", "credential-B"),
        ]);

        test_session(None).authenticate(&mut request).await.unwrap();

        let relocated: Vec<_> = request
            .headers()
            .get_all("original-authorization")
            .iter()
            .map(|v| v.to_str().unwrap().to_string())
            .collect();
        assert_eq!(relocated, ["credential-A", "credential-B"]);
        assert!(
            request
                .headers()
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("AWS4-HMAC-SHA256 ")
        );
    }

    #[tokio::test]
    async fn conflicting_signer_headers_are_relocated_not_dropped() {
        let mut request = request_with(&[
            ("x-amz-date", "19700101T000000Z"),
            ("x-amz-security-token", "caller-token"),
        ]);

        test_session(Some("signer-token"))
            .authenticate(&mut request)
            .await
            .unwrap();

        assert_eq!(
            request.headers().get("original-x-amz-date").unwrap(),
            "19700101T000000Z"
        );
        let token = request
            .headers()
            .get("original-x-amz-security-token")
            .unwrap();
        assert_eq!(token, "caller-token");
        assert!(token.is_sensitive());
        assert_eq!(
            request.headers().get("x-amz-security-token").unwrap(),
            "signer-token"
        );
    }

    /// Java calls `resolveCredentials()` inside `sign`, so a refreshing
    /// provider is consulted again for every request rather than once.
    #[tokio::test]
    async fn credentials_are_resolved_once_per_request() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let calls = Arc::new(AtomicUsize::new(0));
        let counted = {
            let calls = calls.clone();
            aws_credential_types::credential_fn::provide_credentials_fn(move || {
                let calls = calls.clone();
                async move {
                    let n = calls.fetch_add(1, Ordering::SeqCst);
                    Ok(aws_credential_types::Credentials::new(
                        format!("AKID{n}"),
                        "secret",
                        None::<String>,
                        None,
                        "test",
                    ))
                }
            })
        };
        let session = SigV4Session {
            delegate: Arc::new(crate::auth::NoopSession),
            signer: test_signer(PayloadHashMode::IcebergRest),
            credentials: SharedCredentialsProvider::new(counted),
        };

        let mut first = request_with(&[]);
        session.authenticate(&mut first).await.unwrap();
        let mut second = request_with(&[]);
        session.authenticate(&mut second).await.unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let auth = |r: &HttpRequest| {
            r.headers()
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap()
                .to_string()
        };
        // Each request signed with the credentials resolved for it.
        assert!(auth(&first).contains("AKID0/"), "{}", auth(&first));
        assert!(auth(&second).contains("AKID1/"), "{}", auth(&second));
    }

    #[tokio::test]
    async fn the_delegate_authenticates_before_signing() {
        // The delegate's bearer token must be relocated and signed over, which
        // can only happen if it was applied first.
        #[derive(Debug)]
        struct Bearer;
        #[async_trait]
        impl AuthSession for Bearer {
            async fn authenticate(&self, request: &mut HttpRequest) -> Result<()> {
                request
                    .headers_mut()
                    .insert("authorization", "Bearer delegate".parse().unwrap());
                Ok(())
            }
        }

        let session = SigV4Session {
            delegate: Arc::new(Bearer),
            signer: test_signer(PayloadHashMode::IcebergRest),
            credentials: test_credentials_provider(),
        };
        let mut request = request_with(&[]);
        session.authenticate(&mut request).await.unwrap();

        assert_eq!(
            request.headers().get("original-authorization").unwrap(),
            "Bearer delegate"
        );
        let auth = request.headers().get("authorization").unwrap();
        assert!(auth.to_str().unwrap().contains("original-authorization"));
    }

    fn props(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[tokio::test]
    async fn static_credentials_come_from_properties() {
        let (signer, credentials) = from_props(&props(&[
            (REST_CATALOG_PROP_SIGNING_REGION, "cn-hangzhou"),
            (REST_CATALOG_PROP_ACCESS_KEY_ID, "AKID"),
            (REST_CATALOG_PROP_SECRET_ACCESS_KEY, "secret"),
            (REST_CATALOG_PROP_SESSION_TOKEN, "token"),
        ]))
        .await
        .unwrap();

        assert_eq!(signer.region, "cn-hangzhou");
        // Java's REST_CATALOG_PROP_SIGNING_NAME_DEFAULT.
        assert_eq!(signer.service, SIGNING_NAME_DEFAULT);
        let resolved = credentials.provide_credentials().await.unwrap();
        assert_eq!(resolved.access_key_id(), "AKID");
        assert_eq!(resolved.session_token(), Some("token"));
    }

    #[tokio::test]
    async fn a_blank_property_counts_as_absent() {
        let err = from_props(&props(&[(REST_CATALOG_PROP_SIGNING_REGION, "   ")]))
            .await
            .unwrap_err();
        assert!(
            err.message().contains(REST_CATALOG_PROP_SIGNING_REGION),
            "{err}"
        );
    }

    /// Half a pair is a typo, not a request for the default chain, so each
    /// direction has to name the property that is missing rather than fall
    /// through to the generic "no credentials" error.
    #[tokio::test]
    async fn half_a_credential_pair_is_rejected_either_way() {
        for (present, missing) in [
            (
                REST_CATALOG_PROP_ACCESS_KEY_ID,
                REST_CATALOG_PROP_SECRET_ACCESS_KEY,
            ),
            (
                REST_CATALOG_PROP_SECRET_ACCESS_KEY,
                REST_CATALOG_PROP_ACCESS_KEY_ID,
            ),
        ] {
            let err = from_props(&props(&[
                (REST_CATALOG_PROP_SIGNING_REGION, "us-east-1"),
                (present, "value"),
            ]))
            .await
            .unwrap_err();
            assert_eq!(err.kind(), ErrorKind::DataInvalid, "{err}");
            let message = err.message();
            assert!(message.contains(present), "{err}");
            assert!(message.contains(missing), "{err}");
            // Not the "bring your own provider" error: this is a typo.
            assert!(!message.contains("credentials provider"), "{err}");
        }
    }

    #[tokio::test]
    async fn without_credentials_the_error_points_at_the_provider() {
        let err = from_props(&props(&[(REST_CATALOG_PROP_SIGNING_REGION, "us-east-1")]))
            .await
            .unwrap_err();
        assert!(err.message().contains("credentials provider"), "{err}");
    }
}
