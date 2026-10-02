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

use chrono::{DateTime, Utc};
use iceberg::{Error, ErrorKind, Result};
use sha2::{Digest, Sha256};

/// Hex SHA-256 of the empty string.
const EMPTY_BODY_HEX_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// How the payload hash is encoded in the `x-amz-content-sha256` header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PayloadHashMode {
    /// Iceberg Java's RESTSigV4 style: base64 header when there is a body, hex
    /// when there is none; the canonical request always uses hex. A caller-set
    /// header is replaced, and moved to `Original-x-amz-content-sha256` when it
    /// differed. Java replaces it too for a bodiless request, but signs the
    /// caller's value when a body is present.
    ///
    /// The base64 header comes from how Java configures the AWS SDK, not from
    /// SigV4. A verifier that hashes the body itself accepts it; one that
    /// takes the payload hash from the header, or checks the header against
    /// the hex hash, rejects it.
    IcebergRest,
    /// Standard AWS SigV4 style: hex everywhere.
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

/// The `x-amz-content-sha256` value. `None` means no body at all, which the
/// two modes encode differently.
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

    /// Signs `request` in place, rewriting it as signing requires: an existing
    /// `Authorization` becomes `Original-Authorization`, userinfo leaves the
    /// URL, and a `+` in the query becomes `%20`.
    ///
    /// A `+` is therefore taken to be an encoded space; write a literal plus as
    /// `%2B`.
    ///
    /// Signs with exactly the `credentials` given and never refreshes them:
    /// with temporary ones (STS, IRSA, an instance role), resolve them from
    /// their provider before each call, as Java's session does per request.
    ///
    /// Fails rather than sign a streaming body or a non-UTF-8 header, neither
    /// of which canonicalizes faithfully.
    ///
    /// Send the result through a client that does not follow redirects: a
    /// redirect replays a signature made for another URL, and across hosts
    /// reqwest drops `Authorization` but keeps `Original-Authorization`.
    ///
    /// `aws_sigv4` traces the headers it is given, and its redaction list does
    /// not cover the `Original-` copy. A `tracing` subscriber that could record
    /// that is muted for the call; with no subscriber, or with `tracing`'s
    /// `log-always` feature, its `log` bridge still forwards those events, so
    /// keep `aws_sigv4` below trace level there.
    pub fn sign(
        &self,
        request: &mut crate::HttpRequest,
        credentials: &aws_credential_types::Credentials,
    ) -> Result<()> {
        self.sign_at(request, credentials, Utc::now())
    }

    fn sign_at(
        &self,
        request: &mut crate::HttpRequest,
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

        // The crate traces what it signs, and redacts `authorization` but not
        // the `Original-` copy, so a bearer token would be logged verbatim.
        // Mute it only when a subscriber could record trace events (the max
        // level stays `OFF` until one is registered): `with_default` marks a
        // dispatcher as set for good, which would silently divert every later
        // event away from an app's `log` bridge.
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

/// The body to sign. Java branches on `encodedBody() == null`, so an absent
/// body and an empty one hash differently.
fn signable_body(request: &crate::HttpRequest) -> Result<Option<Vec<u8>>> {
    match request.body() {
        crate::HttpRequestBody::Empty => Ok(None),
        crate::HttpRequestBody::Buffered(bytes) => Ok(Some(bytes.to_vec())),
        crate::HttpRequestBody::Streaming => Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "cannot sign a streaming request body",
        )),
    }
}

/// The headers to sign. Skipping a non-UTF-8 one would leave it unsigned but
/// still on the wire, which AWS rejects for `x-amz-*` and is hard to diagnose.
fn signable_headers(request: &crate::HttpRequest) -> Result<Vec<(&str, &str)>> {
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
/// query. Both AWS and Java read `+` as a space, so the signature is unchanged;
/// this makes the sent URL agree with an RFC 3986 verifier too.
fn rewrite_url_for_signing(request: &mut crate::HttpRequest) {
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
    // The header is ours to set: IcebergRest puts base64 there, while the
    // canonical request keeps hex.
    settings.payload_checksum_kind = PayloadChecksumKind::NoHeader;
    let mut excluded = settings.excluded_headers.take().unwrap_or_default();
    excluded.extend([
        // Java's list, spelled out even where the crate's defaults overlap, so
        // a minor release cannot start signing a header a proxy rewrites.
        "connection".into(),
        "expect".into(),
        "transfer-encoding".into(),
        "user-agent".into(),
        "x-amzn-trace-id".into(),
        // Not on Java's list, but a proxy appends to it as well.
        "x-forwarded-for".into(),
        // Relocation appends to these after signing, so a caller-supplied one
        // would otherwise be signed and then changed on the wire.
        "original-x-amz-date".into(),
        "original-x-amz-content-sha256".into(),
        "original-x-amz-security-token".into(),
    ]);
    settings.excluded_headers = Some(excluded);
    settings
}

/// Java's `convertHeaders`: renames `Authorization` so SigV4 can take the
/// name. Runs before signing, so the relocated copy is signed too.
fn convert_headers(request: &mut crate::HttpRequest) {
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

/// Java's `updateRequestHeaders`: installs the signed headers, moving a
/// conflicting caller value aside rather than dropping it.
fn update_request_headers(
    request: &mut crate::HttpRequest,
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

/// Moves `name`'s values aside when they differ from the one about to be
/// signed, so a caller's header is not silently dropped.
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
        // The original may carry a credential (e.g. a session token).
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HttpRequest;

    const EMPTY_HEX: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    #[test]
    fn signing_rewrites_an_ambiguous_plus_out_of_the_query() {
        use chrono::TimeZone;

        // reqwest writes a space as `+`, which verifiers read either as a
        // literal plus or as a space. Signing rewrites it to `%20`.
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

        // The request that goes out no longer carries the ambiguous form.
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

    /// Java branches on `encodedBody() == null`, so a body that is present but
    /// empty is hashed like any other rather than taking the absent-body path.
    #[test]
    fn content_sha256_header_separates_an_empty_body_from_an_absent_one() {
        let empty = content_sha256_header(Some(b""), PayloadHashMode::IcebergRest);
        assert_eq!(empty, "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=");
        assert_ne!(
            empty,
            content_sha256_header(None, PayloadHashMode::IcebergRest)
        );
    }

    /// The same distinction, but through `sign_at`, so that collapsing the two
    /// while reading the body off the request cannot go unnoticed.
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

    /// Pins a signature this crate produced, so a change in canonicalization
    /// is caught; `signatures_match_iceberg_java` is the check against Java.
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

    /// Collects every event field a subscriber would have been handed.
    #[derive(Clone, Default)]
    struct CapturedLog(std::sync::Arc<std::sync::Mutex<String>>);

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

    /// `aws_sigv4` traces the headers it is given, and its redaction list does
    /// not cover the `Original-` copy of a relocated bearer token.
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
            // Without this the assertion below would pass even if nothing was
            // ever captured.
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

    /// A caller-supplied `Original-x-amz-*` must not be signed: relocation
    /// appends to it afterwards, which would change a signed value on the wire.
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

        // `HttpRequest::new` is public, so a hand-built request can carry
        // userinfo that the wire Host never has.
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

        // A catalog URI with a trailing slash produces `//v1/...`; the signed
        // path has to collapse it the way `Aws4Signer` does.
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

        // Java's `updateRequestHeaders` moves a conflicting caller value to
        // `Original-<name>` rather than dropping it, credentials included.
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

        // `authorization` must stay out of `SignedHeaders`: the signer replaces
        // it, so signing the caller's value would guarantee a mismatch. The
        // crate's own defaults carry that exclusion.
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
        // The relocated copy, on the other hand, is signed over — that is the
        // point of renaming it before signing rather than after.
        assert!(
            signed.split(';').any(|h| h == "original-authorization"),
            "{signed}"
        );
    }

    /// Java groups all `Authorization` values under the relocated name, so
    /// repeated credentials must survive together and stay redacted.
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

        // A proxy or an HTTP/2 hop may drop or rewrite these, so signing them
        // would make the request fail verification. Java's `AbstractAws4Signer`
        // ignores the first two as well.
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

        // Both carry a credential, so a `Debug`-formatted request must not
        // print them.
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

        // The signer overwrites `x-amz-content-sha256`; the caller's value
        // moves aside instead of vanishing, after signing as Java does.
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

        // What a non-AWS S3-compatible catalog vends: its own signing name
        // rather than `execute-api`, its own region, and STS credentials.
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

    /// Empty body uses the hex constant and existing headers are signed too
    /// (mirrors Java's `TestRESTSigV4AuthSession::authenticateWithoutBody`).
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

    /// The signed `host` must include an explicit non-default port, matching
    /// what reqwest/hyper put on the wire and what the AWS SDK signs.
    #[test]
    fn iceberg_mode_signs_the_hex_payload_hash_not_the_base64_header() {
        // The IcebergRest split: `x-amz-content-sha256` carries base64, but the
        // canonical request must hash in hex. A body is required to tell them
        // apart — every other signing test uses an empty one, where the header
        // is the hex constant and the two values coincide.
        //
        // Java has no counterpart: there the split lives inside the AWS SDK
        // (`SignerChecksumParams` puts a base64 checksum in the header while
        // `Aws4Signer` canonicalizes hex), so `TestRESTSigV4Signer` only checks
        // that the header is present. Reimplementing the signer makes the
        // invariant ours to keep.
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

    /// What Iceberg Java's `RESTSigV4AuthSession` (iceberg-aws 1.10.1, AWS SDK
    /// 2.29.52, an `AuthSession.EMPTY` delegate, these credentials as `rest.*`
    /// properties) sent for the same requests: a check against an independent
    /// implementation, where the other pins in this file only catch changes.
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

    /// AWS SDK v2 parity (`doubleUrlEncode`): the canonical URI encodes the
    /// serialized path once more — literal `,` becomes `%2C`, an encoded
    /// `%2C` becomes `%252C` — while plain paths stay byte-identical.
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

    /// The AWS SigV4 test suite's `post-x-www-form-urlencoded` case, which
    /// signs a hex `x-amz-content-sha256` as this mode does.
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
        let signer = test_signer(PayloadHashMode::IcebergRest);
        let mut req = HttpRequest::new(
            reqwest::Client::new()
                .get("https://rest.example.com/v1/config")
                .build()
                .unwrap(),
        );
        let before = Utc::now();

        signer.sign(&mut req, &test_credentials()).unwrap();

        let date = req.headers().get("x-amz-date").unwrap().to_str().unwrap();
        let stamped = chrono::NaiveDateTime::parse_from_str(date, "%Y%m%dT%H%M%SZ")
            .unwrap()
            .and_utc();
        assert_eq!(date.len(), "YYYYMMDDTHHMMSSZ".len(), "{date}");
        assert!((stamped - before).num_seconds().abs() < 60, "{date}");
    }
}
