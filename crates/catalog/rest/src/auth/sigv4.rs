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
#[cfg(test)]
use hmac::{Hmac, Mac};
use iceberg::{Error, ErrorKind, Result};
use sha2::{Digest, Sha256};

/// Hex SHA-256 of the empty string.
const EMPTY_BODY_HEX_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// How the payload hash is encoded in the `x-amz-content-sha256` header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PayloadHashMode {
    /// Iceberg Java's RESTSigV4 style: base64 header when there is a body, hex
    /// when there is none; the canonical request always uses hex.
    IcebergRest,
    /// Standard AWS SigV4 style: hex everywhere (e.g. AWS Glue).
    StandardAws,
}

/// Derives the AWS SigV4 signing key.
#[cfg(test)]
fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key).expect("HMAC takes a key of any size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn hex_sha256(data: &[u8]) -> String {
    encode_hex(&Sha256::digest(data))
}

#[cfg(test)]
fn hex_hmac_sha256(key: &[u8], data: &[u8]) -> String {
    encode_hex(&hmac_sha256(key, data))
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn base64_encode(bytes: &[u8]) -> String {
    base64::engine::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes)
}

#[cfg(test)]
fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
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
    pub fn new(region: String, service: String, mode: PayloadHashMode) -> Self {
        Self {
            region,
            service,
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
    /// Fails rather than sign a streaming body or a non-UTF-8 header, neither
    /// of which canonicalizes faithfully.
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
        use aws_sigv4::http_request::{
            PayloadChecksumKind, PercentEncodingMode, SignableBody, SignableRequest,
            SigningSettings, UriPathNormalizationMode, sign,
        };
        use aws_sigv4::sign::v4;
        use tracing::subscriber::NoSubscriber;

        // Java branches on `encodedBody() == null`, so absent and empty differ.
        let body: Option<Vec<u8>> = match request.body() {
            crate::HttpRequestBody::Empty => None,
            crate::HttpRequestBody::Buffered(bytes) => Some(bytes.to_vec()),
            crate::HttpRequestBody::Streaming => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    "cannot sign a streaming request body",
                ));
            }
        };
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
        request
            .headers_mut()
            .insert(CONTENT_SHA256, content_header.parse().unwrap());

        // The wire Host never carries userinfo, so signing it would mismatch
        // and feed the password to the HMAC. Only hand-built requests have it.
        if !request.url().username().is_empty() || request.url().password().is_some() {
            let url = request.url_mut();
            let _ = url.set_username("");
            let _ = url.set_password(None);
        }

        // Both AWS and Java read `+` as a space, so this leaves the signature
        // alone; it makes the sent URL agree with an RFC 3986 verifier too.
        if let Some(query) = request.url().query().filter(|q| q.contains('+')) {
            let unambiguous = query.replace('+', "%20");
            request.url_mut().set_query(Some(&unambiguous));
        }

        let mut settings = SigningSettings::default();
        // `Aws4Signer` defaults: normalize the path and double-encode it.
        settings.percent_encoding_mode = PercentEncodingMode::Double;
        settings.uri_path_normalization_mode = UriPathNormalizationMode::Enabled;
        // The header is ours to set: IcebergRest puts base64 there, while the
        // canonical request keeps hex.
        settings.payload_checksum_kind = PayloadChecksumKind::NoHeader;
        // In Java's ignore list but not the crate's defaults.
        let mut excluded = settings.excluded_headers.take().unwrap_or_default();
        excluded.extend([
            "expect".into(),
            "connection".into(),
            "x-forwarded-for".into(),
        ]);
        settings.excluded_headers = Some(excluded);

        let identity = credentials.clone().into();
        let params = v4::SigningParams::builder()
            .identity(&identity)
            .region(&self.region)
            .name(&self.service)
            .time(now.into())
            .settings(settings)
            .build()
            .map_err(|e| {
                Error::new(ErrorKind::Unexpected, "failed to build SigV4 params").with_source(e)
            })?
            .into();

        // Skipping one would leave it unsigned but still on the wire, which
        // AWS rejects for `x-amz-*` and is hard to diagnose.
        let headers: Vec<(&str, &str)> = request
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
            .collect::<Result<_>>()?;
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
        let signed =
            tracing::subscriber::with_default(NoSubscriber::default(), || sign(signable, &params));
        let (instructions, _signature) = signed
            .map_err(|e| Error::new(ErrorKind::Unexpected, "SigV4 signing failed").with_source(e))?
            .into_parts();

        update_request_headers(request, instructions, displaced_content_hash)
    }
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

        let signer = SigV4Signer::new(
            "us-east-1".to_string(),
            "execute-api".to_string(),
            PayloadHashMode::StandardAws,
        );
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

    /// The signature the previous hand-rolled signer produced for this case,
    /// pinned so a change in canonicalization is caught.
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
        SigV4Signer::new("us-east-1".to_string(), "execute-api".to_string(), mode)
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
        let signer = SigV4Signer::new(
            "us-east-1".to_string(),
            "execute-api".to_string(),
            PayloadHashMode::StandardAws,
        );
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
        // ignores them too.
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
        let signer = SigV4Signer::new(
            "us-east-1".to_string(),
            "execute-api".to_string(),
            PayloadHashMode::StandardAws,
        );
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
        let signer = SigV4Signer::new(
            "us-east-1".to_string(),
            "custom-service".to_string(),
            PayloadHashMode::IcebergRest,
        );
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
    fn signing_key_and_signature_match_aws_vector() {
        let secret = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
        let date = "20150830";
        let region = "us-east-1";
        let service = "service";
        let key = signing_key(secret, date, region, service);

        let string_to_sign = "AWS4-HMAC-SHA256\n\
20150830T123600Z\n\
20150830/us-east-1/service/aws4_request\n\
bb579772317eb040ac9ed261061d46c1f17a8133879d6129b6e1c25292927e63";
        let sig = hex_hmac_sha256(&key, string_to_sign.as_bytes());
        assert_eq!(
            sig,
            "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
    }

    #[test]
    fn signs_request_iceberg_mode() {
        let creds = aws_credential_types::Credentials::new(
            "AKIDEXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            Some("SESSIONTOKEN".to_string()),
            None,
            "test",
        );
        let signer = SigV4Signer::new(
            "us-east-1".to_string(),
            "glue".to_string(),
            PayloadHashMode::IcebergRest,
        );
        let client = reqwest::Client::new();
        let mut req = HttpRequest::new(
            client
                .post("https://rest.example.com/v1/namespaces")
                .body("{}")
                .build()
                .unwrap(),
        );

        signer.sign(&mut req, &creds).unwrap();

        let h = req.headers();
        assert!(
            h.get("authorization")
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/")
        );
        assert!(h.contains_key("x-amz-date"));
        assert_eq!(h.get("x-amz-security-token").unwrap(), "SESSIONTOKEN");
        let csha = h.get("x-amz-content-sha256").unwrap().to_str().unwrap();
        assert_eq!(csha, "RBNvo1WzZ4oRRq0W9+hknpT7T8If536DEMBg9hyq/4o=");
    }

    /// Empty body uses the hex constant and existing headers are signed too
    /// (mirrors Java's `TestRESTSigV4AuthSession::authenticateWithoutBody`).
    #[test]
    fn signs_empty_body_and_all_headers() {
        let creds = aws_credential_types::Credentials::new(
            "AKIDEXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            None::<String>,
            None,
            "test",
        );
        let signer = SigV4Signer::new(
            "us-east-1".to_string(),
            "glue".to_string(),
            PayloadHashMode::IcebergRest,
        );
        let client = reqwest::Client::new();
        let mut req = HttpRequest::new(
            client
                .get("https://rest.example.com/v1/config")
                .header("content-type", "application/json")
                .header("content-encoding", "gzip")
                .build()
                .unwrap(),
        );

        signer.sign(&mut req, &creds).unwrap();

        let h = req.headers();
        assert_eq!(h.get("x-amz-content-sha256").unwrap(), EMPTY_HEX);
        assert!(!h.contains_key("x-amz-security-token"));
        let auth = h.get("authorization").unwrap().to_str().unwrap();
        assert!(auth.starts_with("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/"));
        assert!(auth.contains(
            "SignedHeaders=content-encoding;content-type;host;x-amz-content-sha256;x-amz-date"
        ));
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
        let signer = SigV4Signer::new(
            "us-east-1".to_string(),
            "execute-api".to_string(),
            PayloadHashMode::IcebergRest,
        );
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
        let signer = SigV4Signer::new(
            "us-east-1".to_string(),
            "glue".to_string(),
            PayloadHashMode::IcebergRest,
        );
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
        let signer = SigV4Signer::new(
            "us-east-1".to_string(),
            "glue".to_string(),
            PayloadHashMode::IcebergRest,
        );
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

    #[test]
    fn signs_request_standard_mode_uses_hex_header() {
        let creds = aws_credential_types::Credentials::new(
            "AKIDEXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            None::<String>,
            None,
            "test",
        );
        let signer = SigV4Signer::new(
            "us-east-1".to_string(),
            "glue".to_string(),
            PayloadHashMode::StandardAws,
        );
        let client = reqwest::Client::new();
        let mut req = HttpRequest::new(
            client
                .post("https://rest.example.com/v1/namespaces")
                .body("hello")
                .build()
                .unwrap(),
        );

        signer.sign(&mut req, &creds).unwrap();

        // StandardAws keeps the header in hex.
        assert_eq!(
            req.headers().get("x-amz-content-sha256").unwrap(),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }
}
