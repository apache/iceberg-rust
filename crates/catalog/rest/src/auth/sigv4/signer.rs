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

use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use iceberg::sensitive::SensitiveString;
use iceberg::{Error, ErrorKind, Result};
use sha2::{Digest, Sha256};

/// Hex SHA-256 of the empty string.
const EMPTY_BODY_HEX_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// How the payload hash is encoded in the `x-amz-content-sha256` header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PayloadHashMode {
    /// Iceberg Java's RESTSigV4 style: base64 header for non-empty bodies, hex
    /// for empty; the canonical request always uses hex.
    IcebergRest,
    /// Standard AWS SigV4 style: hex everywhere (e.g. AWS Glue).
    StandardAws,
}

/// Derives the AWS SigV4 signing key.
fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key).expect("HMAC takes a key of any size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn hex_sha256(data: &[u8]) -> String {
    encode_hex(&Sha256::digest(data))
}

fn hex_hmac_sha256(key: &[u8], data: &[u8]) -> String {
    encode_hex(&hmac_sha256(key, data))
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn base64_encode(bytes: &[u8]) -> String {
    base64::engine::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes)
}

fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

/// Computes the value of the `x-amz-content-sha256` header.
fn content_sha256_header(body: &[u8], mode: PayloadHashMode) -> String {
    match mode {
        PayloadHashMode::StandardAws => hex_sha256(body),
        PayloadHashMode::IcebergRest => {
            if body.is_empty() {
                EMPTY_BODY_HEX_SHA256.to_string()
            } else {
                base64_encode(&Sha256::digest(body))
            }
        }
    }
}

/// Builds the SigV4 canonical request. `headers` are (lowercased, trimmed)
/// pairs; `payload_hash` is always the hex sha256 of the body.
fn canonical_request(
    method: &str,
    canonical_uri: &str,
    canonical_query: &str,
    headers: &[(String, String)],
    payload_hash: &str,
) -> String {
    let mut sorted = headers.to_vec();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));

    let canonical_headers: String = sorted.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();
    let signed_headers = sorted
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");

    format!(
        "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    )
}

/// Builds the SigV4 string-to-sign.
fn string_to_sign(amz_date: &str, scope: &str, canonical_request: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex_sha256(canonical_request.as_bytes())
    )
}

/// Static AWS-style credentials used for SigV4 signing of catalog requests.
#[derive(Clone)]
pub struct AwsCredentials {
    /// AWS access key id.
    pub access_key_id: String,
    /// AWS secret access key.
    pub secret_access_key: SensitiveString,
    /// Optional STS session token.
    pub session_token: Option<SensitiveString>,
}

/// AWS SigV4 signer following Iceberg Java's `RESTSigV4AuthSession`: it adds the
/// required amz headers and signs all request headers except a small blacklist.
#[derive(Clone)]
pub struct SigV4Signer {
    credentials: AwsCredentials,
    region: String,
    service: String,
    mode: PayloadHashMode,
}

impl SigV4Signer {
    /// Creates a new SigV4 signer.
    pub fn new(
        credentials: AwsCredentials,
        region: String,
        service: String,
        mode: PayloadHashMode,
    ) -> Self {
        Self {
            credentials,
            region,
            service,
            mode,
        }
    }

    /// Signs `request` in place.
    pub fn sign(&self, request: &mut reqwest::Request) -> Result<()> {
        self.sign_at(request, Utc::now())
    }

    fn sign_at(&self, request: &mut reqwest::Request, now: DateTime<Utc>) -> Result<()> {
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date = now.format("%Y%m%d").to_string();
        let scope = format!("{date}/{}/{}/aws4_request", self.region, self.service);

        // The signed `host` must match what the HTTP layer sends: an explicit
        // Host header wins; otherwise it derives from the URL, including a
        // non-default port (the url crate strips scheme-default ports).
        let explicit_host = request
            .headers()
            .get(reqwest::header::HOST)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.trim().to_string());
        let host = if let Some(host) = explicit_host {
            host
        } else {
            let h = request
                .url()
                .host_str()
                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "request url has no host"))?;
            match request.url().port() {
                Some(port) => format!("{h}:{port}"),
                None => h.to_string(),
            }
        };
        // AWS SDK v2 parity (Aws4Signer defaults `normalizePath=true`,
        // `doubleUrlEncode=true`): the normalized, already-percent-encoded
        // path is encoded once more keeping `/` — e.g. `%2C` becomes `%252C`.
        let canonical_uri = uri_encode(&normalize_path(request.url().path()), false);
        // Canonicalized from the raw query, not `query_pairs()`: that form-
        // decodes, turning a wire `+` into a space that re-encodes as `%20`,
        // while SigV4 verifiers read `+` as a literal plus.
        let mut encoded_pairs: Vec<(String, String)> = request
            .url()
            .query()
            .unwrap_or_default()
            .split('&')
            .filter(|pair| !pair.is_empty())
            .map(|pair| {
                let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
                (
                    uri_encode(&percent_decode(name), true),
                    uri_encode(&percent_decode(value), true),
                )
            })
            .collect();
        encoded_pairs.sort();
        let canonical_query = encoded_pairs
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");

        let body: &[u8] = match request.body() {
            None => &[],
            Some(b) => b.as_bytes().ok_or_else(|| {
                Error::new(
                    ErrorKind::FeatureUnsupported,
                    "cannot sign a streaming request body",
                )
            })?,
        };
        let payload_hex = hex_sha256(body);
        let content_header = content_sha256_header(body, self.mode);

        let mut headers = vec![
            ("host".to_string(), host),
            ("x-amz-content-sha256".to_string(), content_header.clone()),
            ("x-amz-date".to_string(), amz_date.clone()),
        ];
        if let Some(tok) = &self.credentials.session_token {
            headers.push(("x-amz-security-token".to_string(), tok.expose().to_string()));
        }

        // Sign every other request header too (except a blacklist), as Iceberg
        // Java / botocore do; signing only host/x-amz-* yields a signature mismatch.
        const SKIP_HEADERS: &[&str] = &[
            "user-agent",
            "authorization",
            "expect",
            "x-amzn-trace-id",
            // Transports and proxies may rewrite these (AWS signers skip them).
            "connection",
            "transfer-encoding",
            "x-forwarded-for",
        ];
        let base_count = headers.len();
        for (name, value) in request.headers().iter() {
            let lname = name.as_str().to_ascii_lowercase();
            if SKIP_HEADERS.contains(&lname.as_str())
                || headers[..base_count].iter().any(|(k, _)| *k == lname)
            {
                continue;
            }
            if let Ok(v) = value.to_str() {
                // Canonical form: sequential spaces collapse to one, repeated
                // header values comma-join in order.
                let v = v.split_whitespace().collect::<Vec<_>>().join(" ");
                if let Some(pos) = headers[base_count..].iter().position(|(k, _)| *k == lname) {
                    let joined = &mut headers[base_count + pos].1;
                    joined.push(',');
                    joined.push_str(&v);
                } else {
                    headers.push((lname, v));
                }
            }
        }

        // Canonical payload hash is ALWAYS hex (the IcebergRest split: header may be base64).
        let creq = canonical_request(
            request.method().as_str(),
            &canonical_uri,
            &canonical_query,
            &headers,
            &payload_hex,
        );
        let sts = string_to_sign(&amz_date, &scope, &creq);
        let key = signing_key(
            self.credentials.secret_access_key.expose(),
            &date,
            &self.region,
            &self.service,
        );
        let signature = hex_hmac_sha256(&key, sts.as_bytes());

        let mut signed = headers.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>();
        signed.sort();
        let signed_headers = signed.join(";");
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
            self.credentials.access_key_id
        );

        let h = request.headers_mut();
        relocate_conflicting(h, "x-amz-date", &amz_date, RELOCATED_AMZ_DATE);
        h.insert("x-amz-date", amz_date.parse().unwrap());
        relocate_conflicting(
            h,
            "x-amz-content-sha256",
            &content_header,
            RELOCATED_CONTENT_SHA256,
        );
        h.insert("x-amz-content-sha256", content_header.parse().unwrap());
        if let Some(tok) = &self.credentials.session_token {
            let mut token_value: reqwest::header::HeaderValue =
                tok.expose().parse().map_err(|e| {
                    Error::new(ErrorKind::DataInvalid, "invalid session token").with_source(e)
                })?;
            // Redacted in `Debug`-formatted requests.
            token_value.set_sensitive(true);
            relocate_conflicting(
                h,
                "x-amz-security-token",
                tok.expose(),
                RELOCATED_SECURITY_TOKEN,
            );
            h.insert("x-amz-security-token", token_value);
        }
        let mut auth_value: reqwest::header::HeaderValue = authorization.parse().map_err(|e| {
            Error::new(ErrorKind::Unexpected, "invalid Authorization header").with_source(e)
        })?;
        auth_value.set_sensitive(true);
        h.insert(reqwest::header::AUTHORIZATION, auth_value);
        Ok(())
    }
}

const RELOCATED_AMZ_DATE: reqwest::header::HeaderName =
    reqwest::header::HeaderName::from_static("original-x-amz-date");
const RELOCATED_CONTENT_SHA256: reqwest::header::HeaderName =
    reqwest::header::HeaderName::from_static("original-x-amz-content-sha256");
const RELOCATED_SECURITY_TOKEN: reqwest::header::HeaderName =
    reqwest::header::HeaderName::from_static("original-x-amz-security-token");

/// Moves `name`'s current values to `Original-<name>` when they differ from the
/// value about to be signed, so a caller's header is not silently dropped
/// (Java's `RESTSigV4AuthSession.updateRequestHeaders`).
fn relocate_conflicting(
    headers: &mut reqwest::header::HeaderMap,
    name: &'static str,
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
            .field("access_key_id", &self.credentials.access_key_id)
            .finish_non_exhaustive()
    }
}

/// Path normalization as AWS signers apply it (botocore `normalize_url_path`,
/// AWS SDK v2 `normalizePath`): duplicate slashes and `.` segments collapse,
/// `..` pops, a trailing slash is kept.
fn normalize_path(path: &str) -> String {
    let mut segments: Vec<&str> = Vec::new();
    for segment in path.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                segments.pop();
            }
            other => segments.push(other),
        }
    }
    let mut normalized = String::from("/");
    normalized.push_str(&segments.join("/"));
    if !normalized.ends_with('/')
        && (path.ends_with('/') || path.ends_with("/.") || path.ends_with("/.."))
    {
        normalized.push('/');
    }
    normalized
}

/// Decodes `%XX` sequences, leaving `+` alone: unlike form decoding, RFC 3986
/// has no special meaning for it, and neither does SigV4.
fn percent_decode(input: &str) -> String {
    fn hex(byte: u8) -> Option<u8> {
        (byte as char).to_digit(16).map(|digit| digit as u8)
    }
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(high), Some(low)) = (hex(bytes[i + 1]), hex(bytes[i + 2]))
        {
            out.push(high * 16 + low);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// RFC 3986 URI encoding (AWS rules). When `encode_slash` is false, `/` is kept.
fn uri_encode(s: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            b'/' if !encode_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const EMPTY_HEX: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    #[test]
    fn test_percent_decode_keeps_plus() {
        // `+` is a literal plus to SigV4, not a space: reqwest writes spaces
        // in query values as `+`, and the signature must match the wire.
        assert_eq!(percent_decode("my+wh"), "my+wh");
        assert_eq!(percent_decode("my%20wh"), "my wh");
        assert_eq!(percent_decode("a%2Fb"), "a/b");
        assert_eq!(percent_decode("bad%zz"), "bad%zz");
    }

    #[test]
    fn test_canonical_query_encodes_a_wire_plus() {
        let mut request = reqwest::Client::new()
            .get("https://rest.example.com/v1/namespaces")
            .query(&[("warehouse", "my wh")])
            .build()
            .unwrap();
        // reqwest wrote the space as `+`, so the canonical query must carry
        // it as `%2B` — form-decoding it to `%20` would break the signature.
        assert!(request.url().query().unwrap().contains("my+wh"));
        let signer = SigV4Signer::new(
            AwsCredentials {
                access_key_id: "ak".to_string(),
                secret_access_key: "sk".to_string().into(),
                session_token: None,
            },
            "us-east-1".to_string(),
            "execute-api".to_string(),
            PayloadHashMode::StandardAws,
        );
        signer.sign(&mut request).unwrap();
        let signed_headers = request
            .headers()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert!(signed_headers.contains("SignedHeaders="));
        // The wire query is untouched by signing.
        assert!(request.url().query().unwrap().contains("my+wh"));
    }

    #[test]
    fn test_normalize_path() {
        assert_eq!(normalize_path(""), "/");
        assert_eq!(normalize_path("/"), "/");
        assert_eq!(normalize_path("//v1/config"), "/v1/config");
        assert_eq!(normalize_path("/a/./b/../c"), "/a/c");
        assert_eq!(normalize_path("/a/b/"), "/a/b/");
    }

    #[test]
    fn content_sha256_header_iceberg_mode() {
        let v = content_sha256_header(b"hello", PayloadHashMode::IcebergRest);
        assert_eq!(v, "LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=");
        let e = content_sha256_header(b"", PayloadHashMode::IcebergRest);
        assert_eq!(e, EMPTY_HEX);
    }

    #[test]
    fn content_sha256_header_standard_mode() {
        let v = content_sha256_header(b"hello", PayloadHashMode::StandardAws);
        assert_eq!(
            v,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
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
    fn canonical_request_get_vanilla() {
        let headers = vec![
            ("host".to_string(), "example.amazonaws.com".to_string()),
            ("x-amz-date".to_string(), "20150830T123600Z".to_string()),
        ];
        let creq = canonical_request("GET", "/", "", &headers, &hex_sha256(b""));
        let expected = "GET\n/\n\n\
host:example.amazonaws.com\n\
x-amz-date:20150830T123600Z\n\
\n\
host;x-amz-date\n\
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assert_eq!(creq, expected);
        assert_eq!(
            hex_sha256(creq.as_bytes()),
            "bb579772317eb040ac9ed261061d46c1f17a8133879d6129b6e1c25292927e63"
        );
    }

    #[test]
    fn signs_request_iceberg_mode() {
        let creds = AwsCredentials {
            access_key_id: "AKIDEXAMPLE".into(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
                .to_string()
                .into(),
            session_token: Some("SESSIONTOKEN".to_string().into()),
        };
        let signer = SigV4Signer::new(
            creds,
            "us-east-1".into(),
            "glue".into(),
            PayloadHashMode::IcebergRest,
        );
        let client = reqwest::Client::new();
        let mut req = client
            .post("https://rest.example.com/v1/namespaces")
            .body("{}")
            .build()
            .unwrap();

        signer.sign(&mut req).unwrap();

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

    #[test]
    fn string_to_sign_get_vanilla() {
        let creq_hash = "bb579772317eb040ac9ed261061d46c1f17a8133879d6129b6e1c25292927e63";
        let headers = vec![
            ("host".to_string(), "example.amazonaws.com".to_string()),
            ("x-amz-date".to_string(), "20150830T123600Z".to_string()),
        ];
        let creq = canonical_request("GET", "/", "", &headers, &hex_sha256(b""));
        assert_eq!(hex_sha256(creq.as_bytes()), creq_hash);
        let sts = string_to_sign(
            "20150830T123600Z",
            "20150830/us-east-1/service/aws4_request",
            &creq,
        );
        assert!(sts.ends_with(creq_hash));
        assert!(sts.starts_with("AWS4-HMAC-SHA256\n20150830T123600Z\n"));
    }

    /// Empty body uses the hex constant and existing headers are signed too
    /// (mirrors Java's `TestRESTSigV4AuthSession::authenticateWithoutBody`).
    #[test]
    fn signs_empty_body_and_all_headers() {
        let creds = AwsCredentials {
            access_key_id: "AKIDEXAMPLE".into(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
                .to_string()
                .into(),
            session_token: None,
        };
        let signer = SigV4Signer::new(
            creds,
            "us-east-1".into(),
            "glue".into(),
            PayloadHashMode::IcebergRest,
        );
        let client = reqwest::Client::new();
        let mut req = client
            .get("https://rest.example.com/v1/config")
            .header("content-type", "application/json")
            .header("content-encoding", "gzip")
            .build()
            .unwrap();

        signer.sign(&mut req).unwrap();

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

        let creds = AwsCredentials {
            access_key_id: "AKIDEXAMPLE".into(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
                .to_string()
                .into(),
            session_token: None,
        };
        let signer = SigV4Signer::new(
            creds,
            "us-east-1".into(),
            "execute-api".into(),
            PayloadHashMode::IcebergRest,
        );
        let body = br#"{"namespace":["ns"]}"#;
        let mut req = reqwest::Client::new()
            .post("https://rest.example.com/v1/namespaces")
            .body(body.to_vec())
            .build()
            .unwrap();
        let now = Utc.with_ymd_and_hms(2015, 8, 30, 12, 36, 0).unwrap();

        signer.sign_at(&mut req, now).unwrap();

        let base64_hash = content_sha256_header(body, PayloadHashMode::IcebergRest);
        let hex_hash = hex_sha256(body);
        assert_eq!(
            req.headers().get("x-amz-content-sha256").unwrap(),
            base64_hash.as_str(),
            "the header carries the base64 checksum"
        );

        // Independently recompute with the hex hash; a signer that canonicalizes
        // the base64 header value instead won't match.
        let headers = vec![
            ("host".to_string(), "rest.example.com".to_string()),
            ("x-amz-content-sha256".to_string(), base64_hash),
            ("x-amz-date".to_string(), "20150830T123600Z".to_string()),
        ];
        let creq = canonical_request("POST", "/v1/namespaces", "", &headers, &hex_hash);
        let sts = string_to_sign(
            "20150830T123600Z",
            "20150830/us-east-1/execute-api/aws4_request",
            &creq,
        );
        let key = signing_key(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20150830",
            "us-east-1",
            "execute-api",
        );
        let expected = hex_hmac_sha256(&key, sts.as_bytes());
        assert!(
            req.headers()
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap()
                .ends_with(&format!("Signature={expected}")),
            "canonical request must hash the body in hex"
        );
    }

    #[test]
    fn signs_host_with_non_default_port() {
        use chrono::TimeZone;

        let creds = AwsCredentials {
            access_key_id: "AKIDEXAMPLE".into(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
                .to_string()
                .into(),
            session_token: None,
        };
        let signer = SigV4Signer::new(
            creds,
            "us-east-1".into(),
            "glue".into(),
            PayloadHashMode::IcebergRest,
        );
        let client = reqwest::Client::new();
        let mut req = client
            .get("https://rest.example.com:8181/v1/config")
            .build()
            .unwrap();
        let now = Utc.with_ymd_and_hms(2015, 8, 30, 12, 36, 0).unwrap();

        signer.sign_at(&mut req, now).unwrap();

        // Independently recompute the signature with `host:port` in the
        // canonical request; a signer that drops the port won't match.
        let headers = vec![
            ("host".to_string(), "rest.example.com:8181".to_string()),
            ("x-amz-content-sha256".to_string(), EMPTY_HEX.to_string()),
            ("x-amz-date".to_string(), "20150830T123600Z".to_string()),
        ];
        let creq = canonical_request("GET", "/v1/config", "", &headers, EMPTY_HEX);
        let sts = string_to_sign(
            "20150830T123600Z",
            "20150830/us-east-1/glue/aws4_request",
            &creq,
        );
        let key = signing_key(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20150830",
            "us-east-1",
            "glue",
        );
        let expected = hex_hmac_sha256(&key, sts.as_bytes());

        let auth = req
            .headers()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(auth.ends_with(&format!("Signature={expected}")), "{auth}");
    }

    /// AWS SDK v2 parity (`doubleUrlEncode`): the canonical URI encodes the
    /// serialized path once more — literal `,` becomes `%2C`, an encoded
    /// `%2C` becomes `%252C` — while plain paths stay byte-identical.
    #[test]
    fn canonical_uri_is_aws_double_encoded() {
        use chrono::TimeZone;

        assert_eq!(uri_encode("/v1/namespaces", false), "/v1/namespaces");
        assert_eq!(
            uri_encode("/v1/namespaces/a%2Cb/tables/x,y", false),
            "/v1/namespaces/a%252Cb/tables/x%2Cy"
        );

        let creds = AwsCredentials {
            access_key_id: "AKIDEXAMPLE".into(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
                .to_string()
                .into(),
            session_token: None,
        };
        let signer = SigV4Signer::new(
            creds,
            "us-east-1".into(),
            "glue".into(),
            PayloadHashMode::IcebergRest,
        );
        let client = reqwest::Client::new();
        let mut req = client
            .get("https://rest.example.com/v1/namespaces/a%2Cb/tables/x,y")
            .build()
            .unwrap();
        let now = Utc.with_ymd_and_hms(2015, 8, 30, 12, 36, 0).unwrap();

        signer.sign_at(&mut req, now).unwrap();

        let headers = vec![
            ("host".to_string(), "rest.example.com".to_string()),
            ("x-amz-content-sha256".to_string(), EMPTY_HEX.to_string()),
            ("x-amz-date".to_string(), "20150830T123600Z".to_string()),
        ];
        let creq = canonical_request(
            "GET",
            "/v1/namespaces/a%252Cb/tables/x%2Cy",
            "",
            &headers,
            EMPTY_HEX,
        );
        let sts = string_to_sign(
            "20150830T123600Z",
            "20150830/us-east-1/glue/aws4_request",
            &creq,
        );
        let key = signing_key(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20150830",
            "us-east-1",
            "glue",
        );
        let expected = hex_hmac_sha256(&key, sts.as_bytes());

        let auth = req
            .headers()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(auth.ends_with(&format!("Signature={expected}")), "{auth}");
    }

    #[test]
    fn signs_request_standard_mode_uses_hex_header() {
        let creds = AwsCredentials {
            access_key_id: "AKIDEXAMPLE".into(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
                .to_string()
                .into(),
            session_token: None,
        };
        let signer = SigV4Signer::new(
            creds,
            "us-east-1".into(),
            "glue".into(),
            PayloadHashMode::StandardAws,
        );
        let client = reqwest::Client::new();
        let mut req = client
            .post("https://rest.example.com/v1/namespaces")
            .body("hello")
            .build()
            .unwrap();

        signer.sign(&mut req).unwrap();

        // StandardAws keeps the header in hex.
        assert_eq!(
            req.headers().get("x-amz-content-sha256").unwrap(),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }
}
