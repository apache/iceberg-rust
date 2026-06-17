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
use iceberg::{Error, ErrorKind, Result};
use reqsign_core::hash::{base64_encode, hex_hmac_sha256, hex_sha256, hmac_sha256};
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
    pub secret_access_key: String,
    /// Optional STS session token.
    pub session_token: Option<String>,
}

/// Signs outgoing REST-catalog requests.
pub trait HttpRequestSigner: Send + Sync + std::fmt::Debug {
    /// Signs `request` in place.
    fn sign(&self, request: &mut reqwest::Request) -> Result<()>;
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

    fn sign_at(&self, request: &mut reqwest::Request, now: DateTime<Utc>) -> Result<()> {
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date = now.format("%Y%m%d").to_string();
        let scope = format!("{date}/{}/{}/aws4_request", self.region, self.service);

        let host = request
            .url()
            .host_str()
            .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "request url has no host"))?
            .to_string();
        let canonical_uri = request.url().path().to_string();
        let mut encoded_pairs: Vec<(String, String)> = request
            .url()
            .query_pairs()
            .map(|(k, v)| (uri_encode(&k, true), uri_encode(&v, true)))
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
            headers.push(("x-amz-security-token".to_string(), tok.clone()));
        }

        // Sign every other request header too (except a blacklist), as Iceberg
        // Java / botocore do; signing only host/x-amz-* yields a signature mismatch.
        const SKIP_HEADERS: &[&str] = &["user-agent", "authorization", "expect", "x-amzn-trace-id"];
        for (name, value) in request.headers().iter() {
            let lname = name.as_str().to_ascii_lowercase();
            if SKIP_HEADERS.contains(&lname.as_str()) || headers.iter().any(|(k, _)| *k == lname) {
                continue;
            }
            if let Ok(v) = value.to_str() {
                headers.push((lname, v.trim().to_string()));
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
            &self.credentials.secret_access_key,
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
        h.insert("x-amz-date", amz_date.parse().unwrap());
        h.insert("x-amz-content-sha256", content_header.parse().unwrap());
        if let Some(tok) = &self.credentials.session_token {
            h.insert(
                "x-amz-security-token",
                tok.parse().map_err(|e| {
                    Error::new(ErrorKind::DataInvalid, "invalid session token").with_source(e)
                })?,
            );
        }
        h.insert(
            reqwest::header::AUTHORIZATION,
            authorization.parse().map_err(|e| {
                Error::new(ErrorKind::Unexpected, "invalid Authorization header").with_source(e)
            })?,
        );
        Ok(())
    }
}

impl HttpRequestSigner for SigV4Signer {
    fn sign(&self, request: &mut reqwest::Request) -> Result<()> {
        self.sign_at(request, Utc::now())
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
        let sig = reqsign_core::hash::hex_hmac_sha256(&key, string_to_sign.as_bytes());
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
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into(),
            session_token: Some("SESSIONTOKEN".into()),
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
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into(),
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

    #[test]
    fn signs_request_standard_mode_uses_hex_header() {
        let creds = AwsCredentials {
            access_key_id: "AKIDEXAMPLE".into(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into(),
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
