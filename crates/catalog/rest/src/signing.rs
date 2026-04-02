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

//! HTTP request signing for the REST catalog.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::{Error, ErrorKind, Result};
use reqsign_aws_v4::{Credential, DefaultCredentialProvider, RequestSigner};
use reqsign_core::{Context, HttpSend, Signer};
use reqsign_file_read_tokio::TokioFileRead;
use reqwest::{Client, Request};

use crate::RestCatalogConfig;

/// A trait for signing HTTP requests.
#[async_trait]
pub(crate) trait HttpRequestSigner: Send + Sync + Debug {
    /// Sign the request by modifying its headers (and potentially other parts).
    async fn sign(&self, parts: &mut http::request::Parts) -> Result<()>;

    /// Sign a full [`reqwest::Request`] by converting it to [`http::request::Parts`] and back.
    async fn sign_request(&self, mut request: Request) -> Result<Request> {
        // We have to use a builder to convert a reqwest::Request to http::request::Parts.
        let (mut parts, _) = http::Request::builder()
            .method(request.method().clone())
            .uri(request.url().as_str())
            .body(())
            .expect("request parts derived from a valid request")
            .into_parts();
        parts.headers = request.headers().clone();
        self.sign(&mut parts).await?;
        *request.headers_mut() = parts.headers;
        Ok(request)
    }
}

/// Try to build a request signer from the config, or return `None` if signing is not enabled.
impl TryFrom<&RestCatalogConfig> for Option<Arc<dyn HttpRequestSigner>> {
    type Error = Error;

    fn try_from(cfg: &RestCatalogConfig) -> Result<Self> {
        if !cfg.sigv4_enabled() {
            return Ok(None);
        }
        let signing_region = cfg.signing_region().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "'{}' is required when '{}' is true",
                    crate::REST_CATALOG_PROP_SIGNING_REGION,
                    crate::REST_CATALOG_PROP_SIGV4_ENABLED,
                ),
            )
        })?;
        let signing_name = cfg.signing_name().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "'{}' is required when '{}' is true",
                    crate::REST_CATALOG_PROP_SIGNING_NAME,
                    crate::REST_CATALOG_PROP_SIGV4_ENABLED,
                ),
            )
        })?;
        Ok(Some(Arc::new(SigV4Signer::new(
            cfg.client().unwrap_or_default(),
            signing_name,
            signing_region,
        ))))
    }
}

/// The HttpRequestSigner implementation for AWS SigV4
#[derive(Debug)]
pub(crate) struct SigV4Signer {
    /// The inner reqwest signer with an AWS credential loader.
    inner: Signer<Credential>,
}

impl SigV4Signer {
    pub(crate) fn new(client: Client, service: &str, region: &str) -> Self {
        let ctx = Context::new()
            .with_file_read(TokioFileRead)
            .with_http_send(ReqwestHttpSend(client));
        let loader = DefaultCredentialProvider::new();
        let signer = RequestSigner::new(service, region);
        Self {
            inner: Signer::new(ctx, loader, signer),
        }
    }
}

#[async_trait]
impl HttpRequestSigner for SigV4Signer {
    async fn sign(&self, parts: &mut http::request::Parts) -> Result<()> {
        // Patch the URI for signing; reqsign-aws-v4 workaround.
        let uri = parts.uri.clone();
        parts.uri = patch_uri_for_signing(&uri)?;
        // Sign with the patched URI.
        self.inner.sign(parts, None).await.map_err(|e| {
            Error::new(ErrorKind::Unexpected, "Failed to sign request with SigV4").with_source(e)
        })?;
        // Restore the original URI in the request; signing should only modify headers.
        parts.uri = uri;
        Ok(())
    }
}

/// Pre-encode percent signs in the URI path for correct SigV4 canonical URI computation.
///
/// Workaround for a bug in `reqsign-aws-v4` where `canonical_request_string` decodes
/// percent-encoded path segments then re-encodes them, losing the double-encoding that
/// AWS SigV4 requires. By replacing `%` with `%25` before signing, reqsign's
/// decode→reencode cycle produces the correct double-encoded form.
///
/// TODO: remove once fixed upstream in apache/opendal (reqsign-aws-v4).
fn patch_uri_for_signing(uri: &http::Uri) -> Result<http::Uri> {
    let path = uri.path().replace('%', "%25");
    let paq = if let Some(query) = uri.query() {
        format!("{path}?{query}")
    } else {
        path
    };
    let mut parts = uri.clone().into_parts();
    parts.path_and_query = Some(paq.parse().map_err(|e| {
        Error::new(ErrorKind::Unexpected, "failed to rebuild URI for signing").with_source(e)
    })?);
    http::Uri::from_parts(parts).map_err(|e| {
        Error::new(ErrorKind::Unexpected, "failed to rebuild URI for signing").with_source(e)
    })
}

/// Bridges reqwest 0.12 with `reqsign_core::HttpSend`.
///
/// The published `reqsign-http-send-reqwest` crate requires reqwest >=0.13,
/// which is incompatible with the workspace, so we provide a minimal adapter.
#[derive(Debug)]
struct ReqwestHttpSend(Client);

/// Implements `HttpSend` for a reqwest 0.12 client.
impl HttpSend for ReqwestHttpSend {
    async fn http_send(
        &self,
        req: http::Request<bytes::Bytes>,
    ) -> reqsign_core::Result<http::Response<bytes::Bytes>> {
        let req = Request::try_from(req).map_err(|e| {
            reqsign_core::Error::unexpected("failed to convert request").with_source(e)
        })?;
        let resp = self.0.execute(req).await.map_err(|e| {
            reqsign_core::Error::unexpected("failed to send request").with_source(e)
        })?;
        let status = resp.status();
        let headers = resp.headers().clone();
        let body = resp.bytes().await.map_err(|e| {
            reqsign_core::Error::unexpected("failed to read response body").with_source(e)
        })?;
        let mut response = http::Response::builder()
            .status(status)
            .body(body)
            .map_err(|e| {
                reqsign_core::Error::unexpected("failed to build response").with_source(e)
            })?;
        *response.headers_mut() = headers;
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_patch_uri_simple_path_unchanged() {
        let uri: http::Uri = "https://glue.us-east-1.amazonaws.com/iceberg/v1/namespaces"
            .parse()
            .unwrap();
        let patched = patch_uri_for_signing(&uri).unwrap();
        assert_eq!(patched.path(), "/iceberg/v1/namespaces");
    }

    #[test]
    fn test_patch_uri_arn_path_double_encodes() {
        // S3 Tables ARN path with percent-encoded colons
        let uri: http::Uri =
            "https://s3tables.us-east-1.amazonaws.com/iceberg/v1/arn%3Aaws%3As3tables%3Aus-east-1%3A123456789012%3Abucket/my-table/namespaces"
                .parse()
                .unwrap();
        let patched = patch_uri_for_signing(&uri).unwrap();
        assert_eq!(
            patched.path(),
            "/iceberg/v1/arn%253Aaws%253As3tables%253Aus-east-1%253A123456789012%253Abucket/my-table/namespaces"
        );
    }

    #[test]
    fn test_patch_uri_preserves_query() {
        let uri: http::Uri =
            "https://example.com/iceberg/v1/arn%3Aaws%3As3tables?pageToken=abc&pageSize=10"
                .parse()
                .unwrap();
        let patched = patch_uri_for_signing(&uri).unwrap();
        assert_eq!(patched.path(), "/iceberg/v1/arn%253Aaws%253As3tables");
        assert_eq!(patched.query(), Some("pageToken=abc&pageSize=10"));
    }

    #[test]
    fn test_patch_uri_preserves_authority() {
        let uri: http::Uri = "https://s3tables.us-east-1.amazonaws.com/iceberg/v1/arn%3Aaws"
            .parse()
            .unwrap();
        let patched = patch_uri_for_signing(&uri).unwrap();
        assert_eq!(
            patched.authority().unwrap().as_str(),
            "s3tables.us-east-1.amazonaws.com"
        );
        assert_eq!(patched.scheme_str(), Some("https"));
    }

    #[test]
    fn test_patch_uri_root_path() {
        let uri: http::Uri = "https://example.com/".parse().unwrap();
        let patched = patch_uri_for_signing(&uri).unwrap();
        assert_eq!(patched.path(), "/");
    }
}
