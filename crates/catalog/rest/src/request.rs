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

//! The request type REST catalog authentication works with.

use http::{HeaderMap, Method};
use iceberg::Result;
#[cfg(feature = "sigv4")]
use reqwest::Url;
use reqwest::{Request, RequestBuilder};

/// An outgoing REST request being authenticated by an
/// [`AuthSession`](crate::AuthSession).
///
/// Wraps the request so a session mutates it through the stable
/// `http` crate types rather than the concrete request type the REST catalog
/// uses internally.
pub struct HttpRequest {
    inner: Request,
}

impl HttpRequest {
    /// Wraps a request, e.g. to unit-test a custom
    /// [`AuthSession`](crate::AuthSession).
    pub fn new(inner: Request) -> Self {
        Self { inner }
    }

    /// Builds the request `builder` describes.
    pub(crate) fn build(builder: RequestBuilder) -> Result<Self> {
        Ok(Self::new(builder.build()?))
    }

    /// The wrapped request, for the client that sends it.
    pub(crate) fn into_inner(self) -> Request {
        self.inner
    }

    /// The request URL.
    #[cfg(feature = "sigv4")]
    pub(crate) fn url(&self) -> &Url {
        self.inner.url()
    }

    /// The mutable request URL, for signers that must rewrite it before
    /// canonicalizing, e.g. to drop userinfo the wire `Host` never carries.
    #[cfg(feature = "sigv4")]
    pub(crate) fn url_mut(&mut self) -> &mut Url {
        self.inner.url_mut()
    }

    /// The request method.
    pub fn method(&self) -> &Method {
        self.inner.method()
    }

    /// The request URL, as a string (scheme, host, path and query).
    pub fn url_str(&self) -> &str {
        self.inner.url().as_str()
    }

    /// The request headers.
    pub fn headers(&self) -> &HeaderMap {
        self.inner.headers()
    }

    /// The mutable request headers, e.g. to add an `Authorization` header.
    pub fn headers_mut(&mut self) -> &mut HeaderMap {
        self.inner.headers_mut()
    }

    /// The request body, distinguishing an absent body from a streaming one:
    /// signers can sign [`HttpRequestBody::Empty`] (empty-payload hash) and
    /// [`HttpRequestBody::Buffered`], but not [`HttpRequestBody::Streaming`].
    pub fn body(&self) -> HttpRequestBody<'_> {
        match self.inner.body() {
            None => HttpRequestBody::Empty,
            Some(body) => match body.as_bytes() {
                Some(bytes) => HttpRequestBody::Buffered(bytes),
                None => HttpRequestBody::Streaming,
            },
        }
    }
}

/// The body of an [`HttpRequest`], as seen by authentication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpRequestBody<'a> {
    /// No body is set.
    Empty,
    /// An in-memory body.
    Buffered(&'a [u8]),
    /// A streaming body, whose bytes are not available for e.g. signing.
    Streaming,
}

impl<'a> HttpRequestBody<'a> {
    /// The signable bytes: empty for [`Self::Empty`], the buffer for
    /// [`Self::Buffered`], and `None` for [`Self::Streaming`].
    pub fn as_bytes(&self) -> Option<&'a [u8]> {
        match self {
            HttpRequestBody::Empty => Some(&[]),
            HttpRequestBody::Buffered(bytes) => Some(bytes),
            HttpRequestBody::Streaming => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use reqwest::Client;

    use super::*;

    #[test]
    fn test_http_request_body_states() {
        let client = Client::new();

        // No body at all.
        let req = client
            .get("https://rest.example.com/v1/config")
            .build()
            .unwrap();
        let http_req = HttpRequest::new(req);
        let body = http_req.body();
        assert_eq!(body, HttpRequestBody::Empty);
        assert_eq!(body.as_bytes(), Some(&[] as &[u8]));

        // An in-memory body.
        let req = client
            .post("https://rest.example.com/v1/namespaces")
            .body("{}")
            .build()
            .unwrap();
        let http_req = HttpRequest::new(req);
        let body = http_req.body();
        assert_eq!(body, HttpRequestBody::Buffered(b"{}"));
        assert_eq!(body.as_bytes(), Some(b"{}" as &[u8]));

        // A streaming body: bytes are unavailable, so it must not sign as empty.
        let req = client
            .post("https://rest.example.com/v1/namespaces")
            .body(reqwest::Body::wrap_stream(futures::stream::once(async {
                Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"chunk"))
            })))
            .build()
            .unwrap();
        let http_req = HttpRequest::new(req);
        let body = http_req.body();
        assert_eq!(body, HttpRequestBody::Streaming);
        assert_eq!(body.as_bytes(), None);
    }
}
