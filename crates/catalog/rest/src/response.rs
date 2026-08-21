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

//! The response type REST catalog requests come back as.

use std::fmt::{Debug, Formatter};

use http::{HeaderMap, StatusCode};
use iceberg::Result;
use reqwest::Response;

use crate::client::format_headers_redacted;

/// A REST catalog response, read into memory.
///
/// The counterpart of [`HttpRequest`](crate::HttpRequest): it keeps the
/// concrete client type inside [`HttpClient`](crate::HttpClient) so callers
/// work with the stable `http` crate types instead. Catalog responses are
/// small JSON documents that every caller reads in full, so the body is
/// buffered rather than streamed.
#[derive(Clone)]
pub struct HttpResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Vec<u8>,
}

impl Debug for HttpResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // A token exchange answers with a credential in the body, and headers
        // may carry `set-cookie`, so both are held back.
        f.debug_struct("HttpResponse")
            .field("status", &self.status)
            // Always redacted: a response carries no config, and the
            // `disable-header-redaction` escape hatch is for the request side.
            .field("headers", &format_headers_redacted(&self.headers, false))
            .field("body", &format_args!("{} bytes", self.body.len()))
            .finish()
    }
}

impl HttpResponse {
    /// Reads `response` into memory.
    pub(crate) async fn read(response: Response) -> Result<Self> {
        let status = response.status();
        let headers = response.headers().clone();
        // `bytes()` builds its error without a URL, so keep the one the
        // response came from.
        let url = response.url().clone();
        Ok(Self {
            status,
            headers,
            body: response
                .bytes()
                .await
                .map_err(|err| err.with_url(url))?
                .to_vec(),
        })
    }

    /// Builds a response, e.g. to unit-test code that consumes one.
    pub fn new(status: StatusCode, headers: HeaderMap, body: Vec<u8>) -> Self {
        Self {
            status,
            headers,
            body,
        }
    }

    /// The response status.
    pub fn status(&self) -> StatusCode {
        self.status
    }

    /// The response headers.
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// The response body.
    pub fn body(&self) -> &[u8] {
        &self.body
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_debug_holds_back_secrets() {
        // The body of a token exchange is a credential, and `set-cookie` is
        // redacted everywhere else in this crate.
        let mut headers = HeaderMap::new();
        headers.insert("set-cookie", "session=secret".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        let debug = format!(
            "{:?}",
            HttpResponse::new(
                StatusCode::OK,
                headers,
                br#"{"access_token": "tok"}"#.to_vec()
            )
        );

        assert!(!debug.contains("secret"), "{debug}");
        assert!(!debug.contains("tok"), "{debug}");
        assert!(debug.contains("content-type"), "{debug}");
        assert!(debug.contains("200"), "{debug}");
    }

    #[test]
    fn test_new_exposes_what_it_was_built_from() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());
        let response = HttpResponse::new(StatusCode::OK, headers, "{}".as_bytes().to_vec());

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "application/json"
        );
        assert_eq!(response.body(), b"{}");
    }
}
