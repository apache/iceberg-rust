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

//! Server capability negotiation via the `endpoints` field of `GET /v1/config`.
//!
//! A REST server may advertise the set of routes it supports in the `endpoints`
//! field of its configuration response, letting clients negotiate optional
//! capabilities instead of assuming every server implements every operation.
//! Each entry is a `"{method} {path}"` string, for example
//! `"POST /v1/{prefix}/namespaces/{namespace}/tables"`; parse one through
//! [`Endpoint`]'s [`FromStr`] implementation.
//!
//! Use [`RestCatalog::supports_endpoint`](crate::RestCatalog::supports_endpoint)
//! to check whether the connected server advertised a given [`Endpoint`].

use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use iceberg::{Error, ErrorKind};
use reqwest::Method;
use serde::de::{Error as DeError, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A single route a REST server advertises support for, parsed from the
/// `endpoints` field of `GET /v1/config`.
///
/// The wire form is `"{method} {path}"` — an HTTP method and a path template
/// separated by a single space, e.g.
/// `"POST /v1/{prefix}/namespaces/{namespace}/tables"`. Parse one with
/// [`str::parse`]: the method is validated and normalized, and the path is the
/// template the server advertises (with `{prefix}`, `{namespace}`, …
/// placeholders), compared verbatim.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Endpoint {
    method: Method,
    path: String,
}

impl Endpoint {
    /// The HTTP method, e.g. `GET` or `POST`.
    pub fn method(&self) -> &str {
        self.method.as_str()
    }

    /// The path template, e.g. `/v1/{prefix}/namespaces`.
    pub fn path(&self) -> &str {
        &self.path
    }
}

impl FromStr for Endpoint {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        // The wire form is exactly `"<method> <path>"` separated by a single
        // space; paths never contain spaces, so require exactly two non-empty
        // parts and a valid HTTP method.
        let mut parts = s.split(' ');
        match (parts.next(), parts.next(), parts.next()) {
            (Some(method), Some(path), None) if !method.is_empty() && !path.is_empty() => {
                let method = Method::from_str(&method.to_ascii_uppercase()).map_err(|_| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("invalid HTTP method in endpoint: {s:?}"),
                    )
                })?;
                Ok(Self {
                    method,
                    path: path.to_string(),
                })
            }
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    r#"invalid endpoint {s:?}: expected "<method> <path>" separated by a single space"#
                ),
            )),
        }
    }
}

impl Display for Endpoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.method, self.path)
    }
}

impl Serialize for Endpoint {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Endpoint {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where D: Deserializer<'de> {
        struct EndpointVisitor;

        impl Visitor<'_> for EndpointVisitor {
            type Value = Endpoint;

            fn expecting(&self, f: &mut Formatter<'_>) -> fmt::Result {
                f.write_str(r#"an endpoint string of the form "<method> <path>""#)
            }

            fn visit_str<E>(self, v: &str) -> std::result::Result<Endpoint, E>
            where E: DeError {
                Endpoint::from_str(v).map_err(E::custom)
            }
        }

        deserializer.deserialize_str(EndpointVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_round_trips() {
        let ep: Endpoint = "POST /v1/{prefix}/namespaces/{namespace}/tables"
            .parse()
            .unwrap();
        assert_eq!(ep.method(), "POST");
        assert_eq!(ep.path(), "/v1/{prefix}/namespaces/{namespace}/tables");

        let json = serde_json::to_string(&ep).unwrap();
        assert_eq!(json, r#""POST /v1/{prefix}/namespaces/{namespace}/tables""#);
        assert_eq!(serde_json::from_str::<Endpoint>(&json).unwrap(), ep);
    }

    #[test]
    fn rejects_malformed_endpoints() {
        for invalid in ["GET", "GET  /v1", " GET /v1", "GET ", " /v1", ""] {
            assert!(
                invalid.parse::<Endpoint>().is_err(),
                "expected {invalid:?} to be rejected"
            );
        }
    }

    #[test]
    fn normalizes_http_method_to_uppercase() {
        assert_eq!("get /v1/x".parse::<Endpoint>().unwrap().method(), "GET");
    }
}
