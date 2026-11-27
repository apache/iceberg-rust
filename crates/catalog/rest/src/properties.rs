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

//! Typed REST catalog properties and property keys.

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::str::FromStr;
use std::time::Duration;

use iceberg::{Error, ErrorKind, Result};
use iceberg_property_macro::Properties;
use reqwest::header::{self, HeaderMap, HeaderName, HeaderValue};

use crate::auth::{AUTH_TYPE_NONE, AUTH_TYPE_OAUTH2};

/// REST catalog URI
pub const REST_CATALOG_PROP_URI: &str = "uri";
/// REST catalog warehouse location
pub const REST_CATALOG_PROP_WAREHOUSE: &str = "warehouse";
/// REST catalog endpoint prefix.
pub const REST_CATALOG_PROP_PREFIX: &str = "prefix";
/// Bearer token used to authenticate REST catalog requests.
pub const REST_CATALOG_PROP_TOKEN: &str = "token";
/// OAuth2 client secret, optionally prefixed by a client ID and colon.
pub const REST_CATALOG_PROP_CREDENTIAL: &str = "credential";
/// OAuth2 token endpoint URI.
pub const REST_CATALOG_PROP_OAUTH2_SERVER_URI: &str = "oauth2-server-uri";
/// OAuth2 scope requested when exchanging credentials.
pub const REST_CATALOG_PROP_SCOPE: &str = "scope";
/// OAuth2 audience requested when exchanging credentials.
pub const REST_CATALOG_PROP_AUDIENCE: &str = "audience";
/// OAuth2 resource requested when exchanging credentials.
pub const REST_CATALOG_PROP_RESOURCE: &str = "resource";
/// Requested page size for namespace and table listings; see
/// [`crate::RestSessionCatalogBuilder::with_page_size`].
pub const REST_CATALOG_PROP_PAGE_SIZE: &str = "rest-page-size";
/// Disable header redaction in error logs and `Debug` output (defaults to
/// false for security)
pub const REST_CATALOG_PROP_DISABLE_HEADER_REDACTION: &str = "disable-header-redaction";
/// Authentication scheme: `none` or `oauth2`. When unset, `oauth2` is used
/// if a `token`, `credential` or `oauth2-server-uri` is configured, `none`
/// otherwise.
pub const REST_CATALOG_PROP_AUTH_TYPE: &str = "rest.auth.type";
/// Maximum number of loaded tables kept for freshness-aware loading (defaults
/// to 100; 0 disables the cache). The cache is keyed by session id, so it only
/// helps callers that reuse a [`iceberg::SessionContext`], and a table changed through
/// this catalog is invalidated only for the session that changed it. Loads that
/// vend short-lived storage credentials are never cached.
pub const REST_CATALOG_PROP_TABLE_CACHE_MAX_ENTRIES: &str = "rest-table-cache.max-entries";
/// How long a loaded table stays cached for freshness-aware loading, in
/// milliseconds (defaults to 5 minutes; must be positive and at most 1000 years).
pub const REST_CATALOG_PROP_TABLE_CACHE_EXPIRE_AFTER_WRITE_MS: &str =
    "rest-table-cache.expire-after-write-ms";

const REST_CATALOG_PROP_HEADER_PREFIX: &str = "header.";

const TABLE_CACHE_MAX_ENTRIES_DEFAULT: u64 = 100;
const TABLE_CACHE_EXPIRE_AFTER_WRITE_MS_DEFAULT: u64 = 5 * 60 * 1000;
/// moka panics on a time-to-live beyond 1000 years.
const TABLE_CACHE_EXPIRE_AFTER_WRITE_MAX_MS: u64 = 1000 * 365 * 24 * 3600 * 1000;

const ICEBERG_REST_SPEC_VERSION: &str = "0.14.1";
const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Typed REST catalog properties.
// PartialEq supports whole-configuration assertions in parsing tests.
#[derive(Clone, Properties, PartialEq)]
pub(crate) struct RestCatalogProperties {
    /// Catalog URI.
    #[property(key = REST_CATALOG_PROP_URI, default = "", getter)]
    uri: String,
    /// Warehouse location or identifier.
    #[property(key = REST_CATALOG_PROP_WAREHOUSE, default = None, getter)]
    warehouse: Option<String>,
    /// Catalog endpoint prefix.
    #[property(key = REST_CATALOG_PROP_PREFIX, default = None, getter)]
    prefix: Option<String>,
    /// Explicit authentication scheme.
    #[property(key = REST_CATALOG_PROP_AUTH_TYPE, default = AuthType::None, getter)]
    auth_type: AuthType,
    /// Explicit headers added to requests.
    #[property(nested, getter)]
    headers: RestHeaders,
    /// Whether header redaction is disabled.
    #[property(
        key = REST_CATALOG_PROP_DISABLE_HEADER_REDACTION,
        default = false,
        parse_with = parse_disable_header_redaction,
        getter
    )]
    disable_header_redaction: bool,
    /// Requested page size for namespace and table listings.
    #[property(key = REST_CATALOG_PROP_PAGE_SIZE, default = None, parse_with = parse_page_size, getter)]
    page_size: Option<u32>,
    /// Maximum number of cached tables.
    #[property(key = REST_CATALOG_PROP_TABLE_CACHE_MAX_ENTRIES, default = TABLE_CACHE_MAX_ENTRIES_DEFAULT, getter)]
    table_cache_max_entries: u64,
    /// How long a loaded table stays cached.
    #[property(
        key = REST_CATALOG_PROP_TABLE_CACHE_EXPIRE_AFTER_WRITE_MS,
        default = Duration::from_millis(TABLE_CACHE_EXPIRE_AFTER_WRITE_MS_DEFAULT),
        parse_with = parse_table_cache_expiry,
        getter
    )]
    table_cache_expire_after_write: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AuthType {
    None,
    OAuth2,
    Custom(String),
}

impl FromStr for AuthType {
    type Err = std::convert::Infallible;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        Ok(match value.to_ascii_lowercase().as_str() {
            AUTH_TYPE_NONE => Self::None,
            AUTH_TYPE_OAUTH2 => Self::OAuth2,
            other => Self::Custom(other.to_string()),
        })
    }
}

impl Debug for RestCatalogProperties {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestCatalogProperties")
            .field("uri", &self.uri)
            .field("warehouse", &self.warehouse)
            .field("prefix", &self.prefix)
            .field("auth_type", &self.auth_type)
            .field("headers", &"[REDACTED]")
            .field("page_size", &self.page_size)
            .field("table_cache_max_entries", &self.table_cache_max_entries)
            .field(
                "table_cache_expire_after_write",
                &self.table_cache_expire_after_write,
            )
            .finish_non_exhaustive()
    }
}

impl RestCatalogProperties {
    pub(crate) fn for_handshake(props: &HashMap<String, String>) -> Result<Self> {
        // These settings are used only after the handshake. Server overrides may
        // replace invalid client values before they are parsed.
        let mut props = props.clone();
        for key in [
            REST_CATALOG_PROP_PAGE_SIZE,
            REST_CATALOG_PROP_TABLE_CACHE_MAX_ENTRIES,
            REST_CATALOG_PROP_TABLE_CACHE_EXPIRE_AFTER_WRITE_MS,
        ] {
            props.remove(key);
        }
        Self::from_properties(&props)
    }

    pub(crate) fn extra_headers(&self) -> HeaderMap {
        self.headers.extra_headers()
    }
}

// PartialEq supports whole-configuration assertions in parsing tests.
#[derive(Clone, PartialEq)]
pub(crate) struct RestHeaders(pub(crate) HeaderMap);

impl RestHeaders {
    pub(crate) fn extra_headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::from_iter([
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            ),
            (
                HeaderName::from_static("x-client-version"),
                HeaderValue::from_static(ICEBERG_REST_SPEC_VERSION),
            ),
            (
                header::USER_AGENT,
                HeaderValue::from_str(&format!("iceberg-rust/{CARGO_PKG_VERSION}")).unwrap(),
            ),
        ]);
        headers.extend(self.0.clone());
        headers
    }

    pub(crate) fn from_properties(props: &HashMap<String, String>) -> Result<Self> {
        let mut headers = HeaderMap::new();
        for (key, value) in props.iter().filter_map(|(key, value)| {
            key.strip_prefix(REST_CATALOG_PROP_HEADER_PREFIX)
                .map(|key| (key, value))
        }) {
            headers.insert(
                HeaderName::from_str(key).map_err(|error| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid header name: {key}"),
                    )
                    .with_source(error)
                })?,
                HeaderValue::from_str(value).map_err(|error| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid value for header: {key}"),
                    )
                    .with_source(error)
                })?,
            );
        }
        Ok(Self(headers))
    }
}

fn parse_disable_header_redaction(value: &str) -> Result<bool> {
    Ok(value.eq_ignore_ascii_case("true"))
}

fn parse_page_size(value: &str) -> Result<u32> {
    value
        .parse::<u32>()
        .ok()
        .filter(|value| (1..=i32::MAX as u32).contains(value))
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Invalid {REST_CATALOG_PROP_PAGE_SIZE}: {value}; expected an integer between 1 and {}",
                    i32::MAX
                ),
            )
        })
}

fn parse_table_cache_expiry(value: &str) -> Result<Duration> {
    value
        .parse::<u64>()
        .ok()
        .filter(|value| (1..=TABLE_CACHE_EXPIRE_AFTER_WRITE_MAX_MS).contains(value))
        .map(Duration::from_millis)
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Invalid {REST_CATALOG_PROP_TABLE_CACHE_EXPIRE_AFTER_WRITE_MS}: {value}; \
                     expected milliseconds between 1 and {TABLE_CACHE_EXPIRE_AFTER_WRITE_MAX_MS}"
                ),
            )
        })
}

#[cfg(test)]
mod tests {
    use mockito::Server;

    use super::*;
    use crate::catalog::RestCatalogTestFixture;

    #[test]
    fn test_catalog_properties() {
        let props = HashMap::from([
            (REST_CATALOG_PROP_URI, "http://localhost:8181"),
            (REST_CATALOG_PROP_WAREHOUSE, "s3://warehouse"),
            ("prefix", "catalog"),
            (REST_CATALOG_PROP_AUTH_TYPE, "OAuth2"),
            ("header.x-custom", "value"),
            (REST_CATALOG_PROP_DISABLE_HEADER_REDACTION, "TRUE"),
            (REST_CATALOG_PROP_PAGE_SIZE, "25"),
            (REST_CATALOG_PROP_TABLE_CACHE_MAX_ENTRIES, "10"),
            (REST_CATALOG_PROP_TABLE_CACHE_EXPIRE_AFTER_WRITE_MS, "500"),
            ("custom.property", "retained-by-catalog"),
        ])
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
        let expected = RestCatalogProperties {
            uri: "http://localhost:8181".to_string(),
            warehouse: Some("s3://warehouse".to_string()),
            prefix: Some("catalog".to_string()),
            auth_type: AuthType::OAuth2,
            headers: RestHeaders(HeaderMap::from_iter([(
                HeaderName::from_static("x-custom"),
                HeaderValue::from_static("value"),
            )])),
            disable_header_redaction: true,
            page_size: Some(25),
            table_cache_max_entries: 10,
            table_cache_expire_after_write: Duration::from_millis(500),
        };

        assert_eq!(
            RestCatalogProperties::from_properties(&props).unwrap(),
            expected
        );
    }

    #[test]
    fn test_catalog_properties_defaults() {
        let expected = RestCatalogProperties {
            uri: String::new(),
            warehouse: None,
            prefix: None,
            auth_type: AuthType::None,
            headers: RestHeaders(HeaderMap::new()),
            disable_header_redaction: false,
            page_size: None,
            table_cache_max_entries: 100,
            table_cache_expire_after_write: Duration::from_millis(300_000),
        };

        assert_eq!(
            RestCatalogProperties::from_properties(&HashMap::new()).unwrap(),
            expected
        );
    }

    #[test]
    fn test_header_redaction_boolean() {
        for (value, expected) in [
            ("true", true),
            ("TRUE", true),
            ("True", true),
            ("false", false),
            ("FALSE", false),
            ("", false),
            ("invalid", false),
            ("1", false),
            (" true ", false),
        ] {
            let properties = RestCatalogProperties::from_properties(&HashMap::from([(
                REST_CATALOG_PROP_DISABLE_HEADER_REDACTION.to_string(),
                value.to_string(),
            )]))
            .unwrap();
            assert_eq!(properties.disable_header_redaction(), expected);
        }
    }

    #[test]
    fn test_numeric_property_boundaries() {
        for (page_size, expiry) in [
            (1, 1),
            (i32::MAX as u32, TABLE_CACHE_EXPIRE_AFTER_WRITE_MAX_MS),
        ] {
            let properties = RestCatalogProperties::from_properties(&HashMap::from([
                (
                    REST_CATALOG_PROP_PAGE_SIZE.to_string(),
                    page_size.to_string(),
                ),
                (
                    REST_CATALOG_PROP_TABLE_CACHE_EXPIRE_AFTER_WRITE_MS.to_string(),
                    expiry.to_string(),
                ),
            ]))
            .unwrap();
            assert_eq!(
                (
                    properties.page_size(),
                    *properties.table_cache_expire_after_write()
                ),
                (Some(page_size), Duration::from_millis(expiry)),
            );
        }
    }

    #[test]
    fn test_catalog_properties_invalid_values() {
        for (key, value) in [
            (REST_CATALOG_PROP_TABLE_CACHE_MAX_ENTRIES, "-1"),
            (
                REST_CATALOG_PROP_TABLE_CACHE_MAX_ENTRIES,
                "18446744073709551616",
            ),
            (
                REST_CATALOG_PROP_TABLE_CACHE_EXPIRE_AFTER_WRITE_MS,
                "invalid",
            ),
            (REST_CATALOG_PROP_TABLE_CACHE_EXPIRE_AFTER_WRITE_MS, "-1"),
            ("header.invalid name", "value"),
            ("header.authorization", "secret\nvalue"),
        ] {
            let error = RestCatalogProperties::from_properties(&HashMap::from([(
                key.to_string(),
                value.to_string(),
            )]))
            .unwrap_err();
            assert_eq!(error.kind(), ErrorKind::DataInvalid);
            assert!(format!("{error}").contains(key.strip_prefix("header.").unwrap_or(key)));
            assert!(!format!("{error:?}").contains("secret\nvalue"));
        }
    }

    #[tokio::test]
    async fn test_http_headers() {
        let server = Server::new_async().await;
        let mut props = HashMap::new();
        props.insert("credential".to_string(), "client1:secret1".to_string());

        let config = RestCatalogTestFixture::builder()
            .uri(server.url())
            .props(props)
            .build();
        let headers: HeaderMap = config.properties().unwrap().extra_headers();

        let expected_headers = HeaderMap::from_iter([
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            ),
            (
                HeaderName::from_static("x-client-version"),
                HeaderValue::from_static(ICEBERG_REST_SPEC_VERSION),
            ),
            (
                header::USER_AGENT,
                HeaderValue::from_str(&format!("iceberg-rust/{CARGO_PKG_VERSION}")).unwrap(),
            ),
        ]);
        assert_eq!(headers, expected_headers);
    }

    #[tokio::test]
    async fn test_http_headers_with_custom_headers() {
        let server = Server::new_async().await;
        let mut props = HashMap::new();
        props.insert("credential".to_string(), "client1:secret1".to_string());
        props.insert(
            "header.content-type".to_string(),
            "application/yaml".to_string(),
        );
        props.insert(
            "header.customized-header".to_string(),
            "some/value".to_string(),
        );

        let config = RestCatalogTestFixture::builder()
            .uri(server.url())
            .props(props)
            .build();
        let headers: HeaderMap = config.properties().unwrap().extra_headers();

        let expected_headers = HeaderMap::from_iter([
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/yaml"),
            ),
            (
                HeaderName::from_static("x-client-version"),
                HeaderValue::from_static(ICEBERG_REST_SPEC_VERSION),
            ),
            (
                header::USER_AGENT,
                HeaderValue::from_str(&format!("iceberg-rust/{CARGO_PKG_VERSION}")).unwrap(),
            ),
            (
                HeaderName::from_static("customized-header"),
                HeaderValue::from_static("some/value"),
            ),
        ]);
        assert_eq!(headers, expected_headers);
    }

    #[test]
    fn test_table_cache_rejects_out_of_range_expiry() {
        for value in ["", "0", "31536000000001", "1.5", "18446744073709551616"] {
            let config = RestCatalogTestFixture::builder()
                .uri("http://localhost".to_string())
                .props(HashMap::from([(
                    REST_CATALOG_PROP_TABLE_CACHE_EXPIRE_AFTER_WRITE_MS.to_string(),
                    value.to_string(),
                )]))
                .build();

            assert_eq!(
                config.properties().unwrap_err().kind(),
                ErrorKind::DataInvalid
            );
        }
    }
}
