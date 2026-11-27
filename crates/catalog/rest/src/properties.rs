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
use std::sync::OnceLock;

use iceberg::properties::{BoolProp, DurationMsProp, U32Prop};
use iceberg::sensitive::SensitiveString;
use iceberg::{Error, ErrorKind, NamespaceIdent, Result, TableIdent};
use iceberg_property_macro::Properties;
use itertools::Itertools;
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
const PATH_V1: &str = "v1";

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
    #[property(key = REST_CATALOG_PROP_AUTH_TYPE, default = None, getter)]
    auth_type: Option<AuthType>,
    /// Configured bearer token.
    #[property(key = REST_CATALOG_PROP_TOKEN, default = None, getter)]
    token: Option<SensitiveString>,
    /// Configured OAuth2 client ID and secret.
    #[property(key = REST_CATALOG_PROP_CREDENTIAL, default = None, getter)]
    credential: Option<OAuth2Credential>,
    /// Explicit OAuth2 token endpoint.
    #[property(key = REST_CATALOG_PROP_OAUTH2_SERVER_URI, default = None, getter)]
    oauth2_server_uri: Option<String>,
    /// Explicit OAuth2 scope.
    #[property(key = REST_CATALOG_PROP_SCOPE, default = None, getter)]
    scope: Option<String>,
    /// OAuth2 audience.
    #[property(key = REST_CATALOG_PROP_AUDIENCE, default = None, getter)]
    audience: Option<String>,
    /// OAuth2 resource.
    #[property(key = REST_CATALOG_PROP_RESOURCE, default = None, getter)]
    resource: Option<String>,
    /// Explicit headers added to requests.
    #[property(nested, getter)]
    headers: RestHeaders,
    /// Lazily initialized catalog endpoints.
    #[property(nested, getter)]
    endpoint_cache: RestEndpointCache,
    /// Whether header redaction is disabled.
    #[property(
        key = REST_CATALOG_PROP_DISABLE_HEADER_REDACTION,
        default = BoolProp::from(false),
        getter
    )]
    disable_header_redaction: BoolProp,
    /// Requested page size for namespace and table listings.
    #[property(key = REST_CATALOG_PROP_PAGE_SIZE, default = None, getter)]
    page_size: Option<U32Prop<1, { i32::MAX as u32 }>>,
    /// Maximum number of cached tables.
    #[property(key = REST_CATALOG_PROP_TABLE_CACHE_MAX_ENTRIES, default = TABLE_CACHE_MAX_ENTRIES_DEFAULT, getter)]
    table_cache_max_entries: u64,
    /// How long a loaded table stays cached.
    #[property(
        key = REST_CATALOG_PROP_TABLE_CACHE_EXPIRE_AFTER_WRITE_MS,
        default = DurationMsProp::try_from(TABLE_CACHE_EXPIRE_AFTER_WRITE_MS_DEFAULT).unwrap(),
        getter
    )]
    table_cache_expire_after_write: DurationMsProp<1, TABLE_CACHE_EXPIRE_AFTER_WRITE_MAX_MS>,
}

/// OAuth2 client credentials.
// PartialEq supports comparing credentials in property parsing tests.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct OAuth2Credential {
    client_id: Option<String>,
    client_secret: SensitiveString,
}

impl OAuth2Credential {
    /// Creates a credential from a client ID and secret.
    pub(crate) fn new(client_id: Option<String>, client_secret: SensitiveString) -> Self {
        Self {
            client_id,
            client_secret,
        }
    }

    /// Returns the optional client ID.
    pub(crate) fn client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    /// Returns the client secret.
    pub(crate) fn client_secret(&self) -> &SensitiveString {
        &self.client_secret
    }
}

impl FromStr for OAuth2Credential {
    type Err = std::convert::Infallible;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        Ok(match value.split_once(':') {
            Some((id, secret)) => Self::new(Some(id.to_string()), secret.to_string().into()),
            None => Self::new(None, value.to_string().into()),
        })
    }
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
            .field("token", &self.token)
            .field("credential", &self.credential)
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

    fn url_prefixed(&self, parts: &[&str]) -> String {
        let base = self.endpoint_cache.base.get_or_init(|| {
            [self.uri().as_str(), PATH_V1]
                .into_iter()
                .chain(self.prefix().as_deref())
                .join("/")
        });
        std::iter::once(base.as_str())
            .chain(parts.iter().copied())
            .join("/")
    }

    pub(crate) fn config_endpoint(&self) -> &str {
        self.endpoint_cache
            .config
            .get_or_init(|| [self.uri().as_str(), PATH_V1, "config"].join("/"))
    }

    pub(crate) fn get_token_endpoint(&self) -> &str {
        self.oauth2_server_uri()
            .as_deref()
            .unwrap_or_else(|| self.default_token_endpoint())
    }

    /// Returns the OAuth2 token endpoint derived from the catalog URI.
    pub(crate) fn default_token_endpoint(&self) -> &str {
        self.endpoint_cache
            .token
            .get_or_init(|| [self.uri().as_str(), PATH_V1, "oauth", "tokens"].join("/"))
    }

    pub(crate) fn namespaces_endpoint(&self) -> &str {
        self.endpoint_cache
            .namespaces
            .get_or_init(|| self.url_prefixed(&["namespaces"]))
    }

    pub(crate) fn namespace_endpoint(&self, ns: &NamespaceIdent) -> String {
        self.url_prefixed(&["namespaces", &ns.to_url_string()])
    }

    pub(crate) fn tables_endpoint(&self, ns: &NamespaceIdent) -> String {
        self.url_prefixed(&["namespaces", &ns.to_url_string(), "tables"])
    }

    pub(crate) fn rename_table_endpoint(&self) -> &str {
        self.endpoint_cache
            .rename_table
            .get_or_init(|| self.url_prefixed(&["tables", "rename"]))
    }

    pub(crate) fn register_table_endpoint(&self, ns: &NamespaceIdent) -> String {
        self.url_prefixed(&["namespaces", &ns.to_url_string(), "register"])
    }

    pub(crate) fn table_endpoint(&self, table: &TableIdent) -> String {
        self.url_prefixed(&[
            "namespaces",
            &table.namespace.to_url_string(),
            "tables",
            &table.name,
        ])
    }

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
        headers.extend(self.headers.0.clone());
        headers
    }

    pub(crate) fn extra_oauth_params(&self) -> HashMap<String, String> {
        let mut params = self.explicit_oauth_params();
        params
            .entry(REST_CATALOG_PROP_SCOPE.to_string())
            .or_insert_with(|| "catalog".to_string());
        params
    }

    pub(crate) fn explicit_oauth_params(&self) -> HashMap<String, String> {
        [
            (REST_CATALOG_PROP_SCOPE, self.scope()),
            (REST_CATALOG_PROP_AUDIENCE, self.audience()),
            (REST_CATALOG_PROP_RESOURCE, self.resource()),
        ]
        .into_iter()
        .filter_map(|(key, value)| value.as_ref().map(|value| (key.to_string(), value.clone())))
        .collect()
    }

    pub(crate) fn resolved_auth_type(&self) -> &AuthType {
        self.auth_type().as_ref().unwrap_or_else(|| {
            if self.token.is_some() || self.credential.is_some() || self.oauth2_server_uri.is_some()
            {
                &AuthType::OAuth2
            } else {
                &AuthType::None
            }
        })
    }
}

#[derive(Clone, Default)]
pub(crate) struct RestEndpointCache {
    base: OnceLock<String>,
    config: OnceLock<String>,
    token: OnceLock<String>,
    namespaces: OnceLock<String>,
    rename_table: OnceLock<String>,
}

impl RestEndpointCache {
    fn from_properties(_props: &HashMap<String, String>) -> Result<Self> {
        Ok(Self::default())
    }
}

// Endpoints derive only from immutable properties; cache initialization must
// not change configuration equality.
impl PartialEq for RestEndpointCache {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

// PartialEq supports whole-configuration assertions in parsing tests.
#[derive(Clone, PartialEq)]
pub(crate) struct RestHeaders(pub(crate) HeaderMap);

impl RestHeaders {
    fn from_properties(props: &HashMap<String, String>) -> Result<Self> {
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
            ("token", "secret"),
            ("credential", "client:secret"),
            ("oauth2-server-uri", "http://localhost:8181/token"),
            ("scope", "custom-scope"),
            ("audience", "custom-audience"),
            ("resource", "custom-resource"),
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
            auth_type: Some(AuthType::OAuth2),
            token: Some("secret".to_string().into()),
            credential: Some(OAuth2Credential {
                client_id: Some("client".to_string()),
                client_secret: "secret".to_string().into(),
            }),
            oauth2_server_uri: Some("http://localhost:8181/token".to_string()),
            scope: Some("custom-scope".to_string()),
            audience: Some("custom-audience".to_string()),
            resource: Some("custom-resource".to_string()),
            headers: RestHeaders(HeaderMap::from_iter([(
                HeaderName::from_static("x-custom"),
                HeaderValue::from_static("value"),
            )])),
            endpoint_cache: RestEndpointCache::default(),
            disable_header_redaction: BoolProp::from(true),
            page_size: Some(U32Prop::try_from(25).unwrap()),
            table_cache_max_entries: 10,
            table_cache_expire_after_write: DurationMsProp::try_from(500).unwrap(),
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
            auth_type: None,
            token: None,
            credential: None,
            oauth2_server_uri: None,
            scope: None,
            audience: None,
            resource: None,
            headers: RestHeaders(HeaderMap::new()),
            endpoint_cache: RestEndpointCache::default(),
            disable_header_redaction: BoolProp::from(false),
            page_size: None,
            table_cache_max_entries: 100,
            table_cache_expire_after_write: DurationMsProp::try_from(300_000).unwrap(),
        };

        assert_eq!(
            RestCatalogProperties::from_properties(&HashMap::new()).unwrap(),
            expected
        );
    }

    #[test]
    fn test_endpoint_cache_initialization_and_equality() {
        let default_token = "http://localhost:8181/v1/oauth/tokens";
        for (token, expected_token) in [
            (None, default_token),
            (Some(""), ""),
            (
                Some("http://localhost:8181/token"),
                "http://localhost:8181/token",
            ),
        ] {
            let mut props = HashMap::from([
                (
                    REST_CATALOG_PROP_URI.to_string(),
                    "http://localhost:8181".to_string(),
                ),
                (REST_CATALOG_PROP_PREFIX.to_string(), "catalog".to_string()),
            ]);
            if let Some(token) = token {
                props.insert(
                    REST_CATALOG_PROP_OAUTH2_SERVER_URI.to_string(),
                    token.to_string(),
                );
            }
            let properties = RestCatalogProperties::from_properties(&props).unwrap();
            let unchanged = properties.clone();
            let cache = properties.endpoint_cache();
            let cells = [
                &cache.base,
                &cache.config,
                &cache.token,
                &cache.namespaces,
                &cache.rename_table,
            ];
            assert_eq!(cells.map(OnceLock::get), [None; 5]);

            assert_eq!(properties.get_token_endpoint(), expected_token);
            let expected_cached = [
                None,
                None,
                token.is_none().then_some(default_token),
                None,
                None,
            ];
            assert_eq!(
                cells.map(|cell| cell.get().map(String::as_str)),
                expected_cached
            );
            assert_eq!(
                [
                    properties.config_endpoint(),
                    properties.namespaces_endpoint(),
                    properties.rename_table_endpoint(),
                    properties.default_token_endpoint(),
                ],
                [
                    "http://localhost:8181/v1/config",
                    "http://localhost:8181/v1/catalog/namespaces",
                    "http://localhost:8181/v1/catalog/tables/rename",
                    default_token,
                ],
            );
            assert_eq!(properties, unchanged);
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

    #[test]
    fn test_credential_parsing_and_redaction_fallback() {
        for (value, client_id, secret) in [
            ("secret", None, "secret"),
            (
                "client:secret:with:colons",
                Some("client"),
                "secret:with:colons",
            ),
            (":secret", Some(""), "secret"),
            ("client:", Some("client"), ""),
            ("", None, ""),
        ] {
            let properties = RestCatalogProperties::from_properties(&HashMap::from([
                ("credential".to_string(), value.to_string()),
                (
                    REST_CATALOG_PROP_DISABLE_HEADER_REDACTION.to_string(),
                    "invalid".to_string(),
                ),
            ]))
            .unwrap();
            let expected = (
                Some(OAuth2Credential {
                    client_id: client_id.map(str::to_string),
                    client_secret: secret.to_string().into(),
                }),
                false,
            );
            assert_eq!(
                (
                    properties.credential().clone(),
                    properties.disable_header_redaction().get()
                ),
                expected,
            );
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
        for value in ["0", "31536000000001"] {
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
