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

//! Session catalog API for Apache Iceberg.

use std::collections::HashMap;
use std::fmt::Debug;

use async_trait::async_trait;
#[cfg(test)]
use mockall::automock;
use typed_builder::TypedBuilder;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::table::Table;
use crate::{Namespace, NamespaceIdent, Result, TableCommit, TableCreation, TableIdent};

/// Context for a session.
///
/// # Example
/// ```rust
/// use iceberg::SessionContext;
///
/// let session = SessionContext::builder()
///     .identity("user123".to_string())
///     .build();
///
/// assert_eq!(session.identity(), Some("user123"));
/// assert!(!session.session_id().is_empty());
/// ```
#[derive(Clone, TypedBuilder)]
pub struct SessionContext {
    /// The unique identifier for this session.
    ///
    /// Note that the session_id may be used for caching session-scoped state
    /// and re-use of a session_id with different session context may result in
    /// unexpected behavior.
    #[builder(default=Uuid::new_v4().to_string())]
    session_id: String,

    /// An optional user or principal associated with the session.
    #[builder(default, setter(strip_option))]
    identity: Option<String>,

    #[builder(default)]
    properties: HashMap<String, String>,

    #[builder(default)]
    credentials: HashMap<String, Credential>,
}

/// Property keys whose values are secrets, or may embed them (headers,
/// connection strings, keys like `adls.account-key` or `s3.sse.key`).
fn is_sensitive_prop(key: &str) -> bool {
    // Keys carry no casing or separator contract (`AWS_SECRET_ACCESS_KEY`,
    // `clientSecret`), so match a lowercased, separator-free form.
    let compact: String = key
        .to_ascii_lowercase()
        .chars()
        .filter(|c| !matches!(c, '.' | '-' | '_'))
        .collect();
    // `authorization`, not the broader `auth`, so credential-free endpoints
    // like `oauth2-server-uri` stay visible.
    compact.starts_with("header")
        || compact == "auth"
        || compact == "sig"
        || [
            "authorization",
            "token",
            "credential",
            "secret",
            "password",
            "passwd",
            "pwd",
            "key",
            "cookie",
            "signature",
            "connectionstring",
        ]
        .iter()
        .any(|pattern| compact.contains(pattern))
}

/// Schemes where `@` separates a container from an account rather than
/// credentials from a host (`abfss://filesystem@account.dfs.core.windows.net`).
const CONTAINER_AT_SCHEMES: &[&str] = &["abfs", "abfss", "wasb", "wasbs"];

/// True when the value itself carries a credential, which happens under
/// innocuous keys: a URI with userinfo (`postgres://user:pass@host/db` as the
/// SQL catalog's `uri`) or a sensitive parameter (`?password=`, a signed URL).
///
/// Best-effort: driver-specific connection strings and URIs nested in a
/// parameter are not decomposed.
fn is_sensitive_value(value: &str) -> bool {
    // URI parsers drop tab and newline characters before parsing.
    let value: String = value
        .chars()
        .filter(|c| !matches!(c, '\t' | '\n' | '\r'))
        .collect();
    let lower = value.to_ascii_lowercase();
    let scheme = lower
        .trim_start()
        .split_once(':')
        .map_or("", |(scheme, _)| scheme);
    if CONTAINER_AT_SCHEMES.contains(&scheme) {
        return false;
    }

    // Userinfo, via the parser (which normalizes `https:user:pass@host`) and
    // via the raw authority (for opaque strings like `jdbc:postgresql://...`).
    let parsed = url::Url::parse(&value).ok();
    if let Some(url) = &parsed
        && (!url.username().is_empty() || url.password().is_some())
    {
        return true;
    }
    if let Some((_, rest)) = lower.split_once("://")
        && rest
            .split(['/', '\\', '?', '#'])
            .next()
            .unwrap_or(rest)
            .contains('@')
    {
        return true;
    }

    // Credential-bearing parameters in the query or the fragment.
    let sections = match &parsed {
        Some(url) => [url.query(), url.fragment()]
            .into_iter()
            .flatten()
            .map(str::to_ascii_lowercase)
            .collect(),
        None => Vec::new(),
    };
    sections.iter().any(|section| {
        section.split(['&', ';']).any(|pair| {
            pair.split_once('=').is_some_and(|(name, _)| {
                // Parsers accept percent-encoded names: `pass%77ord`.
                is_sensitive_prop(&percent_decode(name.trim()))
            })
        })
    })
}

/// Decodes `%XX` sequences (malformed ones pass through verbatim).
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
    String::from_utf8_lossy(&out).to_ascii_lowercase()
}

impl Debug for SessionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let properties: HashMap<&str, &str> = self
            .properties
            .iter()
            .map(|(key, value)| {
                let value = if is_sensitive_prop(key) || is_sensitive_value(value) {
                    "[REDACTED]"
                } else {
                    value.as_str()
                };
                (key.as_str(), value)
            })
            .collect();
        f.debug_struct("SessionContext")
            .field("session_id", &self.session_id)
            .field("identity", &self.identity)
            .field("properties", &properties)
            .field("credentials", &self.credentials)
            .finish()
    }
}

impl SessionContext {
    /// Creates a new unique but empty session.
    pub fn empty() -> Self {
        Self::builder().build()
    }

    /// Returns the identifier for this session.
    ///
    /// The identifier may be used for caching state within a session.
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Returns a string that identifies the current user or principal.
    pub fn identity(&self) -> Option<&str> {
        self.identity.as_deref()
    }

    /// Returns a map of properties currently set for the session.
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }

    /// Returns the session's credential map.
    pub fn credentials(&self) -> &HashMap<String, Credential> {
        &self.credentials
    }
}

/// A string-like type containing sensitive information such as passwords or tokens.
///
/// It is redacted from logs and automatically zeroized.
///
/// # Example
/// ```rust
/// use iceberg::Credential;
///
/// let sensitive_value = "my-pw-12345";
/// let credential = Credential::from(sensitive_value.to_string());
///
/// // Not contained in debug logs.
/// assert!(!format!("{:?}", credential).contains(sensitive_value));
/// ```
#[derive(Clone)]
pub struct Credential(Zeroizing<String>);

impl Credential {
    /// Returns the raw value of the credential.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Credential([REDACTED])")
    }
}

impl From<String> for Credential {
    fn from(value: String) -> Self {
        Self(Zeroizing::new(value))
    }
}

/// The catalog API for Iceberg Rust that includes session handling.
#[async_trait]
#[cfg_attr(test, automock)]
pub trait SessionCatalog: Debug + Send + Sync {
    /// List namespaces inside the catalog.
    async fn list_namespaces(
        &self,
        context: &SessionContext,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>>;

    /// Create a new namespace inside the catalog.
    async fn create_namespace(
        &self,
        context: &SessionContext,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace>;

    /// Get a namespace information from the catalog.
    async fn get_namespace(
        &self,
        context: &SessionContext,
        namespace: &NamespaceIdent,
    ) -> Result<Namespace>;

    /// Check if namespace exists in catalog.
    async fn namespace_exists(
        &self,
        context: &SessionContext,
        namespace: &NamespaceIdent,
    ) -> Result<bool>;

    /// Update a namespace inside the catalog.
    ///
    /// # Behavior
    ///
    /// The properties must be the full set of namespace.
    async fn update_namespace(
        &self,
        context: &SessionContext,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()>;

    /// Drop a namespace from the catalog, or returns error if it doesn't exist.
    async fn drop_namespace(
        &self,
        context: &SessionContext,
        namespace: &NamespaceIdent,
    ) -> Result<()>;

    /// List tables from namespace.
    async fn list_tables(
        &self,
        context: &SessionContext,
        namespace: &NamespaceIdent,
    ) -> Result<Vec<TableIdent>>;

    /// Create a new table inside the namespace.
    async fn create_table(
        &self,
        context: &SessionContext,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table>;

    /// Load table from the catalog.
    async fn load_table(&self, context: &SessionContext, table: &TableIdent) -> Result<Table>;

    /// Drop a table from the catalog, or returns error if it doesn't exist.
    async fn drop_table(&self, context: &SessionContext, table: &TableIdent) -> Result<()>;

    /// Drop a table from the catalog and delete the underlying table data.
    ///
    /// Implementations should load the table metadata, drop the table
    /// from the catalog, then delete all associated data and metadata files.
    /// The [`drop_table_data`](super::utils::drop_table_data) utility function can
    /// be used for the file cleanup step.
    async fn purge_table(&self, context: &SessionContext, table: &TableIdent) -> Result<()>;

    /// Check if a table exists in the catalog.
    async fn table_exists(&self, context: &SessionContext, table: &TableIdent) -> Result<bool>;

    /// Rename a table in the catalog.
    async fn rename_table(
        &self,
        context: &SessionContext,
        src: &TableIdent,
        dest: &TableIdent,
    ) -> Result<()>;

    /// Register an existing table to the catalog.
    async fn register_table(
        &self,
        context: &SessionContext,
        table: &TableIdent,
        metadata_location: String,
    ) -> Result<Table>;

    /// Update a table to the catalog.
    async fn update_table(&self, context: &SessionContext, commit: TableCommit) -> Result<Table>;
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use uuid::Uuid;

    use crate::{Credential, SessionCatalog, SessionContext};

    #[test]
    fn test_is_sensitive_prop_matching() {
        for key in [
            "token",
            "credential",
            "clientSecret",
            "password",
            "s3.sse.key",
            "AWS_SECRET_ACCESS_KEY",
            "connection-string",
            "connection_string",
            "connectionString",
            "adls.connection.string",
            "header.authorization",
            "Header_Authorization",
            "Authorization",
            "Proxy-Authorization",
            "auth",
            "Cookie",
            "Set-Cookie",
        ] {
            assert!(super::is_sensitive_prop(key), "should match: {key}");
        }
        for key in [
            "warehouse",
            "uri",
            "s3.region",
            "prefix",
            // Credential-free auth configuration stays visible.
            "oauth2-server-uri",
            "adls.authority-host",
        ] {
            assert!(!super::is_sensitive_prop(key), "should not match: {key}");
        }
    }

    #[test]
    fn test_is_sensitive_value_matching() {
        for value in [
            "postgres://user:password@host/db",
            "mysql://root@localhost:3306/iceberg",
            "https:user:pass@example.com/path",
            "jdbc:postgresql://user:pass@db/iceberg",
            "https://bucket.s3.amazonaws.com/key?X-Amz-Signature=abc123",
            "https://account.blob.core.windows.net/c?sv=2024&sig=abc",
            "postgresql://localhost/db?user=u&password=qp-secret",
            "postgresql://localhost/db?user=u&pass%77ord=enc-secret",
            "https://client.example/cb#access_token=frag-secret",
            "postgresql://localhost/db?pass\tword=tab-secret",
        ] {
            assert!(super::is_sensitive_value(value), "should match: {value}");
        }
        for value in [
            "https://catalog.example.com/iceberg",
            "https://host/path?region=us-east-1",
            "sqlite::memory:",
            "s3://bucket/warehouse",
            "user@example.com",
            "mailto:user@example.com",
            "https://docs.example.com/page#section-2",
            // Azure storage `@` separates container from account.
            "abfss://filesystem@account.dfs.core.windows.net/warehouse",
            "wasbs://container@account.blob.core.windows.net/path",
            " abfss://filesystem@account.dfs.core.windows.net/path",
        ] {
            assert!(
                !super::is_sensitive_value(value),
                "should not match: {value}"
            );
        }
    }

    #[test]
    fn test_percent_decode() {
        assert_eq!(super::percent_decode("pass%77ord"), "password");
        assert_eq!(super::percent_decode("PASS%57ORD"), "password");
        // Malformed or truncated escapes pass through verbatim.
        assert_eq!(super::percent_decode("pass%zzord"), "pass%zzord");
        assert_eq!(super::percent_decode("password%7"), "password%7");
        assert_eq!(super::percent_decode("password%"), "password%");
        // Invalid UTF-8 decodes lossily instead of panicking.
        assert!(!super::percent_decode("token%ff").is_empty());
    }

    #[test]
    fn test_session_context_debug_redacts_secret_properties() {
        let context = SessionContext::builder()
            .properties(HashMap::from([
                ("token".to_string(), "tok-secret".to_string()),
                ("s3.secret-access-key".to_string(), "sk-secret".to_string()),
                // Keys have no casing/separator contract.
                (
                    "AWS_SECRET_ACCESS_KEY".to_string(),
                    "env-secret".to_string(),
                ),
                ("clientSecret".to_string(), "camel-secret".to_string()),
                ("connectionString".to_string(), "cs-secret".to_string()),
                (
                    "adls.connection.string".to_string(),
                    "dotted-secret".to_string(),
                ),
                ("Header.Authorization".to_string(), "hdr-secret".to_string()),
                (
                    "Header_Authorization".to_string(),
                    "hdr2-secret".to_string(),
                ),
                // Redacted by value: the keys are innocuous, the values not.
                (
                    "uri".to_string(),
                    "postgres://user:uri-secret@host/db".to_string(),
                ),
                (
                    "endpoint".to_string(),
                    "https://host/o?password=query-secret".to_string(),
                ),
                (
                    "callback".to_string(),
                    "https://app/cb#access_token=frag-secret".to_string(),
                ),
                (
                    "download".to_string(),
                    "https://bucket/o?X-Amz-Signature=signed-secret".to_string(),
                ),
                ("warehouse".to_string(), "wh1".to_string()),
            ]))
            .credentials(HashMap::from([(
                "oauth2".to_string(),
                Credential::from("cred-secret".to_string()),
            )]))
            .build();

        let out = format!("{context:?}");
        assert!(!out.contains("tok-secret"));
        assert!(!out.contains("sk-secret"));
        assert!(!out.contains("env-secret"));
        assert!(!out.contains("camel-secret"));
        assert!(!out.contains("cs-secret"));
        assert!(!out.contains("dotted-secret"));
        assert!(!out.contains("hdr-secret"));
        assert!(!out.contains("hdr2-secret"));
        assert!(!out.contains("uri-secret"));
        assert!(!out.contains("query-secret"));
        assert!(!out.contains("frag-secret"));
        assert!(!out.contains("signed-secret"));
        assert!(!out.contains("cred-secret"));
        assert!(out.contains("[REDACTED]"));
        assert!(out.contains("wh1"));
    }

    #[test]
    fn test_empty_session_context_has_uuid_session_id() {
        let session = SessionContext::empty();
        let session_id = session.session_id();

        assert!(Uuid::parse_str(session_id).is_ok());
    }

    #[test]
    fn test_empty_sessions_get_unique_id() {
        let session1 = SessionContext::empty();
        let session2 = SessionContext::empty();

        assert_ne!(session1.session_id(), session2.session_id())
    }

    #[test]
    fn test_empty_sessions_get_unique_id_via_builder() {
        let session1 = SessionContext::builder().build();
        let session2 = SessionContext::builder().build();

        assert_ne!(session1.session_id(), session2.session_id());
    }

    #[test]
    fn test_session_with_credentials_does_not_display_them() {
        let sensitive_value = "my-pw-123456";
        let session = SessionContext::builder()
            .credentials(HashMap::from([(
                "key".to_string(),
                Credential::from(sensitive_value.to_string()),
            )]))
            .build();

        let logged = format!("{:?}", session);
        assert!(!logged.contains(sensitive_value))
    }

    #[test]
    fn test_credential_redacts_value() {
        let sensitive_value = "my-pw-12346";

        let logged = format!("{:?}", Credential::from(sensitive_value.to_string()));
        assert!(!logged.contains(sensitive_value));
    }

    #[test]
    fn test_types_are_send_sync() {
        assert_send_sync::<Credential>();
        assert_send_sync::<SessionContext>();
        assert_send_sync::<dyn SessionCatalog>();

        fn _dyn_compatible(_: &dyn SessionCatalog) {}
    }

    fn assert_send_sync<T: Send + Sync + ?Sized>() {}
}
