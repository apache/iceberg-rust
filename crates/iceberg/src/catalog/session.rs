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
use std::sync::Arc;

use async_trait::async_trait;
#[cfg(test)]
use mockall::automock;
use typed_builder::TypedBuilder;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::table::Table;
use crate::{Catalog, Namespace, NamespaceIdent, Result, TableCommit, TableCreation, TableIdent};

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
#[derive(Debug, Clone, TypedBuilder)]
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

impl dyn SessionCatalog {
    /// Bind this catalog to a session, exposing the ordinary Catalog API.
    ///
    /// # Example
    /// ```
    /// # fn into_catalog(session_catalog: Arc<dyn SessionCatalog>, id: String) {
    /// let session = SessionContext::builder().session_id(id).build();
    ///
    /// // Use the plain catalog API for the duration of this session.
    /// let catalog = session_catalog.into_catalog(session);
    /// # let _ = catalog;
    /// # }
    /// ```
    pub fn into_catalog(self: Arc<Self>, session: SessionContext) -> Arc<dyn Catalog> {
        Arc::new(SessionBoundCatalog {
            inner: self,
            session,
        })
    }
}

/// Allows any [`SessionCatalog`] to implement the [`Catalog`] trait.
#[derive(Debug)]
struct SessionBoundCatalog {
    inner: Arc<dyn SessionCatalog>,
    session: SessionContext,
}

#[async_trait]
impl Catalog for SessionBoundCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(&self.session, parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        self.inner
            .create_namespace(&self.session, namespace, properties)
            .await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        self.inner.get_namespace(&self.session, namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        self.inner.namespace_exists(&self.session, namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        self.inner
            .update_namespace(&self.session, namespace, properties)
            .await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        self.inner.drop_namespace(&self.session, namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        self.inner.list_tables(&self.session, namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        self.inner
            .create_table(&self.session, namespace, creation)
            .await
    }

    async fn load_table(&self, table: &TableIdent) -> Result<Table> {
        self.inner.load_table(&self.session, table).await
    }

    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        self.inner.drop_table(&self.session, table).await
    }

    async fn purge_table(&self, table: &TableIdent) -> Result<()> {
        self.inner.purge_table(&self.session, table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        self.inner.table_exists(&self.session, table).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        self.inner.rename_table(&self.session, src, dest).await
    }

    async fn register_table(&self, table: &TableIdent, metadata_location: String) -> Result<Table> {
        self.inner
            .register_table(&self.session, table, metadata_location)
            .await
    }

    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        self.inner.update_table(&self.session, commit).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use uuid::Uuid;

    use crate::{Credential, SessionCatalog, SessionContext};

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
