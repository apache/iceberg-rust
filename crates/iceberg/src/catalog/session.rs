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
use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use typed_builder::TypedBuilder;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::io::StorageFactory;
use crate::runtime::Runtime;
use crate::table::Table;
use crate::{Namespace, NamespaceIdent, Result, TableCommit, TableCreation, TableIdent};

/// Context for a session.
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

/// Common interface for all session catalog builders.
pub trait SessionCatalogBuilder: Default + Debug + Send + Sync {
    /// The session catalog type that this builder creates.
    type C: SessionCatalog;

    /// Set a custom StorageFactory to use for storage operations.
    ///
    /// When a StorageFactory is provided, the catalog will use it to build FileIO
    /// instances for all storage operations instead of using the default factory.
    ///
    /// # Arguments
    ///
    /// * `storage_factory` - The StorageFactory to use for creating storage instances
    fn with_storage_factory(self, storage_factory: Arc<dyn StorageFactory>) -> Self;

    /// Set a custom tokio Runtime to use for spawning async tasks.
    ///
    /// When a Runtime is provided, the catalog will propagate it to all tables
    /// it creates. Tasks such as scan planning and delete file processing
    /// will be spawned on this runtime.
    fn with_runtime(self, runtime: Runtime) -> Self;

    /// Create a new session catalog instance.
    fn load(
        self,
        name: impl Into<String>,
        props: HashMap<String, String>,
    ) -> impl Future<Output = Result<Self::C>> + Send;
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::{Credential, SessionContext};

    #[test]
    fn test_empty_session_context_has_uuid_session_id() {
        let session = SessionContext::empty();
        let session_id = session.session_id();

        assert!(Uuid::parse_str(session_id).is_ok());
    }

    #[test]
    fn test_credential_redacts_value() {
        let sensitive_value = "my-pw-12346";

        let logged = format!("{:?}", Credential::from(sensitive_value.to_string()));
        assert!(!logged.contains(sensitive_value));
    }
}
