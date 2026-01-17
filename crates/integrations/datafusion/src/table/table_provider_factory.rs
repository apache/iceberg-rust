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

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider, TableProviderFactory};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::CreateExternalTable;
use datafusion::sql::TableReference;
use iceberg::io::FileIO;
use iceberg::table::StaticTable;
use iceberg::{Catalog, Error, ErrorKind, NamespaceIdent, Result, TableIdent};

use super::{IcebergStaticTableProvider, IcebergTableProvider};
use crate::to_datafusion_error;

/// A factory that implements DataFusion's `TableProviderFactory` to create Iceberg table providers.
///
/// This factory supports two modes of operation:
///
/// 1. **Catalog-backed mode**: When constructed with a catalog via `new_with_catalog()`, creates
///    `IcebergTableProvider` instances backed by the catalog with full read/write support.
///
/// 2. **Static mode**: When constructed without a catalog via `new()`, creates static
///    `IcebergStaticTableProvider` instances from metadata file paths (backward compatible behavior).
///
/// /// # Example (Catalog-backed Mode)
///
/// ```ignore
/// use std::sync::Arc;
///
/// use datafusion::execution::session_state::SessionStateBuilder;
/// use datafusion::prelude::*;
/// use iceberg_datafusion::IcebergTableProviderFactory;
///
/// #[tokio::main]
/// async fn main() {
///     // Assume `catalog` is a pre-built Iceberg catalog (e.g., REST, Glue, HMS)
///     let catalog: Arc<dyn iceberg::Catalog> = /* ... */;
///
///     let mut state = SessionStateBuilder::new().with_default_features().build();
///
///     // Register the factory with an injected catalog
///     state.table_factories_mut().insert(
///         "ICEBERG".to_string(),
///         Arc::new(IcebergTableProviderFactory::new_with_catalog(catalog)),
///     );
///
///     let ctx = SessionContext::new_with_state(state);
///
///     // Create an external table backed by the catalog
///     // The table name is used to look up the table in the catalog
///     ctx.sql("CREATE EXTERNAL TABLE my_ns.my_table STORED AS ICEBERG LOCATION ''")
///         .await
///         .expect("Failed to create table");
/// }
/// ```
///
/// # Example (Static Mode)
///
/// The following example demonstrates how to create an Iceberg external table using SQL in
/// a DataFusion session with `IcebergTableProviderFactory`:
///
/// ```
/// use std::sync::Arc;
///
/// use datafusion::execution::session_state::SessionStateBuilder;
/// use datafusion::prelude::*;
/// use datafusion::sql::TableReference;
/// use iceberg_datafusion::IcebergTableProviderFactory;
///
/// #[tokio::main]
/// async fn main() {
///     // Create a new session context
///     let mut state = SessionStateBuilder::new().with_default_features().build();
///
///     // Register the IcebergTableProviderFactory in the session
///     state.table_factories_mut().insert(
///         "ICEBERG".to_string(),
///         Arc::new(IcebergTableProviderFactory::new()),
///     );
///
///     let ctx = SessionContext::new_with_state(state);
///
///     // Define the table reference and the location of the Iceberg metadata file
///     let table_ref = TableReference::bare("my_iceberg_table");
///     // /path/to/iceberg/metadata
///     let metadata_file_path = format!(
///         "{}/testdata/table_metadata/{}",
///         env!("CARGO_MANIFEST_DIR"),
///         "TableMetadataV2.json"
///     );
///
///     // SQL command to create the Iceberg external table
///     let sql = format!(
///         "CREATE EXTERNAL TABLE {} STORED AS ICEBERG LOCATION '{}'",
///         table_ref, metadata_file_path
///     );
///
///     // Execute the SQL to create the external table
///     ctx.sql(&sql).await.expect("Failed to create table");
///
///     // Verify the table was created by retrieving the table provider
///     let table_provider = ctx
///         .table_provider(table_ref)
///         .await
///         .expect("Table not found");
///
///     println!("Iceberg external table created successfully.");
/// }
/// ```
///
/// # Note
/// This factory is designed to work with the DataFusion query engine,
/// specifically for handling Iceberg tables in external table commands.
/// In static mode, only reading Iceberg tables is supported.
/// In catalog-backed mode, both reading and writing are supported.
///
/// # Errors
/// An error will be returned if any unsupported feature, such as partition columns,
/// order expressions, constraints, or column defaults, is detected in the table creation command.
#[derive(Debug, Clone)]
pub struct IcebergTableProviderFactory {
    /// Optional catalog for creating catalog-backed table providers.
    /// When None, falls back to static table provider creation.
    catalog: Option<Arc<dyn Catalog>>,
}

impl IcebergTableProviderFactory {
    /// Creates a new factory without a catalog.
    ///
    /// Tables created by this factory will be static (read-only) providers
    /// loaded from metadata file paths specified in LOCATION.
    pub fn new() -> Self {
        Self { catalog: None }
    }

    /// Creates a new factory with an injected catalog.
    ///
    /// Tables created by this factory will be catalog-backed providers
    /// with full read/write support. The table name from the `CREATE EXTERNAL TABLE`
    /// command will be used to look up the table in the catalog.
    ///
    /// # Arguments
    ///
    /// * `catalog` - An Iceberg catalog instance
    pub fn new_with_catalog(catalog: Arc<dyn Catalog>) -> Self {
        Self {
            catalog: Some(catalog),
        }
    }
}

impl Default for IcebergTableProviderFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TableProviderFactory for IcebergTableProviderFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> DFResult<Arc<dyn TableProvider>> {
        check_cmd(cmd).map_err(to_datafusion_error)?;

        let table_name = &cmd.name;
        let table_name_with_ns = complement_namespace_if_necessary(table_name);

        match &self.catalog {
            Some(catalog) => {
                // Catalog-backed: create IcebergTableProvider
                let (namespace, name) = parse_table_reference(&table_name_with_ns);
                let table_ident = TableIdent::new(namespace.clone(), name.clone());

                // Check if table exists before attempting to load
                if !catalog
                    .table_exists(&table_ident)
                    .await
                    .map_err(to_datafusion_error)?
                {
                    return Err(to_datafusion_error(Error::new(
                        ErrorKind::TableNotFound,
                        format!("Table '{table_ident}' not found in catalog"),
                    )));
                }

                let provider = IcebergTableProvider::try_new(catalog.clone(), namespace, name)
                    .await
                    .map_err(to_datafusion_error)?;
                Ok(Arc::new(provider))
            }
            None => {
                // Static: create IcebergStaticTableProvider
                let metadata_file_path = &cmd.location;
                let options = &cmd.options;

                let table = create_static_table(table_name_with_ns, metadata_file_path, options)
                    .await
                    .map_err(to_datafusion_error)?
                    .into_table();

                let provider = IcebergStaticTableProvider::try_new_from_table(table)
                    .await
                    .map_err(to_datafusion_error)?;

                Ok(Arc::new(provider))
            }
        }
    }
}

fn check_cmd(cmd: &CreateExternalTable) -> Result<()> {
    let CreateExternalTable {
        schema,
        table_partition_cols,
        order_exprs,
        constraints,
        column_defaults,
        ..
    } = cmd;

    // Check if any of the fields violate the constraints in a single condition
    let is_invalid = !schema.fields().is_empty()
        || !table_partition_cols.is_empty()
        || !order_exprs.is_empty()
        || !constraints.is_empty()
        || !column_defaults.is_empty();

    if is_invalid {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Currently we only support reading existing icebergs tables in external table command. To create new table, please use catalog provider.",
        ));
    }

    Ok(())
}

/// Parses a TableReference into namespace and table name components.
///
/// This function extracts the namespace and table name from a DataFusion `TableReference`,
/// following these rules:
/// - `Bare` names (e.g., `my_table`) use the "default" namespace
/// - `Partial` names (e.g., `my_ns.my_table`) use the schema as the namespace
/// - `Full` names (e.g., `catalog.my_ns.my_table`) use the schema as the namespace,
///   ignoring the catalog component (which is the DataFusion catalog, not the Iceberg catalog)
fn parse_table_reference(table_ref: &TableReference) -> (NamespaceIdent, String) {
    match table_ref {
        TableReference::Bare { table } => (
            NamespaceIdent::new("default".to_string()),
            table.to_string(),
        ),
        TableReference::Partial { schema, table } => {
            (NamespaceIdent::new(schema.to_string()), table.to_string())
        }
        TableReference::Full { schema, table, .. } => {
            // For fully qualified names, use schema as namespace
            // (catalog is typically the DataFusion catalog, not Iceberg catalog)
            (NamespaceIdent::new(schema.to_string()), table.to_string())
        }
    }
}

/// Complements the namespace of a table name if necessary.
///
/// # Note
/// If the table name is a bare name, it will be complemented with the 'default' namespace.
/// Otherwise, it will be returned as is. Because Iceberg tables are always namespaced, but DataFusion
/// external table commands maybe not include the namespace, this function ensures that the namespace is always present.
///
/// # See also
/// - [`iceberg::NamespaceIdent`]
/// - [`datafusion::sql::planner::SqlToRel::external_table_to_plan`]
fn complement_namespace_if_necessary(table_name: &TableReference) -> Cow<'_, TableReference> {
    match table_name {
        TableReference::Bare { table } => {
            Cow::Owned(TableReference::partial("default", table.as_ref()))
        }
        other => Cow::Borrowed(other),
    }
}

async fn create_static_table(
    table_name: Cow<'_, TableReference>,
    metadata_file_path: &str,
    props: &HashMap<String, String>,
) -> Result<StaticTable> {
    let table_ident = TableIdent::from_strs(table_name.to_vec())?;
    let file_io = FileIO::from_path(metadata_file_path)?
        .with_props(props)
        .build()?;
    StaticTable::from_metadata_file(metadata_file_path, table_ident, file_io).await
}

#[cfg(test)]
mod tests {

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::TableProviderFactory;
    use datafusion::common::{Constraints, DFSchema};
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::logical_expr::CreateExternalTable;
    use datafusion::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::TableReference;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
    use iceberg::{CatalogBuilder, TableCreation};
    use tempfile::TempDir;

    use super::*;

    fn table_metadata_v2_schema() -> Schema {
        Schema::new(vec![
            Field::new("x", DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("y", DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
            Field::new("z", DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "3".to_string(),
            )])),
        ])
    }

    fn table_metadata_location() -> String {
        format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2.json"
        )
    }

    fn create_external_table_cmd() -> CreateExternalTable {
        let metadata_file_path = table_metadata_location();

        CreateExternalTable {
            name: TableReference::partial("static_ns", "static_table"),
            location: metadata_file_path,
            schema: Arc::new(DFSchema::empty()),
            file_type: "iceberg".to_string(),
            options: Default::default(),
            table_partition_cols: Default::default(),
            order_exprs: Default::default(),
            constraints: Constraints::default(),
            column_defaults: Default::default(),
            if_not_exists: Default::default(),
            or_replace: false,
            temporary: false,
            definition: Default::default(),
            unbounded: Default::default(),
        }
    }

    #[tokio::test]
    async fn test_schema_of_created_table() {
        let factory = IcebergTableProviderFactory::new();

        let state = SessionStateBuilder::new().build();
        let cmd = create_external_table_cmd();

        let table_provider = factory
            .create(&state, &cmd)
            .await
            .expect("create table failed");

        let expected_schema = table_metadata_v2_schema();
        let actual_schema = table_provider.schema();

        assert_eq!(actual_schema.as_ref(), &expected_schema);
    }

    #[tokio::test]
    async fn test_schema_of_created_external_table_sql() {
        let mut state = SessionStateBuilder::new().with_default_features().build();
        state.table_factories_mut().insert(
            "ICEBERG".to_string(),
            Arc::new(IcebergTableProviderFactory::new()),
        );
        let ctx = SessionContext::new_with_state(state);

        // All external tables in DataFusion use bare names.
        // See https://github.com/apache/datafusion/blob/main/datafusion/sql/src/statement.rs#L1038-#L1039
        let table_ref = TableReference::bare("static_table");

        // Create the external table
        let sql = format!(
            "CREATE EXTERNAL TABLE {} STORED AS ICEBERG LOCATION '{}'",
            table_ref,
            table_metadata_location()
        );
        let _df = ctx.sql(&sql).await.expect("create table failed");

        // Get the created external table
        let table_provider = ctx
            .table_provider(table_ref)
            .await
            .expect("table not found");

        // Check the schema of the created table
        let expected_schema = table_metadata_v2_schema();
        let actual_schema = table_provider.schema();

        assert_eq!(actual_schema.as_ref(), &expected_schema);
    }

    #[test]
    fn test_parse_table_reference() {
        // Bare name should use "default" namespace
        let table_ref = TableReference::bare("my_table");
        let (namespace, table_name) = parse_table_reference(&table_ref);

        assert_eq!(namespace.as_ref(), &vec!["default".to_string()]);
        assert_eq!(table_name, "my_table");

        // Partial name should use schema as namespace
        let table_ref = TableReference::partial("my_namespace", "my_table");
        let (namespace, table_name) = parse_table_reference(&table_ref);

        assert_eq!(namespace.as_ref(), &vec!["my_namespace".to_string()]);
        assert_eq!(table_name, "my_table");

        // Full name should use schema as namespace, ignoring catalog
        let table_ref = TableReference::full("my_catalog", "my_namespace", "my_table");
        let (namespace, table_name) = parse_table_reference(&table_ref);

        assert_eq!(namespace.as_ref(), &vec!["my_namespace".to_string()]);
        assert_eq!(table_name, "my_table");
    }

    #[tokio::test]
    async fn test_factory_with_catalog_creates_catalog_backed_provider() {
        // Set up a memory catalog with a test table
        let temp_dir = TempDir::new().unwrap();
        let warehouse_path = temp_dir.path().to_str().unwrap().to_string();

        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_path.clone())]),
            )
            .await
            .unwrap();

        let namespace = iceberg::NamespaceIdent::new("test_ns".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();

        let schema = IcebergSchema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let table_creation = TableCreation::builder()
            .name("test_table".to_string())
            .location(format!("{warehouse_path}/test_table"))
            .schema(schema)
            .properties(HashMap::new())
            .build();

        catalog
            .create_table(&namespace, table_creation)
            .await
            .unwrap();

        // Create factory with catalog
        let factory = IcebergTableProviderFactory::new_with_catalog(Arc::new(catalog));

        // Create external table command
        let cmd = CreateExternalTable {
            name: TableReference::partial("test_ns", "test_table"),
            location: String::new(), // Location is ignored when catalog is present
            schema: Arc::new(DFSchema::empty()),
            file_type: "iceberg".to_string(),
            options: Default::default(),
            table_partition_cols: Default::default(),
            order_exprs: Default::default(),
            constraints: Constraints::default(),
            column_defaults: Default::default(),
            if_not_exists: Default::default(),
            or_replace: false,
            temporary: false,
            definition: Default::default(),
            unbounded: Default::default(),
        };

        let state = SessionStateBuilder::new().build();
        let table_provider = factory
            .create(&state, &cmd)
            .await
            .expect("create table failed");

        // Verify the schema matches the catalog table
        let schema = table_provider.schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");

        // Verify it's a catalog-backed provider by checking it supports writes
        // (IcebergStaticTableProvider does not support writes)
        let ctx = SessionContext::new();
        ctx.register_table("test_table", table_provider).unwrap();

        // This should succeed for catalog-backed provider
        let result = ctx.sql("INSERT INTO test_table VALUES (1, 'test')").await;
        assert!(
            result.is_ok(),
            "Catalog-backed provider should support INSERT"
        );
    }

    #[tokio::test]
    async fn test_factory_without_catalog_creates_static_provider() {
        // Create factory without catalog (default)
        let factory = IcebergTableProviderFactory::new();

        let state = SessionStateBuilder::new().build();
        let cmd = create_external_table_cmd();

        let table_provider = factory
            .create(&state, &cmd)
            .await
            .expect("create table failed");

        // Verify the schema matches the static table
        let expected_schema = table_metadata_v2_schema();
        let actual_schema = table_provider.schema();
        assert_eq!(actual_schema.as_ref(), &expected_schema);

        // Verify it's a static provider by checking it rejects writes
        let ctx = SessionContext::new();
        ctx.register_table("static_table", table_provider).unwrap();

        // This should fail for static provider
        let result = ctx.sql("INSERT INTO static_table VALUES (1, 2, 3)").await;
        // The error should occur during planning or execution
        assert!(
            result.is_err() || {
                let df = result.unwrap();
                df.collect().await.is_err()
            },
            "Static provider should reject INSERT"
        );
    }

    #[tokio::test]
    async fn test_factory_with_catalog_returns_error_for_nonexistent_table() {
        // Set up a memory catalog without any tables
        let temp_dir = TempDir::new().unwrap();
        let warehouse_path = temp_dir.path().to_str().unwrap().to_string();

        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_path)]),
            )
            .await
            .unwrap();

        let namespace = iceberg::NamespaceIdent::new("test_ns".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();

        // Create factory with catalog
        let factory = IcebergTableProviderFactory::new_with_catalog(Arc::new(catalog));

        // Create external table command for a non-existent table
        let cmd = CreateExternalTable {
            name: TableReference::partial("test_ns", "nonexistent_table"),
            location: String::new(),
            schema: Arc::new(DFSchema::empty()),
            file_type: "iceberg".to_string(),
            options: Default::default(),
            table_partition_cols: Default::default(),
            order_exprs: Default::default(),
            constraints: Constraints::default(),
            column_defaults: Default::default(),
            if_not_exists: Default::default(),
            or_replace: false,
            temporary: false,
            definition: Default::default(),
            unbounded: Default::default(),
        };

        let state = SessionStateBuilder::new().build();
        let result = factory.create(&state, &cmd).await;

        // Should return an error because the table doesn't exist
        assert!(result.is_err());
        let err = result.unwrap_err();
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("not found"),
            "Error message should indicate table not found: {err_msg}",
        );
    }
}
