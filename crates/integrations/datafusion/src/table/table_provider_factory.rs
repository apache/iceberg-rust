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
use iceberg::io::{FileIOBuilder, LocalFsStorageFactory, StorageFactory};
use iceberg::spec::Transform;
use iceberg::table::{StaticTable, Table};
use iceberg::{Error, ErrorKind, Result, TableIdent};

use super::IcebergStaticTableProvider;
use crate::to_datafusion_error;

/// A factory that implements DataFusion's `TableProviderFactory` to create `IcebergTableProvider` instances.
///
/// # Example
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
/// A `PARTITIONED BY` clause is supported for identity-partitioned tables. Because the
/// table's partitioning is defined entirely by the Iceberg metadata, and DataFusion only
/// accepts plain column names here (not transforms like `bucket[N]` or `day`), the clause
/// must list the identity partition columns of the table's default partition spec, in order:
///
/// ```sql
/// CREATE EXTERNAL TABLE my_iceberg_table
/// STORED AS ICEBERG LOCATION '/path/to/metadata.json'
/// PARTITIONED BY (event_date)
/// ```
///
/// Tables partitioned with non-identity transforms can still be registered by omitting the
/// `PARTITIONED BY` clause.
///
/// # Note
/// This factory is designed to work with the DataFusion query engine,
/// specifically for handling Iceberg tables in external table commands.
/// Currently, this implementation supports only reading Iceberg tables, with
/// the creation of new tables not yet available.
///
/// # Errors
/// An error will be returned if any unsupported feature, such as order expressions,
/// constraints, or column defaults, is detected in the table creation command, if a
/// `PARTITIONED BY` clause is used on a table with a non-identity transform, or if the
/// `PARTITIONED BY` columns do not match the table's identity partition columns.
#[derive(Debug, Default)]
pub struct IcebergTableProviderFactory {
    storage_factory: Option<Arc<dyn StorageFactory>>,
}

impl IcebergTableProviderFactory {
    pub fn new() -> Self {
        Self {
            storage_factory: None,
        }
    }

    /// Create a new factory with a custom storage factory for creating FileIO instances.
    pub fn new_with_storage_factory(storage_factory: Arc<dyn StorageFactory>) -> Self {
        Self {
            storage_factory: Some(storage_factory),
        }
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
        let metadata_file_path = &cmd.location;
        let options = &cmd.options;

        let table_name_with_ns = complement_namespace_if_necessary(table_name);

        let storage_factory = self
            .storage_factory
            .clone()
            .unwrap_or_else(|| Arc::new(LocalFsStorageFactory));

        let table = create_static_table(
            table_name_with_ns,
            metadata_file_path,
            options,
            storage_factory,
        )
        .await
        .map_err(to_datafusion_error)?
        .into_table();

        // The Iceberg metadata is the source of truth for partitioning, so a
        // `PARTITIONED BY` clause is only accepted when it agrees with the
        // table's default partition spec.
        validate_partition_columns(&table, &cmd.table_partition_cols).map_err(to_datafusion_error)?;

        let provider = IcebergStaticTableProvider::try_new_from_table(table)
            .await
            .map_err(to_datafusion_error)?;

        Ok(Arc::new(provider))
    }
}

fn check_cmd(cmd: &CreateExternalTable) -> Result<()> {
    let CreateExternalTable {
        schema,
        order_exprs,
        constraints,
        column_defaults,
        ..
    } = cmd;

    // Check if any of the fields violate the constraints in a single condition
    let is_invalid = !schema.fields().is_empty()
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

/// Validates the `PARTITIONED BY` columns from a `CREATE EXTERNAL TABLE` command
/// against the table's default partition spec.
///
/// `CREATE EXTERNAL TABLE ... STORED AS ICEBERG` loads an existing table, so the
/// partitioning is fully determined by the Iceberg metadata. DataFusion's grammar only
/// accepts plain column names in `PARTITIONED BY` (it cannot express transforms such as
/// `bucket[N]` or `day`), so the clause is only supported for identity-partitioned tables.
/// For an identity transform the partition field name equals its source column name, so the
/// clause must list those column names in order.
///
/// An empty clause (no `PARTITIONED BY`) skips this check: any table, including one with
/// non-identity transforms, can still be registered for read-only access without declaring
/// its partitioning.
fn validate_partition_columns(table: &Table, declared_partition_cols: &[String]) -> Result<()> {
    if declared_partition_cols.is_empty() {
        return Ok(());
    }

    let spec = table.metadata().default_partition_spec();

    // DataFusion cannot express non-identity transforms in `PARTITIONED BY`, so a table
    // partitioned by any other transform cannot be described by this clause.
    //
    // TODO: support non-identity transforms (bucket/truncate/year/month/day/hour) once
    // DataFusion's `PARTITIONED BY` grammar can express transform functions such as
    // `bucket(16, id)` or `days(ts)`. See https://github.com/apache/iceberg-rust/issues/2050.
    if let Some(field) = spec
        .fields()
        .iter()
        .find(|f| f.transform != Transform::Identity)
    {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            format!(
                "PARTITIONED BY only supports identity-partitioned tables, but partition field \
                 '{}' uses the '{}' transform. Omit the PARTITIONED BY clause to register this table.",
                field.name, field.transform
            ),
        ));
    }

    let actual: Vec<&str> = spec.fields().iter().map(|f| f.name.as_str()).collect();
    let declared: Vec<&str> = declared_partition_cols.iter().map(String::as_str).collect();

    if declared != actual {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "PARTITIONED BY columns {declared:?} do not match the table's identity partition \
                 columns {actual:?}. The PARTITIONED BY clause must list those columns in order, \
                 or be omitted."
            ),
        ));
    }

    Ok(())
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
    storage_factory: Arc<dyn StorageFactory>,
) -> Result<StaticTable> {
    let table_ident = TableIdent::from_strs(table_name.to_vec())?;
    let file_io = FileIOBuilder::new(storage_factory)
        .with_props(props)
        .build();
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

    fn bucket_partitioned_table_metadata_location() -> String {
        format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2WithBucketPartition.json"
        )
    }

    fn multi_identity_partitioned_table_metadata_location() -> String {
        format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2WithMultiIdentityPartition.json"
        )
    }

    fn create_external_table_cmd() -> CreateExternalTable {
        create_external_table_cmd_with_partition_cols(Default::default())
    }

    fn create_external_table_cmd_with_partition_cols(
        table_partition_cols: Vec<String>,
    ) -> CreateExternalTable {
        let metadata_file_path = table_metadata_location();

        CreateExternalTable {
            name: TableReference::partial("static_ns", "static_table"),
            location: metadata_file_path,
            schema: Arc::new(DFSchema::empty()),
            file_type: "iceberg".to_string(),
            options: Default::default(),
            table_partition_cols,
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

    // `TableMetadataV2.json` has a default partition spec with a single identity
    // partition field named "x".

    #[tokio::test]
    async fn test_partitioned_by_matching_partition_spec() {
        let factory = IcebergTableProviderFactory::new();
        let state = SessionStateBuilder::new().build();
        let cmd = create_external_table_cmd_with_partition_cols(vec!["x".to_string()]);

        let table_provider = factory
            .create(&state, &cmd)
            .await
            .expect("create table with matching PARTITIONED BY should succeed");

        assert_eq!(table_provider.schema().as_ref(), &table_metadata_v2_schema());
    }

    #[tokio::test]
    async fn test_partitioned_by_mismatching_partition_spec() {
        let factory = IcebergTableProviderFactory::new();
        let state = SessionStateBuilder::new().build();
        let cmd = create_external_table_cmd_with_partition_cols(vec!["y".to_string()]);

        let err = factory
            .create(&state, &cmd)
            .await
            .expect_err("create table with mismatching PARTITIONED BY should fail");

        assert!(
            err.to_string()
                .contains("do not match the table's identity partition columns"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_partitioned_by_multiple_identity_columns() {
        let factory = IcebergTableProviderFactory::new();
        let state = SessionStateBuilder::new().build();

        // The table is partitioned by identity(x), identity(y).
        let mut cmd =
            create_external_table_cmd_with_partition_cols(vec!["x".to_string(), "y".to_string()]);
        cmd.location = multi_identity_partitioned_table_metadata_location();

        factory
            .create(&state, &cmd)
            .await
            .expect("PARTITIONED BY (x, y) should match the multi-column partition spec");
    }

    #[tokio::test]
    async fn test_partitioned_by_multiple_identity_columns_wrong_order() {
        let factory = IcebergTableProviderFactory::new();
        let state = SessionStateBuilder::new().build();

        // Declaring the partition columns in the wrong order must fail, since partition
        // field order is significant in Iceberg.
        let mut cmd =
            create_external_table_cmd_with_partition_cols(vec!["y".to_string(), "x".to_string()]);
        cmd.location = multi_identity_partitioned_table_metadata_location();

        let err = factory
            .create(&state, &cmd)
            .await
            .expect_err("PARTITIONED BY (y, x) should fail when the spec order is (x, y)");

        assert!(
            err.to_string()
                .contains("do not match the table's identity partition columns"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_partitioned_by_partial_columns_fails() {
        let factory = IcebergTableProviderFactory::new();
        let state = SessionStateBuilder::new().build();

        // Declaring only a subset of the partition columns must fail: the table is
        // partitioned by identity(x), identity(y) but only `x` is declared.
        let mut cmd = create_external_table_cmd_with_partition_cols(vec!["x".to_string()]);
        cmd.location = multi_identity_partitioned_table_metadata_location();

        let err = factory
            .create(&state, &cmd)
            .await
            .expect_err("PARTITIONED BY (x) should fail when the spec is (x, y)");

        assert!(
            err.to_string()
                .contains("do not match the table's identity partition columns"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_partitioned_by_rejects_non_identity_transform() {
        let factory = IcebergTableProviderFactory::new();
        let state = SessionStateBuilder::new().build();

        // The table is partitioned by `bucket[4](x)`, which cannot be expressed as a
        // plain column name in `PARTITIONED BY`.
        let mut cmd = create_external_table_cmd_with_partition_cols(vec!["x_bucket".to_string()]);
        cmd.location = bucket_partitioned_table_metadata_location();

        let err = factory
            .create(&state, &cmd)
            .await
            .expect_err("PARTITIONED BY on a bucket-partitioned table should fail");

        let msg = err.to_string();
        assert!(
            msg.contains("only supports identity-partitioned tables")
                && msg.contains("bucket[4]"),
            "unexpected error: {msg}"
        );
    }

    #[tokio::test]
    async fn test_non_identity_partition_table_registers_without_partitioned_by() {
        // Omitting PARTITIONED BY must still allow registering a non-identity partitioned
        // table for read-only access.
        let factory = IcebergTableProviderFactory::new();
        let state = SessionStateBuilder::new().build();

        let mut cmd = create_external_table_cmd();
        cmd.location = bucket_partitioned_table_metadata_location();

        factory
            .create(&state, &cmd)
            .await
            .expect("registering a bucket-partitioned table without PARTITIONED BY should succeed");
    }

}
