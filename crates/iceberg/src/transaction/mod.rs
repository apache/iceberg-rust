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

//! This module contains transaction api.
//!
//! The transaction API enables changes to be made to an existing table.
//!
//! Note that this may also have side effects, such as producing new manifest
//! files.
//!
//! Below is a basic example using the "fast-append" action:
//!
//! ```ignore
//! use iceberg::transaction::{ApplyTransactionAction, Transaction};
//! use iceberg::Catalog;
//!
//! // Create a transaction.
//! let tx = Transaction::new(my_table);
//!
//! // Create a `FastAppendAction` which will not rewrite or append
//! // to existing metadata. This will create a new manifest.
//! let action = tx.fast_append().add_data_files(my_data_files);
//!
//! // Apply the fast-append action to the given transaction, returning
//! // the newly updated `Transaction`.
//! let tx = action.apply(tx).unwrap();
//!
//!
//! // End the transaction by committing to an `iceberg::Catalog`
//! // implementation. This will cause a table update to occur.
//! let table = tx
//!     .commit(&some_catalog_impl)
//!     .await
//!     .unwrap();
//! ```

/// The `ApplyTransactionAction` trait provides an `apply` method
/// that allows users to apply a transaction action to a `Transaction`.
mod action;

pub use action::*;
mod append;
mod expire_snapshots;
mod snapshot;
mod sort_order;
mod update_location;
mod update_properties;
mod update_schema;
mod update_statistics;
mod upgrade_format_version;

use std::sync::Arc;
use std::time::Duration;

use backon::{BackoffBuilder, ExponentialBackoff, ExponentialBuilder, RetryableWithContext};
pub use update_schema::AddColumn;

use crate::error::Result;
use crate::spec::TableProperties;
use crate::table::Table;
use crate::transaction::action::TransactionActionEntry;
pub use crate::transaction::append::FastAppendAction;
pub use crate::transaction::expire_snapshots::ExpireSnapshotsAction;
pub use crate::transaction::sort_order::ReplaceSortOrderAction;
pub use crate::transaction::update_location::UpdateLocationAction;
pub use crate::transaction::update_properties::UpdatePropertiesAction;
pub use crate::transaction::update_schema::UpdateSchemaAction;
pub use crate::transaction::update_statistics::UpdateStatisticsAction;
pub use crate::transaction::upgrade_format_version::UpgradeFormatVersionAction;
use crate::{Catalog, Error, ErrorKind, TableCommit, TableRequirement, TableUpdate};

/// Table transaction.
pub struct Transaction {
    table: Table,
    actions: Vec<TransactionActionEntry>,
    /// Newly added `(snapshot_id, manifest_list)` pairs submitted in prior commit
    /// attempts so retries can recognize a commit whose response was lost.
    submitted_snapshots: Vec<(i64, String)>,
}

impl Transaction {
    /// Creates a new transaction.
    pub fn new(table: &Table) -> Self {
        Self {
            table: table.clone(),
            actions: vec![],
            submitted_snapshots: vec![],
        }
    }

    fn update_table_metadata(table: Table, updates: &[TableUpdate]) -> Result<Table> {
        let mut metadata_builder = table.metadata().clone().into_builder(None);
        for update in updates {
            metadata_builder = update.clone().apply(metadata_builder)?;
        }

        Ok(table.with_metadata(Arc::new(metadata_builder.build()?.metadata)))
    }

    /// Applies an [`ActionCommit`] to the given [`Table`], returning a new [`Table`] with updated metadata.
    /// Also appends any derived [`TableUpdate`]s and [`TableRequirement`]s to the provided vectors.
    fn apply(
        table: Table,
        mut action_commit: ActionCommit,
        existing_updates: &mut Vec<TableUpdate>,
        existing_requirements: &mut Vec<TableRequirement>,
    ) -> Result<Table> {
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        for requirement in &requirements {
            requirement.check(Some(table.metadata()))?;
        }

        let updated_table = Self::update_table_metadata(table, &updates)?;

        existing_updates.extend(updates);
        Self::merge_requirements(existing_requirements, requirements);

        Ok(updated_table)
    }

    // Catalogs check against the base, so keep only the first requirement per value.
    fn merge_requirements(
        existing_requirements: &mut Vec<TableRequirement>,
        requirements: Vec<TableRequirement>,
    ) {
        for requirement in requirements {
            if !existing_requirements
                .iter()
                .any(|existing| Self::asserts_same_value(existing, &requirement))
            {
                existing_requirements.push(requirement);
            }
        }
    }

    fn asserts_same_value(left: &TableRequirement, right: &TableRequirement) -> bool {
        match (left, right) {
            (
                TableRequirement::RefSnapshotIdMatch { r#ref: left, .. },
                TableRequirement::RefSnapshotIdMatch { r#ref: right, .. },
            ) => left == right,
            _ => std::mem::discriminant(left) == std::mem::discriminant(right),
        }
    }

    /// Sets table to a new version.
    pub fn upgrade_table_version(&self) -> UpgradeFormatVersionAction {
        UpgradeFormatVersionAction::new()
    }

    /// Update table's property.
    pub fn update_table_properties(&self) -> UpdatePropertiesAction {
        UpdatePropertiesAction::new()
    }

    /// Creates an update schema action.
    pub fn update_schema(&self) -> UpdateSchemaAction {
        UpdateSchemaAction::new()
    }

    /// Creates a fast append action.
    pub fn fast_append(&self) -> FastAppendAction {
        FastAppendAction::new()
    }

    /// Creates replace sort order action.
    pub fn replace_sort_order(&self) -> ReplaceSortOrderAction {
        ReplaceSortOrderAction::new()
    }

    /// Set the location of table
    pub fn update_location(&self) -> UpdateLocationAction {
        UpdateLocationAction::new()
    }

    /// Update the statistics of table
    pub fn update_statistics(&self) -> UpdateStatisticsAction {
        UpdateStatisticsAction::new()
    }

    /// Expire snapshots from the table metadata.
    pub fn expire_snapshots(&self) -> ExpireSnapshotsAction {
        ExpireSnapshotsAction::new()
    }

    /// Commit transaction.
    ///
    /// Returns `ErrorKind::PreconditionFailed` if the table is read-only
    /// (`Table::readonly()`), even when the transaction has no actions.
    pub async fn commit(self, catalog: &dyn Catalog) -> Result<Table> {
        if self.table.readonly() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "Cannot modify a read-only table",
            )
            .with_context("table", self.table.identifier().to_string()));
        }

        if self.actions.is_empty() {
            // nothing to commit
            return Ok(self.table);
        }

        let table_props = self.table.metadata().table_properties();

        let backoff = Self::build_backoff(table_props)?;
        let tx = self;

        (|mut tx: Transaction| async {
            let result = tx.do_commit(catalog).await;
            (tx, result)
        })
        .retry(backoff)
        .sleep(tokio::time::sleep)
        .context(tx)
        .when(|e| e.retryable())
        .await
        .1
    }

    fn build_backoff(props: TableProperties<'_>) -> Result<ExponentialBackoff> {
        Ok(ExponentialBuilder::new()
            .with_min_delay(Duration::from_millis(props.commit_min_retry_wait_ms()?))
            .with_max_delay(Duration::from_millis(props.commit_max_retry_wait_ms()?))
            .with_total_delay(Some(Duration::from_millis(
                props.commit_total_retry_timeout_ms()?,
            )))
            .with_max_times(props.commit_num_retries()?)
            .with_factor(2.0)
            .with_jitter()
            .build())
    }

    async fn do_commit(&mut self, catalog: &dyn Catalog) -> Result<Table> {
        let refreshed = catalog.load_table(self.table.identifier()).await?;

        // Any surviving witness proves an earlier attempt landed: a `TableCommit`
        // is atomic on the catalog, even when it adds several snapshots and one of
        // them was expired concurrently.
        if self.submitted_snapshots.iter().any(|(id, manifest_list)| {
            refreshed
                .metadata()
                .snapshot_by_id(*id)
                .is_some_and(|snapshot| snapshot.manifest_list() == manifest_list)
        }) {
            return Ok(refreshed);
        }

        if self.table.metadata() != refreshed.metadata()
            || self.table.metadata_location() != refreshed.metadata_location()
        {
            // current base is stale, use refreshed as base and re-apply transaction actions
            self.table = refreshed.clone();
        }

        let mut current_table = self.table.clone();
        let mut existing_updates: Vec<TableUpdate> = vec![];
        let mut existing_requirements: Vec<TableRequirement> = vec![];

        for entry in self.actions.iter_mut() {
            let action_commit = entry.commit(&current_table).await?;
            // apply action commit to current_table
            current_table = Self::apply(
                current_table,
                action_commit,
                &mut existing_updates,
                &mut existing_requirements,
            )?;
        }

        self.submitted_snapshots
            .extend(existing_updates.iter().filter_map(|update| {
                match update {
                    TableUpdate::AddSnapshot { snapshot }
                        if !snapshot.manifest_list().is_empty()
                            && self
                                .table
                                .metadata()
                                .snapshot_by_id(snapshot.snapshot_id())
                                .is_none()
                            && current_table
                                .metadata()
                                .snapshot_by_id(snapshot.snapshot_id())
                                .is_some_and(|s| s.manifest_list() == snapshot.manifest_list()) =>
                    {
                        Some((snapshot.snapshot_id(), snapshot.manifest_list().to_string()))
                    }
                    _ => None,
                }
            }));

        let table_commit = TableCommit::builder()
            .ident(self.table.identifier().to_owned())
            .updates(existing_updates)
            .requirements(existing_requirements)
            .build();

        catalog.update_table(table_commit).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use uuid::Uuid;

    use crate::catalog::MockCatalog;
    use crate::io::FileIO;
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, NullOrder,
        PartitionSpec, PrimitiveType, SortDirection, SortField, Struct, TableMetadata,
        TableProperties, Transform, Type,
    };
    use crate::table::{StaticTable, Table};
    use crate::test_utils::{make_encrypted_table, test_runtime};
    use crate::transaction::{AddColumn, ApplyTransactionAction, Transaction};
    use crate::{
        Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
        TableIdent, TableRequirement, TableUpdate,
    };

    pub fn make_v1_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV1Valid.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json")
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIO::new_with_memory())
            .runtime(test_runtime())
            .build()
            .unwrap()
    }

    pub fn make_v2_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2Valid.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json")
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIO::new_with_memory())
            .runtime(test_runtime())
            .build()
            .unwrap()
    }

    pub fn make_v2_minimal_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2ValidMinimal.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json")
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIO::new_with_memory())
            .runtime(test_runtime())
            .build()
            .unwrap()
    }

    pub(crate) fn make_v3_minimal_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV3ValidMinimal.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json")
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIO::new_with_memory())
            .runtime(test_runtime())
            .build()
            .unwrap()
    }

    pub(super) fn with_bucket_partition_spec(table: Table) -> Table {
        let spec = PartitionSpec::builder(table.metadata().current_schema().clone())
            .add_partition_field("x", "x", Transform::Identity)
            .unwrap()
            .add_partition_field("y", "y_bucket", Transform::Bucket(16))
            .unwrap()
            .build()
            .unwrap()
            .into_unbound();
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .add_default_partition_spec(spec)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        table.with_metadata(Arc::new(metadata))
    }

    async fn make_v2_minimal_readonly_table() -> Table {
        let table = make_v2_minimal_table();
        StaticTable::from_metadata(
            table.metadata().clone(),
            table.identifier().clone(),
            FileIO::new_with_memory(),
        )
        .await
        .unwrap()
        .into_table()
    }

    pub(crate) async fn make_v3_minimal_table_in_catalog(catalog: &impl Catalog) -> Table {
        let table_ident =
            TableIdent::from_strs([format!("ns1-{}", Uuid::new_v4()), "test1".to_string()])
                .unwrap();

        catalog
            .create_namespace(table_ident.namespace(), HashMap::new())
            .await
            .unwrap();

        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV3ValidMinimal.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let base_metadata = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        let table_creation = TableCreation::builder()
            .schema((**base_metadata.current_schema()).clone())
            .partition_spec((**base_metadata.default_partition_spec()).clone())
            .sort_order((**base_metadata.default_sort_order()).clone())
            .name(table_ident.name().to_string())
            .format_version(crate::spec::FormatVersion::V3)
            .build();

        catalog
            .create_table(table_ident.namespace(), table_creation)
            .await
            .unwrap()
    }

    /// Helper function to create a test table with retry properties
    pub(super) fn setup_test_table(num_retries: &str) -> Table {
        with_retry_properties(make_v2_table(), num_retries)
    }

    /// Sets fast commit retry properties on `table`.
    pub(super) fn with_retry_properties(table: Table, num_retries: &str) -> Table {
        // Set retry properties
        let mut props = HashMap::new();
        props.insert("commit.retry.min-wait-ms".to_string(), "10".to_string());
        props.insert("commit.retry.max-wait-ms".to_string(), "100".to_string());
        props.insert(
            "commit.retry.total-timeout-ms".to_string(),
            "1000".to_string(),
        );
        props.insert(
            "commit.retry.num-retries".to_string(),
            num_retries.to_string(),
        );

        // Update table properties
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .set_properties(props)
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        table.with_metadata(Arc::new(metadata))
    }

    /// Helper function to create a transaction with a simple update action
    fn create_test_transaction(table: &Table) -> Transaction {
        let tx = Transaction::new(table);
        tx.update_table_properties()
            .set("test.key".to_string(), "test.value".to_string())
            .apply(tx)
            .unwrap()
    }

    /// Helper function to set up a mock catalog with retryable errors
    fn setup_mock_catalog_with_retryable_errors(
        success_after_attempts: Option<u32>,
        expected_calls: usize,
    ) -> MockCatalog {
        let mut mock_catalog = MockCatalog::new();

        mock_catalog
            .expect_load_table()
            .returning_st(|_| Box::pin(async move { Ok(make_v2_table()) }));

        let attempts = AtomicU32::new(0);
        mock_catalog
            .expect_update_table()
            .times(expected_calls)
            .returning_st(move |_| {
                if let Some(success_after_attempts) = success_after_attempts {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    if attempts.load(Ordering::SeqCst) <= success_after_attempts {
                        Box::pin(async move {
                            Err(
                                Error::new(ErrorKind::CatalogCommitConflicts, "Commit conflict")
                                    .with_retryable(true),
                            )
                        })
                    } else {
                        Box::pin(async move { Ok(make_v2_table()) })
                    }
                } else {
                    // Always fail with retryable error
                    Box::pin(async move {
                        Err(
                            Error::new(ErrorKind::CatalogCommitConflicts, "Commit conflict")
                                .with_retryable(true),
                        )
                    })
                }
            });

        mock_catalog
    }

    /// Helper function to set up a mock catalog with non-retryable error
    fn setup_mock_catalog_with_non_retryable_error() -> MockCatalog {
        let mut mock_catalog = MockCatalog::new();

        mock_catalog
            .expect_load_table()
            .returning_st(|_| Box::pin(async move { Ok(make_v2_table()) }));

        mock_catalog
            .expect_update_table()
            .times(1) // Should only be called once since error is not retryable
            .returning_st(move |_| {
                Box::pin(async move {
                    Err(Error::new(ErrorKind::Unexpected, "Non-retryable error")
                        .with_retryable(false))
                })
            });

        mock_catalog
    }

    #[tokio::test]
    async fn test_commit_retryable_error() {
        // Create a test table with retry properties
        let table = setup_test_table("3");

        // Create a transaction with a simple update action
        let tx = create_test_transaction(&table);

        // Create a mock catalog that fails twice then succeeds
        let mock_catalog = setup_mock_catalog_with_retryable_errors(Some(2), 3);

        // Commit the transaction
        let result = tx.commit(&mock_catalog).await;

        // Verify the result
        assert!(result.is_ok(), "Transaction should eventually succeed");
    }

    #[tokio::test]
    async fn test_commit_non_retryable_error() {
        // Create a test table with retry properties
        let table = setup_test_table("3");

        // Create a transaction with a simple update action
        let tx = create_test_transaction(&table);

        // Create a mock catalog that fails with non-retryable error
        let mock_catalog = setup_mock_catalog_with_non_retryable_error();

        // Commit the transaction
        let result = tx.commit(&mock_catalog).await;

        // Verify the result
        assert!(result.is_err(), "Transaction should fail immediately");
        if let Err(err) = result {
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert_eq!(err.message(), "Non-retryable error");
            assert!(!err.retryable(), "Error should not be retryable");
        }
    }

    #[tokio::test]
    async fn test_commit_max_retries_exceeded() {
        // Create a test table with retry properties (only allow 2 retries)
        let table = setup_test_table("2");

        // Create a transaction with a simple update action
        let tx = create_test_transaction(&table);

        // Create a mock catalog that always fails with retryable error
        let mock_catalog = setup_mock_catalog_with_retryable_errors(None, 3); // Initial attempt + 2 retries = 3 total attempts

        // Commit the transaction
        let result = tx.commit(&mock_catalog).await;

        // Verify the result
        assert!(result.is_err(), "Transaction should fail after max retries");
        if let Err(err) = result {
            assert_eq!(err.kind(), ErrorKind::CatalogCommitConflicts);
            assert_eq!(err.message(), "Commit conflict");
            assert!(err.retryable(), "Error should be retryable");
        }
    }

    #[test]
    fn test_commit_backoff_is_jittered() {
        let table = setup_test_table("4");
        let delays = || {
            Transaction::build_backoff(table.metadata().table_properties())
                .unwrap()
                .collect::<Vec<_>>()
        };

        assert_ne!(delays(), delays());
    }

    #[tokio::test]
    async fn test_transaction_snapshot_summary() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let mut file_seq = 0u32;
        let mut append_file = |table: &Table, record_count: u64, file_size: u64| {
            file_seq += 1;
            let file = DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(format!("test/{file_seq}.parquet"))
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(file_size)
                .record_count(record_count)
                .partition(Struct::from_iter([Some(Literal::long(1))]))
                .partition_spec_id(0)
                .build()
                .unwrap();
            let tx = Transaction::new(table);
            tx.fast_append()
                .add_data_files(vec![file])
                .apply(tx)
                .unwrap()
        };

        let table = append_file(&table, /*record_count=*/ 10, /*file_size=*/ 100)
            .commit(&catalog)
            .await
            .unwrap();
        let table = append_file(&table, /*record_count=*/ 20, /*file_size=*/ 200)
            .commit(&catalog)
            .await
            .unwrap();

        let summary = &table
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties;

        assert_eq!(summary.get("total-records").unwrap(), "30");
        assert_eq!(summary.get("total-data-files").unwrap(), "2");
        assert_eq!(summary.get("total-files-size").unwrap(), "300");
    }

    #[tokio::test]
    async fn test_commit_two_fast_appends_in_one_transaction() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let data_file = |path: &str| {
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(path.to_string())
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(100)
                .record_count(10)
                .partition(Struct::from_iter([Some(Literal::long(1))]))
                .partition_spec_id(0)
                .build()
                .unwrap()
        };

        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .add_data_files(vec![data_file("test/1.parquet")])
            .apply(tx)
            .unwrap();
        let tx = tx
            .fast_append()
            .add_data_files(vec![data_file("test/2.parquet")])
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let current = table.metadata().current_snapshot().unwrap();
        let parent = table
            .metadata()
            .snapshot_by_id(current.parent_snapshot_id().unwrap())
            .unwrap();
        assert_eq!(
            (
                table.metadata().snapshots().count(),
                parent.parent_snapshot_id(),
                current.summary().additional_properties["total-data-files"].as_str(),
            ),
            (2, None, "2")
        );
    }

    #[tokio::test]
    async fn test_commit_add_column_then_sort_on_it_in_one_transaction() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let tx = Transaction::new(&table);
        let tx = tx
            .update_schema()
            .add_column(AddColumn::optional(
                "new_col",
                Type::Primitive(PrimitiveType::Int),
            ))
            .apply(tx)
            .unwrap();
        let tx = tx
            .replace_sort_order()
            .asc("new_col", NullOrder::First)
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let new_col_id = table
            .metadata()
            .current_schema()
            .field_by_name("new_col")
            .unwrap()
            .id;
        assert_eq!(table.metadata().default_sort_order().fields, vec![
            SortField {
                source_id: new_col_id,
                transform: Transform::Identity,
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            }
        ]);
    }

    #[test]
    fn test_merge_requirements_keeps_the_first_requirement_on_each_value() {
        let main = |snapshot_id| TableRequirement::RefSnapshotIdMatch {
            r#ref: "main".to_string(),
            snapshot_id,
        };
        let branch = TableRequirement::RefSnapshotIdMatch {
            r#ref: "branch".to_string(),
            snapshot_id: Some(2),
        };
        let mut requirements = vec![main(None), TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: 0,
        }];

        Transaction::merge_requirements(&mut requirements, vec![
            main(Some(1)),
            branch.clone(),
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: 1,
            },
            TableRequirement::DefaultSpecIdMatch { default_spec_id: 0 },
        ]);

        assert_eq!(requirements, vec![
            main(None),
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: 0
            },
            branch,
            TableRequirement::DefaultSpecIdMatch { default_spec_id: 0 },
        ]);
    }

    #[tokio::test]
    async fn test_commit_to_encrypted_table() {
        let table = make_encrypted_table().await.with_metadata_location(
            "memory:///table/metadata/00000-9c12d441-03fe-4693-9a96-a0705ddf69c1.metadata.json"
                .to_string(),
        );
        let refreshed_table = table.clone();
        let update_table = table.clone();
        let mut mock_catalog = MockCatalog::new();
        mock_catalog
            .expect_load_table()
            .times(1)
            .returning_st(move |_| {
                let refreshed_table = refreshed_table.clone();
                Box::pin(async move { Ok(refreshed_table) })
            });
        mock_catalog
            .expect_update_table()
            .times(1)
            .returning_st(move |commit| {
                let update_table = update_table.clone();
                Box::pin(async move { commit.apply(update_table) })
            });

        let tx = Transaction::new(&table);
        let tx = tx
            .update_table_properties()
            .set("test.key".to_string(), "test.value".to_string())
            .apply(tx)
            .unwrap();

        let updated_table = tx.commit(&mock_catalog).await.unwrap();

        assert_eq!(
            updated_table
                .metadata()
                .properties()
                .get(TableProperties::PROPERTY_ENCRYPTION_KEY_ID)
                .map(String::as_str),
            Some("master-1")
        );
        assert_eq!(
            updated_table
                .metadata()
                .properties()
                .get("test.key")
                .map(String::as_str),
            Some("test.value")
        );
        assert!(updated_table.encryption_manager().is_some());
    }

    #[tokio::test]
    async fn test_commit_readonly_table_fails() {
        let table = make_v2_minimal_readonly_table().await;
        let catalog = MockCatalog::new();

        // make sure even empty transactions fail
        let err = Transaction::new(&table).commit(&catalog).await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(err.message(), "Cannot modify a read-only table");
        assert!(!err.retryable(), "Error should not be retryable");
    }

    #[tokio::test]
    async fn test_commit_readonly_table_with_actions_fails() {
        let table = make_v2_minimal_readonly_table().await;
        let catalog = MockCatalog::new();

        let err = create_test_transaction(&table)
            .commit(&catalog)
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
    }

    #[tokio::test]
    async fn test_commit_empty_transaction_is_noop() {
        let table = make_v2_minimal_table();
        let catalog = MockCatalog::new();

        let result = Transaction::new(&table).commit(&catalog).await.unwrap();

        assert_eq!(result.metadata(), table.metadata());
    }

    #[derive(Debug, Clone, Copy)]
    enum FaultMode {
        /// Durably commit attempt 1, then return retryable `CatalogCommitConflicts`.
        PostCommitConflictOnce,
        /// Durably commit attempt 1, then expire one of its snapshots and return
        /// retryable `CatalogCommitConflicts`.
        PostCommitConflictThenExpireOneSnapshot,
        /// Durably commit attempt 1, then return retryable `CatalogCommitConflicts`
        /// and fail the next `load_table` with a retryable error.
        PostCommitConflictThenLoadFailure,
        /// Fail attempt 1 before committing, after a concurrent writer committed a
        /// snapshot with the same id but a different manifest list.
        PreCommitConflictWithSameSnapshotId,
        /// Fail attempt 1 before committing with retryable `CatalogCommitConflicts`.
        PreCommitConflictOnce,
        /// Stash attempt 1 without committing, then land attempt 1 when attempt 2 commits.
        DelayedFirstCommit,
    }

    #[derive(Debug)]
    struct FaultyCatalog {
        inner: Box<dyn Catalog>,
        mode: FaultMode,
        update_calls: AtomicUsize,
        load_calls: AtomicUsize,
        concurrent_file_after_fault: Mutex<Option<DataFile>>,
        delayed_commit: Mutex<Option<TableCommit>>,
        first_attempt_snapshots: Mutex<Vec<(i64, String)>>,
    }

    impl FaultyCatalog {
        async fn new(mode: FaultMode) -> Self {
            Self {
                inner: Box::new(new_memory_catalog().await),
                mode,
                update_calls: AtomicUsize::new(0),
                load_calls: AtomicUsize::new(0),
                concurrent_file_after_fault: Mutex::new(None),
                delayed_commit: Mutex::new(None),
                first_attempt_snapshots: Mutex::new(vec![]),
            }
        }

        fn inject_concurrent_append(&self, file: DataFile) {
            *self.concurrent_file_after_fault.lock().unwrap() = Some(file);
        }

        fn update_calls(&self) -> usize {
            self.update_calls.load(Ordering::SeqCst)
        }

        fn load_calls(&self) -> usize {
            self.load_calls.load(Ordering::SeqCst)
        }

        fn first_attempt_snapshots(&self) -> Vec<(i64, String)> {
            self.first_attempt_snapshots.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl Catalog for FaultyCatalog {
        async fn list_namespaces(
            &self,
            parent: Option<&NamespaceIdent>,
        ) -> Result<Vec<NamespaceIdent>> {
            self.inner.list_namespaces(parent).await
        }

        async fn create_namespace(
            &self,
            namespace: &NamespaceIdent,
            properties: HashMap<String, String>,
        ) -> Result<Namespace> {
            self.inner.create_namespace(namespace, properties).await
        }

        async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
            self.inner.get_namespace(namespace).await
        }

        async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
            self.inner.namespace_exists(namespace).await
        }

        async fn update_namespace(
            &self,
            namespace: &NamespaceIdent,
            properties: HashMap<String, String>,
        ) -> Result<()> {
            self.inner.update_namespace(namespace, properties).await
        }

        async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
            self.inner.drop_namespace(namespace).await
        }

        async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
            self.inner.list_tables(namespace).await
        }

        async fn create_table(
            &self,
            namespace: &NamespaceIdent,
            creation: TableCreation,
        ) -> Result<Table> {
            self.inner.create_table(namespace, creation).await
        }

        async fn load_table(&self, table: &TableIdent) -> Result<Table> {
            let call = self.load_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if matches!(self.mode, FaultMode::PostCommitConflictThenLoadFailure) && call == 2 {
                return Err(
                    Error::new(ErrorKind::Unexpected, "simulated transient load failure")
                        .with_retryable(true),
                );
            }
            self.inner.load_table(table).await
        }

        async fn drop_table(&self, table: &TableIdent) -> Result<()> {
            self.inner.drop_table(table).await
        }

        async fn purge_table(&self, table: &TableIdent) -> Result<()> {
            self.inner.purge_table(table).await
        }

        async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
            self.inner.table_exists(table).await
        }

        async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
            self.inner.rename_table(src, dest).await
        }

        async fn register_table(
            &self,
            table_ident: &TableIdent,
            metadata_location: String,
        ) -> Result<Table> {
            self.inner
                .register_table(table_ident, metadata_location)
                .await
        }

        async fn update_table(&self, mut commit: TableCommit) -> Result<Table> {
            let call = self.update_calls.fetch_add(1, Ordering::SeqCst) + 1;
            match self.mode {
                FaultMode::PostCommitConflictOnce
                | FaultMode::PostCommitConflictThenLoadFailure
                    if call == 1 =>
                {
                    let committed = self.inner.update_table(commit).await?;
                    let extra_file = self.concurrent_file_after_fault.lock().unwrap().take();
                    if let Some(file) = extra_file {
                        let tx = Transaction::new(&committed);
                        let tx = tx.fast_append().add_data_files(vec![file]).apply(tx)?;
                        let _ = tx.commit(&*self.inner).await?;
                    }
                    Err(Error::new(
                        ErrorKind::CatalogCommitConflicts,
                        "simulated conflict after commit succeeded",
                    )
                    .with_retryable(true))
                }
                FaultMode::PostCommitConflictThenExpireOneSnapshot if call == 1 => {
                    // `Transaction` checks each action's requirements against the
                    // intermediate table. A catalog re-checks them all against its
                    // own base, which the second append's requirement (the first
                    // append's snapshot, created by this same commit) cannot
                    // satisfy, so this test double applies the updates alone.
                    let _ = commit.take_requirements();
                    let committed = self.inner.update_table(commit).await?;
                    let current_snapshot_id = committed
                        .metadata()
                        .current_snapshot()
                        .unwrap()
                        .snapshot_id();
                    let expired_snapshot_id = committed
                        .metadata()
                        .snapshots()
                        .map(|snapshot| snapshot.snapshot_id())
                        .find(|id| *id != current_snapshot_id)
                        .expect("attempt 1 should commit two snapshots");
                    *self.first_attempt_snapshots.lock().unwrap() = committed
                        .metadata()
                        .snapshots()
                        .map(|snapshot| {
                            (snapshot.snapshot_id(), snapshot.manifest_list().to_string())
                        })
                        .collect();
                    let expire = TableCommit::builder()
                        .ident(committed.identifier().clone())
                        .updates(vec![TableUpdate::RemoveSnapshots {
                            snapshot_ids: vec![expired_snapshot_id],
                        }])
                        .requirements(vec![])
                        .build();
                    self.inner.update_table(expire).await?;
                    Err(Error::new(
                        ErrorKind::CatalogCommitConflicts,
                        "simulated conflict after a concurrent snapshot expiration",
                    )
                    .with_retryable(true))
                }
                FaultMode::PreCommitConflictWithSameSnapshotId if call == 1 => {
                    let ident = commit.identifier().clone();
                    let requirements = commit.take_requirements();
                    let table = self.inner.load_table(&ident).await?;
                    let mut updates = Vec::new();
                    for update in commit.take_updates() {
                        match update {
                            TableUpdate::AddSnapshot { mut snapshot } => {
                                // Another writer committed the same snapshot id with
                                // its own manifest list, which must not be mistaken
                                // for this attempt's output.
                                let foreign = format!("{}.foreign", snapshot.manifest_list());
                                let content = table
                                    .file_io()
                                    .new_input(snapshot.manifest_list())?
                                    .read()
                                    .await?;
                                table.file_io().new_output(&foreign)?.write(content).await?;
                                snapshot.manifest_list = foreign;
                                updates.push(TableUpdate::AddSnapshot { snapshot });
                            }
                            update => updates.push(update),
                        }
                    }
                    let foreign = TableCommit::builder()
                        .ident(ident)
                        .updates(updates)
                        .requirements(requirements)
                        .build();
                    self.inner.update_table(foreign).await?;
                    Err(Error::new(
                        ErrorKind::CatalogCommitConflicts,
                        "simulated conflict with the same snapshot id under a different manifest list",
                    )
                    .with_retryable(true))
                }
                FaultMode::PreCommitConflictOnce if call == 1 => {
                    let extra_file = self.concurrent_file_after_fault.lock().unwrap().take();
                    if let Some(file) = extra_file {
                        let table = self.inner.load_table(commit.identifier()).await?;
                        let tx = Transaction::new(&table);
                        let tx = tx.fast_append().add_data_files(vec![file]).apply(tx)?;
                        let _ = tx.commit(&*self.inner).await?;
                    }
                    Err(Error::new(
                        ErrorKind::CatalogCommitConflicts,
                        "simulated genuine pre-commit conflict",
                    )
                    .with_retryable(true))
                }
                FaultMode::DelayedFirstCommit if call == 1 => {
                    *self.delayed_commit.lock().unwrap() = Some(commit);
                    Err(Error::new(
                        ErrorKind::CatalogCommitConflicts,
                        "simulated delayed in-flight commit",
                    )
                    .with_retryable(true))
                }
                FaultMode::DelayedFirstCommit if call == 2 => {
                    let first_commit = self
                        .delayed_commit
                        .lock()
                        .unwrap()
                        .take()
                        .expect("attempt 1 commit should be stashed");
                    let _ = self.inner.update_table(first_commit).await?;
                    Err(Error::new(
                        ErrorKind::CatalogCommitConflicts,
                        "simulated conflict because delayed attempt 1 just landed",
                    )
                    .with_retryable(true))
                }
                _ => self.inner.update_table(commit).await,
            }
        }
    }

    fn make_test_data_file(path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition(Struct::from_iter([Some(Literal::long(1))]))
            .partition_spec_id(0)
            .build()
            .unwrap()
    }

    /// Rewrites a manifest list path from attempt 0 to `attempt`, so a test can
    /// check that the retry wrote nothing there.
    fn manifest_list_at_attempt(path: &str, attempt: u64) -> String {
        assert!(
            path.contains("-0-"),
            "expected an attempt 0 manifest list, got {path}"
        );
        path.replacen("-0-", &format!("-{attempt}-"), 1)
    }

    /// The manifest list written for the committed snapshot must be the one from
    /// attempt 0, and the retry must not leave attempt 1's manifest list behind.
    async fn assert_attempt_0_committed_without_orphan(committed: &Table, commit_uuid: Uuid) {
        let snapshot = committed.metadata().current_snapshot().unwrap();
        assert!(
            snapshot
                .manifest_list()
                .contains(&format!("-0-{commit_uuid}")),
            "unexpected manifest list {}",
            snapshot.manifest_list()
        );
        assert!(
            committed
                .file_io()
                .exists(snapshot.manifest_list())
                .await
                .unwrap()
        );
        let retried = manifest_list_at_attempt(snapshot.manifest_list(), 1);
        assert!(
            !committed.file_io().exists(&retried).await.unwrap(),
            "retry wrote an orphan manifest list at {retried}"
        );
    }

    #[tokio::test]
    async fn test_commit_retry_detects_already_committed_snapshot_with_check_duplicate() {
        let catalog = FaultyCatalog::new(FaultMode::PostCommitConflictOnce).await;
        let table = with_retry_properties(make_v3_minimal_table_in_catalog(&catalog).await, "3");
        let commit_uuid = Uuid::now_v7();

        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .set_commit_uuid(commit_uuid)
            .add_data_files(vec![make_test_data_file("test/file1.parquet")])
            .apply(tx)
            .unwrap();

        let committed = tx.commit(&catalog).await.unwrap();
        assert_eq!(catalog.update_calls(), 1);
        assert_eq!(committed.metadata().snapshots().count(), 1);
        assert_attempt_0_committed_without_orphan(&committed, commit_uuid).await;
    }

    #[tokio::test]
    async fn test_commit_retry_detects_already_committed_snapshot_without_check_duplicate() {
        let catalog = FaultyCatalog::new(FaultMode::PostCommitConflictOnce).await;
        let table = with_retry_properties(make_v3_minimal_table_in_catalog(&catalog).await, "3");
        let commit_uuid = Uuid::now_v7();

        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .with_check_duplicate(false)
            .set_commit_uuid(commit_uuid)
            .add_data_files(vec![make_test_data_file("test/file1.parquet")])
            .apply(tx)
            .unwrap();

        let committed = tx.commit(&catalog).await.unwrap();
        assert_eq!(catalog.update_calls(), 1);
        assert_eq!(committed.metadata().snapshots().count(), 1);
        assert_attempt_0_committed_without_orphan(&committed, commit_uuid).await;
    }

    #[tokio::test]
    async fn test_commit_retry_detects_already_committed_snapshot_with_concurrent_writer() {
        let catalog = FaultyCatalog::new(FaultMode::PostCommitConflictOnce).await;
        let table = with_retry_properties(make_v3_minimal_table_in_catalog(&catalog).await, "3");
        catalog.inject_concurrent_append(make_test_data_file("test/concurrent.parquet"));
        let commit_uuid = Uuid::now_v7();

        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .set_commit_uuid(commit_uuid)
            .add_data_files(vec![make_test_data_file("test/file1.parquet")])
            .apply(tx)
            .unwrap();

        let committed = tx.commit(&catalog).await.unwrap();
        assert_eq!(catalog.update_calls(), 1);
        assert_eq!(committed.metadata().snapshots().count(), 2);
        let current = committed.metadata().current_snapshot().unwrap();
        let parent_id = current.parent_snapshot_id().unwrap();
        // Our snapshot survived underneath the concurrent write.
        let parent = committed.metadata().snapshot_by_id(parent_id).unwrap();
        assert!(
            parent.manifest_list().contains(&commit_uuid.to_string()),
            "unexpected parent manifest list {}",
            parent.manifest_list()
        );
    }

    #[tokio::test]
    async fn test_commit_retry_rebases_on_genuine_pre_commit_conflict() {
        let catalog = FaultyCatalog::new(FaultMode::PreCommitConflictOnce).await;
        let table = with_retry_properties(make_v3_minimal_table_in_catalog(&catalog).await, "3");
        catalog.inject_concurrent_append(make_test_data_file("test/other.parquet"));

        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .add_data_files(vec![make_test_data_file("test/file1.parquet")])
            .apply(tx)
            .unwrap();

        let committed = tx.commit(&catalog).await.unwrap();
        assert_eq!(catalog.update_calls(), 2);
        assert_eq!(committed.metadata().snapshots().count(), 2);
    }

    #[tokio::test]
    async fn test_commit_retry_detects_delayed_first_attempt_after_second_attempt() {
        let catalog = FaultyCatalog::new(FaultMode::DelayedFirstCommit).await;
        let table = with_retry_properties(make_v3_minimal_table_in_catalog(&catalog).await, "3");
        let commit_uuid = Uuid::now_v7();

        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .set_commit_uuid(commit_uuid)
            .add_data_files(vec![make_test_data_file("test/file1.parquet")])
            .apply(tx)
            .unwrap();

        let committed = tx.commit(&catalog).await.unwrap();
        assert_eq!(catalog.update_calls(), 2);
        assert_eq!(committed.metadata().snapshots().count(), 1);

        // Attempt 1 is the one that landed; attempt 2 was discarded with the
        // conflict it returned, and attempt 3 wrote nothing.
        let snapshot = committed.metadata().current_snapshot().unwrap();
        assert!(
            snapshot
                .manifest_list()
                .contains(&format!("-0-{commit_uuid}")),
            "unexpected manifest list {}",
            snapshot.manifest_list()
        );
        let third_attempt = manifest_list_at_attempt(snapshot.manifest_list(), 2);
        assert!(
            !committed.file_io().exists(&third_attempt).await.unwrap(),
            "attempt 3 wrote a manifest list at {third_attempt}"
        );
    }

    /// A transaction that adds several snapshots commits them atomically. If one
    /// of them is expired concurrently, a surviving witness still proves the
    /// attempt landed and the retry must not re-run the actions.
    #[tokio::test]
    async fn test_commit_retry_detects_partially_expired_multi_snapshot_commit() {
        let catalog = FaultyCatalog::new(FaultMode::PostCommitConflictThenExpireOneSnapshot).await;
        let table = with_retry_properties(make_v3_minimal_table_in_catalog(&catalog).await, "3");

        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .add_data_files(vec![make_test_data_file("test/file1.parquet")])
            .apply(tx)
            .unwrap();
        let tx = tx
            .fast_append()
            .add_data_files(vec![make_test_data_file("test/file2.parquet")])
            .apply(tx)
            .unwrap();

        let committed = tx.commit(&catalog).await.unwrap();
        assert_eq!(catalog.update_calls(), 1);
        assert_eq!(committed.metadata().snapshots().count(), 1);

        let submitted = catalog.first_attempt_snapshots();
        assert_eq!(submitted.len(), 2);
        let surviving = committed.metadata().current_snapshot().unwrap();
        assert!(
            submitted
                .iter()
                .any(|(_, manifest_list)| manifest_list == surviving.manifest_list()),
            "the surviving snapshot {} was not submitted in attempt 1",
            surviving.manifest_list()
        );
        for (_, manifest_list) in &submitted {
            assert!(committed.file_io().exists(manifest_list).await.unwrap());
            let retried = manifest_list_at_attempt(manifest_list, 1);
            assert!(
                !committed.file_io().exists(&retried).await.unwrap(),
                "retry wrote an orphan manifest list at {retried}"
            );
        }
    }

    /// A failed refresh must not discard the witnesses recorded by the attempt
    /// that landed.
    #[tokio::test]
    async fn test_commit_retry_survives_transient_load_failure() {
        let catalog = FaultyCatalog::new(FaultMode::PostCommitConflictThenLoadFailure).await;
        let table = with_retry_properties(make_v3_minimal_table_in_catalog(&catalog).await, "3");
        let commit_uuid = Uuid::now_v7();

        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .set_commit_uuid(commit_uuid)
            .add_data_files(vec![make_test_data_file("test/file1.parquet")])
            .apply(tx)
            .unwrap();

        let committed = tx.commit(&catalog).await.unwrap();
        assert_eq!(catalog.load_calls(), 3);
        assert_eq!(catalog.update_calls(), 1);
        assert_eq!(committed.metadata().snapshots().count(), 1);
        assert_attempt_0_committed_without_orphan(&committed, commit_uuid).await;
    }

    /// A witness is only proof when the manifest list matches. A concurrent
    /// writer that picks the same snapshot id with its own manifest list must not
    /// make the retry report that snapshot as its own commit.
    #[tokio::test]
    async fn test_commit_retry_does_not_trust_snapshot_with_different_manifest_list() {
        let catalog = FaultyCatalog::new(FaultMode::PreCommitConflictWithSameSnapshotId).await;
        let table = with_retry_properties(make_v3_minimal_table_in_catalog(&catalog).await, "3");

        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .with_check_duplicate(false)
            .add_data_files(vec![make_test_data_file("test/file1.parquet")])
            .apply(tx)
            .unwrap();

        let err = match tx.commit(&catalog).await {
            Ok(table) => panic!(
                "retry accepted a snapshot with a different manifest list: {}",
                table.metadata().current_snapshot().unwrap().manifest_list()
            ),
            Err(err) => err,
        };
        assert_eq!(
            err.kind(),
            ErrorKind::DataInvalid,
            "unexpected error {err:?}"
        );

        let refreshed = catalog.load_table(table.identifier()).await.unwrap();
        let current = refreshed.metadata().current_snapshot().unwrap();
        assert!(current.manifest_list().ends_with(".foreign"));
    }
}

#[cfg(test)]
mod test_row_lineage {
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, Struct,
    };
    use crate::transaction::tests::make_v3_minimal_table_in_catalog;
    use crate::transaction::{ApplyTransactionAction, Transaction};

    #[tokio::test]
    async fn test_fast_append_with_row_lineage() {
        // Helper function to create a data file with specified number of rows
        fn file_with_rows(record_count: u64) -> DataFile {
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(format!("test/{record_count}.parquet"))
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(100)
                .record_count(record_count)
                .partition(Struct::from_iter([Some(Literal::long(0))]))
                .partition_spec_id(0)
                .build()
                .unwrap()
        }
        let catalog = new_memory_catalog().await;

        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Check initial state - next_row_id should be 0
        assert_eq!(table.metadata().next_row_id(), 0);

        // First fast append with 30 rows
        let tx = Transaction::new(&table);
        let data_file_30 = file_with_rows(30);
        let action = tx.fast_append().add_data_files(vec![data_file_30]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Check snapshot and table state after first append
        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.first_row_id(), Some(0));
        assert_eq!(table.metadata().next_row_id(), 30);

        // Check written manifest for first_row_id
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();

        assert_eq!(manifest_list.entries().len(), 1);
        let manifest_file = &manifest_list.entries()[0];
        assert_eq!(manifest_file.first_row_id, Some(0));

        // Second fast append with 17 and 11 rows
        let tx = Transaction::new(&table);
        let data_file_17 = file_with_rows(17);
        let data_file_11 = file_with_rows(11);
        let action = tx
            .fast_append()
            .add_data_files(vec![data_file_17, data_file_11]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Check snapshot and table state after second append
        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.first_row_id(), Some(30));
        assert_eq!(table.metadata().next_row_id(), 30 + 17 + 11);

        // Check written manifest for first_row_id
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
        assert_eq!(manifest_list.entries().len(), 2);
        let manifest_file = &manifest_list.entries()[1];
        assert_eq!(manifest_file.first_row_id, Some(30));
    }
}
