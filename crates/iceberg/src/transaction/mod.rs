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

use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use backon::{BackoffBuilder, ExponentialBackoff, ExponentialBuilder, RetryableWithContext};
pub use update_schema::AddColumn;

use crate::error::Result;
use crate::spec::{TableMetadata, TableMetadataBuilder, TableProperties};
use crate::table::Table;
use crate::transaction::action::BoxedTransactionAction;
use crate::transaction::append::FastAppendAction;
use crate::transaction::expire_snapshots::ExpireSnapshotsAction;
use crate::transaction::sort_order::ReplaceSortOrderAction;
use crate::transaction::update_location::UpdateLocationAction;
use crate::transaction::update_properties::UpdatePropertiesAction;
use crate::transaction::update_schema::UpdateSchemaAction;
use crate::transaction::update_statistics::UpdateStatisticsAction;
use crate::transaction::upgrade_format_version::UpgradeFormatVersionAction;
use crate::{Catalog, StagedCreateCatalog, TableCommit, TableRequirement, TableUpdate};

/// Table transaction.
#[derive(Clone)]
pub struct Transaction {
    table: Table,
    actions: Vec<BoxedTransactionAction>,
}

impl Transaction {
    /// Creates a new transaction.
    pub fn new(table: &Table) -> Self {
        Self {
            table: table.clone(),
            actions: vec![],
        }
    }

    /// Returns the base this transaction's actions are applied on top of.
    ///
    /// It may already be stale — [`Transaction::commit`] re-reads the table and replays the
    /// actions against whatever it finds. Do not treat it as the table's current state.
    pub fn table(&self) -> &Table {
        &self.table
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
        existing_requirements.extend(requirements);

        Ok(updated_table)
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

    /// Commits against an existing table, retrying while the catalog reports a conflict.
    pub async fn commit(self, catalog: &dyn Catalog) -> Result<Table> {
        if self.actions.is_empty() {
            // nothing to commit
            return Ok(self.table);
        }

        let backoff = Self::build_backoff(self.table.metadata().table_properties()?)?;
        let tx = self;

        (|mut tx: Transaction| async {
            let result = tx.do_commit_update(catalog).await;
            (tx, result)
        })
        .retry(backoff)
        .sleep(tokio::time::sleep)
        .context(tx)
        .when(|e| e.retryable())
        .await
        .1
    }

    fn build_backoff(props: TableProperties) -> Result<ExponentialBackoff> {
        Ok(ExponentialBuilder::new()
            .with_min_delay(Duration::from_millis(props.commit_min_retry_wait_ms()))
            .with_max_delay(Duration::from_millis(props.commit_max_retry_wait_ms()))
            .with_total_delay(Some(Duration::from_millis(
                props.commit_total_retry_timeout_ms(),
            )))
            .with_max_times(props.commit_num_retries())
            .with_factor(2.0)
            .build())
    }

    async fn do_commit_update(&mut self, catalog: &dyn Catalog) -> Result<Table> {
        let refreshed = catalog.load_table(self.table.identifier()).await?;

        if self.table.metadata() != refreshed.metadata()
            || self.table.metadata_location() != refreshed.metadata_location()
        {
            // current base is stale, use refreshed as base and re-apply transaction actions
            self.table = refreshed;
        }

        let mut updates = vec![];
        let mut requirements = vec![];
        self.run_actions(self.table.clone(), &mut updates, &mut requirements)
            .await?;

        catalog
            .update_table(
                TableCommit::builder()
                    .ident(self.table.identifier().to_owned())
                    .updates(updates)
                    .requirements(requirements)
                    .build(),
            )
            .await
    }

    /// Runs every action against `base` in order, threading each action's result into the
    /// next, and appends the updates and requirements they derive to the given vectors.
    async fn run_actions(
        &self,
        base: Table,
        updates: &mut Vec<TableUpdate>,
        requirements: &mut Vec<TableRequirement>,
    ) -> Result<Table> {
        let mut current_table = base;

        for action in &self.actions {
            let action_commit = Arc::clone(action).commit(&current_table).await?;
            current_table = Self::apply(current_table, action_commit, updates, requirements)?;
        }

        Ok(current_table)
    }
}

/// A transaction that creates the table it is opened on.
///
/// Handed out by [`StagedCreateCatalog::create_table_transaction`] over metadata the catalog
/// has staged but not registered. Actions are added exactly as on a [`Transaction`] — it
/// derefs to one — so data files can be written before anything can read the table. Only
/// [`CreateTableTransaction::commit`] makes it appear, atomically and already populated.
///
/// Separate from [`Transaction`] so that it can only be committed to a catalog that supports
/// staged creates.
#[derive(Clone)]
pub struct CreateTableTransaction(Transaction);

impl CreateTableTransaction {
    /// Creates a transaction that creates `table` when committed.
    ///
    /// `table` is metadata a catalog has staged: it is not registered anywhere until the
    /// transaction commits. Intended for [`TransactionalCatalog::create_table_transaction`]
    /// implementations rather than direct use.
    ///
    /// [`TransactionalCatalog::create_table_transaction`]: crate::StagedCreateCatalog::create_table_transaction
    pub fn new(table: Table) -> Self {
        Self(Transaction {
            table,
            actions: vec![],
        })
    }

    /// Creates the table, with everything this transaction's actions produced already in it.
    ///
    /// The commit describes the whole table rather than a diff, since the catalog has nothing
    /// to diff against. It never refreshes a base table, because there is none, and it is
    /// never retried: it asserts that the table does not exist, so a conflict means another
    /// writer created it and no number of retries can make the assertion hold again.
    pub async fn commit(self, catalog: &dyn StagedCreateCatalog) -> Result<Table> {
        // Derived before the actions run, so the list describes the staged table alone and
        // the actions' own updates follow it.
        let mut updates = create_updates(self.0.table.metadata());

        // Requirements the actions derive are assertions about a base table. A create has no
        // base, so they are trivially true; the catalog asserts non-existence itself.
        let mut discarded_requirements = vec![];
        self.0
            .run_actions(
                self.0.table.clone(),
                &mut updates,
                &mut discarded_requirements,
            )
            .await?;

        catalog
            .commit_create_table(self.0.table.identifier().to_owned(), updates)
            .await
    }
}

impl Deref for CreateTableTransaction {
    type Target = Transaction;

    fn deref(&self) -> &Transaction {
        &self.0
    }
}

/// Builds the update list that recreates `metadata` starting from an empty table.
///
/// A staged create is committed against a table that does not exist, so the commit has to
/// carry the whole table rather than a diff. Receivers apply these updates to an empty
/// metadata builder, which is why the schema, spec and sort order are referred to by
/// [`TableMetadataBuilder::LAST_ADDED`] rather than by their concrete ids, and why the
/// format version is sent as an upgrade from nothing.
///
/// Only the current schema and the default spec and sort order are emitted: a staged
/// create has no history to preserve, and snapshots produced by the transaction's actions
/// are appended after this list by [`CreateTableTransaction::commit`].
fn create_updates(metadata: &TableMetadata) -> Vec<TableUpdate> {
    let mut updates = vec![
        TableUpdate::AssignUuid {
            uuid: metadata.uuid(),
        },
        TableUpdate::UpgradeFormatVersion {
            format_version: metadata.format_version(),
        },
        TableUpdate::AddSchema {
            schema: metadata.current_schema().as_ref().clone(),
        },
        TableUpdate::SetCurrentSchema {
            schema_id: TableMetadataBuilder::LAST_ADDED,
        },
        TableUpdate::AddSpec {
            spec: metadata
                .default_partition_spec()
                .as_ref()
                .clone()
                .into_unbound(),
        },
        TableUpdate::SetDefaultSpec {
            spec_id: TableMetadataBuilder::LAST_ADDED,
        },
        TableUpdate::AddSortOrder {
            sort_order: metadata.default_sort_order().as_ref().clone(),
        },
        TableUpdate::SetDefaultSortOrder {
            sort_order_id: i64::from(TableMetadataBuilder::LAST_ADDED),
        },
        TableUpdate::SetLocation {
            location: metadata.location().to_string(),
        },
    ];
    if !metadata.properties().is_empty() {
        updates.push(TableUpdate::SetProperties {
            updates: metadata.properties().clone(),
        });
    }
    updates.extend(
        metadata
            .encryption_keys_iter()
            .cloned()
            .map(|encryption_key| TableUpdate::AddEncryptionKey { encryption_key }),
    );
    updates
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use crate::catalog::MockCatalog;
    use crate::io::FileIO;
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, FormatVersion, Literal, NestedField,
        NullOrder, PartitionSpec, PrimitiveType, Schema, SortDirection, SortField, SortOrder,
        Struct, TableMetadata, TableMetadataBuilder, TableProperties, Transform, Type,
    };
    use crate::table::Table;
    use crate::test_utils::{make_encrypted_table, test_runtime};
    use crate::transaction::{ApplyTransactionAction, Transaction, create_updates};
    use crate::{Catalog, Error, ErrorKind, TableCreation, TableIdent, TableUpdate};

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

    pub(crate) async fn make_v3_minimal_table_in_catalog(catalog: &impl Catalog) -> Table {
        let table_ident =
            TableIdent::from_strs([format!("ns1-{}", uuid::Uuid::new_v4()), "test1".to_string()])
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
            .format_version(FormatVersion::V3)
            .build();

        catalog
            .create_table(table_ident.namespace(), table_creation)
            .await
            .unwrap()
    }

    /// Helper function to create a test table with retry properties
    pub(super) fn setup_test_table(num_retries: &str) -> Table {
        let table = make_v2_table();

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

    /// A staged create is committed against a table that does not exist, so `create_updates`
    /// has to describe the whole table. Pinning the list keeps an update from silently going
    /// missing, which would leave the created table subtly different from the staged one.
    #[test]
    fn test_create_updates_describes_the_whole_staged_table() {
        let metadata = staged_metadata(FormatVersion::V3);

        let last_added = TableMetadataBuilder::LAST_ADDED;
        assert_eq!(
            vec![
                TableUpdate::AssignUuid {
                    uuid: metadata.uuid()
                },
                TableUpdate::UpgradeFormatVersion {
                    format_version: FormatVersion::V3
                },
                TableUpdate::AddSchema {
                    schema: metadata.current_schema().as_ref().clone()
                },
                TableUpdate::SetCurrentSchema {
                    schema_id: last_added
                },
                TableUpdate::AddSpec {
                    spec: metadata
                        .default_partition_spec()
                        .as_ref()
                        .clone()
                        .into_unbound()
                },
                TableUpdate::SetDefaultSpec {
                    spec_id: last_added
                },
                TableUpdate::AddSortOrder {
                    sort_order: metadata.default_sort_order().as_ref().clone()
                },
                TableUpdate::SetDefaultSortOrder {
                    sort_order_id: i64::from(last_added)
                },
                TableUpdate::SetLocation {
                    location: "s3://bucket/staged".to_string()
                },
                TableUpdate::SetProperties {
                    updates: metadata.properties().clone()
                },
            ],
            create_updates(&metadata)
        );
        assert!(
            metadata.properties().contains_key("custom.property"),
            "creation properties should survive into the staged metadata"
        );
    }

    /// Metadata as a catalog stages it: built from a [`TableCreation`], never persisted.
    fn staged_metadata(format_version: FormatVersion) -> TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "ts", Type::Primitive(PrimitiveType::Date)).into(),
            ])
            .build()
            .unwrap();
        let partition_spec = PartitionSpec::builder(schema.clone())
            .add_partition_field("ts", "ts_day", Transform::Day)
            .unwrap()
            .build()
            .unwrap();
        let sort_order = SortOrder::builder()
            .with_sort_field(
                SortField::builder()
                    .source_id(1)
                    .transform(Transform::Identity)
                    .direction(SortDirection::Ascending)
                    .null_order(NullOrder::First)
                    .build(),
            )
            .build(&schema)
            .unwrap();

        TableMetadataBuilder::from_table_creation(
            TableCreation::builder()
                .name("staged".to_string())
                .location("s3://bucket/staged".to_string())
                .schema(schema)
                .partition_spec(partition_spec)
                .sort_order(sort_order)
                .properties(HashMap::from([(
                    "custom.property".to_string(),
                    "custom.value".to_string(),
                )]))
                .format_version(format_version)
                .build(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }

    /// The whole point of the list: a catalog that owns its metadata rebuilds the staged table
    /// from it, exactly as a REST server does. Anything `create_updates` leaves out is lost.
    #[test]
    fn test_create_updates_rebuild_the_staged_table() {
        for format_version in [FormatVersion::V1, FormatVersion::V2, FormatVersion::V3] {
            let staged = staged_metadata(format_version);
            let rebuilt = TableMetadata::from_updates(create_updates(&staged)).unwrap();

            // Stamped at build time, and no part of what the updates describe.
            let without_timestamp = |metadata: &TableMetadata| {
                let mut json = serde_json::to_value(metadata).unwrap();
                json.as_object_mut().unwrap().remove("last-updated-ms");
                json
            };
            assert_eq!(
                without_timestamp(&staged),
                without_timestamp(&rebuilt),
                "rebuilt {format_version} metadata differs from the staged metadata"
            );
        }
    }

    /// The properties update is skipped entirely when there is nothing to set, since some
    /// catalogs reject an empty one.
    #[test]
    fn test_create_updates_omits_empty_properties() {
        let table = make_v2_table();
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .remove_properties(
                &table
                    .metadata()
                    .properties()
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>(),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        assert!(metadata.properties().is_empty());
        assert!(
            !create_updates(&metadata)
                .iter()
                .any(|update| matches!(update, TableUpdate::SetProperties { .. }))
        );
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
