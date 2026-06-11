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
mod cherry_pick;
mod delete_files;
mod expire_cleanup;
mod expire_snapshots;
pub use expire_cleanup::{
    CleanupFailure, CleanupFailureKind, CleanupReport, ExpireSnapshotsCleanup,
};
mod manage_snapshots;
mod merge_append;
mod overwrite_files;
mod replace_partitions;
mod rewrite_files;
mod rewrite_manifests;
mod row_delta;
mod snapshot;
mod sort_order;
mod update_location;
mod update_partition_spec;
mod update_properties;
mod update_schema;
mod update_statistics;
mod upgrade_format_version;

use std::sync::Arc;
use std::time::Duration;

use backon::{BackoffBuilder, ExponentialBackoff, ExponentialBuilder, RetryableWithContext};

use crate::error::Result;
use crate::spec::TableProperties;
use crate::table::Table;
use crate::transaction::action::BoxedTransactionAction;
use crate::transaction::append::FastAppendAction;
use crate::transaction::cherry_pick::CherryPickAction;
use crate::transaction::delete_files::DeleteFilesAction;
use crate::transaction::expire_snapshots::ExpireSnapshotsAction;
use crate::transaction::manage_snapshots::ManageSnapshotsAction;
use crate::transaction::merge_append::MergeAppendAction;
use crate::transaction::overwrite_files::OverwriteFilesAction;
use crate::transaction::replace_partitions::ReplacePartitionsAction;
use crate::transaction::rewrite_files::RewriteFilesAction;
use crate::transaction::rewrite_manifests::RewriteManifestsAction;
use crate::transaction::row_delta::RowDeltaAction;
use crate::transaction::sort_order::ReplaceSortOrderAction;
use crate::transaction::update_location::UpdateLocationAction;
use crate::transaction::update_partition_spec::UpdatePartitionSpecAction;
use crate::transaction::update_properties::UpdatePropertiesAction;
use crate::transaction::update_schema::UpdateSchemaAction;
use crate::transaction::update_statistics::UpdateStatisticsAction;
use crate::transaction::upgrade_format_version::UpgradeFormatVersionAction;
use crate::{Catalog, TableCommit, TableRequirement, TableUpdate};

/// Table transaction.
#[derive(Clone)]
pub struct Transaction {
    table: Table,
    /// The id of the table's current snapshot when this transaction was created (Java
    /// `SnapshotProducer.base.currentSnapshot()` captured at construction). This is the starting point for
    /// serializable-isolation conflict validation: an action's [`TransactionAction::validate`] enumerates the
    /// snapshots the refreshed base has that are NEWER than this id (the concurrent commits). It is captured
    /// ONCE in [`Transaction::new`] and is its OWN field precisely so it SURVIVES the staleness re-base in
    /// [`Transaction::do_commit`] (which overwrites `self.table` with the refreshed base, losing the original
    /// head). `None` means the table had no snapshots yet at transaction start.
    starting_snapshot_id: Option<i64>,
    actions: Vec<BoxedTransactionAction>,
}

impl Transaction {
    /// Creates a new transaction.
    pub fn new(table: &Table) -> Self {
        Self {
            table: table.clone(),
            starting_snapshot_id: table.metadata().current_snapshot_id(),
            actions: vec![],
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

    /// Creates a fast append action.
    pub fn fast_append(&self) -> FastAppendAction {
        FastAppendAction::new()
    }

    /// Creates a merge-append action: append data files in one `Operation::Append` snapshot (exactly like
    /// [`Self::fast_append`]) and then MERGE the resulting manifest list into a minimal number of
    /// manifests (Java `MergeAppend`). Java's `Table.newAppend()` returns this MERGING producer, whereas
    /// `newFastAppend()` returns the non-merging one this fork exposes as [`Self::fast_append`].
    ///
    /// The merge honors three table properties (read at commit time, Java `ManifestMergeManager`):
    /// - `commit.manifest-merge.enabled` (default `true`) — when `false`, the manifest list is left as-is
    ///   (the action then behaves like a fast append).
    /// - `commit.manifest.min-count-to-merge` (default `100`) — the bin holding this commit's NEW added
    ///   manifest is merged only once it accumulates at least this many manifests.
    /// - `commit.manifest.target-size-bytes` (default 8 MB) — the bin-packing target weight (by manifest
    ///   length).
    ///
    /// Merged manifests preserve every carried-forward entry's provenance (original snapshot id + data /
    /// file sequence numbers, status `Existing`); this commit's added entries stay `Added` and re-inherit
    /// the new snapshot's sequence number. The live file set is identical to the equivalent fast append.
    ///
    /// **Deferred (vs Java):** delete-manifest merging (delete manifests are carried forward unchanged),
    /// `appendManifest`, and the retry cache / orphan cleanup. See the
    /// [`merge_append`](crate::transaction) module for the full Java contract and deviations.
    pub fn merge_append(&self) -> MergeAppendAction {
        MergeAppendAction::new()
    }

    /// Creates a delete-files action (remove data files from the table by path / `DataFile`
    /// reference). Delete-by-row-filter / partition-predicate is not yet supported.
    pub fn delete_files(&self) -> DeleteFilesAction {
        DeleteFilesAction::new()
    }

    /// Creates an overwrite-files action (add data files AND remove data files in one snapshot). The
    /// recorded operation is dynamic, matching Java `BaseOverwriteFiles`: add-only → `Append`,
    /// delete-only → `Delete`, both → `Overwrite`. Opt-in filter-based concurrent-commit conflict
    /// validation is available via [`OverwriteFilesAction::validate_no_conflicting_data`]; overwrite-by-row-
    /// filter and `validateNoConflictingDeletes` are not yet supported.
    pub fn overwrite_files(&self) -> OverwriteFilesAction {
        OverwriteFilesAction::new()
    }

    /// Creates a replace-partitions action (dynamic partition overwrite). For every partition an added
    /// file belongs to, the action replaces all existing live data files in that same partition, then
    /// adds the new files, in one `Overwrite` snapshot (Java `BaseReplacePartitions`). On an unpartitioned
    /// table this is a full-table replace. Static `replaceByRowFilter` and concurrent-commit conflict
    /// validation are not yet supported.
    pub fn replace_partitions(&self) -> ReplacePartitionsAction {
        ReplacePartitionsAction::new()
    }

    /// Creates a rewrite-files action (the compaction-commit primitive): atomically replace a set of
    /// data files with a new set in one `Replace` snapshot (Java `BaseRewriteFiles`). The files to delete
    /// must be non-empty and present in the current snapshot. Rewriting DELETE files is not yet supported.
    ///
    /// **Preserving outstanding equality deletes:** by default the added files take a fresh, higher data
    /// sequence number, so an outstanding merge-on-read EQUALITY delete (which applies only to data with a
    /// strictly lower data seq) stops applying to them and silently resurrects deleted rows. Call
    /// [`RewriteFilesAction::data_sequence_number`] with the (max) data seq of the replaced files to preserve
    /// the seq so the deletes still apply (Java `RewriteFiles.dataSequenceNumber`). Without it this is the
    /// caller's responsibility — exactly as in Java, which has no guard against the hazard.
    /// [`RewriteFilesAction::validate`] rejects a commit when a concurrent row-level delete conflicts with a
    /// replaced data file (Java `validateNoNewDeletesForDataFiles`).
    pub fn rewrite_files(
        &self,
        files_to_delete: impl IntoIterator<Item = crate::spec::DataFile>,
        files_to_add: impl IntoIterator<Item = crate::spec::DataFile>,
    ) -> RewriteFilesAction {
        RewriteFilesAction::new().rewrite_files(files_to_delete, files_to_add)
    }

    /// Creates a rewrite-manifests action: re-organize the table's manifests without changing its
    /// live data files, producing one `Operation::Replace` snapshot (Java `BaseRewriteManifests`). Use
    /// [`RewriteManifestsAction::cluster_by`] to re-group matching data manifests' live entries into new
    /// manifests (provenance preserved), and/or [`RewriteManifestsAction::add_manifest`] +
    /// [`RewriteManifestsAction::delete_manifest`] for explicit replacement. A no-op rewrite keeps every
    /// manifest as-is. The live set is identical before and after.
    ///
    /// **Deferred:** `add_manifest` on a V1 table (`FeatureUnsupported`), the parallel `scanManifestsWith`
    /// executor, and metadata-level interop with Java (this is a 🟡 unit-proven action).
    pub fn rewrite_manifests(&self) -> RewriteManifestsAction {
        RewriteManifestsAction::new()
    }

    /// Creates a row-delta action (the merge-on-read write commit): add data files AND add row-level
    /// DELETE files (position / equality / deletion vectors) in ONE snapshot (Java `BaseRowDelta`).
    /// The added delete files are written into a DELETE manifest alongside the DATA manifest, and
    /// inherit the new snapshot's sequence number so they apply to data from earlier snapshots. The
    /// recorded operation is dynamic, matching Java `BaseRowDelta`: adds-data-only → `Append`,
    /// adds-deletes-only → `Delete`, both → `Overwrite`.
    ///
    /// Added delete files are FORMAT-VERSION gated at commit (Java `validateDeleteFileForVersion`):
    /// V1 rejects all deletes, V2 rejects Puffin deletion vectors, V3 REQUIRES position deletes to be
    /// deletion vectors; equality deletes are exempt at every version. Opt-in concurrent-commit
    /// conflict validation (`validate_no_conflicting_data_files` / `validate_no_conflicting_delete_files`
    /// / `validate_data_files_exist`) and the always-on deletion-vector conflict check
    /// (`validateAddedDVs`) are supported; the previous-deletes MERGE for DVs is deferred — a DV add
    /// for a data file with a live position-scoped delete is rejected (the fresh-DV-only door, see
    /// [`row_delta`](crate::transaction::row_delta)).
    pub fn row_delta(&self) -> RowDeltaAction {
        RowDeltaAction::new()
    }

    /// Creates replace sort order action.
    pub fn replace_sort_order(&self) -> ReplaceSortOrderAction {
        ReplaceSortOrderAction::new()
    }

    /// Creates a manage-snapshots action (branch/tag lifecycle, rollback, fast-forward, retention).
    pub fn manage_snapshots(&self) -> ManageSnapshotsAction {
        ManageSnapshotsAction::new()
    }

    /// Creates an expire-snapshots action — the METADATA retention semantics of Java
    /// `ExpireSnapshots` (`Table.expireSnapshots()` / core `RemoveSnapshots`): per-branch
    /// age+count retention, branch/tag `max_ref_age_ms` ref expiry (`main` never expires),
    /// unreferenced-snapshot retention, and explicit [`ExpireSnapshotsAction::expire_snapshot_id`],
    /// honoring the `history.expire.*` table properties.
    ///
    /// **THIS ACTION NEVER DELETES FILES.** It emits `RemoveSnapshots` / `RemoveSnapshotRef`
    /// updates and nothing else. Physical cleanup of newly-unreachable manifest lists /
    /// manifests / content files / statistics files (Java's `cleanExpiredFiles(true)` default,
    /// `ReachableFileCleanup`) is the EXPLICIT post-commit step [`ExpireSnapshotsCleanup`] —
    /// run it via [`ExpireSnapshotsCleanup::commit_and_clean`], which commits the transaction
    /// and cleans only on success (the Java `RemoveSnapshots.commit()` ordering). See
    /// [`expire_snapshots`](crate::transaction::expire_snapshots) for the retention contract
    /// and [`expire_cleanup`](crate::transaction::expire_cleanup) for the cleanup contract.
    pub fn expire_snapshots(&self) -> ExpireSnapshotsAction {
        ExpireSnapshotsAction::new()
    }

    /// Creates a cherry-pick action: PUBLISH a staged snapshot onto `main` (write-audit-publish), mirroring
    /// Java `CherryPickOperation` (exposed in Java via `ManageSnapshots.cherrypick`). Given the id of a
    /// snapshot that exists in metadata but is not on `main` (e.g. a WAP audit job's staged snapshot), it
    /// either fast-forwards `main` to that snapshot (when its parent is the current head — no new snapshot) or
    /// replays its added (and, for a dynamic-overwrite snapshot, removed) data files into a new published
    /// snapshot tagged with `source-snapshot-id` / `published-wap-id`. Rejects an already-published snapshot,
    /// a double cherry-pick (via `source-snapshot-id`), and a duplicate `wap.id`. See
    /// [`crate::transaction::cherry_pick`] for the full contract.
    pub fn cherry_pick(&self, snapshot_id: i64) -> CherryPickAction {
        CherryPickAction::new(snapshot_id)
    }

    /// Creates an update-partition-spec action (partition evolution: add/remove/rename fields).
    pub fn update_partition_spec(&self) -> UpdatePartitionSpecAction {
        UpdatePartitionSpecAction::new()
    }

    /// Creates an update-schema action (schema evolution: add/rename/update/delete/move columns,
    /// identifier fields, union-by-name).
    pub fn update_schema(&self) -> UpdateSchemaAction {
        UpdateSchemaAction::new()
    }

    /// Set the location of table
    pub fn update_location(&self) -> UpdateLocationAction {
        UpdateLocationAction::new()
    }

    /// Update the statistics of table
    pub fn update_statistics(&self) -> UpdateStatisticsAction {
        UpdateStatisticsAction::new()
    }

    /// Commit transaction.
    pub async fn commit(self, catalog: &dyn Catalog) -> Result<Table> {
        if self.actions.is_empty() {
            // nothing to commit
            return Ok(self.table);
        }

        let table_props = self.table.metadata().table_properties()?;

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

    fn build_backoff(props: TableProperties) -> Result<ExponentialBackoff> {
        Ok(ExponentialBuilder::new()
            .with_min_delay(Duration::from_millis(props.commit_min_retry_wait_ms))
            .with_max_delay(Duration::from_millis(props.commit_max_retry_wait_ms))
            .with_total_delay(Some(Duration::from_millis(
                props.commit_total_retry_timeout_ms,
            )))
            .with_max_times(props.commit_num_retries)
            .with_factor(2.0)
            .build())
    }

    async fn do_commit(&mut self, catalog: &dyn Catalog) -> Result<Table> {
        let refreshed = catalog.load_table(self.table.identifier()).await?;

        if self.table.metadata() != refreshed.metadata()
            || self.table.metadata_location() != refreshed.metadata_location()
        {
            // current base is stale, use refreshed as base and re-apply transaction actions
            self.table = refreshed.clone();
        }

        let mut current_table = self.table.clone();
        let mut existing_updates: Vec<TableUpdate> = vec![];
        let mut existing_requirements: Vec<TableRequirement> = vec![];

        // Serializable-isolation conflict validation (Java `SnapshotProducer.validate`): run each action's
        // `validate` against the REFRESHED base BEFORE re-applying any updates. `current_table` here is the
        // refreshed base, so an action can enumerate the snapshots it has that are newer than
        // `starting_snapshot_id` (the concurrent commits) and reject conflicts. A validation failure is
        // non-retryable, so it propagates out of the retry loop instead of looping (Java's
        // non-retryable `ValidationException`). The default `validate` is a no-op, so opt-out actions skip it.
        for action in &self.actions {
            Arc::clone(action)
                .validate(self.starting_snapshot_id, &current_table)
                .await?;
        }

        for action in &self.actions {
            let action_commit = Arc::clone(action).commit(&current_table).await?;
            // apply action commit to current_table
            current_table = Self::apply(
                current_table,
                action_commit,
                &mut existing_updates,
                &mut existing_requirements,
            )?;
        }

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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use crate::catalog::MockCatalog;
    use crate::io::FileIO;
    use crate::spec::TableMetadata;
    use crate::table::Table;
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::{Catalog, Error, ErrorKind, TableCreation, TableIdent};

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
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIO::new_with_memory())
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
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIO::new_with_memory())
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
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIO::new_with_memory())
            .build()
            .unwrap()
    }

    /// Create a fresh table in `catalog` from a minimal-metadata template (the shared body of
    /// [`make_v2_minimal_table_in_catalog`] / [`make_v3_minimal_table_in_catalog`]). The
    /// V2ValidMinimal and V3ValidMinimal templates carry the SAME schema fields (x/y/z longs;
    /// V3's `x` additionally has `initial-default`/`write-default` = 1, irrelevant to tests that
    /// write x explicitly) and the IDENTICAL partition spec (`identity(x)`), so a test can pick
    /// the format version its delete content requires (V2 ⇒ parquet position deletes, V3 ⇒
    /// deletion vectors — the `validateDeleteFileForVersion` gate) without changing any fixture
    /// data.
    async fn make_minimal_table_in_catalog(
        catalog: &impl Catalog,
        metadata_template: &str,
        format_version: crate::spec::FormatVersion,
    ) -> Table {
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
            metadata_template
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let base_metadata = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        let table_creation = TableCreation::builder()
            .schema((**base_metadata.current_schema()).clone())
            .partition_spec((**base_metadata.default_partition_spec()).clone())
            .sort_order((**base_metadata.default_sort_order()).clone())
            .name(table_ident.name().to_string())
            .format_version(format_version)
            .build();

        catalog
            .create_table(table_ident.namespace(), table_creation)
            .await
            .unwrap()
    }

    /// A fresh V2 table in `catalog` — the fixture for tests that commit PARQUET position deletes
    /// (V3 requires deletion vectors instead: Java `validateDeleteFileForVersion`, "Must use DVs
    /// for position deletes in V3"). Same schema/spec as the V3 fixture.
    pub(crate) async fn make_v2_minimal_table_in_catalog(catalog: &impl Catalog) -> Table {
        make_minimal_table_in_catalog(
            catalog,
            "TableMetadataV2ValidMinimal.json",
            crate::spec::FormatVersion::V2,
        )
        .await
    }

    pub(crate) async fn make_v3_minimal_table_in_catalog(catalog: &impl Catalog) -> Table {
        make_minimal_table_in_catalog(
            catalog,
            "TableMetadataV3ValidMinimal.json",
            crate::spec::FormatVersion::V3,
        )
        .await
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
        let manifest_list = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

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
        let manifest_list = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert_eq!(manifest_list.entries().len(), 2);
        let manifest_file = &manifest_list.entries()[1];
        assert_eq!(manifest_file.first_row_id, Some(30));
    }
}
