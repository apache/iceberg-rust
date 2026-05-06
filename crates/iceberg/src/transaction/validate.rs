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

//! Snapshot validation utilities used by transaction actions that need to
//! detect concurrent writes to the same table state (for example
//! `RewriteFiles`, `RowDelta` and `OverwriteFiles`).
//!
//! The shape of [`SnapshotValidator`] mirrors the Java equivalents on
//! `MergingSnapshotProducer` (`validateNoNewDeletesForDataFiles`,
//! `validationHistory`).

use std::collections::HashSet;
use std::sync::{Arc, LazyLock};

use futures::SinkExt;
use futures::future::try_join_all;

use crate::delete_file_index::DeleteFileIndex;
use crate::scan::DeleteFileContext;
use crate::spec::{
    DataContentType, DataFile, FormatVersion, INITIAL_SEQUENCE_NUMBER, ManifestContentType,
    ManifestFile, Operation, SnapshotRef,
};
use crate::table::Table;
use crate::util::snapshot::ancestors_between;
use crate::{Error, ErrorKind, Result};

/// Snapshot operations that publish new delete files.
///
/// Mirrors `MergingSnapshotProducer.VALIDATE_ADDED_DELETE_FILES_OPERATIONS`
/// in iceberg-java. Used as the default operation filter when a validator
/// looks for delete files added since a starting snapshot.
// Allowed dead-code: consumed by the methods on `SnapshotValidator`, which
// will be wired into transaction actions (e.g. `RewriteFilesAction`) in a
// follow-up PR tracking https://github.com/apache/iceberg-rust/issues/1607.
#[allow(dead_code)]
pub(crate) static VALIDATE_ADDED_DELETE_FILES_OPERATIONS: LazyLock<HashSet<Operation>> =
    LazyLock::new(|| HashSet::from([Operation::Overwrite, Operation::Delete]));

/// Trait that a transaction action implements when it needs to validate the
/// snapshot it is about to commit against the table state, typically to
/// detect conflicts with concurrent writes.
///
/// All methods have safe default implementations: [`SnapshotValidator::validate`]
/// is a no-op and the helper methods can be called as-is by any implementor
/// without further customisation. Operations such as `RewriteFiles` will
/// override [`SnapshotValidator::validate`] to chain the helpers they need.
// Allowed dead-code: the first consumer of this trait will be the
// `RewriteFilesAction` introduced as part of
// https://github.com/apache/iceberg-rust/issues/1607.
#[allow(dead_code)]
pub(crate) trait SnapshotValidator {
    /// Validates the operation against `base` rooted at `parent_snapshot_id`.
    ///
    /// The default implementation is a no-op and individual operations
    /// (e.g. `RewriteFiles`, `RowDelta`) override it to compose the helper
    /// methods on this trait.
    ///
    /// # Arguments
    ///
    /// * `base` - The base table to validate against.
    /// * `parent_snapshot_id` - Snapshot id this commit will be applied on
    ///   top of, usually the latest snapshot of the base table. `None` when
    ///   the table currently has no snapshots.
    async fn validate(&self, _base: &Table, _parent_snapshot_id: Option<i64>) -> Result<()> {
        Ok(())
    }

    /// Returns the manifests and snapshot ids that participated in the
    /// requested operations between two points on the table's snapshot
    /// history.
    ///
    /// The traversal is performed in descending order from
    /// `to_snapshot_id` (inclusive) down to `from_snapshot_id`
    /// (exclusive). When `from_snapshot_id` is `None` the traversal walks
    /// to the root snapshot.
    ///
    /// Manifests are kept when:
    /// * the snapshot's [`Operation`] is in `matching_operations`, and
    /// * the manifest's [`ManifestContentType`] equals
    ///   `manifest_content_type`, and
    /// * the manifest is owned by the snapshot (i.e.
    ///   `manifest.added_snapshot_id == snapshot.snapshot_id()`), so each
    ///   manifest is visited at most once even when ancestors share
    ///   manifests.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::DataInvalid`] when the chain ends before
    /// reaching `from_snapshot_id` (history broken or `from_snapshot_id`
    /// is not an ancestor of `to_snapshot_id`).
    async fn validation_history(
        &self,
        base: &Table,
        from_snapshot_id: Option<i64>,
        to_snapshot_id: i64,
        matching_operations: &HashSet<Operation>,
        manifest_content_type: ManifestContentType,
    ) -> Result<(Vec<ManifestFile>, HashSet<i64>)> {
        let mut manifests: Vec<ManifestFile> = vec![];
        let mut new_snapshots: HashSet<i64> = HashSet::new();
        let mut last_snapshot: Option<SnapshotRef> = None;

        let metadata = Arc::new(base.metadata().clone());
        let snapshots = ancestors_between(&metadata, to_snapshot_id, from_snapshot_id);

        for current_snapshot in snapshots {
            last_snapshot = Some(current_snapshot.clone());

            if !matching_operations.contains(&current_snapshot.summary().operation) {
                continue;
            }

            new_snapshots.insert(current_snapshot.snapshot_id());

            let manifest_list = current_snapshot
                .load_manifest_list(base.file_io(), base.metadata())
                .await?;
            for entry in manifest_list.entries() {
                if entry.content == manifest_content_type
                    && entry.added_snapshot_id == current_snapshot.snapshot_id()
                {
                    manifests.push(entry.clone());
                }
            }
        }

        if let Some(last) = last_snapshot
            && last.parent_snapshot_id() != from_snapshot_id
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot determine history between starting snapshot {} and the last known ancestor {}",
                    from_snapshot_id
                        .map(|id| id.to_string())
                        .unwrap_or_else(|| "None".to_string()),
                    last.snapshot_id()
                ),
            ));
        }

        Ok((manifests, new_snapshots))
    }

    /// Validates that no new delete files have been published in the
    /// snapshot range `(from_snapshot_id, to_snapshot_id]` that would
    /// apply to any of the provided `data_files`.
    ///
    /// This is a no-op for V1 tables (which have no delete files) and
    /// when `to_snapshot_id` is `None` (no current table state).
    ///
    /// # Arguments
    ///
    /// * `base` - Base table to validate against.
    /// * `from_snapshot_id` - Starting snapshot id (exclusive); `None`
    ///   walks to the root.
    /// * `to_snapshot_id` - Ending snapshot id (inclusive); `None` when
    ///   the table currently has no snapshots.
    /// * `data_files` - Data files whose lifetime must not be reduced by
    ///   a concurrent delete.
    /// * `ignore_equality_deletes` - When `true`, only position-deletes
    ///   are treated as conflicts. Useful for `RewriteFiles`, where
    ///   equality deletes published at a later sequence number remain
    ///   logically applicable to the rewritten outputs.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::DataInvalid`] when at least one conflicting
    /// delete file is found.
    async fn validate_no_new_deletes_for_data_files(
        &self,
        base: &Table,
        from_snapshot_id: Option<i64>,
        to_snapshot_id: Option<i64>,
        data_files: &[DataFile],
        ignore_equality_deletes: bool,
    ) -> Result<()> {
        // V1 tables have no delete files; skip. Mirrors Java's
        // `if (parent == null || base.formatVersion() < 2) { return; }`
        // in MergingSnapshotProducer#validateNoNewDeletesForDataFiles.
        if to_snapshot_id.is_none() || base.metadata().format_version() == FormatVersion::V1 {
            return Ok(());
        }
        let to_snapshot_id = to_snapshot_id.unwrap();

        let (delete_manifests, _) = self
            .validation_history(
                base,
                from_snapshot_id,
                to_snapshot_id,
                &VALIDATE_ADDED_DELETE_FILES_OPERATIONS,
                ManifestContentType::Deletes,
            )
            .await?;

        let (delete_file_index, mut delete_file_tx) = DeleteFileIndex::new();
        let manifests = try_join_all(
            delete_manifests
                .iter()
                .map(|f| f.load_manifest(base.file_io())),
        )
        .await?;
        for entry in manifests.iter().flat_map(|manifest| manifest.entries()) {
            let delete_file_ctx = DeleteFileContext {
                manifest_entry: entry.clone(),
                partition_spec_id: entry.data_file().partition_spec_id,
            };
            delete_file_tx.send(delete_file_ctx).await.map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to publish delete file context to validator index",
                )
                .with_source(err)
            })?;
        }
        drop(delete_file_tx);

        let starting_sequence_number = match from_snapshot_id {
            Some(from_snapshot_id) => match base.metadata().snapshot_by_id(from_snapshot_id) {
                Some(snapshot) => snapshot.sequence_number(),
                None => INITIAL_SEQUENCE_NUMBER,
            },
            None => INITIAL_SEQUENCE_NUMBER,
        };

        for data_file in data_files {
            let delete_files = delete_file_index
                .get_deletes_for_data_file(data_file, Some(starting_sequence_number))
                .await;

            if ignore_equality_deletes {
                if delete_files
                    .iter()
                    .any(|delete_file| delete_file.file_type == DataContentType::PositionDeletes)
                {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Cannot commit, found new positional delete for data file: {}",
                            data_file.file_path
                        ),
                    ));
                }
            } else if !delete_files.is_empty() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot commit, found new delete for data file: {}",
                        data_file.file_path
                    ),
                ));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::scan::tests::TableTestFixture;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, Operation, Struct,
    };
    use crate::transaction::tests::{make_v1_table, make_v2_table};

    /// Bare validator used to exercise the trait's default impls.
    struct NoopValidator;
    impl SnapshotValidator for NoopValidator {}

    fn unpartitioned_data_file(path: &str) -> DataFile {
        DataFileBuilder::default()
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::Data)
            .record_count(1)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .file_size_in_bytes(1)
            .build()
            .unwrap()
    }

    fn partitioned_data_file(partition: Struct, spec_id: i32, path: &str) -> DataFile {
        DataFileBuilder::default()
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::Data)
            .record_count(1)
            .partition(partition)
            .partition_spec_id(spec_id)
            .file_size_in_bytes(1)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_default_validate_returns_ok() {
        let table = make_v2_table();
        NoopValidator.validate(&table, None).await.unwrap();
        NoopValidator.validate(&table, Some(1)).await.unwrap();
    }

    #[tokio::test]
    async fn test_validate_no_new_deletes_short_circuits_when_no_current_snapshot() {
        // `to_snapshot_id == None` means the table has no current state.
        let table = make_v2_table();
        let files = vec![unpartitioned_data_file("/path/a.parquet")];

        NoopValidator
            .validate_no_new_deletes_for_data_files(&table, None, None, &files, false)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_validate_no_new_deletes_short_circuits_for_v1_tables() {
        // V1 tables cannot have row-level deletes, so the validator must
        // return Ok before attempting any I/O on manifest list files
        // (which is important because `make_v1_table` does not write any
        // manifest_list files to disk).
        let table = make_v1_table();
        let files = vec![unpartitioned_data_file("/path/a.parquet")];

        NoopValidator
            .validate_no_new_deletes_for_data_files(&table, None, Some(1), &files, false)
            .await
            .unwrap();

        NoopValidator
            .validate_no_new_deletes_for_data_files(&table, Some(0), Some(1), &files, true)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_validation_history_walks_full_chain_when_no_operations_match() {
        // `new_with_deep_history` builds 5 chained snapshots in-memory.
        // None of their `Operation`s are `Replace`, so passing
        // `{Replace}` skips every `load_manifest_list` call, exercising
        // pure history traversal without touching the file system.
        let fixture = TableTestFixture::new_with_deep_history();
        let metadata = fixture.table.metadata();
        let to_snapshot_id = metadata.current_snapshot().unwrap().snapshot_id();
        let matching_operations: HashSet<Operation> = HashSet::from([Operation::Replace]);

        let (manifests, snapshot_ids) = NoopValidator
            .validation_history(
                &fixture.table,
                None,
                to_snapshot_id,
                &matching_operations,
                ManifestContentType::Deletes,
            )
            .await
            .unwrap();

        assert!(manifests.is_empty(), "no operation should match the filter");
        assert!(snapshot_ids.is_empty());
    }

    #[tokio::test]
    async fn test_validation_history_excludes_oldest_snapshot() {
        // (S1, S5] should visit S5..S2 (4 snapshots, none matching), so
        // the parent of the last visited snapshot (S2) should equal S1
        // and the function must succeed.
        let fixture = TableTestFixture::new_with_deep_history();
        let metadata = fixture.table.metadata();
        let mut snapshots: Vec<_> = metadata.snapshots().collect();
        snapshots.sort_by_key(|s| s.sequence_number());
        let s1_id = snapshots.first().unwrap().snapshot_id();
        let s5_id = snapshots.last().unwrap().snapshot_id();

        let (manifests, _) = NoopValidator
            .validation_history(
                &fixture.table,
                Some(s1_id),
                s5_id,
                &HashSet::from([Operation::Replace]),
                ManifestContentType::Deletes,
            )
            .await
            .unwrap();
        assert!(manifests.is_empty());
    }

    #[tokio::test]
    async fn test_validation_history_errors_when_history_broken() {
        // Asking for `from_snapshot_id` that is not an ancestor of
        // `to_snapshot_id` exhausts the chain without finding it; the
        // last visited snapshot is the root (parent_id = None) which
        // does not equal `Some(999)`, so the helper must error out.
        let fixture = TableTestFixture::new_with_deep_history();
        let to_snapshot_id = fixture
            .table
            .metadata()
            .current_snapshot()
            .unwrap()
            .snapshot_id();

        let result = NoopValidator
            .validation_history(
                &fixture.table,
                Some(999),
                to_snapshot_id,
                &HashSet::from([Operation::Replace]),
                ManifestContentType::Deletes,
            )
            .await;

        let err = result.expect_err("history must be reported as broken");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains("Cannot determine history"),
            "unexpected message: {}",
            err.message()
        );
    }

    #[tokio::test]
    async fn test_partitioned_data_file_helper_compiles() {
        // Sanity-only: keep `partitioned_data_file` referenced so future
        // V2 happy-path tests (which need a full manifest fixture) can
        // re-use it without the helper being deleted as dead code.
        let _file =
            partitioned_data_file(Struct::from_iter([Some(Literal::long(0))]), 0, "/p.parquet");
    }
}
