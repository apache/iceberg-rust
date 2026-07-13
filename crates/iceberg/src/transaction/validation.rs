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

//! Stateless write-time conflict validation helpers.
//!
//! These are free functions (no `self`, no trait): they take `&Table`, snapshot
//! ids, and data files, and are pure with respect to the caller. Keeping them as
//! free functions keeps the operation trait lean — delete-class operations compose
//! these helpers from their `validate` override, and the results are recomputed
//! against the freshly refreshed base on every commit attempt (never cached).
//!
//! Bodies are ported from iceberg-rust PR #2590.

use std::collections::HashSet;

use futures::SinkExt;
use futures::future::try_join_all;

use crate::delete_file_index::DeleteFileIndex;
use crate::error::Result;
use crate::scan::DeleteFileContext;
use crate::spec::{
    DataContentType, DataFile, FormatVersion, INITIAL_SEQUENCE_NUMBER, ManifestContentType,
    ManifestFile, Operation,
};
use crate::table::Table;
use crate::util::snapshot::ancestors_between;
use crate::{Error, ErrorKind};

/// Predicate replacing the old `VALIDATE_ADDED_DELETE_FILES_OPERATIONS` static set.
///
/// Returns true for operations whose snapshots may introduce delete files that
/// could conflict with a concurrent rewrite: `Overwrite` / `Delete` => `true`;
/// `Append` / `Replace` => `false`.
// Consumed by delete-class operations (e.g. `RewriteFilesOperation`) wired up in a
// later task; kept `pub(crate)` for that consumer.
#[allow(dead_code)]
pub(crate) fn may_add_conflicting_deletes(operation: Operation) -> bool {
    matches!(operation, Operation::Overwrite | Operation::Delete)
}

/// Collect the manifests and snapshot ids added between two points in history,
/// filtered by operation set and manifest content type.
///
/// Ported from #2590: walks [`ancestors_between`]`(to, from)`, asserts last-ancestor
/// consistency (the oldest walked snapshot's parent must equal `from_snapshot_id`),
/// and returns the matching manifests + the set of snapshot ids.
///
/// # Arguments
///
/// * `base` - The base table to retrieve history from.
/// * `from_snapshot_id` - The starting snapshot ID (exclusive), or `None` to start
///   from the beginning of history.
/// * `to_snapshot_id` - The ending snapshot ID (inclusive).
/// * `matching_operations` - Set of operations to match when collecting snapshots.
/// * `content_type` - The content type of manifests to collect.
///
/// # Errors
///
/// Returns an error if the history between the snapshots cannot be determined
/// (broken ancestor chain).
pub(crate) async fn validation_history(
    base: &Table,
    from_snapshot_id: Option<i64>,
    to_snapshot_id: i64,
    matching_operations: &HashSet<Operation>,
    content_type: ManifestContentType,
) -> Result<(Vec<ManifestFile>, HashSet<i64>)> {
    let mut manifests: Vec<ManifestFile> = vec![];
    let mut new_snapshots = HashSet::new();
    let mut last_snapshot = None;

    let metadata = base.metadata_ref();
    let snapshots = ancestors_between(&metadata, to_snapshot_id, from_snapshot_id);

    for current_snapshot in snapshots {
        last_snapshot = Some(current_snapshot.clone());

        // Find all snapshots with the matching operations
        // and their manifest files with the matching content type
        if matching_operations.contains(&current_snapshot.summary().operation) {
            new_snapshots.insert(current_snapshot.snapshot_id());

            let manifest_list = base.manifest_list_reader(&current_snapshot).load().await?;

            for manifest in manifest_list.entries() {
                if manifest.content == content_type
                    && manifest.added_snapshot_id == current_snapshot.snapshot_id()
                {
                    manifests.push(manifest.clone());
                }
            }
        }
    }

    if let Some(last_snapshot) = last_snapshot
        && last_snapshot.parent_snapshot_id() != from_snapshot_id
    {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Cannot determine history between starting snapshot {} and the last known ancestor {}",
                from_snapshot_id.unwrap_or(-1),
                last_snapshot.snapshot_id()
            ),
        ));
    }

    Ok((manifests, new_snapshots))
}

/// Fail if any delete file added since `from_snapshot_id` targets any of the given
/// data files.
///
/// Ported from #2590: gathers the delete manifests via [`validation_history`] over
/// the operations that [`may_add_conflicting_deletes`], builds a
/// [`DeleteFileIndex`] (its sender is scoped inside a block so the channel closes
/// and we do not deadlock while populating), scopes the query by
/// `starting_sequence_number`, and queries per data file. When
/// `ignore_equality_deletes` is true, only positional deletes are treated as
/// conflicts.
///
/// # Arguments
///
/// * `base` - The base table to validate against.
/// * `from_snapshot_id` - The starting snapshot ID (exclusive), or `None` to start
///   from the beginning of history.
/// * `to_snapshot_id` - The ending snapshot ID (inclusive), or `None` if there is
///   no current table state.
/// * `data_files` - The data files to check for conflicting delete files.
/// * `ignore_equality_deletes` - Whether to ignore equality deletes and only check
///   for positional deletes.
///
/// # Errors
///
/// Returns [`ErrorKind::DataInvalid`] if new conflicting delete files are found for
/// any of the data files.
// Composed by delete-class operations' `validate` override (e.g.
// `RewriteFilesOperation`).
pub(crate) async fn validate_no_new_deletes_for_data_files(
    base: &Table,
    from_snapshot_id: Option<i64>,
    to_snapshot_id: Option<i64>,
    data_files: &[DataFile],
    ignore_equality_deletes: bool,
) -> Result<()> {
    // Delete files only exist in format version 2 and above. If there is no current
    // table state, or the table is V1, there cannot be any new delete files to
    // conflict with.
    let Some(to_snapshot_id) = to_snapshot_id else {
        return Ok(());
    };
    if base.metadata().format_version() == FormatVersion::V1 {
        return Ok(());
    }

    // Operations whose snapshots may add delete files. These are exactly the
    // operations for which `may_add_conflicting_deletes` returns true; this set is
    // the replacement for the old `VALIDATE_ADDED_DELETE_FILES_OPERATIONS` static.
    let matching_operations: HashSet<Operation> =
        HashSet::from([Operation::Overwrite, Operation::Delete]);

    // Get matching delete files that have been added since the from_snapshot_id
    let (delete_manifests, _) = validation_history(
        base,
        from_snapshot_id,
        to_snapshot_id,
        &matching_operations,
        ManifestContentType::Deletes,
    )
    .await?;

    // Build the delete file index from the matching delete manifests.
    //
    // `DeleteFileIndex::new` spawns a background task that populates the index by
    // collecting from the channel until *all* senders are dropped; queries on the
    // returned index (`get_deletes_for_data_file`) block until that population
    // completes. We therefore scope the sender to this block so it is dropped as
    // soon as every delete file has been sent, allowing the index to finish
    // populating before we query it below. Holding the sender past this point would
    // deadlock.
    let delete_file_index = {
        let (delete_file_index, mut delete_file_tx) =
            DeleteFileIndex::new(base.runtime().clone());
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
            delete_file_tx.send(delete_file_ctx).await?;
        }
        // `delete_file_tx` is dropped here as the block ends, closing the channel.
        delete_file_index
    };

    // Get starting seq num from starting snapshot if available
    let starting_sequence_number = if let Some(from_snapshot_id) = from_snapshot_id {
        match base.metadata().snapshot_by_id(from_snapshot_id) {
            Some(snapshot) => snapshot.sequence_number(),
            None => INITIAL_SEQUENCE_NUMBER,
        }
    } else {
        INITIAL_SEQUENCE_NUMBER
    };

    // Validate if there are deletes using delete file index
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
                        "Cannot commit, found new positional delete for added data file: {}",
                        data_file.file_path
                    ),
                ));
            }
        } else if !delete_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot commit, found new delete for added data file: {}",
                    data_file.file_path
                ),
            ));
        }
    }

    Ok(())
}
