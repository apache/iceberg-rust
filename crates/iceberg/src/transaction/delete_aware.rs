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

use std::collections::HashSet;
use std::future::Future;

use futures::SinkExt;
use futures::future::try_join_all;
use once_cell::sync::Lazy;

use crate::delete_file_index::DeleteFileIndex;
use crate::error::Result;
use crate::scan::DeleteFileContext;
use crate::spec::{
    DataContentType, DataFile, FormatVersion, INITIAL_SEQUENCE_NUMBER, ManifestContentType,
    ManifestFile, Operation,
};
use crate::table::Table;
use crate::transaction::manifest_filter::ManifestFilterManager;
use crate::transaction::snapshot::SnapshotProduceOperation;
use crate::util::snapshot::ancestors_between;
use crate::{Error, ErrorKind};

/// Operations whose snapshots may add delete files.
static VALIDATE_ADDED_DELETE_FILES_OPERATIONS: Lazy<HashSet<Operation>> =
    Lazy::new(|| HashSet::from([Operation::Overwrite, Operation::Delete]));

/// An additive sub-trait of [`SnapshotProduceOperation`] implemented only by delete-class
/// operations (delete, overwrite, rewrite).
///
/// Append-only operations implement only the base [`SnapshotProduceOperation`] and carry
/// none of the validation/filtering surface defined here. A `DeleteAwareOperation` adds two
/// capabilities on top of the base trait:
///
/// - **Write-time conflict validation** via [`validate`](DeleteAwareOperation::validate),
///   invoked by the action against the refreshed base table before any snapshot is written.
/// - **Manifest filtering** via the owned [`ManifestFilterManager`]s reached through
///   [`data_filter`](DeleteAwareOperation::data_filter) and
///   [`delete_filter`](DeleteAwareOperation::delete_filter), which rewrite carried-forward
///   manifests to drop the files the operation removes.
///
/// The filter managers are owned fields on the operation, populated incrementally as the
/// action builds the operation. Because the operation is rebuilt on each commit attempt, the
/// managers are reconstructed deterministically per attempt from the action's stored inputs.
#[allow(unused)]
pub(crate) trait DeleteAwareOperation: SnapshotProduceOperation {
    /// Per-operation conflict check against the refreshed base table, run before any write.
    ///
    /// Implemented per operation; each operation composes the reusable validation helpers to
    /// detect concurrent changes that would make the pending operation incorrect.
    fn validate(
        &self,
        base: &Table,
        parent_snapshot_id: Option<i64>,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Accessor to the operation's owned data-manifest filter manager.
    ///
    /// Built up as the operation is constructed and mutated during manifest production.
    fn data_filter(&mut self) -> &mut ManifestFilterManager;

    /// Accessor to the operation's owned delete-manifest filter manager.
    ///
    /// Built up as the operation is constructed and mutated during manifest production.
    fn delete_filter(&mut self) -> &mut ManifestFilterManager;

    /// Retrieves the history of snapshots between two points with matching operations and content
    /// type.
    ///
    /// # Arguments
    ///
    /// * `base` - The base table to retrieve history from.
    /// * `from_snapshot_id` - The starting snapshot ID (exclusive), or None to start from the
    ///   beginning.
    /// * `to_snapshot_id` - The ending snapshot ID (inclusive).
    /// * `matching_operations` - Set of operations to match when collecting snapshots.
    /// * `manifest_content_type` - The content type of manifests to collect.
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// * A vector of manifest files matching the criteria.
    /// * A set of snapshot IDs that were collected.
    ///
    /// # Errors
    ///
    /// Returns an error if the history between the snapshots cannot be determined.
    fn validation_history<'a>(
        &'a self,
        base: &'a Table,
        from_snapshot_id: Option<i64>,
        to_snapshot_id: i64,
        matching_operations: &'a HashSet<Operation>,
        manifest_content_type: ManifestContentType,
    ) -> impl Future<Output = Result<(Vec<ManifestFile>, HashSet<i64>)>> + Send + 'a {
        async move {
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
                        if manifest.content == manifest_content_type
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
    }

    /// Validates that there are no new delete files for the given data files.
    ///
    /// # Arguments
    ///
    /// * `base` - The base table to validate against.
    /// * `from_snapshot_id` - The starting snapshot ID (exclusive), or None to start from the
    ///   beginning.
    /// * `to_snapshot_id` - The ending snapshot ID (inclusive), or None if there is no current
    ///   table state.
    /// * `data_files` - The data files to check for conflicting delete files.
    /// * `ignore_equality_deletes` - Whether to ignore equality deletes and only check for
    ///   positional deletes.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or an error if validation fails.
    ///
    /// # Errors
    ///
    /// Returns an error if new delete files are found for any of the data files.
    fn validate_no_new_deletes_for_data_files<'a>(
        &'a self,
        base: &'a Table,
        from_snapshot_id: Option<i64>,
        to_snapshot_id: Option<i64>,
        data_files: &'a [DataFile],
        ignore_equality_deletes: bool,
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        async move {
            // Delete files only exist in format version 2 and above. If there is no current table
            // state, or the table is V1, there cannot be any new delete files to conflict with.
            let Some(to_snapshot_id) = to_snapshot_id else {
                return Ok(());
            };
            if base.metadata().format_version() == FormatVersion::V1 {
                return Ok(());
            }

            // Get matching delete files that have been added since the from_snapshot_id
            let (delete_manifests, _) = self
                .validation_history(
                    base,
                    from_snapshot_id,
                    to_snapshot_id,
                    &VALIDATE_ADDED_DELETE_FILES_OPERATIONS,
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
            // populating before we query it below. Holding the sender past this point
            // would deadlock.
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
                    if delete_files.iter().any(|delete_file| {
                        delete_file.file_type == DataContentType::PositionDeletes
                    }) {
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
    }
}
