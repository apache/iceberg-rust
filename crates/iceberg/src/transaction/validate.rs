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
use std::sync::Arc;

use futures::SinkExt;
use futures::future::try_join_all;
use once_cell::sync::Lazy;

use crate::delete_file_index::DeleteFileIndex;
use crate::error::Result;
use crate::scan::DeleteFileContext;
use crate::spec::{
    DataContentType, DataFile, FormatVersion, INITIAL_SEQUENCE_NUMBER, ManifestContentType,
    ManifestFile, Operation, SnapshotRef,
};
use crate::table::Table;
use crate::util::snapshot::ancestors_between;
use crate::{Error, ErrorKind};

static VALIDATE_ADDED_DELETE_FILES_OPERATIONS: Lazy<HashSet<Operation>> =
    Lazy::new(|| HashSet::from([Operation::Overwrite, Operation::Delete]));

/// A trait for validating snapshots in an Iceberg table.
///
/// This trait provides methods to validate snapshots and their history,
/// ensuring data integrity and consistency across table operations.
pub(crate) trait SnapshotValidator {
    /// Validates a snapshot against a table.
    ///
    /// # Arguments
    ///
    /// * `base` - The base table to validate against
    /// * `parent_snapshot_id` - The ID of the parent snapshot, if any. This is usually
    ///   the latest snapshot of the base table, unless it's a non-main branch
    ///   (note: writing to branches is not currently supported)
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or an error if validation fails
    async fn validate(&self, _base: &Table, _parent_snapshot_id: Option<i64>) -> Result<()> {
        Ok(())
    }

    /// Retrieves the history of snapshots between two points with matching operations and content type.
    ///
    /// # Arguments
    ///
    /// * `base` - The base table to retrieve history from
    /// * `from_snapshot_id` - The starting snapshot ID (exclusive), or None to start from the beginning
    /// * `to_snapshot_id` - The ending snapshot ID (inclusive)
    /// * `matching_operations` - Set of operations to match when collecting snapshots
    /// * `manifest_content_type` - The content type of manifests to collect
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// * A vector of manifest files matching the criteria
    /// * A set of snapshot IDs that were collected
    ///
    /// # Errors
    ///
    /// Returns an error if the history between the snapshots cannot be determined
    async fn validation_history(
        &self,
        base: &Table,
        from_snapshot_id: Option<i64>,
        to_snapshot_id: i64,
        matching_operations: &HashSet<Operation>,
        manifest_content_type: ManifestContentType,
    ) -> Result<(Vec<ManifestFile>, HashSet<i64>)> {
        let mut manifests: Vec<ManifestFile> = vec![];
        let mut new_snapshots = HashSet::new();
        let mut last_snapshot: Option<SnapshotRef> = None;

        let snapshots = ancestors_between(
            &Arc::new(base.metadata().clone()),
            to_snapshot_id,
            from_snapshot_id,
        );

        for current_snapshot in snapshots {
            last_snapshot = Some(current_snapshot.clone());

            // Find all snapshots with the matching operations
            // and their manifest files with the matching content type
            if matching_operations.contains(&current_snapshot.summary().operation) {
                new_snapshots.insert(current_snapshot.snapshot_id());
                current_snapshot
                    .load_manifest_list(base.file_io(), base.metadata())
                    .await?
                    .entries()
                    .iter()
                    .for_each(|manifest| {
                        if manifest.content == manifest_content_type
                            && manifest.added_snapshot_id == current_snapshot.snapshot_id()
                        {
                            manifests.push(manifest.clone());
                        }
                    });
            }
        }

        if last_snapshot.is_some()
            && last_snapshot.clone().unwrap().parent_snapshot_id() != from_snapshot_id
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot determine history between starting snapshot {} and the last known ancestor {}",
                    from_snapshot_id.unwrap_or(-1),
                    last_snapshot.unwrap().snapshot_id()
                ),
            ));
        }

        Ok((manifests, new_snapshots))
    }

    /// Validates that there are no new delete files for the given data files.
    ///
    /// # Arguments
    ///
    /// * `base` - The base table to validate against
    /// * `from_snapshot_id` - The starting snapshot ID (exclusive), or None to start from the beginning
    /// * `to_snapshot_id` - The ending snapshot ID (inclusive), or None if there is no current table state
    /// * `data_files` - The data files to check for conflicting delete files
    /// * `ignore_equality_deletes` - Whether to ignore equality deletes and only check for positional deletes
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or an error if validation fails
    ///
    /// # Errors
    ///
    /// Returns an error if new delete files are found for any of the data files
    async fn validate_no_new_deletes_for_data_files(
        &self,
        base: &Table,
        from_snapshot_id: Option<i64>,
        to_snapshot_id: Option<i64>,
        data_files: &[DataFile],
        ignore_equality_deletes: bool,
    ) -> Result<()> {
        // If there is no current table state, no files have been added
        if to_snapshot_id.is_none() || base.metadata().format_version() != FormatVersion::V1 {
            return Ok(());
        }
        let to_snapshot_id = to_snapshot_id.unwrap();

        // Get matching delete files have been added since the from_snapshot_id
        let (delete_manifests, _) = self
            .validation_history(
                base,
                from_snapshot_id,
                to_snapshot_id,
                &VALIDATE_ADDED_DELETE_FILES_OPERATIONS,
                ManifestContentType::Deletes,
            )
            .await?;

        // Build delete file index
        let (delete_file_index, mut delete_file_tx) = DeleteFileIndex::new();
        let manifests = try_join_all(
            delete_manifests
                .iter()
                .map(|f| f.load_manifest(base.file_io()))
                .collect::<Vec<_>>(),
        )
        .await?;
        let manifest_entries = manifests.iter().flat_map(|manifest| manifest.entries());
        for entry in manifest_entries {
            let delete_file_ctx = DeleteFileContext {
                manifest_entry: entry.clone(),
                partition_spec_id: entry.data_file().partition_spec_id,
            };
            delete_file_tx.send(delete_file_ctx).await?;
        }

        // Get starting seq num from starting snapshot if available
        let starting_sequence_number = if let Some(from_snapshot_id) = from_snapshot_id {
            match base.metadata().snapshots.get(&from_snapshot_id) {
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
}
