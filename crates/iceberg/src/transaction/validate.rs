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

use futures::future::try_join_all;
use futures::{Sink, SinkExt};
use once_cell::sync::Lazy;

use crate::delete_file_index::DeleteFileIndex;
use crate::error::Result;
use crate::scan::DeleteFileContext;
use crate::spec::{DataFile, ManifestContentType, ManifestFile, Operation, SnapshotRef};
use crate::table::Table;
use crate::util::snapshot::ancestors_between;
use crate::{Error, ErrorKind};

static VALIDATE_ADDED_DELETE_FILES_OPERATIONS: Lazy<HashSet<Operation>> =
    Lazy::new(|| HashSet::from([Operation::Overwrite, Operation::Delete]));

pub(crate) trait SnapshotValidator {
    // todo doc
    // table: base table
    // snapshot: parent snapshot
    // usually snapshot is the latest snapshot of base table, unless it's non-main branch
    // but we don't support writing to branches as of now
    fn validate(&self, _table: &Table, _snapshot: Option<&SnapshotRef>) -> Result<()> {
        // todo: add default implementation
        Ok(())
    }

    // todo doc
    async fn validation_history(
        &self,
        base: &Table,
        to_snapshot: SnapshotRef, // todo maybe the naming/variable order can be better, or just snapshot id is better? this is parent
        from_snapshot_id: Option<i64>,
        matching_operations: &HashSet<Operation>,
        manifest_content_type: ManifestContentType,
    ) -> Result<(Vec<ManifestFile>, HashSet<i64>)> {
        let mut manifests: Vec<ManifestFile> = vec![];
        let mut new_snapshots = HashSet::new();
        let mut last_snapshot: Option<SnapshotRef> = None;

        let snapshots = ancestors_between(
            &Arc::new(base.metadata().clone()),
            to_snapshot.snapshot_id(),
            from_snapshot_id.clone(),
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

    #[allow(dead_code)]
    async fn validate_no_new_delete_files_for_data_files(
        &self,
        base: &Table,
        from_snapshot_id: Option<i64>,
        _data_files: &[DataFile],
        to_snapshot: SnapshotRef,
    ) -> Result<()> {
        // Get matching delete files have been added since the from_snapshot_id
        let (delete_manifests, snapshot_ids) = self
            .validation_history(
                base,
                to_snapshot,
                from_snapshot_id,
                &VALIDATE_ADDED_DELETE_FILES_OPERATIONS,
                ManifestContentType::Deletes,
            )
            .await?;

        // Building delete file index
        let (_delete_file_index, mut delete_file_tx) = DeleteFileIndex::new();
        let manifests = try_join_all(
            delete_manifests
                .iter()
                .map(|f| f.load_manifest(base.file_io()))
                .collect::<Vec<_>>(),
        )
        .await?;

        let delete_files_ctx = manifests
            .iter()
            .flat_map(|manifest| manifest.entries())
            .map(|entry| DeleteFileContext {
                manifest_entry: entry.clone(),
                partition_spec_id: entry.data_file().partition_spec_id,
            })
            .collect::<Vec<_>>();

        for ctx in delete_files_ctx {
            delete_file_tx.send(ctx).await?
        }

        // todo validate if there are deletes

        Ok(())
    }
}
