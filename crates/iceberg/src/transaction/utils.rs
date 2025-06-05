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

use futures::stream::{self, StreamExt};
use futures::TryStreamExt;
use itertools::Itertools;

use crate::error::Result;
use crate::io::FileIO;
use crate::spec::{ManifestFile, Snapshot, TableMetadataRef};

const DEFAULT_DELETE_CONCURRENCY_LIMIT: usize = 10;

pub struct ReachableFileCleanupStrategy {
    file_io: FileIO,
}

impl ReachableFileCleanupStrategy {
    pub fn new(file_io: FileIO) -> Self {
        Self { file_io }
    }
}

impl ReachableFileCleanupStrategy {
    pub async fn clean_files(
        &self,
        before_expiration: &TableMetadataRef,
        after_expiration: &TableMetadataRef,
    ) -> Result<()> {
        let mut manifest_lists_to_delete: HashSet<&str> = HashSet::default();
        let mut expired_snapshots = Vec::default();
        for snapshot in before_expiration.snapshots() {
            if after_expiration
                .snapshot_by_id(snapshot.snapshot_id())
                .is_none()
            {
                expired_snapshots.push(snapshot);
                manifest_lists_to_delete.insert(snapshot.manifest_list());
            }
        }

        let deletion_candidates = {
            let mut deletion_candidates = HashSet::default();
            // This part can also be parallelized if `load_manifest_list` is a bottleneck
            // and if the underlying FileIO supports concurrent reads efficiently.
            for snapshot in expired_snapshots {
                let manifest_list = snapshot
                    .load_manifest_list(&self.file_io, before_expiration)
                    .await?;

                for manifest_file in manifest_list.entries() {
                    deletion_candidates.insert(manifest_file.clone());
                }
            }
            deletion_candidates
        };

        if !deletion_candidates.is_empty() {
            let (manifests_to_delete, referenced_manifests) = self
                .prune_referenced_manifests(
                    after_expiration.snapshots(),
                    after_expiration,
                    deletion_candidates,
                )
                .await?;

            if !manifests_to_delete.is_empty() {
                let files_to_delete = self
                    .find_files_to_delete(&manifests_to_delete, &referenced_manifests)
                    .await?;

                stream::iter(files_to_delete)
                    .map(|file_path| self.file_io.delete(file_path))
                    .buffer_unordered(DEFAULT_DELETE_CONCURRENCY_LIMIT)
                    .try_collect::<Vec<_>>()
                    .await?;

                stream::iter(manifests_to_delete)
                    .map(|manifest_file| self.file_io.delete(manifest_file.manifest_path))
                    .buffer_unordered(DEFAULT_DELETE_CONCURRENCY_LIMIT)
                    .try_collect::<Vec<_>>()
                    .await?;
            }
        }

        let manifest_lists_to_delete = manifest_lists_to_delete
            .iter()
            .map(|path| self.file_io.delete(path))
            .collect_vec();

        stream::iter(manifest_lists_to_delete)
            .buffer_unordered(DEFAULT_DELETE_CONCURRENCY_LIMIT)
            .try_collect::<Vec<_>>()
            .await?;

        Ok(())
    }

    async fn prune_referenced_manifests(
        &self,
        snapshots: impl Iterator<Item = &Arc<Snapshot>>,
        table_meta_data_ref: &TableMetadataRef,
        mut deletion_candidates: HashSet<ManifestFile>,
    ) -> Result<(HashSet<ManifestFile>, HashSet<ManifestFile>)> {
        let mut referenced_manifests = HashSet::default();
        for snapshot in snapshots {
            let manifest_list = snapshot
                .load_manifest_list(&self.file_io, table_meta_data_ref)
                .await?;

            for manifest_file in manifest_list.entries() {
                deletion_candidates.remove(manifest_file);
                referenced_manifests.insert(manifest_file.clone());

                if deletion_candidates.is_empty() {
                    break;
                }
            }
        }

        Ok((deletion_candidates, referenced_manifests))
    }

    async fn find_files_to_delete(
        &self,
        manifest_files: &HashSet<ManifestFile>,
        referenced_manifests: &HashSet<ManifestFile>,
    ) -> Result<HashSet<String>> {
        let mut files_to_delete = HashSet::default();
        for manifest_file in manifest_files {
            let m = manifest_file.load_manifest(&self.file_io).await.unwrap();
            for entry in m.entries() {
                files_to_delete.insert(entry.data_file().file_path().to_owned());
            }
        }

        if files_to_delete.is_empty() {
            return Ok(files_to_delete);
        }

        for manifest_file in referenced_manifests {
            let m = manifest_file.load_manifest(&self.file_io).await.unwrap();
            for entry in m.entries() {
                files_to_delete.remove(entry.data_file().file_path());
            }
        }

        Ok(files_to_delete)
    }
}
