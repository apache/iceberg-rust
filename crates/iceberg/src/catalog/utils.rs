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

//! Utility functions for catalog operations.

use std::collections::HashSet;

use futures::{TryStreamExt, stream};

use crate::Result;
use crate::io::FileIO;
use crate::spec::TableMetadata;

const DELETE_CONCURRENCY: usize = 10;

/// Deletes all data and metadata files referenced by the given table metadata.
///
/// This mirrors the Java implementation's `CatalogUtil.dropTableData`.
/// It collects all manifest files, manifest lists, previous metadata files,
/// statistics files, and partition statistics files, then deletes them.
///
/// Data files within manifests are only deleted if the `gc.enabled` table
/// property is `true` (the default), to avoid corrupting other tables that
/// may share the same data files.
pub async fn drop_table_data(
    io: &FileIO,
    metadata: &TableMetadata,
    metadata_location: Option<&str>,
) -> Result<()> {
    let mut manifest_lists_to_delete: HashSet<String> = HashSet::new();
    let mut manifests_to_delete: HashSet<String> = HashSet::new();

    // Load all manifest lists concurrently
    let results: Vec<_> =
        futures::future::try_join_all(metadata.snapshots().map(|snapshot| async {
            let manifest_list = snapshot.load_manifest_list(io, metadata).await?;
            Ok::<_, crate::Error>((snapshot.manifest_list().to_string(), manifest_list))
        }))
        .await?;

    for (manifest_list_location, manifest_list) in results {
        if !manifest_list_location.is_empty() {
            manifest_lists_to_delete.insert(manifest_list_location);
        }
        for manifest_file in manifest_list.entries() {
            manifests_to_delete.insert(manifest_file.manifest_path.clone());
        }
    }

    // Delete data files only if gc.enabled is true, to avoid corrupting shared tables
    if metadata.table_properties()?.gc_enabled {
        delete_data_files(io, &manifests_to_delete).await?;
    }

    // Delete manifest files
    io.delete_stream(stream::iter(manifests_to_delete)).await?;

    // Delete manifest lists
    io.delete_stream(stream::iter(manifest_lists_to_delete))
        .await?;

    // Delete previous metadata files
    let prev_metadata_paths: Vec<String> = metadata
        .metadata_log()
        .iter()
        .map(|m| m.metadata_file.clone())
        .collect();
    io.delete_stream(stream::iter(prev_metadata_paths)).await?;

    // Delete statistics files
    let stats_paths: Vec<String> = metadata
        .statistics_iter()
        .map(|s| s.statistics_path.clone())
        .collect();
    io.delete_stream(stream::iter(stats_paths)).await?;

    // Delete partition statistics files
    let partition_stats_paths: Vec<String> = metadata
        .partition_statistics_iter()
        .map(|s| s.statistics_path.clone())
        .collect();
    io.delete_stream(stream::iter(partition_stats_paths))
        .await?;

    // Delete the current metadata file
    if let Some(location) = metadata_location {
        io.delete(location).await?;
    }

    Ok(())
}

/// Reads manifests concurrently and deletes the data files referenced within.
async fn delete_data_files(io: &FileIO, manifest_paths: &HashSet<String>) -> Result<()> {
    stream::iter(manifest_paths.iter().map(Ok))
        .try_for_each_concurrent(DELETE_CONCURRENCY, |manifest_path| async move {
            let input = io.new_input(manifest_path)?;
            let manifest_content = input.read().await?;
            let manifest = crate::spec::Manifest::parse_avro(&manifest_content)?;

            let data_file_paths = manifest
                .entries()
                .iter()
                .map(|entry| entry.data_file.file_path().to_string())
                .collect::<Vec<_>>();

            io.delete_stream(stream::iter(data_file_paths)).await
        })
        .await
}
