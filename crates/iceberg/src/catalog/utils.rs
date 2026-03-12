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

use crate::io::FileIO;
use crate::spec::TableMetadata;
use crate::Result;

/// Property key for enabling garbage collection on drop.
/// When set to `false`, data files will not be deleted when a table is dropped.
/// Defaults to `true`.
pub const GC_ENABLED: &str = "gc.enabled";
const GC_ENABLED_DEFAULT: bool = true;

/// Deletes all data and metadata files referenced by the given table metadata.
///
/// This mirrors the Java implementation's `CatalogUtil.dropTableData`.
/// It collects all manifest files, manifest lists, previous metadata files,
/// statistics files, and partition statistics files, then deletes them.
///
/// Data files within manifests are only deleted if the `gc.enabled` table
/// property is `true` (the default), to avoid corrupting other tables that
/// may share the same data files.
///
/// Individual file deletion failures are suppressed to complete as much
/// cleanup as possible, matching the Java behavior.
pub async fn drop_table_data(
    io: &FileIO,
    metadata: &TableMetadata,
    metadata_location: Option<&str>,
) -> Result<()> {
    let mut manifest_lists_to_delete: HashSet<String> = HashSet::new();
    let mut manifests_to_delete: HashSet<String> = HashSet::new();

    for snapshot in metadata.snapshots() {
        // Collect the manifest list location
        let manifest_list_location = snapshot.manifest_list();
        if !manifest_list_location.is_empty() {
            manifest_lists_to_delete.insert(manifest_list_location.to_string());
        }

        // Load all manifests from this snapshot
        match snapshot.load_manifest_list(io, metadata).await {
            Ok(manifest_list) => {
                for manifest_file in manifest_list.entries() {
                    manifests_to_delete.insert(manifest_file.manifest_path.clone());
                }
            }
            Err(_) => {
                // Suppress failure to continue cleanup
            }
        }
    }

    let gc_enabled = metadata
        .properties()
        .get(GC_ENABLED)
        .and_then(|v| v.parse::<bool>().ok())
        .unwrap_or(GC_ENABLED_DEFAULT);

    // Delete data files only if gc.enabled is true, to avoid corrupting shared tables
    if gc_enabled {
        delete_data_files(io, &manifests_to_delete).await;
    }

    // Delete manifest files
    delete_files(io, manifests_to_delete.iter().map(String::as_str)).await;

    // Delete manifest lists
    delete_files(io, manifest_lists_to_delete.iter().map(String::as_str)).await;

    // Delete previous metadata files
    delete_files(
        io,
        metadata.metadata_log().iter().map(|m| m.metadata_file.as_str()),
    )
    .await;

    // Delete statistics files
    delete_files(
        io,
        metadata
            .statistics_iter()
            .map(|s| s.statistics_path.as_str()),
    )
    .await;

    // Delete partition statistics files
    delete_files(
        io,
        metadata
            .partition_statistics_iter()
            .map(|s| s.statistics_path.as_str()),
    )
    .await;

    // Delete the current metadata file
    if let Some(location) = metadata_location {
        let _ = io.delete(location).await;
    }

    Ok(())
}

/// Reads each manifest and deletes the data files referenced within.
async fn delete_data_files(io: &FileIO, manifest_paths: &HashSet<String>) {
    for manifest_path in manifest_paths {
        let input = match io.new_input(manifest_path) {
            Ok(input) => input,
            Err(_) => continue,
        };

        let manifest_content = match input.read().await {
            Ok(content) => content,
            Err(_) => continue,
        };

        let manifest = match crate::spec::Manifest::parse_avro(&manifest_content) {
            Ok(manifest) => manifest,
            Err(_) => continue,
        };

        for entry in manifest.entries() {
            let _ = io.delete(entry.data_file.file_path()).await;
        }
    }
}

/// Deletes a collection of files, suppressing individual failures.
async fn delete_files<'a>(io: &FileIO, paths: impl Iterator<Item = &'a str>) {
    for path in paths {
        let _ = io.delete(path).await;
    }
}
