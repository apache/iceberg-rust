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

//! Shared engine for snapshot-producing operations that both add and delete files.
//!
//! [`MergingSnapshotProducer`] is the Rust equivalent of Java's
//! `MergingSnapshotProducer`. It handles manifest filtering, new manifest
//! creation, summary computation, and delegates the final snapshot commit to
//! [`SnapshotProducer`].

use std::collections::{HashMap, HashSet};

use uuid::Uuid;

use crate::error::Result;
use crate::io::OutputFile;
use crate::spec::{
    DataContentType, DataFile, DataFileFormat, FormatVersion, ManifestContentType, ManifestEntry,
    ManifestFile, ManifestStatus, ManifestWriter, ManifestWriterBuilder, Operation, PartitionSpec,
    SchemaRef, SnapshotSummaryCollector, Summary, update_snapshot_summaries,
};
use crate::table::Table;
use crate::transaction::ActionCommit;
use crate::transaction::snapshot::SnapshotProducer;
use crate::{Error, ErrorKind};

/// Create a manifest writer that handles encryption when available.
fn new_manifest_writer(
    table: &Table,
    output_file: OutputFile,
    snapshot_id: Option<i64>,
    schema: SchemaRef,
    partition_spec: PartitionSpec,
) -> Result<ManifestWriter> {
    let builder = if let Some(em) = table.encryption_manager() {
        ManifestWriterBuilder::new_from_encrypted(
            em.encrypt(output_file),
            snapshot_id,
            schema,
            partition_spec,
        )?
    } else {
        ManifestWriterBuilder::new(output_file, snapshot_id, schema, partition_spec)
    };

    match table.metadata().format_version() {
        FormatVersion::V1 => Ok(builder.build_v1()),
        FormatVersion::V2 => Ok(builder.build_v2_data()),
        FormatVersion::V3 => Ok(builder.build_v3_data()),
    }
}

/// Filters existing manifests by removing entries for deleted data files.
///
/// This is equivalent to Java's `ManifestFilterManager`. When a rewrite or
/// overwrite operation deletes files, the filter manager rewrites affected
/// manifests so that deleted entries are dropped and surviving entries are
/// re-emitted with [`ManifestStatus::Existing`].
pub(crate) struct ManifestFilterManager {
    deleted_file_paths: HashSet<String>,
    fail_missing_delete_paths: bool,
}

impl ManifestFilterManager {
    pub(crate) fn new(fail_missing_delete_paths: bool) -> Self {
        Self {
            deleted_file_paths: HashSet::new(),
            fail_missing_delete_paths,
        }
    }

    pub(crate) fn add_delete(&mut self, path: String) {
        self.deleted_file_paths.insert(path);
    }

    /// Filter `manifests` by removing entries whose file path is in the delete
    /// set. Returns the surviving manifests plus a [`SnapshotSummaryCollector`]
    /// that recorded metrics for every removed file.
    pub(crate) async fn filter_manifests(
        &self,
        table: &Table,
        manifests: Vec<ManifestFile>,
        snapshot_id: i64,
    ) -> Result<(Vec<ManifestFile>, SnapshotSummaryCollector)> {
        if self.deleted_file_paths.is_empty() {
            return Ok((manifests, SnapshotSummaryCollector::default()));
        }

        let mut result: Vec<ManifestFile> = Vec::with_capacity(manifests.len());
        let mut removed_collector = SnapshotSummaryCollector::default();
        let mut found_paths: HashSet<String> = HashSet::new();

        let schema = table.metadata().current_schema().clone();
        let partition_spec = table.metadata().default_partition_spec().clone();

        for manifest_file in &manifests {
            // Only filter data manifests; pass delete manifests through unchanged.
            if manifest_file.content != ManifestContentType::Data {
                result.push(manifest_file.clone());
                continue;
            }

            let manifest = manifest_file.load_manifest(table.file_io()).await?;

            // Check whether this manifest contains any files we want to delete.
            let has_deletes = manifest
                .entries()
                .iter()
                .any(|e| e.is_alive() && self.deleted_file_paths.contains(e.file_path()));

            if !has_deletes {
                // Manifest is unaffected — pass through verbatim.
                result.push(manifest_file.clone());
                continue;
            }

            // Manifest requires the current partition spec to match so we can
            // rewrite it correctly. Non-default specs need a writer
            // parameterised by the source spec, which is not yet implemented.
            if manifest_file.partition_spec_id != table.metadata().default_partition_spec_id() {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    format!(
                        "Cannot rewrite manifest with partition spec {} (table default is {}). \
                         Rewriting manifests with non-default partition specs is not yet supported.",
                        manifest_file.partition_spec_id,
                        table.metadata().default_partition_spec_id(),
                    ),
                ));
            }

            // Rewrite: keep surviving entries as EXISTING, drop deleted ones.
            let mut surviving_entries: Vec<ManifestEntry> = Vec::new();
            for entry in manifest.entries() {
                if entry.is_alive() && self.deleted_file_paths.contains(entry.file_path()) {
                    // Record removal metrics.
                    found_paths.insert(entry.file_path().to_string());
                    removed_collector.remove_file(
                        entry.data_file(),
                        schema.clone(),
                        partition_spec.clone(),
                    );
                } else if entry.is_alive() {
                    // Surviving entry — re-emit as EXISTING with original ids preserved.
                    let existing = ManifestEntry::builder()
                        .status(ManifestStatus::Existing)
                        .snapshot_id(
                            entry
                                .snapshot_id()
                                .unwrap_or(manifest_file.added_snapshot_id),
                        )
                        .sequence_number(entry.sequence_number().unwrap_or(0))
                        .file_sequence_number(entry.file_sequence_number.unwrap_or(0))
                        .data_file(entry.data_file().clone())
                        .build();
                    surviving_entries.push(existing);
                }
                // Already-deleted entries (status == Deleted) are dropped.
            }

            if surviving_entries.is_empty() {
                // Manifest is now empty — omit entirely.
                continue;
            }

            // Write the filtered manifest.
            let new_manifest_path = format!(
                "{}/{}-m-filter-{}.{}",
                table.metadata().metadata_location()?,
                Uuid::now_v7(),
                manifest_file.partition_spec_id,
                DataFileFormat::Avro,
            );
            let output_file = table.file_io().new_output(new_manifest_path)?;
            // Use the current snapshot_id so that the manifest list writer
            // can assign sequence numbers to this rewritten manifest.
            let mut writer = new_manifest_writer(
                table,
                output_file,
                Some(snapshot_id),
                schema.clone(),
                partition_spec.as_ref().clone(),
            )?;
            for entry in surviving_entries {
                writer.add_entry(entry)?;
            }
            let new_manifest = writer.write_manifest_file().await?;
            result.push(new_manifest);
        }

        // Validate that every delete target was found.
        if self.fail_missing_delete_paths {
            let missing: Vec<&str> = self
                .deleted_file_paths
                .iter()
                .filter(|p| !found_paths.contains(p.as_str()))
                .map(String::as_str)
                .collect();
            if !missing.is_empty() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Failed to find the following files to delete in the current snapshot: {}",
                        missing.join(", "),
                    ),
                ));
            }
        }

        Ok((result, removed_collector))
    }
}

/// Shared engine for operations that both add and remove data files.
///
/// This struct is the Rust equivalent of Java's `MergingSnapshotProducer`.
/// It manages:
/// - Tracking files to add and delete
/// - Filtering existing manifests to remove deleted files
/// - Writing new manifests for added files
/// - Computing snapshot summaries that account for both additions and removals
/// - Delegating final snapshot creation to [`SnapshotProducer`]
///
/// Concrete transaction actions like [`RewriteFilesAction`] own an instance
/// of this struct and configure it with the appropriate [`Operation`] type
/// and validation rules.
pub(crate) struct MergingSnapshotProducer {
    operation: Operation,
    pub(crate) added_data_files: Vec<DataFile>,
    pub(crate) deleted_data_files: Vec<DataFile>,
    filter_manager: ManifestFilterManager,
    snapshot_properties: HashMap<String, String>,
    commit_uuid: Uuid,
}

impl MergingSnapshotProducer {
    pub(crate) fn new(operation: Operation) -> Self {
        Self {
            operation,
            added_data_files: Vec::new(),
            deleted_data_files: Vec::new(),
            filter_manager: ManifestFilterManager::new(true),
            snapshot_properties: HashMap::new(),
            commit_uuid: Uuid::now_v7(),
        }
    }

    pub(crate) fn add_data_file(&mut self, file: DataFile) {
        self.added_data_files.push(file);
    }

    pub(crate) fn delete_data_file(&mut self, file: DataFile) {
        self.filter_manager.add_delete(file.file_path.clone());
        self.deleted_data_files.push(file);
    }

    /// Produce manifests, compute summary, and commit a new snapshot.
    pub(crate) async fn commit_snapshot(&self, table: &Table) -> Result<ActionCommit> {
        // Create the SnapshotProducer first so we can use its snapshot_id
        // for new manifests. This ensures the manifest list writer can
        // assign sequence numbers to manifests from this snapshot.
        let snapshot_producer =
            SnapshotProducer::new(table, self.commit_uuid, HashMap::new(), Vec::new());
        let snapshot_id = snapshot_producer.snapshot_id;

        // 1. Load existing manifests from the current snapshot.
        let existing_manifests = match table.metadata().current_snapshot() {
            Some(snapshot) => {
                let manifest_list = table.manifest_list_reader(snapshot).load().await?;
                manifest_list
                    .entries()
                    .iter()
                    .filter(|e| {
                        e.has_added_files() || e.has_existing_files() || e.has_deleted_files()
                    })
                    .cloned()
                    .collect()
            }
            None => Vec::new(),
        };

        // 2. Filter existing manifests — remove deleted file entries.
        let (mut filtered_manifests, removed_collector) = self
            .filter_manager
            .filter_manifests(table, existing_manifests, snapshot_id)
            .await?;

        // 3. Write a new manifest for added files.
        if !self.added_data_files.is_empty() {
            let added_manifest = self.write_added_manifest(table, snapshot_id).await?;
            filtered_manifests.push(added_manifest);
        }

        // 4. Compute summary (added + removed).
        let summary = self.build_summary(table, removed_collector)?;

        // 5. Delegate to SnapshotProducer for manifest list + snapshot creation.
        snapshot_producer
            .commit_with_manifests(filtered_manifests, summary)
            .await
    }

    async fn write_added_manifest(&self, table: &Table, snapshot_id: i64) -> Result<ManifestFile> {
        let new_manifest_path = format!(
            "{}/{}-m-added.{}",
            table.metadata().metadata_location()?,
            self.commit_uuid,
            DataFileFormat::Avro,
        );
        let output_file = table.file_io().new_output(new_manifest_path)?;
        let schema = table.metadata().current_schema().clone();
        let partition_spec = table.metadata().default_partition_spec().as_ref().clone();

        let mut writer = new_manifest_writer(
            table,
            output_file,
            Some(snapshot_id),
            schema,
            partition_spec,
        )?;

        let format_version = table.metadata().format_version();
        for data_file in &self.added_data_files {
            if data_file.content_type() != DataContentType::Data {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Only data content type is allowed for rewrite files",
                ));
            }
            let entry_builder = ManifestEntry::builder()
                .status(ManifestStatus::Added)
                .data_file(data_file.clone());
            let entry = if format_version == FormatVersion::V1 {
                // V1 requires snapshot_id on each entry.
                entry_builder.snapshot_id(snapshot_id).build()
            } else {
                entry_builder.build()
            };
            writer.add_entry(entry)?;
        }

        writer.write_manifest_file().await
    }

    fn build_summary(
        &self,
        table: &Table,
        removed_collector: SnapshotSummaryCollector,
    ) -> Result<Summary> {
        let table_metadata = table.metadata_ref();
        let schema = table_metadata.current_schema().clone();
        let partition_spec = table_metadata.default_partition_spec().clone();

        let mut collector = SnapshotSummaryCollector::default();
        for file in &self.added_data_files {
            collector.add_file(file, schema.clone(), partition_spec.clone());
        }
        // Merge removal metrics from the filter manager.
        collector.merge(removed_collector);

        let mut additional_properties = self.snapshot_properties.clone();
        additional_properties.extend(collector.build());

        let summary = Summary {
            operation: self.operation.clone(),
            additional_properties,
        };

        let previous_snapshot = table_metadata.current_snapshot();
        update_snapshot_summaries(summary, previous_snapshot.map(|s| s.summary()), false)
    }
}
