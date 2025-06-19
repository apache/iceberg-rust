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

//! Expire snapshots maintenance operation
//!
//! This module implements the expire snapshots operation that removes old snapshots
//! and their associated metadata files from the table while keeping the table
//! in a consistent state.

use std::collections::HashSet;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::runtime::JoinHandle;
use crate::spec::SnapshotRef;
use crate::table::Table;
use crate::transaction::Transaction;
use crate::{Catalog, Error, ErrorKind, TableUpdate};

/// Result of the expire snapshots operation. Contains information about how many files were
/// deleted.
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExpireSnapshotsResult {
    /// Number of data files deleted. Data file deletion is not supported by this action yet, this
    /// will always be 0.
    pub deleted_data_files_count: u64,
    /// Number of position delete files deleted. Position delete file deletion is not supported by
    /// this action yet, this will always be 0.
    pub deleted_position_delete_files_count: u64,
    /// Number of equality delete files deleted. Equality delete file deletion is not supported by
    /// this action yet, this will always be 0.
    pub deleted_equality_delete_files_count: u64,
    /// Number of manifest files deleted
    pub deleted_manifest_files_count: u64,
    /// Number of manifest list files deleted
    pub deleted_manifest_lists_count: u64,
    /// Number of statistics files deleted. Statistics file deletion is not supported by this action
    /// yet, this will always be 0.
    pub deleted_statistics_files_count: u64,
}

/// Configuration for the expire snapshots operation
#[derive(Debug, Clone)]
pub struct ExpireSnapshotsConfig {
    /// Timestamp in milliseconds. Snapshots older than this will be expired
    pub older_than_ms: Option<i64>,
    /// Minimum number of snapshots to retain
    pub retain_last: Option<u32>,
    /// Maximum number of concurrent file deletions
    pub max_concurrent_deletes: Option<u32>,
    /// Specific snapshot IDs to expire
    pub snapshot_ids: Vec<i64>,
    /// Whether to perform a dry run. If true, the operation will not delete any files, but will
    /// still identify the files to delete and return the result.
    pub dry_run: bool,
}

impl Default for ExpireSnapshotsConfig {
    fn default() -> Self {
        Self {
            older_than_ms: None,
            retain_last: Some(1), // Default to retaining at least 1 snapshot
            max_concurrent_deletes: None,
            snapshot_ids: vec![],
            dry_run: false,
        }
    }
}

/// Trait for performing expire snapshots operations
///
/// This trait provides a low-level API for expiring snapshots that can be
/// extended with different implementations for different environments.
#[async_trait]
pub trait ExpireSnapshots: Send + Sync {
    /// Execute the expire snapshots operation
    async fn execute(&self, catalog: &dyn Catalog) -> Result<ExpireSnapshotsResult>;
}

/// Implementation of the expire snapshots operation
pub struct ExpireSnapshotsAction {
    table: Table,
    config: ExpireSnapshotsConfig,
}

impl ExpireSnapshotsAction {
    /// Create a new expire snapshots action
    pub fn new(table: Table) -> Self {
        Self {
            table,
            config: ExpireSnapshotsConfig::default(),
        }
    }

    /// Set the timestamp threshold for expiring snapshots
    pub fn expire_older_than(mut self, timestamp_ms: i64) -> Self {
        self.config.older_than_ms = Some(timestamp_ms);
        self
    }

    /// Set the dry run flag
    pub fn dry_run(mut self, dry_run: bool) -> Self {
        self.config.dry_run = dry_run;
        self
    }

    /// Set the minimum number of snapshots to retain. If the number of snapshots is less than 1,
    /// it will be automatically adjusted to 1, following the behavior in Spark.
    pub fn retain_last(mut self, num_snapshots: u32) -> Self {
        if num_snapshots < 1 {
            self.config.retain_last = Some(1);
        } else {
            self.config.retain_last = Some(num_snapshots);
        }
        self
    }

    /// Set specific snapshot IDs to expire. An empty list is equivalent to the default behavior
    /// of expiring all but `retain_last` snapshots! When only expiring specific snapshots, please
    /// ensure that the list of snapshot IDs is non-empty before using this method.
    pub fn expire_snapshot_ids(mut self, snapshot_ids: Vec<i64>) -> Self {
        self.config.snapshot_ids = snapshot_ids;
        self
    }

    /// Set the maximum number of concurrent file deletions
    pub fn max_concurrent_deletes(mut self, max_deletes: u32) -> Self {
        if max_deletes > 0 {
            self.config.max_concurrent_deletes = Some(max_deletes);
        }
        self
    }

    /// Determine which snapshots should be expired based on the configuration. This will:
    ///
    /// - Sort snapshots by timestamp (oldest first)
    /// - Apply filters if supplied. If multiple filters are supplied, the result will be the
    ///   intersection of the results of each filter.
    ///   - If specific snapshot IDs are provided, only expire those
    ///   - If `older_than_ms` is provided, expire snapshots older than this timestamp
    ///   - If `retain_last` is provided, retain the last `retain_last` snapshots
    /// - Never expire the current snapshot!
    ///
    /// Returns a Vec of SnapshotRefs that should be expired (references removed, and deleted).
    fn identify_snapshots_to_expire(&self) -> Result<Vec<SnapshotRef>> {
        let metadata = self.table.metadata();
        let all_snapshots: Vec<SnapshotRef> = metadata.snapshots().cloned().collect();

        if all_snapshots.is_empty() {
            return Ok(vec![]);
        }

        if !self.config.snapshot_ids.is_empty() {
            let snapshot_id_set: HashSet<i64> = self.config.snapshot_ids.iter().cloned().collect();
            let snapshots_to_expire: Vec<SnapshotRef> = all_snapshots
                .into_iter()
                .filter(|snapshot| snapshot_id_set.contains(&snapshot.snapshot_id()))
                .collect();

            if let Some(current_snapshot_id) = metadata.current_snapshot_id() {
                if snapshot_id_set.contains(&current_snapshot_id) {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Cannot expire the current snapshot",
                    ));
                }
            }

            return Ok(snapshots_to_expire);
        }

        let mut sorted_snapshots = all_snapshots;
        sorted_snapshots.sort_by_key(|snapshot| snapshot.timestamp_ms());

        let mut snapshots_to_expire = vec![];
        let retain_last = self.config.retain_last.unwrap_or(1) as usize;

        if sorted_snapshots.len() <= retain_last {
            return Ok(vec![]);
        }

        let mut candidates = sorted_snapshots;

        candidates.truncate(candidates.len().saturating_sub(retain_last));

        if let Some(older_than_ms) = self.config.older_than_ms {
            candidates.retain(|snapshot| snapshot.timestamp_ms() < older_than_ms);
        }

        // NEVER expire the current snapshot!
        if let Some(current_snapshot_id) = metadata.current_snapshot_id() {
            candidates.retain(|snapshot| snapshot.snapshot_id() != current_snapshot_id);
        }

        snapshots_to_expire.extend(candidates);

        Ok(snapshots_to_expire)
    }

    /// Collect all files that should be deleted along with the expired snapshots
    async fn collect_files_to_delete(
        &self,
        expired_snapshots: &[SnapshotRef],
    ) -> Result<Vec<String>> {
        let mut files_to_delete = Vec::new();
        let file_io = self.table.file_io();
        let metadata = self.table.metadata();

        // Collect files from snapshots that are being expired
        let mut expired_manifest_lists = HashSet::new();
        let mut expired_manifests = HashSet::new();

        for snapshot in expired_snapshots {
            expired_manifest_lists.insert(snapshot.manifest_list().to_string());

            match snapshot.load_manifest_list(file_io, metadata).await {
                Ok(manifest_list) => {
                    for manifest_entry in manifest_list.entries() {
                        expired_manifests.insert(manifest_entry.manifest_path.clone());
                    }
                }
                Err(e) => {
                    // Log warning but continue - the manifest list file might already be deleted
                    eprintln!(
                        "Warning: Failed to load manifest list {}: {}",
                        snapshot.manifest_list(),
                        e
                    );
                }
            }
        }

        // Collect files that are still referenced by remaining snapshots
        let remaining_snapshots: Vec<SnapshotRef> = metadata
            .snapshots()
            .filter(|snapshot| {
                !expired_snapshots
                    .iter()
                    .any(|exp| exp.snapshot_id() == snapshot.snapshot_id())
            })
            .cloned()
            .collect();

        let mut still_referenced_manifest_lists = HashSet::new();
        let mut still_referenced_manifests = HashSet::new();

        for snapshot in &remaining_snapshots {
            still_referenced_manifest_lists.insert(snapshot.manifest_list().to_string());

            match snapshot.load_manifest_list(file_io, metadata).await {
                Ok(manifest_list) => {
                    for manifest_entry in manifest_list.entries() {
                        still_referenced_manifests.insert(manifest_entry.manifest_path.clone());
                    }
                }
                Err(e) => {
                    // Log warning but continue
                    eprintln!(
                        "Warning: Failed to load manifest list {}: {}",
                        snapshot.manifest_list(),
                        e
                    );
                }
            }
        }

        for manifest_list_path in expired_manifest_lists {
            if !still_referenced_manifest_lists.contains(&manifest_list_path) {
                files_to_delete.push(manifest_list_path);
            }
        }

        for manifest_path in expired_manifests {
            if !still_referenced_manifests.contains(&manifest_path) {
                files_to_delete.push(manifest_path);
            }
        }

        Ok(files_to_delete)
    }

    async fn process_file_deletion(&self, result: &mut ExpireSnapshotsResult, file_path: String) {
        if file_path.ends_with(".avro") && file_path.contains("snap-") {
            result.deleted_manifest_lists_count += 1;
        } else if file_path.ends_with(".avro") {
            result.deleted_manifest_files_count += 1;
        }
    }

    /// Delete files concurrently with respect to max_concurrent_deletes setting
    /// Should not be called if dry_run is true, but this is checked for extra safety.
    async fn delete_files(&self, files_to_delete: Vec<String>) -> Result<ExpireSnapshotsResult> {
        let mut result = ExpireSnapshotsResult::default();

        if self.config.dry_run {
            for file_path in files_to_delete {
                self.process_file_deletion(&mut result, file_path).await;
            }
            return Ok(result);
        }

        let file_io = self.table.file_io();

        if files_to_delete.is_empty() {
            return Ok(result);
        }

        let num_concurrent_deletes = self.config.max_concurrent_deletes.unwrap_or(1) as usize;
        let mut delete_tasks: Vec<JoinHandle<Vec<Result<String>>>> =
            Vec::with_capacity(num_concurrent_deletes);

        eprintln!("Num concurrent deletes: {}", num_concurrent_deletes);

        for task_index in 0..num_concurrent_deletes {
            // Ideally we'd use a semaphore here to allow each thread to delete as fast as possible.
            // However, we can't assume that tokio::sync::Semaphore is available, and AsyncStd
            // does not appear to have a usable Semaphore. Instead, we'll pre-sort the files into
            // `num_concurrent_deletes` equal size chunks and spawn a task for each chunk.
            let task_file_paths: Vec<String> = files_to_delete
                .iter()
                .skip(task_index)
                .step_by(num_concurrent_deletes)
                .cloned()
                .collect();
            let file_io_clone = file_io.clone();
            let task = crate::runtime::spawn(async move {
                let mut results: Vec<Result<String>> = Vec::new();
                for file_path in task_file_paths {
                    match file_io_clone.delete(&file_path).await {
                        Ok(_) => {
                            eprintln!("Deleted file: {:?}", file_path);
                            results.push(Ok(file_path));
                        }
                        Err(e) => {
                            eprintln!("Error deleting file: {:?}", e);
                            results.push(Err(e));
                        }
                    }
                }
                results
            });

            delete_tasks.push(task);
        }

        for task in delete_tasks {
            let file_delete_results = task.await;
            for file_delete_result in file_delete_results {
                eprintln!("Deleted file: {:?}", file_delete_result);
                match file_delete_result {
                    Ok(deleted_path) => {
                        self.process_file_deletion(&mut result, deleted_path).await;
                    }
                    Err(e) => {
                        eprintln!("Warning: File deletion task failed: {}", e);
                    }
                }
            }
        }

        Ok(result)
    }
}

#[async_trait]
impl ExpireSnapshots for ExpireSnapshotsAction {
    /// The main entrypoint for the expire snapshots action. This will:
    ///
    /// - Validate the table state
    /// - Identify snapshots to expire
    /// - Update the table metadata to remove expired snapshots
    /// - Collect files to delete
    /// - Delete the files
    async fn execute(&self, catalog: &dyn Catalog) -> Result<ExpireSnapshotsResult> {
        if self.table.readonly() {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Cannot expire snapshots on a readonly table",
            ));
        }

        let snapshots_to_expire = self.identify_snapshots_to_expire()?;

        if snapshots_to_expire.is_empty() {
            return Ok(ExpireSnapshotsResult::default());
        }

        let files_to_delete = self.collect_files_to_delete(&snapshots_to_expire).await?;

        if self.config.dry_run {
            let mut result = ExpireSnapshotsResult::default();
            for file_path in files_to_delete {
                self.process_file_deletion(&mut result, file_path).await;
            }
            return Ok(result);
        }

        // update the table metadata to remove the expired snapshots _before_ deleting anything!
        // TODO: make this retry
        let mut transaction = Transaction::new(&self.table);
        let mut snapshot_ids: Vec<i64> = snapshots_to_expire
            .iter()
            .map(|s| s.snapshot_id())
            .collect();
        // sort for a deterministic output
        snapshot_ids.sort();
        transaction.apply(
            vec![TableUpdate::RemoveSnapshots { snapshot_ids }],
            // no requirements here. if the table's main branch was rewound while this operation is
            // running, this will potentially corrupt the table by deleting the wrong snapshots.
            // but, if this fails because of a concurrent update, we have to repeat the logic.
            // TODO: verify that this is running against the latest metadata version. if the commit
            // fails, refresh the metadata and try again if and only if new snapshots were added.
            vec![],
        )?;
        transaction.commit(catalog).await?;

        let result = self.delete_files(files_to_delete).await?;

        Ok(result)
    }
}

/// Builder for creating expire snapshots operations
pub struct ExpireSnapshotsBuilder {
    table: Table,
}

impl ExpireSnapshotsBuilder {
    /// Create a new builder for the given table
    pub fn new(table: Table) -> Self {
        Self { table }
    }

    /// Build an expire snapshots action with default configuration
    pub fn build(self) -> ExpireSnapshotsAction {
        ExpireSnapshotsAction::new(self.table)
    }
}

// Extension trait to add expire_snapshots method to Table
impl Table {
    /// Create a new expire snapshots builder for this table
    pub fn expire_snapshots(&self) -> ExpireSnapshotsBuilder {
        ExpireSnapshotsBuilder::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use chrono::Utc;
    use uuid::Uuid;

    use super::*;
    use crate::io::{FileIOBuilder, OutputFile};
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, FormatVersion, ManifestEntry,
        ManifestListWriter, ManifestStatus, ManifestWriterBuilder, NestedField, Operation,
        PrimitiveType, Schema, Snapshot, Struct, Summary, TableMetadataBuilder, Type,
    };
    use crate::table::{Table, TableBuilder};
    use crate::{Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent};

    #[derive(Debug)]
    struct MockCatalog {
        table: Table,
        update_table_calls: Mutex<Vec<TableCommit>>,
    }

    impl MockCatalog {
        fn new(table: Table) -> Self {
            Self {
                table,
                update_table_calls: Mutex::new(vec![]),
            }
        }
    }

    #[async_trait]
    impl Catalog for MockCatalog {
        async fn load_table(&self, _table_ident: &TableIdent) -> Result<Table> {
            Ok(self.table.clone())
        }

        async fn drop_table(&self, _table_ident: &TableIdent) -> Result<()> {
            unimplemented!()
        }

        async fn table_exists(&self, _table_ident: &TableIdent) -> Result<bool> {
            unimplemented!()
        }

        async fn rename_table(
            &self,
            _src_table_ident: &TableIdent,
            _dst_table_ident: &TableIdent,
        ) -> Result<()> {
            unimplemented!()
        }

        async fn update_table(&self, commit: TableCommit) -> Result<Table> {
            self.update_table_calls.lock().unwrap().push(commit);
            Ok(self.table.clone())
        }

        async fn create_table(
            &self,
            _namespace: &NamespaceIdent,
            _creation: TableCreation,
        ) -> Result<Table> {
            unimplemented!()
        }

        async fn list_tables(&self, _namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
            unimplemented!()
        }

        async fn create_namespace(
            &self,
            _namespace: &NamespaceIdent,
            _properties: HashMap<String, String>,
        ) -> Result<Namespace> {
            unimplemented!()
        }

        async fn get_namespace(&self, _namespace: &NamespaceIdent) -> Result<Namespace> {
            unimplemented!()
        }

        async fn list_namespaces(
            &self,
            _parent: Option<&NamespaceIdent>,
        ) -> Result<Vec<NamespaceIdent>> {
            unimplemented!()
        }

        async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> Result<bool> {
            unimplemented!()
        }

        async fn update_namespace(
            &self,
            _namespace: &NamespaceIdent,
            _properties: HashMap<String, String>,
        ) -> Result<()> {
            unimplemented!()
        }

        async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> Result<()> {
            unimplemented!()
        }
    }

    struct TableTestFixture {
        pub table: Table,
        pub base_time: i64,
    }

    impl TableTestFixture {
        async fn new() -> Self {
            let file_io = FileIOBuilder::new("memory").build().unwrap();

            let schema = Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap();

            // rewind time to allow transaction to succeed, since it autogenerates a new timestamp
            let base_time = Utc::now().timestamp_millis() - (24 * 60 * 60 * 1000);

            let table_metadata_builder = TableMetadataBuilder::new(
                schema,
                crate::spec::PartitionSpec::unpartition_spec(),
                crate::spec::SortOrder::unsorted_order(),
                "memory://test/table".to_string(),
                FormatVersion::V2,
                HashMap::new(),
            )
            .unwrap();
            let table_metadata_builder = table_metadata_builder.set_last_updated_ms(base_time);

            let metadata = table_metadata_builder.build().unwrap().metadata;

            let table = TableBuilder::new()
                .metadata(Arc::new(metadata))
                .identifier(TableIdent::from_strs(["test", "table"]).unwrap())
                .file_io(file_io)
                .build()
                .unwrap();

            Self { table, base_time }
        }

        fn next_manifest_file(&self) -> OutputFile {
            self.table
                .file_io()
                .new_output(format!(
                    "memory://test/table/metadata/manifest_{}.avro",
                    Uuid::new_v4()
                ))
                .unwrap()
        }

        fn next_manifest_list_file(&self) -> OutputFile {
            self.table
                .file_io()
                .new_output(format!(
                    "memory://test/table/metadata/snap-{}-manifest-list.avro",
                    Uuid::new_v4()
                ))
                .unwrap()
        }

        fn next_data_file_path(&self) -> String {
            format!("memory://test/table/data/data_{}.parquet", Uuid::new_v4())
        }

        async fn add_snapshot(&mut self, snapshot_id: i64) {
            eprintln!("Adding snapshot: {}", snapshot_id);
            let parent_id = if snapshot_id == 1000 {
                None
            } else {
                Some(snapshot_id - 1)
            };
            eprintln!("Parent id: {:?}", parent_id);

            let manifest_file = self.next_manifest_file();
            let manifest_list_file = self.next_manifest_list_file();
            let manifest_list_file_path = manifest_list_file.location().to_string();

            let mut writer = ManifestWriterBuilder::new(
                manifest_file,
                Some(snapshot_id),
                None,
                self.table.metadata().current_schema().clone(),
                self.table
                    .metadata()
                    .default_partition_spec()
                    .as_ref()
                    .clone(),
            )
            .build_v2_data();

            writer
                .add_entry(
                    ManifestEntry::builder()
                        .status(ManifestStatus::Added)
                        .data_file(
                            DataFileBuilder::default()
                                .partition_spec_id(0)
                                .content(DataContentType::Data)
                                .file_path(self.next_data_file_path())
                                .file_format(DataFileFormat::Parquet)
                                .file_size_in_bytes(100)
                                .record_count(1)
                                .partition(Struct::from_iter([]))
                                .key_metadata(None)
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .unwrap();
            let data_file_manifest = writer.write_manifest_file().await.unwrap();

            // Write to manifest list
            let mut manifest_list_write =
                ManifestListWriter::v2(manifest_list_file, snapshot_id, parent_id, snapshot_id + 1);
            manifest_list_write
                .add_manifests(vec![data_file_manifest].into_iter())
                .unwrap();
            manifest_list_write.close().await.unwrap();

            let snapshot = Snapshot::builder()
                .with_snapshot_id(snapshot_id)
                .with_parent_snapshot_id(parent_id)
                .with_sequence_number(snapshot_id + 1)
                .with_timestamp_ms(self.base_time + (snapshot_id * 60000))
                .with_manifest_list(manifest_list_file_path)
                .with_summary(Summary {
                    operation: Operation::Append,
                    additional_properties: HashMap::new(),
                })
                .with_schema_id(0)
                .build();

            let mut table_metadata_builder =
                TableMetadataBuilder::new_from_metadata(self.table.metadata().clone(), None);

            table_metadata_builder = table_metadata_builder.add_snapshot(snapshot).unwrap();

            table_metadata_builder = table_metadata_builder.assign_uuid(Uuid::new_v4());
            table_metadata_builder = table_metadata_builder
                .set_ref(crate::spec::MAIN_BRANCH, crate::spec::SnapshotReference {
                    snapshot_id,
                    retention: crate::spec::SnapshotRetention::Branch {
                        min_snapshots_to_keep: Some(10),
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                })
                .unwrap();

            let metadata = table_metadata_builder.build().unwrap().metadata;

            let mut table = self.table.clone();
            table.with_metadata(Arc::new(metadata));

            self.table = table;
        }
    }

    /// Helper function to create a table with snapshots
    async fn create_table_with_snapshots(snapshot_count: usize) -> Table {
        let mut table_fixture = TableTestFixture::new().await;

        for i in 0..snapshot_count {
            table_fixture.add_snapshot((i + 1000) as i64).await;
        }

        table_fixture.table
    }

    #[tokio::test]
    async fn test_expire_snapshots_zero_snapshots() {
        let table = create_table_with_snapshots(0).await;
        let catalog = MockCatalog::new(table.clone());

        let action = table.expire_snapshots().build();
        let result = action.execute(&catalog).await.unwrap();

        // Should complete successfully with no deletions
        assert_eq!(result.deleted_data_files_count, 0);
        assert_eq!(result.deleted_manifest_files_count, 0);
        assert_eq!(result.deleted_manifest_lists_count, 0);
        assert_eq!(result.deleted_position_delete_files_count, 0);
        assert_eq!(result.deleted_equality_delete_files_count, 0);
        assert_eq!(result.deleted_statistics_files_count, 0);

        assert_eq!(catalog.update_table_calls.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_expire_snapshots_one_snapshot() {
        let table = create_table_with_snapshots(1).await;
        let catalog = MockCatalog::new(table.clone());

        let action = table.expire_snapshots().build();
        let result = action.execute(&catalog).await.unwrap();

        assert_eq!(result.deleted_data_files_count, 0);
        assert_eq!(result.deleted_manifest_files_count, 0);
        assert_eq!(result.deleted_manifest_lists_count, 0);
        assert_eq!(result.deleted_position_delete_files_count, 0);
        assert_eq!(result.deleted_equality_delete_files_count, 0);
        assert_eq!(result.deleted_statistics_files_count, 0);

        assert_eq!(catalog.update_table_calls.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_expire_snapshots_many_snapshots_retain_last() {
        let table = create_table_with_snapshots(5).await;
        let catalog = MockCatalog::new(table.clone());

        let action = table.expire_snapshots().build().retain_last(2);

        let snapshots_to_expire = action.identify_snapshots_to_expire().unwrap();

        assert_eq!(snapshots_to_expire.len(), 3);

        let mut ids_to_expire: Vec<i64> = snapshots_to_expire
            .iter()
            .map(|s| s.snapshot_id())
            .collect();

        ids_to_expire.sort();

        assert_eq!(ids_to_expire, vec![1000, 1001, 1002]);

        let result = action.execute(&catalog).await.unwrap();

        assert_eq!(result.deleted_manifest_lists_count, 3);
        assert_eq!(result.deleted_manifest_files_count, 3);
        assert_eq!(result.deleted_data_files_count, 0);
        assert_eq!(result.deleted_position_delete_files_count, 0);
        assert_eq!(result.deleted_equality_delete_files_count, 0);

        assert_eq!(catalog.update_table_calls.lock().unwrap().len(), 1);

        let mut commit = catalog.update_table_calls.lock().unwrap().pop().unwrap();
        let updates = commit.take_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0], TableUpdate::RemoveSnapshots {
            snapshot_ids: vec![1000, 1001, 1002]
        });
    }

    #[tokio::test]
    async fn test_expire_snapshots_older_than_timestamp() {
        // Test case 3b: Table with many snapshots, expire based on timestamp
        let table = create_table_with_snapshots(5).await;
        let catalog = MockCatalog::new(table.clone());

        // Get the timestamp of the middle snapshot and use it as threshold
        let middle_timestamp = table
            .metadata()
            .snapshots()
            .find(|s| s.snapshot_id() == 1002)
            .unwrap()
            .timestamp_ms();

        let action = table
            .expire_snapshots()
            .build()
            .expire_older_than(middle_timestamp)
            .retain_last(1); // Keep at least 1

        let snapshots_to_expire = action.identify_snapshots_to_expire().unwrap();

        // Should expire snapshots older than the middle one, but keep at least 1
        // So we expect snapshots 1000 and 1001 to be expired
        assert_eq!(snapshots_to_expire.len(), 2);

        let mut ids_to_expire: Vec<i64> = snapshots_to_expire
            .iter()
            .map(|s| s.snapshot_id())
            .collect();
        ids_to_expire.sort();

        assert_eq!(ids_to_expire, vec![1000, 1001]);

        let result = action.execute(&catalog).await.unwrap();

        assert_eq!(result.deleted_manifest_lists_count, 2);
        assert_eq!(result.deleted_manifest_files_count, 2);
        assert_eq!(result.deleted_data_files_count, 0);
        assert_eq!(result.deleted_position_delete_files_count, 0);
        assert_eq!(result.deleted_equality_delete_files_count, 0);
        assert_eq!(result.deleted_statistics_files_count, 0);

        assert_eq!(catalog.update_table_calls.lock().unwrap().len(), 1);

        let mut commit = catalog.update_table_calls.lock().unwrap().pop().unwrap();
        let updates = commit.take_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0], TableUpdate::RemoveSnapshots {
            snapshot_ids: vec![1000, 1001]
        });
    }

    #[tokio::test]
    async fn test_expire_specific_snapshot_ids() {
        let table = create_table_with_snapshots(5).await;
        let catalog = MockCatalog::new(table.clone());

        let action = table
            .expire_snapshots()
            .build()
            .expire_snapshot_ids(vec![1001, 1003]);

        let snapshots_to_expire = action.identify_snapshots_to_expire().unwrap();

        assert_eq!(snapshots_to_expire.len(), 2);

        let mut ids_to_expire: Vec<i64> = snapshots_to_expire
            .iter()
            .map(|s| s.snapshot_id())
            .collect();

        ids_to_expire.sort();

        assert_eq!(ids_to_expire, vec![1001, 1003]);

        let result = action.execute(&catalog).await.unwrap();

        assert_eq!(result.deleted_manifest_lists_count, 2);
        assert_eq!(result.deleted_manifest_files_count, 2);
        assert_eq!(result.deleted_data_files_count, 0);
        assert_eq!(result.deleted_position_delete_files_count, 0);
        assert_eq!(result.deleted_equality_delete_files_count, 0);
        assert_eq!(result.deleted_statistics_files_count, 0);

        assert_eq!(catalog.update_table_calls.lock().unwrap().len(), 1);

        let mut commit = catalog.update_table_calls.lock().unwrap().pop().unwrap();
        let updates = commit.take_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0], TableUpdate::RemoveSnapshots {
            snapshot_ids: vec![1001, 1003]
        });
    }

    #[tokio::test]
    async fn test_expire_current_snapshot_error() {
        let table = create_table_with_snapshots(3).await;
        let current_snapshot_id = table.metadata().current_snapshot_id().unwrap();

        let action = table
            .expire_snapshots()
            .build()
            .expire_snapshot_ids(vec![current_snapshot_id]);

        let result = action.identify_snapshots_to_expire();

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(
            error
                .message()
                .contains("Cannot expire the current snapshot")
        );
    }

    #[tokio::test]
    async fn test_expire_readonly_table_error() {
        let table = create_table_with_snapshots(3).await;
        let catalog = MockCatalog::new(table.clone());

        let readonly_table = TableBuilder::new()
            .metadata(table.metadata_ref())
            .identifier(table.identifier().clone())
            .file_io(table.file_io().clone())
            .readonly(true)
            .build()
            .unwrap();

        let action = readonly_table.expire_snapshots().build();
        let result = action.execute(&catalog).await;

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::FeatureUnsupported);
        assert!(
            error
                .message()
                .contains("Cannot expire snapshots on a readonly table")
        );
    }

    #[tokio::test]
    async fn test_retain_last_minimum_validation() {
        let table = create_table_with_snapshots(3).await;
        let catalog = MockCatalog::new(table.clone());

        let action = table.expire_snapshots().build().retain_last(0);

        assert_eq!(action.config.retain_last, Some(1));

        let result = action.execute(&catalog).await.unwrap();

        assert_eq!(result.deleted_manifest_lists_count, 2);
        assert_eq!(result.deleted_manifest_files_count, 2);
        assert_eq!(result.deleted_data_files_count, 0);
        assert_eq!(result.deleted_position_delete_files_count, 0);
        assert_eq!(result.deleted_equality_delete_files_count, 0);
        assert_eq!(result.deleted_statistics_files_count, 0);

        assert_eq!(catalog.update_table_calls.lock().unwrap().len(), 1);

        let mut commit = catalog.update_table_calls.lock().unwrap().pop().unwrap();
        let updates = commit.take_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0], TableUpdate::RemoveSnapshots {
            snapshot_ids: vec![1000, 1001]
        });
    }

    #[tokio::test]
    async fn test_max_concurrent_deletes_configuration() {
        let table = create_table_with_snapshots(3).await;
        let catalog = MockCatalog::new(table.clone());

        let action = table.expire_snapshots().build().max_concurrent_deletes(5);

        assert_eq!(action.config.max_concurrent_deletes, Some(5));

        let action2 = table.expire_snapshots().build().max_concurrent_deletes(0);

        assert_eq!(action2.config.max_concurrent_deletes, None);

        let result = action.execute(&catalog).await.unwrap();

        assert_eq!(result.deleted_manifest_lists_count, 2);
        assert_eq!(result.deleted_manifest_files_count, 2);
        assert_eq!(result.deleted_data_files_count, 0);

        assert_eq!(catalog.update_table_calls.lock().unwrap().len(), 1);

        let mut commit = catalog.update_table_calls.lock().unwrap().pop().unwrap();
        let updates = commit.take_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0], TableUpdate::RemoveSnapshots {
            snapshot_ids: vec![1000, 1001]
        });
    }

    #[tokio::test]
    async fn test_empty_snapshot_ids_list() {
        let table = create_table_with_snapshots(3).await;
        let catalog = MockCatalog::new(table.clone());

        let action = table.expire_snapshots().build().expire_snapshot_ids(vec![]);

        let snapshots_to_expire = action.identify_snapshots_to_expire().unwrap();

        assert_eq!(snapshots_to_expire.len(), 2);

        let result = action.execute(&catalog).await.unwrap();

        assert_eq!(result.deleted_manifest_lists_count, 2);
        assert_eq!(result.deleted_manifest_files_count, 2);
        assert_eq!(result.deleted_data_files_count, 0);

        assert_eq!(catalog.update_table_calls.lock().unwrap().len(), 1);

        let mut commit = catalog.update_table_calls.lock().unwrap().pop().unwrap();
        let updates = commit.take_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0], TableUpdate::RemoveSnapshots {
            snapshot_ids: vec![1000, 1001]
        });
    }

    #[tokio::test]
    async fn test_nonexistent_snapshot_ids() {
        let table = create_table_with_snapshots(3).await;
        let catalog = MockCatalog::new(table.clone());

        let action = table
            .expire_snapshots()
            .build()
            .expire_snapshot_ids(vec![9999, 8888]);

        let snapshots_to_expire = action.identify_snapshots_to_expire().unwrap();

        assert_eq!(snapshots_to_expire.len(), 0);

        let result = action.execute(&catalog).await.unwrap();

        assert_eq!(result.deleted_manifest_lists_count, 0);
        assert_eq!(result.deleted_manifest_files_count, 0);
        assert_eq!(result.deleted_data_files_count, 0);

        assert_eq!(catalog.update_table_calls.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_builder_pattern() {
        let table = create_table_with_snapshots(5).await;

        let action = table
            .expire_snapshots()
            .build()
            .expire_older_than(Utc::now().timestamp_millis())
            .retain_last(3)
            .max_concurrent_deletes(10)
            .expire_snapshot_ids(vec![1001, 1002]);

        assert!(action.config.older_than_ms.is_some());
        assert_eq!(action.config.retain_last, Some(3));
        assert_eq!(action.config.max_concurrent_deletes, Some(10));
        assert_eq!(action.config.snapshot_ids, vec![1001, 1002]);
    }

    #[tokio::test]
    async fn test_collect_files_to_delete_logic() {
        let table = create_table_with_snapshots(4).await;

        let action = table.expire_snapshots().build().retain_last(2);

        let snapshots_to_expire = action.identify_snapshots_to_expire().unwrap();
        assert_eq!(snapshots_to_expire.len(), 2);

        let files_to_delete = action
            .collect_files_to_delete(&snapshots_to_expire)
            .await
            .unwrap();

        assert_eq!(files_to_delete.len(), 4);

        // should be two of each type of file
        let manifest_files = files_to_delete.iter().filter(|f| f.contains("manifest_"));
        assert_eq!(manifest_files.count(), 2);

        let manifest_list_files = files_to_delete.iter().filter(|f| f.contains("snap-"));
        assert_eq!(manifest_list_files.count(), 2);

        let data_files = files_to_delete.iter().filter(|f| f.contains("data_"));
        assert_eq!(data_files.count(), 0);
    }

    #[tokio::test]
    async fn test_default_configuration() {
        let table = create_table_with_snapshots(3).await;
        let action = table.expire_snapshots().build();

        assert_eq!(action.config.older_than_ms, None);
        assert_eq!(action.config.retain_last, Some(1));
        assert_eq!(action.config.max_concurrent_deletes, None);
        assert!(action.config.snapshot_ids.is_empty());
    }

    #[tokio::test]
    async fn test_dry_run() {
        let table = create_table_with_snapshots(3).await;
        let catalog = MockCatalog::new(table.clone());
        let action = table.expire_snapshots().build().dry_run(true);

        let result = action.execute(&catalog).await.unwrap();

        assert_eq!(result.deleted_manifest_lists_count, 2);
        assert_eq!(result.deleted_manifest_files_count, 2);
        assert_eq!(result.deleted_data_files_count, 0);
        assert_eq!(result.deleted_position_delete_files_count, 0);
        assert_eq!(result.deleted_equality_delete_files_count, 0);
        assert_eq!(result.deleted_statistics_files_count, 0);

        assert_eq!(catalog.update_table_calls.lock().unwrap().len(), 0);
    }
}
