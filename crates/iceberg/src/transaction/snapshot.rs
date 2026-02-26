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

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::ops::RangeFrom;

use uuid::Uuid;

use crate::error::Result;
use crate::spec::{
    DataContentType, DataFile, DataFileFormat, FormatVersion, MAIN_BRANCH, ManifestContentType,
    ManifestEntry, ManifestFile, ManifestListWriter, ManifestWriter, ManifestWriterBuilder,
    Operation, Snapshot, SnapshotReference, SnapshotRetention, SnapshotSummaryCollector, Struct,
    StructType, Summary, TableProperties, update_snapshot_summaries,
};
use crate::table::Table;
use crate::transaction::ActionCommit;
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

const META_ROOT_PATH: &str = "metadata";

/// A trait that defines how different table operations produce new snapshots.
///
/// `SnapshotProduceOperation` is used by [`SnapshotProducer`] to customize snapshot creation
/// based on the type of operation being performed (e.g., `Append`, `Overwrite`, `Delete`, etc.).
/// Each operation type implements this trait to specify:
/// - Which operation type to record in the snapshot summary
/// - Which existing manifest files should be included in the new snapshot
/// - Which manifest entries should be marked as deleted
///
/// # When it accomplishes
///
/// This trait is used during the snapshot creation process in [`SnapshotProducer::commit()`]:
///
/// 1. **Operation Type Recording**: The `operation()` method determines which operation type
///    (e.g., `Operation::Append`, `Operation::Overwrite`) is recorded in the snapshot summary.
///    This metadata helps track what kind of change was made to the table.
///
/// 2. **Manifest File Selection**: The `existing_manifest()` method determines which existing
///    manifest files from the current snapshot should be carried forward to the new snapshot.
///    For example:
///    - An `Append` operation typically includes all existing manifests plus new ones
///    - An `Overwrite` operation might exclude manifests for partitions being overwritten
///
/// 3. **Delete Entry Processing**: The `delete_entries()` method is intended for future delete
///    operations to specify which manifest entries should be marked as deleted.
pub(crate) trait SnapshotProduceOperation: Send + Sync {
    /// Returns the operation type that will be recorded in the snapshot summary.
    ///
    /// This determines what kind of operation is being performed (e.g., `Append`, `Overwrite`),
    /// which is stored in the snapshot metadata for tracking and auditing purposes.
    fn operation(&self) -> Operation;

    /// Returns manifest entries that should be marked as deleted in the new snapshot.
    #[allow(unused)]
    fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProducer,
    ) -> impl Future<Output = Result<Vec<ManifestEntry>>> + Send;

    /// Returns existing manifest files that should be included in the new snapshot.
    ///
    /// This method determines which manifest files from the current snapshot should be
    /// carried forward to the new snapshot. The selection depends on the operation type:
    ///
    /// - **Append operations**: Typically include all existing manifests
    /// - **Overwrite operations**: May exclude manifests for partitions being overwritten
    /// - **Delete operations**: May exclude manifests for partitions being deleted
    fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> impl Future<Output = Result<Vec<ManifestFile>>> + Send;
}

pub(crate) struct DefaultManifestProcess;

impl ManifestProcess for DefaultManifestProcess {
    fn process_manifests(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
        manifests: Vec<ManifestFile>,
    ) -> Vec<ManifestFile> {
        manifests
    }
}

pub(crate) trait ManifestProcess: Send + Sync {
    fn process_manifests(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
        manifests: Vec<ManifestFile>,
    ) -> Vec<ManifestFile>;
}

pub(crate) struct SnapshotProducer<'a> {
    pub(crate) table: &'a Table,
    snapshot_id: i64,
    commit_uuid: Uuid,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    // A counter used to generate unique manifest file names.
    // It starts from 0 and increments for each new manifest file.
    // Note: This counter is limited to the range of (0..u64::MAX).
    manifest_counter: RangeFrom<u64>,
}

impl<'a> SnapshotProducer<'a> {
    pub(crate) fn new(
        table: &'a Table,
        commit_uuid: Uuid,
        key_metadata: Option<Vec<u8>>,
        snapshot_properties: HashMap<String, String>,
        added_data_files: Vec<DataFile>,
        added_delete_files: Vec<DataFile>,
    ) -> Self {
        Self {
            table,
            snapshot_id: Self::generate_unique_snapshot_id(table),
            commit_uuid,
            key_metadata,
            snapshot_properties,
            added_data_files,
            added_delete_files,
            manifest_counter: (0..),
        }
    }

    pub(crate) fn validate_added_data_files(&self) -> Result<()> {
        for data_file in &self.added_data_files {
            if data_file.content_type() != DataContentType::Data {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Only data content type is allowed for fast append",
                ));
            }
            // Check if the data file partition spec id matches the table default partition spec id.
            if self.table.metadata().default_partition_spec_id() != data_file.partition_spec_id {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Data file partition spec id does not match table default partition spec id",
                ));
            }
            Self::validate_partition_value(
                data_file.partition(),
                self.table.metadata().default_partition_type(),
            )?;
        }

        Ok(())
    }

    /// Validates added delete files.
    ///
    /// Checks that:
    /// - Delete files are not used with format version 1
    /// - Delete files have valid content types (PositionDeletes or EqualityDeletes)
    /// - Equality delete files have equality_ids set
    /// - Delete files reference valid partition specs
    /// - Partition values are compatible with partition types
    pub(crate) fn validate_added_delete_files(&self) -> Result<()> {
        let format_version = self.table.metadata().format_version();
        if format_version == FormatVersion::V1 && !self.added_delete_files.is_empty() {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Delete files are not supported in format version 1. Upgrade the table to format version 2 or later.",
            ));
        }

        for delete_file in &self.added_delete_files {
            match delete_file.content_type() {
                DataContentType::PositionDeletes => {
                    if delete_file.equality_ids().is_some() {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            "Position delete files must not have equality_ids set",
                        ));
                    }
                }
                DataContentType::EqualityDeletes => {
                    let ids = delete_file.equality_ids().ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "Equality delete files must have equality_ids set",
                        )
                    })?;
                    if ids.is_empty() {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            "Equality delete files must have equality_ids set",
                        ));
                    }
                }
                DataContentType::Data => {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Data content type is not allowed for delete files",
                    ));
                }
            }

            // TODO: This validation is too strict for partition evolution scenarios where delete
            // files may reference older partition specs. Once manifest-per-spec is implemented,
            // relax this to check that the spec_id exists rather than matching the default.
            if self.table.metadata().default_partition_spec_id() != delete_file.partition_spec_id {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Delete file partition spec id does not match table default partition spec id",
                ));
            }
            Self::validate_partition_value(
                delete_file.partition(),
                self.table.metadata().default_partition_type(),
            )?;
        }

        Ok(())
    }

    pub(crate) async fn validate_duplicate_files(&self) -> Result<()> {
        let mut seen_data_files: HashSet<&str> = HashSet::new();
        let mut intra_batch_data_duplicates = Vec::new();
        for data_file in &self.added_data_files {
            if !seen_data_files.insert(data_file.file_path.as_str()) {
                intra_batch_data_duplicates.push(data_file.file_path.clone());
            }
        }
        if !intra_batch_data_duplicates.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot add duplicate data files in the same batch: {}",
                    intra_batch_data_duplicates.join(", ")
                ),
            ));
        }

        let mut seen_delete_files: HashSet<&str> = HashSet::new();
        let mut intra_batch_delete_duplicates = Vec::new();
        for delete_file in &self.added_delete_files {
            if !seen_delete_files.insert(delete_file.file_path.as_str()) {
                intra_batch_delete_duplicates.push(delete_file.file_path.clone());
            }
        }
        if !intra_batch_delete_duplicates.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot add duplicate delete files in the same batch: {}",
                    intra_batch_delete_duplicates.join(", ")
                ),
            ));
        }

        let new_data_files = seen_data_files;
        let new_delete_files = seen_delete_files;

        // Check for cross-type duplicates: same path cannot appear in both data and delete files
        let cross_type_duplicates: Vec<_> = new_data_files
            .intersection(&new_delete_files)
            .map(|s| s.to_string())
            .collect();
        if !cross_type_duplicates.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot add the same file path as both a data file and a delete file: {}",
                    cross_type_duplicates.join(", ")
                ),
            ));
        }

        let mut duplicate_data_files = Vec::new();
        let mut duplicate_delete_files = Vec::new();

        if let Some(current_snapshot) = self.table.metadata().current_snapshot() {
            let manifest_list = current_snapshot
                .load_manifest_list(self.table.file_io(), &self.table.metadata_ref())
                .await?;
            for manifest_list_entry in manifest_list.entries() {
                let manifest = manifest_list_entry
                    .load_manifest(self.table.file_io())
                    .await?;
                for entry in manifest.entries() {
                    let file_path = entry.file_path();
                    if entry.is_alive() {
                        if new_data_files.contains(file_path) {
                            duplicate_data_files.push(file_path.to_string());
                        }
                        if new_delete_files.contains(file_path) {
                            duplicate_delete_files.push(file_path.to_string());
                        }
                    }
                }
            }
        }

        if !duplicate_data_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot add data files that are already referenced by table, files: {}",
                    duplicate_data_files.join(", ")
                ),
            ));
        }

        if !duplicate_delete_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot add delete files that are already referenced by table, files: {}",
                    duplicate_delete_files.join(", ")
                ),
            ));
        }

        Ok(())
    }

    fn generate_unique_snapshot_id(table: &Table) -> i64 {
        let generate_random_id = || -> i64 {
            let (lhs, rhs) = Uuid::new_v4().as_u64_pair();
            let snapshot_id = (lhs ^ rhs) as i64;
            if snapshot_id < 0 {
                -snapshot_id
            } else {
                snapshot_id
            }
        };
        let mut snapshot_id = generate_random_id();

        while table
            .metadata()
            .snapshots()
            .any(|s| s.snapshot_id() == snapshot_id)
        {
            snapshot_id = generate_random_id();
        }
        snapshot_id
    }

    fn new_manifest_writer(&mut self, content: ManifestContentType) -> Result<ManifestWriter> {
        let new_manifest_path = format!(
            "{}/{}/{}-m{}.{}",
            self.table.metadata().location(),
            META_ROOT_PATH,
            self.commit_uuid,
            self.manifest_counter.next().unwrap(),
            DataFileFormat::Avro
        );
        let output_file = self.table.file_io().new_output(new_manifest_path)?;
        let builder = ManifestWriterBuilder::new(
            output_file,
            Some(self.snapshot_id),
            self.key_metadata.clone(),
            self.table.metadata().current_schema().clone(),
            self.table
                .metadata()
                .default_partition_spec()
                .as_ref()
                .clone(),
        );
        match self.table.metadata().format_version() {
            FormatVersion::V1 => Ok(builder.build_v1()),
            FormatVersion::V2 => match content {
                ManifestContentType::Data => Ok(builder.build_v2_data()),
                ManifestContentType::Deletes => Ok(builder.build_v2_deletes()),
            },
            FormatVersion::V3 => match content {
                ManifestContentType::Data => Ok(builder.build_v3_data()),
                ManifestContentType::Deletes => Ok(builder.build_v3_deletes()),
            },
        }
    }

    // Check if the partition value is compatible with the partition type.
    fn validate_partition_value(
        partition_value: &Struct,
        partition_type: &StructType,
    ) -> Result<()> {
        if partition_value.fields().len() != partition_type.fields().len() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Partition value is not compatible with partition type",
            ));
        }

        for (value, field) in partition_value.fields().iter().zip(partition_type.fields()) {
            let field = field.field_type.as_primitive_type().ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Partition field should only be primitive type.",
                )
            })?;
            if let Some(value) = value
                && !field.compatible(&value.as_primitive_literal().unwrap())
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Partition value is not compatible partition type",
                ));
            }
        }
        Ok(())
    }

    // Write manifest file for added data files and return the ManifestFile for ManifestList.
    async fn write_added_manifest(&mut self) -> Result<ManifestFile> {
        let added_data_files = std::mem::take(&mut self.added_data_files);
        if added_data_files.is_empty() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "No added data files found when write an added manifest file",
            ));
        }

        let snapshot_id = self.snapshot_id;
        let format_version = self.table.metadata().format_version();
        let manifest_entries = added_data_files.into_iter().map(|data_file| {
            let builder = ManifestEntry::builder()
                .status(crate::spec::ManifestStatus::Added)
                .data_file(data_file);
            if format_version == FormatVersion::V1 {
                builder.snapshot_id(snapshot_id).build()
            } else {
                // For format version > 1, we set the snapshot id at the inherited time to avoid rewrite the manifest file when
                // commit failed.
                builder.build()
            }
        });
        let mut writer = self.new_manifest_writer(ManifestContentType::Data)?;
        for entry in manifest_entries {
            writer.add_entry(entry)?;
        }
        writer.write_manifest_file().await
    }

    async fn write_delete_manifest(&mut self) -> Result<ManifestFile> {
        let added_delete_files = std::mem::take(&mut self.added_delete_files);
        if added_delete_files.is_empty() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "No added delete files found when write a delete manifest file",
            ));
        }

        let manifest_entries = added_delete_files.into_iter().map(|delete_file| {
            ManifestEntry::builder()
                .status(crate::spec::ManifestStatus::Added)
                .data_file(delete_file)
                .build()
        });
        let mut writer = self.new_manifest_writer(ManifestContentType::Deletes)?;
        for entry in manifest_entries {
            writer.add_entry(entry)?;
        }
        writer.write_manifest_file().await
    }

    async fn manifest_file<OP: SnapshotProduceOperation, MP: ManifestProcess>(
        &mut self,
        snapshot_produce_operation: &OP,
        manifest_process: &MP,
    ) -> Result<Vec<ManifestFile>> {
        let has_data_files = !self.added_data_files.is_empty();
        let has_delete_files = !self.added_delete_files.is_empty();
        let has_properties = !self.snapshot_properties.is_empty();

        if !has_data_files && !has_delete_files && !has_properties {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "No added data files, delete files, or snapshot properties found when write a manifest file",
            ));
        }

        let existing_manifests = snapshot_produce_operation.existing_manifest(self).await?;
        let mut manifest_files = existing_manifests;

        if has_data_files {
            let added_manifest = self.write_added_manifest().await?;
            manifest_files.push(added_manifest);
        }

        if has_delete_files {
            let delete_manifest = self.write_delete_manifest().await?;
            manifest_files.push(delete_manifest);
        }

        let manifest_files = manifest_process.process_manifests(self, manifest_files);
        Ok(manifest_files)
    }

    // Returns a `Summary` of the current snapshot
    fn summary<OP: SnapshotProduceOperation>(
        &self,
        snapshot_produce_operation: &OP,
    ) -> Result<Summary> {
        let mut summary_collector = SnapshotSummaryCollector::default();
        let table_metadata = self.table.metadata_ref();

        let partition_summary_limit = if let Some(limit) = table_metadata
            .properties()
            .get(TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT)
        {
            if let Ok(limit) = limit.parse::<u64>() {
                limit
            } else {
                TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT
            }
        } else {
            TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT
        };

        summary_collector.set_partition_summary_limit(partition_summary_limit);

        for data_file in &self.added_data_files {
            summary_collector.add_file(
                data_file,
                table_metadata.current_schema().clone(),
                table_metadata.default_partition_spec().clone(),
            );
        }

        for delete_file in &self.added_delete_files {
            summary_collector.add_file(
                delete_file,
                table_metadata.current_schema().clone(),
                table_metadata.default_partition_spec().clone(),
            );
        }

        let previous_snapshot = table_metadata
            .snapshot_by_id(self.snapshot_id)
            .and_then(|snapshot| snapshot.parent_snapshot_id())
            .and_then(|parent_id| table_metadata.snapshot_by_id(parent_id));

        let mut additional_properties = summary_collector.build();
        additional_properties.extend(self.snapshot_properties.clone());

        let summary = Summary {
            operation: snapshot_produce_operation.operation(),
            additional_properties,
        };

        update_snapshot_summaries(
            summary,
            previous_snapshot.map(|s| s.summary()),
            snapshot_produce_operation.operation() == Operation::Overwrite,
        )
    }

    fn generate_manifest_list_file_path(&self, attempt: i64) -> String {
        format!(
            "{}/{}/snap-{}-{}-{}.{}",
            self.table.metadata().location(),
            META_ROOT_PATH,
            self.snapshot_id,
            attempt,
            self.commit_uuid,
            DataFileFormat::Avro
        )
    }

    /// Finished building the action and return the [`ActionCommit`] to the transaction.
    pub(crate) async fn commit<OP: SnapshotProduceOperation, MP: ManifestProcess>(
        mut self,
        snapshot_produce_operation: OP,
        process: MP,
    ) -> Result<ActionCommit> {
        let manifest_list_path = self.generate_manifest_list_file_path(0);
        let next_seq_num = self.table.metadata().next_sequence_number();
        let first_row_id = self.table.metadata().next_row_id();
        let mut manifest_list_writer = match self.table.metadata().format_version() {
            FormatVersion::V1 => ManifestListWriter::v1(
                self.table
                    .file_io()
                    .new_output(manifest_list_path.clone())?,
                self.snapshot_id,
                self.table.metadata().current_snapshot_id(),
            ),
            FormatVersion::V2 => ManifestListWriter::v2(
                self.table
                    .file_io()
                    .new_output(manifest_list_path.clone())?,
                self.snapshot_id,
                self.table.metadata().current_snapshot_id(),
                next_seq_num,
            ),
            FormatVersion::V3 => ManifestListWriter::v3(
                self.table
                    .file_io()
                    .new_output(manifest_list_path.clone())?,
                self.snapshot_id,
                self.table.metadata().current_snapshot_id(),
                next_seq_num,
                Some(first_row_id),
            ),
        };

        // Calling self.summary() before self.manifest_file() is important because self.added_data_files
        // will be set to an empty vec after self.manifest_file() returns, resulting in an empty summary
        // being generated.
        let summary = self.summary(&snapshot_produce_operation).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "Failed to create snapshot summary.").with_source(err)
        })?;

        let new_manifests = self
            .manifest_file(&snapshot_produce_operation, &process)
            .await?;

        manifest_list_writer.add_manifests(new_manifests.into_iter())?;
        let writer_next_row_id = manifest_list_writer.next_row_id();
        manifest_list_writer.close().await?;

        let commit_ts = chrono::Utc::now().timestamp_millis();
        let new_snapshot = Snapshot::builder()
            .with_manifest_list(manifest_list_path)
            .with_snapshot_id(self.snapshot_id)
            .with_parent_snapshot_id(self.table.metadata().current_snapshot_id())
            .with_sequence_number(next_seq_num)
            .with_summary(summary)
            .with_schema_id(self.table.metadata().current_schema_id())
            .with_timestamp_ms(commit_ts);

        let new_snapshot = if let Some(writer_next_row_id) = writer_next_row_id {
            let assigned_rows = writer_next_row_id - self.table.metadata().next_row_id();
            new_snapshot
                .with_row_range(first_row_id, assigned_rows)
                .build()
        } else {
            new_snapshot.build()
        };

        let updates = vec![
            TableUpdate::AddSnapshot {
                snapshot: new_snapshot,
            },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: SnapshotReference::new(
                    self.snapshot_id,
                    SnapshotRetention::branch(None, None, None),
                ),
            },
        ];

        let requirements = vec![
            TableRequirement::UuidMatch {
                uuid: self.table.metadata().uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: self.table.metadata().current_snapshot_id(),
            },
        ];

        Ok(ActionCommit::new(updates, requirements))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::spec::{DataContentType, DataFileBuilder, DataFileFormat, Literal, Struct};
    use crate::transaction::tests::{make_v1_table, make_v2_minimal_table};

    fn make_position_delete_file(spec_id: i32) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/delete-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(spec_id)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap()
    }

    fn make_equality_delete_file_with_ids(
        spec_id: i32,
        equality_ids: Option<Vec<i32>>,
    ) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path("test/eq-delete-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(spec_id)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .equality_ids(equality_ids)
            .build()
            .unwrap()
    }

    fn make_data_file_as_delete(spec_id: i32) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(spec_id)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap()
    }

    #[test]
    fn test_validate_delete_files_rejected_in_v1() {
        let table = make_v1_table();
        let spec_id = table.metadata().default_partition_spec_id();
        let delete_file = make_position_delete_file(spec_id);

        let producer =
            SnapshotProducer::new(&table, Uuid::now_v7(), None, HashMap::new(), vec![], vec![
                delete_file,
            ]);

        let result = producer.validate_added_delete_files();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.message()
                .contains("Delete files are not supported in format version 1")
        );
    }

    #[test]
    fn test_validate_equality_delete_requires_equality_ids() {
        let table = make_v2_minimal_table();
        let spec_id = table.metadata().default_partition_spec_id();
        let delete_file = make_equality_delete_file_with_ids(spec_id, None);

        let producer =
            SnapshotProducer::new(&table, Uuid::now_v7(), None, HashMap::new(), vec![], vec![
                delete_file,
            ]);

        let result = producer.validate_added_delete_files();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.message()
                .contains("Equality delete files must have equality_ids set")
        );
    }

    #[test]
    fn test_validate_equality_delete_rejects_empty_equality_ids() {
        let table = make_v2_minimal_table();
        let spec_id = table.metadata().default_partition_spec_id();
        let delete_file = make_equality_delete_file_with_ids(spec_id, Some(vec![]));

        let producer =
            SnapshotProducer::new(&table, Uuid::now_v7(), None, HashMap::new(), vec![], vec![
                delete_file,
            ]);

        let result = producer.validate_added_delete_files();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.message()
                .contains("Equality delete files must have equality_ids set")
        );
    }

    #[test]
    fn test_validate_delete_files_rejects_data_content_type() {
        let table = make_v2_minimal_table();
        let spec_id = table.metadata().default_partition_spec_id();
        let data_file = make_data_file_as_delete(spec_id);

        let producer =
            SnapshotProducer::new(&table, Uuid::now_v7(), None, HashMap::new(), vec![], vec![
                data_file,
            ]);

        let result = producer.validate_added_delete_files();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.message()
                .contains("Data content type is not allowed for delete files")
        );
    }

    #[test]
    fn test_validate_position_delete_file_succeeds() {
        let table = make_v2_minimal_table();
        let spec_id = table.metadata().default_partition_spec_id();
        let delete_file = make_position_delete_file(spec_id);

        let producer =
            SnapshotProducer::new(&table, Uuid::now_v7(), None, HashMap::new(), vec![], vec![
                delete_file,
            ]);

        let result = producer.validate_added_delete_files();
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_equality_delete_file_with_ids_succeeds() {
        let table = make_v2_minimal_table();
        let spec_id = table.metadata().default_partition_spec_id();
        let delete_file = make_equality_delete_file_with_ids(spec_id, Some(vec![1]));

        let producer =
            SnapshotProducer::new(&table, Uuid::now_v7(), None, HashMap::new(), vec![], vec![
                delete_file,
            ]);

        let result = producer.validate_added_delete_files();
        assert!(result.is_ok());
    }

    #[test]
    fn test_write_delete_manifest_precondition_empty_files() {
        let table = make_v2_minimal_table();

        let mut producer =
            SnapshotProducer::new(&table, Uuid::now_v7(), None, HashMap::new(), vec![], vec![]);

        let result = futures::executor::block_on(producer.write_delete_manifest());
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.message()
                .contains("No added delete files found when write a delete manifest file")
        );
    }

    fn make_data_file_with_path(spec_id: i32, path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(spec_id)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap()
    }

    fn make_position_delete_file_with_path(spec_id: i32, path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(spec_id)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap()
    }

    #[test]
    fn test_validate_cross_type_duplicate_files_rejected() {
        let table = make_v2_minimal_table();
        let spec_id = table.metadata().default_partition_spec_id();
        let shared_path = "test/shared-file.parquet";
        let data_file = make_data_file_with_path(spec_id, shared_path);
        let delete_file = make_position_delete_file_with_path(spec_id, shared_path);

        let producer = SnapshotProducer::new(
            &table,
            Uuid::now_v7(),
            None,
            HashMap::new(),
            vec![data_file],
            vec![delete_file],
        );

        let result = futures::executor::block_on(producer.validate_duplicate_files());
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.message()
                .contains("Cannot add the same file path as both a data file and a delete file")
        );
    }

    #[test]
    fn test_validate_position_delete_rejects_equality_ids() {
        let table = make_v2_minimal_table();
        let spec_id = table.metadata().default_partition_spec_id();
        let delete_file = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/pos-delete-with-eq-ids.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(spec_id)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .equality_ids(Some(vec![1, 2]))
            .build()
            .unwrap();

        let producer =
            SnapshotProducer::new(&table, Uuid::now_v7(), None, HashMap::new(), vec![], vec![
                delete_file,
            ]);

        let result = producer.validate_added_delete_files();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.message()
                .contains("Position delete files must not have equality_ids set")
        );
    }
}
