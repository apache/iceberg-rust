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
use crate::io::OutputFile;
use crate::spec::{
    DataFile, DataFileFormat, FormatVersion, MAIN_BRANCH, ManifestEntry, ManifestFile,
    ManifestListWriter, ManifestWriterBuilder, Operation, PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT,
    PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT, Snapshot, SnapshotReference, SnapshotRetention,
    SnapshotSummaryCollector, Struct, StructType, Summary, update_snapshot_summaries,
};
use crate::table::Table;
use crate::transaction::ActionCommit;
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

const META_ROOT_PATH: &str = "metadata";

pub(crate) trait SnapshotProduceOperation: Send + Sync {
    fn operation(&self) -> Operation;
    #[allow(unused)]
    fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProducer,
    ) -> impl Future<Output = Result<Vec<ManifestEntry>>> + Send;
    fn existing_manifest(
        &self,
        table: &Table,
    ) -> impl Future<Output = Result<Vec<ManifestFile>>> + Send;
}

pub(crate) struct DefaultManifestProcess;

impl ManifestProcess for DefaultManifestProcess {
    fn process_manifests(&self, manifests: Vec<ManifestFile>) -> Vec<ManifestFile> {
        manifests
    }
}

pub(crate) trait ManifestProcess: Send + Sync {
    fn process_manifests(&self, manifests: Vec<ManifestFile>) -> Vec<ManifestFile>;
}

pub(crate) struct SnapshotProducer {
    snapshot_id: i64,
    commit_uuid: Uuid,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    // A counter used to generate unique manifest file names.
    // It starts from 0 and increments for each new manifest file.
    // Note: This counter is limited to the range of (0..u64::MAX).
    manifest_counter: RangeFrom<u64>,
}

impl SnapshotProducer {
    pub(crate) fn new(
        table: &Table,
        commit_uuid: Uuid,
        key_metadata: Option<Vec<u8>>,
        snapshot_properties: HashMap<String, String>,
        added_data_files: Vec<DataFile>,
    ) -> Self {
        Self {
            snapshot_id: Self::generate_unique_snapshot_id(table),
            commit_uuid,
            key_metadata,
            snapshot_properties,
            added_data_files,
            manifest_counter: (0..),
        }
    }

    pub(crate) fn validate_added_data_files(
        table: &Table,
        added_data_files: &[DataFile],
    ) -> Result<()> {
        for data_file in added_data_files {
            if data_file.content_type() != crate::spec::DataContentType::Data {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Only data content type is allowed for fast append",
                ));
            }
            // Check if the data file partition spec id matches the table default partition spec id.
            if table.metadata().default_partition_spec_id() != data_file.partition_spec_id {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Data file partition spec id does not match table default partition spec id",
                ));
            }
            Self::validate_partition_value(
                data_file.partition(),
                table.metadata().default_partition_type(),
            )?;
        }

        Ok(())
    }

    pub(crate) async fn validate_duplicate_files(
        table: &Table,
        added_data_files: &[DataFile],
    ) -> Result<()> {
        let new_files: HashSet<&str> = added_data_files
            .iter()
            .map(|df| df.file_path.as_str())
            .collect();

        let mut referenced_files = Vec::new();
        if let Some(current_snapshot) = table.metadata().current_snapshot() {
            let manifest_list = current_snapshot
                .load_manifest_list(table.file_io(), &table.metadata_ref())
                .await?;
            for manifest_list_entry in manifest_list.entries() {
                let manifest = manifest_list_entry.load_manifest(table.file_io()).await?;
                for entry in manifest.entries() {
                    let file_path = entry.file_path();
                    if new_files.contains(file_path) && entry.is_alive() {
                        referenced_files.push(file_path.to_string());
                    }
                }
            }
        }

        if !referenced_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot add files that are already referenced by table, files: {}",
                    referenced_files.join(", ")
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

    fn new_manifest_output(&mut self, table: &Table) -> Result<OutputFile> {
        let new_manifest_path = format!(
            "{}/{}/{}-m{}.{}",
            table.metadata().location(),
            META_ROOT_PATH,
            self.commit_uuid,
            self.manifest_counter.next().unwrap(),
            DataFileFormat::Avro
        );
        table.file_io().new_output(new_manifest_path)
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
            if let Some(value) = value {
                if !field.compatible(&value.as_primitive_literal().unwrap()) {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Partition value is not compatible partition type",
                    ));
                }
            }
        }
        Ok(())
    }

    // Write manifest file for added data files and return the ManifestFile for ManifestList.
    async fn write_added_manifest(&mut self, table: &Table) -> Result<ManifestFile> {
        let added_data_files = std::mem::take(&mut self.added_data_files);
        if added_data_files.is_empty() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "No added data files found when write a manifest file",
            ));
        }

        let snapshot_id = self.snapshot_id;
        let format_version = table.metadata().format_version();
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
        let mut writer = {
            let builder = ManifestWriterBuilder::new(
                self.new_manifest_output(table)?,
                Some(self.snapshot_id),
                self.key_metadata.clone(),
                table.metadata().current_schema().clone(),
                table.metadata().default_partition_spec().as_ref().clone(),
            );
            if table.metadata().format_version() == FormatVersion::V1 {
                builder.build_v1()
            } else {
                builder.build_v2_data()
            }
        };
        for entry in manifest_entries {
            writer.add_entry(entry)?;
        }
        writer.write_manifest_file().await
    }

    async fn manifest_file<OP: SnapshotProduceOperation, MP: ManifestProcess>(
        &mut self,
        table: &Table,
        snapshot_produce_operation: &OP,
        manifest_process: &MP,
    ) -> Result<Vec<ManifestFile>> {
        let added_manifest = self.write_added_manifest(table).await?;
        let existing_manifests = snapshot_produce_operation.existing_manifest(table).await?;
        // # TODO
        // Support process delete entries.

        let mut manifest_files = vec![added_manifest];
        manifest_files.extend(existing_manifests);
        let manifest_files = manifest_process.process_manifests(manifest_files);
        Ok(manifest_files)
    }

    // Returns a `Summary` of the current snapshot
    fn summary<OP: SnapshotProduceOperation>(
        &self,
        table: &Table,
        snapshot_produce_operation: &OP,
    ) -> Result<Summary> {
        let mut summary_collector = SnapshotSummaryCollector::default();
        let table_metadata = table.metadata_ref();

        let partition_summary_limit = if let Some(limit) = table_metadata
            .properties()
            .get(PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT)
        {
            if let Ok(limit) = limit.parse::<u64>() {
                limit
            } else {
                PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT
            }
        } else {
            PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT
        };

        summary_collector.set_partition_summary_limit(partition_summary_limit);

        for data_file in &self.added_data_files {
            summary_collector.add_file(
                data_file,
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

    fn generate_manifest_list_file_path(&self, table: &Table, attempt: i64) -> String {
        format!(
            "{}/{}/snap-{}-{}-{}.{}",
            table.metadata().location(),
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
        table: &Table,
        snapshot_produce_operation: OP,
        process: MP,
    ) -> Result<ActionCommit> {
        let new_manifests = self
            .manifest_file(table, &snapshot_produce_operation, &process)
            .await?;
        let next_seq_num = table.metadata().next_sequence_number();

        let summary = self
            .summary(table, &snapshot_produce_operation)
            .map_err(|err| {
                Error::new(ErrorKind::Unexpected, "Failed to create snapshot summary.")
                    .with_source(err)
            })?;

        let manifest_list_path = self.generate_manifest_list_file_path(table, 0);

        let mut manifest_list_writer = match table.metadata().format_version() {
            FormatVersion::V1 => ManifestListWriter::v1(
                table.file_io().new_output(manifest_list_path.clone())?,
                self.snapshot_id,
                table.metadata().current_snapshot_id(),
            ),
            FormatVersion::V2 => ManifestListWriter::v2(
                table.file_io().new_output(manifest_list_path.clone())?,
                self.snapshot_id,
                table.metadata().current_snapshot_id(),
                next_seq_num,
            ),
        };
        manifest_list_writer.add_manifests(new_manifests.into_iter())?;
        manifest_list_writer.close().await?;

        let commit_ts = chrono::Utc::now().timestamp_millis();
        let new_snapshot = Snapshot::builder()
            .with_manifest_list(manifest_list_path)
            .with_snapshot_id(self.snapshot_id)
            .with_parent_snapshot_id(table.metadata().current_snapshot_id())
            .with_sequence_number(next_seq_num)
            .with_summary(summary)
            .with_schema_id(table.metadata().current_schema_id())
            .with_timestamp_ms(commit_ts)
            .build();

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
                uuid: table.metadata().uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: table.metadata().current_snapshot_id(),
            },
        ];

        Ok(ActionCommit::new(updates, requirements))
    }
}
