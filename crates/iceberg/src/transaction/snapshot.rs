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

use std::collections::HashMap;
use std::future::Future;
use std::ops::RangeFrom;

use uuid::Uuid;

use crate::error::Result;
use crate::io::OutputFile;
use crate::spec::{
    DataFile, DataFileFormat, FormatVersion, ManifestEntry, ManifestFile, ManifestListWriter,
    ManifestWriterBuilder, Operation, Snapshot, SnapshotReference, SnapshotRetention, Struct,
    StructType, Summary, MAIN_BRANCH,
};
use crate::transaction::Transaction;
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

const META_ROOT_PATH: &str = "metadata";

pub(crate) trait SnapshotProduceOperation: Send + Sync {
    fn operation(&self) -> Operation;
    #[allow(unused)]
    fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProduceAction,
    ) -> impl Future<Output = Result<Vec<ManifestEntry>>> + Send;
    fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProduceAction,
    ) -> impl Future<Output = Result<Vec<ManifestFile>>> + Send;
}

pub(crate) struct DefaultManifestProcess;

impl ManifestProcess for DefaultManifestProcess {
    fn process_manifeset(&self, manifests: Vec<ManifestFile>) -> Vec<ManifestFile> {
        manifests
    }
}

pub(crate) trait ManifestProcess: Send + Sync {
    fn process_manifeset(&self, manifests: Vec<ManifestFile>) -> Vec<ManifestFile>;
}

pub(crate) struct SnapshotProduceAction<'a> {
    pub tx: Transaction<'a>,
    snapshot_id: i64,
    key_metadata: Vec<u8>,
    commit_uuid: Uuid,
    snapshot_properties: HashMap<String, String>,
    pub added_data_files: Vec<DataFile>,
    // A counter used to generate unique manifest file names.
    // It starts from 0 and increments for each new manifest file.
    // Note: This counter is limited to the range of (0..u64::MAX).
    manifest_counter: RangeFrom<u64>,
}

impl<'a> SnapshotProduceAction<'a> {
    pub(crate) fn new(
        tx: Transaction<'a>,
        snapshot_id: i64,
        key_metadata: Vec<u8>,
        commit_uuid: Uuid,
        snapshot_properties: HashMap<String, String>,
    ) -> Result<Self> {
        Ok(Self {
            tx,
            snapshot_id,
            commit_uuid,
            snapshot_properties,
            added_data_files: vec![],
            manifest_counter: (0..),
            key_metadata,
        })
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
            if !field
                .field_type
                .as_primitive_type()
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "Partition field should only be primitive type.",
                    )
                })?
                .compatible(&value.as_primitive_literal().unwrap())
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Partition value is not compatible partition type",
                ));
            }
        }
        Ok(())
    }

    /// Add data files to the snapshot.
    pub fn add_data_files(
        &mut self,
        data_files: impl IntoIterator<Item = DataFile>,
    ) -> Result<&mut Self> {
        let data_files: Vec<DataFile> = data_files.into_iter().collect();
        for data_file in &data_files {
            if data_file.content_type() != crate::spec::DataContentType::Data {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Only data content type is allowed for fast append",
                ));
            }
            // Check if the data file partition spec id matches the table default partition spec id.
            if self.tx.table.metadata().default_partition_spec_id() != data_file.partition_spec_id {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Data file partition spec id does not match table default partition spec id",
                ));
            }
            Self::validate_partition_value(
                data_file.partition(),
                self.tx.table.metadata().default_partition_type(),
            )?;
        }
        self.added_data_files.extend(data_files);
        Ok(self)
    }

    fn new_manifest_output(&mut self) -> Result<OutputFile> {
        let new_manifest_path = format!(
            "{}/{}/{}-m{}.{}",
            self.tx.table.metadata().location(),
            META_ROOT_PATH,
            self.commit_uuid,
            self.manifest_counter.next().unwrap(),
            DataFileFormat::Avro
        );
        self.tx.table.file_io().new_output(new_manifest_path)
    }

    // Write manifest file for added data files and return the ManifestFile for ManifestList.
    async fn write_added_manifest(&mut self) -> Result<ManifestFile> {
        let added_data_files = std::mem::take(&mut self.added_data_files);
        let snapshot_id = self.snapshot_id;
        let manifest_entries = added_data_files.into_iter().map(|data_file| {
            let builder = ManifestEntry::builder()
                .status(crate::spec::ManifestStatus::Added)
                .data_file(data_file);
            if self.tx.table.metadata().format_version() == FormatVersion::V1 {
                builder.snapshot_id(snapshot_id).build()
            } else {
                // For format version > 1, we set the snapshot id at the inherited time to avoid rewrite the manifest file when
                // commit failed.
                builder.build()
            }
        });
        let mut writer = {
            let builder = ManifestWriterBuilder::new(
                self.new_manifest_output()?,
                Some(self.snapshot_id),
                self.key_metadata.clone(),
                self.tx.table.metadata().current_schema().clone(),
                self.tx
                    .table
                    .metadata()
                    .default_partition_spec()
                    .as_ref()
                    .clone(),
            );
            if self.tx.table.metadata().format_version() == FormatVersion::V1 {
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
        snapshot_produce_operation: &OP,
        manifest_process: &MP,
    ) -> Result<Vec<ManifestFile>> {
        let added_manifest = self.write_added_manifest().await?;
        let existing_manifests = snapshot_produce_operation.existing_manifest(self).await?;
        // # TODO
        // Support process delete entries.

        let mut manifest_files = vec![added_manifest];
        manifest_files.extend(existing_manifests);
        let manifest_files = manifest_process.process_manifeset(manifest_files);
        Ok(manifest_files)
    }

    // # TODO
    // Fulfill this function
    fn summary<OP: SnapshotProduceOperation>(&self, snapshot_produce_operation: &OP) -> Summary {
        Summary {
            operation: snapshot_produce_operation.operation(),
            additional_properties: self.snapshot_properties.clone(),
        }
    }

    fn generate_manifest_list_file_path(&self, attempt: i64) -> String {
        format!(
            "{}/{}/snap-{}-{}-{}.{}",
            self.tx.table.metadata().location(),
            META_ROOT_PATH,
            self.snapshot_id,
            attempt,
            self.commit_uuid,
            DataFileFormat::Avro
        )
    }

    /// Finished building the action and apply it to the transaction.
    pub async fn apply<OP: SnapshotProduceOperation, MP: ManifestProcess>(
        mut self,
        snapshot_produce_operation: OP,
        process: MP,
    ) -> Result<Transaction<'a>> {
        let new_manifests = self
            .manifest_file(&snapshot_produce_operation, &process)
            .await?;
        let next_seq_num = self.tx.table.metadata().next_sequence_number();

        let summary = self.summary(&snapshot_produce_operation);

        let manifest_list_path = self.generate_manifest_list_file_path(0);

        let mut manifest_list_writer = match self.tx.table.metadata().format_version() {
            FormatVersion::V1 => ManifestListWriter::v1(
                self.tx
                    .table
                    .file_io()
                    .new_output(manifest_list_path.clone())?,
                self.snapshot_id,
                self.tx.table.metadata().current_snapshot_id(),
            ),
            FormatVersion::V2 => ManifestListWriter::v2(
                self.tx
                    .table
                    .file_io()
                    .new_output(manifest_list_path.clone())?,
                self.snapshot_id,
                self.tx.table.metadata().current_snapshot_id(),
                next_seq_num,
            ),
        };
        manifest_list_writer.add_manifests(new_manifests.into_iter())?;
        manifest_list_writer.close().await?;

        let commit_ts = chrono::Utc::now().timestamp_millis();
        let new_snapshot = Snapshot::builder()
            .with_manifest_list(manifest_list_path)
            .with_snapshot_id(self.snapshot_id)
            .with_parent_snapshot_id(self.tx.table.metadata().current_snapshot_id())
            .with_sequence_number(next_seq_num)
            .with_summary(summary)
            .with_schema_id(self.tx.table.metadata().current_schema_id())
            .with_timestamp_ms(commit_ts)
            .build();

        self.tx.append_updates(vec![
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
        ])?;
        self.tx.append_requirements(vec![
            TableRequirement::UuidMatch {
                uuid: self.tx.table.metadata().uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: self.tx.table.metadata().current_snapshot_id(),
            },
        ])?;
        Ok(self.tx)
    }
}
