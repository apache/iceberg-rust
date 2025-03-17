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

use std::cmp::min;

use apache_avro::{to_value, Writer as AvroWriter};
use bytes::Bytes;
use itertools::Itertools;
use serde_json::to_vec;

use super::_serde::{manifest_schema_v1, manifest_schema_v2, ManifestEntryV1, ManifestEntryV2};
use super::{Datum, FormatVersion, ManifestContentType,
    PartitionSpec, PrimitiveType, UNASSIGNED_SEQUENCE_NUMBER,
};
use crate::error::Result;
use crate::io::OutputFile;
use crate::spec::manifest::data_file::{DataContentType, DataFile};
use crate::spec::manifest::entry::ManifestStatus;
use crate::spec::manifest::{ManifestEntry, ManifestMetadata};
use crate::spec::{
    FieldSummary, ManifestFile, PrimitiveLiteral, SchemaRef, StructType, UNASSIGNED_SNAPSHOT_ID,
};
use crate::{Error, ErrorKind};

/// A manifest writer.
pub struct ManifestWriter {
    output: OutputFile,

    snapshot_id: Option<i64>,

    added_files: u32,
    added_rows: u64,
    existing_files: u32,
    existing_rows: u64,
    deleted_files: u32,
    deleted_rows: u64,

    min_seq_num: Option<i64>,

    key_metadata: Vec<u8>,

    manifest_entries: Vec<ManifestEntry>,

    metadata: ManifestMetadata,
}

/// The builder used to create a [`ManifestWriter`].
pub struct ManifestWriterBuilder {
    output: OutputFile,
    snapshot_id: Option<i64>,
    key_metadata: Vec<u8>,
    schema: SchemaRef,
    partition_spec: PartitionSpec,
}

impl ManifestWriterBuilder {
    /// Create a new builder.
    pub fn new(
        output: OutputFile,
        snapshot_id: Option<i64>,
        key_metadata: Vec<u8>,
        schema: SchemaRef,
        partition_spec: PartitionSpec,
    ) -> Self {
        Self {
            output,
            snapshot_id,
            key_metadata,
            schema,
            partition_spec,
        }
    }

    /// Build a [`ManifestWriter`] for format version 1.
    pub fn build_v1(self) -> ManifestWriter {
        let metadata = ManifestMetadata::builder()
            .schema_id(self.schema.schema_id())
            .schema(self.schema)
            .partition_spec(self.partition_spec)
            .format_version(FormatVersion::V1)
            .content(ManifestContentType::Data)
            .build();
        ManifestWriter::new(self.output, self.snapshot_id, self.key_metadata, metadata)
    }

    /// Build a [`ManifestWriter`] for format version 2, data content.
    pub fn build_v2_data(self) -> ManifestWriter {
        let metadata = ManifestMetadata::builder()
            .schema_id(self.schema.schema_id())
            .schema(self.schema)
            .partition_spec(self.partition_spec)
            .format_version(FormatVersion::V2)
            .content(ManifestContentType::Data)
            .build();
        ManifestWriter::new(self.output, self.snapshot_id, self.key_metadata, metadata)
    }

    /// Build a [`ManifestWriter`] for format version 2, deletes content.
    pub fn build_v2_deletes(self) -> ManifestWriter {
        let metadata = ManifestMetadata::builder()
            .schema_id(self.schema.schema_id())
            .schema(self.schema)
            .partition_spec(self.partition_spec)
            .format_version(FormatVersion::V2)
            .content(ManifestContentType::Deletes)
            .build();
        ManifestWriter::new(self.output, self.snapshot_id, self.key_metadata, metadata)
    }
}

struct PartitionFieldStats {
    partition_type: PrimitiveType,
    summary: FieldSummary,
}

impl PartitionFieldStats {
    pub(crate) fn new(partition_type: PrimitiveType) -> Self {
        Self {
            partition_type,
            summary: FieldSummary::default(),
        }
    }

    pub(crate) fn update(&mut self, value: Option<PrimitiveLiteral>) -> Result<()> {
        let Some(value) = value else {
            self.summary.contains_null = true;
            return Ok(());
        };
        if !self.partition_type.compatible(&value) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "value is not compatitable with type",
            ));
        }
        let value = Datum::new(self.partition_type.clone(), value);

        if value.is_nan() {
            self.summary.contains_nan = Some(true);
            return Ok(());
        }

        self.summary.lower_bound = Some(self.summary.lower_bound.take().map_or(
            value.clone(),
            |original| {
                if value < original {
                    value.clone()
                } else {
                    original
                }
            },
        ));
        self.summary.upper_bound = Some(self.summary.upper_bound.take().map_or(
            value.clone(),
            |original| {
                if value > original {
                    value
                } else {
                    original
                }
            },
        ));

        Ok(())
    }

    pub(crate) fn finish(mut self) -> FieldSummary {
        // Always set contains_nan
        self.summary.contains_nan = self.summary.contains_nan.or(Some(false));
        self.summary
    }
}

impl ManifestWriter {
    /// Create a new manifest writer.
    pub(crate) fn new(
        output: OutputFile,
        snapshot_id: Option<i64>,
        key_metadata: Vec<u8>,
        metadata: ManifestMetadata,
    ) -> Self {
        Self {
            output,
            snapshot_id,
            added_files: 0,
            added_rows: 0,
            existing_files: 0,
            existing_rows: 0,
            deleted_files: 0,
            deleted_rows: 0,
            min_seq_num: None,
            key_metadata,
            manifest_entries: Vec::new(),
            metadata,
        }
    }

    fn construct_partition_summaries(
        &mut self,
        partition_type: &StructType,
    ) -> Result<Vec<FieldSummary>> {
        let mut field_stats: Vec<_> = partition_type
            .fields()
            .iter()
            .map(|f| PartitionFieldStats::new(f.field_type.as_primitive_type().unwrap().clone()))
            .collect();
        for partition in self.manifest_entries.iter().map(|e| &e.data_file.partition) {
            for (literal, stat) in partition.iter().zip_eq(field_stats.iter_mut()) {
                let primitive_literal = literal.map(|v| v.as_primitive_literal().unwrap());
                stat.update(primitive_literal)?;
            }
        }
        Ok(field_stats.into_iter().map(|stat| stat.finish()).collect())
    }

    fn check_data_file(&self, data_file: &DataFile) -> Result<()> {
        match self.metadata.content {
            ManifestContentType::Data => {
                if data_file.content != DataContentType::Data {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Content type of entry {:?} should have DataContentType::Data",
                            data_file.content
                        ),
                    ));
                }
            }
            ManifestContentType::Deletes => {
                if data_file.content != DataContentType::EqualityDeletes
                    && data_file.content != DataContentType::PositionDeletes
                {
                    return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Content type of entry {:?} should have DataContentType::EqualityDeletes or DataContentType::PositionDeletes", data_file.content),
                ));
                }
            }
        }
        Ok(())
    }

    /// Add a new manifest entry. This method will update following status of the entry:
    /// - Update the entry status to `Added`
    /// - Set the snapshot id to the current snapshot id
    /// - Set the sequence number to `None` if it is invalid(smaller than 0)
    /// - Set the file sequence number to `None`
    pub(crate) fn add_entry(&mut self, mut entry: ManifestEntry) -> Result<()> {
        self.check_data_file(&entry.data_file)?;
        if entry.sequence_number().is_some_and(|n| n >= 0) {
            entry.status = ManifestStatus::Added;
            entry.snapshot_id = self.snapshot_id;
            entry.file_sequence_number = None;
        } else {
            entry.status = ManifestStatus::Added;
            entry.snapshot_id = self.snapshot_id;
            entry.sequence_number = None;
            entry.file_sequence_number = None;
        };
        self.add_entry_inner(entry)?;
        Ok(())
    }

    /// Add file as an added entry with a specific sequence number. The entry's snapshot ID will be this manifest's snapshot ID. The entry's data sequence
    /// number will be the provided data sequence number. The entry's file sequence number will be
    /// assigned at commit.
    pub fn add_file(&mut self, data_file: DataFile, sequence_number: i64) -> Result<()> {
        self.check_data_file(&data_file)?;
        let entry = ManifestEntry {
            status: ManifestStatus::Added,
            snapshot_id: self.snapshot_id,
            sequence_number: (sequence_number >= 0).then_some(sequence_number),
            file_sequence_number: None,
            data_file,
        };
        self.add_entry_inner(entry)?;
        Ok(())
    }

    /// Add a delete manifest entry. This method will update following status of the entry:
    /// - Update the entry status to `Deleted`
    /// - Set the snapshot id to the current snapshot id
    ///
    /// # TODO
    /// Remove this allow later
    #[allow(dead_code)]
    pub(crate) fn add_delete_entry(&mut self, mut entry: ManifestEntry) -> Result<()> {
        self.check_data_file(&entry.data_file)?;
        entry.status = ManifestStatus::Deleted;
        entry.snapshot_id = self.snapshot_id;
        self.add_entry_inner(entry)?;
        Ok(())
    }

    /// Add a file as delete manifest entry. The entry's snapshot ID will be this manifest's snapshot ID.
    /// However, the original data and file sequence numbers of the file must be preserved when
    /// the file is marked as deleted.
    pub fn add_delete_file(
        &mut self,
        data_file: DataFile,
        sequence_number: i64,
        file_sequence_number: Option<i64>,
    ) -> Result<()> {
        self.check_data_file(&data_file)?;
        let entry = ManifestEntry {
            status: ManifestStatus::Deleted,
            snapshot_id: self.snapshot_id,
            sequence_number: Some(sequence_number),
            file_sequence_number,
            data_file,
        };
        self.add_entry_inner(entry)?;
        Ok(())
    }

    /// Add an existing manifest entry. This method will update following status of the entry:
    /// - Update the entry status to `Existing`
    ///
    /// # TODO
    /// Remove this allow later
    #[allow(dead_code)]
    pub(crate) fn add_existing_entry(&mut self, mut entry: ManifestEntry) -> Result<()> {
        self.check_data_file(&entry.data_file)?;
        entry.status = ManifestStatus::Existing;
        self.add_entry_inner(entry)?;
        Ok(())
    }

    /// Add an file as existing manifest entry. The original data and file sequence numbers, snapshot ID,
    /// which were assigned at commit, must be preserved when adding an existing entry.
    pub fn add_existing_file(
        &mut self,
        data_file: DataFile,
        snapshot_id: i64,
        sequence_number: i64,
        file_sequence_number: Option<i64>,
    ) -> Result<()> {
        self.check_data_file(&data_file)?;
        let entry = ManifestEntry {
            status: ManifestStatus::Existing,
            snapshot_id: Some(snapshot_id),
            sequence_number: Some(sequence_number),
            file_sequence_number,
            data_file,
        };
        self.add_entry_inner(entry)?;
        Ok(())
    }

    fn add_entry_inner(&mut self, entry: ManifestEntry) -> Result<()> {
        // Check if the entry has sequence number
        if (entry.status == ManifestStatus::Deleted || entry.status == ManifestStatus::Existing)
            && (entry.sequence_number.is_none() || entry.file_sequence_number.is_none())
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Manifest entry with status Existing or Deleted should have sequence number",
            ));
        }

        // Update the statistics
        match entry.status {
            ManifestStatus::Added => {
                self.added_files += 1;
                self.added_rows += entry.data_file.record_count;
            }
            ManifestStatus::Deleted => {
                self.deleted_files += 1;
                self.deleted_rows += entry.data_file.record_count;
            }
            ManifestStatus::Existing => {
                self.existing_files += 1;
                self.existing_rows += entry.data_file.record_count;
            }
        }
        if entry.is_alive() {
            if let Some(seq_num) = entry.sequence_number {
                self.min_seq_num = Some(self.min_seq_num.map_or(seq_num, |v| min(v, seq_num)));
            }
        }
        self.manifest_entries.push(entry);
        Ok(())
    }

    /// Write manifest file and return it.
    pub async fn write_manifest_file(mut self) -> Result<ManifestFile> {
        // Create the avro writer
        let partition_type = self
            .metadata
            .partition_spec
            .partition_type(&self.metadata.schema)?;
        let table_schema = &self.metadata.schema;
        let avro_schema = match self.metadata.format_version {
            FormatVersion::V1 => manifest_schema_v1(&partition_type)?,
            FormatVersion::V2 => manifest_schema_v2(&partition_type)?,
        };
        let mut avro_writer = AvroWriter::new(&avro_schema, Vec::new());
        avro_writer.add_user_metadata(
            "schema".to_string(),
            to_vec(table_schema).map_err(|err| {
                Error::new(ErrorKind::DataInvalid, "Fail to serialize table schema")
                    .with_source(err)
            })?,
        )?;
        avro_writer.add_user_metadata(
            "schema-id".to_string(),
            table_schema.schema_id().to_string(),
        )?;
        avro_writer.add_user_metadata(
            "partition-spec".to_string(),
            to_vec(&self.metadata.partition_spec.fields()).map_err(|err| {
                Error::new(ErrorKind::DataInvalid, "Fail to serialize partition spec")
                    .with_source(err)
            })?,
        )?;
        avro_writer.add_user_metadata(
            "partition-spec-id".to_string(),
            self.metadata.partition_spec.spec_id().to_string(),
        )?;
        avro_writer.add_user_metadata(
            "format-version".to_string(),
            (self.metadata.format_version as u8).to_string(),
        )?;
        if self.metadata.format_version == FormatVersion::V2 {
            avro_writer
                .add_user_metadata("content".to_string(), self.metadata.content.to_string())?;
        }

        let partition_summary = self.construct_partition_summaries(&partition_type)?;
        // Write manifest entries
        for entry in std::mem::take(&mut self.manifest_entries) {
            let value = match self.metadata.format_version {
                FormatVersion::V1 => to_value(ManifestEntryV1::try_from(entry, &partition_type)?)?
                    .resolve(&avro_schema)?,
                FormatVersion::V2 => to_value(ManifestEntryV2::try_from(entry, &partition_type)?)?
                    .resolve(&avro_schema)?,
            };

            avro_writer.append(value)?;
        }

        let content = avro_writer.into_inner()?;
        let length = content.len();
        self.output.write(Bytes::from(content)).await?;

        Ok(ManifestFile {
            manifest_path: self.output.location().to_string(),
            manifest_length: length as i64,
            partition_spec_id: self.metadata.partition_spec.spec_id(),
            content: self.metadata.content,
            // sequence_number and min_sequence_number with UNASSIGNED_SEQUENCE_NUMBER will be replace with
            // real sequence number in `ManifestListWriter`.
            sequence_number: UNASSIGNED_SEQUENCE_NUMBER,
            min_sequence_number: self.min_seq_num.unwrap_or(UNASSIGNED_SEQUENCE_NUMBER),
            added_snapshot_id: self.snapshot_id.unwrap_or(UNASSIGNED_SNAPSHOT_ID),
            added_files_count: Some(self.added_files),
            existing_files_count: Some(self.existing_files),
            deleted_files_count: Some(self.deleted_files),
            added_rows_count: Some(self.added_rows),
            existing_rows_count: Some(self.existing_rows),
            deleted_rows_count: Some(self.deleted_rows),
            partitions: partition_summary,
            key_metadata: self.key_metadata,
        })
    }
}
