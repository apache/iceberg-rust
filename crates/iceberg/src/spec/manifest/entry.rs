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

use std::sync::Arc;

use typed_builder::TypedBuilder;

use crate::error::Result;
use crate::spec::manifest::data_file::{DataContentType, DataFile, DataFileFormat};
use crate::spec::{ManifestFile, INITIAL_SEQUENCE_NUMBER};
use crate::{Error, ErrorKind};

/// Reference to [`ManifestEntry`].
pub type ManifestEntryRef = Arc<ManifestEntry>;

/// A manifest is an immutable Avro file that lists data files or delete
/// files, along with each file’s partition data tuple, metrics, and tracking
/// information.
#[derive(Debug, PartialEq, Eq, Clone, TypedBuilder)]
pub struct ManifestEntry {
    /// field: 0
    ///
    /// Used to track additions and deletions.
    pub status: ManifestStatus,
    /// field id: 1
    ///
    /// Snapshot id where the file was added, or deleted if status is 2.
    /// Inherited when null.
    #[builder(default, setter(strip_option(fallback = snapshot_id_opt)))]
    pub snapshot_id: Option<i64>,
    /// field id: 3
    ///
    /// Data sequence number of the file.
    /// Inherited when null and status is 1 (added).
    #[builder(default, setter(strip_option(fallback = sequence_number_opt)))]
    pub sequence_number: Option<i64>,
    /// field id: 4
    ///
    /// File sequence number indicating when the file was added.
    /// Inherited when null and status is 1 (added).
    #[builder(default, setter(strip_option(fallback = file_sequence_number_opt)))]
    pub file_sequence_number: Option<i64>,
    /// field id: 2
    ///
    /// File path, partition tuple, metrics, …
    pub data_file: DataFile,
}

impl ManifestEntry {
    /// Check if this manifest entry is deleted.
    pub fn is_alive(&self) -> bool {
        matches!(
            self.status,
            ManifestStatus::Added | ManifestStatus::Existing
        )
    }

    /// Status of this manifest entry
    pub fn status(&self) -> ManifestStatus {
        self.status
    }

    /// Content type of this manifest entry.
    #[inline]
    pub fn content_type(&self) -> DataContentType {
        self.data_file.content
    }

    /// File format of this manifest entry.
    #[inline]
    pub fn file_format(&self) -> DataFileFormat {
        self.data_file.file_format
    }

    /// Data file path of this manifest entry.
    #[inline]
    pub fn file_path(&self) -> &str {
        &self.data_file.file_path
    }

    /// Data file record count of the manifest entry.
    #[inline]
    pub fn record_count(&self) -> u64 {
        self.data_file.record_count
    }

    /// Inherit data from manifest list, such as snapshot id, sequence number.
    pub(crate) fn inherit_data(&mut self, snapshot_entry: &ManifestFile) {
        if self.snapshot_id.is_none() {
            self.snapshot_id = Some(snapshot_entry.added_snapshot_id);
        }

        if self.sequence_number.is_none()
            && (self.status == ManifestStatus::Added
                || snapshot_entry.sequence_number == INITIAL_SEQUENCE_NUMBER)
        {
            self.sequence_number = Some(snapshot_entry.sequence_number);
        }

        if self.file_sequence_number.is_none()
            && (self.status == ManifestStatus::Added
                || snapshot_entry.sequence_number == INITIAL_SEQUENCE_NUMBER)
        {
            self.file_sequence_number = Some(snapshot_entry.sequence_number);
        }
    }

    /// Snapshot id
    #[inline]
    pub fn snapshot_id(&self) -> Option<i64> {
        self.snapshot_id
    }

    /// Data sequence number.
    #[inline]
    pub fn sequence_number(&self) -> Option<i64> {
        self.sequence_number
    }

    /// File size in bytes.
    #[inline]
    pub fn file_size_in_bytes(&self) -> u64 {
        self.data_file.file_size_in_bytes
    }

    /// get a reference to the actual data file
    #[inline]
    pub fn data_file(&self) -> &DataFile {
        &self.data_file
    }
}

/// Used to track additions and deletions in ManifestEntry.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ManifestStatus {
    /// Value: 0
    Existing = 0,
    /// Value: 1
    Added = 1,
    /// Value: 2
    ///
    /// Deletes are informational only and not used in scans.
    Deleted = 2,
}

impl TryFrom<i32> for ManifestStatus {
    type Error = Error;

    fn try_from(v: i32) -> Result<ManifestStatus> {
        match v {
            0 => Ok(ManifestStatus::Existing),
            1 => Ok(ManifestStatus::Added),
            2 => Ok(ManifestStatus::Deleted),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!("manifest status {} is invalid", v),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::manifest::metadata::ManifestMetadata;
    use crate::spec::writer::ManifestWriterBuilder;
    use crate::spec::{
        Datum, FormatVersion, Literal, Manifest, ManifestContentType, NestedField, PartitionSpec,
        PrimitiveType, Schema, Struct, Transform, Type,
    };

    #[tokio::test]
    async fn test_add_delete_existing() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::optional(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::optional(
                        2,
                        "name",
                        Type::Primitive(PrimitiveType::String),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let metadata = ManifestMetadata {
            schema_id: 0,
            schema: schema.clone(),
            partition_spec: PartitionSpec::builder(schema)
                .with_spec_id(0)
                .build()
                .unwrap(),
            content: ManifestContentType::Data,
            format_version: FormatVersion::V2,
        };
        let mut entries = vec![
                ManifestEntry {
                    status: ManifestStatus::Added,
                    snapshot_id: None,
                    sequence_number: Some(1),
                    file_sequence_number: Some(1),
                    data_file: DataFile {
                        content: DataContentType::Data,
                        file_path: "s3a://icebergdata/demo/s1/t1/data/00000-0-ba56fbfa-f2ff-40c9-bb27-565ad6dc2be8-00000.parquet".to_string(),
                        file_format: DataFileFormat::Parquet,
                        partition: Struct::empty(),
                        record_count: 1,
                        file_size_in_bytes: 5442,
                        column_sizes: HashMap::from([(1, 61), (2, 73)]),
                        value_counts: HashMap::from([(1, 1), (2, 1)]),
                        null_value_counts: HashMap::from([(1, 0), (2, 0)]),
                        nan_value_counts: HashMap::new(),
                        lower_bounds: HashMap::new(),
                        upper_bounds: HashMap::new(),
                        key_metadata: Some(Vec::new()),
                        split_offsets: vec![4],
                        equality_ids: Vec::new(),
                        sort_order_id: None,
                    },
                },
                ManifestEntry {
                    status: ManifestStatus::Deleted,
                    snapshot_id: Some(1),
                    sequence_number: Some(1),
                    file_sequence_number: Some(1),
                    data_file: DataFile {
                        content: DataContentType::Data,
                        file_path: "s3a://icebergdata/demo/s1/t1/data/00000-0-ba56fbfa-f2ff-40c9-bb27-565ad6dc2be8-00000.parquet".to_string(),
                        file_format: DataFileFormat::Parquet,
                        partition: Struct::empty(),
                        record_count: 1,
                        file_size_in_bytes: 5442,
                        column_sizes: HashMap::from([(1, 61), (2, 73)]),
                        value_counts: HashMap::from([(1, 1), (2, 1)]),
                        null_value_counts: HashMap::from([(1, 0), (2, 0)]),
                        nan_value_counts: HashMap::new(),
                        lower_bounds: HashMap::new(),
                        upper_bounds: HashMap::new(),
                        key_metadata: Some(Vec::new()),
                        split_offsets: vec![4],
                        equality_ids: Vec::new(),
                        sort_order_id: None,
                    },
                },
                ManifestEntry {
                    status: ManifestStatus::Existing,
                    snapshot_id: Some(1),
                    sequence_number: Some(1),
                    file_sequence_number: Some(1),
                    data_file: DataFile {
                        content: DataContentType::Data,
                        file_path: "s3a://icebergdata/demo/s1/t1/data/00000-0-ba56fbfa-f2ff-40c9-bb27-565ad6dc2be8-00000.parquet".to_string(),
                        file_format: DataFileFormat::Parquet,
                        partition: Struct::empty(),
                        record_count: 1,
                        file_size_in_bytes: 5442,
                        column_sizes: HashMap::from([(1, 61), (2, 73)]),
                        value_counts: HashMap::from([(1, 1), (2, 1)]),
                        null_value_counts: HashMap::from([(1, 0), (2, 0)]),
                        nan_value_counts: HashMap::new(),
                        lower_bounds: HashMap::new(),
                        upper_bounds: HashMap::new(),
                        key_metadata: Some(Vec::new()),
                        split_offsets: vec![4],
                        equality_ids: Vec::new(),
                        sort_order_id: None,
                    },
                },
            ];

        // write manifest to file
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("test_manifest.avro");
        let io = FileIOBuilder::new_fs_io().build().unwrap();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();
        let mut writer = ManifestWriterBuilder::new(
            output_file,
            Some(3),
            vec![],
            metadata.schema.clone(),
            metadata.partition_spec.clone(),
        )
        .build_v2_data();
        writer.add_entry(entries[0].clone()).unwrap();
        writer.add_delete_entry(entries[1].clone()).unwrap();
        writer.add_existing_entry(entries[2].clone()).unwrap();
        writer.write_manifest_file().await.unwrap();

        // read back the manifest file and check the content
        let actual_manifest =
            Manifest::parse_avro(fs::read(path).expect("read_file must succeed").as_slice())
                .unwrap();

        // The snapshot id is assigned when the entry is added and delete to the manifest. Existing entries are keep original.
        entries[0].snapshot_id = Some(3);
        entries[1].snapshot_id = Some(3);
        // file sequence number is assigned to None when the entry is added and delete to the manifest.
        entries[0].file_sequence_number = None;
        assert_eq!(actual_manifest, Manifest::new(metadata, entries));
    }

    #[tokio::test]
    async fn test_manifest_summary() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::optional(
                        1,
                        "time",
                        Type::Primitive(PrimitiveType::Date),
                    )),
                    Arc::new(NestedField::optional(
                        2,
                        "v_float",
                        Type::Primitive(PrimitiveType::Float),
                    )),
                    Arc::new(NestedField::optional(
                        3,
                        "v_double",
                        Type::Primitive(PrimitiveType::Double),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .add_partition_field("time", "year_of_time", Transform::Year)
            .unwrap()
            .add_partition_field("v_float", "f", Transform::Identity)
            .unwrap()
            .add_partition_field("v_double", "d", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();
        let metadata = ManifestMetadata {
            schema_id: 0,
            schema,
            partition_spec,
            content: ManifestContentType::Data,
            format_version: FormatVersion::V2,
        };
        let entries = vec![
                ManifestEntry {
                    status: ManifestStatus::Added,
                    snapshot_id: None,
                    sequence_number: None,
                    file_sequence_number: None,
                    data_file: DataFile {
                        content: DataContentType::Data,
                        file_path: "s3a://icebergdata/demo/s1/t1/data/00000-0-ba56fbfa-f2ff-40c9-bb27-565ad6dc2be8-00000.parquet".to_string(),
                        file_format: DataFileFormat::Parquet,
                        partition: Struct::from_iter(
                            vec![
                                Some(Literal::int(2021)),
                                Some(Literal::float(1.0)),
                                Some(Literal::double(2.0)),
                            ]
                        ),
                        record_count: 1,
                        file_size_in_bytes: 5442,
                        column_sizes: HashMap::from([(0,73),(6,34),(2,73),(7,61),(3,61),(5,62),(9,79),(10,73),(1,61),(4,73),(8,73)]),
                        value_counts: HashMap::from([(4,1),(5,1),(2,1),(0,1),(3,1),(6,1),(8,1),(1,1),(10,1),(7,1),(9,1)]),
                        null_value_counts: HashMap::from([(1,0),(6,0),(2,0),(8,0),(0,0),(3,0),(5,0),(9,0),(7,0),(4,0),(10,0)]),
                        nan_value_counts: HashMap::new(),
                        lower_bounds: HashMap::new(),
                        upper_bounds: HashMap::new(),
                        key_metadata: None,
                        split_offsets: vec![4],
                        equality_ids: Vec::new(),
                        sort_order_id: None,
                    }
                },
                    ManifestEntry {
                        status: ManifestStatus::Added,
                        snapshot_id: None,
                        sequence_number: None,
                        file_sequence_number: None,
                        data_file: DataFile {
                            content: DataContentType::Data,
                            file_path: "s3a://icebergdata/demo/s1/t1/data/00000-0-ba56fbfa-f2ff-40c9-bb27-565ad6dc2be8-00000.parquet".to_string(),
                            file_format: DataFileFormat::Parquet,
                            partition: Struct::from_iter(
                                vec![
                                    Some(Literal::int(1111)),
                                    Some(Literal::float(15.5)),
                                    Some(Literal::double(25.5)),
                                ]
                            ),
                            record_count: 1,
                            file_size_in_bytes: 5442,
                            column_sizes: HashMap::from([(0,73),(6,34),(2,73),(7,61),(3,61),(5,62),(9,79),(10,73),(1,61),(4,73),(8,73)]),
                            value_counts: HashMap::from([(4,1),(5,1),(2,1),(0,1),(3,1),(6,1),(8,1),(1,1),(10,1),(7,1),(9,1)]),
                            null_value_counts: HashMap::from([(1,0),(6,0),(2,0),(8,0),(0,0),(3,0),(5,0),(9,0),(7,0),(4,0),(10,0)]),
                            nan_value_counts: HashMap::new(),
                            lower_bounds: HashMap::new(),
                            upper_bounds: HashMap::new(),
                            key_metadata: None,
                            split_offsets: vec![4],
                            equality_ids: Vec::new(),
                            sort_order_id: None,
                        }
                    },
                    ManifestEntry {
                        status: ManifestStatus::Added,
                        snapshot_id: None,
                        sequence_number: None,
                        file_sequence_number: None,
                        data_file: DataFile {
                            content: DataContentType::Data,
                            file_path: "s3a://icebergdata/demo/s1/t1/data/00000-0-ba56fbfa-f2ff-40c9-bb27-565ad6dc2be8-00000.parquet".to_string(),
                            file_format: DataFileFormat::Parquet,
                            partition: Struct::from_iter(
                                vec![
                                    Some(Literal::int(1211)),
                                    Some(Literal::float(f32::NAN)),
                                    Some(Literal::double(1.0)),
                                ]
                            ),
                            record_count: 1,
                            file_size_in_bytes: 5442,
                            column_sizes: HashMap::from([(0,73),(6,34),(2,73),(7,61),(3,61),(5,62),(9,79),(10,73),(1,61),(4,73),(8,73)]),
                            value_counts: HashMap::from([(4,1),(5,1),(2,1),(0,1),(3,1),(6,1),(8,1),(1,1),(10,1),(7,1),(9,1)]),
                            null_value_counts: HashMap::from([(1,0),(6,0),(2,0),(8,0),(0,0),(3,0),(5,0),(9,0),(7,0),(4,0),(10,0)]),
                            nan_value_counts: HashMap::new(),
                            lower_bounds: HashMap::new(),
                            upper_bounds: HashMap::new(),
                            key_metadata: None,
                            split_offsets: vec![4],
                            equality_ids: Vec::new(),
                            sort_order_id: None,
                        }
                    },
                    ManifestEntry {
                        status: ManifestStatus::Added,
                        snapshot_id: None,
                        sequence_number: None,
                        file_sequence_number: None,
                        data_file: DataFile {
                            content: DataContentType::Data,
                            file_path: "s3a://icebergdata/demo/s1/t1/data/00000-0-ba56fbfa-f2ff-40c9-bb27-565ad6dc2be8-00000.parquet".to_string(),
                            file_format: DataFileFormat::Parquet,
                            partition: Struct::from_iter(
                                vec![
                                    Some(Literal::int(1111)),
                                    None,
                                    Some(Literal::double(11.0)),
                                ]
                            ),
                            record_count: 1,
                            file_size_in_bytes: 5442,
                            column_sizes: HashMap::from([(0,73),(6,34),(2,73),(7,61),(3,61),(5,62),(9,79),(10,73),(1,61),(4,73),(8,73)]),
                            value_counts: HashMap::from([(4,1),(5,1),(2,1),(0,1),(3,1),(6,1),(8,1),(1,1),(10,1),(7,1),(9,1)]),
                            null_value_counts: HashMap::from([(1,0),(6,0),(2,0),(8,0),(0,0),(3,0),(5,0),(9,0),(7,0),(4,0),(10,0)]),
                            nan_value_counts: HashMap::new(),
                            lower_bounds: HashMap::new(),
                            upper_bounds: HashMap::new(),
                            key_metadata: None,
                            split_offsets: vec![4],
                            equality_ids: Vec::new(),
                            sort_order_id: None,
                        }
                    },
            ];

        // write manifest to file
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("test_manifest.avro");
        let io = FileIOBuilder::new_fs_io().build().unwrap();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();
        let mut writer = ManifestWriterBuilder::new(
            output_file,
            Some(1),
            vec![],
            metadata.schema.clone(),
            metadata.partition_spec.clone(),
        )
        .build_v2_data();
        for entry in &entries {
            writer.add_entry(entry.clone()).unwrap();
        }
        let res = writer.write_manifest_file().await.unwrap();

        assert_eq!(res.partitions.len(), 3);
        assert_eq!(res.partitions[0].lower_bound, Some(Datum::int(1111)));
        assert_eq!(res.partitions[0].upper_bound, Some(Datum::int(2021)));
        assert!(!res.partitions[0].contains_null);
        assert_eq!(res.partitions[0].contains_nan, Some(false));

        assert_eq!(res.partitions[1].lower_bound, Some(Datum::float(1.0)));
        assert_eq!(res.partitions[1].upper_bound, Some(Datum::float(15.5)));
        assert!(res.partitions[1].contains_null);
        assert_eq!(res.partitions[1].contains_nan, Some(true));

        assert_eq!(res.partitions[2].lower_bound, Some(Datum::double(1.0)));
        assert_eq!(res.partitions[2].upper_bound, Some(Datum::double(25.5)));
        assert!(!res.partitions[2].contains_null);
        assert_eq!(res.partitions[2].contains_nan, Some(false));
    }
}
