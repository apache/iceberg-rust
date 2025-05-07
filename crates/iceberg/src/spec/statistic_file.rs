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

//! Statistic Files for TableMetadata

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::spec::{DataContentType, DataFile, Snapshot, Struct};
use crate::{Error, ErrorKind};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
/// Represents a statistics file
pub struct StatisticsFile {
    /// The snapshot id of the statistics file.
    pub snapshot_id: i64,
    /// Path of the statistics file
    pub statistics_path: String,
    /// File size in bytes
    pub file_size_in_bytes: i64,
    /// File footer size in bytes
    pub file_footer_size_in_bytes: i64,
    /// Base64-encoded implementation-specific key metadata for encryption.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_metadata: Option<String>,
    /// Blob metadata
    pub blob_metadata: Vec<BlobMetadata>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
/// Represents a blob of metadata, which is a part of a statistics file
pub struct BlobMetadata {
    /// Type of the blob.
    pub r#type: String,
    /// Snapshot id of the blob.
    pub snapshot_id: i64,
    /// Sequence number of the blob.
    pub sequence_number: i64,
    /// Fields of the blob.
    pub fields: Vec<i32>,
    /// Properties of the blob.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
/// Statistics file for a partition
pub struct PartitionStatisticsFile {
    /// The snapshot id of the statistics file.
    pub snapshot_id: i64,
    /// Path of the statistics file
    pub statistics_path: String,
    /// File size in bytes
    pub file_size_in_bytes: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Statistics for partition pruning
pub struct PartitionStats {
    partition: Struct,
    spec_id: i32,
    data_record_count: u64,
    data_file_count: u32,
    total_data_file_size_in_bytes: u64,
    position_delete_record_count: u64,
    position_delete_file_count: u32,
    equality_delete_record_count: u64,
    equality_delete_file_count: u32,
    total_record_count: u64,
    last_updated_at: Option<i64>,
    last_updated_snapshot_id: Option<i64>,
}

impl PartitionStats {
    /// Creates new `PartitionStats` instance based on partition struct
    /// spec id
    pub fn new(partition: Struct, spec_id: i32) -> Self {
        Self {
            partition,
            spec_id,
            data_record_count: 0,
            data_file_count: 0,
            total_data_file_size_in_bytes: 0,
            position_delete_record_count: 0,
            position_delete_file_count: 0,
            equality_delete_record_count: 0,
            equality_delete_file_count: 0,
            total_record_count: 0,
            last_updated_at: None,
            last_updated_snapshot_id: None,
        }
    }

    /// Returns partition struct
    pub fn partition(&self) -> &Struct {
        &self.partition
    }

    /// Returns partition spec id
    pub fn spec_id(&self) -> i32 {
        self.spec_id
    }

    /// Returns the total number of data records in the partition.
    pub fn data_record_count(&self) -> u64 {
        self.data_record_count
    }

    /// Returns the number of data files in the partition
    pub fn data_file_count(&self) -> u32 {
        self.data_file_count
    }

    /// Returns the total size in bytes of all data files in the partition
    pub fn total_data_file_size_in_bytes(&self) -> u64 {
        self.total_data_file_size_in_bytes
    }

    /// Returns the total number of records in position delete files
    pub fn position_delete_record_count(&self) -> u64 {
        self.position_delete_record_count
    }

    /// Returns the number of position delete files
    pub fn position_delete_file_count(&self) -> u32 {
        self.position_delete_file_count
    }

    /// Returns the total number of records in equality delete files
    pub fn equality_delete_record_count(&self) -> u64 {
        self.equality_delete_record_count
    }

    /// Returns the number of equality delete files
    pub fn equality_delete_file_count(&self) -> u32 {
        self.equality_delete_file_count
    }

    /// Returns the total record count in the partition
    pub fn total_record_count(&self) -> u64 {
        self.total_record_count
    }

    /// Returns the timestamp of the last snapshot update
    pub fn last_updated_at(&self) -> Option<i64> {
        self.last_updated_at
    }

    /// Returns the snapshot id of the last update
    pub fn last_updated_snapshot_id(&self) -> Option<i64> {
        self.last_updated_snapshot_id
    }

    /// Updates the partition statistics based on the given `DataFile` and its corresponding `Snapshot`.
    pub fn live_entry(&mut self, file: DataFile, snapshot: Snapshot) -> Result<(), Error> {
        if file.partition_spec_id != self.spec_id {
            return Err(Error::new(ErrorKind::Unexpected, "Spec IDs must match."));
        }

        match file.content_type() {
            DataContentType::Data => {
                self.data_record_count += file.record_count();
                self.data_file_count += 1;
                self.total_data_file_size_in_bytes += file.file_size_in_bytes();
            }
            DataContentType::PositionDeletes => {
                self.position_delete_record_count += file.record_count();
                self.position_delete_file_count += 1;
            }
            DataContentType::EqualityDeletes => {
                self.equality_delete_record_count += file.record_count();
                self.equality_delete_file_count += 1;
            }
        }

        self.update_snapshot_info(snapshot.snapshot_id(), snapshot.timestamp_ms());
        Ok(())
    }

    fn update_snapshot_info(&mut self, snapshot_id: i64, updated_at: i64) {
        if self.last_updated_at.is_none() || self.last_updated_at.unwrap() < updated_at {
            self.last_updated_at = Some(updated_at);
            self.last_updated_snapshot_id = Some(snapshot_id);
        }
    }
}

#[cfg(test)]
mod test {
    use std::fmt::Debug;

    use serde::de::DeserializeOwned;
    use serde_json::json;

    use super::*;
    use crate::spec::{DataFileFormat, Datum, Literal, Operation, Summary};

    fn test_serde_json<T: Serialize + DeserializeOwned + PartialEq + Debug>(
        json: serde_json::Value,
        expected: T,
    ) {
        let json_str = json.to_string();
        let actual: T = serde_json::from_str(&json_str).expect("Failed to parse from json");
        assert_eq!(actual, expected, "Parsed value is not equal to expected");

        let restored: T = serde_json::from_str(
            &serde_json::to_string(&actual).expect("Failed to serialize to json"),
        )
        .expect("Failed to parse from serialized json");

        assert_eq!(
            restored, expected,
            "Parsed restored value is not equal to expected"
        );
    }

    #[test]
    fn test_blob_metadata_serde() {
        test_serde_json(
            json!({
                "type": "boring-type",
                "snapshot-id": 1940541653261589030i64,
                "sequence-number": 2,
                "fields": [
                        1
                ],
                "properties": {
                        "prop-key": "prop-value"
                }
            }),
            BlobMetadata {
                r#type: "boring-type".to_string(),
                snapshot_id: 1940541653261589030,
                sequence_number: 2,
                fields: vec![1],
                properties: vec![("prop-key".to_string(), "prop-value".to_string())]
                    .into_iter()
                    .collect(),
            },
        );
    }

    #[test]
    fn test_blob_metadata_serde_no_properties() {
        test_serde_json(
            json!({
                "type": "boring-type",
                "snapshot-id": 1940541653261589030i64,
                "sequence-number": 2,
                "fields": [
                        1
                ]
            }),
            BlobMetadata {
                r#type: "boring-type".to_string(),
                snapshot_id: 1940541653261589030,
                sequence_number: 2,
                fields: vec![1],
                properties: HashMap::new(),
            },
        );
    }

    #[test]
    fn test_statistics_file_serde() {
        test_serde_json(
            json!({
              "snapshot-id": 3055729675574597004i64,
              "statistics-path": "s3://a/b/stats.puffin",
              "file-size-in-bytes": 413,
              "file-footer-size-in-bytes": 42,
              "blob-metadata": [
                {
                  "type": "ndv",
                  "snapshot-id": 3055729675574597004i64,
                  "sequence-number": 1,
                  "fields": [1]
                }
              ]
            }),
            StatisticsFile {
                snapshot_id: 3055729675574597004i64,
                statistics_path: "s3://a/b/stats.puffin".to_string(),
                file_size_in_bytes: 413,
                file_footer_size_in_bytes: 42,
                key_metadata: None,
                blob_metadata: vec![BlobMetadata {
                    r#type: "ndv".to_string(),
                    snapshot_id: 3055729675574597004i64,
                    sequence_number: 1,
                    fields: vec![1],
                    properties: HashMap::new(),
                }],
            },
        );
    }

    #[test]
    fn test_partition_statistics_serde() {
        test_serde_json(
            json!({
              "snapshot-id": 3055729675574597004i64,
              "statistics-path": "s3://a/b/partition-stats.parquet",
              "file-size-in-bytes": 43
            }),
            PartitionStatisticsFile {
                snapshot_id: 3055729675574597004,
                statistics_path: "s3://a/b/partition-stats.parquet".to_string(),
                file_size_in_bytes: 43,
            },
        );
    }

    #[test]
    fn test_partition_stats() -> Result<(), Error> {
        let partition = Struct::from_iter(vec![Some(Literal::string("x"))]);

        let spec_id = 0;
        let mut stats = PartitionStats::new(partition.clone(), spec_id);

        // test data file
        let snapshot1 = Snapshot::builder()
            .with_snapshot_id(1)
            .with_sequence_number(0)
            .with_timestamp_ms(1000)
            .with_manifest_list("manifest_list_path".to_string())
            .with_summary(Summary {
                operation: Operation::default(),
                additional_properties: HashMap::new(),
            })
            .build();

        let data_file = DataFile {
            content: DataContentType::Data,
            file_path: "test.parquet".to_string(),
            file_format: DataFileFormat::Parquet,
            partition: partition.clone(),
            record_count: 1,
            file_size_in_bytes: 874,
            column_sizes: HashMap::from([(1, 46), (2, 48), (3, 48)]),
            value_counts: HashMap::from([(1, 1), (2, 1), (3, 1)]),
            null_value_counts: HashMap::from([(1, 0), (2, 0), (3, 0)]),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::from([
                (1, Datum::long(1)),
                (2, Datum::string("a")),
                (3, Datum::string("x")),
            ]),
            upper_bounds: HashMap::from([
                (1, Datum::long(1)),
                (2, Datum::string("a")),
                (3, Datum::string("x")),
            ]),
            key_metadata: None,
            split_offsets: vec![4],
            equality_ids: vec![],
            sort_order_id: Some(0),
            partition_spec_id: spec_id,
            content_offset: None,
            content_size_in_bytes: None,
            first_row_id: None,
            referenced_data_file: None,
        };
        stats.live_entry(data_file, snapshot1.clone())?;
        assert_eq!(stats.data_record_count(), 1);
        assert_eq!(stats.data_file_count(), 1);
        assert_eq!(stats.total_data_file_size_in_bytes(), 874);
        assert_eq!(stats.last_updated_snapshot_id(), Some(1));
        assert_eq!(stats.last_updated_at(), Some(1000));

        // test position delete file
        let snapshot2 = Snapshot::builder()
            .with_snapshot_id(2)
            .with_sequence_number(1)
            .with_timestamp_ms(2000)
            .with_manifest_list("manifest_list_path".to_string())
            .with_summary(Summary {
                operation: Operation::default(),
                additional_properties: HashMap::new(),
            })
            .build();

        let posdel_file = DataFile {
            content: DataContentType::PositionDeletes,
            file_path: "test.parquet".to_string(),
            file_format: DataFileFormat::Parquet,
            partition: partition.clone(),
            record_count: 5,
            file_size_in_bytes: 500,
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            key_metadata: None,
            split_offsets: vec![10],
            equality_ids: vec![],
            sort_order_id: None,
            partition_spec_id: spec_id,
            content_offset: None,
            content_size_in_bytes: None,
            first_row_id: None,
            referenced_data_file: None,
        };

        stats.live_entry(posdel_file, snapshot2.clone())?;
        assert_eq!(stats.position_delete_record_count(), 5);
        assert_eq!(stats.position_delete_file_count(), 1);

        assert_eq!(stats.last_updated_snapshot_id(), Some(2));
        assert_eq!(stats.last_updated_at(), Some(2000));

        // test equality delete file
        let snapshot3 = Snapshot::builder()
            .with_snapshot_id(3)
            .with_sequence_number(2)
            .with_timestamp_ms(3000)
            .with_manifest_list("manifest_list_path".to_string())
            .with_summary(Summary {
                operation: Operation::default(),
                additional_properties: HashMap::new(),
            })
            .build();

        let eqdel_file = DataFile {
            content: DataContentType::EqualityDeletes,
            file_path: "test.parquet".to_string(),
            file_format: DataFileFormat::Parquet,
            partition: partition.clone(),
            record_count: 3,
            file_size_in_bytes: 300,
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            key_metadata: None,
            split_offsets: vec![15],
            equality_ids: vec![],
            sort_order_id: None,
            partition_spec_id: spec_id,
            content_offset: None,
            content_size_in_bytes: None,
            first_row_id: None,
            referenced_data_file: None,
        };
        stats.live_entry(eqdel_file, snapshot3.clone())?;
        assert_eq!(stats.equality_delete_record_count(), 3);
        assert_eq!(stats.equality_delete_file_count(), 1);
        assert_eq!(stats.last_updated_snapshot_id(), Some(3));
        assert_eq!(stats.last_updated_at(), Some(3000));

        let snapshot4 = Snapshot::builder()
            .with_snapshot_id(4)
            .with_sequence_number(3)
            .with_timestamp_ms(4000)
            .with_manifest_list("manifest_list_path".to_string())
            .with_summary(Summary {
                operation: Operation::default(),
                additional_properties: HashMap::new(),
            })
            .build();

        let wrong_file = DataFile {
            content: DataContentType::Data,
            file_path: "test.parquet".to_string(),
            file_format: DataFileFormat::Parquet,
            partition,
            record_count: 2,
            file_size_in_bytes: 900,
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            key_metadata: None,
            split_offsets: vec![20],
            equality_ids: vec![],
            sort_order_id: Some(0),
            partition_spec_id: spec_id + 1, // mismatch spec id.
            content_offset: None,
            content_size_in_bytes: None,
            first_row_id: None,
            referenced_data_file: None,
        };

        let result = stats.live_entry(wrong_file, snapshot4);
        assert!(result.is_err());

        Ok(())
    }
}
