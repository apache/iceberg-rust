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
use std::str::FromStr;

use serde_derive::{Deserialize, Serialize};
use serde_with::{DeserializeFromStr, SerializeDisplay};

use super::{Datum, Struct};
use crate::error::Result;
use crate::{Error, ErrorKind};

/// Data file carries data file path, partition tuple, metrics, …
#[derive(Debug, PartialEq, Clone, Eq, Builder)]
pub struct DataFile {
    /// field id: 134
    ///
    /// Type of content stored by the data file: data, equality deletes,
    /// or position deletes (all v1 files are data files)
    pub(crate) content: DataContentType,
    /// field id: 100
    ///
    /// Full URI for the file with FS scheme
    pub(crate) file_path: String,
    /// field id: 101
    ///
    /// String file format name, avro, orc or parquet
    pub(crate) file_format: DataFileFormat,
    /// field id: 102
    ///
    /// Partition data tuple, schema based on the partition spec output using
    /// partition field ids for the struct field ids
    pub(crate) partition: Struct,
    /// field id: 103
    ///
    /// Number of records in this file
    pub(crate) record_count: u64,
    /// field id: 104
    ///
    /// Total file size in bytes
    pub(crate) file_size_in_bytes: u64,
    /// field id: 108
    /// key field id: 117
    /// value field id: 118
    ///
    /// Map from column id to the total size on disk of all regions that
    /// store the column. Does not include bytes necessary to read other
    /// columns, like footers. Leave null for row-oriented formats (Avro)
    #[builder(default)]
    pub(crate) column_sizes: HashMap<i32, u64>,
    /// field id: 109
    /// key field id: 119
    /// value field id: 120
    ///
    /// Map from column id to number of values in the column (including null
    /// and NaN values)
    #[builder(default)]
    pub(crate) value_counts: HashMap<i32, u64>,
    /// field id: 110
    /// key field id: 121
    /// value field id: 122
    ///
    /// Map from column id to number of null values in the column
    #[builder(default)]
    pub(crate) null_value_counts: HashMap<i32, u64>,
    /// field id: 137
    /// key field id: 138
    /// value field id: 139
    ///
    /// Map from column id to number of NaN values in the column
    #[builder(default)]
    pub(crate) nan_value_counts: HashMap<i32, u64>,
    /// field id: 125
    /// key field id: 126
    /// value field id: 127
    ///
    /// Map from column id to lower bound in the column serialized as binary.
    /// Each value must be less than or equal to all non-null, non-NaN values
    /// in the column for the file.
    ///
    /// Reference:
    ///
    /// - [Binary single-value serialization](https://iceberg.apache.org/spec/#binary-single-value-serialization)
    #[builder(default)]
    pub(crate) lower_bounds: HashMap<i32, Datum>,
    /// field id: 128
    /// key field id: 129
    /// value field id: 130
    ///
    /// Map from column id to upper bound in the column serialized as binary.
    /// Each value must be greater than or equal to all non-null, non-Nan
    /// values in the column for the file.
    ///
    /// Reference:
    ///
    /// - [Binary single-value serialization](https://iceberg.apache.org/spec/#binary-single-value-serialization)
    #[builder(default)]
    pub(crate) upper_bounds: HashMap<i32, Datum>,
    /// field id: 131
    ///
    /// Implementation-specific key metadata for encryption
    #[builder(default)]
    pub(crate) key_metadata: Option<Vec<u8>>,
    /// field id: 132
    /// element field id: 133
    ///
    /// Split offsets for the data file. For example, all row group offsets
    /// in a Parquet file. Must be sorted ascending
    #[builder(default)]
    pub(crate) split_offsets: Vec<i64>,
    /// field id: 135
    /// element field id: 136
    ///
    /// Field ids used to determine row equality in equality delete files.
    /// Required when content is EqualityDeletes and should be null
    /// otherwise. Fields with ids listed in this column must be present
    /// in the delete file
    #[builder(default)]
    pub(crate) equality_ids: Vec<i32>,
    /// field id: 140
    ///
    /// ID representing sort order for this file.
    ///
    /// If sort order ID is missing or unknown, then the order is assumed to
    /// be unsorted. Only data files and equality delete files should be
    /// written with a non-null order id. Position deletes are required to be
    /// sorted by file and position, not a table order, and should set sort
    /// order id to null. Readers must ignore sort order id for position
    /// delete files.
    #[builder(default, setter(strip_option))]
    pub(crate) sort_order_id: Option<i32>,
}

impl DataFile {
    /// Get the content type of the data file (data, equality deletes, or position deletes)
    pub fn content_type(&self) -> DataContentType {
        self.content
    }
    /// Get the file path as full URI with FS scheme
    pub fn file_path(&self) -> &str {
        &self.file_path
    }
    /// Get the file format of the file (avro, orc or parquet).
    pub fn file_format(&self) -> DataFileFormat {
        self.file_format
    }
    /// Get the partition values of the file.
    pub fn partition(&self) -> &Struct {
        &self.partition
    }
    /// Get the record count in the data file.
    pub fn record_count(&self) -> u64 {
        self.record_count
    }
    /// Get the file size in bytes.
    pub fn file_size_in_bytes(&self) -> u64 {
        self.file_size_in_bytes
    }
    /// Get the column sizes.
    /// Map from column id to the total size on disk of all regions that
    /// store the column. Does not include bytes necessary to read other
    /// columns, like footers. Null for row-oriented formats (Avro)
    pub fn column_sizes(&self) -> &HashMap<i32, u64> {
        &self.column_sizes
    }
    /// Get the columns value counts for the data file.
    /// Map from column id to number of values in the column (including null
    /// and NaN values)
    pub fn value_counts(&self) -> &HashMap<i32, u64> {
        &self.value_counts
    }
    /// Get the null value counts of the data file.
    /// Map from column id to number of null values in the column
    pub fn null_value_counts(&self) -> &HashMap<i32, u64> {
        &self.null_value_counts
    }
    /// Get the nan value counts of the data file.
    /// Map from column id to number of NaN values in the column
    pub fn nan_value_counts(&self) -> &HashMap<i32, u64> {
        &self.nan_value_counts
    }
    /// Get the lower bounds of the data file values per column.
    /// Map from column id to lower bound in the column serialized as binary.
    pub fn lower_bounds(&self) -> &HashMap<i32, Datum> {
        &self.lower_bounds
    }
    /// Get the upper bounds of the data file values per column.
    /// Map from column id to upper bound in the column serialized as binary.
    pub fn upper_bounds(&self) -> &HashMap<i32, Datum> {
        &self.upper_bounds
    }
    /// Get the Implementation-specific key metadata for the data file.
    pub fn key_metadata(&self) -> Option<&[u8]> {
        self.key_metadata.as_deref()
    }
    /// Get the split offsets of the data file.
    /// For example, all row group offsets in a Parquet file.
    pub fn split_offsets(&self) -> &[i64] {
        &self.split_offsets
    }
    /// Get the equality ids of the data file.
    /// Field ids used to determine row equality in equality delete files.
    /// null when content is not EqualityDeletes.
    pub fn equality_ids(&self) -> &[i32] {
        &self.equality_ids
    }
    /// Get the sort order id of the data file.
    /// Only data files and equality delete files should be
    /// written with a non-null order id. Position deletes are required to be
    /// sorted by file and position, not a table order, and should set sort
    /// order id to null. Readers must ignore sort order id for position
    /// delete files.
    pub fn sort_order_id(&self) -> Option<i32> {
        self.sort_order_id
    }
}

/// Type of content stored by the data file: data, equality deletes, or
/// position deletes (all v1 files are data files)
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
pub enum DataContentType {
    /// value: 0
    Data = 0,
    /// value: 1
    PositionDeletes = 1,
    /// value: 2
    EqualityDeletes = 2,
}

impl TryFrom<i32> for DataContentType {
    type Error = Error;

    fn try_from(v: i32) -> Result<DataContentType> {
        match v {
            0 => Ok(DataContentType::Data),
            1 => Ok(DataContentType::PositionDeletes),
            2 => Ok(DataContentType::EqualityDeletes),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!("data content type {} is invalid", v),
            )),
        }
    }
}

/// Format of this data.
#[derive(Debug, PartialEq, Eq, Clone, Copy, SerializeDisplay, DeserializeFromStr)]
pub enum DataFileFormat {
    /// Avro file format: <https://avro.apache.org/>
    Avro,
    /// Orc file format: <https://orc.apache.org/>
    Orc,
    /// Parquet file format: <https://parquet.apache.org/>
    Parquet,
}

impl FromStr for DataFileFormat {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "avro" => Ok(Self::Avro),
            "orc" => Ok(Self::Orc),
            "parquet" => Ok(Self::Parquet),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Unsupported data file format: {}", s),
            )),
        }
    }
}

impl std::fmt::Display for DataFileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataFileFormat::Avro => write!(f, "avro"),
            DataFileFormat::Orc => write!(f, "orc"),
            DataFileFormat::Parquet => write!(f, "parquet"),
        }
    }
}
