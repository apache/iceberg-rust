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

use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::Result;
use crate::expr::BoundPredicate;
use crate::spec::{
    DataContentType, DataFileFormat, Literal, ManifestEntryRef, NameMapping, PartitionSpec,
    PrimitiveLiteral, Schema, SchemaRef, Struct, StructType,
};

/// A stream of [`FileScanTask`].
pub type FileScanTaskStream = BoxStream<'static, Result<FileScanTask>>;

mod partition_serde {
    use std::result::Result as StdResult;

    use serde::ser::Error as _;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::{Literal, PrimitiveLiteral, Struct};

    /// A self-describing representation for the physical values in a partition struct.
    ///
    /// The logical types are carried separately by the task's schema and partition spec. Keeping
    /// this representation physical avoids duplicating that context while still distinguishing
    /// values such as `Int` and `Long`. Floats and 128-bit integers use byte representations so
    /// that every value can round-trip through JSON, including NaNs and infinities.
    #[derive(Serialize, Deserialize)]
    #[serde(tag = "type", content = "value", rename_all = "kebab-case")]
    enum SerializablePrimitiveLiteral {
        Boolean(bool),
        Int(i32),
        Long(i64),
        Float([u8; 4]),
        Double([u8; 8]),
        String(String),
        Binary(Vec<u8>),
        Int128([u8; 16]),
        UInt128([u8; 16]),
        AboveMax,
        BelowMin,
    }

    impl TryFrom<&Literal> for SerializablePrimitiveLiteral {
        type Error = &'static str;

        fn try_from(value: &Literal) -> StdResult<Self, Self::Error> {
            match value {
                Literal::Primitive(PrimitiveLiteral::Boolean(value)) => Ok(Self::Boolean(*value)),
                Literal::Primitive(PrimitiveLiteral::Int(value)) => Ok(Self::Int(*value)),
                Literal::Primitive(PrimitiveLiteral::Long(value)) => Ok(Self::Long(*value)),
                Literal::Primitive(PrimitiveLiteral::Float(value)) => {
                    Ok(Self::Float(value.to_bits().to_be_bytes()))
                }
                Literal::Primitive(PrimitiveLiteral::Double(value)) => {
                    Ok(Self::Double(value.to_bits().to_be_bytes()))
                }
                Literal::Primitive(PrimitiveLiteral::String(value)) => {
                    Ok(Self::String(value.clone()))
                }
                Literal::Primitive(PrimitiveLiteral::Binary(value)) => {
                    Ok(Self::Binary(value.clone()))
                }
                Literal::Primitive(PrimitiveLiteral::Int128(value)) => {
                    Ok(Self::Int128(value.to_be_bytes()))
                }
                Literal::Primitive(PrimitiveLiteral::UInt128(value)) => {
                    Ok(Self::UInt128(value.to_be_bytes()))
                }
                Literal::Primitive(PrimitiveLiteral::AboveMax) => Ok(Self::AboveMax),
                Literal::Primitive(PrimitiveLiteral::BelowMin) => Ok(Self::BelowMin),
                Literal::Struct(_) | Literal::List(_) | Literal::Map(_) => {
                    Err("partition structs can contain only primitive literal values")
                }
            }
        }
    }

    impl From<SerializablePrimitiveLiteral> for Literal {
        fn from(value: SerializablePrimitiveLiteral) -> Self {
            let value = match value {
                SerializablePrimitiveLiteral::Boolean(value) => PrimitiveLiteral::Boolean(value),
                SerializablePrimitiveLiteral::Int(value) => PrimitiveLiteral::Int(value),
                SerializablePrimitiveLiteral::Long(value) => PrimitiveLiteral::Long(value),
                SerializablePrimitiveLiteral::Float(value) => {
                    PrimitiveLiteral::Float(f32::from_bits(u32::from_be_bytes(value)).into())
                }
                SerializablePrimitiveLiteral::Double(value) => {
                    PrimitiveLiteral::Double(f64::from_bits(u64::from_be_bytes(value)).into())
                }
                SerializablePrimitiveLiteral::String(value) => PrimitiveLiteral::String(value),
                SerializablePrimitiveLiteral::Binary(value) => PrimitiveLiteral::Binary(value),
                SerializablePrimitiveLiteral::Int128(value) => {
                    PrimitiveLiteral::Int128(i128::from_be_bytes(value))
                }
                SerializablePrimitiveLiteral::UInt128(value) => {
                    PrimitiveLiteral::UInt128(u128::from_be_bytes(value))
                }
                SerializablePrimitiveLiteral::AboveMax => PrimitiveLiteral::AboveMax,
                SerializablePrimitiveLiteral::BelowMin => PrimitiveLiteral::BelowMin,
            };
            Literal::Primitive(value)
        }
    }

    pub(super) fn serialize<S>(
        partition: &Option<Struct>,
        serializer: S,
    ) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let partition = partition
            .as_ref()
            .map(|partition| {
                partition
                    .iter()
                    .map(|value| {
                        value
                            .map(SerializablePrimitiveLiteral::try_from)
                            .transpose()
                    })
                    .collect::<StdResult<Vec<_>, _>>()
            })
            .transpose()
            .map_err(S::Error::custom)?;

        partition.serialize(serializer)
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> StdResult<Option<Struct>, D::Error>
    where D: Deserializer<'de> {
        let partition =
            Option::<Vec<Option<SerializablePrimitiveLiteral>>>::deserialize(deserializer)?;

        Ok(partition.map(|partition| {
            partition
                .into_iter()
                .map(|value| value.map(Literal::from))
                .collect()
        }))
    }
}

/// A task to scan part of file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TypedBuilder)]
#[builder(field_defaults(setter(prefix = "with_")))]
pub struct FileScanTask {
    /// The total size of the data file in bytes, from the manifest entry.
    /// Used to skip a stat/HEAD request when reading Parquet footers.
    pub file_size_in_bytes: u64,
    /// The start offset of the file to scan.
    pub start: u64,
    /// The length of the file to scan.
    pub length: u64,
    /// The number of records in the file to scan.
    ///
    /// This is an optional field, and only available if we are
    /// reading the entire data file.
    #[builder(default)]
    pub record_count: Option<u64>,

    /// The first row id assigned to the data file.
    ///
    /// Used to derive the `_row_id` metadata column: for a row without an
    /// explicit `_row_id`, it is this value plus the row's ordinal position.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub first_row_id: Option<i64>,

    /// The data sequence number of the file, as opposed to its file sequence
    /// number: the sequence number preserved when a file is carried forward
    /// across a rewrite. May be null for an existing entry in a malformed
    /// manifest that lacks one.
    ///
    /// Used to derive the `_last_updated_sequence_number` metadata column.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub data_sequence_number: Option<i64>,

    /// The data file path corresponding to the task.
    pub data_file_path: String,

    /// The format of the file to scan.
    pub data_file_format: DataFileFormat,

    /// The schema of the file to scan.
    pub schema: SchemaRef,
    /// The field ids to project.
    pub project_field_ids: Vec<i32>,
    /// The predicate to filter.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub predicate: Option<BoundPredicate>,

    /// The list of delete files that may need to be applied to this data file
    #[builder(default)]
    pub deletes: Vec<FileScanTaskDeleteFile>,

    /// Partition data from the manifest entry, used to identify which columns can use
    /// constant values from partition metadata vs. reading from the data file.
    /// Per the Iceberg spec, only identity-transformed partition fields should use constants.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "partition_serde")]
    #[builder(default)]
    pub partition: Option<Struct>,

    /// The partition spec for this file, used to distinguish identity transforms
    /// (which use partition metadata constants) from non-identity transforms like
    /// bucket/truncate (which must read source columns from the data file).
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub partition_spec: Option<Arc<PartitionSpec>>,

    /// Name mapping from table metadata (property: schema.name-mapping.default),
    /// used to resolve field IDs from column names when Parquet files lack field IDs
    /// or have field ID conflicts.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub name_mapping: Option<Arc<NameMapping>>,

    /// The unified partition type across all specs in the table.
    /// When `RESERVED_FIELD_ID_PARTITION` is in the projected field IDs, the reader
    /// uses this type along with the task's partition_spec and partition data to
    /// materialize the `_partition` struct column at read time.
    ///
    /// This is a table-level value (same for all tasks in a scan), stored per-task
    /// so that readers are self-contained without needing back-pointers to table
    /// metadata. The cost is one Arc clone per task.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub unified_partition_type: Option<Arc<StructType>>,

    /// Whether this scan task should treat column names as case-sensitive when binding predicates.
    pub case_sensitive: bool,

    /// Key metadata for encrypted data files (Parquet Modular Encryption).
    /// When present, the reader uses this to build `FileDecryptionProperties`.
    ///
    /// Note on the trust boundary: for the standard encryption scheme this
    /// carries `StandardKeyMetadata`, whose payload is the *plaintext* DEK.
    /// Because `FileScanTask` derives `Serialize`, that plaintext DEK is part
    /// of the serialized scan plan should these tasks ever be serialized and sent
    /// over the network.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub key_metadata: Option<Box<[u8]>>,
}

impl FileScanTask {
    /// Returns the data file path of this file scan task.
    pub fn data_file_path(&self) -> &str {
        &self.data_file_path
    }

    /// Returns the project field id of this file scan task.
    pub fn project_field_ids(&self) -> &[i32] {
        &self.project_field_ids
    }

    /// Returns the predicate of this file scan task.
    pub fn predicate(&self) -> Option<&BoundPredicate> {
        self.predicate.as_ref()
    }

    /// Returns the schema of this file scan task as a reference
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Returns the schema of this file scan task as a SchemaRef
    pub fn schema_ref(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[derive(Debug)]
pub(crate) struct DeleteFileContext {
    pub(crate) manifest_entry: ManifestEntryRef,
    pub(crate) partition_spec_id: i32,
}

impl From<&DeleteFileContext> for FileScanTaskDeleteFile {
    fn from(ctx: &DeleteFileContext) -> Self {
        FileScanTaskDeleteFile::builder()
            .with_file_path(ctx.manifest_entry.file_path().to_string())
            .with_file_size_in_bytes(ctx.manifest_entry.file_size_in_bytes())
            .with_file_type(ctx.manifest_entry.content_type())
            .with_partition_spec_id(ctx.partition_spec_id)
            .with_equality_ids(ctx.manifest_entry.data_file.equality_ids.clone())
            .with_referenced_data_file(ctx.manifest_entry.data_file.referenced_data_file.clone())
            .with_content_offset(ctx.manifest_entry.data_file.content_offset)
            .with_content_size_in_bytes(ctx.manifest_entry.data_file.content_size_in_bytes)
            .with_record_count(Some(ctx.manifest_entry.record_count()))
            .with_key_metadata(
                ctx.manifest_entry
                    .data_file
                    .key_metadata
                    .as_deref()
                    .map(Box::from),
            )
            .build()
    }
}

/// A task to scan part of file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TypedBuilder)]
#[builder(field_defaults(setter(prefix = "with_")))]
pub struct FileScanTaskDeleteFile {
    /// The delete file path
    pub file_path: String,

    /// The total size of the delete file in bytes, from the manifest entry.
    pub file_size_in_bytes: u64,

    /// delete file type
    pub file_type: DataContentType,

    /// partition id
    pub partition_spec_id: i32,

    /// equality ids for equality deletes (null for anything other than equality-deletes)
    #[builder(default)]
    pub equality_ids: Option<Vec<i32>>,

    /// For a deletion vector, the location of the data file whose rows it deletes. Required for
    /// deletion vectors, and may also be set on a position delete file scoped to one data file.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub referenced_data_file: Option<String>,

    /// For a deletion vector, the offset of the blob within its Puffin file. Set only for
    /// deletion vectors, where it locates the blob for direct access.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub content_offset: Option<i64>,

    /// For a deletion vector, the length in bytes of the blob within its Puffin file.
    /// Required together with `content_offset`; both are absent for non-DV delete files.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub content_size_in_bytes: Option<i64>,

    /// The number of records in the delete file, from the manifest entry; for a deletion vector,
    /// the cardinality of its bitmap. `None` only for a task not built from a manifest entry.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub record_count: Option<u64>,

    /// Key metadata for encrypted delete files (Parquet Modular Encryption).
    /// When present, the reader uses this to build `FileDecryptionProperties`.
    ///
    /// Same plaintext-DEK trust boundary as [`FileScanTask::key_metadata`]:
    /// this is serialized into the scan plan and crosses the planner -> worker
    /// channel in the clear for the standard encryption scheme.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub key_metadata: Option<Box<[u8]>>,
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct PartitionWrapper {
        #[serde(with = "partition_serde")]
        partition: Option<Struct>,
    }

    #[test]
    fn test_partition_serde_round_trip_all_primitive_literals() {
        let partition = Struct::from_iter([
            Some(Literal::Primitive(PrimitiveLiteral::Boolean(true))),
            Some(Literal::Primitive(PrimitiveLiteral::Int(i32::MIN))),
            Some(Literal::Primitive(PrimitiveLiteral::Long(i64::MAX))),
            Some(Literal::Primitive(PrimitiveLiteral::Float(
                f32::INFINITY.into(),
            ))),
            Some(Literal::Primitive(PrimitiveLiteral::Double(
                f64::NEG_INFINITY.into(),
            ))),
            Some(Literal::Primitive(PrimitiveLiteral::String(
                "partition".to_string(),
            ))),
            Some(Literal::Primitive(PrimitiveLiteral::Binary(vec![
                0, 1, 255,
            ]))),
            Some(Literal::Primitive(PrimitiveLiteral::Int128(i128::MIN))),
            Some(Literal::Primitive(PrimitiveLiteral::UInt128(u128::MAX))),
            Some(Literal::Primitive(PrimitiveLiteral::AboveMax)),
            Some(Literal::Primitive(PrimitiveLiteral::BelowMin)),
            None,
        ]);
        let expected = PartitionWrapper {
            partition: Some(partition),
        };

        let serialized = serde_json::to_string(&expected).unwrap();
        let actual = serde_json::from_str(&serialized).unwrap();

        assert_eq!(expected, actual);
    }
}
