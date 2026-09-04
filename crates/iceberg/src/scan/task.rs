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

use crate::expr::BoundPredicate;
use crate::spec::{
    DataContentType, DataFileFormat, ManifestEntryRef, NameMapping, PartitionSpec, Schema,
    SchemaRef, Struct, StructType,
};
use crate::{Error, ErrorKind, Result};

/// A stream of [`FileScanTask`].
pub type FileScanTaskStream = BoxStream<'static, Result<FileScanTask>>;

/// A task to scan part of file.
#[derive(Debug, Clone, Deserialize, PartialEq, TypedBuilder)]
#[serde(try_from = "crate::scan::task::_serde::FileScanTaskSerde")]
#[builder(
    field_defaults(setter(prefix = "with_")),
    build_method(into = Result<FileScanTask>)
)]
pub struct FileScanTask {
    /// The total size of the data file in bytes, from the manifest entry.
    /// Used to skip a stat/HEAD request when reading Parquet footers.
    file_size_in_bytes: u64,
    /// The start offset of the file to scan.
    start: u64,
    /// The length of the file to scan.
    length: u64,
    /// The number of records in the file to scan.
    ///
    /// This is an optional field, and only available if we are
    /// reading the entire data file.
    #[builder(default)]
    record_count: Option<u64>,

    /// The first row id assigned to the data file.
    ///
    /// Used to derive the `_row_id` metadata column: for a row without an
    /// explicit `_row_id`, it is this value plus the row's ordinal position.
    #[builder(default)]
    first_row_id: Option<i64>,

    /// The data sequence number of the file, as opposed to its file sequence
    /// number: the sequence number preserved when a file is carried forward
    /// across a rewrite. May be null for an existing entry in a malformed
    /// manifest that lacks one.
    ///
    /// Used to derive the `_last_updated_sequence_number` metadata column.
    #[builder(default)]
    data_sequence_number: Option<i64>,

    /// The data file path corresponding to the task.
    data_file_path: String,

    /// The format of the file to scan.
    data_file_format: DataFileFormat,

    /// The schema of the file to scan.
    schema: SchemaRef,
    /// The field ids to project.
    project_field_ids: Vec<i32>,
    /// The predicate to filter.
    #[builder(default)]
    predicate: Option<BoundPredicate>,

    /// The list of delete files that may need to be applied to this data file
    #[builder(default)]
    deletes: Vec<FileScanTaskDeleteFile>,

    /// Partition data from the manifest entry, used to identify which columns can use
    /// constant values from partition metadata vs. reading from the data file.
    /// Per the Iceberg spec, only identity-transformed partition fields should use constants.
    #[builder(default)]
    partition: Option<Struct>,

    /// The partition spec for this file, used to distinguish identity transforms
    /// (which use partition metadata constants) from non-identity transforms like
    /// bucket/truncate (which must read source columns from the data file).
    #[builder(default)]
    partition_spec: Option<Arc<PartitionSpec>>,

    /// Name mapping from table metadata (property: schema.name-mapping.default),
    /// used to resolve field IDs from column names when Parquet files lack field IDs
    /// or have field ID conflicts.
    #[builder(default)]
    name_mapping: Option<Arc<NameMapping>>,

    /// The unified partition type across all specs in the table.
    /// When `RESERVED_FIELD_ID_PARTITION` is in the projected field IDs, the reader
    /// uses this type along with the task's partition_spec and partition data to
    /// materialize the `_partition` struct column at read time.
    ///
    /// This is a table-level value (same for all tasks in a scan), stored per-task
    /// so that readers are self-contained without needing back-pointers to table
    /// metadata. The cost is one Arc clone per task.
    #[builder(default)]
    unified_partition_type: Option<Arc<StructType>>,

    /// Whether this scan task should treat column names as case-sensitive when binding predicates.
    case_sensitive: bool,

    /// Key metadata for encrypted data files (Parquet Modular Encryption).
    /// When present, the reader uses this to build `FileDecryptionProperties`.
    ///
    /// Note on the trust boundary: for the standard encryption scheme this
    /// carries `StandardKeyMetadata`, whose payload is the *plaintext* DEK.
    /// Because `FileScanTask` implements [`Serialize`], that plaintext DEK is part
    /// of the serialized scan plan should these tasks ever be serialized and sent
    /// over the network.
    #[builder(default)]
    key_metadata: Option<Box<[u8]>>,
}

impl FileScanTask {
    /// Returns the total size of the data file in bytes.
    pub fn file_size_in_bytes(&self) -> u64 {
        self.file_size_in_bytes
    }

    /// Returns the start offset of the file to scan.
    pub fn start(&self) -> u64 {
        self.start
    }

    /// Returns the length of the file to scan.
    pub fn length(&self) -> u64 {
        self.length
    }

    /// Returns the number of records in the file when the whole file is scanned.
    pub fn record_count(&self) -> Option<u64> {
        self.record_count
    }

    /// Returns the first row id assigned to the data file.
    pub fn first_row_id(&self) -> Option<i64> {
        self.first_row_id
    }

    /// Returns the data sequence number of the file.
    pub fn data_sequence_number(&self) -> Option<i64> {
        self.data_sequence_number
    }

    /// Returns the data file path of this file scan task.
    pub fn data_file_path(&self) -> &str {
        &self.data_file_path
    }

    /// Returns the format of the data file.
    pub fn data_file_format(&self) -> DataFileFormat {
        self.data_file_format
    }

    /// Returns the schema of this file scan task as a reference.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Returns the schema of this file scan task as a [`SchemaRef`].
    pub fn schema_ref(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Returns the project field id of this file scan task.
    pub fn project_field_ids(&self) -> &[i32] {
        &self.project_field_ids
    }

    /// Returns the predicate of this file scan task.
    pub fn predicate(&self) -> Option<&BoundPredicate> {
        self.predicate.as_ref()
    }

    /// Returns the delete files that may need to be applied to the data file.
    pub fn deletes(&self) -> &[FileScanTaskDeleteFile] {
        &self.deletes
    }

    /// Returns the partition data from the manifest entry.
    pub fn partition(&self) -> Option<&Struct> {
        self.partition.as_ref()
    }

    /// Returns the partition spec for the data file.
    pub fn partition_spec(&self) -> Option<&Arc<PartitionSpec>> {
        self.partition_spec.as_ref()
    }

    /// Returns the name mapping used to resolve field ids.
    pub fn name_mapping(&self) -> Option<&Arc<NameMapping>> {
        self.name_mapping.as_ref()
    }

    /// Returns the unified partition type across all table partition specs.
    pub fn unified_partition_type(&self) -> Option<&Arc<StructType>> {
        self.unified_partition_type.as_ref()
    }

    /// Returns whether names are treated as case-sensitive.
    pub fn case_sensitive(&self) -> bool {
        self.case_sensitive
    }

    /// Returns the key metadata for the encrypted data file.
    pub fn key_metadata(&self) -> Option<&[u8]> {
        self.key_metadata.as_deref()
    }

    fn validate(&self) -> Result<()> {
        match (self.partition.as_ref(), self.partition_spec.as_deref()) {
            (None, None) => Ok(()),
            (None, Some(partition_spec)) if partition_spec.is_unpartitioned() => Ok(()),
            (None, Some(_)) => Err(Error::new(
                ErrorKind::DataInvalid,
                "FileScanTask with a partitioned spec requires partition values",
            )),
            (Some(partition), None) if partition.fields().is_empty() => Ok(()),
            (Some(_), None) => Err(Error::new(
                ErrorKind::DataInvalid,
                "Non-empty FileScanTask partition requires a partition spec",
            )),
            (Some(partition), Some(partition_spec))
                if partition.fields().len() != partition_spec.fields().len() =>
            {
                Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "FileScanTask partition has {} fields but partition spec has {} fields",
                        partition.fields().len(),
                        partition_spec.fields().len()
                    ),
                ))
            }
            (Some(_), Some(partition_spec)) => {
                partition_spec.partition_type(&self.schema)?;
                Ok(())
            }
        }
    }
}

impl From<FileScanTask> for Result<FileScanTask> {
    fn from(task: FileScanTask) -> Self {
        task.validate()?;
        Ok(task)
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
            .with_file_format(ctx.manifest_entry.data_file().file_format())
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

    /// The delete file's format, from the manifest entry. A `PositionDeletes` entry written as
    /// `Puffin` is a V3 deletion vector; one written as `Parquet` is a position delete file.
    pub file_format: DataFileFormat,

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

    /// Key metadata for an encrypted delete file. When present, the reader uses this to
    /// decrypt the file: for a Parquet equality or position delete file, this builds
    /// `FileDecryptionProperties` (Parquet Modular Encryption); for a deletion vector, whose
    /// Puffin file has no native encryption, this wraps the range read in an
    /// `EncryptedInputFile` (AGS1 stream encryption).
    ///
    /// Same plaintext-DEK trust boundary as [`FileScanTask::key_metadata`]:
    /// this is serialized into the scan plan and crosses the planner -> worker
    /// channel in the clear for the standard encryption scheme.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub key_metadata: Option<Box<[u8]>>,
}

mod _serde {
    use std::sync::Arc;

    use serde::{Deserialize, Serialize};

    use super::{FileScanTask, FileScanTaskDeleteFile};
    use crate::expr::BoundPredicate;
    use crate::spec::{
        DataFileFormat, Literal, NameMapping, PartitionSpec, RawLiteral, SchemaRef, StructType,
        Type,
    };
    use crate::{Error, ErrorKind, Result};

    #[derive(Deserialize)]
    pub(super) struct FileScanTaskSerde {
        file_size_in_bytes: u64,
        start: u64,
        length: u64,
        record_count: Option<u64>,
        first_row_id: Option<i64>,
        data_sequence_number: Option<i64>,
        data_file_path: String,
        data_file_format: DataFileFormat,
        schema: SchemaRef,
        project_field_ids: Vec<i32>,
        predicate: Option<BoundPredicate>,
        deletes: Vec<FileScanTaskDeleteFile>,
        #[serde(default)]
        partition: Option<RawLiteral>,
        #[serde(default)]
        partition_spec: Option<Arc<PartitionSpec>>,
        #[serde(default)]
        name_mapping: Option<Arc<NameMapping>>,
        #[serde(default)]
        unified_partition_type: Option<Arc<StructType>>,
        case_sensitive: bool,
        #[serde(default)]
        key_metadata: Option<Box<[u8]>>,
    }

    #[derive(Serialize)]
    struct FileScanTaskRefSerde<'a> {
        file_size_in_bytes: u64,
        start: u64,
        length: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        record_count: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        first_row_id: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        data_sequence_number: Option<i64>,
        data_file_path: &'a str,
        data_file_format: DataFileFormat,
        schema: &'a SchemaRef,
        project_field_ids: &'a [i32],
        #[serde(skip_serializing_if = "Option::is_none")]
        predicate: Option<&'a BoundPredicate>,
        deletes: &'a [FileScanTaskDeleteFile],
        #[serde(skip_serializing_if = "Option::is_none")]
        partition: Option<RawLiteral>,
        #[serde(skip_serializing_if = "Option::is_none")]
        partition_spec: Option<&'a Arc<PartitionSpec>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        name_mapping: Option<&'a Arc<NameMapping>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        unified_partition_type: Option<&'a Arc<StructType>>,
        case_sensitive: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        key_metadata: Option<&'a [u8]>,
    }

    fn partition_type(
        partition_spec: Option<&PartitionSpec>,
        schema: &crate::spec::Schema,
    ) -> Result<Type> {
        let partition_type = match partition_spec {
            Some(partition_spec) => partition_spec.partition_type(schema)?,
            None => PartitionSpec::unpartition_spec().partition_type(schema)?,
        };
        Ok(Type::Struct(partition_type))
    }

    impl<'a> TryFrom<&'a FileScanTask> for FileScanTaskRefSerde<'a> {
        type Error = Error;

        fn try_from(value: &'a FileScanTask) -> Result<Self> {
            let partition = value
                .partition
                .as_ref()
                .map(|partition| {
                    let partition_type =
                        partition_type(value.partition_spec.as_deref(), &value.schema)?;
                    RawLiteral::try_from(Literal::Struct(partition.clone()), &partition_type)
                })
                .transpose()?;

            Ok(Self {
                file_size_in_bytes: value.file_size_in_bytes,
                start: value.start,
                length: value.length,
                record_count: value.record_count,
                first_row_id: value.first_row_id,
                data_sequence_number: value.data_sequence_number,
                data_file_path: &value.data_file_path,
                data_file_format: value.data_file_format,
                schema: &value.schema,
                project_field_ids: &value.project_field_ids,
                predicate: value.predicate.as_ref(),
                deletes: &value.deletes,
                partition,
                partition_spec: value.partition_spec.as_ref(),
                name_mapping: value.name_mapping.as_ref(),
                unified_partition_type: value.unified_partition_type.as_ref(),
                case_sensitive: value.case_sensitive,
                key_metadata: value.key_metadata.as_deref(),
            })
        }
    }

    impl Serialize for FileScanTask {
        fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
        where S: serde::Serializer {
            FileScanTaskRefSerde::try_from(self)
                .map_err(serde::ser::Error::custom)?
                .serialize(serializer)
        }
    }

    impl TryFrom<FileScanTaskSerde> for FileScanTask {
        type Error = Error;

        fn try_from(value: FileScanTaskSerde) -> Result<Self> {
            let partition = value
                .partition
                .map(|partition| {
                    let partition_type =
                        partition_type(value.partition_spec.as_deref(), &value.schema)?;
                    let partition = partition.try_into(&partition_type).map_err(|err| {
                        if value.partition_spec.is_none() {
                            Error::new(
                                ErrorKind::DataInvalid,
                                "Non-empty FileScanTask partition requires a partition spec",
                            )
                            .with_source(err)
                        } else {
                            err
                        }
                    })?;
                    match partition {
                        Some(Literal::Struct(partition)) => Ok(partition),
                        _ => Err(Error::new(
                            ErrorKind::DataInvalid,
                            "FileScanTask partition must be a struct",
                        )),
                    }
                })
                .transpose()?;

            let task = Self {
                file_size_in_bytes: value.file_size_in_bytes,
                start: value.start,
                length: value.length,
                record_count: value.record_count,
                first_row_id: value.first_row_id,
                data_sequence_number: value.data_sequence_number,
                data_file_path: value.data_file_path,
                data_file_format: value.data_file_format,
                schema: value.schema,
                project_field_ids: value.project_field_ids,
                predicate: value.predicate,
                deletes: value.deletes,
                partition,
                partition_spec: value.partition_spec,
                name_mapping: value.name_mapping,
                unified_partition_type: value.unified_partition_type,
                case_sensitive: value.case_sensitive,
                key_metadata: value.key_metadata,
            };
            task.validate()?;
            Ok(task)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorKind;
    use crate::spec::{Literal, NestedField, PrimitiveType, Transform, Type};

    fn build_file_scan_task(
        schema: SchemaRef,
        partition: Option<Struct>,
        partition_spec: Option<Arc<PartitionSpec>>,
    ) -> Result<FileScanTask> {
        FileScanTask::builder()
            .with_file_size_in_bytes(100)
            .with_start(0)
            .with_length(100)
            .with_data_file_path("data_file_path".to_string())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema)
            .with_project_field_ids(vec![])
            .with_partition(partition)
            .with_partition_spec(partition_spec)
            .with_case_sensitive(false)
            .build()
    }

    fn schema_and_spec(
        primitive_type: PrimitiveType,
        transform: Transform,
    ) -> (SchemaRef, Arc<PartitionSpec>) {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "x",
                    Type::Primitive(primitive_type),
                ))])
                .build()
                .unwrap(),
        );
        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .add_partition_field("x", "x_partition", transform)
                .unwrap()
                .build()
                .unwrap(),
        );
        (schema, partition_spec)
    }

    #[test]
    fn test_file_scan_task_builder_rejects_non_empty_partition_without_spec() {
        // Regression test for https://github.com/apache/iceberg-rust/issues/3130.
        let err = build_file_scan_task(
            Arc::new(Schema::builder().build().unwrap()),
            Some(Struct::from_iter([Some(Literal::long(42))])),
            None,
        )
        .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            err.message(),
            "Non-empty FileScanTask partition requires a partition spec"
        );
    }

    #[test]
    fn test_file_scan_task_builder_accepts_empty_partition_without_spec() {
        build_file_scan_task(
            Arc::new(Schema::builder().build().unwrap()),
            Some(Struct::empty()),
            None,
        )
        .unwrap();
    }

    #[test]
    fn test_file_scan_task_builder_rejects_partitioned_spec_without_partition() {
        let (schema, partition_spec) = schema_and_spec(PrimitiveType::Long, Transform::Identity);

        let err = build_file_scan_task(schema, None, Some(partition_spec)).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            err.message(),
            "FileScanTask with a partitioned spec requires partition values"
        );
    }

    #[test]
    fn test_file_scan_task_builder_accepts_unpartitioned_spec_without_partition() {
        build_file_scan_task(
            Arc::new(Schema::builder().build().unwrap()),
            None,
            Some(Arc::new(PartitionSpec::unpartition_spec())),
        )
        .unwrap();
    }

    #[test]
    fn test_file_scan_task_builder_rejects_partition_arity_mismatch() {
        let (schema, partition_spec) = schema_and_spec(PrimitiveType::Long, Transform::Identity);

        let err =
            build_file_scan_task(schema, Some(Struct::empty()), Some(partition_spec)).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("partition has 0 fields"));
        assert!(err.message().contains("partition spec has 1 fields"));
    }

    #[test]
    fn test_file_scan_task_builder_rejects_dropped_partition_source_column() {
        let (_historical_schema, partition_spec) =
            schema_and_spec(PrimitiveType::Long, Transform::Identity);
        let current_schema = Arc::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    2,
                    "y",
                    Type::Primitive(PrimitiveType::String),
                ))])
                .build()
                .unwrap(),
        );

        let err = build_file_scan_task(
            current_schema,
            Some(Struct::from_iter([Some(Literal::long(42))])),
            Some(partition_spec),
        )
        .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(err.message().contains("No column with source column id 1"));
    }

    #[test]
    fn test_file_scan_task_builder_rejects_partition_spec_incompatible_with_schema() {
        let (_historical_schema, partition_spec) =
            schema_and_spec(PrimitiveType::Timestamp, Transform::Day);
        let current_schema = Arc::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "x",
                    Type::Primitive(PrimitiveType::String),
                ))])
                .build()
                .unwrap(),
        );

        let err = build_file_scan_task(
            current_schema,
            Some(Struct::from_iter([Some(Literal::date(20_000))])),
            Some(partition_spec),
        )
        .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }
}
