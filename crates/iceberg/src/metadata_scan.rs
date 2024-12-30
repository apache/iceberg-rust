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

//! Metadata table api.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{
    make_builder, ArrayBuilder, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, GenericBinaryBuilder, Int32Builder,
    Int64Builder, Int8Builder, LargeBinaryBuilder, ListBuilder, MapBuilder, PrimitiveBuilder,
    StringBuilder, StructBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
    TimestampNanosecondBuilder,
};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, Float32Type, Float64Type, Int32Type, Int64Type, Int8Type,
    Time64MicrosecondType, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, UInt64Type,
};
use arrow_array::{
    ArrayRef, ArrowPrimitiveType, Datum as ArrowDatum, OffsetSizeTrait, RecordBatch, StructArray,
};
use arrow_schema::{DataType, Field, FieldRef, Fields, Schema, TimeUnit};
use itertools::Itertools;
use ordered_float::OrderedFloat;

use crate::arrow::{get_arrow_datum, schema_to_arrow_schema, type_to_arrow_type};
use crate::spec::{
    DataFile, Datum, Literal, ManifestFile, PartitionField, PartitionSpec, PrimitiveLiteral,
    SchemaRef, Struct, TableMetadata,
};
use crate::table::Table;
use crate::{Error, ErrorKind, Result};

/// Metadata table is used to inspect a table's history, snapshots, and other metadata as a table.
///
/// References:
/// - <https://github.com/apache/iceberg/blob/ac865e334e143dfd9e33011d8cf710b46d91f1e5/core/src/main/java/org/apache/iceberg/MetadataTableType.java#L23-L39>
/// - <https://iceberg.apache.org/docs/latest/spark-queries/#querying-with-sql>
/// - <https://py.iceberg.apache.org/api/#inspecting-tables>
#[derive(Debug)]
pub struct MetadataTable<'a>(&'a Table);

impl<'a> MetadataTable<'a> {
    /// Creates a new metadata scan.
    pub(super) fn new(table: &'a Table) -> Self {
        Self(table)
    }

    /// Get the snapshots table.
    pub fn snapshots(&self) -> SnapshotsTable {
        SnapshotsTable { table: self.0 }
    }

    /// Returns the current manifest file's entries.
    pub fn entries(&self) -> EntriesTable {
        EntriesTable { table: self.0 }
    }

    /// Get the manifests table.
    pub fn manifests(&self) -> ManifestsTable {
        ManifestsTable { table: self.0 }
    }
}

/// Snapshots table.
pub struct SnapshotsTable<'a> {
    table: &'a Table,
}

impl<'a> SnapshotsTable<'a> {
    /// Returns the schema of the snapshots table.
    pub fn schema(&self) -> Schema {
        Schema::new(vec![
            Field::new(
                "committed_at",
                DataType::Timestamp(TimeUnit::Millisecond, Some("+00:00".into())),
                false,
            ),
            Field::new("snapshot_id", DataType::Int64, false),
            Field::new("parent_id", DataType::Int64, true),
            Field::new("operation", DataType::Utf8, false),
            Field::new("manifest_list", DataType::Utf8, false),
            Field::new(
                "summary",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("keys", DataType::Utf8, false),
                                Field::new("values", DataType::Utf8, true),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                false,
            ),
        ])
    }

    /// Scans the snapshots table.
    pub fn scan(&self) -> Result<RecordBatch> {
        let mut committed_at =
            PrimitiveBuilder::<TimestampMillisecondType>::new().with_timezone("+00:00");
        let mut snapshot_id = PrimitiveBuilder::<Int64Type>::new();
        let mut parent_id = PrimitiveBuilder::<Int64Type>::new();
        let mut operation = StringBuilder::new();
        let mut manifest_list = StringBuilder::new();
        let mut summary = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());

        for snapshot in self.table.metadata().snapshots() {
            committed_at.append_value(snapshot.timestamp_ms());
            snapshot_id.append_value(snapshot.snapshot_id());
            parent_id.append_option(snapshot.parent_snapshot_id());
            manifest_list.append_value(snapshot.manifest_list());
            operation.append_value(snapshot.summary().operation.as_str());
            for (key, value) in &snapshot.summary().additional_properties {
                summary.keys().append_value(key);
                summary.values().append_value(value);
            }
            summary.append(true)?;
        }

        Ok(RecordBatch::try_new(Arc::new(self.schema()), vec![
            Arc::new(committed_at.finish()),
            Arc::new(snapshot_id.finish()),
            Arc::new(parent_id.finish()),
            Arc::new(operation.finish()),
            Arc::new(manifest_list.finish()),
            Arc::new(summary.finish()),
        ])?)
    }
}

/// Entries table containing the manifest file's entries.
///
/// The table has one row for each manifest file entry in the current snapshot's manifest list file.
pub struct EntriesTable<'a> {
    table: &'a Table,
}

impl<'a> EntriesTable<'a> {
    /// Get the schema for the manifest entries table.
    pub fn schema(&self) -> Schema {
        // https://github.com/apache/iceberg-python/blob/0e5086ceb77351bc0b6ec3a592f5eda70a0afe46/pyiceberg/table/inspect.py#L92
        // https://github.com/apache/iceberg/blob/8a70fe0ff5f241aec8856f8091c77fdce35ad256/core/src/main/java/org/apache/iceberg/BaseEntriesTable.java#L46-L57
        Schema::new(vec![
            Field::new("status", DataType::Int32, false),
            Field::new("snapshot_id", DataType::Int64, true),
            Field::new("sequence_number", DataType::Int64, true),
            Field::new("file_sequence_number", DataType::Int64, true),
            Field::new(
                "data_file",
                DataType::Struct(DataFileStructBuilder::fields(self.table.metadata())),
                false,
            ),
            Field::new(
                "readable_metrics",
                DataType::Struct(
                    ReadableMetricsStructBuilder::fields(self.table.metadata().current_schema())
                        .expect("Failed to build schema for readable metrics"),
                ),
                false,
            ),
        ])
    }

    /// Scan the manifest entries table.
    pub async fn scan(&self) -> Result<RecordBatch> {
        let table_metadata = self.table.metadata();
        let file_io = self.table.file_io();
        let current_snapshot = table_metadata.current_snapshot().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "Cannot scan entries for table without current snapshot",
            )
        })?;

        let mut status = PrimitiveBuilder::<Int32Type>::new();
        let mut snapshot_id = PrimitiveBuilder::<Int64Type>::new();
        let mut sequence_number = PrimitiveBuilder::<Int64Type>::new();
        let mut file_sequence_number = PrimitiveBuilder::<Int64Type>::new();
        let mut data_file = DataFileStructBuilder::new(table_metadata);
        let mut readable_metrics =
            ReadableMetricsStructBuilder::new(table_metadata.current_schema())?;
        for manifest_file in current_snapshot
            .load_manifest_list(file_io, table_metadata)
            .await?
            .entries()
        {
            for manifest_entry in manifest_file.load_manifest(file_io).await?.entries() {
                status.append_value(manifest_entry.status() as i32);
                snapshot_id.append_option(manifest_entry.snapshot_id());
                sequence_number.append_option(manifest_entry.sequence_number());
                file_sequence_number.append_option(manifest_entry.file_sequence_number());
                data_file.append(manifest_file, manifest_entry.data_file())?;
                readable_metrics.append(manifest_entry.data_file())?;
            }
        }

        Ok(RecordBatch::try_new(Arc::new(self.schema()), vec![
            Arc::new(status.finish()),
            Arc::new(snapshot_id.finish()),
            Arc::new(sequence_number.finish()),
            Arc::new(file_sequence_number.finish()),
            Arc::new(data_file.finish()),
            Arc::new(readable_metrics.finish()),
        ])?)
    }
}

/// Manifests table.
pub struct ManifestsTable<'a> {
    table: &'a Table,
}

impl<'a> ManifestsTable<'a> {
    fn partition_summary_fields(&self) -> Vec<Field> {
        vec![
            Field::new("contains_null", DataType::Boolean, false),
            Field::new("contains_nan", DataType::Boolean, true),
            Field::new("lower_bound", DataType::Utf8, true),
            Field::new("upper_bound", DataType::Utf8, true),
        ]
    }

    /// Returns the schema of the manifests table.
    pub fn schema(&self) -> Schema {
        Schema::new(vec![
            Field::new("content", DataType::Int8, false),
            Field::new("path", DataType::Utf8, false),
            Field::new("length", DataType::Int64, false),
            Field::new("partition_spec_id", DataType::Int32, false),
            Field::new("added_snapshot_id", DataType::Int64, false),
            Field::new("added_data_files_count", DataType::Int32, false),
            Field::new("existing_data_files_count", DataType::Int32, false),
            Field::new("deleted_data_files_count", DataType::Int32, false),
            Field::new("added_delete_files_count", DataType::Int32, false),
            Field::new("existing_delete_files_count", DataType::Int32, false),
            Field::new("deleted_delete_files_count", DataType::Int32, false),
            Field::new(
                "partition_summaries",
                DataType::List(Arc::new(Field::new_struct(
                    "item",
                    self.partition_summary_fields(),
                    false,
                ))),
                false,
            ),
        ])
    }

    /// Scans the manifests table.
    pub async fn scan(&self) -> Result<RecordBatch> {
        let mut content = PrimitiveBuilder::<Int8Type>::new();
        let mut path = StringBuilder::new();
        let mut length = PrimitiveBuilder::<Int64Type>::new();
        let mut partition_spec_id = PrimitiveBuilder::<Int32Type>::new();
        let mut added_snapshot_id = PrimitiveBuilder::<Int64Type>::new();
        let mut added_data_files_count = PrimitiveBuilder::<Int32Type>::new();
        let mut existing_data_files_count = PrimitiveBuilder::<Int32Type>::new();
        let mut deleted_data_files_count = PrimitiveBuilder::<Int32Type>::new();
        let mut added_delete_files_count = PrimitiveBuilder::<Int32Type>::new();
        let mut existing_delete_files_count = PrimitiveBuilder::<Int32Type>::new();
        let mut deleted_delete_files_count = PrimitiveBuilder::<Int32Type>::new();
        let mut partition_summaries = ListBuilder::new(StructBuilder::from_fields(
            Fields::from(self.partition_summary_fields()),
            0,
        ))
        .with_field(Arc::new(Field::new_struct(
            "item",
            self.partition_summary_fields(),
            false,
        )));

        if let Some(snapshot) = self.table.metadata().current_snapshot() {
            let manifest_list = snapshot
                .load_manifest_list(self.table.file_io(), &self.table.metadata_ref())
                .await?;
            for manifest in manifest_list.entries() {
                content.append_value(manifest.content as i8);
                path.append_value(manifest.manifest_path.clone());
                length.append_value(manifest.manifest_length);
                partition_spec_id.append_value(manifest.partition_spec_id);
                added_snapshot_id.append_value(manifest.added_snapshot_id);
                added_data_files_count.append_value(manifest.added_files_count.unwrap_or(0) as i32);
                existing_data_files_count
                    .append_value(manifest.existing_files_count.unwrap_or(0) as i32);
                deleted_data_files_count
                    .append_value(manifest.deleted_files_count.unwrap_or(0) as i32);
                added_delete_files_count
                    .append_value(manifest.added_files_count.unwrap_or(0) as i32);
                existing_delete_files_count
                    .append_value(manifest.existing_files_count.unwrap_or(0) as i32);
                deleted_delete_files_count
                    .append_value(manifest.deleted_files_count.unwrap_or(0) as i32);

                let partition_summaries_builder = partition_summaries.values();
                for summary in &manifest.partitions {
                    partition_summaries_builder
                        .field_builder::<BooleanBuilder>(0)
                        .unwrap()
                        .append_value(summary.contains_null);
                    partition_summaries_builder
                        .field_builder::<BooleanBuilder>(1)
                        .unwrap()
                        .append_option(summary.contains_nan);
                    partition_summaries_builder
                        .field_builder::<StringBuilder>(2)
                        .unwrap()
                        .append_option(summary.lower_bound.as_ref().map(|v| v.to_string()));
                    partition_summaries_builder
                        .field_builder::<StringBuilder>(3)
                        .unwrap()
                        .append_option(summary.upper_bound.as_ref().map(|v| v.to_string()));
                    partition_summaries_builder.append(true);
                }
                partition_summaries.append(true);
            }
        }

        Ok(RecordBatch::try_new(Arc::new(self.schema()), vec![
            Arc::new(content.finish()),
            Arc::new(path.finish()),
            Arc::new(length.finish()),
            Arc::new(partition_spec_id.finish()),
            Arc::new(added_snapshot_id.finish()),
            Arc::new(added_data_files_count.finish()),
            Arc::new(existing_data_files_count.finish()),
            Arc::new(deleted_data_files_count.finish()),
            Arc::new(added_delete_files_count.finish()),
            Arc::new(existing_delete_files_count.finish()),
            Arc::new(deleted_delete_files_count.finish()),
            Arc::new(partition_summaries.finish()),
        ])?)
    }
}

struct DataFileStructBuilder<'a> {
    // Need to keep table metadata to retrieve partition specs based on partition spec ids
    table_metadata: &'a TableMetadata,
    // Below are the field builders of the "data_file" struct
    content: Int8Builder,
    file_path: StringBuilder,
    file_format: StringBuilder,
    partition: PartitionValuesStructBuilder,
    record_count: PrimitiveBuilder<UInt64Type>,
    file_size_in_bytes: PrimitiveBuilder<UInt64Type>,
    column_sizes: MapBuilder<Int32Builder, PrimitiveBuilder<UInt64Type>>,
    value_counts: MapBuilder<Int32Builder, PrimitiveBuilder<UInt64Type>>,
    null_value_counts: MapBuilder<Int32Builder, PrimitiveBuilder<UInt64Type>>,
    nan_value_counts: MapBuilder<Int32Builder, PrimitiveBuilder<UInt64Type>>,
    lower_bounds: MapBuilder<Int32Builder, BinaryBuilder>,
    upper_bounds: MapBuilder<Int32Builder, BinaryBuilder>,
    key_metadata: BinaryBuilder,
    split_offsets: ListBuilder<Int64Builder>,
    equality_ids: ListBuilder<Int32Builder>,
    sort_order_ids: Int32Builder,
}

impl<'a> DataFileStructBuilder<'a> {
    fn new(table_metadata: &'a TableMetadata) -> Self {
        Self {
            table_metadata,
            content: Int8Builder::new(),
            file_path: StringBuilder::new(),
            file_format: StringBuilder::new(),
            partition: PartitionValuesStructBuilder::new(table_metadata),
            record_count: PrimitiveBuilder::new(),
            file_size_in_bytes: PrimitiveBuilder::new(),
            column_sizes: MapBuilder::new(None, Int32Builder::new(), PrimitiveBuilder::new()),
            value_counts: MapBuilder::new(None, Int32Builder::new(), PrimitiveBuilder::new()),
            null_value_counts: MapBuilder::new(None, Int32Builder::new(), PrimitiveBuilder::new()),
            nan_value_counts: MapBuilder::new(None, Int32Builder::new(), PrimitiveBuilder::new()),
            lower_bounds: MapBuilder::new(None, Int32Builder::new(), BinaryBuilder::new()),
            upper_bounds: MapBuilder::new(None, Int32Builder::new(), BinaryBuilder::new()),
            key_metadata: BinaryBuilder::new(),
            split_offsets: ListBuilder::new(PrimitiveBuilder::new()),
            equality_ids: ListBuilder::new(PrimitiveBuilder::new()),
            sort_order_ids: Int32Builder::new(),
        }
    }

    fn fields(table_metadata: &TableMetadata) -> Fields {
        vec![
            Field::new("content", DataType::Int8, false),
            Field::new("file_path", DataType::Utf8, false),
            Field::new("file_format", DataType::Utf8, false),
            Field::new(
                "partition",
                DataType::Struct(PartitionValuesStructBuilder::combined_partition_fields(
                    table_metadata,
                )),
                false,
            ),
            Field::new("record_count", DataType::UInt64, false),
            Field::new("file_size_in_bytes", DataType::UInt64, false),
            Field::new(
                "column_sizes",
                column_id_to_value_type(DataType::UInt64),
                true,
            ),
            Field::new(
                "value_counts",
                column_id_to_value_type(DataType::UInt64),
                true,
            ),
            Field::new(
                "null_value_counts",
                column_id_to_value_type(DataType::UInt64),
                true,
            ),
            Field::new(
                "nan_value_counts",
                column_id_to_value_type(DataType::UInt64),
                true,
            ),
            Field::new(
                "lower_bounds",
                column_id_to_value_type(DataType::Binary),
                true,
            ),
            Field::new(
                "upper_bounds",
                column_id_to_value_type(DataType::Binary),
                true,
            ),
            Field::new("key_metadata", DataType::Binary, true),
            Field::new(
                "split_offsets",
                DataType::new_list(DataType::Int64, true),
                true,
            ),
            Field::new(
                "equality_ids",
                DataType::new_list(DataType::Int32, true),
                true,
            ),
            Field::new("sort_order_id", DataType::Int32, true),
        ]
        .into()
    }

    fn append(&mut self, manifest_file: &ManifestFile, data_file: &DataFile) -> Result<()> {
        self.content.append_value(data_file.content as i8);
        self.file_path.append_value(data_file.file_path());
        self.file_format
            .append_value(data_file.file_format().to_string().to_uppercase());
        self.partition.append(
            self.partition_spec(manifest_file)?.clone().fields(),
            data_file.partition(),
        )?;
        self.record_count.append_value(data_file.record_count());
        self.file_size_in_bytes
            .append_value(data_file.file_size_in_bytes());

        self.column_sizes.append_map(data_file.column_sizes())?;
        self.value_counts.append_map(data_file.value_counts())?;
        self.null_value_counts
            .append_map(data_file.null_value_counts())?;
        self.nan_value_counts
            .append_map(data_file.nan_value_counts())?;
        self.lower_bounds.append_map(data_file.lower_bounds())?;
        self.upper_bounds.append_map(data_file.upper_bounds())?;
        self.key_metadata.append_option(data_file.key_metadata());
        self.split_offsets.append_list(data_file.split_offsets());
        self.equality_ids.append_list(data_file.equality_ids());
        self.sort_order_ids.append_option(data_file.sort_order_id());
        Ok(())
    }

    fn partition_spec(&self, manifest_file: &ManifestFile) -> Result<&PartitionSpec> {
        self.table_metadata
            .partition_spec_by_id(manifest_file.partition_spec_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Partition spec not found for manifest file",
                )
            })
            .map(|spec| spec.as_ref())
    }

    fn finish(&mut self) -> StructArray {
        let inner_arrays: Vec<ArrayRef> = vec![
            Arc::new(self.content.finish()),
            Arc::new(self.file_path.finish()),
            Arc::new(self.file_format.finish()),
            Arc::new(self.partition.finish()),
            Arc::new(self.record_count.finish()),
            Arc::new(self.file_size_in_bytes.finish()),
            Arc::new(self.column_sizes.finish()),
            Arc::new(self.value_counts.finish()),
            Arc::new(self.null_value_counts.finish()),
            Arc::new(self.nan_value_counts.finish()),
            Arc::new(self.lower_bounds.finish()),
            Arc::new(self.upper_bounds.finish()),
            Arc::new(self.key_metadata.finish()),
            Arc::new(self.split_offsets.finish()),
            Arc::new(self.equality_ids.finish()),
            Arc::new(self.sort_order_ids.finish()),
        ];

        StructArray::from(
            Self::fields(self.table_metadata)
                .into_iter()
                .cloned()
                .zip_eq(inner_arrays)
                .collect::<Vec<(FieldRef, ArrayRef)>>(),
        )
    }
}

struct PartitionValuesStructBuilder {
    combined_fields: Fields,
    partition_builders: Vec<Box<dyn ArrayBuilder>>,
}

impl PartitionValuesStructBuilder {
    fn new(table_metadata: &TableMetadata) -> Self {
        let combined_fields = Self::combined_partition_fields(table_metadata);
        Self {
            partition_builders: combined_fields
                .iter()
                .map(|field| make_builder(field.data_type(), 0))
                .collect_vec(),
            combined_fields,
        }
    }

    /// Build the combined partition spec unioning past and current partition specs
    fn combined_partition_fields(table_metadata: &TableMetadata) -> Fields {
        let combined_fields: HashMap<i32, &PartitionField> = table_metadata
            .partition_specs_iter()
            .flat_map(|spec| spec.fields())
            .map(|field| (field.field_id, field))
            .collect();

        combined_fields
            .into_iter()
            // Sort by field id to get a deterministic order
            .sorted_by_key(|(id, _)| *id)
            .map(|(_, field)| {
                let source_type = &table_metadata
                    .current_schema()
                    .field_by_id(field.source_id)
                    .unwrap()
                    .field_type;
                let result_type = field.transform.result_type(source_type).unwrap();
                Field::new(
                    field.name.clone(),
                    type_to_arrow_type(&result_type).unwrap(),
                    true,
                )
            })
            .collect()
    }

    fn append(
        &mut self,
        partition_fields: &[PartitionField],
        partition_values: &Struct,
    ) -> Result<()> {
        for (field, value) in partition_fields.iter().zip_eq(partition_values.iter()) {
            let index = self.find_field(&field.name)?;
            let data_type = self.combined_fields[index].data_type();

            match value {
                Some(literal) => {
                    self.partition_builders[index].append_literal(data_type, literal)?
                }
                None => self.partition_builders[index].append_null(data_type)?,
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> StructArray {
        let arrays: Vec<ArrayRef> = self
            .partition_builders
            .iter_mut()
            .map::<ArrayRef, _>(|builder| Arc::new(builder.finish()))
            .collect();
        StructArray::from(
            self.combined_fields
                .iter()
                .cloned()
                .zip_eq(arrays)
                .collect::<Vec<(FieldRef, ArrayRef)>>(),
        )
    }

    fn find_field(&self, name: &str) -> Result<usize> {
        match self.combined_fields.find(name) {
            Some((index, _)) => Ok(index),
            None => Err(Error::new(
                ErrorKind::Unexpected,
                format!("Field not found: {}", name),
            )),
        }
    }
}

struct ReadableMetricsStructBuilder<'a> {
    table_schema: &'a SchemaRef,
    column_builders: Vec<PerColumnReadableMetricsBuilder>,
}

impl<'a> ReadableMetricsStructBuilder<'a> {
    /// Helper to construct per-column readable metrics. The metrics are "readable" in that the reported
    /// and lower and upper bounds are reported as deserialized values.
    fn fields(table_schema: &SchemaRef) -> Result<Fields> {
        let arrow_schema = schema_to_arrow_schema(table_schema)?;

        Ok(arrow_schema
            .fields()
            .iter()
            .map(|field| {
                Field::new(
                    field.name(),
                    DataType::Struct(PerColumnReadableMetricsBuilder::fields(field.data_type())),
                    false,
                )
            })
            .collect_vec()
            .into())
    }

    fn new(table_schema: &'a SchemaRef) -> Result<ReadableMetricsStructBuilder> {
        Ok(Self {
            table_schema,
            column_builders: table_schema
                .as_struct()
                .fields()
                .iter()
                .map(|field| {
                    type_to_arrow_type(&field.field_type).map(|arrow_type| {
                        PerColumnReadableMetricsBuilder::new_for_field(field.id, &arrow_type)
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        })
    }

    fn append(&mut self, data_file: &DataFile) -> Result<()> {
        for column_builder in &mut self.column_builders {
            column_builder.append(data_file)?;
        }
        Ok(())
    }

    fn finish(&mut self) -> StructArray {
        let fields: Vec<FieldRef> = Self::fields(self.table_schema)
            // We already checked the schema conversion in the constructor
            .unwrap()
            .into_iter()
            .cloned()
            .collect();
        let arrays: Vec<ArrayRef> = self
            .column_builders
            .iter_mut()
            .map::<ArrayRef, _>(|builder| Arc::new(builder.finish()))
            .collect();
        StructArray::from(
            fields
                .into_iter()
                .zip_eq(arrays)
                .collect::<Vec<(FieldRef, ArrayRef)>>(),
        )
    }
}

/// Builds a readable metrics struct for a single column.
struct PerColumnReadableMetricsBuilder {
    field_id: i32,
    data_type: DataType,
    column_size: PrimitiveBuilder<UInt64Type>,
    value_count: PrimitiveBuilder<UInt64Type>,
    null_value_count: PrimitiveBuilder<UInt64Type>,
    nan_value_count: PrimitiveBuilder<UInt64Type>,
    lower_bound: Box<dyn ArrayBuilder>,
    upper_bound: Box<dyn ArrayBuilder>,
}

impl PerColumnReadableMetricsBuilder {
    fn fields(data_type: &DataType) -> Fields {
        vec![
            Field::new("column_size", DataType::UInt64, true),
            Field::new("value_count", DataType::UInt64, true),
            Field::new("null_value_count", DataType::UInt64, true),
            Field::new("nan_value_count", DataType::UInt64, true),
            Field::new("lower_bound", data_type.clone(), true),
            Field::new("upper_bound", data_type.clone(), true),
        ]
        .into()
    }

    fn new_for_field(field_id: i32, data_type: &DataType) -> Self {
        Self {
            field_id,
            data_type: data_type.clone(),
            column_size: PrimitiveBuilder::new(),
            value_count: PrimitiveBuilder::new(),
            null_value_count: PrimitiveBuilder::new(),
            nan_value_count: PrimitiveBuilder::new(),
            lower_bound: make_builder(data_type, 0),
            upper_bound: make_builder(data_type, 0),
        }
    }

    fn append(&mut self, data_file: &DataFile) -> Result<()> {
        self.column_size
            .append_option(data_file.column_sizes().get(&self.field_id).cloned());
        self.value_count
            .append_option(data_file.value_counts().get(&self.field_id).cloned());
        self.null_value_count
            .append_option(data_file.null_value_counts().get(&self.field_id).cloned());
        self.nan_value_count
            .append_option(data_file.nan_value_counts().get(&self.field_id).cloned());
        match data_file.lower_bounds().get(&self.field_id) {
            Some(datum) => self
                .lower_bound
                .append_datum(get_arrow_datum(datum)?.as_ref())?,
            None => self.lower_bound.append_null(&self.data_type)?,
        }
        match data_file.upper_bounds().get(&self.field_id) {
            Some(datum) => self
                .upper_bound
                .append_datum(get_arrow_datum(datum)?.as_ref())?,
            None => self.upper_bound.append_null(&self.data_type)?,
        }
        Ok(())
    }

    fn finish(&mut self) -> StructArray {
        let inner_arrays: Vec<ArrayRef> = vec![
            Arc::new(self.column_size.finish()),
            Arc::new(self.value_count.finish()),
            Arc::new(self.null_value_count.finish()),
            Arc::new(self.nan_value_count.finish()),
            Arc::new(self.lower_bound.finish()),
            Arc::new(self.upper_bound.finish()),
        ];

        StructArray::from(
            Self::fields(&self.data_type)
                .into_iter()
                .cloned()
                .zip_eq(inner_arrays)
                .collect::<Vec<(FieldRef, ArrayRef)>>(),
        )
    }
}

/// Construct a new struct type that maps from column ids (i32) to the provided value type.
/// Keys, values, and the whole struct are non-nullable.
fn column_id_to_value_type(value_type: DataType) -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("keys", DataType::Int32, false),
                    Field::new("values", value_type, true),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}

/// Utility to append slices to a [ListBuilder].
trait ListBuilderExt<T> {
    /// Append the slice and finish the current array slot.
    fn append_list(&mut self, list: &[T]);
}

/// Utility to append slices of primitive values to a [ListBuilder]
impl<T: ArrowPrimitiveType> ListBuilderExt<T::Native> for ListBuilder<PrimitiveBuilder<T>> {
    fn append_list(&mut self, list: &[T::Native]) {
        self.values().append_slice(list);
        self.append(true)
    }
}

/// Utility to append [HashMap] instances to [MapBuilder].
trait MapBuilderExt<K, V> {
    /// Append the map to this builder and finish the current array slot.
    fn append_map(&mut self, map: &HashMap<K, V>) -> Result<()>;
}

/// Utility to append a [HashMap] of primitive values to a [MapBuilder].
impl<K: ArrowPrimitiveType, V: ArrowPrimitiveType> MapBuilderExt<K::Native, V::Native>
    for MapBuilder<PrimitiveBuilder<K>, PrimitiveBuilder<V>>
where K::Native: Ord
{
    fn append_map(&mut self, map: &HashMap<K::Native, V::Native>) -> Result<()> {
        // Order by keys to get deterministic and matching order between rows
        for (k, v) in map.iter().sorted_by_key(|(k, _)| *k) {
            self.keys().append_value(*k);
            self.values().append_value(*v);
        }
        self.append(true)?;
        Ok(())
    }
}

/// Utility to append a [HashMap] of [Datum] values to a [MapBuilder].
impl<K: ArrowPrimitiveType, O: OffsetSizeTrait> MapBuilderExt<K::Native, Datum>
    for MapBuilder<PrimitiveBuilder<K>, GenericBinaryBuilder<O>>
where K::Native: Ord
{
    fn append_map(&mut self, map: &HashMap<K::Native, Datum>) -> Result<()> {
        // Order by keys to get deterministic and matching order between rows
        for (k, v) in map.iter().sorted_by_key(|(k, _)| *k) {
            self.keys().append_value(*k);
            self.values().append_value(v.to_bytes()?);
        }
        self.append(true)?;
        Ok(())
    }
}

/// Helper trait to extend [ArrayBuilder] with methods that cast on a provided [DataType].
/// This allows reuse of large pattern matching statements where we're dealing with, potentially,
/// any primitive type (as when building structs of partition values or upper and lower bounds).
trait AnyPrimitiveArrayBuilderExt {
    /// Append an [[arrow_array::Datum]] value.
    fn append_datum(&mut self, value: &dyn ArrowDatum) -> Result<()>;
    /// Append a literal with the provided [DataType]. We're not solely relying on the literal to
    /// infer the type because [Literal] values do not specify the expected type of builder. E.g.,
    /// a [PrimitiveLiteral::Long] may go into an array builder for longs but also for timestamps.
    fn append_literal(&mut self, data_type: &DataType, value: &Literal) -> Result<()>;
    /// Append a null value for the provided [DataType].
    fn append_null(&mut self, data_type: &DataType) -> Result<()>;
    /// Cast this builder to a specific type or return [Error].
    fn downcast_mut<T: ArrayBuilder>(&mut self) -> Result<&mut T>;
}

impl AnyPrimitiveArrayBuilderExt for dyn ArrayBuilder {
    fn append_datum(&mut self, value: &dyn ArrowDatum) -> Result<()> {
        let (array, is_scalar) = value.get();
        assert!(is_scalar, "Can only append scalar datum");

        match array.data_type() {
            DataType::Boolean => self
                .downcast_mut::<BooleanBuilder>()?
                .append_value(array.as_boolean().value(0)),
            DataType::Int32 => self
                .downcast_mut::<Int32Builder>()?
                .append_value(array.as_primitive::<Int32Type>().value(0)),
            DataType::Int64 => self
                .downcast_mut::<Int64Builder>()?
                .append_value(array.as_primitive::<Int64Type>().value(0)),
            DataType::Float32 => self
                .downcast_mut::<Float32Builder>()?
                .append_value(array.as_primitive::<Float32Type>().value(0)),
            DataType::Float64 => self
                .downcast_mut::<Float64Builder>()?
                .append_value(array.as_primitive::<Float64Type>().value(0)),
            DataType::Decimal128(_, _) => self
                .downcast_mut::<Decimal128Builder>()?
                .append_value(array.as_primitive::<Decimal128Type>().value(0)),
            DataType::Date32 => self
                .downcast_mut::<Date32Builder>()?
                .append_value(array.as_primitive::<Date32Type>().value(0)),
            DataType::Time64(TimeUnit::Microsecond) => self
                .downcast_mut::<Time64MicrosecondBuilder>()?
                .append_value(array.as_primitive::<Time64MicrosecondType>().value(0)),
            DataType::Timestamp(TimeUnit::Microsecond, _) => self
                .downcast_mut::<TimestampMicrosecondBuilder>()?
                .append_value(array.as_primitive::<TimestampMicrosecondType>().value(0)),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => self
                .downcast_mut::<TimestampNanosecondBuilder>()?
                .append_value(array.as_primitive::<TimestampNanosecondType>().value(0)),
            DataType::Utf8 => self
                .downcast_mut::<StringBuilder>()?
                .append_value(array.as_string::<i32>().value(0)),
            DataType::FixedSizeBinary(_) => self
                .downcast_mut::<BinaryBuilder>()?
                .append_value(array.as_fixed_size_binary().value(0)),
            DataType::LargeBinary => self
                .downcast_mut::<LargeBinaryBuilder>()?
                .append_value(array.as_binary::<i64>().value(0)),
            _ => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    format!("Cannot append data type: {:?}", array.data_type(),),
                ));
            }
        }
        Ok(())
    }

    fn append_literal(&mut self, data_type: &DataType, value: &Literal) -> Result<()> {
        let Some(primitive) = value.as_primitive_literal() else {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Expected primitive type",
            ));
        };

        match (data_type, primitive.clone()) {
            (DataType::Boolean, PrimitiveLiteral::Boolean(value)) => {
                self.downcast_mut::<BooleanBuilder>()?.append_value(value)
            }
            (DataType::Int32, PrimitiveLiteral::Int(value)) => {
                self.downcast_mut::<Int32Builder>()?.append_value(value)
            }
            (DataType::Int64, PrimitiveLiteral::Long(value)) => {
                self.downcast_mut::<Int64Builder>()?.append_value(value)
            }
            (DataType::Float32, PrimitiveLiteral::Float(OrderedFloat(value))) => {
                self.downcast_mut::<Float32Builder>()?.append_value(value)
            }
            (DataType::Float64, PrimitiveLiteral::Double(OrderedFloat(value))) => {
                self.downcast_mut::<Float64Builder>()?.append_value(value)
            }
            (DataType::Utf8, PrimitiveLiteral::String(value)) => {
                self.downcast_mut::<StringBuilder>()?.append_value(value)
            }
            (DataType::FixedSizeBinary(_), PrimitiveLiteral::Binary(value)) => self
                .downcast_mut::<FixedSizeBinaryBuilder>()?
                .append_value(value)?,
            (DataType::LargeBinary, PrimitiveLiteral::Binary(value)) => self
                .downcast_mut::<LargeBinaryBuilder>()?
                .append_value(value),
            (_, _) => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    format!(
                        "Builder of type {:?} does not accept literal {:?}",
                        data_type, primitive
                    ),
                ));
            }
        }

        Ok(())
    }

    fn append_null(&mut self, data_type: &DataType) -> Result<()> {
        match data_type {
            DataType::Boolean => self.downcast_mut::<BooleanBuilder>()?.append_null(),
            DataType::Int32 => self.downcast_mut::<Int32Builder>()?.append_null(),
            DataType::Int64 => self.downcast_mut::<Int64Builder>()?.append_null(),
            DataType::Float32 => self.downcast_mut::<Float32Builder>()?.append_null(),
            DataType::Float64 => self.downcast_mut::<Float64Builder>()?.append_null(),
            DataType::Decimal128(_, _) => self.downcast_mut::<Decimal128Builder>()?.append_null(),
            DataType::Date32 => self.downcast_mut::<Date32Builder>()?.append_null(),
            DataType::Time64(TimeUnit::Microsecond) => self
                .downcast_mut::<Time64MicrosecondBuilder>()?
                .append_null(),
            DataType::Timestamp(TimeUnit::Microsecond, _) => self
                .downcast_mut::<TimestampMicrosecondBuilder>()?
                .append_null(),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => self
                .downcast_mut::<TimestampNanosecondBuilder>()?
                .append_null(),
            DataType::Utf8 => self.downcast_mut::<StringBuilder>()?.append_null(),
            DataType::FixedSizeBinary(_) => {
                self.downcast_mut::<FixedSizeBinaryBuilder>()?.append_null()
            }
            DataType::LargeBinary => self.downcast_mut::<LargeBinaryBuilder>()?.append_null(),
            _ => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    format!("Cannot append null values for data type: {:?}", data_type),
                ))
            }
        }
        Ok(())
    }

    fn downcast_mut<T: ArrayBuilder>(&mut self) -> Result<&mut T> {
        self.as_any_mut().downcast_mut::<T>().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "Failed to cast builder to expected type",
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;
    use arrow_cast::pretty::pretty_format_batches;
    use expect_test::{expect, Expect};
    use itertools::Itertools;

    use super::*;
    use crate::scan::tests::TableTestFixture;

    /// Snapshot testing to check the resulting record batch.
    ///
    /// - `expected_schema/data`: put `expect![[""]]` as a placeholder,
    ///   and then run test with `UPDATE_EXPECT=1 cargo test` to automatically update the result,
    ///   or use rust-analyzer (see [video](https://github.com/rust-analyzer/expect-test)).
    ///   Check the doc of [`expect_test`] for more details.
    /// - `ignore_check_columns`: Some columns are not stable, so we can skip them.
    /// - `sort_column`: The order of the data might be non-deterministic, so we can sort it by a column.
    fn check_record_batch(
        record_batch: RecordBatch,
        expected_schema: Expect,
        expected_data: Expect,
        ignore_check_columns: &[&str],
        sort_column: Option<&str>,
    ) {
        let mut columns = record_batch.columns().to_vec();
        if let Some(sort_column) = sort_column {
            let column = record_batch.column_by_name(sort_column).unwrap();
            let indices = arrow_ord::sort::sort_to_indices(column, None, None).unwrap();
            columns = columns
                .iter()
                .map(|column| arrow_select::take::take(column.as_ref(), &indices, None).unwrap())
                .collect_vec();
        }

        // Filter columns
        let (fields, columns): (Vec<_>, Vec<_>) = record_batch
            .schema()
            .fields
            .iter()
            .cloned()
            .zip_eq(columns)
            .filter(|(field, _)| !ignore_check_columns.contains(&field.name().as_str()))
            // Filter columns nested within structs as well
            .map(|(field, column)| match field.data_type() {
                DataType::Struct(fields) => {
                    let struct_array = column.as_any().downcast_ref::<StructArray>().unwrap();
                    let filtered: Vec<(FieldRef, ArrayRef)> = fields
                        .iter()
                        .cloned()
                        .zip_eq(struct_array.columns().iter().cloned())
                        .filter(|(field, _)| !ignore_check_columns.contains(&field.name().as_str()))
                        .collect_vec();
                    let filtered_type = DataType::Struct(
                        filtered
                            .iter()
                            .map(|(f, _)| f)
                            .cloned()
                            .collect_vec()
                            .into(),
                    );
                    (
                        Field::new(field.name(), filtered_type, field.is_nullable()).into(),
                        Arc::new(StructArray::from(filtered)) as ArrayRef,
                    )
                }
                _ => (field, column),
            })
            .unzip();

        expected_schema.assert_eq(&format!(
            "{}",
            record_batch.schema().fields().iter().format(",\n")
        ));
        expected_data.assert_eq(
            &pretty_format_batches(&[
                RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
            ])
            .unwrap()
            .to_string(),
        );
    }

    #[test]
    fn test_snapshots_table() {
        let table = TableTestFixture::new().table;
        let record_batch = table.metadata_table().snapshots().scan().unwrap();
        check_record_batch(
            record_batch,
            expect![[r#"
                Field { name: "committed_at", data_type: Timestamp(Millisecond, Some("+00:00")), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "snapshot_id", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "parent_id", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "operation", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "manifest_list", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "summary", data_type: Map(Field { name: "entries", data_type: Struct([Field { name: "keys", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "values", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }"#]],
            expect![[r#"
                +--------------------------+---------------------+---------------------+-----------+---------+
                | committed_at             | snapshot_id         | parent_id           | operation | summary |
                +--------------------------+---------------------+---------------------+-----------+---------+
                | 2018-01-04T21:22:35.770Z | 3051729675574597004 |                     | append    | {}      |
                | 2019-04-12T20:29:15.770Z | 3055729675574597004 | 3051729675574597004 | append    | {}      |
                +--------------------------+---------------------+---------------------+-----------+---------+"#]],
            &["manifest_list"],
            Some("committed_at"),
        );
    }

    #[tokio::test]
    async fn test_entries_table() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let table = fixture.table;
        let record_batch = table.metadata_table().entries().scan().await.unwrap();

        check_record_batch(
            record_batch,
            expect![[r#"
                Field { name: "status", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "snapshot_id", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "sequence_number", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "file_sequence_number", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "data_file", data_type: Struct([Field { name: "content", data_type: Int8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "file_path", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "file_format", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "partition", data_type: Struct([Field { name: "x", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "record_count", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "file_size_in_bytes", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "column_sizes", data_type: Map(Field { name: "entries", data_type: Struct([Field { name: "keys", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "values", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_counts", data_type: Map(Field { name: "entries", data_type: Struct([Field { name: "keys", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "values", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_counts", data_type: Map(Field { name: "entries", data_type: Struct([Field { name: "keys", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "values", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_counts", data_type: Map(Field { name: "entries", data_type: Struct([Field { name: "keys", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "values", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bounds", data_type: Map(Field { name: "entries", data_type: Struct([Field { name: "keys", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "values", data_type: Binary, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bounds", data_type: Map(Field { name: "entries", data_type: Struct([Field { name: "keys", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "values", data_type: Binary, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "key_metadata", data_type: Binary, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "split_offsets", data_type: List(Field { name: "item", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "equality_ids", data_type: List(Field { name: "item", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "sort_order_id", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "readable_metrics", data_type: Struct([Field { name: "x", data_type: Struct([Field { name: "column_size", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "y", data_type: Struct([Field { name: "column_size", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "z", data_type: Struct([Field { name: "column_size", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "a", data_type: Struct([Field { name: "column_size", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "dbl", data_type: Struct([Field { name: "column_size", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "i32", data_type: Struct([Field { name: "column_size", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "i64", data_type: Struct([Field { name: "column_size", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "bool", data_type: Struct([Field { name: "column_size", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Boolean, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Boolean, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "float", data_type: Struct([Field { name: "column_size", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Float32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Float32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "decimal", data_type: Struct([Field { name: "column_size", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Decimal128(3, 2), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Decimal128(3, 2), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "date", data_type: Struct([Field { name: "column_size", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Date32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Date32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "timestamp", data_type: Struct([Field { name: "column_size", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Timestamp(Microsecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Timestamp(Microsecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "timestamptz", data_type: Struct([Field { name: "column_size", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Timestamp(Microsecond, Some("+00:00")), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Timestamp(Microsecond, Some("+00:00")), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "timestampns", data_type: Struct([Field { name: "column_size", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "timestamptzns", data_type: Struct([Field { name: "column_size", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Timestamp(Nanosecond, Some("+00:00")), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Timestamp(Nanosecond, Some("+00:00")), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "uuid", data_type: Struct([Field { name: "column_size", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: FixedSizeBinary(16), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: FixedSizeBinary(16), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "fixed", data_type: Struct([Field { name: "column_size", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: FixedSizeBinary(4), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: FixedSizeBinary(4), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "binary", data_type: Struct([Field { name: "column_size", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: LargeBinary, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: LargeBinary, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }"#]],
            expect![[r#"
                +--------+---------------------+-----------------+----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                | status | snapshot_id         | sequence_number | file_sequence_number | data_file                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | readable_metrics                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
                +--------+---------------------+-----------------+----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                | 1      | 3055729675574597004 | 1               | 1                    | {content: 0, file_format: PARQUET, partition: {x: 100}, record_count: 1, file_size_in_bytes: 100, column_sizes: {1: 1, 2: 1}, value_counts: {1: 2, 2: 2}, null_value_counts: {1: 3, 2: 3}, nan_value_counts: {1: 4, 2: 4}, lower_bounds: {1: 6400000000000000, 4: 6c6f776572, 5: cdcccccccccc10c0, 6: d6ffffff, 8: 00, 9: 666686c0, 11: 00000000, 12: 0000000000000000, 13: 0000000000000000}, upper_bounds: {1: 6400000000000000, 4: 7570706572, 5: cdcccccccccc1040, 6: 2a000000, 8: 01, 9: 66668640, 11: 01000000, 12: 0100000000000000, 13: 0100000000000000}, key_metadata: , split_offsets: [], equality_ids: [], sort_order_id: } | {x: {column_size: 1, value_count: 2, null_value_count: 3, nan_value_count: 4, lower_bound: 100, upper_bound: 100}, y: {column_size: 1, value_count: 2, null_value_count: 3, nan_value_count: 4, lower_bound: , upper_bound: }, z: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, a: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: lower, upper_bound: upper}, dbl: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: 0.0, upper_bound: 4.0}, i32: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: -42, upper_bound: 42}, i64: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, bool: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: false, upper_bound: true}, float: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: 0.0, upper_bound: 4.0}, decimal: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, date: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: 1970-01-01, upper_bound: 1970-01-02}, timestamp: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: 1970-01-01T00:00:00, upper_bound: 1970-01-01T00:00:00.000001}, timestamptz: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: 1970-01-01T00:00:00Z, upper_bound: 1970-01-01T00:00:00.000001Z}, timestampns: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestamptzns: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, uuid: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, fixed: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, binary: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }} |
                | 2      | 3051729675574597004 | 0               | 0                    | {content: 0, file_format: PARQUET, partition: {x: 200}, record_count: 1, file_size_in_bytes: 100, column_sizes: {}, value_counts: {}, null_value_counts: {}, nan_value_counts: {}, lower_bounds: {}, upper_bounds: {}, key_metadata: , split_offsets: [], equality_ids: [], sort_order_id: }                                                                                                                                                                                                                                                                                                                                             | {x: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, y: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, z: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, a: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, dbl: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, i32: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, i64: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, bool: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, float: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, decimal: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, date: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestamp: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestamptz: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestampns: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestamptzns: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, uuid: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, fixed: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, binary: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }}                                                                                                                                                                   |
                | 0      | 3051729675574597004 | 0               | 0                    | {content: 0, file_format: PARQUET, partition: {x: 300}, record_count: 1, file_size_in_bytes: 100, column_sizes: {}, value_counts: {}, null_value_counts: {}, nan_value_counts: {}, lower_bounds: {}, upper_bounds: {}, key_metadata: , split_offsets: [], equality_ids: [], sort_order_id: }                                                                                                                                                                                                                                                                                                                                             | {x: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, y: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, z: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, a: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, dbl: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, i32: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, i64: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, bool: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, float: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, decimal: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, date: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestamp: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestamptz: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestampns: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestamptzns: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, uuid: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, fixed: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, binary: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }}                                                                                                                                                                   |
                +--------+---------------------+-----------------+----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+"#]],
            &["file_path"],
            None,
        );
    }

    #[tokio::test]
    async fn test_manifests_table() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let record_batch = fixture
            .table
            .metadata_table()
            .manifests()
            .scan()
            .await
            .unwrap();

        check_record_batch(
            record_batch,
            expect![[r#"
                Field { name: "content", data_type: Int8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "path", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "length", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "partition_spec_id", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "added_snapshot_id", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "added_data_files_count", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "existing_data_files_count", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "deleted_data_files_count", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "added_delete_files_count", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "existing_delete_files_count", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "deleted_delete_files_count", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "partition_summaries", data_type: List(Field { name: "item", data_type: Struct([Field { name: "contains_null", data_type: Boolean, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "contains_nan", data_type: Boolean, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }"#]],
            expect![[r#"
                content: PrimitiveArray<Int8>
                [
                  0,
                ],
                path: (skipped),
                length: (skipped),
                partition_spec_id: PrimitiveArray<Int32>
                [
                  0,
                ],
                added_snapshot_id: PrimitiveArray<Int64>
                [
                  3055729675574597004,
                ],
                added_data_files_count: PrimitiveArray<Int32>
                [
                  1,
                ],
                existing_data_files_count: PrimitiveArray<Int32>
                [
                  1,
                ],
                deleted_data_files_count: PrimitiveArray<Int32>
                [
                  1,
                ],
                added_delete_files_count: PrimitiveArray<Int32>
                [
                  1,
                ],
                existing_delete_files_count: PrimitiveArray<Int32>
                [
                  1,
                ],
                deleted_delete_files_count: PrimitiveArray<Int32>
                [
                  1,
                ],
                partition_summaries: ListArray
                [
                  StructArray
                -- validity:
                [
                  valid,
                ]
                [
                -- child 0: "contains_null" (Boolean)
                BooleanArray
                [
                  false,
                ]
                -- child 1: "contains_nan" (Boolean)
                BooleanArray
                [
                  false,
                ]
                -- child 2: "lower_bound" (Utf8)
                StringArray
                [
                  "100",
                ]
                -- child 3: "upper_bound" (Utf8)
                StringArray
                [
                  "300",
                ]
                ],
                ]"#]],
            &["path", "length"],
            Some("path"),
        );
    }
}
