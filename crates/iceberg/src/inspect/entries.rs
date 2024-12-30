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
use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, Int32Builder, Int64Builder, Int8Builder, ListBuilder, MapBuilder, StringBuilder,
};
use arrow_array::{ArrayRef, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, FieldRef, Fields, Schema};
use async_stream::try_stream;
use futures::StreamExt;
use itertools::Itertools;

use crate::arrow::builder::AnyArrayBuilder;
use crate::arrow::{get_arrow_datum, schema_to_arrow_schema, type_to_arrow_type};
use crate::scan::ArrowRecordBatchStream;
use crate::spec::{
    DataFile, ManifestFile, PartitionField, PartitionSpec, SchemaRef, Struct, TableMetadata,
};
use crate::table::Table;
use crate::{Error, ErrorKind, Result};

/// Entries table containing the entries of the current snapshot's manifest files.
///
/// The table has one row for each manifest file entry in the current snapshot's manifest list file.
/// For reference, see the Java implementation of [`ManifestEntry`][1].
///
/// [1]: https://github.com/apache/iceberg/blob/apache-iceberg-1.7.1/core/src/main/java/org/apache/iceberg/ManifestEntry.java
pub struct EntriesTable<'a> {
    table: &'a Table,
}

impl<'a> EntriesTable<'a> {
    /// Create a new Entries table instance.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Get the schema for the manifest entries table.
    pub fn schema(&self) -> Schema {
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
    pub async fn scan(&self) -> Result<ArrowRecordBatchStream> {
        let current_snapshot = self.table.metadata().current_snapshot().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "Cannot scan entries for table without current snapshot",
            )
        })?;

        let manifest_list = current_snapshot
            .load_manifest_list(self.table.file_io(), self.table.metadata())
            .await?;

        // Copy to ensure that the stream can take ownership of these dependencies
        let arrow_schema = Arc::new(self.schema());
        let table_metadata = self.table.metadata_ref();
        let file_io = Arc::new(self.table.file_io().clone());

        Ok(try_stream! {
            for manifest_file in manifest_list.entries() {
                let mut status = Int32Builder::new();
                let mut snapshot_id = Int64Builder::new();
                let mut sequence_number = Int64Builder::new();
                let mut file_sequence_number = Int64Builder::new();
                let mut data_file = DataFileStructBuilder::new(&table_metadata);
                let mut readable_metrics =
                    ReadableMetricsStructBuilder::new(table_metadata.current_schema())?;

                for manifest_entry in manifest_file.load_manifest(&file_io).await?.entries() {
                    status.append_value(manifest_entry.status() as i32);
                    snapshot_id.append_option(manifest_entry.snapshot_id());
                    sequence_number.append_option(manifest_entry.sequence_number());
                    file_sequence_number.append_option(manifest_entry.file_sequence_number());
                    data_file.append(manifest_file, manifest_entry.data_file())?;
                    readable_metrics.append(manifest_entry.data_file())?;
                }

                let batch = RecordBatch::try_new(arrow_schema.clone(), vec![
                    Arc::new(status.finish()),
                    Arc::new(snapshot_id.finish()),
                    Arc::new(sequence_number.finish()),
                    Arc::new(file_sequence_number.finish()),
                    Arc::new(data_file.finish()),
                    Arc::new(readable_metrics.finish()),
                ])?;

                yield batch;
            }
        }
        .boxed())
    }
}

/// Builds the struct describing data files listed in a table manifest.
///
/// For reference, see the Java implementation of [`DataFile`][1].
///
/// [1]: https://github.com/apache/iceberg/blob/apache-iceberg-1.7.1/api/src/main/java/org/apache/iceberg/DataFile.java
struct DataFileStructBuilder<'a> {
    // Reference to table metadata to retrieve partition specs based on partition spec ids
    table_metadata: &'a TableMetadata,
    // Below are the field builders of the "data_file" struct
    content: Int8Builder,
    file_path: StringBuilder,
    file_format: StringBuilder,
    partition: PartitionValuesStructBuilder,
    record_count: Int64Builder,
    file_size_in_bytes: Int64Builder,
    column_sizes: MapBuilder<Int32Builder, Int64Builder>,
    value_counts: MapBuilder<Int32Builder, Int64Builder>,
    null_value_counts: MapBuilder<Int32Builder, Int64Builder>,
    nan_value_counts: MapBuilder<Int32Builder, Int64Builder>,
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
            record_count: Int64Builder::new(),
            file_size_in_bytes: Int64Builder::new(),
            column_sizes: MapBuilder::new(None, Int32Builder::new(), Int64Builder::new()),
            value_counts: MapBuilder::new(None, Int32Builder::new(), Int64Builder::new()),
            null_value_counts: MapBuilder::new(None, Int32Builder::new(), Int64Builder::new()),
            nan_value_counts: MapBuilder::new(None, Int32Builder::new(), Int64Builder::new()),
            lower_bounds: MapBuilder::new(None, Int32Builder::new(), BinaryBuilder::new()),
            upper_bounds: MapBuilder::new(None, Int32Builder::new(), BinaryBuilder::new()),
            key_metadata: BinaryBuilder::new(),
            split_offsets: ListBuilder::new(Int64Builder::new()),
            equality_ids: ListBuilder::new(Int32Builder::new()),
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
            Field::new("record_count", DataType::Int64, false),
            Field::new("file_size_in_bytes", DataType::Int64, false),
            Field::new(
                "column_sizes",
                Self::column_id_to_value_type(DataType::Int64),
                true,
            ),
            Field::new(
                "value_counts",
                Self::column_id_to_value_type(DataType::Int64),
                true,
            ),
            Field::new(
                "null_value_counts",
                Self::column_id_to_value_type(DataType::Int64),
                true,
            ),
            Field::new(
                "nan_value_counts",
                Self::column_id_to_value_type(DataType::Int64),
                true,
            ),
            Field::new(
                "lower_bounds",
                Self::column_id_to_value_type(DataType::Binary),
                true,
            ),
            Field::new(
                "upper_bounds",
                Self::column_id_to_value_type(DataType::Binary),
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

    fn append(&mut self, manifest_file: &ManifestFile, data_file: &DataFile) -> Result<()> {
        self.content.append_value(data_file.content as i8);
        self.file_path.append_value(data_file.file_path());
        self.file_format
            .append_value(data_file.file_format().to_string().to_uppercase());
        self.partition.append(
            self.partition_spec(manifest_file)?.clone().fields(),
            data_file.partition(),
        )?;
        self.record_count
            .append_value(data_file.record_count() as i64);
        self.file_size_in_bytes
            .append_value(data_file.file_size_in_bytes() as i64);

        // Sort keys to get matching order between rows
        for (k, v) in data_file.column_sizes.iter().sorted_by_key(|(k, _)| *k) {
            self.column_sizes.keys().append_value(*k);
            self.column_sizes.values().append_value(*v as i64);
        }
        self.column_sizes.append(true)?;

        for (k, v) in data_file.value_counts.iter().sorted_by_key(|(k, _)| *k) {
            self.value_counts.keys().append_value(*k);
            self.value_counts.values().append_value(*v as i64);
        }
        self.value_counts.append(true)?;

        for (k, v) in data_file
            .null_value_counts
            .iter()
            .sorted_by_key(|(k, _)| *k)
        {
            self.null_value_counts.keys().append_value(*k);
            self.null_value_counts.values().append_value(*v as i64);
        }
        self.null_value_counts.append(true)?;

        for (k, v) in data_file.nan_value_counts.iter().sorted_by_key(|(k, _)| *k) {
            self.nan_value_counts.keys().append_value(*k);
            self.nan_value_counts.values().append_value(*v as i64);
        }
        self.nan_value_counts.append(true)?;

        for (k, v) in data_file.lower_bounds.iter().sorted_by_key(|(k, _)| *k) {
            self.lower_bounds.keys().append_value(*k);
            self.lower_bounds.values().append_value(v.to_bytes()?);
        }
        self.lower_bounds.append(true)?;

        for (k, v) in data_file.upper_bounds.iter().sorted_by_key(|(k, _)| *k) {
            self.upper_bounds.keys().append_value(*k);
            self.upper_bounds.values().append_value(v.to_bytes()?);
        }
        self.upper_bounds.append(true)?;

        self.key_metadata.append_option(data_file.key_metadata());

        self.split_offsets
            .values()
            .append_slice(data_file.split_offsets());
        self.split_offsets.append(true);

        self.equality_ids
            .values()
            .append_slice(data_file.equality_ids());
        self.equality_ids.append(true);

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

/// Builds a readable metrics struct for a single column.
///
/// For reference, see [Java][1] and [Python][2] implementations.
///
/// [1]: https://github.com/apache/iceberg/blob/4a432839233f2343a9eae8255532f911f06358ef/core/src/main/java/org/apache/iceberg/MetricsUtil.java#L337
/// [2]: https://github.com/apache/iceberg-python/blob/a051584a3684392d2db6556449eb299145d47d15/pyiceberg/table/inspect.py#L101-L110
struct PerColumnReadableMetricsBuilder {
    field_id: i32,
    data_type: DataType,
    column_size: Int64Builder,
    value_count: Int64Builder,
    null_value_count: Int64Builder,
    nan_value_count: Int64Builder,
    lower_bound: AnyArrayBuilder,
    upper_bound: AnyArrayBuilder,
}

impl PerColumnReadableMetricsBuilder {
    fn fields(data_type: &DataType) -> Fields {
        vec![
            Field::new("column_size", DataType::Int64, true),
            Field::new("value_count", DataType::Int64, true),
            Field::new("null_value_count", DataType::Int64, true),
            Field::new("nan_value_count", DataType::Int64, true),
            Field::new("lower_bound", data_type.clone(), true),
            Field::new("upper_bound", data_type.clone(), true),
        ]
        .into()
    }

    fn new_for_field(field_id: i32, data_type: &DataType) -> Self {
        Self {
            field_id,
            data_type: data_type.clone(),
            column_size: Int64Builder::new(),
            value_count: Int64Builder::new(),
            null_value_count: Int64Builder::new(),
            nan_value_count: Int64Builder::new(),
            lower_bound: AnyArrayBuilder::new(data_type),
            upper_bound: AnyArrayBuilder::new(data_type),
        }
    }

    fn append(&mut self, data_file: &DataFile) -> Result<()> {
        self.column_size.append_option(
            data_file
                .column_sizes()
                .get(&self.field_id)
                .map(|&v| v as i64),
        );
        self.value_count.append_option(
            data_file
                .value_counts()
                .get(&self.field_id)
                .map(|&v| v as i64),
        );
        self.null_value_count.append_option(
            data_file
                .null_value_counts()
                .get(&self.field_id)
                .map(|&v| v as i64),
        );
        self.nan_value_count.append_option(
            data_file
                .nan_value_counts()
                .get(&self.field_id)
                .map(|&v| v as i64),
        );
        match data_file.lower_bounds().get(&self.field_id) {
            Some(datum) => self
                .lower_bound
                .append_datum(get_arrow_datum(datum)?.as_ref())?,
            None => self.lower_bound.append_null()?,
        }
        match data_file.upper_bounds().get(&self.field_id) {
            Some(datum) => self
                .upper_bound
                .append_datum(get_arrow_datum(datum)?.as_ref())?,
            None => self.upper_bound.append_null()?,
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

/// Build a [StructArray] with partition columns as fields and partition values as rows.
struct PartitionValuesStructBuilder {
    fields: Fields,
    builders: Vec<AnyArrayBuilder>,
}

impl PartitionValuesStructBuilder {
    /// Construct a new builder from the combined partition columns of the table metadata.
    fn new(table_metadata: &TableMetadata) -> Self {
        let combined_fields = Self::combined_partition_fields(table_metadata);
        Self {
            builders: combined_fields
                .iter()
                .map(|field| AnyArrayBuilder::new(field.data_type()))
                .collect_vec(),
            fields: combined_fields,
        }
    }

    /// Build the combined partition spec union-ing past and current partition specs
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

            match value {
                Some(literal) => self.builders[index].append_literal(literal)?,
                None => self.builders[index].append_null()?,
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> StructArray {
        let arrays: Vec<ArrayRef> = self
            .builders
            .iter_mut()
            .map::<ArrayRef, _>(|builder| Arc::new(builder.finish()))
            .collect();
        StructArray::from(
            self.fields
                .iter()
                .cloned()
                .zip_eq(arrays)
                .collect::<Vec<(FieldRef, ArrayRef)>>(),
        )
    }

    fn find_field(&self, name: &str) -> Result<usize> {
        match self.fields.find(name) {
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

#[cfg(test)]
mod tests {
    use expect_test::expect;

    use crate::inspect::metadata_table::tests::check_record_batches;
    use crate::scan::tests::TableTestFixture;

    #[tokio::test]
    async fn test_entries_table() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;
        let table = fixture.table;

        let batch_stream = table.inspect().entries().scan().await.unwrap();

        check_record_batches(
            batch_stream,
            expect![[r#"
                Field { name: "status", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "snapshot_id", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "sequence_number", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "file_sequence_number", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "data_file", data_type: Struct([Field { name: "content", data_type: Int8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "file_path", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "file_format", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "partition", data_type: Struct([Field { name: "x", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "record_count", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "file_size_in_bytes", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "column_sizes", data_type: Map(Field { name: "entries", data_type: Struct([Field { name: "keys", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "values", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_counts", data_type: Map(Field { name: "entries", data_type: Struct([Field { name: "keys", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "values", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_counts", data_type: Map(Field { name: "entries", data_type: Struct([Field { name: "keys", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "values", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_counts", data_type: Map(Field { name: "entries", data_type: Struct([Field { name: "keys", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "values", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bounds", data_type: Map(Field { name: "entries", data_type: Struct([Field { name: "keys", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "values", data_type: Binary, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bounds", data_type: Map(Field { name: "entries", data_type: Struct([Field { name: "keys", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "values", data_type: Binary, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "key_metadata", data_type: Binary, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "split_offsets", data_type: List(Field { name: "item", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "equality_ids", data_type: List(Field { name: "item", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "sort_order_id", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "readable_metrics", data_type: Struct([Field { name: "x", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "y", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "z", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "a", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "dbl", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "i32", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "i64", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "bool", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Boolean, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Boolean, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "float", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Float32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Float32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "decimal", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Decimal128(3, 2), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Decimal128(3, 2), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "date", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Date32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Date32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "timestamp", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Timestamp(Microsecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Timestamp(Microsecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "timestamptz", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Timestamp(Microsecond, Some("+00:00")), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Timestamp(Microsecond, Some("+00:00")), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "timestampns", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "timestamptzns", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Timestamp(Nanosecond, Some("+00:00")), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Timestamp(Nanosecond, Some("+00:00")), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "binary", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: LargeBinary, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: LargeBinary, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }"#]],
            expect![[r#"
                +--------+---------------------+-----------------+----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                | status | snapshot_id         | sequence_number | file_sequence_number | data_file                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | readable_metrics                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
                +--------+---------------------+-----------------+----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                | 1      | 3055729675574597004 | 1               | 1                    | {content: 0, file_format: PARQUET, partition: {x: 100}, record_count: 1, file_size_in_bytes: 100, column_sizes: {1: 1, 2: 1}, value_counts: {1: 2, 2: 2}, null_value_counts: {1: 3, 2: 3}, nan_value_counts: {1: 4, 2: 4}, lower_bounds: {1: 0100000000000000, 2: 0200000000000000, 3: 0300000000000000, 4: 417061636865, 5: 0000000000005940, 6: 64000000, 7: 6400000000000000, 8: 00, 9: 0000c842, 11: 00000000, 12: 0000000000000000, 13: 0000000000000000}, upper_bounds: {1: 0100000000000000, 2: 0500000000000000, 3: 0400000000000000, 4: 49636562657267, 5: 0000000000006940, 6: c8000000, 7: c800000000000000, 8: 01, 9: 00004843, 11: 00000000, 12: 0000000000000000, 13: 0000000000000000}, key_metadata: , split_offsets: [], equality_ids: [], sort_order_id: } | {x: {column_size: 1, value_count: 2, null_value_count: 3, nan_value_count: 4, lower_bound: 1, upper_bound: 1}, y: {column_size: 1, value_count: 2, null_value_count: 3, nan_value_count: 4, lower_bound: 2, upper_bound: 5}, z: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: 3, upper_bound: 4}, a: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: Apache, upper_bound: Iceberg}, dbl: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: 100.0, upper_bound: 200.0}, i32: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: 100, upper_bound: 200}, i64: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: 100, upper_bound: 200}, bool: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: false, upper_bound: true}, float: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: 100.0, upper_bound: 200.0}, decimal: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, date: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: 1970-01-01, upper_bound: 1970-01-01}, timestamp: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: 1970-01-01T00:00:00, upper_bound: 1970-01-01T00:00:00}, timestamptz: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: 1970-01-01T00:00:00Z, upper_bound: 1970-01-01T00:00:00Z}, timestampns: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestamptzns: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, binary: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }} |
                | 2      | 3055729675574597004 | 0               | 0                    | {content: 0, file_format: PARQUET, partition: {x: 200}, record_count: 1, file_size_in_bytes: 100, column_sizes: {}, value_counts: {}, null_value_counts: {}, nan_value_counts: {}, lower_bounds: {}, upper_bounds: {}, key_metadata: , split_offsets: [], equality_ids: [], sort_order_id: }                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | {x: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, y: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, z: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, a: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, dbl: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, i32: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, i64: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, bool: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, float: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, decimal: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, date: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestamp: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestamptz: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestampns: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestamptzns: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, binary: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }}                                                                                                                                                                       |
                | 0      | 3051729675574597004 | 0               | 0                    | {content: 0, file_format: PARQUET, partition: {x: 300}, record_count: 1, file_size_in_bytes: 100, column_sizes: {}, value_counts: {}, null_value_counts: {}, nan_value_counts: {}, lower_bounds: {}, upper_bounds: {}, key_metadata: , split_offsets: [], equality_ids: [], sort_order_id: }                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | {x: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, y: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, z: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, a: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, dbl: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, i32: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, i64: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, bool: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, float: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, decimal: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, date: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestamp: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestamptz: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestampns: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestamptzns: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, binary: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }}                                                                                                                                                                       |
                +--------+---------------------+-----------------+----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+"#]],
            &[],
            &["file_path"],
            None,
        ).await;
    }
}
