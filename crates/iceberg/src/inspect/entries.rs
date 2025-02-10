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
use std::string::ToString;
use std::sync::Arc;

use arrow_array::builder::{
    Int32Builder, Int64Builder, LargeBinaryBuilder, ListBuilder, MapBuilder, MapFieldNames,
    StringBuilder,
};
use arrow_array::{ArrayRef, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, FieldRef, Fields};
use async_stream::try_stream;
use futures::StreamExt;
use itertools::Itertools;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::arrow::builder::AnyPrimitiveArrayBuilder;
use crate::arrow::{
    get_arrow_datum, schema_to_arrow_schema, type_to_arrow_type, DEFAULT_MAP_FIELD_NAME,
};
use crate::scan::ArrowRecordBatchStream;
use crate::spec::{
    join_schemas, DataFile, ManifestFile, NestedField, NestedFieldRef, PartitionField,
    PartitionSpec, PartitionSpecRef, PrimitiveType, Struct, TableMetadata, Transform, Type,
    MAP_KEY_FIELD_NAME, MAP_VALUE_FIELD_NAME,
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
    pub fn schema(&self) -> crate::spec::Schema {
        let schema = self.manifest_entry_schema();
        let readable_metric_schema = ReadableMetricsStructBuilder::readable_metrics_schema(
            self.table.metadata().current_schema(),
            &schema,
        );
        join_schemas(&schema, &readable_metric_schema).unwrap()
    }

    fn manifest_entry_schema(&self) -> crate::spec::Schema {
        let fields = vec![
            NestedField::required(0, "status", Type::Primitive(PrimitiveType::Int)),
            NestedField::optional(1, "snapshot_id", Type::Primitive(PrimitiveType::Long)),
            NestedField::optional(3, "sequence_number", Type::Primitive(PrimitiveType::Long)),
            NestedField::optional(
                4,
                "file_sequence_number",
                Type::Primitive(PrimitiveType::Long),
            ),
            NestedField::required(
                2,
                "data_file",
                Type::Struct(
                    DataFileStructBuilder::schema(self.table.metadata())
                        .as_struct()
                        .clone(),
                ),
            ),
        ];
        crate::spec::Schema::builder()
            .with_fields(fields.into_iter().map(Arc::new).collect_vec())
            .build()
            .unwrap()
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
        let schema = self.schema();
        let arrow_schema = Arc::new(schema_to_arrow_schema(&schema)?);
        let table_metadata = self.table.metadata_ref();
        let file_io = Arc::new(self.table.file_io().clone());
        let readable_metrics_schema = schema
            .field_by_name("readable_metrics")
            .and_then(|field| field.field_type.clone().to_struct_type())
            .unwrap();

        Ok(try_stream! {
            for manifest_file in manifest_list.entries() {
                let mut status = Int32Builder::new();
                let mut snapshot_id = Int64Builder::new();
                let mut sequence_number = Int64Builder::new();
                let mut file_sequence_number = Int64Builder::new();
                let mut data_file = DataFileStructBuilder::new(&table_metadata);
                let mut readable_metrics =
                    ReadableMetricsStructBuilder::new(
                    table_metadata.current_schema(), &readable_metrics_schema);

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
    content: Int32Builder,
    file_path: StringBuilder,
    file_format: StringBuilder,
    partition: PartitionValuesStructBuilder,
    record_count: Int64Builder,
    file_size_in_bytes: Int64Builder,
    column_sizes: MapBuilder<Int32Builder, Int64Builder>,
    value_counts: MapBuilder<Int32Builder, Int64Builder>,
    null_value_counts: MapBuilder<Int32Builder, Int64Builder>,
    nan_value_counts: MapBuilder<Int32Builder, Int64Builder>,
    lower_bounds: MapBuilder<Int32Builder, LargeBinaryBuilder>,
    upper_bounds: MapBuilder<Int32Builder, LargeBinaryBuilder>,
    key_metadata: LargeBinaryBuilder,
    split_offsets: ListBuilder<Int64Builder>,
    equality_ids: ListBuilder<Int32Builder>,
    sort_order_ids: Int32Builder,
}

impl<'a> DataFileStructBuilder<'a> {
    fn new(table_metadata: &'a TableMetadata) -> Self {
        let map_field_names = Some(MapFieldNames {
            entry: DEFAULT_MAP_FIELD_NAME.to_string(),
            key: MAP_KEY_FIELD_NAME.to_string(),
            value: MAP_VALUE_FIELD_NAME.to_string(),
        });

        Self {
            table_metadata,
            content: Int32Builder::new(),
            file_path: StringBuilder::new(),
            file_format: StringBuilder::new(),
            partition: PartitionValuesStructBuilder::new(table_metadata),
            record_count: Int64Builder::new(),
            file_size_in_bytes: Int64Builder::new(),
            column_sizes: MapBuilder::new(
                map_field_names.clone(),
                Int32Builder::new(),
                Int64Builder::new(),
            )
            .with_keys_field(key_field(117, DataType::Int32))
            .with_values_field(value_field(118, DataType::Int64)),
            value_counts: MapBuilder::new(
                map_field_names.clone(),
                Int32Builder::new(),
                Int64Builder::new(),
            )
            .with_keys_field(key_field(119, DataType::Int32))
            .with_values_field(value_field(120, DataType::Int64)),
            null_value_counts: MapBuilder::new(
                map_field_names.clone(),
                Int32Builder::new(),
                Int64Builder::new(),
            )
            .with_keys_field(key_field(121, DataType::Int32))
            .with_values_field(value_field(122, DataType::Int64)),
            nan_value_counts: MapBuilder::new(
                map_field_names.clone(),
                Int32Builder::new(),
                Int64Builder::new(),
            )
            .with_keys_field(key_field(138, DataType::Int32))
            .with_values_field(value_field(139, DataType::Int64)),
            lower_bounds: MapBuilder::new(
                map_field_names.clone(),
                Int32Builder::new(),
                LargeBinaryBuilder::new(),
            )
            .with_keys_field(key_field(126, DataType::Int32))
            .with_values_field(value_field(127, DataType::LargeBinary)),
            upper_bounds: MapBuilder::new(
                map_field_names.clone(),
                Int32Builder::new(),
                LargeBinaryBuilder::new(),
            )
            .with_keys_field(key_field(129, DataType::Int32))
            .with_values_field(value_field(130, DataType::LargeBinary)),
            key_metadata: LargeBinaryBuilder::new(),
            split_offsets: ListBuilder::new(Int64Builder::new())
                .with_field(list_field(133, DataType::Int64)),
            equality_ids: ListBuilder::new(Int32Builder::new())
                .with_field(list_field(136, DataType::Int32)),
            sort_order_ids: Int32Builder::new(),
        }
    }

    fn schema(table_metadata: &TableMetadata) -> crate::spec::Schema {
        let partition_type = PartitionValuesStructBuilder::partition_type(table_metadata);

        let fields = vec![
            NestedField::required(134, "content", Type::Primitive(PrimitiveType::Int)),
            NestedField::required(100, "file_path", Type::Primitive(PrimitiveType::String)),
            NestedField::required(101, "file_format", Type::Primitive(PrimitiveType::String)),
            NestedField::required(102, "partition", Type::Struct(partition_type)),
            NestedField::required(103, "record_count", Type::Primitive(PrimitiveType::Long)),
            NestedField::required(
                104,
                "file_size_in_bytes",
                Type::Primitive(PrimitiveType::Long),
            ),
            NestedField::required_map(108, "column_sizes")
                .key(117, Type::Primitive(PrimitiveType::Int))
                .value(118, Type::Primitive(PrimitiveType::Long), true)
                .build(),
            NestedField::required_map(109, "value_counts")
                .key(119, Type::Primitive(PrimitiveType::Int))
                .value(120, Type::Primitive(PrimitiveType::Long), true)
                .build(),
            NestedField::required_map(110, "null_value_counts")
                .key(121, Type::Primitive(PrimitiveType::Int))
                .value(122, Type::Primitive(PrimitiveType::Long), true)
                .build(),
            NestedField::required_map(137, "nan_value_counts")
                .key(138, Type::Primitive(PrimitiveType::Int))
                .value(139, Type::Primitive(PrimitiveType::Long), true)
                .build(),
            NestedField::required_map(125, "lower_bounds")
                .key(126, Type::Primitive(PrimitiveType::Int))
                .value(127, Type::Primitive(PrimitiveType::Binary), true)
                .build(),
            NestedField::required_map(128, "upper_bounds")
                .key(129, Type::Primitive(PrimitiveType::Int))
                .value(130, Type::Primitive(PrimitiveType::Binary), true)
                .build(),
            NestedField::optional(131, "key_metadata", Type::Primitive(PrimitiveType::Binary)),
            NestedField::required_list(132, "split_offsets")
                .element_field(133, Type::Primitive(PrimitiveType::Long), true)
                .build(),
            NestedField::required_list(135, "equality_ids")
                .element_field(136, Type::Primitive(PrimitiveType::Int), true)
                .build(),
            NestedField::optional(140, "sort_order_id", Type::Primitive(PrimitiveType::Int)),
        ];

        crate::spec::Schema::builder()
            .with_fields(fields.into_iter().map(Arc::new).collect_vec())
            .build()
            .unwrap()
    }

    fn append(&mut self, manifest_file: &ManifestFile, data_file: &DataFile) -> Result<()> {
        self.content.append_value(data_file.content as i32);
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
        let schema = schema_to_arrow_schema(&Self::schema(self.table_metadata)).unwrap();

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
            schema
                .fields()
                .iter()
                .cloned()
                .zip_eq(inner_arrays)
                .collect_vec(),
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
    data_table_field_id: i32,
    metadata_fields: Fields,
    column_size: Int64Builder,
    value_count: Int64Builder,
    null_value_count: Int64Builder,
    nan_value_count: Int64Builder,
    lower_bound: AnyPrimitiveArrayBuilder,
    upper_bound: AnyPrimitiveArrayBuilder,
}

impl PerColumnReadableMetricsBuilder {
    fn struct_type(
        field_ids: &mut IncrementingFieldId,
        data_type: &Type,
    ) -> crate::spec::StructType {
        let fields = vec![
            NestedField::optional(
                field_ids.next_id(),
                "column_size",
                Type::Primitive(PrimitiveType::Long),
            ),
            NestedField::optional(
                field_ids.next_id(),
                "value_count",
                Type::Primitive(PrimitiveType::Long),
            ),
            NestedField::optional(
                field_ids.next_id(),
                "null_value_count",
                Type::Primitive(PrimitiveType::Long),
            ),
            NestedField::optional(
                field_ids.next_id(),
                "nan_value_count",
                Type::Primitive(PrimitiveType::Long),
            ),
            NestedField::optional(field_ids.next_id(), "lower_bound", data_type.clone()),
            NestedField::optional(field_ids.next_id(), "upper_bound", data_type.clone()),
        ]
        .into_iter()
        .map(Arc::new)
        .collect_vec();
        crate::spec::StructType::new(fields)
    }

    fn new_for_field(
        data_table_field_id: i32,
        data_type: &DataType,
        metadata_fields: Fields,
    ) -> Self {
        Self {
            data_table_field_id,
            metadata_fields,
            column_size: Int64Builder::new(),
            value_count: Int64Builder::new(),
            null_value_count: Int64Builder::new(),
            nan_value_count: Int64Builder::new(),
            lower_bound: AnyPrimitiveArrayBuilder::new(data_type),
            upper_bound: AnyPrimitiveArrayBuilder::new(data_type),
        }
    }

    fn append(&mut self, data_file: &DataFile) -> Result<()> {
        self.column_size.append_option(
            data_file
                .column_sizes()
                .get(&self.data_table_field_id)
                .map(|&v| v as i64),
        );
        self.value_count.append_option(
            data_file
                .value_counts()
                .get(&self.data_table_field_id)
                .map(|&v| v as i64),
        );
        self.null_value_count.append_option(
            data_file
                .null_value_counts()
                .get(&self.data_table_field_id)
                .map(|&v| v as i64),
        );
        self.nan_value_count.append_option(
            data_file
                .nan_value_counts()
                .get(&self.data_table_field_id)
                .map(|&v| v as i64),
        );
        match data_file.lower_bounds().get(&self.data_table_field_id) {
            Some(datum) => self
                .lower_bound
                .append_datum(get_arrow_datum(datum)?.as_ref())?,
            None => self.lower_bound.append_null()?,
        }
        match data_file.upper_bounds().get(&self.data_table_field_id) {
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
            self.metadata_fields
                .into_iter()
                .cloned()
                .zip_eq(inner_arrays)
                .collect::<Vec<(FieldRef, ArrayRef)>>(),
        )
    }
}

/// Build a [StructArray] with partition columns as fields and partition values as rows.
struct PartitionValuesStructBuilder {
    builders: Vec<AnyPrimitiveArrayBuilder>,
    partition_fields: Fields,
}

impl PartitionValuesStructBuilder {
    /// Construct a new builder from the combined partition columns of the table metadata.
    fn new(table_metadata: &TableMetadata) -> Self {
        let combined_struct_type = Self::partition_type(table_metadata);
        let DataType::Struct(partition_fields) =
            type_to_arrow_type(&Type::Struct(combined_struct_type)).unwrap()
        else {
            panic!("Converted Arrow type was not struct")
        };
        Self {
            builders: partition_fields
                .iter()
                .map(|field| AnyPrimitiveArrayBuilder::new(field.data_type()))
                .collect(),
            partition_fields,
        }
    }

    /// Builds a unified partition type considering all specs in the table.
    ///
    /// Based on Iceberg Java's [`Partitioning#partitionType`][1].
    ///
    /// [1]: https://github.com/apache/iceberg/blob/7e0cd3fa1e51d3c80f6c8cff23a03dca86f942fa/core/src/main/java/org/apache/iceberg/Partitioning.java#L240
    fn partition_type(table_metadata: &TableMetadata) -> crate::spec::StructType {
        Self::build_partition_projection_type(
            table_metadata.current_schema(),
            table_metadata.partition_specs_iter(),
            Self::all_fields_ids(table_metadata.partition_specs_iter()),
        )
    }

    /// Based on Iceberg Java's [`Partitioning#buildPartitionProjectionType`][1] with the difference
    /// that we pass along the [Schema] to map [PartitionField] to the current type.
    //
    /// [1]: https://github.com/apache/iceberg/blob/7e0cd3fa1e51d3c80f6c8cff23a03dca86f942fa/core/src/main/java/org/apache/iceberg/Partitioning.java#L255
    fn build_partition_projection_type<'a>(
        schema: &crate::spec::Schema,
        specs: impl Iterator<Item = &'a PartitionSpecRef>,
        projected_field_ids: HashSet<i32>,
    ) -> crate::spec::StructType {
        let mut field_map: HashMap<i32, PartitionField> = HashMap::new();
        let mut type_map: HashMap<i32, Type> = HashMap::new();
        let mut name_map: HashMap<i32, String> = HashMap::new();

        // Sort specs by ID in descending order to get latest field names
        let sorted_specs = specs
            .sorted_by_key(|spec| spec.spec_id())
            .rev()
            .collect_vec();

        for spec in sorted_specs {
            for field in spec.fields() {
                let field_id = field.field_id;

                if !projected_field_ids.contains(&field_id) {
                    continue;
                }

                let partition_type = spec.partition_type(schema).unwrap();
                let struct_field = partition_type.field_by_id(field_id).unwrap();
                let existing_field = field_map.get(&field_id);

                match existing_field {
                    None => {
                        field_map.insert(field_id, field.clone());
                        type_map.insert(field_id, struct_field.field_type.as_ref().clone());
                        name_map.insert(field_id, struct_field.name.clone());
                    }
                    Some(existing_field) => {
                        // verify the fields are compatible as they may conflict in v1 tables
                        if !Self::equivalent_ignoring_name(existing_field, field) {
                            panic!(
                                "Conflicting partition fields: ['{existing_field:?}', '{field:?}']",
                            );
                        }

                        // use the correct type for dropped partitions in v1 tables
                        if Self::is_void_transform(existing_field)
                            && !Self::is_void_transform(field)
                        {
                            field_map.insert(field_id, field.clone());
                            type_map.insert(field_id, struct_field.field_type.as_ref().clone());
                        }
                    }
                }
            }
        }

        let sorted_struct_fields = field_map
            .into_keys()
            .sorted()
            .map(|field_id| {
                NestedField::optional(field_id, &name_map[&field_id], type_map[&field_id].clone())
            })
            .map(Arc::new)
            .collect_vec();

        crate::spec::StructType::new(sorted_struct_fields)
    }

    fn is_void_transform(field: &PartitionField) -> bool {
        field.transform == Transform::Void
    }

    fn equivalent_ignoring_name(field: &PartitionField, another_field: &PartitionField) -> bool {
        field.field_id == another_field.field_id
            && field.source_id == another_field.source_id
            && Self::compatible_transforms(field.transform, another_field.transform)
    }

    fn compatible_transforms(t1: Transform, t2: Transform) -> bool {
        t1 == t2 || t1 == Transform::Void || t2 == Transform::Void
    }

    // collects IDs of all partition field used across specs
    fn all_fields_ids<'a>(specs: impl Iterator<Item = &'a PartitionSpecRef>) -> HashSet<i32> {
        specs
            .flat_map(|spec| spec.fields())
            .map(|partition| partition.field_id)
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
            self.partition_fields
                .iter()
                .cloned()
                .zip_eq(arrays)
                .collect::<Vec<(FieldRef, ArrayRef)>>(),
        )
    }

    fn find_field(&self, name: &str) -> Result<usize> {
        match self.partition_fields.find(name) {
            Some((index, _)) => Ok(index),
            None => Err(Error::new(
                ErrorKind::Unexpected,
                format!("Field not found: {}", name),
            )),
        }
    }
}

struct ReadableMetricsStructBuilder {
    column_builders: Vec<PerColumnReadableMetricsBuilder>,
    column_fields: Fields,
}

impl ReadableMetricsStructBuilder {
    /// Calculates a dynamic schema for `readable_metrics` to add to metadata tables. The type
    /// will be a struct containing all primitive columns in the data table.
    ///
    /// We take the table's schema to get the set of fields in the table. We also take the manifest
    /// entry schema to get the highest field ID in the entries metadata table to know which field
    /// ID to begin with.
    fn readable_metrics_schema(
        data_table_schema: &crate::spec::Schema,
        manifest_entry_schema: &crate::spec::Schema,
    ) -> crate::spec::Schema {
        let mut field_ids = IncrementingFieldId(manifest_entry_schema.highest_field_id() + 1);
        let mut per_column_readable_metrics_fields: Vec<NestedFieldRef> = Vec::new();

        for data_table_field in Self::sorted_primitive_fields(data_table_schema) {
            per_column_readable_metrics_fields.push(Arc::new(NestedField::required(
                field_ids.next_id(),
                &data_table_field.name,
                Type::Struct(PerColumnReadableMetricsBuilder::struct_type(
                    &mut field_ids,
                    &data_table_field.field_type,
                )),
            )));
        }

        crate::spec::Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                field_ids.next_id(),
                "readable_metrics",
                Type::Struct(crate::spec::StructType::new(
                    per_column_readable_metrics_fields,
                )),
            ))])
            .build()
            .unwrap()
    }

    fn sorted_primitive_fields(data_table_schema: &crate::spec::Schema) -> Vec<NestedFieldRef> {
        let mut fields = data_table_schema
            .as_struct()
            .fields()
            .iter()
            .filter(|field| field.field_type.is_primitive())
            .cloned()
            .collect_vec();
        fields.sort_by_key(|field| field.name.clone());
        fields
    }

    fn new(
        data_table_schema: &crate::spec::Schema,
        readable_metrics_schema: &crate::spec::StructType,
    ) -> ReadableMetricsStructBuilder {
        let DataType::Struct(column_fields) =
            type_to_arrow_type(&Type::Struct(readable_metrics_schema.clone())).unwrap()
        else {
            panic!("Converted Arrow type was not struct")
        };
        let column_builders = readable_metrics_schema
            .fields()
            .iter()
            .zip_eq(Self::sorted_primitive_fields(data_table_schema))
            .map(|(readable_metrics_field, data_field)| {
                let DataType::Struct(fields) =
                    type_to_arrow_type(&readable_metrics_field.field_type).unwrap()
                else {
                    panic!("Readable metrics field was not a struct")
                };
                let arrow_type = type_to_arrow_type(&data_field.field_type).unwrap();
                PerColumnReadableMetricsBuilder::new_for_field(data_field.id, &arrow_type, fields)
            })
            .collect_vec();

        Self {
            column_fields,
            column_builders,
        }
    }

    fn append(&mut self, data_file: &DataFile) -> Result<()> {
        for column_builder in &mut self.column_builders {
            column_builder.append(data_file)?;
        }
        Ok(())
    }

    fn finish(&mut self) -> StructArray {
        let arrays: Vec<ArrayRef> = self
            .column_builders
            .iter_mut()
            .map::<ArrayRef, _>(|builder| Arc::new(builder.finish()))
            .collect();

        let inner_arrays: Vec<(FieldRef, ArrayRef)> = self
            .column_fields
            .into_iter()
            .cloned()
            .zip_eq(arrays)
            .collect_vec();

        StructArray::from(inner_arrays)
    }
}

/// Helper to serve increment field ids.
struct IncrementingFieldId(i32);

impl IncrementingFieldId {
    fn next_id(&mut self) -> i32 {
        let current = self.0;
        self.0 += 1;
        current
    }
}

fn key_field(field_id: i32, data_type: DataType) -> FieldRef {
    Arc::new(
        Field::new("key", data_type, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            field_id.to_string(),
        )])),
    )
}

// All value fields are required.
fn value_field(field_id: i32, data_type: DataType) -> FieldRef {
    Arc::new(
        Field::new("value", data_type, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            field_id.to_string(),
        )])),
    )
}

// All element fields are required.
fn list_field(field_id: i32, data_type: DataType) -> FieldRef {
    Arc::new(
        Field::new("element", data_type, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            field_id.to_string(),
        )])),
    )
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
        let inspect = table.inspect();
        let entries_table = inspect.entries();

        let batch_stream = entries_table.scan().await.unwrap();

        check_record_batches(
            batch_stream,
            expect![[r#"
                Field { name: "status", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "0"} },
                Field { name: "snapshot_id", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1"} },
                Field { name: "sequence_number", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "3"} },
                Field { name: "file_sequence_number", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "4"} },
                Field { name: "data_file", data_type: Struct([Field { name: "content", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "134"} }, Field { name: "file_path", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "100"} }, Field { name: "file_format", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "101"} }, Field { name: "partition", data_type: Struct([Field { name: "x", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1000"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "102"} }, Field { name: "record_count", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "103"} }, Field { name: "file_size_in_bytes", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "104"} }, Field { name: "column_sizes", data_type: Map(Field { name: "key_value", data_type: Struct([Field { name: "key", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "117"} }, Field { name: "value", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "118"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "108"} }, Field { name: "value_counts", data_type: Map(Field { name: "key_value", data_type: Struct([Field { name: "key", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "119"} }, Field { name: "value", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "120"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "109"} }, Field { name: "null_value_counts", data_type: Map(Field { name: "key_value", data_type: Struct([Field { name: "key", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "121"} }, Field { name: "value", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "122"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "110"} }, Field { name: "nan_value_counts", data_type: Map(Field { name: "key_value", data_type: Struct([Field { name: "key", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "138"} }, Field { name: "value", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "139"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "137"} }, Field { name: "lower_bounds", data_type: Map(Field { name: "key_value", data_type: Struct([Field { name: "key", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "126"} }, Field { name: "value", data_type: LargeBinary, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "127"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "125"} }, Field { name: "upper_bounds", data_type: Map(Field { name: "key_value", data_type: Struct([Field { name: "key", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "129"} }, Field { name: "value", data_type: LargeBinary, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "130"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "128"} }, Field { name: "key_metadata", data_type: LargeBinary, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "131"} }, Field { name: "split_offsets", data_type: List(Field { name: "element", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "133"} }), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "132"} }, Field { name: "equality_ids", data_type: List(Field { name: "element", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "136"} }), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "135"} }, Field { name: "sort_order_id", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "140"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "2"} },
                Field { name: "readable_metrics", data_type: Struct([Field { name: "a", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1002"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1003"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1004"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1005"} }, Field { name: "lower_bound", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1006"} }, Field { name: "upper_bound", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1007"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1001"} }, Field { name: "binary", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1009"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1010"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1011"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1012"} }, Field { name: "lower_bound", data_type: LargeBinary, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1013"} }, Field { name: "upper_bound", data_type: LargeBinary, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1014"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1008"} }, Field { name: "bool", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1016"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1017"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1018"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1019"} }, Field { name: "lower_bound", data_type: Boolean, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1020"} }, Field { name: "upper_bound", data_type: Boolean, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1021"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1015"} }, Field { name: "date", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1023"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1024"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1025"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1026"} }, Field { name: "lower_bound", data_type: Date32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1027"} }, Field { name: "upper_bound", data_type: Date32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1028"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1022"} }, Field { name: "dbl", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1030"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1031"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1032"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1033"} }, Field { name: "lower_bound", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1034"} }, Field { name: "upper_bound", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1035"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1029"} }, Field { name: "decimal", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1037"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1038"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1039"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1040"} }, Field { name: "lower_bound", data_type: Decimal128(3, 2), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1041"} }, Field { name: "upper_bound", data_type: Decimal128(3, 2), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1042"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1036"} }, Field { name: "float", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1044"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1045"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1046"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1047"} }, Field { name: "lower_bound", data_type: Float32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1048"} }, Field { name: "upper_bound", data_type: Float32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1049"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1043"} }, Field { name: "i32", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1051"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1052"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1053"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1054"} }, Field { name: "lower_bound", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1055"} }, Field { name: "upper_bound", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1056"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1050"} }, Field { name: "i64", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1058"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1059"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1060"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1061"} }, Field { name: "lower_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1062"} }, Field { name: "upper_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1063"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1057"} }, Field { name: "timestamp", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1065"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1066"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1067"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1068"} }, Field { name: "lower_bound", data_type: Timestamp(Microsecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1069"} }, Field { name: "upper_bound", data_type: Timestamp(Microsecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1070"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1064"} }, Field { name: "timestampns", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1072"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1073"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1074"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1075"} }, Field { name: "lower_bound", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1076"} }, Field { name: "upper_bound", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1077"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1071"} }, Field { name: "timestamptz", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1079"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1080"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1081"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1082"} }, Field { name: "lower_bound", data_type: Timestamp(Microsecond, Some("+00:00")), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1083"} }, Field { name: "upper_bound", data_type: Timestamp(Microsecond, Some("+00:00")), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1084"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1078"} }, Field { name: "timestamptzns", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1086"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1087"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1088"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1089"} }, Field { name: "lower_bound", data_type: Timestamp(Nanosecond, Some("+00:00")), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1090"} }, Field { name: "upper_bound", data_type: Timestamp(Nanosecond, Some("+00:00")), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1091"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1085"} }, Field { name: "x", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1093"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1094"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1095"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1096"} }, Field { name: "lower_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1097"} }, Field { name: "upper_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1098"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1092"} }, Field { name: "y", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1100"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1101"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1102"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1103"} }, Field { name: "lower_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1104"} }, Field { name: "upper_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1105"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1099"} }, Field { name: "z", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1107"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1108"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1109"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1110"} }, Field { name: "lower_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1111"} }, Field { name: "upper_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1112"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1106"} }]), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1113"} }"#]],
            expect![[r#"
                +--------+---------------------+-----------------+----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                | status | snapshot_id         | sequence_number | file_sequence_number | data_file                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | readable_metrics                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
                +--------+---------------------+-----------------+----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                | 1      | 3055729675574597004 | 1               | 1                    | {content: 0, file_format: PARQUET, partition: {x: 100}, record_count: 1, file_size_in_bytes: 100, column_sizes: {1: 1, 2: 1}, value_counts: {1: 2, 2: 2}, null_value_counts: {1: 3, 2: 3}, nan_value_counts: {1: 4, 2: 4}, lower_bounds: {1: 0100000000000000, 2: 0200000000000000, 3: 0300000000000000, 4: 417061636865, 5: 0000000000005940, 6: 64000000, 7: 6400000000000000, 8: 00, 9: 0000c842, 11: 00000000, 12: 0000000000000000, 13: 0000000000000000}, upper_bounds: {1: 0100000000000000, 2: 0500000000000000, 3: 0400000000000000, 4: 49636562657267, 5: 0000000000006940, 6: c8000000, 7: c800000000000000, 8: 01, 9: 00004843, 11: 00000000, 12: 0000000000000000, 13: 0000000000000000}, key_metadata: , split_offsets: [], equality_ids: [], sort_order_id: } | {a: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: Apache, upper_bound: Iceberg}, binary: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, bool: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: false, upper_bound: true}, date: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: 1970-01-01, upper_bound: 1970-01-01}, dbl: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: 100.0, upper_bound: 200.0}, decimal: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, float: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: 100.0, upper_bound: 200.0}, i32: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: 100, upper_bound: 200}, i64: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: 100, upper_bound: 200}, timestamp: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: 1970-01-01T00:00:00, upper_bound: 1970-01-01T00:00:00}, timestampns: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestamptz: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: 1970-01-01T00:00:00Z, upper_bound: 1970-01-01T00:00:00Z}, timestamptzns: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, x: {column_size: 1, value_count: 2, null_value_count: 3, nan_value_count: 4, lower_bound: 1, upper_bound: 1}, y: {column_size: 1, value_count: 2, null_value_count: 3, nan_value_count: 4, lower_bound: 2, upper_bound: 5}, z: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: 3, upper_bound: 4}} |
                | 2      | 3055729675574597004 | 0               | 0                    | {content: 0, file_format: PARQUET, partition: {x: 200}, record_count: 1, file_size_in_bytes: 100, column_sizes: {}, value_counts: {}, null_value_counts: {}, nan_value_counts: {}, lower_bounds: {}, upper_bounds: {}, key_metadata: , split_offsets: [], equality_ids: [], sort_order_id: }                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | {a: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, binary: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, bool: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, date: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, dbl: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, decimal: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, float: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, i32: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, i64: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestamp: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestampns: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestamptz: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestamptzns: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, x: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, y: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, z: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }}                                                                                                                                                                       |
                | 0      | 3051729675574597004 | 0               | 0                    | {content: 0, file_format: PARQUET, partition: {x: 300}, record_count: 1, file_size_in_bytes: 100, column_sizes: {}, value_counts: {}, null_value_counts: {}, nan_value_counts: {}, lower_bounds: {}, upper_bounds: {}, key_metadata: , split_offsets: [], equality_ids: [], sort_order_id: }                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | {a: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, binary: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, bool: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, date: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, dbl: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, decimal: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, float: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, i32: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, i64: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestamp: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestampns: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestamptz: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestamptzns: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, x: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, y: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, z: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }}                                                                                                                                                                       |
                +--------+---------------------+-----------------+----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+"#]],
            &[],
            &["file_path"],
            None,
        )
            .await;
    }
}
