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

use std::any::type_name;
use std::string::ToString;
use std::sync::Arc;

use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, FixedSizeBinaryBuilder,
    Float32Builder, Float64Builder, Int32Builder, Int64Builder, LargeBinaryBuilder, ListBuilder,
    MapBuilder, PrimitiveBuilder, StringBuilder, StructBuilder, TimestampMicrosecondBuilder,
    TimestampNanosecondBuilder,
};
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{ArrowPrimitiveType, RecordBatch, StructArray};
use arrow_schema::{DataType, Fields, TimeUnit};
use async_stream::try_stream;
use futures::StreamExt;
use itertools::Itertools;
use ordered_float::OrderedFloat;

use crate::arrow::{schema_to_arrow_schema, type_to_arrow_type};
use crate::inspect::metrics::ReadableMetricsStructBuilder;
use crate::scan::ArrowRecordBatchStream;
use crate::spec::{
    DataFile, Datum, ManifestFile, NestedFieldRef, PrimitiveLiteral, Schema, Struct, TableMetadata,
    Type,
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
                let mut data_file = DataFileStructBuilder::new(&table_metadata)?;
                let mut readable_metrics =
                    ReadableMetricsStructBuilder::new(
                    table_metadata.current_schema(), &readable_metrics_schema)?;

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

    /// Get the schema for the manifest entries table.
    pub fn schema(&self) -> Schema {
        let partition_type = crate::spec::partition_type(self.table.metadata()).unwrap();
        let schema = Schema::builder()
            .with_fields(crate::spec::_const_schema::manifest_schema_fields_v2(
                &partition_type,
            ))
            .build()
            .unwrap();
        let readable_metric_schema = ReadableMetricsStructBuilder::readable_metrics_schema(
            self.table.metadata().current_schema(),
            &schema,
        );
        join_schemas(&schema, &readable_metric_schema.unwrap()).unwrap()
    }
}

/// Builds the struct describing data files listed in a table manifest.
///
/// For reference, see the Java implementation of [`DataFile`][1].
///
/// [1]: https://github.com/apache/iceberg/blob/apache-iceberg-1.7.1/api/src/main/java/org/apache/iceberg/DataFile.java
struct DataFileStructBuilder<'a> {
    /// Builder for data file struct (including the partition struct).
    struct_builder: StructBuilder,
    /// Arrow fields of the combined partition struct of all partition specs.
    /// We require this to reconstruct the field builders in the partition [`StructBuilder`].
    combined_partition_fields: Fields,
    /// Table metadata to look up partition specs by partition spec id.
    table_metadata: &'a TableMetadata,
}

impl<'a> DataFileStructBuilder<'a> {
    fn new(table_metadata: &'a TableMetadata) -> Result<Self> {
        let combined_partition_type = crate::spec::partition_type(table_metadata)?;
        let data_file_fields =
            crate::spec::_const_schema::data_file_fields_v2(&combined_partition_type);
        let data_file_schema = Schema::builder().with_fields(data_file_fields).build()?;
        let DataType::Struct(combined_partition_fields) =
            type_to_arrow_type(&Type::Struct(combined_partition_type))?
        else {
            panic!("Converted Arrow type was not struct")
        };

        Ok(DataFileStructBuilder {
            struct_builder: StructBuilder::from_fields(
                schema_to_arrow_schema(&data_file_schema)?.fields,
                0,
            ),
            combined_partition_fields,
            table_metadata,
        })
    }

    fn append(&mut self, manifest_file: &ManifestFile, data_file: &DataFile) -> Result<()> {
        // Content type
        self.field_builder::<Int32Builder>(0)?
            .append_value(data_file.content as i32);

        // File path
        self.field_builder::<StringBuilder>(1)?
            .append_value(data_file.file_path());

        // File format
        self.field_builder::<StringBuilder>(2)?
            .append_value(data_file.file_format().to_string().to_uppercase());

        // Partitions
        self.append_partition_values(manifest_file.partition_spec_id, data_file.partition())?;

        // Record count
        self.field_builder::<Int64Builder>(4)?
            .append_value(data_file.record_count() as i64);

        // File size in bytes
        self.field_builder::<Int64Builder>(5)?
            .append_value(data_file.file_size_in_bytes() as i64);

        // Column sizes
        let (column_size_keys, column_size_values): (Vec<i32>, Vec<i64>) = data_file
            .column_sizes()
            .iter()
            .sorted_by_key(|(k, _)| *k)
            .map(|(k, v)| (k, *v as i64))
            .unzip();
        self.append_to_map_field::<Int32Builder, Int64Builder>(
            6,
            |key_builder| key_builder.append_slice(column_size_keys.as_slice()),
            |value_builder| value_builder.append_slice(column_size_values.as_slice()),
        )?;

        // Value counts
        let (value_count_keys, value_count_values): (Vec<i32>, Vec<i64>) = data_file
            .value_counts()
            .iter()
            .sorted_by_key(|(k, _)| *k)
            .map(|(k, v)| (k, *v as i64))
            .unzip();
        self.append_to_primitive_map_field::<Int32Type, Int64Type>(
            7,
            value_count_keys.as_slice(),
            value_count_values.as_slice(),
        )?;

        // Null value counts
        let (null_count_keys, null_count_values): (Vec<i32>, Vec<i64>) = data_file
            .null_value_counts()
            .iter()
            .sorted_by_key(|(k, _)| *k)
            .map(|(k, v)| (k, *v as i64))
            .unzip();
        self.append_to_primitive_map_field::<Int32Type, Int64Type>(
            8,
            null_count_keys.as_slice(),
            null_count_values.as_slice(),
        )?;

        // Nan value counts
        let (nan_count_keys, nan_count_values): (Vec<i32>, Vec<i64>) = data_file
            .nan_value_counts()
            .iter()
            .sorted_by_key(|(k, _)| *k)
            .map(|(k, v)| (k, *v as i64))
            .unzip();
        self.append_to_primitive_map_field::<Int32Type, Int64Type>(
            9,
            nan_count_keys.as_slice(),
            nan_count_values.as_slice(),
        )?;

        // Lower bounds
        let (lower_bound_keys, lower_bound_values): (Vec<i32>, Vec<Datum>) = data_file
            .lower_bounds()
            .iter()
            .sorted_by_key(|(k, _)| *k)
            .map(|(k, v)| (k, v.clone()))
            .unzip();
        self.append_to_map_field::<Int32Builder, LargeBinaryBuilder>(
            10,
            |key_builder| key_builder.append_slice(lower_bound_keys.as_slice()),
            |value_builder| {
                for v in &lower_bound_values {
                    value_builder.append_value(v.to_bytes().unwrap())
                }
            },
        )?;

        // Upper bounds
        let (upper_bound_keys, upper_bound_values): (Vec<i32>, Vec<Datum>) = data_file
            .upper_bounds()
            .iter()
            .sorted_by_key(|(k, _)| *k)
            .map(|(k, v)| (k, v.clone()))
            .unzip();
        self.append_to_map_field::<Int32Builder, LargeBinaryBuilder>(
            11,
            |key_builder| key_builder.append_slice(upper_bound_keys.as_slice()),
            |value_builder| {
                for v in &upper_bound_values {
                    value_builder.append_value(v.to_bytes().unwrap())
                }
            },
        )?;

        // Key metadata
        self.field_builder::<LargeBinaryBuilder>(12)?
            .append_option(data_file.key_metadata());

        // Split offsets
        self.append_to_list_field::<Int64Type>(13, data_file.split_offsets())?;

        // Equality ids
        self.append_to_list_field::<Int32Type>(14, data_file.equality_ids())?;

        // Sort order ids
        self.field_builder::<Int32Builder>(15)?
            .append_option(data_file.sort_order_id());

        // Append an element in the struct
        self.struct_builder.append(true);
        Ok(())
    }

    fn field_builder<T: ArrayBuilder>(&mut self, index: usize) -> Result<&mut T> {
        self.struct_builder.field_builder_or_err::<T>(index)
    }

    fn append_to_list_field<T: ArrowPrimitiveType>(
        &mut self,
        index: usize,
        values: &[T::Native],
    ) -> Result<()> {
        let list_builder = self.field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(index)?;
        list_builder
            .values()
            .cast_or_err::<PrimitiveBuilder<T>>()?
            .append_slice(values);
        list_builder.append(true);
        Ok(())
    }

    fn append_to_map_field<K: ArrayBuilder, V: ArrayBuilder>(
        &mut self,
        index: usize,
        key_func: impl Fn(&mut K),
        value_func: impl Fn(&mut V),
    ) -> Result<()> {
        let map_builder =
            self.field_builder::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>(index)?;
        key_func(map_builder.keys().cast_or_err::<K>()?);
        value_func(map_builder.values().cast_or_err::<V>()?);
        Ok(map_builder.append(true)?)
    }

    fn append_to_primitive_map_field<K: ArrowPrimitiveType, V: ArrowPrimitiveType>(
        &mut self,
        index: usize,
        keys: &[K::Native],
        values: &[V::Native],
    ) -> Result<()> {
        let map_builder =
            self.field_builder::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>(index)?;
        map_builder
            .keys()
            .cast_or_err::<PrimitiveBuilder<K>>()?
            .append_slice(keys);
        map_builder
            .values()
            .cast_or_err::<PrimitiveBuilder<V>>()?
            .append_slice(values);
        Ok(map_builder.append(true)?)
    }

    fn append_partition_values(
        &mut self,
        partition_spec_id: i32,
        partition_values: &Struct,
    ) -> Result<()> {
        // Get the partition fields for the partition spec id in the manifest file
        let partition_spec = self
            .table_metadata
            .partition_spec_by_id(partition_spec_id)
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Partition spec not found"))?
            .fields();
        // Clone here so we don't hold an immutable reference as we mutably-borrow the builder below
        let combined_partition_fields = self.combined_partition_fields.clone();
        // Get the partition struct builder
        let partition_builder = self.field_builder::<StructBuilder>(3)?;
        // Iterate the manifest's partition fields with the respect partition values from the data file
        for (partition_field, partition_value) in
            partition_spec.iter().zip_eq(partition_values.iter())
        {
            let (combined_index, combined_partition_spec_field) = combined_partition_fields
                .find(&partition_field.name)
                .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Partition field not found"))?;
            let partition_type = combined_partition_spec_field.data_type();
            let partition_value: Option<PrimitiveLiteral> = partition_value
                .map(|value| -> std::result::Result<PrimitiveLiteral, Error> {
                    value.as_primitive_literal().ok_or({
                        Error::new(
                            ErrorKind::FeatureUnsupported,
                            "Only primitive types support in partition struct",
                        )
                    })
                })
                .transpose()?;

            // Append a literal to a field builder cast based on the expected partition field type.
            // We cannot solely rely on the literal type, because it doesn't sufficiently specify
            // the underlying type. E.g., a `PrimtivieLiteral::Long` could represent either a long
            // or a timestamp.
            match (partition_type, partition_value.clone()) {
                (DataType::Boolean, Some(PrimitiveLiteral::Boolean(value))) => partition_builder
                    .field_builder_or_err::<BooleanBuilder>(combined_index)?
                    .append_value(value),
                (DataType::Boolean, None) => partition_builder
                    .field_builder_or_err::<BooleanBuilder>(combined_index)?
                    .append_null(),
                (DataType::Int32, Some(PrimitiveLiteral::Int(value))) => partition_builder
                    .field_builder_or_err::<Int32Builder>(combined_index)?
                    .append_value(value),
                (DataType::Int32, None) => partition_builder
                    .field_builder_or_err::<Int32Builder>(combined_index)?
                    .append_null(),
                (DataType::Int64, Some(PrimitiveLiteral::Long(value))) => partition_builder
                    .field_builder_or_err::<Int64Builder>(combined_index)?
                    .append_value(value),
                (DataType::Int64, None) => partition_builder
                    .field_builder_or_err::<Int64Builder>(combined_index)?
                    .append_null(),
                (DataType::Float32, Some(PrimitiveLiteral::Float(OrderedFloat(value)))) => {
                    partition_builder
                        .field_builder_or_err::<Float32Builder>(combined_index)?
                        .append_value(value)
                }
                (DataType::Float32, None) => partition_builder
                    .field_builder_or_err::<Float32Builder>(combined_index)?
                    .append_null(),
                (DataType::Float64, Some(PrimitiveLiteral::Double(OrderedFloat(value)))) => {
                    partition_builder
                        .field_builder_or_err::<Float64Builder>(combined_index)?
                        .append_value(value)
                }
                (DataType::Float64, None) => partition_builder
                    .field_builder_or_err::<Float64Builder>(combined_index)?
                    .append_null(),
                (DataType::Utf8, Some(PrimitiveLiteral::String(value))) => partition_builder
                    .field_builder_or_err::<StringBuilder>(combined_index)?
                    .append_value(value),
                (DataType::Utf8, None) => partition_builder
                    .field_builder_or_err::<StringBuilder>(combined_index)?
                    .append_null(),
                (DataType::FixedSizeBinary(_), Some(PrimitiveLiteral::Binary(value))) => {
                    partition_builder
                        .field_builder_or_err::<FixedSizeBinaryBuilder>(combined_index)?
                        .append_value(value)?
                }
                (DataType::FixedSizeBinary(_), None) => partition_builder
                    .field_builder_or_err::<FixedSizeBinaryBuilder>(combined_index)?
                    .append_null(),
                (DataType::LargeBinary, Some(PrimitiveLiteral::Binary(value))) => partition_builder
                    .field_builder_or_err::<LargeBinaryBuilder>(combined_index)?
                    .append_value(value),
                (DataType::LargeBinary, None) => partition_builder
                    .field_builder_or_err::<LargeBinaryBuilder>(combined_index)?
                    .append_null(),
                (DataType::Date32, Some(PrimitiveLiteral::Int(value))) => partition_builder
                    .field_builder_or_err::<Date32Builder>(combined_index)?
                    .append_value(value),
                (DataType::Date32, None) => partition_builder
                    .field_builder_or_err::<Date32Builder>(combined_index)?
                    .append_null(),
                (
                    DataType::Timestamp(TimeUnit::Microsecond, _),
                    Some(PrimitiveLiteral::Long(value)),
                ) => partition_builder
                    .field_builder_or_err::<TimestampMicrosecondBuilder>(combined_index)?
                    .append_value(value),
                (DataType::Timestamp(TimeUnit::Microsecond, _), None) => partition_builder
                    .field_builder_or_err::<TimestampMicrosecondBuilder>(combined_index)?
                    .append_null(),
                (
                    DataType::Timestamp(TimeUnit::Nanosecond, _),
                    Some(PrimitiveLiteral::Long(value)),
                ) => partition_builder
                    .field_builder_or_err::<TimestampNanosecondBuilder>(combined_index)?
                    .append_value(value),
                (DataType::Timestamp(TimeUnit::Nanosecond, _), None) => partition_builder
                    .field_builder_or_err::<TimestampNanosecondBuilder>(combined_index)?
                    .append_null(),
                (DataType::Decimal128(_, _), Some(PrimitiveLiteral::Int128(value))) => {
                    partition_builder
                        .field_builder_or_err::<Decimal128Builder>(combined_index)?
                        .append_value(value)
                }
                (DataType::Decimal128(_, _), None) => partition_builder
                    .field_builder_or_err::<Decimal128Builder>(combined_index)?
                    .append_null(),
                (_, _) => {
                    return Err(Error::new(
                        ErrorKind::FeatureUnsupported,
                        format!(
                            "Cannot build partition struct with field type {:?} and partition value {:?}",
                            partition_type, partition_value
                        ),
                    ));
                }
            }
        }

        // Append an element in the struct
        partition_builder.append(true);
        Ok(())
    }

    fn finish(&mut self) -> StructArray {
        self.struct_builder.finish()
    }
}

/// Join two schemas by concatenating fields. Return [`Error`] if the schemas have fields with the
/// same field id but different types.
fn join_schemas(left: &Schema, right: &Schema) -> Result<Schema> {
    let mut joined_fields: Vec<NestedFieldRef> = left.as_struct().fields().to_vec();

    for right_field in right.as_struct().fields() {
        match left.field_by_id(right_field.id) {
            None => {
                joined_fields.push(right_field.clone());
            }
            Some(left_field) => {
                if left_field != right_field {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Schemas have different columns with the same id: {:?}, {:?}",
                            left_field, right_field
                        ),
                    ));
                }
            }
        }
    }

    Schema::builder().with_fields(joined_fields).build()
}

/// Helper to cast a field builder in a [`StructBuilder`] to a specific builder type or return an
/// [`Error`].
trait StructBuilderExt {
    fn field_builder_or_err<T: ArrayBuilder>(&mut self, index: usize) -> Result<&mut T>;
}

impl StructBuilderExt for StructBuilder {
    fn field_builder_or_err<T: ArrayBuilder>(&mut self, index: usize) -> Result<&mut T> {
        self.field_builder::<T>(index).ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                format!(
                    "Field builder not found for index {index} and type {}",
                    type_name::<T>()
                ),
            )
        })
    }
}

/// Helper to cast a [`Box<dyn ArrayBuilder>`] to a specific type or return an [`Error`].
trait ArrayBuilderExt {
    fn cast_or_err<T: ArrayBuilder>(&mut self) -> Result<&mut T>;
}

impl ArrayBuilderExt for Box<dyn ArrayBuilder> {
    fn cast_or_err<T: ArrayBuilder>(&mut self) -> Result<&mut T> {
        self.as_any_mut().downcast_mut::<T>().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Cannot cast builder to type {}", type_name::<T>()),
            )
        })
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
                Field { name: "data_file", data_type: Struct([Field { name: "content", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "134"} }, Field { name: "file_path", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "100"} }, Field { name: "file_format", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "101"} }, Field { name: "partition", data_type: Struct([Field { name: "x", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1000"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "102"} }, Field { name: "record_count", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "103"} }, Field { name: "file_size_in_bytes", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "104"} }, Field { name: "column_sizes", data_type: Map(Field { name: "key_value", data_type: Struct([Field { name: "key", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "117"} }, Field { name: "value", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "118"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "108"} }, Field { name: "value_counts", data_type: Map(Field { name: "key_value", data_type: Struct([Field { name: "key", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "119"} }, Field { name: "value", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "120"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "109"} }, Field { name: "null_value_counts", data_type: Map(Field { name: "key_value", data_type: Struct([Field { name: "key", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "121"} }, Field { name: "value", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "122"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "110"} }, Field { name: "nan_value_counts", data_type: Map(Field { name: "key_value", data_type: Struct([Field { name: "key", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "138"} }, Field { name: "value", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "139"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "137"} }, Field { name: "lower_bounds", data_type: Map(Field { name: "key_value", data_type: Struct([Field { name: "key", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "126"} }, Field { name: "value", data_type: LargeBinary, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "127"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "125"} }, Field { name: "upper_bounds", data_type: Map(Field { name: "key_value", data_type: Struct([Field { name: "key", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "129"} }, Field { name: "value", data_type: LargeBinary, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "130"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "128"} }, Field { name: "key_metadata", data_type: LargeBinary, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "131"} }, Field { name: "split_offsets", data_type: List(Field { name: "element", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "133"} }), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "132"} }, Field { name: "equality_ids", data_type: List(Field { name: "element", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "136"} }), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "135"} }, Field { name: "sort_order_id", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "140"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "2"} },
                Field { name: "readable_metrics", data_type: Struct([Field { name: "a", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1001"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1002"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1003"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1004"} }, Field { name: "lower_bound", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1005"} }, Field { name: "upper_bound", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1006"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1007"} }, Field { name: "binary", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1008"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1009"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1010"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1011"} }, Field { name: "lower_bound", data_type: LargeBinary, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1012"} }, Field { name: "upper_bound", data_type: LargeBinary, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1013"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1014"} }, Field { name: "bool", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1015"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1016"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1017"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1018"} }, Field { name: "lower_bound", data_type: Boolean, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1019"} }, Field { name: "upper_bound", data_type: Boolean, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1020"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1021"} }, Field { name: "date", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1022"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1023"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1024"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1025"} }, Field { name: "lower_bound", data_type: Date32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1026"} }, Field { name: "upper_bound", data_type: Date32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1027"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1028"} }, Field { name: "dbl", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1029"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1030"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1031"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1032"} }, Field { name: "lower_bound", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1033"} }, Field { name: "upper_bound", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1034"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1035"} }, Field { name: "decimal", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1036"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1037"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1038"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1039"} }, Field { name: "lower_bound", data_type: Decimal128(3, 2), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1040"} }, Field { name: "upper_bound", data_type: Decimal128(3, 2), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1041"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1042"} }, Field { name: "float", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1043"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1044"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1045"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1046"} }, Field { name: "lower_bound", data_type: Float32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1047"} }, Field { name: "upper_bound", data_type: Float32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1048"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1049"} }, Field { name: "i32", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1050"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1051"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1052"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1053"} }, Field { name: "lower_bound", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1054"} }, Field { name: "upper_bound", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1055"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1056"} }, Field { name: "i64", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1057"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1058"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1059"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1060"} }, Field { name: "lower_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1061"} }, Field { name: "upper_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1062"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1063"} }, Field { name: "timestamp", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1064"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1065"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1066"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1067"} }, Field { name: "lower_bound", data_type: Timestamp(Microsecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1068"} }, Field { name: "upper_bound", data_type: Timestamp(Microsecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1069"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1070"} }, Field { name: "timestampns", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1071"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1072"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1073"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1074"} }, Field { name: "lower_bound", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1075"} }, Field { name: "upper_bound", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1076"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1077"} }, Field { name: "timestamptz", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1078"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1079"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1080"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1081"} }, Field { name: "lower_bound", data_type: Timestamp(Microsecond, Some("+00:00")), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1082"} }, Field { name: "upper_bound", data_type: Timestamp(Microsecond, Some("+00:00")), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1083"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1084"} }, Field { name: "timestamptzns", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1085"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1086"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1087"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1088"} }, Field { name: "lower_bound", data_type: Timestamp(Nanosecond, Some("+00:00")), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1089"} }, Field { name: "upper_bound", data_type: Timestamp(Nanosecond, Some("+00:00")), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1090"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1091"} }, Field { name: "x", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1092"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1093"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1094"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1095"} }, Field { name: "lower_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1096"} }, Field { name: "upper_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1097"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1098"} }, Field { name: "y", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1099"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1100"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1101"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1102"} }, Field { name: "lower_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1103"} }, Field { name: "upper_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1104"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1105"} }, Field { name: "z", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1106"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1107"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1108"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1109"} }, Field { name: "lower_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1110"} }, Field { name: "upper_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1111"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1112"} }]), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1113"} }"#]],
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
