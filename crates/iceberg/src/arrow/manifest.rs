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

//! Conversion of manifest entries to Arrow.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{
    ArrayBuilder, Int32Builder, Int64Builder, LargeBinaryBuilder, ListBuilder, MapBuilder,
    MapFieldNames, StringBuilder,
};
use arrow_array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray, Float32Array,
    Float64Array, Int32Array, Int64Array, LargeBinaryArray, NullArray, RecordBatch, StringArray,
    StructArray, Time64MicrosecondArray, TimestampMicrosecondArray, TimestampNanosecondArray,
};
use arrow_schema::{DataType, Field, FieldRef};

use crate::arrow::{UTC_TIME_ZONE, schema_to_arrow_schema};
use crate::error::invalid_data;
use crate::spec::{
    DataFileFormat, Datum, Literal, Manifest, ManifestEntryRef, NestedField, PrimitiveLiteral,
    PrimitiveType, Schema, StructType, manifest_entry_fields,
};
use crate::{Error, ErrorKind, Result};

/// Converts the entries of `manifest` into a [`RecordBatch`] with one row per
/// entry.
///
/// The schema is the spec's `manifest_entry` struct for the latest format
/// version, with field IDs in the Arrow field metadata, so manifests of every
/// format version convert to the same schema apart from `data_file.partition`,
/// which follows the manifest's partition spec. Lower and upper bounds use the
/// spec's binary single-value serialization. The metric maps list their
/// entries in no particular order, and a metric map with no entries is empty
/// rather than null.
///
/// Entries are converted as they are in `manifest`. Fields that readers
/// inherit from the manifest list, such as `snapshot_id` and
/// `sequence_number`, stay null in a manifest from [`Manifest::parse_avro`].
/// [`ManifestReader::read`](crate::spec::ManifestReader::read) fills them in.
///
/// # Example
///
/// ```
/// use std::sync::Arc;
///
/// use iceberg::arrow::manifest_to_record_batch;
/// use iceberg::spec::{
///     DataContentType, DataFileBuilder, DataFileFormat, FormatVersion, Manifest,
///     ManifestContentType, ManifestEntry, ManifestMetadata, ManifestStatus, NestedField,
///     PartitionSpec, PrimitiveType, Schema, Type,
/// };
///
/// let schema = Arc::new(
///     Schema::builder()
///         .with_fields(vec![
///             NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
///         ])
///         .build()?,
/// );
/// let data_file = DataFileBuilder::default()
///     .content(DataContentType::Data)
///     .file_path("s3://bucket/data/0.parquet".to_string())
///     .file_format(DataFileFormat::Parquet)
///     .record_count(100)
///     .file_size_in_bytes(4096)
///     .build()?;
/// let manifest = Manifest::new(
///     ManifestMetadata {
///         schema: schema.clone(),
///         schema_id: 0,
///         partition_spec: PartitionSpec::builder(schema).build()?,
///         format_version: FormatVersion::V2,
///         content: ManifestContentType::Data,
///     },
///     vec![
///         ManifestEntry::builder()
///             .status(ManifestStatus::Added)
///             .data_file(data_file)
///             .build(),
///     ],
/// );
///
/// let batch = manifest_to_record_batch(&manifest)?;
/// assert_eq!(batch.num_rows(), 1);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn manifest_to_record_batch(manifest: &Manifest) -> Result<RecordBatch> {
    let metadata = manifest.metadata();
    let partition_type = metadata
        .partition_spec()
        .partition_type(metadata.schema())?;
    let schema = Arc::new(schema_to_arrow_schema(
        &Schema::builder()
            .with_fields(manifest_entry_fields(&partition_type))
            .build()?,
    )?);
    let DataType::Struct(data_file_fields) = schema.field_with_name("data_file")?.data_type()
    else {
        unreachable!()
    };
    let child = |name: &str| data_file_fields.find(name).unwrap().1;

    let entries = manifest.entries();
    let len = entries.len();
    let mut status = Int32Builder::with_capacity(len);
    let mut snapshot_id = Int64Builder::with_capacity(len);
    let mut sequence_number = Int64Builder::with_capacity(len);
    let mut file_sequence_number = Int64Builder::with_capacity(len);
    let mut content = Int32Builder::with_capacity(len);
    let mut file_path = StringBuilder::new();
    let mut file_format = StringBuilder::new();
    let mut record_count = Int64Builder::with_capacity(len);
    let mut file_size_in_bytes = Int64Builder::with_capacity(len);
    let mut column_sizes = map_builder(child("column_sizes"), Int64Builder::new());
    let mut value_counts = map_builder(child("value_counts"), Int64Builder::new());
    let mut null_value_counts = map_builder(child("null_value_counts"), Int64Builder::new());
    let mut nan_value_counts = map_builder(child("nan_value_counts"), Int64Builder::new());
    let mut lower_bounds = map_builder(child("lower_bounds"), LargeBinaryBuilder::new());
    let mut upper_bounds = map_builder(child("upper_bounds"), LargeBinaryBuilder::new());
    let mut key_metadata = LargeBinaryBuilder::new();
    let mut split_offsets =
        ListBuilder::new(Int64Builder::new()).with_field(list_element(child("split_offsets")));
    let mut equality_ids =
        ListBuilder::new(Int32Builder::new()).with_field(list_element(child("equality_ids")));
    let mut sort_order_id = Int32Builder::with_capacity(len);
    let mut first_row_id = Int64Builder::with_capacity(len);
    let mut referenced_data_file = StringBuilder::new();
    let mut content_offset = Int64Builder::with_capacity(len);
    let mut content_size_in_bytes = Int64Builder::with_capacity(len);

    for entry in entries {
        status.append_value(entry.status as i32);
        snapshot_id.append_option(entry.snapshot_id);
        sequence_number.append_option(entry.sequence_number);
        file_sequence_number.append_option(entry.file_sequence_number);

        let data_file = &entry.data_file;
        content.append_value(data_file.content as i32);
        file_path.append_value(&data_file.file_path);
        // Writers store the format name in upper case.
        file_format.append_value(match data_file.file_format {
            DataFileFormat::Avro => "AVRO",
            DataFileFormat::Orc => "ORC",
            DataFileFormat::Parquet => "PARQUET",
            DataFileFormat::Puffin => "PUFFIN",
        });
        record_count.append_value(data_file.record_count.try_into()?);
        file_size_in_bytes.append_value(data_file.file_size_in_bytes.try_into()?);
        append_counts(&mut column_sizes, &data_file.column_sizes)?;
        append_counts(&mut value_counts, &data_file.value_counts)?;
        append_counts(&mut null_value_counts, &data_file.null_value_counts)?;
        append_counts(&mut nan_value_counts, &data_file.nan_value_counts)?;
        append_bounds(&mut lower_bounds, &data_file.lower_bounds)?;
        append_bounds(&mut upper_bounds, &data_file.upper_bounds)?;
        key_metadata.append_option(data_file.key_metadata.as_deref());
        match &data_file.split_offsets {
            Some(offsets) => {
                split_offsets.values().append_slice(offsets);
                split_offsets.append(true);
            }
            None => split_offsets.append(false),
        }
        match &data_file.equality_ids {
            Some(ids) => {
                equality_ids.values().append_slice(ids);
                equality_ids.append(true);
            }
            None => equality_ids.append(false),
        }
        sort_order_id.append_option(data_file.sort_order_id);
        first_row_id.append_option(data_file.first_row_id);
        referenced_data_file.append_option(data_file.referenced_data_file.as_deref());
        content_offset.append_option(data_file.content_offset);
        content_size_in_bytes.append_option(data_file.content_size_in_bytes);
    }

    let partition = partition_column(entries, &partition_type, child("partition"))?;
    let data_file = StructArray::try_new(
        data_file_fields.clone(),
        vec![
            Arc::new(content.finish()),
            Arc::new(file_path.finish()),
            Arc::new(file_format.finish()),
            partition,
            Arc::new(record_count.finish()),
            Arc::new(file_size_in_bytes.finish()),
            Arc::new(column_sizes.finish()),
            Arc::new(value_counts.finish()),
            Arc::new(null_value_counts.finish()),
            Arc::new(nan_value_counts.finish()),
            Arc::new(lower_bounds.finish()),
            Arc::new(upper_bounds.finish()),
            Arc::new(key_metadata.finish()),
            Arc::new(split_offsets.finish()),
            Arc::new(equality_ids.finish()),
            Arc::new(sort_order_id.finish()),
            Arc::new(first_row_id.finish()),
            Arc::new(referenced_data_file.finish()),
            Arc::new(content_offset.finish()),
            Arc::new(content_size_in_bytes.finish()),
        ],
        None,
    )?;

    Ok(RecordBatch::try_new(schema, vec![
        Arc::new(status.finish()),
        Arc::new(snapshot_id.finish()),
        Arc::new(sequence_number.finish()),
        Arc::new(file_sequence_number.finish()),
        Arc::new(data_file),
    ])?)
}

fn list_element(field: &Field) -> FieldRef {
    let DataType::List(element) = field.data_type() else {
        unreachable!()
    };
    element.clone()
}

/// Creates a builder for the `map<int, ...>` column `field` that produces the
/// key and value fields of its schema, including their field IDs.
fn map_builder<V: ArrayBuilder>(field: &Field, values: V) -> MapBuilder<Int32Builder, V> {
    let DataType::Map(entries, _) = field.data_type() else {
        unreachable!()
    };
    let DataType::Struct(key_value) = entries.data_type() else {
        unreachable!()
    };
    let (key, value) = (&key_value[0], &key_value[1]);
    let names = MapFieldNames {
        entry: entries.name().clone(),
        key: key.name().clone(),
        value: value.name().clone(),
    };
    MapBuilder::new(Some(names), Int32Builder::new(), values)
        .with_keys_field(key.clone())
        .with_values_field(value.clone())
}

fn append_counts(
    builder: &mut MapBuilder<Int32Builder, Int64Builder>,
    counts: &HashMap<i32, u64>,
) -> Result<()> {
    for (&field_id, &count) in counts {
        builder.keys().append_value(field_id);
        builder.values().append_value(count.try_into()?);
    }
    Ok(builder.append(true)?)
}

fn append_bounds(
    builder: &mut MapBuilder<Int32Builder, LargeBinaryBuilder>,
    bounds: &HashMap<i32, Datum>,
) -> Result<()> {
    for (&field_id, bound) in bounds {
        builder.keys().append_value(field_id);
        builder.values().append_value(bound.to_bytes()?);
    }
    Ok(builder.append(true)?)
}

/// Builds the `data_file.partition` column, one child array per partition field.
fn partition_column(
    entries: &[ManifestEntryRef],
    partition_type: &StructType,
    field: &Field,
) -> Result<ArrayRef> {
    if let Some(entry) = entries
        .iter()
        .find(|entry| entry.data_file.partition.fields().len() != partition_type.fields().len())
    {
        return Err(invalid_data!(
            "partition of {} has {} values, but the partition spec has {} fields",
            entry.data_file.file_path,
            entry.data_file.partition.fields().len(),
            partition_type.fields().len()
        ));
    }

    let DataType::Struct(fields) = field.data_type() else {
        unreachable!()
    };
    if fields.is_empty() {
        return Ok(Arc::new(StructArray::new_empty_fields(entries.len(), None)));
    }
    let columns = partition_type
        .fields()
        .iter()
        .zip(fields)
        .enumerate()
        .map(|(index, (partition_field, arrow_field))| {
            let values: Vec<_> = entries
                .iter()
                .map(|entry| match &entry.data_file.partition[index] {
                    None => Ok(None),
                    Some(Literal::Primitive(value)) => Ok(Some(value)),
                    Some(value) => Err(invalid_data!(
                        "partition field {} has non-primitive value {value:?}",
                        partition_field.name
                    )),
                })
                .collect::<Result<_>>()?;
            partition_array(partition_field, arrow_field.data_type(), &values)
        })
        .collect::<Result<_>>()?;
    Ok(Arc::new(StructArray::try_new(
        fields.clone(),
        columns,
        None,
    )?))
}

/// Builds the array of one partition field from its values in each entry.
fn partition_array(
    field: &NestedField,
    data_type: &DataType,
    values: &[Option<&PrimitiveLiteral>],
) -> Result<ArrayRef> {
    let primitive_type = field.field_type.as_primitive_type().ok_or_else(|| {
        invalid_data!(
            "partition field {} has non-primitive type {}",
            field.name,
            field.field_type
        )
    })?;
    // Collects the values, returning an error for a value of another type.
    fn collect<'a, T, A: FromIterator<Option<T>>>(
        field: &NestedField,
        values: &[Option<&'a PrimitiveLiteral>],
        value: impl Fn(&'a PrimitiveLiteral) -> Option<T>,
    ) -> Result<A> {
        values
            .iter()
            .map(|literal| {
                literal
                    .map(|literal| {
                        value(literal).ok_or_else(|| {
                            invalid_data!(
                                "partition field {} of type {} can't hold {literal:?}",
                                field.name,
                                field.field_type
                            )
                        })
                    })
                    .transpose()
            })
            .collect()
    }
    fn int(literal: &PrimitiveLiteral) -> Option<i32> {
        match literal {
            PrimitiveLiteral::Int(v) => Some(*v),
            _ => None,
        }
    }
    fn long(literal: &PrimitiveLiteral) -> Option<i64> {
        match literal {
            PrimitiveLiteral::Long(v) => Some(*v),
            _ => None,
        }
    }
    fn binary(literal: &PrimitiveLiteral) -> Option<&[u8]> {
        match literal {
            PrimitiveLiteral::Binary(v) => Some(v),
            _ => None,
        }
    }
    let array: ArrayRef =
        match primitive_type {
            PrimitiveType::Boolean => Arc::new(collect::<_, BooleanArray>(
                field,
                values,
                |literal| match literal {
                    PrimitiveLiteral::Boolean(v) => Some(*v),
                    _ => None,
                },
            )?),
            PrimitiveType::Int => Arc::new(collect::<_, Int32Array>(field, values, int)?),
            PrimitiveType::Date => Arc::new(collect::<_, Date32Array>(field, values, int)?),
            PrimitiveType::Long => Arc::new(collect::<_, Int64Array>(field, values, long)?),
            PrimitiveType::Time => {
                Arc::new(collect::<_, Time64MicrosecondArray>(field, values, long)?)
            }
            PrimitiveType::Timestamp => Arc::new(collect::<_, TimestampMicrosecondArray>(
                field, values, long,
            )?),
            PrimitiveType::Timestamptz => Arc::new(
                collect::<_, TimestampMicrosecondArray>(field, values, long)?
                    .with_timezone(UTC_TIME_ZONE),
            ),
            PrimitiveType::TimestampNs => {
                Arc::new(collect::<_, TimestampNanosecondArray>(field, values, long)?)
            }
            PrimitiveType::TimestamptzNs => Arc::new(
                collect::<_, TimestampNanosecondArray>(field, values, long)?
                    .with_timezone(UTC_TIME_ZONE),
            ),
            PrimitiveType::Float => Arc::new(collect::<_, Float32Array>(
                field,
                values,
                |literal| match literal {
                    PrimitiveLiteral::Float(v) => Some(v.0),
                    _ => None,
                },
            )?),
            PrimitiveType::Double => Arc::new(collect::<_, Float64Array>(
                field,
                values,
                |literal| match literal {
                    PrimitiveLiteral::Double(v) => Some(v.0),
                    _ => None,
                },
            )?),
            PrimitiveType::Decimal { .. } => {
                let DataType::Decimal128(precision, scale) = *data_type else {
                    unreachable!()
                };
                Arc::new(
                    collect::<_, Decimal128Array>(field, values, |literal| match literal {
                        PrimitiveLiteral::Int128(v) => Some(*v),
                        _ => None,
                    })?
                    .with_precision_and_scale(precision, scale)?,
                )
            }
            PrimitiveType::String => Arc::new(collect::<_, StringArray>(
                field,
                values,
                |literal| match literal {
                    PrimitiveLiteral::String(v) => Some(v.as_str()),
                    _ => None,
                },
            )?),
            PrimitiveType::Uuid => {
                let uuids: Vec<_> = collect(field, values, |literal| match literal {
                    PrimitiveLiteral::UInt128(v) => Some(v.to_be_bytes()),
                    _ => None,
                })?;
                Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    uuids.into_iter(),
                    16,
                )?)
            }
            PrimitiveType::Fixed(_) => {
                let DataType::FixedSizeBinary(width) = *data_type else {
                    return Err(Error::new(
                        ErrorKind::FeatureUnsupported,
                        format!("fixed partition field {} is {data_type}", field.name),
                    ));
                };
                let bytes: Vec<_> = collect(field, values, binary)?;
                Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(bytes.into_iter(), width)
                        .map_err(|err| {
                            invalid_data!(
                                "partition field {} has a value of the wrong width",
                                field.name
                            )
                            .with_source(err)
                        })?,
                )
            }
            PrimitiveType::Binary => {
                Arc::new(collect::<_, LargeBinaryArray>(field, values, binary)?)
            }
            PrimitiveType::Unknown => {
                // Only nulls fit, so this returns an error for any value.
                collect::<(), Vec<_>>(field, values, |_| None)?;
                Arc::new(NullArray::new(values.len()))
            }
        };
    Ok(array)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use apache_avro::Reader as AvroReader;
    use apache_avro::types::Value as AvroValue;
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Int32Type, Int64Type};
    use arrow_array::{
        Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
        Float32Array, Float64Array, Int32Array, Int64Array, LargeBinaryArray, NullArray,
        StringArray, StructArray, Time64MicrosecondArray, TimestampMicrosecondArray,
        TimestampNanosecondArray,
    };
    use uuid::Uuid;

    use super::*;
    use crate::arrow::arrow_schema_to_schema;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, FormatVersion, ListType,
        Literal, ManifestContentType, ManifestEntry, ManifestMetadata, ManifestStatus, MapType,
        NestedField, PartitionSpec, PrimitiveType, Schema, Struct, StructType, Transform, Type,
    };

    const UUID: &str = "f79c3e09-677c-4bbd-a479-3f349cb785e7";

    fn table_schema() -> Schema {
        let columns = [
            (1, "b", PrimitiveType::Boolean),
            (2, "i", PrimitiveType::Int),
            (3, "l", PrimitiveType::Long),
            (4, "f", PrimitiveType::Float),
            (5, "d", PrimitiveType::Double),
            (6, "dec", PrimitiveType::Decimal {
                precision: 38,
                scale: 10,
            }),
            (7, "date", PrimitiveType::Date),
            (8, "time", PrimitiveType::Time),
            (9, "ts", PrimitiveType::Timestamp),
            (10, "tstz", PrimitiveType::Timestamptz),
            (11, "ts_ns", PrimitiveType::TimestampNs),
            (12, "tstz_ns", PrimitiveType::TimestamptzNs),
            (13, "s", PrimitiveType::String),
            (14, "u", PrimitiveType::Uuid),
            (15, "fx", PrimitiveType::Fixed(3)),
            (16, "bin", PrimitiveType::Binary),
            (17, "unk", PrimitiveType::Unknown),
        ];
        Schema::builder()
            .with_fields(
                columns.into_iter().map(|(id, name, ty)| {
                    NestedField::optional(id, name, Type::Primitive(ty)).into()
                }),
            )
            .build()
            .unwrap()
    }

    fn partition_spec(fields: &[(&str, Transform)]) -> PartitionSpec {
        fields
            .iter()
            .fold(
                PartitionSpec::builder(table_schema()).with_spec_id(0),
                |builder, (source, transform)| {
                    builder
                        .add_partition_field(*source, format!("{source}_{transform}"), *transform)
                        .unwrap()
                },
            )
            .build()
            .unwrap()
    }

    fn manifest(partition_spec: PartitionSpec, entries: Vec<ManifestEntry>) -> Manifest {
        Manifest::new(
            ManifestMetadata {
                schema: Arc::new(table_schema()),
                schema_id: 0,
                partition_spec,
                format_version: FormatVersion::V2,
                content: ManifestContentType::Data,
            },
            entries,
        )
    }

    fn data_file(content: DataContentType, path: &str) -> DataFileBuilder {
        let mut builder = DataFileBuilder::default();
        builder
            .content(content)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .record_count(1)
            .file_size_in_bytes(10);
        builder
    }

    fn entry(status: ManifestStatus, data_file: DataFile) -> ManifestEntry {
        ManifestEntry {
            status,
            snapshot_id: None,
            sequence_number: None,
            file_sequence_number: None,
            data_file,
        }
    }

    fn data_file_column(batch: &RecordBatch, name: &str) -> ArrayRef {
        batch
            .column_by_name("data_file")
            .unwrap()
            .as_struct()
            .column_by_name(name)
            .unwrap()
            .clone()
    }

    /// Entries of the map in `row` of a `map<int, long>` column, sorted by key.
    fn long_map(column: &ArrayRef, row: usize) -> Vec<(i32, i64)> {
        let entries = column.as_map().value(row);
        let keys = entries.column(0).as_primitive::<Int32Type>();
        let values = entries.column(1).as_primitive::<Int64Type>();
        let mut pairs: Vec<_> = keys
            .values()
            .iter()
            .copied()
            .zip(values.values().iter().copied())
            .collect();
        pairs.sort_unstable();
        pairs
    }

    /// Entries of the map in `row` of a `map<int, binary>` column, sorted by key.
    fn binary_map(column: &ArrayRef, row: usize) -> Vec<(i32, Vec<u8>)> {
        let entries = column.as_map().value(row);
        let keys = entries.column(0).as_primitive::<Int32Type>();
        let values = entries.column(1).as_binary::<i64>();
        let mut pairs: Vec<_> = keys
            .values()
            .iter()
            .copied()
            .zip(values.iter().map(|v| v.unwrap().to_vec()))
            .collect();
        pairs.sort_unstable();
        pairs
    }

    fn int_map_type(key_id: i32, value_id: i32, value: PrimitiveType) -> Type {
        Type::Map(MapType::new(
            NestedField::map_key_element(key_id, Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::map_value_element(value_id, Type::Primitive(value), true).into(),
        ))
    }

    fn list_type(element_id: i32, element: PrimitiveType) -> Type {
        Type::List(ListType::new(
            NestedField::list_element(element_id, Type::Primitive(element), true).into(),
        ))
    }

    #[test]
    fn test_schema_is_spec_manifest_entry() {
        let spec = partition_spec(&[("i", Transform::Identity), ("s", Transform::Truncate(2))]);
        let batch = manifest_to_record_batch(&manifest(spec, vec![])).unwrap();
        assert_eq!(batch.num_rows(), 0);

        let long = || Type::Primitive(PrimitiveType::Long);
        let int = || Type::Primitive(PrimitiveType::Int);
        let string = || Type::Primitive(PrimitiveType::String);
        let data_file = StructType::new(vec![
            NestedField::required(134, "content", int()).into(),
            NestedField::required(100, "file_path", string()).into(),
            NestedField::required(101, "file_format", string()).into(),
            NestedField::required(
                102,
                "partition",
                Type::Struct(StructType::new(vec![
                    NestedField::optional(1000, "i_identity", int()).into(),
                    NestedField::optional(1001, "s_truncate[2]", string()).into(),
                ])),
            )
            .into(),
            NestedField::required(103, "record_count", long()).into(),
            NestedField::required(104, "file_size_in_bytes", long()).into(),
            NestedField::optional(
                108,
                "column_sizes",
                int_map_type(117, 118, PrimitiveType::Long),
            )
            .into(),
            NestedField::optional(
                109,
                "value_counts",
                int_map_type(119, 120, PrimitiveType::Long),
            )
            .into(),
            NestedField::optional(
                110,
                "null_value_counts",
                int_map_type(121, 122, PrimitiveType::Long),
            )
            .into(),
            NestedField::optional(
                137,
                "nan_value_counts",
                int_map_type(138, 139, PrimitiveType::Long),
            )
            .into(),
            NestedField::optional(
                125,
                "lower_bounds",
                int_map_type(126, 127, PrimitiveType::Binary),
            )
            .into(),
            NestedField::optional(
                128,
                "upper_bounds",
                int_map_type(129, 130, PrimitiveType::Binary),
            )
            .into(),
            NestedField::optional(131, "key_metadata", Type::Primitive(PrimitiveType::Binary))
                .into(),
            NestedField::optional(132, "split_offsets", list_type(133, PrimitiveType::Long)).into(),
            NestedField::optional(135, "equality_ids", list_type(136, PrimitiveType::Int)).into(),
            NestedField::optional(140, "sort_order_id", int()).into(),
            NestedField::optional(142, "first_row_id", long()).into(),
            NestedField::optional(143, "referenced_data_file", string()).into(),
            NestedField::optional(144, "content_offset", long()).into(),
            NestedField::optional(145, "content_size_in_bytes", long()).into(),
        ]);
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(0, "status", int()).into(),
                NestedField::optional(1, "snapshot_id", long()).into(),
                NestedField::optional(3, "sequence_number", long()).into(),
                NestedField::optional(4, "file_sequence_number", long()).into(),
                NestedField::required(2, "data_file", Type::Struct(data_file)).into(),
            ])
            .build()
            .unwrap();
        assert_eq!(
            arrow_schema_to_schema(&batch.schema()).unwrap().as_struct(),
            expected.as_struct()
        );
    }

    #[test]
    fn test_entry_and_data_file_columns() {
        let added = DataFile {
            column_sizes: HashMap::from([(1, 11)]),
            value_counts: HashMap::from([(1, 10)]),
            null_value_counts: HashMap::from([(1, 1)]),
            nan_value_counts: HashMap::from([(4, 0)]),
            lower_bounds: HashMap::from([(2, Datum::int(-1))]),
            upper_bounds: HashMap::from([(2, Datum::int(7))]),
            key_metadata: Some(vec![1, 2]),
            split_offsets: Some(vec![4, 100]),
            sort_order_id: Some(0),
            first_row_id: Some(1000),
            ..data_file(DataContentType::Data, "s3://b/data/0.parquet")
                .record_count(10)
                .file_size_in_bytes(100)
                .build()
                .unwrap()
        };
        let position_deletes = DataFile {
            file_format: DataFileFormat::Puffin,
            referenced_data_file: Some("s3://b/data/0.parquet".to_string()),
            content_offset: Some(4),
            content_size_in_bytes: Some(40),
            ..data_file(DataContentType::PositionDeletes, "s3://b/data/1.puffin")
                .build()
                .unwrap()
        };
        let equality_deletes = DataFile {
            equality_ids: Some(vec![1, 3]),
            split_offsets: Some(vec![]),
            ..data_file(DataContentType::EqualityDeletes, "s3://b/data/2.parquet")
                .build()
                .unwrap()
        };
        let entries = vec![
            ManifestEntry {
                snapshot_id: Some(1),
                sequence_number: Some(2),
                file_sequence_number: Some(3),
                ..entry(ManifestStatus::Added, added)
            },
            entry(ManifestStatus::Existing, position_deletes),
            ManifestEntry {
                snapshot_id: Some(5),
                ..entry(ManifestStatus::Deleted, equality_deletes)
            },
        ];
        let batch = manifest_to_record_batch(&manifest(partition_spec(&[]), entries)).unwrap();
        assert_eq!(batch.num_rows(), 3);

        let column = |name: &str| batch.column_by_name(name).unwrap().clone();
        assert_eq!(
            column("status").as_ref(),
            &Int32Array::from(vec![1, 0, 2]) as &dyn Array
        );
        assert_eq!(
            column("snapshot_id").as_ref(),
            &Int64Array::from(vec![Some(1), None, Some(5)]) as &dyn Array
        );
        assert_eq!(
            column("sequence_number").as_ref(),
            &Int64Array::from(vec![Some(2), None, None]) as &dyn Array
        );
        assert_eq!(
            column("file_sequence_number").as_ref(),
            &Int64Array::from(vec![Some(3), None, None]) as &dyn Array
        );

        let column = |name: &str| data_file_column(&batch, name);
        assert_eq!(
            column("content").as_ref(),
            &Int32Array::from(vec![0, 1, 2]) as &dyn Array
        );
        assert_eq!(
            column("file_path").as_ref(),
            &StringArray::from(vec![
                "s3://b/data/0.parquet",
                "s3://b/data/1.puffin",
                "s3://b/data/2.parquet"
            ]) as &dyn Array
        );
        // Writers, including `ManifestWriter`, store the format name in upper case.
        assert_eq!(
            column("file_format").as_ref(),
            &StringArray::from(vec!["PARQUET", "PUFFIN", "PARQUET"]) as &dyn Array
        );
        assert_eq!(
            column("record_count").as_ref(),
            &Int64Array::from(vec![10, 1, 1]) as &dyn Array
        );
        assert_eq!(
            column("file_size_in_bytes").as_ref(),
            &Int64Array::from(vec![100, 10, 10]) as &dyn Array
        );
        for (name, expected) in [
            ("column_sizes", (1, 11)),
            ("value_counts", (1, 10)),
            ("null_value_counts", (1, 1)),
            ("nan_value_counts", (4, 0)),
        ] {
            let map = column(name);
            assert_eq!(map.null_count(), 0, "{name}");
            assert_eq!(long_map(&map, 0), vec![expected], "{name}");
            assert_eq!(long_map(&map, 1), vec![], "{name}");
            assert_eq!(long_map(&map, 2), vec![], "{name}");
        }
        assert_eq!(binary_map(&column("lower_bounds"), 0), vec![(
            2,
            (-1i32).to_le_bytes().to_vec()
        )]);
        assert_eq!(binary_map(&column("upper_bounds"), 0), vec![(
            2,
            7i32.to_le_bytes().to_vec()
        )]);
        assert_eq!(binary_map(&column("lower_bounds"), 1), vec![]);
        assert_eq!(
            column("key_metadata").as_ref(),
            &LargeBinaryArray::from(vec![Some(&[1u8, 2][..]), None, None]) as &dyn Array
        );
        let split_offsets = column("split_offsets");
        let split_offsets = split_offsets.as_list::<i32>();
        assert!(split_offsets.is_valid(0) && split_offsets.is_null(1) && split_offsets.is_valid(2));
        assert_eq!(
            split_offsets.value(0).as_ref(),
            &Int64Array::from(vec![4, 100]) as &dyn Array
        );
        assert_eq!(split_offsets.value(2).len(), 0);
        let equality_ids = column("equality_ids");
        let equality_ids = equality_ids.as_list::<i32>();
        assert!(equality_ids.is_null(0) && equality_ids.is_null(1) && equality_ids.is_valid(2));
        assert_eq!(
            equality_ids.value(2).as_ref(),
            &Int32Array::from(vec![1, 3]) as &dyn Array
        );
        assert_eq!(
            column("sort_order_id").as_ref(),
            &Int32Array::from(vec![Some(0), None, None]) as &dyn Array
        );
        assert_eq!(
            column("first_row_id").as_ref(),
            &Int64Array::from(vec![Some(1000), None, None]) as &dyn Array
        );
        assert_eq!(
            column("referenced_data_file").as_ref(),
            &StringArray::from(vec![None, Some("s3://b/data/0.parquet"), None]) as &dyn Array
        );
        assert_eq!(
            column("content_offset").as_ref(),
            &Int64Array::from(vec![None, Some(4), None]) as &dyn Array
        );
        assert_eq!(
            column("content_size_in_bytes").as_ref(),
            &Int64Array::from(vec![None, Some(40), None]) as &dyn Array
        );
    }

    #[test]
    fn test_metric_maps_with_several_columns() {
        let uuid = Uuid::parse_str(UUID).unwrap();
        let file = DataFile {
            value_counts: HashMap::from([(1, 5), (3, 6), (13, 7)]),
            lower_bounds: HashMap::from([
                (1, Datum::bool(false)),
                (3, Datum::long(-2)),
                (13, Datum::string("abc")),
                (14, Datum::uuid(uuid)),
            ]),
            ..data_file(DataContentType::Data, "s3://b/data/0.parquet")
                .build()
                .unwrap()
        };
        let batch = manifest_to_record_batch(&manifest(partition_spec(&[]), vec![entry(
            ManifestStatus::Added,
            file,
        )]))
        .unwrap();

        assert_eq!(
            long_map(&data_file_column(&batch, "value_counts"), 0),
            vec![(1, 5), (3, 6), (13, 7)]
        );
        assert_eq!(
            binary_map(&data_file_column(&batch, "lower_bounds"), 0),
            vec![
                (1, vec![0]),
                (3, (-2i64).to_le_bytes().to_vec()),
                (13, b"abc".to_vec()),
                (14, uuid.as_bytes().to_vec()),
            ]
        );
    }

    #[test]
    fn test_partition_values() {
        let identity_columns = [
            "b", "i", "l", "f", "d", "dec", "date", "time", "ts", "tstz", "ts_ns", "tstz_ns", "s",
            "u", "fx", "bin",
        ];
        let mut fields: Vec<_> = identity_columns
            .iter()
            .map(|column| (*column, Transform::Identity))
            .collect();
        fields.extend([
            ("i", Transform::Bucket(4)),
            ("ts", Transform::Day),
            ("s", Transform::Truncate(2)),
            ("l", Transform::Void),
        ]);
        let spec = partition_spec(&fields);

        let uuid = Uuid::parse_str(UUID).unwrap();
        let values = Struct::from_iter([
            Some(Literal::bool(true)),
            Some(Literal::int(-7)),
            Some(Literal::long(1i64 << 40)),
            Some(Literal::float(1.5f32)),
            Some(Literal::double(-2.25)),
            Some(Literal::decimal(-12_345)),
            Some(Literal::date(19_000)),
            Some(Literal::time(3_600_000_000)),
            Some(Literal::timestamp(1_700_000_000_000_000)),
            Some(Literal::timestamptz(1_700_000_000_000_001)),
            Some(Literal::timestamp_nano(1_700_000_000_000_000_002)),
            Some(Literal::timestamptz_nano(1_700_000_000_000_000_003)),
            Some(Literal::string("abc")),
            Some(Literal::uuid(uuid)),
            Some(Literal::fixed([1u8, 2, 3])),
            Some(Literal::binary([9u8])),
            Some(Literal::int(3)),
            Some(Literal::date(19_675)),
            Some(Literal::string("ab")),
            None,
        ]);
        let nulls = Struct::from_iter(fields.iter().map(|_| None));
        let entries = [values, nulls]
            .into_iter()
            .map(|partition| {
                entry(ManifestStatus::Added, DataFile {
                    partition,
                    ..data_file(DataContentType::Data, "s3://b/data/0.parquet")
                        .build()
                        .unwrap()
                })
            })
            .collect();
        let batch = manifest_to_record_batch(&manifest(spec, entries)).unwrap();

        let partition = data_file_column(&batch, "partition");
        let partition = partition.as_struct();
        assert_eq!(partition.null_count(), 0);
        let expected: Vec<ArrayRef> = vec![
            Arc::new(BooleanArray::from(vec![Some(true), None])),
            Arc::new(Int32Array::from(vec![Some(-7), None])),
            Arc::new(Int64Array::from(vec![Some(1i64 << 40), None])),
            Arc::new(Float32Array::from(vec![Some(1.5), None])),
            Arc::new(Float64Array::from(vec![Some(-2.25), None])),
            Arc::new(
                Decimal128Array::from(vec![Some(-12_345), None])
                    .with_precision_and_scale(38, 10)
                    .unwrap(),
            ),
            Arc::new(Date32Array::from(vec![Some(19_000), None])),
            Arc::new(Time64MicrosecondArray::from(vec![
                Some(3_600_000_000),
                None,
            ])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(1_700_000_000_000_000),
                None,
            ])),
            Arc::new(
                TimestampMicrosecondArray::from(vec![Some(1_700_000_000_000_001), None])
                    .with_timezone("+00:00"),
            ),
            Arc::new(TimestampNanosecondArray::from(vec![
                Some(1_700_000_000_000_000_002),
                None,
            ])),
            Arc::new(
                TimestampNanosecondArray::from(vec![Some(1_700_000_000_000_000_003), None])
                    .with_timezone("+00:00"),
            ),
            Arc::new(StringArray::from(vec![Some("abc"), None])),
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    [Some(uuid.as_bytes().to_vec()), None].into_iter(),
                    16,
                )
                .unwrap(),
            ),
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    [Some(vec![1u8, 2, 3]), None].into_iter(),
                    3,
                )
                .unwrap(),
            ),
            Arc::new(LargeBinaryArray::from(vec![Some(&[9u8][..]), None])),
            Arc::new(Int32Array::from(vec![Some(3), None])),
            Arc::new(Date32Array::from(vec![Some(19_675), None])),
            Arc::new(StringArray::from(vec![Some("ab"), None])),
            Arc::new(Int64Array::from(vec![None, None])),
        ];
        assert_eq!(partition.num_columns(), expected.len());
        for (index, expected) in expected.iter().enumerate() {
            assert_eq!(
                partition.column(index).as_ref(),
                expected.as_ref(),
                "partition field {}",
                partition.fields()[index].name()
            );
        }
    }

    #[test]
    fn test_unpartitioned_partition_column() {
        let entries = (0..3)
            .map(|i| {
                entry(
                    ManifestStatus::Added,
                    data_file(DataContentType::Data, &format!("s3://b/data/{i}.parquet"))
                        .build()
                        .unwrap(),
                )
            })
            .collect();
        let batch = manifest_to_record_batch(&manifest(partition_spec(&[]), entries)).unwrap();
        let partition = data_file_column(&batch, "partition");
        let partition: &StructArray = partition.as_struct();
        assert_eq!(partition.num_columns(), 0);
        assert_eq!(partition.len(), 3);
        assert_eq!(partition.null_count(), 0);
    }

    #[test]
    fn test_unknown_partition_field() {
        let spec = || partition_spec(&[("unk", Transform::Void)]);
        let entries = vec![entry(ManifestStatus::Added, DataFile {
            partition: Struct::from_iter([None]),
            ..data_file(DataContentType::Data, "s3://b/data/0.parquet")
                .build()
                .unwrap()
        })];
        let batch = manifest_to_record_batch(&manifest(spec(), entries)).unwrap();
        let partition = data_file_column(&batch, "partition");
        assert_eq!(
            partition.as_struct().column(0).as_ref(),
            &NullArray::new(1) as &dyn Array
        );

        let entries = vec![entry(ManifestStatus::Added, DataFile {
            partition: Struct::from_iter([Some(Literal::int(1))]),
            ..data_file(DataContentType::Data, "s3://b/data/0.parquet")
                .build()
                .unwrap()
        })];
        let err = manifest_to_record_batch(&manifest(spec(), entries)).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn test_partition_value_of_wrong_type_is_an_error() {
        let file = DataFile {
            partition: Struct::from_iter([Some(Literal::string("not an int"))]),
            ..data_file(DataContentType::Data, "s3://b/data/0.parquet")
                .build()
                .unwrap()
        };
        let manifest = manifest(partition_spec(&[("i", Transform::Identity)]), vec![entry(
            ManifestStatus::Added,
            file,
        )]);
        let err = manifest_to_record_batch(&manifest).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn test_fixed_partition_value_of_wrong_width_is_an_error() {
        let file = DataFile {
            partition: Struct::from_iter([Some(Literal::fixed([1u8, 2]))]),
            ..data_file(DataContentType::Data, "s3://b/data/0.parquet")
                .build()
                .unwrap()
        };
        let manifest = manifest(partition_spec(&[("fx", Transform::Identity)]), vec![entry(
            ManifestStatus::Added,
            file,
        )]);
        let err = manifest_to_record_batch(&manifest).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn test_partition_value_with_wrong_arity_is_an_error() {
        let file = DataFile {
            partition: Struct::from_iter([Some(Literal::int(1)), Some(Literal::int(2))]),
            ..data_file(DataContentType::Data, "s3://b/data/0.parquet")
                .build()
                .unwrap()
        };
        let manifest = manifest(partition_spec(&[("i", Transform::Identity)]), vec![entry(
            ManifestStatus::Added,
            file,
        )]);
        let err = manifest_to_record_batch(&manifest).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn test_every_format_version_has_the_same_schema() {
        let schemas: Vec<_> = [FormatVersion::V1, FormatVersion::V2, FormatVersion::V3]
            .into_iter()
            .map(|format_version| {
                let manifest = Manifest::new(
                    ManifestMetadata {
                        format_version,
                        ..manifest(partition_spec(&[("i", Transform::Identity)]), vec![])
                            .metadata()
                            .clone()
                    },
                    vec![],
                );
                manifest_to_record_batch(&manifest).unwrap().schema()
            })
            .collect();
        assert_eq!(schemas[0], schemas[1]);
        assert_eq!(schemas[1], schemas[2]);
    }

    #[test]
    fn test_count_beyond_long_range_is_an_error() {
        let file = DataFile {
            column_sizes: HashMap::from([(1, u64::MAX)]),
            ..data_file(DataContentType::Data, "s3://b/data/0.parquet")
                .build()
                .unwrap()
        };
        let manifest = manifest(partition_spec(&[]), vec![entry(
            ManifestStatus::Added,
            file,
        )]);
        let err = manifest_to_record_batch(&manifest).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    /// `(key, value)` pairs of the map field `name` of an Avro `data_file`
    /// record, sorted by key.
    fn avro_map<T: Ord>(
        data_file: &[(String, AvroValue)],
        name: &str,
        value: impl Fn(&AvroValue) -> T,
    ) -> Vec<(i32, T)> {
        let (_, AvroValue::Union(_, map)) =
            data_file.iter().find(|(field, _)| field == name).unwrap()
        else {
            panic!("{name} is not a union");
        };
        let AvroValue::Array(pairs) = map.as_ref() else {
            panic!("{name} is not an array");
        };
        let mut pairs: Vec<_> = pairs
            .iter()
            .map(|pair| {
                let AvroValue::Record(pair) = pair else {
                    panic!("{name} entry is not a record");
                };
                let AvroValue::Int(key) = pair[0].1 else {
                    panic!("{name} key is not an int");
                };
                (key, value(&pair[1].1))
            })
            .collect();
        pairs.sort_unstable();
        pairs
    }

    #[test]
    fn test_manifest_written_by_pyiceberg() {
        let bs = std::fs::read(format!(
            "{}/testdata/manifests/pyiceberg-v2-data.avro",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap();
        let batch = manifest_to_record_batch(&Manifest::parse_avro(&bs).unwrap()).unwrap();

        let records: Vec<_> = AvroReader::new(bs.as_slice())
            .unwrap()
            .map(|record| match record.unwrap() {
                AvroValue::Record(fields) => fields,
                _ => panic!("manifest entry is not a record"),
            })
            .collect();
        assert_eq!(batch.num_rows(), records.len());
        for (row, record) in records.iter().enumerate() {
            let Some((_, AvroValue::Record(data_file))) =
                record.iter().find(|(name, _)| name == "data_file")
            else {
                panic!("data_file is not a record");
            };
            let long = |value: &AvroValue| match value {
                AvroValue::Long(v) => *v,
                _ => panic!("map value is not a long"),
            };
            let bytes = |value: &AvroValue| match value {
                AvroValue::Bytes(v) => v.clone(),
                _ => panic!("map value is not bytes"),
            };
            for name in ["column_sizes", "value_counts", "null_value_counts"] {
                assert_eq!(
                    long_map(&data_file_column(&batch, name), row),
                    avro_map(data_file, name, long),
                    "{name}"
                );
            }
            for name in ["lower_bounds", "upper_bounds"] {
                let expected = avro_map(data_file, name, bytes);
                assert!(!expected.is_empty(), "{name}");
                assert_eq!(
                    binary_map(&data_file_column(&batch, name), row),
                    expected,
                    "{name}"
                );
            }
        }
    }
}
