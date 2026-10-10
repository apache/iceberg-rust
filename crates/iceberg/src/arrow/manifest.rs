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

//! Reading manifest entries into Arrow.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_arith::boolean::{and, is_null};
use arrow_array::cast::AsArray;
use arrow_array::types::{TimestampMicrosecondType, TimestampNanosecondType};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, ListArray, MapArray, RecordBatch,
    StructArray, new_null_array,
};
use arrow_avro::reader::ReaderBuilder;
use arrow_avro::schema::AvroSchema;
use arrow_cast::{CastOptions, cast_with_options};
use arrow_ord::cmp::eq;
use arrow_schema::{ArrowError, DataType, Field, Fields, TimeUnit};
use arrow_select::zip::zip;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use serde_json::Value as JsonValue;

use crate::arrow::schema_to_arrow_schema;
use crate::error::invalid_data;
use crate::spec::{
    ManifestContentType, ManifestFile, ManifestMetadata, ManifestStatus, Schema,
    manifest_entry_fields,
};
use crate::{Error, Result};

/// Field ID of `data_file`.
const DATA_FILE_FIELD_ID: i64 = 2;
/// Field ID of `data_file.partition`.
const PARTITION_FIELD_ID: i64 = 102;
/// Field ID of `data_file.content`.
const CONTENT_FIELD_ID: i32 = 134;
/// Field ID of `sequence_number`.
const SEQUENCE_NUMBER_FIELD_ID: i32 = 3;
/// Field ID of `file_sequence_number`.
const FILE_SEQUENCE_NUMBER_FIELD_ID: i32 = 4;

/// Reads the entries of a manifest file into a [`RecordBatch`] with one row
/// per entry.
///
/// `bytes` is the manifest file and `manifest_file` is its entry in the
/// manifest list. The schema is the spec's `manifest_entry` struct for the
/// latest format version, with field IDs in the Arrow field metadata, so
/// manifests of every format version read into the same schema apart from
/// `data_file.partition`, which follows the manifest's partition spec.
///
/// Fields are matched to the manifest's Avro schema by field ID. A v1
/// manifest reads with the spec's v2+ defaults, and null snapshot IDs,
/// sequence numbers, and first row IDs are inherited from `manifest_file`.
/// Lower and upper bounds keep the binary single-value serialization they were
/// written with.
///
/// # Example
///
/// ```
/// use iceberg::arrow::read_manifest_entries;
/// use iceberg::spec::{ManifestContentType, ManifestFile};
///
/// let bytes = std::fs::read(concat!(
///     env!("CARGO_MANIFEST_DIR"),
///     "/testdata/manifests/pyiceberg-v2-data.avro"
/// ))?;
/// let manifest_file = ManifestFile {
///     manifest_path: "s3://bucket/metadata/manifest.avro".to_string(),
///     manifest_length: bytes.len() as i64,
///     partition_spec_id: 0,
///     content: ManifestContentType::Data,
///     sequence_number: 1,
///     min_sequence_number: 1,
///     added_snapshot_id: 1,
///     added_files_count: None,
///     existing_files_count: None,
///     deleted_files_count: None,
///     added_rows_count: None,
///     existing_rows_count: None,
///     deleted_rows_count: None,
///     partitions: None,
///     key_metadata: None,
///     first_row_id: None,
/// };
///
/// let batch = read_manifest_entries(&bytes, &manifest_file)?;
/// assert_eq!(batch.num_rows(), 2);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn read_manifest_entries(bytes: &[u8], manifest_file: &ManifestFile) -> Result<RecordBatch> {
    // usize::MAX decodes the whole manifest into one batch. First row ID
    // inheritance is a running sum over every entry in the file, and an empty
    // manifest still returns its schema. arrow-avro sizes its buffers
    // independently of the batch size, so this doesn't preallocate.
    let builder = || ReaderBuilder::new().with_batch_size(usize::MAX);
    let mut reader = builder().build(bytes).map_err(invalid_manifest)?;
    let header = reader.avro_header();
    let metadata: HashMap<String, Vec<u8>> = header
        .metadata()
        .map(|(key, value)| (String::from_utf8_lossy(key).into_owned(), value.to_vec()))
        .collect();
    let metadata = ManifestMetadata::parse(&metadata)?;
    let mut avro_schema: JsonValue = serde_json::from_slice(
        header
            .get("avro.schema")
            .ok_or_else(|| invalid_data!("manifest has no avro.schema"))?,
    )?;
    let partition_type = metadata
        .partition_spec()
        .partition_type(metadata.schema())?;
    let schema = Arc::new(schema_to_arrow_schema(
        &Schema::builder()
            .with_fields(manifest_entry_fields(&partition_type))
            .build()?,
    )?);

    // TODO(#3257): arrow-avro 59 can't decode a record with no fields, which
    // is the partition of every unpartitioned manifest, so project it out and
    // build it in `missing_field`. arrow-avro 60 fixes this
    // (apache/arrow-rs#10771).
    if remove_empty_partition(&mut avro_schema) {
        reader = builder()
            .with_reader_schema(AvroSchema::new(avro_schema.to_string()))
            .build(bytes)
            .map_err(invalid_manifest)?;
    }

    let Some(batch) = reader.next() else {
        return Ok(RecordBatch::new_empty(schema));
    };
    let entries = project_struct(
        &StructArray::from(batch.map_err(invalid_manifest)?),
        &avro_schema,
        schema.fields(),
    )?;
    Ok(RecordBatch::from(inherit(entries, manifest_file)?))
}

/// Removes `data_file.partition` from a manifest's Avro schema if it has no
/// fields, and returns whether it did.
fn remove_empty_partition(avro_schema: &mut JsonValue) -> bool {
    let has_field_id = |field: &JsonValue, id: i64| field["field-id"].as_i64() == Some(id);
    let Some(data_file_fields) = avro_schema["fields"]
        .as_array_mut()
        .and_then(|fields| {
            fields
                .iter_mut()
                .find(|field| has_field_id(field, DATA_FILE_FIELD_ID))
        })
        .and_then(|data_file| data_file["type"]["fields"].as_array_mut())
    else {
        return false;
    };
    let len = data_file_fields.len();
    data_file_fields.retain(|field| {
        !(has_field_id(field, PARTITION_FIELD_ID)
            && field["type"]["fields"]
                .as_array()
                .is_some_and(Vec::is_empty))
    });
    data_file_fields.len() != len
}

fn invalid_manifest(err: ArrowError) -> Error {
    invalid_data!("failed to read manifest").with_source(err)
}

/// Returns the field ID of a field in the schema built from
/// `manifest_entry_fields`, which gives every field one.
fn field_id(field: &Field) -> i32 {
    field.metadata()[PARQUET_FIELD_ID_META_KEY].parse().unwrap()
}

/// Returns the type of a nullable Avro field, `["null", T]`, as `T`.
fn non_null_type(avro_type: &JsonValue) -> &JsonValue {
    match avro_type.as_array().map(Vec::as_slice) {
        Some([JsonValue::String(null), branch] | [branch, JsonValue::String(null)])
            if null == "null" =>
        {
            branch
        }
        _ => avro_type,
    }
}

/// Builds each of `fields` from the column of `array` that the Avro record
/// `record` gives the same field ID.
fn project_struct(array: &StructArray, record: &JsonValue, fields: &Fields) -> Result<StructArray> {
    let avro_fields = record["fields"]
        .as_array()
        .ok_or_else(|| invalid_data!("manifest Avro type {record} is not a record"))?;
    let columns = fields
        .iter()
        .map(|field| {
            let id = field_id(field);
            match avro_fields
                .iter()
                .position(|avro_field| avro_field["field-id"].as_i64() == Some(id.into()))
            {
                Some(index) => project(
                    array.column(index),
                    non_null_type(&avro_fields[index]["type"]),
                    field,
                ),
                None => missing_field(field, array.len()),
            }
        })
        .collect::<Result<_>>()?;
    StructArray::try_new_with_length(fields.clone(), columns, array.nulls().cloned(), array.len())
        .map_err(invalid_manifest)
}

/// Builds the column for a field that the manifest doesn't have.
fn missing_field(field: &Field, len: usize) -> Result<ArrayRef> {
    // v1 manifests have no content or sequence number columns, which v2+
    // readers read as 0.
    match field_id(field) {
        CONTENT_FIELD_ID => Ok(Arc::new(Int32Array::from_value(0, len))),
        SEQUENCE_NUMBER_FIELD_ID | FILE_SEQUENCE_NUMBER_FIELD_ID => {
            Ok(Arc::new(Int64Array::from_value(0, len)))
        }
        _ if field.is_nullable() => Ok(new_null_array(field.data_type(), len)),
        _ if matches!(field.data_type(), DataType::Struct(fields) if fields.is_empty()) => {
            Ok(Arc::new(StructArray::new_empty_fields(len, None)))
        }
        id => Err(invalid_data!(
            "manifest has no required field {} with field ID {id}",
            field.name()
        )),
    }
}

/// Converts the decoded `array` of Avro type `avro_type` to `field`, the
/// spec's Arrow field for it.
fn project(array: &ArrayRef, avro_type: &JsonValue, field: &Field) -> Result<ArrayRef> {
    let type_mismatch = || {
        invalid_data!(
            "manifest field {} has Avro type {avro_type}, which doesn't match {}",
            field.name(),
            field.data_type()
        )
    };
    Ok(match field.data_type() {
        DataType::Struct(fields) => Arc::new(project_struct(
            array.as_struct_opt().ok_or_else(type_mismatch)?,
            avro_type,
            fields,
        )?),
        DataType::List(element) => {
            let list = array.as_list_opt::<i32>().ok_or_else(type_mismatch)?;
            let values = project(list.values(), non_null_type(&avro_type["items"]), element)?;
            Arc::new(
                ListArray::try_new(
                    element.clone(),
                    list.offsets().clone(),
                    values,
                    list.nulls().cloned(),
                )
                .map_err(invalid_manifest)?,
            )
        }
        // Avro stores maps with int keys as arrays of key-value records.
        DataType::Map(entries, _) => {
            let list = array.as_list_opt::<i32>().ok_or_else(type_mismatch)?;
            let DataType::Struct(key_value) = entries.data_type() else {
                unreachable!()
            };
            let entries_array = project_struct(
                list.values().as_struct_opt().ok_or_else(type_mismatch)?,
                &avro_type["items"],
                key_value,
            )?;
            Arc::new(
                MapArray::try_new(
                    entries.clone(),
                    list.offsets().clone(),
                    entries_array,
                    list.nulls().cloned(),
                    false,
                )
                .map_err(invalid_manifest)?,
            )
        }
        // arrow-avro gives every timestamp a UTC time zone, so only the type
        // changes.
        DataType::Timestamp(TimeUnit::Microsecond, time_zone) => Arc::new(
            array
                .as_primitive_opt::<TimestampMicrosecondType>()
                .ok_or_else(type_mismatch)?
                .clone()
                .with_timezone_opt(time_zone.clone()),
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, time_zone) => Arc::new(
            array
                .as_primitive_opt::<TimestampNanosecondType>()
                .ok_or_else(type_mismatch)?
                .clone()
                .with_timezone_opt(time_zone.clone()),
        ),
        data_type if array.data_type() == data_type => array.clone(),
        // Without `safe: false`, a value that doesn't fit the spec's type would
        // become null instead of an error.
        data_type => cast_with_options(array, data_type, &CastOptions {
            safe: false,
            ..Default::default()
        })
        .map_err(invalid_manifest)?,
    })
}

/// Fills in the values that the spec says entries inherit from the manifest
/// list.
fn inherit(entries: StructArray, manifest_file: &ManifestFile) -> Result<StructArray> {
    let (fields, mut columns, nulls) = entries.into_parts();
    let index = |name: &str| fields.find(name).unwrap().0;
    let (status, snapshot_id, sequence_number, file_sequence_number, data_file) = (
        index("status"),
        index("snapshot_id"),
        index("sequence_number"),
        index("file_sequence_number"),
        index("data_file"),
    );

    columns[snapshot_id] =
        fill_nulls(&columns[snapshot_id], None, manifest_file.added_snapshot_id)?;
    if columns[sequence_number].null_count() > 0 || columns[file_sequence_number].null_count() > 0 {
        let added = eq(
            &columns[status],
            &Int32Array::new_scalar(ManifestStatus::Added as i32),
        )?;
        for column in [sequence_number, file_sequence_number] {
            columns[column] = fill_nulls(
                &columns[column],
                Some(&added),
                manifest_file.sequence_number,
            )?;
        }
    }
    if manifest_file.content == ManifestContentType::Data {
        let (data_file_fields, mut data_file_columns, data_file_nulls) =
            columns[data_file].as_struct().clone().into_parts();
        let index = |name: &str| data_file_fields.find(name).unwrap().0;
        let first_row_id = index("first_row_id");
        data_file_columns[first_row_id] = inherit_first_row_ids(
            data_file_columns[first_row_id].as_primitive(),
            columns[status].as_primitive(),
            data_file_columns[index("record_count")].as_primitive(),
            manifest_file,
        )?;
        columns[data_file] = Arc::new(StructArray::new(
            data_file_fields,
            data_file_columns,
            data_file_nulls,
        ));
    }
    Ok(StructArray::new(fields, columns, nulls))
}

/// Replaces the nulls of `array` with `value`, only in the rows that `rows`
/// selects when given.
fn fill_nulls(array: &ArrayRef, rows: Option<&BooleanArray>, value: i64) -> Result<ArrayRef> {
    if array.null_count() == 0 {
        return Ok(array.clone());
    }
    let mask = match rows {
        Some(rows) => and(&is_null(array)?, rows)?,
        None => is_null(array)?,
    };
    Ok(zip(&mask, &Int64Array::new_scalar(value), array)?)
}

/// Assigns first row IDs to the data files that lack one, following first row
/// ID inheritance. As in Java's `ManifestReader`, deleted entries take no row
/// IDs, and a manifest without a first row ID clears them all.
fn inherit_first_row_ids(
    first_row_ids: &Int64Array,
    status: &Int32Array,
    record_counts: &Int64Array,
    manifest_file: &ManifestFile,
) -> Result<ArrayRef> {
    let Some(manifest_first_row_id) = manifest_file.first_row_id else {
        return Ok(new_null_array(&DataType::Int64, first_row_ids.len()));
    };
    if first_row_ids.null_count() == 0 {
        return Ok(Arc::new(first_row_ids.clone()));
    }
    let mut next_row_id = i64::try_from(manifest_first_row_id)?;
    let first_row_ids: Int64Array = first_row_ids
        .iter()
        .zip(status.values())
        .zip(record_counts.values())
        .map(
            |((first_row_id, &status), &record_count)| match first_row_id {
                None if status != ManifestStatus::Deleted as i32 => {
                    let assigned = next_row_id;
                    next_row_id = next_row_id.checked_add(record_count).ok_or_else(|| {
                        invalid_data!(
                            "row ID overflow assigning first row IDs in {}",
                            manifest_file.manifest_path
                        )
                    })?;
                    Ok(Some(assigned))
                }
                first_row_id => Ok(first_row_id),
            },
        )
        .collect::<Result<_>>()?;
    Ok(Arc::new(first_row_ids))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use apache_avro::types::Value as AvroValue;
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Int32Type, Int64Type};
    use arrow_array::{
        Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
        Float32Array, Float64Array, Int32Array, Int64Array, LargeBinaryArray, StringArray,
        Time64MicrosecondArray, TimestampMicrosecondArray, TimestampNanosecondArray,
    };

    use super::*;
    use crate::ErrorKind;
    use crate::arrow::arrow_schema_to_schema;
    use crate::io::FileIO;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, ListType, Literal,
        Manifest, ManifestEntry, ManifestStatus, ManifestWriter, ManifestWriterBuilder, MapType,
        NestedField, PartitionSpec, PrimitiveLiteral, PrimitiveType, Schema, Struct, StructType,
        Transform, Type,
    };

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
            (14, "fx", PrimitiveType::Fixed(3)),
            (15, "bin", PrimitiveType::Binary),
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

    /// Writes a manifest with `ManifestWriter` and returns its bytes and its
    /// manifest list entry.
    async fn write_manifest(
        spec: PartitionSpec,
        snapshot_id: Option<i64>,
        build: impl FnOnce(ManifestWriterBuilder) -> ManifestWriter,
        add: impl FnOnce(&mut ManifestWriter),
    ) -> (Vec<u8>, ManifestFile) {
        let io = FileIO::new_with_memory();
        let output = io.new_output("memory:///manifest.avro").unwrap();
        let mut writer = build(ManifestWriterBuilder::new(
            output,
            snapshot_id,
            Arc::new(table_schema()),
            spec,
        ));
        add(&mut writer);
        let manifest_file = writer.write_manifest_file().await.unwrap();
        let bytes = io
            .new_input("memory:///manifest.avro")
            .unwrap()
            .read()
            .await
            .unwrap()
            .to_vec();
        (bytes, manifest_file)
    }

    fn column(batch: &RecordBatch, name: &str) -> ArrayRef {
        batch.column_by_name(name).unwrap().clone()
    }

    fn data_file_column(batch: &RecordBatch, name: &str) -> ArrayRef {
        column(batch, "data_file")
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

    /// Rewrites a manifest with `edit_schema` applied to its Avro schema and
    /// `edit_record` applied to each record.
    fn rewrite_manifest(
        bytes: &[u8],
        edit_schema: impl Fn(&mut serde_json::Value),
        edit_record: impl Fn(&mut AvroValue),
    ) -> Vec<u8> {
        let reader = apache_avro::Reader::new(bytes).unwrap();
        let mut schema = serde_json::to_value(reader.writer_schema()).unwrap();
        edit_schema(&mut schema);
        let schema = apache_avro::Schema::parse(&schema).unwrap();
        let metadata = reader.user_metadata().clone();
        let mut writer = apache_avro::Writer::new(&schema, Vec::new()).unwrap();
        for (key, value) in metadata {
            writer.add_user_metadata(key, value).unwrap();
        }
        for record in reader {
            let mut record = record.unwrap();
            edit_record(&mut record);
            writer.append_value(record).unwrap();
        }
        writer.into_inner().unwrap()
    }

    /// Calls `edit` on every record field object in an Avro schema.
    fn for_each_avro_field(schema: &mut serde_json::Value, edit: &impl Fn(&mut serde_json::Value)) {
        match schema {
            serde_json::Value::Object(object) => {
                if let Some(serde_json::Value::Array(fields)) = object.get_mut("fields") {
                    for field in fields {
                        edit(field);
                        for_each_avro_field(field.get_mut("type").unwrap(), edit);
                    }
                }
                if let Some(items) = object.get_mut("items") {
                    for_each_avro_field(items, edit);
                }
            }
            serde_json::Value::Array(branches) => {
                for branch in branches {
                    for_each_avro_field(branch, edit);
                }
            }
            _ => {}
        }
    }

    /// Renames the record field `from` to `to` in every Avro record value.
    fn rename_avro_field(value: &mut AvroValue, from: &str, to: &str) {
        match value {
            AvroValue::Record(fields) => {
                for (name, field) in fields {
                    if name == from {
                        *name = to.to_string();
                    }
                    rename_avro_field(field, from, to);
                }
            }
            AvroValue::Union(_, value) => rename_avro_field(value, from, to),
            AvroValue::Array(items) => {
                for item in items {
                    rename_avro_field(item, from, to);
                }
            }
            _ => {}
        }
    }

    #[tokio::test]
    async fn test_schema_is_spec_manifest_entry() {
        let spec = partition_spec(&[("i", Transform::Identity), ("s", Transform::Truncate(2))]);
        let (bytes, manifest_file) =
            write_manifest(spec, Some(1), |b| b.build_v2_data(), |_| {}).await;
        let batch = read_manifest_entries(&bytes, &manifest_file).unwrap();
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

    #[tokio::test]
    async fn test_data_file_columns() {
        let added = DataFile {
            column_sizes: HashMap::from([(1, 11)]),
            value_counts: HashMap::from([(1, 10), (3, 9)]),
            null_value_counts: HashMap::from([(1, 1)]),
            nan_value_counts: HashMap::from([(4, 0)]),
            lower_bounds: HashMap::from([(2, Datum::int(-1)), (13, Datum::string("abc"))]),
            upper_bounds: HashMap::from([(2, Datum::int(7))]),
            key_metadata: Some(vec![1, 2]),
            split_offsets: Some(vec![4, 100]),
            sort_order_id: Some(0),
            ..data_file(DataContentType::Data, "s3://b/data/0.parquet")
                .record_count(10)
                .file_size_in_bytes(100)
                .build()
                .unwrap()
        };
        let existing = data_file(DataContentType::Data, "s3://b/data/1.parquet")
            .build()
            .unwrap();
        let (bytes, manifest_file) = write_manifest(
            partition_spec(&[]),
            Some(1),
            |b| b.build_v2_data(),
            |writer| {
                writer.add_file(added, 2).unwrap();
                writer.add_existing_file(existing, 5, 3, Some(3)).unwrap();
            },
        )
        .await;
        let batch = read_manifest_entries(&bytes, &manifest_file).unwrap();
        assert_eq!(batch.num_rows(), 2);

        assert_eq!(
            column(&batch, "status").as_ref(),
            &Int32Array::from(vec![1, 0]) as &dyn Array
        );
        assert_eq!(
            column(&batch, "snapshot_id").as_ref(),
            &Int64Array::from(vec![1, 5]) as &dyn Array
        );
        assert_eq!(
            column(&batch, "sequence_number").as_ref(),
            &Int64Array::from(vec![2, 3]) as &dyn Array
        );

        let column = |name: &str| data_file_column(&batch, name);
        assert_eq!(
            column("content").as_ref(),
            &Int32Array::from(vec![0, 0]) as &dyn Array
        );
        assert_eq!(
            column("file_path").as_ref(),
            &StringArray::from(vec!["s3://b/data/0.parquet", "s3://b/data/1.parquet"])
                as &dyn Array
        );
        assert_eq!(
            column("file_format").as_ref(),
            &StringArray::from(vec!["PARQUET", "PARQUET"]) as &dyn Array
        );
        assert_eq!(
            column("record_count").as_ref(),
            &Int64Array::from(vec![10, 1]) as &dyn Array
        );
        assert_eq!(
            column("file_size_in_bytes").as_ref(),
            &Int64Array::from(vec![100, 10]) as &dyn Array
        );
        assert_eq!(long_map(&column("column_sizes"), 0), vec![(1, 11)]);
        assert_eq!(long_map(&column("value_counts"), 0), vec![(1, 10), (3, 9)]);
        assert_eq!(long_map(&column("null_value_counts"), 0), vec![(1, 1)]);
        assert_eq!(long_map(&column("nan_value_counts"), 0), vec![(4, 0)]);
        assert_eq!(long_map(&column("value_counts"), 1), vec![]);
        assert_eq!(binary_map(&column("lower_bounds"), 0), vec![
            (2, (-1i32).to_le_bytes().to_vec()),
            (13, b"abc".to_vec()),
        ]);
        assert_eq!(binary_map(&column("upper_bounds"), 0), vec![(
            2,
            7i32.to_le_bytes().to_vec()
        )]);
        assert_eq!(
            column("key_metadata").as_ref(),
            &LargeBinaryArray::from(vec![Some(&[1u8, 2][..]), None]) as &dyn Array
        );
        let split_offsets = column("split_offsets");
        assert_eq!(
            split_offsets.as_list::<i32>().value(0).as_ref(),
            &Int64Array::from(vec![4, 100]) as &dyn Array
        );
        assert_eq!(
            column("sort_order_id").as_ref(),
            &Int32Array::from(vec![Some(0), None]) as &dyn Array
        );
    }

    #[tokio::test]
    async fn test_delete_file_columns() {
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
            ..data_file(DataContentType::EqualityDeletes, "s3://b/data/2.parquet")
                .build()
                .unwrap()
        };
        let (bytes, manifest_file) = write_manifest(
            partition_spec(&[]),
            Some(1),
            |b| b.build_v3_deletes(),
            |writer| {
                writer.add_file(position_deletes, 2).unwrap();
                writer.add_file(equality_deletes, 2).unwrap();
            },
        )
        .await;
        let batch = read_manifest_entries(&bytes, &manifest_file).unwrap();

        let column = |name: &str| data_file_column(&batch, name);
        assert_eq!(
            column("content").as_ref(),
            &Int32Array::from(vec![1, 2]) as &dyn Array
        );
        assert_eq!(
            column("file_format").as_ref(),
            &StringArray::from(vec!["PUFFIN", "PARQUET"]) as &dyn Array
        );
        assert_eq!(
            column("referenced_data_file").as_ref(),
            &StringArray::from(vec![Some("s3://b/data/0.parquet"), None]) as &dyn Array
        );
        assert_eq!(
            column("content_offset").as_ref(),
            &Int64Array::from(vec![Some(4), None]) as &dyn Array
        );
        assert_eq!(
            column("content_size_in_bytes").as_ref(),
            &Int64Array::from(vec![Some(40), None]) as &dyn Array
        );
        let equality_ids = column("equality_ids");
        let equality_ids = equality_ids.as_list::<i32>();
        assert!(equality_ids.is_null(0));
        assert_eq!(
            equality_ids.value(1).as_ref(),
            &Int32Array::from(vec![1, 3]) as &dyn Array
        );
    }

    #[tokio::test]
    async fn test_partition_values() {
        let identity_columns = [
            "b", "i", "l", "f", "d", "dec", "date", "time", "ts", "tstz", "ts_ns", "tstz_ns", "s",
            "fx", "bin",
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
            Some(Literal::fixed([1u8, 2, 3])),
            Some(Literal::binary([9u8])),
            Some(Literal::int(3)),
            Some(Literal::date(19_675)),
            Some(Literal::string("ab")),
            None,
        ]);
        let nulls = Struct::from_iter(fields.iter().map(|_| None));
        let (bytes, manifest_file) = write_manifest(
            spec,
            Some(1),
            |b| b.build_v2_data(),
            |writer| {
                for (i, partition) in [values, nulls].into_iter().enumerate() {
                    let file = DataFile {
                        partition,
                        ..data_file(DataContentType::Data, &format!("s3://b/data/{i}.parquet"))
                            .build()
                            .unwrap()
                    };
                    writer.add_file(file, 1).unwrap();
                }
            },
        )
        .await;
        let batch = read_manifest_entries(&bytes, &manifest_file).unwrap();

        let partition = data_file_column(&batch, "partition");
        let partition = partition.as_struct();
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

    #[tokio::test]
    async fn test_v1_manifest_reads_with_v2_defaults() {
        let file = data_file(DataContentType::Data, "s3://b/data/0.parquet")
            .build()
            .unwrap();
        let (bytes, mut manifest_file) = write_manifest(
            partition_spec(&[]),
            Some(1),
            |b| b.build_v1(),
            |writer| writer.add_file(file, -1).unwrap(),
        )
        .await;
        // The v1 default must win over inheritance, so give the manifest list
        // entry a sequence number that inheritance would use.
        manifest_file.sequence_number = 9;
        let batch = read_manifest_entries(&bytes, &manifest_file).unwrap();

        assert_eq!(
            data_file_column(&batch, "content").as_ref(),
            &Int32Array::from(vec![0]) as &dyn Array
        );
        for name in ["sequence_number", "file_sequence_number"] {
            assert_eq!(
                column(&batch, name).as_ref(),
                &Int64Array::from(vec![0]) as &dyn Array,
                "{name}"
            );
        }
        assert_eq!(
            column(&batch, "snapshot_id").as_ref(),
            &Int64Array::from(vec![1]) as &dyn Array
        );
    }

    #[tokio::test]
    async fn test_snapshot_id_and_sequence_number_inheritance() {
        let file = |i: usize| {
            data_file(DataContentType::Data, &format!("s3://b/data/{i}.parquet"))
                .build()
                .unwrap()
        };
        let (bytes, mut manifest_file) = write_manifest(
            partition_spec(&[]),
            None,
            |b| b.build_v2_data(),
            |writer| {
                writer.add_file(file(0), -1).unwrap();
                writer.add_existing_file(file(1), 3, 4, Some(4)).unwrap();
            },
        )
        .await;
        manifest_file.added_snapshot_id = 42;
        manifest_file.sequence_number = 7;
        let batch = read_manifest_entries(&bytes, &manifest_file).unwrap();

        assert_eq!(
            column(&batch, "snapshot_id").as_ref(),
            &Int64Array::from(vec![42, 3]) as &dyn Array
        );
        for name in ["sequence_number", "file_sequence_number"] {
            assert_eq!(
                column(&batch, name).as_ref(),
                &Int64Array::from(vec![7, 4]) as &dyn Array,
                "{name}"
            );
        }
    }

    async fn first_row_id_manifest() -> (Vec<u8>, ManifestFile) {
        let file = |i: usize, record_count: u64, first_row_id: Option<i64>| DataFile {
            first_row_id,
            ..data_file(DataContentType::Data, &format!("s3://b/data/{i}.parquet"))
                .record_count(record_count)
                .build()
                .unwrap()
        };
        let entry = |status, data_file| ManifestEntry {
            status,
            snapshot_id: Some(1),
            sequence_number: Some(1),
            file_sequence_number: Some(1),
            data_file,
        };
        write_manifest(
            partition_spec(&[]),
            Some(1),
            |b| b.build_v3_data(),
            |writer| {
                writer.add_file(file(0, 10, None), 1).unwrap();
                writer
                    .add_existing_entry(entry(ManifestStatus::Existing, file(1, 3, Some(5))))
                    .unwrap();
                writer
                    .add_delete_entry(entry(ManifestStatus::Deleted, file(2, 4, None)))
                    .unwrap();
                writer
                    .add_existing_entry(entry(ManifestStatus::Existing, file(3, 6, None)))
                    .unwrap();
                writer.add_file(file(4, 2, None), 1).unwrap();
            },
        )
        .await
    }

    #[tokio::test]
    async fn test_first_row_id_inheritance() {
        let (bytes, mut manifest_file) = first_row_id_manifest().await;
        manifest_file.first_row_id = Some(100);
        let batch = read_manifest_entries(&bytes, &manifest_file).unwrap();
        // Deleted entries don't take row IDs, and entries with a first_row_id keep it.
        assert_eq!(
            data_file_column(&batch, "first_row_id").as_ref(),
            &Int64Array::from(vec![Some(100), Some(5), None, Some(110), Some(116)]) as &dyn Array
        );

        manifest_file.first_row_id = None;
        let batch = read_manifest_entries(&bytes, &manifest_file).unwrap();
        assert_eq!(data_file_column(&batch, "first_row_id").null_count(), 5);
    }

    #[tokio::test]
    async fn test_first_row_id_overflow_is_an_error() {
        let (bytes, mut manifest_file) = first_row_id_manifest().await;
        manifest_file.first_row_id = Some(i64::MAX as u64 - 5);
        let err = read_manifest_entries(&bytes, &manifest_file).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[tokio::test]
    async fn test_fields_are_matched_by_field_id() {
        let file = DataFile {
            partition: Struct::from_iter([Some(Literal::int(7))]),
            ..data_file(DataContentType::Data, "s3://b/data/0.parquet")
                .build()
                .unwrap()
        };
        let (bytes, manifest_file) = write_manifest(
            partition_spec(&[("i", Transform::Identity)]),
            Some(1),
            |b| b.build_v2_data(),
            |writer| writer.add_file(file, 1).unwrap(),
        )
        .await;
        let renames = [("file_path", "path"), ("i_identity", "renamed_partition")];
        let bytes = rewrite_manifest(
            &bytes,
            |schema| {
                for_each_avro_field(schema, &|field| {
                    for (from, to) in renames {
                        if field["name"] == from {
                            field["name"] = to.into();
                        }
                    }
                })
            },
            |record| {
                for (from, to) in renames {
                    rename_avro_field(record, from, to);
                }
            },
        );
        let batch = read_manifest_entries(&bytes, &manifest_file).unwrap();

        assert_eq!(
            data_file_column(&batch, "file_path").as_ref(),
            &StringArray::from(vec!["s3://b/data/0.parquet"]) as &dyn Array
        );
        let partition = data_file_column(&batch, "partition");
        assert_eq!(
            partition.as_struct().column(0).as_ref(),
            &Int32Array::from(vec![7]) as &dyn Array
        );
    }

    #[tokio::test]
    async fn test_missing_required_field_is_an_error() {
        let file = data_file(DataContentType::Data, "s3://b/data/0.parquet")
            .build()
            .unwrap();
        let (bytes, manifest_file) = write_manifest(
            partition_spec(&[]),
            Some(1),
            |b| b.build_v2_data(),
            |writer| writer.add_file(file, 1).unwrap(),
        )
        .await;
        // A file_path under another field ID leaves the required file_path missing.
        let bytes = rewrite_manifest(
            &bytes,
            |schema| {
                for_each_avro_field(schema, &|field| {
                    if field["name"] == "file_path" {
                        field["field-id"] = 999.into();
                    }
                })
            },
            |_| {},
        );
        let err = read_manifest_entries(&bytes, &manifest_file).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[tokio::test]
    async fn test_manifest_written_by_pyiceberg() {
        let bytes = std::fs::read(format!(
            "{}/testdata/manifests/pyiceberg-v2-data.avro",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap();
        let manifest = Manifest::parse_avro(&bytes).unwrap();
        let (_, manifest_file) =
            write_manifest(partition_spec(&[]), Some(1), |b| b.build_v2_data(), |_| {}).await;
        let batch = read_manifest_entries(&bytes, &manifest_file).unwrap();

        assert_eq!(batch.num_rows(), manifest.entries().len());
        let paths = data_file_column(&batch, "file_path");
        let partition = data_file_column(&batch, "partition");
        let category = partition.as_struct().column(0).as_string::<i32>().clone();
        for (row, entry) in manifest.entries().iter().enumerate() {
            let data_file = entry.data_file();
            assert_eq!(paths.as_string::<i32>().value(row), data_file.file_path());
            let Some(Literal::Primitive(PrimitiveLiteral::String(expected))) =
                &data_file.partition()[0]
            else {
                panic!("partition value is not a string");
            };
            assert_eq!(category.value(row), expected);
            let mut lower_bounds: Vec<_> = data_file
                .lower_bounds()
                .iter()
                .map(|(id, bound)| (*id, bound.to_bytes().unwrap().to_vec()))
                .collect();
            lower_bounds.sort_unstable();
            assert!(!lower_bounds.is_empty());
            assert_eq!(
                binary_map(&data_file_column(&batch, "lower_bounds"), row),
                lower_bounds
            );
            let mut value_counts: Vec<_> = data_file
                .value_counts()
                .iter()
                .map(|(id, count)| (*id, *count as i64))
                .collect();
            value_counts.sort_unstable();
            assert_eq!(
                long_map(&data_file_column(&batch, "value_counts"), row),
                value_counts
            );
        }
    }
}
