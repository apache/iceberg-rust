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

//! Reading manifests and manifest lists into Arrow.

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use arrow_arith::boolean::{and, is_null};
use arrow_array::builder::Int64Builder;
use arrow_array::cast::AsArray;
use arrow_array::types::{TimestampMicrosecondType, TimestampNanosecondType};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, ListArray, MapArray, RecordBatch,
    StructArray, new_null_array,
};
use arrow_avro::reader::{Reader, ReaderBuilder};
use arrow_avro::schema::AvroSchema;
use arrow_cast::{CastOptions, cast_with_options};
use arrow_ord::cmp::eq;
use arrow_schema::{ArrowError, DataType, Field, Fields, SchemaRef, TimeUnit};
use arrow_select::zip::zip;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use serde_json::Value as JsonValue;

use crate::arrow::schema_to_arrow_schema;
use crate::error::invalid_data;
use crate::spec::{
    DATA_FILE_CONTENT_FIELD_ID, DATA_FILE_FIELD_ID, DATA_FILE_PARTITION_FIELD_ID,
    FILE_SEQUENCE_NUMBER_FIELD_ID, FormatVersion, MANIFEST_CONTENT_FIELD_ID,
    MANIFEST_SEQUENCE_NUMBER_FIELD_ID, MIN_SEQUENCE_NUMBER_FIELD_ID, ManifestContentType,
    ManifestFile, ManifestMetadata, ManifestStatus, SEQUENCE_NUMBER_FIELD_ID, Schema,
    manifest_entry_fields, manifest_file_fields,
};
use crate::{Error, Result};

/// Rows per batch. A row's size grows with the table's column count through
/// the column stats maps. With stats on every column of a 100-column table of
/// longs, 1024 rows take about 9 MB.
const BATCH_SIZE: usize = 1024;

/// Reads the entries of a manifest file as batches with one row per entry.
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
/// # Errors
///
/// Returns an error if the manifest's header can't be read or names an
/// unsupported format version. Errors in the entries come from the reader,
/// which yields no batches after one.
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
/// let mut rows = 0;
/// for batch in read_manifest_entries(&bytes, &manifest_file)? {
///     rows += batch?.num_rows();
/// }
/// assert_eq!(rows, 2);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn read_manifest_entries<B: AsRef<[u8]>>(
    bytes: B,
    manifest_file: &ManifestFile,
) -> Result<ManifestEntryBatchReader<B>> {
    ManifestEntryBatchReader::try_new(bytes, manifest_file, BATCH_SIZE)
}

/// Batches of the entries of a manifest file, returned by
/// [`read_manifest_entries`].
pub struct ManifestEntryBatchReader<B: AsRef<[u8]>> {
    inner: ProjectingReader<B>,
    inheritance: Inheritance,
}

/// The values that a manifest's entries inherit from the manifest list.
struct Inheritance {
    manifest_path: String,
    added_snapshot_id: i64,
    sequence_number: i64,
    content: ManifestContentType,
    /// The first row ID of the next entry that inherits one, which advances
    /// across batches. `None` for a manifest without a first row ID.
    next_row_id: Option<i64>,
}

impl<B: AsRef<[u8]>> ManifestEntryBatchReader<B> {
    fn try_new(bytes: B, manifest_file: &ManifestFile, batch_size: usize) -> Result<Self> {
        // TODO(#3257): read the header from the reader below once the
        // workaround for empty partitions is gone. The reader can't give
        // `bytes` back, so the header is read from a borrowing reader first.
        let (metadata, mut avro_schema) = {
            let header_reader = ReaderBuilder::new()
                .build(bytes.as_ref())
                .map_err(invalid_manifest)?;
            let header = header_reader.avro_header();
            let metadata: HashMap<String, Vec<u8>> = header
                .metadata()
                .map(|(key, value)| (String::from_utf8_lossy(key).into_owned(), value.to_vec()))
                .collect();
            let avro_schema: JsonValue = serde_json::from_slice(
                header
                    .get("avro.schema")
                    .ok_or_else(|| invalid_data!("manifest has no avro.schema"))?,
            )?;
            (ManifestMetadata::parse(&metadata)?, avro_schema)
        };
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
        let mut builder = ReaderBuilder::new().with_batch_size(batch_size);
        if remove_empty_partition(&mut avro_schema) {
            builder = builder.with_reader_schema(AvroSchema::new(avro_schema.to_string()));
        }
        let reader = builder
            .build(Cursor::new(bytes))
            .map_err(invalid_manifest)?;

        // v1 manifests have no content or sequence number columns, which v2+
        // readers read as 0. A v2+ manifest may lack file_sequence_number (Java
        // wrote none before 1.1.0), which reads as null and is inherited.
        let zero_defaults: &[i32] = match metadata.format_version() {
            FormatVersion::V1 => &[
                DATA_FILE_CONTENT_FIELD_ID,
                SEQUENCE_NUMBER_FIELD_ID,
                FILE_SEQUENCE_NUMBER_FIELD_ID,
            ],
            _ => &[DATA_FILE_CONTENT_FIELD_ID],
        };
        Ok(Self {
            inner: ProjectingReader {
                reader,
                schema,
                avro_schema,
                zero_defaults,
                invalid: invalid_manifest,
                finished: false,
            },
            inheritance: Inheritance {
                manifest_path: manifest_file.manifest_path.clone(),
                added_snapshot_id: manifest_file.added_snapshot_id,
                sequence_number: manifest_file.sequence_number,
                content: manifest_file.content,
                next_row_id: manifest_file.first_row_id.map(i64::try_from).transpose()?,
            },
        })
    }

    /// Returns the schema of every batch.
    pub fn schema(&self) -> SchemaRef {
        self.inner.schema.clone()
    }
}

impl<B: AsRef<[u8]>> Iterator for ManifestEntryBatchReader<B> {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let inheritance = &mut self.inheritance;
        self.inner
            .next_batch(|entries| inherit(entries, inheritance))
    }
}

/// Reads the entries of a manifest list as batches with one row per manifest.
///
/// The schema is the spec's `manifest_file` struct for the latest format
/// version, with field IDs in the Arrow field metadata, so manifest lists of
/// every format version read into the same schema. Fields are matched to the
/// manifest list's Avro schema by field ID. The format version comes from the
/// `format-version` key that writers put in the file's metadata, and a
/// manifest list without one is v1. A v1 manifest list reads with the spec's
/// v2+ defaults. Partition summary bounds keep the binary single-value
/// serialization they were written with.
///
/// # Errors
///
/// Returns an error if the manifest list's header can't be read or names an
/// unsupported format version. Errors in the entries come from the reader,
/// which yields no batches after one.
///
/// # Example
///
/// ```
/// use iceberg::arrow::read_manifest_list;
///
/// let bytes = std::fs::read(concat!(
///     env!("CARGO_MANIFEST_DIR"),
///     "/testdata/manifests_lists/manifest-list-v2-2.avro"
/// ))?;
/// let mut rows = 0;
/// for batch in read_manifest_list(&bytes)? {
///     rows += batch?.num_rows();
/// }
/// assert_eq!(rows, 1);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn read_manifest_list<B: AsRef<[u8]>>(bytes: B) -> Result<ManifestListBatchReader<B>> {
    ManifestListBatchReader::try_new(bytes, BATCH_SIZE)
}

/// Batches of the entries of a manifest list, returned by
/// [`read_manifest_list`].
pub struct ManifestListBatchReader<B: AsRef<[u8]>> {
    inner: ProjectingReader<B>,
}

impl<B: AsRef<[u8]>> ManifestListBatchReader<B> {
    fn try_new(bytes: B, batch_size: usize) -> Result<Self> {
        let reader = ReaderBuilder::new()
            .with_batch_size(batch_size)
            .build(Cursor::new(bytes))
            .map_err(invalid_manifest_list)?;
        let header = reader.avro_header();
        let avro_schema: JsonValue = serde_json::from_slice(
            header
                .get("avro.schema")
                .ok_or_else(|| invalid_data!("manifest list has no avro.schema"))?,
        )?;
        // The spec doesn't define this key, but Java, PyIceberg, and iceberg-rust
        // all write it, and rejecting an unknown version keeps a newer manifest
        // list from reading as an older one.
        let format_version = match header.get("format-version") {
            Some(bytes) => serde_json::from_slice::<FormatVersion>(bytes).map_err(|err| {
                invalid_data!("failed to parse format version in manifest list metadata")
                    .with_source(err)
            })?,
            None => FormatVersion::V1,
        };
        let schema = Arc::new(schema_to_arrow_schema(
            &Schema::builder()
                .with_fields(manifest_file_fields())
                .build()?,
        )?);
        // v1 manifest lists have no content or sequence number columns, which v2+
        // readers read as 0.
        let zero_defaults: &[i32] = match format_version {
            FormatVersion::V1 => &[
                MANIFEST_CONTENT_FIELD_ID,
                MANIFEST_SEQUENCE_NUMBER_FIELD_ID,
                MIN_SEQUENCE_NUMBER_FIELD_ID,
            ],
            _ => &[],
        };
        Ok(Self {
            inner: ProjectingReader {
                reader,
                schema,
                avro_schema,
                zero_defaults,
                invalid: invalid_manifest_list,
                finished: false,
            },
        })
    }

    /// Returns the schema of every batch.
    pub fn schema(&self) -> SchemaRef {
        self.inner.schema.clone()
    }
}

impl<B: AsRef<[u8]>> Iterator for ManifestListBatchReader<B> {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next_batch(Ok)
    }
}

/// Decodes an Avro file in batches and projects them onto `schema`.
struct ProjectingReader<B: AsRef<[u8]>> {
    reader: Reader<Cursor<B>>,
    schema: SchemaRef,
    avro_schema: JsonValue,
    zero_defaults: &'static [i32],
    invalid: fn(ArrowError) -> Error,
    /// Set after an error. First row ID inheritance can stop partway through
    /// a batch, so the batches after an error would get the wrong row IDs.
    finished: bool,
}

impl<B: AsRef<[u8]>> ProjectingReader<B> {
    /// Returns the next batch with `finish` applied to its projected rows.
    fn next_batch(
        &mut self,
        finish: impl FnOnce(StructArray) -> Result<StructArray>,
    ) -> Option<Result<RecordBatch>> {
        if self.finished {
            return None;
        }
        let batch = self.reader.next()?.map_err(self.invalid).and_then(|batch| {
            let rows = project_struct(
                &StructArray::from(batch),
                &self.avro_schema,
                self.schema.fields(),
                self.zero_defaults,
            )?;
            Ok(RecordBatch::from(finish(rows)?))
        });
        self.finished = batch.is_err();
        Some(batch)
    }
}

/// Removes `data_file.partition` from a manifest's Avro schema if it has no
/// fields, and returns whether it did.
fn remove_empty_partition(avro_schema: &mut JsonValue) -> bool {
    let has_field_id = |field: &JsonValue, id: i32| field["field-id"].as_i64() == Some(id.into());
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
        !(has_field_id(field, DATA_FILE_PARTITION_FIELD_ID)
            && field["type"]["fields"]
                .as_array()
                .is_some_and(Vec::is_empty))
    });
    data_file_fields.len() != len
}

fn invalid_manifest(err: ArrowError) -> Error {
    invalid_data!("failed to read manifest").with_source(err)
}

fn invalid_manifest_list(err: ArrowError) -> Error {
    invalid_data!("failed to read manifest list").with_source(err)
}

fn invalid_column(err: ArrowError) -> Error {
    invalid_data!("failed to build Arrow column from Avro").with_source(err)
}

/// Returns the field ID of a field in the schema built from
/// `manifest_entry_fields` or `manifest_file_fields`, which give every field
/// one.
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
/// `record` gives the same field ID. A field that `record` lacks reads as 0 if
/// its ID is in `zero_defaults`, and as null otherwise.
fn project_struct(
    array: &StructArray,
    record: &JsonValue,
    fields: &Fields,
    zero_defaults: &[i32],
) -> Result<StructArray> {
    let avro_fields = record["fields"]
        .as_array()
        .ok_or_else(|| invalid_data!("Avro type {record} is not a record"))?;
    let mut columns = Vec::with_capacity(fields.len());
    for field in fields {
        let id = field_id(field);
        columns.push(
            match avro_fields
                .iter()
                .position(|avro_field| avro_field["field-id"].as_i64() == Some(id.into()))
            {
                Some(index) => project(
                    array.column(index),
                    non_null_type(&avro_fields[index]["type"]),
                    field,
                    zero_defaults,
                )?,
                None => missing_field(field, array.len(), zero_defaults)?,
            },
        );
    }
    StructArray::try_new_with_length(fields.clone(), columns, array.nulls().cloned(), array.len())
        .map_err(invalid_column)
}

/// Builds the column for a field that the Avro file doesn't have.
fn missing_field(field: &Field, len: usize, zero_defaults: &[i32]) -> Result<ArrayRef> {
    let id = field_id(field);
    let zero = zero_defaults.contains(&id);
    match field.data_type() {
        DataType::Int32 if zero => Ok(Arc::new(Int32Array::from_value(0, len))),
        DataType::Int64 if zero => Ok(Arc::new(Int64Array::from_value(0, len))),
        _ if field.is_nullable() => Ok(new_null_array(field.data_type(), len)),
        DataType::Struct(fields) if fields.is_empty() => {
            Ok(Arc::new(StructArray::new_empty_fields(len, None)))
        }
        _ => Err(invalid_data!(
            "Avro file has no required field {} with field ID {id}",
            field.name()
        )),
    }
}

/// Converts the decoded `array` of Avro type `avro_type` to `field`, the
/// spec's Arrow field for it.
fn project(
    array: &ArrayRef,
    avro_type: &JsonValue,
    field: &Field,
    zero_defaults: &[i32],
) -> Result<ArrayRef> {
    let type_mismatch = || {
        invalid_data!(
            "field {} has Avro type {avro_type}, which doesn't match {}",
            field.name(),
            field.data_type()
        )
    };
    Ok(match field.data_type() {
        DataType::Struct(fields) => Arc::new(project_struct(
            array.as_struct_opt().ok_or_else(type_mismatch)?,
            avro_type,
            fields,
            zero_defaults,
        )?),
        DataType::List(element) => {
            let list = array.as_list_opt::<i32>().ok_or_else(type_mismatch)?;
            let values = project(
                list.values(),
                non_null_type(&avro_type["items"]),
                element,
                zero_defaults,
            )?;
            Arc::new(
                ListArray::try_new(
                    element.clone(),
                    list.offsets().clone(),
                    values,
                    list.nulls().cloned(),
                )
                .map_err(invalid_column)?,
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
                zero_defaults,
            )?;
            Arc::new(
                MapArray::try_new(
                    entries.clone(),
                    list.offsets().clone(),
                    entries_array,
                    list.nulls().cloned(),
                    false,
                )
                .map_err(invalid_column)?,
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
        .map_err(invalid_column)?,
    })
}

/// Fills in the values that the spec says entries inherit from the manifest
/// list.
fn inherit(entries: StructArray, inheritance: &mut Inheritance) -> Result<StructArray> {
    let (fields, mut columns, nulls) = entries.into_parts();
    let index = |name: &str| fields.find(name).unwrap().0;
    let (status, snapshot_id, sequence_number, file_sequence_number, data_file) = (
        index("status"),
        index("snapshot_id"),
        index("sequence_number"),
        index("file_sequence_number"),
        index("data_file"),
    );

    columns[snapshot_id] = fill_nulls(&columns[snapshot_id], None, inheritance.added_snapshot_id)?;
    if columns[sequence_number].null_count() > 0 || columns[file_sequence_number].null_count() > 0 {
        let added = eq(
            &columns[status],
            &Int32Array::new_scalar(ManifestStatus::Added as i32),
        )?;
        for column in [sequence_number, file_sequence_number] {
            columns[column] =
                fill_nulls(&columns[column], Some(&added), inheritance.sequence_number)?;
        }
    }
    if inheritance.content == ManifestContentType::Data {
        let (data_file_fields, mut data_file_columns, data_file_nulls) =
            columns[data_file].as_struct().clone().into_parts();
        let index = |name: &str| data_file_fields.find(name).unwrap().0;
        let first_row_id = index("first_row_id");
        data_file_columns[first_row_id] = inherit_first_row_ids(
            data_file_columns[first_row_id].as_primitive(),
            columns[status].as_primitive(),
            data_file_columns[index("record_count")].as_primitive(),
            inheritance,
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
/// ID inheritance. Deleted entries take no row IDs, because the spec's
/// suggested manifest `first_row_id` increment, `added_rows_count` plus
/// `existing_rows_count`, leaves no room for them. A manifest without a first
/// row ID belongs to a snapshot without row lineage, so all its row IDs are
/// null.
fn inherit_first_row_ids(
    first_row_ids: &Int64Array,
    status: &Int32Array,
    record_counts: &Int64Array,
    inheritance: &mut Inheritance,
) -> Result<ArrayRef> {
    let Some(next_row_id) = inheritance.next_row_id.as_mut() else {
        return Ok(new_null_array(&DataType::Int64, first_row_ids.len()));
    };
    if first_row_ids.null_count() == 0 {
        return Ok(Arc::new(first_row_ids.clone()));
    }
    let mut builder = Int64Builder::with_capacity(first_row_ids.len());
    for ((first_row_id, &status), &record_count) in first_row_ids
        .iter()
        .zip(status.values())
        .zip(record_counts.values())
    {
        match first_row_id {
            None if status != ManifestStatus::Deleted as i32 => {
                if record_count < 0 {
                    return Err(invalid_data!(
                        "negative record_count {record_count} assigning first row IDs in {}",
                        inheritance.manifest_path
                    ));
                }
                builder.append_value(*next_row_id);
                *next_row_id = next_row_id.checked_add(record_count).ok_or_else(|| {
                    invalid_data!(
                        "row ID overflow assigning first row IDs in {}",
                        inheritance.manifest_path
                    )
                })?;
            }
            first_row_id => builder.append_option(first_row_id),
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use apache_avro::types::Value as AvroValue;
    use apache_avro::{Codec, DeflateSettings, ZstandardSettings};
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Int32Type, Int64Type};
    use arrow_array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
        FixedSizeBinaryArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
        Time64MicrosecondArray, TimestampMicrosecondArray, TimestampNanosecondArray,
    };
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    use arrow_schema::SchemaRef;
    use arrow_select::concat::concat_batches;

    use super::*;
    use crate::ErrorKind;
    use crate::arrow::arrow_schema_to_schema;
    use crate::io::FileIO;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, FieldSummary, ListType,
        Literal, Manifest, ManifestEntry, ManifestList, ManifestListWriter, ManifestStatus,
        ManifestWriter, ManifestWriterBuilder, MapType, NestedField, PartitionSpec,
        PrimitiveLiteral, PrimitiveType, Schema, Struct, StructType, Transform, Type,
        UNASSIGNED_SEQUENCE_NUMBER,
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

    fn concat(
        schema: SchemaRef,
        batches: impl Iterator<Item = Result<RecordBatch>>,
    ) -> Result<RecordBatch> {
        let batches = batches.collect::<Result<Vec<_>>>()?;
        Ok(concat_batches(&schema, &batches)?)
    }

    fn read_entries(bytes: &[u8], manifest_file: &ManifestFile) -> Result<RecordBatch> {
        let reader = read_manifest_entries(bytes, manifest_file)?;
        concat(reader.schema(), reader)
    }

    fn read_list(bytes: &[u8]) -> Result<RecordBatch> {
        let reader = read_manifest_list(bytes)?;
        concat(reader.schema(), reader)
    }

    /// Checks that reading in batches of 1 to 3 rows gives nonempty batches of
    /// the reader's schema that concatenate to the file read as one batch.
    fn assert_batches_match_whole<I: Iterator<Item = Result<RecordBatch>>>(
        read: impl Fn(usize) -> (SchemaRef, I),
    ) {
        let (schema, batches) = read(usize::MAX);
        let whole = concat(schema.clone(), batches).unwrap();
        for batch_size in 1..=3 {
            let (batch_schema, batches) = read(batch_size);
            assert_eq!(batch_schema, schema);
            let batches: Vec<_> = batches.map(Result::unwrap).collect();
            for batch in &batches {
                assert_eq!(batch.schema(), schema, "batch size {batch_size}");
                assert!(
                    (1..=batch_size).contains(&batch.num_rows()),
                    "batch size {batch_size}"
                );
            }
            assert_eq!(
                concat_batches(&schema, &batches).unwrap(),
                whole,
                "batch size {batch_size}"
            );
        }
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
        let values = entries.column(1).as_binary::<i32>();
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

    /// Rewrites an Avro file with the user metadata `key` set to `value`, or
    /// without it if `value` is `None`.
    fn set_avro_metadata(bytes: &[u8], key: &str, value: Option<&str>) -> Vec<u8> {
        let reader = apache_avro::Reader::new(bytes).unwrap();
        let schema = reader.writer_schema().clone();
        let mut metadata = reader.user_metadata().clone();
        match value {
            Some(value) => metadata.insert(key.to_string(), value.as_bytes().to_vec()),
            None => metadata.remove(key),
        };
        let mut writer = apache_avro::Writer::new(&schema, Vec::new()).unwrap();
        for (key, value) in metadata {
            writer.add_user_metadata(key, value).unwrap();
        }
        for record in reader {
            writer.append_value(record.unwrap()).unwrap();
        }
        writer.into_inner().unwrap()
    }

    /// Rewrites a manifest with `codec`, `edit_schema` applied to its Avro
    /// schema, and `edit_record` applied to each record.
    fn rewrite_manifest(
        bytes: &[u8],
        codec: Codec,
        edit_schema: impl Fn(&mut serde_json::Value),
        edit_record: impl Fn(&mut AvroValue),
    ) -> Vec<u8> {
        let reader = apache_avro::Reader::new(bytes).unwrap();
        let mut schema = serde_json::to_value(reader.writer_schema()).unwrap();
        edit_schema(&mut schema);
        let schema = apache_avro::Schema::parse(&schema).unwrap();
        let metadata = reader.user_metadata().clone();
        let mut writer = apache_avro::Writer::with_codec(&schema, Vec::new(), codec).unwrap();
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

    /// Renames record fields in an Avro file's schema and records, keeping
    /// their field IDs.
    fn rename_fields(bytes: &[u8], renames: &[(&str, &str)]) -> Vec<u8> {
        rewrite_manifest(
            bytes,
            Codec::Null,
            |schema| {
                for_each_avro_field(schema, &|field| {
                    for (from, to) in renames {
                        if field["name"] == *from {
                            field["name"] = (*to).into();
                        }
                    }
                })
            },
            |record| {
                for (from, to) in renames {
                    rename_avro_field(record, from, to);
                }
            },
        )
    }

    #[tokio::test]
    async fn test_schema_is_spec_manifest_entry() {
        let spec = partition_spec(&[("i", Transform::Identity), ("s", Transform::Truncate(2))]);
        let (bytes, manifest_file) =
            write_manifest(spec, Some(1), |b| b.build_v2_data(), |_| {}).await;
        let batch = read_entries(&bytes, &manifest_file).unwrap();
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
        let batch = read_entries(&bytes, &manifest_file).unwrap();
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
            &BinaryArray::from(vec![Some(&[1u8, 2][..]), None]) as &dyn Array
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
        let batch = read_entries(&bytes, &manifest_file).unwrap();

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
        let batch = read_entries(&bytes, &manifest_file).unwrap();

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
            Arc::new(BinaryArray::from(vec![Some(&[9u8][..]), None])),
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
        let batch = read_entries(&bytes, &manifest_file).unwrap();

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
                writer.add_delete_file(file(1), 4, Some(4)).unwrap();
                writer.add_existing_file(file(2), 3, 5, Some(5)).unwrap();
            },
        )
        .await;
        // ManifestWriter rejects an existing entry without sequence numbers.
        let bytes = rewrite_manifest(
            &bytes,
            Codec::Null,
            |_| {},
            |record| {
                let AvroValue::Record(fields) = record else {
                    panic!("manifest entry is not a record");
                };
                if fields.contains(&("status".to_string(), AvroValue::Int(0))) {
                    for (name, value) in fields {
                        if name.ends_with("sequence_number") {
                            *value = AvroValue::Union(0, Box::new(AvroValue::Null));
                        }
                    }
                }
            },
        );
        manifest_file.added_snapshot_id = 42;
        manifest_file.sequence_number = 7;
        let batch = read_entries(&bytes, &manifest_file).unwrap();

        assert_eq!(
            column(&batch, "snapshot_id").as_ref(),
            &Int64Array::from(vec![42, 42, 3]) as &dyn Array
        );
        for name in ["sequence_number", "file_sequence_number"] {
            assert_eq!(
                column(&batch, name).as_ref(),
                &Int64Array::from(vec![Some(7), Some(4), None]) as &dyn Array,
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn test_v2_manifest_without_file_sequence_number_inherits() {
        let file = |i: usize| {
            data_file(DataContentType::Data, &format!("s3://b/data/{i}.parquet"))
                .build()
                .unwrap()
        };
        let (bytes, mut manifest_file) = write_manifest(
            partition_spec(&[]),
            Some(1),
            |b| b.build_v2_data(),
            |writer| {
                writer.add_file(file(0), -1).unwrap();
                writer.add_existing_file(file(1), 3, 5, Some(5)).unwrap();
            },
        )
        .await;
        // Java wrote v2 manifests without file_sequence_number before 1.1.0.
        let bytes = rewrite_manifest(
            &bytes,
            Codec::Null,
            |schema| {
                schema["fields"]
                    .as_array_mut()
                    .unwrap()
                    .retain(|field| field["name"] != "file_sequence_number")
            },
            |record| {
                let AvroValue::Record(fields) = record else {
                    panic!("manifest entry is not a record");
                };
                fields.retain(|(name, _)| name != "file_sequence_number");
            },
        );
        manifest_file.sequence_number = 7;
        let batch = read_entries(&bytes, &manifest_file).unwrap();

        assert_eq!(
            column(&batch, "file_sequence_number").as_ref(),
            &Int64Array::from(vec![Some(7), None]) as &dyn Array
        );
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
        let batch = read_entries(&bytes, &manifest_file).unwrap();
        // Deleted entries don't take row IDs, and entries with a first_row_id keep it.
        assert_eq!(
            data_file_column(&batch, "first_row_id").as_ref(),
            &Int64Array::from(vec![Some(100), Some(5), None, Some(110), Some(116)]) as &dyn Array
        );

        manifest_file.first_row_id = None;
        let batch = read_entries(&bytes, &manifest_file).unwrap();
        assert_eq!(data_file_column(&batch, "first_row_id").null_count(), 5);

        manifest_file.first_row_id = Some(i64::MAX as u64 - 5);
        let err = read_entries(&bytes, &manifest_file).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);

        // A negative record_count would assign overlapping row IDs.
        let bytes = negate_record_count(&bytes, 10);
        manifest_file.first_row_id = Some(100);
        let err = read_entries(&bytes, &manifest_file).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    /// Negates the `record_count` of the data files whose count is
    /// `record_count`.
    fn negate_record_count(bytes: &[u8], record_count: i64) -> Vec<u8> {
        rewrite_manifest(
            bytes,
            Codec::Null,
            |_| {},
            |record| {
                let AvroValue::Record(fields) = record else {
                    panic!("manifest entry is not a record");
                };
                let Some((_, AvroValue::Record(data_file))) =
                    fields.iter_mut().find(|(name, _)| name == "data_file")
                else {
                    panic!("manifest entry has no data_file record");
                };
                for (name, value) in data_file {
                    if name == "record_count" && *value == AvroValue::Long(record_count) {
                        *value = AvroValue::Long(-record_count);
                    }
                }
            },
        )
    }

    #[tokio::test]
    async fn test_manifest_batches_match_whole_manifest() {
        let (first_row_id_bytes, mut first_row_id_manifest_file) = first_row_id_manifest().await;
        first_row_id_manifest_file.first_row_id = Some(100);
        let v1_files = (0..3).map(|i| {
            data_file(DataContentType::Data, &format!("s3://b/data/{i}.parquet"))
                .build()
                .unwrap()
        });
        let v1 = write_manifest(
            partition_spec(&[]),
            Some(1),
            |b| b.build_v1(),
            |writer| v1_files.for_each(|file| writer.add_file(file, -1).unwrap()),
        )
        .await;
        let empty = write_manifest(
            partition_spec(&[("i", Transform::Identity)]),
            Some(1),
            |b| b.build_v2_data(),
            |_| {},
        )
        .await;
        let pyiceberg_bytes = std::fs::read(format!(
            "{}/testdata/manifests/pyiceberg-v2-data.avro",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap();
        let pyiceberg_manifest_file = ManifestFile {
            sequence_number: 1,
            ..listed_manifest("s3://b/metadata/0.avro", ManifestContentType::Data)
        };

        for (bytes, manifest_file) in [
            (first_row_id_bytes, first_row_id_manifest_file),
            v1,
            empty,
            (pyiceberg_bytes, pyiceberg_manifest_file),
        ] {
            assert_batches_match_whole(|batch_size| {
                let reader =
                    ManifestEntryBatchReader::try_new(&bytes, &manifest_file, batch_size).unwrap();
                (reader.schema(), reader)
            });
        }
    }

    #[tokio::test]
    async fn test_unknown_manifest_format_version_is_an_error() {
        let (bytes, manifest_file) = first_row_id_manifest().await;
        let bytes = set_avro_metadata(&bytes, "format-version", Some("4"));
        let Err(err) = read_manifest_entries(&bytes, &manifest_file) else {
            panic!("a v4 manifest read as an older version");
        };
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[tokio::test]
    async fn test_manifest_error_ends_the_batches() {
        let (bytes, mut manifest_file) = first_row_id_manifest().await;
        manifest_file.first_row_id = Some(100);
        // The last of the five entries fails first row ID inheritance, so the
        // third batch of two is an error.
        let bytes = negate_record_count(&bytes, 2);
        let mut reader = ManifestEntryBatchReader::try_new(&bytes, &manifest_file, 2).unwrap();
        assert_eq!(reader.next().unwrap().unwrap().num_rows(), 2);
        assert_eq!(reader.next().unwrap().unwrap().num_rows(), 2);
        assert_eq!(
            reader.next().unwrap().unwrap_err().kind(),
            ErrorKind::DataInvalid
        );
        assert!(reader.next().is_none());
    }

    #[tokio::test]
    async fn test_compressed_manifests() {
        let (bytes, manifest_file) = first_row_id_manifest().await;
        let expected = read_entries(&bytes, &manifest_file).unwrap();
        for codec in [
            Codec::Deflate(DeflateSettings::default()),
            Codec::Snappy,
            Codec::Zstandard(ZstandardSettings::default()),
        ] {
            let bytes = rewrite_manifest(&bytes, codec, |_| {}, |_| {});
            assert_eq!(
                read_entries(&bytes, &manifest_file).unwrap(),
                expected,
                "{codec:?}"
            );
        }
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
        let bytes = rename_fields(&bytes, &[
            ("file_path", "path"),
            ("i_identity", "renamed_partition"),
        ]);
        let batch = read_entries(&bytes, &manifest_file).unwrap();

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
            Codec::Null,
            |schema| {
                for_each_avro_field(schema, &|field| {
                    if field["name"] == "file_path" {
                        field["field-id"] = 999.into();
                    }
                })
            },
            |_| {},
        );
        let err = read_entries(&bytes, &manifest_file).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn test_manifest_written_by_pyiceberg() {
        let bytes = std::fs::read(format!(
            "{}/testdata/manifests/pyiceberg-v2-data.avro",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap();
        let manifest = Manifest::parse_avro(&bytes).unwrap();
        let manifest_file = ManifestFile {
            manifest_path: "s3://bucket/metadata/manifest.avro".to_string(),
            manifest_length: bytes.len() as i64,
            partition_spec_id: 0,
            content: ManifestContentType::Data,
            sequence_number: 1,
            min_sequence_number: 1,
            added_snapshot_id: 1,
            added_files_count: None,
            existing_files_count: None,
            deleted_files_count: None,
            added_rows_count: None,
            existing_rows_count: None,
            deleted_rows_count: None,
            partitions: None,
            key_metadata: None,
            first_row_id: None,
        };
        let batch = read_entries(&bytes, &manifest_file).unwrap();

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
        }
    }

    /// A manifest list entry that a manifest list writer for snapshot 1
    /// assigns sequence numbers to.
    fn listed_manifest(path: &str, content: ManifestContentType) -> ManifestFile {
        ManifestFile {
            manifest_path: path.to_string(),
            manifest_length: 100,
            partition_spec_id: 0,
            content,
            sequence_number: UNASSIGNED_SEQUENCE_NUMBER,
            min_sequence_number: UNASSIGNED_SEQUENCE_NUMBER,
            added_snapshot_id: 1,
            added_files_count: Some(1),
            existing_files_count: Some(2),
            deleted_files_count: Some(3),
            added_rows_count: Some(10),
            existing_rows_count: Some(20),
            deleted_rows_count: Some(30),
            partitions: None,
            key_metadata: None,
            first_row_id: None,
        }
    }

    /// Writes a manifest list for snapshot 1 with sequence number 5 and, in
    /// v3, first row ID 100.
    async fn write_manifest_list(version: FormatVersion, manifests: Vec<ManifestFile>) -> Vec<u8> {
        let io = FileIO::new_with_memory();
        let path = "memory:///manifest-list.avro";
        let output = io.new_output(path).unwrap().writer().await.unwrap();
        let mut writer = match version {
            FormatVersion::V1 => ManifestListWriter::v1(output, 1, None),
            FormatVersion::V2 => ManifestListWriter::v2(output, 1, None, 5),
            FormatVersion::V3 => ManifestListWriter::v3(output, 1, None, 5, Some(100)),
        };
        writer.add_manifests(manifests.into_iter()).unwrap();
        writer.close().await.unwrap();
        io.new_input(path).unwrap().read().await.unwrap().to_vec()
    }

    #[tokio::test]
    async fn test_schema_is_spec_manifest_file() {
        let bytes = write_manifest_list(FormatVersion::V3, vec![]).await;
        let batch = read_list(&bytes).unwrap();
        assert_eq!(batch.num_rows(), 0);

        let long = || Type::Primitive(PrimitiveType::Long);
        let int = || Type::Primitive(PrimitiveType::Int);
        let boolean = || Type::Primitive(PrimitiveType::Boolean);
        let binary = || Type::Primitive(PrimitiveType::Binary);
        let field_summary = StructType::new(vec![
            NestedField::required(509, "contains_null", boolean()).into(),
            NestedField::optional(518, "contains_nan", boolean()).into(),
            NestedField::optional(510, "lower_bound", binary()).into(),
            NestedField::optional(511, "upper_bound", binary()).into(),
        ]);
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(500, "manifest_path", Type::Primitive(PrimitiveType::String))
                    .into(),
                NestedField::required(501, "manifest_length", long()).into(),
                NestedField::required(502, "partition_spec_id", int()).into(),
                NestedField::required(517, "content", int()).into(),
                NestedField::required(515, "sequence_number", long()).into(),
                NestedField::required(516, "min_sequence_number", long()).into(),
                NestedField::required(503, "added_snapshot_id", long()).into(),
                NestedField::optional(504, "added_files_count", int()).into(),
                NestedField::optional(505, "existing_files_count", int()).into(),
                NestedField::optional(506, "deleted_files_count", int()).into(),
                NestedField::optional(512, "added_rows_count", long()).into(),
                NestedField::optional(513, "existing_rows_count", long()).into(),
                NestedField::optional(514, "deleted_rows_count", long()).into(),
                NestedField::optional(
                    507,
                    "partitions",
                    Type::List(ListType::new(
                        NestedField::list_element(508, Type::Struct(field_summary), true).into(),
                    )),
                )
                .into(),
                NestedField::optional(519, "key_metadata", binary()).into(),
                NestedField::optional(520, "first_row_id", long()).into(),
            ])
            .build()
            .unwrap();
        assert_eq!(
            arrow_schema_to_schema(&batch.schema()).unwrap().as_struct(),
            expected.as_struct()
        );
    }

    #[tokio::test]
    async fn test_manifest_list_columns() {
        let data = ManifestFile {
            partitions: Some(vec![FieldSummary {
                contains_null: true,
                contains_nan: Some(false),
                lower_bound: Some(Datum::int(1).to_bytes().unwrap()),
                upper_bound: Some(Datum::int(9).to_bytes().unwrap()),
            }]),
            key_metadata: Some(vec![1, 2]),
            ..listed_manifest("s3://b/metadata/data.avro", ManifestContentType::Data)
        };
        let existing_deletes = ManifestFile {
            partition_spec_id: 1,
            sequence_number: 3,
            min_sequence_number: 2,
            added_snapshot_id: 0,
            added_files_count: Some(0),
            ..listed_manifest("s3://b/metadata/deletes.avro", ManifestContentType::Deletes)
        };
        let bytes = write_manifest_list(FormatVersion::V3, vec![data, existing_deletes]).await;
        let batch = read_list(&bytes).unwrap();

        let schema = batch.schema();
        let DataType::List(summary) = schema.field_with_name("partitions").unwrap().data_type()
        else {
            panic!("partitions is not a list");
        };
        let DataType::Struct(summary_fields) = summary.data_type() else {
            panic!("partitions element is not a struct");
        };
        let summaries = StructArray::new(
            summary_fields.clone(),
            vec![
                Arc::new(BooleanArray::from(vec![true])),
                Arc::new(BooleanArray::from(vec![Some(false)])),
                Arc::new(BinaryArray::from(vec![&1i32.to_le_bytes()[..]])),
                Arc::new(BinaryArray::from(vec![&9i32.to_le_bytes()[..]])),
            ],
            None,
        );
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![
                "s3://b/metadata/data.avro",
                "s3://b/metadata/deletes.avro",
            ])),
            Arc::new(Int64Array::from(vec![100, 100])),
            Arc::new(Int32Array::from(vec![0, 1])),
            Arc::new(Int32Array::from(vec![0, 1])),
            Arc::new(Int64Array::from(vec![5, 3])),
            Arc::new(Int64Array::from(vec![5, 2])),
            Arc::new(Int64Array::from(vec![1, 0])),
            Arc::new(Int32Array::from(vec![1, 0])),
            Arc::new(Int32Array::from(vec![2, 2])),
            Arc::new(Int32Array::from(vec![3, 3])),
            Arc::new(Int64Array::from(vec![10, 10])),
            Arc::new(Int64Array::from(vec![20, 20])),
            Arc::new(Int64Array::from(vec![30, 30])),
            Arc::new(ListArray::new(
                summary.clone(),
                OffsetBuffer::from_lengths([1, 0]),
                Arc::new(summaries),
                Some(NullBuffer::from(vec![true, false])),
            )),
            Arc::new(BinaryArray::from(vec![Some(&[1u8, 2][..]), None])),
            // The writer assigns the data manifest the snapshot's first row ID.
            Arc::new(Int64Array::from(vec![Some(100), None])),
        ];
        assert_eq!(batch, RecordBatch::try_new(schema, columns).unwrap());
    }

    #[tokio::test]
    async fn test_v1_manifest_list_reads_with_v2_defaults() {
        let unknown_counts = ManifestFile {
            added_files_count: None,
            existing_files_count: None,
            deleted_files_count: None,
            added_rows_count: None,
            existing_rows_count: None,
            deleted_rows_count: None,
            ..listed_manifest("s3://b/metadata/1.avro", ManifestContentType::Data)
        };
        let bytes = write_manifest_list(FormatVersion::V1, vec![
            listed_manifest("s3://b/metadata/0.avro", ManifestContentType::Data),
            unknown_counts,
        ])
        .await;
        // A manifest list without a format version reads as v1.
        let without_format_version = set_avro_metadata(&bytes, "format-version", None);
        for bytes in [bytes, without_format_version] {
            let batch = read_list(&bytes).unwrap();

            assert_eq!(
                column(&batch, "content").as_ref(),
                &Int32Array::from(vec![0, 0]) as &dyn Array
            );
            for name in ["sequence_number", "min_sequence_number"] {
                assert_eq!(
                    column(&batch, name).as_ref(),
                    &Int64Array::from(vec![0, 0]) as &dyn Array,
                    "{name}"
                );
            }
            assert_eq!(
                column(&batch, "added_files_count").as_ref(),
                &Int32Array::from(vec![Some(1), None]) as &dyn Array
            );
            assert_eq!(
                column(&batch, "deleted_rows_count").as_ref(),
                &Int64Array::from(vec![Some(30), None]) as &dyn Array
            );
            assert_eq!(column(&batch, "first_row_id").null_count(), 2);
        }
    }

    #[tokio::test]
    async fn test_manifest_list_fields_are_matched_by_field_id() {
        let manifest = ManifestFile {
            partitions: Some(vec![FieldSummary {
                contains_null: true,
                contains_nan: None,
                lower_bound: None,
                upper_bound: None,
            }]),
            ..listed_manifest("s3://b/metadata/0.avro", ManifestContentType::Data)
        };
        let bytes = write_manifest_list(FormatVersion::V2, vec![manifest]).await;
        let bytes = rename_fields(&bytes, &[
            ("manifest_path", "path"),
            ("added_files_count", "added_data_files_count"),
            ("contains_null", "has_null"),
        ]);
        let batch = read_list(&bytes).unwrap();

        assert_eq!(
            column(&batch, "manifest_path").as_ref(),
            &StringArray::from(vec!["s3://b/metadata/0.avro"]) as &dyn Array
        );
        assert_eq!(
            column(&batch, "added_files_count").as_ref(),
            &Int32Array::from(vec![1]) as &dyn Array
        );
        let partitions = column(&batch, "partitions");
        assert_eq!(
            partitions
                .as_list::<i32>()
                .value(0)
                .as_struct()
                .column(0)
                .as_ref(),
            &BooleanArray::from(vec![true]) as &dyn Array
        );
    }

    #[tokio::test]
    async fn test_invalid_manifest_list_is_an_error() {
        let err = read_list(&[]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);

        let bytes = write_manifest_list(FormatVersion::V2, vec![listed_manifest(
            "s3://b/metadata/0.avro",
            ManifestContentType::Data,
        )])
        .await;
        // A manifest_path under another field ID leaves the required
        // manifest_path missing.
        let moved_manifest_path = rewrite_manifest(
            &bytes,
            Codec::Null,
            |schema| {
                for_each_avro_field(schema, &|field| {
                    if field["name"] == "manifest_path" {
                        field["field-id"] = 999.into();
                    }
                })
            },
            |_| {},
        );
        // Only v1 manifest lists read a missing sequence number as 0.
        let without_sequence_number = rewrite_manifest(
            &bytes,
            Codec::Null,
            |schema| {
                schema["fields"]
                    .as_array_mut()
                    .unwrap()
                    .retain(|field| field["name"] != "sequence_number")
            },
            |record| {
                let AvroValue::Record(fields) = record else {
                    panic!("manifest list entry is not a record");
                };
                fields.retain(|(name, _)| name != "sequence_number");
            },
        );
        let unknown_format_version = set_avro_metadata(&bytes, "format-version", Some("4"));
        for bytes in [
            moved_manifest_path,
            without_sequence_number,
            unknown_format_version,
        ] {
            let err = read_list(&bytes).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::DataInvalid);
        }
    }

    #[test]
    fn test_manifest_lists_written_by_java() {
        // Written by Java 1.4.0, with the v1 file count names, and 1.5.0.
        for name in ["manifest-list-v2-1.avro", "manifest-list-v2-2.avro"] {
            let bytes = std::fs::read(format!(
                "{}/testdata/manifests_lists/{name}",
                env!("CARGO_MANIFEST_DIR")
            ))
            .unwrap();
            let manifest_list =
                ManifestList::parse_with_version(&bytes, FormatVersion::V2).unwrap();
            let batch = read_list(&bytes).unwrap();

            assert_eq!(batch.num_rows(), manifest_list.entries().len(), "{name}");
            let paths = column(&batch, "manifest_path");
            let sequence_numbers = column(&batch, "sequence_number");
            let added_files_counts = column(&batch, "added_files_count");
            let partitions = column(&batch, "partitions");
            for (row, manifest) in manifest_list.entries().iter().enumerate() {
                assert_eq!(
                    paths.as_string::<i32>().value(row),
                    manifest.manifest_path,
                    "{name}"
                );
                assert_eq!(
                    sequence_numbers.as_primitive::<Int64Type>().value(row),
                    manifest.sequence_number,
                    "{name}"
                );
                assert_eq!(
                    added_files_counts.as_primitive::<Int32Type>().value(row),
                    manifest.added_files_count.unwrap() as i32,
                    "{name}"
                );
                let summaries = partitions.as_list::<i32>().value(row);
                let lower_bounds: Vec<_> = summaries
                    .as_struct()
                    .column_by_name("lower_bound")
                    .unwrap()
                    .as_binary::<i32>()
                    .iter()
                    .map(|bound| bound.map(<[u8]>::to_vec))
                    .collect();
                let expected: Vec<_> = manifest
                    .partitions
                    .iter()
                    .flatten()
                    .map(|summary| summary.lower_bound.as_ref().map(|bound| bound.to_vec()))
                    .collect();
                assert_eq!(lower_bounds, expected, "{name}");
            }
        }
    }

    #[tokio::test]
    async fn test_manifest_list_batches_match_whole_list() {
        for version in [FormatVersion::V1, FormatVersion::V2, FormatVersion::V3] {
            for len in [0, 3] {
                let manifests = (0..len)
                    .map(|i| {
                        listed_manifest(
                            &format!("s3://b/metadata/{i}.avro"),
                            ManifestContentType::Data,
                        )
                    })
                    .collect();
                let bytes = write_manifest_list(version, manifests).await;
                assert_batches_match_whole(|batch_size| {
                    let reader = ManifestListBatchReader::try_new(&bytes, batch_size).unwrap();
                    (reader.schema(), reader)
                });
            }
        }
    }
}
