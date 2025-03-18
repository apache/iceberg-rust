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

use apache_avro::Schema as AvroSchema;
use once_cell::sync::Lazy;
use serde_derive::{Deserialize, Serialize};
use serde_with::serde_as;

use super::ManifestEntry;
use crate::avro::schema_to_avro_schema;
use crate::error::Result;
use crate::spec::manifest::data_file;
use crate::spec::{
    Datum, ListType, Literal, MapType, NestedField, NestedFieldRef, PrimitiveType, RawLiteral,
    Schema, Struct, StructType, Type,
};
use crate::{Error, ErrorKind};

#[derive(Serialize, Deserialize)]
pub(super) struct ManifestEntryV2 {
    status: i32,
    snapshot_id: Option<i64>,
    sequence_number: Option<i64>,
    file_sequence_number: Option<i64>,
    data_file: DataFileSerde,
}

impl ManifestEntryV2 {
    pub fn try_from(value: ManifestEntry, partition_type: &StructType) -> Result<Self> {
        Ok(Self {
            status: value.status as i32,
            snapshot_id: value.snapshot_id,
            sequence_number: value.sequence_number,
            file_sequence_number: value.file_sequence_number,
            data_file: DataFileSerde::try_from(value.data_file, partition_type, false)?,
        })
    }

    pub fn try_into(self, partition_type: &StructType, schema: &Schema) -> Result<ManifestEntry> {
        Ok(ManifestEntry {
            status: self.status.try_into()?,
            snapshot_id: self.snapshot_id,
            sequence_number: self.sequence_number,
            file_sequence_number: self.file_sequence_number,
            data_file: self.data_file.try_into(partition_type, schema)?,
        })
    }
}

#[derive(Serialize, Deserialize)]
pub(super) struct ManifestEntryV1 {
    status: i32,
    pub snapshot_id: i64,
    data_file: DataFileSerde,
}

impl ManifestEntryV1 {
    pub fn try_from(value: ManifestEntry, partition_type: &StructType) -> Result<Self> {
        Ok(Self {
            status: value.status as i32,
            snapshot_id: value.snapshot_id.unwrap_or_default(),
            data_file: DataFileSerde::try_from(value.data_file, partition_type, true)?,
        })
    }

    pub fn try_into(self, partition_type: &StructType, schema: &Schema) -> Result<ManifestEntry> {
        Ok(ManifestEntry {
            status: self.status.try_into()?,
            snapshot_id: Some(self.snapshot_id),
            sequence_number: Some(0),
            file_sequence_number: Some(0),
            data_file: self.data_file.try_into(partition_type, schema)?,
        })
    }
}

#[serde_as]
#[derive(Serialize, Deserialize)]
pub(super) struct DataFileSerde {
    #[serde(default)]
    content: i32,
    file_path: String,
    file_format: String,
    partition: RawLiteral,
    record_count: i64,
    file_size_in_bytes: i64,
    #[serde(skip_deserializing, skip_serializing_if = "Option::is_none")]
    block_size_in_bytes: Option<i64>,
    column_sizes: Option<Vec<I64Entry>>,
    value_counts: Option<Vec<I64Entry>>,
    null_value_counts: Option<Vec<I64Entry>>,
    nan_value_counts: Option<Vec<I64Entry>>,
    lower_bounds: Option<Vec<BytesEntry>>,
    upper_bounds: Option<Vec<BytesEntry>>,
    key_metadata: Option<serde_bytes::ByteBuf>,
    split_offsets: Option<Vec<i64>>,
    #[serde(default)]
    equality_ids: Option<Vec<i32>>,
    sort_order_id: Option<i32>,
}

impl DataFileSerde {
    pub fn try_from(
        value: data_file::DataFile,
        partition_type: &StructType,
        is_version_1: bool,
    ) -> Result<Self> {
        let block_size_in_bytes = if is_version_1 { Some(0) } else { None };
        Ok(Self {
            content: value.content as i32,
            file_path: value.file_path,
            file_format: value.file_format.to_string().to_ascii_uppercase(),
            partition: RawLiteral::try_from(
                Literal::Struct(value.partition),
                &Type::Struct(partition_type.clone()),
            )?,
            record_count: value.record_count.try_into()?,
            file_size_in_bytes: value.file_size_in_bytes.try_into()?,
            block_size_in_bytes,
            column_sizes: Some(to_i64_entry(value.column_sizes)?),
            value_counts: Some(to_i64_entry(value.value_counts)?),
            null_value_counts: Some(to_i64_entry(value.null_value_counts)?),
            nan_value_counts: Some(to_i64_entry(value.nan_value_counts)?),
            lower_bounds: Some(to_bytes_entry(value.lower_bounds)?),
            upper_bounds: Some(to_bytes_entry(value.upper_bounds)?),
            key_metadata: value.key_metadata.map(serde_bytes::ByteBuf::from),
            split_offsets: Some(value.split_offsets),
            equality_ids: Some(value.equality_ids),
            sort_order_id: value.sort_order_id,
        })
    }

    pub fn try_into(
        self,
        partition_type: &StructType,
        schema: &Schema,
    ) -> Result<data_file::DataFile> {
        let partition = self
            .partition
            .try_into(&Type::Struct(partition_type.clone()))?
            .map(|v| {
                if let Literal::Struct(v) = v {
                    Ok(v)
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        "partition value is not a struct",
                    ))
                }
            })
            .transpose()?
            .unwrap_or(Struct::empty());
        Ok(data_file::DataFile {
            content: self.content.try_into()?,
            file_path: self.file_path,
            file_format: self.file_format.parse()?,
            partition,
            record_count: self.record_count.try_into()?,
            file_size_in_bytes: self.file_size_in_bytes.try_into()?,
            column_sizes: self
                .column_sizes
                .map(parse_i64_entry)
                .transpose()?
                .unwrap_or_default(),
            value_counts: self
                .value_counts
                .map(parse_i64_entry)
                .transpose()?
                .unwrap_or_default(),
            null_value_counts: self
                .null_value_counts
                .map(parse_i64_entry)
                .transpose()?
                .unwrap_or_default(),
            nan_value_counts: self
                .nan_value_counts
                .map(parse_i64_entry)
                .transpose()?
                .unwrap_or_default(),
            lower_bounds: self
                .lower_bounds
                .map(|v| parse_bytes_entry(v, schema))
                .transpose()?
                .unwrap_or_default(),
            upper_bounds: self
                .upper_bounds
                .map(|v| parse_bytes_entry(v, schema))
                .transpose()?
                .unwrap_or_default(),
            key_metadata: self.key_metadata.map(|v| v.to_vec()),
            split_offsets: self.split_offsets.unwrap_or_default(),
            equality_ids: self.equality_ids.unwrap_or_default(),
            sort_order_id: self.sort_order_id,
        })
    }
}

#[serde_as]
#[derive(Serialize, Deserialize)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
struct BytesEntry {
    key: i32,
    value: serde_bytes::ByteBuf,
}

fn parse_bytes_entry(v: Vec<BytesEntry>, schema: &Schema) -> Result<HashMap<i32, Datum>> {
    let mut m = HashMap::with_capacity(v.len());
    for entry in v {
        // We ignore the entry if the field is not found in the schema, due to schema evolution.
        if let Some(field) = schema.field_by_id(entry.key) {
            let data_type = field
                .field_type
                .as_primitive_type()
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("field {} is not a primitive type", field.name),
                    )
                })?
                .clone();
            m.insert(entry.key, Datum::try_from_bytes(&entry.value, data_type)?);
        }
    }
    Ok(m)
}

fn to_bytes_entry(v: impl IntoIterator<Item = (i32, Datum)>) -> Result<Vec<BytesEntry>> {
    let iter = v.into_iter();
    // Reserve the capacity to the lower bound.
    let mut bs = Vec::with_capacity(iter.size_hint().0);
    for (k, d) in iter {
        bs.push(BytesEntry {
            key: k,
            value: d.to_bytes()?,
        });
    }
    Ok(bs)
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub(crate) struct I64Entry {
    key: i32,
    value: i64,
}

pub(crate) fn parse_i64_entry(v: Vec<I64Entry>) -> Result<HashMap<i32, u64>> {
    let mut m = HashMap::with_capacity(v.len());
    for entry in v {
        // We ignore the entry if it's value is negative since these entries are supposed to be used for
        // counting, which should never be negative.
        if let Ok(v) = entry.value.try_into() {
            m.insert(entry.key, v);
        }
    }
    Ok(m)
}

fn to_i64_entry(entries: HashMap<i32, u64>) -> Result<Vec<I64Entry>> {
    entries
        .iter()
        .map(|e| {
            Ok(I64Entry {
                key: *e.0,
                value: (*e.1).try_into()?,
            })
        })
        .collect()
}

static STATUS: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::required(
            0,
            "status",
            Type::Primitive(PrimitiveType::Int),
        ))
    })
};

static SNAPSHOT_ID_V1: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::required(
            1,
            "snapshot_id",
            Type::Primitive(PrimitiveType::Long),
        ))
    })
};

static SNAPSHOT_ID_V2: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::optional(
            1,
            "snapshot_id",
            Type::Primitive(PrimitiveType::Long),
        ))
    })
};

static SEQUENCE_NUMBER: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::optional(
            3,
            "sequence_number",
            Type::Primitive(PrimitiveType::Long),
        ))
    })
};

static FILE_SEQUENCE_NUMBER: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::optional(
            4,
            "file_sequence_number",
            Type::Primitive(PrimitiveType::Long),
        ))
    })
};

static CONTENT: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::required(
            134,
            "content",
            Type::Primitive(PrimitiveType::Int),
        ))
    })
};

static FILE_PATH: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::required(
            100,
            "file_path",
            Type::Primitive(PrimitiveType::String),
        ))
    })
};

static FILE_FORMAT: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::required(
            101,
            "file_format",
            Type::Primitive(PrimitiveType::String),
        ))
    })
};

static RECORD_COUNT: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::required(
            103,
            "record_count",
            Type::Primitive(PrimitiveType::Long),
        ))
    })
};

static FILE_SIZE_IN_BYTES: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::required(
            104,
            "file_size_in_bytes",
            Type::Primitive(PrimitiveType::Long),
        ))
    })
};

// Deprecated. Always write a default in v1. Do not write in v2.
static BLOCK_SIZE_IN_BYTES: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::required(
            105,
            "block_size_in_bytes",
            Type::Primitive(PrimitiveType::Long),
        ))
    })
};

static COLUMN_SIZES: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::optional(
            108,
            "column_sizes",
            Type::Map(MapType {
                key_field: Arc::new(NestedField::required(
                    117,
                    "key",
                    Type::Primitive(PrimitiveType::Int),
                )),
                value_field: Arc::new(NestedField::required(
                    118,
                    "value",
                    Type::Primitive(PrimitiveType::Long),
                )),
            }),
        ))
    })
};

static VALUE_COUNTS: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::optional(
            109,
            "value_counts",
            Type::Map(MapType {
                key_field: Arc::new(NestedField::required(
                    119,
                    "key",
                    Type::Primitive(PrimitiveType::Int),
                )),
                value_field: Arc::new(NestedField::required(
                    120,
                    "value",
                    Type::Primitive(PrimitiveType::Long),
                )),
            }),
        ))
    })
};

static NULL_VALUE_COUNTS: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::optional(
            110,
            "null_value_counts",
            Type::Map(MapType {
                key_field: Arc::new(NestedField::required(
                    121,
                    "key",
                    Type::Primitive(PrimitiveType::Int),
                )),
                value_field: Arc::new(NestedField::required(
                    122,
                    "value",
                    Type::Primitive(PrimitiveType::Long),
                )),
            }),
        ))
    })
};

static NAN_VALUE_COUNTS: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::optional(
            137,
            "nan_value_counts",
            Type::Map(MapType {
                key_field: Arc::new(NestedField::required(
                    138,
                    "key",
                    Type::Primitive(PrimitiveType::Int),
                )),
                value_field: Arc::new(NestedField::required(
                    139,
                    "value",
                    Type::Primitive(PrimitiveType::Long),
                )),
            }),
        ))
    })
};

static LOWER_BOUNDS: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::optional(
            125,
            "lower_bounds",
            Type::Map(MapType {
                key_field: Arc::new(NestedField::required(
                    126,
                    "key",
                    Type::Primitive(PrimitiveType::Int),
                )),
                value_field: Arc::new(NestedField::required(
                    127,
                    "value",
                    Type::Primitive(PrimitiveType::Binary),
                )),
            }),
        ))
    })
};

static UPPER_BOUNDS: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::optional(
            128,
            "upper_bounds",
            Type::Map(MapType {
                key_field: Arc::new(NestedField::required(
                    129,
                    "key",
                    Type::Primitive(PrimitiveType::Int),
                )),
                value_field: Arc::new(NestedField::required(
                    130,
                    "value",
                    Type::Primitive(PrimitiveType::Binary),
                )),
            }),
        ))
    })
};

static KEY_METADATA: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::optional(
            131,
            "key_metadata",
            Type::Primitive(PrimitiveType::Binary),
        ))
    })
};

static SPLIT_OFFSETS: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::optional(
            132,
            "split_offsets",
            Type::List(ListType {
                element_field: Arc::new(NestedField::required(
                    133,
                    "element",
                    Type::Primitive(PrimitiveType::Long),
                )),
            }),
        ))
    })
};

static EQUALITY_IDS: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::optional(
            135,
            "equality_ids",
            Type::List(ListType {
                element_field: Arc::new(NestedField::required(
                    136,
                    "element",
                    Type::Primitive(PrimitiveType::Int),
                )),
            }),
        ))
    })
};

static SORT_ORDER_ID: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::optional(
            140,
            "sort_order_id",
            Type::Primitive(PrimitiveType::Int),
        ))
    })
};

fn data_file_fields_v2(partition_type: &StructType) -> Vec<NestedFieldRef> {
    vec![
        CONTENT.clone(),
        FILE_PATH.clone(),
        FILE_FORMAT.clone(),
        Arc::new(NestedField::required(
            102,
            "partition",
            Type::Struct(partition_type.clone()),
        )),
        RECORD_COUNT.clone(),
        FILE_SIZE_IN_BYTES.clone(),
        COLUMN_SIZES.clone(),
        VALUE_COUNTS.clone(),
        NULL_VALUE_COUNTS.clone(),
        NAN_VALUE_COUNTS.clone(),
        LOWER_BOUNDS.clone(),
        UPPER_BOUNDS.clone(),
        KEY_METADATA.clone(),
        SPLIT_OFFSETS.clone(),
        EQUALITY_IDS.clone(),
        SORT_ORDER_ID.clone(),
    ]
}

pub(super) fn manifest_schema_v1(partition_type: &StructType) -> Result<AvroSchema> {
    let fields = vec![
        STATUS.clone(),
        SNAPSHOT_ID_V1.clone(),
        Arc::new(NestedField::required(
            2,
            "data_file",
            Type::Struct(StructType::new(data_file_fields_v1(partition_type))),
        )),
    ];
    let schema = Schema::builder().with_fields(fields).build()?;
    schema_to_avro_schema("manifest_entry", &schema)
}

pub(super) fn manifest_schema_v2(partition_type: &StructType) -> Result<AvroSchema> {
    let fields = vec![
        STATUS.clone(),
        SNAPSHOT_ID_V2.clone(),
        SEQUENCE_NUMBER.clone(),
        FILE_SEQUENCE_NUMBER.clone(),
        Arc::new(NestedField::required(
            2,
            "data_file",
            Type::Struct(StructType::new(data_file_fields_v2(partition_type))),
        )),
    ];
    let schema = Schema::builder().with_fields(fields).build()?;
    schema_to_avro_schema("manifest_entry", &schema)
}

pub fn data_file_fields_v1(partition_type: &StructType) -> Vec<NestedFieldRef> {
    vec![
        FILE_PATH.clone(),
        FILE_FORMAT.clone(),
        Arc::new(NestedField::required(
            102,
            "partition",
            Type::Struct(partition_type.clone()),
        )),
        RECORD_COUNT.clone(),
        FILE_SIZE_IN_BYTES.clone(),
        BLOCK_SIZE_IN_BYTES.clone(),
        COLUMN_SIZES.clone(),
        VALUE_COUNTS.clone(),
        NULL_VALUE_COUNTS.clone(),
        NAN_VALUE_COUNTS.clone(),
        LOWER_BOUNDS.clone(),
        UPPER_BOUNDS.clone(),
        KEY_METADATA.clone(),
        SPLIT_OFFSETS.clone(),
        SORT_ORDER_ID.clone(),
    ]
}

#[allow(dead_code)]
pub(super) fn data_file_schema_v1(partition_type: &StructType) -> Result<AvroSchema> {
    let schema = Schema::builder()
        .with_fields(data_file_fields_v1(partition_type))
        .build()?;
    schema_to_avro_schema("data_file", &schema)
}

#[allow(dead_code)]
pub(super) fn data_file_schema_v2(partition_type: &StructType) -> Result<AvroSchema> {
    let schema = Schema::builder()
        .with_fields(data_file_fields_v2(partition_type))
        .build()?;
    schema_to_avro_schema("data_file", &schema)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::{Cursor, Read, Write};
    use std::sync::Arc;

    use apache_avro::{from_value, to_value, Reader as AvroReader, Writer as AvroWriter};

    use super::{data_file_schema_v1, data_file_schema_v2, DataFileSerde, *};
    use crate::spec::manifest::_serde::{parse_i64_entry, I64Entry};
    use crate::spec::{
        DataContentType, DataFile, DataFileFormat, FormatVersion, NestedField, PrimitiveType,
        Schema, StructType, Type,
    };

    /// Convert data files to avro bytes and write to writer.
    /// Return the bytes written.
    pub fn write_data_files_to_avro<W: Write>(
        writer: &mut W,
        data_files: impl IntoIterator<Item = DataFile>,
        partition_type: &StructType,
        version: FormatVersion,
    ) -> Result<usize> {
        let avro_schema = match version {
            FormatVersion::V1 => data_file_schema_v1(partition_type).unwrap(),
            FormatVersion::V2 => data_file_schema_v2(partition_type).unwrap(),
        };
        let mut writer = AvroWriter::new(&avro_schema, writer);

        for data_file in data_files {
            let value = to_value(DataFileSerde::try_from(data_file, partition_type, true)?)?
                .resolve(&avro_schema)?;
            writer.append(value)?;
        }

        Ok(writer.flush()?)
    }

    /// Parse data files from avro bytes.
    pub fn read_data_files_from_avro<R: Read>(
        reader: &mut R,
        schema: &Schema,
        partition_type: &StructType,
        version: FormatVersion,
    ) -> Result<Vec<DataFile>> {
        let avro_schema = match version {
            FormatVersion::V1 => data_file_schema_v1(partition_type).unwrap(),
            FormatVersion::V2 => data_file_schema_v2(partition_type).unwrap(),
        };

        let reader = AvroReader::with_schema(&avro_schema, reader)?;
        reader
            .into_iter()
            .map(|value| from_value::<DataFileSerde>(&value?)?.try_into(partition_type, schema))
            .collect::<Result<Vec<_>>>()
    }

    #[test]
    fn test_parse_negative_manifest_entry() {
        let entries = vec![I64Entry { key: 1, value: -1 }, I64Entry {
            key: 2,
            value: 3,
        }];

        let ret = parse_i64_entry(entries).unwrap();

        let expected_ret = HashMap::from([(2, 3)]);
        assert_eq!(ret, expected_ret, "Negative i64 entry should be ignored!");
    }

    #[tokio::test]
    async fn test_data_file_serialize_deserialize() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::optional(
                        1,
                        "v1",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::optional(
                        2,
                        "v2",
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::optional(
                        3,
                        "v3",
                        Type::Primitive(PrimitiveType::String),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let data_files = vec![data_file::DataFile {
            content: DataContentType::Data,
            file_path: "s3://testbucket/iceberg_data/iceberg_ctl/iceberg_db/iceberg_tbl/data/00000-7-45268d71-54eb-476c-b42c-942d880c04a1-00001.parquet".to_string(),
            file_format: DataFileFormat::Parquet,
            partition: Struct::empty(),
            record_count: 1,
            file_size_in_bytes: 875,
            column_sizes: HashMap::from([(1,47),(2,48),(3,52)]),
            value_counts: HashMap::from([(1,1),(2,1),(3,1)]),
            null_value_counts: HashMap::from([(1,0),(2,0),(3,0)]),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::from([(1,Datum::int(1)),(2,Datum::string("a")),(3,Datum::string("AC/DC"))]),
            upper_bounds: HashMap::from([(1,Datum::int(1)),(2,Datum::string("a")),(3,Datum::string("AC/DC"))]),
            key_metadata: None,
            split_offsets: vec![4],
            equality_ids: vec![],
            sort_order_id: Some(0),
        }];

        let mut buffer = Vec::new();
        let _ = write_data_files_to_avro(
            &mut buffer,
            data_files.clone().into_iter(),
            &StructType::new(vec![]),
            FormatVersion::V2,
        )
        .unwrap();

        let actual_data_file = read_data_files_from_avro(
            &mut Cursor::new(buffer),
            &schema,
            &StructType::new(vec![]),
            FormatVersion::V2,
        )
        .unwrap();

        assert_eq!(data_files, actual_data_file);
    }
}
