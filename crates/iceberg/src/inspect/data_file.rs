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

//! The shared `data_file` projection used by the `files` family AND the `entries` metadata table.
//!
//! Both expose the same column set of an Iceberg [`crate::spec::DataFile`] — content, file path/format,
//! partition, record/size counts, the metrics maps, the list columns, and the V3 deletion-vector fields —
//! mirroring Java `DataFile.getType(partitionType).fields()` (field ids from `api/DataFile.java`). They
//! differ ONLY in shape:
//!
//! - [`crate::inspect::FilesTable`] FLATTENS the projection to top-level columns (Java `BaseFilesTable`,
//!   whose schema IS `DataFile.getType(...)`).
//! - [`crate::inspect::EntriesTable`] NESTS the projection under a single `data_file` STRUCT column (Java
//!   `BaseEntriesTable` / `ManifestEntry.wrapFileSchema`).
//!
//! This module is the single source of truth for the field list ([`data_file_fields`]) and the row
//! builder ([`DataFileStructBuilder`]) so the two tables cannot drift — the Rule of Three (2nd use).
//!
//! The raw metrics maps (`column_sizes`/`value_counts`/…/`lower_bounds`/`upper_bounds`) are part of this
//! projection. The separate `readable_metrics` virtual STRUCT column (Java
//! `MetricsUtil.readableMetricsStruct` — the per-column typed/human-readable view of those metrics) is
//! built by [`crate::inspect::readable_metrics`] and appended alongside this projection by both tables.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::StructArray;
use arrow_array::builder::{
    ArrayBuilder, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int32Builder, Int64Builder, LargeBinaryBuilder, ListBuilder, MapBuilder,
    StringBuilder, StructBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
    TimestampNanosecondBuilder,
};
use arrow_schema::Fields;

use crate::spec::{
    Datum, ListType, Literal, MapType, NestedField, NestedFieldRef, PrimitiveLiteral,
    PrimitiveType, StructType, Type,
};
use crate::{Error, ErrorKind, Result};

/// The boxed `MapBuilder` shape `StructBuilder::from_fields` produces for a `DataType::Map` child (its
/// key/value field metadata is preserved by `make_builder`, so we only supply the values).
type DynMapBuilder = MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>;
/// The boxed `ListBuilder` shape `StructBuilder::from_fields` produces for a `DataType::List` child.
type DynListBuilder = ListBuilder<Box<dyn ArrayBuilder>>;

/// The 21 `data_file` columns, mirroring Java `DataFile.getType(partitionType).fields()` — the
/// canonical `DataFile` field ids from `api/DataFile.java`. The `partition` column carries the table's
/// DEFAULT partition type. `readable_metrics` is deferred.
///
/// `files` uses these directly as its top-level columns; `entries` nests them under a `data_file` struct.
pub(super) fn data_file_fields(partition_type: &StructType) -> Vec<NestedFieldRef> {
    vec![
        Arc::new(NestedField::optional(
            134,
            "content",
            Type::Primitive(PrimitiveType::Int),
        )),
        Arc::new(NestedField::required(
            100,
            "file_path",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::required(
            101,
            "file_format",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            141,
            "spec_id",
            Type::Primitive(PrimitiveType::Int),
        )),
        Arc::new(NestedField::required(
            102,
            "partition",
            Type::Struct(partition_type.clone()),
        )),
        Arc::new(NestedField::required(
            103,
            "record_count",
            Type::Primitive(PrimitiveType::Long),
        )),
        Arc::new(NestedField::required(
            104,
            "file_size_in_bytes",
            Type::Primitive(PrimitiveType::Long),
        )),
        Arc::new(NestedField::optional(
            108,
            "column_sizes",
            int_long_map(117, 118),
        )),
        Arc::new(NestedField::optional(
            109,
            "value_counts",
            int_long_map(119, 120),
        )),
        Arc::new(NestedField::optional(
            110,
            "null_value_counts",
            int_long_map(121, 122),
        )),
        Arc::new(NestedField::optional(
            137,
            "nan_value_counts",
            int_long_map(138, 139),
        )),
        Arc::new(NestedField::optional(
            125,
            "lower_bounds",
            int_binary_map(126, 127),
        )),
        Arc::new(NestedField::optional(
            128,
            "upper_bounds",
            int_binary_map(129, 130),
        )),
        Arc::new(NestedField::optional(
            131,
            "key_metadata",
            Type::Primitive(PrimitiveType::Binary),
        )),
        Arc::new(NestedField::optional(132, "split_offsets", long_list(133))),
        Arc::new(NestedField::optional(135, "equality_ids", int_list(136))),
        Arc::new(NestedField::optional(
            140,
            "sort_order_id",
            Type::Primitive(PrimitiveType::Int),
        )),
        Arc::new(NestedField::optional(
            142,
            "first_row_id",
            Type::Primitive(PrimitiveType::Long),
        )),
        Arc::new(NestedField::optional(
            143,
            "referenced_data_file",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            144,
            "content_offset",
            Type::Primitive(PrimitiveType::Long),
        )),
        Arc::new(NestedField::optional(
            145,
            "content_size_in_bytes",
            Type::Primitive(PrimitiveType::Long),
        )),
    ]
}

/// Iceberg `map<int, long>` with the given key/value field ids (the metrics-count maps).
fn int_long_map(key_id: i32, value_id: i32) -> Type {
    Type::Map(MapType {
        key_field: Arc::new(NestedField::map_key_element(
            key_id,
            Type::Primitive(PrimitiveType::Int),
        )),
        value_field: Arc::new(NestedField::map_value_element(
            value_id,
            Type::Primitive(PrimitiveType::Long),
            true,
        )),
    })
}

/// Iceberg `map<int, binary>` with the given key/value field ids (the lower/upper-bound maps).
fn int_binary_map(key_id: i32, value_id: i32) -> Type {
    Type::Map(MapType {
        key_field: Arc::new(NestedField::map_key_element(
            key_id,
            Type::Primitive(PrimitiveType::Int),
        )),
        value_field: Arc::new(NestedField::map_value_element(
            value_id,
            Type::Primitive(PrimitiveType::Binary),
            true,
        )),
    })
}

/// Iceberg `list<long>` (required element) with the given element field id (split offsets).
fn long_list(element_id: i32) -> Type {
    Type::List(ListType {
        element_field: Arc::new(NestedField::list_element(
            element_id,
            Type::Primitive(PrimitiveType::Long),
            true,
        )),
    })
}

/// Iceberg `list<int>` (required element) with the given element field id (equality ids).
fn int_list(element_id: i32) -> Type {
    Type::List(ListType {
        element_field: Arc::new(NestedField::list_element(
            element_id,
            Type::Primitive(PrimitiveType::Int),
            true,
        )),
    })
}

/// Accumulates `data_file` rows into a single Arrow [`StructBuilder`] (one child per `DataFile` column).
///
/// The struct's child fields are the Arrow conversion of [`data_file_fields`] — so the produced
/// [`StructArray`] is exactly the `data_file` STRUCT the `entries` table nests, and its `.columns()` are
/// exactly the top-level columns the `files` table flattens. One builder, both shapes.
pub(super) struct DataFileStructBuilder<'a> {
    partition_type: &'a StructType,
    builder: StructBuilder,
}

impl<'a> DataFileStructBuilder<'a> {
    /// Creates a builder over the given Arrow `data_file` struct fields (the converted [`data_file_fields`])
    /// and the table's DEFAULT partition type (used to dispatch the partition tuple's per-field types).
    pub(super) fn new(data_file_arrow_fields: &Fields, partition_type: &'a StructType) -> Self {
        Self {
            partition_type,
            builder: StructBuilder::from_fields(data_file_arrow_fields.clone(), 0),
        }
    }

    /// Appends one row built from a [`crate::spec::DataFile`].
    pub(super) fn append(&mut self, data_file: &crate::spec::DataFile) -> Result<()> {
        let b = &mut self.builder;

        struct_child::<Int32Builder>(b, 0)?.append_value(data_file.content_type() as i32);
        struct_child::<StringBuilder>(b, 1)?.append_value(data_file.file_path());
        // Java's `FilesTable`/`ManifestEntriesTable` render `file_format` as the UPPERCASE `FileFormat`
        // enum NAME (`PARQUET`/`AVRO`/`ORC`) via `format.toString()`. `DataFileFormat`'s `Display` is
        // lowercase (the on-disk manifest string), so upper-case ONLY here in the inspection projection to
        // match Java exactly — the on-disk write path (Display/serde) is unchanged.
        struct_child::<StringBuilder>(b, 2)?
            .append_value(data_file.file_format().to_string().to_uppercase());
        struct_child::<Int32Builder>(b, 3)?.append_value(data_file.partition_spec_id);

        let partition_builder = struct_child::<StructBuilder>(b, 4)?;
        append_partition(
            partition_builder,
            self.partition_type,
            data_file.partition(),
        )?;

        struct_child::<Int64Builder>(b, 5)?.append_value(data_file.record_count() as i64);
        struct_child::<Int64Builder>(b, 6)?.append_value(data_file.file_size_in_bytes() as i64);

        append_count_map(
            struct_child::<DynMapBuilder>(b, 7)?,
            data_file.column_sizes(),
        )?;
        append_count_map(
            struct_child::<DynMapBuilder>(b, 8)?,
            data_file.value_counts(),
        )?;
        append_count_map(
            struct_child::<DynMapBuilder>(b, 9)?,
            data_file.null_value_counts(),
        )?;
        append_count_map(
            struct_child::<DynMapBuilder>(b, 10)?,
            data_file.nan_value_counts(),
        )?;
        append_bound_map(
            struct_child::<DynMapBuilder>(b, 11)?,
            data_file.lower_bounds(),
        )?;
        append_bound_map(
            struct_child::<DynMapBuilder>(b, 12)?,
            data_file.upper_bounds(),
        )?;

        struct_child::<LargeBinaryBuilder>(b, 13)?.append_option(data_file.key_metadata());

        append_i64_list(
            struct_child::<DynListBuilder>(b, 14)?,
            data_file.split_offsets(),
        )?;
        append_i32_list(
            struct_child::<DynListBuilder>(b, 15)?,
            data_file.equality_ids().as_deref(),
        )?;

        struct_child::<Int32Builder>(b, 16)?.append_option(data_file.sort_order_id());
        struct_child::<Int64Builder>(b, 17)?.append_option(data_file.first_row_id());
        struct_child::<StringBuilder>(b, 18)?.append_option(data_file.referenced_data_file());
        struct_child::<Int64Builder>(b, 19)?.append_option(data_file.content_offset());
        struct_child::<Int64Builder>(b, 20)?.append_option(data_file.content_size_in_bytes());

        // The struct itself is always present (a row is never a null data_file).
        b.append(true);
        Ok(())
    }

    /// Appends a NULL `data_file` struct row (every child gets a null, the struct slot is null). Unused by
    /// `files`/`entries` today (a manifest entry always carries a data_file) but kept for completeness so a
    /// future optional-struct caller cannot misuse the builder.
    #[allow(dead_code)]
    pub(super) fn append_null(&mut self) {
        self.builder.append_null();
    }

    /// Finishes into a single [`StructArray`] — the `data_file` column for `entries`, or (via
    /// [`StructArray::columns`]) the flattened top-level columns for `files`.
    pub(super) fn finish(mut self) -> StructArray {
        self.builder.finish()
    }
}

/// Looks up a typed child builder of a [`StructBuilder`] by index, erroring (never panicking) if the
/// child at that index is not the expected type — a programming-error guard, since the struct fields are
/// constructed from [`data_file_fields`] in this same module.
fn struct_child<T: arrow_array::builder::ArrayBuilder>(
    builder: &mut StructBuilder,
    index: usize,
) -> Result<&mut T> {
    builder.field_builder::<T>(index).ok_or_else(|| {
        Error::new(
            ErrorKind::Unexpected,
            format!("data_file struct child builder at index {index} has an unexpected type"),
        )
    })
}

/// Downcasts a boxed (`Box<dyn ArrayBuilder>`) inner builder to a concrete type, erroring rather than
/// panicking — a programming-error guard, since the builder shapes come from this module's field list.
fn dyn_child<'a, T: ArrayBuilder>(
    builder: &'a mut Box<dyn ArrayBuilder>,
    what: &str,
) -> Result<&'a mut T> {
    builder.as_any_mut().downcast_mut::<T>().ok_or_else(|| {
        Error::new(
            ErrorKind::Unexpected,
            format!("data_file {what} builder has an unexpected inner type"),
        )
    })
}

/// Appends a `map<int, long>` value (one of the metrics-count maps), keys sorted for determinism. The map
/// builder is the boxed shape `make_builder` produces, so we downcast its key/value inner builders.
fn append_count_map(builder: &mut DynMapBuilder, map: &HashMap<i32, u64>) -> Result<()> {
    let mut keys: Vec<&i32> = map.keys().collect();
    keys.sort_unstable();
    for key in keys {
        dyn_child::<Int32Builder>(builder.keys(), "count map key")?.append_value(*key);
        dyn_child::<Int64Builder>(builder.values(), "count map value")?
            .append_value(map[key] as i64);
    }
    builder.append(true)?;
    Ok(())
}

/// Appends a `map<int, binary>` value (lower/upper bounds), keys sorted; values are the raw serialized
/// single-value bytes (Java map<int, binary>).
fn append_bound_map(builder: &mut DynMapBuilder, map: &HashMap<i32, Datum>) -> Result<()> {
    let mut keys: Vec<&i32> = map.keys().collect();
    keys.sort_unstable();
    for key in keys {
        dyn_child::<Int32Builder>(builder.keys(), "bound map key")?.append_value(*key);
        dyn_child::<LargeBinaryBuilder>(builder.values(), "bound map value")?
            .append_value(map[key].to_bytes()?);
    }
    builder.append(true)?;
    Ok(())
}

/// Appends an optional `list<long>` value (split offsets).
fn append_i64_list(builder: &mut DynListBuilder, values: Option<&[i64]>) -> Result<()> {
    match values {
        Some(values) => {
            let inner = dyn_child::<Int64Builder>(builder.values(), "split_offsets element")?;
            for value in values {
                inner.append_value(*value);
            }
            builder.append(true);
        }
        None => builder.append(false),
    }
    Ok(())
}

/// Appends an optional `list<int>` value (equality ids).
fn append_i32_list(builder: &mut DynListBuilder, values: Option<&[i32]>) -> Result<()> {
    match values {
        Some(values) => {
            let inner = dyn_child::<Int32Builder>(builder.values(), "equality_ids element")?;
            for value in values {
                inner.append_value(*value);
            }
            builder.append(true);
        }
        None => builder.append(false),
    }
    Ok(())
}

/// Appends one partition tuple to the partition [`StructBuilder`], dispatching each field on its
/// primitive type. The partition `Struct`'s values are aligned with `partition_type`'s fields.
///
/// Shared in-module helper: `files`/`entries` reach it through [`DataFileStructBuilder::append`], and
/// the `partitions` aggregating table reuses it directly for its `partition` column (Rule of Three).
pub(super) fn append_partition(
    builder: &mut StructBuilder,
    partition_type: &StructType,
    partition: &crate::spec::Struct,
) -> Result<()> {
    for (index, field) in partition_type.fields().iter().enumerate() {
        let primitive_type = field.field_type.as_primitive_type().ok_or_else(|| {
            Error::new(
                ErrorKind::FeatureUnsupported,
                format!(
                    "partition field '{}' has non-primitive type {:?}; not supported in the data_file metadata projection",
                    field.name, field.field_type
                ),
            )
        })?;
        let value = partition
            .fields()
            .get(index)
            .and_then(|value| value.as_ref());
        append_partition_field(builder, index, primitive_type, value)?;
    }
    builder.append(true);
    Ok(())
}

/// Appends a single partition-field value (or null) to the struct child builder at `index`, dispatching
/// on the field's primitive type. Mirrors the Arrow types produced by `type_to_arrow_type`.
fn append_partition_field(
    builder: &mut StructBuilder,
    index: usize,
    primitive_type: &PrimitiveType,
    value: Option<&Literal>,
) -> Result<()> {
    let primitive = match value {
        Some(Literal::Primitive(primitive)) => Some(primitive),
        Some(other) => {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!("non-primitive partition literal {other:?} is not supported"),
            ));
        }
        None => None,
    };

    macro_rules! append_typed {
        ($builder_ty:ty, $extract:expr) => {{
            let child = builder.field_builder::<$builder_ty>(index).ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("partition child builder at index {index} has an unexpected type"),
                )
            })?;
            match primitive {
                Some(primitive) => child.append_value($extract(primitive)?),
                None => child.append_null(),
            }
        }};
    }

    match primitive_type {
        PrimitiveType::Boolean => append_typed!(BooleanBuilder, extract_bool),
        PrimitiveType::Int => append_typed!(Int32Builder, extract_i32),
        PrimitiveType::Long => append_typed!(Int64Builder, extract_i64),
        PrimitiveType::Float => append_typed!(Float32Builder, extract_f32),
        PrimitiveType::Double => append_typed!(Float64Builder, extract_f64),
        PrimitiveType::Date => append_typed!(Date32Builder, extract_i32),
        PrimitiveType::Time => append_typed!(Time64MicrosecondBuilder, extract_i64),
        PrimitiveType::Timestamp => append_typed!(TimestampMicrosecondBuilder, extract_i64),
        PrimitiveType::TimestampNs => {
            append_typed!(TimestampNanosecondBuilder, extract_i64)
        }
        PrimitiveType::String => append_typed!(StringBuilder, extract_string),
        PrimitiveType::Binary => append_typed!(BinaryBuilder, extract_binary),
        PrimitiveType::Decimal { .. } => append_typed!(Decimal128Builder, extract_i128),
        other => {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!(
                    "partition field type {other:?} is not supported in the data_file metadata projection"
                ),
            ));
        }
    }
    Ok(())
}

fn type_mismatch(primitive: &PrimitiveLiteral) -> Error {
    Error::new(
        ErrorKind::DataInvalid,
        format!("partition literal {primitive:?} does not match its partition field type"),
    )
}

fn extract_bool(primitive: &PrimitiveLiteral) -> Result<bool> {
    match primitive {
        PrimitiveLiteral::Boolean(value) => Ok(*value),
        other => Err(type_mismatch(other)),
    }
}

fn extract_i32(primitive: &PrimitiveLiteral) -> Result<i32> {
    match primitive {
        PrimitiveLiteral::Int(value) => Ok(*value),
        other => Err(type_mismatch(other)),
    }
}

fn extract_i64(primitive: &PrimitiveLiteral) -> Result<i64> {
    match primitive {
        PrimitiveLiteral::Long(value) => Ok(*value),
        other => Err(type_mismatch(other)),
    }
}

fn extract_f32(primitive: &PrimitiveLiteral) -> Result<f32> {
    match primitive {
        PrimitiveLiteral::Float(value) => Ok(value.into_inner()),
        other => Err(type_mismatch(other)),
    }
}

fn extract_f64(primitive: &PrimitiveLiteral) -> Result<f64> {
    match primitive {
        PrimitiveLiteral::Double(value) => Ok(value.into_inner()),
        other => Err(type_mismatch(other)),
    }
}

fn extract_string(primitive: &PrimitiveLiteral) -> Result<&str> {
    match primitive {
        PrimitiveLiteral::String(value) => Ok(value.as_str()),
        other => Err(type_mismatch(other)),
    }
}

fn extract_binary(primitive: &PrimitiveLiteral) -> Result<&[u8]> {
    match primitive {
        PrimitiveLiteral::Binary(value) => Ok(value.as_slice()),
        other => Err(type_mismatch(other)),
    }
}

fn extract_i128(primitive: &PrimitiveLiteral) -> Result<i128> {
    match primitive {
        PrimitiveLiteral::Int128(value) => Ok(*value),
        other => Err(type_mismatch(other)),
    }
}
