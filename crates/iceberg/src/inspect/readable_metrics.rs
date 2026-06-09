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

//! The `readable_metrics` virtual column shared by the `files` family and the `entries` metadata table.
//!
//! `readable_metrics` (Java `MetricsUtil.READABLE_METRICS` / `MetricsUtil.readableMetricsStruct`) is a
//! STRUCT with ONE sub-field per LEAF (primitive-typed) column of the DATA table's current schema. Each
//! sub-field is itself a struct of six human-readable per-column metrics:
//!
//! | sub-field          | type           | source (Java `READABLE_METRIC_COLS`, `MetricsUtil.java:140-193`) |
//! |--------------------|----------------|------------------------------------------------------------------|
//! | `column_size`      | long, opt      | `DataFile.column_sizes[fieldId]`                                 |
//! | `value_count`      | long, opt      | `DataFile.value_counts[fieldId]`                                 |
//! | `null_value_count` | long, opt      | `DataFile.null_value_counts[fieldId]`                            |
//! | `nan_value_count`  | long, opt      | `DataFile.nan_value_counts[fieldId]`                             |
//! | `lower_bound`      | COLUMN type, opt | `Conversions.fromByteBuffer(field.type(), lower_bounds[fieldId])` |
//! | `upper_bound`      | COLUMN type, opt | `Conversions.fromByteBuffer(field.type(), upper_bounds[fieldId])` |
//!
//! The four counts are read from the file's metric maps BY FIELD ID and are null when the file does not
//! carry that metric. The lower/upper bounds are the file's bound for that column DECODED to the COLUMN's
//! own type — NOT the raw bytes the `lower_bounds`/`upper_bounds` map columns expose. This is the inverse of
//! the raw map: the raw column serializes the typed bound to bytes (`Datum::to_bytes`); `readable_metrics`
//! re-decodes those bytes against the COLUMN's primitive type (`Datum::try_from_bytes`), exactly mirroring
//! Java `Conversions.fromByteBuffer(field.type(), buffer)` — so an evolved field (e.g. an `int`-encoded
//! bound on a column promoted to `long`) is widened to the column's current type, not left as the bound's
//! stored type.
//!
//! ## Field-id scheme (Java `readableMetricsSchema`, `MetricsUtil.java:356-393`)
//!
//! Java assigns ids from a counter seeded at the host metadata table's `highestFieldId()` and
//! pre-increments (`AtomicInteger.incrementAndGet()`): for each leaf column it assigns the column-struct
//! id, then the six metric sub-field ids, and finally the top-level `readable_metrics` id. This port
//! reproduces that pre-increment counter EXACTLY.
//!
//! **One documented divergence — iteration order.** Java iterates `dataTableSchema.idToName().keySet()`,
//! which is Java-`HashMap` order: arbitrary (bucket-dependent) and not portable across JVMs. This port
//! iterates leaf columns in ASCENDING data-table field-id order, which is deterministic. Java then SORTS
//! the emitted column sub-fields by name (`fields.sort(Comparator.comparing(NestedField::name))`) AFTER id
//! assignment, so a column's id depends on the (arbitrary) iteration order, not its name; this port sorts
//! by name too, but its ids follow the deterministic ascending-field-id assignment. The emitted column
//! ORDER (by name) therefore matches Java; the exact interior ids match Java only when Java's HashMap order
//! happens to coincide with ascending field-id order. Full byte-level id parity with a specific JVM is a
//! Java/Spark-interop concern tracked separately; this scheme is the deterministic, self-consistent port.
//!
//! References:
//! - <https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/MetricsUtil.java>
//! - `BaseFilesTable` / `BaseEntriesTable` append it via `TypeUtil.join(schema, readableMetricsSchema(...))`.

use std::sync::Arc;

use arrow_array::StructArray;
use arrow_array::builder::{
    BooleanBuilder, Date32Builder, Decimal128Builder, FixedSizeBinaryBuilder, Float32Builder,
    Float64Builder, Int32Builder, Int64Builder, LargeBinaryBuilder, StringBuilder, StructBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder, TimestampNanosecondBuilder,
};
use arrow_schema::Fields;

use crate::spec::{
    Datum, NestedField, NestedFieldRef, PrimitiveLiteral, PrimitiveType, Schema, StructType, Type,
};
use crate::{Error, ErrorKind, Result};

/// The fixed top-level field name of the virtual column (Java `MetricsUtil.READABLE_METRICS`).
pub(super) const READABLE_METRICS: &str = "readable_metrics";

/// One leaf column of the data table that gets a `readable_metrics` sub-struct: its data-table field id
/// (the key into the file's metric maps), its dotted name, and its primitive type (used to type + decode
/// the bounds). Sorted by name for the emitted struct order (Java sorts the fields by name).
struct LeafColumn {
    field_id: i32,
    dotted_name: String,
    primitive_type: PrimitiveType,
}

/// Collects the data table's LEAF (primitive-typed) columns — every field id whose type is primitive,
/// including primitives nested inside structs/lists/maps — keyed by dotted name. Mirrors Java's
/// `idToName().keySet()` filtered by `findField(id).type().isPrimitiveType()`.
///
/// Returned sorted by name, which is the order Java emits the `readable_metrics` sub-fields in.
fn leaf_columns(data_schema: &Schema) -> Vec<LeafColumn> {
    let mut columns: Vec<LeafColumn> = data_schema
        .field_id_to_name_map()
        .iter()
        .filter_map(|(field_id, dotted_name)| {
            let field = data_schema.field_by_id(*field_id)?;
            let primitive_type = field.field_type.as_primitive_type()?.clone();
            Some(LeafColumn {
                field_id: *field_id,
                dotted_name: dotted_name.clone(),
                primitive_type,
            })
        })
        .collect();
    // Java assigns ids in idToName() (HashMap) order; we assign in ASCENDING field-id order for
    // determinism (see the module docs). The emitted sub-fields are then sorted by name (as Java does),
    // which `build_field` applies after id assignment.
    columns.sort_by_key(|column| column.field_id);
    columns
}

/// The six readable-metric sub-field names, in Java `READABLE_METRIC_COLS` order. The first four are
/// `long`; the last two (`lower_bound` / `upper_bound`) carry the column's own type.
const COLUMN_SIZE: &str = "column_size";
const VALUE_COUNT: &str = "value_count";
const NULL_VALUE_COUNT: &str = "null_value_count";
const NAN_VALUE_COUNT: &str = "nan_value_count";
const LOWER_BOUND: &str = "lower_bound";
const UPPER_BOUND: &str = "upper_bound";

/// Builds the `readable_metrics` STRUCT field appended to the `files` family / `entries` schema.
///
/// `metadata_highest_field_id` is the host metadata table's highest field id (Java's
/// `metadataTableSchema.highestFieldId()`) — the seed for the pre-increment id counter. The returned field
/// is the top-level `readable_metrics` struct; an empty data schema (no primitive columns) yields an empty
/// struct, matching Java (which would emit an empty `readable_metrics` struct).
pub(super) fn readable_metrics_field(
    data_schema: &Schema,
    metadata_highest_field_id: i32,
) -> NestedFieldRef {
    let columns = leaf_columns(data_schema);

    // Pre-increment counter seeded at the host table's highest id (Java `AtomicInteger(highestFieldId)`
    // then `incrementAndGet()`), so the first id assigned is `highest + 1`.
    let mut next_id = metadata_highest_field_id;
    let mut next = || {
        next_id += 1;
        next_id
    };

    let mut column_fields: Vec<NestedFieldRef> = Vec::with_capacity(columns.len());
    for column in &columns {
        // Java assigns the column-struct id FIRST, then its six sub-field ids.
        let column_struct_id = next();
        let sub_fields = vec![
            Arc::new(NestedField::optional(
                next(),
                COLUMN_SIZE,
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                next(),
                VALUE_COUNT,
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                next(),
                NULL_VALUE_COUNT,
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                next(),
                NAN_VALUE_COUNT,
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                next(),
                LOWER_BOUND,
                Type::Primitive(column.primitive_type.clone()),
            )),
            Arc::new(NestedField::optional(
                next(),
                UPPER_BOUND,
                Type::Primitive(column.primitive_type.clone()),
            )),
        ];
        column_fields.push(Arc::new(
            NestedField::optional(
                column_struct_id,
                &column.dotted_name,
                Type::Struct(StructType::new(sub_fields)),
            )
            .with_doc(format!("Metrics for column {}", column.dotted_name)),
        ));
    }

    // The data-table field-id order already sorts the ids ascending; the emitted sub-fields are sorted by
    // name (Java `fields.sort(...)` by name). The id counter has already advanced past all columns; the
    // top-level `readable_metrics` field takes the next id.
    column_fields.sort_by(|left, right| left.name.cmp(&right.name));

    Arc::new(
        NestedField::optional(
            next(),
            READABLE_METRICS,
            Type::Struct(StructType::new(column_fields)),
        )
        .with_doc("Column metrics in readable form"),
    )
}

/// Accumulates `readable_metrics` rows into a single Arrow [`StructBuilder`] whose children are the
/// per-column metric structs (in the same name order as [`readable_metrics_field`] emitted them).
///
/// Shared by `files` (which flattens nothing — `readable_metrics` is one of its top-level columns) and
/// `entries` (which appends it as a top-level column beside `data_file`).
pub(super) struct ReadableMetricsBuilder {
    /// The leaf columns in the SAME order as the Arrow struct's child fields (sorted by name).
    columns: Vec<LeafColumn>,
    builder: StructBuilder,
}

impl ReadableMetricsBuilder {
    /// Creates a builder over the `readable_metrics` Arrow struct fields (the converted
    /// [`readable_metrics_field`]) and the data table's current schema (to recover each leaf column's id +
    /// type, keyed by the Arrow child name = the column's dotted name).
    pub(super) fn try_new(
        readable_metrics_arrow_fields: &Fields,
        data_schema: &Schema,
    ) -> Result<Self> {
        // The Arrow child fields are the per-column structs, in name order. Re-resolve each back to its
        // data-table leaf (id + primitive type) by dotted name so the row builder stays aligned with the
        // schema even though both are derived from `leaf_columns`.
        let mut columns = Vec::with_capacity(readable_metrics_arrow_fields.len());
        for arrow_field in readable_metrics_arrow_fields {
            let dotted_name = arrow_field.name();
            let field_id = data_schema.field_id_by_name(dotted_name).ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("readable_metrics column '{dotted_name}' is not in the data schema"),
                )
            })?;
            let primitive_type = data_schema
                .field_by_id(field_id)
                .and_then(|field| field.field_type.as_primitive_type())
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        format!("readable_metrics column '{dotted_name}' is not primitive-typed"),
                    )
                })?
                .clone();
            columns.push(LeafColumn {
                field_id,
                dotted_name: dotted_name.clone(),
                primitive_type,
            });
        }

        Ok(Self {
            builder: StructBuilder::from_fields(readable_metrics_arrow_fields.clone(), 0),
            columns,
        })
    }

    /// Appends one `readable_metrics` row built from a [`crate::spec::DataFile`]: for every leaf column,
    /// the four counts (by field id, null when absent) and the lower/upper bound decoded to the column's
    /// type (null when absent).
    pub(super) fn append(&mut self, data_file: &crate::spec::DataFile) -> Result<()> {
        for (index, column) in self.columns.iter().enumerate() {
            let column_builder = self
                .builder
                .field_builder::<StructBuilder>(index)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "readable_metrics child builder for column '{}' has an unexpected type",
                            column.dotted_name
                        ),
                    )
                })?;
            append_column_metrics(column_builder, column, data_file)?;
        }
        // The readable_metrics struct itself is always present.
        self.builder.append(true);
        Ok(())
    }

    /// Finishes into the single `readable_metrics` [`StructArray`] column.
    pub(super) fn finish(mut self) -> StructArray {
        self.builder.finish()
    }
}

/// Appends one column's six metric values to its per-column [`StructBuilder`] (sub-field order =
/// [`readable_metrics_field`]'s: the four counts then lower/upper bound).
fn append_column_metrics(
    builder: &mut StructBuilder,
    column: &LeafColumn,
    data_file: &crate::spec::DataFile,
) -> Result<()> {
    append_count(builder, 0, data_file.column_sizes().get(&column.field_id))?;
    append_count(builder, 1, data_file.value_counts().get(&column.field_id))?;
    append_count(
        builder,
        2,
        data_file.null_value_counts().get(&column.field_id),
    )?;
    append_count(
        builder,
        3,
        data_file.nan_value_counts().get(&column.field_id),
    )?;
    append_bound(
        builder,
        4,
        column,
        data_file.lower_bounds().get(&column.field_id),
    )?;
    append_bound(
        builder,
        5,
        column,
        data_file.upper_bounds().get(&column.field_id),
    )?;
    builder.append(true);
    Ok(())
}

/// Appends an optional `long` count (one of the four count metrics) to the sub-field at `index`.
fn append_count(builder: &mut StructBuilder, index: usize, value: Option<&u64>) -> Result<()> {
    let child = builder
        .field_builder::<Int64Builder>(index)
        .ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                format!("readable_metrics count sub-field at index {index} has an unexpected type"),
            )
        })?;
    child.append_option(value.map(|value| *value as i64));
    Ok(())
}

/// Appends an optional lower/upper bound to the sub-field at `index`, DECODED to the column's own type.
///
/// The stored bound (a `Datum` carrying its own type) is re-serialized and re-decoded against the COLUMN's
/// primitive type — the exact inverse of the raw `lower_bounds`/`upper_bounds` map columns, mirroring Java
/// `Conversions.fromByteBuffer(field.type(), buffer)`. An absent bound appends a null of the column type.
fn append_bound(
    builder: &mut StructBuilder,
    index: usize,
    column: &LeafColumn,
    stored: Option<&Datum>,
) -> Result<()> {
    let decoded = match stored {
        Some(datum) => {
            let bytes = datum.to_bytes()?;
            Some(Datum::try_from_bytes(
                &bytes,
                column.primitive_type.clone(),
            )?)
        }
        None => None,
    };
    append_typed_bound(builder, index, &column.primitive_type, decoded.as_ref())
}

/// Appends a single decoded bound value (or a null) to the sub-field at `index`, dispatching on the
/// column's primitive type — mirroring the Arrow type the bound sub-field was declared with.
fn append_typed_bound(
    builder: &mut StructBuilder,
    index: usize,
    primitive_type: &PrimitiveType,
    bound: Option<&Datum>,
) -> Result<()> {
    let literal = bound.map(Datum::literal);

    macro_rules! append_typed {
        ($builder_ty:ty, $extract:expr) => {{
            let child = builder.field_builder::<$builder_ty>(index).ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "readable_metrics bound sub-field at index {index} has an unexpected type"
                    ),
                )
            })?;
            match literal {
                Some(literal) => child.append_value($extract(literal)?),
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
        PrimitiveType::Timestamptz => append_typed!(TimestampMicrosecondBuilder, extract_i64),
        PrimitiveType::TimestampNs => append_typed!(TimestampNanosecondBuilder, extract_i64),
        PrimitiveType::TimestamptzNs => append_typed!(TimestampNanosecondBuilder, extract_i64),
        PrimitiveType::String => append_typed!(StringBuilder, extract_string),
        // `binary` maps to Arrow LargeBinary (not Binary) per `type_to_arrow_type`, so `make_builder`
        // produces a `LargeBinaryBuilder`.
        PrimitiveType::Binary => append_typed!(LargeBinaryBuilder, extract_binary),
        PrimitiveType::Decimal { .. } => append_typed!(Decimal128Builder, extract_i128),
        // `fixed[n]` maps to Arrow FixedSizeBinary(n); its builder append is fallible (width must match).
        PrimitiveType::Fixed(_) => {
            let child = builder
                .field_builder::<FixedSizeBinaryBuilder>(index)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "readable_metrics fixed bound sub-field at index {index} has an unexpected type"
                        ),
                    )
                })?;
            match literal {
                Some(literal) => child.append_value(extract_binary(literal)?)?,
                None => child.append_null(),
            }
        }
        PrimitiveType::Uuid => {
            // UUID renders as 16 fixed bytes (Arrow FixedSizeBinary(16) per `type_to_arrow_type`).
            let child = builder
                .field_builder::<FixedSizeBinaryBuilder>(index)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "readable_metrics uuid bound sub-field at index {index} has an unexpected type"
                        ),
                    )
                })?;
            match literal {
                Some(PrimitiveLiteral::UInt128(value)) => {
                    child.append_value(value.to_be_bytes())?
                }
                Some(other) => return Err(bound_type_mismatch(other)),
                None => child.append_null(),
            }
        }
    }
    Ok(())
}

fn bound_type_mismatch(literal: &PrimitiveLiteral) -> Error {
    Error::new(
        ErrorKind::DataInvalid,
        format!("readable_metrics bound literal {literal:?} does not match its column type"),
    )
}

fn extract_bool(literal: &PrimitiveLiteral) -> Result<bool> {
    match literal {
        PrimitiveLiteral::Boolean(value) => Ok(*value),
        other => Err(bound_type_mismatch(other)),
    }
}

fn extract_i32(literal: &PrimitiveLiteral) -> Result<i32> {
    match literal {
        PrimitiveLiteral::Int(value) => Ok(*value),
        other => Err(bound_type_mismatch(other)),
    }
}

fn extract_i64(literal: &PrimitiveLiteral) -> Result<i64> {
    match literal {
        PrimitiveLiteral::Long(value) => Ok(*value),
        other => Err(bound_type_mismatch(other)),
    }
}

fn extract_f32(literal: &PrimitiveLiteral) -> Result<f32> {
    match literal {
        PrimitiveLiteral::Float(value) => Ok(value.into_inner()),
        other => Err(bound_type_mismatch(other)),
    }
}

fn extract_f64(literal: &PrimitiveLiteral) -> Result<f64> {
    match literal {
        PrimitiveLiteral::Double(value) => Ok(value.into_inner()),
        other => Err(bound_type_mismatch(other)),
    }
}

fn extract_string(literal: &PrimitiveLiteral) -> Result<&str> {
    match literal {
        PrimitiveLiteral::String(value) => Ok(value.as_str()),
        other => Err(bound_type_mismatch(other)),
    }
}

fn extract_binary(literal: &PrimitiveLiteral) -> Result<&[u8]> {
    match literal {
        PrimitiveLiteral::Binary(value) => Ok(value.as_slice()),
        other => Err(bound_type_mismatch(other)),
    }
}

fn extract_i128(literal: &PrimitiveLiteral) -> Result<i128> {
    match literal {
        PrimitiveLiteral::Int128(value) => Ok(*value),
        other => Err(bound_type_mismatch(other)),
    }
}

/// Extracts the Arrow child `Fields` of a `readable_metrics` struct column from a converted metadata-table
/// Arrow schema (used by `files`/`entries` to build the [`ReadableMetricsBuilder`]).
pub(super) fn readable_metrics_struct_fields(
    arrow_schema: &arrow_schema::Schema,
) -> Result<Fields> {
    match arrow_schema.field_with_name(READABLE_METRICS)?.data_type() {
        arrow_schema::DataType::Struct(fields) => Ok(fields.clone()),
        other => Err(Error::new(
            ErrorKind::Unexpected,
            format!("readable_metrics column must be a struct, got {other:?}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::spec::{ListType, MapType, NestedField, PrimitiveType, Schema, Type};

    /// A small data schema with one column of each of several primitive types, plus a nested struct and a
    /// list-of-primitive, to exercise leaf detection + dotted naming + the per-column bound type.
    fn data_schema() -> Schema {
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "name",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    3,
                    "score",
                    Type::Primitive(PrimitiveType::Double),
                )),
            ])
            .build()
            .unwrap()
    }

    #[test]
    fn test_readable_metrics_one_struct_per_leaf_column_sorted_by_name() {
        // RISK: wrong column set / order — `readable_metrics` must have exactly one sub-field per LEAF
        // (primitive) column, named by its dotted path, emitted in NAME order (Java sorts by name).
        let field = readable_metrics_field(&data_schema(), 100);
        let Type::Struct(struct_type) = &*field.field_type else {
            panic!("readable_metrics must be a struct");
        };
        let names: Vec<&str> = struct_type
            .fields()
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        // Sorted by name: id, name, score.
        assert_eq!(names, vec!["id", "name", "score"]);
        assert_eq!(field.name, "readable_metrics");
    }

    #[test]
    fn test_readable_metrics_each_column_is_a_six_field_metric_struct() {
        // RISK: wrong sub-struct shape — each per-column struct must have exactly the 6 Java metric
        // sub-fields in Java order.
        let field = readable_metrics_field(&data_schema(), 100);
        let Type::Struct(struct_type) = &*field.field_type else {
            panic!("struct");
        };
        let id_column = struct_type
            .fields()
            .iter()
            .find(|f| f.name == "id")
            .unwrap();
        let Type::Struct(metrics) = &*id_column.field_type else {
            panic!("per-column metric must be a struct");
        };
        let sub_names: Vec<&str> = metrics.fields().iter().map(|f| f.name.as_str()).collect();
        assert_eq!(sub_names, vec![
            "column_size",
            "value_count",
            "null_value_count",
            "nan_value_count",
            "lower_bound",
            "upper_bound",
        ]);
    }

    #[test]
    fn test_readable_metrics_lower_upper_bound_carry_the_column_type() {
        // RISK (the key typed-bound property): the lower/upper bound sub-fields must carry the COLUMN's
        // OWN type (long for `id`, string for `name`, double for `score`), NOT binary like the raw map.
        let field = readable_metrics_field(&data_schema(), 100);
        let Type::Struct(struct_type) = &*field.field_type else {
            panic!("struct");
        };
        let bound_types = |column: &str| -> (Type, Type) {
            let col = struct_type
                .fields()
                .iter()
                .find(|f| f.name == column)
                .unwrap();
            let Type::Struct(metrics) = &*col.field_type else {
                panic!("struct");
            };
            let lower = (*metrics
                .fields()
                .iter()
                .find(|f| f.name == "lower_bound")
                .unwrap()
                .field_type)
                .clone();
            let upper = (*metrics
                .fields()
                .iter()
                .find(|f| f.name == "upper_bound")
                .unwrap()
                .field_type)
                .clone();
            (lower, upper)
        };
        assert_eq!(
            bound_types("id"),
            (
                Type::Primitive(PrimitiveType::Long),
                Type::Primitive(PrimitiveType::Long)
            )
        );
        assert_eq!(
            bound_types("name"),
            (
                Type::Primitive(PrimitiveType::String),
                Type::Primitive(PrimitiveType::String)
            )
        );
        assert_eq!(
            bound_types("score"),
            (
                Type::Primitive(PrimitiveType::Double),
                Type::Primitive(PrimitiveType::Double)
            )
        );
        // The four count sub-fields are always long regardless of column type.
        let id_col = struct_type
            .fields()
            .iter()
            .find(|f| f.name == "id")
            .unwrap();
        let Type::Struct(metrics) = &*id_col.field_type else {
            panic!("struct");
        };
        for count in [
            "column_size",
            "value_count",
            "null_value_count",
            "nan_value_count",
        ] {
            let sub = metrics.fields().iter().find(|f| f.name == count).unwrap();
            assert_eq!(*sub.field_type, Type::Primitive(PrimitiveType::Long));
        }
    }

    #[test]
    fn test_readable_metrics_field_id_scheme_matches_java_pre_increment_counter() {
        // RISK (the load-bearing interop property): the field ids must follow Java
        // `readableMetricsSchema`'s pre-increment counter seeded at the host metadata table's highest id.
        // With a seed of 100 and leaf columns assigned in ASCENDING data-table-field-id order
        // (id=1, name=2, score=3):
        //   id     struct=101, [column_size..upper_bound]=102..107
        //   name   struct=108, 109..114
        //   score  struct=115, 116..121
        //   top-level readable_metrics = 122
        // The emitted column order is by NAME (id, name, score) but the ids follow the assignment order.
        let field = readable_metrics_field(&data_schema(), 100);
        assert_eq!(field.id, 122, "top-level readable_metrics id");

        let Type::Struct(struct_type) = &*field.field_type else {
            panic!("struct");
        };
        let column = |name: &str| {
            struct_type
                .fields()
                .iter()
                .find(|f| f.name == name)
                .unwrap()
        };

        assert_eq!(column("id").id, 101);
        assert_eq!(column("name").id, 108);
        assert_eq!(column("score").id, 115);

        // The six sub-field ids of the FIRST-assigned column (`id`) are 102..=107 in Java order.
        let Type::Struct(id_metrics) = &*column("id").field_type else {
            panic!("struct");
        };
        let sub_ids: Vec<i32> = id_metrics.fields().iter().map(|f| f.id).collect();
        assert_eq!(sub_ids, vec![102, 103, 104, 105, 106, 107]);

        // The six sub-field ids of `score` (last assigned) are 116..=121.
        let Type::Struct(score_metrics) = &*column("score").field_type else {
            panic!("struct");
        };
        let score_sub_ids: Vec<i32> = score_metrics.fields().iter().map(|f| f.id).collect();
        assert_eq!(score_sub_ids, vec![116, 117, 118, 119, 120, 121]);
    }

    #[test]
    fn test_readable_metrics_includes_primitive_inside_struct_and_list() {
        // RISK: leaf detection — Java includes EVERY primitive field id (incl. primitives nested inside a
        // struct or a list element), keyed by dotted path. A struct-only or top-level-only filter would
        // drop `payload.flag` and `tags.element`.
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "payload",
                    Type::Struct(crate::spec::StructType::new(vec![Arc::new(
                        NestedField::optional(3, "flag", Type::Primitive(PrimitiveType::Boolean)),
                    )])),
                )),
                Arc::new(NestedField::optional(
                    4,
                    "tags",
                    Type::List(ListType {
                        element_field: Arc::new(NestedField::list_element(
                            5,
                            Type::Primitive(PrimitiveType::String),
                            true,
                        )),
                    }),
                )),
            ])
            .build()
            .unwrap();
        let field = readable_metrics_field(&schema, 100);
        let Type::Struct(struct_type) = &*field.field_type else {
            panic!("struct");
        };
        let names: Vec<&str> = struct_type
            .fields()
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        // Sorted by dotted name. The map/struct containers themselves are NOT leaves; their primitive
        // descendants ARE (Java keys by dotted path).
        assert_eq!(names, vec!["id", "payload.flag", "tags.element"]);
    }

    #[test]
    fn test_readable_metrics_complex_only_columns_are_excluded() {
        // RISK: a non-primitive (map) column with no primitive descendants beyond key/value must not
        // produce a struct of its own; only its primitive key/value leaves do.
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "m",
                Type::Map(MapType {
                    key_field: Arc::new(NestedField::map_key_element(
                        2,
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    value_field: Arc::new(NestedField::map_value_element(
                        3,
                        Type::Primitive(PrimitiveType::Long),
                        true,
                    )),
                }),
            ))])
            .build()
            .unwrap();
        let field = readable_metrics_field(&schema, 50);
        let Type::Struct(struct_type) = &*field.field_type else {
            panic!("struct");
        };
        let names: Vec<&str> = struct_type
            .fields()
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        // The map container `m` is NOT a leaf; its primitive key + value ARE.
        assert_eq!(names, vec!["m.key", "m.value"]);
    }

    #[test]
    fn test_readable_metrics_builder_decodes_string_and_binary_bounds() {
        // RISK (a type the fixture-based files/entries tests don't cover): a STRING column's bound decodes
        // to Utf8 and a BINARY column's bound decodes to LargeBinary (NOT plain Binary — `binary` maps to
        // Arrow LargeBinary, so the builder must downcast a `LargeBinaryBuilder`). A wrong builder choice
        // ("unexpected type") or a binary→Binary mismatch would panic/err here.
        use arrow_array::Array;
        use arrow_array::cast::AsArray;

        use crate::arrow::schema_to_arrow_schema;
        use crate::spec::{DataContentType, DataFileBuilder, DataFileFormat, Datum, Struct};

        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                Arc::new(NestedField::optional(
                    1,
                    "name",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "blob",
                    Type::Primitive(PrimitiveType::Binary),
                )),
            ])
            .build()
            .unwrap();

        // Build the readable_metrics-only Arrow fields (host highest id = 10).
        let field = readable_metrics_field(&schema, 10);
        let one_field_schema = Schema::builder().with_fields(vec![field]).build().unwrap();
        let arrow = schema_to_arrow_schema(&one_field_schema).unwrap();
        let arrow_fields = readable_metrics_struct_fields(&arrow).unwrap();

        // A DataFile carrying a string lower bound on `name` and a binary lower bound on `blob`.
        let data_file = DataFileBuilder::default()
            .partition_spec_id(0)
            .content(DataContentType::Data)
            .file_path("x".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(1)
            .record_count(1)
            .partition(Struct::empty())
            .lower_bounds(std::collections::HashMap::from([
                (1, Datum::string("alpha")),
                (2, Datum::binary(vec![1u8, 2u8, 3u8])),
            ]))
            .build()
            .unwrap();

        let mut builder = ReadableMetricsBuilder::try_new(&arrow_fields, &schema).unwrap();
        builder.append(&data_file).unwrap();
        let array = builder.finish();

        // `name` lower_bound decodes to the string "alpha".
        let name_struct = array.column_by_name("name").unwrap().as_struct();
        let name_lower = name_struct
            .column_by_name("lower_bound")
            .unwrap()
            .as_string::<i32>();
        assert_eq!(name_lower.value(0), "alpha");

        // `blob` lower_bound decodes to LargeBinary [1,2,3] (the binary→LargeBinary mapping).
        let blob_struct = array.column_by_name("blob").unwrap().as_struct();
        let blob_lower = blob_struct
            .column_by_name("lower_bound")
            .unwrap()
            .as_binary::<i64>();
        assert!(!blob_lower.is_null(0), "blob.lower_bound must be present");
        assert_eq!(blob_lower.value(0), &[1u8, 2u8, 3u8]);
        // No upper bound was set → null.
        let blob_upper = blob_struct
            .column_by_name("upper_bound")
            .unwrap()
            .as_binary::<i64>();
        assert!(blob_upper.is_null(0), "blob.upper_bound must be NULL");
    }

    #[test]
    fn test_readable_metrics_builder_routes_each_metric_to_its_own_sub_field() {
        // RISK (each of the six metric SOURCES must feed its OWN sub-field): the fixture-based files/entries
        // value tests leave several counts null, so a swap between two NULL-in-fixture count sources (e.g.
        // null_value_count ↔ nan_value_count) survives them. Here every one of the six metrics carries a
        // DISTINCT non-null value on the same column, so any cross-wiring of a metric source to the wrong
        // sub-field (column_size/value_count/null_value_count/nan_value_count/lower_bound/upper_bound) is
        // observable: each sub-field must report exactly its own source value.
        use arrow_array::cast::AsArray;
        use arrow_array::types::Int64Type;

        use crate::arrow::schema_to_arrow_schema;
        use crate::spec::{DataContentType, DataFileBuilder, DataFileFormat, Datum, Struct};

        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "n",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();

        let field = readable_metrics_field(&schema, 10);
        let one_field_schema = Schema::builder().with_fields(vec![field]).build().unwrap();
        let arrow = schema_to_arrow_schema(&one_field_schema).unwrap();
        let arrow_fields = readable_metrics_struct_fields(&arrow).unwrap();

        // Six distinct values, one per metric source, all on column id 1.
        let data_file = DataFileBuilder::default()
            .partition_spec_id(0)
            .content(DataContentType::Data)
            .file_path("x".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(1)
            .record_count(1)
            .partition(Struct::empty())
            .column_sizes(std::collections::HashMap::from([(1, 11u64)]))
            .value_counts(std::collections::HashMap::from([(1, 22u64)]))
            .null_value_counts(std::collections::HashMap::from([(1, 33u64)]))
            .nan_value_counts(std::collections::HashMap::from([(1, 44u64)]))
            .lower_bounds(std::collections::HashMap::from([(1, Datum::long(55))]))
            .upper_bounds(std::collections::HashMap::from([(1, Datum::long(66))]))
            .build()
            .unwrap();

        let mut builder = ReadableMetricsBuilder::try_new(&arrow_fields, &schema).unwrap();
        builder.append(&data_file).unwrap();
        let array = builder.finish();

        let metrics = array.column_by_name("n").unwrap().as_struct();
        let count = |name: &str| {
            metrics
                .column_by_name(name)
                .unwrap()
                .as_primitive::<Int64Type>()
                .value(0)
        };
        assert_eq!(count("column_size"), 11, "column_size source");
        assert_eq!(count("value_count"), 22, "value_count source");
        assert_eq!(count("null_value_count"), 33, "null_value_count source");
        assert_eq!(count("nan_value_count"), 44, "nan_value_count source");
        assert_eq!(count("lower_bound"), 55, "lower_bound source");
        assert_eq!(count("upper_bound"), 66, "upper_bound source");
    }
}
