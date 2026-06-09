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

//! Shared `partition_summaries` Arrow builder + [`FieldSummary`] conversion.
//!
//! The `manifests` and `all_manifests` metadata tables both expose a manifest's per-partition-field
//! [`FieldSummary`] list as a nested Arrow `list<struct<contains_null, contains_nan, lower_bound,
//! upper_bound>>` column, where the lower/upper bounds are rendered to human-readable STRINGS via the
//! partition field's primitive type (Java `ManifestsTable.partitionSummariesToRows`). This module is the
//! single home for that builder + conversion so the two tables cannot drift.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, GenericListBuilder, ListBuilder, StringBuilder, StructBuilder,
};
use arrow_schema::{DataType, Field, Fields};

use crate::Result;
use crate::arrow::schema_to_arrow_schema;
use crate::spec::{Datum, FieldSummary, StructType};

/// Builds the empty `partition_summaries` list builder for a metadata table whose Iceberg `schema`
/// carries a `partition_summaries` `list<struct<...>>` column (the `manifests` / `all_manifests` shape).
///
/// Returns the list builder with its `item` struct element field id pinned to `9` (the Java
/// `partition_summaries` element id) so the produced Arrow array carries the right `PARQUET:field_id`.
pub(super) fn partition_summary_builder(
    schema: &crate::spec::Schema,
) -> Result<GenericListBuilder<i32, StructBuilder>> {
    let arrow_schema = schema_to_arrow_schema(schema)?;
    let partition_summary_fields = match arrow_schema
        .field_with_name("partition_summaries")?
        .data_type()
    {
        DataType::List(list_type) => match list_type.data_type() {
            DataType::Struct(fields) => fields.to_vec(),
            _ => unreachable!("partition_summaries element must be a struct"),
        },
        _ => unreachable!("partition_summaries must be a list"),
    };

    let builder = ListBuilder::new(StructBuilder::from_fields(
        Fields::from(partition_summary_fields.clone()),
        0,
    ))
    .with_field(Arc::new(
        Field::new_struct("item", partition_summary_fields, false).with_metadata(HashMap::from([
            ("PARQUET:field_id".to_string(), "9".to_string()),
        ])),
    ));

    Ok(builder)
}

/// Appends one manifest's [`FieldSummary`] list as a single list element to `builder`, rendering each
/// summary's lower/upper bound bytes to a human-readable string via the corresponding partition field's
/// primitive type (Java `ManifestsTable.partitionSummariesToRows`).
///
/// `partition_struct` is the manifest's partition-spec struct type; its fields align positionally with
/// `partitions`. An empty `partitions` slice appends an empty (but non-null) list element.
pub(super) fn append_partition_summaries(
    builder: &mut GenericListBuilder<i32, StructBuilder>,
    partitions: &[FieldSummary],
    partition_struct: &StructType,
) -> Result<()> {
    let values = builder.values();
    for (summary, field) in partitions.iter().zip(partition_struct.fields()) {
        let primitive_type = field
            .field_type
            .as_primitive_type()
            .ok_or_else(|| {
                crate::Error::new(
                    crate::ErrorKind::DataInvalid,
                    format!(
                        "partition field {} must have a primitive type to render its bounds",
                        field.name
                    ),
                )
            })?
            .clone();

        values
            .field_builder::<BooleanBuilder>(0)
            .expect("contains_null builder is a BooleanBuilder")
            .append_value(summary.contains_null);
        values
            .field_builder::<BooleanBuilder>(1)
            .expect("contains_nan builder is a BooleanBuilder")
            .append_option(summary.contains_nan);

        let lower_bound = bound_to_string(
            summary.lower_bound.as_ref().map(|b| b.as_ref()),
            &primitive_type,
        )?;
        values
            .field_builder::<StringBuilder>(2)
            .expect("lower_bound builder is a StringBuilder")
            .append_option(lower_bound);

        let upper_bound = bound_to_string(
            summary.upper_bound.as_ref().map(|b| b.as_ref()),
            &primitive_type,
        )?;
        values
            .field_builder::<StringBuilder>(3)
            .expect("upper_bound builder is a StringBuilder")
            .append_option(upper_bound);

        values.append(true);
    }
    builder.append(true);
    Ok(())
}

/// Renders a single optional bound's serialized bytes to a human-readable string via `primitive_type`,
/// returning `None` when the bound is absent (Java renders an absent bound as a null cell).
///
/// Uses [`Datum::to_human_string`] — NOT `Datum::to_string` — to match Java
/// `ManifestsTable.partitionSummariesToRows`, which renders each bound via
/// `Transform.toHumanString(type, value)`. For a STRING-typed partition bound, Java's `toHumanString`
/// default case calls `Object.toString()`, yielding the RAW string (e.g. `a`); `Datum::to_string` would
/// instead JSON-quote it (`"a"`). The two agree for every non-string primitive, so this only affects
/// string partition bounds — the interop A2 `manifests` fixture (partitioned by identity(category), a
/// string) is what surfaced the divergence.
fn bound_to_string(
    bound: Option<&[u8]>,
    primitive_type: &crate::spec::PrimitiveType,
) -> Result<Option<String>> {
    match bound {
        None => Ok(None),
        Some(bytes) => {
            let datum = Datum::try_from_bytes(bytes, primitive_type.clone())?;
            Ok(Some(datum.to_human_string()))
        }
    }
}
