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

//! Shared helpers for the vortex file format integration.

use vortex::error::VortexError;
use vortex::extension::datetime::TimeUnit;

use crate::{Error, ErrorKind, Result};

/// Converts a [`VortexError`] into an iceberg [`Error`].
pub(crate) fn to_iceberg_error(err: VortexError) -> Error {
    Error::new(ErrorKind::Unexpected, "Vortex error").with_source(err)
}

/// Losslessly converts a temporal value between units; conversions that would
/// lose precision (e.g. microseconds to seconds) are rejected.
pub(crate) fn convert_temporal_value(value: i64, from: TimeUnit, to: TimeUnit) -> Result<i64> {
    fn nanos_per(unit: TimeUnit) -> i64 {
        match unit {
            TimeUnit::Nanoseconds => 1,
            TimeUnit::Microseconds => 1_000,
            TimeUnit::Milliseconds => 1_000_000,
            TimeUnit::Seconds => 1_000_000_000,
            TimeUnit::Days => 86_400_000_000_000,
        }
    }

    let (from_nanos, to_nanos) = (nanos_per(from), nanos_per(to));
    if from_nanos % to_nanos != 0 {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            format!("Lossy temporal unit conversion from {from} to {to}"),
        ));
    }
    value.checked_mul(from_nanos / to_nanos).ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Temporal value {value} overflows when converted from {from} to {to}"),
        )
    })
}

/// Iceberg schema persisted in Vortex's user metadata, including nested field IDs.
pub(crate) const VORTEX_SCHEMA_KEY: &str = "iceberg.schema";

/// Restore Iceberg IDs and canonical UUID annotations while retaining physical Arrow types.
pub(crate) fn attach_field_id_metadata(
    batch: arrow_array::RecordBatch,
    schema: &crate::spec::Schema,
) -> Result<arrow_array::RecordBatch> {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    fn map_field(field: &Field, parent: &str, schema: &crate::spec::Schema) -> Field {
        let path = if parent.is_empty() {
            field.name().clone()
        } else {
            format!("{parent}.{}", field.name())
        };
        let dtype = match field.data_type() {
            DataType::Struct(fields) => DataType::Struct(
                fields
                    .iter()
                    .map(|f| Arc::new(map_field(f, &path, schema)))
                    .collect(),
            ),
            DataType::List(f) => DataType::List(Arc::new(map_field(
                &f.as_ref().clone().with_name("element"),
                &path,
                schema,
            ))),
            DataType::LargeList(f) => DataType::LargeList(Arc::new(map_field(
                &f.as_ref().clone().with_name("element"),
                &path,
                schema,
            ))),
            DataType::FixedSizeList(f, n) => DataType::FixedSizeList(
                Arc::new(map_field(
                    &f.as_ref().clone().with_name("element"),
                    &path,
                    schema,
                )),
                *n,
            ),
            DataType::Map(f, sorted) => {
                // Arrow's entries struct is not an Iceberg field.
                let entry_type = match f.data_type() {
                    DataType::Struct(fields) => DataType::Struct(
                        fields
                            .iter()
                            .map(|f| Arc::new(map_field(f, &path, schema)))
                            .collect(),
                    ),
                    dtype => dtype.clone(),
                };
                DataType::Map(
                    Arc::new(f.as_ref().clone().with_data_type(entry_type)),
                    *sorted,
                )
            }
            dtype => dtype.clone(),
        };
        let mut field = field.clone().with_data_type(dtype);
        if !field.metadata().contains_key(PARQUET_FIELD_ID_META_KEY)
            && let Some(id) = schema.field_id_by_name(&path)
        {
            let mut metadata = field.metadata().clone();
            metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string());
            field = field.with_metadata(metadata);
        }
        if field.data_type() == &DataType::FixedSizeBinary(16)
            && let Some(iceberg_field) = field
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .and_then(|id| id.parse::<i32>().ok())
                .and_then(|id| schema.field_by_id(id))
            && matches!(
                iceberg_field.field_type.as_ref(),
                crate::spec::Type::Primitive(crate::spec::PrimitiveType::Uuid)
            )
        {
            field = field.with_extension_type(arrow_schema::extension::Uuid);
        }
        field
    }
    let fields: Vec<_> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| map_field(f, "", schema))
        .collect();
    let options = arrow_array::RecordBatchOptions::new()
        .with_row_count(Some(batch.num_rows()))
        .with_match_field_names(false);
    Ok(arrow_array::RecordBatch::try_new_with_options(
        Arc::new(ArrowSchema::new(fields)),
        batch.columns().to_vec(),
        &options,
    )?)
}

/// Evolve nested structs by ID before the top-level batch transformer runs.
/// Arrow's generic struct cast cannot insert missing children or resolve renames by ID.
pub(crate) fn evolve_nested_fields(
    batch: arrow_array::RecordBatch,
    schema: &crate::spec::Schema,
    session: &vortex::session::VortexSession,
) -> Result<arrow_array::RecordBatch> {
    use std::sync::Arc;

    use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, StructArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use crate::spec::{Schema, Type};

    fn evolve(
        array: &ArrayRef,
        source: &Field,
        target: &Type,
        session: &vortex::session::VortexSession,
    ) -> Result<ArrayRef> {
        match (source.data_type(), target) {
            (DataType::Struct(source_fields), Type::Struct(target)) => {
                let source_array = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "Expected a struct array"))?;
                let schema = Schema::builder()
                    .with_fields(target.fields().to_vec())
                    .build()?;
                // The batch schema carries the restored IDs; the underlying Vortex
                // StructArray's fields still have its original metadata-free schema.
                let child_batch = RecordBatch::try_new_with_options(
                    Arc::new(ArrowSchema::new(source_fields.clone())),
                    source_array.columns().to_vec(),
                    &RecordBatchOptions::new()
                        .with_row_count(Some(array.len()))
                        .with_match_field_names(false),
                )?;
                let child_batch = evolve_nested_fields(child_batch, &schema, session)?;
                let ids: Vec<_> = target.fields().iter().map(|f| f.id).collect();
                let child_batch =
                    super::record_batch_transformer::RecordBatchTransformerBuilder::new(
                        Arc::new(schema),
                        &ids,
                    )
                    .build()
                    .process_record_batch(child_batch)?;
                Ok(Arc::new(StructArray::try_new_with_length(
                    child_batch.schema().fields().clone(),
                    child_batch.columns().to_vec(),
                    source_array.nulls().cloned(),
                    array.len(),
                )?))
            }
            (
                DataType::List(field)
                | DataType::LargeList(field)
                | DataType::FixedSizeList(field, _),
                Type::List(target),
            ) => {
                let data = array.to_data();
                let child = arrow_array::make_array(data.child_data()[0].clone());
                let child = evolve(&child, field, &target.element_field.field_type, session)?;
                let field = Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(child.data_type().clone()),
                );
                let dtype = match source.data_type() {
                    DataType::List(_) => DataType::List(field),
                    DataType::LargeList(_) => DataType::LargeList(field),
                    DataType::FixedSizeList(_, n) => DataType::FixedSizeList(field, *n),
                    _ => unreachable!(),
                };
                Ok(arrow_array::make_array(
                    data.into_builder()
                        .data_type(dtype)
                        .child_data(vec![child.to_data()])
                        .build()?,
                ))
            }
            (DataType::Map(field, sorted), Type::Map(target)) => {
                let data = array.to_data();
                let child = arrow_array::make_array(data.child_data()[0].clone());
                let target = Type::Struct(crate::spec::StructType::new(vec![
                    target.key_field.clone(),
                    target.value_field.clone(),
                ]));
                let child = evolve(&child, field, &target, session)?;
                let dtype = DataType::Map(
                    Arc::new(
                        field
                            .as_ref()
                            .clone()
                            .with_name("key_value")
                            .with_data_type(child.data_type().clone()),
                    ),
                    *sorted,
                );
                Ok(arrow_array::make_array(
                    data.into_builder()
                        .data_type(dtype)
                        .child_data(vec![child.to_data()])
                        .build()?,
                ))
            }
            (DataType::Struct(_), Type::Variant(_)) => {
                use vortex::array::VortexSessionExecute;
                use vortex::arrow::ArrowSessionExt;
                let target_dtype = super::schema::type_to_arrow_type(target)?;
                if source.data_type() == &target_dtype {
                    return Ok(array.clone());
                }
                // A generic struct cast would discard typed_value in shredded
                // variants. Let Vortex unshred it into Iceberg's metadata/value
                // representation before schema adaptation.
                let source = source
                    .clone()
                    .with_extension_type(super::schema::VariantExtensionType);
                let target = source.clone().with_data_type(target_dtype);
                let array = session
                    .arrow()
                    .from_arrow_array(array.clone(), &source)
                    .map_err(to_iceberg_error)?;
                session
                    .arrow()
                    .execute_arrow(array, Some(&target), &mut session.create_execution_ctx())
                    .map_err(to_iceberg_error)
            }
            (
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
                Type::Primitive(
                    crate::spec::PrimitiveType::Fixed(_) | crate::spec::PrimitiveType::Uuid,
                ),
            ) => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!("Reading Vortex binary as Iceberg {target} is not supported"),
            )),
            _ => Ok(array.clone()),
        }
    }
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
        let id = field
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .and_then(|v| v.parse().ok());
        let array = match id.and_then(|id| schema.field_by_id(id)) {
            Some(target) => evolve(array, field, &target.field_type, session)?,
            None => array.clone(),
        };
        fields.push(
            field
                .as_ref()
                .clone()
                .with_data_type(array.data_type().clone()),
        );
        columns.push(array);
    }
    Ok(RecordBatch::try_new_with_options(
        Arc::new(ArrowSchema::new(fields)),
        columns,
        &RecordBatchOptions::new()
            .with_row_count(Some(batch.num_rows()))
            .with_match_field_names(false),
    )?)
}
