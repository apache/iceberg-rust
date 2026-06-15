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

//! UTC timestamp coercion for Arrow RecordBatches.
//!
//! Arrow engines may produce timestamps with timezone "UTC" while Iceberg's
//! canonical Arrow schema uses "+00:00". This module handles the lossless cast
//! between UTC-equivalent timezone representations so the parquet writer can
//! accept data from either convention.
//!
//! Uses [`ArrowSchemaVisitor`] to walk the source batch schema and produce a
//! coerced schema where UTC-equivalent timezones are normalized to match the
//! target. This follows the same pattern as [`crate::arrow::int96`].

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_cast::cast;
use arrow_schema::{
    DataType, Field, FieldRef, Fields, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};

use crate::arrow::schema::{ArrowSchemaVisitor, DEFAULT_MAP_FIELD_NAME, visit_schema};
use crate::{Error, ErrorKind, Result};

/// Coerce timestamp columns in `batch` to match `target_schema` when the only
/// difference is a UTC-equivalent timezone alias (e.g. "UTC" vs "+00:00").
pub(crate) fn coerce_timestamp_columns(
    batch: &RecordBatch,
    target_schema: &ArrowSchemaRef,
) -> Result<RecordBatch> {
    if batch.schema() == *target_schema {
        return Ok(batch.clone());
    }

    let mut visitor = TimestampTzCoercionVisitor::new(target_schema);
    let coerced_schema = Arc::new(visit_schema(&batch.schema(), &mut visitor)?);

    if !visitor.changed {
        return Ok(batch.clone());
    }

    let mut cols = batch.columns().to_vec();
    for (idx, (col, target_field)) in cols.clone().iter().zip(coerced_schema.fields()).enumerate() {
        if col.data_type() != target_field.data_type() {
            cols[idx] = cast(col, target_field.data_type())?;
        }
    }

    RecordBatch::try_new(coerced_schema, cols).map_err(|err| {
        Error::new(
            ErrorKind::DataInvalid,
            "Failed to rebuild record batch after casting to target schema.",
        )
        .with_source(err)
    })
}

/// Visitor that walks the source (batch) schema and produces a coerced schema
/// where UTC-equivalent timestamp timezones are normalized to match the target.
///
/// For each primitive field, if the source has `Timestamp(unit, Some(tz))` and the
/// target has the same unit but a different UTC-equivalent timezone, we output
/// the target's timezone in the coerced schema.
struct TimestampTzCoercionVisitor<'a> {
    target_schema: &'a ArrowSchemaRef,
    field_stack: Vec<FieldRef>,
    target_field_stack: Vec<DataType>,
    changed: bool,
}

impl<'a> TimestampTzCoercionVisitor<'a> {
    fn new(target_schema: &'a ArrowSchemaRef) -> Self {
        Self {
            target_schema,
            field_stack: Vec::new(),
            target_field_stack: Vec::new(),
            changed: false,
        }
    }

    fn current_target_type(&self) -> Option<&DataType> {
        self.target_field_stack.last()
    }
}

impl ArrowSchemaVisitor for TimestampTzCoercionVisitor<'_> {
    type T = Field;
    type U = ArrowSchema;

    fn before_field(&mut self, field: &FieldRef) -> Result<()> {
        self.field_stack.push(field.clone());

        let target_type = if self.target_field_stack.is_empty() {
            self.target_schema
                .field_with_name(field.name())
                .ok()
                .map(|f| f.data_type().clone())
        } else {
            match self.target_field_stack.last() {
                Some(DataType::Struct(fields)) => fields
                    .find(field.name())
                    .map(|(_, f)| f.data_type().clone()),
                _ => None,
            }
        };
        self.target_field_stack
            .push(target_type.unwrap_or_else(|| field.data_type().clone()));
        Ok(())
    }

    fn after_field(&mut self, _field: &FieldRef) -> Result<()> {
        self.field_stack.pop();
        self.target_field_stack.pop();
        Ok(())
    }

    fn before_list_element(&mut self, field: &FieldRef) -> Result<()> {
        self.field_stack.push(field.clone());
        let target_type = match self.target_field_stack.last() {
            Some(DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _)) => {
                f.data_type().clone()
            }
            _ => field.data_type().clone(),
        };
        self.target_field_stack.push(target_type);
        Ok(())
    }

    fn after_list_element(&mut self, _field: &FieldRef) -> Result<()> {
        self.field_stack.pop();
        self.target_field_stack.pop();
        Ok(())
    }

    fn before_map_key(&mut self, field: &FieldRef) -> Result<()> {
        self.field_stack.push(field.clone());
        let target_type = match self.target_field_stack.last() {
            Some(DataType::Map(entries, _)) => match entries.data_type() {
                DataType::Struct(fields) if fields.len() == 2 => fields[0].data_type().clone(),
                _ => field.data_type().clone(),
            },
            _ => field.data_type().clone(),
        };
        self.target_field_stack.push(target_type);
        Ok(())
    }

    fn after_map_key(&mut self, _field: &FieldRef) -> Result<()> {
        self.field_stack.pop();
        self.target_field_stack.pop();
        Ok(())
    }

    fn before_map_value(&mut self, field: &FieldRef) -> Result<()> {
        self.field_stack.push(field.clone());
        let target_type = match self.target_field_stack.last() {
            Some(DataType::Map(entries, _)) => match entries.data_type() {
                DataType::Struct(fields) if fields.len() == 2 => fields[1].data_type().clone(),
                _ => field.data_type().clone(),
            },
            _ => field.data_type().clone(),
        };
        self.target_field_stack.push(target_type);
        Ok(())
    }

    fn after_map_value(&mut self, _field: &FieldRef) -> Result<()> {
        self.field_stack.pop();
        self.target_field_stack.pop();
        Ok(())
    }

    fn schema(&mut self, schema: &ArrowSchema, values: Vec<Field>) -> Result<ArrowSchema> {
        Ok(ArrowSchema::new_with_metadata(
            values,
            schema.metadata().clone(),
        ))
    }

    fn r#struct(&mut self, _fields: &Fields, results: Vec<Field>) -> Result<Field> {
        let field_info = self
            .field_stack
            .last()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Field stack underflow in struct"))?;
        Ok(Field::new(
            field_info.name(),
            DataType::Struct(Fields::from(results)),
            field_info.is_nullable(),
        )
        .with_metadata(field_info.metadata().clone()))
    }

    fn list(&mut self, list: &DataType, value: Field) -> Result<Field> {
        let field_info = self
            .field_stack
            .last()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Field stack underflow in list"))?;
        let list_type = match list {
            DataType::List(_) => DataType::List(Arc::new(value)),
            DataType::LargeList(_) => DataType::LargeList(Arc::new(value)),
            DataType::FixedSizeList(_, size) => DataType::FixedSizeList(Arc::new(value), *size),
            _ => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Expected list type, got {list}"),
                ));
            }
        };
        Ok(
            Field::new(field_info.name(), list_type, field_info.is_nullable())
                .with_metadata(field_info.metadata().clone()),
        )
    }

    fn map(&mut self, map: &DataType, key_value: Field, value: Field) -> Result<Field> {
        let field_info = self
            .field_stack
            .last()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Field stack underflow in map"))?;
        let sorted = match map {
            DataType::Map(_, sorted) => *sorted,
            _ => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Expected map type, got {map}"),
                ));
            }
        };
        let struct_field = Field::new(
            DEFAULT_MAP_FIELD_NAME,
            DataType::Struct(Fields::from(vec![key_value, value])),
            false,
        );
        Ok(Field::new(
            field_info.name(),
            DataType::Map(Arc::new(struct_field), sorted),
            field_info.is_nullable(),
        )
        .with_metadata(field_info.metadata().clone()))
    }

    fn primitive(&mut self, p: &DataType) -> Result<Field> {
        let field_info = self.field_stack.last().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "Field stack underflow in primitive")
        })?;

        let target_type = self.current_target_type().cloned();
        let coerced_type = if let (
            DataType::Timestamp(unit, Some(src_tz)),
            Some(DataType::Timestamp(target_unit, Some(target_tz))),
        ) = (p, &target_type)
        {
            if unit == target_unit && src_tz != target_tz && is_utc(src_tz) && is_utc(target_tz) {
                self.changed = true;
                DataType::Timestamp(*unit, Some(target_tz.clone()))
            } else {
                p.clone()
            }
        } else {
            p.clone()
        };

        Ok(
            Field::new(field_info.name(), coerced_type, field_info.is_nullable())
                .with_metadata(field_info.metadata().clone()),
        )
    }
}

fn is_utc(tz: &str) -> bool {
    matches!(tz, "UTC" | "+00:00")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int32Array, ListArray, RecordBatch, StructArray};
    use arrow_schema::{DataType, Field, Fields, TimeUnit};

    use super::*;

    #[test]
    fn test_noop_when_matching() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
            "x",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![
            1, 2, 3,
        ])) as ArrayRef])
        .unwrap();
        let result = coerce_timestamp_columns(&batch, &schema).unwrap();
        assert_eq!(result.schema(), batch.schema());
    }

    #[test]
    fn test_passes_through_non_utc_mismatches() {
        let source_schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
            "x",
            DataType::Int32,
            false,
        )]));
        let target_schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
            "x",
            DataType::Utf8,
            false,
        )]));
        let batch =
            RecordBatch::try_new(source_schema.clone(), vec![
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            ])
            .unwrap();
        let result = coerce_timestamp_columns(&batch, &target_schema).unwrap();
        assert_eq!(result.schema(), source_schema);
    }

    #[test]
    fn test_coerce_utc_to_plus_zero() {
        let source_schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        )]));
        let target_schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            true,
        )]));

        let ts_array = Arc::new(
            arrow_array::TimestampMicrosecondArray::from(vec![
                Some(1_000_000),
                Some(2_000_000),
                Some(3_000_000),
            ])
            .with_timezone("UTC"),
        ) as ArrayRef;

        let batch = RecordBatch::try_new(source_schema, vec![ts_array]).unwrap();
        let result = coerce_timestamp_columns(&batch, &target_schema).unwrap();
        assert_eq!(result.schema(), target_schema);
        assert_eq!(result.num_rows(), 3);
    }

    #[test]
    fn test_coerce_plus_zero_to_utc() {
        let source_schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
            true,
        )]));
        let target_schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        )]));

        let ts_array = Arc::new(
            arrow_array::TimestampNanosecondArray::from(vec![Some(1_000_000)])
                .with_timezone("+00:00"),
        ) as ArrayRef;

        let batch = RecordBatch::try_new(source_schema, vec![ts_array]).unwrap();
        let result = coerce_timestamp_columns(&batch, &target_schema).unwrap();
        assert_eq!(result.schema(), target_schema);
    }

    #[test]
    fn test_no_coerce_different_units() {
        let source_schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        )]));
        let target_schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
            true,
        )]));

        let ts_array = Arc::new(
            arrow_array::TimestampMicrosecondArray::from(vec![Some(1_000_000)])
                .with_timezone("UTC"),
        ) as ArrayRef;

        let batch = RecordBatch::try_new(source_schema.clone(), vec![ts_array]).unwrap();
        let result = coerce_timestamp_columns(&batch, &target_schema).unwrap();
        assert_eq!(result.schema(), source_schema);
    }

    #[test]
    fn test_no_coerce_non_utc_timezone() {
        let source_schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("America/New_York".into())),
            true,
        )]));
        let target_schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            true,
        )]));

        let ts_array = Arc::new(
            arrow_array::TimestampMicrosecondArray::from(vec![Some(1_000_000)])
                .with_timezone("America/New_York"),
        ) as ArrayRef;

        let batch = RecordBatch::try_new(source_schema.clone(), vec![ts_array]).unwrap();
        let result = coerce_timestamp_columns(&batch, &target_schema).unwrap();
        assert_eq!(result.schema(), source_schema);
    }

    #[test]
    fn test_coerce_timestamp_columns_with_struct() {
        let source_struct_field = Field::new(
            "s",
            DataType::Struct(Fields::from(vec![
                Field::new("id", DataType::Int32, false),
                Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                ),
            ])),
            false,
        );
        let target_struct_field = Field::new(
            "s",
            DataType::Struct(Fields::from(vec![
                Field::new("id", DataType::Int32, false),
                Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                    true,
                ),
            ])),
            false,
        );

        let source_schema = Arc::new(arrow_schema::Schema::new(vec![source_struct_field]));
        let target_schema = Arc::new(arrow_schema::Schema::new(vec![target_struct_field]));

        let id_array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let ts_array = Arc::new(
            arrow_array::TimestampMicrosecondArray::from(vec![
                Some(1_000_000),
                Some(2_000_000),
                Some(3_000_000),
            ])
            .with_timezone("UTC"),
        ) as ArrayRef;
        let struct_array = Arc::new(StructArray::from(vec![
            (Arc::new(Field::new("id", DataType::Int32, false)), id_array),
            (
                Arc::new(Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                )),
                ts_array,
            ),
        ])) as ArrayRef;

        let batch = RecordBatch::try_new(source_schema, vec![struct_array]).unwrap();
        let result = coerce_timestamp_columns(&batch, &target_schema).unwrap();

        assert_eq!(result.schema(), target_schema);
        assert_eq!(result.num_rows(), 3);
    }

    #[test]
    fn test_coerce_timestamp_columns_with_list() {
        let source_field = Field::new(
            "ts_list",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ))),
            false,
        );
        let target_field = Field::new(
            "ts_list",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                true,
            ))),
            false,
        );

        let source_schema = Arc::new(arrow_schema::Schema::new(vec![source_field]));
        let target_schema = Arc::new(arrow_schema::Schema::new(vec![target_field]));

        let ts_values = arrow_array::TimestampMicrosecondArray::from(vec![
            Some(1_000_000),
            Some(2_000_000),
            Some(3_000_000),
        ])
        .with_timezone("UTC");
        let offsets = arrow_buffer::OffsetBuffer::from_lengths([2, 1]);
        let list_array = Arc::new(ListArray::new(
            Arc::new(Field::new(
                "item",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            )),
            offsets,
            Arc::new(ts_values),
            None,
        )) as ArrayRef;

        let batch = RecordBatch::try_new(source_schema, vec![list_array]).unwrap();
        let result = coerce_timestamp_columns(&batch, &target_schema).unwrap();

        assert_eq!(result.schema(), target_schema);
        assert_eq!(result.num_rows(), 2);
    }
}
