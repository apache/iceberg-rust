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

//! INT96 timestamp coercion for Parquet files.

use std::sync::Arc;

use arrow_schema::{
    DataType, Field, Fields, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef, TimeUnit,
};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::arrow::schema::{ArrowSchemaVisitor, DEFAULT_MAP_FIELD_NAME, visit_schema};
use crate::error::Result;
use crate::spec::{PrimitiveType, Schema, Type};
use crate::{Error, ErrorKind};

/// Coerce Arrow schema types for INT96 columns to match the Iceberg table schema.
///
/// arrow-rs defaults INT96 to `Timestamp(Nanosecond)`, which overflows i64 for dates outside
/// ~1677-2262. We use arrow-rs's schema hint mechanism to read INT96 at the resolution
/// specified by the Iceberg schema (`timestamp` → microsecond, `timestamp_ns` → nanosecond).
///
/// Iceberg Java handles this differently: it bypasses parquet-mr with a custom column reader
/// (`GenericParquetReaders.TimestampInt96Reader`). We achieve the same result via schema hints.
///
/// References:
/// - Iceberg spec primitive types: <https://iceberg.apache.org/spec/#primitive-types>
/// - arrow-rs schema hint support: <https://github.com/apache/arrow-rs/pull/7285>
pub(crate) fn coerce_int96_timestamps(
    arrow_schema: &ArrowSchemaRef,
    iceberg_schema: &Schema,
) -> Option<Arc<ArrowSchema>> {
    let mut visitor = Int96CoercionVisitor::new(iceberg_schema);
    let coerced = visit_schema(arrow_schema, &mut visitor).ok()?;
    if visitor.changed {
        Some(Arc::new(coerced))
    } else {
        None
    }
}

/// Visitor that coerces `Timestamp(Nanosecond)` Arrow fields to the resolution
/// indicated by the Iceberg schema.
struct Int96CoercionVisitor<'a> {
    iceberg_schema: &'a Schema,
    // TODO(#2310): use FieldRef (Arc<Field>) once ArrowSchemaVisitor passes FieldRef.
    field_stack: Vec<Field>,
    changed: bool,
}

impl<'a> Int96CoercionVisitor<'a> {
    fn new(iceberg_schema: &'a Schema) -> Self {
        Self {
            iceberg_schema,
            field_stack: Vec::new(),
            changed: false,
        }
    }

    /// Determine the target TimeUnit for a Timestamp(Nanosecond) field based on the
    /// Iceberg schema. Falls back to microsecond when field IDs are unavailable,
    /// matching Iceberg Java behavior.
    fn target_unit(&self, field: &Field) -> Option<TimeUnit> {
        if !matches!(
            field.data_type(),
            DataType::Timestamp(TimeUnit::Nanosecond, _)
        ) {
            return None;
        }

        let target = field
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .and_then(|id_str| id_str.parse::<i32>().ok())
            .and_then(|field_id| self.iceberg_schema.field_by_id(field_id))
            .and_then(|f| match &*f.field_type {
                Type::Primitive(PrimitiveType::Timestamp | PrimitiveType::Timestamptz) => {
                    Some(TimeUnit::Microsecond)
                }
                Type::Primitive(PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs) => {
                    Some(TimeUnit::Nanosecond)
                }
                _ => None,
            })
            // Iceberg Java reads INT96 as microseconds by default
            .unwrap_or(TimeUnit::Microsecond);

        if target == TimeUnit::Nanosecond {
            None
        } else {
            Some(target)
        }
    }
}

impl ArrowSchemaVisitor for Int96CoercionVisitor<'_> {
    type T = Field;
    type U = ArrowSchema;

    fn before_field(&mut self, field: &Field) -> Result<()> {
        self.field_stack.push(field.as_ref().clone());
        Ok(())
    }

    fn after_field(&mut self, _field: &Field) -> Result<()> {
        self.field_stack.pop();
        Ok(())
    }

    fn before_list_element(&mut self, field: &Field) -> Result<()> {
        self.field_stack.push(field.as_ref().clone());
        Ok(())
    }

    fn after_list_element(&mut self, _field: &Field) -> Result<()> {
        self.field_stack.pop();
        Ok(())
    }

    fn before_map_key(&mut self, field: &Field) -> Result<()> {
        self.field_stack.push(field.as_ref().clone());
        Ok(())
    }

    fn after_map_key(&mut self, _field: &Field) -> Result<()> {
        self.field_stack.pop();
        Ok(())
    }

    fn before_map_value(&mut self, field: &Field) -> Result<()> {
        self.field_stack.push(field.as_ref().clone());
        Ok(())
    }

    fn after_map_value(&mut self, _field: &Field) -> Result<()> {
        self.field_stack.pop();
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

        if let Some(target_unit) = self.target_unit(field_info) {
            let tz = match field_info.data_type() {
                DataType::Timestamp(_, tz) => tz.clone(),
                _ => None,
            };
            self.changed = true;
            Ok(Field::new(
                field_info.name(),
                DataType::Timestamp(target_unit, tz),
                field_info.is_nullable(),
            )
            .with_metadata(field_info.metadata().clone()))
        } else {
            Ok(
                Field::new(field_info.name(), p.clone(), field_info.is_nullable())
                    .with_metadata(field_info.metadata().clone()),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use super::coerce_int96_timestamps;
    use crate::spec::{ListType, MapType, NestedField, PrimitiveType, Schema, StructType, Type};

    fn iceberg_schema_with_timestamp() -> Schema {
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::optional(1, "ts", Type::Primitive(PrimitiveType::Timestamp)).into(),
                NestedField::required(2, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap()
    }

    fn field_id_meta(id: i32) -> HashMap<String, String> {
        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())])
    }

    #[test]
    fn test_coerce_timestamp_ns_to_us() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true)
                .with_metadata(field_id_meta(1)),
            Field::new("id", DataType::Int32, false).with_metadata(field_id_meta(2)),
        ]));
        let iceberg = iceberg_schema_with_timestamp();

        let coerced = coerce_int96_timestamps(&arrow_schema, &iceberg).unwrap();
        assert_eq!(
            coerced.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        // Non-timestamp field unchanged
        assert_eq!(coerced.field(1).data_type(), &DataType::Int32);
    }

    #[test]
    fn test_coerce_timestamptz_ns_to_us() {
        let iceberg = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::optional(1, "ts", Type::Primitive(PrimitiveType::Timestamptz)).into(),
            ])
            .build()
            .unwrap();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            )
            .with_metadata(field_id_meta(1)),
        ]));

        let coerced = coerce_int96_timestamps(&arrow_schema, &iceberg).unwrap();
        assert_eq!(
            coerced.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn test_no_coercion_when_iceberg_is_timestamp_ns() {
        let iceberg = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::optional(1, "ts", Type::Primitive(PrimitiveType::TimestampNs)).into(),
            ])
            .build()
            .unwrap();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true)
                .with_metadata(field_id_meta(1)),
        ]));

        assert!(coerce_int96_timestamps(&arrow_schema, &iceberg).is_none());
    }

    #[test]
    fn test_no_coercion_when_iceberg_is_timestamptz_ns() {
        let iceberg = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::optional(1, "ts", Type::Primitive(PrimitiveType::TimestamptzNs))
                    .into(),
            ])
            .build()
            .unwrap();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            )
            .with_metadata(field_id_meta(1)),
        ]));

        assert!(coerce_int96_timestamps(&arrow_schema, &iceberg).is_none());
    }

    #[test]
    fn test_no_coercion_when_already_microsecond() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true)
                .with_metadata(field_id_meta(1)),
            Field::new("id", DataType::Int32, false).with_metadata(field_id_meta(2)),
        ]));
        let iceberg = iceberg_schema_with_timestamp();

        assert!(coerce_int96_timestamps(&arrow_schema, &iceberg).is_none());
    }

    // Without field IDs, the visitor can't look up the Iceberg type and falls back
    // to microsecond to match Iceberg Java behavior.
    #[test]
    fn test_defaults_to_us_without_field_ids() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]));
        let iceberg = iceberg_schema_with_timestamp();

        let coerced = coerce_int96_timestamps(&arrow_schema, &iceberg).unwrap();
        assert_eq!(
            coerced.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    // Field ID exists but points to a non-timestamp Iceberg type. The field_by_id
    // lookup succeeds but the match arm returns None, so unwrap_or falls back to
    // microsecond.
    #[test]
    fn test_defaults_to_us_when_iceberg_type_is_not_timestamp() {
        let iceberg = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::optional(1, "ts", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true)
                .with_metadata(field_id_meta(1)),
        ]));

        let coerced = coerce_int96_timestamps(&arrow_schema, &iceberg).unwrap();
        assert_eq!(
            coerced.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn test_coerce_preserves_field_metadata() {
        let mut meta = field_id_meta(1);
        meta.insert("custom_key".to_string(), "custom_value".to_string());

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true)
                .with_metadata(meta.clone()),
        ]));
        let iceberg = iceberg_schema_with_timestamp();

        let coerced = coerce_int96_timestamps(&arrow_schema, &iceberg).unwrap();
        assert_eq!(coerced.field(0).metadata(), &meta);
    }

    #[test]
    fn test_coerce_timestamp_in_struct() {
        let iceberg = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(
                    1,
                    "data",
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(2, "ts", Type::Primitive(PrimitiveType::Timestamp))
                            .into(),
                    ])),
                )
                .into(),
            ])
            .build()
            .unwrap();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "data",
                DataType::Struct(
                    vec![
                        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true)
                            .with_metadata(field_id_meta(2)),
                    ]
                    .into(),
                ),
                false,
            )
            .with_metadata(field_id_meta(1)),
        ]));

        let coerced = coerce_int96_timestamps(&arrow_schema, &iceberg).unwrap();
        let inner = match coerced.field(0).data_type() {
            DataType::Struct(fields) => fields,
            other => panic!("Expected Struct, got {other}"),
        };
        assert_eq!(
            inner[0].data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn test_coerce_timestamp_in_list() {
        let iceberg = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::optional(
                    1,
                    "timestamps",
                    Type::List(ListType {
                        element_field: NestedField::optional(
                            2,
                            "element",
                            Type::Primitive(PrimitiveType::Timestamp),
                        )
                        .into(),
                    }),
                )
                .into(),
            ])
            .build()
            .unwrap();

        let element_field = Field::new(
            "element",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )
        .with_metadata(field_id_meta(2));
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("timestamps", DataType::List(Arc::new(element_field)), true)
                .with_metadata(field_id_meta(1)),
        ]));

        let coerced = coerce_int96_timestamps(&arrow_schema, &iceberg).unwrap();
        let element_dt = match coerced.field(0).data_type() {
            DataType::List(f) => f.data_type(),
            other => panic!("Expected List, got {other}"),
        };
        assert_eq!(
            element_dt,
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn test_coerce_timestamp_in_map_value() {
        let iceberg = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::optional(
                    1,
                    "ts_map",
                    Type::Map(MapType {
                        key_field: NestedField::required(
                            2,
                            "key",
                            Type::Primitive(PrimitiveType::String),
                        )
                        .into(),
                        value_field: NestedField::optional(
                            3,
                            "value",
                            Type::Primitive(PrimitiveType::Timestamp),
                        )
                        .into(),
                    }),
                )
                .into(),
            ])
            .build()
            .unwrap();

        let key_field = Field::new("key", DataType::Utf8, false).with_metadata(field_id_meta(2));
        let value_field = Field::new(
            "value",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )
        .with_metadata(field_id_meta(3));
        let entries_field = Field::new(
            "key_value",
            DataType::Struct(vec![key_field, value_field].into()),
            false,
        );
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "ts_map",
                DataType::Map(Arc::new(entries_field), false),
                true,
            )
            .with_metadata(field_id_meta(1)),
        ]));

        let coerced = coerce_int96_timestamps(&arrow_schema, &iceberg).unwrap();
        let value_dt = match coerced.field(0).data_type() {
            DataType::Map(entries, _) => match entries.data_type() {
                DataType::Struct(fields) => fields[1].data_type().clone(),
                other => panic!("Expected Struct inside Map, got {other}"),
            },
            other => panic!("Expected Map, got {other}"),
        };
        assert_eq!(value_dt, DataType::Timestamp(TimeUnit::Microsecond, None));
    }
}
