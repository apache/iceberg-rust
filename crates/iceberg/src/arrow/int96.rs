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
    DataType, Field, FieldRef, Fields, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef, TimeUnit,
};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::basic::Type as PhysicalType;
use parquet::schema::types::{ColumnDescPtr, SchemaDescriptor};

use crate::arrow::schema::{ArrowSchemaVisitor, UTC_TIME_ZONE, visit_schema};
use crate::error::Result;
use crate::spec::{PrimitiveType, Schema, Type};
use crate::{Error, ErrorKind};

/// Coerce Arrow schema types for INT96 columns to match the Iceberg table schema.
///
/// arrow-rs defaults INT96 to `Timestamp(Nanosecond)`, which overflows i64 for dates outside
/// ~1677-2262. We use arrow-rs's schema hint mechanism to read INT96 at the resolution
/// specified by the Iceberg schema (`timestamp` → microsecond, `timestamp_ns` → nanosecond).
/// The Arrow schema must contain all physical columns in file order, before projection.
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
    parquet_schema: &SchemaDescriptor,
) -> Option<Arc<ArrowSchema>> {
    if !parquet_schema
        .columns()
        .iter()
        .any(|column| column.physical_type() == PhysicalType::INT96)
    {
        return None;
    }
    let mut visitor = Int96CoercionVisitor::new(iceberg_schema, parquet_schema);
    let coerced = visit_schema(arrow_schema, &mut visitor).ok()?;
    if visitor.changed && visitor.columns.len() == 0 {
        Some(Arc::new(coerced))
    } else {
        None
    }
}

/// Visitor that coerces INT96 Arrow fields to the Iceberg timestamp type.
struct Int96CoercionVisitor<'a> {
    iceberg_schema: &'a Schema,
    columns: std::slice::Iter<'a, ColumnDescPtr>,
    field_stack: Vec<FieldRef>,
    changed: bool,
}

impl<'a> Int96CoercionVisitor<'a> {
    fn new(iceberg_schema: &'a Schema, parquet_schema: &'a SchemaDescriptor) -> Self {
        Self {
            iceberg_schema,
            columns: parquet_schema.columns().iter(),
            field_stack: Vec::new(),
            changed: false,
        }
    }

    fn target_type(&self, field: &FieldRef, tz: &Option<Arc<str>>) -> DataType {
        field
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .and_then(|id_str| id_str.parse::<i32>().ok())
            .and_then(|field_id| self.iceberg_schema.field_by_id(field_id))
            .and_then(|f| match &*f.field_type {
                Type::Primitive(PrimitiveType::Timestamp) => {
                    Some(DataType::Timestamp(TimeUnit::Microsecond, None))
                }
                Type::Primitive(PrimitiveType::Timestamptz) => Some(DataType::Timestamp(
                    TimeUnit::Microsecond,
                    Some(UTC_TIME_ZONE.into()),
                )),
                Type::Primitive(PrimitiveType::TimestampNs) => {
                    Some(DataType::Timestamp(TimeUnit::Nanosecond, None))
                }
                Type::Primitive(PrimitiveType::TimestamptzNs) => Some(DataType::Timestamp(
                    TimeUnit::Nanosecond,
                    Some(UTC_TIME_ZONE.into()),
                )),
                _ => None,
            })
            // Iceberg Java reads INT96 as microseconds by default
            .unwrap_or_else(|| DataType::Timestamp(TimeUnit::Microsecond, tz.clone()))
    }
}

impl ArrowSchemaVisitor for Int96CoercionVisitor<'_> {
    type T = Field;
    type U = ArrowSchema;

    fn before_field(&mut self, field: &FieldRef) -> Result<()> {
        self.field_stack.push(field.clone());
        Ok(())
    }

    fn after_field(&mut self, _field: &FieldRef) -> Result<()> {
        self.field_stack.pop();
        Ok(())
    }

    fn before_list_element(&mut self, field: &FieldRef) -> Result<()> {
        self.field_stack.push(field.clone());
        Ok(())
    }

    fn after_list_element(&mut self, _field: &FieldRef) -> Result<()> {
        self.field_stack.pop();
        Ok(())
    }

    fn before_map_key(&mut self, field: &FieldRef) -> Result<()> {
        self.field_stack.push(field.clone());
        Ok(())
    }

    fn after_map_key(&mut self, _field: &FieldRef) -> Result<()> {
        self.field_stack.pop();
        Ok(())
    }

    fn before_map_value(&mut self, field: &FieldRef) -> Result<()> {
        self.field_stack.push(field.clone());
        Ok(())
    }

    fn after_map_value(&mut self, _field: &FieldRef) -> Result<()> {
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
        Ok(field_info
            .as_ref()
            .clone()
            .with_data_type(DataType::Struct(Fields::from(results))))
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
        Ok(field_info.as_ref().clone().with_data_type(list_type))
    }

    fn map(&mut self, map: &DataType, key_value: Field, value: Field) -> Result<Field> {
        let field_info = self
            .field_stack
            .last()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Field stack underflow in map"))?;
        let (entries, sorted) = match map {
            DataType::Map(entries, sorted) => (entries, *sorted),
            _ => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Expected map type, got {map}"),
                ));
            }
        };
        let struct_field = entries
            .as_ref()
            .clone()
            .with_data_type(DataType::Struct(Fields::from(vec![key_value, value])));
        Ok(field_info
            .as_ref()
            .clone()
            .with_data_type(DataType::Map(Arc::new(struct_field), sorted)))
    }

    fn primitive(&mut self, p: &DataType) -> Result<Field> {
        let field_info = self.field_stack.last().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "Field stack underflow in primitive")
        })?;

        // Before projection, every primitive (including list elements and map
        // keys/values) consumes one physical column in file order.
        let column = self.columns.next().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "Missing Parquet column for Arrow primitive",
            )
        })?;
        let target = match p {
            DataType::Timestamp(_, tz) if column.physical_type() == PhysicalType::INT96 => {
                self.target_type(field_info, tz)
            }
            _ => field_info.data_type().clone(),
        };
        self.changed |= target != *field_info.data_type();
        Ok(field_info.as_ref().clone().with_data_type(target))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, FieldRef, Schema as ArrowSchema, TimeUnit};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::schema::parser::parse_message_type;
    use parquet::schema::types::SchemaDescriptor;

    use super::coerce_int96_timestamps;
    use crate::arrow::schema::UTC_TIME_ZONE;
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

    fn parquet_schema(message: &str) -> SchemaDescriptor {
        SchemaDescriptor::new(Arc::new(parse_message_type(message).unwrap()))
    }

    #[test]
    fn test_coerce_timestamp_ns_to_us() {
        let parquet = parquet_schema("message schema { optional int96 ts; required int32 id; }");
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true)
                .with_metadata(field_id_meta(1)),
            Field::new("id", DataType::Int32, false).with_metadata(field_id_meta(2)),
        ]));
        let iceberg = iceberg_schema_with_timestamp();

        let coerced = coerce_int96_timestamps(&arrow_schema, &iceberg, &parquet).unwrap();
        assert_eq!(
            coerced.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        // Non-timestamp field unchanged
        assert_eq!(coerced.field(1).data_type(), &DataType::Int32);
    }

    #[test]
    fn test_coerce_timestamptz_ns_to_us() {
        let parquet = parquet_schema("message schema { optional int96 ts; }");
        let iceberg = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::optional(1, "ts", Type::Primitive(PrimitiveType::Timestamptz)).into(),
            ])
            .build()
            .unwrap();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true)
                .with_metadata(field_id_meta(1)),
        ]));

        let coerced = coerce_int96_timestamps(&arrow_schema, &iceberg, &parquet).unwrap();
        assert_eq!(
            coerced.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some(UTC_TIME_ZONE.into()))
        );
    }

    #[test]
    fn test_no_coercion_when_iceberg_is_timestamp_ns() {
        let parquet = parquet_schema("message schema { optional int96 ts; }");
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

        assert!(coerce_int96_timestamps(&arrow_schema, &iceberg, &parquet).is_none());
    }

    #[test]
    fn test_no_coercion_when_iceberg_is_timestamptz_ns() {
        let parquet = parquet_schema("message schema { optional int96 ts; }");
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
                DataType::Timestamp(TimeUnit::Nanosecond, Some(UTC_TIME_ZONE.into())),
                true,
            )
            .with_metadata(field_id_meta(1)),
        ]));

        assert!(coerce_int96_timestamps(&arrow_schema, &iceberg, &parquet).is_none());
    }

    #[test]
    fn test_no_coercion_when_already_microsecond() {
        let parquet = parquet_schema("message schema { optional int96 ts; required int32 id; }");
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true)
                .with_metadata(field_id_meta(1)),
            Field::new("id", DataType::Int32, false).with_metadata(field_id_meta(2)),
        ]));
        let iceberg = iceberg_schema_with_timestamp();

        assert!(coerce_int96_timestamps(&arrow_schema, &iceberg, &parquet).is_none());
    }

    #[test]
    fn test_no_coercion_without_int96() {
        let parquet =
            parquet_schema("message schema { optional int64 ts (TIMESTAMP(NANOS,false)) = 1; }");
        let arrow_schema =
            Arc::new(parquet::arrow::parquet_to_arrow_schema(&parquet, None).unwrap());
        let iceberg = iceberg_schema_with_timestamp();

        assert_eq!(
            coerce_int96_timestamps(&arrow_schema, &iceberg, &parquet),
            None
        );
    }

    #[test]
    fn test_no_coercion_with_mismatched_leaf_count() {
        let ts = Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true)
            .with_metadata(field_id_meta(1));
        let id = Field::new("id", DataType::Int32, false).with_metadata(field_id_meta(2));
        let iceberg = iceberg_schema_with_timestamp();
        for (message, fields) in [
            ("message schema { optional int96 ts; }", vec![
                ts.clone(),
                id,
            ]),
            (
                "message schema { optional int96 ts; required int32 id; }",
                vec![ts],
            ),
        ] {
            let parquet = parquet_schema(message);
            let arrow_schema = Arc::new(ArrowSchema::new(fields));

            assert_eq!(
                coerce_int96_timestamps(&arrow_schema, &iceberg, &parquet),
                None
            );
        }
    }

    #[test]
    fn test_only_int96_timestamps_are_coerced() {
        let parquet = parquet_schema(
            "message schema {
                optional int64 micros (TIMESTAMP(MICROS,false)) = 1;
                optional int64 nanos (TIMESTAMP(NANOS,false)) = 2;
                optional int96 legacy = 3;
                optional int64 adjusted (TIMESTAMP(NANOS,true)) = 4;
            }",
        );
        let arrow_schema =
            Arc::new(parquet::arrow::parquet_to_arrow_schema(&parquet, None).unwrap());
        for (timestamp_type, unit) in [
            (PrimitiveType::Timestamptz, TimeUnit::Microsecond),
            (PrimitiveType::TimestamptzNs, TimeUnit::Nanosecond),
        ] {
            let iceberg = Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "micros", Type::Primitive(PrimitiveType::Timestamptz))
                        .into(),
                    NestedField::optional(2, "nanos", Type::Primitive(PrimitiveType::Timestamptz))
                        .into(),
                    NestedField::optional(3, "legacy", Type::Primitive(timestamp_type)).into(),
                    NestedField::optional(4, "adjusted", Type::Primitive(PrimitiveType::Timestamp))
                        .into(),
                ])
                .build()
                .unwrap();
            let mut fields: Vec<FieldRef> = arrow_schema.fields().iter().cloned().collect();
            fields[2] = Arc::new(
                fields[2]
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Timestamp(unit, Some(UTC_TIME_ZONE.into()))),
            );
            let expected = Arc::new(ArrowSchema::new_with_metadata(
                fields,
                arrow_schema.metadata().clone(),
            ));

            assert_eq!(
                coerce_int96_timestamps(&arrow_schema, &iceberg, &parquet),
                Some(expected)
            );
        }
    }

    // Without field IDs, the visitor can't look up the Iceberg type and falls back
    // to microsecond to match Iceberg Java behavior.
    #[test]
    fn test_defaults_to_us_without_field_ids() {
        let parquet = parquet_schema("message schema { optional int96 ts; }");
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]));
        let iceberg = iceberg_schema_with_timestamp();

        let coerced = coerce_int96_timestamps(&arrow_schema, &iceberg, &parquet).unwrap();
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
        let parquet = parquet_schema("message schema { optional int96 ts; }");
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

        let coerced = coerce_int96_timestamps(&arrow_schema, &iceberg, &parquet).unwrap();
        assert_eq!(
            coerced.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn test_coerce_preserves_field_metadata() {
        let parquet = parquet_schema("message schema { optional int96 ts; }");
        let mut meta = field_id_meta(1);
        meta.insert("custom_key".to_string(), "custom_value".to_string());

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true)
                .with_metadata(meta.clone()),
        ]));
        let iceberg = iceberg_schema_with_timestamp();

        let coerced = coerce_int96_timestamps(&arrow_schema, &iceberg, &parquet).unwrap();
        assert_eq!(coerced.field(0).metadata(), &meta);
    }

    #[test]
    fn test_coerce_timestamp_in_struct() {
        let parquet =
            parquet_schema("message schema { required group data { optional int96 ts; } }");
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

        let coerced = coerce_int96_timestamps(&arrow_schema, &iceberg, &parquet).unwrap();
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
        let parquet = parquet_schema(
            "message schema { optional group timestamps (LIST) {
                repeated group list { optional int96 element; }
            } }",
        );
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

        let coerced = coerce_int96_timestamps(&arrow_schema, &iceberg, &parquet).unwrap();
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
        let parquet = parquet_schema(
            "message schema { optional group ts_map (MAP) {
                repeated group key_value { required binary key (STRING); optional int96 value; }
            } }",
        );
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
            DataType::Struct(vec![key_field.clone(), value_field].into()),
            false,
        )
        .with_metadata(HashMap::from([(
            "custom_key".to_string(),
            "custom_value".to_string(),
        )]));
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "ts_map",
                DataType::Map(Arc::new(entries_field), false),
                true,
            )
            .with_metadata(field_id_meta(1)),
        ]));

        let coerced = coerce_int96_timestamps(&arrow_schema, &iceberg, &parquet).unwrap();
        let expected_entries = Field::new(
            "key_value",
            DataType::Struct(
                vec![
                    key_field,
                    Field::new(
                        "value",
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        true,
                    )
                    .with_metadata(field_id_meta(3)),
                ]
                .into(),
            ),
            false,
        )
        .with_metadata(HashMap::from([(
            "custom_key".to_string(),
            "custom_value".to_string(),
        )]));
        let expected = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "ts_map",
                DataType::Map(Arc::new(expected_entries), false),
                true,
            )
            .with_metadata(field_id_meta(1)),
        ]));
        assert_eq!(coerced, expected);
    }
}
