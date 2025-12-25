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

//! Arrow schema field ID assignment using breadth-first traversal

use std::sync::Arc;

use arrow_schema::{DataType, Fields, Schema as ArrowSchema, TimeUnit};

use super::get_field_doc;
use crate::error::Result;
use crate::spec::{
    ListType, MapType, NestedField, NestedFieldRef, PrimitiveType, Schema, StructType, Type,
};
use crate::{Error, ErrorKind};

/// Helper for assigning field IDs using breadth-first traversal.
///
/// This struct implements BFS traversal to assign field IDs level-by-level,
/// similar to how `ReassignFieldIds` works in the spec module. All fields at
/// one level are assigned IDs before descending to nested fields.
pub(super) struct ArrowSchemaIdAssigner {
    next_id: i32,
}

impl ArrowSchemaIdAssigner {
    pub(super) fn new(start_id: i32) -> Self {
        Self { next_id: start_id }
    }

    fn next_field_id(&mut self) -> i32 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    pub(super) fn convert_schema(&mut self, schema: &ArrowSchema) -> Result<Schema> {
        let fields = self.convert_fields(schema.fields())?;
        Schema::builder().with_fields(fields).build()
    }

    fn convert_fields(&mut self, fields: &Fields) -> Result<Vec<NestedFieldRef>> {
        // First pass: convert all fields at this level and assign IDs
        let fields_with_types: Vec<_> = fields
            .iter()
            .map(|field| {
                let id = self.next_field_id();
                let field_type = arrow_type_to_primitive_or_placeholder(field.data_type())?;
                Ok((field, id, field_type))
            })
            .collect::<Result<Vec<_>>>()?;

        // Second pass: recursively process nested types
        fields_with_types
            .into_iter()
            .map(|(field, id, field_type)| {
                let final_type = self.process_nested_type(field.data_type(), field_type)?;
                let doc = get_field_doc(field);
                Ok(Arc::new(NestedField {
                    id,
                    doc,
                    name: field.name().clone(),
                    required: !field.is_nullable(),
                    field_type: Box::new(final_type),
                    initial_default: None,
                    write_default: None,
                }))
            })
            .collect()
    }

    fn process_nested_type(&mut self, arrow_type: &DataType, placeholder: Type) -> Result<Type> {
        match arrow_type {
            DataType::Struct(fields) => {
                let nested_fields = self.convert_fields(fields)?;
                Ok(Type::Struct(StructType::new(nested_fields)))
            }
            DataType::List(element_field)
            | DataType::LargeList(element_field)
            | DataType::FixedSizeList(element_field, _) => {
                let element_id = self.next_field_id();
                let element_type =
                    arrow_type_to_primitive_or_placeholder(element_field.data_type())?;
                let final_element_type =
                    self.process_nested_type(element_field.data_type(), element_type)?;

                let doc = get_field_doc(element_field);
                let mut element = NestedField::list_element(
                    element_id,
                    final_element_type,
                    !element_field.is_nullable(),
                );
                if let Some(doc) = doc {
                    element = element.with_doc(doc);
                }
                Ok(Type::List(ListType {
                    element_field: Arc::new(element),
                }))
            }
            DataType::Map(field, _) => match field.data_type() {
                DataType::Struct(fields) if fields.len() == 2 => {
                    let key_field = &fields[0];
                    let value_field = &fields[1];

                    let key_id = self.next_field_id();
                    let key_type = arrow_type_to_primitive_or_placeholder(key_field.data_type())?;
                    let final_key_type =
                        self.process_nested_type(key_field.data_type(), key_type)?;

                    let value_id = self.next_field_id();
                    let value_type =
                        arrow_type_to_primitive_or_placeholder(value_field.data_type())?;
                    let final_value_type =
                        self.process_nested_type(value_field.data_type(), value_type)?;

                    let key_doc = get_field_doc(key_field);
                    let mut key = NestedField::map_key_element(key_id, final_key_type);
                    if let Some(doc) = key_doc {
                        key = key.with_doc(doc);
                    }

                    let value_doc = get_field_doc(value_field);
                    let mut value = NestedField::map_value_element(
                        value_id,
                        final_value_type,
                        !value_field.is_nullable(),
                    );
                    if let Some(doc) = value_doc {
                        value = value.with_doc(doc);
                    }

                    Ok(Type::Map(MapType {
                        key_field: Arc::new(key),
                        value_field: Arc::new(value),
                    }))
                }
                _ => Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Map field must have struct type with 2 fields",
                )),
            },
            _ => Ok(placeholder), // Primitive type, return as-is
        }
    }
}

/// Convert Arrow type to Iceberg type for primitives, or return a placeholder for complex types
fn arrow_type_to_primitive_or_placeholder(ty: &DataType) -> Result<Type> {
    match ty {
        DataType::Boolean => Ok(Type::Primitive(PrimitiveType::Boolean)),
        DataType::Int8 | DataType::Int16 | DataType::Int32 => {
            Ok(Type::Primitive(PrimitiveType::Int))
        }
        DataType::UInt8 | DataType::UInt16 => Ok(Type::Primitive(PrimitiveType::Int)),
        DataType::UInt32 => Ok(Type::Primitive(PrimitiveType::Long)),
        DataType::Int64 => Ok(Type::Primitive(PrimitiveType::Long)),
        DataType::UInt64 => Err(Error::new(
            ErrorKind::DataInvalid,
            "UInt64 is not supported. Use Int64 for values ≤ 9,223,372,036,854,775,807 or Decimal(20,0) for full uint64 range.",
        )),
        DataType::Float32 => Ok(Type::Primitive(PrimitiveType::Float)),
        DataType::Float64 => Ok(Type::Primitive(PrimitiveType::Double)),
        DataType::Decimal128(p, s) => Type::decimal(*p as u32, *s as u32).map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                "Failed to create decimal type".to_string(),
            )
            .with_source(e)
        }),
        DataType::Date32 => Ok(Type::Primitive(PrimitiveType::Date)),
        DataType::Time64(unit) if unit == &TimeUnit::Microsecond => {
            Ok(Type::Primitive(PrimitiveType::Time))
        }
        DataType::Timestamp(unit, None) if unit == &TimeUnit::Microsecond => {
            Ok(Type::Primitive(PrimitiveType::Timestamp))
        }
        DataType::Timestamp(unit, None) if unit == &TimeUnit::Nanosecond => {
            Ok(Type::Primitive(PrimitiveType::TimestampNs))
        }
        DataType::Timestamp(unit, Some(zone))
            if unit == &TimeUnit::Microsecond
                && (zone.as_ref() == "UTC" || zone.as_ref() == "+00:00") =>
        {
            Ok(Type::Primitive(PrimitiveType::Timestamptz))
        }
        DataType::Timestamp(unit, Some(zone))
            if unit == &TimeUnit::Nanosecond
                && (zone.as_ref() == "UTC" || zone.as_ref() == "+00:00") =>
        {
            Ok(Type::Primitive(PrimitiveType::TimestamptzNs))
        }
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            Ok(Type::Primitive(PrimitiveType::Binary))
        }
        DataType::FixedSizeBinary(width) => {
            Ok(Type::Primitive(PrimitiveType::Fixed(*width as u64)))
        }
        DataType::Utf8View | DataType::Utf8 | DataType::LargeUtf8 => {
            Ok(Type::Primitive(PrimitiveType::String))
        }
        // For complex types, return a placeholder that will be replaced
        DataType::Struct(_)
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Map(_, _) => {
            Ok(Type::Primitive(PrimitiveType::Boolean)) // Placeholder
        }
        other => Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Unsupported Arrow data type: {other}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::Field;

    use super::*;
    use crate::arrow::DEFAULT_MAP_FIELD_NAME;

    #[test]
    fn test_arrow_schema_to_schema_with_assigned_ids() {
        // Create an Arrow schema without field IDs (like DataFusion CREATE TABLE)
        // Include nested structures to test ID assignment in BFS order
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            // Struct field with nested fields
            Field::new(
                "address",
                DataType::Struct(Fields::from(vec![
                    Field::new("street", DataType::Utf8, false),
                    Field::new("city", DataType::Utf8, false),
                    Field::new("zip", DataType::Int32, true),
                ])),
                true,
            ),
            // List field
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("element", DataType::Utf8, false))),
                true,
            ),
            // Map field
            Field::new(
                "properties",
                DataType::Map(
                    Arc::new(Field::new(
                        DEFAULT_MAP_FIELD_NAME,
                        DataType::Struct(Fields::from(vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Int32, true),
                        ])),
                        false,
                    )),
                    false,
                ),
                false,
            ),
            Field::new("value", DataType::Float64, false),
        ]);

        // Convert to Iceberg schema with auto-assigned IDs
        let mut assigner = ArrowSchemaIdAssigner::new(1);
        let iceberg_schema = assigner.convert_schema(&arrow_schema).unwrap();

        // Verify the schema structure
        let fields = iceberg_schema.as_struct().fields();
        assert_eq!(fields.len(), 6);

        // BFS ordering: top-level fields get IDs 1-6, then nested fields get IDs 7+

        // Check field 1: id
        assert_eq!(fields[0].id, 1);
        assert_eq!(fields[0].name, "id");
        assert!(fields[0].required);
        assert!(matches!(
            fields[0].field_type.as_ref(),
            Type::Primitive(PrimitiveType::Int)
        ));

        // Check field 2: name
        assert_eq!(fields[1].id, 2);
        assert_eq!(fields[1].name, "name");
        assert!(!fields[1].required);
        assert!(matches!(
            fields[1].field_type.as_ref(),
            Type::Primitive(PrimitiveType::String)
        ));

        // Check field 3: address (struct with nested fields)
        assert_eq!(fields[2].id, 3);
        assert_eq!(fields[2].name, "address");
        assert!(!fields[2].required);
        if let Type::Struct(struct_type) = fields[2].field_type.as_ref() {
            let nested_fields = struct_type.fields();
            assert_eq!(nested_fields.len(), 3);
            // Nested field IDs are assigned after all top-level fields (7, 8, 9)
            assert_eq!(nested_fields[0].id, 7);
            assert_eq!(nested_fields[0].name, "street");
            assert_eq!(nested_fields[1].id, 8);
            assert_eq!(nested_fields[1].name, "city");
            assert_eq!(nested_fields[2].id, 9);
            assert_eq!(nested_fields[2].name, "zip");
        } else {
            panic!("Expected struct type for address field");
        }

        // Check field 4: tags (list)
        assert_eq!(fields[3].id, 4);
        assert_eq!(fields[3].name, "tags");
        assert!(!fields[3].required);
        if let Type::List(list_type) = fields[3].field_type.as_ref() {
            // List element ID is assigned after top-level fields
            assert_eq!(list_type.element_field.id, 10);
            assert!(list_type.element_field.required);
        } else {
            panic!("Expected list type for tags field");
        }

        // Check field 5: properties (map)
        assert_eq!(fields[4].id, 5);
        assert_eq!(fields[4].name, "properties");
        assert!(fields[4].required);
        if let Type::Map(map_type) = fields[4].field_type.as_ref() {
            // Map key and value IDs are assigned after top-level fields
            assert_eq!(map_type.key_field.id, 11);
            assert_eq!(map_type.value_field.id, 12);
            assert!(!map_type.value_field.required);
        } else {
            panic!("Expected map type for properties field");
        }

        // Check field 6: value
        assert_eq!(fields[5].id, 6);
        assert_eq!(fields[5].name, "value");
        assert!(fields[5].required);
        assert!(matches!(
            fields[5].field_type.as_ref(),
            Type::Primitive(PrimitiveType::Double)
        ));
    }
}
