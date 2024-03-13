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

use hive_metastore::FieldSchema;
use iceberg::spec::{visit_schema, NestedFieldRef, PrimitiveType, Schema, SchemaVisitor};
use iceberg::Result;

pub(crate) type HiveSchema = Vec<FieldSchema>;

#[derive(Debug)]
pub(crate) struct HiveSchemaBuilder {
    cols: HiveSchema,
    context: Vec<NestedFieldRef>,
}

impl HiveSchemaBuilder {
    /// Create new `HiveSchemaBuilder`
    pub fn new() -> Self {
        HiveSchemaBuilder {
            cols: Vec::new(),
            context: Vec::new(),
        }
    }

    /// Convert `Schema` into `HiveSchema` by applying the `SchemaVisitor`
    pub fn from_iceberg(&mut self, schema: &Schema) -> Result<HiveSchema> {
        visit_schema(schema, self)?;
        Ok(self.cols.clone())
    }

    /// Check if visitor is in `StructType`
    fn is_inside_struct(&self) -> bool {
        !self.context.is_empty()
    }
}

impl SchemaVisitor for HiveSchemaBuilder {
    type T = String;

    fn schema(
        &mut self,
        _schema: &iceberg::spec::Schema,
        value: Self::T,
    ) -> iceberg::Result<Self::T> {
        Ok(value)
    }

    fn before_struct_field(
        &mut self,
        field: &iceberg::spec::NestedFieldRef,
    ) -> iceberg::Result<()> {
        self.context.push(field.clone());
        Ok(())
    }

    fn r#struct(
        &mut self,
        r#_struct: &iceberg::spec::StructType,
        results: Vec<Self::T>,
    ) -> iceberg::Result<Self::T> {
        Ok(format!("struct<{}>", results.join(", ")))
    }

    fn after_struct_field(
        &mut self,
        _field: &iceberg::spec::NestedFieldRef,
    ) -> iceberg::Result<()> {
        self.context.pop();
        Ok(())
    }

    fn field(
        &mut self,
        field: &iceberg::spec::NestedFieldRef,
        value: Self::T,
    ) -> iceberg::Result<Self::T> {
        if self.is_inside_struct() {
            return Ok(format!("{}:{}", field.name, value));
        }

        self.cols.push(FieldSchema {
            name: Some(field.name.clone().into()),
            r#type: Some(value.clone().into()),
            comment: field.doc.clone().map(|doc| doc.into()),
        });

        Ok(value)
    }

    fn list(
        &mut self,
        _list: &iceberg::spec::ListType,
        value: Self::T,
    ) -> iceberg::Result<Self::T> {
        Ok(format!("array<{}>", value))
    }

    fn map(
        &mut self,
        _map: &iceberg::spec::MapType,
        key_value: Self::T,
        value: Self::T,
    ) -> iceberg::Result<Self::T> {
        Ok(format!("map<{},{}>", key_value, value))
    }

    fn primitive(&mut self, p: &iceberg::spec::PrimitiveType) -> iceberg::Result<Self::T> {
        let hive_type = match p {
            PrimitiveType::Boolean => "boolean".to_string(),
            PrimitiveType::Int => "int".to_string(),
            PrimitiveType::Long => "bigint".to_string(),
            PrimitiveType::Float => "float".to_string(),
            PrimitiveType::Double => "double".to_string(),
            PrimitiveType::Date => "date".to_string(),
            PrimitiveType::Timestamp | PrimitiveType::Timestamptz => "timestamp".to_string(),
            PrimitiveType::Time | PrimitiveType::String | PrimitiveType::Uuid => {
                "string".to_string()
            }
            PrimitiveType::Binary | PrimitiveType::Fixed(_) => "binary".to_string(),
            PrimitiveType::Decimal { precision, scale } => {
                format!("decimal({},{})", precision, scale)
            }
        };

        Ok(hive_type)
    }
}

#[cfg(test)]
mod tests {
    use iceberg::{
        spec::{ListType, MapType, NestedField, Schema, StructType, Type},
        Result,
    };

    use super::*;

    #[test]
    fn test_schema_with_nested_maps() -> Result<()> {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![NestedField::required(
                1,
                "quux",
                Type::Map(MapType {
                    key_field: NestedField::map_key_element(
                        2,
                        Type::Primitive(PrimitiveType::String),
                    )
                    .into(),
                    value_field: NestedField::map_value_element(
                        3,
                        Type::Map(MapType {
                            key_field: NestedField::map_key_element(
                                4,
                                Type::Primitive(PrimitiveType::String),
                            )
                            .into(),
                            value_field: NestedField::map_value_element(
                                5,
                                Type::Primitive(PrimitiveType::Int),
                                true,
                            )
                            .into(),
                        }),
                        true,
                    )
                    .into(),
                }),
            )
            .into()])
            .build()?;

        let result = HiveSchemaBuilder::new().from_iceberg(&schema)?;

        let expected = vec![FieldSchema {
            name: Some("quux".into()),
            r#type: Some("map<string,map<string,int>>".into()),
            comment: None,
        }];

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn test_schema_with_struct_inside_list() -> Result<()> {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![NestedField::required(
                1,
                "location",
                Type::List(ListType {
                    element_field: NestedField::list_element(
                        2,
                        Type::Struct(StructType::new(vec![
                            NestedField::optional(
                                3,
                                "latitude",
                                Type::Primitive(PrimitiveType::Float),
                            )
                            .into(),
                            NestedField::optional(
                                4,
                                "longitude",
                                Type::Primitive(PrimitiveType::Float),
                            )
                            .into(),
                        ])),
                        true,
                    )
                    .into(),
                }),
            )
            .into()])
            .build()?;

        let result = HiveSchemaBuilder::new().from_iceberg(&schema)?;

        let expected = vec![FieldSchema {
            name: Some("location".into()),
            r#type: Some("array<struct<latitude:float, longitude:float>>".into()),
            comment: None,
        }];

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn test_schema_with_structs() -> Result<()> {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![NestedField::required(
                1,
                "person",
                Type::Struct(StructType::new(vec![
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(3, "age", Type::Primitive(PrimitiveType::Int)).into(),
                ])),
            )
            .into()])
            .build()?;

        let result = HiveSchemaBuilder::new().from_iceberg(&schema)?;

        let expected = vec![FieldSchema {
            name: Some("person".into()),
            r#type: Some("struct<name:string, age:int>".into()),
            comment: None,
        }];

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn test_schema_with_simple_fields() -> Result<()> {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "foo", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "bar", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()?;
        let result = HiveSchemaBuilder::new().from_iceberg(&schema)?;

        let expected = vec![
            FieldSchema {
                name: Some("foo".into()),
                r#type: Some("int".into()),
                comment: None,
            },
            FieldSchema {
                name: Some("bar".into()),
                r#type: Some("string".into()),
                comment: None,
            },
        ];

        assert_eq!(result, expected);

        Ok(())
    }
}
