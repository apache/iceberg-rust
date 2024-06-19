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

//! Conversion between iceberg and avro schema.
use std::collections::BTreeMap;

use apache_avro::schema::{
    ArraySchema, DecimalSchema, FixedSchema, MapSchema, Name, RecordField as AvroRecordField,
    RecordFieldOrder, RecordSchema, UnionSchema,
};
use apache_avro::Schema as AvroSchema;
use itertools::{Either, Itertools};
use serde_json::{Number, Value};

use crate::spec::{
    visit_schema, ListType, MapType, NestedField, NestedFieldRef, PrimitiveType, Schema, SchemaVisitor, StructType, Type
};
use crate::{ensure_data_valid, Error, ErrorKind, Result};

const ELEMENT_ID: &str = "element-id";
const FILED_ID_PROP: &str = "field-id";
const KEY_ID: &str = "key-id";
const VALUE_ID: &str = "value-id";
const UUID_BYTES: usize = 16;
const UUID_LOGICAL_TYPE: &str = "uuid";
const MAP_LOGICAL_TYPE: &str = "map";
// # TODO: https://github.com/apache/iceberg-rust/issues/86
// This const may better to maintain in avro-rs.
const LOGICAL_TYPE: &str = "logicalType";

struct SchemaToAvroSchema {
    schema: String,
}

type AvroSchemaOrField = Either<AvroSchema, AvroRecordField>;

impl SchemaVisitor for SchemaToAvroSchema {
    type T = AvroSchemaOrField;

    fn schema(&mut self, _schema: &Schema, value: AvroSchemaOrField) -> Result<AvroSchemaOrField> {
        let mut avro_schema = value.unwrap_left();

        if let AvroSchema::Record(record) = &mut avro_schema {
            record.name = Name::from(self.schema.as_str());
        } else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Schema result must be avro record!",
            ));
        }

        Ok(Either::Left(avro_schema))
    }

    fn field(
        &mut self,
        field: &NestedFieldRef,
        avro_schema: AvroSchemaOrField,
    ) -> Result<AvroSchemaOrField> {
        let mut field_schema = avro_schema.unwrap_left();
        if let AvroSchema::Record(record) = &mut field_schema {
            record.name = Name::from(format!("r{}", field.id).as_str());
        }

        if !field.required {
            field_schema = avro_optional(field_schema)?;
        }

        let mut avro_record_field = AvroRecordField {
            name: field.name.clone(),
            schema: field_schema,
            order: RecordFieldOrder::Ignore,
            position: 0,
            doc: field.doc.clone(),
            aliases: None,
            default: None,
            custom_attributes: Default::default(),
        };

        if !field.required {
            avro_record_field.default = Some(Value::Null);
        }
        avro_record_field.custom_attributes.insert(
            FILED_ID_PROP.to_string(),
            Value::Number(Number::from(field.id)),
        );

        Ok(Either::Right(avro_record_field))
    }

    fn r#struct(
        &mut self,
        _struct: &StructType,
        results: Vec<AvroSchemaOrField>,
    ) -> Result<AvroSchemaOrField> {
        let avro_fields = results.into_iter().map(|r| r.unwrap_right()).collect_vec();

        Ok(Either::Left(
            // The name of this record schema should be determined later, by schema name or field
            // name, here we use a temporary placeholder to do it.
            avro_record_schema("null", avro_fields)?,
        ))
    }

    fn list(&mut self, list: &ListType, value: AvroSchemaOrField) -> Result<AvroSchemaOrField> {
        let mut field_schema = value.unwrap_left();

        if let AvroSchema::Record(record) = &mut field_schema {
            record.name = Name::from(format!("r{}", list.element_field.id).as_str());
        }

        if !list.element_field.required {
            field_schema = avro_optional(field_schema)?;
        }

        Ok(Either::Left(AvroSchema::Array(ArraySchema {
            items: Box::new(field_schema),
            attributes: BTreeMap::from([(
                ELEMENT_ID.to_string(),
                Value::Number(Number::from(list.element_field.id)),
            )]),
        })))
    }

    fn map(
        &mut self,
        map: &MapType,
        key_value: AvroSchemaOrField,
        value: AvroSchemaOrField,
    ) -> Result<AvroSchemaOrField> {
        let key_field_schema = key_value.unwrap_left();
        let mut value_field_schema = value.unwrap_left();
        if !map.value_field.required {
            value_field_schema = avro_optional(value_field_schema)?;
        }

        if matches!(key_field_schema, AvroSchema::String) {
            Ok(Either::Left(AvroSchema::Map(MapSchema {
                types: Box::new(value_field_schema),
                attributes: BTreeMap::from([
                    (
                        KEY_ID.to_string(),
                        Value::Number(Number::from(map.key_field.id)),
                    ),
                    (
                        VALUE_ID.to_string(),
                        Value::Number(Number::from(map.value_field.id)),
                    ),
                ]),
            })))
        } else {
            // Avro map requires that key must be string type. Here we convert it to array if key is
            // not string type.
            let key_field = {
                let mut field = AvroRecordField {
                    name: map.key_field.name.clone(),
                    doc: None,
                    aliases: None,
                    default: None,
                    schema: key_field_schema,
                    order: RecordFieldOrder::Ascending,
                    position: 0,
                    custom_attributes: Default::default(),
                };
                field.custom_attributes.insert(
                    FILED_ID_PROP.to_string(),
                    Value::Number(Number::from(map.key_field.id)),
                );
                field
            };

            let value_field = {
                let mut field = AvroRecordField {
                    name: map.value_field.name.clone(),
                    doc: None,
                    aliases: None,
                    default: None,
                    schema: value_field_schema,
                    order: RecordFieldOrder::Ignore,
                    position: 0,
                    custom_attributes: Default::default(),
                };
                field.custom_attributes.insert(
                    FILED_ID_PROP.to_string(),
                    Value::Number(Number::from(map.value_field.id)),
                );
                field
            };

            let fields = vec![key_field, value_field];
            let item_avro_schema = avro_record_schema(
                format!("k{}_v{}", map.key_field.id, map.value_field.id).as_str(),
                fields,
            )?;

            Ok(Either::Left(AvroSchema::Array(ArraySchema {
                items: Box::new(item_avro_schema),
                attributes: BTreeMap::from([(
                    LOGICAL_TYPE.to_string(),
                    Value::String(MAP_LOGICAL_TYPE.to_string()),
                )]),
            })))
        }
    }

    fn primitive(&mut self, p: &PrimitiveType) -> Result<AvroSchemaOrField> {
        let avro_schema = match p {
            PrimitiveType::Boolean => AvroSchema::Boolean,
            PrimitiveType::Int => AvroSchema::Int,
            PrimitiveType::Long => AvroSchema::Long,
            PrimitiveType::Float => AvroSchema::Float,
            PrimitiveType::Double => AvroSchema::Double,
            PrimitiveType::Date => AvroSchema::Date,
            PrimitiveType::Time => AvroSchema::TimeMicros,
            PrimitiveType::Timestamp => AvroSchema::TimestampMicros,
            PrimitiveType::Timestamptz => AvroSchema::TimestampMicros,
            PrimitiveType::String => AvroSchema::String,
            PrimitiveType::Uuid => avro_fixed_schema(UUID_BYTES, Some(UUID_LOGICAL_TYPE))?,
            PrimitiveType::Fixed(len) => avro_fixed_schema((*len) as usize, None)?,
            PrimitiveType::Binary => AvroSchema::Bytes,
            PrimitiveType::Decimal { precision, scale } => {
                avro_decimal_schema(*precision as usize, *scale as usize)?
            }
        };
        Ok(Either::Left(avro_schema))
    }
}

/// Converting iceberg schema to avro schema.
pub(crate) fn schema_to_avro_schema(name: impl ToString, schema: &Schema) -> Result<AvroSchema> {
    let mut converter = SchemaToAvroSchema {
        schema: name.to_string(),
    };

    visit_schema(schema, &mut converter).map(Either::unwrap_left)
}

fn avro_record_schema(name: &str, fields: Vec<AvroRecordField>) -> Result<AvroSchema> {
    let lookup = fields
        .iter()
        .enumerate()
        .map(|f| (f.1.name.clone(), f.0))
        .collect();

    Ok(AvroSchema::Record(RecordSchema {
        name: Name::new(name)?,
        aliases: None,
        doc: None,
        fields,
        lookup,
        attributes: Default::default(),
    }))
}

pub(crate) fn avro_fixed_schema(len: usize, logical_type: Option<&str>) -> Result<AvroSchema> {
    let attributes = if let Some(logical_type) = logical_type {
        BTreeMap::from([(
            LOGICAL_TYPE.to_string(),
            Value::String(logical_type.to_string()),
        )])
    } else {
        Default::default()
    };
    Ok(AvroSchema::Fixed(FixedSchema {
        name: Name::new(format!("fixed_{len}").as_str())?,
        aliases: None,
        doc: None,
        size: len,
        attributes,
        default: None,
    }))
}

pub(crate) fn avro_decimal_schema(precision: usize, scale: usize) -> Result<AvroSchema> {
    // Avro decimal logical type annotates Avro bytes _or_ fixed types.
    // https://avro.apache.org/docs/1.11.1/specification/_print/#decimal
    // Iceberg spec: Stored as _fixed_ using the minimum number of bytes for the given precision.
    // https://iceberg.apache.org/spec/#avro
    Ok(AvroSchema::Decimal(DecimalSchema {
        precision,
        scale,
        inner: Box::new(AvroSchema::Fixed(FixedSchema {
            // Name is not restricted by the spec.
            // Refer to iceberg-python https://github.com/apache/iceberg-python/blob/d8bc1ca9af7957ce4d4db99a52c701ac75db7688/pyiceberg/utils/schema_conversion.py#L574-L582
            name: Name::new(&format!("decimal_{precision}_{scale}")).unwrap(),
            aliases: None,
            doc: None,
            size: crate::spec::Type::decimal_required_bytes(precision as u32)? as usize,
            attributes: Default::default(),
            default: None,
        })),
    }))
}

fn avro_optional(avro_schema: AvroSchema) -> Result<AvroSchema> {
    Ok(AvroSchema::Union(UnionSchema::new(vec![
        AvroSchema::Null,
        avro_schema,
    ])?))
}

fn is_avro_optional(avro_schema: &AvroSchema) -> bool {
    match avro_schema {
        AvroSchema::Union(union) => union.is_nullable(),
        _ => false,
    }
}

/// Post order avro schema visitor.
pub(crate) trait AvroSchemaVisitor {
    type T;

    fn record(&mut self, record: &RecordSchema, fields: Vec<Self::T>) -> Result<Self::T>;

    fn union(&mut self, union: &UnionSchema, options: Vec<Self::T>) -> Result<Self::T>;

    fn array(&mut self, array: &ArraySchema, item: Self::T) -> Result<Self::T>;
    fn map(&mut self, map: &MapSchema, value: Self::T) -> Result<Self::T>;
    fn map_array(&mut self, array: &RecordSchema, key: Self::T, value: Self::T) -> Result<Self::T>;

    fn primitive(&mut self, schema: &AvroSchema) -> Result<Self::T>;
}

/// Visit avro schema in post order visitor.
pub(crate) fn visit<V: AvroSchemaVisitor>(schema: &AvroSchema, visitor: &mut V) -> Result<V::T> {
    match schema {
        AvroSchema::Record(record) => {
            let field_results = record
                .fields
                .iter()
                .map(|f| visit(&f.schema, visitor))
                .collect::<Result<Vec<V::T>>>()?;

            visitor.record(record, field_results)
        }
        AvroSchema::Union(union) => {
            let option_results = union
                .variants()
                .iter()
                .map(|f| visit(f, visitor))
                .collect::<Result<Vec<V::T>>>()?;

            visitor.union(union, option_results)
        }
        AvroSchema::Array(item) => {
            if let Some(logical_type) = item
                .attributes
                .get(LOGICAL_TYPE)
                .and_then(|v| Value::as_str(v))
            {
                if logical_type == MAP_LOGICAL_TYPE {
                    if let AvroSchema::Record(record_schema) = &*item.items {
                        let key = visit(&record_schema.fields[0].schema, visitor)?;
                        let value = visit(&record_schema.fields[1].schema, visitor)?;
                        return visitor.map_array(record_schema, key, value);
                    } else {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            "Can't convert avro map schema, item is not a record.",
                        ));
                    }
                } else {
                    return Err(Error::new(
                        ErrorKind::FeatureUnsupported,
                        format!(
                            "Logical type {logical_type} is not support in iceberg array type.",
                        ),
                    ));
                }
            }
            let item_result = visit(&item.items, visitor)?;
            visitor.array(item, item_result)
        }
        AvroSchema::Map(inner) => {
            let item_result = visit(&inner.types, visitor)?;
            visitor.map(inner, item_result)
        }
        schema => visitor.primitive(schema),
    }
}

struct AvroSchemaToSchema;

impl AvroSchemaVisitor for AvroSchemaToSchema {
    // Only `AvroSchema::Null` will return `None`
    type T = Option<Type>;

    fn record(
        &mut self,
        record: &RecordSchema,
        field_types: Vec<Option<Type>>,
    ) -> Result<Option<Type>> {
        let mut fields = Vec::with_capacity(field_types.len());
        for (avro_field, typ) in record.fields.iter().zip_eq(field_types) {
            let field_id = avro_field
                .custom_attributes
                .get(FILED_ID_PROP)
                .and_then(Value::as_i64)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Can't convert field, missing field id: {avro_field:?}"),
                    )
                })?;

            let optional = is_avro_optional(&avro_field.schema);

            let mut field = if optional {
                NestedField::optional(field_id as i32, &avro_field.name, typ.unwrap())
            } else {
                NestedField::required(field_id as i32, &avro_field.name, typ.unwrap())
            };

            if let Some(doc) = &avro_field.doc {
                field = field.with_doc(doc);
            }

            fields.push(field.into());
        }

        Ok(Some(Type::Struct(StructType::new(fields))))
    }

    fn union(
        &mut self,
        union: &UnionSchema,
        mut options: Vec<Option<Type>>,
    ) -> Result<Option<Type>> {
        ensure_data_valid!(
            options.len() <= 2 && !options.is_empty(),
            "Can't convert avro union type {:?} to iceberg.",
            union
        );

        if options.len() > 1 {
            ensure_data_valid!(
                options[0].is_none(),
                "Can't convert avro union type {:?} to iceberg.",
                union
            );
        }

        if options.len() == 1 {
            Ok(Some(options.remove(0).unwrap()))
        } else {
            Ok(Some(options.remove(1).unwrap()))
        }
    }

    fn array(&mut self, array: &ArraySchema, item: Option<Type>) -> Result<Self::T> {
        let element_field_id = array
            .attributes
            .get(ELEMENT_ID)
            .and_then(Value::as_i64)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Can't convert avro array schema, missing element id.",
                )
            })?
            .try_into()
            .map_err(|_| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Can't convert avro array schema, element id is not a valid i32.",
                )
            })?;
        let element_field = NestedField::list_element(
            element_field_id,
            item.unwrap(),
            !is_avro_optional(&array.items),
        )
        .into();
        Ok(Some(Type::List(ListType { element_field })))
    }

    fn map(&mut self, map: &MapSchema, value: Option<Type>) -> Result<Option<Type>> {
        let key_field_id = map
            .attributes
            .get(KEY_ID)
            .and_then(Value::as_i64)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Can't convert avro map schema, missing key id.",
                )
            })?
            .try_into()
            .map_err(|_| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Can't convert avro map schema, key id is not a valid i32.",
                )
            })?;
        let key_field =
            NestedField::map_key_element(key_field_id, Type::Primitive(PrimitiveType::String));
        let value_field_id = map
            .attributes
            .get(VALUE_ID)
            .and_then(Value::as_i64)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Can't convert avro map schema, missing value id.",
                )
            })?
            .try_into()
            .map_err(|_| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Can't convert avro map schema, value id is not a valid i32.",
                )
            })?;
        let value_field = NestedField::map_value_element(
            value_field_id,
            value.unwrap(),
            !is_avro_optional(&map.types),
        );
        Ok(Some(Type::Map(MapType {
            key_field: key_field.into(),
            value_field: value_field.into(),
        })))
    }

    fn primitive(&mut self, schema: &AvroSchema) -> Result<Option<Type>> {
        let typ = match schema {
            AvroSchema::Decimal(decimal) => {
                Type::decimal(decimal.precision as u32, decimal.scale as u32)?
            }
            AvroSchema::Date => Type::Primitive(PrimitiveType::Date),
            AvroSchema::TimeMicros => Type::Primitive(PrimitiveType::Time),
            AvroSchema::TimestampMicros => Type::Primitive(PrimitiveType::Timestamp),
            AvroSchema::Boolean => Type::Primitive(PrimitiveType::Boolean),
            AvroSchema::Int => Type::Primitive(PrimitiveType::Int),
            AvroSchema::Long => Type::Primitive(PrimitiveType::Long),
            AvroSchema::Float => Type::Primitive(PrimitiveType::Float),
            AvroSchema::Double => Type::Primitive(PrimitiveType::Double),
            AvroSchema::String | AvroSchema::Enum(_) => Type::Primitive(PrimitiveType::String),
            AvroSchema::Fixed(fixed) => {
                if let Some(logical_type) = fixed.attributes.get(LOGICAL_TYPE) {
                    let logical_type = logical_type.as_str().ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "logicalType in attributes of avro schema is not a string type",
                        )
                    })?;
                    match logical_type {
                        UUID_LOGICAL_TYPE => Type::Primitive(PrimitiveType::Uuid),
                        ty => {
                            return Err(Error::new(
                                ErrorKind::FeatureUnsupported,
                                format!(
                                    "Logical type {ty} is not support in iceberg primitive type.",
                                ),
                            ))
                        }
                    }
                } else {
                    Type::Primitive(PrimitiveType::Fixed(fixed.size as u64))
                }
            }
            AvroSchema::Bytes => Type::Primitive(PrimitiveType::Binary),
            AvroSchema::Null => return Ok(None),
            _ => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Unable to convert avro {schema} to iceberg primitive type.",
                ))
            }
        };

        Ok(Some(typ))
    }

    fn map_array(&mut self, array: &RecordSchema, key: Self::T, value: Self::T) -> Result<Self::T> {
        let key = key.ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Can't convert avro map schema, missing key schema.",
            )
        })?;
        let value = value.ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Can't convert avro map schema, missing value schema.",
            )
        })?;
        let key_id = array.fields[0]
            .custom_attributes
            .get(FILED_ID_PROP)
            .and_then(Value::as_i64)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Can't convert avro map schema, missing key id.",
                )
            })?
            .try_into()
            .map_err(|_| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Can't convert avro map schema, key id is not a valid i32.",
                )
            })?;
        let value_id = array.fields[1]
            .custom_attributes
            .get(FILED_ID_PROP)
            .and_then(Value::as_i64)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Can't convert avro map schema, missing value id.",
                )
            })?
            .try_into()
            .map_err(|_| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Can't convert avro map schema, value id is not a valid i32.",
                )
            })?;
        let key_field = NestedField::required(key_id, array.fields[0].name.clone(), key);
        let value_field = if is_avro_optional(&array.fields[1].schema) {
            NestedField::optional(value_id, array.fields[1].name.clone(), value)
        } else {
            NestedField::required(value_id, array.fields[1].name.clone(), value)
        };
        Ok(Some(Type::Map(MapType {
            key_field: key_field.into(),
            value_field: value_field.into(),
        })))
    }
}

/// Converts avro schema to iceberg schema.
pub(crate) fn avro_schema_to_schema(avro_schema: &AvroSchema) -> Result<Schema> {
    if let AvroSchema::Record(_) = avro_schema {
        let mut converter = AvroSchemaToSchema;
        let typ = visit(avro_schema, &mut converter)?.expect("Iceberg schema should not be none.");
        if let Type::Struct(s) = typ {
            Schema::builder()
                .with_fields(s.fields().iter().cloned())
                .build()
        } else {
            Err(Error::new(
                ErrorKind::Unexpected,
                format!("Expected to convert avro record schema to struct type, but {typ}"),
            ))
        }
    } else {
        Err(Error::new(
            ErrorKind::DataInvalid,
            "Can't convert non record avro schema to iceberg schema: {avro_schema}",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::fs::read_to_string;
    use std::sync::Arc;

    use apache_avro::schema::{Namespace, UnionSchema};
    use apache_avro::Schema as AvroSchema;

    use super::*;
    use crate::avro::schema::AvroSchemaToSchema;
    use crate::spec::{ListType, MapType, NestedField, PrimitiveType, Schema, StructType, Type};

    fn read_test_data_file_to_avro_schema(filename: &str) -> AvroSchema {
        let input = read_to_string(format!(
            "{}/testdata/{}",
            env!("CARGO_MANIFEST_DIR"),
            filename
        ))
        .unwrap();

        AvroSchema::parse_str(input.as_str()).unwrap()
    }

    fn check_schema_conversion(avro_schema: AvroSchema, expected_iceberg_schema: Schema) {
        let converted_iceberg_schema = avro_schema_to_schema(&avro_schema).unwrap();
        assert_eq!(expected_iceberg_schema, converted_iceberg_schema);

        let converted_avro_schema = schema_to_avro_schema(
            avro_schema.name().unwrap().fullname(Namespace::None),
            &expected_iceberg_schema,
        )
        .unwrap();
        assert_eq!(avro_schema, converted_avro_schema);

        let converted_converted_iceberg_schema = avro_schema_to_schema(&avro_schema).unwrap();
        assert_eq!(expected_iceberg_schema, converted_converted_iceberg_schema);
    }

    #[test]
    fn test_manifest_file_v1_schema() {
        let fields = vec![
            NestedField::required(500, "manifest_path", PrimitiveType::String.into())
                .with_doc("Location URI with FS scheme")
                .into(),
            NestedField::required(501, "manifest_length", PrimitiveType::Long.into())
                .with_doc("Total file size in bytes")
                .into(),
            NestedField::required(502, "partition_spec_id", PrimitiveType::Int.into())
                .with_doc("Spec ID used to write")
                .into(),
            NestedField::optional(503, "added_snapshot_id", PrimitiveType::Long.into())
                .with_doc("Snapshot ID that added the manifest")
                .into(),
            NestedField::optional(504, "added_data_files_count", PrimitiveType::Int.into())
                .with_doc("Added entry count")
                .into(),
            NestedField::optional(505, "existing_data_files_count", PrimitiveType::Int.into())
                .with_doc("Existing entry count")
                .into(),
            NestedField::optional(506, "deleted_data_files_count", PrimitiveType::Int.into())
                .with_doc("Deleted entry count")
                .into(),
            NestedField::optional(
                507,
                "partitions",
                ListType {
                    element_field: NestedField::list_element(
                        508,
                        StructType::new(vec![
                            NestedField::required(
                                509,
                                "contains_null",
                                PrimitiveType::Boolean.into(),
                            )
                            .with_doc("True if any file has a null partition value")
                            .into(),
                            NestedField::optional(
                                518,
                                "contains_nan",
                                PrimitiveType::Boolean.into(),
                            )
                            .with_doc("True if any file has a nan partition value")
                            .into(),
                            NestedField::optional(510, "lower_bound", PrimitiveType::Binary.into())
                                .with_doc("Partition lower bound for all files")
                                .into(),
                            NestedField::optional(511, "upper_bound", PrimitiveType::Binary.into())
                                .with_doc("Partition upper bound for all files")
                                .into(),
                        ])
                        .into(),
                        true,
                    )
                    .into(),
                }
                .into(),
            )
            .with_doc("Summary for each partition")
            .into(),
            NestedField::optional(512, "added_rows_count", PrimitiveType::Long.into())
                .with_doc("Added rows count")
                .into(),
            NestedField::optional(513, "existing_rows_count", PrimitiveType::Long.into())
                .with_doc("Existing rows count")
                .into(),
            NestedField::optional(514, "deleted_rows_count", PrimitiveType::Long.into())
                .with_doc("Deleted rows count")
                .into(),
        ];

        let iceberg_schema = Schema::builder().with_fields(fields).build().unwrap();
        check_schema_conversion(
            read_test_data_file_to_avro_schema("avro_schema_manifest_file_v1.json"),
            iceberg_schema,
        );
    }

    #[test]
    fn test_avro_list_required_primitive() {
        let avro_schema = {
            AvroSchema::parse_str(
                r#"
{
    "type": "record",
    "name": "avro_schema",
    "fields": [
        {
            "name": "array_with_string",
            "type": {
                "type": "array",
                "items": "string",
                "default": [],
                "element-id": 101
            },
            "field-id": 100
        }
    ]
}"#,
            )
            .unwrap()
        };

        let iceberg_schema = {
            Schema::builder()
                .with_fields(vec![NestedField::required(
                    100,
                    "array_with_string",
                    ListType {
                        element_field: NestedField::list_element(
                            101,
                            PrimitiveType::String.into(),
                            true,
                        )
                        .into(),
                    }
                    .into(),
                )
                .into()])
                .build()
                .unwrap()
        };

        check_schema_conversion(avro_schema, iceberg_schema);
    }

    #[test]
    fn test_avro_list_wrapped_primitive() {
        let avro_schema = {
            AvroSchema::parse_str(
                r#"
{
    "type": "record",
    "name": "avro_schema",
    "fields": [
        {
            "name": "array_with_string",
            "type": {
                "type": "array",
                "items": {"type": "string"},
                "default": [],
                "element-id": 101
            },
            "field-id": 100
        }
    ]
}
"#,
            )
            .unwrap()
        };

        let iceberg_schema = {
            Schema::builder()
                .with_fields(vec![NestedField::required(
                    100,
                    "array_with_string",
                    ListType {
                        element_field: NestedField::list_element(
                            101,
                            PrimitiveType::String.into(),
                            true,
                        )
                        .into(),
                    }
                    .into(),
                )
                .into()])
                .build()
                .unwrap()
        };

        check_schema_conversion(avro_schema, iceberg_schema);
    }

    #[test]
    fn test_avro_list_required_record() {
        let avro_schema = {
            AvroSchema::parse_str(
                r#"
{
    "type": "record",
    "name": "avro_schema",
    "fields": [
        {
            "name": "array_with_record",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "r101",
                    "fields": [
                        {
                            "name": "contains_null",
                            "type": "boolean",
                            "field-id": 102
                        },
                        {
                            "name": "contains_nan",
                            "type": ["null", "boolean"],
                            "field-id": 103
                        }
                    ]
                },
                "element-id": 101
            },
            "field-id": 100
        }
    ]
}
"#,
            )
            .unwrap()
        };

        let iceberg_schema = {
            Schema::builder()
                .with_fields(vec![NestedField::required(
                    100,
                    "array_with_record",
                    ListType {
                        element_field: NestedField::list_element(
                            101,
                            StructType::new(vec![
                                NestedField::required(
                                    102,
                                    "contains_null",
                                    PrimitiveType::Boolean.into(),
                                )
                                .into(),
                                NestedField::optional(
                                    103,
                                    "contains_nan",
                                    PrimitiveType::Boolean.into(),
                                )
                                .into(),
                            ])
                            .into(),
                            true,
                        )
                        .into(),
                    }
                    .into(),
                )
                .into()])
                .build()
                .unwrap()
        };

        check_schema_conversion(avro_schema, iceberg_schema);
    }

    #[test]
    fn test_schema_with_array_map() {
        let avro_schema = {
            AvroSchema::parse_str(
                r#"
{
    "type": "record",
    "name": "avro_schema",
    "fields": [
        {
            "name": "optional",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "k102_v103",
                    "fields": [
                        {
                            "name": "key",
                            "type": "boolean",
                            "field-id": 102
                        },
                        {
                            "name": "value",
                            "type": ["null", "boolean"],
                            "field-id": 103
                        }
                    ]
                },
                "default": [],
                "logicalType": "map"
            },
            "field-id": 100
        },{
            "name": "required",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "k105_v106",
                    "fields": [
                        {
                            "name": "key",
                            "type": "boolean",
                            "field-id": 105
                        },
                        {
                            "name": "value",
                            "type": "boolean",
                            "field-id": 106
                        }
                    ]
                },
                "default": [],
                "logicalType": "map"
            },
            "field-id": 104
        }
    ]
}
"#,
            )
            .unwrap()
        };

        let iceberg_schema = {
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        100,
                        "optional",
                        Type::Map(MapType {
                            key_field: NestedField::map_key_element(
                                102,
                                PrimitiveType::Boolean.into(),
                            )
                            .into(),
                            value_field: NestedField::map_value_element(
                                103,
                                PrimitiveType::Boolean.into(),
                                false,
                            )
                            .into(),
                        }),
                    )),
                    Arc::new(NestedField::required(
                        104,
                        "required",
                        Type::Map(MapType {
                            key_field: NestedField::map_key_element(
                                105,
                                PrimitiveType::Boolean.into(),
                            )
                            .into(),
                            value_field: NestedField::map_value_element(
                                106,
                                PrimitiveType::Boolean.into(),
                                true,
                            )
                            .into(),
                        }),
                    )),
                ])
                .build()
                .unwrap()
        };

        check_schema_conversion(avro_schema, iceberg_schema);
    }

    #[test]
    fn test_resolve_union() {
        let avro_schema = UnionSchema::new(vec![
            AvroSchema::Null,
            AvroSchema::String,
            AvroSchema::Boolean,
        ])
        .unwrap();

        let mut converter = AvroSchemaToSchema;

        let options = avro_schema
            .variants()
            .iter()
            .map(|v| converter.primitive(v).unwrap())
            .collect();
        assert!(converter.union(&avro_schema, options).is_err());
    }

    #[test]
    fn test_string_type() {
        let mut converter = AvroSchemaToSchema;
        let avro_schema = AvroSchema::String;

        assert_eq!(
            Some(PrimitiveType::String.into()),
            converter.primitive(&avro_schema).unwrap()
        );
    }

    #[test]
    fn test_map_type() {
        let avro_schema = {
            AvroSchema::parse_str(
                r#"
{
    "type": "map",
    "values": ["null", "long"],
    "key-id": 101,
    "value-id": 102
}
"#,
            )
            .unwrap()
        };

        let AvroSchema::Map(avro_schema) = avro_schema else {
            unreachable!()
        };

        let mut converter = AvroSchemaToSchema;
        let iceberg_type = Type::Map(MapType {
            key_field: NestedField::map_key_element(101, PrimitiveType::String.into()).into(),
            value_field: NestedField::map_value_element(102, PrimitiveType::Long.into(), false)
                .into(),
        });

        assert_eq!(
            iceberg_type,
            converter
                .map(&avro_schema, Some(PrimitiveType::Long.into()))
                .unwrap()
                .unwrap()
        );
    }

    #[test]
    fn test_fixed_type() {
        let avro_schema = {
            AvroSchema::parse_str(
                r#"
            {"name": "test", "type": "fixed", "size": 22}
            "#,
            )
            .unwrap()
        };

        let mut converter = AvroSchemaToSchema;

        let iceberg_type = Type::from(PrimitiveType::Fixed(22));

        assert_eq!(
            iceberg_type,
            converter.primitive(&avro_schema).unwrap().unwrap()
        );
    }

    #[test]
    fn test_unknown_primitive() {
        let mut converter = AvroSchemaToSchema;

        assert!(converter.primitive(&AvroSchema::Duration).is_err());
    }

    #[test]
    fn test_no_field_id() {
        let avro_schema = {
            AvroSchema::parse_str(
                r#"
{
    "type": "record",
    "name": "avro_schema",
    "fields": [
        {
            "name": "array_with_string",
            "type": "string"
        }
    ]
}
"#,
            )
            .unwrap()
        };

        assert!(avro_schema_to_schema(&avro_schema).is_err());
    }

    #[test]
    fn test_decimal_type() {
        let avro_schema = {
            AvroSchema::parse_str(
                r#"
      {"type": "bytes", "logicalType": "decimal", "precision": 25, "scale": 19}
            "#,
            )
            .unwrap()
        };

        let mut converter = AvroSchemaToSchema;

        assert_eq!(
            Type::decimal(25, 19).unwrap(),
            converter.primitive(&avro_schema).unwrap().unwrap()
        );
    }

    #[test]
    fn test_date_type() {
        let mut converter = AvroSchemaToSchema;

        assert_eq!(
            Type::from(PrimitiveType::Date),
            converter.primitive(&AvroSchema::Date).unwrap().unwrap()
        );
    }
}
