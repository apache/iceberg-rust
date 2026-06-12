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

use apache_avro::Schema as AvroSchema;
use apache_avro::schema::{
    ArraySchema, DecimalSchema, FixedSchema, MapSchema, Name, RecordField as AvroRecordField,
    RecordFieldOrder, RecordSchema, UnionSchema,
};
use itertools::{Either, Itertools};
use serde_json::{Number, Value};

use crate::spec::{
    ListType, MapType, NestedField, NestedFieldRef, PrimitiveType, Schema, SchemaVisitor,
    StructType, Type, visit_schema,
};
use crate::{Error, ErrorKind, Result, ensure_data_valid};

const ELEMENT_ID: &str = "element-id";
const FIELD_ID_PROP: &str = "field-id";
const KEY_ID: &str = "key-id";
const VALUE_ID: &str = "value-id";
const MAP_LOGICAL_TYPE: &str = "map";
/// The Avro logical-type name Java stamps on a variant record
/// (`org.apache.iceberg.avro.VariantLogicalType.NAME`, 1.10.0).
const VARIANT_LOGICAL_TYPE: &str = "variant";
/// Field names of the Avro variant record (Java `AvroSchemaUtil.isVariantSchema`, 1.10.0).
const VARIANT_METADATA_FIELD: &str = "metadata";
const VARIANT_VALUE_FIELD: &str = "value";
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

        let default = if let Some(literal) = &field.initial_default {
            Some(literal.clone().try_into_json(&field.field_type)?)
        } else if !field.required {
            Some(Value::Null)
        } else {
            None
        };

        let mut avro_record_field = AvroRecordField {
            name: field.name.clone(),
            schema: field_schema,
            order: RecordFieldOrder::Ignore,
            position: 0,
            doc: field.doc.clone(),
            aliases: None,
            default,
            custom_attributes: Default::default(),
        };

        avro_record_field.custom_attributes.insert(
            FIELD_ID_PROP.to_string(),
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
        let mut key_field_schema = key_value.unwrap_left();
        let mut value_field_schema = value.unwrap_left();
        // A variant key/value record is renamed to Java's `r<fieldId>` (1.10.0 `TypeToSchema`
        // names every record from its field-id stack; live-Java-probed: a variant map value
        // converts to a record named `r8` for value-id 8). Without the rename, two variant-valued
        // maps in one schema would emit two records both named "variant" — an Avro-spec
        // duplicate-definition that Java's `Schema.Parser` rejects with "Can't redefine: variant".
        rename_variant_record(&mut key_field_schema, map.key_field.id);
        rename_variant_record(&mut value_field_schema, map.value_field.id);
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
                    FIELD_ID_PROP.to_string(),
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
                    FIELD_ID_PROP.to_string(),
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

    fn variant(&mut self) -> Result<AvroSchemaOrField> {
        Ok(Either::Left(avro_variant_schema()?))
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
            PrimitiveType::TimestampNs => AvroSchema::TimestampNanos,
            PrimitiveType::TimestamptzNs => AvroSchema::TimestampNanos,
            PrimitiveType::String => AvroSchema::String,
            PrimitiveType::Uuid => AvroSchema::Uuid,
            PrimitiveType::Fixed(len) => avro_fixed_schema((*len) as usize)?,
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

/// Builds the Avro schema Java 1.10.0 emits for an Iceberg `variant` column
/// (`TypeToSchema.variant`, bytecode-pinned): a record with two REQUIRED `bytes` fields —
/// `metadata` then `value` — annotated with the logical type `"variant"`
/// (`VariantLogicalType.NAME`).
///
/// The record name here is Java's no-enclosing-field fallback `"variant"`; the rename hooks in
/// [`SchemaToAvroSchema::field`] / [`SchemaToAvroSchema::list`] (any record) and
/// [`SchemaToAvroSchema::map`] (via [`rename_variant_record`]) rename it to `r{field_id}`,
/// matching Java's `r<fieldId>` naming in every placement (Java derives the name from its
/// field-id stack; this converter renames records after the fact — same resulting name,
/// live-Java-probed against 1.10.0 `AvroSchemaUtil.convert`).
///
/// apache-avro 0.21 carries the unknown logical type as a custom attribute on the record —
/// verified against the vendored crate: `parse_record` keeps `logicalType` in
/// `RecordSchema.attributes` (only `fields` is excluded) and `Serialize` writes attributes back,
/// so the Java-shaped JSON round-trips exactly.
fn avro_variant_schema() -> Result<AvroSchema> {
    let metadata_field = AvroRecordField {
        name: VARIANT_METADATA_FIELD.to_string(),
        schema: AvroSchema::Bytes,
        order: RecordFieldOrder::Ignore,
        position: 0,
        doc: None,
        aliases: None,
        default: None,
        custom_attributes: Default::default(),
    };
    let value_field = AvroRecordField {
        name: VARIANT_VALUE_FIELD.to_string(),
        schema: AvroSchema::Bytes,
        order: RecordFieldOrder::Ignore,
        position: 1,
        doc: None,
        aliases: None,
        default: None,
        custom_attributes: Default::default(),
    };

    let mut avro_schema =
        avro_record_schema(VARIANT_LOGICAL_TYPE, vec![metadata_field, value_field])?;
    if let AvroSchema::Record(record) = &mut avro_schema {
        record.attributes.insert(
            LOGICAL_TYPE.to_string(),
            Value::String(VARIANT_LOGICAL_TYPE.to_string()),
        );
    }
    Ok(avro_schema)
}

/// Renames a variant record (identified by its `"variant"` logical-type attribute) to Java's
/// `r<fieldId>` recipe name for the enclosing map key/value field.
///
/// [`SchemaToAvroSchema::field`] and [`SchemaToAvroSchema::list`] already rename ANY record they
/// enclose; the map visitor historically renames nothing (a struct map value keeps the `"null"`
/// placeholder — a pre-existing divergence from Java's `r<fieldId>`), so this hook renames only
/// the variant record to avoid changing pre-existing struct naming while matching Java's shape
/// (live-probed: 1.10.0 emits `r8` for a variant map value with value-id 8).
fn rename_variant_record(schema: &mut AvroSchema, field_id: i32) {
    if let AvroSchema::Record(record) = schema
        && record
            .attributes
            .get(LOGICAL_TYPE)
            .and_then(Value::as_str)
            .is_some_and(|logical_type| logical_type == VARIANT_LOGICAL_TYPE)
    {
        record.name = Name::from(format!("r{field_id}").as_str());
    }
}

/// Whether an Avro record has the variant SHAPE: exactly two fields, `metadata` and `value`,
/// both of Avro type `bytes` — the exact checks in Java 1.10.0 `AvroSchemaUtil.isVariantSchema`
/// (field lookup is by NAME, so field order is not part of the shape check, matching Java's
/// `getField` calls).
fn is_variant_record_shape(record: &RecordSchema) -> bool {
    if record.fields.len() != 2 {
        return false;
    }
    let field_is_bytes = |name: &str| {
        record
            .lookup
            .get(name)
            .and_then(|&position| record.fields.get(position))
            .is_some_and(|field| matches!(field.schema, AvroSchema::Bytes))
    };
    field_is_bytes(VARIANT_METADATA_FIELD) && field_is_bytes(VARIANT_VALUE_FIELD)
}

pub(crate) fn avro_fixed_schema(len: usize) -> Result<AvroSchema> {
    Ok(AvroSchema::Fixed(FixedSchema {
        name: Name::new(format!("fixed_{len}").as_str())?,
        aliases: None,
        doc: None,
        size: len,
        attributes: Default::default(),
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
    // There are two representation for iceberg map in avro: array of key-value records, or map when keys are strings (optional),
    // ref: https://iceberg.apache.org/spec/#avro
    fn map_array(&mut self, array: &RecordSchema, key: Self::T, value: Self::T) -> Result<Self::T>;

    fn primitive(&mut self, schema: &AvroSchema) -> Result<Self::T>;

    /// Called for a record annotated with the `variant` logical type (already shape-validated).
    ///
    /// Default mirrors Java 1.10.0 `AvroSchemaVisitor.variant`'s
    /// `UnsupportedOperationException("Unsupported type: variant")`. Unlike Java's 3-arg
    /// `variant(Schema, T, T)`, the metadata/value child results are not computed — both children
    /// are statically `bytes` and Java's only overrider (`SchemaToType.variant`) ignores them.
    fn variant(&mut self, _variant: &RecordSchema) -> Result<Self::T> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Unsupported type: variant",
        ))
    }
}

/// Visit avro schema in post order visitor.
pub(crate) fn visit<V: AvroSchemaVisitor>(schema: &AvroSchema, visitor: &mut V) -> Result<V::T> {
    match schema {
        AvroSchema::Record(record) => {
            // A record carrying the `variant` logical type is the schema of an Iceberg variant
            // column, not a struct. Mirrors Java 1.10.0 `AvroSchemaVisitor.visit`: route through
            // the shape check (`isVariantSchema`, by-name so field order is irrelevant) to
            // `variant()`. The malformed-shape rejection uses Java's `Preconditions` message
            // ("Invalid variant record: %s") but is a DELIBERATE fail-loud divergence
            // (live-Java-probed by the reviewer): on Java's PARSE path, Avro's registered-factory
            // `fromSchemaIgnoreInvalid` silently DROPS an invalid variant logical type and reads
            // the record as a plain struct (Java's in-code precondition is unreachable for parsed
            // schemas). Silently misreading claimed-variant bytes as a struct is exactly the
            // corruption this boundary must refuse, so Rust rejects loudly instead.
            if record
                .attributes
                .get(LOGICAL_TYPE)
                .and_then(Value::as_str)
                .is_some_and(|logical_type| logical_type == VARIANT_LOGICAL_TYPE)
            {
                if !is_variant_record_shape(record) {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid variant record: {}", record.name),
                    ));
                }
                return visitor.variant(record);
            }

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

impl AvroSchemaToSchema {
    /// A convenient way to get element id(i32) from attributes.
    #[inline]
    fn get_element_id_from_attributes(
        attributes: &BTreeMap<String, Value>,
        name: &str,
    ) -> Result<i32> {
        attributes
            .get(name)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Can't convert avro array schema, missing element id.",
                )
            })?
            .as_i64()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Can't convert avro array schema, element id is not a valid i64 number.",
                )
            })?
            .try_into()
            .map_err(|_| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Can't convert avro array schema, element id is not a valid i32.",
                )
            })
    }
}

impl AvroSchemaVisitor for AvroSchemaToSchema {
    // Only `AvroSchema::Null` will return `None`
    type T = Option<Type>;

    fn record(
        &mut self,
        record: &RecordSchema,
        field_types: Vec<Option<Type>>,
    ) -> Result<Option<Type>> {
        let mut fields = Vec::with_capacity(field_types.len());
        for (avro_field, field_type) in record.fields.iter().zip_eq(field_types) {
            let field_id =
                Self::get_element_id_from_attributes(&avro_field.custom_attributes, FIELD_ID_PROP)?;

            let optional = is_avro_optional(&avro_field.schema);

            let mut field =
                NestedField::new(field_id, &avro_field.name, field_type.unwrap(), !optional);

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
        let element_field_id = Self::get_element_id_from_attributes(&array.attributes, ELEMENT_ID)?;
        let element_field = NestedField::list_element(
            element_field_id,
            item.unwrap(),
            !is_avro_optional(&array.items),
        )
        .into();
        Ok(Some(Type::List(ListType { element_field })))
    }

    fn map(&mut self, map: &MapSchema, value: Option<Type>) -> Result<Option<Type>> {
        let key_field_id = Self::get_element_id_from_attributes(&map.attributes, KEY_ID)?;
        let key_field =
            NestedField::map_key_element(key_field_id, Type::Primitive(PrimitiveType::String));
        let value_field_id = Self::get_element_id_from_attributes(&map.attributes, VALUE_ID)?;
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

    /// A shape-validated variant record converts to the Iceberg variant type — Java 1.10.0
    /// `SchemaToType.variant` returns `Types.VariantType.get()`.
    fn variant(&mut self, _variant: &RecordSchema) -> Result<Option<Type>> {
        Ok(Some(Type::Variant))
    }

    fn primitive(&mut self, schema: &AvroSchema) -> Result<Option<Type>> {
        let schema_type = match schema {
            AvroSchema::Decimal(decimal) => {
                Type::decimal(decimal.precision as u32, decimal.scale as u32)?
            }
            AvroSchema::Date => Type::Primitive(PrimitiveType::Date),
            AvroSchema::TimeMicros => Type::Primitive(PrimitiveType::Time),
            AvroSchema::TimestampMicros => Type::Primitive(PrimitiveType::Timestamp),
            AvroSchema::TimestampNanos => Type::Primitive(PrimitiveType::TimestampNs),
            AvroSchema::Boolean => Type::Primitive(PrimitiveType::Boolean),
            AvroSchema::Int => Type::Primitive(PrimitiveType::Int),
            AvroSchema::Long => Type::Primitive(PrimitiveType::Long),
            AvroSchema::Float => Type::Primitive(PrimitiveType::Float),
            AvroSchema::Double => Type::Primitive(PrimitiveType::Double),
            AvroSchema::Uuid => Type::Primitive(PrimitiveType::Uuid),
            AvroSchema::String | AvroSchema::Enum(_) => Type::Primitive(PrimitiveType::String),
            AvroSchema::Fixed(fixed) => Type::Primitive(PrimitiveType::Fixed(fixed.size as u64)),
            AvroSchema::Bytes => Type::Primitive(PrimitiveType::Binary),
            AvroSchema::Null => return Ok(None),
            _ => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Unable to convert avro {schema} to iceberg primitive type.",
                ));
            }
        };

        Ok(Some(schema_type))
    }

    fn map_array(
        &mut self,
        array: &RecordSchema,
        key: Option<Type>,
        value: Option<Type>,
    ) -> Result<Self::T> {
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
        let key_id = Self::get_element_id_from_attributes(
            &array.fields[0].custom_attributes,
            FIELD_ID_PROP,
        )?;
        let value_id = Self::get_element_id_from_attributes(
            &array.fields[1].custom_attributes,
            FIELD_ID_PROP,
        )?;
        let key_field = NestedField::map_key_element(key_id, key);
        let value_field = NestedField::map_value_element(
            value_id,
            value,
            !is_avro_optional(&array.fields[1].schema),
        );
        Ok(Some(Type::Map(MapType {
            key_field: key_field.into(),
            value_field: value_field.into(),
        })))
    }
}

// # TODO
// Fix this when we have used `avro_schema_to_schema` inner.
#[allow(unused)]
/// Converts avro schema to iceberg schema.
pub(crate) fn avro_schema_to_schema(avro_schema: &AvroSchema) -> Result<Schema> {
    if let AvroSchema::Record(_) = avro_schema {
        let mut converter = AvroSchemaToSchema;
        let schema_type =
            visit(avro_schema, &mut converter)?.expect("Iceberg schema should not be none.");
        if let Type::Struct(s) = schema_type {
            Schema::builder()
                .with_fields(s.fields().iter().cloned())
                .build()
        } else {
            Err(Error::new(
                ErrorKind::Unexpected,
                format!("Expected to convert avro record schema to struct type, but {schema_type}"),
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

    use apache_avro::Schema as AvroSchema;
    use apache_avro::schema::{Namespace, UnionSchema};

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

    /// Help function to check schema conversion between avro and iceberg:
    /// 1. avro to iceberg
    /// 2. iceberg to avro
    /// 3. iceberg to avro to iceberg back
    fn check_schema_conversion(avro_schema: AvroSchema, iceberg_schema: Schema) {
        // 1. avro to iceberg
        let converted_iceberg_schema = avro_schema_to_schema(&avro_schema).unwrap();
        assert_eq!(iceberg_schema, converted_iceberg_schema);

        // 2. iceberg to avro
        let converted_avro_schema = schema_to_avro_schema(
            avro_schema.name().unwrap().fullname(Namespace::None),
            &iceberg_schema,
        )
        .unwrap();
        assert_eq!(avro_schema, converted_avro_schema);

        // 3.iceberg to avro to iceberg back
        let converted_avro_converted_iceberg_schema =
            avro_schema_to_schema(&converted_avro_schema).unwrap();
        assert_eq!(iceberg_schema, converted_avro_converted_iceberg_schema);
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
                .with_fields(vec![
                    NestedField::required(
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
                    .into(),
                ])
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
                .with_fields(vec![
                    NestedField::required(
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
                    .into(),
                ])
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
                .with_fields(vec![
                    NestedField::required(
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
                    .into(),
                ])
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
                "element-id": 101,
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
        }, {
            "name": "string_map",
            "type": {
                "type": "map",
                "values": ["null", "long"],
                "key-id": 108,
                "value-id": 109
            },
            "field-id": 107
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
                    Arc::new(NestedField::required(
                        107,
                        "string_map",
                        Type::Map(MapType {
                            key_field: NestedField::map_key_element(
                                108,
                                PrimitiveType::String.into(),
                            )
                            .into(),
                            value_field: NestedField::map_value_element(
                                109,
                                PrimitiveType::Long.into(),
                                false,
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

    /// Unwraps an optional (`[null, T]`) Avro schema to `T`, then expects a record.
    fn expect_record_schema(schema: &AvroSchema) -> &RecordSchema {
        let inner = match schema {
            AvroSchema::Union(union) => &union.variants()[1],
            other => other,
        };
        match inner {
            AvroSchema::Record(record) => record,
            other => panic!("expected an avro record, got {other:?}"),
        }
    }

    /// Asserts the Java 1.10.0 variant record shape (`TypeToSchema.variant`, bytecode-pinned):
    /// two required `bytes` fields `metadata` then `value`, the record named `expected_name`,
    /// and the `"logicalType": "variant"` attribute present.
    fn assert_variant_record_shape(record: &RecordSchema, expected_name: &str) {
        assert_eq!(record.name.name, expected_name, "variant record name");
        assert_eq!(
            record.fields.len(),
            2,
            "variant record has exactly 2 fields"
        );
        assert_eq!(record.fields[0].name, "metadata");
        assert!(matches!(record.fields[0].schema, AvroSchema::Bytes));
        assert_eq!(record.fields[1].name, "value");
        assert!(matches!(record.fields[1].schema, AvroSchema::Bytes));
        assert_eq!(
            record.attributes.get(LOGICAL_TYPE).and_then(Value::as_str),
            Some(VARIANT_LOGICAL_TYPE),
            "the variant record must carry the variant logical type"
        );
    }

    // RISK: the Avro shape of a variant column is Java 1.10.0's on-wire contract
    // (`TypeToSchema.variant`): a record `r<fieldId>` with REQUIRED bytes fields
    // `metadata`/`value` and logical type "variant". apache-avro's `Schema` equality ignores
    // custom attributes (where the logical type lives), so this test asserts the attribute and
    // shape EXPLICITLY in the Iceberg→Avro direction, for a variant at top level, nested in a
    // struct, in a list element, and in a (string-keyed) map value.
    #[test]
    fn test_variant_to_avro_emits_java_shape_in_all_placements() {
        let iceberg_schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "v", Type::Variant).into(),
                NestedField::required(
                    2,
                    "payload",
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(3, "nested_v", Type::Variant).into(),
                    ])),
                )
                .into(),
                NestedField::required(
                    4,
                    "events",
                    Type::List(ListType {
                        element_field: NestedField::list_element(5, Type::Variant, false).into(),
                    }),
                )
                .into(),
                NestedField::required(
                    6,
                    "tags",
                    Type::Map(MapType {
                        key_field: NestedField::map_key_element(
                            7,
                            Type::Primitive(PrimitiveType::String),
                        )
                        .into(),
                        value_field: NestedField::map_value_element(8, Type::Variant, false).into(),
                    }),
                )
                .into(),
            ])
            .build()
            .unwrap();

        let avro_schema = schema_to_avro_schema("avro_schema", &iceberg_schema).unwrap();
        let AvroSchema::Record(root) = &avro_schema else {
            panic!("root must be a record");
        };

        // Top-level field: record renamed to r1 (Java `r<fieldId>`).
        assert_variant_record_shape(expect_record_schema(&root.fields[0].schema), "r1");

        // Nested in a struct: the inner field's record is renamed to r3.
        let payload = expect_record_schema(&root.fields[1].schema);
        assert_variant_record_shape(expect_record_schema(&payload.fields[0].schema), "r3");

        // List element: renamed to r5.
        let AvroSchema::Array(array) = &root.fields[2].schema else {
            panic!("events must be an avro array");
        };
        assert_variant_record_shape(expect_record_schema(&array.items), "r5");

        // Map value: renamed to r8, exactly Java's name (live-probed: 1.10.0
        // `AvroSchemaUtil.convert` emits `"name" : "r8"` for a variant map value with value-id 8).
        let AvroSchema::Map(map) = &root.fields[3].schema else {
            panic!("tags must be an avro map");
        };
        assert_variant_record_shape(expect_record_schema(&map.types), "r8");
    }

    // RISK (live-Java-probed, reviewer): two variant-valued maps in one schema must emit two
    // DISTINCTLY-named records. With a shared fallback name ("variant" twice), the emitted JSON
    // violates Avro's unique-name rule and Java's `Schema.Parser` rejects it with
    // "Can't redefine: variant" — a cross-engine read failure for any schema with two or more
    // map-variant placements.
    #[test]
    fn test_two_map_variant_values_get_unique_record_names() {
        let map_of_variant = |key_id: i32, value_id: i32| {
            Type::Map(MapType {
                key_field: NestedField::map_key_element(
                    key_id,
                    Type::Primitive(PrimitiveType::String),
                )
                .into(),
                value_field: NestedField::map_value_element(value_id, Type::Variant, false).into(),
            })
        };
        let iceberg_schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "tags_a", map_of_variant(2, 3)).into(),
                NestedField::required(4, "tags_b", map_of_variant(5, 6)).into(),
            ])
            .build()
            .unwrap();

        let avro_schema = schema_to_avro_schema("avro_schema", &iceberg_schema).unwrap();
        let AvroSchema::Record(root) = &avro_schema else {
            panic!("root must be a record");
        };
        let AvroSchema::Map(map_a) = &root.fields[0].schema else {
            panic!("tags_a must be an avro map");
        };
        let AvroSchema::Map(map_b) = &root.fields[1].schema else {
            panic!("tags_b must be an avro map");
        };
        assert_variant_record_shape(expect_record_schema(&map_a.types), "r3");
        assert_variant_record_shape(expect_record_schema(&map_b.types), "r6");

        // The serialized JSON re-parses (no duplicate definitions) and converts back losslessly.
        let avro_json = serde_json::to_string(&avro_schema).expect("serialize avro schema");
        let reparsed = AvroSchema::parse_str(&avro_json).expect("re-parse the avro schema JSON");
        let round_tripped = avro_schema_to_schema(&reparsed).expect("convert back to iceberg");
        assert_eq!(iceberg_schema, round_tripped);
    }

    // RISK (live-Java-probed, reviewer): the variant shape check must be ORDER-INSENSITIVE —
    // Java 1.10.0 `AvroSchemaUtil.isVariantSchema` looks fields up BY NAME (`getField`), and a
    // registered-`VariantLogicalType` Java parse accepts a `value`-before-`metadata` record as a
    // variant column. An index-based Rust check would wrongly reject Java-readable schemas.
    #[test]
    fn test_value_before_metadata_variant_record_is_accepted() {
        let avro_schema = AvroSchema::parse_str(
            r#"
{
    "type": "record",
    "name": "avro_schema",
    "fields": [
        {
            "name": "v",
            "type": {
                "type": "record",
                "name": "r1",
                "fields": [
                    {"name": "value", "type": "bytes"},
                    {"name": "metadata", "type": "bytes"}
                ],
                "logicalType": "variant"
            },
            "field-id": 1
        }
    ]
}
"#,
        )
        .unwrap();

        let expected = Schema::builder()
            .with_fields(vec![NestedField::required(1, "v", Type::Variant).into()])
            .build()
            .unwrap();
        assert_eq!(avro_schema_to_schema(&avro_schema).unwrap(), expected);
    }

    // RISK: the WIRE round-trip — serializing the converted Avro schema to JSON must keep the
    // variant logical type (apache-avro writes record attributes back), and re-parsing + the
    // Avro→Iceberg conversion must land on `Type::Variant` again in every placement. A drop
    // anywhere in this loop would silently degrade a variant column to a plain 2-field struct
    // (whose field-id-less fields then fail conversion) on the next reader.
    #[test]
    fn test_variant_avro_json_round_trip_back_to_iceberg() {
        let iceberg_schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "v", Type::Variant).into(),
                NestedField::required(
                    4,
                    "events",
                    Type::List(ListType {
                        element_field: NestedField::list_element(5, Type::Variant, false).into(),
                    }),
                )
                .into(),
                NestedField::required(
                    6,
                    "tags",
                    Type::Map(MapType {
                        key_field: NestedField::map_key_element(
                            7,
                            Type::Primitive(PrimitiveType::String),
                        )
                        .into(),
                        value_field: NestedField::map_value_element(8, Type::Variant, false).into(),
                    }),
                )
                .into(),
            ])
            .build()
            .unwrap();

        let avro_schema = schema_to_avro_schema("avro_schema", &iceberg_schema).unwrap();
        let avro_json = serde_json::to_string(&avro_schema).expect("serialize avro schema");
        assert!(
            avro_json.contains(r#""logicalType":"variant""#),
            "the serialized avro schema must carry the variant logical type, got: {avro_json}"
        );

        let reparsed = AvroSchema::parse_str(&avro_json).expect("re-parse the avro schema JSON");
        let round_tripped = avro_schema_to_schema(&reparsed).expect("convert back to iceberg");
        assert_eq!(
            iceberg_schema, round_tripped,
            "the variant columns must survive the avro JSON round-trip"
        );
    }

    // RISK: the Avro→Iceberg direction must read JAVA-written schema JSON (the Java-shaped
    // document, not our own output): a `[null, record(logicalType=variant)]` field converts to an
    // optional `Type::Variant` column. Mirrors Java `SchemaToType.variant` returning
    // `VariantType.get()`.
    #[test]
    fn test_java_shaped_variant_avro_schema_converts_to_iceberg() {
        let avro_schema = AvroSchema::parse_str(
            r#"
{
    "type": "record",
    "name": "avro_schema",
    "fields": [
        {
            "name": "v",
            "type": ["null", {
                "type": "record",
                "name": "r1",
                "fields": [
                    {"name": "metadata", "type": "bytes"},
                    {"name": "value", "type": "bytes"}
                ],
                "logicalType": "variant"
            }],
            "default": null,
            "field-id": 1
        }
    ]
}
"#,
        )
        .unwrap();

        let expected = Schema::builder()
            .with_fields(vec![NestedField::optional(1, "v", Type::Variant).into()])
            .build()
            .unwrap();
        assert_eq!(avro_schema_to_schema(&avro_schema).unwrap(), expected);
    }

    // RISK: a record CLAIMING the variant logical type but with the wrong shape must be rejected
    // with Java's message text ("Invalid variant record: %s" — `AvroSchemaVisitor.visit` /
    // `VariantLogicalType.validate`), never silently treated as a struct or as a variant.
    // Deliberate fail-loud divergence: live Java's PARSE path drops the invalid logical type
    // (`fromSchemaIgnoreInvalid`) and silently reads a struct — see the `visit()` comment.
    #[test]
    fn test_malformed_variant_record_is_rejected() {
        // Wrong field count (missing `value`).
        let avro_schema = AvroSchema::parse_str(
            r#"
{
    "type": "record",
    "name": "avro_schema",
    "fields": [
        {
            "name": "v",
            "type": {
                "type": "record",
                "name": "r1",
                "fields": [
                    {"name": "metadata", "type": "bytes"}
                ],
                "logicalType": "variant"
            },
            "field-id": 1
        }
    ]
}
"#,
        )
        .unwrap();
        let error = avro_schema_to_schema(&avro_schema)
            .expect_err("a one-field variant record must be rejected");
        assert!(
            error.message().contains("Invalid variant record: r1"),
            "must carry Java's Invalid-variant-record message, got: {}",
            error.message()
        );

        // Right field names, wrong field type (`value` is a string, not bytes).
        let avro_schema = AvroSchema::parse_str(
            r#"
{
    "type": "record",
    "name": "avro_schema",
    "fields": [
        {
            "name": "v",
            "type": {
                "type": "record",
                "name": "r1",
                "fields": [
                    {"name": "metadata", "type": "bytes"},
                    {"name": "value", "type": "string"}
                ],
                "logicalType": "variant"
            },
            "field-id": 1
        }
    ]
}
"#,
        )
        .unwrap();
        let error = avro_schema_to_schema(&avro_schema)
            .expect_err("a non-bytes value field must be rejected");
        assert!(
            error.message().contains("Invalid variant record: r1"),
            "must carry Java's Invalid-variant-record message, got: {}",
            error.message()
        );
    }

    // RISK (no false positive): a PLAIN record that merely happens to have `metadata`/`value`
    // bytes fields — but NO variant logical type — must stay a struct. Java only routes through
    // `variant()` when `getLogicalType() instanceof VariantLogicalType`; shape alone must not
    // reclassify a user struct.
    #[test]
    fn test_plain_metadata_value_record_stays_a_struct() {
        let avro_schema = AvroSchema::parse_str(
            r#"
{
    "type": "record",
    "name": "avro_schema",
    "fields": [
        {
            "name": "v",
            "type": {
                "type": "record",
                "name": "r1",
                "fields": [
                    {"name": "metadata", "type": "bytes", "field-id": 2},
                    {"name": "value", "type": "bytes", "field-id": 3}
                ]
            },
            "field-id": 1
        }
    ]
}
"#,
        )
        .unwrap();

        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(
                    1,
                    "v",
                    Type::Struct(StructType::new(vec![
                        NestedField::required(
                            2,
                            "metadata",
                            Type::Primitive(PrimitiveType::Binary),
                        )
                        .into(),
                        NestedField::required(3, "value", Type::Primitive(PrimitiveType::Binary))
                            .into(),
                    ])),
                )
                .into(),
            ])
            .build()
            .unwrap();
        assert_eq!(avro_schema_to_schema(&avro_schema).unwrap(), expected);
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

    #[test]
    fn test_uuid_type() {
        let avro_schema = {
            AvroSchema::parse_str(
                r#"
            {"name": "test", "type": "fixed", "size": 16, "logicalType": "uuid"}
            "#,
            )
            .unwrap()
        };

        let mut converter = AvroSchemaToSchema;

        let iceberg_type = Type::from(PrimitiveType::Uuid);

        assert_eq!(
            iceberg_type,
            converter.primitive(&avro_schema).unwrap().unwrap()
        );
    }
}
