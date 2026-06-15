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

//! Helper types for Schema serialization/deserialization.
//!
//! For deserialization the input first gets read into either the [SchemaV1] or [SchemaV2] struct
//! and then converted into the [Schema] struct. Serialization works the other way around.
//! [SchemaV1] and [SchemaV2] are internal structs only used for serialization and deserialization.

use serde::Deserialize;
use serde::Serialize;

use super::{DEFAULT_SCHEMA_ID, Schema};
use crate::spec::StructType;
use crate::{Error, Result};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
// IMPORTANT: V2 must precede V1. Serde untagged tries variants in declaration order;
// V2's required `schema_id: i32` distinguishes it from V1's optional field.
// Swapping the order will silently break deserialization.
pub(super) enum SchemaEnum {
    V2(SchemaV2),
    V1(SchemaV1),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// Defines the structure of a v2 schema for serialization/deserialization
pub(crate) struct SchemaV2 {
    pub schema_id: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier_field_ids: Option<Vec<i32>>,
    #[serde(flatten)]
    pub fields: StructType,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// Defines the structure of a v1 schema for serialization/deserialization.
///
/// This is a permissive fallback shape for JSON lacking `schema-id`,
/// not a faithful V1 spec representation. It accepts `identifier-field-ids`
/// even though the V1 spec doesn't define them (Postel's law).
pub(crate) struct SchemaV1 {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier_field_ids: Option<Vec<i32>>,
    #[serde(flatten)]
    pub fields: StructType,
}

impl TryFrom<SchemaEnum> for Schema {
    type Error = Error;
    fn try_from(value: SchemaEnum) -> Result<Self> {
        match value {
            SchemaEnum::V2(value) => value.try_into(),
            SchemaEnum::V1(value) => value.try_into(),
        }
    }
}

// Always serialize as V2. The Iceberg spec treats V2 as the canonical format;
// V1 schemas are upgraded on read. Schema-id defaults to 0 when absent from
// the source JSON.
impl From<Schema> for SchemaEnum {
    fn from(value: Schema) -> Self {
        SchemaEnum::V2(value.into())
    }
}

impl TryFrom<SchemaV2> for Schema {
    type Error = Error;
    fn try_from(value: SchemaV2) -> Result<Self> {
        Schema::builder()
            .with_schema_id(value.schema_id)
            .with_fields(value.fields.fields().iter().cloned())
            .with_identifier_field_ids(value.identifier_field_ids.unwrap_or_default())
            .build()
    }
}

impl TryFrom<SchemaV1> for Schema {
    type Error = Error;
    fn try_from(value: SchemaV1) -> Result<Self> {
        Schema::builder()
            .with_schema_id(value.schema_id.unwrap_or(DEFAULT_SCHEMA_ID))
            .with_fields(value.fields.fields().iter().cloned())
            .with_identifier_field_ids(value.identifier_field_ids.unwrap_or_default())
            .build()
    }
}

// Note: alias_to_id is intentionally excluded from serialization.
// Per Iceberg spec, aliases are not part of the JSON schema representation.
// They must be reconstructed from external sources after deserialization.
impl From<Schema> for SchemaV2 {
    fn from(value: Schema) -> Self {
        SchemaV2 {
            schema_id: value.schema_id,
            identifier_field_ids: if value.identifier_field_ids.is_empty() {
                None
            } else {
                let mut ids: Vec<i32> = value.identifier_field_ids.into_iter().collect();
                ids.sort_unstable();
                Some(ids)
            },
            fields: value.r#struct,
        }
    }
}

impl From<Schema> for SchemaV1 {
    fn from(value: Schema) -> Self {
        SchemaV1 {
            schema_id: Some(value.schema_id),
            identifier_field_ids: if value.identifier_field_ids.is_empty() {
                None
            } else {
                let mut ids: Vec<i32> = value.identifier_field_ids.into_iter().collect();
                ids.sort_unstable();
                Some(ids)
            },
            fields: value.r#struct,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::schema::tests::table_schema_simple;
    use crate::spec::{PrimitiveType, Type};

    fn check_schema_serde(json: &str, expected_schema: Schema) {
        let deserialized_schema: Schema = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized_schema, expected_schema);

        let serialized_json = serde_json::to_string(&expected_schema).unwrap();
        let round_tripped: Schema = serde_json::from_str(&serialized_json).unwrap();

        assert_eq!(round_tripped, deserialized_schema);
    }

    #[test]
    fn test_serde_with_schema_id() {
        let (schema, record) = table_schema_simple();

        check_schema_serde(record, schema.clone());

        // Verify it deserializes as V2 (has schema-id)
        let schema_enum: SchemaEnum = serde_json::from_str(record).unwrap();
        assert!(matches!(schema_enum, SchemaEnum::V2(_)));
    }

    #[test]
    fn test_serde_without_schema_id() {
        let (schema, _) = table_schema_simple();

        // Construct a V1 JSON without schema-id using programmatic manipulation
        let mut json_value: serde_json::Value = serde_json::to_value(&schema).unwrap();
        json_value.as_object_mut().unwrap().remove("schema-id");
        let v1_json = serde_json::to_string(&json_value).unwrap();

        let mut expected = schema;
        expected.schema_id = DEFAULT_SCHEMA_ID;

        check_schema_serde(&v1_json, expected);

        // Verify it deserializes as V1 (no schema-id)
        let schema_enum: SchemaEnum = serde_json::from_str(&v1_json).unwrap();
        assert!(matches!(schema_enum, SchemaEnum::V1(_)));
    }

    #[test]
    fn test_schema_v2_fields() {
        let record = r#"
        {
            "type": "struct",
            "schema-id": 1,
            "fields": [ {
            "id": 1,
            "name": "id",
            "required": true,
            "type": "uuid"
            }, {
            "id": 2,
            "name": "data",
            "required": false,
            "type": "int"
            } ]
            }
        "#;

        let result: SchemaV2 = serde_json::from_str(record).unwrap();
        assert_eq!(1, result.schema_id);
        assert_eq!(
            Box::new(Type::Primitive(PrimitiveType::Uuid)),
            result.fields[0].field_type
        );
        assert_eq!(1, result.fields[0].id);
        assert!(result.fields[0].required);

        assert_eq!(
            Box::new(Type::Primitive(PrimitiveType::Int)),
            result.fields[1].field_type
        );
        assert_eq!(2, result.fields[1].id);
        assert!(!result.fields[1].required);
    }

    #[test]
    fn test_derived_fields_work_after_round_trip() {
        let (schema, record) = table_schema_simple();
        let deserialized: Schema = serde_json::from_str(record).unwrap();

        // Verify lookup by name works (exercises name_to_id index)
        assert_eq!(
            deserialized.field_by_name("foo").map(|f| f.id),
            schema.field_by_name("foo").map(|f| f.id)
        );
        assert_eq!(
            deserialized.field_by_name("bar").map(|f| f.id),
            schema.field_by_name("bar").map(|f| f.id)
        );

        // Verify field_by_id works (exercises id_to_field index)
        assert!(deserialized.field_by_id(1).is_some());
        assert!(deserialized.field_by_id(2).is_some());
        assert!(deserialized.field_by_id(999).is_none());
    }

    #[test]
    fn test_identifier_field_ids_sorted_on_serialization() {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![3, 1, 2])
            .with_fields(vec![
                crate::spec::NestedField::required(1, "a", Type::Primitive(PrimitiveType::Int))
                    .into(),
                crate::spec::NestedField::required(2, "b", Type::Primitive(PrimitiveType::Int))
                    .into(),
                crate::spec::NestedField::required(3, "c", Type::Primitive(PrimitiveType::Int))
                    .into(),
            ])
            .build()
            .unwrap();

        let serialized = serde_json::to_string(&schema).unwrap();
        let json_value: serde_json::Value = serde_json::from_str(&serialized).unwrap();
        let ids = json_value["identifier-field-ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_i64().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![1, 2, 3]);
    }
}
