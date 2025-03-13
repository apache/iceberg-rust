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

//! Iceberg name mapping.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DefaultOnNull};

use crate::spec::schema::Schema;
use crate::spec::{
    visit_schema, ListType, MapType, NestedFieldRef, PrimitiveType, SchemaVisitor, StructType,
};
use crate::Result;

/// Iceberg fallback field name to ID mapping.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct NameMapping {
    pub root: Vec<MappedField>,
    pub id_to_field: HashMap<i32, MappedField>,
    pub name_to_field: HashMap<String, MappedField>,
}

impl NameMapping {
    pub fn new(
        root: Vec<MappedField>,
        id_to_field: HashMap<i32, MappedField>,
        name_to_field: HashMap<String, MappedField>,
    ) -> Self {
        Self {
            root,
            id_to_field,
            name_to_field,
        }
    }

    /// Parses name_mapping from JSON.
    pub fn parse_name_mapping(name_mapping: &str) -> Result<Self> {
        let parsed_name_mapping: NameMapping = serde_json::from_str(name_mapping)?;
        Ok(parsed_name_mapping)
    }

    /// Returns an index mapping id to `MappedField`` by visiting the schema.
    fn index_by_id(mapping: &[MappedField]) -> HashMap<i32, MappedField> {
        visit_name_mapping(mapping, &IndexById {})
    }

    /// Returns an index mapping names to `MappedField`` by visiting the schema.
    fn index_by_name(mapping: &[MappedField]) -> HashMap<String, MappedField> {
        visit_name_mapping(mapping, &IndexByName {})
    }

    pub fn create_mapping_from_schema(schema: &Schema) -> Self {
        let mapped_fields = visit_schema(schema, &mut CreateMapping).unwrap();

        let id_to_field = NameMapping::index_by_id(&mapped_fields);
        let name_to_field = NameMapping::index_by_name(&mapped_fields);

        NameMapping::new(mapped_fields, id_to_field, name_to_field)
    }
}

/// Maps field names to IDs.
#[serde_as]
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct MappedField {
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Unique identifier for field.
    pub field_id: Option<i32>,
    pub names: Vec<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde_as(deserialize_as = "DefaultOnNull")]
    pub fields: Vec<MappedField>,
}

impl MappedField {
    fn new(names: Vec<String>, fields: Vec<MappedField>, field_id: Option<i32>) -> Self {
        Self {
            field_id,
            names,
            fields,
        }
    }
}

/// A trait for visiting and transforming a name mapping
trait NameMappingVisitor {
    /// Aggregated result of `MappedField`s
    type S;
    /// Result type for processing one `MappedField`
    type T;

    /// Handles entire `NameMapping` field
    fn mapping(&self, field_result: Self::S) -> Self::S;

    /// Handles accessing multiple `MappedField`
    fn fields(&self, field_results: Vec<Self::T>) -> Self::S;

    /// Handles a single `MappedField`
    fn field(&self, field: &MappedField, field_result: Self::S) -> Self::T;
}

/// Recursively visits the entire name mapping using visitor
fn visit_name_mapping<V>(nm: &[MappedField], visitor: &V) -> V::S
where V: NameMappingVisitor {
    let root_result = visit_fields(nm, visitor);
    visitor.mapping(root_result)
}

/// Recursively visits a slice of mapped fields using visitor
fn visit_fields<V>(fields: &[MappedField], visitor: &V) -> V::S
where V: NameMappingVisitor {
    let mut results: Vec<V::T> = Vec::new();

    for field in fields {
        let child_result = visit_fields(&field.fields, visitor);
        let field_result = visitor.field(field, child_result);
        results.push(field_result);
    }

    visitor.fields(results)
}

struct IndexByName {}

impl NameMappingVisitor for IndexByName {
    type S = HashMap<String, MappedField>;
    type T = HashMap<String, MappedField>;

    fn mapping(&self, field_result: HashMap<String, MappedField>) -> HashMap<String, MappedField> {
        field_result
    }

    fn fields(
        &self,
        field_results: Vec<HashMap<String, MappedField>>,
    ) -> HashMap<String, MappedField> {
        field_results
            .into_iter()
            .fold(HashMap::new(), |mut acc, map| {
                acc.extend(map);
                acc
            })
    }

    fn field(
        &self,
        field: &MappedField,
        field_result: HashMap<String, MappedField>,
    ) -> HashMap<String, MappedField> {
        let mut result = HashMap::new();

        for name in &field.names {
            for (child_key, child_field) in field_result.iter() {
                let composite_key = format!("{}.{}", name, child_key);
                result.insert(composite_key, child_field.clone());
            }
        }

        for name in &field.names {
            result.insert(name.clone(), field.clone());
        }

        result
    }
}

struct IndexById {}

impl NameMappingVisitor for IndexById {
    type S = HashMap<i32, MappedField>;
    type T = HashMap<i32, MappedField>;

    fn mapping(&self, field_result: HashMap<i32, MappedField>) -> HashMap<i32, MappedField> {
        field_result
    }

    fn fields(&self, field_results: Vec<HashMap<i32, MappedField>>) -> HashMap<i32, MappedField> {
        field_results
            .into_iter()
            .fold(HashMap::new(), |mut acc, map| {
                acc.extend(map);
                acc
            })
    }

    fn field(
        &self,
        field: &MappedField,
        _field_result: HashMap<i32, MappedField>,
    ) -> HashMap<i32, MappedField> {
        let mut result = HashMap::new();

        while let Some(id) = &field.field_id {
            result.insert(*id, field.clone());
        }

        result
    }
}

struct CreateMapping;

impl SchemaVisitor for CreateMapping {
    type T = Vec<MappedField>;

    fn schema(&mut self, _schema: &Schema, value: Self::T) -> Result<Self::T> {
        Ok(value)
    }

    fn field(&mut self, _field: &NestedFieldRef, value: Self::T) -> Result<Self::T> {
        Ok(value)
    }

    fn r#struct(&mut self, struct_type: &StructType, results: Vec<Self::T>) -> Result<Self::T> {
        let mapped_fields = struct_type
            .fields()
            .iter()
            .zip(results)
            .map(|(field, result)| {
                MappedField::new(vec![field.name.clone()], result, Some(field.id))
            })
            .collect::<Vec<MappedField>>();

        Ok(mapped_fields)
    }

    fn list(&mut self, list: &ListType, value: Self::T) -> Result<Self::T> {
        Ok(vec![MappedField::new(
            vec!["element".to_string()],
            value,
            Some(list.element_field.id),
        )])
    }

    fn map(&mut self, map: &MapType, key_value: Self::T, value: Self::T) -> Result<Self::T> {
        Ok(vec![
            MappedField::new(vec!["key".to_string()], key_value, Some(map.key_field.id)),
            MappedField::new(vec!["value".to_string()], value, Some(map.value_field.id)),
        ])
    }

    fn primitive(&mut self, _p: &PrimitiveType) -> Result<Self::T> {
        Ok([].to_vec())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_json_mapped_field_deserialization() {
        let expected = MappedField {
            field_id: Some(1),
            names: vec!["id".to_string(), "record_id".to_string()],
            fields: vec![],
        };
        let mapped_field = r#"
        {
            "field-id": 1,
            "names": ["id", "record_id"]
        }
        "#;

        let mapped_field: MappedField = serde_json::from_str(mapped_field).unwrap();
        assert_eq!(mapped_field, expected);

        let mapped_field_with_null_fields = r#"
        {
            "field-id": 1,
            "names": ["id", "record_id"],
            "fields": null
        }
        "#;

        let mapped_field_with_null_fields: MappedField =
            serde_json::from_str(mapped_field_with_null_fields).unwrap();
        assert_eq!(mapped_field_with_null_fields, expected);
    }

    #[test]
    fn test_json_mapped_field_no_names_deserialization() {
        let expected = MappedField {
            field_id: Some(1),
            names: vec![],
            fields: vec![],
        };
        let mapped_field = r#"
        {
            "field-id": 1,
            "names": []
        }
        "#;

        let mapped_field: MappedField = serde_json::from_str(mapped_field).unwrap();
        assert_eq!(mapped_field, expected);

        let mapped_field_with_null_fields = r#"
        {
            "field-id": 1,
            "names": [],
            "fields": null
        }
        "#;

        let mapped_field_with_null_fields: MappedField =
            serde_json::from_str(mapped_field_with_null_fields).unwrap();
        assert_eq!(mapped_field_with_null_fields, expected);
    }

    #[test]
    fn test_json_mapped_field_no_field_id_deserialization() {
        let expected = MappedField {
            field_id: None,
            names: vec!["id".to_string(), "record_id".to_string()],
            fields: vec![],
        };
        let mapped_field = r#"
        {
            "names": ["id", "record_id"]
        }
        "#;

        let mapped_field: MappedField = serde_json::from_str(mapped_field).unwrap();
        assert_eq!(mapped_field, expected);

        let mapped_field_with_null_fields = r#"
        {
            "names": ["id", "record_id"],
            "fields": null
        }
        "#;

        let mapped_field_with_null_fields: MappedField =
            serde_json::from_str(mapped_field_with_null_fields).unwrap();
        assert_eq!(mapped_field_with_null_fields, expected);
    }

    #[test]
    fn test_json_name_mapping_deserialization() {
        let json_str = r#"
        {
            "root": [
                {
                    "field-id": 1,
                    "names": ["id", "record_id"],
                    "fields": []
                },
                {
                    "field-id": 2,
                    "names": ["data"],
                    "fields": []
                },
                {
                    "field-id": 3,
                    "names": ["location"],
                    "fields": [
                        {
                            "field-id": 4,
                            "names": ["latitude", "lat"],
                            "fields": []
                        },
                        {
                            "field-id": 5,
                            "names": ["longitude", "long"],
                            "fields": []
                        }
                    ]
                }
            ],
            "id-to-field": {
                "1": {
                    "field-id": 1,
                    "names": ["id", "record_id"],
                    "fields": []
                },
                "2": {
                    "field-id": 2,
                    "names": ["data"],
                    "fields": []
                },
                "3": {
                    "field-id": 3,
                    "names": ["location"],
                    "fields": [
                        {
                            "field-id": 4,
                            "names": ["latitude", "lat"],
                            "fields": []
                        },
                        {
                            "field-id": 5,
                            "names": ["longitude", "long"],
                            "fields": []
                        }
                    ]
                },
                "4": {
                    "field-id": 4,
                    "names": ["latitude", "lat"],
                    "fields": []
                },
                "5": {
                    "field-id": 5,
                    "names": ["longitude", "long"],
                    "fields": []
                }
            },
            "name-to-field": {
                "id": {
                    "field-id": 1,
                    "names": ["id", "record_id"],
                    "fields": []
                },
                "record_id": {
                    "field-id": 1,
                    "names": ["id", "record_id"],
                    "fields": []
                },
                "data": {
                    "field-id": 2,
                    "names": ["data"],
                    "fields": []
                },
                "location": {
                    "field-id": 3,
                    "names": ["location"],
                    "fields": [
                        {
                            "field-id": 4,
                            "names": ["latitude", "lat"],
                            "fields": []
                        },
                        {
                            "field-id": 5,
                            "names": ["longitude", "long"],
                            "fields": []
                        }
                    ]
                },
                "latitude": {
                    "field-id": 4,
                    "names": ["latitude", "lat"],
                    "fields": []
                },
                "lat": {
                    "field-id": 4,
                    "names": ["latitude", "lat"],
                    "fields": []
                },
                "longitude": {
                    "field-id": 5,
                    "names": ["longitude", "long"],
                    "fields": []
                },
                "long": {
                    "field-id": 5,
                    "names": ["longitude", "long"],
                    "fields": []
                }
            }
        }
        "#;

        let name_mapping: NameMapping = serde_json::from_str(json_str).unwrap();

        let expected = NameMapping {
            root: vec![
                MappedField {
                    field_id: Some(1),
                    names: vec!["id".into(), "record_id".into()],
                    fields: vec![],
                },
                MappedField {
                    field_id: Some(2),
                    names: vec!["data".into()],
                    fields: vec![],
                },
                MappedField {
                    field_id: Some(3),
                    names: vec!["location".into()],
                    fields: vec![
                        MappedField {
                            field_id: Some(4),
                            names: vec!["latitude".into(), "lat".into()],
                            fields: vec![],
                        },
                        MappedField {
                            field_id: Some(5),
                            names: vec!["longitude".into(), "long".into()],
                            fields: vec![],
                        },
                    ],
                },
            ],
            id_to_field: {
                let mut map = HashMap::new();
                map.insert(1, MappedField {
                    field_id: Some(1),
                    names: vec!["id".into(), "record_id".into()],
                    fields: vec![],
                });
                map.insert(2, MappedField {
                    field_id: Some(2),
                    names: vec!["data".into()],
                    fields: vec![],
                });
                map.insert(3, MappedField {
                    field_id: Some(3),
                    names: vec!["location".into()],
                    fields: vec![
                        MappedField {
                            field_id: Some(4),
                            names: vec!["latitude".into(), "lat".into()],
                            fields: vec![],
                        },
                        MappedField {
                            field_id: Some(5),
                            names: vec!["longitude".into(), "long".into()],
                            fields: vec![],
                        },
                    ],
                });
                map.insert(4, MappedField {
                    field_id: Some(4),
                    names: vec!["latitude".into(), "lat".into()],
                    fields: vec![],
                });
                map.insert(5, MappedField {
                    field_id: Some(5),
                    names: vec!["longitude".into(), "long".into()],
                    fields: vec![],
                });
                map
            },
            name_to_field: {
                let mut map = HashMap::new();
                map.insert("id".into(), MappedField {
                    field_id: Some(1),
                    names: vec!["id".into(), "record_id".into()],
                    fields: vec![],
                });
                map.insert("record_id".into(), MappedField {
                    field_id: Some(1),
                    names: vec!["id".into(), "record_id".into()],
                    fields: vec![],
                });
                map.insert("data".into(), MappedField {
                    field_id: Some(2),
                    names: vec!["data".into()],
                    fields: vec![],
                });
                map.insert("location".into(), MappedField {
                    field_id: Some(3),
                    names: vec!["location".into()],
                    fields: vec![
                        MappedField {
                            field_id: Some(4),
                            names: vec!["latitude".into(), "lat".into()],
                            fields: vec![],
                        },
                        MappedField {
                            field_id: Some(5),
                            names: vec!["longitude".into(), "long".into()],
                            fields: vec![],
                        },
                    ],
                });
                map.insert("latitude".into(), MappedField {
                    field_id: Some(4),
                    names: vec!["latitude".into(), "lat".into()],
                    fields: vec![],
                });
                map.insert("lat".into(), MappedField {
                    field_id: Some(4),
                    names: vec!["latitude".into(), "lat".into()],
                    fields: vec![],
                });
                map.insert("longitude".into(), MappedField {
                    field_id: Some(5),
                    names: vec!["longitude".into(), "long".into()],
                    fields: vec![],
                });
                map.insert("long".into(), MappedField {
                    field_id: Some(5),
                    names: vec!["longitude".into(), "long".into()],
                    fields: vec![],
                });
                map
            },
        };

        assert_eq!(name_mapping, expected);
    }

    #[test]
    fn test_json_name_mapping_serialization() {
        let name_mapping = NameMapping {
            root: vec![
                MappedField {
                    field_id: None,
                    names: vec!["foo".into()],
                    fields: vec![],
                },
                MappedField {
                    field_id: Some(2),
                    names: vec!["bar".into()],
                    fields: vec![],
                },
                MappedField {
                    field_id: Some(3),
                    names: vec!["baz".into()],
                    fields: vec![],
                },
                MappedField {
                    field_id: Some(4),
                    names: vec!["qux".into()],
                    fields: vec![MappedField {
                        field_id: Some(5),
                        names: vec!["element".into()],
                        fields: vec![],
                    }],
                },
                MappedField {
                    field_id: Some(6),
                    names: vec!["quux".into()],
                    fields: vec![
                        MappedField {
                            field_id: Some(7),
                            names: vec!["key".into()],
                            fields: vec![],
                        },
                        MappedField {
                            field_id: Some(8),
                            names: vec!["value".into()],
                            fields: vec![
                                MappedField {
                                    field_id: Some(9),
                                    names: vec!["key".into()],
                                    fields: vec![],
                                },
                                MappedField {
                                    field_id: Some(10),
                                    names: vec!["value".into()],
                                    fields: vec![],
                                },
                            ],
                        },
                    ],
                },
                MappedField {
                    field_id: Some(11),
                    names: vec!["location".into()],
                    fields: vec![MappedField {
                        field_id: Some(12),
                        names: vec!["element".into()],
                        fields: vec![
                            MappedField {
                                field_id: Some(13),
                                names: vec!["latitude".into()],
                                fields: vec![],
                            },
                            MappedField {
                                field_id: Some(14),
                                names: vec!["longitude".into()],
                                fields: vec![],
                            },
                        ],
                    }],
                },
                MappedField {
                    field_id: Some(15),
                    names: vec!["person".into()],
                    fields: vec![
                        MappedField {
                            field_id: Some(16),
                            names: vec!["name".into()],
                            fields: vec![],
                        },
                        MappedField {
                            field_id: Some(17),
                            names: vec!["age".into()],
                            fields: vec![],
                        },
                    ],
                },
            ],
            id_to_field: {
                let mut m = HashMap::new();
                m.insert(2, MappedField {
                    field_id: Some(2),
                    names: vec!["bar".into()],
                    fields: vec![],
                });
                m.insert(3, MappedField {
                    field_id: Some(3),
                    names: vec!["baz".into()],
                    fields: vec![],
                });
                m
            },
            name_to_field: {
                let mut m = HashMap::new();
                m.insert("foo".into(), MappedField {
                    field_id: None,
                    names: vec!["foo".into()],
                    fields: vec![],
                });
                m.insert("bar".into(), MappedField {
                    field_id: Some(2),
                    names: vec!["bar".into()],
                    fields: vec![],
                });
                m.insert("baz".into(), MappedField {
                    field_id: Some(3),
                    names: vec!["baz".into()],
                    fields: vec![],
                });
                m
            },
        };

        let serialized = serde_json::to_string(&name_mapping).unwrap();
        let expected = json!({
            "root": [
                { "names": ["foo"] },
                { "field-id": 2, "names": ["bar"] },
                { "field-id": 3, "names": ["baz"] },
                { "field-id": 4, "names": ["qux"], "fields": [
                    { "field-id": 5, "names": ["element"] }
                ] },
                { "field-id": 6, "names": ["quux"], "fields": [
                    { "field-id": 7, "names": ["key"] },
                    { "field-id": 8, "names": ["value"], "fields": [
                        { "field-id": 9, "names": ["key"] },
                        { "field-id": 10, "names": ["value"] }
                    ] }
                ] },
                { "field-id": 11, "names": ["location"], "fields": [
                    { "field-id": 12, "names": ["element"], "fields": [
                        { "field-id": 13, "names": ["latitude"] },
                        { "field-id": 14, "names": ["longitude"] }
                    ] }
                ] },
                { "field-id": 15, "names": ["person"], "fields": [
                    { "field-id": 16, "names": ["name"] },
                    { "field-id": 17, "names": ["age"] }
                ] }
            ],
            "id-to-field": serde_json::Value::Null,
            "name-to-field": serde_json::Value::Null
        });

        let serialized_value: serde_json::Value = serde_json::from_str(&serialized).unwrap();
        assert_eq!(serialized_value.get("root"), expected.get("root"));
    }
}
