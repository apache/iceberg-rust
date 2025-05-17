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

mod visitor;

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_with::{DefaultOnNull, serde_as};
use visitor::NameMappingVisitor;

use crate::spec::{
    LIST_FIELD_NAME, ListType, MAP_KEY_FIELD_NAME, MAP_VALUE_FIELD_NAME, MapType, NestedFieldRef,
    PrimitiveType, Schema, SchemaVisitor, StructType, visit_schema,
};
use crate::{Error, Result};

/// Property name for name mapping.
pub const DEFAULT_SCHEMA_NAME_MAPPING: &str = "schema.name-mapping.default";

/// Iceberg fallback field name to ID mapping.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(transparent)]
pub struct NameMapping {
    // this is the one field that actually gets round‑tripped
    root: Vec<Arc<MappedField>>,
    // both of these get skipped during (de)serialization and default to empty
    #[serde(skip)]
    id_to_field: HashMap<i32, Arc<MappedField>>,
    #[serde(skip)]
    name_to_field: HashMap<String, Arc<MappedField>>,
}

impl NameMapping {
    /// Creates new `NameMapping` instance
    pub fn new(schema: &Schema) -> Self {
        // Create `MappedField` instances by visiting schema.
        let root = visit_schema(schema, &mut CreateMapping).unwrap();

        let id_to_field = NameMapping::index_by_id(&root);
        let name_to_field = NameMapping::index_by_name(&root);

        Self {
            root,
            id_to_field,
            name_to_field,
        }
    }

    /// Get a reference to fields which are to be mapped from name to field ID.
    pub fn fields(&self) -> &[Arc<MappedField>] {
        &self.root
    }

    /// Parses name_mapping from JSON.
    pub fn parse_name_mapping(name_mapping: &str) -> Result<Self> {
        let parsed_name_mapping: NameMapping = serde_json::from_str(name_mapping)?;
        Ok(parsed_name_mapping)
    }

    /// Returns an index mapping id to `MappedField`` by visiting the schema.
    fn index_by_id(mapping: &Vec<Arc<MappedField>>) -> HashMap<i32, Arc<MappedField>> {
        visit_name_mapping(mapping, &IndexById {}).unwrap()
    }

    /// Returns an index mapping names to `MappedField` by visiting the schema.
    fn index_by_name(mapping: &Vec<Arc<MappedField>>) -> HashMap<String, Arc<MappedField>> {
        visit_name_mapping(mapping, &IndexByName {}).unwrap()
    }
}

impl FromStr for NameMapping {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        NameMapping::parse_name_mapping(s)
    }
}

/// Maps field names to IDs.
#[serde_as]
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct MappedField {
    #[serde(skip_serializing_if = "Option::is_none")]
    field_id: Option<i32>,
    names: Vec<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde_as(deserialize_as = "DefaultOnNull")]
    fields: Vec<Arc<MappedField>>,
}

impl MappedField {
    /// Create a new [`MappedField`].
    pub fn new(field_id: Option<i32>, names: Vec<String>, fields: Vec<Arc<MappedField>>) -> Self {
        Self {
            field_id,
            names,
            fields,
        }
    }

    /// Iceberg field ID when a field's name is present within `names`.
    pub fn field_id(&self) -> Option<i32> {
        self.field_id
    }

    /// Get a reference to names for a mapped field.
    pub fn names(&self) -> &[String] {
        &self.names
    }
}

/// Recursively visits the entire name mapping using visitor
fn visit_name_mapping<V>(name_mapping: &Vec<Arc<MappedField>>, visitor: &V) -> Result<V::S>
where
    V: NameMappingVisitor,
    V::S: IntoIterator + FromIterator<<V::S as IntoIterator>::Item>,
{
    let root_result = visit_fields(name_mapping, visitor);
    Ok(visitor.mapping(root_result))
}

/// Recursively visits a slice of mapped fields using visitor
fn visit_fields<V>(fields: &Vec<Arc<MappedField>>, visitor: &V) -> V::S
where
    V: NameMappingVisitor,
    V::S: IntoIterator + FromIterator<<V::S as IntoIterator>::Item>,
{
    let mut results: Vec<V::S> = Vec::with_capacity(fields.len());
    for f in fields {
        let child_s = visit_fields(&f.fields, visitor);
        let this_s = visitor.field(f, child_s);
        results.push(this_s);
    }

    let merged: V::S = results.into_iter().flat_map(|m| m.into_iter()).collect();

    visitor.mapping(merged)
}

struct IndexByName {}

impl NameMappingVisitor for IndexByName {
    type S = HashMap<String, Arc<MappedField>>;

    fn mapping(
        &self,
        field_result: HashMap<String, Arc<MappedField>>,
    ) -> HashMap<String, Arc<MappedField>> {
        field_result
    }

    fn field(
        &self,
        field: &MappedField,
        child_result: HashMap<String, Arc<MappedField>>,
    ) -> HashMap<String, Arc<MappedField>> {
        let mut result = child_result;

        for name in &field.names {
            for (child_key, child_field) in result.clone().iter() {
                let composite_key = format!("{}.{}", name, child_key);
                result.insert(composite_key, child_field.clone());
            }
        }

        for name in &field.names {
            result.insert(name.clone(), Arc::new(field.clone()));
        }
        result
    }
}

struct IndexById {}

impl NameMappingVisitor for IndexById {
    type S = HashMap<i32, Arc<MappedField>>;

    fn mapping(
        &self,
        field_result: HashMap<i32, Arc<MappedField>>,
    ) -> HashMap<i32, Arc<MappedField>> {
        field_result
    }

    fn field(
        &self,
        field: &MappedField,
        field_results: HashMap<i32, Arc<MappedField>>,
    ) -> HashMap<i32, Arc<MappedField>> {
        let mut result = field_results;

        if let Some(id) = field.field_id {
            result.insert(id, Arc::new(field.clone()));
        }
        result
    }
}

struct CreateMapping;

impl SchemaVisitor for CreateMapping {
    type T = Vec<Arc<MappedField>>;

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
                Arc::new(MappedField::new(
                    Some(field.id),
                    vec![field.name.clone()],
                    result,
                ))
            })
            .collect::<Vec<Arc<MappedField>>>();

        Ok(mapped_fields)
    }

    fn list(&mut self, list: &ListType, value: Self::T) -> Result<Self::T> {
        Ok(vec![Arc::new(MappedField::new(
            Some(list.element_field.id),
            vec![LIST_FIELD_NAME.to_string()],
            value,
        ))])
    }

    fn map(&mut self, map: &MapType, key_value: Self::T, value: Self::T) -> Result<Self::T> {
        Ok(vec![
            Arc::new(MappedField::new(
                Some(map.key_field.id),
                vec![MAP_KEY_FIELD_NAME.to_string()],
                key_value,
            )),
            Arc::new(MappedField::new(
                Some(map.value_field.id),
                vec![MAP_VALUE_FIELD_NAME.to_string()],
                value,
            )),
        ])
    }

    fn primitive(&mut self, _p: &PrimitiveType) -> Result<Self::T> {
        Ok([].to_vec())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::spec::{MAP_VALUE_FIELD_NAME, NestedField, Type};

    fn make_field(
        field_id: Option<i32>,
        names: Vec<&str>,
        fields: Vec<Arc<MappedField>>,
    ) -> Arc<MappedField> {
        Arc::new(MappedField {
            field_id,
            names: names.into_iter().map(String::from).collect(),
            fields,
        })
    }

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
        let name_mapping = r#"
        [
            {
                "field-id": 1,
                "names": [
                    "id",
                    "record_id"
                ]
            },
            {
                "field-id": 2,
                "names": [
                    "data"
                ]
            },
            {
                "field-id": 3,
                "names": [
                    "location"
                ],
                "fields": [
                    {
                        "field-id": 4,
                        "names": [
                            "latitude",
                            "lat"
                        ]
                    },
                    {
                        "field-id": 5,
                        "names": [
                            "longitude",
                            "long"
                        ]
                    }
                ]
            }
        ]
        "#;

        let name_mapping: NameMapping = serde_json::from_str(name_mapping).unwrap();
        assert_eq!(name_mapping, NameMapping {
            root: vec![
                Arc::new(MappedField {
                    field_id: Some(1),
                    names: vec!["id".to_string(), "record_id".to_string()],
                    fields: vec![]
                }),
                Arc::new(MappedField {
                    field_id: Some(2),
                    names: vec!["data".to_string()],
                    fields: vec![]
                }),
                Arc::new(MappedField {
                    field_id: Some(3),
                    names: vec!["location".to_string()],
                    fields: vec![
                        Arc::new(MappedField {
                            field_id: Some(4),
                            names: vec!["latitude".to_string(), "lat".to_string()],
                            fields: vec![]
                        }),
                        Arc::new(MappedField {
                            field_id: Some(5),
                            names: vec!["longitude".to_string(), "long".to_string()],
                            fields: vec![]
                        }),
                    ]
                })
            ],
            id_to_field: HashMap::new(),
            name_to_field: HashMap::new(),
        });
    }

    #[test]
    fn test_json_name_mapping_serialization() {
        let name_mapping = NameMapping {
            root: vec![
                Arc::new(MappedField {
                    field_id: None,
                    names: vec!["foo".to_string()],
                    fields: vec![],
                }),
                Arc::new(MappedField {
                    field_id: Some(2),
                    names: vec!["bar".to_string()],
                    fields: vec![],
                }),
                Arc::new(MappedField {
                    field_id: Some(3),
                    names: vec!["baz".to_string()],
                    fields: vec![],
                }),
                Arc::new(MappedField {
                    field_id: Some(4),
                    names: vec!["qux".to_string()],
                    fields: vec![Arc::new(MappedField {
                        field_id: Some(5),
                        names: vec![LIST_FIELD_NAME.to_string()],
                        fields: vec![],
                    })],
                }),
                Arc::new(MappedField {
                        field_id: Some(6),
                        names: vec!["quux".to_string()],
                        fields: vec![
                            Arc::new(MappedField {
                                field_id: Some(7),
                                names: vec![MAP_KEY_FIELD_NAME.to_string()],
                                fields: vec![],
                            }),
                            Arc::new(MappedField {
                                field_id: Some(8),
                                names: vec![MAP_VALUE_FIELD_NAME.to_string()],
                                fields: vec![
                                    Arc::new(MappedField {
                                        field_id: Some(9),
                                        names: vec![MAP_KEY_FIELD_NAME.to_string()],
                                        fields: vec![],
                                    }),
                                    Arc::new(MappedField {
                                        field_id: Some(10),
                                        names: vec![MAP_VALUE_FIELD_NAME.to_string()],
                                        fields: vec![],
                                    }),
                                ],
                            }),
                        ],
                    },
                ),
                Arc::new(MappedField {
                    field_id: Some(11),
                    names: vec!["location".to_string()],
                    fields: vec![Arc::new(MappedField {
                        field_id: Some(12),
                        names: vec![LIST_FIELD_NAME.to_string()],
                        fields: vec![
                            Arc::new(MappedField {
                                field_id: Some(13),
                                names: vec!["latitude".to_string()],
                                fields: vec![],
                            }),
                            Arc::new(MappedField {
                                field_id: Some(14),
                                names: vec!["longitude".to_string()],
                                fields: vec![],
                            }),
                        ],
                    })],
                }),
                Arc::new(MappedField {
                    field_id: Some(15),
                    names: vec!["person".to_string()],
                    fields: vec![
                        Arc::new(MappedField {
                            field_id: Some(16),
                            names: vec!["name".to_string()],
                            fields: vec![],
                        }),
                        Arc::new(MappedField {
                            field_id: Some(17),
                            names: vec!["age".to_string()],
                            fields: vec![],
                        }),
                    ],
                }),
            ],
            id_to_field: HashMap::new(),
            name_to_field: HashMap::new(),
        };
        let expected = r#"[{"names":["foo"]},{"field-id":2,"names":["bar"]},{"field-id":3,"names":["baz"]},{"field-id":4,"names":["qux"],"fields":[{"field-id":5,"names":["element"]}]},{"field-id":6,"names":["quux"],"fields":[{"field-id":7,"names":["key"]},{"field-id":8,"names":["value"],"fields":[{"field-id":9,"names":["key"]},{"field-id":10,"names":["value"]}]}]},{"field-id":11,"names":["location"],"fields":[{"field-id":12,"names":["element"],"fields":[{"field-id":13,"names":["latitude"]},{"field-id":14,"names":["longitude"]}]}]},{"field-id":15,"names":["person"],"fields":[{"field-id":16,"names":["name"]},{"field-id":17,"names":["age"]}]}]"#;
        assert_eq!(serde_json::to_string(&name_mapping).unwrap(), expected);
    }

    #[test]
    fn test_index_by_id_and_index_by_name() {
        let field1 = make_field(Some(1), vec!["a", "alpha"], vec![]);
        let field3 = make_field(Some(3), vec!["c", "charlie"], vec![]);
        let field2 = make_field(Some(2), vec!["b"], vec![field3.clone()]);

        let root = vec![field1.clone(), field2.clone()];

        let id_to_field = NameMapping::index_by_id(&root);
        let expected_id: HashMap<i32, Arc<MappedField>> = vec![
            (1, field1.clone()),
            (2, field2.clone()),
            (3, field3.clone()),
        ]
        .into_iter()
        .collect();
        assert_eq!(id_to_field, expected_id);

        let name_to_field = NameMapping::index_by_name(&root);
        let expected_name: HashMap<String, Arc<MappedField>> = vec![
            ("a".to_string(), field1.clone()),
            ("alpha".to_string(), field1.clone()),
            ("b".to_string(), field2.clone()),
            ("b.c".to_string(), field3.clone()),
            ("b.charlie".to_string(), field3.clone()),
            ("c".to_string(), field3.clone()),
            ("charlie".to_string(), field3.clone()),
        ]
        .into_iter()
        .collect();
        assert_eq!(name_to_field, expected_name);
    }

    #[test]
    fn test_create_name_mapping() {
        let field_1 = NestedField::required(10, "id", Type::Primitive(PrimitiveType::Int));
        let field_2 = NestedField::optional(20, "data", Type::Primitive(PrimitiveType::String));
        let struct_1 = NestedField::optional(31, "latitude", Type::Primitive(PrimitiveType::Float));
        let struct_2 =
            NestedField::optional(32, "longitude", Type::Primitive(PrimitiveType::Float));
        let struct_type = Type::Struct(StructType::new(vec![
            struct_1.clone().into(),
            struct_2.clone().into(),
        ]));
        let field_3 = NestedField::required(30, "location", struct_type);

        let schema = Schema::builder()
            .with_schema_id(100)
            .with_identifier_field_ids(vec![10])
            .with_fields(vec![
                field_1.clone().into(),
                field_2.clone().into(),
                field_3.clone().into(),
            ])
            .build()
            .unwrap();

        let mapping = NameMapping::new(&schema);

        let field1 = make_field(Some(10), vec!["id"], vec![]);
        let field2 = make_field(Some(20), vec!["data"], vec![]);
        let field3 = make_field(Some(30), vec!["location"], vec![
            make_field(Some(31), vec!["latitude"], vec![]),
            make_field(Some(32), vec!["longitude"], vec![]),
        ]);

        let mut expected_id = HashMap::new();

        expected_id.insert(10, field1);
        expected_id.insert(20, field2);
        expected_id.insert(30, field3.clone());
        expected_id.insert(31, field3.fields[0].clone());
        expected_id.insert(32, field3.fields[1].clone());
        assert_eq!(mapping.id_to_field, expected_id);

        let expected_name: HashMap<String, Arc<MappedField>> = vec![
            (
                "id".to_string(),
                mapping.id_to_field.get(&10).unwrap().clone(),
            ),
            (
                "data".to_string(),
                mapping.id_to_field.get(&20).unwrap().clone(),
            ),
            (
                "location".to_string(),
                mapping.id_to_field.get(&30).unwrap().clone(),
            ),
            (
                "location.latitude".to_string(),
                mapping.id_to_field.get(&31).unwrap().clone(),
            ),
            (
                "location.longitude".to_string(),
                mapping.id_to_field.get(&32).unwrap().clone(),
            ),
            (
                "latitude".to_string(),
                mapping.id_to_field.get(&31).unwrap().clone(),
            ),
            (
                "longitude".to_string(),
                mapping.id_to_field.get(&32).unwrap().clone(),
            ),
        ]
        .into_iter()
        .collect();

        assert_eq!(mapping.name_to_field, expected_name);
    }
}
