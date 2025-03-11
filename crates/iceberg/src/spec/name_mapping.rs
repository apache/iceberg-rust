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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DefaultOnNull};

use crate::spec::schema::{PartnerAccessor, Schema, SchemaVisitor, SchemaWithPartnerVisitor};
use crate::spec::{
    visit_schema, visit_schema_with_partner, visit_type, ListType, MapType, NestedField,
    NestedFieldRef, PrimitiveType, StructType, Type,
};
use crate::{Error, ErrorKind, Result};

/// Iceberg fallback field name to ID mapping.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(transparent)]
pub struct NameMapping {
    /// Holds mapped fields
    pub root: Vec<MappedField>,
}

/// Maps field names to IDs.
#[serde_as]
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct MappedField {
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Unique identifier for field.
    pub field_id: Option<i32>,

    /// Contains multiple names to map to a field (for schema evolution).
    pub names: Vec<String>,

    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde_as(deserialize_as = "DefaultOnNull")]
    /// Holds mappings for nested fields.
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

impl NameMapping {
    /// Parses name_mapping from JSON.
    pub fn parse_name_mapping(name_mapping: &str) -> Result<Self> {
        let parsed_name_mapping: NameMapping = serde_json::from_str(name_mapping)?;
        Ok(parsed_name_mapping)
    }

    /// Creates a mapping from a schema
    pub fn create_mapping_from_schema(schema: &Schema) -> NameMapping {
        let mapped_fields = visit_schema(schema, &mut CreateMapping).unwrap();
       
        NameMapping {
            root: mapped_fields,
        }
    }

    /// Updates an existing name mapping with new updates and additions.
    pub fn update_mapping(
        mapping: NameMapping,
        updates: HashMap<i32, NestedField>,
        adds: HashMap<i32, Vec<NestedField>>,
    ) -> NameMapping {
        let visitor = UpdateMapping::new(updates, adds);
        let updated_root = visit_name_mapping(&mapping, &visitor);

        NameMapping { root: updated_root }
    }

    /// Returns an index mapping names to `MappedField`` by visiting the schema.
    pub fn index_by_name(schema: &Schema) -> HashMap<String, MappedField> {
        let mapping = Self::create_mapping_from_schema(schema);
        visit_name_mapping(&mapping, &IndexByName)
    }

    /// Applies the name mapping to the given schema.
    pub fn apply_name_mapping(schema: &Schema, mapping: &NameMapping) -> Result<Type> {
        let partner = mapping
            .root
            .first()
            .ok_or_else(|| {
                Box::new(Error::new(
                    ErrorKind::DataInvalid,
                    "NameMapping must have at least one root mapping",
                ))
            })
            .unwrap();

        visit_schema_with_partner(
            schema,
            partner,
            &mut NameMappingProjectionVisitor::new(),
            &NameMappingAccessor,
        )
    }
}

/// A trait for visiting and transforming a name mapping
pub trait NameMappingVisitor {
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
pub fn visit_name_mapping<V>(nm: &NameMapping, visitor: &V) -> V::S
where V: NameMappingVisitor {
    let root_result = visit_fields(&nm.root, visitor);
    visitor.mapping(root_result)
}

/// Recursively visits a slice of mapped fields using visitor
pub fn visit_fields<V>(fields: &[MappedField], visitor: &V) -> V::S
where V: NameMappingVisitor {
    let mut results: Vec<V::T> = Vec::new();

    for field in fields {
        let child_result = visit_fields(&field.fields, visitor);
        let field_result = visitor.field(field, child_result);
        results.push(field_result);
    }

    visitor.fields(results)
}

struct IndexByName;

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

    // Create
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

struct UpdateMapping {
    updates: HashMap<i32, NestedField>,
    adds: HashMap<i32, Vec<NestedField>>,
}

impl UpdateMapping {
    fn new(updates: HashMap<i32, NestedField>, adds: HashMap<i32, Vec<NestedField>>) -> Self {
        Self { updates, adds }
    }

    pub fn remove_reassigned_names(
        field: &MappedField,
        assignments: &HashMap<String, i32>,
    ) -> Option<MappedField> {
        // Collect names to remove.
        let removed_names: HashSet<String> = field
            .names
            .iter()
            .filter_map(|name| {
                if let Some(&assigned_id) = assignments.get(name) {
                    if field.field_id != Some(assigned_id) {
                        Some(name.clone())
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        let remaining_names: Vec<String> = field
            .names
            .iter()
            .filter(|name| !removed_names.contains(*name))
            .cloned()
            .collect();

        // If there are any names left, return a new MappedField with the same field_id and child fields.
        if !remaining_names.is_empty() {
            Some(MappedField::new(
                remaining_names,
                field.fields.clone(),
                field.field_id,
            ))
        } else {
            None
        }
    }

    /// Adds new fields to the existing mapping for a given parent ID.
    pub fn add_new_fields(
        &self,
        mapped_fields: Vec<MappedField>,
        parent_id: i32,
    ) -> Vec<MappedField> {
        if let Some(fields_to_add) = self.adds.get(&parent_id) {
            let new_fields: Vec<MappedField> = fields_to_add
                .iter()
                .map(|add| {
                    let child_mappings = visit_type(&add.field_type, &mut CreateMapping)?;
                    Ok(MappedField::new(
                        vec![add.name.clone()],
                        child_mappings,
                        Some(add.id),
                    ))
                })
                .collect::<Result<Vec<MappedField>>>()
                .unwrap();

            let reassignments: HashMap<String, i32> = fields_to_add
                .iter()
                .map(|f| (f.name.clone(), f.id))
                .collect();

            let updated_fields: Vec<MappedField> = mapped_fields
                .into_iter()
                .filter_map(|field| Self::remove_reassigned_names(&field, &reassignments))
                .collect();

            [updated_fields, new_fields].concat()
        } else {
            mapped_fields
        }
    }
}

impl NameMappingVisitor for UpdateMapping {
    type S = Vec<MappedField>;
    type T = MappedField;

    // For the whole mapping, simply call add_new_fields with parent_id -1.
    fn mapping(&self, field_result: Self::S) -> Self::S {
        self.add_new_fields(field_result, -1)
    }

    // Build reassignments and update each field.
    fn fields(&self, field_results: Vec<Self::T>) -> Self::S {
        // Build a reassignment map: for each field in field_results with a field_id,
        // if there's an update in updates, map update.name to that field_id.
        let mut reassignments = HashMap::new();
        for f in &field_results {
            if let Some(field_id) = f.field_id {
                if let Some(update) = self.updates.get(&field_id) {
                    reassignments.insert(update.name.clone(), field_id);
                }
            }
        }

        field_results
            .into_iter()
            .filter_map(|field| Self::remove_reassigned_names(&field, &reassignments))
            .collect()
    }

    // Update its names if needed and then update its child fields
    fn field(&self, field: &MappedField, field_result: Self::S) -> Self::T {
        if field.field_id.is_none() {
            return field.clone();
        }
        let mut field_names = field.names.clone();

        // If there's an update for this field, append if name not already included
        if let Some(update) = self.updates.get(&field.field_id.unwrap()) {
            if !field_names.contains(&update.name) {
                field_names.push(update.name.clone());
            }
        }

        // Update the child fields
        let updated_child_fields = self.add_new_fields(field_result, field.field_id.unwrap());
        MappedField::new(field_names, updated_child_fields, field.field_id)
    }
}

struct NameMappingAccessor;

impl PartnerAccessor<MappedField> for NameMappingAccessor {
    fn struct_partner<'a>(&self, schema_partner: &'a MappedField) -> Result<&'a MappedField> {
        Ok(schema_partner)
    }

    /// Look for name which contains the requested field name
    fn field_partner<'a>(
        &self,
        struct_partner: &'a MappedField,
        field: &NestedField,
    ) -> Result<&'a MappedField> {
        struct_partner
            .fields
            .iter()
            .find(|candidate| candidate.names.contains(&field.name))
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Field partner not found for field name: {}", field.name),
                )
            })
    }

    /// Look for name which contains "element"
    fn list_element_partner<'a>(&self, list_partner: &'a MappedField) -> Result<&'a MappedField> {
        list_partner
            .fields
            .iter()
            .find(|candidate| candidate.names.contains(&"element".to_string()))
            .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "List element partner not found"))
    }

    /// Look for name which contains "key"
    fn map_key_partner<'a>(&self, map_partner: &'a MappedField) -> Result<&'a MappedField> {
        map_partner
            .fields
            .iter()
            .find(|candidate| candidate.names.contains(&"key".to_string()))
            .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "Map key partner not found"))
    }

    /// For a map value partner, iterate over the nested fields and look for one whose names contain "value".
    fn map_value_partner<'a>(&self, map_partner: &'a MappedField) -> Result<&'a MappedField> {
        map_partner
            .fields
            .iter()
            .find(|candidate| candidate.names.contains(&"value".to_string()))
            .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "Map value partner not found"))
    }
}

struct NameMappingProjectionVisitor {
    current_path: Vec<String>,
}

impl NameMappingProjectionVisitor {
    pub fn new() -> Self {
        Self {
            current_path: Vec::new(),
        }
    }

    fn current_path_str(&self) -> String {
        self.current_path.join(".")
    }
}

impl SchemaWithPartnerVisitor<MappedField> for NameMappingProjectionVisitor {
    // Using `Type` as the result type.
    type T = Type;

    fn before_struct_field(
        &mut self,
        field: &NestedFieldRef,
        _partner: &MappedField,
    ) -> Result<()> {
        self.current_path.push(field.name.clone());
        Ok(())
    }

    fn after_struct_field(
        &mut self,
        _field: &NestedFieldRef,
        _partner: &MappedField,
    ) -> Result<()> {
        self.current_path.pop();
        Ok(())
    }

    fn before_list_element(
        &mut self,
        _field: &NestedFieldRef,
        _partner: &MappedField,
    ) -> Result<()> {
        self.current_path.push("element".to_string());
        Ok(())
    }

    fn after_list_element(
        &mut self,
        _field: &NestedFieldRef,
        _partner: &MappedField,
    ) -> Result<()> {
        self.current_path.pop();
        Ok(())
    }

    fn before_map_key(&mut self, _field: &NestedFieldRef, _partner: &MappedField) -> Result<()> {
        self.current_path.push("key".to_string());
        Ok(())
    }

    fn after_map_key(&mut self, _field: &NestedFieldRef, _partner: &MappedField) -> Result<()> {
        self.current_path.pop();
        Ok(())
    }

    fn before_map_value(&mut self, _field: &NestedFieldRef, _partner: &MappedField) -> Result<()> {
        self.current_path.push("value".to_string());
        Ok(())
    }

    fn after_map_value(&mut self, _field: &NestedFieldRef, _partner: &MappedField) -> Result<()> {
        self.current_path.pop();
        Ok(())
    }

    // TODO: implement schema and field functions
    fn schema(
        &mut self,
        _schema: &Schema,
        _partner: &MappedField,
        _value: Self::T,
    ) -> Result<Self::T> {
        unimplemented!()
    }

    fn field(
        &mut self,
        _field: &NestedFieldRef,
        _partner: &MappedField,
        _value: Self::T,
    ) -> Result<Self::T> {
        unimplemented!()
    }

    fn r#struct(
        &mut self,
        _struct_type: &StructType,
        _partner: &MappedField,
        _results: Vec<Self::T>,
    ) -> Result<Self::T> {
        unimplemented!()
    }

    fn list(&mut self, list: &ListType, partner: &MappedField, value: Self::T) -> Result<Self::T> {
        let element_field = partner
            .fields
            .iter()
            .find(|f| f.names.contains(&"element".to_string()))
            .ok_or_else(|| {
                Box::new(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Could not find field with name: {}",
                        self.current_path_str()
                    ),
                ))
            })
            .unwrap();

        let element_id = element_field
            .field_id
            .ok_or_else(|| {
                Box::new(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "List element field ID missing in NameMapping: {}",
                        self.current_path_str()
                    ),
                ))
            })
            .unwrap();

        let element_field = Arc::new(
            NestedField::list_element(element_id, value, list.element_field.required)
                .with_doc(list.element_field.doc.clone().unwrap_or_default())
                .with_initial_default(list.element_field.initial_default.clone().unwrap())
                .with_write_default(list.element_field.write_default.clone().unwrap()),
        );

        let new_list = ListType { element_field };

        Ok(Type::List(new_list))
    }

    fn map(
        &mut self,
        map: &MapType,
        partner: &MappedField,
        key_value: Self::T,
        value: Self::T,
    ) -> Result<Self::T> {
        let key_field = partner
            .fields
            .iter()
            .find(|f| f.names.contains(&"key".to_string()))
            .ok_or_else(|| {
                Box::new(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Could not find field with name: {}",
                        self.current_path_str()
                    ),
                ))
            })
            .unwrap();

        let value_field = partner
            .fields
            .iter()
            .find(|f| f.names.contains(&"value".to_string()))
            .ok_or_else(|| {
                Box::new(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Could not find field with name: {}",
                        self.current_path_str()
                    ),
                ))
            })
            .unwrap();

        let key_id = key_field
            .field_id
            .ok_or_else(|| {
                Box::new(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Map key field ID missing in NameMapping: {}",
                        self.current_path_str()
                    ),
                ))
            })
            .unwrap();

        let value_id = value_field
            .field_id
            .ok_or_else(|| {
                Box::new(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Map value field ID missing in NameMapping: {}",
                        self.current_path_str()
                    ),
                ))
            })
            .unwrap();

        let key_field = Arc::new(
            NestedField::map_key_element(key_id, key_value)
                .with_doc(map.key_field.doc.clone().unwrap())
                .with_initial_default(map.key_field.initial_default.clone().unwrap())
                .with_write_default(map.key_field.write_default.clone().unwrap()),
        );

        let value_field = Arc::new(
            NestedField::map_value_element(value_id, value, map.value_field.required)
                .with_doc(map.value_field.doc.clone().unwrap_or_default())
                .with_initial_default(map.value_field.initial_default.clone().unwrap())
                .with_write_default(map.value_field.write_default.clone().unwrap()),
        );

        let new_map = MapType {
            key_field,
            value_field,
        };

        Ok(Type::Map(new_map))
    }

    fn primitive(&mut self, p: &PrimitiveType, _partner: &MappedField) -> Result<Self::T> {
        Ok(Type::Primitive(p.clone()))
    }
}

#[cfg(test)]
mod tests {
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
                MappedField {
                    field_id: Some(1),
                    names: vec!["id".to_string(), "record_id".to_string()],
                    fields: vec![]
                },
                MappedField {
                    field_id: Some(2),
                    names: vec!["data".to_string()],
                    fields: vec![]
                },
                MappedField {
                    field_id: Some(3),
                    names: vec!["location".to_string()],
                    fields: vec![
                        MappedField {
                            field_id: Some(4),
                            names: vec!["latitude".to_string(), "lat".to_string()],
                            fields: vec![]
                        },
                        MappedField {
                            field_id: Some(5),
                            names: vec!["longitude".to_string(), "long".to_string()],
                            fields: vec![]
                        },
                    ]
                }
            ]
        });
    }

    #[test]
    fn test_json_name_mapping_serialization() {
        let name_mapping = NameMapping {
            root: vec![
                MappedField {
                    field_id: None,
                    names: vec!["foo".to_string()],
                    fields: vec![],
                },
                MappedField {
                    field_id: Some(2),
                    names: vec!["bar".to_string()],
                    fields: vec![],
                },
                MappedField {
                    field_id: Some(3),
                    names: vec!["baz".to_string()],
                    fields: vec![],
                },
                MappedField {
                    field_id: Some(4),
                    names: vec!["qux".to_string()],
                    fields: vec![MappedField {
                        field_id: Some(5),
                        names: vec!["element".to_string()],
                        fields: vec![],
                    }],
                },
                MappedField {
                    field_id: Some(6),
                    names: vec!["quux".to_string()],
                    fields: vec![
                        MappedField {
                            field_id: Some(7),
                            names: vec!["key".to_string()],
                            fields: vec![],
                        },
                        MappedField {
                            field_id: Some(8),
                            names: vec!["value".to_string()],
                            fields: vec![
                                MappedField {
                                    field_id: Some(9),
                                    names: vec!["key".to_string()],
                                    fields: vec![],
                                },
                                MappedField {
                                    field_id: Some(10),
                                    names: vec!["value".to_string()],
                                    fields: vec![],
                                },
                            ],
                        },
                    ],
                },
                MappedField {
                    field_id: Some(11),
                    names: vec!["location".to_string()],
                    fields: vec![MappedField {
                        field_id: Some(12),
                        names: vec!["element".to_string()],
                        fields: vec![
                            MappedField {
                                field_id: Some(13),
                                names: vec!["latitude".to_string()],
                                fields: vec![],
                            },
                            MappedField {
                                field_id: Some(14),
                                names: vec!["longitude".to_string()],
                                fields: vec![],
                            },
                        ],
                    }],
                },
                MappedField {
                    field_id: Some(15),
                    names: vec!["person".to_string()],
                    fields: vec![
                        MappedField {
                            field_id: Some(16),
                            names: vec!["name".to_string()],
                            fields: vec![],
                        },
                        MappedField {
                            field_id: Some(17),
                            names: vec!["age".to_string()],
                            fields: vec![],
                        },
                    ],
                },
            ],
        };
        let expected = r#"[{"names":["foo"]},{"field-id":2,"names":["bar"]},{"field-id":3,"names":["baz"]},{"field-id":4,"names":["qux"],"fields":[{"field-id":5,"names":["element"]}]},{"field-id":6,"names":["quux"],"fields":[{"field-id":7,"names":["key"]},{"field-id":8,"names":["value"],"fields":[{"field-id":9,"names":["key"]},{"field-id":10,"names":["value"]}]}]},{"field-id":11,"names":["location"],"fields":[{"field-id":12,"names":["element"],"fields":[{"field-id":13,"names":["latitude"]},{"field-id":14,"names":["longitude"]}]}]},{"field-id":15,"names":["person"],"fields":[{"field-id":16,"names":["name"]},{"field-id":17,"names":["age"]}]}]"#;
        assert_eq!(serde_json::to_string(&name_mapping).unwrap(), expected);
    }

    fn create_schema() -> Schema {
        Schema::builder()
            .with_schema_id(100)
            .with_fields(vec![
                NestedField::required(1, "field1", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "field2", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap()
    }

    #[test]
    fn test_create_mapping_from_schema() {
        let schema = create_schema();
        let mapped_fields = visit_schema(&schema, &mut CreateMapping)
            .expect("Failed to create mapping from schema");
        let mapping = NameMapping {
            root: mapped_fields,
        };

        assert_eq!(mapping.root.len(), 2);
        let mapped_field1 = &mapping.root[0];
        assert_eq!(mapped_field1.field_id, Some(1));
        assert!(mapped_field1.names.contains(&"field1".to_string()));

        let mapped_field2 = &mapping.root[1];
        assert_eq!(mapped_field2.field_id, Some(2));
        assert!(mapped_field2.names.contains(&"field2".to_string()));
    }

    #[test]
    fn test_index_by_name() {
        let schema = create_schema();

        let index = NameMapping::index_by_name(&schema);
        let mapped1 = index.get("field1").expect("field1 not found in index");
        assert_eq!(mapped1.field_id, Some(1));
        let mapped2 = index.get("field2").expect("field2 not found in index");
        assert_eq!(mapped2.field_id, Some(2));
    }

    #[test]
    fn test_update_mapping() {
        let initial_mapping = NameMapping {
            root: vec![
                MappedField::new(vec!["foo".to_string()], vec![], Some(1)),
                MappedField::new(vec!["bar".to_string()], vec![], Some(2)),
            ],
        };

        let mut updates = HashMap::new();
        let updated_field =
            NestedField::required(1, "foo_updated", Type::Primitive(PrimitiveType::Int));
        updates.insert(1, updated_field);

        let adds = HashMap::new();

        let new_mapping = NameMapping::update_mapping(initial_mapping, updates, adds);

        let mapped1 = &new_mapping.root[0];
        assert!(mapped1.names.contains(&"foo_updated".to_string()));

        let mapped2 = &new_mapping.root[1];
        assert!(mapped2.names.contains(&"bar".to_string()));
    }
}
