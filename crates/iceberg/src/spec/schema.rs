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

//! This module defines schema in iceberg.

use crate::error::Result;
use crate::spec::datatypes::{
    ListType, MapType, NestedFieldRef, PrimitiveType, StructType, Type, LIST_FILED_NAME,
    MAP_KEY_FIELD_NAME, MAP_VALUE_FIELD_NAME,
};
use crate::{ensure_data_valid, Error, ErrorKind};
use bimap::BiHashMap;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use _serde::SchemaEnum;

/// Type alias for schema id.
pub type SchemaId = i32;
/// Reference to [`Schema`].
pub type SchemaRef = Arc<Schema>;
const DEFAULT_SCHEMA_ID: SchemaId = 0;

/// Defines schema in iceberg.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(try_from = "SchemaEnum", into = "SchemaEnum")]
pub struct Schema {
    r#struct: StructType,
    schema_id: SchemaId,
    highest_field_id: i32,
    identifier_field_ids: HashSet<i32>,

    alias_to_id: BiHashMap<String, i32>,
    id_to_field: HashMap<i32, NestedFieldRef>,

    name_to_id: HashMap<String, i32>,
    id_to_name: HashMap<i32, String>,
}

impl PartialEq for Schema {
    fn eq(&self, other: &Self) -> bool {
        self.r#struct == other.r#struct
            && self.schema_id == other.schema_id
            && self.identifier_field_ids == other.identifier_field_ids
    }
}

impl Eq for Schema {}

/// Schema builder.
#[derive(Debug)]
pub struct SchemaBuilder {
    schema_id: i32,
    fields: Vec<NestedFieldRef>,
    alias_to_id: BiHashMap<String, i32>,
    identifier_field_ids: HashSet<i32>,
}

impl SchemaBuilder {
    /// Add fields to schema builder.
    pub fn with_fields(mut self, fields: impl IntoIterator<Item = NestedFieldRef>) -> Self {
        self.fields.extend(fields);
        self
    }

    /// Set schema id.
    pub fn with_schema_id(mut self, schema_id: i32) -> Self {
        self.schema_id = schema_id;
        self
    }

    /// Set identifier field ids.
    pub fn with_identifier_field_ids(mut self, ids: impl IntoIterator<Item = i32>) -> Self {
        self.identifier_field_ids.extend(ids);
        self
    }

    /// Set alias to filed id mapping.
    pub fn with_alias(mut self, alias_to_id: BiHashMap<String, i32>) -> Self {
        self.alias_to_id = alias_to_id;
        self
    }

    /// Builds the schema.
    pub fn build(self) -> Result<Schema> {
        let highest_field_id = self.fields.iter().map(|f| f.id).max().unwrap_or(0);

        let r#struct = StructType::new(self.fields);
        let id_to_field = index_by_id(&r#struct)?;

        Self::validate_identifier_ids(
            &r#struct,
            &id_to_field,
            self.identifier_field_ids.iter().copied(),
        )?;

        let (name_to_id, id_to_name) = {
            let mut index = IndexByName::default();
            visit_struct(&r#struct, &mut index)?;
            index.indexes()
        };

        Ok(Schema {
            r#struct,
            schema_id: self.schema_id,
            highest_field_id,
            identifier_field_ids: self.identifier_field_ids,

            alias_to_id: self.alias_to_id,
            id_to_field,

            name_to_id,
            id_to_name,
        })
    }

    fn validate_identifier_ids(
        r#struct: &StructType,
        id_to_field: &HashMap<i32, NestedFieldRef>,
        identifier_field_ids: impl Iterator<Item = i32>,
    ) -> Result<()> {
        let id_to_parent = index_parents(r#struct)?;
        for identifier_field_id in identifier_field_ids {
            let field = id_to_field.get(&identifier_field_id).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot add identifier field {identifier_field_id}: field does not exist"
                    ),
                )
            })?;
            ensure_data_valid!(
                field.required,
                "Cannot add identifier field: {} is an optional field",
                field.name
            );
            if let Type::Primitive(p) = field.field_type.as_ref() {
                ensure_data_valid!(
                    !matches!(p, PrimitiveType::Double | PrimitiveType::Float),
                    "Cannot add identifier field {}: cannot be a float or double type",
                    field.name
                );
            } else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot add field {} as an identifier field: not a primitive type field",
                        field.name
                    ),
                ));
            }

            let mut cur_field_id = identifier_field_id;
            while let Some(parent) = id_to_parent.get(&cur_field_id) {
                let parent_field = id_to_field
                    .get(parent)
                    .expect("Field id should not disappear.");
                ensure_data_valid!(
                    parent_field.field_type.is_struct(),
                    "Cannot add field {} as an identifier field: must not be nested in {:?}",
                    field.name,
                    parent_field
                );
                ensure_data_valid!(parent_field.required, "Cannot add field {} as an identifier field: must not be nested in an optional field {}", field.name, parent_field);
                cur_field_id = *parent;
            }
        }

        Ok(())
    }
}

impl Schema {
    /// Create a schema builder.
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder {
            schema_id: DEFAULT_SCHEMA_ID,
            fields: vec![],
            identifier_field_ids: HashSet::default(),
            alias_to_id: BiHashMap::default(),
        }
    }

    /// Get field by field id.
    pub fn field_by_id(&self, field_id: i32) -> Option<&NestedFieldRef> {
        self.id_to_field.get(&field_id)
    }

    /// Get field by field name.
    ///
    /// Both full name and short name could work here.
    pub fn field_by_name(&self, field_name: &str) -> Option<&NestedFieldRef> {
        self.name_to_id
            .get(field_name)
            .and_then(|id| self.field_by_id(*id))
    }

    /// Get field by alias.
    pub fn field_by_alias(&self, alias: &str) -> Option<&NestedFieldRef> {
        self.alias_to_id
            .get_by_left(alias)
            .and_then(|id| self.field_by_id(*id))
    }

    /// Returns [`highest_field_id`].
    #[inline]
    pub fn highest_field_id(&self) -> i32 {
        self.highest_field_id
    }

    /// Returns [`schema_id`].
    #[inline]
    pub fn schema_id(&self) -> i32 {
        self.schema_id
    }

    /// Returns [`r#struct`].
    #[inline]
    pub fn as_struct(&self) -> &StructType {
        &self.r#struct
    }

    /// Get field id by full name.
    pub fn field_id_by_name(&self, name: &str) -> Option<i32> {
        self.name_to_id.get(name).copied()
    }

    /// Get field id by full name.
    pub fn name_by_field_id(&self, field_id: i32) -> Option<&str> {
        self.id_to_name.get(&field_id).map(String::as_str)
    }
}

impl Display for Schema {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "table {{")?;
        for field in self.as_struct().fields() {
            writeln!(f, "  {}", field)?;
        }
        writeln!(f, "}}")
    }
}

/// A post order schema visitor.
///
/// For order of methods called, please refer to [`visit_schema`].
pub trait SchemaVisitor {
    /// Return type of this visitor.
    type T;

    /// Called before struct field.
    fn before_struct_field(&mut self, _field: &NestedFieldRef) -> Result<()> {
        Ok(())
    }
    /// Called after struct field.
    fn after_struct_field(&mut self, _field: &NestedFieldRef) -> Result<()> {
        Ok(())
    }
    /// Called before list field.
    fn before_list_element(&mut self, _field: &NestedFieldRef) -> Result<()> {
        Ok(())
    }
    /// Called after list field.
    fn after_list_element(&mut self, _field: &NestedFieldRef) -> Result<()> {
        Ok(())
    }
    /// Called before map key field.
    fn before_map_key(&mut self, _field: &NestedFieldRef) -> Result<()> {
        Ok(())
    }
    /// Called after map key field.
    fn after_map_key(&mut self, _field: &NestedFieldRef) -> Result<()> {
        Ok(())
    }
    /// Called before map value field.
    fn before_map_value(&mut self, _field: &NestedFieldRef) -> Result<()> {
        Ok(())
    }
    /// Called after map value field.
    fn after_map_value(&mut self, _field: &NestedFieldRef) -> Result<()> {
        Ok(())
    }

    /// Called after schema's type visited.
    fn schema(&mut self, schema: &Schema, value: Self::T) -> Result<Self::T>;
    /// Called after struct's field type visited.
    fn field(&mut self, field: &NestedFieldRef, value: Self::T) -> Result<Self::T>;
    /// Called after struct's fields visited.
    fn r#struct(&mut self, r#struct: &StructType, results: Vec<Self::T>) -> Result<Self::T>;
    /// Called after list fields visited.
    fn list(&mut self, list: &ListType, value: Self::T) -> Result<Self::T>;
    /// Called after map's key and value fields visited.
    fn map(&mut self, map: &MapType, key_value: Self::T, value: Self::T) -> Result<Self::T>;
    /// Called when see a primitive type.
    fn primitive(&mut self, p: &PrimitiveType) -> Result<Self::T>;
}

/// Visiting a type in post order.
pub fn visit_type<V: SchemaVisitor>(r#type: &Type, visitor: &mut V) -> Result<V::T> {
    match r#type {
        Type::Primitive(p) => visitor.primitive(p),
        Type::List(list) => {
            visitor.before_list_element(&list.element_field)?;
            let value = visit_type(&list.element_field.field_type, visitor)?;
            visitor.after_list_element(&list.element_field)?;
            visitor.list(list, value)
        }
        Type::Map(map) => {
            let key_result = {
                visitor.before_map_key(&map.key_field)?;
                let ret = visit_type(&map.key_field.field_type, visitor)?;
                visitor.after_map_key(&map.key_field)?;
                ret
            };

            let value_result = {
                visitor.before_map_value(&map.value_field)?;
                let ret = visit_type(&map.value_field.field_type, visitor)?;
                visitor.after_map_value(&map.value_field)?;
                ret
            };

            visitor.map(map, key_result, value_result)
        }
        Type::Struct(s) => visit_struct(s, visitor),
    }
}

/// Visit struct type in post order.
pub fn visit_struct<V: SchemaVisitor>(s: &StructType, visitor: &mut V) -> Result<V::T> {
    let mut results = Vec::with_capacity(s.fields().len());
    for field in s.fields() {
        visitor.before_struct_field(field)?;
        let result = visit_type(&field.field_type, visitor)?;
        visitor.after_struct_field(field)?;
        let result = visitor.field(field, result)?;
        results.push(result);
    }

    visitor.r#struct(s, results)
}

/// Visit schema in post order.
pub fn visit_schema<V: SchemaVisitor>(schema: &Schema, visitor: &mut V) -> Result<V::T> {
    let result = visit_struct(&schema.r#struct, visitor)?;
    visitor.schema(schema, result)
}

/// Creates an field id to field map.
pub fn index_by_id(r#struct: &StructType) -> Result<HashMap<i32, NestedFieldRef>> {
    struct IndexById(HashMap<i32, NestedFieldRef>);

    impl SchemaVisitor for IndexById {
        type T = ();

        fn schema(&mut self, _schema: &Schema, _value: ()) -> Result<()> {
            Ok(())
        }

        fn field(&mut self, field: &NestedFieldRef, _value: ()) -> Result<()> {
            self.0.insert(field.id, field.clone());
            Ok(())
        }

        fn r#struct(&mut self, _struct: &StructType, _results: Vec<Self::T>) -> Result<Self::T> {
            Ok(())
        }

        fn list(&mut self, list: &ListType, _value: Self::T) -> Result<Self::T> {
            self.0
                .insert(list.element_field.id, list.element_field.clone());
            Ok(())
        }

        fn map(&mut self, map: &MapType, _key_value: Self::T, _value: Self::T) -> Result<Self::T> {
            self.0.insert(map.key_field.id, map.key_field.clone());
            self.0.insert(map.value_field.id, map.value_field.clone());
            Ok(())
        }

        fn primitive(&mut self, _: &PrimitiveType) -> Result<Self::T> {
            Ok(())
        }
    }

    let mut index = IndexById(HashMap::new());
    visit_struct(r#struct, &mut index)?;
    Ok(index.0)
}

/// Creates a field id to parent field id map.
pub fn index_parents(r#struct: &StructType) -> Result<HashMap<i32, i32>> {
    struct IndexByParent {
        parents: Vec<i32>,
        result: HashMap<i32, i32>,
    }

    impl SchemaVisitor for IndexByParent {
        type T = ();

        fn before_struct_field(&mut self, field: &NestedFieldRef) -> Result<()> {
            self.parents.push(field.id);
            Ok(())
        }

        fn after_struct_field(&mut self, _field: &NestedFieldRef) -> Result<()> {
            self.parents.pop();
            Ok(())
        }

        fn before_list_element(&mut self, field: &NestedFieldRef) -> Result<()> {
            self.parents.push(field.id);
            Ok(())
        }

        fn after_list_element(&mut self, _field: &NestedFieldRef) -> Result<()> {
            self.parents.pop();
            Ok(())
        }

        fn before_map_key(&mut self, field: &NestedFieldRef) -> Result<()> {
            self.parents.push(field.id);
            Ok(())
        }

        fn after_map_key(&mut self, _field: &NestedFieldRef) -> Result<()> {
            self.parents.pop();
            Ok(())
        }

        fn before_map_value(&mut self, field: &NestedFieldRef) -> Result<()> {
            self.parents.push(field.id);
            Ok(())
        }

        fn after_map_value(&mut self, _field: &NestedFieldRef) -> Result<()> {
            self.parents.pop();
            Ok(())
        }

        fn schema(&mut self, _schema: &Schema, _value: Self::T) -> Result<Self::T> {
            Ok(())
        }

        fn field(&mut self, field: &NestedFieldRef, _value: Self::T) -> Result<Self::T> {
            if let Some(parent) = self.parents.last().copied() {
                self.result.insert(field.id, parent);
            }
            Ok(())
        }

        fn r#struct(&mut self, _struct: &StructType, _results: Vec<Self::T>) -> Result<Self::T> {
            Ok(())
        }

        fn list(&mut self, _list: &ListType, _value: Self::T) -> Result<Self::T> {
            Ok(())
        }

        fn map(&mut self, _map: &MapType, _key_value: Self::T, _value: Self::T) -> Result<Self::T> {
            Ok(())
        }

        fn primitive(&mut self, _p: &PrimitiveType) -> Result<Self::T> {
            Ok(())
        }
    }

    let mut index = IndexByParent {
        parents: vec![],
        result: HashMap::new(),
    };
    visit_struct(r#struct, &mut index)?;
    Ok(index.result)
}

#[derive(Default)]
struct IndexByName {
    // Maybe radix tree is better here?
    name_to_id: HashMap<String, i32>,
    short_name_to_id: HashMap<String, i32>,

    field_names: Vec<String>,
    short_field_names: Vec<String>,
}

impl IndexByName {
    fn add_field(&mut self, name: &str, field_id: i32) -> Result<()> {
        let full_name = self
            .field_names
            .iter()
            .map(String::as_str)
            .chain(vec![name])
            .join(".");
        if let Some(existing_field_id) = self.name_to_id.get(full_name.as_str()) {
            return Err(Error::new(ErrorKind::DataInvalid, format!("Invalid schema: multiple fields for name {full_name}: {field_id} and {existing_field_id}")));
        } else {
            self.name_to_id.insert(full_name, field_id);
        }

        let full_short_name = self
            .short_field_names
            .iter()
            .map(String::as_str)
            .chain(vec![name])
            .join(".");
        self.short_name_to_id
            .entry(full_short_name)
            .or_insert_with(|| field_id);
        Ok(())
    }

    /// Returns two indexes: full name to field id, and id to full name.
    ///
    /// In the first index, short names are returned.
    /// In second index, short names are not returned.
    pub fn indexes(mut self) -> (HashMap<String, i32>, HashMap<i32, String>) {
        self.short_name_to_id.reserve(self.name_to_id.len());
        for (name, id) in &self.name_to_id {
            self.short_name_to_id.insert(name.clone(), *id);
        }

        let id_to_name = self.name_to_id.into_iter().map(|e| (e.1, e.0)).collect();
        (self.short_name_to_id, id_to_name)
    }
}

impl SchemaVisitor for IndexByName {
    type T = ();

    fn before_struct_field(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.field_names.push(field.name.to_string());
        self.short_field_names.push(field.name.to_string());
        Ok(())
    }

    fn after_struct_field(&mut self, _field: &NestedFieldRef) -> Result<()> {
        self.field_names.pop();
        self.short_field_names.pop();
        Ok(())
    }

    fn before_list_element(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.field_names.push(field.name.clone());
        if !field.field_type.is_struct() {
            self.short_field_names.push(field.name.to_string());
        }

        Ok(())
    }

    fn after_list_element(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.field_names.pop();
        if !field.field_type.is_struct() {
            self.short_field_names.pop();
        }

        Ok(())
    }

    fn before_map_key(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.before_struct_field(field)
    }

    fn after_map_key(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.after_struct_field(field)
    }

    fn before_map_value(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.field_names.push(field.name.to_string());
        if !field.field_type.is_struct() {
            self.short_field_names.push(field.name.to_string());
        }
        Ok(())
    }

    fn after_map_value(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.field_names.pop();
        if !field.field_type.is_struct() {
            self.short_field_names.pop();
        }

        Ok(())
    }

    fn schema(&mut self, _schema: &Schema, _value: Self::T) -> Result<Self::T> {
        Ok(())
    }

    fn field(&mut self, field: &NestedFieldRef, _value: Self::T) -> Result<Self::T> {
        self.add_field(field.name.as_str(), field.id)
    }

    fn r#struct(&mut self, _struct: &StructType, _results: Vec<Self::T>) -> Result<Self::T> {
        Ok(())
    }

    fn list(&mut self, list: &ListType, _value: Self::T) -> Result<Self::T> {
        self.add_field(LIST_FILED_NAME, list.element_field.id)
    }

    fn map(&mut self, map: &MapType, _key_value: Self::T, _value: Self::T) -> Result<Self::T> {
        self.add_field(MAP_KEY_FIELD_NAME, map.key_field.id)?;
        self.add_field(MAP_VALUE_FIELD_NAME, map.value_field.id)
    }

    fn primitive(&mut self, _p: &PrimitiveType) -> Result<Self::T> {
        Ok(())
    }
}

pub(super) mod _serde {
    /// This is a helper module that defines types to help with serialization/deserialization.
    /// For deserialization the input first gets read into either the [SchemaV1] or [SchemaV2] struct
    /// and then converted into the [Schema] struct. Serialization works the other way around.
    /// [SchemaV1] and [SchemaV2] are internal struct that are only used for serialization and deserialization.
    use serde::{Deserialize, Serialize};

    use crate::{spec::StructType, Error, Result};

    use super::{Schema, DEFAULT_SCHEMA_ID};

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(untagged)]
    /// Enum for Schema serialization/deserializaion
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
    /// Defines the structure of a v1 schema for serialization/deserialization
    pub(crate) struct SchemaV1 {
        #[serde(skip_serializing_if = "Option::is_none")]
        pub schema_id: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub identifier_field_ids: Option<Vec<i32>>,
        #[serde(flatten)]
        pub fields: StructType,
    }

    /// Helper to serialize/deserializa Schema
    impl TryFrom<SchemaEnum> for Schema {
        type Error = Error;
        fn try_from(value: SchemaEnum) -> Result<Self> {
            match value {
                SchemaEnum::V2(value) => value.try_into(),
                SchemaEnum::V1(value) => value.try_into(),
            }
        }
    }

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

    impl From<Schema> for SchemaV2 {
        fn from(value: Schema) -> Self {
            SchemaV2 {
                schema_id: value.schema_id,
                identifier_field_ids: if value.identifier_field_ids.is_empty() {
                    None
                } else {
                    Some(value.identifier_field_ids.into_iter().collect())
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
                    Some(value.identifier_field_ids.into_iter().collect())
                },
                fields: value.r#struct,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::spec::datatypes::Type::{List, Map, Primitive, Struct};
    use crate::spec::datatypes::{
        ListType, MapType, NestedField, NestedFieldRef, PrimitiveType, StructType, Type,
    };
    use crate::spec::schema::Schema;
    use crate::spec::schema::_serde::{SchemaEnum, SchemaV1, SchemaV2};
    use std::collections::HashMap;

    use super::DEFAULT_SCHEMA_ID;

    fn check_schema_serde(json: &str, expected_type: Schema, _expected_enum: SchemaEnum) {
        let desered_type: Schema = serde_json::from_str(json).unwrap();
        assert_eq!(desered_type, expected_type);
        assert!(matches!(desered_type.clone(), _expected_enum));

        let sered_json = serde_json::to_string(&expected_type).unwrap();
        let parsed_json_value = serde_json::from_str::<Schema>(&sered_json).unwrap();

        assert_eq!(parsed_json_value, desered_type);
    }

    #[test]
    fn test_serde_with_schema_id() {
        let (schema, record) = table_schema_simple();

        let x: SchemaV2 = serde_json::from_str(record).unwrap();
        check_schema_serde(record, schema, SchemaEnum::V2(x));
    }

    #[test]
    fn test_serde_without_schema_id() {
        let (mut schema, record) = table_schema_simple();
        // we remove the ""schema-id": 1," string from example
        let new_record = record.replace("\"schema-id\":1,", "");
        // By default schema_id field is set to DEFAULT_SCHEMA_ID when no value is set in json
        schema.schema_id = DEFAULT_SCHEMA_ID;

        let x: SchemaV1 = serde_json::from_str(new_record.as_str()).unwrap();
        check_schema_serde(&new_record, schema, SchemaEnum::V1(x));
    }

    #[test]
    fn test_construct_schema() {
        let field1: NestedFieldRef =
            NestedField::required(1, "f1", Type::Primitive(PrimitiveType::Boolean)).into();
        let field2: NestedFieldRef =
            NestedField::optional(2, "f2", Type::Primitive(PrimitiveType::Int)).into();

        let schema = Schema::builder()
            .with_fields(vec![field1.clone()])
            .with_fields(vec![field2.clone()])
            .with_schema_id(3)
            .build()
            .unwrap();

        assert_eq!(3, schema.schema_id());
        assert_eq!(2, schema.highest_field_id());
        assert_eq!(Some(&field1), schema.field_by_id(1));
        assert_eq!(Some(&field2), schema.field_by_id(2));
        assert_eq!(None, schema.field_by_id(3));
    }

    #[test]
    fn schema() {
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

    fn table_schema_simple<'a>() -> (Schema, &'a str) {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![2])
            .with_fields(vec![
                NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .build()
            .unwrap();
        let record = r#"{
            "type":"struct",
            "schema-id":1,
            "fields":[
                {
                    "id":1,
                    "name":"foo",
                    "required":false,
                    "type":"string"
                },
                {
                    "id":2,
                    "name":"bar",
                    "required":true,
                    "type":"int"
                },
                {
                    "id":3,
                    "name":"baz",
                    "required":false,
                    "type":"boolean"
                }
            ],
            "identifier-field-ids":[2]
        }"#;
        (schema, record)
    }

    fn table_schema_nested() -> Schema {
        Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![2])
            .with_fields(vec![
                NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
                NestedField::required(
                    4,
                    "qux",
                    Type::List(ListType {
                        element_field: NestedField::list_element(
                            5,
                            Type::Primitive(PrimitiveType::String),
                            true,
                        )
                        .into(),
                    }),
                )
                .into(),
                NestedField::required(
                    6,
                    "quux",
                    Type::Map(MapType {
                        key_field: NestedField::map_key_element(
                            7,
                            Type::Primitive(PrimitiveType::String),
                        )
                        .into(),
                        value_field: NestedField::map_value_element(
                            8,
                            Type::Map(MapType {
                                key_field: NestedField::map_key_element(
                                    9,
                                    Type::Primitive(PrimitiveType::String),
                                )
                                .into(),
                                value_field: NestedField::map_value_element(
                                    10,
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
                .into(),
                NestedField::required(
                    11,
                    "location",
                    Type::List(ListType {
                        element_field: NestedField::list_element(
                            12,
                            Type::Struct(StructType::new(vec![
                                NestedField::optional(
                                    13,
                                    "latitude",
                                    Type::Primitive(PrimitiveType::Float),
                                )
                                .into(),
                                NestedField::optional(
                                    14,
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
                .into(),
                NestedField::optional(
                    15,
                    "person",
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(16, "name", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::required(17, "age", Type::Primitive(PrimitiveType::Int))
                            .into(),
                    ])),
                )
                .into(),
            ])
            .build()
            .unwrap()
    }

    #[test]
    fn test_schema_display() {
        let expected_str = "
table {
  1: foo: optional string\x20
  2: bar: required int\x20
  3: baz: optional boolean\x20
}
";

        assert_eq!(expected_str, format!("\n{}", table_schema_simple().0));
    }

    #[test]
    fn test_schema_build_failed_on_duplicate_names() {
        let ret = Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![1])
            .with_fields(vec![
                NestedField::required(1, "foo", Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "bar", Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "baz", Primitive(PrimitiveType::Boolean)).into(),
                NestedField::optional(4, "baz", Primitive(PrimitiveType::Boolean)).into(),
            ])
            .build();

        assert!(ret
            .unwrap_err()
            .message()
            .contains("Invalid schema: multiple fields for name baz"));
    }

    #[test]
    fn test_schema_index_by_name() {
        let expected_name_to_id = HashMap::from(
            [
                ("foo", 1),
                ("bar", 2),
                ("baz", 3),
                ("qux", 4),
                ("qux.element", 5),
                ("quux", 6),
                ("quux.key", 7),
                ("quux.value", 8),
                ("quux.value.key", 9),
                ("quux.value.value", 10),
                ("location", 11),
                ("location.element", 12),
                ("location.element.latitude", 13),
                ("location.element.longitude", 14),
                ("location.latitude", 13),
                ("location.longitude", 14),
                ("person", 15),
                ("person.name", 16),
                ("person.age", 17),
            ]
            .map(|e| (e.0.to_string(), e.1)),
        );

        let schema = table_schema_nested();
        assert_eq!(&expected_name_to_id, &schema.name_to_id);
    }

    #[test]
    fn test_schema_find_column_name() {
        let expected_column_name = HashMap::from([
            (1, "foo"),
            (2, "bar"),
            (3, "baz"),
            (4, "qux"),
            (5, "qux.element"),
            (6, "quux"),
            (7, "quux.key"),
            (8, "quux.value"),
            (9, "quux.value.key"),
            (10, "quux.value.value"),
            (11, "location"),
            (12, "location.element"),
            (13, "location.element.latitude"),
            (14, "location.element.longitude"),
        ]);

        let schema = table_schema_nested();
        for (id, name) in expected_column_name {
            assert_eq!(
                Some(name),
                schema.name_by_field_id(id),
                "Column name for field id {} not match.",
                id
            );
        }
    }

    #[test]
    fn test_schema_find_column_name_not_found() {
        let schema = table_schema_nested();

        assert!(schema.name_by_field_id(99).is_none());
    }

    #[test]
    fn test_schema_find_column_name_by_id_simple() {
        let expected_id_to_name = HashMap::from([(1, "foo"), (2, "bar"), (3, "baz")]);

        let schema = table_schema_simple().0;

        for (id, name) in expected_id_to_name {
            assert_eq!(
                Some(name),
                schema.name_by_field_id(id),
                "Column name for field id {} not match.",
                id
            );
        }
    }

    #[test]
    fn test_schema_find_simple() {
        let schema = table_schema_simple().0;

        assert_eq!(
            Some(schema.r#struct.fields()[0].clone()),
            schema.field_by_id(1).cloned()
        );
        assert_eq!(
            Some(schema.r#struct.fields()[1].clone()),
            schema.field_by_id(2).cloned()
        );
        assert_eq!(
            Some(schema.r#struct.fields()[2].clone()),
            schema.field_by_id(3).cloned()
        );

        assert!(schema.field_by_id(4).is_none());
        assert!(schema.field_by_name("non exist").is_none());
    }

    #[test]
    fn test_schema_find_nested() {
        let expected_id_to_field: HashMap<i32, NestedField> = HashMap::from([
            (
                1,
                NestedField::optional(1, "foo", Primitive(PrimitiveType::String)),
            ),
            (
                2,
                NestedField::required(2, "bar", Primitive(PrimitiveType::Int)),
            ),
            (
                3,
                NestedField::optional(3, "baz", Primitive(PrimitiveType::Boolean)),
            ),
            (
                4,
                NestedField::required(
                    4,
                    "qux",
                    Type::List(ListType {
                        element_field: NestedField::list_element(
                            5,
                            Type::Primitive(PrimitiveType::String),
                            true,
                        )
                        .into(),
                    }),
                ),
            ),
            (
                5,
                NestedField::required(5, "element", Primitive(PrimitiveType::String)),
            ),
            (
                6,
                NestedField::required(
                    6,
                    "quux",
                    Map(MapType {
                        key_field: NestedField::map_key_element(
                            7,
                            Primitive(PrimitiveType::String),
                        )
                        .into(),
                        value_field: NestedField::map_value_element(
                            8,
                            Map(MapType {
                                key_field: NestedField::map_key_element(
                                    9,
                                    Primitive(PrimitiveType::String),
                                )
                                .into(),
                                value_field: NestedField::map_value_element(
                                    10,
                                    Primitive(PrimitiveType::Int),
                                    true,
                                )
                                .into(),
                            }),
                            true,
                        )
                        .into(),
                    }),
                ),
            ),
            (
                7,
                NestedField::required(7, "key", Primitive(PrimitiveType::String)),
            ),
            (
                8,
                NestedField::required(
                    8,
                    "value",
                    Map(MapType {
                        key_field: NestedField::map_key_element(
                            9,
                            Primitive(PrimitiveType::String),
                        )
                        .into(),
                        value_field: NestedField::map_value_element(
                            10,
                            Primitive(PrimitiveType::Int),
                            true,
                        )
                        .into(),
                    }),
                ),
            ),
            (
                9,
                NestedField::required(9, "key", Primitive(PrimitiveType::String)),
            ),
            (
                10,
                NestedField::required(10, "value", Primitive(PrimitiveType::Int)),
            ),
            (
                11,
                NestedField::required(
                    11,
                    "location",
                    List(ListType {
                        element_field: NestedField::list_element(
                            12,
                            Struct(StructType::new(vec![
                                NestedField::optional(
                                    13,
                                    "latitude",
                                    Primitive(PrimitiveType::Float),
                                )
                                .into(),
                                NestedField::optional(
                                    14,
                                    "longitude",
                                    Primitive(PrimitiveType::Float),
                                )
                                .into(),
                            ])),
                            true,
                        )
                        .into(),
                    }),
                ),
            ),
            (
                12,
                NestedField::list_element(
                    12,
                    Struct(StructType::new(vec![
                        NestedField::optional(13, "latitude", Primitive(PrimitiveType::Float))
                            .into(),
                        NestedField::optional(14, "longitude", Primitive(PrimitiveType::Float))
                            .into(),
                    ])),
                    true,
                ),
            ),
            (
                13,
                NestedField::optional(13, "latitude", Primitive(PrimitiveType::Float)),
            ),
            (
                14,
                NestedField::optional(14, "longitude", Primitive(PrimitiveType::Float)),
            ),
            (
                15,
                NestedField::optional(
                    15,
                    "person",
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(16, "name", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::required(17, "age", Type::Primitive(PrimitiveType::Int))
                            .into(),
                    ])),
                ),
            ),
            (
                16,
                NestedField::optional(16, "name", Type::Primitive(PrimitiveType::String)),
            ),
            (
                17,
                NestedField::required(17, "age", Type::Primitive(PrimitiveType::Int)),
            ),
        ]);

        let schema = table_schema_nested();
        for (id, field) in expected_id_to_field {
            assert_eq!(
                Some(&field),
                schema.field_by_id(id).map(|f| f.as_ref()),
                "Field for {} not match.",
                id
            );
        }
    }
}
