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

use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use _serde::SchemaEnum;
use bimap::BiHashMap;
use itertools::{zip_eq, Itertools};
use serde::{Deserialize, Serialize};

use super::NestedField;
use crate::error::Result;
use crate::expr::accessor::StructAccessor;
use crate::spec::datatypes::{
    ListType, MapType, NestedFieldRef, PrimitiveType, StructType, Type, LIST_FIELD_NAME,
    MAP_KEY_FIELD_NAME, MAP_VALUE_FIELD_NAME,
};
use crate::{ensure_data_valid, Error, ErrorKind};

/// Type alias for schema id.
pub type SchemaId = i32;
/// Reference to [`Schema`].
pub type SchemaRef = Arc<Schema>;
pub(crate) const DEFAULT_SCHEMA_ID: SchemaId = 0;

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
    lowercase_name_to_id: HashMap<String, i32>,
    id_to_name: HashMap<i32, String>,

    field_id_to_accessor: HashMap<i32, Arc<StructAccessor>>,
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
    reassign_field_ids_from: Option<i32>,
}

impl SchemaBuilder {
    /// Add fields to schema builder.
    pub fn with_fields(mut self, fields: impl IntoIterator<Item = NestedFieldRef>) -> Self {
        self.fields.extend(fields);
        self
    }

    /// Reassign all field-ids (including nested) on build.
    /// Reassignment starts from the field-id specified in `start_from` (inclusive).
    ///
    /// All specified aliases and identifier fields will be updated to the new field-ids.
    pub(crate) fn with_reassigned_field_ids(mut self, start_from: u32) -> Self {
        self.reassign_field_ids_from = Some(start_from.try_into().unwrap_or(i32::MAX));
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
        let field_id_to_accessor = self.build_accessors();

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

        let lowercase_name_to_id = name_to_id
            .iter()
            .map(|(k, v)| (k.to_lowercase(), *v))
            .collect();

        let highest_field_id = id_to_field.keys().max().cloned().unwrap_or(0);

        let mut schema = Schema {
            r#struct,
            schema_id: self.schema_id,
            highest_field_id,
            identifier_field_ids: self.identifier_field_ids,
            alias_to_id: self.alias_to_id,
            id_to_field,

            name_to_id,
            lowercase_name_to_id,
            id_to_name,

            field_id_to_accessor,
        };

        if let Some(start_from) = self.reassign_field_ids_from {
            let mut id_reassigner = ReassignFieldIds::new(start_from);
            let new_fields = id_reassigner.reassign_field_ids(schema.r#struct.fields().to_vec())?;
            let new_identifier_field_ids =
                id_reassigner.apply_to_identifier_fields(schema.identifier_field_ids)?;
            let new_alias_to_id = id_reassigner.apply_to_aliases(schema.alias_to_id.clone())?;

            schema = Schema::builder()
                .with_schema_id(schema.schema_id)
                .with_fields(new_fields)
                .with_identifier_field_ids(new_identifier_field_ids)
                .with_alias(new_alias_to_id)
                .build()?;
        }

        Ok(schema)
    }

    fn build_accessors(&self) -> HashMap<i32, Arc<StructAccessor>> {
        let mut map = HashMap::new();

        for (pos, field) in self.fields.iter().enumerate() {
            match field.field_type.as_ref() {
                Type::Primitive(prim_type) => {
                    // add an accessor for this field
                    let accessor = Arc::new(StructAccessor::new(pos, prim_type.clone()));
                    map.insert(field.id, accessor.clone());
                }

                Type::Struct(nested) => {
                    // add accessors for nested fields
                    for (field_id, accessor) in Self::build_accessors_nested(nested.fields()) {
                        let new_accessor = Arc::new(StructAccessor::wrap(pos, accessor));
                        map.insert(field_id, new_accessor.clone());
                    }
                }
                _ => {
                    // Accessors don't get built for Map or List types
                }
            }
        }

        map
    }

    fn build_accessors_nested(fields: &[NestedFieldRef]) -> Vec<(i32, Box<StructAccessor>)> {
        let mut results = vec![];
        for (pos, field) in fields.iter().enumerate() {
            match field.field_type.as_ref() {
                Type::Primitive(prim_type) => {
                    let accessor = Box::new(StructAccessor::new(pos, prim_type.clone()));
                    results.push((field.id, accessor));
                }
                Type::Struct(nested) => {
                    let nested_accessors = Self::build_accessors_nested(nested.fields());

                    let wrapped_nested_accessors =
                        nested_accessors.into_iter().map(|(id, accessor)| {
                            let new_accessor = Box::new(StructAccessor::wrap(pos, accessor));
                            (id, new_accessor.clone())
                        });

                    results.extend(wrapped_nested_accessors);
                }
                _ => {
                    // Accessors don't get built for Map or List types
                }
            }
        }

        results
    }

    /// According to [the spec](https://iceberg.apache.org/spec/#identifier-fields), the identifier fields
    /// must meet the following requirements:
    /// - Float, double, and optional fields cannot be used as identifier fields.
    /// - Identifier fields may be nested in structs but cannot be nested within maps or lists.
    /// - A nested field cannot be used as an identifier field if it is nested in an optional struct, to avoid null values in identifiers.
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
            reassign_field_ids_from: None,
        }
    }

    /// Create a new schema builder from a schema.
    pub fn into_builder(self) -> SchemaBuilder {
        SchemaBuilder {
            schema_id: self.schema_id,
            fields: self.r#struct.fields().to_vec(),
            alias_to_id: self.alias_to_id,
            identifier_field_ids: self.identifier_field_ids,
            reassign_field_ids_from: None,
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

    /// Get field by field name, but in case-insensitive way.
    ///
    /// Both full name and short name could work here.
    pub fn field_by_name_case_insensitive(&self, field_name: &str) -> Option<&NestedFieldRef> {
        self.lowercase_name_to_id
            .get(&field_name.to_lowercase())
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
    pub fn schema_id(&self) -> SchemaId {
        self.schema_id
    }

    /// Returns [`r#struct`].
    #[inline]
    pub fn as_struct(&self) -> &StructType {
        &self.r#struct
    }

    /// Returns [`identifier_field_ids`].
    #[inline]
    pub fn identifier_field_ids(&self) -> impl ExactSizeIterator<Item = i32> + '_ {
        self.identifier_field_ids.iter().copied()
    }

    /// Get field id by full name.
    pub fn field_id_by_name(&self, name: &str) -> Option<i32> {
        self.name_to_id.get(name).copied()
    }

    /// Get field id by full name.
    pub fn name_by_field_id(&self, field_id: i32) -> Option<&str> {
        self.id_to_name.get(&field_id).map(String::as_str)
    }

    /// Get an accessor for retrieving data in a struct
    pub fn accessor_by_field_id(&self, field_id: i32) -> Option<Arc<StructAccessor>> {
        self.field_id_to_accessor.get(&field_id).cloned()
    }

    /// Check if this schema is identical to another schema semantically - excluding schema id.
    pub(crate) fn is_same_schema(&self, other: &SchemaRef) -> bool {
        self.as_struct().eq(other.as_struct())
            && self.identifier_field_ids().eq(other.identifier_field_ids())
    }

    /// Change the schema id of this schema.
    // This is redundant with the `with_schema_id` method on the builder, but useful
    // as it is infallible in contrast to the builder `build()` method.
    pub(crate) fn with_schema_id(self, schema_id: SchemaId) -> Self {
        Self { schema_id, ..self }
    }

    /// Return A HashMap matching field ids to field names.
    pub(crate) fn field_id_to_name_map(&self) -> &HashMap<i32, String> {
        &self.id_to_name
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

/// Creates a field id to field map.
pub fn index_by_id(r#struct: &StructType) -> Result<HashMap<i32, NestedFieldRef>> {
    struct IndexById(HashMap<i32, NestedFieldRef>);

    impl SchemaVisitor for IndexById {
        type T = ();

        fn schema(&mut self, _schema: &Schema, _value: ()) -> Result<()> {
            Ok(())
        }

        fn field(&mut self, field: &NestedFieldRef, _value: ()) -> Result<()> {
            try_insert_field(&mut self.0, field.id, field.clone())
        }

        fn r#struct(&mut self, _struct: &StructType, _results: Vec<Self::T>) -> Result<Self::T> {
            Ok(())
        }

        fn list(&mut self, list: &ListType, _value: Self::T) -> Result<Self::T> {
            try_insert_field(
                &mut self.0,
                list.element_field.id,
                list.element_field.clone(),
            )
        }

        fn map(&mut self, map: &MapType, _key_value: Self::T, _value: Self::T) -> Result<Self::T> {
            try_insert_field(&mut self.0, map.key_field.id, map.key_field.clone())?;
            try_insert_field(&mut self.0, map.value_field.id, map.value_field.clone())
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
            if let Some(parent) = self.parents.last().copied() {
                self.result.insert(field.id, parent);
            }
            self.parents.push(field.id);
            Ok(())
        }

        fn after_struct_field(&mut self, _field: &NestedFieldRef) -> Result<()> {
            self.parents.pop();
            Ok(())
        }

        fn before_list_element(&mut self, field: &NestedFieldRef) -> Result<()> {
            if let Some(parent) = self.parents.last().copied() {
                self.result.insert(field.id, parent);
            }
            self.parents.push(field.id);
            Ok(())
        }

        fn after_list_element(&mut self, _field: &NestedFieldRef) -> Result<()> {
            self.parents.pop();
            Ok(())
        }

        fn before_map_key(&mut self, field: &NestedFieldRef) -> Result<()> {
            if let Some(parent) = self.parents.last().copied() {
                self.result.insert(field.id, parent);
            }
            self.parents.push(field.id);
            Ok(())
        }

        fn after_map_key(&mut self, _field: &NestedFieldRef) -> Result<()> {
            self.parents.pop();
            Ok(())
        }

        fn before_map_value(&mut self, field: &NestedFieldRef) -> Result<()> {
            if let Some(parent) = self.parents.last().copied() {
                self.result.insert(field.id, parent);
            }
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

        fn field(&mut self, _field: &NestedFieldRef, _value: Self::T) -> Result<Self::T> {
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
        self.add_field(LIST_FIELD_NAME, list.element_field.id)
    }

    fn map(&mut self, map: &MapType, _key_value: Self::T, _value: Self::T) -> Result<Self::T> {
        self.add_field(MAP_KEY_FIELD_NAME, map.key_field.id)?;
        self.add_field(MAP_VALUE_FIELD_NAME, map.value_field.id)
    }

    fn primitive(&mut self, _p: &PrimitiveType) -> Result<Self::T> {
        Ok(())
    }
}

struct PruneColumn {
    selected: HashSet<i32>,
    select_full_types: bool,
}

/// Visit a schema and returns only the fields selected by id set
pub fn prune_columns(
    schema: &Schema,
    selected: impl IntoIterator<Item = i32>,
    select_full_types: bool,
) -> Result<Type> {
    let mut visitor = PruneColumn::new(HashSet::from_iter(selected), select_full_types);
    let result = visit_schema(schema, &mut visitor);

    match result {
        Ok(s) => {
            if let Some(struct_type) = s {
                Ok(struct_type)
            } else {
                Ok(Type::Struct(StructType::default()))
            }
        }
        Err(e) => Err(e),
    }
}

impl PruneColumn {
    fn new(selected: HashSet<i32>, select_full_types: bool) -> Self {
        Self {
            selected,
            select_full_types,
        }
    }

    fn project_selected_struct(projected_field: Option<Type>) -> Result<StructType> {
        match projected_field {
            // If the field is a StructType, return it as such
            Some(Type::Struct(s)) => Ok(s),
            Some(_) => Err(Error::new(
                ErrorKind::Unexpected,
                "Projected field with struct type must be struct".to_string(),
            )),
            // If projected_field is None or not a StructType, return an empty StructType
            None => Ok(StructType::default()),
        }
    }
    fn project_list(list: &ListType, element_result: Type) -> Result<ListType> {
        if *list.element_field.field_type == element_result {
            return Ok(list.clone());
        }
        Ok(ListType {
            element_field: Arc::new(NestedField {
                id: list.element_field.id,
                name: list.element_field.name.clone(),
                required: list.element_field.required,
                field_type: Box::new(element_result),
                doc: list.element_field.doc.clone(),
                initial_default: list.element_field.initial_default.clone(),
                write_default: list.element_field.write_default.clone(),
            }),
        })
    }
    fn project_map(map: &MapType, value_result: Type) -> Result<MapType> {
        if *map.value_field.field_type == value_result {
            return Ok(map.clone());
        }
        Ok(MapType {
            key_field: map.key_field.clone(),
            value_field: Arc::new(NestedField {
                id: map.value_field.id,
                name: map.value_field.name.clone(),
                required: map.value_field.required,
                field_type: Box::new(value_result),
                doc: map.value_field.doc.clone(),
                initial_default: map.value_field.initial_default.clone(),
                write_default: map.value_field.write_default.clone(),
            }),
        })
    }
}

/// Join two schemas by concatenating fields. Return [Error] if the schemas have different columns
/// with the same id.
pub fn join_schemas(left: &Schema, right: &Schema) -> Result<Schema> {
    let mut joined_fields: Vec<NestedFieldRef> =
        left.as_struct().fields().iter().cloned().collect_vec();

    for right_field in right.as_struct().fields() {
        match left.field_by_id(right_field.id) {
            None => {
                joined_fields.push(right_field.clone());
            }
            Some(left_field) => {
                if left_field != right_field {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Schemas have different columns with the same id: {:?}, {:?}",
                            left_field, right_field
                        ),
                    ));
                }
            }
        }
    }

    Schema::builder().with_fields(joined_fields).build()
}

impl SchemaVisitor for PruneColumn {
    type T = Option<Type>;

    fn schema(&mut self, _schema: &Schema, value: Option<Type>) -> Result<Option<Type>> {
        Ok(Some(value.unwrap()))
    }

    fn field(&mut self, field: &NestedFieldRef, value: Option<Type>) -> Result<Option<Type>> {
        if self.selected.contains(&field.id) {
            if self.select_full_types {
                Ok(Some(*field.field_type.clone()))
            } else if field.field_type.is_struct() {
                return Ok(Some(Type::Struct(PruneColumn::project_selected_struct(
                    value,
                )?)));
            } else if !field.field_type.is_nested() {
                return Ok(Some(*field.field_type.clone()));
            } else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Can't project list or map field directly when not selecting full type."
                        .to_string(),
                )
                .with_context("field_id", field.id.to_string())
                .with_context("field_type", field.field_type.to_string()));
            }
        } else {
            Ok(value)
        }
    }

    fn r#struct(
        &mut self,
        r#struct: &StructType,
        results: Vec<Option<Type>>,
    ) -> Result<Option<Type>> {
        let fields = r#struct.fields();
        let mut selected_field = Vec::with_capacity(fields.len());
        let mut same_type = true;

        for (field, projected_type) in zip_eq(fields.iter(), results.iter()) {
            if let Some(projected_type) = projected_type {
                if *field.field_type == *projected_type {
                    selected_field.push(field.clone());
                } else {
                    same_type = false;
                    let new_field = NestedField {
                        id: field.id,
                        name: field.name.clone(),
                        required: field.required,
                        field_type: Box::new(projected_type.clone()),
                        doc: field.doc.clone(),
                        initial_default: field.initial_default.clone(),
                        write_default: field.write_default.clone(),
                    };
                    selected_field.push(Arc::new(new_field));
                }
            }
        }

        if !selected_field.is_empty() {
            if selected_field.len() == fields.len() && same_type {
                return Ok(Some(Type::Struct(r#struct.clone())));
            } else {
                return Ok(Some(Type::Struct(StructType::new(selected_field))));
            }
        }
        Ok(None)
    }

    fn list(&mut self, list: &ListType, value: Option<Type>) -> Result<Option<Type>> {
        if self.selected.contains(&list.element_field.id) {
            if self.select_full_types {
                Ok(Some(Type::List(list.clone())))
            } else if list.element_field.field_type.is_struct() {
                let projected_struct = PruneColumn::project_selected_struct(value).unwrap();
                return Ok(Some(Type::List(PruneColumn::project_list(
                    list,
                    Type::Struct(projected_struct),
                )?)));
            } else if list.element_field.field_type.is_primitive() {
                return Ok(Some(Type::List(list.clone())));
            } else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Cannot explicitly project List or Map types, List element {} of type {} was selected", list.element_field.id, list.element_field.field_type),
                ));
            }
        } else if let Some(result) = value {
            Ok(Some(Type::List(PruneColumn::project_list(list, result)?)))
        } else {
            Ok(None)
        }
    }

    fn map(
        &mut self,
        map: &MapType,
        _key_value: Option<Type>,
        value: Option<Type>,
    ) -> Result<Option<Type>> {
        if self.selected.contains(&map.value_field.id) {
            if self.select_full_types {
                Ok(Some(Type::Map(map.clone())))
            } else if map.value_field.field_type.is_struct() {
                let projected_struct =
                    PruneColumn::project_selected_struct(Some(value.unwrap())).unwrap();
                return Ok(Some(Type::Map(PruneColumn::project_map(
                    map,
                    Type::Struct(projected_struct),
                )?)));
            } else if map.value_field.field_type.is_primitive() {
                return Ok(Some(Type::Map(map.clone())));
            } else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Cannot explicitly project List or Map types, Map value {} of type {} was selected", map.value_field.id, map.value_field.field_type),
                ));
            }
        } else if let Some(value_result) = value {
            return Ok(Some(Type::Map(PruneColumn::project_map(
                map,
                value_result,
            )?)));
        } else if self.selected.contains(&map.key_field.id) {
            Ok(Some(Type::Map(map.clone())))
        } else {
            Ok(None)
        }
    }

    fn primitive(&mut self, _p: &PrimitiveType) -> Result<Option<Type>> {
        Ok(None)
    }
}

struct ReassignFieldIds {
    next_field_id: i32,
    old_to_new_id: HashMap<i32, i32>,
}

fn try_insert_field<V>(map: &mut HashMap<i32, V>, field_id: i32, value: V) -> Result<()> {
    map.insert(field_id, value).map_or_else(
        || Ok(()),
        |_| {
            Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Found duplicate 'field.id' {}. Field ids must be unique.",
                    field_id
                ),
            ))
        },
    )
}

// We are not using the visitor here, as post order traversal is not desired.
// Instead we want to re-assign all fields on one level first before diving deeper.
impl ReassignFieldIds {
    fn new(start_from: i32) -> Self {
        Self {
            next_field_id: start_from,
            old_to_new_id: HashMap::new(),
        }
    }

    fn reassign_field_ids(&mut self, fields: Vec<NestedFieldRef>) -> Result<Vec<NestedFieldRef>> {
        // Visit fields on the same level first
        let outer_fields = fields
            .into_iter()
            .map(|field| {
                try_insert_field(&mut self.old_to_new_id, field.id, self.next_field_id)?;
                let new_field = Arc::unwrap_or_clone(field).with_id(self.next_field_id);
                self.increase_next_field_id()?;
                Ok(Arc::new(new_field))
            })
            .collect::<Result<Vec<_>>>()?;

        // Now visit nested fields
        outer_fields
            .into_iter()
            .map(|field| {
                if field.field_type.is_primitive() {
                    Ok(field)
                } else {
                    let mut new_field = Arc::unwrap_or_clone(field);
                    *new_field.field_type = self.reassign_ids_visit_type(*new_field.field_type)?;
                    Ok(Arc::new(new_field))
                }
            })
            .collect()
    }

    fn reassign_ids_visit_type(&mut self, field_type: Type) -> Result<Type> {
        match field_type {
            Type::Primitive(s) => Ok(Type::Primitive(s)),
            Type::Struct(s) => {
                let new_fields = self.reassign_field_ids(s.fields().to_vec())?;
                Ok(Type::Struct(StructType::new(new_fields)))
            }
            Type::List(l) => {
                self.old_to_new_id
                    .insert(l.element_field.id, self.next_field_id);
                let mut element_field = Arc::unwrap_or_clone(l.element_field);
                element_field.id = self.next_field_id;
                self.increase_next_field_id()?;
                *element_field.field_type =
                    self.reassign_ids_visit_type(*element_field.field_type)?;
                Ok(Type::List(ListType {
                    element_field: Arc::new(element_field),
                }))
            }
            Type::Map(m) => {
                self.old_to_new_id
                    .insert(m.key_field.id, self.next_field_id);
                let mut key_field = Arc::unwrap_or_clone(m.key_field);
                key_field.id = self.next_field_id;
                self.increase_next_field_id()?;
                *key_field.field_type = self.reassign_ids_visit_type(*key_field.field_type)?;

                self.old_to_new_id
                    .insert(m.value_field.id, self.next_field_id);
                let mut value_field = Arc::unwrap_or_clone(m.value_field);
                value_field.id = self.next_field_id;
                self.increase_next_field_id()?;
                *value_field.field_type = self.reassign_ids_visit_type(*value_field.field_type)?;

                Ok(Type::Map(MapType {
                    key_field: Arc::new(key_field),
                    value_field: Arc::new(value_field),
                }))
            }
        }
    }

    fn increase_next_field_id(&mut self) -> Result<()> {
        self.next_field_id = self.next_field_id.checked_add(1).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Field ID overflowed, cannot add more fields",
            )
        })?;
        Ok(())
    }

    fn apply_to_identifier_fields(&self, field_ids: HashSet<i32>) -> Result<HashSet<i32>> {
        field_ids
            .into_iter()
            .map(|id| {
                self.old_to_new_id.get(&id).copied().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Identifier Field ID {} not found", id),
                    )
                })
            })
            .collect()
    }

    fn apply_to_aliases(&self, alias: BiHashMap<String, i32>) -> Result<BiHashMap<String, i32>> {
        alias
            .into_iter()
            .map(|(name, id)| {
                self.old_to_new_id
                    .get(&id)
                    .copied()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!("Field with id {} for alias {} not found", id, name),
                        )
                    })
                    .map(|new_id| (name, new_id))
            })
            .collect()
    }
}

pub(super) mod _serde {
    /// This is a helper module that defines types to help with serialization/deserialization.
    /// For deserialization the input first gets read into either the [SchemaV1] or [SchemaV2] struct
    /// and then converted into the [Schema] struct. Serialization works the other way around.
    /// [SchemaV1] and [SchemaV2] are internal struct that are only used for serialization and deserialization.
    use serde::Deserialize;
    /// This is a helper module that defines types to help with serialization/deserialization.
    /// For deserialization the input first gets read into either the [SchemaV1] or [SchemaV2] struct
    /// and then converted into the [Schema] struct. Serialization works the other way around.
    /// [SchemaV1] and [SchemaV2] are internal struct that are only used for serialization and deserialization.
    use serde::Serialize;

    use super::{Schema, DEFAULT_SCHEMA_ID};
    use crate::spec::StructType;
    use crate::{Error, Result};

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
    use std::collections::{HashMap, HashSet};

    use bimap::BiHashMap;

    use super::DEFAULT_SCHEMA_ID;
    use crate::spec::datatypes::Type::{List, Map, Primitive, Struct};
    use crate::spec::datatypes::{
        ListType, MapType, NestedField, NestedFieldRef, PrimitiveType, StructType, Type,
    };
    use crate::spec::schema::Schema;
    use crate::spec::schema::_serde::{SchemaEnum, SchemaV1, SchemaV2};
    use crate::spec::values::Map as MapValue;
    use crate::spec::{index_parents, prune_columns, Datum, Literal};

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

    pub fn table_schema_nested() -> Schema {
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
    fn test_schema_into_builder() {
        let original_schema = table_schema_nested();
        let builder = original_schema.clone().into_builder();
        let schema = builder.build().unwrap();

        assert_eq!(original_schema, schema);
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
    fn test_schema_index_by_name_case_insensitive() {
        let expected_name_to_id = HashMap::from(
            [
                ("fOo", 1),
                ("Bar", 2),
                ("BAz", 3),
                ("quX", 4),
                ("quX.ELEment", 5),
                ("qUUx", 6),
                ("QUUX.KEY", 7),
                ("QUUX.Value", 8),
                ("qUUX.VALUE.Key", 9),
                ("qUux.VaLue.Value", 10),
                ("lOCAtION", 11),
                ("LOCAtioN.ELeMENt", 12),
                ("LoCATion.element.LATitude", 13),
                ("locatION.ElemeNT.LONgitude", 14),
                ("LOCAtiON.LATITUDE", 13),
                ("LOCATION.LONGITUDE", 14),
                ("PERSon", 15),
                ("PERSON.Name", 16),
                ("peRSON.AGe", 17),
            ]
            .map(|e| (e.0.to_string(), e.1)),
        );

        let schema = table_schema_nested();
        for (name, id) in expected_name_to_id {
            assert_eq!(
                Some(id),
                schema.field_by_name_case_insensitive(&name).map(|f| f.id)
            );
        }
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

    #[test]
    fn test_build_accessors() {
        let schema = table_schema_nested();

        let test_struct = crate::spec::Struct::from_iter(vec![
            Some(Literal::string("foo value")),
            Some(Literal::int(1002)),
            Some(Literal::bool(true)),
            Some(Literal::List(vec![
                Some(Literal::string("qux item 1")),
                Some(Literal::string("qux item 2")),
            ])),
            Some(Literal::Map(MapValue::from([(
                Literal::string("quux key 1"),
                Some(Literal::Map(MapValue::from([(
                    Literal::string("quux nested key 1"),
                    Some(Literal::int(1000)),
                )]))),
            )]))),
            Some(Literal::List(vec![Some(Literal::Struct(
                crate::spec::Struct::from_iter(vec![
                    Some(Literal::float(52.509_09)),
                    Some(Literal::float(-1.885_249)),
                ]),
            ))])),
            Some(Literal::Struct(crate::spec::Struct::from_iter(vec![
                Some(Literal::string("Testy McTest")),
                Some(Literal::int(33)),
            ]))),
        ]);

        assert_eq!(
            schema
                .accessor_by_field_id(1)
                .unwrap()
                .get(&test_struct)
                .unwrap(),
            Some(Datum::string("foo value"))
        );
        assert_eq!(
            schema
                .accessor_by_field_id(2)
                .unwrap()
                .get(&test_struct)
                .unwrap(),
            Some(Datum::int(1002))
        );
        assert_eq!(
            schema
                .accessor_by_field_id(3)
                .unwrap()
                .get(&test_struct)
                .unwrap(),
            Some(Datum::bool(true))
        );
        assert_eq!(
            schema
                .accessor_by_field_id(16)
                .unwrap()
                .get(&test_struct)
                .unwrap(),
            Some(Datum::string("Testy McTest"))
        );
        assert_eq!(
            schema
                .accessor_by_field_id(17)
                .unwrap()
                .get(&test_struct)
                .unwrap(),
            Some(Datum::int(33))
        );
    }

    #[test]
    fn test_schema_prune_columns_string() {
        let expected_type = Type::from(
            Schema::builder()
                .with_fields(vec![NestedField::optional(
                    1,
                    "foo",
                    Type::Primitive(PrimitiveType::String),
                )
                .into()])
                .build()
                .unwrap()
                .as_struct()
                .clone(),
        );
        let schema = table_schema_nested();
        let selected: HashSet<i32> = HashSet::from([1]);
        let result = prune_columns(&schema, selected, false);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_type);
    }

    #[test]
    fn test_schema_prune_columns_string_full() {
        let expected_type = Type::from(
            Schema::builder()
                .with_fields(vec![NestedField::optional(
                    1,
                    "foo",
                    Type::Primitive(PrimitiveType::String),
                )
                .into()])
                .build()
                .unwrap()
                .as_struct()
                .clone(),
        );
        let schema = table_schema_nested();
        let selected: HashSet<i32> = HashSet::from([1]);
        let result = prune_columns(&schema, selected, true);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_type);
    }

    #[test]
    fn test_schema_prune_columns_list() {
        let expected_type = Type::from(
            Schema::builder()
                .with_fields(vec![NestedField::required(
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
                .into()])
                .build()
                .unwrap()
                .as_struct()
                .clone(),
        );
        let schema = table_schema_nested();
        let selected: HashSet<i32> = HashSet::from([5]);
        let result = prune_columns(&schema, selected, false);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_type);
    }

    #[test]
    fn test_prune_columns_list_itself() {
        let schema = table_schema_nested();
        let selected: HashSet<i32> = HashSet::from([4]);
        let result = prune_columns(&schema, selected, false);
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_prune_columns_list_full() {
        let expected_type = Type::from(
            Schema::builder()
                .with_fields(vec![NestedField::required(
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
                .into()])
                .build()
                .unwrap()
                .as_struct()
                .clone(),
        );
        let schema = table_schema_nested();
        let selected: HashSet<i32> = HashSet::from([5]);
        let result = prune_columns(&schema, selected, true);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_type);
    }

    #[test]
    fn test_prune_columns_map() {
        let expected_type = Type::from(
            Schema::builder()
                .with_fields(vec![NestedField::required(
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
                .into()])
                .build()
                .unwrap()
                .as_struct()
                .clone(),
        );
        let schema = table_schema_nested();
        let selected: HashSet<i32> = HashSet::from([9]);
        let result = prune_columns(&schema, selected, false);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_type);
    }

    #[test]
    fn test_prune_columns_map_itself() {
        let schema = table_schema_nested();
        let selected: HashSet<i32> = HashSet::from([6]);
        let result = prune_columns(&schema, selected, false);
        assert!(result.is_err());
    }

    #[test]
    fn test_prune_columns_map_full() {
        let expected_type = Type::from(
            Schema::builder()
                .with_fields(vec![NestedField::required(
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
                .into()])
                .build()
                .unwrap()
                .as_struct()
                .clone(),
        );
        let schema = table_schema_nested();
        let selected: HashSet<i32> = HashSet::from([9]);
        let result = prune_columns(&schema, selected, true);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_type);
    }

    #[test]
    fn test_prune_columns_map_key() {
        let expected_type = Type::from(
            Schema::builder()
                .with_fields(vec![NestedField::required(
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
                .into()])
                .build()
                .unwrap()
                .as_struct()
                .clone(),
        );
        let schema = table_schema_nested();
        let selected: HashSet<i32> = HashSet::from([10]);
        let result = prune_columns(&schema, selected, false);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_type);
    }

    #[test]
    fn test_prune_columns_struct() {
        let expected_type = Type::from(
            Schema::builder()
                .with_fields(vec![NestedField::optional(
                    15,
                    "person",
                    Type::Struct(StructType::new(vec![NestedField::optional(
                        16,
                        "name",
                        Type::Primitive(PrimitiveType::String),
                    )
                    .into()])),
                )
                .into()])
                .build()
                .unwrap()
                .as_struct()
                .clone(),
        );
        let schema = table_schema_nested();
        let selected: HashSet<i32> = HashSet::from([16]);
        let result = prune_columns(&schema, selected, false);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_type);
    }

    #[test]
    fn test_prune_columns_struct_full() {
        let expected_type = Type::from(
            Schema::builder()
                .with_fields(vec![NestedField::optional(
                    15,
                    "person",
                    Type::Struct(StructType::new(vec![NestedField::optional(
                        16,
                        "name",
                        Type::Primitive(PrimitiveType::String),
                    )
                    .into()])),
                )
                .into()])
                .build()
                .unwrap()
                .as_struct()
                .clone(),
        );
        let schema = table_schema_nested();
        let selected: HashSet<i32> = HashSet::from([16]);
        let result = prune_columns(&schema, selected, true);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_type);
    }

    #[test]
    fn test_prune_columns_empty_struct() {
        let schema_with_empty_struct_field = Schema::builder()
            .with_fields(vec![NestedField::optional(
                15,
                "person",
                Type::Struct(StructType::new(vec![])),
            )
            .into()])
            .build()
            .unwrap();
        let expected_type = Type::from(
            Schema::builder()
                .with_fields(vec![NestedField::optional(
                    15,
                    "person",
                    Type::Struct(StructType::new(vec![])),
                )
                .into()])
                .build()
                .unwrap()
                .as_struct()
                .clone(),
        );
        let selected: HashSet<i32> = HashSet::from([15]);
        let result = prune_columns(&schema_with_empty_struct_field, selected, false);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_type);
    }

    #[test]
    fn test_prune_columns_empty_struct_full() {
        let schema_with_empty_struct_field = Schema::builder()
            .with_fields(vec![NestedField::optional(
                15,
                "person",
                Type::Struct(StructType::new(vec![])),
            )
            .into()])
            .build()
            .unwrap();
        let expected_type = Type::from(
            Schema::builder()
                .with_fields(vec![NestedField::optional(
                    15,
                    "person",
                    Type::Struct(StructType::new(vec![])),
                )
                .into()])
                .build()
                .unwrap()
                .as_struct()
                .clone(),
        );
        let selected: HashSet<i32> = HashSet::from([15]);
        let result = prune_columns(&schema_with_empty_struct_field, selected, true);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_type);
    }

    #[test]
    fn test_prune_columns_struct_in_map() {
        let schema_with_struct_in_map_field = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![NestedField::required(
                6,
                "id_to_person",
                Type::Map(MapType {
                    key_field: NestedField::map_key_element(7, Type::Primitive(PrimitiveType::Int))
                        .into(),
                    value_field: NestedField::map_value_element(
                        8,
                        Type::Struct(StructType::new(vec![
                            NestedField::optional(10, "name", Primitive(PrimitiveType::String))
                                .into(),
                            NestedField::required(11, "age", Primitive(PrimitiveType::Int)).into(),
                        ])),
                        true,
                    )
                    .into(),
                }),
            )
            .into()])
            .build()
            .unwrap();
        let expected_type = Type::from(
            Schema::builder()
                .with_fields(vec![NestedField::required(
                    6,
                    "id_to_person",
                    Type::Map(MapType {
                        key_field: NestedField::map_key_element(
                            7,
                            Type::Primitive(PrimitiveType::Int),
                        )
                        .into(),
                        value_field: NestedField::map_value_element(
                            8,
                            Type::Struct(StructType::new(vec![NestedField::required(
                                11,
                                "age",
                                Primitive(PrimitiveType::Int),
                            )
                            .into()])),
                            true,
                        )
                        .into(),
                    }),
                )
                .into()])
                .build()
                .unwrap()
                .as_struct()
                .clone(),
        );
        let selected: HashSet<i32> = HashSet::from([11]);
        let result = prune_columns(&schema_with_struct_in_map_field, selected, false);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_type);
    }
    #[test]
    fn test_prune_columns_struct_in_map_full() {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![NestedField::required(
                6,
                "id_to_person",
                Type::Map(MapType {
                    key_field: NestedField::map_key_element(7, Type::Primitive(PrimitiveType::Int))
                        .into(),
                    value_field: NestedField::map_value_element(
                        8,
                        Type::Struct(StructType::new(vec![
                            NestedField::optional(10, "name", Primitive(PrimitiveType::String))
                                .into(),
                            NestedField::required(11, "age", Primitive(PrimitiveType::Int)).into(),
                        ])),
                        true,
                    )
                    .into(),
                }),
            )
            .into()])
            .build()
            .unwrap();
        let expected_type = Type::from(
            Schema::builder()
                .with_fields(vec![NestedField::required(
                    6,
                    "id_to_person",
                    Type::Map(MapType {
                        key_field: NestedField::map_key_element(
                            7,
                            Type::Primitive(PrimitiveType::Int),
                        )
                        .into(),
                        value_field: NestedField::map_value_element(
                            8,
                            Type::Struct(StructType::new(vec![NestedField::required(
                                11,
                                "age",
                                Primitive(PrimitiveType::Int),
                            )
                            .into()])),
                            true,
                        )
                        .into(),
                    }),
                )
                .into()])
                .build()
                .unwrap()
                .as_struct()
                .clone(),
        );
        let selected: HashSet<i32> = HashSet::from([11]);
        let result = prune_columns(&schema, selected, true);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_type);
    }

    #[test]
    fn test_prune_columns_select_original_schema() {
        let schema = table_schema_nested();
        let selected: HashSet<i32> = (0..schema.highest_field_id() + 1).collect();
        let result = prune_columns(&schema, selected, true);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Type::Struct(schema.as_struct().clone()));
    }

    #[test]
    fn test_highest_field_id() {
        let schema = table_schema_nested();
        assert_eq!(17, schema.highest_field_id());

        let schema = table_schema_simple().0;
        assert_eq!(3, schema.highest_field_id());
    }

    #[test]
    fn test_highest_field_id_no_fields() {
        let schema = Schema::builder().with_schema_id(1).build().unwrap();
        assert_eq!(0, schema.highest_field_id());
    }

    #[test]
    fn test_reassign_ids() {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![3])
            .with_alias(BiHashMap::from_iter(vec![("bar_alias".to_string(), 3)]))
            .with_fields(vec![
                NestedField::optional(5, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(3, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(4, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .build()
            .unwrap();

        let reassigned_schema = schema
            .into_builder()
            .with_reassigned_field_ids(0)
            .build()
            .unwrap();

        let expected = Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![1])
            .with_alias(BiHashMap::from_iter(vec![("bar_alias".to_string(), 1)]))
            .with_fields(vec![
                NestedField::optional(0, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(1, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .build()
            .unwrap();

        pretty_assertions::assert_eq!(expected, reassigned_schema);
        assert_eq!(reassigned_schema.highest_field_id(), 2);
    }

    #[test]
    fn test_reassigned_ids_nested() {
        let schema = table_schema_nested();
        let reassigned_schema = schema
            .into_builder()
            .with_alias(BiHashMap::from_iter(vec![("bar_alias".to_string(), 2)]))
            .with_reassigned_field_ids(0)
            .build()
            .unwrap();

        let expected = Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![1])
            .with_alias(BiHashMap::from_iter(vec![("bar_alias".to_string(), 1)]))
            .with_fields(vec![
                NestedField::optional(0, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(1, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
                NestedField::required(
                    3,
                    "qux",
                    Type::List(ListType {
                        element_field: NestedField::list_element(
                            7,
                            Type::Primitive(PrimitiveType::String),
                            true,
                        )
                        .into(),
                    }),
                )
                .into(),
                NestedField::required(
                    4,
                    "quux",
                    Type::Map(MapType {
                        key_field: NestedField::map_key_element(
                            8,
                            Type::Primitive(PrimitiveType::String),
                        )
                        .into(),
                        value_field: NestedField::map_value_element(
                            9,
                            Type::Map(MapType {
                                key_field: NestedField::map_key_element(
                                    10,
                                    Type::Primitive(PrimitiveType::String),
                                )
                                .into(),
                                value_field: NestedField::map_value_element(
                                    11,
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
                    5,
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
                    6,
                    "person",
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(15, "name", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::required(16, "age", Type::Primitive(PrimitiveType::Int))
                            .into(),
                    ])),
                )
                .into(),
            ])
            .build()
            .unwrap();

        pretty_assertions::assert_eq!(expected, reassigned_schema);
        assert_eq!(reassigned_schema.highest_field_id(), 16);
        assert_eq!(reassigned_schema.field_by_id(6).unwrap().name, "person");
        assert_eq!(reassigned_schema.field_by_id(16).unwrap().name, "age");
    }

    #[test]
    fn test_reassign_ids_fails_with_duplicate_ids() {
        let reassigned_schema = Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![5])
            .with_alias(BiHashMap::from_iter(vec![("bar_alias".to_string(), 3)]))
            .with_fields(vec![
                NestedField::required(5, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(3, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .with_reassigned_field_ids(0)
            .build()
            .unwrap_err();

        assert!(reassigned_schema.message().contains("'field.id' 3"));
    }

    #[test]
    fn test_field_ids_must_be_unique() {
        let reassigned_schema = Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![5])
            .with_alias(BiHashMap::from_iter(vec![("bar_alias".to_string(), 3)]))
            .with_fields(vec![
                NestedField::required(5, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(3, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .build()
            .unwrap_err();

        assert!(reassigned_schema.message().contains("'field.id' 3"));
    }

    #[test]
    fn test_reassign_ids_empty_schema() {
        let schema = Schema::builder().with_schema_id(1).build().unwrap();
        let reassigned_schema = schema
            .clone()
            .into_builder()
            .with_reassigned_field_ids(0)
            .build()
            .unwrap();

        assert_eq!(schema, reassigned_schema);
        assert_eq!(schema.highest_field_id(), 0);
    }

    #[test]
    fn test_index_parent() {
        let schema = table_schema_nested();
        let result = index_parents(&schema.r#struct).unwrap();
        assert_eq!(result.get(&5).unwrap(), &4);
        assert_eq!(result.get(&7).unwrap(), &6);
        assert_eq!(result.get(&8).unwrap(), &6);
        assert_eq!(result.get(&9).unwrap(), &8);
        assert_eq!(result.get(&10).unwrap(), &8);
        assert_eq!(result.get(&12).unwrap(), &11);
        assert_eq!(result.get(&13).unwrap(), &12);
        assert_eq!(result.get(&14).unwrap(), &12);
        assert_eq!(result.get(&16).unwrap(), &15);
        assert_eq!(result.get(&17).unwrap(), &15);
    }

    #[test]
    fn test_identifier_field_ids() {
        // field in map
        assert!(Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![2])
            .with_fields(vec![NestedField::required(
                1,
                "Map",
                Type::Map(MapType::new(
                    NestedField::map_key_element(2, Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::map_value_element(
                        3,
                        Type::Primitive(PrimitiveType::Boolean),
                        true,
                    )
                    .into(),
                )),
            )
            .into()])
            .build()
            .is_err());
        assert!(Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![3])
            .with_fields(vec![NestedField::required(
                1,
                "Map",
                Type::Map(MapType::new(
                    NestedField::map_key_element(2, Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::map_value_element(
                        3,
                        Type::Primitive(PrimitiveType::Boolean),
                        true,
                    )
                    .into(),
                )),
            )
            .into()])
            .build()
            .is_err());

        // field in list
        assert!(Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![2])
            .with_fields(vec![NestedField::required(
                1,
                "List",
                Type::List(ListType::new(
                    NestedField::list_element(2, Type::Primitive(PrimitiveType::String), true)
                        .into(),
                )),
            )
            .into()])
            .build()
            .is_err());

        // field in optional struct
        assert!(Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![2])
            .with_fields(vec![NestedField::optional(
                1,
                "Struct",
                Type::Struct(StructType::new(vec![
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(3, "age", Type::Primitive(PrimitiveType::Int)).into(),
                ])),
            )
            .into()])
            .build()
            .is_err());

        // float and double
        assert!(Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![1])
            .with_fields(vec![NestedField::required(
                1,
                "Float",
                Type::Primitive(PrimitiveType::Float),
            )
            .into()])
            .build()
            .is_err());
        assert!(Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![1])
            .with_fields(vec![NestedField::required(
                1,
                "Double",
                Type::Primitive(PrimitiveType::Double),
            )
            .into()])
            .build()
            .is_err());

        // optional field
        assert!(Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![1])
            .with_fields(vec![NestedField::required(
                1,
                "Required",
                Type::Primitive(PrimitiveType::String),
            )
            .into()])
            .build()
            .is_ok());
        assert!(Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![1])
            .with_fields(vec![NestedField::optional(
                1,
                "Optional",
                Type::Primitive(PrimitiveType::String),
            )
            .into()])
            .build()
            .is_err());
    }
}
