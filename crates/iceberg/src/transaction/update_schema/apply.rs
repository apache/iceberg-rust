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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::{AddColumn, MovePosition, SchemaOperation};
use crate::spec::{
    ListType, Literal, MapType, NestedField, NestedFieldRef, PrimitiveLiteral, PrimitiveType,
    SCHEMA_NAME_DELIMITER, Schema, StructType, Type,
};
use crate::{Error, ErrorKind, Result};

// ---------------------------------------------------------------------------
// ID assignment helpers
// ---------------------------------------------------------------------------

/// Recursively assign fresh field IDs to a `NestedField` and all its nested sub-fields.
///
/// This follows the same recursive pattern as `ReassignFieldIds::reassign_ids_visit_type`
/// from `crate::spec::schema::id_reassigner`, but operates on new fields with placeholder
/// IDs rather than reassigning an existing schema. `ReassignFieldIds` cannot be used
/// directly here because it rejects duplicate old IDs (all new fields share placeholder
/// ID `DEFAULT_FIELD_ID`).
pub(super) fn assign_fresh_ids(field: &NestedField, next_id: &mut i32) -> NestedFieldRef {
    *next_id += 1;
    let new_id = *next_id;
    let new_type = assign_fresh_ids_to_type(&field.field_type, next_id);

    Arc::new(NestedField {
        id: new_id,
        name: field.name.clone(),
        required: field.required,
        field_type: Box::new(new_type),
        doc: field.doc.clone(),
        initial_default: field.initial_default.clone(),
        write_default: field.write_default.clone(),
    })
}

/// Recursively assign fresh field IDs to all nested fields within a `Type`.
fn assign_fresh_ids_to_type(field_type: &Type, next_id: &mut i32) -> Type {
    match field_type {
        Type::Primitive(_) => field_type.clone(),
        // Variant carries no nested fields, so there is nothing to reassign
        // (matches id_reassigner.rs).
        Type::Variant(v) => Type::Variant(*v),
        Type::Struct(struct_type) => {
            let new_fields: Vec<NestedFieldRef> = struct_type
                .fields()
                .iter()
                .map(|f| assign_fresh_ids(f, next_id))
                .collect();
            Type::Struct(StructType::new(new_fields))
        }
        Type::List(list_type) => {
            let new_element = assign_fresh_ids(&list_type.element_field, next_id);
            Type::List(ListType {
                element_field: new_element,
            })
        }
        Type::Map(map_type) => {
            let new_key = assign_fresh_ids(&map_type.key_field, next_id);
            let new_value = assign_fresh_ids(&map_type.value_field, next_id);
            Type::Map(MapType {
                key_field: new_key,
                value_field: new_value,
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Parent path resolution
// ---------------------------------------------------------------------------

/// Resolve a parent path to the target struct's parent field ID and a reference
/// to its `StructType`.
///
/// If the parent is a map, navigates to the value field. If a list, navigates to
/// the element field. The final target must be a struct type.
fn resolve_parent_target<'a>(
    base_schema: &'a Schema,
    parent: &str,
    case_sensitive: bool,
) -> Result<(i32, &'a StructType)> {
    let parent_field = if case_sensitive {
        base_schema.field_by_name(parent)
    } else {
        base_schema.field_by_name_case_insensitive(parent)
    };

    parent_field
        .ok_or_else(|| {
            Error::new(
                ErrorKind::PreconditionFailed,
                format!("Cannot find parent struct: {parent}"),
            )
        })
        .and_then(|parent_field| match parent_field.field_type.as_ref() {
            Type::Struct(s) => Ok((parent_field.id, s)),
            Type::Map(m) => match m.value_field.field_type.as_ref() {
                Type::Struct(s) => Ok((m.value_field.id, s)),
                _ => Err(Error::new(
                    ErrorKind::PreconditionFailed,
                    format!("Cannot add column: map value of '{parent}' is not a struct"),
                )),
            },
            Type::List(l) => match l.element_field.field_type.as_ref() {
                Type::Struct(s) => Ok((l.element_field.id, s)),
                _ => Err(Error::new(
                    ErrorKind::PreconditionFailed,
                    format!("Cannot add column: list element of '{parent}' is not a struct"),
                )),
            },
            _ => Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!(
                    "Cannot add to column {parent}: {} is not a struct",
                    parent_field.field_type
                ),
            )),
        })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MoveKind {
    First,
    Before,
    After,
}

struct PendingMove {
    field_id: i32,
    reference_field_id: Option<i32>,
    kind: MoveKind,
}

pub(super) struct PendingSchemaUpdate<'a> {
    pub(super) schema: &'a Schema,
    pub(super) updates: HashMap<i32, NestedFieldRef>,
    pub(super) deletes: HashSet<i32>,
    pub(super) additions: HashMap<Option<i32>, Vec<i32>>,
    moves: HashMap<Option<i32>, Vec<PendingMove>>,
    id_to_parent: HashMap<i32, i32>,
    pub(super) added_name_to_id: HashMap<String, i32>,
    last_column_id: i32,
    allow_incompatible_changes: bool,
    case_sensitive: bool,
    identifier_field_names: Option<HashSet<String>>,
}

impl<'a> PendingSchemaUpdate<'a> {
    pub(super) fn new(
        schema: &'a Schema,
        last_column_id: i32,
        allow_incompatible_changes: bool,
        case_sensitive: bool,
        identifier_field_names: Option<HashSet<String>>,
    ) -> Self {
        let mut id_to_parent = HashMap::new();
        index_parent_ids(schema.as_struct().fields(), None, &mut id_to_parent);

        Self {
            schema,
            updates: HashMap::new(),
            deletes: HashSet::new(),
            additions: HashMap::new(),
            moves: HashMap::new(),
            id_to_parent,
            added_name_to_id: HashMap::new(),
            last_column_id,
            allow_incompatible_changes,
            case_sensitive,
            identifier_field_names,
        }
    }

    pub(super) fn apply_operations(&mut self, operations: &[SchemaOperation]) -> Result<()> {
        for operation in operations {
            match operation {
                SchemaOperation::Add(add) => self.add_column(add)?,
                SchemaOperation::Delete(name) => self.delete_column(name)?,
                SchemaOperation::Rename { name, new_name } => self.rename_column(name, new_name)?,
                SchemaOperation::SetRequired { name, required } => {
                    self.set_required(name, *required)?
                }
                SchemaOperation::UpdateType { name, new_type } => {
                    self.update_type(name, new_type)?
                }
                SchemaOperation::UpdateDoc { name, doc } => self.update_doc(name, doc.clone())?,
                SchemaOperation::UpdateDefault { name, default } => {
                    self.update_default(name, default.clone())?
                }
                SchemaOperation::Move { name, position } => self.move_column(name, position)?,
                SchemaOperation::UnionByName(new_schema) => self.union_by_name(new_schema)?,
            }
        }

        Ok(())
    }

    fn add_column(&mut self, add: &AddColumn) -> Result<()> {
        if add.parent.is_none() && add.name.contains(SCHEMA_NAME_DELIMITER) {
            return Err(precondition(format!(
                "Cannot add column with ambiguous name: {}. Set a parent to add a nested column",
                add.name
            )));
        }

        let initial_default =
            convert_default(&add.field_type, add.initial_default.as_ref(), &add.name)?;
        let write_default =
            convert_default(&add.field_type, add.write_default.as_ref(), &add.name)?;

        if add.required && add.initial_default.is_none() && !self.allow_incompatible_changes {
            return Err(precondition(format!(
                "Incompatible change: cannot add required column without an initial default: {}",
                add.name
            )));
        }

        let (parent_id, full_name, sibling_fields) = if let Some(parent) = &add.parent {
            let (target_id, parent_struct) =
                resolve_parent_target(self.schema, parent, self.case_sensitive)?;
            if self.deletes.contains(&target_id) {
                return Err(precondition(format!(
                    "Cannot add to a column that will be deleted: {parent}"
                )));
            }
            let canonical_parent = self
                .schema
                .name_by_field_id(target_id)
                .unwrap_or(parent.as_str());
            (
                Some(target_id),
                format!("{canonical_parent}.{}", add.name),
                parent_struct.fields(),
            )
        } else {
            (None, add.name.clone(), self.schema.as_struct().fields())
        };

        if sibling_fields.iter().any(|field| {
            names_equal(&field.name, &add.name, self.case_sensitive)
                && !self.deletes.contains(&field.id)
        }) || self
            .added_name_to_id
            .contains_key(&self.normalized_name(&full_name))
        {
            return Err(precondition(format!(
                "Cannot add column, name already exists: {full_name}"
            )));
        }

        let mut pending = (*add.to_nested_field()).clone();
        pending.initial_default = initial_default;
        pending.write_default = write_default;
        let field = assign_fresh_ids(&pending, &mut self.last_column_id);
        index_added_field(&field, parent_id, &mut self.id_to_parent);
        self.added_name_to_id
            .insert(self.normalized_name(&full_name), field.id);
        self.additions.entry(parent_id).or_default().push(field.id);
        self.updates.insert(field.id, field);
        Ok(())
    }

    fn delete_column(&mut self, name: &str) -> Result<()> {
        let field = self
            .find_field(name)
            .ok_or_else(|| precondition(format!("Cannot delete missing column: {name}")))?;

        if self.additions.contains_key(&Some(field.id)) {
            return Err(precondition(format!(
                "Cannot delete a column that has additions: {name}"
            )));
        }
        if self.updates.contains_key(&field.id) {
            return Err(precondition(format!(
                "Cannot delete a column that has updates: {name}"
            )));
        }

        self.deletes.insert(field.id);
        Ok(())
    }

    fn rename_column(&mut self, name: &str, new_name: &str) -> Result<()> {
        let field = self
            .find_field(name)
            .ok_or_else(|| precondition(format!("Cannot rename missing column: {name}")))?;
        if self.deletes.contains(&field.id) {
            return Err(precondition(format!(
                "Cannot rename a column that will be deleted: {name}"
            )));
        }

        let current = self
            .updates
            .get(&field.id)
            .cloned()
            .unwrap_or_else(|| field.clone());
        if current.name != new_name {
            let mut updated = (*current).clone();
            updated.name = new_name.to_string();
            self.updates.insert(field.id, Arc::new(updated));
        }
        Ok(())
    }

    fn set_required(&mut self, name: &str, required: bool) -> Result<()> {
        let field = self.field_for_update(name)?;
        if field.required == required {
            return Ok(());
        }
        if self.deletes.contains(&field.id) {
            return Err(precondition(format!(
                "Cannot update a column that will be deleted: {name}"
            )));
        }

        let is_defaulted_add = self.added_name_to_id.values().any(|id| *id == field.id)
            && field.initial_default.is_some();
        if required && !is_defaulted_add && !self.allow_incompatible_changes {
            return Err(precondition(format!(
                "Cannot change column nullability: {name}: optional -> required"
            )));
        }

        let mut updated = (*field).clone();
        updated.required = required;
        self.updates.insert(updated.id, Arc::new(updated));
        Ok(())
    }

    fn update_type(&mut self, name: &str, new_type: &PrimitiveType) -> Result<()> {
        let field = self.field_for_update(name)?;
        if self.deletes.contains(&field.id) {
            return Err(precondition(format!(
                "Cannot update a column that will be deleted: {name}"
            )));
        }
        if field.field_type.as_ref() == &Type::Primitive(new_type.clone()) {
            return Ok(());
        }
        if !is_promotion_allowed(&field.field_type, new_type) {
            return Err(precondition(format!(
                "Cannot change column type: {name}: {} -> {new_type}",
                field.field_type
            )));
        }

        let new_field_type = Type::Primitive(new_type.clone());
        let mut updated = (*field).clone();
        updated.initial_default =
            convert_default(&new_field_type, field.initial_default.as_ref(), name)?;
        updated.write_default =
            convert_default(&new_field_type, field.write_default.as_ref(), name)?;
        updated.field_type = Box::new(new_field_type);
        self.updates.insert(updated.id, Arc::new(updated));
        Ok(())
    }

    fn update_doc(&mut self, name: &str, doc: Option<String>) -> Result<()> {
        let field = self.field_for_update(name)?;
        if self.deletes.contains(&field.id) {
            return Err(precondition(format!(
                "Cannot update a column that will be deleted: {name}"
            )));
        }
        if field.doc == doc {
            return Ok(());
        }

        let mut updated = (*field).clone();
        updated.doc = doc;
        self.updates.insert(updated.id, Arc::new(updated));
        Ok(())
    }

    fn update_default(&mut self, name: &str, default: Option<Literal>) -> Result<()> {
        let field = self.field_for_update(name)?;
        if self.deletes.contains(&field.id) {
            return Err(precondition(format!(
                "Cannot update a column that will be deleted: {name}"
            )));
        }
        let default = convert_default(&field.field_type, default.as_ref(), name)?;
        if field.write_default == default {
            return Ok(());
        }

        let mut updated = (*field).clone();
        updated.write_default = default;
        self.updates.insert(updated.id, Arc::new(updated));
        Ok(())
    }

    fn move_column(&mut self, name: &str, position: &MovePosition) -> Result<()> {
        let field_id = self
            .field_id_for_move(name)
            .ok_or_else(|| precondition(format!("Cannot move missing column: {name}")))?;
        let parent_id = self.id_to_parent.get(&field_id).copied();

        if let Some(parent_id) = parent_id {
            let parent = self.field_by_id(parent_id).ok_or_else(|| {
                precondition(format!("Cannot find parent field for column: {name}"))
            })?;
            if !parent.field_type.is_struct() {
                return Err(precondition(format!(
                    "Cannot move fields in non-struct type: {}",
                    parent.field_type
                )));
            }
        }

        let (kind, reference_field_id) = match position {
            MovePosition::First => (MoveKind::First, None),
            MovePosition::Before(reference) | MovePosition::After(reference) => {
                let reference_id = self.field_id_for_move(reference).ok_or_else(|| {
                    precondition(format!(
                        "Cannot move {name} relative to missing column: {reference}"
                    ))
                })?;
                if reference_id == field_id {
                    return Err(precondition(format!(
                        "Cannot move {name} before or after itself"
                    )));
                }
                if self.id_to_parent.get(&reference_id).copied() != parent_id {
                    return Err(precondition(format!(
                        "Cannot move field {name} to a different struct"
                    )));
                }
                let kind = if matches!(position, MovePosition::Before(_)) {
                    MoveKind::Before
                } else {
                    MoveKind::After
                };
                (kind, Some(reference_id))
            }
        };

        self.moves.entry(parent_id).or_default().push(PendingMove {
            field_id,
            reference_field_id,
            kind,
        });
        Ok(())
    }

    fn union_by_name(&mut self, new_schema: &Schema) -> Result<()> {
        let existing = self.schema.as_struct().clone();
        self.union_struct(None, &existing, new_schema.as_struct())
    }

    fn union_struct(
        &mut self,
        parent_id: Option<i32>,
        existing: &StructType,
        new: &StructType,
    ) -> Result<()> {
        for new_field in new.fields() {
            let existing_field = existing
                .fields()
                .iter()
                .find(|field| names_equal(&field.name, &new_field.name, self.case_sensitive))
                .cloned();
            if let Some(existing_field) = existing_field {
                self.union_field(&existing_field, new_field)?;
            } else {
                let parent = parent_id
                    .and_then(|id| self.schema.name_by_field_id(id))
                    .map(str::to_string);
                let full_name = parent
                    .as_deref()
                    .map(|parent| format!("{parent}.{}", new_field.name))
                    .unwrap_or_else(|| new_field.name.clone());
                self.add_column(&AddColumn {
                    parent,
                    name: new_field.name.clone(),
                    required: false,
                    field_type: (*new_field.field_type).clone(),
                    doc: new_field.doc.clone(),
                    initial_default: new_field.initial_default.clone(),
                    write_default: new_field.initial_default.clone(),
                })?;
                if new_field.write_default != new_field.initial_default {
                    self.update_default(&full_name, new_field.write_default.clone())?;
                }
            }
        }
        Ok(())
    }

    fn union_field(
        &mut self,
        existing_field: &NestedFieldRef,
        new_field: &NestedFieldRef,
    ) -> Result<()> {
        let name = self
            .schema
            .name_by_field_id(existing_field.id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "Field {} is missing from the schema name index",
                        existing_field.id
                    ),
                )
            })?
            .to_string();

        if !new_field.required && existing_field.required {
            self.set_required(&name, false)?;
        }
        if new_field.doc.is_some() && new_field.doc != existing_field.doc {
            self.update_doc(&name, new_field.doc.clone())?;
        }
        if new_field.write_default.is_some()
            && new_field.write_default != existing_field.write_default
        {
            self.update_default(&name, new_field.write_default.clone())?;
        }

        match (
            existing_field.field_type.as_ref(),
            new_field.field_type.as_ref(),
        ) {
            (Type::Primitive(existing), Type::Primitive(new)) => {
                // A narrower incoming type is already represented by the existing wider type.
                if existing != new && !is_promotion_allowed(&Type::Primitive(new.clone()), existing)
                {
                    self.update_type(&name, new)?;
                }
            }
            (Type::Struct(existing), Type::Struct(new)) => {
                self.union_struct(Some(existing_field.id), existing, new)?
            }
            (Type::List(existing), Type::List(new)) => {
                self.union_field(&existing.element_field, &new.element_field)?
            }
            (Type::Map(existing), Type::Map(new)) => {
                self.union_field(&existing.key_field, &new.key_field)?;
                self.union_field(&existing.value_field, &new.value_field)?;
            }
            (existing, new) => {
                return Err(precondition(format!(
                    "Cannot merge column {name}: incompatible types {existing} and {new}"
                )));
            }
        }
        Ok(())
    }

    pub(super) fn apply(&self) -> Result<Schema> {
        let protected_identifier_ids: HashSet<i32> = if self.identifier_field_names.is_some() {
            HashSet::new()
        } else {
            self.schema.identifier_field_ids().collect()
        };
        for identifier_id in protected_identifier_ids {
            let identifier = self.schema.field_by_id(identifier_id).ok_or_else(|| {
                precondition(format!("Identifier field {identifier_id} does not exist"))
            })?;
            if self.deletes.contains(&identifier_id) {
                return Err(precondition(format!(
                    "Cannot delete identifier field {}. To force deletion, also replace the identifier fields",
                    identifier.name
                )));
            }
            let mut parent_id = self.id_to_parent.get(&identifier_id).copied();
            while let Some(id) = parent_id {
                if self.deletes.contains(&id) {
                    return Err(precondition(format!(
                        "Cannot delete field {id} because it contains identifier field {}",
                        identifier.name
                    )));
                }
                parent_id = self.id_to_parent.get(&id).copied();
            }
        }

        let fields = rebuild_fields(
            self.schema.as_struct().fields(),
            &self.updates,
            &self.additions,
            &self.deletes,
            &self.moves,
            None,
        )?;
        let schema_without_identifiers = Schema::builder()
            .with_fields(fields.clone())
            .build()
            .map_err(|err| precondition("Cannot apply schema update").with_source(err))?;

        let identifier_ids = if let Some(names) = &self.identifier_field_names {
            names
                .iter()
                .map(|name| {
                    self.find_in_schema(&schema_without_identifiers, name)
                        .map(|field| field.id)
                        .ok_or_else(|| {
                            precondition(format!(
                                "Cannot add field {name} as an identifier field: not found in updated schema"
                            ))
                        })
                })
                .collect::<Result<HashSet<_>>>()?
        } else {
            self.schema.identifier_field_ids().collect()
        };

        Schema::builder()
            .with_fields(fields)
            .with_identifier_field_ids(identifier_ids)
            .build()
            .map_err(|err| {
                precondition("Invalid identifier fields for updated schema").with_source(err)
            })
    }

    fn find_field(&self, name: &str) -> Option<&NestedFieldRef> {
        self.find_in_schema(self.schema, name)
    }

    fn find_in_schema<'b>(&self, schema: &'b Schema, name: &str) -> Option<&'b NestedFieldRef> {
        if self.case_sensitive {
            schema.field_by_name(name)
        } else {
            schema.field_by_name_case_insensitive(name)
        }
    }

    fn field_for_update(&self, name: &str) -> Result<NestedFieldRef> {
        if let Some(field) = self.find_field(name) {
            return Ok(self
                .updates
                .get(&field.id)
                .cloned()
                .unwrap_or_else(|| field.clone()));
        }
        if let Some(id) = self.added_name_to_id.get(&self.normalized_name(name)) {
            return self.updates.get(id).cloned().ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Added column is missing its pending field",
                )
            });
        }
        Err(precondition(format!(
            "Cannot update missing column: {name}"
        )))
    }

    fn field_id_for_move(&self, name: &str) -> Option<i32> {
        self.added_name_to_id
            .get(&self.normalized_name(name))
            .copied()
            .or_else(|| self.find_field(name).map(|field| field.id))
    }

    fn field_by_id(&self, id: i32) -> Option<&NestedFieldRef> {
        self.updates
            .get(&id)
            .or_else(|| self.schema.field_by_id(id))
    }

    fn normalized_name(&self, name: &str) -> String {
        if self.case_sensitive {
            name.to_string()
        } else {
            name.to_lowercase()
        }
    }
}

fn precondition(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::PreconditionFailed, message)
}

fn names_equal(left: &str, right: &str, case_sensitive: bool) -> bool {
    if case_sensitive {
        left == right
    } else {
        left.to_lowercase() == right.to_lowercase()
    }
}

fn convert_default(
    field_type: &Type,
    default: Option<&Literal>,
    name: &str,
) -> Result<Option<Literal>> {
    let Some(default) = default else {
        return Ok(None);
    };
    let converted = match (field_type, default) {
        (Type::Primitive(field_type), Literal::Primitive(value))
            if field_type.compatible(value) =>
        {
            default.clone()
        }
        (
            Type::Primitive(PrimitiveType::Long),
            Literal::Primitive(PrimitiveLiteral::Int(value)),
        ) => Literal::long(*value),
        (
            Type::Primitive(PrimitiveType::Double),
            Literal::Primitive(PrimitiveLiteral::Float(value)),
        ) => Literal::double(value.0 as f64),
        _ => {
            return Err(precondition(format!(
                "Invalid default for column {name}: default is incompatible with {field_type}"
            )));
        }
    };
    converted
        .clone()
        .try_into_json(field_type)
        .map(|_| Some(converted))
        .map_err(|err| {
            precondition(format!(
                "Invalid default for column {name}: default is incompatible with {field_type}"
            ))
            .with_source(err)
        })
}

fn is_promotion_allowed(from: &Type, to: &PrimitiveType) -> bool {
    let Type::Primitive(from) = from else {
        return false;
    };
    if from == to {
        return true;
    }
    match (from, to) {
        (PrimitiveType::Int, PrimitiveType::Long)
        | (PrimitiveType::Float, PrimitiveType::Double) => true,
        (
            PrimitiveType::Decimal { precision, scale },
            PrimitiveType::Decimal {
                precision: new_precision,
                scale: new_scale,
            },
        ) => scale == new_scale && precision <= new_precision,
        _ => false,
    }
}

fn index_parent_ids(
    fields: &[NestedFieldRef],
    parent_id: Option<i32>,
    result: &mut HashMap<i32, i32>,
) {
    for field in fields {
        if let Some(parent_id) = parent_id {
            result.insert(field.id, parent_id);
        }
        index_type_parent_ids(&field.field_type, field.id, result);
    }
}

fn index_type_parent_ids(field_type: &Type, parent_id: i32, result: &mut HashMap<i32, i32>) {
    match field_type {
        Type::Struct(struct_type) => {
            index_parent_ids(struct_type.fields(), Some(parent_id), result)
        }
        Type::List(list_type) => {
            result.insert(list_type.element_field.id, parent_id);
            index_type_parent_ids(
                &list_type.element_field.field_type,
                list_type.element_field.id,
                result,
            );
        }
        Type::Map(map_type) => {
            result.insert(map_type.key_field.id, parent_id);
            result.insert(map_type.value_field.id, parent_id);
            index_type_parent_ids(
                &map_type.key_field.field_type,
                map_type.key_field.id,
                result,
            );
            index_type_parent_ids(
                &map_type.value_field.field_type,
                map_type.value_field.id,
                result,
            );
        }
        Type::Primitive(_) | Type::Variant(_) => {}
    }
}

fn index_added_field(
    field: &NestedFieldRef,
    parent_id: Option<i32>,
    result: &mut HashMap<i32, i32>,
) {
    if let Some(parent_id) = parent_id {
        result.insert(field.id, parent_id);
    }
    index_type_parent_ids(&field.field_type, field.id, result);
}

// ---------------------------------------------------------------------------
// Schema tree rebuild
// ---------------------------------------------------------------------------

/// Rebuild a struct's fields and then apply additions and moves in call order.
fn rebuild_fields(
    fields: &[NestedFieldRef],
    updates: &HashMap<i32, NestedFieldRef>,
    additions: &HashMap<Option<i32>, Vec<i32>>,
    deletes: &HashSet<i32>,
    moves: &HashMap<Option<i32>, Vec<PendingMove>>,
    parent_id: Option<i32>,
) -> Result<Vec<NestedFieldRef>> {
    let mut rebuilt =
        Vec::with_capacity(fields.len() + additions.get(&parent_id).map_or(0, Vec::len));
    for field in fields {
        if !deletes.contains(&field.id) {
            rebuilt.push(rebuild_field(field, updates, additions, deletes, moves)?);
        }
    }
    if let Some(added_ids) = additions.get(&parent_id) {
        for id in added_ids {
            rebuilt.push(updates.get(id).cloned().ok_or_else(|| {
                Error::new(ErrorKind::Unexpected, "Added column is missing its field")
            })?);
        }
    }
    if let Some(pending_moves) = moves.get(&parent_id) {
        apply_moves(&mut rebuilt, pending_moves)?;
    }
    Ok(rebuilt)
}

/// Recursively rebuild a single field. If the field (or any descendant) is a struct
/// that has pending additions, those additions are appended to the struct's fields.
/// Fields whose IDs appear in `delete_ids` are filtered out at every struct level.
fn rebuild_field(
    field: &NestedFieldRef,
    updates: &HashMap<i32, NestedFieldRef>,
    additions: &HashMap<Option<i32>, Vec<i32>>,
    deletes: &HashSet<i32>,
    moves: &HashMap<Option<i32>, Vec<PendingMove>>,
) -> Result<NestedFieldRef> {
    let pending = updates.get(&field.id).unwrap_or(field);
    match field.field_type.as_ref() {
        Type::Primitive(_) | Type::Variant(_) => Ok(pending.clone()),
        Type::Struct(s) => {
            let new_fields = rebuild_fields(
                s.fields(),
                updates,
                additions,
                deletes,
                moves,
                Some(field.id),
            )?;
            Ok(Arc::new(NestedField {
                id: pending.id,
                name: pending.name.clone(),
                required: pending.required,
                field_type: Box::new(Type::Struct(StructType::new(new_fields))),
                doc: pending.doc.clone(),
                initial_default: pending.initial_default.clone(),
                write_default: pending.write_default.clone(),
            }))
        }
        Type::List(l) => {
            if deletes.contains(&l.element_field.id) {
                return Err(precondition(format!(
                    "Cannot delete element type from list: {}",
                    field.name
                )));
            }
            let new_element = rebuild_field(&l.element_field, updates, additions, deletes, moves)?;
            Ok(Arc::new(NestedField {
                id: pending.id,
                name: pending.name.clone(),
                required: pending.required,
                field_type: Box::new(Type::List(ListType {
                    element_field: new_element,
                })),
                doc: pending.doc.clone(),
                initial_default: pending.initial_default.clone(),
                write_default: pending.write_default.clone(),
            }))
        }
        Type::Map(m) => {
            let key_id = m.key_field.id;
            if deletes.contains(&key_id) {
                return Err(precondition(format!(
                    "Cannot delete map keys: {}",
                    field.name
                )));
            }
            if updates.contains_key(&key_id)
                || additions.contains_key(&Some(key_id))
                || moves.contains_key(&Some(key_id))
            {
                return Err(precondition(format!(
                    "Cannot alter map keys: {}",
                    field.name
                )));
            }
            if deletes.contains(&m.value_field.id) {
                return Err(precondition(format!(
                    "Cannot delete value type from map: {}",
                    field.name
                )));
            }
            let new_value = rebuild_field(&m.value_field, updates, additions, deletes, moves)?;
            Ok(Arc::new(NestedField {
                id: pending.id,
                name: pending.name.clone(),
                required: pending.required,
                field_type: Box::new(Type::Map(MapType {
                    key_field: m.key_field.clone(),
                    value_field: new_value,
                })),
                doc: pending.doc.clone(),
                initial_default: pending.initial_default.clone(),
                write_default: pending.write_default.clone(),
            }))
        }
    }
}

fn apply_moves(fields: &mut Vec<NestedFieldRef>, moves: &[PendingMove]) -> Result<()> {
    for pending_move in moves {
        let from = fields
            .iter()
            .position(|field| field.id == pending_move.field_id)
            .ok_or_else(|| {
                precondition("Cannot move a column that is not in the updated struct")
            })?;
        let field = fields.remove(from);
        let index = match pending_move.kind {
            MoveKind::First => 0,
            MoveKind::Before | MoveKind::After => {
                let reference_id = pending_move.reference_field_id.ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "Relative move is missing its reference",
                    )
                })?;
                let reference = fields
                    .iter()
                    .position(|field| field.id == reference_id)
                    .ok_or_else(|| {
                        precondition(
                            "Cannot move relative to a column that is not in the updated struct",
                        )
                    })?;
                if pending_move.kind == MoveKind::After {
                    reference + 1
                } else {
                    reference
                }
            }
        };
        fields.insert(index, field);
    }
    Ok(())
}
