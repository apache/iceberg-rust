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

//! Update schema in iceberg

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::{
    datatypes, visit_struct, ListType, MapType, NestedField, NestedFieldRef, Schema, SchemaRef,
    SchemaVisitor, StructType, Type,
};
use crate::transaction::Transaction;
use crate::{Error, TableUpdate};

/// intermediate struct for updating schema
pub struct UpdateSchema<'a> {
    adds: HashMap<i32, Vec<NestedField>>,
    deletes: HashSet<i32>,
    updates: HashMap<i32, NestedField>,
    moves: HashMap<i32, Vec<Move>>,
    last_column_id: i32,
    case_sensitive: bool,
    allow_incompatible_changes: bool,
    schema: SchemaRef,
    transaction: Transaction<'a>,
    identifier_field_ids: HashSet<i32>,
    id_to_parent: HashMap<i32, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoveOperation {
    First,
    Before,
    After,
}

#[derive(Debug)]
pub struct Move {
    field_id: i32,
    full_name: String,
    other_field_id: Option<i32>,
    op: MoveOperation,
}

impl<'a> UpdateSchema<'a> {
    pub fn new(
        transaction: Transaction<'a>,
        allow_incompatible_changes: bool,
        case_sensitive: bool,
        schema: Option<SchemaRef>,
    ) -> Self {
        let current_schema =
            schema.unwrap_or_else(|| transaction.get_table().metadata().current_schema().clone());
        let last_column_id = current_schema.highest_field_id() + 1;
        let id_to_parent = current_schema.id_to_parent();
        UpdateSchema {
            adds: HashMap::new(),
            deletes: HashSet::new(),
            updates: HashMap::new(),
            moves: HashMap::new(),
            identifier_field_ids: current_schema
                .identifier_field_ids()
                .collect::<HashSet<i32>>(),
            last_column_id,
            case_sensitive,
            allow_incompatible_changes,
            schema: current_schema,
            transaction,
            id_to_parent,
        }
    }

    fn field_by_name(&self, field_name: &str) -> Result<&NestedFieldRef, Error> {
        let name = if self.case_sensitive {
            self.schema.field_by_name_case_insensitive(field_name)
        } else {
            self.schema.field_by_name(field_name)
        };
        name.ok_or_else(|| {
            Error::new(
                crate::ErrorKind::DataInvalid,
                format!("the name {:?} is invalid", field_name.to_string()),
            )
        })
    }

    fn assign_new_column_id(&mut self) -> i32 {
        let id = self.last_column_id;
        self.last_column_id += 1;
        id
    }

    /// api used to add a column
    pub fn add_column(
        mut self,
        path: Vec<String>,
        field_type: Type,
        doc: &str,
        required: bool,
    ) -> Result<Self, Error> {
        if path.is_empty() {
            return Err(Error::new(
                crate::ErrorKind::DataInvalid,
                "Should have a name for new column",
            ));
        }
        if required && !self.allow_incompatible_changes {
            return Err(Error::new(
                crate::ErrorKind::FeatureUnsupported,
                "Cannot add a required column without allowing incompatible changes.",
            ));
        }

        let name = path.last().unwrap();
        let parent_path = path[..path.len() - 1].join(".");
        let parent_id = if !parent_path.is_empty() {
            let parent_field = self.field_by_name(&parent_path)?;
            if !parent_field.field_type.is_struct() {
                return Err(Error::new(
                    crate::ErrorKind::DataInvalid,
                    "the parent field type is not struct",
                ));
            }
            self.schema.field_id_by_name(&parent_path).unwrap()
        } else {
            0
        };

        let new_id = self.assign_new_column_id();
        let field = NestedField::new(new_id, name, field_type, required).with_doc(doc);

        self.adds.entry(parent_id).or_default().push(field);

        Ok(self)
    }

    /// api used to delete a column
    pub fn delete_column(mut self, path: Vec<String>) -> Result<Self, Error> {
        let full_name = path.join(".");
        let field = self
            .schema
            .field_by_name(&full_name)
            .expect("Field not found.");

        if self.adds.contains_key(&field.id) || self.updates.contains_key(&field.id) {
            return Err(Error::new(
                crate::ErrorKind::DataInvalid,
                "cannot delete the column which is on the add/update list",
            ));
        }
        self.deletes.insert(field.id);
        Ok(self)
    }

    pub fn rename_column(mut self, old_path: Vec<String>, new_name: String) -> Result<Self, Error> {
        let full_old_name = old_path.join(".");

        let field = self.field_by_name(&full_old_name)?.clone();

        if self.deletes.contains(&field.id) {
            return Err(Error::new(
                crate::ErrorKind::DataInvalid,
                format!(
                    "Cannot rename a column that will be deleted: '{}'",
                    full_old_name
                ),
            ));
        }

        let updated_field = self
            .updates
            .entry(field.id)
            .or_insert_with(|| field.as_ref().clone());

        updated_field.name = new_name;
        Ok(self)
    }
    pub fn update_column(
        mut self,
        path: Vec<String>,
        field_type: Option<Type>,
        required: Option<bool>,
        doc: Option<String>,
    ) -> Result<Self, Error> {
        let full_name = path.join(".");
        if field_type.is_none() && required.is_none() && doc.is_none() {
            return Ok(self);
        }

        let field = self.field_by_name(&full_name)?.clone();

        if self.deletes.contains(&field.id) {
            return Err(Error::new(
                crate::ErrorKind::DataInvalid,
                format!(
                    "Cannot update a column that will be deleted: '{}'",
                    full_name
                ),
            ));
        }

        if let Some(ref new_field_type) = field_type {
            if !field.field_type.is_primitive() || !new_field_type.is_primitive() {
                return Err(Error::new(
                    crate::ErrorKind::DataInvalid,
                    format!(
                        "Cannot change column type: {:?} is not a primitive type.",
                        field.field_type
                    ),
                ));
            }

            // TODO: support update with compatible data type
            if !self.allow_incompatible_changes && *field.field_type != *new_field_type {
                return Err(Error::new(
                    crate::ErrorKind::DataInvalid,
                    format!(
                        "Cannot change column type: {}: {:?} -> {:?}",
                        full_name, field.field_type, new_field_type
                    ),
                ));
            }
        }

        if let Some(updated) = self.updates.get_mut(&field.id) {
            updated.field_type =
                Box::new(field_type.unwrap_or_else(|| *updated.field_type.clone()));
            updated.doc = doc.clone().or_else(|| updated.doc.clone());
        } else {
            self.updates.insert(field.id, NestedField {
                id: field.id,
                name: field.name.clone(),
                field_type: Box::new(field_type.unwrap_or_else(|| *field.field_type.clone())),
                doc: doc.clone().or_else(|| field.doc.clone()),
                required: field.required,
                initial_default: field.initial_default.clone(),
                write_default: field.write_default.clone(),
            });
        }

        if let Some(required) = required {
            self.set_column_requirement(path, required)?;
        }

        Ok(self)
    }

    fn set_column_requirement(&mut self, path: Vec<String>, required: bool) -> Result<(), Error> {
        let full_name = path.join(".");
        let field = self.field_by_name(&full_name)?.clone();

        if self.deletes.contains(&field.id) {
            return Err(Error::new(
                crate::ErrorKind::DataInvalid,
                format!(
                    "Cannot update requirement for a column that will be deleted: '{}'",
                    full_name
                ),
            ));
        }

        if let Some(updated) = self.updates.get_mut(&field.id) {
            updated.required = required;
        } else {
            self.updates.insert(field.id, NestedField {
                id: field.id,
                name: field.name.clone(),
                field_type: field.field_type.clone(),
                doc: field.doc.clone(),
                required,
                initial_default: field.initial_default.clone(),
                write_default: field.write_default.clone(),
            });
        }

        Ok(())
    }

    fn find_for_move(&self, name: &str) -> Result<i32, Error> {
        Ok(self
            .schema
            .field_by_name(name)
            .ok_or_else(|| Error::new(crate::ErrorKind::DataInvalid, "the name doesn't exist"))?
            .id)
    }

    /// 核心的字段移动逻辑
    fn move_field(&mut self, mv: Move) -> Result<(), Error> {
        if let Some(parent_name) = self.id_to_parent.get(&mv.field_id) {
            let parent_field = self.field_by_name(parent_name)?.clone();
            if parent_field.field_type.is_struct() {
                if matches!(mv.op, MoveOperation::Before | MoveOperation::After) {
                    if let Some(other_field_id) = mv.other_field_id {
                        if self.id_to_parent.get(&mv.field_id)
                            != self.id_to_parent.get(&other_field_id)
                        {
                            return Err(Error::new(
                                crate::ErrorKind::DataInvalid,
                                format!("Cannot move field {} to a different struct", mv.full_name),
                            ));
                        }
                    } else {
                        return Err(Error::new(
                            crate::ErrorKind::DataInvalid,
                            "Expected other field when performing before/after move".to_string(),
                        ));
                    }
                }

                self.moves.entry(parent_field.id).or_default().push(mv);
            } else {
                return Err(Error::new(
                    crate::ErrorKind::DataInvalid,
                    "Cannot move fields in non-struct type: {:?}",
                ));
            }
        } else {
            // on the top level
            if matches!(mv.op, MoveOperation::Before | MoveOperation::After) {
                if let Some(other_field_id) = mv.other_field_id {
                    if self.id_to_parent.contains_key(&other_field_id) {
                        return Err(Error::new(
                            crate::ErrorKind::DataInvalid,
                            format!("Cannot move field {} to a different struct", mv.full_name),
                        ));
                    }
                } else {
                    return Err(Error::new(
                        crate::ErrorKind::DataInvalid,
                        "Expected other field when performing before/after move",
                    ));
                }
            }

            self.moves.entry(0).or_default().push(mv);
        }
        Ok(())
    }

    /// move to first pos
    pub fn move_first(&mut self, path: &str) -> Result<(), Error> {
        let full_name = path.to_string();

        let field_id = self.find_for_move(&full_name)?;

        self.move_field(Move {
            field_id,
            full_name,
            other_field_id: None,
            op: MoveOperation::First,
        })
    }

    /// move to pos before
    pub fn move_before(&mut self, path: &str, before_path: &str) -> Result<(), Error> {
        let full_name = path.to_string();
        let field_id = self.find_for_move(&full_name)?;

        let before_full_name = before_path.to_string();
        let before_field_id = self.find_for_move(&before_full_name)?;

        if field_id == before_field_id {
            panic!("Cannot move {} before itself", full_name);
        }

        self.move_field(Move {
            field_id,
            full_name,
            other_field_id: Some(before_field_id),
            op: MoveOperation::Before,
        })
    }

    /// move to pos after
    pub fn move_after(&mut self, path: &str, after_path: &str) -> Result<(), Error> {
        let full_name = path.to_string();
        let field_id = self.find_for_move(&full_name)?;

        let after_full_name = after_path.to_string();
        let after_field_id = self.find_for_move(&after_full_name)?;

        if field_id == after_field_id {
            panic!("Cannot move {} after itself", full_name);
        }

        self.move_field(Move {
            field_id,
            full_name,
            other_field_id: Some(after_field_id),
            op: MoveOperation::After,
        })
    }

    pub fn apply(mut self) -> Result<Transaction<'a>, Error> {
        let mut applier = ChangeApplier {
            adds: &self.adds,
            deletes: &self.deletes,
            updates: &self.updates,
            moves: &self.moves,
        };

        let struct_result = visit_struct(self.schema.as_struct(), &mut applier)?
            .ok_or_else(|| Error::new(crate::ErrorKind::DataInvalid, "the name doesn't exist"))?;

        if let Type::Struct(ref new_struct) = struct_result {
            let mut field_ids = HashSet::new();
            for field_id in &self.identifier_field_ids {
                if !new_struct.fields().iter().any(|f| f.id == *field_id) {
                    return Err(Error::new(
                        crate::ErrorKind::DataInvalid,
                        format!(
                            "Cannot find identifier field with ID {}. Update identifier fields first.",
                            field_id
                        ),
                    ));
                }
                field_ids.insert(*field_id);
            }

            let next_schema_id = 1 + self
                .transaction
                .table
                .metadata()
                .schemas
                .iter()
                .map(|schema| schema.0)
                .max()
                .unwrap_or(&0);

            let schema = Schema::builder()
                .with_fields(new_struct.fields().to_owned())
                .with_schema_id(next_schema_id)
                .with_identifier_field_ids(field_ids)
                .build()?;

            self.transaction.append_updates(vec![
                TableUpdate::AddSchema { schema },
                TableUpdate::SetCurrentSchema {
                    schema_id: next_schema_id,
                },
            ])?;
            Ok(self.transaction)
        } else {
            Err(Error::new(
                crate::ErrorKind::DataInvalid,
                "Failed to generate a new schema",
            ))
        }
    }
}

pub struct ChangeApplier<'a> {
    adds: &'a HashMap<i32, Vec<NestedField>>,
    deletes: &'a HashSet<i32>,
    updates: &'a HashMap<i32, NestedField>,
    moves: &'a HashMap<i32, Vec<Move>>,
}

impl<'a> SchemaVisitor for ChangeApplier<'a> {
    type T = Option<Type>;

    fn schema(&mut self, _schema: &Schema, struct_result: Self::T) -> Result<Self::T, Error> {
        let added = self.adds.get(&0);
        let moves = self.moves.get(&0);
        if let Some(Type::Struct(ref struct_type)) = struct_result {
            if added.is_some() || moves.is_some() {
                let new_fields = add_and_move_fields(
                    struct_type.fields(),
                    added.unwrap_or(&vec![]),
                    moves.unwrap_or(&vec![]),
                );
                return Ok(Some(Type::Struct(StructType::new(new_fields))));
            }
        }

        Ok(struct_result)
    }

    fn list(&mut self, list: &ListType, element_result: Self::T) -> Result<Self::T, Error> {
        if let Some(Type::Struct(_)) = element_result {
            let element_field = self
                .field(&list.element_field, element_result)?
                .ok_or_else(|| {
                    Error::new(crate::ErrorKind::DataInvalid, "the name doesn't exist")
                })?;
            Ok(Some(Type::List(ListType::new(Arc::new(NestedField {
                id: list.element_field.id,
                name: list.element_field.name.clone(),
                field_type: Box::new(element_field),
                required: list.element_field.required,
                doc: list.element_field.doc.clone(),
                initial_default: list.element_field.initial_default.clone(),
                write_default: list.element_field.write_default.clone(),
            })))))
        } else {
            Err(Error::new(
                crate::ErrorKind::DataInvalid,
                "Cannot delete element type from list",
            ))
        }
    }

    fn primitive(&mut self, p: &datatypes::PrimitiveType) -> Result<Self::T, Error> {
        Ok(Some(Type::Primitive(p.clone())))
    }

    fn map(
        &mut self,
        map: &MapType,
        key_result: Self::T,
        value_result: Self::T,
    ) -> Result<Self::T, Error> {
        let key_id = map.key_field.id;

        if self.deletes.contains(&key_id) {
            return Err(Error::new(
                crate::ErrorKind::DataInvalid,
                format!("Cannot delete map keys: {:?}", map.key_field),
            ));
        }

        if self.updates.contains_key(&key_id) {
            return Err(Error::new(
                crate::ErrorKind::DataInvalid,
                format!("Cannot update map keys: {:?}", map.key_field),
            ));
        }

        if self.adds.contains_key(&key_id) {
            return Err(Error::new(
                crate::ErrorKind::DataInvalid,
                format!("Cannot add fields to map keys: {:?}", map.key_field),
            ));
        }

        if *map.key_field.field_type != key_result.unwrap() {
            return Err(Error::new(
                crate::ErrorKind::DataInvalid,
                format!("Cannot alter map keys: {:?}", map.key_field),
            ));
        }

        let value_field = self.field(&map.value_field, value_result)?;
        if value_field.is_none() {
            return Err(Error::new(
                crate::ErrorKind::DataInvalid,
                "Cannot delete map".to_string(),
            ));
        }
        let value_field = value_field.unwrap();
        if let Type::Struct(ref struct_type) = value_field {
            if struct_type.fields().is_empty() {
                return Err(Error::new(
                    crate::ErrorKind::DataInvalid,
                    format!("Cannot delete value type from map: {:?}", map.value_field),
                ));
            }
        }

        Ok(Some(Type::Map(MapType {
            key_field: map.key_field.clone(),
            value_field: Arc::new(NestedField {
                id: map.value_field.id,
                name: map.value_field.name.clone(),
                required: map.value_field.required,
                field_type: Box::new(value_field),
                doc: map.value_field.doc.clone(),
                initial_default: map.value_field.initial_default.clone(),
                write_default: map.value_field.write_default.clone(),
            }),
        })))
    }

    fn r#struct(
        &mut self,
        r#struct: &StructType,
        field_results: Vec<Self::T>,
    ) -> Result<Self::T, Error> {
        let mut new_fields = Vec::new();
        let mut has_changes = false;

        for (idx, field_result) in field_results.into_iter().enumerate() {
            if field_result.is_none() {
                has_changes = true;
                continue;
            }
            let field = &r#struct
                .field_by_id((idx + 1) as i32)
                .ok_or_else(|| Error::new(crate::ErrorKind::DataInvalid, "the name doesn't exist"))?
                .clone();

            if field.id == -1 {
                has_changes = true;
                continue;
            }
            if let Some(update) = self.updates.get(&field.id) {
                has_changes = true;
                new_fields.push(Arc::new(update.clone()));
                continue;
            }

            new_fields.push(field.clone());
        }
        if let Some(added_fields) = self.adds.get(&0) {
            has_changes = true;
            for added_field in added_fields {
                new_fields.push(Arc::new(added_field.clone()));
            }
        }
        if has_changes {
            Ok(Some(Type::Struct(StructType::new(new_fields))))
        } else {
            Ok(Some(Type::Struct(r#struct.clone())))
        }
    }

    fn field(&mut self, field: &NestedFieldRef, field_result: Self::T) -> Result<Self::T, Error> {
        if self.deletes.contains(&field.id) {
            return Ok(None);
        }

        if let Some(update) = self.updates.get(&field.id) {
            return Ok(Some(update.field_type.as_ref().clone()));
        }
        if let Some(Type::Struct(ref struct_type)) = field_result {
            let added = self.adds.get(&field.id);
            let moves = self.moves.get(&field.id);

            if added.is_some() || moves.is_some() {
                let new_fields = add_and_move_fields(
                    struct_type.fields(),
                    added.unwrap_or(&vec![]),
                    moves.unwrap_or(&vec![]),
                );
                return Ok(Some(Type::Struct(StructType::new(new_fields))));
            }
        }

        Ok(field_result)
    }
}

fn add_and_move_fields(
    fields: &[NestedFieldRef],
    added: &[NestedField],
    moves: &[Move],
) -> Vec<NestedFieldRef> {
    let mut new_fields: Vec<NestedFieldRef> = fields.to_vec();

    for mv in moves {
        match mv.op {
            MoveOperation::First => {
                if let Some(pos) = new_fields.iter().position(|f| f.id == mv.field_id) {
                    let field = new_fields.remove(pos);
                    new_fields.insert(0, field);
                }
            }
            MoveOperation::Before => {
                if let Some(target_pos) = new_fields.iter().position(|f| f.id == mv.field_id) {
                    if let Some(pos) = new_fields
                        .iter()
                        .position(|f| f.id == mv.other_field_id.unwrap())
                    {
                        let field = new_fields.remove(pos);
                        new_fields.insert(target_pos, field);
                    }
                }
            }
            MoveOperation::After => {
                if let Some(target_pos) = new_fields.iter().position(|f| f.id == mv.field_id) {
                    if let Some(pos) = new_fields
                        .iter()
                        .position(|f| f.id == mv.other_field_id.unwrap())
                    {
                        let field = new_fields.remove(pos);
                        new_fields.insert(target_pos + 1, field);
                    }
                }
            }
        }
    }
    for added_field in added {
        new_fields.push(NestedFieldRef::from(added_field.clone()));
    }

    new_fields
}
