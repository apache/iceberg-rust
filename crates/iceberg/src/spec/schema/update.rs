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

use std::collections::HashMap;
use std::sync::Arc;

use typed_builder::TypedBuilder;

use crate::spec::schema::index::index_parents;
use crate::spec::{
    ListType, Literal, MapType, NestedField, NestedFieldRef, PrimitiveType, Schema, SchemaRef,
    SchemaVisitor, StructType, Type, visit_schema,
};
use crate::{Error, ErrorKind, Result, ensure_data_valid};

const TABLE_ROOT_ID: i32 = -1;

/// Operations that can be applied to a schema to produce a new schema. These are used in `UpdateSchemaAction` and are not intended to be used directly by end users. Instead, end users should use `UpdateSchema` which will be converted into a list of `SchemaOperation`s.
pub enum SchemaOperation {
    /// Add a column to the schema
    Add(AddColumn),
    /// Update a column's type, doc, or default value
    Update(UpdateColumn),
    /// Rename a column
    Rename(RenameColumn),
    /// Delete a column
    Delete(DeleteColumn),
    /// Move a column
    Move(MoveColumn),
}

/// A column to be added to the schema.
#[derive(TypedBuilder)]
pub struct AddColumn {
    #[builder(default, setter(strip_option))]
    parent: Option<String>,
    #[builder(setter(into))]
    name: String,
    #[builder(default = true)]
    is_optional: bool,
    r#type: Type,
    #[builder(default, setter(strip_option))]
    doc: Option<Option<String>>,
    #[builder(default, setter(strip_option))]
    default_value: Option<Literal>,
}

/// A column to be deleted from the schema.
pub struct DeleteColumn {
    name: String,
}

impl DeleteColumn {
    /// Create a new `DeleteColumn` with the given column name.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

/// A column to be renamed in the schema.
#[derive(TypedBuilder)]
pub struct RenameColumn {
    #[builder(setter(into))]
    name: String,
    #[builder(setter(into))]
    new_name: String,
}

/// A column to be updated in the schema.
#[derive(TypedBuilder)]
pub struct UpdateColumn {
    #[builder(setter(into))]
    name: String,
    #[builder(default, setter(strip_option))]
    new_type: Option<Type>,
    #[builder(default, setter(strip_option))]
    new_doc: Option<Option<String>>,
    #[builder(default, setter(strip_option))]
    new_default_value: Option<Option<Literal>>,
}

/// A column to be moved in the schema.
pub struct MoveColumn {
    name: String,
    reference_name: String,
    move_type: MoveType,
}

impl MoveColumn {
    /// Move the column to the first position.
    pub fn first(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            reference_name: String::new(),
            move_type: MoveType::First,
        }
    }

    /// Move the column before the reference column.
    pub fn before(name: impl Into<String>, reference: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            reference_name: reference.into(),
            move_type: MoveType::Before,
        }
    }

    /// Move the column after the reference column.
    pub fn after(name: impl Into<String>, reference: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            reference_name: reference.into(),
            move_type: MoveType::After,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MoveType {
    First,
    Before,
    After,
}

#[derive(Clone, Debug)]
struct Move {
    field_id: i32,
    reference_field_id: i32,
    r#type: MoveType,
}

impl Move {
    fn first(field_id: i32) -> Self {
        Move::new(field_id, TABLE_ROOT_ID, MoveType::First)
    }

    fn before(field_id: i32, reference_field_id: i32) -> Self {
        Move::new(field_id, reference_field_id, MoveType::Before)
    }

    fn after(field_id: i32, reference_field_id: i32) -> Self {
        Move::new(field_id, reference_field_id, MoveType::After)
    }

    fn new(field_id: i32, reference_field_id: i32, r#type: MoveType) -> Self {
        Move {
            field_id,
            reference_field_id,
            r#type,
        }
    }

    fn field_id(&self) -> i32 {
        self.field_id
    }

    fn reference_field_id(&self) -> i32 {
        self.reference_field_id
    }

    fn r#type(&self) -> MoveType {
        self.r#type
    }
}

/// Applies a list of `SchemaOperation`s to a `Schema` to produce a new `Schema`. This is used in `UpdateSchemaAction` to apply schema changes as part of a transaction commit. This function validates that the schema operations are valid (e.g. that added columns do not have duplicate names, that deleted columns exist, etc.) and returns an error if any invalid operations are found. If all operations are valid, it returns the updated schema.
pub fn schema_update(schema: SchemaRef, operations: &[SchemaOperation]) -> Result<SchemaRef> {
    let mut updates: HashMap<i32, NestedFieldRef> = HashMap::new();
    let mut deletes = Vec::new();
    let mut moves: HashMap<_, Vec<_>> = HashMap::new();
    let mut parent_to_added_ids: HashMap<_, Vec<_>> = HashMap::new();
    let mut id_to_parent = index_parents(&schema.r#struct).unwrap();
    let mut last_column_id = schema.highest_field_id;
    let mut added_name_to_id = HashMap::new();
    let mut identifier_field_ids = schema.identifier_field_ids.clone();

    for operation in operations {
        match operation {
            SchemaOperation::Add(add) => {
                let (parent, name, is_optional, field_type, doc, default_value) = (
                    &add.parent,
                    &add.name,
                    add.is_optional,
                    &add.r#type,
                    &add.doc,
                    &add.default_value,
                );
                let mut parent_id = TABLE_ROOT_ID;
                let full_name = if let Some(parent) = parent {
                    let parent_field = schema.field_by_name(parent).ok_or(Error::new(
                        ErrorKind::PreconditionFailed,
                        format!("Cannot find parent struct: {}", parent),
                    ))?;
                    let parent_field = if parent_field.field_type.is_nested() {
                        let parent_type = parent_field.field_type.as_ref();
                        match parent_type {
                            Type::List(nested) => nested.element_field.as_ref(), // fields are added to the element type
                            Type::Map(nested) => nested.value_field.as_ref(), // fields are added to the map value type
                            _ => parent_field,
                        }
                    } else {
                        parent_field
                    };
                    ensure_data_valid!(
                        parent_field.field_type.is_struct(),
                        "Cannot add to non-struct column: {}: {}",
                        &parent,
                        parent_field.field_type
                    );
                    parent_id = parent_field.id;
                    let full_name = format!("{}.{}", parent, name);
                    let current_field = schema.field_by_name(&full_name);
                    ensure_data_valid!(
                        !deletes.contains(&parent_id),
                        "Can not add a column that will be deleted: {}",
                        name
                    );
                    ensure_data_valid!(
                        current_field.is_none() || deletes.contains(&current_field.unwrap().id),
                        "Cannot add column, name already exists: {}",
                        &name
                    );
                    full_name
                } else {
                    let current_field = schema.field_by_name(name);
                    ensure_data_valid!(
                        current_field.is_none() || deletes.contains(&current_field.unwrap().id),
                        "Cannot add column, name already exists: {}",
                        &name
                    );
                    name.clone()
                };
                ensure_data_valid!(
                    default_value.is_some() || is_optional,
                    "Incompatible change: cannot add required column without a default value: {}",
                    full_name
                );
                last_column_id += 1;
                let new_id = last_column_id;
                added_name_to_id.insert(full_name, new_id);

                if parent_id != TABLE_ROOT_ID {
                    id_to_parent.insert(new_id, parent_id);
                }
                // TODO: Maybe we can use `ReassignFieldIds`?
                let assigned_type = assign_fresh_ids(field_type.clone(), &mut last_column_id);
                let mut new_field = NestedField::new(new_id, name, assigned_type, !is_optional);
                if let Some(doc) = doc {
                    new_field.doc = doc.clone();
                }
                new_field.write_default = default_value.clone();
                new_field.initial_default = default_value.clone();
                updates.insert(new_id, new_field.into());
                parent_to_added_ids
                    .entry(parent_id)
                    .or_default()
                    .push(new_id);
            }
            SchemaOperation::Delete(delete) => {
                let field = schema.field_by_name(&delete.name).ok_or_else(|| {
                    Error::new(
                        ErrorKind::PreconditionFailed,
                        format!("Cannot delete missing column: {}", delete.name),
                    )
                })?;
                ensure_data_valid!(
                    !parent_to_added_ids.contains_key(&field.id),
                    "Cannot delete a column that has updates: {}",
                    delete.name
                );
                ensure_data_valid!(
                    !updates.contains_key(&field.id),
                    "Cannot delete a column that has updates: {}",
                    delete.name
                );
                deletes.push(field.id);
            }
            SchemaOperation::Rename(rename) => {
                let (name, new_name) = (&rename.name, &rename.new_name);
                let field = schema.field_by_name(name).ok_or(Error::new(
                    ErrorKind::PreconditionFailed,
                    format!("Cannot rename missing column: {}", name),
                ))?;
                ensure_data_valid!(
                    !deletes.contains(&field.id),
                    "Cannot rename a column that will be deleted: {}",
                    name
                );
                // merge with an update, if present
                let field_id = field.id;
                let update = updates.get(&field_id);
                let new_field = if let Some(update) = update {
                    Arc::unwrap_or_clone(update.clone()).with_name(new_name)
                } else {
                    Arc::unwrap_or_clone(field.clone()).with_name(new_name)
                };
                updates.insert(field_id, Arc::new(new_field));
                if identifier_field_ids.contains(&field_id) {
                    identifier_field_ids.remove(&field_id);
                    identifier_field_ids.insert(field_id);
                }
            }
            SchemaOperation::Update(update) => {
                let (name, new_type, new_doc, new_default_value) = (
                    &update.name,
                    &update.new_type,
                    &update.new_doc,
                    &update.new_default_value,
                );
                let field = find_for_update(name, schema.clone(), &updates, &added_name_to_id)?
                    .ok_or(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Cannot update missing column: {}", name),
                    ))?;
                ensure_data_valid!(
                    !deletes.contains(&field.id),
                    "Cannot update column that will be deleted: {}",
                    name,
                );
                let mut new_field = Arc::unwrap_or_clone(field.clone());
                if let Some(new_type) = new_type {
                    *new_field.field_type = new_type.clone();
                }
                if let Some(new_doc) = new_doc {
                    new_field.doc = new_doc.clone();
                }
                if let Some(new_default_value) = new_default_value {
                    new_field.write_default = new_default_value.clone();
                }
                updates.insert(field.id, Arc::new(new_field));
            }
            SchemaOperation::Move(r#move) => {
                let (name, reference_name, move_type) =
                    (&r#move.name, &r#move.reference_name, &r#move.move_type);
                let field_id =
                    find_for_move(name, schema.clone(), &added_name_to_id)?.ok_or(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Cannot move missing column: {}", name),
                    ))?;
                let r#move = if move_type == &MoveType::First {
                    Move::first(field_id)
                } else {
                    let reference_field_id = find_for_move(
                        reference_name,
                        schema.clone(),
                        &added_name_to_id,
                    )?
                    .ok_or(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Cannot move relative to missing column: {}", reference_name),
                    ))?;
                    match move_type {
                        MoveType::Before => Move::before(field_id, reference_field_id),
                        MoveType::After => Move::after(field_id, reference_field_id),
                        _ => unreachable!(),
                    }
                };
                let parent_id = id_to_parent.get(&field_id);
                if let Some(&parent_id) = parent_id {
                    let parent = schema.field_by_id(parent_id).unwrap();
                    ensure_data_valid!(
                        parent.field_type.is_struct(),
                        "Cannot move field in non-struct type: {}",
                        parent
                    );
                    if r#move.r#type == MoveType::After || r#move.r#type == MoveType::Before {
                        ensure_data_valid!(
                            parent_id == *id_to_parent.get(&r#move.reference_field_id).unwrap(),
                            "Cannot move field {} to a different struct",
                            name,
                        );
                    }
                    moves.entry(parent_id).or_default().push(r#move);
                } else {
                    if move_type == &MoveType::After || move_type == &MoveType::Before {
                        ensure_data_valid!(
                            !id_to_parent.contains_key(&r#move.reference_field_id),
                            "Cannot move field {} to a different struct",
                            name,
                        );
                    }
                    moves.entry(TABLE_ROOT_ID).or_default().push(r#move);
                }
            }
        }
    }
    // apply schema changes
    let mut visitor = ApplyChangesVisitor {
        deletes,
        updates,
        parent_to_added_ids,
        moves,
    };
    let struct_type = visit_schema(schema.as_ref(), &mut visitor)?
        .unwrap()
        .to_struct_type()
        .unwrap();
    // validate identifier requirements based on the latest schema
    Ok(Schema::builder()
        .with_fields(struct_type.fields().to_vec())
        .with_identifier_field_ids(identifier_field_ids)
        .build()?
        .into())
}

fn find_for_update(
    name: &str,
    schema: SchemaRef,
    updates: &HashMap<i32, NestedFieldRef>,
    added_name_to_id: &HashMap<String, i32>,
) -> Result<Option<NestedFieldRef>> {
    let field = schema.field_by_name(name);
    if let Some(field) = field {
        let pending_update = updates.get(&field.id);
        if let Some(pending_update) = pending_update {
            Ok(Some(pending_update.clone()))
        } else {
            Ok(Some(field.clone()))
        }
    } else {
        let added_id = added_name_to_id.get(name);
        if let Some(added_id) = added_id {
            Ok(updates.get(added_id).cloned())
        } else {
            Ok(None)
        }
    }
}

fn find_for_move(
    name: &str,
    schema: SchemaRef,
    added_name_to_id: &HashMap<String, i32>,
) -> Result<Option<i32>> {
    let added_id = added_name_to_id.get(name);
    if let Some(added_id) = added_id {
        return Ok(Some(*added_id));
    }
    let field = schema.field_by_name(name);
    if let Some(field) = field {
        return Ok(Some(field.id));
    }
    Ok(None)
}

fn assign_fresh_ids(field_type: Type, next_id: &mut i32) -> Type {
    match field_type {
        Type::Primitive(_) => field_type,
        Type::Struct(s) => {
            let new_fields = s
                .fields()
                .iter()
                .map(|field| {
                    *next_id += 1;
                    let new_field_id = *next_id;
                    let new_type = assign_fresh_ids((*field.field_type).clone(), next_id);
                    Arc::new(NestedField::new(
                        new_field_id,
                        &field.name,
                        new_type,
                        field.required,
                    ))
                })
                .collect();
            Type::Struct(StructType::new(new_fields))
        }
        Type::List(list) => {
            *next_id += 1;
            let element_id = *next_id;
            let element_type = assign_fresh_ids((*list.element_field.field_type).clone(), next_id);
            Type::List(ListType::new(Arc::new(NestedField::new(
                element_id,
                &list.element_field.name,
                element_type,
                list.element_field.required,
            ))))
        }
        Type::Map(map) => {
            *next_id += 1;
            let key_id = *next_id;
            *next_id += 1;
            let value_id = *next_id;
            let key_type = assign_fresh_ids((*map.key_field.field_type).clone(), next_id);
            let value_type = assign_fresh_ids((*map.value_field.field_type).clone(), next_id);
            Type::Map(MapType::new(
                Arc::new(NestedField::new(
                    key_id,
                    &map.key_field.name,
                    key_type,
                    true,
                )),
                Arc::new(NestedField::new(
                    value_id,
                    &map.value_field.name,
                    value_type,
                    map.value_field.required,
                )),
            ))
        }
    }
}

struct ApplyChangesVisitor {
    deletes: Vec<i32>,
    updates: HashMap<i32, NestedFieldRef>,
    parent_to_added_ids: HashMap<i32, Vec<i32>>,
    moves: HashMap<i32, Vec<Move>>,
}

impl SchemaVisitor for ApplyChangesVisitor {
    type T = Option<Type>;

    fn schema(&mut self, _schema: &Schema, value: Self::T) -> Result<Self::T> {
        let added_fields: Vec<NestedFieldRef> = self
            .parent_to_added_ids
            .get(&TABLE_ROOT_ID)
            .unwrap_or(&vec![])
            .iter()
            .map(|id| self.updates.get(id).unwrap().clone())
            .collect();
        let fields = add_and_move_fields(
            value.clone().unwrap().to_struct_type().unwrap().fields(),
            &added_fields,
            self.moves.get(&TABLE_ROOT_ID).unwrap_or(&vec![]),
        );
        if !fields.is_empty() {
            return Ok(Some(Type::Struct(StructType::new(fields))));
        }
        Ok(value)
    }

    fn r#struct(&mut self, r#struct: &StructType, results: Vec<Self::T>) -> Result<Self::T> {
        let mut has_change = false;
        let mut new_fields: Vec<NestedFieldRef> = Vec::with_capacity(results.len());
        for (result_type, field) in results.iter().zip(r#struct.fields()) {
            if result_type.is_none() {
                has_change = true;
                continue;
            }
            let result_type = result_type.clone().unwrap();
            let update = self.updates.get(&field.id);
            let updated = if let Some(update) = update {
                Arc::unwrap_or_clone(update.clone()).of_type(Box::new(result_type))
            } else {
                Arc::unwrap_or_clone(field.clone()).of_type(Box::new(result_type))
            };
            if field.as_ref() == &updated {
                new_fields.push(field.clone());
            } else {
                has_change = true;
                new_fields.push(updated.into());
            }
        }
        if has_change {
            return Ok(Some(Type::Struct(StructType::new(new_fields))));
        }
        Ok(Some(Type::Struct(r#struct.clone())))
    }

    fn field(&mut self, field: &NestedFieldRef, value: Self::T) -> Result<Self::T> {
        let field_id = field.id;
        // handle deletes
        if self.deletes.contains(&field_id) {
            return Ok(None);
        }
        // handle updates
        let update = self.updates.get(&field_id);
        if let Some(update) = update
            && update.field_type.as_ref() != field.field_type.as_ref()
        {
            return Ok(Some(*update.field_type.clone()));
        }
        // handle adds
        let new_fields: Vec<_> = self
            .parent_to_added_ids
            .get(&field_id)
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|id| self.updates.get(id))
            .cloned()
            .collect();
        let columns_to_move = self.moves.get(&field_id).cloned().unwrap_or(vec![]);
        if !new_fields.is_empty() || !columns_to_move.is_empty() {
            let fields = add_and_move_fields(
                value.clone().unwrap().to_struct_type().unwrap().fields(),
                &new_fields,
                &columns_to_move,
            );
            if !fields.is_empty() {
                return Ok(Some(Type::Struct(StructType::new(fields))));
            }
        }
        Ok(value)
    }

    fn list(&mut self, list: &ListType, element_result: Self::T) -> Result<Self::T> {
        let element_field = list.element_field.clone();
        let element_type = self
            .field(&element_field, element_result)?
            .ok_or(Error::new(
                ErrorKind::PreconditionFailed,
                format!("Cannot delete list element type from list: {:?}", list),
            ))?;
        let element_update = self.updates.get(&element_field.id);
        let is_element_optional = if let Some(element_update) = element_update {
            !element_update.required
        } else {
            !element_field.required
        };
        let is_element_required = !is_element_optional;
        if is_element_required == element_field.required
            && &element_type == list.element_field.field_type.as_ref()
        {
            return Ok(Some(Type::List(list.clone())));
        }
        if is_element_optional {
            Ok(Some(Type::List(ListType::optional(
                list.element_field.id,
                element_type,
            ))))
        } else {
            Ok(Some(Type::List(ListType::required(
                list.element_field.id,
                element_type,
            ))))
        }
    }

    fn map(
        &mut self,
        map: &MapType,
        key_result: Self::T,
        value_result: Self::T,
    ) -> Result<Self::T> {
        let key_id = map.key_field.id;
        if self.deletes.contains(&key_id) {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!("Cannot delete map keys: {:?}", map),
            ));
        } else if self.updates.contains_key(&key_id) {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!("Cannot update map keys: {:?}", map),
            ));
        } else if self.parent_to_added_ids.contains_key(&key_id) {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!("Cannot add fields to map keys: {:?}", map),
            ));
        } else if map.key_field.field_type.as_ref() != &key_result.unwrap() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!("Cannot alter map keys: {:?}", map),
            ));
        }
        let value_field = map.value_field.clone();
        let value_type = self.field(&value_field, value_result)?.ok_or(Error::new(
            ErrorKind::PreconditionFailed,
            format!("Cannot delete value type from map: {:?}", map),
        ))?;
        let value_update = self.updates.get(&value_field.id);
        let is_value_required = if let Some(update) = value_update {
            update.required
        } else {
            map.value_field.required
        };
        if is_value_required == map.value_field.required
            && map.value_field.field_type.as_ref() == &value_type
        {
            return Ok(Some(Type::Map(map.clone())));
        }
        if is_value_required {
            Ok(Some(Type::Map(MapType::required(
                map.key_field.id,
                *map.key_field.field_type.clone(),
                map.value_field.id,
                value_type,
            ))))
        } else {
            Ok(Some(Type::Map(MapType::optional(
                map.key_field.id,
                *map.key_field.field_type.clone(),
                map.value_field.id,
                value_type,
            ))))
        }
    }

    fn primitive(&mut self, p: &PrimitiveType) -> Result<Self::T> {
        Ok(Some(Type::Primitive(p.clone())))
    }
}

fn add_and_move_fields(
    fields: &[NestedFieldRef],
    adds: &[NestedFieldRef],
    moves: &[Move],
) -> Vec<NestedFieldRef> {
    if !adds.is_empty() {
        if !moves.is_empty() {
            return move_fields(&add_fields(fields, adds), moves);
        }
        return add_fields(fields, adds);
    } else if !moves.is_empty() {
        return move_fields(fields, moves);
    }
    vec![]
}

fn add_fields(fields: &[NestedFieldRef], adds: &[NestedFieldRef]) -> Vec<NestedFieldRef> {
    let mut new_fields = fields.to_owned();
    new_fields.extend(adds.iter().cloned());
    new_fields
}

fn move_fields(fields: &[NestedFieldRef], moves: &[Move]) -> Vec<NestedFieldRef> {
    let mut reordered = fields.to_vec();
    for r#move in moves {
        let idx = reordered
            .iter()
            .position(|f| f.id == r#move.field_id())
            .unwrap();
        let to_move = reordered.remove(idx);
        match r#move.r#type() {
            MoveType::First => {
                reordered.insert(0, to_move);
            }
            MoveType::Before => {
                let before_idx = reordered
                    .iter()
                    .position(|f| f.id == r#move.reference_field_id())
                    .unwrap();
                reordered.insert(before_idx, to_move);
            }
            MoveType::After => {
                let after_idx = reordered
                    .iter()
                    .position(|f| f.id == r#move.reference_field_id())
                    .unwrap();
                reordered.insert(after_idx + 1, to_move);
            }
        }
    }
    reordered
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, LazyLock};

    use crate::spec::{
        AddColumn, ListType, Literal, MapType, NestedField, PrimitiveType, RenameColumn, Schema,
        SchemaOperation, StructType, Type, UpdateColumn, schema_update,
    };

    static SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Int.into()).into(),
                NestedField::optional(2, "data", PrimitiveType::String.into()).into(),
                NestedField::optional(
                    3,
                    "preferences",
                    StructType::new(vec![
                        NestedField::required(8, "feature1", PrimitiveType::Boolean.into()).into(),
                        NestedField::optional(9, "feature2", PrimitiveType::Boolean.into()).into(),
                    ])
                    .into(),
                )
                .with_doc("struct of named boolean options")
                .into(),
                NestedField::required(
                    4,
                    "locations",
                    MapType::required(
                        10,
                        StructType::new(vec![
                            NestedField::required(20, "address", PrimitiveType::String.into())
                                .into(),
                            NestedField::required(21, "city", PrimitiveType::String.into()).into(),
                            NestedField::required(22, "state", PrimitiveType::String.into()).into(),
                            NestedField::required(23, "zip", PrimitiveType::Int.into()).into(),
                        ])
                        .into(),
                        11,
                        StructType::new(vec![
                            NestedField::required(12, "lat", PrimitiveType::Float.into()).into(),
                            NestedField::required(13, "long", PrimitiveType::Float.into()).into(),
                        ])
                        .into(),
                    )
                    .into(),
                )
                .with_doc("map of address to coordinate")
                .into(),
                NestedField::optional(
                    5,
                    "points",
                    ListType::new(
                        NestedField::list_optional_element(
                            14,
                            StructType::new(vec![
                                NestedField::required(15, "x", PrimitiveType::Long.into()).into(),
                                NestedField::required(16, "y", PrimitiveType::Long.into()).into(),
                            ])
                            .into(),
                        )
                        .into(),
                    )
                    .into(),
                )
                .with_doc("2-D cartesian points")
                .into(),
                NestedField::required(
                    6,
                    "doubles",
                    ListType::new(
                        NestedField::list_required_element(17, PrimitiveType::Double.into()).into(),
                    )
                    .into(),
                )
                .into(),
                NestedField::optional(
                    7,
                    "properties",
                    MapType::optional(
                        18,
                        PrimitiveType::String.into(),
                        19,
                        PrimitiveType::String.into(),
                    )
                    .into(),
                )
                .with_doc("string map of properties")
                .into(),
            ])
            .build()
            .unwrap()
    });

    #[test]
    fn no_changes() {
        let base = SCHEMA.clone();
        let expected = SCHEMA.clone();
        let updated = schema_update(Arc::new(base), &[]).unwrap();
        assert_eq!(updated.as_ref(), &expected);
    }

    #[test]
    #[ignore = "not yet implemented"]
    fn delete_fields() {}

    #[test]
    #[ignore = "not yet implemented"]
    fn delete_fields_case_sensitive_disabled() {
        todo!()
    }

    #[test]
    fn update_types() {
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                NestedField::optional(2, "data", PrimitiveType::String.into()).into(),
                NestedField::optional(
                    3,
                    "preferences",
                    StructType::new(vec![
                        NestedField::required(8, "feature1", PrimitiveType::Boolean.into()).into(),
                        NestedField::optional(9, "feature2", PrimitiveType::Boolean.into()).into(),
                    ])
                    .into(),
                )
                .with_doc("struct of named boolean options")
                .into(),
                NestedField::required(
                    4,
                    "locations",
                    MapType::required(
                        10,
                        StructType::new(vec![
                            NestedField::required(20, "address", PrimitiveType::String.into())
                                .into(),
                            NestedField::required(21, "city", PrimitiveType::String.into()).into(),
                            NestedField::required(22, "state", PrimitiveType::String.into()).into(),
                            NestedField::required(23, "zip", PrimitiveType::Int.into()).into(),
                        ])
                        .into(),
                        11,
                        StructType::new(vec![
                            NestedField::required(12, "lat", PrimitiveType::Double.into()).into(),
                            NestedField::required(13, "long", PrimitiveType::Double.into()).into(),
                        ])
                        .into(),
                    )
                    .into(),
                )
                .with_doc("map of address to coordinate")
                .into(),
                NestedField::optional(
                    5,
                    "points",
                    ListType::new(
                        NestedField::list_optional_element(
                            14,
                            StructType::new(vec![
                                NestedField::required(15, "x", PrimitiveType::Long.into()).into(),
                                NestedField::required(16, "y", PrimitiveType::Long.into()).into(),
                            ])
                            .into(),
                        )
                        .into(),
                    )
                    .into(),
                )
                .with_doc("2-D cartesian points")
                .into(),
                NestedField::required(
                    6,
                    "doubles",
                    ListType::new(
                        NestedField::list_required_element(17, PrimitiveType::Double.into()).into(),
                    )
                    .into(),
                )
                .into(),
                NestedField::optional(
                    7,
                    "properties",
                    MapType::optional(
                        18,
                        PrimitiveType::String.into(),
                        19,
                        PrimitiveType::String.into(),
                    )
                    .into(),
                )
                .with_doc("string map of properties")
                .into(),
            ])
            .build()
            .unwrap();
        let updated = schema_update(Arc::new(SCHEMA.clone()), &[
            SchemaOperation::Update(
                UpdateColumn::builder()
                    .name("id")
                    .new_type(PrimitiveType::Long.into())
                    .build(),
            ),
            SchemaOperation::Update(
                UpdateColumn::builder()
                    .name("locations.lat")
                    .new_type(PrimitiveType::Double.into())
                    .build(),
            ),
            SchemaOperation::Update(
                UpdateColumn::builder()
                    .name("locations.long")
                    .new_type(PrimitiveType::Double.into())
                    .build(),
            ),
        ])
        .unwrap();
        assert_eq!(&expected, updated.as_ref());
    }

    #[test]
    #[ignore = "not yet implemented"]
    fn update_type_preserves_other_metadata() {
        todo!()
    }

    #[test]
    #[ignore = "not yet implemented"]
    fn update_doc_preserves_other_metadata() {
        todo!()
    }

    #[test]
    #[ignore = "not yet implemented"]
    fn update_default_preserves_other_metadata() {
        todo!()
    }

    #[test]
    #[ignore = "not yet implemented"]
    fn update_types_case_insensitive() {
        todo!()
    }

    #[test]
    #[ignore = "not yet implemented"]
    fn update_failure() {
        todo!()
    }

    #[test]
    fn rename() {
        let renamed = schema_update(Arc::new(SCHEMA.clone()), &[
            SchemaOperation::Rename(
                RenameColumn::builder()
                    .name("data")
                    .new_name("json")
                    .build(),
            ),
            SchemaOperation::Rename(
                RenameColumn::builder()
                    .name("preferences")
                    .new_name("options")
                    .build(),
            ),
            SchemaOperation::Rename(
                RenameColumn::builder()
                    .name("preferences.feature2")
                    .new_name("newfeature")
                    .build(),
            ),
            SchemaOperation::Rename(
                RenameColumn::builder()
                    .name("locations.lat")
                    .new_name("latitude")
                    .build(),
            ),
            SchemaOperation::Rename(
                RenameColumn::builder()
                    .name("points.x")
                    .new_name("X")
                    .build(),
            ),
            SchemaOperation::Rename(
                RenameColumn::builder()
                    .name("points.y")
                    .new_name("Y")
                    .build(),
            ),
        ]);
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Int.into()).into(),
                NestedField::optional(2, "json", PrimitiveType::String.into()).into(),
                NestedField::optional(
                    3,
                    "options",
                    StructType::new(vec![
                        NestedField::required(8, "feature1", PrimitiveType::Boolean.into()).into(),
                        NestedField::optional(9, "newfeature", PrimitiveType::Boolean.into())
                            .into(),
                    ])
                    .into(),
                )
                .with_doc("struct of named boolean options")
                .into(),
                NestedField::required(
                    4,
                    "locations",
                    MapType::new(
                        NestedField::map_key_element(
                            10,
                            StructType::new(vec![
                                NestedField::required(20, "address", PrimitiveType::String.into())
                                    .into(),
                                NestedField::required(21, "city", PrimitiveType::String.into())
                                    .into(),
                                NestedField::required(22, "state", PrimitiveType::String.into())
                                    .into(),
                                NestedField::required(23, "zip", PrimitiveType::Int.into()).into(),
                            ])
                            .into(),
                        )
                        .into(),
                        NestedField::map_value_element(
                            11,
                            StructType::new(vec![
                                NestedField::required(12, "latitude", PrimitiveType::Float.into())
                                    .into(),
                                NestedField::required(13, "long", PrimitiveType::Float.into())
                                    .into(),
                            ])
                            .into(),
                            true,
                        )
                        .into(),
                    )
                    .into(),
                )
                .with_doc("map of address to coordinate")
                .into(),
                NestedField::optional(
                    5,
                    "points",
                    ListType::new(
                        NestedField::list_element(
                            14,
                            StructType::new(vec![
                                NestedField::required(15, "X", PrimitiveType::Long.into()).into(),
                                NestedField::required(16, "Y", PrimitiveType::Long.into()).into(),
                            ])
                            .into(),
                            false,
                        )
                        .into(),
                    )
                    .into(),
                )
                .with_doc("2-D cartesian points")
                .into(),
                NestedField::required(
                    6,
                    "doubles",
                    ListType::new(
                        NestedField::required(17, "element", PrimitiveType::Double.into()).into(),
                    )
                    .into(),
                )
                .into(),
                NestedField::optional(
                    7,
                    "properties",
                    MapType::optional(
                        18,
                        PrimitiveType::String.into(),
                        19,
                        PrimitiveType::String.into(),
                    )
                    .into(),
                )
                .with_doc("string map of properties")
                .into(),
            ])
            .build()
            .unwrap();
        assert_eq!(renamed.unwrap().as_ref(), &expected);
    }

    #[test]
    #[ignore = "not yet implemented"]
    fn rename_case_insensitive() {}

    #[test]
    fn add_fields() {
        let added = schema_update(Arc::new(SCHEMA.clone()), &[
            SchemaOperation::Add(
                AddColumn::builder()
                    .name("topLevel")
                    .r#type(Type::Primitive(PrimitiveType::Decimal {
                        precision: 9,
                        scale: 2,
                    }))
                    .build(),
            ),
            SchemaOperation::Add(
                AddColumn::builder()
                    .parent("locations".to_string())
                    .name("alt")
                    .r#type(Type::Primitive(PrimitiveType::Float))
                    .build(),
            ),
            SchemaOperation::Add(
                AddColumn::builder()
                    .parent("points".to_string())
                    .name("z")
                    .r#type(Type::Primitive(PrimitiveType::Long))
                    .build(),
            ),
            SchemaOperation::Add(
                AddColumn::builder()
                    .parent("points".to_string())
                    .name("t.t")
                    .r#type(Type::Primitive(PrimitiveType::Long))
                    .build(),
            ),
        ])
        .unwrap();

        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Int.into()).into(),
                NestedField::optional(2, "data", PrimitiveType::String.into()).into(),
                NestedField::optional(
                    3,
                    "preferences",
                    StructType::new(vec![
                        NestedField::required(8, "feature1", PrimitiveType::Boolean.into()).into(),
                        NestedField::optional(9, "feature2", PrimitiveType::Boolean.into()).into(),
                    ])
                    .into(),
                )
                .with_doc("struct of named boolean options")
                .into(),
                NestedField::required(
                    4,
                    "locations",
                    MapType::new(
                        NestedField::map_key_element(
                            10,
                            StructType::new(vec![
                                NestedField::required(20, "address", PrimitiveType::String.into())
                                    .into(),
                                NestedField::required(21, "city", PrimitiveType::String.into())
                                    .into(),
                                NestedField::required(22, "state", PrimitiveType::String.into())
                                    .into(),
                                NestedField::required(23, "zip", PrimitiveType::Int.into()).into(),
                            ])
                            .into(),
                        )
                        .into(),
                        NestedField::map_value_element(
                            11,
                            StructType::new(vec![
                                NestedField::required(12, "lat", PrimitiveType::Float.into())
                                    .into(),
                                NestedField::required(13, "long", PrimitiveType::Float.into())
                                    .into(),
                                NestedField::optional(25, "alt", PrimitiveType::Float.into())
                                    .into(),
                            ])
                            .into(),
                            true,
                        )
                        .into(),
                    )
                    .into(),
                )
                .with_doc("map of address to coordinate")
                .into(),
                NestedField::optional(
                    5,
                    "points",
                    ListType::optional(
                        14,
                        StructType::new(vec![
                            NestedField::required(15, "x", PrimitiveType::Long.into()).into(),
                            NestedField::required(16, "y", PrimitiveType::Long.into()).into(),
                            NestedField::optional(26, "z", PrimitiveType::Long.into()).into(),
                            NestedField::optional(27, "t.t", PrimitiveType::Long.into()).into(),
                        ])
                        .into(),
                    )
                    .into(),
                )
                .with_doc("2-D cartesian points")
                .into(),
                NestedField::required(
                    6,
                    "doubles",
                    ListType::new(
                        NestedField::required(17, "element", PrimitiveType::Double.into()).into(),
                    )
                    .into(),
                )
                .into(),
                NestedField::optional(
                    7,
                    "properties",
                    MapType::optional(
                        18,
                        PrimitiveType::String.into(),
                        19,
                        PrimitiveType::String.into(),
                    )
                    .into(),
                )
                .with_doc("string map of properties")
                .into(),
                NestedField::optional(
                    24,
                    "topLevel",
                    PrimitiveType::Decimal {
                        precision: 9,
                        scale: 2,
                    }
                    .into(),
                )
                .into(),
            ])
            .build()
            .unwrap();

        assert_eq!(added.as_struct(), expected.as_struct());
    }

    #[test]
    fn add_column_with_default() {
        let schema: Arc<Schema> = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "id", PrimitiveType::Int.into()).into(),
                ])
                .build()
                .unwrap(),
        );
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "id", PrimitiveType::Int.into()).into(),
                NestedField::optional(2, "data", PrimitiveType::String.into())
                    .with_doc("description")
                    .with_initial_default(Literal::string("unknown"))
                    .with_write_default(Literal::string("unknown"))
                    .into(),
            ])
            .build()
            .unwrap();
        let result = schema_update(schema.clone(), &[SchemaOperation::Add(
            AddColumn::builder()
                .name("data")
                .r#type(Type::Primitive(PrimitiveType::String))
                .doc(Some("description".into()))
                .default_value(Literal::string("unknown"))
                .build(),
        )])
        .unwrap();
        assert_eq!(&expected, result.as_ref());
    }

    #[test]
    fn add_column_with_update_column_default() {
        let schema: Arc<Schema> = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "id", PrimitiveType::Int.into()).into(),
                ])
                .build()
                .unwrap(),
        );
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "id", PrimitiveType::Int.into()).into(),
                NestedField::optional(2, "data", PrimitiveType::String.into())
                    .with_write_default(Literal::string("unknown"))
                    .into(),
            ])
            .build()
            .unwrap();
        let result = schema_update(schema.clone(), &[
            SchemaOperation::Add(
                AddColumn::builder()
                    .name("data")
                    .r#type(PrimitiveType::String.into())
                    .build(),
            ),
            SchemaOperation::Update(
                UpdateColumn::builder()
                    .name("data")
                    .new_default_value(Some(Literal::string("unknown")))
                    .build(),
            ),
        ])
        .unwrap();
        assert_eq!(&expected, result.as_ref());
    }

    #[test]
    fn add_nested_struct() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", PrimitiveType::Int.into()).into(),
                ])
                .build()
                .unwrap(),
        );
        let struct_type = StructType::new(vec![
            NestedField::required(1, "lat", PrimitiveType::Int.into()).into(),
            NestedField::optional(2, "long", PrimitiveType::Int.into()).into(),
        ]);
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Int.into()).into(),
                NestedField::optional(
                    2,
                    "location",
                    StructType::new(vec![
                        NestedField::required(3, "lat", PrimitiveType::Int.into()).into(),
                        NestedField::optional(4, "long", PrimitiveType::Int.into()).into(),
                    ])
                    .into(),
                )
                .into(),
            ])
            .build()
            .unwrap();

        let result = schema_update(schema.clone(), &[SchemaOperation::Add(
            AddColumn::builder()
                .name("location")
                .r#type(Type::Struct(struct_type))
                .build(),
        )])
        .unwrap();
        assert_eq!(&expected, result.as_ref());
    }

    #[test]
    fn add_nested_map_of_structs() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", PrimitiveType::Int.into()).into(),
                ])
                .build()
                .unwrap(),
        );
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Int.into()).into(),
                NestedField::optional(
                    2,
                    "locations",
                    MapType::optional(
                        3,
                        StructType::new(vec![
                            NestedField::required(5, "address", PrimitiveType::String.into())
                                .into(),
                            NestedField::required(6, "city", PrimitiveType::String.into()).into(),
                            NestedField::required(7, "state", PrimitiveType::String.into()).into(),
                            NestedField::required(8, "zip", PrimitiveType::Int.into()).into(),
                        ])
                        .into(),
                        4,
                        StructType::new(vec![
                            NestedField::required(9, "lat", PrimitiveType::Int.into()).into(),
                            NestedField::optional(10, "long", PrimitiveType::Int.into()).into(),
                        ])
                        .into(),
                    )
                    .into(),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let map = MapType::optional(
            1,
            StructType::new(vec![
                NestedField::required(20, "address", PrimitiveType::String.into()).into(),
                NestedField::required(21, "city", PrimitiveType::String.into()).into(),
                NestedField::required(22, "state", PrimitiveType::String.into()).into(),
                NestedField::required(23, "zip", PrimitiveType::Int.into()).into(),
            ])
            .into(),
            2,
            StructType::new(vec![
                NestedField::required(9, "lat", PrimitiveType::Int.into()).into(),
                NestedField::optional(8, "long", PrimitiveType::Int.into()).into(),
            ])
            .into(),
        );
        let result = schema_update(schema, &[SchemaOperation::Add(
            AddColumn::builder()
                .name("locations")
                .r#type(map.into())
                .build(),
        )])
        .unwrap();
        assert_eq!(&expected, result.as_ref())
    }

    #[test]
    #[ignore = "not yet implemented"]
    fn add_nested_list_of_structs() {
        todo!()
    }

    #[test]
    #[ignore = "not yet implemented"]
    fn add_required_column_without_default() {
        todo!()
    }

    #[test]
    #[ignore = "not yet implemented"]
    fn add_required_column_with_default() {
        todo!()
    }

    #[test]
    #[ignore = "not yet implemented"]
    fn add_required_column_with_update_column_default() {
        todo!()
    }

    #[test]
    #[ignore = "not yet implemented"]
    fn add_required_column_case_insensitive() {
        todo!()
    }

    #[test]
    #[ignore = "not yet implemented"]
    fn add_multiple_required_column_case_insensitive() {
        todo!()
    }

    #[test]
    #[ignore = "not yet implemented"]
    fn make_column_optional() {
        todo!()
    }

    #[test]
    #[ignore = "not yet implemented"]
    fn require_column() {
        todo!()
    }
}
