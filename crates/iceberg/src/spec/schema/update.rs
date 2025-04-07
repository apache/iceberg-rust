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

use super::{NestedField, SchemaRef};
use crate::spec::Type;
use crate::transaction::Transaction;
use crate::{Error, ErrorKind};

pub const TABLE_ROOT_ID: i32 = -1;

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoveOperation {
    First,
    Before,
    After,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct Move {
    field_id: i32,
    full_name: String,
    other_field_id: Option<i32>,
    op: MoveOperation,
}

#[allow(dead_code)]
pub struct UpdateSchema<'a> {
    transaction: Transaction<'a>,
    schema: SchemaRef,
    adds: HashMap<i32, Vec<NestedField>>,
    deletes: HashSet<i32>,
    updates: HashMap<i32, NestedField>,
    moves: HashMap<i32, Vec<Move>>,
    added_name_to_id: HashMap<String, i32>,
    last_column_id: i32,
    identifier_field_ids: HashSet<i32>,
    case_sensitive: bool,
    allow_incompatible_changes: bool,
}

#[allow(dead_code)]
impl<'a> UpdateSchema<'a> {
    pub fn new(
        transaction: Transaction<'a>,
        allow_incompatible_changes: bool,
        case_sensitive: bool,
        schema: Option<SchemaRef>,
    ) -> Self {
        let current_schema =
            schema.unwrap_or_else(|| transaction.table().metadata().current_schema().clone());
        let last_column_id = current_schema.highest_field_id() + 1;

        UpdateSchema {
            transaction,
            schema: current_schema.clone(),
            adds: HashMap::new(),
            deletes: HashSet::new(),
            updates: HashMap::new(),
            moves: HashMap::new(),
            added_name_to_id: HashMap::new(),
            identifier_field_ids: current_schema
                .identifier_field_ids()
                .collect::<HashSet<i32>>(),
            last_column_id,
            case_sensitive,
            allow_incompatible_changes,
        }
    }

    /// Adds a new column to a nested struct or a new top-level column.
    ///
    /// Because `"."` may be interpreted as a column path separator or used in field names,
    /// it is not allowed to add a nested column by passing in a string. To add to nested
    /// structures or to add fields with names that contain `"."`, use a tuple instead to
    /// indicate the path.
    ///
    /// If the type is a nested type, its field IDs are reassigned when added to the existing
    /// schema.
    ///
    /// # Arguments
    ///
    /// * `path` - Name for the new column.
    /// * `field_type` - Type for the new column.
    /// * `doc` - Documentation string for the new column.
    /// * `required` - Whether the new column is required.
    ///
    /// # Returns
    ///
    /// This method returns a reference to `Self` to allow for method chaining.
    fn add_column(
        &mut self,
        column_name: Vec<String>,
        field_type: Type,
        doc: Option<String>,
        required: bool,
    ) -> Result<&mut Self, Error> {
        if column_name.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot add column without name.",
            ));
        }

        for name in &column_name {
            if name.contains('.') {
                return Err(Error::new(ErrorKind::DataInvalid, format!(
                    "Cannot add column with ambiguous name: {}, provide a vector of names without periods",
                    name
                )));
            }
        }

        if required && !self.allow_incompatible_changes {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Cannot add column because there is no initial value",
            ));
        }

        let name = column_name.last().unwrap();
        let parent = column_name[..column_name.len() - 1].to_vec();

        let full_name = column_name.join(".");
        let parent_full_path = parent.join(".");
        let mut parent_id: i32 = TABLE_ROOT_ID;

        if let Some(existing_field) = if self.case_sensitive {
            self.schema.field_by_name(&full_name)
        } else {
            self.schema.field_by_name_case_insensitive(&full_name)
        } {
            if !self.deletes.contains(&existing_field.id) {
                return Err(Error::new(
                    crate::ErrorKind::DataInvalid,
                    format!("Cannot add column {}, to non-struct type.", name),
                ));
            }
        }

        if parent.is_empty() {
            let parent_field = if self.case_sensitive {
                self.schema.field_by_name(&parent_full_path)
            } else {
                self.schema
                    .field_by_name_case_insensitive(&parent_full_path)
            }
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Parent column name '{}' doesn't exist in the schema",
                        parent_full_path
                    ),
                )
            })?;

            let parent_type = parent_field.field_type.clone();

            let parent_clone = match &*parent_field.field_type {
                Type::Map(map_type) => map_type.value_field.clone(),
                Type::List(list_type) => list_type.element_field.clone(),
                _ => parent_field.clone(),
            };

            if !parent_type.is_struct() {
                return Err(Error::new(
                    crate::ErrorKind::DataInvalid,
                    format!("Cannot add column,{} , to non-struct type.", name),
                ));
            }

            parent_id = parent_clone.id;
        }

        let new_id = self.assign_new_column_id();

        self.added_name_to_id.insert(full_name.clone(), new_id);
        let new_field = NestedField::new(new_id, name, field_type, required).with_doc(doc.unwrap());

        self.adds.entry(parent_id).or_default().push(new_field);

        Ok(self)
    }

    /// Deletes a column from a table.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the column.
    ///
    /// # Returns
    ///
    /// Returns the `UpdateSchema` with the delete operation staged.

    fn delete_column(&mut self, column_name: Vec<String>) -> Result<&mut Self, Error> {
        let full_name = column_name.join(".");

        let field = if self.case_sensitive {
            self.schema.field_by_name(&full_name)
        } else {
            self.schema.field_by_name_case_insensitive(&full_name)
        }
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Delete column name,'{}' , doesn't exist in the schema",
                    full_name
                ),
            )
        })?;

        if self.adds.contains_key(&field.id) || self.updates.contains_key(&field.id) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot delete a column that is to be added or updated",
            ));
        }

        self.deletes.insert(field.id);

        Ok(self)
    }

    /// Update the name of a column.
    ///
    /// # Arguments
    ///
    /// * `path_from` - The path to the column to be renamed.
    /// * `new_name` - The new path of the column.
    ///
    /// # Returns
    ///
    /// An `UpdateSchema` instance with the rename operation staged.
    fn rename_column(
        &mut self,
        column_name: Vec<String>,
        new_name: String,
    ) -> Result<&mut Self, Error> {
        let full_name = column_name.join(".");

        let field = if self.case_sensitive {
            self.schema.field_by_name(&full_name)
        } else {
            self.schema.field_by_name_case_insensitive(&full_name)
        }
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Column to be renamed,'{}' , doesn't exist in the schema",
                    full_name
                ),
            )
        })
        .unwrap();

        if self.deletes.contains(&field.id) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot rename a column that is to be deleted.",
            ));
        }

        let updated_field = self
            .updates
            .entry(field.id)
            .or_insert_with(|| field.as_ref().clone());

        updated_field.name = new_name;

        Ok(self)
    }

    /// Update the type, documentation, or nullability of a column.
    ///
    /// If all of `field_type`, `required`, and `doc` are None, no update is performed.
    ///
    /// # Arguments
    ///
    /// * `column_name` - The full path to the column as a vector of strings.
    /// * `field_type` - Optional new type for the column.
    /// * `required` - Optional new nullability for the column.
    /// * `doc` - Optional new documentation string for the column.
    ///
    /// # Returns
    ///
    /// A mutable reference to self with the update staged.
    pub fn update_column(
        &mut self,
        column_name: Vec<String>,
        field_type: Option<Type>,
        required: Option<bool>,
        doc: Option<String>,
    ) -> Result<&mut Self, Error> {
        if field_type.is_none() && required.is_none() && doc.is_none() {
            return Ok(self);
        }

        let full_name = column_name.join(".");
        let field = if self.case_sensitive {
            self.schema.field_by_name(&full_name)
        } else {
            self.schema.field_by_name_case_insensitive(&full_name)
        }
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Field '{}' not found in schema", full_name),
            )
        })?;

        if self.deletes.contains(&field.id) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Cannot update a column that will be deleted: {}", full_name),
            ));
        }

        if let Some(ref new_field_type) = field_type {
            if !field.field_type.is_primitive() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot change column type: {:?} is not a primitive.",
                        field.field_type
                    ),
                ));
            }

            if !self.allow_incompatible_changes && *field.field_type != *new_field_type {
                // TODO: allow field type conversions
                return Err(Error::new(
                    crate::ErrorKind::DataInvalid,
                    format!(
                        "Cannot change column type: {}: {:?} -> {:?}",
                        full_name, field.field_type, new_field_type
                    ),
                ));
            }
        }

        if let Some(existing) = self.updates.get_mut(&field.id) {
            if let Some(new_field_type) = field_type {
                existing.field_type = Box::new(new_field_type);
            }
            if let Some(new_doc) = doc.clone() {
                existing.doc = Some(new_doc);
            }
        } else {
            self.updates.insert(field.id, NestedField {
                id: field.id,
                name: field.name.clone(),
                field_type: Box::new(field_type.unwrap_or_else(|| *field.field_type.clone())),
                doc: Some(doc.unwrap_or_else(|| field.doc.clone().unwrap())),
                required: field.required,
                initial_default: field.initial_default.clone(),
                write_default: field.write_default.clone(),
            });
        }

        if let Some(req) = required {
            self.set_column_requirement(column_name, req)?;
        }

        Ok(self)
    }

    /// Set the column's nullability (i.e. required flag).
    ///
    /// # Arguments
    ///
    /// * `column_name` - The full path to the column as a vector of strings.
    /// * `required` - The desired required flag (true for required, false for optional).
    ///
    /// # Returns
    ///
    /// An empty Ok(()) on success.
    pub fn set_column_requirement(
        &mut self,
        column_name: Vec<String>,
        required: bool,
    ) -> Result<(), Error> {
        let full_name = column_name.join(".");
        let field = if self.case_sensitive {
            self.schema.field_by_name(&full_name)
        } else {
            self.schema.field_by_name_case_insensitive(&full_name)
        }
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Column '{}' not found in schema", full_name),
            )
        })?;

        if field.required == required {
            return Ok(());
        }

        if !self.allow_incompatible_changes && required {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!(
                    "Cannot change column nullability: {}: optional -> required",
                    full_name
                ),
            ));
        }

        if self.deletes.contains(&field.id) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Cannot update a column that will be deleted: {}", full_name),
            ));
        }

        if let Some(updated_field) = self.updates.get_mut(&field.id) {
            updated_field.required = required;
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

    fn assign_new_column_id(&mut self) -> i32 {
        let id = self.last_column_id;
        self.last_column_id += 1;
        id
    }
}
