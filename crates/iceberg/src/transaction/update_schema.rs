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

use async_trait::async_trait;
use typed_builder::TypedBuilder;

use crate::spec::{
    IndexByName, ListType, Literal, MapType, NestedField, NestedFieldRef, PrimitiveType, Schema,
    SchemaRef, SchemaVisitor, StructType, Type, VariantType, index_parents, visit_schema,
    visit_struct,
};
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, Result, TableRequirement, TableUpdate, ensure_precondition};

const TABLE_ROOT_ID: i32 = -1;

#[derive(Debug)]
pub struct UpdateSchemaAction {
    schema: SchemaRef,

    updates: HashMap<i32, NestedFieldRef>,
    deletes: Vec<i32>,
    moves: HashMap<i32, Vec<Move>>,
    parent_to_added_ids: HashMap<i32, Vec<i32>>,
    id_to_parent: HashMap<i32, i32>,
    added_name_to_id: HashMap<String, i32>,
    identifier_field_ids: HashSet<i32>,
    allow_incompatible_changes: bool,
    last_column_id: i32,
    case_sensitive: bool,
    identifier_field_names: Option<HashSet<String>>,
}

impl UpdateSchemaAction {
    pub(crate) fn new(schema: SchemaRef, last_column_id: i32) -> Result<Self> {
        let id_to_parent = index_parents(schema.as_struct())?;
        Ok(Self {
            schema: schema.clone(),
            updates: HashMap::new(),
            deletes: Vec::new(),
            moves: HashMap::new(),
            parent_to_added_ids: HashMap::new(),
            id_to_parent,
            added_name_to_id: HashMap::new(),
            identifier_field_ids: schema.identifier_field_ids().collect(),
            allow_incompatible_changes: false,
            last_column_id,
            case_sensitive: true,
            identifier_field_names: None,
        })
    }

    pub fn add(mut self, add: AddColumn) -> Result<Self> {
        let (parent, name, is_optional, field_type, doc, default_value) = (
            &add.parent,
            &add.name,
            add.is_optional,
            &add.r#type,
            &add.doc,
            &add.default_value,
        );
        if parent.is_none() && name.contains(".") {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!(
                    "Cannot add column with ambiguous name: {}, use addColumn(parent, name, type)",
                    name
                ),
            ));
        }
        let mut parent_id = TABLE_ROOT_ID;
        let full_name = if let Some(Some(parent)) = parent {
            let parent_field = self.find_field(parent).ok_or(Error::new(
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
            ensure_precondition!(
                parent_field.field_type.is_struct(),
                "Cannot add to non-struct column: {}: {}",
                &parent,
                parent_field.field_type
            );
            parent_id = parent_field.id;
            let full_name = format!("{}.{}", parent, name);
            let current_field = self.find_field(&full_name);
            ensure_precondition!(
                !self.deletes.contains(&parent_id),
                "Can not add a column that will be deleted: {}",
                name
            );
            ensure_precondition!(
                current_field.is_none() || self.deletes.contains(&current_field.unwrap().id),
                "Cannot add column, name already exists: {}.{}",
                &parent,
                &name
            );
            full_name
        } else {
            let current_field = self.find_field(name);
            ensure_precondition!(
                current_field.is_none() || self.deletes.contains(&current_field.unwrap().id),
                "Cannot add column, name already exists: {}",
                &name
            );
            name.clone()
        };
        ensure_precondition!(
            default_value.is_some() || is_optional || self.allow_incompatible_changes,
            "Incompatible change: cannot add required column without a default value: {}",
            full_name
        );
        self.last_column_id += 1;
        let new_id = self.last_column_id;
        self.added_name_to_id
            .insert(self.case_sensitivity_aware_name(&full_name), new_id);

        if parent_id != TABLE_ROOT_ID {
            self.id_to_parent.insert(new_id, parent_id);
        }
        // TODO: Maybe we can use `ReassignFieldIds`?
        let assigned_type = assign_fresh_ids(field_type.clone(), &mut self.last_column_id);
        let mut new_field = NestedField::new(new_id, name, assigned_type, !is_optional);
        new_field.doc = doc.clone();
        new_field.write_default = default_value.clone();
        new_field.initial_default = default_value.clone();
        self.updates.insert(new_id, new_field.into());
        self.parent_to_added_ids
            .entry(parent_id)
            .or_default()
            .push(new_id);
        Ok(self)
    }

    pub fn update(mut self, update: UpdateColumn) -> Result<Self> {
        let (name, ops) = (&update.name, &update.op);
        let field = self.find_for_update(name)?.ok_or(Error::new(
            ErrorKind::PreconditionFailed,
            format!("Cannot update missing column: {}", name),
        ))?;
        ensure_precondition!(
            !self.deletes.contains(&field.id),
            "Cannot update column that will be deleted: {}",
            name,
        );
        let mut new_field = Arc::unwrap_or_clone(field.clone());
        for op in ops {
            match op {
                UpdateColumnOperation::Required(new_required) => {
                    if (*new_required && !field.required) || (!*new_required && field.required) {
                        let is_default_add = self
                            .added_name_to_id
                            .contains_key(&self.case_sensitivity_aware_name(name))
                            && field.initial_default.is_some();
                        ensure_precondition!(
                            !*new_required || is_default_add || self.allow_incompatible_changes,
                            "Cannot change column nullability: {}: optional -> required",
                            name
                        );
                        new_field.required = *new_required;
                    }
                }
                UpdateColumnOperation::Type(new_type) => {
                    ensure_precondition!(
                        is_promotion_allowed(field.field_type.as_ref(), new_type),
                        "Cannot promote {} from type {} to type {}",
                        name,
                        field.field_type,
                        new_type
                    );
                    *new_field.field_type = new_type.clone().into();
                }
                UpdateColumnOperation::Doc(new_doc) => {
                    new_field.doc = new_doc.clone();
                }
                UpdateColumnOperation::DefaultValue(new_default_value) => {
                    new_field.write_default = new_default_value.clone();
                }
            }
        }
        self.updates.insert(field.id, Arc::new(new_field));
        Ok(self)
    }

    pub fn require_column(self, name: &str) -> Result<Self> {
        self.update(UpdateColumn::builder(name).with_required(true).build())
    }

    pub fn rename(mut self, rename: RenameColumn) -> Result<Self> {
        let (name, new_name) = (&rename.name, &rename.new_name);
        let field = self.find_field(name).ok_or(Error::new(
            ErrorKind::PreconditionFailed,
            format!("Cannot rename missing column: {}", name),
        ))?;
        ensure_precondition!(
            !self.deletes.contains(&field.id),
            "Cannot rename a column that will be deleted: {}",
            name
        );
        // merge with an update, if present
        let field_id = field.id;
        let update = self.updates.get(&field_id);
        let new_field = if let Some(update) = update {
            Arc::unwrap_or_clone(update.clone()).with_name(new_name)
        } else {
            Arc::unwrap_or_clone(field.clone()).with_name(new_name)
        };
        self.updates.insert(field_id, Arc::new(new_field));
        if self.identifier_field_ids.contains(&field_id) {
            self.identifier_field_ids.remove(&field_id);
            self.identifier_field_ids.insert(field_id);
        }
        Ok(self)
    }

    pub fn delete(mut self, delete: DeleteColumn) -> Result<Self> {
        let field = self.find_field(&delete.name).ok_or_else(|| {
            Error::new(
                ErrorKind::PreconditionFailed,
                format!("Cannot delete missing column: {}", delete.name),
            )
        })?;
        ensure_precondition!(
            !self.parent_to_added_ids.contains_key(&field.id),
            "Cannot delete a column that has additions: {}",
            delete.name
        );
        ensure_precondition!(
            !self.updates.contains_key(&field.id),
            "Cannot delete a column that has updates: {}",
            delete.name
        );
        self.deletes.push(field.id);
        Ok(self)
    }

    pub fn move_column(mut self, move_column: MoveColumn) -> Result<Self> {
        let (name, reference_name, move_type) = (
            &move_column.name,
            &move_column.reference_name,
            &move_column.move_type,
        );
        let field_id = self.find_for_move(name)?.ok_or(Error::new(
            ErrorKind::PreconditionFailed,
            format!("Cannot move missing column: {}", name),
        ))?;
        let r#move = if move_type == &MoveType::First {
            Move::first(field_id)
        } else {
            let reference_field_id = self.find_for_move(reference_name)?.ok_or(Error::new(
                ErrorKind::PreconditionFailed,
                format!("Cannot move relative to missing column: {}", reference_name),
            ))?;
            match move_type {
                MoveType::Before => Move::before(field_id, reference_field_id),
                MoveType::After => Move::after(field_id, reference_field_id),
                _ => unreachable!(),
            }
        };
        let parent_id = self.id_to_parent.get(&field_id);
        if let Some(&parent_id) = parent_id {
            let parent = self.schema.field_by_id(parent_id).unwrap();
            ensure_precondition!(
                parent.field_type.is_struct(),
                "Cannot move fields in non-struct type: {}",
                parent.field_type
            );
            if r#move.r#type == MoveType::After || r#move.r#type == MoveType::Before {
                ensure_precondition!(
                    parent_id == *self.id_to_parent.get(&r#move.reference_field_id).unwrap(),
                    "Cannot move field {} to a different struct",
                    name,
                );
            }
            self.moves.entry(parent_id).or_default().push(r#move);
        } else {
            if move_type == &MoveType::After || move_type == &MoveType::Before {
                ensure_precondition!(
                    !self.id_to_parent.contains_key(&r#move.reference_field_id),
                    "Cannot move field {} to a different struct",
                    name,
                );
            }
            self.moves.entry(TABLE_ROOT_ID).or_default().push(r#move);
        }
        Ok(self)
    }

    pub fn allow_incompatible_changes(mut self) -> Self {
        self.allow_incompatible_changes = true;
        self
    }

    pub fn case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
        self
    }

    pub fn set_identifier_fields(
        mut self,
        identifier_field_names: impl IntoIterator<Item = String>,
    ) -> Self {
        self.identifier_field_names = Some(identifier_field_names.into_iter().collect());
        self
    }

    pub fn apply(&self) -> Result<SchemaRef> {
        let schema = self.schema.clone();

        for &id in &self.identifier_field_ids {
            let field = schema.field_by_id(id);
            if let Some(field) = field {
                ensure_precondition!(
                    !self.deletes.contains(&id),
                    "Cannot delete identifier field: {}. To force deletion, also call setIdentifierFields to update identifier fields.",
                    field.name
                );
                let mut parent_id = self.id_to_parent.get(&id);
                while let Some(p_id) = parent_id {
                    ensure_precondition!(
                        !self.deletes.contains(p_id),
                        "Cannot delete field {} as it will delete nested identifier field {}.",
                        p_id,
                        field.name
                    );
                    parent_id = self.id_to_parent.get(p_id);
                }
            }
        }
        // apply schema changes
        let mut visitor = ApplyChangesVisitor {
            deletes: &self.deletes,
            updates: &self.updates,
            parent_to_added_ids: &self.parent_to_added_ids,
            moves: &self.moves,
        };
        let struct_type = visit_schema(schema.as_ref(), &mut visitor)?
            .unwrap()
            .to_struct_type()
            .unwrap();

        // validate identifier requirements based on the latest schema (validate id done in Schema::build)
        let fresh_identifier_ids = if let Some(identifier_field_names) =
            &self.identifier_field_names
        {
            let (name_to_id, _) = {
                let mut index = IndexByName::default();
                visit_struct(&struct_type, &mut index)?;
                index.indexes()
            };
            let mut fresh_identifier_ids = HashSet::new();
            for name in identifier_field_names {
                ensure_precondition!(
                    name_to_id.contains_key(name),
                    "Cannot add field {} as an identifier field: not found in current schema or added columns",
                    name
                );
                let id = name_to_id.get(name).unwrap();
                fresh_identifier_ids.insert(*id);
            }
            fresh_identifier_ids
        } else {
            self.identifier_field_ids.clone()
        };
        let schema = Schema::builder()
            .with_fields(struct_type.fields().to_vec())
            .with_identifier_field_ids(fresh_identifier_ids)
            .build()?
            .into();
        Ok(schema)
    }

    fn find_field(&self, field_name: &str) -> Option<&NestedFieldRef> {
        if self.case_sensitive {
            self.schema.field_by_name(field_name)
        } else {
            self.schema.field_by_name_case_insensitive(field_name)
        }
    }

    fn find_for_update(&self, name: &str) -> Result<Option<NestedFieldRef>> {
        let field = self.find_field(name);
        if let Some(field) = field {
            let pending_update = self.updates.get(&field.id);
            if let Some(pending_update) = pending_update {
                Ok(Some(pending_update.clone()))
            } else {
                Ok(Some(field.clone()))
            }
        } else {
            let added_id = self
                .added_name_to_id
                .get(&self.case_sensitivity_aware_name(name));
            if let Some(added_id) = added_id {
                Ok(self.updates.get(added_id).cloned())
            } else {
                Ok(None)
            }
        }
    }

    fn find_for_move(&self, name: &str) -> Result<Option<i32>> {
        let added_id = self
            .added_name_to_id
            .get(&self.case_sensitivity_aware_name(name));
        if let Some(added_id) = added_id {
            return Ok(Some(*added_id));
        }
        let field = self.find_field(name);
        if let Some(field) = field {
            return Ok(Some(field.id));
        }
        Ok(None)
    }

    fn case_sensitivity_aware_name(&self, name: &str) -> String {
        if self.case_sensitive {
            name.into()
        } else {
            name.to_lowercase()
        }
    }
}

#[async_trait]
impl TransactionAction for UpdateSchemaAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let current_schema_id = table.metadata().current_schema_id();
        let last_column_id = table.metadata().last_column_id();

        let schema = self.apply()?;

        // TODO: apply changes to metadata(properties)
        // e.g. parse and update the mapping, transform the metrics
        Ok(ActionCommit::new(
            vec![
                TableUpdate::AddSchema {
                    schema: Arc::unwrap_or_clone(schema),
                },
                TableUpdate::SetCurrentSchema { schema_id: -1 },
            ],
            vec![
                TableRequirement::CurrentSchemaIdMatch { current_schema_id },
                TableRequirement::LastAssignedFieldIdMatch {
                    last_assigned_field_id: last_column_id,
                },
            ],
        ))
    }
}

/// A column to be added to the schema.
#[derive(TypedBuilder)]
pub struct AddColumn {
    #[builder(default, setter(strip_option))]
    parent: Option<Option<String>>,
    #[builder(setter(into))]
    name: String,
    #[builder(default = true)]
    is_optional: bool,
    r#type: Type,
    #[builder(default, setter(strip_option))]
    doc: Option<String>,
    #[builder(default, setter(strip_option))]
    default_value: Option<Literal>,
}

impl AddColumn {
    /// Create a new required `AddColumn` with the given name and type.
    pub fn required(name: impl Into<String>, r#type: Type) -> Self {
        Self {
            parent: None,
            name: name.into(),
            is_optional: false,
            r#type,
            doc: None,
            default_value: None,
        }
    }

    /// Create a new optional `AddColumn` with the given name and type.
    pub fn optional(name: impl Into<String>, r#type: Type) -> Self {
        Self {
            parent: None,
            name: name.into(),
            is_optional: true,
            r#type,
            doc: None,
            default_value: None,
        }
    }

    /// Set the parent for the `AddColumn`.
    pub fn parent(mut self, parent: impl Into<Option<String>>) -> Self {
        self.parent = Some(parent.into());
        self
    }
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

impl RenameColumn {
    /// Create a new `RenameColumn` with the given column name and new name.
    pub fn new(name: impl Into<String>, new_name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            new_name: new_name.into(),
        }
    }
}

/// A column to be updated in the schema.
pub struct UpdateColumn {
    name: String,
    op: Vec<UpdateColumnOperation>,
}

impl UpdateColumn {
    /// Returns a builder for creating an `UpdateColumn`.
    pub fn builder(name: impl Into<String>) -> UpdateColumnBuilder {
        UpdateColumnBuilder {
            name: name.into(),
            is_required: None,
            new_type: None,
            new_doc: None,
            new_default_value: None,
        }
    }
}

/// A builder for constructing `UpdateColumn`.
pub struct UpdateColumnBuilder {
    name: String,
    is_required: Option<bool>,
    new_type: Option<PrimitiveType>,
    new_doc: Option<Option<String>>,
    new_default_value: Option<Option<Literal>>,
}

impl UpdateColumnBuilder {
    /// Set the required status for the column.
    pub fn with_required(mut self, is_required: bool) -> Self {
        self.is_required = Some(is_required);
        self
    }

    /// Set the new type for the column.
    pub fn with_type(mut self, new_type: PrimitiveType) -> Self {
        self.new_type = Some(new_type);
        self
    }

    /// Set the doc for the column.
    pub fn with_doc(mut self, new_doc: Option<String>) -> Self {
        self.new_doc = Some(new_doc);
        self
    }

    /// Set the default value for the column.
    pub fn with_default_value(mut self, new_default_value: Option<Literal>) -> Self {
        self.new_default_value = Some(new_default_value);
        self
    }

    /// Build the `UpdateColumn`.
    pub fn build(self) -> UpdateColumn {
        let mut ops = Vec::new();
        if let Some(is_required) = self.is_required {
            ops.push(UpdateColumnOperation::Required(is_required));
        }
        if let Some(new_type) = self.new_type {
            ops.push(UpdateColumnOperation::Type(new_type));
        }
        if let Some(new_doc) = self.new_doc {
            ops.push(UpdateColumnOperation::Doc(new_doc));
        }
        if let Some(new_default_value) = self.new_default_value {
            ops.push(UpdateColumnOperation::DefaultValue(new_default_value));
        }
        UpdateColumn {
            name: self.name,
            op: ops,
        }
    }
}

enum UpdateColumnOperation {
    Required(bool),
    Type(PrimitiveType),
    Doc(Option<String>),
    DefaultValue(Option<Literal>),
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

struct ApplyChangesVisitor<'a> {
    deletes: &'a Vec<i32>,
    updates: &'a HashMap<i32, NestedFieldRef>,
    parent_to_added_ids: &'a HashMap<i32, Vec<i32>>,
    moves: &'a HashMap<i32, Vec<Move>>,
}

impl SchemaVisitor for ApplyChangesVisitor<'_> {
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
                Arc::unwrap_or_clone(update.clone()).with_type(result_type)
            } else {
                Arc::unwrap_or_clone(field.clone()).with_type(result_type)
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

    fn variant(&mut self, v: &VariantType) -> Result<Self::T> {
        Ok(Some(Type::Variant(*v)))
    }
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
                    Arc::unwrap_or_clone(field.clone())
                        .with_id(new_field_id)
                        .with_type(new_type)
                        .into()
                })
                .collect();
            StructType::new(new_fields).into()
        }
        Type::List(list) => {
            *next_id += 1;
            let element_id = *next_id;
            let element_type = assign_fresh_ids((*list.element_field.field_type).clone(), next_id);
            ListType::new(
                Arc::unwrap_or_clone(list.element_field.clone())
                    .with_id(element_id)
                    .with_type(element_type)
                    .into(),
            )
            .into()
        }
        Type::Map(map) => {
            *next_id += 1;
            let key_id = *next_id;
            *next_id += 1;
            let value_id = *next_id;
            let key_type = assign_fresh_ids((*map.key_field.field_type).clone(), next_id);
            let value_type = assign_fresh_ids((*map.value_field.field_type).clone(), next_id);
            let key_field = Arc::unwrap_or_clone(map.key_field.clone())
                .with_id(key_id)
                .with_type(key_type);
            let value_field = Arc::unwrap_or_clone(map.value_field.clone())
                .with_id(value_id)
                .with_type(value_type);
            MapType::new(key_field.into(), value_field.into()).into()
        }
        Type::Variant(_) => VariantType.into(),
    }
}

fn is_promotion_allowed(from: &Type, to: &PrimitiveType) -> bool {
    let from = match from {
        Type::Primitive(p) => p,
        _ => return false,
    };
    if from == to {
        return true;
    }
    match from {
        PrimitiveType::Int => {
            matches!(to, PrimitiveType::Long)
        }
        PrimitiveType::Float => matches!(to, PrimitiveType::Double),
        PrimitiveType::Decimal {
            precision: p,
            scale: s,
        } => {
            matches!(
                to,
                PrimitiveType::Decimal {
                    precision: to_p,
                    scale: to_s
                } if to_p >= p && to_s == s
            )
        }
        _ => false,
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
    use std::collections::HashSet;
    use std::sync::{Arc, LazyLock};

    use super::{
        AddColumn, DeleteColumn, MoveColumn, RenameColumn, UpdateColumn, UpdateSchemaAction,
    };
    use crate::spec::{
        ListType, Literal, MapType, NestedField, PrimitiveLiteral, PrimitiveType, Schema,
        StructType, Type, prune_columns,
    };
    use crate::test_utils::{get_projected_ids_of_schema, get_projected_ids_of_type};
    use crate::{ErrorKind, Result};

    const SCHEMA_LAST_COLUMN_ID: i32 = 23;

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
                    ListType::optional(
                        14,
                        StructType::new(vec![
                            NestedField::required(15, "x", PrimitiveType::Long.into()).into(),
                            NestedField::required(16, "y", PrimitiveType::Long.into()).into(),
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
                    ListType::required(17, PrimitiveType::Double.into()).into(),
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
    fn test_no_changes() {
        let base = Arc::new(SCHEMA.clone());
        let expected = SCHEMA.clone();
        let updated = UpdateSchemaAction::new(base, SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .apply()
            .unwrap();
        assert_eq!(updated.as_ref(), &expected);
    }

    #[test]
    fn test_delete_fields() -> Result<()> {
        let columns = [
            "id",
            "data",
            "preferences",
            "preferences.feature1",
            "preferences.feature2",
            "locations",
            "locations.lat",
            "locations.long",
            "points",
            "points.x",
            "points.y",
            "doubles",
            "properties",
        ];
        let all_ids: HashSet<i32> = get_projected_ids_of_schema(&SCHEMA);
        for column in columns {
            let mut selected = all_ids.clone();
            let nested = SCHEMA.field_by_name(column).unwrap();
            selected.remove(&nested.id);
            for id in get_projected_ids_of_type(nested.field_type.as_ref()) {
                selected.remove(&id);
            }
            let del = UpdateSchemaAction::new(Arc::new(SCHEMA.clone()), SCHEMA_LAST_COLUMN_ID)?
                .delete(DeleteColumn::new(column))?
                .apply()?;

            let struct_type = prune_columns(&SCHEMA, selected, false)?
                .to_struct_type()
                .unwrap();
            assert_eq!(&struct_type, del.as_struct());
        }
        Ok(())
    }

    #[test]
    fn test_delete_fields_case_sensitive_disabled() -> Result<()> {
        let columns = [
            "Id",
            "Data",
            "Preferences",
            "Preferences.feature1",
            "Preferences.feature2",
            "Locations",
            "Locations.lat",
            "Locations.long",
            "Points",
            "Points.x",
            "Points.y",
            "Doubles",
            "Properties",
        ];
        let all_ids: HashSet<i32> = get_projected_ids_of_schema(&SCHEMA);
        for column in columns {
            let mut selected = all_ids.clone();
            let nested = SCHEMA.field_by_name_case_insensitive(column).unwrap();
            selected.remove(&nested.id);
            for id in get_projected_ids_of_type(nested.field_type.as_ref()) {
                selected.remove(&id);
            }
            let del = UpdateSchemaAction::new(Arc::new(SCHEMA.clone()), SCHEMA_LAST_COLUMN_ID)?
                .case_sensitive(false)
                .delete(DeleteColumn::new(column))?
                .apply()?;

            let struct_type = prune_columns(&SCHEMA, selected, false)?
                .to_struct_type()
                .unwrap();
            assert_eq!(&struct_type, del.as_struct());
        }
        Ok(())
    }

    #[test]
    fn test_update_types() -> Result<()> {
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
                    ListType::optional(
                        14,
                        StructType::new(vec![
                            NestedField::required(15, "x", PrimitiveType::Long.into()).into(),
                            NestedField::required(16, "y", PrimitiveType::Long.into()).into(),
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
                    ListType::required(17, PrimitiveType::Double.into()).into(),
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
        let updated = UpdateSchemaAction::new(Arc::new(SCHEMA.clone()), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .update(
                UpdateColumn::builder("id")
                    .with_type(PrimitiveType::Long)
                    .build(),
            )?
            .update(
                UpdateColumn::builder("locations.lat")
                    .with_type(PrimitiveType::Double)
                    .build(),
            )?
            .update(
                UpdateColumn::builder("locations.long")
                    .with_type(PrimitiveType::Double)
                    .build(),
            )?
            .apply()?;
        assert_eq!(&expected, updated.as_ref());
        Ok(())
    }

    #[test]
    fn test_update_type_preserves_other_metadata() -> Result<()> {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "i", PrimitiveType::Int.into())
                        .with_doc("description")
                        .with_initial_default(PrimitiveLiteral::Int(34).into())
                        .with_write_default(PrimitiveLiteral::Int(35).into())
                        .into(),
                ])
                .build()
                .unwrap(),
        );
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "i", PrimitiveType::Long.into())
                    .with_doc("description")
                    .with_initial_default(PrimitiveLiteral::Int(34).into())
                    .with_write_default(PrimitiveLiteral::Int(35).into())
                    .into(),
            ])
            .build()
            .unwrap();
        let updated = UpdateSchemaAction::new(schema, 1)
            .unwrap()
            .update(
                UpdateColumn::builder("i")
                    .with_type(PrimitiveType::Long)
                    .build(),
            )?
            .apply()?;
        assert_eq!(&expected, updated.as_ref());
        Ok(())
    }

    #[test]
    fn test_update_doc_preserves_other_metadata() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "i", PrimitiveType::Int.into())
                        .with_doc("description")
                        .with_initial_default(PrimitiveLiteral::Int(34).into())
                        .with_write_default(PrimitiveLiteral::Int(35).into())
                        .into(),
                ])
                .build()
                .unwrap(),
        );
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "i", PrimitiveType::Int.into())
                    .with_doc("longer description")
                    .with_initial_default(PrimitiveLiteral::Int(34).into())
                    .with_write_default(PrimitiveLiteral::Int(35).into())
                    .into(),
            ])
            .build()
            .unwrap();
        let updated = UpdateSchemaAction::new(schema, 1)
            .unwrap()
            .update(
                UpdateColumn::builder("i")
                    .with_doc(Some("longer description".to_string()))
                    .build(),
            )
            .unwrap()
            .apply()
            .unwrap();
        assert_eq!(&expected, updated.as_ref());
    }

    #[test]
    fn test_update_default_preserves_other_metadata() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "i", PrimitiveType::Int.into())
                        .with_doc("description")
                        .with_initial_default(PrimitiveLiteral::Int(34).into())
                        .with_write_default(PrimitiveLiteral::Int(35).into())
                        .into(),
                ])
                .build()
                .unwrap(),
        );
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "i", PrimitiveType::Int.into())
                    .with_doc("description")
                    .with_initial_default(PrimitiveLiteral::Int(34).into())
                    .with_write_default(PrimitiveLiteral::Int(123456).into())
                    .into(),
            ])
            .build()
            .unwrap();
        let updated = UpdateSchemaAction::new(schema, 1)
            .unwrap()
            .update(
                UpdateColumn::builder("i")
                    .with_default_value(Some(PrimitiveLiteral::Int(123456).into()))
                    .build(),
            )
            .unwrap()
            .apply()
            .unwrap();
        assert_eq!(&expected, updated.as_ref());
    }

    #[test]
    fn test_update_types_case_insensitive() -> Result<()> {
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
                    ListType::optional(
                        14,
                        StructType::new(vec![
                            NestedField::required(15, "x", PrimitiveType::Long.into()).into(),
                            NestedField::required(16, "y", PrimitiveType::Long.into()).into(),
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
                    ListType::required(17, PrimitiveType::Double.into()).into(),
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

        let updated = UpdateSchemaAction::new(Arc::new(SCHEMA.clone()), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .case_sensitive(false)
            .update(
                UpdateColumn::builder("ID")
                    .with_type(PrimitiveType::Long)
                    .build(),
            )?
            .update(
                UpdateColumn::builder("Locations.Lat")
                    .with_type(PrimitiveType::Double)
                    .build(),
            )?
            .update(
                UpdateColumn::builder("Locations.Long")
                    .with_type(PrimitiveType::Double)
                    .build(),
            )?
            .apply()?;

        assert_eq!(&expected, updated.as_ref());

        Ok(())
    }

    #[test]
    fn test_update_failure() -> Result<()> {
        let allowed_updates: HashSet<(PrimitiveType, PrimitiveType)> = HashSet::from([
            (PrimitiveType::Int, PrimitiveType::Long),
            (PrimitiveType::Float, PrimitiveType::Double),
            (
                PrimitiveType::Decimal {
                    precision: 9,
                    scale: 2,
                },
                PrimitiveType::Decimal {
                    precision: 18,
                    scale: 2,
                },
            ),
        ]);
        let primitives = vec![
            PrimitiveType::Boolean,
            PrimitiveType::Int,
            PrimitiveType::Long,
            PrimitiveType::Float,
            PrimitiveType::Double,
            PrimitiveType::Date,
            PrimitiveType::Time,
            PrimitiveType::Timestamp,
            PrimitiveType::Timestamptz,
            PrimitiveType::String,
            PrimitiveType::Uuid,
            PrimitiveType::Binary,
            PrimitiveType::Fixed(3),
            PrimitiveType::Fixed(4),
            PrimitiveType::Decimal {
                precision: 9,
                scale: 2,
            },
            PrimitiveType::Decimal {
                precision: 9,
                scale: 3,
            },
            PrimitiveType::Decimal {
                precision: 18,
                scale: 2,
            },
            // TODO: Geometry types and Geography types
        ];
        for from in &primitives {
            for to in &primitives {
                let from_schema = Arc::new(
                    Schema::builder()
                        .with_fields(vec![
                            NestedField::required(1, "col", from.clone().into()).into(),
                        ])
                        .build()
                        .unwrap(),
                );
                if from == to || allowed_updates.contains(&(from.clone(), to.clone())) {
                    let expected = Schema::builder()
                        .with_fields(vec![
                            NestedField::required(1, "col", to.clone().into()).into(),
                        ])
                        .build()
                        .unwrap();
                    let result = UpdateSchemaAction::new(from_schema, 1)
                        .unwrap()
                        .update(UpdateColumn::builder("col").with_type(to.clone()).build())?
                        .apply()
                        .unwrap();
                    assert_eq!(&expected, result.as_ref());
                    continue;
                }
                let result = UpdateSchemaAction::new(from_schema, 1)
                    .unwrap()
                    .update(UpdateColumn::builder("col").with_type(to.clone()).build())
                    .and_then(|a| a.apply());
                let err = result.unwrap_err();
                assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
                assert_eq!(
                    err.message(),
                    format!("Cannot promote col from type {} to type {}", from, to)
                );
            }
        }
        Ok(())
    }

    #[test]
    fn test_rename() -> Result<()> {
        let renamed = UpdateSchemaAction::new(Arc::new(SCHEMA.clone()), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .rename(
                RenameColumn::builder()
                    .name("data")
                    .new_name("json")
                    .build(),
            )?
            .rename(
                RenameColumn::builder()
                    .name("preferences")
                    .new_name("options")
                    .build(),
            )?
            .rename(
                RenameColumn::builder()
                    .name("preferences.feature2")
                    .new_name("newfeature")
                    .build(),
            )?
            .rename(
                RenameColumn::builder()
                    .name("locations.lat")
                    .new_name("latitude")
                    .build(),
            )?
            .rename(
                RenameColumn::builder()
                    .name("points.x")
                    .new_name("X")
                    .build(),
            )?
            .rename(
                RenameColumn::builder()
                    .name("points.y")
                    .new_name("Y")
                    .build(),
            )?
            .apply()?;
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
                            NestedField::required(12, "latitude", PrimitiveType::Float.into())
                                .into(),
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
                    ListType::optional(
                        14,
                        StructType::new(vec![
                            NestedField::required(15, "X", PrimitiveType::Long.into()).into(),
                            NestedField::required(16, "Y", PrimitiveType::Long.into()).into(),
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
                    ListType::required(17, PrimitiveType::Double.into()).into(),
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
        assert_eq!(renamed.as_ref(), &expected);
        Ok(())
    }

    #[test]
    fn test_rename_case_insensitive() -> Result<()> {
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
                            NestedField::required(12, "latitude", PrimitiveType::Float.into())
                                .into(),
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
                    ListType::optional(
                        14,
                        StructType::new(vec![
                            NestedField::required(15, "X", PrimitiveType::Long.into()).into(),
                            NestedField::required(16, "y.y", PrimitiveType::Long.into()).into(),
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
                    ListType::required(17, PrimitiveType::Double.into()).into(),
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

        let updated = UpdateSchemaAction::new(Arc::new(SCHEMA.clone()), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .case_sensitive(false)
            .rename(
                RenameColumn::builder()
                    .name("Data")
                    .new_name("json")
                    .build(),
            )?
            .rename(
                RenameColumn::builder()
                    .name("Preferences")
                    .new_name("options")
                    .build(),
            )?
            .rename(
                RenameColumn::builder()
                    .name("Preferences.Feature2")
                    .new_name("newfeature")
                    .build(),
            )?
            .rename(
                RenameColumn::builder()
                    .name("Locations.Lat")
                    .new_name("latitude")
                    .build(),
            )?
            .rename(
                RenameColumn::builder()
                    .name("Points.X")
                    .new_name("X")
                    .build(),
            )?
            .rename(
                RenameColumn::builder()
                    .name("Points.Y")
                    .new_name("y.y")
                    .build(),
            )?
            .rename(
                RenameColumn::builder()
                    .name("Data")
                    .new_name("json")
                    .build(),
            )?
            .apply()?;

        assert_eq!(updated.as_ref(), &expected);

        Ok(())
    }

    #[test]
    fn test_add_fields() -> Result<()> {
        let added = UpdateSchemaAction::new(Arc::new(SCHEMA.clone()), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .add(
                AddColumn::builder()
                    .name("topLevel")
                    .r#type(Type::Primitive(PrimitiveType::Decimal {
                        precision: 9,
                        scale: 2,
                    }))
                    .build(),
            )?
            .add(
                AddColumn::builder()
                    .parent(Some("locations".to_string()))
                    .name("alt")
                    .r#type(Type::Primitive(PrimitiveType::Float))
                    .build(),
            )?
            .add(
                AddColumn::builder()
                    .parent(Some("points".to_string()))
                    .name("z")
                    .r#type(Type::Primitive(PrimitiveType::Long))
                    .build(),
            )?
            .add(
                AddColumn::builder()
                    .parent(Some("points".to_string()))
                    .name("t.t")
                    .r#type(Type::Primitive(PrimitiveType::Long))
                    .build(),
            )?
            .apply()?;

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
                            NestedField::optional(25, "alt", PrimitiveType::Float.into()).into(),
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
                    ListType::required(17, PrimitiveType::Double.into()).into(),
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
        Ok(())
    }

    #[test]
    fn test_add_column_with_default() -> Result<()> {
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
        let result = UpdateSchemaAction::new(schema.clone(), 1)
            .unwrap()
            .add(
                AddColumn::builder()
                    .name("data")
                    .r#type(Type::Primitive(PrimitiveType::String))
                    .doc("description".into())
                    .default_value(Literal::string("unknown"))
                    .build(),
            )?
            .apply()?;
        assert_eq!(&expected, result.as_ref());
        Ok(())
    }

    #[test]
    fn test_add_column_with_update_column_default() -> Result<()> {
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
        let result = UpdateSchemaAction::new(schema.clone(), 1)
            .unwrap()
            .add(
                AddColumn::builder()
                    .name("data")
                    .r#type(PrimitiveType::String.into())
                    .build(),
            )?
            .update(
                UpdateColumn::builder("data")
                    .with_default_value(Some(Literal::string("unknown")))
                    .build(),
            )?
            .apply()?;
        assert_eq!(&expected, result.as_ref());
        Ok(())
    }

    #[test]
    fn test_add_nested_struct() -> Result<()> {
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

        let result = UpdateSchemaAction::new(schema.clone(), 1)
            .unwrap()
            .add(
                AddColumn::builder()
                    .name("location")
                    .r#type(Type::Struct(struct_type))
                    .build(),
            )?
            .apply()?;
        assert_eq!(&expected, result.as_ref());
        Ok(())
    }

    #[test]
    fn test_add_nested_map_of_structs() -> Result<()> {
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
        let result = UpdateSchemaAction::new(schema, 1)
            .unwrap()
            .add(
                AddColumn::builder()
                    .name("locations")
                    .r#type(map.into())
                    .build(),
            )?
            .apply()?;
        assert_eq!(&expected, result.as_ref());
        Ok(())
    }

    #[test]
    fn test_add_nested_list_of_structs() -> Result<()> {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", PrimitiveType::Int.into()).into(),
                ])
                .build()
                .unwrap(),
        );
        let list = ListType::optional(
            1,
            StructType::new(vec![
                NestedField::required(9, "lat", PrimitiveType::Int.into()).into(),
                NestedField::optional(8, "long", PrimitiveType::Int.into()).into(),
            ])
            .into(),
        );
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Int.into()).into(),
                NestedField::optional(
                    2,
                    "locations",
                    ListType::optional(
                        3,
                        StructType::new(vec![
                            NestedField::required(4, "lat", PrimitiveType::Int.into()).into(),
                            NestedField::optional(5, "long", PrimitiveType::Int.into()).into(),
                        ])
                        .into(),
                    )
                    .into(),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let result = UpdateSchemaAction::new(schema, 1)
            .unwrap()
            .add(
                AddColumn::builder()
                    .name("locations")
                    .r#type(list.into())
                    .build(),
            )?
            .apply()?;
        assert_eq!(&expected, result.as_ref());
        Ok(())
    }

    #[test]
    fn test_add_required_column_without_default() -> Result<()> {
        let schema = Arc::new(
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
                NestedField::required(2, "data", PrimitiveType::String.into())
                    .with_doc("description")
                    .with_initial_default(PrimitiveLiteral::String("unknown".into()).into())
                    .with_write_default(PrimitiveLiteral::String("unknown".into()).into())
                    .into(),
            ])
            .build()
            .unwrap();

        let result = UpdateSchemaAction::new(schema.clone(), 1)
            .unwrap()
            .add(
                AddColumn::builder()
                    .name("data")
                    .is_optional(false)
                    .r#type(PrimitiveType::String.into())
                    .doc("description".into())
                    .default_value(PrimitiveLiteral::String("unknown".into()).into())
                    .build(),
            )?
            .apply()?;
        assert_eq!(&expected, result.as_ref());
        Ok(())
    }

    #[test]
    fn test_add_required_column_with_default() -> Result<()> {
        let schema = Arc::new(
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
                NestedField::required(2, "data", PrimitiveType::String.into())
                    .with_doc("description")
                    .with_initial_default(PrimitiveLiteral::String("unknown".into()).into())
                    .with_write_default(PrimitiveLiteral::String("unknown".into()).into())
                    .into(),
            ])
            .build()
            .unwrap();

        let result = UpdateSchemaAction::new(schema.clone(), 1)
            .unwrap()
            .add(
                AddColumn::builder()
                    .name("data")
                    .r#type(PrimitiveType::String.into())
                    .is_optional(false)
                    .doc("description".into())
                    .default_value(PrimitiveLiteral::String("unknown".into()).into())
                    .build(),
            )?
            .apply()?;

        assert_eq!(&expected, result.as_ref());
        Ok(())
    }

    #[test]
    fn test_add_required_column_with_update_column_default() -> Result<()> {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "id", PrimitiveType::Int.into()).into(),
                ])
                .build()
                .unwrap(),
        );
        let err = UpdateSchemaAction::new(schema.clone(), 1)
            .unwrap()
            .add(
                AddColumn::builder()
                    .name("data")
                    .r#type(PrimitiveType::String.into())
                    .is_optional(false)
                    .build(),
            )
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(
            err.message(),
            "Incompatible change: cannot add required column without a default value: data"
        );
        Ok(())
    }

    #[test]
    fn test_add_required_column_case_insensitive() {
        let schema: Arc<_> = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Int.into()).into(),
            ])
            .build()
            .unwrap()
            .into();
        let err = UpdateSchemaAction::new(schema.clone(), 1)
            .unwrap()
            .case_sensitive(false)
            .allow_incompatible_changes()
            .add(
                AddColumn::builder()
                    .name("ID")
                    .r#type(PrimitiveType::String.into())
                    .is_optional(false)
                    .build(),
            )
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(err.message(), "Cannot add column, name already exists: ID");
    }

    #[test]
    #[ignore = "I do not think it is valuable"]
    fn test_add_multiple_required_column_case_insensitive() {}

    #[test]
    fn test_make_column_optional() -> Result<()> {
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
                NestedField::optional(1, "id", PrimitiveType::Int.into()).into(),
            ])
            .build()
            .unwrap();
        let result = UpdateSchemaAction::new(schema.clone(), 1)
            .unwrap()
            .update(UpdateColumn::builder("id").with_required(false).build())?
            .apply()?;
        assert_eq!(&expected, result.as_ref());
        Ok(())
    }

    #[test]
    fn test_require_column() -> Result<()> {
        let column = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "id", PrimitiveType::Int.into()).into(),
            ])
            .build()
            .unwrap();
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Int.into()).into(),
            ])
            .build()
            .unwrap();

        // required to required is not an incompatible change
        assert_eq!(
            UpdateSchemaAction::new(Arc::new(expected.clone()), 1)
                .unwrap()
                .update(UpdateColumn::builder("id").with_required(true).build())?
                .apply()?
                .as_ref(),
            &expected
        );

        let result = UpdateSchemaAction::new(Arc::new(column), 1)
            .unwrap()
            .allow_incompatible_changes()
            .update(UpdateColumn::builder("id").with_required(true).build())?
            .apply()?;
        assert_eq!(&expected, result.as_ref());
        Ok(())
    }

    #[test]
    fn test_add_column_with_default_to_required_column() -> Result<()> {
        let schema = Arc::new(
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
                NestedField::required(2, "data", PrimitiveType::String.into())
                    .with_initial_default(Literal::string("unknown"))
                    .with_write_default(Literal::string("unknown"))
                    .into(),
            ])
            .build()
            .unwrap();
        let result = UpdateSchemaAction::new(schema.clone(), 1)
            .unwrap()
            .add(
                AddColumn::builder()
                    .name("data")
                    .r#type(Type::Primitive(PrimitiveType::String))
                    .default_value(Literal::string("unknown"))
                    .build(),
            )?
            .update(UpdateColumn::builder("data").with_required(true).build())?
            .apply()?;
        assert_eq!(&expected, result.as_ref());
        Ok(())
    }

    #[test]
    fn test_add_column_with_update_column_default_to_required_column() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "id", PrimitiveType::Int.into()).into(),
                ])
                .build()
                .unwrap(),
        );
        let err = UpdateSchemaAction::new(schema.clone(), 1)
            .unwrap()
            .add(
                AddColumn::builder()
                    .name("data")
                    .r#type(PrimitiveType::String.into())
                    .build(),
            )
            .unwrap()
            .require_column("data")
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(
            err.message(),
            "Cannot change column nullability: data: optional -> required"
        );
    }

    #[test]
    fn test_require_column_case_insensitive() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "id", PrimitiveType::Int.into()).into(),
                ])
                .build()
                .unwrap(),
        );
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Int.into()).into(),
            ])
            .build()
            .unwrap();
        let result = UpdateSchemaAction::new(schema.clone(), 1)
            .unwrap()
            .case_sensitive(false)
            .allow_incompatible_changes()
            .update(UpdateColumn::builder("ID").with_required(true).build())
            .unwrap()
            .apply()
            .unwrap();
        assert_eq!(&expected, result.as_ref());
    }

    #[test]
    fn test_mixed_changes() -> Result<()> {
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Long.into())
                    .with_doc("unique id")
                    .into(),
                NestedField::required(2, "json", PrimitiveType::String.into()).into(),
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
                            NestedField::required(12, "latitude", PrimitiveType::Double.into())
                                .with_doc("latitude")
                                .into(),
                            NestedField::optional(25, "alt", PrimitiveType::Float.into()).into(),
                            NestedField::required(28, "description", PrimitiveType::String.into())
                                .with_doc("Location description")
                                .into(),
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
                    ListType::optional(
                        14,
                        StructType::new(vec![
                            NestedField::optional(15, "X", PrimitiveType::Long.into()).into(),
                            NestedField::required(16, "y.y", PrimitiveType::Long.into()).into(),
                            NestedField::optional(26, "z", PrimitiveType::Long.into()).into(),
                            NestedField::optional(27, "t.t", PrimitiveType::Long.into())
                                .with_doc("name with '.'")
                                .into(),
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
                    ListType::required(17, PrimitiveType::Double.into()).into(),
                )
                .into(),
                NestedField::optional(
                    24,
                    "toplevel",
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

        let updated = UpdateSchemaAction::new(Arc::new(SCHEMA.clone()), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .add(
                AddColumn::builder()
                    .name("toplevel")
                    .r#type(Type::Primitive(PrimitiveType::Decimal {
                        precision: 9,
                        scale: 2,
                    }))
                    .build(),
            )?
            .add(
                AddColumn::builder()
                    .parent(Some("locations".into()))
                    .name("alt")
                    .r#type(Type::Primitive(PrimitiveType::Float))
                    .build(),
            )?
            .add(
                AddColumn::builder()
                    .parent(Some("points".into()))
                    .name("z")
                    .r#type(Type::Primitive(PrimitiveType::Long))
                    .build(),
            )?
            .add(
                AddColumn::builder()
                    .parent(Some("points".to_string()))
                    .name("t.t")
                    .r#type(Type::Primitive(PrimitiveType::Long))
                    .doc("name with '.'".into())
                    .build(),
            )?
            .rename(
                RenameColumn::builder()
                    .name("data")
                    .new_name("json")
                    .build(),
            )?
            .rename(
                RenameColumn::builder()
                    .name("preferences")
                    .new_name("options")
                    .build(),
            )?
            .rename(
                RenameColumn::builder()
                    .name("preferences.feature2")
                    .new_name("newfeature")
                    .build(),
            )?
            .rename(
                RenameColumn::builder()
                    .name("locations.lat")
                    .new_name("latitude")
                    .build(),
            )?
            .rename(
                RenameColumn::builder()
                    .name("points.x")
                    .new_name("X")
                    .build(),
            )?
            .rename(
                RenameColumn::builder()
                    .name("points.y")
                    .new_name("y.y")
                    .build(),
            )?
            .update(
                UpdateColumn::builder("id")
                    .with_type(PrimitiveType::Long)
                    .build(),
            )?
            .update(
                UpdateColumn::builder("id")
                    .with_doc(Some("unique id".into()))
                    .build(),
            )?
            .update(
                UpdateColumn::builder("locations.lat")
                    .with_type(PrimitiveType::Double)
                    .build(),
            )?
            .update(
                UpdateColumn::builder("locations.lat")
                    .with_doc(Some("latitude".into()))
                    .build(),
            )?
            .delete(DeleteColumn::new("locations.long"))?
            .delete(DeleteColumn::new("properties"))?
            .update(
                UpdateColumn::builder("points.x")
                    .with_required(false)
                    .build(),
            )?
            .allow_incompatible_changes()
            .update(UpdateColumn::builder("data").with_required(true).build())?
            .add(
                AddColumn::builder()
                    .parent(Some("locations".into()))
                    .name("description")
                    .r#type(Type::Primitive(PrimitiveType::String))
                    .is_optional(false)
                    .doc("Location description".into())
                    .build(),
            )?
            .apply()?;

        assert_eq!(&expected, updated.as_ref());
        Ok(())
    }

    #[test]
    fn test_ambiguous_add() {
        // preferences.booleans could be top-level or a field of preferences
        let result = UpdateSchemaAction::new(Arc::new(SCHEMA.clone()), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .add(
                AddColumn::builder()
                    .name("preferences.booleans")
                    .r#type(PrimitiveType::Boolean.into())
                    .build(),
            )
            .and_then(|a| a.apply());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert!(
            err.message()
                .starts_with("Cannot add column with ambiguous name: preferences.booleans")
        );
    }

    #[test]
    fn test_add_already_exists() {
        let err = UpdateSchemaAction::new(Arc::new(SCHEMA.clone()), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .add(
                AddColumn::builder()
                    .parent(Some("preferences".to_string()))
                    .name("feature1")
                    .r#type(PrimitiveType::Boolean.into())
                    .build(),
            )
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(
            err.message(),
            "Cannot add column, name already exists: preferences.feature1"
        );

        let err = UpdateSchemaAction::new(Arc::new(SCHEMA.clone()), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .add(
                AddColumn::builder()
                    .name("preferences")
                    .r#type(PrimitiveType::Boolean.into())
                    .build(),
            )
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(
            err.message(),
            "Cannot add column, name already exists: preferences"
        );
    }

    #[test]
    fn test_delete_then_add() -> Result<()> {
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
                NestedField::optional(2, "id", PrimitiveType::Int.into()).into(),
            ])
            .build()
            .unwrap();

        let updated = UpdateSchemaAction::new(schema, 1)
            .unwrap()
            .delete(DeleteColumn::new("id"))?
            .add(
                AddColumn::builder()
                    .name("id")
                    .r#type(PrimitiveType::Int.into())
                    .build(),
            )?
            .apply()?;
        assert_eq!(updated.as_struct(), expected.as_struct());
        Ok(())
    }

    #[test]
    fn test_delete_then_add_nested() -> Result<()> {
        let expected_nested = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Int.into()).into(),
                NestedField::optional(2, "data", PrimitiveType::String.into()).into(),
                NestedField::optional(
                    3,
                    "preferences",
                    StructType::new(vec![
                        NestedField::optional(9, "feature2", PrimitiveType::Boolean.into()).into(),
                        NestedField::optional(24, "feature1", PrimitiveType::Boolean.into()).into(),
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
                    ListType::optional(
                        14,
                        StructType::new(vec![
                            NestedField::required(15, "x", PrimitiveType::Long.into()).into(),
                            NestedField::required(16, "y", PrimitiveType::Long.into()).into(),
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
                    ListType::required(17, PrimitiveType::Double.into()).into(),
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

        let updated_nested =
            UpdateSchemaAction::new(Arc::new(SCHEMA.clone()), SCHEMA_LAST_COLUMN_ID)
                .unwrap()
                .delete(DeleteColumn::new("preferences.feature1"))?
                .add(
                    AddColumn::builder()
                        .parent(Some("preferences".to_string()))
                        .name("feature1")
                        .r#type(PrimitiveType::Boolean.into())
                        .build(),
                )?
                .apply()?;
        assert_eq!(updated_nested.as_struct(), expected_nested.as_struct());
        Ok(())
    }

    #[test]
    fn test_delete_missing_column() {
        let err = UpdateSchemaAction::new(Arc::new(SCHEMA.clone()), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .delete(DeleteColumn::new("col"))
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(err.message(), "Cannot delete missing column: col");
    }

    #[test]
    fn test_add_delete_conflict() {
        let err = UpdateSchemaAction::new(Arc::new(SCHEMA.clone()), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .add(
                AddColumn::builder()
                    .name("col")
                    .r#type(PrimitiveType::Int.into())
                    .build(),
            )
            .unwrap()
            .delete(DeleteColumn::new("col"))
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(err.message(), "Cannot delete missing column: col");

        let err = UpdateSchemaAction::new(Arc::new(SCHEMA.clone()), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .add(
                AddColumn::builder()
                    .parent(Some("preferences".to_string()))
                    .name("feature3")
                    .r#type(PrimitiveType::Int.into())
                    .build(),
            )
            .unwrap()
            .delete(DeleteColumn::new("preferences"))
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(
            err.message(),
            "Cannot delete a column that has additions: preferences"
        );
    }

    #[test]
    fn test_rename_missing_column() {
        let err = UpdateSchemaAction::new(Arc::new(SCHEMA.clone()), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .rename(RenameColumn::builder().name("col").new_name("fail").build())
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(err.message(), "Cannot rename missing column: col");
    }

    #[test]
    fn test_rename_delete_conflict() {
        let err = UpdateSchemaAction::new(Arc::new(SCHEMA.clone()), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .rename(RenameColumn::builder().name("id").new_name("col").build())
            .unwrap()
            .delete(DeleteColumn::new("id"))
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(err.message(), "Cannot delete a column that has updates: id");

        let err = UpdateSchemaAction::new(Arc::new(SCHEMA.clone()), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .rename(RenameColumn::builder().name("id").new_name("col").build())
            .unwrap()
            .delete(DeleteColumn::new("col"))
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(err.message(), "Cannot delete missing column: col");
    }

    #[test]
    fn test_delete_rename_conflict() {
        let err = UpdateSchemaAction::new(Arc::new(SCHEMA.clone()), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .delete(DeleteColumn::new("id"))
            .unwrap()
            .rename(
                RenameColumn::builder()
                    .name("id")
                    .new_name("identifier")
                    .build(),
            )
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(
            err.message(),
            "Cannot rename a column that will be deleted: id"
        );
    }

    #[test]
    fn test_update_missing_column() {
        let err = UpdateSchemaAction::new(Arc::new(SCHEMA.clone()), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .update(
                UpdateColumn::builder("col")
                    .with_type(PrimitiveType::Date)
                    .build(),
            )
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(err.message(), "Cannot update missing column: col");
    }

    #[test]
    fn test_update_missing_column_doc() {
        let err = UpdateSchemaAction::new(Arc::new(SCHEMA.clone()), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .update(
                UpdateColumn::builder("col")
                    .with_doc(Some("description".into()))
                    .build(),
            )
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(err.message(), "Cannot update missing column: col");
    }

    #[test]
    fn test_update_missing_column_default_value() {
        let err = UpdateSchemaAction::new(Arc::new(SCHEMA.clone()), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .update(
                UpdateColumn::builder("col")
                    .with_default_value(Some(Literal::int(34)))
                    .build(),
            )
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(err.message(), "Cannot update missing column: col");
    }

    #[test]
    fn test_update_delete_conflict() {
        let err = UpdateSchemaAction::new(Arc::new(SCHEMA.clone()), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .update(
                UpdateColumn::builder("id")
                    .with_type(PrimitiveType::Long)
                    .build(),
            )
            .unwrap()
            .delete(DeleteColumn::new("id"))
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(err.message(), "Cannot delete a column that has updates: id");
    }

    #[test]
    fn test_delete_update_conflict() {
        let err = UpdateSchemaAction::new(Arc::new(SCHEMA.clone()), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .delete(DeleteColumn::new("id"))
            .unwrap()
            .update(
                UpdateColumn::builder("id")
                    .with_type(PrimitiveType::Long)
                    .build(),
            )
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(
            err.message(),
            "Cannot update column that will be deleted: id"
        );
    }

    #[test]
    fn test_delete_map_key() {
        let err = UpdateSchemaAction::new(Arc::new(SCHEMA.clone()), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .delete(DeleteColumn::new("locations.key"))
            .unwrap()
            .apply()
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert!(err.message().starts_with("Cannot delete map keys"));
    }

    #[test]
    fn test_delete_map_value() {
        let err = UpdateSchemaAction::new(Arc::new(SCHEMA.clone()), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .delete(DeleteColumn::new("locations.value"))
            .unwrap()
            .apply()
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert!(
            err.message()
                .starts_with("Cannot delete value type from map")
        );
    }

    #[test]
    fn test_add_field_to_map_key() {
        let err = UpdateSchemaAction::new(Arc::new(SCHEMA.clone()), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .add(
                AddColumn::builder()
                    .parent(Some("locations.key".to_string()))
                    .name("address_line_2")
                    .r#type(PrimitiveType::String.into())
                    .build(),
            )
            .unwrap()
            .apply()
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert!(err.message().starts_with("Cannot add fields to map keys"));
    }

    #[test]
    fn test_alter_map_key() {
        let err = UpdateSchemaAction::new(Arc::new(SCHEMA.clone()), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .update(
                UpdateColumn::builder("locations.key.zip")
                    .with_type(PrimitiveType::Long)
                    .build(),
            )
            .unwrap()
            .apply()
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert!(err.message().starts_with("Cannot alter map keys"));
    }

    #[test]
    fn test_update_map_key() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(
                        1,
                        "m",
                        MapType::optional(
                            2,
                            PrimitiveType::Int.into(),
                            3,
                            PrimitiveType::Double.into(),
                        )
                        .into(),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let err = UpdateSchemaAction::new(schema, 3)
            .unwrap()
            .update(
                UpdateColumn::builder("m.key")
                    .with_type(PrimitiveType::Long)
                    .build(),
            )
            .unwrap()
            .apply()
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert!(err.message().starts_with("Cannot update map keys"));
    }

    #[test]
    fn test_update_added_column_type() -> Result<()> {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "i", PrimitiveType::Int.into()).into(),
                ])
                .build()
                .unwrap(),
        );
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "i", PrimitiveType::Int.into()).into(),
                NestedField::optional(2, "value", PrimitiveType::Long.into()).into(),
            ])
            .build()
            .unwrap();
        let updated = UpdateSchemaAction::new(schema, 1)
            .unwrap()
            .add(
                AddColumn::builder()
                    .name("value")
                    .r#type(PrimitiveType::Int.into())
                    .build(),
            )?
            .update(
                UpdateColumn::builder("value")
                    .with_type(PrimitiveType::Long)
                    .build(),
            )?
            .apply()?;
        assert_eq!(updated.as_struct(), expected.as_struct());
        Ok(())
    }

    #[test]
    fn test_update_added_column_doc() -> Result<()> {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "i", PrimitiveType::Int.into()).into(),
                ])
                .build()
                .unwrap(),
        );
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "i", PrimitiveType::Int.into()).into(),
                NestedField::optional(2, "value", PrimitiveType::Long.into())
                    .with_doc("a value")
                    .into(),
            ])
            .build()
            .unwrap();
        let updated = UpdateSchemaAction::new(schema, 1)
            .unwrap()
            .add(
                AddColumn::builder()
                    .name("value")
                    .r#type(PrimitiveType::Long.into())
                    .build(),
            )?
            .update(
                UpdateColumn::builder("value")
                    .with_doc(Some("a value".into()))
                    .build(),
            )?
            .apply()?;
        assert_eq!(updated.as_struct(), expected.as_struct());
        Ok(())
    }

    #[test]
    fn test_update_deleted_column_doc() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "i", PrimitiveType::Int.into()).into(),
                ])
                .build()
                .unwrap(),
        );
        let err = UpdateSchemaAction::new(schema, 3)
            .unwrap()
            .delete(DeleteColumn::new("i"))
            .unwrap()
            .update(
                UpdateColumn::builder("i")
                    .with_doc(Some("a value".into()))
                    .build(),
            )
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(
            err.message(),
            "Cannot update column that will be deleted: i"
        );
    }

    #[test]
    fn test_multiple_moves() -> Result<()> {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "a", PrimitiveType::Int.into()).into(),
                    NestedField::required(2, "b", PrimitiveType::Int.into()).into(),
                    NestedField::required(3, "c", PrimitiveType::Int.into()).into(),
                    NestedField::required(4, "d", PrimitiveType::Int.into()).into(),
                ])
                .build()
                .unwrap(),
        );
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(3, "c", PrimitiveType::Int.into()).into(),
                NestedField::required(2, "b", PrimitiveType::Int.into()).into(),
                NestedField::required(4, "d", PrimitiveType::Int.into()).into(),
                NestedField::required(1, "a", PrimitiveType::Int.into()).into(),
            ])
            .build()
            .unwrap();
        let actual = UpdateSchemaAction::new(schema, 4)
            .unwrap()
            .move_column(MoveColumn::first("d"))?
            .move_column(MoveColumn::first("c"))?
            .move_column(MoveColumn::after("b", "d"))?
            .move_column(MoveColumn::before("d", "a"))?
            .apply()?;
        assert_eq!(actual.as_struct(), expected.as_struct());
        Ok(())
    }

    #[test]
    fn test_move_top_level_column_first() -> Result<()> {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                    NestedField::required(2, "data", PrimitiveType::String.into()).into(),
                ])
                .build()
                .unwrap(),
        );
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(2, "data", PrimitiveType::String.into()).into(),
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
            ])
            .build()
            .unwrap();
        let actual = UpdateSchemaAction::new(schema, 2)
            .unwrap()
            .move_column(MoveColumn::first("data"))?
            .apply()?;
        assert_eq!(actual.as_struct(), expected.as_struct());
        Ok(())
    }

    #[test]
    fn test_move_top_level_column_before_first() -> Result<()> {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                    NestedField::required(2, "data", PrimitiveType::String.into()).into(),
                ])
                .build()
                .unwrap(),
        );
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(2, "data", PrimitiveType::String.into()).into(),
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
            ])
            .build()
            .unwrap();
        let actual = UpdateSchemaAction::new(schema, 2)
            .unwrap()
            .move_column(MoveColumn::before("data", "id"))?
            .apply()?;
        assert_eq!(actual.as_struct(), expected.as_struct());
        Ok(())
    }

    #[test]
    fn test_move_top_level_column_after_last() -> Result<()> {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                    NestedField::required(2, "data", PrimitiveType::String.into()).into(),
                ])
                .build()
                .unwrap(),
        );
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(2, "data", PrimitiveType::String.into()).into(),
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
            ])
            .build()
            .unwrap();
        let actual = UpdateSchemaAction::new(schema, 2)
            .unwrap()
            .move_column(MoveColumn::after("id", "data"))?
            .apply()?;
        assert_eq!(actual.as_struct(), expected.as_struct());
        Ok(())
    }

    #[test]
    fn test_move_top_level_column_after() -> Result<()> {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                    NestedField::required(2, "data", PrimitiveType::String.into()).into(),
                    NestedField::optional(3, "ts", PrimitiveType::Timestamptz.into()).into(),
                ])
                .build()
                .unwrap(),
        );
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                NestedField::optional(3, "ts", PrimitiveType::Timestamptz.into()).into(),
                NestedField::required(2, "data", PrimitiveType::String.into()).into(),
            ])
            .build()
            .unwrap();
        let actual = UpdateSchemaAction::new(schema, 3)
            .unwrap()
            .move_column(MoveColumn::after("ts", "id"))?
            .apply()?;
        assert_eq!(actual.as_struct(), expected.as_struct());
        Ok(())
    }

    #[test]
    fn test_move_top_level_column_before() -> Result<()> {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(3, "ts", PrimitiveType::Timestamptz.into()).into(),
                    NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                    NestedField::required(2, "data", PrimitiveType::String.into()).into(),
                ])
                .build()
                .unwrap(),
        );
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                NestedField::optional(3, "ts", PrimitiveType::Timestamptz.into()).into(),
                NestedField::required(2, "data", PrimitiveType::String.into()).into(),
            ])
            .build()
            .unwrap();
        let actual = UpdateSchemaAction::new(schema, 3)
            .unwrap()
            .move_column(MoveColumn::before("ts", "data"))?
            .apply()?;
        assert_eq!(actual.as_struct(), expected.as_struct());
        Ok(())
    }

    /*
        Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    required(3, "count", Types.LongType.get()),
                    required(4, "data", Types.StringType.get()))));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    required(4, "data", Types.StringType.get()),
                    required(3, "count", Types.LongType.get()))));

    Schema actual = new SchemaUpdate(schema, 4).moveFirst("struct.data").apply();

    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
     */
    #[test]
    fn test_move_nested_field_first() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                    NestedField::required(
                        2,
                        "struct",
                        StructType::new(vec![
                            NestedField::required(3, "count", PrimitiveType::Long.into()).into(),
                            NestedField::required(4, "data", PrimitiveType::String.into()).into(),
                        ])
                        .into(),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                NestedField::required(
                    2,
                    "struct",
                    StructType::new(vec![
                        NestedField::required(4, "data", PrimitiveType::String.into()).into(),
                        NestedField::required(3, "count", PrimitiveType::Long.into()).into(),
                    ])
                    .into(),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let actual = UpdateSchemaAction::new(schema, 4)
            .unwrap()
            .move_column(MoveColumn::first("struct.data"))
            .unwrap()
            .apply()
            .unwrap();
        assert_eq!(actual.as_struct(), expected.as_struct());
    }

    /*
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    required(3, "count", Types.LongType.get()),
                    required(4, "data", Types.StringType.get()))));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    required(4, "data", Types.StringType.get()),
                    required(3, "count", Types.LongType.get()))));

    Schema actual = new SchemaUpdate(schema, 4).moveBefore("struct.data", "struct.count").apply();

    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
     */
    #[test]
    fn test_move_nested_field_before_first() {
        let schema: Arc<_> = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                NestedField::required(
                    2,
                    "struct",
                    StructType::new(vec![
                        NestedField::required(3, "count", PrimitiveType::Long.into()).into(),
                        NestedField::required(4, "data", PrimitiveType::String.into()).into(),
                    ])
                    .into(),
                )
                .into(),
            ])
            .build()
            .unwrap()
            .into();
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                NestedField::required(
                    2,
                    "struct",
                    StructType::new(vec![
                        NestedField::required(4, "data", PrimitiveType::String.into()).into(),
                        NestedField::required(3, "count", PrimitiveType::Long.into()).into(),
                    ])
                    .into(),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let actual = UpdateSchemaAction::new(schema, 4)
            .unwrap()
            .move_column(MoveColumn::before("struct.data", "struct.count"))
            .unwrap()
            .apply()
            .unwrap();
        assert_eq!(actual.as_ref(), &expected);
    }

    /*
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    required(3, "count", Types.LongType.get()),
                    required(4, "data", Types.StringType.get()))));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    required(4, "data", Types.StringType.get()),
                    required(3, "count", Types.LongType.get()))));

    Schema actual = new SchemaUpdate(schema, 4).moveAfter("struct.count", "struct.data").apply();

    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
     */
    #[test]
    fn test_move_nested_field_after_last() {
        let schema: Arc<_> = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                NestedField::required(
                    2,
                    "struct",
                    StructType::new(vec![
                        NestedField::required(3, "count", PrimitiveType::Long.into()).into(),
                        NestedField::required(4, "data", PrimitiveType::String.into()).into(),
                    ])
                    .into(),
                )
                .into(),
            ])
            .build()
            .unwrap()
            .into();
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                NestedField::required(
                    2,
                    "struct",
                    StructType::new(vec![
                        NestedField::required(4, "data", PrimitiveType::String.into()).into(),
                        NestedField::required(3, "count", PrimitiveType::Long.into()).into(),
                    ])
                    .into(),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let actual = UpdateSchemaAction::new(schema, 4)
            .unwrap()
            .move_column(MoveColumn::after("struct.count", "struct.data"))
            .unwrap()
            .apply()
            .unwrap();
        assert_eq!(actual.as_ref(), &expected);
    }

    /*
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    required(3, "count", Types.LongType.get()),
                    required(4, "data", Types.StringType.get()),
                    optional(5, "ts", Types.TimestampType.withZone()))));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    required(3, "count", Types.LongType.get()),
                    optional(5, "ts", Types.TimestampType.withZone()),
                    required(4, "data", Types.StringType.get()))));

    Schema actual = new SchemaUpdate(schema, 5).moveAfter("struct.ts", "struct.count").apply();

    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
     */
    #[test]
    fn test_move_nested_field_after() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                    NestedField::required(
                        2,
                        "struct",
                        StructType::new(vec![
                            NestedField::required(3, "count", PrimitiveType::Long.into()).into(),
                            NestedField::required(4, "data", PrimitiveType::String.into()).into(),
                            NestedField::optional(5, "ts", PrimitiveType::Timestamp.into()).into(),
                        ])
                        .into(),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                NestedField::required(
                    2,
                    "struct",
                    StructType::new(vec![
                        NestedField::required(3, "count", PrimitiveType::Long.into()).into(),
                        NestedField::optional(5, "ts", PrimitiveType::Timestamp.into()).into(),
                        NestedField::required(4, "data", PrimitiveType::String.into()).into(),
                    ])
                    .into(),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let actual = UpdateSchemaAction::new(schema, 5)
            .unwrap()
            .move_column(MoveColumn::after("struct.ts", "struct.count"))
            .unwrap()
            .apply()
            .unwrap();
        assert_eq!(actual.as_ref(), &expected);
    }

    /*
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    optional(5, "ts", Types.TimestampType.withZone()),
                    required(3, "count", Types.LongType.get()),
                    required(4, "data", Types.StringType.get()))));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    required(3, "count", Types.LongType.get()),
                    optional(5, "ts", Types.TimestampType.withZone()),
                    required(4, "data", Types.StringType.get()))));

    Schema actual = new SchemaUpdate(schema, 5).moveBefore("struct.ts", "struct.data").apply();

    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
     */
    #[test]
    fn test_move_nested_field_before() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                    NestedField::required(
                        2,
                        "struct",
                        StructType::new(vec![
                            NestedField::optional(5, "ts", PrimitiveType::Timestamp.into()).into(),
                            NestedField::required(3, "count", PrimitiveType::Long.into()).into(),
                            NestedField::required(4, "data", PrimitiveType::String.into()).into(),
                        ])
                        .into(),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                NestedField::required(
                    2,
                    "struct",
                    StructType::new(vec![
                        NestedField::required(3, "count", PrimitiveType::Long.into()).into(),
                        NestedField::optional(5, "ts", PrimitiveType::Timestamp.into()).into(),
                        NestedField::required(4, "data", PrimitiveType::String.into()).into(),
                    ])
                    .into(),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let actual = UpdateSchemaAction::new(schema, 5)
            .unwrap()
            .move_column(MoveColumn::before("struct.ts", "struct.data"))
            .unwrap()
            .apply()
            .unwrap();
        assert_eq!(actual.as_ref(), &expected);
    }

    /*
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "list",
                Types.ListType.ofOptional(
                    6,
                    Types.StructType.of(
                        optional(5, "ts", Types.TimestampType.withZone()),
                        required(3, "count", Types.LongType.get()),
                        required(4, "data", Types.StringType.get())))));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "list",
                Types.ListType.ofOptional(
                    6,
                    Types.StructType.of(
                        required(3, "count", Types.LongType.get()),
                        optional(5, "ts", Types.TimestampType.withZone()),
                        required(4, "data", Types.StringType.get())))));

    Schema actual = new SchemaUpdate(schema, 6).moveBefore("list.ts", "list.data").apply();

    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
     */
    #[test]
    fn test_move_list_element_field() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                    NestedField::required(
                        2,
                        "list",
                        ListType::required(
                            6,
                            StructType::new(vec![
                                NestedField::optional(5, "ts", PrimitiveType::Timestamp.into())
                                    .into(),
                                NestedField::required(3, "count", PrimitiveType::Long.into())
                                    .into(),
                                NestedField::required(4, "data", PrimitiveType::String.into())
                                    .into(),
                            ])
                            .into(),
                        )
                        .into(),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                NestedField::required(
                    2,
                    "list",
                    ListType::required(
                        6,
                        StructType::new(vec![
                            NestedField::required(3, "count", PrimitiveType::Long.into()).into(),
                            NestedField::optional(5, "ts", PrimitiveType::Timestamp.into()).into(),
                            NestedField::required(4, "data", PrimitiveType::String.into()).into(),
                        ])
                        .into(),
                    )
                    .into(),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let actual = UpdateSchemaAction::new(schema, 6)
            .unwrap()
            .move_column(MoveColumn::before("list.ts", "list.data"))
            .unwrap()
            .apply()
            .unwrap();
        assert_eq!(actual.as_ref(), &expected);
    }

    #[test]
    /*
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "map",
                Types.MapType.ofOptional(
                    6,
                    7,
                    Types.StringType.get(),
                    Types.StructType.of(
                        optional(5, "ts", Types.TimestampType.withZone()),
                        required(3, "count", Types.LongType.get()),
                        required(4, "data", Types.StringType.get())))));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "map",
                Types.MapType.ofOptional(
                    6,
                    7,
                    Types.StringType.get(),
                    Types.StructType.of(
                        required(3, "count", Types.LongType.get()),
                        optional(5, "ts", Types.TimestampType.withZone()),
                        required(4, "data", Types.StringType.get())))));

    Schema actual = new SchemaUpdate(schema, 7).moveBefore("map.ts", "map.data").apply();

    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
     */
    fn test_move_map_value_struct_field() {
        let schema: Arc<_> = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                NestedField::required(
                    2,
                    "map",
                    MapType::optional(
                        6,
                        PrimitiveType::String.into(),
                        7,
                        StructType::new(vec![
                            NestedField::optional(5, "ts", PrimitiveType::Timestamp.into()).into(),
                            NestedField::required(3, "count", PrimitiveType::Long.into()).into(),
                            NestedField::required(4, "data", PrimitiveType::String.into()).into(),
                        ])
                        .into(),
                    )
                    .into(),
                )
                .into(),
            ])
            .build()
            .unwrap()
            .into();
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                NestedField::required(
                    2,
                    "map",
                    MapType::optional(
                        6,
                        PrimitiveType::String.into(),
                        7,
                        StructType::new(vec![
                            NestedField::required(3, "count", PrimitiveType::Long.into()).into(),
                            NestedField::optional(5, "ts", PrimitiveType::Timestamp.into()).into(),
                            NestedField::required(4, "data", PrimitiveType::String.into()).into(),
                        ])
                        .into(),
                    )
                    .into(),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let actual = UpdateSchemaAction::new(schema, 7)
            .unwrap()
            .move_column(MoveColumn::before("map.ts", "map.data"))
            .unwrap()
            .apply()
            .unwrap();
        assert_eq!(actual.as_ref(), &expected);
    }

    #[test]
    fn test_move_added_top_level_column() -> Result<()> {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                    NestedField::required(2, "data", PrimitiveType::String.into()).into(),
                ])
                .build()
                .unwrap(),
        );
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                NestedField::optional(3, "ts", PrimitiveType::Timestamptz.into()).into(),
                NestedField::required(2, "data", PrimitiveType::String.into()).into(),
            ])
            .build()
            .unwrap();
        let actual = UpdateSchemaAction::new(schema, 2)
            .unwrap()
            .add(
                AddColumn::builder()
                    .name("ts")
                    .r#type(PrimitiveType::Timestamptz.into())
                    .build(),
            )?
            .move_column(MoveColumn::after("ts", "id"))?
            .apply()?;
        assert_eq!(actual.as_struct(), expected.as_struct());
        Ok(())
    }

    #[test]
    fn test_move_added_top_level_column_after_added_column() -> Result<()> {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                    NestedField::required(2, "data", PrimitiveType::String.into()).into(),
                ])
                .build()
                .unwrap(),
        );
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                NestedField::optional(3, "ts", PrimitiveType::Timestamptz.into()).into(),
                NestedField::optional(4, "count", PrimitiveType::Long.into()).into(),
                NestedField::required(2, "data", PrimitiveType::String.into()).into(),
            ])
            .build()
            .unwrap();
        let actual = UpdateSchemaAction::new(schema, 2)
            .unwrap()
            .add(
                AddColumn::builder()
                    .name("ts")
                    .r#type(PrimitiveType::Timestamptz.into())
                    .build(),
            )?
            .add(
                AddColumn::builder()
                    .name("count")
                    .r#type(PrimitiveType::Long.into())
                    .build(),
            )?
            .move_column(MoveColumn::after("ts", "id"))?
            .move_column(MoveColumn::after("count", "ts"))?
            .apply()?;
        assert_eq!(actual.as_struct(), expected.as_struct());
        Ok(())
    }

    #[test]
    /*
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    required(3, "count", Types.LongType.get()),
                    required(4, "data", Types.StringType.get()))));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    optional(5, "ts", Types.TimestampType.withZone()),
                    required(3, "count", Types.LongType.get()),
                    required(4, "data", Types.StringType.get()))));

    Schema actual =
        new SchemaUpdate(schema, 4)
            .addColumn("struct", "ts", Types.TimestampType.withZone())
            .moveBefore("struct.ts", "struct.count")
            .apply();

    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
     */
    fn test_move_added_nested_struct_field() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                    NestedField::required(
                        2,
                        "struct",
                        StructType::new(vec![
                            NestedField::required(3, "count", PrimitiveType::Long.into()).into(),
                            NestedField::required(4, "data", PrimitiveType::String.into()).into(),
                        ])
                        .into(),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                NestedField::required(
                    2,
                    "struct",
                    StructType::new(vec![
                        NestedField::optional(5, "ts", PrimitiveType::Timestamp.into()).into(),
                        NestedField::required(3, "count", PrimitiveType::Long.into()).into(),
                        NestedField::required(4, "data", PrimitiveType::String.into()).into(),
                    ])
                    .into(),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let actual = UpdateSchemaAction::new(schema, 4)
            .unwrap()
            .add(
                AddColumn::builder()
                    .name("ts")
                    .r#type(PrimitiveType::Timestamp.into())
                    .parent(Some("struct".into()))
                    .build(),
            )
            .unwrap()
            .move_column(MoveColumn::before("struct.ts", "struct.count"))
            .unwrap()
            .apply()
            .unwrap();
        assert_eq!(actual.as_struct(), expected.as_struct());
    }

    /*
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    required(3, "count", Types.LongType.get()),
                    required(4, "data", Types.StringType.get()))));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    optional(6, "size", Types.LongType.get()),
                    optional(5, "ts", Types.TimestampType.withZone()),
                    required(3, "count", Types.LongType.get()),
                    required(4, "data", Types.StringType.get()))));

    Schema actual =
        new SchemaUpdate(schema, 4)
            .addColumn("struct", "ts", Types.TimestampType.withZone())
            .addColumn("struct", "size", Types.LongType.get())
            .moveBefore("struct.ts", "struct.count")
            .moveBefore("struct.size", "struct.ts")
            .apply();

    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
     */
    #[test]
    fn test_move_added_nested_struct_field_before_added_column() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                    NestedField::required(
                        2,
                        "struct",
                        StructType::new(vec![
                            NestedField::required(3, "count", PrimitiveType::Long.into()).into(),
                            NestedField::required(4, "data", PrimitiveType::String.into()).into(),
                        ])
                        .into(),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                NestedField::required(
                    2,
                    "struct",
                    StructType::new(vec![
                        NestedField::optional(6, "size", PrimitiveType::Long.into()).into(),
                        NestedField::optional(5, "ts", PrimitiveType::Timestamp.into()).into(),
                        NestedField::required(3, "count", PrimitiveType::Long.into()).into(),
                        NestedField::required(4, "data", PrimitiveType::String.into()).into(),
                    ])
                    .into(),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let actual = UpdateSchemaAction::new(schema, 4)
            .unwrap()
            .add(
                AddColumn::builder()
                    .name("ts")
                    .r#type(PrimitiveType::Timestamp.into())
                    .parent(Some("struct".into()))
                    .build(),
            )
            .unwrap()
            .add(
                AddColumn::builder()
                    .name("size")
                    .r#type(PrimitiveType::Long.into())
                    .parent(Some("struct".into()))
                    .build(),
            )
            .unwrap()
            .move_column(MoveColumn::before("struct.ts", "struct.count"))
            .unwrap()
            .move_column(MoveColumn::before("struct.size", "struct.ts"))
            .unwrap()
            .apply()
            .unwrap();
        assert_eq!(actual.as_struct(), expected.as_struct());
    }

    #[test]
    #[ignore = "not yet implemented: self-reference move check"]
    fn test_move_self_reference_fails() {}

    #[test]
    fn test_move_missing_column_fails() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                    NestedField::required(2, "data", PrimitiveType::String.into()).into(),
                ])
                .build()
                .unwrap(),
        );

        let err = UpdateSchemaAction::new(schema.clone(), 2)
            .unwrap()
            .move_column(MoveColumn::first("items"))
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(err.message(), "Cannot move missing column: items");

        let err = UpdateSchemaAction::new(schema.clone(), 2)
            .unwrap()
            .move_column(MoveColumn::before("items", "id"))
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(err.message(), "Cannot move missing column: items");

        let err = UpdateSchemaAction::new(schema, 2)
            .unwrap()
            .move_column(MoveColumn::after("items", "data"))
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(err.message(), "Cannot move missing column: items");
    }

    #[test]
    fn test_move_before_add_fails() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                    NestedField::required(2, "data", PrimitiveType::String.into()).into(),
                ])
                .build()
                .unwrap(),
        );

        let err = UpdateSchemaAction::new(schema.clone(), 2)
            .unwrap()
            .move_column(MoveColumn::first("ts"))
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(err.message(), "Cannot move missing column: ts");

        let err = UpdateSchemaAction::new(schema.clone(), 2)
            .unwrap()
            .move_column(MoveColumn::before("ts", "id"))
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(err.message(), "Cannot move missing column: ts");

        let err = UpdateSchemaAction::new(schema, 2)
            .unwrap()
            .move_column(MoveColumn::after("ts", "data"))
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(err.message(), "Cannot move missing column: ts");
    }

    #[test]
    fn test_move_missing_reference_column_fails() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                    NestedField::required(2, "data", PrimitiveType::String.into()).into(),
                ])
                .build()
                .unwrap(),
        );

        let err = UpdateSchemaAction::new(schema.clone(), 2)
            .unwrap()
            .move_column(MoveColumn::before("id", "items"))
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(
            err.message(),
            "Cannot move relative to missing column: items"
        );

        let err = UpdateSchemaAction::new(schema, 2)
            .unwrap()
            .move_column(MoveColumn::after("data", "items"))
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(
            err.message(),
            "Cannot move relative to missing column: items"
        );
    }

    /*
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "data", Types.StringType.get()),
            optional(
                3,
                "map",
                Types.MapType.ofRequired(4, 5, Types.StringType.get(), Types.StringType.get())));

    assertThatThrownBy(() -> new SchemaUpdate(schema, 5).moveBefore("map.key", "map.value").apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot move fields in non-struct type: map<string, string>");
     */
    #[test]
    fn test_move_primitive_map_key_fails() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                    NestedField::required(2, "data", PrimitiveType::String.into()).into(),
                    NestedField::optional(
                        3,
                        "map",
                        MapType::optional(
                            4,
                            PrimitiveType::String.into(),
                            5,
                            PrimitiveType::String.into(),
                        )
                        .into(),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let err = UpdateSchemaAction::new(schema, 5)
            .unwrap()
            .move_column(MoveColumn::before("map.key", "map.value"))
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(err.message(), "Cannot move fields in non-struct type: map");
    }

    /*
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "data", Types.StringType.get()),
            optional(
                3,
                "map",
                Types.MapType.ofRequired(4, 5, Types.StringType.get(), Types.StructType.of())));

    assertThatThrownBy(() -> new SchemaUpdate(schema, 5).moveBefore("map.value", "map.key").apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot move fields in non-struct type: map<string, struct<>>");
     */
    #[test]
    fn test_move_primitive_map_value_fails() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                    NestedField::required(2, "data", PrimitiveType::String.into()).into(),
                    NestedField::optional(
                        3,
                        "map",
                        MapType::optional(
                            4,
                            PrimitiveType::String.into(),
                            5,
                            StructType::new(vec![]).into(),
                        )
                        .into(),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let err = UpdateSchemaAction::new(schema, 5)
            .unwrap()
            .move_column(MoveColumn::before("map.value", "map.key"))
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(err.message(), "Cannot move fields in non-struct type: map");
    }

    /*
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "data", Types.StringType.get()),
            optional(3, "list", Types.ListType.ofRequired(4, Types.StringType.get())));

    assertThatThrownBy(() -> new SchemaUpdate(schema, 4).moveBefore("list.element", "list").apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot move fields in non-struct type: list<string>");
     */
    #[test]
    fn test_move_primitive_list_element_fails() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                    NestedField::required(2, "data", PrimitiveType::String.into()).into(),
                    NestedField::optional(
                        3,
                        "list",
                        ListType::optional(4, PrimitiveType::String.into()).into(),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let err = UpdateSchemaAction::new(schema, 4)
            .unwrap()
            .move_column(MoveColumn::before("list.element", "list"))
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(err.message(), "Cannot move fields in non-struct type: list");
    }

    /*
    Schema schema =
        new Schema(
            required(1, "a", Types.IntegerType.get()),
            required(2, "b", Types.IntegerType.get()),
            required(
                3,
                "struct",
                Types.StructType.of(
                    required(4, "x", Types.IntegerType.get()),
                    required(5, "y", Types.IntegerType.get()))));

    assertThatThrownBy(() -> new SchemaUpdate(schema, 5).moveBefore("a", "struct.x").apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot move field a to a different struct");
     */
    #[test]
    fn test_move_top_level_between_structs_fails() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "a", PrimitiveType::Int.into()).into(),
                    NestedField::required(2, "b", PrimitiveType::Int.into()).into(),
                    NestedField::required(
                        3,
                        "struct",
                        StructType::new(vec![
                            NestedField::required(4, "x", PrimitiveType::Int.into()).into(),
                            NestedField::required(5, "y", PrimitiveType::Int.into()).into(),
                        ])
                        .into(),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let err = UpdateSchemaAction::new(schema, 5)
            .unwrap()
            .move_column(MoveColumn::before("a", "struct.x"))
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(err.message(), "Cannot move field a to a different struct");
    }

    /*
    Schema schema =
        new Schema(
            required(
                1,
                "s1",
                Types.StructType.of(
                    required(3, "a", Types.IntegerType.get()),
                    required(4, "b", Types.IntegerType.get()))),
            required(
                2,
                "s2",
                Types.StructType.of(
                    required(5, "x", Types.IntegerType.get()),
                    required(6, "y", Types.IntegerType.get()))));

    assertThatThrownBy(() -> new SchemaUpdate(schema, 6).moveBefore("s2.x", "s1.a").apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot move field s2.x to a different struct");
     */
    #[test]
    fn test_move_between_structs_fails() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(
                        1,
                        "s1",
                        StructType::new(vec![
                            NestedField::required(3, "a", PrimitiveType::Int.into()).into(),
                            NestedField::required(4, "b", PrimitiveType::Int.into()).into(),
                        ])
                        .into(),
                    )
                    .into(),
                    NestedField::required(
                        2,
                        "s2",
                        StructType::new(vec![
                            NestedField::required(5, "x", PrimitiveType::Int.into()).into(),
                            NestedField::required(6, "y", PrimitiveType::Int.into()).into(),
                        ])
                        .into(),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let err = UpdateSchemaAction::new(schema, 6)
            .unwrap()
            .move_column(MoveColumn::before("s2.x", "s1.a"))
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(
            err.message(),
            "Cannot move field s2.x to a different struct"
        );
    }

    /*
    Schema newSchema =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID).setIdentifierFields("id").apply();

    assertThat(newSchema.identifierFieldIds())
        .as("add an existing field as identifier field should succeed")
        .containsExactly(newSchema.findField("id").fieldId());
     */
    #[test]
    fn test_add_existing_identifier_fields() {
        let schema: Arc<_> = SCHEMA.clone().into();
        let new_schema = UpdateSchemaAction::new(schema.clone(), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .set_identifier_fields(vec!["id".to_string()])
            .apply()
            .unwrap();
        assert_eq!(
            new_schema.identifier_field_ids().collect::<Vec<i32>>(),
            schema
                .clone()
                .field_by_name("id")
                .map(|f| f.id)
                .map(|id| vec![id])
                .unwrap()
        );
    }

    /*
    Schema newSchema =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
            .allowIncompatibleChanges()
            .addRequiredColumn("new_field", Types.StringType.get())
            .setIdentifierFields("id", "new_field")
            .apply();

    assertThat(newSchema.identifierFieldIds())
        .as("add column then set as identifier should succeed")
        .containsExactly(
            newSchema.findField("id").fieldId(), newSchema.findField("new_field").fieldId());

    newSchema =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
            .allowIncompatibleChanges()
            .setIdentifierFields("id", "new_field")
            .addRequiredColumn("new_field", Types.StringType.get())
            .apply();

    assertThat(newSchema.identifierFieldIds())
        .as("set identifier then add column should succeed")
        .containsExactly(
            newSchema.findField("id").fieldId(), newSchema.findField("new_field").fieldId());
     */
    #[test]
    fn test_add_new_identifier_field_columns() {
        let schema: Arc<_> = SCHEMA.clone().into();
        let id_field_id = schema.clone().field_by_name("id").unwrap().id;

        // Test: add column then set as identifier should succeed
        let new_schema = UpdateSchemaAction::new(schema.clone(), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .allow_incompatible_changes()
            .add(AddColumn::required(
                "new_field",
                PrimitiveType::String.into(),
            ))
            .unwrap()
            .set_identifier_fields(vec!["id".to_string(), "new_field".to_string()])
            .apply()
            .unwrap();

        let new_field_id = new_schema.field_by_name("new_field").unwrap().id;
        let identifier_ids: HashSet<i32> = new_schema.identifier_field_ids().collect();
        assert_eq!(
            identifier_ids,
            vec![id_field_id, new_field_id].into_iter().collect()
        );

        // Test: set identifier then add column should succeed
        let new_schema = UpdateSchemaAction::new(schema.clone(), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .allow_incompatible_changes()
            .set_identifier_fields(vec!["id".to_string(), "new_field".to_string()])
            .add(AddColumn::required(
                "new_field",
                PrimitiveType::String.into(),
            ))
            .unwrap()
            .apply()
            .unwrap();

        let new_field_id = new_schema.field_by_name("new_field").unwrap().id;
        let identifier_ids: HashSet<i32> = new_schema.identifier_field_ids().collect();
        assert_eq!(
            identifier_ids,
            vec![id_field_id, new_field_id].into_iter().collect()
        );
    }

    /*
    Schema newSchema =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
            .allowIncompatibleChanges()
            .addRequiredColumn(
                "required_struct",
                Types.StructType.of(
                    Types.NestedField.required(
                        SCHEMA_LAST_COLUMN_ID + 2, "field", Types.StringType.get())))
            .apply();

    newSchema =
        new SchemaUpdate(newSchema, SCHEMA_LAST_COLUMN_ID + 2)
            .setIdentifierFields("required_struct.field")
            .apply();

    assertThat(newSchema.identifierFieldIds())
        .as("set existing nested field as identifier should succeed")
        .containsExactly(newSchema.findField("required_struct.field").fieldId());

    newSchema =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
            .allowIncompatibleChanges()
            .addRequiredColumn(
                "new",
                Types.StructType.of(
                    Types.NestedField.required(
                        SCHEMA_LAST_COLUMN_ID + 2, "field", Types.StringType.get())))
            .setIdentifierFields("new.field")
            .apply();

    assertThat(newSchema.identifierFieldIds())
        .as("set newly added nested field as identifier should succeed")
        .containsExactly(newSchema.findField("new.field").fieldId());

    newSchema =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
            .allowIncompatibleChanges()
            .addRequiredColumn(
                "new",
                Types.StructType.of(
                    Types.NestedField.required(
                        SCHEMA_LAST_COLUMN_ID + 2,
                        "field",
                        Types.StructType.of(
                            Types.NestedField.required(
                                SCHEMA_LAST_COLUMN_ID + 3, "nested", Types.StringType.get())))))
            .setIdentifierFields("new.field.nested")
            .apply();

    assertThat(newSchema.identifierFieldIds())
        .as("set newly added multi-layer nested field as identifier should succeed")
        .containsExactly(newSchema.findField("new.field.nested").fieldId());
     */
    #[test]
    fn test_add_nested_identifier_field_columns() {
        let schema: Arc<_> = SCHEMA.clone().into();

        let new_schema = UpdateSchemaAction::new(schema.clone(), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .allow_incompatible_changes()
            .add(AddColumn::required(
                "required_struct",
                Type::Struct(StructType::new(vec![
                    NestedField::required(
                        SCHEMA_LAST_COLUMN_ID + 2,
                        "field",
                        PrimitiveType::String.into(),
                    )
                    .into(),
                ])),
            ))
            .unwrap()
            .apply()
            .unwrap();

        let new_schema = UpdateSchemaAction::new(new_schema.clone(), SCHEMA_LAST_COLUMN_ID + 2)
            .unwrap()
            .set_identifier_fields(vec!["required_struct.field".to_string()])
            .apply()
            .unwrap();

        let identifier_ids: Vec<i32> = new_schema.identifier_field_ids().collect();
        assert_eq!(identifier_ids, vec![
            new_schema
                .field_by_name("required_struct.field")
                .unwrap()
                .id
        ]);

        let new_schema = UpdateSchemaAction::new(schema.clone(), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .allow_incompatible_changes()
            .add(AddColumn::required(
                "new",
                Type::Struct(StructType::new(vec![
                    NestedField::required(
                        SCHEMA_LAST_COLUMN_ID + 2,
                        "field",
                        PrimitiveType::String.into(),
                    )
                    .into(),
                ])),
            ))
            .unwrap()
            .set_identifier_fields(vec!["new.field".to_string()])
            .apply()
            .unwrap();

        let new_field_id = new_schema.field_by_name("new.field").unwrap().id;
        let identifier_ids: Vec<i32> = new_schema.identifier_field_ids().collect();
        assert_eq!(identifier_ids, vec![new_field_id]);

        let new_schema = UpdateSchemaAction::new(schema.clone(), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .allow_incompatible_changes()
            .add(AddColumn::required(
                "new",
                Type::Struct(StructType::new(vec![
                    NestedField::required(
                        SCHEMA_LAST_COLUMN_ID + 2,
                        "field",
                        Type::Struct(StructType::new(vec![
                            NestedField::required(
                                SCHEMA_LAST_COLUMN_ID + 3,
                                "nested",
                                PrimitiveType::String.into(),
                            )
                            .into(),
                        ])),
                    )
                    .into(),
                ])),
            ))
            .unwrap()
            .set_identifier_fields(vec!["new.field.nested".to_string()])
            .apply()
            .unwrap();

        let nested_field_id = new_schema.field_by_name("new.field.nested").unwrap().id;
        let identifier_ids: Vec<i32> = new_schema.identifier_field_ids().collect();
        assert_eq!(identifier_ids, vec![nested_field_id]);
    }

    /*
    Schema newSchema =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
            .allowIncompatibleChanges()
            .addRequiredColumn(null, "dot.field", Types.StringType.get())
            .setIdentifierFields("id", "dot.field")
            .apply();

    assertThat(newSchema.identifierFieldIds())
        .as("add a field with dot as identifier should succeed")
        .containsExactly(
            newSchema.findField("id").fieldId(), newSchema.findField("dot.field").fieldId());
     */
    #[test]
    fn test_add_dotted_identifier_field_columns() {
        let schema: Arc<_> = SCHEMA.clone().into();

        let id_field_id = schema.field_by_name("id").unwrap().id;

        let new_schema = UpdateSchemaAction::new(schema.clone(), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .allow_incompatible_changes()
            .add(AddColumn::required("dot.field", PrimitiveType::String.into()).parent(None))
            .unwrap()
            .set_identifier_fields(vec!["id".to_string(), "dot.field".to_string()])
            .apply()
            .unwrap();

        let dot_field_id = new_schema.field_by_name("dot.field").unwrap().id;

        let identifier_ids: HashSet<i32> = new_schema.identifier_field_ids().collect();
        assert_eq!(
            identifier_ids,
            vec![id_field_id, dot_field_id].into_iter().collect()
        );
    }

    /*
    Schema newSchema =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
            .allowIncompatibleChanges()
            .addRequiredColumn("new_field", Types.StringType.get())
            .addRequiredColumn("new_field2", Types.StringType.get())
            .setIdentifierFields("id", "new_field", "new_field2")
            .apply();

    newSchema =
        new SchemaUpdate(newSchema, SCHEMA_LAST_COLUMN_ID)
            .setIdentifierFields("new_field", "new_field2")
            .apply();

    assertThat(newSchema.identifierFieldIds())
        .as("remove an identifier field should succeed")
        .containsExactly(
            newSchema.findField("new_field").fieldId(),
            newSchema.findField("new_field2").fieldId());

    newSchema =
        new SchemaUpdate(newSchema, SCHEMA_LAST_COLUMN_ID)
            .setIdentifierFields(Sets.newHashSet())
            .apply();

    assertThat(newSchema.identifierFieldIds()).isEmpty();
     */
    #[test]
    fn test_remove_identifier_fields() {
        let schema: Arc<_> = SCHEMA.clone().into();
        let new_schema = UpdateSchemaAction::new(schema.clone(), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .set_identifier_fields(vec!["id".to_string()])
            .apply()
            .unwrap();
        let new_schema = UpdateSchemaAction::new(new_schema.clone(), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .set_identifier_fields(vec![])
            .apply()
            .unwrap();
        assert!(
            new_schema
                .identifier_field_ids()
                .collect::<Vec<i32>>()
                .is_empty()
        );
    }

    /*
    Schema testSchema =
        new Schema(
            optional(1, "id", Types.IntegerType.get()),
            required(2, "float", Types.FloatType.get()),
            required(3, "double", Types.DoubleType.get()));

    assertThatThrownBy(() -> new Schema(testSchema.asStruct().fields(), ImmutableSet.of(999)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot add fieldId 999 as an identifier field: field does not exist");

    assertThatThrownBy(() -> new Schema(testSchema.asStruct().fields(), ImmutableSet.of(1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot add field id as an identifier field: not a required field");

    assertThatThrownBy(() -> new Schema(testSchema.asStruct().fields(), ImmutableSet.of(2)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot add field float as an identifier field: must not be float or double field");

    assertThatThrownBy(() -> new Schema(testSchema.asStruct().fields(), ImmutableSet.of(3)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot add field double as an identifier field: must not be float or double field");

    assertThatThrownBy(
            () ->
                new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
                    .setIdentifierFields("unknown")
                    .apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot add field unknown as an identifier field: not found in current schema or added columns");

    assertThatThrownBy(
            () ->
                new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
                    .setIdentifierFields("locations")
                    .apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot add field locations as an identifier field: not a primitive type field");

    assertThatThrownBy(
            () ->
                new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID).setIdentifierFields("data").apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot add field data as an identifier field: not a required field");

    assertThatThrownBy(
            () ->
                new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
                    .setIdentifierFields("locations.key.zip")
                    .apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            "Cannot add field zip as an identifier field: must not be nested in "
                + SCHEMA.findField("locations"));

    assertThatThrownBy(
            () ->
                new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
                    .setIdentifierFields("points.element.x")
                    .apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            "Cannot add field x as an identifier field: must not be nested in "
                + SCHEMA.findField("points"));

    Schema newSchema =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
            .allowIncompatibleChanges()
            .addRequiredColumn("col_float", Types.FloatType.get())
            .addRequiredColumn("col_double", Types.DoubleType.get())
            .addRequiredColumn(
                "new",
                Types.StructType.of(
                    Types.NestedField.required(
                        SCHEMA_LAST_COLUMN_ID + 4,
                        "fields",
                        Types.ListType.ofRequired(
                            SCHEMA_LAST_COLUMN_ID + 5,
                            Types.StructType.of(
                                Types.NestedField.required(
                                    SCHEMA_LAST_COLUMN_ID + 6,
                                    "nested",
                                    Types.StringType.get()))))))
            .addRequiredColumn(
                "new_map",
                Types.MapType.ofRequired(
                    SCHEMA_LAST_COLUMN_ID + 8,
                    SCHEMA_LAST_COLUMN_ID + 9,
                    Types.StructType.of(
                        required(SCHEMA_LAST_COLUMN_ID + 10, "key_col", Types.StringType.get())),
                    Types.StructType.of(
                        required(SCHEMA_LAST_COLUMN_ID + 11, "val_col", Types.StringType.get()))),
                "map of address to coordinate")
            .addRequiredColumn(
                "required_list",
                Types.ListType.ofRequired(
                    SCHEMA_LAST_COLUMN_ID + 13,
                    Types.StructType.of(
                        required(SCHEMA_LAST_COLUMN_ID + 14, "x", Types.LongType.get()),
                        required(SCHEMA_LAST_COLUMN_ID + 15, "y", Types.LongType.get()))))
            .apply();

    int lastColId = SCHEMA_LAST_COLUMN_ID + 15;

    assertThatThrownBy(
            () ->
                new SchemaUpdate(newSchema, lastColId)
                    .setIdentifierFields("required_list.element.x")
                    .apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            "Cannot add field x as an identifier field: must not be nested in "
                + newSchema.findField("required_list"));

    assertThatThrownBy(
            () -> new SchemaUpdate(newSchema, lastColId).setIdentifierFields("col_double").apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot add field col_double as an identifier field: must not be float or double field");

    assertThatThrownBy(
            () -> new SchemaUpdate(newSchema, lastColId).setIdentifierFields("col_float").apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot add field col_float as an identifier field: must not be float or double field");

    assertThatThrownBy(
            () ->
                new SchemaUpdate(newSchema, lastColId)
                    .setIdentifierFields("new_map.value.val_col")
                    .apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            "Cannot add field val_col as an identifier field: must not be nested in "
                + newSchema.findField("new_map"));

    assertThatThrownBy(
            () ->
                new SchemaUpdate(newSchema, lastColId)
                    .setIdentifierFields("new.fields.element.nested")
                    .apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            "Cannot add field nested as an identifier field: must not be nested in "
                + newSchema.findField("new.fields"));

    assertThatThrownBy(
            () ->
                new SchemaUpdate(newSchema, lastColId)
                    .setIdentifierFields("preferences.feature1")
                    .apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot add field feature1 as an identifier field: must not be nested in an optional field "
                + newSchema.findField("preferences"));
     */
    #[test]
    fn test_set_identifier_fields_fails() {
        let schema: Arc<_> = SCHEMA.clone().into();

        let new_schema = UpdateSchemaAction::new(schema.clone(), SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .allow_incompatible_changes()
            .add(AddColumn::required(
                "col_float",
                PrimitiveType::Float.into(),
            ))
            .unwrap()
            .add(AddColumn::required(
                "col_double",
                PrimitiveType::Double.into(),
            ))
            .unwrap()
            .add(AddColumn::required(
                "new",
                Type::Struct(StructType::new(vec![
                    NestedField::required(
                        SCHEMA_LAST_COLUMN_ID + 4,
                        "fields",
                        Type::List(ListType::required(
                            SCHEMA_LAST_COLUMN_ID + 5,
                            Type::Struct(StructType::new(vec![
                                NestedField::required(
                                    SCHEMA_LAST_COLUMN_ID + 6,
                                    "nested",
                                    PrimitiveType::String.into(),
                                )
                                .into(),
                            ])),
                        )),
                    )
                    .into(),
                ])),
            ))
            .unwrap()
            .add(AddColumn::required(
                "new_map",
                Type::Map(MapType::required(
                    SCHEMA_LAST_COLUMN_ID + 8,
                    PrimitiveType::String.into(),
                    SCHEMA_LAST_COLUMN_ID + 9,
                    Type::Struct(StructType::new(vec![
                        NestedField::required(
                            SCHEMA_LAST_COLUMN_ID + 11,
                            "val_col",
                            PrimitiveType::String.into(),
                        )
                        .into(),
                    ])),
                )),
            ))
            .unwrap()
            .add(AddColumn::required(
                "required_list",
                Type::List(ListType::required(
                    SCHEMA_LAST_COLUMN_ID + 13,
                    Type::Struct(StructType::new(vec![
                        NestedField::required(
                            SCHEMA_LAST_COLUMN_ID + 14,
                            "x",
                            PrimitiveType::Long.into(),
                        )
                        .into(),
                        NestedField::required(
                            SCHEMA_LAST_COLUMN_ID + 15,
                            "y",
                            PrimitiveType::Long.into(),
                        )
                        .into(),
                    ])),
                )),
            ))
            .unwrap()
            .apply()
            .unwrap();

        let last_col_id = SCHEMA_LAST_COLUMN_ID + 15;

        let err = UpdateSchemaAction::new(new_schema.clone(), last_col_id)
            .unwrap()
            .set_identifier_fields(vec!["required_list.element.x".to_string()])
            .apply()
            .unwrap_err();
        assert_eq!(
            err.message(),
            format!(
                "Cannot add field x as an identifier field: must not be nested in {:?}",
                new_schema.field_by_name("required_list").unwrap()
            )
        );

        let err = UpdateSchemaAction::new(new_schema.clone(), last_col_id)
            .unwrap()
            .set_identifier_fields(vec!["col_double".to_string()])
            .apply()
            .unwrap_err();
        assert_eq!(
            err.message(),
            "Cannot add identifier field col_double: cannot be a float or double type"
        );

        let err = UpdateSchemaAction::new(new_schema.clone(), last_col_id)
            .unwrap()
            .set_identifier_fields(vec!["col_float".to_string()])
            .apply()
            .unwrap_err();
        assert_eq!(
            err.message(),
            "Cannot add identifier field col_float: cannot be a float or double type"
        );

        let err = UpdateSchemaAction::new(new_schema.clone(), last_col_id)
            .unwrap()
            .set_identifier_fields(vec!["new_map.value.val_col".to_string()])
            .apply()
            .unwrap_err();
        assert_eq!(
            err.message(),
            format!(
                "Cannot add field val_col as an identifier field: must not be nested in {:?}",
                new_schema.field_by_name("new_map").unwrap()
            )
        );

        let err = UpdateSchemaAction::new(new_schema.clone(), last_col_id)
            .unwrap()
            .set_identifier_fields(vec!["new.fields.element.nested".to_string()])
            .apply()
            .unwrap_err();
        assert_eq!(
            err.message(),
            format!(
                "Cannot add field nested as an identifier field: must not be nested in {:?}",
                new_schema.field_by_name("new.fields").unwrap()
            )
        );

        let err = UpdateSchemaAction::new(new_schema, last_col_id)
            .unwrap()
            .set_identifier_fields(vec!["preferences.feature1".to_string()])
            .apply()
            .unwrap_err();
        assert_eq!(
            err.message(),
            format!(
                "Cannot add field feature1 as an identifier field: must not be nested in an optional field {}",
                schema.field_by_name("preferences").unwrap()
            )
        );
    }

    /*
    Schema schemaWithIdentifierFields =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID).setIdentifierFields("id").apply();

    assertThat(
            new SchemaUpdate(schemaWithIdentifierFields, SCHEMA_LAST_COLUMN_ID)
                .deleteColumn("id")
                .setIdentifierFields(Sets.newHashSet())
                .apply()
                .identifierFieldIds())
        .as("delete column and then reset identifier field should succeed")
        .isEmpty();

    assertThat(
            new SchemaUpdate(schemaWithIdentifierFields, SCHEMA_LAST_COLUMN_ID)
                .setIdentifierFields(Sets.newHashSet())
                .deleteColumn("id")
                .apply()
                .identifierFieldIds())
        .as("delete reset identifier field and then delete column should succeed")
        .isEmpty();
     */
    #[test]
    fn test_delete_identifier_field_columns() {
        let schema: Arc<_> = SCHEMA.clone().into();

        let schema_with_identifier_fields =
            UpdateSchemaAction::new(schema.clone(), SCHEMA_LAST_COLUMN_ID)
                .unwrap()
                .set_identifier_fields(vec!["id".to_string()])
                .apply()
                .unwrap();

        let err =
            UpdateSchemaAction::new(schema_with_identifier_fields.clone(), SCHEMA_LAST_COLUMN_ID)
                .unwrap()
                .delete(DeleteColumn::new("id"))
                .unwrap()
                .set_identifier_fields(vec![])
                .apply()
                .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(
            err.message(),
            "Cannot delete identifier field: id. To force deletion, also call setIdentifierFields to update identifier fields."
        );

        let err = UpdateSchemaAction::new(schema_with_identifier_fields, SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .set_identifier_fields(vec![])
            .delete(DeleteColumn::new("id"))
            .unwrap()
            .apply()
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(
            err.message(),
            "Cannot delete identifier field: id. To force deletion, also call setIdentifierFields to update identifier fields."
        );
    }

    /*
    Schema schemaWithIdentifierFields =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID).setIdentifierFields("id").apply();

    assertThatThrownBy(
            () ->
                new SchemaUpdate(schemaWithIdentifierFields, SCHEMA_LAST_COLUMN_ID)
                    .deleteColumn("id")
                    .apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot delete identifier field 1: id: required int. To force deletion, also call setIdentifierFields to update identifier fields.");
     */
    #[test]
    fn test_delete_identifier_field_columns_fails() {
        let schema: Arc<_> = SCHEMA.clone().into();
        let schema_with_identifier_fields =
            UpdateSchemaAction::new(schema.clone(), SCHEMA_LAST_COLUMN_ID)
                .unwrap()
                .set_identifier_fields(vec!["id".to_string()])
                .apply()
                .unwrap();

        let err = UpdateSchemaAction::new(schema_with_identifier_fields, SCHEMA_LAST_COLUMN_ID)
            .unwrap()
            .delete(DeleteColumn::new("id"))
            .unwrap()
            .apply()
            .unwrap_err();

        assert_eq!(
            err.message(),
            "Cannot delete identifier field: id. To force deletion, also call setIdentifierFields to update identifier fields."
        );
    }

    /*
    Schema newSchema =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
            .allowIncompatibleChanges()
            .addRequiredColumn(
                "out",
                Types.StructType.of(
                    Types.NestedField.required(
                        SCHEMA_LAST_COLUMN_ID + 2, "nested", Types.StringType.get())))
            .setIdentifierFields("out.nested")
            .apply();

    assertThatThrownBy(
            () ->
                new SchemaUpdate(newSchema, SCHEMA_LAST_COLUMN_ID + 2).deleteColumn("out").apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot delete field 24: out: required struct<25: nested: required string> "
                + "as it will delete nested identifier field 25: nested: required string");
     */
    #[test]
    fn test_delete_containing_nested_identifier_field_columns_fails() {
        let schema: Arc<_> = SCHEMA.clone().into();

        // Add a struct column with a nested identifier field
        let schema_with_nested_field =
            UpdateSchemaAction::new(schema.clone(), SCHEMA_LAST_COLUMN_ID)
                .unwrap()
                .allow_incompatible_changes()
                .add(AddColumn::required(
                    "out",
                    Type::Struct(StructType::new(vec![Arc::new(NestedField::required(
                        SCHEMA_LAST_COLUMN_ID + 2,
                        "nested",
                        Type::Primitive(PrimitiveType::String),
                    ))])),
                ))
                .unwrap()
                .set_identifier_fields(vec!["out.nested".to_string()])
                .apply()
                .unwrap();

        // Try to delete the struct column containing the nested identifier field
        let err = UpdateSchemaAction::new(schema_with_nested_field, SCHEMA_LAST_COLUMN_ID + 2)
            .unwrap()
            .delete(DeleteColumn::new("out"))
            .unwrap()
            .apply()
            .unwrap_err();

        assert!(err.message().contains("Cannot delete field"));
        assert!(
            err.message()
                .contains("as it will delete nested identifier field")
        );
    }

    /*
    Schema schemaWithIdentifierFields =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID).setIdentifierFields("id").apply();

    Schema newSchema =
        new SchemaUpdate(schemaWithIdentifierFields, SCHEMA_LAST_COLUMN_ID)
            .renameColumn("id", "id2")
            .apply();

    assertThat(newSchema.identifierFieldIds())
        .as("rename should not affect identifier fields")
        .containsExactly(SCHEMA.findField("id").fieldId());
     */
    #[test]
    fn test_rename_identifier_fields() {
        let schema: Arc<_> = SCHEMA.clone().into();
        let id_field_id = schema.field_by_name("id").unwrap().id;

        let schema_with_identifier_fields =
            UpdateSchemaAction::new(schema.clone(), SCHEMA_LAST_COLUMN_ID)
                .unwrap()
                .set_identifier_fields(vec!["id".to_string()])
                .apply()
                .unwrap();

        let new_schema =
            UpdateSchemaAction::new(schema_with_identifier_fields, SCHEMA_LAST_COLUMN_ID)
                .unwrap()
                .rename(RenameColumn::new("id", "id2"))
                .unwrap()
                .apply()
                .unwrap();

        assert_eq!(
            new_schema.identifier_field_ids().collect::<Vec<i32>>(),
            vec![id_field_id]
        );
    }

    /*
    Schema schemaWithIdentifierFields =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID).setIdentifierFields("id").apply();

    Schema newSchema =
        new SchemaUpdate(schemaWithIdentifierFields, SCHEMA_LAST_COLUMN_ID)
            .moveAfter("id", "locations")
            .apply();

    assertThat(newSchema.identifierFieldIds())
        .as("move after should not affect identifier fields")
        .containsExactly(SCHEMA.findField("id").fieldId());

    newSchema =
        new SchemaUpdate(schemaWithIdentifierFields, SCHEMA_LAST_COLUMN_ID)
            .moveBefore("id", "locations")
            .apply();

    assertThat(newSchema.identifierFieldIds())
        .as("move before should not affect identifier fields")
        .containsExactly(SCHEMA.findField("id").fieldId());

    newSchema =
        new SchemaUpdate(schemaWithIdentifierFields, SCHEMA_LAST_COLUMN_ID).moveFirst("id").apply();

    assertThat(newSchema.identifierFieldIds())
        .as("move first should not affect identifier fields")
        .containsExactly(SCHEMA.findField("id").fieldId())
     */
    #[test]
    fn test_move_identifier_fields() {
        let schema: Arc<_> = SCHEMA.clone().into();
        let schema_with_identifier_fields =
            UpdateSchemaAction::new(schema.clone(), SCHEMA_LAST_COLUMN_ID)
                .unwrap()
                .set_identifier_fields(vec!["id".to_string()])
                .apply()
                .unwrap();

        let new_schema =
            UpdateSchemaAction::new(schema_with_identifier_fields.clone(), SCHEMA_LAST_COLUMN_ID)
                .unwrap()
                .move_column(MoveColumn::after("id", "locations"))
                .unwrap()
                .apply()
                .unwrap();

        assert_eq!(
            new_schema.identifier_field_ids().collect::<Vec<i32>>(),
            schema
                .clone()
                .field_by_name("id")
                .map(|f| f.id)
                .map(|id| vec![id])
                .unwrap()
        );

        let new_schema =
            UpdateSchemaAction::new(schema_with_identifier_fields.clone(), SCHEMA_LAST_COLUMN_ID)
                .unwrap()
                .move_column(MoveColumn::before("id", "locations"))
                .unwrap()
                .apply()
                .unwrap();

        assert_eq!(
            new_schema.identifier_field_ids().collect::<Vec<i32>>(),
            schema
                .clone()
                .field_by_name("id")
                .map(|f| f.id)
                .map(|id| vec![id])
                .unwrap()
        );

        let new_schema =
            UpdateSchemaAction::new(schema_with_identifier_fields.clone(), SCHEMA_LAST_COLUMN_ID)
                .unwrap()
                .move_column(MoveColumn::first("id"))
                .unwrap()
                .apply()
                .unwrap();

        assert_eq!(
            new_schema.identifier_field_ids().collect::<Vec<i32>>(),
            schema
                .clone()
                .field_by_name("id")
                .map(|f| f.id)
                .map(|id| vec![id])
                .unwrap()
        );
    }

    /*
    Schema schemaWithIdentifierFields =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID).setIdentifierFields("id").apply();

    Schema newSchema =
        new SchemaUpdate(schemaWithIdentifierFields, SCHEMA_LAST_COLUMN_ID)
            .caseSensitive(false)
            .moveAfter("iD", "locations")
            .apply();

    assertThat(newSchema.identifierFieldIds())
        .as("move after should not affect identifier fields")
        .containsExactly(SCHEMA.findField("id").fieldId());

    newSchema =
        new SchemaUpdate(schemaWithIdentifierFields, SCHEMA_LAST_COLUMN_ID)
            .caseSensitive(false)
            .moveBefore("ID", "locations")
            .apply();

    assertThat(newSchema.identifierFieldIds())
        .as("move before should not affect identifier fields")
        .containsExactly(SCHEMA.findField("id").fieldId());

    newSchema =
        new SchemaUpdate(schemaWithIdentifierFields, SCHEMA_LAST_COLUMN_ID)
            .caseSensitive(false)
            .moveFirst("ID")
            .apply();

    assertThat(newSchema.identifierFieldIds())
        .as("move first should not affect identifier fields")
        .containsExactly(SCHEMA.findField("id").fieldId());
     */
    #[test]
    fn test_move_identifier_fields_case_insensitive() {
        let schema: Arc<_> = SCHEMA.clone().into();
        let schema_with_identifier_fields =
            UpdateSchemaAction::new(schema.clone(), SCHEMA_LAST_COLUMN_ID)
                .unwrap()
                .set_identifier_fields(vec!["id".to_string()])
                .apply()
                .unwrap();
        let new_schema =
            UpdateSchemaAction::new(schema_with_identifier_fields.clone(), SCHEMA_LAST_COLUMN_ID)
                .unwrap()
                .case_sensitive(false)
                .move_column(MoveColumn::after("iD", "locations"))
                .unwrap()
                .apply()
                .unwrap();
        assert_eq!(
            new_schema.identifier_field_ids().collect::<Vec<i32>>(),
            schema
                .clone()
                .field_by_name("id")
                .map(|f| f.id)
                .map(|id| vec![id])
                .unwrap()
        );

        let new_schema =
            UpdateSchemaAction::new(schema_with_identifier_fields.clone(), SCHEMA_LAST_COLUMN_ID)
                .unwrap()
                .case_sensitive(false)
                .move_column(MoveColumn::before("ID", "locations"))
                .unwrap()
                .apply()
                .unwrap();

        assert_eq!(
            new_schema.identifier_field_ids().collect::<Vec<i32>>(),
            schema
                .clone()
                .field_by_name("id")
                .map(|f| f.id)
                .map(|id| vec![id])
                .unwrap()
        );

        let new_schema =
            UpdateSchemaAction::new(schema_with_identifier_fields.clone(), SCHEMA_LAST_COLUMN_ID)
                .unwrap()
                .case_sensitive(false)
                .move_column(MoveColumn::first("ID"))
                .unwrap()
                .apply()
                .unwrap();

        assert_eq!(
            new_schema.identifier_field_ids().collect::<Vec<i32>>(),
            schema
                .clone()
                .field_by_name("id")
                .map(|f| f.id)
                .map(|id| vec![id])
                .unwrap()
        );
    }

    #[test]
    fn test_move_top_deleted_column_after_another_column() -> Result<()> {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                    NestedField::required(2, "data", PrimitiveType::String.into()).into(),
                    NestedField::required(3, "data_1", PrimitiveType::String.into()).into(),
                ])
                .build()
                .unwrap(),
        );
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(2, "data", PrimitiveType::String.into()).into(),
                NestedField::required(4, "id", PrimitiveType::Int.into()).into(),
                NestedField::required(3, "data_1", PrimitiveType::String.into()).into(),
            ])
            .build()
            .unwrap();
        let actual = UpdateSchemaAction::new(schema, 3)
            .unwrap()
            .allow_incompatible_changes()
            .delete(DeleteColumn::new("id"))?
            .add(
                AddColumn::builder()
                    .name("id")
                    .r#type(PrimitiveType::Int.into())
                    .is_optional(false)
                    .build(),
            )?
            .move_column(MoveColumn::after("id", "data"))?
            .apply()?;
        assert_eq!(actual.as_struct(), expected.as_struct());
        Ok(())
    }

    #[test]
    fn test_move_top_deleted_column_before_another_column() -> Result<()> {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                    NestedField::required(2, "data", PrimitiveType::String.into()).into(),
                    NestedField::required(3, "data_1", PrimitiveType::String.into()).into(),
                ])
                .build()
                .unwrap(),
        );
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(2, "data", PrimitiveType::String.into()).into(),
                NestedField::required(4, "id", PrimitiveType::Int.into()).into(),
                NestedField::required(3, "data_1", PrimitiveType::String.into()).into(),
            ])
            .build()
            .unwrap();
        let actual = UpdateSchemaAction::new(schema, 3)
            .unwrap()
            .allow_incompatible_changes()
            .delete(DeleteColumn::new("id"))?
            .add(
                AddColumn::builder()
                    .name("id")
                    .r#type(PrimitiveType::Int.into())
                    .is_optional(false)
                    .build(),
            )?
            .move_column(MoveColumn::before("id", "data_1"))?
            .apply()?;
        assert_eq!(actual.as_struct(), expected.as_struct());
        Ok(())
    }

    #[test]
    fn test_move_top_deleted_column_to_first() -> Result<()> {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                    NestedField::required(2, "data", PrimitiveType::String.into()).into(),
                    NestedField::required(3, "data_1", PrimitiveType::String.into()).into(),
                ])
                .build()
                .unwrap(),
        );
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(4, "id", PrimitiveType::Int.into()).into(),
                NestedField::required(2, "data", PrimitiveType::String.into()).into(),
                NestedField::required(3, "data_1", PrimitiveType::String.into()).into(),
            ])
            .build()
            .unwrap();
        let actual = UpdateSchemaAction::new(schema, 3)
            .unwrap()
            .allow_incompatible_changes()
            .delete(DeleteColumn::new("id"))?
            .add(
                AddColumn::builder()
                    .name("id")
                    .r#type(PrimitiveType::Int.into())
                    .is_optional(false)
                    .build(),
            )?
            .move_column(MoveColumn::first("id"))?
            .apply()?;
        assert_eq!(actual.as_struct(), expected.as_struct());
        Ok(())
    }

    /*
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    required(3, "count", Types.LongType.get()),
                    required(4, "data", Types.StringType.get()),
                    required(5, "data_1", Types.StringType.get()))));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    required(3, "count", Types.LongType.get()),
                    required(6, "data", Types.IntegerType.get()),
                    required(5, "data_1", Types.StringType.get()))));

    Schema actual =
        new SchemaUpdate(schema, 5)
            .allowIncompatibleChanges()
            .deleteColumn("struct.data")
            .addRequiredColumn("struct", "data", Types.IntegerType.get())
            .moveAfter("struct.data", "struct.count")
            .apply();

    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
     */
    #[test]
    fn test_move_deleted_nested_struct_field_after_another_column() {
        let schema: Arc<_> = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                NestedField::required(
                    2,
                    "struct",
                    StructType::new(vec![
                        NestedField::required(3, "count", PrimitiveType::Long.into()).into(),
                        NestedField::required(4, "data", PrimitiveType::String.into()).into(),
                        NestedField::required(5, "data_1", PrimitiveType::String.into()).into(),
                    ])
                    .into(),
                )
                .into(),
            ])
            .build()
            .unwrap()
            .into();
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                NestedField::required(
                    2,
                    "struct",
                    StructType::new(vec![
                        NestedField::required(3, "count", PrimitiveType::Long.into()).into(),
                        NestedField::required(6, "data", PrimitiveType::Int.into()).into(),
                        NestedField::required(5, "data_1", PrimitiveType::String.into()).into(),
                    ])
                    .into(),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let actual = UpdateSchemaAction::new(schema, 5)
            .unwrap()
            .allow_incompatible_changes()
            .delete(DeleteColumn::new("struct.data"))
            .unwrap()
            .add(
                AddColumn::builder()
                    .name("data")
                    .r#type(PrimitiveType::Int.into())
                    .parent(Some("struct".into()))
                    .is_optional(false)
                    .build(),
            )
            .unwrap()
            .move_column(MoveColumn::after("struct.data", "struct.count"))
            .unwrap()
            .apply()
            .unwrap();
        assert_eq!(actual.as_struct(), expected.as_struct());
    }

    /*
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    required(3, "count", Types.LongType.get()),
                    required(4, "data", Types.StringType.get()),
                    required(5, "data_1", Types.StringType.get()))));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    required(3, "count", Types.LongType.get()),
                    required(6, "data", Types.IntegerType.get()),
                    required(5, "data_1", Types.StringType.get()))));

    Schema actual =
        new SchemaUpdate(schema, 5)
            .allowIncompatibleChanges()
            .deleteColumn("struct.data")
            .addRequiredColumn("struct", "data", Types.IntegerType.get())
            .moveBefore("struct.data", "struct.data_1")
            .apply();

    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
     */
    #[test]
    fn test_move_deleted_nested_struct_field_before_another_column() {
        let schema: Arc<_> = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                NestedField::required(
                    2,
                    "struct",
                    StructType::new(vec![
                        NestedField::required(3, "count", PrimitiveType::Long.into()).into(),
                        NestedField::required(4, "data", PrimitiveType::String.into()).into(),
                        NestedField::required(5, "data_1", PrimitiveType::String.into()).into(),
                    ])
                    .into(),
                )
                .into(),
            ])
            .build()
            .unwrap()
            .into();
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                NestedField::required(
                    2,
                    "struct",
                    StructType::new(vec![
                        NestedField::required(3, "count", PrimitiveType::Long.into()).into(),
                        NestedField::required(6, "data", PrimitiveType::Int.into()).into(),
                        NestedField::required(5, "data_1", PrimitiveType::String.into()).into(),
                    ])
                    .into(),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let actual = UpdateSchemaAction::new(schema, 5)
            .unwrap()
            .allow_incompatible_changes()
            .delete(DeleteColumn::new("struct.data"))
            .unwrap()
            .add(
                AddColumn::builder()
                    .name("data")
                    .r#type(PrimitiveType::Int.into())
                    .parent(Some("struct".into()))
                    .is_optional(false)
                    .build(),
            )
            .unwrap()
            .move_column(MoveColumn::before("struct.data", "struct.data_1"))
            .unwrap()
            .apply()
            .unwrap();
        assert_eq!(actual.as_struct(), expected.as_struct());
    }

    /*
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    required(3, "count", Types.LongType.get()),
                    required(4, "data", Types.StringType.get()),
                    required(5, "data_1", Types.StringType.get()))));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    required(6, "data", Types.IntegerType.get()),
                    required(3, "count", Types.LongType.get()),
                    required(5, "data_1", Types.StringType.get()))));

    Schema actual =
        new SchemaUpdate(schema, 5)
            .allowIncompatibleChanges()
            .deleteColumn("struct.data")
            .addRequiredColumn("struct", "data", Types.IntegerType.get())
            .moveFirst("struct.data")
            .apply();

    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
     */
    #[test]
    fn test_move_deleted_nested_struct_field_to_first() {
        let schema: Arc<_> = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                NestedField::required(
                    2,
                    "struct",
                    StructType::new(vec![
                        NestedField::required(3, "count", PrimitiveType::Long.into()).into(),
                        NestedField::required(4, "data", PrimitiveType::String.into()).into(),
                        NestedField::required(5, "data_1", PrimitiveType::String.into()).into(),
                    ])
                    .into(),
                )
                .into(),
            ])
            .build()
            .unwrap()
            .into();
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                NestedField::required(
                    2,
                    "struct",
                    StructType::new(vec![
                        NestedField::required(6, "data", PrimitiveType::Int.into()).into(),
                        NestedField::required(3, "count", PrimitiveType::Long.into()).into(),
                        NestedField::required(5, "data_1", PrimitiveType::String.into()).into(),
                    ])
                    .into(),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let actual = UpdateSchemaAction::new(schema, 5)
            .unwrap()
            .allow_incompatible_changes()
            .delete(DeleteColumn::new("struct.data"))
            .unwrap()
            .add(
                AddColumn::builder()
                    .name("data")
                    .r#type(PrimitiveType::Int.into())
                    .parent(Some("struct".into()))
                    .is_optional(false)
                    .build(),
            )
            .unwrap()
            .move_column(MoveColumn::first("struct.data"))
            .unwrap()
            .apply()
            .unwrap();
        assert_eq!(actual.as_struct(), expected.as_struct());
    }

    #[test]
    #[ignore = "not yet implemented: PrimitiveType::Unknown not supported in iceberg-rust"]
    fn test_add_unknown() {}

    #[test]
    #[ignore = "not yet implemented: PrimitiveType::Unknown not supported in iceberg-rust"]
    fn test_add_unknown_non_null_default() {}

    #[test]
    #[ignore = "not yet implemented: PrimitiveType::Unknown not supported in iceberg-rust"]
    fn test_add_required_unknown() {}

    #[test]
    fn test_case_insensitive_add_top_level_and_move() -> Result<()> {
        let schema: Arc<_> = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
            ])
            .build()?
            .into();
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::optional(2, "data", PrimitiveType::String.into()).into(),
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
            ])
            .build()?;
        let actual = UpdateSchemaAction::new(schema, 1)?
            .case_sensitive(false)
            .add(
                AddColumn::builder()
                    .name("data")
                    .r#type(PrimitiveType::String.into())
                    .build(),
            )?
            .move_column(MoveColumn::first("dAtA"))?
            .apply()?;
        assert_eq!(actual.as_struct(), expected.as_struct());
        Ok(())
    }

    #[test]
    fn test_case_insensitive_add_nested_and_move() -> Result<()> {
        let schema: Arc<_> = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                NestedField::optional(
                    2,
                    "struct",
                    StructType::new(vec![
                        NestedField::required(3, "field1", PrimitiveType::String.into()).into(),
                    ])
                    .into(),
                )
                .into(),
            ])
            .build()?
            .into();
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                NestedField::optional(
                    2,
                    "struct",
                    StructType::new(vec![
                        NestedField::optional(4, "field2", PrimitiveType::Int.into()).into(),
                        NestedField::required(3, "field1", PrimitiveType::String.into()).into(),
                    ])
                    .into(),
                )
                .into(),
            ])
            .build()?;
        let actual = UpdateSchemaAction::new(schema, 3)?
            .case_sensitive(false)
            .add(
                AddColumn::builder()
                    .name("field2")
                    .r#type(PrimitiveType::Int.into())
                    .parent(Some("STRUCT".into()))
                    .build(),
            )?
            .move_column(MoveColumn::first("STRUCT.FIELD2"))?
            .apply()?;
        assert_eq!(actual.as_struct(), expected.as_struct());
        Ok(())
    }

    #[test]
    fn test_case_insensitive_move_after_newly_added_field() -> Result<()> {
        let schema: Arc<_> = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                NestedField::optional(
                    2,
                    "struct",
                    StructType::new(vec![
                        NestedField::required(3, "field1", PrimitiveType::String.into()).into(),
                    ])
                    .into(),
                )
                .into(),
            ])
            .build()?
            .into();
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                NestedField::optional(
                    2,
                    "struct",
                    StructType::new(vec![
                        NestedField::required(3, "field1", PrimitiveType::String.into()).into(),
                        NestedField::optional(4, "field2", PrimitiveType::Int.into()).into(),
                        NestedField::optional(5, "field3", PrimitiveType::Double.into()).into(),
                    ])
                    .into(),
                )
                .into(),
            ])
            .build()?;
        let actual = UpdateSchemaAction::new(schema, 3)?
            .case_sensitive(false)
            .add(
                AddColumn::builder()
                    .name("field2")
                    .r#type(PrimitiveType::Int.into())
                    .parent(Some("STRUCT".into()))
                    .build(),
            )?
            .add(
                AddColumn::builder()
                    .parent(Some("STRUCT".into()))
                    .name("field3")
                    .r#type(PrimitiveType::Double.into())
                    .build(),
            )?
            .move_column(MoveColumn::after("STRUCT.FIELD2", "STRUCT.FIELD1"))?
            .apply()?;
        assert_eq!(actual.as_struct(), expected.as_struct());
        Ok(())
    }
}
