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
    DEFAULT_SCHEMA_NAME_MAPPING, ListType, Literal, MapType, MappedField, NameMapping, NestedField,
    NestedFieldRef, PrimitiveLiteral, PrimitiveType, SCHEMA_NAME_DELIMITER, Schema, StructType,
    Type,
};
use crate::table::Table;
use crate::transaction::action::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, Result, TableRequirement, TableUpdate};

// Default ID for a new column. This will be re-assigned to a fresh ID at commit time.
const DEFAULT_FIELD_ID: i32 = 0;

const COLUMN_PROPERTY_PREFIXES: [&str; 3] = [
    "write.metadata.metrics.column.",
    "write.parquet.bloom-filter-enabled.column.",
    "write.parquet.stats-enabled.column.",
];

/// Declarative specification for adding a column in [`UpdateSchemaAction`].
///
/// Use helper constructors such as [`AddColumn::optional`] and [`AddColumn::required`],
/// optionally setting `parent` and `doc` through [`AddColumn::builder`], then pass the value to
/// [`UpdateSchemaAction::add_column`].
#[derive(TypedBuilder)]
pub struct AddColumn {
    #[builder(default = None, setter(strip_option, into))]
    parent: Option<String>,
    #[builder(setter(into))]
    name: String,
    #[builder(default = false)]
    required: bool,
    field_type: Type,
    #[builder(default = None, setter(strip_option, into))]
    doc: Option<String>,
    #[builder(default = None, setter(strip_option))]
    initial_default: Option<Literal>,
    #[builder(default = None, setter(strip_option))]
    write_default: Option<Literal>,
}

impl AddColumn {
    /// Create a root-level optional column specification.
    pub fn optional(name: impl ToString, field_type: Type) -> Self {
        Self::builder()
            .name(name.to_string())
            .field_type(field_type)
            .required(false)
            .build()
    }

    /// Create a root-level required column specification.
    pub fn required(name: impl ToString, field_type: Type, initial_default: Literal) -> Self {
        Self::builder()
            .name(name.to_string())
            .field_type(field_type)
            .required(true)
            .initial_default(initial_default.clone())
            .write_default(initial_default)
            .build()
    }

    fn to_nested_field(&self) -> NestedFieldRef {
        let mut field = NestedField::new(
            DEFAULT_FIELD_ID,
            self.name.clone(),
            self.field_type.clone(),
            self.required,
        );

        field.doc = self.doc.clone();
        field.initial_default = self.initial_default.clone();
        field.write_default = self.write_default.clone();
        Arc::new(field)
    }
}

/// Schema evolution API modeled after Apache Iceberg Java's `SchemaUpdate` implementation.
///
/// Operations are replayed against the latest table metadata on every transaction commit attempt.
/// This keeps field-id assignment and validation correct when the transaction is retried after a
/// concurrent commit.
///
/// # Example
///
/// ```ignore
/// let tx = Transaction::new(&table);
/// let action = tx.update_schema()
///     .add_column(AddColumn::optional("new_col", Type::Primitive(PrimitiveType::Int)))
///     .add_column(
///         AddColumn::builder()
///             .parent("person")
///             .name("email")
///             .field_type(Type::Primitive(PrimitiveType::String))
///             .build()
///     )
///     .rename_column("old_col", "legacy_col")
///     .move_column_first("new_col");
/// let tx = action.apply(tx).unwrap();
/// let table = tx.commit(&catalog).await.unwrap();
/// ```
pub struct UpdateSchemaAction {
    operations: Vec<SchemaOperation>,
    allow_incompatible_changes: bool,
    case_sensitive: bool,
    identifier_field_names: Option<HashSet<String>>,
}

enum SchemaOperation {
    Add(AddColumn),
    Delete(String),
    Rename {
        name: String,
        new_name: String,
    },
    SetRequired {
        name: String,
        required: bool,
    },
    UpdateType {
        name: String,
        new_type: PrimitiveType,
    },
    UpdateDoc {
        name: String,
        doc: Option<String>,
    },
    UpdateDefault {
        name: String,
        default: Option<Literal>,
    },
    Move {
        name: String,
        position: MovePosition,
    },
    UnionByName(Schema),
}

enum MovePosition {
    First,
    Before(String),
    After(String),
}

impl UpdateSchemaAction {
    /// Creates a new empty `UpdateSchemaAction`.
    pub(crate) fn new() -> Self {
        Self {
            operations: Vec::new(),
            allow_incompatible_changes: false,
            case_sensitive: true,
            identifier_field_names: None,
        }
    }

    // --- Root-level additions ---

    /// Add a column to the table schema.
    ///
    /// To add a root-level column, leave `AddColumn::parent` as `None`.
    /// For nested additions, set `parent` with [`AddColumn::builder`].
    /// If the parent resolves to a map/list, the column is added to map value/list element.
    pub fn add_column(mut self, add_column: AddColumn) -> Self {
        self.operations.push(SchemaOperation::Add(add_column));
        self
    }

    // --- Other builder methods ---

    /// Record a column deletion by name.
    ///
    /// At commit time, the column must exist in the current schema.
    pub fn delete_column(mut self, name: impl ToString) -> Self {
        self.operations
            .push(SchemaOperation::Delete(name.to_string()));
        self
    }

    /// Rename a column while preserving its field ID and all other metadata.
    ///
    /// `new_name` is an unqualified leaf name. Nested columns are selected using a dotted `name`.
    pub fn rename_column(mut self, name: impl ToString, new_name: impl ToString) -> Self {
        self.operations.push(SchemaOperation::Rename {
            name: name.to_string(),
            new_name: new_name.to_string(),
        });
        self
    }

    /// Change an optional column to required.
    ///
    /// This is rejected unless the column was added with an initial default or
    /// [`allow_incompatible_changes`](Self::allow_incompatible_changes) is enabled.
    pub fn require_column(mut self, name: impl ToString) -> Self {
        self.operations.push(SchemaOperation::SetRequired {
            name: name.to_string(),
            required: true,
        });
        self
    }

    /// Change a required column to optional.
    pub fn make_column_optional(mut self, name: impl ToString) -> Self {
        self.operations.push(SchemaOperation::SetRequired {
            name: name.to_string(),
            required: false,
        });
        self
    }

    /// Promote a primitive column to `new_type`.
    ///
    /// Iceberg permits `int` to `long`, `float` to `double`, and decimal precision widening at
    /// an unchanged scale.
    pub fn update_column_type(mut self, name: impl ToString, new_type: PrimitiveType) -> Self {
        self.operations.push(SchemaOperation::UpdateType {
            name: name.to_string(),
            new_type,
        });
        self
    }

    /// Set or clear a column's documentation string.
    pub fn update_column_doc(mut self, name: impl ToString, doc: Option<String>) -> Self {
        self.operations.push(SchemaOperation::UpdateDoc {
            name: name.to_string(),
            doc,
        });
        self
    }

    /// Set or clear a column's write default.
    ///
    /// Updating a default does not change the initial default used for rows written before the
    /// column was added.
    pub fn update_column_default(mut self, name: impl ToString, default: Option<Literal>) -> Self {
        self.operations.push(SchemaOperation::UpdateDefault {
            name: name.to_string(),
            default,
        });
        self
    }

    /// Move a column to the first position in its containing struct.
    pub fn move_column_first(mut self, name: impl ToString) -> Self {
        self.operations.push(SchemaOperation::Move {
            name: name.to_string(),
            position: MovePosition::First,
        });
        self
    }

    /// Move a column immediately before a sibling column.
    pub fn move_column_before(mut self, name: impl ToString, before_name: impl ToString) -> Self {
        self.operations.push(SchemaOperation::Move {
            name: name.to_string(),
            position: MovePosition::Before(before_name.to_string()),
        });
        self
    }

    /// Move a column immediately after a sibling column.
    pub fn move_column_after(mut self, name: impl ToString, after_name: impl ToString) -> Self {
        self.operations.push(SchemaOperation::Move {
            name: name.to_string(),
            position: MovePosition::After(after_name.to_string()),
        });
        self
    }

    /// Add and safely evolve columns to form a union with `new_schema`, matched by name.
    pub fn union_by_name(mut self, new_schema: Schema) -> Self {
        self.operations
            .push(SchemaOperation::UnionByName(new_schema));
        self
    }

    /// Replace the table's identifier fields with the supplied column names.
    pub fn set_identifier_fields<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: ToString,
    {
        self.identifier_field_names =
            Some(names.into_iter().map(|name| name.to_string()).collect());
        self
    }

    /// Resolve column names without regard to case when `case_sensitive` is false.
    pub fn case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
        self
    }

    /// Permit schema changes that may be incompatible with older data files.
    pub fn allow_incompatible_changes(mut self) -> Self {
        self.allow_incompatible_changes = true;
        self
    }
}

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
fn assign_fresh_ids(field: &NestedField, next_id: &mut i32) -> NestedFieldRef {
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

struct PendingSchemaUpdate<'a> {
    schema: &'a Schema,
    updates: HashMap<i32, NestedFieldRef>,
    deletes: HashSet<i32>,
    additions: HashMap<Option<i32>, Vec<i32>>,
    moves: HashMap<Option<i32>, Vec<PendingMove>>,
    id_to_parent: HashMap<i32, i32>,
    added_name_to_id: HashMap<String, i32>,
    last_column_id: i32,
    allow_incompatible_changes: bool,
    case_sensitive: bool,
    identifier_field_names: Option<HashSet<String>>,
}

impl<'a> PendingSchemaUpdate<'a> {
    fn new(
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

    fn apply_operations(&mut self, operations: &[SchemaOperation]) -> Result<()> {
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

    fn apply(&self) -> Result<Schema> {
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

fn update_name_mapping(table: &Table, pending: &PendingSchemaUpdate<'_>) -> Option<TableUpdate> {
    let raw_mapping = table
        .metadata()
        .properties()
        .get(DEFAULT_SCHEMA_NAME_MAPPING)?;
    let mapping: NameMapping = match serde_json::from_str(raw_mapping) {
        Ok(mapping) => mapping,
        Err(err) => {
            tracing::warn!(
                error = %err,
                "Failed to update external schema name mapping"
            );
            return None;
        }
    };
    let fields = update_mapped_fields(mapping.fields(), None, pending);
    let serialized = match serde_json::to_string(&NameMapping::new(fields)) {
        Ok(serialized) => serialized,
        Err(err) => {
            tracing::warn!(
                error = %err,
                "Failed to serialize updated external schema name mapping"
            );
            return None;
        }
    };
    Some(TableUpdate::SetProperties {
        updates: HashMap::from([(DEFAULT_SCHEMA_NAME_MAPPING.to_string(), serialized)]),
    })
}

fn update_column_properties(
    table: &Table,
    pending: &PendingSchemaUpdate<'_>,
    schema: &Schema,
) -> Vec<TableUpdate> {
    let deleted_columns: HashSet<String> = pending
        .deletes
        .iter()
        .filter_map(|id| pending.schema.name_by_field_id(*id).map(str::to_string))
        .collect();
    let added_ids: HashSet<i32> = pending.added_name_to_id.values().copied().collect();
    let renamed_columns: HashMap<String, String> = pending
        .updates
        .keys()
        .filter(|id| !added_ids.contains(id))
        .filter_map(|id| {
            let old_name = pending.schema.name_by_field_id(*id)?;
            let new_name = schema.name_by_field_id(*id)?;
            (old_name != new_name).then(|| (old_name.to_string(), new_name.to_string()))
        })
        .collect();

    if deleted_columns.is_empty() && renamed_columns.is_empty() {
        return Vec::new();
    }

    let mut removals = Vec::new();
    let mut updates = HashMap::new();
    for (key, value) in table.metadata().properties() {
        let Some(prefix) = COLUMN_PROPERTY_PREFIXES
            .iter()
            .find(|prefix| key.starts_with(**prefix))
        else {
            continue;
        };
        let column = &key[prefix.len()..];
        if let Some(new_name) = renamed_columns.get(column) {
            removals.push(key.clone());
            updates.insert(format!("{prefix}{new_name}"), value.clone());
        } else if deleted_columns.contains(column) {
            removals.push(key.clone());
        }
    }

    removals.sort();
    let mut property_updates = Vec::with_capacity(2);
    if !removals.is_empty() {
        property_updates.push(TableUpdate::RemoveProperties { removals });
    }
    if !updates.is_empty() {
        property_updates.push(TableUpdate::SetProperties { updates });
    }
    property_updates
}

fn update_mapped_fields(
    fields: &[MappedField],
    parent_id: Option<i32>,
    pending: &PendingSchemaUpdate<'_>,
) -> Vec<MappedField> {
    let mut updated: Vec<MappedField> = fields
        .iter()
        .map(|field| update_mapped_field(field, pending))
        .collect();
    if let Some(added_ids) = pending.additions.get(&parent_id) {
        updated.extend(
            added_ids
                .iter()
                .filter_map(|id| pending.updates.get(id))
                .map(mapped_field_from_nested),
        );
    }

    let assignments: HashMap<String, i32> = updated
        .iter()
        .filter_map(|field| {
            let id = field.field_id()?;
            let update = pending.updates.get(&id)?;
            Some((update.name.clone(), id))
        })
        .collect();
    updated
        .into_iter()
        .map(|field| {
            let names = field
                .names()
                .iter()
                .filter(|name| {
                    assignments
                        .get(*name)
                        .is_none_or(|assigned_id| Some(*assigned_id) == field.field_id())
                })
                .cloned()
                .collect();
            MappedField::new(
                field.field_id(),
                names,
                field
                    .fields()
                    .iter()
                    .map(|child| (**child).clone())
                    .collect(),
            )
        })
        .collect()
}

fn update_mapped_field(field: &MappedField, pending: &PendingSchemaUpdate<'_>) -> MappedField {
    let mut names = field.names().to_vec();
    if let Some(id) = field.field_id()
        && let Some(update) = pending.updates.get(&id)
        && !names.contains(&update.name)
    {
        names.push(update.name.clone());
    }
    let existing_children: Vec<MappedField> = field
        .fields()
        .iter()
        .map(|child| (**child).clone())
        .collect();
    let children = update_mapped_fields(&existing_children, field.field_id(), pending);
    MappedField::new(field.field_id(), names, children)
}

fn mapped_field_from_nested(field: &NestedFieldRef) -> MappedField {
    MappedField::new(
        Some(field.id),
        vec![field.name.clone()],
        mapped_fields_from_type(&field.field_type),
    )
}

fn mapped_fields_from_type(field_type: &Type) -> Vec<MappedField> {
    match field_type {
        Type::Struct(struct_type) => struct_type
            .fields()
            .iter()
            .map(mapped_field_from_nested)
            .collect(),
        Type::List(list_type) => vec![mapped_field_from_nested(&list_type.element_field)],
        Type::Map(map_type) => vec![
            mapped_field_from_nested(&map_type.key_field),
            mapped_field_from_nested(&map_type.value_field),
        ],
        Type::Primitive(_) | Type::Variant(_) => Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// TransactionAction implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl TransactionAction for UpdateSchemaAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let base_schema = table.metadata().current_schema();
        let last_column_id = table.metadata().last_column_id();
        let mut pending = PendingSchemaUpdate::new(
            base_schema,
            last_column_id,
            self.allow_incompatible_changes,
            self.case_sensitive,
            self.identifier_field_names.clone(),
        );
        pending.apply_operations(&self.operations)?;
        let schema = pending.apply()?;
        let mapping_update = update_name_mapping(table, &pending);
        let column_property_updates = update_column_properties(table, &pending, &schema);

        let mut updates = vec![
            TableUpdate::AddSchema { schema },
            TableUpdate::SetCurrentSchema { schema_id: -1 },
        ];
        if let Some(mapping_update) = mapping_update {
            updates.push(mapping_update);
        }
        updates.extend(column_property_updates);

        let requirements = vec![
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: base_schema.schema_id(),
            },
            TableRequirement::LastAssignedFieldIdMatch {
                last_assigned_field_id: last_column_id,
            },
        ];

        Ok(ActionCommit::new(updates, requirements))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::BufReader;
    use std::sync::Arc;

    use as_any::Downcast;

    use crate::spec::{
        DEFAULT_SCHEMA_ID, DEFAULT_SCHEMA_NAME_MAPPING, Literal, NameMapping, NestedField,
        PrimitiveType, Schema, Struct, StructType, TableMetadata, Type, VariantType,
    };
    use crate::table::Table;
    use crate::transaction::Transaction;
    use crate::transaction::action::{ApplyTransactionAction, TransactionAction};
    use crate::transaction::tests::make_v2_table;
    use crate::transaction::update_schema::{AddColumn, DEFAULT_FIELD_ID, UpdateSchemaAction};
    use crate::{Error, ErrorKind, TableIdent, TableRequirement, TableUpdate};

    // The V2 test table has:
    //   last_column_id: 3
    //   current schema (id=1): x(1, req, long), y(2, req, long), z(3, req, long)
    //   identifier_field_ids: [1, 2]

    /// Build a V2 test table that includes nested types:
    ///
    ///   last_column_id: 14
    ///   current schema (id=0):
    ///     x(1, req, long)           -- identifier
    ///     y(2, req, long)           -- identifier
    ///     z(3, req, long)
    ///     person(4, opt, struct)
    ///       name(5, opt, string)
    ///       age(6, req, int)
    ///     tags(7, opt, list<struct>)
    ///       element(8, req, struct)
    ///         key(9, opt, string)
    ///         value(10, opt, string)
    ///     props(11, opt, map<string, struct>)
    ///       key(12, req, string)
    ///       value(13, req, struct)
    ///         data(14, opt, string)
    fn make_v2_table_with_nested() -> Table {
        let json = r#"{
            "format-version": 2,
            "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c2",
            "location": "s3://bucket/test/location",
            "last-sequence-number": 0,
            "last-updated-ms": 1602638573590,
            "last-column-id": 14,
            "current-schema-id": 0,
            "schemas": [
                {
                    "type": "struct",
                    "schema-id": 0,
                    "identifier-field-ids": [1, 2],
                    "fields": [
                        {"id": 1, "name": "x", "required": true, "type": "long"},
                        {"id": 2, "name": "y", "required": true, "type": "long"},
                        {"id": 3, "name": "z", "required": true, "type": "long"},
                        {"id": 4, "name": "person", "required": false, "type": {
                            "type": "struct",
                            "fields": [
                                {"id": 5, "name": "name", "required": false, "type": "string"},
                                {"id": 6, "name": "age", "required": true, "type": "int"}
                            ]
                        }},
                        {"id": 7, "name": "tags", "required": false, "type": {
                            "type": "list",
                            "element-id": 8,
                            "element": {
                                "type": "struct",
                                "fields": [
                                    {"id": 9, "name": "key", "required": false, "type": "string"},
                                    {"id": 10, "name": "value", "required": false, "type": "string"}
                                ]
                            },
                            "element-required": true
                        }},
                        {"id": 11, "name": "props", "required": false, "type": {
                            "type": "map",
                            "key-id": 12,
                            "key": "string",
                            "value-id": 13,
                            "value": {
                                "type": "struct",
                                "fields": [
                                    {"id": 14, "name": "data", "required": false, "type": "string"}
                                ]
                            },
                            "value-required": true
                        }}
                    ]
                }
            ],
            "default-spec-id": 0,
            "partition-specs": [
                {"spec-id": 0, "fields": []}
            ],
            "last-partition-id": 999,
            "default-sort-order-id": 0,
            "sort-orders": [
                {"order-id": 0, "fields": []}
            ],
            "properties": {},
            "current-snapshot-id": -1,
            "snapshots": []
        }"#;

        let reader = BufReader::new(json.as_bytes());
        let metadata = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(metadata)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(crate::io::FileIO::new_with_memory())
            .runtime(crate::test_utils::test_runtime())
            .build()
            .unwrap()
    }

    async fn apply_schema(table: &Table, action: UpdateSchemaAction) -> Schema {
        let mut commit = Arc::new(action).commit(table).await.unwrap();
        match commit.take_updates().remove(0) {
            TableUpdate::AddSchema { schema } => schema,
            update => panic!("expected AddSchema, got {update:?}"),
        }
    }

    async fn commit_error(table: &Table, action: UpdateSchemaAction) -> Error {
        match Arc::new(action).commit(table).await {
            Ok(_) => panic!("expected schema update to fail"),
            Err(error) => error,
        }
    }

    // -----------------------------------------------------------------------
    // Existing root-level tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_assign_fresh_ids_variant() {
        // Variant carries no sub-fields, so fresh-id assignment only renames the field
        // itself and leaves the type untouched.
        let mut next_id = 10;
        let field = NestedField::optional(1, "data", Type::Variant(VariantType));
        let assigned = super::assign_fresh_ids(&field, &mut next_id);

        assert_eq!(assigned.id, 11);
        assert_eq!(*assigned.field_type, Type::Variant(VariantType));
        assert_eq!(next_id, 11);
    }

    #[tokio::test]
    async fn test_add_column() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        let action = tx.update_schema().add_column(AddColumn::optional(
            "new_col",
            Type::Primitive(PrimitiveType::Int),
        ));

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        assert_eq!(updates.len(), 2);

        // Extract the new schema from the AddSchema update.
        let new_schema = match &updates[0] {
            TableUpdate::AddSchema { schema } => schema,
            other => panic!("expected AddSchema, got {other:?}"),
        };

        let expected_schema = table
            .metadata()
            .current_schema()
            .as_ref()
            .clone()
            .into_builder()
            .with_schema_id(DEFAULT_SCHEMA_ID)
            .with_fields([
                NestedField::optional(4, "new_col", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();
        assert_eq!(new_schema, &expected_schema);

        assert_eq!(updates[1], TableUpdate::SetCurrentSchema { schema_id: -1 });

        // Verify requirement.
        assert_eq!(requirements.len(), 2);
        assert_eq!(requirements[0], TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: table.metadata().current_schema().schema_id()
        });
        assert_eq!(
            requirements[1],
            TableRequirement::LastAssignedFieldIdMatch {
                last_assigned_field_id: table.metadata().last_column_id()
            }
        );
    }

    #[tokio::test]
    async fn test_commit_replays_operations_against_latest_metadata() {
        let table = make_v2_table();
        let action = Arc::new(Transaction::new(&table).update_schema().add_column(
            AddColumn::optional("added", Type::Primitive(PrimitiveType::String)),
        ));

        let mut initial_commit = action.clone().commit(&table).await.unwrap();
        let initial_schema = match initial_commit.take_updates().remove(0) {
            TableUpdate::AddSchema { schema } => schema,
            update => panic!("expected AddSchema, got {update:?}"),
        };
        assert_eq!(initial_schema.field_by_name("added").unwrap().id, 4);

        let mut concurrent_fields = table
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .to_vec();
        concurrent_fields.push(
            NestedField::optional(4, "concurrent", Type::Primitive(PrimitiveType::Boolean)).into(),
        );
        let concurrent_schema = Schema::builder()
            .with_fields(concurrent_fields)
            .with_identifier_field_ids(table.metadata().current_schema().identifier_field_ids())
            .build()
            .unwrap();
        let refreshed_metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .add_schema(concurrent_schema)
            .unwrap()
            .set_current_schema(-1)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let refreshed = table.with_metadata(Arc::new(refreshed_metadata));

        let mut replayed_commit = action.commit(&refreshed).await.unwrap();
        let replayed_schema = match replayed_commit.take_updates().remove(0) {
            TableUpdate::AddSchema { schema } => schema,
            update => panic!("expected AddSchema, got {update:?}"),
        };
        assert_eq!(replayed_schema.field_by_name("concurrent").unwrap().id, 4);
        assert_eq!(replayed_schema.field_by_name("added").unwrap().id, 5);
        assert_eq!(replayed_commit.take_requirements(), vec![
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: refreshed.metadata().current_schema_id(),
            },
            TableRequirement::LastAssignedFieldIdMatch {
                last_assigned_field_id: 4,
            },
        ]);
    }

    #[tokio::test]
    async fn test_add_column_with_doc() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        let action = tx.update_schema().add_column(
            AddColumn::builder()
                .name("documented_col")
                .field_type(Type::Primitive(PrimitiveType::String))
                .doc("A documented column")
                .build(),
        );

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_schema = match &updates[0] {
            TableUpdate::AddSchema { schema } => schema,
            other => panic!("expected AddSchema, got {other:?}"),
        };

        let field = new_schema
            .field_by_name("documented_col")
            .expect("documented_col should exist");
        assert_eq!(field.id, 4);
        assert!(!field.required);
        assert_eq!(field.doc.as_deref(), Some("A documented column"));
    }

    #[tokio::test]
    async fn test_add_required_column_with_initial_default() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        let action = tx.update_schema().add_column(AddColumn::required(
            "req_col",
            Type::Primitive(PrimitiveType::Int),
            Literal::int(0),
        ));

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_schema = match &updates[0] {
            TableUpdate::AddSchema { schema } => schema,
            other => panic!("expected AddSchema, got {other:?}"),
        };

        let field = new_schema
            .field_by_name("req_col")
            .expect("req_col should exist");
        assert_eq!(field.id, 4);
        assert!(field.required);
        assert_eq!(field.initial_default, Some(Literal::int(0)));
        assert_eq!(field.write_default, Some(Literal::int(0)));
    }

    #[tokio::test]
    async fn test_add_column_name_conflict_fails() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        // "x" already exists in the V2 test schema.
        let action = tx.update_schema().add_column(AddColumn::optional(
            "x",
            Type::Primitive(PrimitiveType::Int),
        ));

        let result = Arc::new(action).commit(&table).await;
        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("should reject adding a column with an existing name"),
        };
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert!(
            err.message().contains("already exists"),
            "error should mention name conflict, got: {}",
            err.message()
        );
    }

    #[tokio::test]
    async fn test_delete_column() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        // z is not an identifier field, so we can delete it.
        let action = tx.update_schema().delete_column("z");

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_schema = match &updates[0] {
            TableUpdate::AddSchema { schema } => schema,
            other => panic!("expected AddSchema, got {other:?}"),
        };

        assert!(
            new_schema.field_by_name("z").is_none(),
            "z should be deleted"
        );
        assert!(new_schema.field_by_name("x").is_some());
        assert!(new_schema.field_by_name("y").is_some());
    }

    #[tokio::test]
    async fn test_delete_missing_column_fails() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        let action = tx.update_schema().delete_column("nonexistent");

        let result = Arc::new(action).commit(&table).await;
        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("should reject deleting a non-existent column"),
        };
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert!(
            err.message().contains("nonexistent"),
            "error should mention the missing column, got: {}",
            err.message()
        );
    }

    #[tokio::test]
    async fn test_add_and_delete_combined() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        // Delete z, add a new column.
        let action = tx
            .update_schema()
            .delete_column("z")
            .add_column(AddColumn::optional(
                "w",
                Type::Primitive(PrimitiveType::Boolean),
            ));

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_schema = match &updates[0] {
            TableUpdate::AddSchema { schema } => schema,
            other => panic!("expected AddSchema, got {other:?}"),
        };

        assert!(
            new_schema.field_by_name("z").is_none(),
            "z should be deleted"
        );
        let w = new_schema.field_by_name("w").expect("w should exist");
        assert_eq!(w.id, 4);
        assert!(!w.required);
    }

    #[tokio::test]
    async fn test_delete_and_readd_same_name() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        // Delete z, then add a new column named z -- should succeed.
        let action = tx
            .update_schema()
            .delete_column("z")
            .add_column(AddColumn::optional(
                "z",
                Type::Primitive(PrimitiveType::Boolean),
            ));

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_schema = match &updates[0] {
            TableUpdate::AddSchema { schema } => schema,
            other => panic!("expected AddSchema, got {other:?}"),
        };

        let z = new_schema
            .field_by_name("z")
            .expect("z should exist with new type");
        assert_eq!(z.id, 4); // new ID, not the old 3
        assert_eq!(*z.field_type, Type::Primitive(PrimitiveType::Boolean));
    }

    #[test]
    fn test_apply() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        let tx = tx
            .update_schema()
            .add_column(AddColumn::optional(
                "new_col",
                Type::Primitive(PrimitiveType::Int),
            ))
            .apply(tx)
            .unwrap();

        assert_eq!(tx.actions.len(), 1);
        (*tx.actions[0])
            .downcast_ref::<UpdateSchemaAction>()
            .expect("UpdateSchemaAction was not applied to Transaction!");
    }

    // -----------------------------------------------------------------------
    // Nested add tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_add_column_to_struct() {
        let table = make_v2_table_with_nested();
        let tx = Transaction::new(&table);

        // Add "email" to the "person" struct.
        let action = tx.update_schema().add_column(
            AddColumn::builder()
                .name("email")
                .field_type(Type::Primitive(PrimitiveType::String))
                .parent("person")
                .build(),
        );

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_schema = match &updates[0] {
            TableUpdate::AddSchema { schema } => schema,
            other => panic!("expected AddSchema, got {other:?}"),
        };

        // "email" should be nested under "person" with ID = last_column_id + 1 = 15.
        let email = new_schema
            .field_by_name("person.email")
            .expect("person.email should exist");
        assert_eq!(email.id, 15);
        assert!(!email.required);
        assert_eq!(*email.field_type, Type::Primitive(PrimitiveType::String));

        // Original nested fields should still be there.
        assert!(new_schema.field_by_name("person.name").is_some());
        assert!(new_schema.field_by_name("person.age").is_some());
    }

    #[tokio::test]
    async fn test_add_column_to_struct_with_doc() {
        let table = make_v2_table_with_nested();
        let tx = Transaction::new(&table);

        let action = tx.update_schema().add_column(
            AddColumn::builder()
                .name("phone")
                .field_type(Type::Primitive(PrimitiveType::String))
                .parent("person")
                .doc("Phone number")
                .build(),
        );

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_schema = match &updates[0] {
            TableUpdate::AddSchema { schema } => schema,
            other => panic!("expected AddSchema, got {other:?}"),
        };

        let phone = new_schema
            .field_by_name("person.phone")
            .expect("person.phone should exist");
        assert_eq!(phone.id, 15);
        assert_eq!(phone.doc.as_deref(), Some("Phone number"));
    }

    #[tokio::test]
    async fn test_add_column_to_list_element_struct() {
        let table = make_v2_table_with_nested();
        let tx = Transaction::new(&table);

        // "tags" is a list<struct{key, value}>. Adding to the list navigates to its
        // element struct automatically.
        let action = tx.update_schema().add_column(
            AddColumn::builder()
                .name("score")
                .field_type(Type::Primitive(PrimitiveType::Double))
                .parent("tags")
                .build(),
        );

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_schema = match &updates[0] {
            TableUpdate::AddSchema { schema } => schema,
            other => panic!("expected AddSchema, got {other:?}"),
        };

        // The list element struct should now contain "score".
        let score = new_schema
            .field_by_name("tags.element.score")
            .expect("tags.element.score should exist");
        assert_eq!(score.id, 15);
        assert!(!score.required);

        // Existing fields preserved.
        assert!(new_schema.field_by_name("tags.element.key").is_some());
        assert!(new_schema.field_by_name("tags.element.value").is_some());
    }

    #[tokio::test]
    async fn test_add_column_to_map_value_struct() {
        let table = make_v2_table_with_nested();
        let tx = Transaction::new(&table);

        // "props" is a map<string, struct{data}>. Adding to the map navigates to its
        // value struct automatically.
        let action = tx.update_schema().add_column(
            AddColumn::builder()
                .name("version")
                .field_type(Type::Primitive(PrimitiveType::Int))
                .parent("props")
                .build(),
        );

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_schema = match &updates[0] {
            TableUpdate::AddSchema { schema } => schema,
            other => panic!("expected AddSchema, got {other:?}"),
        };

        let version = new_schema
            .field_by_name("props.value.version")
            .expect("props.value.version should exist");
        assert_eq!(version.id, 15);

        // Existing map value fields preserved.
        assert!(new_schema.field_by_name("props.value.data").is_some());
    }

    #[tokio::test]
    async fn test_add_column_to_nonexistent_parent_fails() {
        let table = make_v2_table_with_nested();
        let tx = Transaction::new(&table);

        let action = tx.update_schema().add_column(
            AddColumn::builder()
                .name("col")
                .field_type(Type::Primitive(PrimitiveType::Int))
                .parent("nonexistent")
                .build(),
        );

        let err = match Arc::new(action).commit(&table).await {
            Err(e) => e,
            Ok(_) => panic!("should reject adding to a nonexistent parent"),
        };
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert!(
            err.message().contains("nonexistent"),
            "error should mention the missing parent, got: {}",
            err.message()
        );
    }

    #[tokio::test]
    async fn test_add_column_to_primitive_parent_fails() {
        let table = make_v2_table_with_nested();
        let tx = Transaction::new(&table);

        // "x" is a primitive (long), not a struct.
        let action = tx.update_schema().add_column(
            AddColumn::builder()
                .name("col")
                .field_type(Type::Primitive(PrimitiveType::Int))
                .parent("x")
                .build(),
        );

        let err = match Arc::new(action).commit(&table).await {
            Err(e) => e,
            Ok(_) => panic!("should reject adding to a primitive parent"),
        };
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert!(
            err.message().contains("not a struct"),
            "error should mention type mismatch, got: {}",
            err.message()
        );
    }

    #[tokio::test]
    async fn test_add_column_to_nested_name_conflict_fails() {
        let table = make_v2_table_with_nested();
        let tx = Transaction::new(&table);

        // "name" already exists in the "person" struct.
        let action = tx.update_schema().add_column(
            AddColumn::builder()
                .name("name")
                .field_type(Type::Primitive(PrimitiveType::String))
                .parent("person")
                .build(),
        );

        let err = match Arc::new(action).commit(&table).await {
            Err(e) => e,
            Ok(_) => panic!("should reject adding a column with conflicting name"),
        };
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert!(
            err.message().contains("already exists"),
            "error should mention name conflict, got: {}",
            err.message()
        );
    }

    #[tokio::test]
    async fn test_root_and_nested_add_combined() {
        let table = make_v2_table_with_nested();
        let tx = Transaction::new(&table);

        // Add a root column and a nested column in the same action.
        let action = tx
            .update_schema()
            .add_column(AddColumn::optional(
                "root_col",
                Type::Primitive(PrimitiveType::Boolean),
            ))
            .add_column(
                AddColumn::builder()
                    .name("email")
                    .field_type(Type::Primitive(PrimitiveType::String))
                    .parent("person")
                    .build(),
            );

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_schema = match &updates[0] {
            TableUpdate::AddSchema { schema } => schema,
            other => panic!("expected AddSchema, got {other:?}"),
        };

        // Root column gets the first fresh ID.
        let root_col = new_schema
            .field_by_name("root_col")
            .expect("root_col should exist");
        assert_eq!(root_col.id, 15);

        // Nested column gets the next ID.
        let email = new_schema
            .field_by_name("person.email")
            .expect("person.email should exist");
        assert_eq!(email.id, 16);
    }

    #[tokio::test]
    async fn test_add_nested_struct_type_with_fresh_ids() {
        // Adding a new column whose TYPE contains nested fields (e.g. a struct column). All sub-fields must receive
        // fresh IDs, not placeholder `DEFAULT_FIELD_ID`.
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        let action = tx.update_schema().add_column(AddColumn::optional(
            "address",
            Type::Struct(StructType::new(vec![
                NestedField::optional(
                    DEFAULT_FIELD_ID,
                    "street",
                    Type::Primitive(PrimitiveType::String),
                )
                .into(),
                NestedField::optional(
                    DEFAULT_FIELD_ID,
                    "city",
                    Type::Primitive(PrimitiveType::String),
                )
                .into(),
            ])),
        ));

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_schema = match &updates[0] {
            TableUpdate::AddSchema { schema } => schema,
            other => panic!("expected AddSchema, got {other:?}"),
        };

        // "address" gets ID 4 (last_column_id=3, +1).
        let address = new_schema
            .field_by_name("address")
            .expect("address should exist");
        assert_eq!(address.id, 4);

        // Sub-fields get IDs 5 and 6.
        let street = new_schema
            .field_by_name("address.street")
            .expect("address.street should exist");
        assert_eq!(street.id, 5);

        let city = new_schema
            .field_by_name("address.city")
            .expect("address.city should exist");
        assert_eq!(city.id, 6);
    }

    #[tokio::test]
    async fn test_rename_root_and_nested_columns_preserves_ids() {
        let table = make_v2_table_with_nested();
        let schema = apply_schema(
            &table,
            Transaction::new(&table)
                .update_schema()
                .rename_column("z", "payload")
                .rename_column("person.name", "full_name"),
        )
        .await;

        assert!(schema.field_by_name("z").is_none());
        assert_eq!(schema.field_by_name("payload").unwrap().id, 3);
        assert!(schema.field_by_name("person.name").is_none());
        assert_eq!(schema.field_by_name("person.full_name").unwrap().id, 5);
        assert_eq!(schema.field_by_name("person").unwrap().id, 4);
    }

    #[tokio::test]
    async fn test_update_type_doc_and_default_preserves_metadata() {
        let table = make_v2_table_with_nested();
        let schema = apply_schema(
            &table,
            Transaction::new(&table)
                .update_schema()
                .update_column_type("person.age", PrimitiveType::Long)
                .update_column_doc("person.age", Some("age in years".to_string()))
                .update_column_default("person.age", Some(Literal::long(18))),
        )
        .await;

        let age = schema.field_by_name("person.age").unwrap();
        assert_eq!(age.id, 6);
        assert!(age.required);
        assert_eq!(
            age.field_type.as_ref(),
            &Type::Primitive(PrimitiveType::Long)
        );
        assert_eq!(age.doc.as_deref(), Some("age in years"));
        assert_eq!(age.write_default, Some(Literal::long(18)));
        assert_eq!(age.initial_default, None);
    }

    #[tokio::test]
    async fn test_type_promotion_casts_existing_defaults() {
        let table = make_v2_table();
        let schema = apply_schema(
            &table,
            Transaction::new(&table)
                .update_schema()
                .add_column(
                    AddColumn::builder()
                        .name("count")
                        .field_type(Type::Primitive(PrimitiveType::Int))
                        .initial_default(Literal::int(7))
                        .write_default(Literal::int(8))
                        .build(),
                )
                .update_column_type("count", PrimitiveType::Long),
        )
        .await;

        let count = schema.field_by_name("count").unwrap();
        assert_eq!(
            count.field_type.as_ref(),
            &Type::Primitive(PrimitiveType::Long)
        );
        assert_eq!(count.initial_default, Some(Literal::long(7)));
        assert_eq!(count.write_default, Some(Literal::long(8)));
    }

    #[tokio::test]
    async fn test_rejects_invalid_type_promotion_and_default() {
        let table = make_v2_table_with_nested();
        let type_error = commit_error(
            &table,
            Transaction::new(&table)
                .update_schema()
                .update_column_type("person.age", PrimitiveType::String),
        )
        .await;
        assert_eq!(type_error.kind(), ErrorKind::PreconditionFailed);
        assert!(type_error.message().contains("Cannot change column type"));

        let default_error = commit_error(
            &table,
            Transaction::new(&table)
                .update_schema()
                .update_column_default("person.age", Some(Literal::string("unknown"))),
        )
        .await;
        assert_eq!(default_error.kind(), ErrorKind::PreconditionFailed);
        assert!(default_error.message().contains("Invalid default"));

        let nested_default_error = commit_error(
            &table,
            Transaction::new(&table).update_schema().add_column(
                AddColumn::builder()
                    .name("details")
                    .field_type(Type::Struct(StructType::new(vec![
                        NestedField::optional(100, "value", Type::Primitive(PrimitiveType::String))
                            .into(),
                    ])))
                    .initial_default(Literal::Struct(Struct::from_iter(vec![Some(
                        Literal::long(1),
                    )])))
                    .build(),
            ),
        )
        .await;
        assert_eq!(nested_default_error.kind(), ErrorKind::PreconditionFailed);
        assert!(nested_default_error.message().contains("Invalid default"));
    }

    #[tokio::test]
    async fn test_nullability_changes_require_explicit_opt_in() {
        let table = make_v2_table_with_nested();
        let error = commit_error(
            &table,
            Transaction::new(&table)
                .update_schema()
                .require_column("person.name"),
        )
        .await;
        assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
        assert!(error.message().contains("optional -> required"));

        let schema = apply_schema(
            &table,
            Transaction::new(&table)
                .update_schema()
                .allow_incompatible_changes()
                .require_column("person.name")
                .make_column_optional("person.age"),
        )
        .await;
        assert!(schema.field_by_name("person.name").unwrap().required);
        assert!(!schema.field_by_name("person.age").unwrap().required);
    }

    #[tokio::test]
    async fn test_required_add_without_default_requires_opt_in() {
        fn required_column() -> AddColumn {
            AddColumn::builder()
                .name("required_col")
                .required(true)
                .field_type(Type::Primitive(PrimitiveType::String))
                .build()
        }

        let table = make_v2_table();
        let error = commit_error(
            &table,
            Transaction::new(&table)
                .update_schema()
                .add_column(required_column()),
        )
        .await;
        assert_eq!(error.kind(), ErrorKind::PreconditionFailed);

        let schema = apply_schema(
            &table,
            Transaction::new(&table)
                .update_schema()
                .allow_incompatible_changes()
                .add_column(required_column()),
        )
        .await;
        assert!(schema.field_by_name("required_col").unwrap().required);
    }

    #[tokio::test]
    async fn test_move_columns_at_root_and_nested_levels() {
        let table = make_v2_table_with_nested();
        let schema = apply_schema(
            &table,
            Transaction::new(&table)
                .update_schema()
                .move_column_first("z")
                .move_column_after("person.name", "person.age"),
        )
        .await;

        let root_ids: Vec<i32> = schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.id)
            .collect();
        assert_eq!(root_ids, vec![3, 1, 2, 4, 7, 11]);
        let Type::Struct(person) = schema.field_by_name("person").unwrap().field_type.as_ref()
        else {
            panic!("person should remain a struct")
        };
        assert_eq!(person.fields()[0].id, 6);
        assert_eq!(person.fields()[1].id, 5);
    }

    #[tokio::test]
    async fn test_move_added_replacement_column() {
        let table = make_v2_table();
        let schema = apply_schema(
            &table,
            Transaction::new(&table)
                .update_schema()
                .delete_column("z")
                .add_column(AddColumn::optional(
                    "z",
                    Type::Primitive(PrimitiveType::String),
                ))
                .move_column_first("z"),
        )
        .await;

        assert_eq!(schema.as_struct().fields()[0].name, "z");
        assert_eq!(schema.as_struct().fields()[0].id, 4);
    }

    #[tokio::test]
    async fn test_move_validation() {
        let table = make_v2_table_with_nested();
        let self_move = commit_error(
            &table,
            Transaction::new(&table)
                .update_schema()
                .move_column_before("z", "z"),
        )
        .await;
        assert!(self_move.message().contains("itself"));

        let cross_struct = commit_error(
            &table,
            Transaction::new(&table)
                .update_schema()
                .move_column_after("person.name", "z"),
        )
        .await;
        assert!(cross_struct.message().contains("different struct"));

        let list_element = commit_error(
            &table,
            Transaction::new(&table)
                .update_schema()
                .move_column_first("tags.element"),
        )
        .await;
        assert!(list_element.message().contains("non-struct"));
    }

    #[tokio::test]
    async fn test_case_insensitive_resolution() {
        let table = make_v2_table_with_nested();
        let schema = apply_schema(
            &table,
            Transaction::new(&table)
                .update_schema()
                .case_sensitive(false)
                .rename_column("PERSON.NAME", "full_name")
                .update_column_type("PERSON.AGE", PrimitiveType::Long),
        )
        .await;
        assert_eq!(schema.field_by_name("person.full_name").unwrap().id, 5);
        assert_eq!(
            schema
                .field_by_name("person.age")
                .unwrap()
                .field_type
                .as_ref(),
            &Type::Primitive(PrimitiveType::Long)
        );
    }

    #[tokio::test]
    async fn test_identifier_field_replacement_controls_deletes() {
        let table = make_v2_table();
        let error = commit_error(
            &table,
            Transaction::new(&table).update_schema().delete_column("x"),
        )
        .await;
        assert!(error.message().contains("identifier field"));

        let schema = apply_schema(
            &table,
            Transaction::new(&table)
                .update_schema()
                .delete_column("x")
                .delete_column("y")
                .set_identifier_fields(["z"]),
        )
        .await;
        assert_eq!(schema.identifier_field_ids().collect::<Vec<_>>(), vec![3]);
        assert!(schema.field_by_name("x").is_none());
    }

    #[tokio::test]
    async fn test_identifier_field_validation_uses_updated_schema() {
        let table = make_v2_table();
        let error = commit_error(
            &table,
            Transaction::new(&table)
                .update_schema()
                .add_column(AddColumn::optional(
                    "candidate",
                    Type::Primitive(PrimitiveType::String),
                ))
                .set_identifier_fields(["candidate"]),
        )
        .await;
        assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
        assert!(error.to_string().contains("optional field"));

        let schema = apply_schema(
            &table,
            Transaction::new(&table)
                .update_schema()
                .add_column(AddColumn::required(
                    "candidate",
                    Type::Primitive(PrimitiveType::String),
                    Literal::string("unknown"),
                ))
                .set_identifier_fields(["candidate"]),
        )
        .await;
        assert_eq!(schema.identifier_field_ids().collect::<Vec<_>>(), vec![4]);
    }

    #[tokio::test]
    async fn test_rejects_map_key_changes() {
        let table = make_v2_table_with_nested();
        let error = commit_error(
            &table,
            Transaction::new(&table)
                .update_schema()
                .update_column_doc("props.key", Some("key docs".to_string())),
        )
        .await;
        assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
        assert!(error.message().contains("Cannot alter map keys"));
    }

    #[tokio::test]
    async fn test_union_by_name_adds_and_evolves_nested_fields() {
        let table = make_v2_table_with_nested();
        let incoming = Schema::builder()
            .with_fields(vec![
                NestedField::optional(
                    100,
                    "person",
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(101, "age", Type::Primitive(PrimitiveType::Long))
                            .with_doc("age in years")
                            .into(),
                        NestedField::required(
                            102,
                            "country",
                            Type::Primitive(PrimitiveType::String),
                        )
                        .into(),
                    ])),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let schema = apply_schema(
            &table,
            Transaction::new(&table)
                .update_schema()
                .union_by_name(incoming),
        )
        .await;

        let age = schema.field_by_name("person.age").unwrap();
        assert_eq!(age.id, 6);
        assert!(!age.required);
        assert_eq!(
            age.field_type.as_ref(),
            &Type::Primitive(PrimitiveType::Long)
        );
        assert_eq!(age.doc.as_deref(), Some("age in years"));
        let country = schema.field_by_name("person.country").unwrap();
        assert_eq!(country.id, 15);
        assert!(!country.required, "union additions must be optional");
    }

    #[tokio::test]
    async fn test_name_mapping_tracks_renames_and_additions() {
        let table = make_v2_table();
        let mapping = r#"[
            {"field-id":1,"names":["x"]},
            {"field-id":2,"names":["y"]},
            {"field-id":3,"names":["z"]}
        ]"#;
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .set_properties(HashMap::from([(
                DEFAULT_SCHEMA_NAME_MAPPING.to_string(),
                mapping.to_string(),
            )]))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let table = table.with_metadata(Arc::new(metadata));
        let action = Transaction::new(&table)
            .update_schema()
            .rename_column("z", "payload")
            .add_column(AddColumn::optional(
                "extra",
                Type::Primitive(PrimitiveType::String),
            ));
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let property_update = commit
            .take_updates()
            .into_iter()
            .find_map(|update| match update {
                TableUpdate::SetProperties { updates } => {
                    updates.get(DEFAULT_SCHEMA_NAME_MAPPING).cloned()
                }
                _ => None,
            })
            .expect("schema update should maintain the name mapping");
        let mapping: NameMapping = serde_json::from_str(&property_update).unwrap();
        let renamed = mapping
            .fields()
            .iter()
            .find(|field| field.field_id() == Some(3))
            .unwrap();
        assert_eq!(renamed.names(), &["z".to_string(), "payload".to_string()]);
        let added = mapping
            .fields()
            .iter()
            .find(|field| field.field_id() == Some(4))
            .unwrap();
        assert_eq!(added.names(), &["extra".to_string()]);
    }

    #[tokio::test]
    async fn test_invalid_name_mapping_does_not_block_update() {
        let table = make_v2_table();
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .set_properties(HashMap::from([(
                DEFAULT_SCHEMA_NAME_MAPPING.to_string(),
                "{not valid json".to_string(),
            )]))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let table = table.with_metadata(Arc::new(metadata));
        let action = Transaction::new(&table)
            .update_schema()
            .rename_column("z", "payload");
        let mut commit = Arc::new(action).commit(&table).await.unwrap();

        let updates = commit.take_updates();
        assert!(updates.iter().any(|update| matches!(
            update,
            TableUpdate::AddSchema { schema }
                if schema.field_by_name("payload").is_some()
        )));
        assert!(!updates.iter().any(|update| matches!(
            update,
            TableUpdate::SetProperties { updates }
                if updates.contains_key(DEFAULT_SCHEMA_NAME_MAPPING)
        )));
    }

    #[tokio::test]
    async fn test_column_properties_follow_renames_and_deletes() {
        let table = make_v2_table_with_nested();
        let metrics_key = "write.metadata.metrics.column.z";
        let bloom_key = "write.parquet.bloom-filter-enabled.column.z";
        let deleted_key = "write.parquet.stats-enabled.column.person.name";
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .set_properties(HashMap::from([
                (metrics_key.to_string(), "full".to_string()),
                (bloom_key.to_string(), "true".to_string()),
                (deleted_key.to_string(), "false".to_string()),
                (
                    "write.metadata.metrics.column.x".to_string(),
                    "counts".to_string(),
                ),
            ]))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let table = table.with_metadata(Arc::new(metadata));
        let action = Transaction::new(&table)
            .update_schema()
            .rename_column("z", "payload")
            .delete_column("person.name");
        let mut commit = Arc::new(action).commit(&table).await.unwrap();

        let updates = commit.take_updates();
        let removals = updates
            .iter()
            .find_map(|update| match update {
                TableUpdate::RemoveProperties { removals } => Some(removals),
                _ => None,
            })
            .unwrap();
        assert_eq!(removals, &vec![
            metrics_key.to_string(),
            bloom_key.to_string(),
            deleted_key.to_string(),
        ]);
        let property_updates = updates
            .iter()
            .find_map(|update| match update {
                TableUpdate::SetProperties { updates } => Some(updates),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            property_updates.get("write.metadata.metrics.column.payload"),
            Some(&"full".to_string())
        );
        assert_eq!(
            property_updates.get("write.parquet.bloom-filter-enabled.column.payload"),
            Some(&"true".to_string())
        );
        assert!(!property_updates.contains_key(deleted_key));
    }
}
