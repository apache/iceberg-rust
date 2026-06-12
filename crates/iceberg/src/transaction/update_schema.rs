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

//! Schema-evolution transaction action — the Rust mirror of Java `org.apache.iceberg.SchemaUpdate`.
//!
//! Builder methods record pending schema operations (add/rename/update/delete/move columns, identifier
//! fields, union-by-name); [`TransactionAction::commit`] replays them in order against the table's
//! current schema, rebuilds the resulting [`Schema`] with fresh field-id assignment for new columns, and
//! emits a [`TableUpdate::AddSchema`] followed by a [`TableUpdate::SetCurrentSchema`] with the
//! `LAST_ADDED` sentinel. Optimistic-concurrency guards on the last-assigned field id and the current
//! schema id are attached as requirements, matching Java's `UpdateRequirements` visitor.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;

use crate::error::Result;
use crate::spec::{
    ListType, Literal, MapType, NestedField, NestedFieldRef, PrimitiveType, Schema, StructType,
    Type, ensure_promotion_allowed,
};
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

/// The `schema_id` sentinel that tells `TableMetadataBuilder::set_current_schema` to use the schema
/// that was just added in the same set of changes (Java's `LAST_ADDED`).
const LAST_ADDED_SCHEMA_ID: i32 = -1;

/// The synthetic parent id Java uses for top-level (table root) columns (`SchemaUpdate.TABLE_ROOT_ID`).
const TABLE_ROOT_ID: i32 = -1;

/// Build a [`crate::Error`] of kind [`ErrorKind::DataInvalid`] with the given message, matching the
/// `IllegalArgumentException` thrown by Java's `Preconditions.checkArgument`.
fn data_invalid(message: String) -> Error {
    Error::new(ErrorKind::DataInvalid, message)
}

/// A pending schema-evolution operation. Builder methods record these in order; `commit` replays them
/// against the table's current schema (mirroring `transaction/update_partition_spec.rs`, which defers
/// all validation until a `Table` is available). The ordering matters: Java's `SchemaUpdate` mutates
/// shared state on each call so later calls observe earlier ones (e.g. the delete-then-conflict checks,
/// move-after-add, identifier-field-after-add).
#[derive(Debug, Clone)]
enum SchemaOp {
    /// Add an optional (`required == false`) or required column under `parent` (None = top level).
    AddColumn {
        parent: Option<String>,
        name: String,
        field_type: Type,
        required: bool,
        doc: Option<String>,
        /// Initial/write default for existing+future rows. A required add WITH a default is allowed
        /// even without `allow_incompatible_changes` (the default backfills existing rows).
        default: Option<Literal>,
    },
    /// Rename a column to `new_name`.
    RenameColumn { name: String, new_name: String },
    /// Update a column to a new primitive type (type promotion).
    UpdateColumn {
        name: String,
        new_type: PrimitiveType,
    },
    /// Replace a column's doc string.
    UpdateColumnDoc { name: String, doc: Option<String> },
    /// Set a column's write default (Java `updateColumnDefault`).
    UpdateColumnDefault { name: String, default: Literal },
    /// Make a column optional (`is_optional == true`) or required (`is_optional == false`).
    UpdateColumnRequirement { name: String, is_optional: bool },
    /// Delete a column (and all its descendants).
    DeleteColumn { name: String },
    /// Move a column to the start of its containing struct.
    MoveFirst { name: String },
    /// Move a column to immediately before `reference`.
    MoveBefore { name: String, reference: String },
    /// Move a column to immediately after `reference`.
    MoveAfter { name: String, reference: String },
    /// Replace the identifier-field set with the named columns.
    SetIdentifierFields { names: Vec<String> },
    /// Merge another schema into this one by canonical field name. Boxed because a `Schema` is large
    /// relative to the other op variants.
    UnionByName { other: Box<Schema> },
}

/// Transaction action for schema evolution — the Rust mirror of Java's `SchemaUpdate`.
///
/// Builder methods record pending ops; [`TransactionAction::commit`] resolves them against the table's
/// current schema and emits a [`TableUpdate::AddSchema`] for the rebuilt schema, followed by a
/// [`TableUpdate::SetCurrentSchema`] with the `LAST_ADDED` (`-1`) sentinel. Optimistic-concurrency
/// guards on the last-assigned field id and the current schema id are attached as requirements.
pub struct UpdateSchemaAction {
    case_sensitive: bool,
    allow_incompatible_changes: bool,
    ops: Vec<SchemaOp>,
}

impl UpdateSchemaAction {
    /// Create a new, empty schema update. Column-name resolution is case-sensitive by default, and
    /// incompatible changes are rejected by default — both matching Java.
    pub fn new() -> Self {
        UpdateSchemaAction {
            case_sensitive: true,
            allow_incompatible_changes: false,
            ops: vec![],
        }
    }

    /// Set whether column-name resolution against the schema is case-sensitive (default `true`).
    pub fn case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
        self
    }

    /// Allow incompatible changes (Java `allowIncompatibleChanges`). This relaxes exactly two guards:
    /// adding a required column without a default, and making an optional column required. It does NOT
    /// relax type promotion.
    pub fn allow_incompatible_changes(mut self) -> Self {
        self.allow_incompatible_changes = true;
        self
    }

    /// Add a new optional top-level column. The name may not contain `.` (Java rejects it here); use
    /// [`add_column_to`](Self::add_column_to) for nested or dotted-leaf columns.
    pub fn add_column(self, name: &str, field_type: Type) -> Self {
        self.add_column_internal(None, name, field_type, false, None, None, true)
    }

    /// Add a new optional top-level column with an initial/write default value (Java
    /// `addColumn(name, type, Literal)`). The default is validated against the column type at commit.
    pub fn add_column_with_default(self, name: &str, field_type: Type, default: Literal) -> Self {
        self.add_column_internal(None, name, field_type, false, None, Some(default), true)
    }

    /// Add a new optional column under `parent` (None = top level). The leaf `name` is taken verbatim
    /// (a `.` in it is NOT a path separator). `parent` is resolved against the schema and may identify a
    /// struct, a list (adds to the element struct) or a map (adds to the value struct).
    pub fn add_column_to(
        self,
        parent: Option<&str>,
        name: &str,
        field_type: Type,
        doc: Option<&str>,
    ) -> Self {
        // Nested form: a `.` in the leaf name is legal, so do not reject it.
        self.add_column_internal(parent, name, field_type, false, doc, None, false)
    }

    /// Add a new optional column under `parent` with an initial/write default value (Java
    /// `addColumn(parent, name, type, doc, Literal)`).
    pub fn add_column_to_with_default(
        self,
        parent: Option<&str>,
        name: &str,
        field_type: Type,
        doc: Option<&str>,
        default: Literal,
    ) -> Self {
        self.add_column_internal(parent, name, field_type, false, doc, Some(default), false)
    }

    /// Add a new required top-level column. Without a default this is an incompatible change and must be
    /// gated by [`allow_incompatible_changes`](Self::allow_incompatible_changes) (validated at commit).
    pub fn add_required_column(self, name: &str, field_type: Type) -> Self {
        self.add_column_internal(None, name, field_type, true, None, None, true)
    }

    /// Add a new required top-level column with an initial/write default (Java
    /// `addRequiredColumn(name, type, Literal)`). A required add WITH a default is allowed even without
    /// [`allow_incompatible_changes`](Self::allow_incompatible_changes), because the default backfills
    /// the existing rows.
    pub fn add_required_column_with_default(
        self,
        name: &str,
        field_type: Type,
        default: Literal,
    ) -> Self {
        self.add_column_internal(None, name, field_type, true, None, Some(default), true)
    }

    /// Add a new required column under `parent` (None = top level). See
    /// [`add_required_column`](Self::add_required_column) for the incompatible-change rule.
    pub fn add_required_column_to(
        self,
        parent: Option<&str>,
        name: &str,
        field_type: Type,
        doc: Option<&str>,
    ) -> Self {
        self.add_column_internal(parent, name, field_type, true, doc, None, false)
    }

    /// Add a new required column under `parent` with an initial/write default (Java
    /// `addRequiredColumn(parent, name, type, doc, Literal)`). The default relaxes the
    /// incompatible-change gate, as in [`add_required_column_with_default`](Self::add_required_column_with_default).
    pub fn add_required_column_to_with_default(
        self,
        parent: Option<&str>,
        name: &str,
        field_type: Type,
        doc: Option<&str>,
        default: Literal,
    ) -> Self {
        self.add_column_internal(parent, name, field_type, true, doc, Some(default), false)
    }

    /// Shared add-column recorder. `reject_dotted_top_level` mirrors Java's top-level `addColumn`
    /// precondition that a `.`-containing name is ambiguous; the nested form passes `false`.
    #[allow(clippy::too_many_arguments)]
    fn add_column_internal(
        mut self,
        parent: Option<&str>,
        name: &str,
        field_type: Type,
        required: bool,
        doc: Option<&str>,
        default: Option<Literal>,
        reject_dotted_top_level: bool,
    ) -> Self {
        self.ops.push(SchemaOp::AddColumn {
            // Stash the ambiguity rule so commit() can surface the exact Java error; carry it as a
            // sentinel through the op rather than failing in the builder (the builder is infallible).
            parent: match (reject_dotted_top_level, parent) {
                (true, _) => Some(AMBIGUOUS_TOP_LEVEL_MARKER.to_string()),
                (false, Some(parent)) => Some(parent.to_string()),
                (false, None) => None,
            },
            name: name.to_string(),
            field_type,
            required,
            doc: doc.map(str::to_string),
            default,
        });
        self
    }

    /// Rename a column.
    pub fn rename_column(mut self, name: &str, new_name: &str) -> Self {
        self.ops.push(SchemaOp::RenameColumn {
            name: name.to_string(),
            new_name: new_name.to_string(),
        });
        self
    }

    /// Update a column to a new primitive type (subject to type-promotion rules).
    pub fn update_column(mut self, name: &str, new_type: PrimitiveType) -> Self {
        self.ops.push(SchemaOp::UpdateColumn {
            name: name.to_string(),
            new_type,
        });
        self
    }

    /// Replace a column's documentation string.
    pub fn update_column_doc(mut self, name: &str, doc: Option<&str>) -> Self {
        self.ops.push(SchemaOp::UpdateColumnDoc {
            name: name.to_string(),
            doc: doc.map(str::to_string),
        });
        self
    }

    /// Set a column's write default (Java `updateColumnDefault(name, Literal)`). This sets only the
    /// `write_default` on an existing field; the initial default (which backfills existing rows) is left
    /// unchanged. The default is validated against the column type at commit.
    pub fn update_column_default(mut self, name: &str, default: Literal) -> Self {
        self.ops.push(SchemaOp::UpdateColumnDefault {
            name: name.to_string(),
            default,
        });
        self
    }

    /// Make a column optional (always allowed).
    pub fn make_column_optional(mut self, name: &str) -> Self {
        self.ops.push(SchemaOp::UpdateColumnRequirement {
            name: name.to_string(),
            is_optional: true,
        });
        self
    }

    /// Make a column required (incompatible change unless gated by
    /// [`allow_incompatible_changes`](Self::allow_incompatible_changes), or a no-op on an already
    /// required field).
    pub fn require_column(mut self, name: &str) -> Self {
        self.ops.push(SchemaOp::UpdateColumnRequirement {
            name: name.to_string(),
            is_optional: false,
        });
        self
    }

    /// Delete a column (and all its descendants).
    pub fn delete_column(mut self, name: &str) -> Self {
        self.ops.push(SchemaOp::DeleteColumn {
            name: name.to_string(),
        });
        self
    }

    /// Move a column to the start of its containing struct.
    pub fn move_first(mut self, name: &str) -> Self {
        self.ops.push(SchemaOp::MoveFirst {
            name: name.to_string(),
        });
        self
    }

    /// Move a column to immediately before `reference` (within the same struct).
    pub fn move_before(mut self, name: &str, reference: &str) -> Self {
        self.ops.push(SchemaOp::MoveBefore {
            name: name.to_string(),
            reference: reference.to_string(),
        });
        self
    }

    /// Move a column to immediately after `reference` (within the same struct).
    pub fn move_after(mut self, name: &str, reference: &str) -> Self {
        self.ops.push(SchemaOp::MoveAfter {
            name: name.to_string(),
            reference: reference.to_string(),
        });
        self
    }

    /// Replace the identifier-field set with `names` (Java `setIdentifierFields`). Validated at commit:
    /// each named field must exist (in the schema or added in this action), be required, primitive, not
    /// deleted, and not nested in a map/list/optional struct.
    pub fn set_identifier_fields<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.ops.push(SchemaOp::SetIdentifierFields {
            names: names.into_iter().map(Into::into).collect(),
        });
        self
    }

    /// Merge `other` into the current schema by canonical field name (Java `unionByNameWith`).
    pub fn union_by_name_with(mut self, other: Schema) -> Self {
        self.ops.push(SchemaOp::UnionByName {
            other: Box::new(other),
        });
        self
    }
}

impl Default for UpdateSchemaAction {
    fn default() -> Self {
        Self::new()
    }
}

/// Sentinel stored in `SchemaOp::AddColumn.parent` to signal the top-level (`.`-rejecting) add form.
/// A real column path can never equal this string, so it is unambiguous.
const AMBIGUOUS_TOP_LEVEL_MARKER: &str = "\0__iceberg_top_level__\0";

/// A pending column move, mirroring Java's `SchemaUpdate.Move`.
#[derive(Debug, Clone)]
enum Move {
    First { field_id: i32 },
    Before { field_id: i32, reference_id: i32 },
    After { field_id: i32, reference_id: i32 },
}

impl Move {
    fn field_id(&self) -> i32 {
        match self {
            Move::First { field_id }
            | Move::Before { field_id, .. }
            | Move::After { field_id, .. } => *field_id,
        }
    }
}

/// The in-flight resolution state — the Rust mirror of `SchemaUpdate`'s mutable fields. Built fresh
/// inside `commit` from the table's current schema, then mutated as each recorded op is replayed.
struct SchemaEvolution<'a> {
    schema: &'a Schema,
    case_sensitive: bool,
    allow_incompatible_changes: bool,
    /// field id → parent field id (root fields absent), indexed once from the base schema.
    id_to_parent: HashMap<i32, i32>,
    /// Field ids marked for deletion (Java `deletes`).
    deletes: HashSet<i32>,
    /// field id → replacement field metadata (Java `updates`; rename/type/doc/nullability merge here).
    updates: HashMap<i32, NestedField>,
    /// parent field id (or TABLE_ROOT_ID) → ids of fields added under it, in add order
    /// (Java `parentToAddedIds`).
    parent_to_added_ids: HashMap<i32, Vec<i32>>,
    /// added field id → the full added NestedField (a parallel store to `updates`, since added fields
    /// are not in the base schema). Java keeps adds in `updates` keyed by their new id.
    added_fields: HashMap<i32, NestedField>,
    /// case-aware full name → added field id (Java `addedNameToId`).
    added_name_to_id: HashMap<String, i32>,
    /// parent field id (or TABLE_ROOT_ID) → moves under that parent, in call order (Java `moves`).
    moves: HashMap<i32, Vec<Move>>,
    /// The working identifier-field name set (Java `identifierFieldNames`), seeded from the base schema
    /// and rewritten by rename / setIdentifierFields.
    identifier_field_names: HashSet<String>,
    /// The next field id to assign (Java `lastColumnId`, post-increment).
    last_column_id: i32,
}

impl<'a> SchemaEvolution<'a> {
    fn new(
        schema: &'a Schema,
        last_column_id: i32,
        case_sensitive: bool,
        allow_incompatible_changes: bool,
    ) -> Result<Self> {
        let mut id_to_parent = HashMap::new();
        index_parents(schema.as_struct(), None, &mut id_to_parent);
        // Seed the identifier-field names from the base schema's current identifier ids.
        let identifier_field_names = schema
            .identifier_field_ids()
            .filter_map(|id| schema.name_by_field_id(id).map(str::to_string))
            .collect();
        Ok(SchemaEvolution {
            schema,
            case_sensitive,
            allow_incompatible_changes,
            id_to_parent,
            deletes: HashSet::new(),
            updates: HashMap::new(),
            parent_to_added_ids: HashMap::new(),
            added_fields: HashMap::new(),
            added_name_to_id: HashMap::new(),
            moves: HashMap::new(),
            identifier_field_names,
            last_column_id,
        })
    }

    /// Resolve a column name to its existing base-schema field, honoring case sensitivity (Java
    /// `findField`).
    fn find_field(&self, name: &str) -> Option<NestedFieldRef> {
        if self.case_sensitive {
            self.schema.field_by_name(name).cloned()
        } else {
            self.schema.field_by_name_case_insensitive(name).cloned()
        }
    }

    /// The case-aware key under which an added column's full name is stored (Java
    /// `caseSensitivityAwareName`).
    fn case_aware_name(&self, name: &str) -> String {
        if self.case_sensitive {
            name.to_string()
        } else {
            name.to_lowercase()
        }
    }

    /// Whether `name` refers to a field added in this action (Java `isAdded`).
    fn is_added(&self, name: &str) -> bool {
        self.added_name_to_id
            .contains_key(&self.case_aware_name(name))
    }

    /// Resolve a column for an update-style op: prefer a pending update over the base field, and fall
    /// back to an added field (Java `findForUpdate`). Returns the current effective NestedField.
    fn find_for_update(&self, name: &str) -> Option<NestedField> {
        if let Some(existing) = self.find_field(name) {
            if let Some(pending) = self.updates.get(&existing.id) {
                return Some(pending.clone());
            }
            return Some((*existing).clone());
        }
        let added_id = self.added_name_to_id.get(&self.case_aware_name(name))?;
        self.added_fields.get(added_id).cloned()
    }

    /// Resolve a column for a move: an added field wins over a base field (Java `findForMove`).
    fn find_for_move(&self, name: &str) -> Option<i32> {
        if let Some(added_id) = self.added_name_to_id.get(&self.case_aware_name(name)) {
            return Some(*added_id);
        }
        self.find_field(name).map(|field| field.id)
    }

    /// Assign the next fresh field id (Java `assignNewColumnId`).
    fn assign_new_column_id(&mut self) -> Result<i32> {
        let next = self.last_column_id.checked_add(1).ok_or_else(|| {
            data_invalid("Field ID overflowed, cannot add more columns".to_string())
        })?;
        self.last_column_id = next;
        Ok(next)
    }

    /// Recursively assign fresh ids to a newly added type — the Rust mirror of Java
    /// `TypeUtil.assignFreshIds(type, this::assignNewColumnId)`, which runs as a `CustomOrderSchemaVisitor`
    /// (pre-order / **level-order**). A primitive is returned as-is.
    ///
    /// The ordering is observable and pinned by Java's `TestSchemaUpdate.testAddNestedMapOfStructs`: a
    /// struct assigns ids for ALL of its immediate fields BEFORE descending into any field's type, and a
    /// map assigns its key id and then its value id BEFORE descending into either. A naive depth-first
    /// walk (assign this field's id, then immediately recurse its type) yields different ids whenever a
    /// nested field has a following sibling, which breaks Java interop and round-trip parity.
    fn assign_fresh_ids(&mut self, field_type: Type) -> Result<Type> {
        match field_type {
            Type::Primitive(primitive) => Ok(Type::Primitive(primitive)),
            // Leaf, like a primitive — Java 1.10.0 `AssignFreshIds.variant` returns the type
            // unchanged (variant has no nested ids to assign).
            Type::Variant => Ok(Type::Variant),
            Type::Struct(struct_type) => {
                let fields = struct_type.fields();
                // Pass 1: assign fresh ids for every immediate field (level-order), before recursing.
                let new_ids: Vec<i32> = fields
                    .iter()
                    .map(|_| self.assign_new_column_id())
                    .collect::<Result<_>>()?;
                // Pass 2: recurse into each field's type (which assigns the next level's ids), in order.
                let mut new_fields = Vec::with_capacity(fields.len());
                for (field, new_id) in fields.iter().zip(new_ids) {
                    let new_type = self.assign_fresh_ids((*field.field_type).clone())?;
                    let mut rebuilt =
                        NestedField::new(new_id, field.name.clone(), new_type, field.required);
                    rebuilt.doc = field.doc.clone();
                    rebuilt.initial_default = field.initial_default.clone();
                    rebuilt.write_default = field.write_default.clone();
                    new_fields.push(Arc::new(rebuilt));
                }
                Ok(Type::Struct(StructType::new(new_fields)))
            }
            Type::List(list_type) => {
                // A list has a single immediate id (the element). Assign it, then recurse the element.
                let element_id = self.assign_new_column_id()?;
                let element = &list_type.element_field;
                let new_element_type = self.assign_fresh_ids((*element.field_type).clone())?;
                Ok(Type::List(ListType::new(Arc::new(
                    NestedField::list_element(element_id, new_element_type, element.required),
                ))))
            }
            Type::Map(map_type) => {
                // Level-order: assign key id THEN value id (both immediate), then recurse key then value.
                let key_id = self.assign_new_column_id()?;
                let value_id = self.assign_new_column_id()?;
                let key = &map_type.key_field;
                let value = &map_type.value_field;
                let new_key_type = self.assign_fresh_ids((*key.field_type).clone())?;
                let new_value_type = self.assign_fresh_ids((*value.field_type).clone())?;
                Ok(Type::Map(MapType::new(
                    Arc::new(NestedField::map_key_element(key_id, new_key_type)),
                    Arc::new(NestedField::map_value_element(
                        value_id,
                        new_value_type,
                        value.required,
                    )),
                )))
            }
        }
    }

    /// Replay `addColumn` / `addRequiredColumn` (Java `internalAddColumn`).
    fn add_column(
        &mut self,
        parent: Option<&str>,
        name: &str,
        field_type: Type,
        required: bool,
        doc: Option<String>,
        default: Option<Literal>,
    ) -> Result<()> {
        let mut parent_id = TABLE_ROOT_ID;
        let full_name;

        if let Some(parent) = parent {
            let parent_field = self
                .find_field(parent)
                .ok_or_else(|| data_invalid(format!("Cannot find parent struct: {parent}")))?;
            // If the parent is a list or map, descend into the element / value struct.
            let parent_struct_field = match parent_field.field_type.as_ref() {
                Type::Map(map) => map.value_field.clone(),
                Type::List(list) => list.element_field.clone(),
                _ => parent_field.clone(),
            };
            if !parent_struct_field.field_type.is_struct() {
                return Err(data_invalid(format!(
                    "Cannot add to non-struct column: {parent}: {}",
                    parent_struct_field.field_type
                )));
            }
            parent_id = parent_struct_field.id;

            if self.deletes.contains(&parent_id) {
                return Err(data_invalid(format!(
                    "Cannot add to a column that will be deleted: {parent}"
                )));
            }

            let nested_name = format!("{parent}.{name}");
            let current = self.find_field(&nested_name);
            if let Some(current) = current
                && !self.deletes.contains(&current.id)
            {
                return Err(data_invalid(format!(
                    "Cannot add column, name already exists: {parent}.{name}"
                )));
            }
            let parent_full_name = self
                .schema
                .name_by_field_id(parent_id)
                .map(str::to_string)
                .unwrap_or_else(|| parent.to_string());
            full_name = format!("{parent_full_name}.{name}");
        } else {
            let current = self.find_field(name);
            if let Some(current) = current
                && !self.deletes.contains(&current.id)
            {
                return Err(data_invalid(format!(
                    "Cannot add column, name already exists: {name}"
                )));
            }
            full_name = name.to_string();
        }

        // Java `internalAddColumn`: a required add is incompatible UNLESS a default is supplied (it
        // backfills existing rows), the column is optional, or `allowIncompatibleChanges` is set.
        if required && default.is_none() && !self.allow_incompatible_changes {
            return Err(data_invalid(format!(
                "Incompatible change: cannot add required column without a default value: {full_name}"
            )));
        }

        let new_id = self.assign_new_column_id()?;
        self.added_name_to_id
            .insert(self.case_aware_name(&full_name), new_id);
        if parent_id != TABLE_ROOT_ID {
            self.id_to_parent.insert(new_id, parent_id);
        }

        let fresh_type = self.assign_fresh_ids(field_type)?;
        let mut new_field = NestedField::new(new_id, name, fresh_type, required);
        new_field.doc = doc;
        // Java sets BOTH the initial default (backfills existing rows) and the write default (default for
        // future writes) on an added column. Validate the default against the (id-assigned) column type.
        if let Some(default) = default {
            validate_default(&default, &new_field.field_type)?;
            new_field.initial_default = Some(default.clone());
            new_field.write_default = Some(default);
        }

        self.added_fields.insert(new_id, new_field);
        self.parent_to_added_ids
            .entry(parent_id)
            .or_default()
            .push(new_id);
        Ok(())
    }

    /// Replay `deleteColumn` (Java `deleteColumn`).
    fn delete_column(&mut self, name: &str) -> Result<()> {
        let field = self
            .find_field(name)
            .ok_or_else(|| data_invalid(format!("Cannot delete missing column: {name}")))?;
        if self.parent_to_added_ids.contains_key(&field.id) {
            return Err(data_invalid(format!(
                "Cannot delete a column that has additions: {name}"
            )));
        }
        if self.updates.contains_key(&field.id) {
            return Err(data_invalid(format!(
                "Cannot delete a column that has updates: {name}"
            )));
        }
        self.deletes.insert(field.id);
        Ok(())
    }

    /// Replay `renameColumn` (Java `renameColumn`).
    fn rename_column(&mut self, name: &str, new_name: &str) -> Result<()> {
        let field = self
            .find_field(name)
            .ok_or_else(|| data_invalid(format!("Cannot rename missing column: {name}")))?;
        if self.deletes.contains(&field.id) {
            return Err(data_invalid(format!(
                "Cannot rename a column that will be deleted: {}",
                field.name
            )));
        }
        // Merge with a pending update, if present.
        let mut new_field = self
            .updates
            .get(&field.id)
            .cloned()
            .unwrap_or_else(|| (*field).clone());
        new_field.name = new_name.to_string();
        self.updates.insert(field.id, new_field);

        // Identifier fields are tracked by name in Java; rewrite the entry on rename.
        if self.identifier_field_names.remove(name) {
            self.identifier_field_names.insert(new_name.to_string());
        }
        Ok(())
    }

    /// Replay `updateColumn` (Java `updateColumn`).
    fn update_column(&mut self, name: &str, new_type: PrimitiveType) -> Result<()> {
        let field = self
            .find_for_update(name)
            .ok_or_else(|| data_invalid(format!("Cannot update missing column: {name}")))?;
        if self.deletes.contains(&field.id) {
            return Err(data_invalid(format!(
                "Cannot update a column that will be deleted: {}",
                field.name
            )));
        }

        // No-op when the type is already the target (Java's `field.type().equals(newType)`).
        if field.field_type.as_ref() == &Type::Primitive(new_type.clone()) {
            return Ok(());
        }

        // Type promotion is NOT relaxed by allow_incompatible_changes.
        ensure_promotion_allowed(&field.field_type, &new_type).map_err(|_| {
            data_invalid(format!(
                "Cannot change column type: {name}: {} -> {new_type}",
                field.field_type
            ))
        })?;

        let mut new_field = field;
        *new_field.field_type = Type::Primitive(new_type);
        self.store_update(new_field);
        Ok(())
    }

    /// Replay `updateColumnDoc` (Java `updateColumnDoc`).
    fn update_column_doc(&mut self, name: &str, doc: Option<String>) -> Result<()> {
        let field = self
            .find_for_update(name)
            .ok_or_else(|| data_invalid(format!("Cannot update missing column: {name}")))?;
        if self.deletes.contains(&field.id) {
            return Err(data_invalid(format!(
                "Cannot update a column that will be deleted: {}",
                field.name
            )));
        }
        if field.doc == doc {
            return Ok(());
        }
        let mut new_field = field;
        new_field.doc = doc;
        self.store_update(new_field);
        Ok(())
    }

    /// Replay `updateColumnDefault` (Java `updateColumnDefault`): set ONLY the `write_default` on an
    /// existing field (the initial default backfills existing rows and is never changed here). A no-op
    /// when the write default already equals the requested value.
    fn update_column_default(&mut self, name: &str, default: &Literal) -> Result<()> {
        let field = self
            .find_for_update(name)
            .ok_or_else(|| data_invalid(format!("Cannot update missing column: {name}")))?;
        if self.deletes.contains(&field.id) {
            return Err(data_invalid(format!(
                "Cannot update a column that will be deleted: {}",
                field.name
            )));
        }
        // Validate the default converts to the column type (Java's `newDefault.to(field.type())`); this
        // also guarantees the value will serialize, avoiding a later panic in the serde path.
        validate_default(default, &field.field_type)?;
        if field.write_default.as_ref() == Some(default) {
            return Ok(());
        }
        let mut new_field = field;
        new_field.write_default = Some(default.clone());
        self.store_update(new_field);
        Ok(())
    }

    /// Replay `requireColumn` / `makeColumnOptional` (Java `internalUpdateColumnRequirement`).
    fn update_column_requirement(&mut self, name: &str, is_optional: bool) -> Result<()> {
        let field = self
            .find_for_update(name)
            .ok_or_else(|| data_invalid(format!("Cannot update missing column: {name}")))?;

        // No-op (required -> required, or optional -> optional) is always allowed, even without the flag.
        if (!is_optional && field.required) || (is_optional && !field.required) {
            return Ok(());
        }

        // A newly added column that carries a default may be made required without the flag.
        let is_defaulted_add = self.is_added(name) && field.initial_default.is_some();
        if !is_optional && !is_defaulted_add && !self.allow_incompatible_changes {
            return Err(data_invalid(format!(
                "Cannot change column nullability: {name}: optional -> required"
            )));
        }
        if self.deletes.contains(&field.id) {
            return Err(data_invalid(format!(
                "Cannot update a column that will be deleted: {}",
                field.name
            )));
        }

        let mut new_field = field;
        new_field.required = !is_optional;
        self.store_update(new_field);
        Ok(())
    }

    /// Store a pending field update in the correct map: an added field's update goes back into
    /// `added_fields` (Java keeps adds in the same `updates` map), a base field's into `updates`.
    fn store_update(&mut self, field: NestedField) {
        if let std::collections::hash_map::Entry::Occupied(mut entry) =
            self.added_fields.entry(field.id)
        {
            entry.insert(field);
        } else {
            self.updates.insert(field.id, field);
        }
    }

    /// Replay a move (Java `moveFirst` / `moveBefore` / `moveAfter` + `internalMove`).
    fn apply_move(&mut self, name: &str, kind: MoveKind<'_>) -> Result<()> {
        let field_id = self
            .find_for_move(name)
            .ok_or_else(|| data_invalid(format!("Cannot move missing column: {name}")))?;

        let move_op = match kind {
            MoveKind::First => Move::First { field_id },
            MoveKind::Before(reference) => {
                let reference_id = self.find_for_move(reference).ok_or_else(|| {
                    data_invalid(format!(
                        "Cannot move {name} before missing column: {reference}"
                    ))
                })?;
                if field_id == reference_id {
                    return Err(data_invalid(format!("Cannot move {name} before itself")));
                }
                Move::Before {
                    field_id,
                    reference_id,
                }
            }
            MoveKind::After(reference) => {
                let reference_id = self.find_for_move(reference).ok_or_else(|| {
                    data_invalid(format!(
                        "Cannot move {name} after missing column: {reference}"
                    ))
                })?;
                if field_id == reference_id {
                    return Err(data_invalid(format!("Cannot move {name} after itself")));
                }
                Move::After {
                    field_id,
                    reference_id,
                }
            }
        };

        self.internal_move(name, move_op)
    }

    /// Java `internalMove`: validate the parent struct and that BEFORE/AFTER references the same struct.
    fn internal_move(&mut self, name: &str, move_op: Move) -> Result<()> {
        let parent_id = self.id_to_parent.get(&move_op.field_id()).copied();
        let reference_id = match &move_op {
            Move::Before { reference_id, .. } | Move::After { reference_id, .. } => {
                Some(*reference_id)
            }
            Move::First { .. } => None,
        };

        if let Some(parent_id) = parent_id {
            // The parent must be a struct (not a list/map element parent).
            let parent_field = self.schema.field_by_id(parent_id);
            if let Some(parent_field) = parent_field
                && !parent_field.field_type.is_struct()
            {
                return Err(data_invalid(format!(
                    "Cannot move fields in non-struct type: {}",
                    parent_field.field_type
                )));
            }
            if let Some(reference_id) = reference_id
                && self.id_to_parent.get(&reference_id).copied() != Some(parent_id)
            {
                return Err(data_invalid(format!(
                    "Cannot move field {name} to a different struct"
                )));
            }
            self.moves.entry(parent_id).or_default().push(move_op);
        } else {
            if let Some(reference_id) = reference_id
                && self.id_to_parent.contains_key(&reference_id)
            {
                return Err(data_invalid(format!(
                    "Cannot move field {name} to a different struct"
                )));
            }
            self.moves.entry(TABLE_ROOT_ID).or_default().push(move_op);
        }
        Ok(())
    }

    /// Replay `setIdentifierFields` (Java `setIdentifierFields`): replace the working set, deduplicating.
    fn set_identifier_fields(&mut self, names: &[String]) {
        self.identifier_field_names = names.iter().cloned().collect();
    }

    /// Build the resulting schema — the Rust mirror of Java `applyChanges`. First validate that no
    /// existing identifier field (or its ancestor) is being deleted, then recursively rebuild the
    /// struct applying deletes/updates/adds/moves, then validate the fresh identifier-field set and let
    /// `Schema::builder` enforce the spec's identifier-field rules.
    fn apply(self) -> Result<Schema> {
        // 1. Existing identifier fields (and their ancestors) must not be deleted.
        for name in &self.identifier_field_names {
            let field = self.find_field(name);
            if let Some(field) = field {
                if self.deletes.contains(&field.id) {
                    return Err(data_invalid(format!(
                        "Cannot delete identifier field {}. To force deletion, also call \
                         set_identifier_fields to update identifier fields.",
                        field.name
                    )));
                }
                let mut parent = self.id_to_parent.get(&field.id).copied();
                while let Some(parent_id) = parent {
                    if self.deletes.contains(&parent_id) {
                        return Err(data_invalid(format!(
                            "Cannot delete field {parent_id} as it will delete nested identifier \
                             field {}",
                            field.name
                        )));
                    }
                    parent = self.id_to_parent.get(&parent_id).copied();
                }
            }
        }

        // 2. Rebuild the top-level struct.
        let new_struct = self.rebuild_struct(self.schema.as_struct(), TABLE_ROOT_ID)?;

        // 3. Resolve the identifier-field names against the rebuilt schema to fresh ids. Build a
        //    throwaway schema for name resolution (Schema indexes names on build).
        let resolved = Schema::builder()
            .with_fields(new_struct.fields().iter().cloned())
            .build()?;
        let mut identifier_field_ids = HashSet::new();
        for name in &self.identifier_field_names {
            let field = if self.case_sensitive {
                resolved.field_by_name(name)
            } else {
                resolved.field_by_name_case_insensitive(name)
            };
            let field = field.ok_or_else(|| {
                data_invalid(format!(
                    "Cannot add field {name} as an identifier field: not found in current schema or \
                     added columns"
                ))
            })?;
            identifier_field_ids.insert(field.id);
        }

        // 4. Final build with identifier ids — `Schema::builder` enforces the spec's identifier rules
        //    (required, primitive, not nested in map/list/optional-struct).
        Schema::builder()
            .with_fields(new_struct.fields().iter().cloned())
            .with_identifier_field_ids(identifier_field_ids)
            .build()
    }

    /// Recursively rebuild a struct, applying field-level deletes/updates and parent-scoped adds/moves.
    /// `parent_id` is the field id of the struct's owner (TABLE_ROOT_ID at the top level), used to look
    /// up the adds and moves scoped to this struct.
    fn rebuild_struct(&self, struct_type: &StructType, parent_id: i32) -> Result<StructType> {
        let mut fields: Vec<NestedFieldRef> = Vec::with_capacity(struct_type.fields().len());
        for field in struct_type.fields() {
            if self.deletes.contains(&field.id) {
                continue;
            }
            let rebuilt = self.rebuild_field(field)?;
            fields.push(Arc::new(rebuilt));
        }

        // Apply adds for this struct (appended in add order), then moves (in call order).
        if let Some(added_ids) = self.parent_to_added_ids.get(&parent_id) {
            for added_id in added_ids {
                let added = self.added_fields.get(added_id).ok_or_else(|| {
                    data_invalid(format!("Internal error: added field {added_id} missing"))
                })?;
                fields.push(Arc::new(added.clone()));
            }
        }
        if let Some(moves) = self.moves.get(&parent_id) {
            fields = apply_moves(fields, moves)?;
        }

        Ok(StructType::new(fields))
    }

    /// Rebuild a single field: apply its pending update (rename/type/doc/nullability) and recurse into
    /// nested struct/list/map children (so deeper deletes/updates/adds/moves take effect).
    fn rebuild_field(&self, field: &NestedFieldRef) -> Result<NestedField> {
        // Start from the pending update if present (carries rename/doc/nullability/promoted type).
        let base = self
            .updates
            .get(&field.id)
            .cloned()
            .unwrap_or_else(|| (**field).clone());

        let new_type = self.rebuild_type(field.id, &base.field_type)?;
        let mut rebuilt = base;
        *rebuilt.field_type = new_type;
        Ok(rebuilt)
    }

    /// Recurse into a field's type, rebuilding nested structures. The `owner_id` is the field id that
    /// owns this type (used to scope adds/moves on a nested struct).
    fn rebuild_type(&self, owner_id: i32, field_type: &Type) -> Result<Type> {
        match field_type {
            Type::Primitive(primitive) => Ok(Type::Primitive(primitive.clone())),
            // Leaf, like a primitive — Java 1.10.0 `SchemaUpdate$ApplyChanges.variant` returns
            // the type unchanged (bytecode: `aload_1; areturn`).
            Type::Variant => Ok(Type::Variant),
            Type::Struct(struct_type) => {
                Ok(Type::Struct(self.rebuild_struct(struct_type, owner_id)?))
            }
            Type::List(list_type) => {
                let element = &list_type.element_field;
                if self.deletes.contains(&element.id) {
                    return Err(data_invalid(format!(
                        "Cannot delete element type from list: {field_type}"
                    )));
                }
                let element_field = self.rebuild_field(element)?;
                Ok(Type::List(ListType::new(Arc::new(element_field))))
            }
            Type::Map(map_type) => {
                let key = &map_type.key_field;
                // Map keys are immutable: no delete / update / add to key (Java `ApplyChanges.map`).
                if self.deletes.contains(&key.id) {
                    return Err(data_invalid(format!(
                        "Cannot delete map keys: {field_type}"
                    )));
                }
                if self.updates.contains_key(&key.id) {
                    return Err(data_invalid(format!(
                        "Cannot update map keys: {field_type}"
                    )));
                }
                if self.parent_to_added_ids.contains_key(&key.id) {
                    return Err(data_invalid(format!(
                        "Cannot add fields to map keys: {field_type}"
                    )));
                }
                let value = &map_type.value_field;
                if self.deletes.contains(&value.id) {
                    return Err(data_invalid(format!(
                        "Cannot delete value type from map: {field_type}"
                    )));
                }
                // Keys may still contain a nested struct that itself changed; rebuild it but reject any
                // change to the key type shape via the immutability checks above.
                let key_type = self.rebuild_type(key.id, &key.field_type)?;
                let value_field = self.rebuild_field(value)?;
                Ok(Type::Map(MapType::new(
                    Arc::new(NestedField::map_key_element(key.id, key_type)),
                    Arc::new(value_field),
                )))
            }
        }
    }
}

/// The three move flavors carried into [`SchemaEvolution::apply_move`].
enum MoveKind<'a> {
    First,
    Before(&'a str),
    After(&'a str),
}

/// Build a field-id → parent-field-id map for a struct tree, the local equivalent of the schema
/// module's (crate-private) `index_parents`. A field's parent is the id of the nearest enclosing
/// struct/list/map field; top-level fields have no entry.
///
/// Recursion is used because the schema is a genuinely recursive tree; Iceberg schemas are shallow
/// (depth is bounded by the nesting the user authored, in practice well under a hundred), so stack
/// overflow is not a concern and an explicit stack would only obscure the parent-tracking.
fn index_parents(struct_type: &StructType, parent_id: Option<i32>, out: &mut HashMap<i32, i32>) {
    for field in struct_type.fields() {
        if let Some(parent_id) = parent_id {
            out.insert(field.id, parent_id);
        }
        index_parents_in_type(field.id, &field.field_type, out);
    }
}

/// Recurse into a field's type to record parent links for its nested children.
fn index_parents_in_type(owner_id: i32, field_type: &Type, out: &mut HashMap<i32, i32>) {
    match field_type {
        Type::Primitive(_) => {}
        // Leaf, like a primitive — variant has no nested fields to parent-link (Java 1.10.0
        // `IndexParents.variant` returns the map unchanged).
        Type::Variant => {}
        Type::Struct(struct_type) => index_parents(struct_type, Some(owner_id), out),
        Type::List(list_type) => {
            let element = &list_type.element_field;
            out.insert(element.id, owner_id);
            index_parents_in_type(element.id, &element.field_type, out);
        }
        Type::Map(map_type) => {
            let key = &map_type.key_field;
            let value = &map_type.value_field;
            out.insert(key.id, owner_id);
            out.insert(value.id, owner_id);
            index_parents_in_type(key.id, &key.field_type, out);
            index_parents_in_type(value.id, &value.field_type, out);
        }
    }
}

/// Reorder a struct's fields per a list of moves applied in order (Java `moveFields`). Each move pulls
/// its field out and re-inserts it at the requested position; later moves observe earlier ones.
fn apply_moves(fields: Vec<NestedFieldRef>, moves: &[Move]) -> Result<Vec<NestedFieldRef>> {
    let mut reordered = fields;
    for move_op in moves {
        let from = reordered
            .iter()
            .position(|field| field.id == move_op.field_id())
            .ok_or_else(|| {
                data_invalid(format!(
                    "Cannot move field {}: not found in struct",
                    move_op.field_id()
                ))
            })?;
        let field = reordered.remove(from);
        match move_op {
            Move::First { .. } => reordered.insert(0, field),
            Move::Before { reference_id, .. } => {
                let index = reordered
                    .iter()
                    .position(|f| f.id == *reference_id)
                    .ok_or_else(|| {
                        data_invalid(format!(
                            "Cannot move before field {reference_id}: not found in struct"
                        ))
                    })?;
                reordered.insert(index, field);
            }
            Move::After { reference_id, .. } => {
                let index = reordered
                    .iter()
                    .position(|f| f.id == *reference_id)
                    .ok_or_else(|| {
                        data_invalid(format!(
                            "Cannot move after field {reference_id}: not found in struct"
                        ))
                    })?;
                reordered.insert(index + 1, field);
            }
        }
    }
    Ok(reordered)
}

#[async_trait]
impl TransactionAction for UpdateSchemaAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let metadata = table.metadata();
        let schema = metadata.current_schema();
        let current_schema_id = metadata.current_schema_id();
        let last_column_id = metadata.last_column_id();

        let mut evolution = SchemaEvolution::new(
            schema,
            last_column_id,
            self.case_sensitive,
            self.allow_incompatible_changes,
        )?;

        for op in &self.ops {
            evolution.replay(op)?;
        }

        let new_schema = evolution.apply()?;

        let updates = vec![
            TableUpdate::AddSchema { schema: new_schema },
            TableUpdate::SetCurrentSchema {
                schema_id: LAST_ADDED_SCHEMA_ID,
            },
        ];

        // Optimistic-concurrency guards. Java's `UpdateRequirements` attaches
        // `AssertLastAssignedFieldId(base.lastColumnId())` for the `AddSchema` update and
        // `AssertCurrentSchemaID(base.currentSchemaId())` for the `SetCurrentSchema` update.
        let requirements = vec![
            TableRequirement::LastAssignedFieldIdMatch {
                last_assigned_field_id: last_column_id,
            },
            TableRequirement::CurrentSchemaIdMatch { current_schema_id },
        ];

        Ok(ActionCommit::new(updates, requirements))
    }
}

impl SchemaEvolution<'_> {
    /// Replay a single recorded op against the evolution state.
    fn replay(&mut self, op: &SchemaOp) -> Result<()> {
        match op {
            SchemaOp::AddColumn {
                parent,
                name,
                field_type,
                required,
                doc,
                default,
            } => {
                let parent = match parent.as_deref() {
                    Some(AMBIGUOUS_TOP_LEVEL_MARKER) => {
                        if name.contains('.') {
                            return Err(data_invalid(format!(
                                "Cannot add column with ambiguous name: {name}, use \
                                 add_column_to(parent, name, type)"
                            )));
                        }
                        None
                    }
                    other => other,
                };
                self.add_column(
                    parent,
                    name,
                    field_type.clone(),
                    *required,
                    doc.clone(),
                    default.clone(),
                )
            }
            SchemaOp::RenameColumn { name, new_name } => self.rename_column(name, new_name),
            SchemaOp::UpdateColumn { name, new_type } => self.update_column(name, new_type.clone()),
            SchemaOp::UpdateColumnDoc { name, doc } => self.update_column_doc(name, doc.clone()),
            SchemaOp::UpdateColumnDefault { name, default } => {
                self.update_column_default(name, default)
            }
            SchemaOp::UpdateColumnRequirement { name, is_optional } => {
                self.update_column_requirement(name, *is_optional)
            }
            SchemaOp::DeleteColumn { name } => self.delete_column(name),
            SchemaOp::MoveFirst { name } => self.apply_move(name, MoveKind::First),
            SchemaOp::MoveBefore { name, reference } => {
                self.apply_move(name, MoveKind::Before(reference))
            }
            SchemaOp::MoveAfter { name, reference } => {
                self.apply_move(name, MoveKind::After(reference))
            }
            SchemaOp::SetIdentifierFields { names } => {
                self.set_identifier_fields(names);
                Ok(())
            }
            SchemaOp::UnionByName { other } => self.union_by_name(other),
        }
    }

    /// Replay `unionByNameWith` (Java `UnionByNameVisitor`): accumulate the changes that evolve the
    /// current schema into the union of itself and `other`. For every field in `other`, add it if it is
    /// new (by canonical name), or — when it already exists — relax required→optional, apply any legal
    /// type promotion, update its doc, and recurse through structs / list elements / map key+values so
    /// nested additions and changes are merged too. Illegal type changes (e.g. an incompatible primitive
    /// change, or a complex column whose incoming type is a primitive) are routed through
    /// [`Self::update_column`], which raises the same "Cannot change column type" error Java does.
    fn union_by_name(&mut self, other: &Schema) -> Result<()> {
        let parent_path: Vec<String> = vec![];
        self.union_struct(other.as_struct(), &parent_path)
    }

    /// Recurse over a struct in the incoming union schema, keyed by dotted canonical name. Mirrors Java
    /// `UnionByNameVisitor.struct`: a missing field is added, an existing field is updated + descended.
    fn union_struct(&mut self, struct_type: &StructType, parent_path: &[String]) -> Result<()> {
        for field in struct_type.fields() {
            let mut path = parent_path.to_vec();
            path.push(field.name.clone());
            let full_name = path.join(".");
            let parent_name = if parent_path.is_empty() {
                None
            } else {
                Some(parent_path.join("."))
            };

            match self.find_for_update(&full_name) {
                None => {
                    // New field: add it (optional, mirroring union's compatible-add policy, no default).
                    // Its own nested fields come along inside `field_type`, so do not also recurse here.
                    self.add_column(
                        parent_name.as_deref(),
                        &field.name,
                        (*field.field_type).clone(),
                        false,
                        field.doc.clone(),
                        None,
                    )?;
                }
                Some(existing) => {
                    self.union_update_existing(&full_name, &existing, field)?;
                    self.union_recurse_into(&full_name, field)?;
                }
            }
        }
        Ok(())
    }

    /// Apply Java `UnionByNameVisitor.updateColumn` to an existing field: relax required→optional, apply
    /// a non-ignorable type change (routed through [`Self::update_column`], which errors on an illegal
    /// one), and update the doc. `incoming` is the matching field in the union schema.
    fn union_update_existing(
        &mut self,
        full_name: &str,
        existing: &NestedField,
        incoming: &NestedField,
    ) -> Result<()> {
        // required -> optional widening (Java `needsOptionalUpdate`).
        if !incoming.required && existing.required {
            self.update_column_requirement(full_name, true)?;
        }

        // Type change (Java `needsTypeUpdate = !isIgnorableTypeUpdate`). An ignorable change is skipped;
        // a non-ignorable one is routed through `update_column`, which both applies a legal promotion and
        // raises "Cannot change column type" for an illegal one (incompatible primitive, or a complex
        // existing type whose incoming type is a primitive). `update_column` only accepts a primitive
        // target, matching Java's `field.type().asPrimitiveType()`.
        if !is_ignorable_type_update(&existing.field_type, &incoming.field_type) {
            let Type::Primitive(incoming_primitive) = incoming.field_type.as_ref() else {
                // Java calls `field.type().asPrimitiveType()` here, which throws for a complex incoming
                // type. The only way to reach this with a non-ignorable change is an existing primitive
                // whose incoming type is complex — reject it with the same shaped message.
                return Err(data_invalid(format!(
                    "Cannot change column type: {full_name}: {} -> {}",
                    existing.field_type, incoming.field_type
                )));
            };
            self.update_column(full_name, incoming_primitive.clone())?;
        }

        // Doc change (Java `needsDocUpdate`).
        if incoming.doc.is_some() && incoming.doc != existing.doc {
            self.update_column_doc(full_name, incoming.doc.clone())?;
        }
        Ok(())
    }

    /// Descend into an existing field's nested type, matching Java's partner traversal through structs,
    /// list elements, and map key+values, so additions and changes nested under a list/map/struct merge.
    /// The element / value partner is reached with the dotted accessor names Iceberg uses
    /// (`<path>.element`, `<path>.key`, `<path>.value`).
    fn union_recurse_into(&mut self, full_name: &str, incoming: &NestedField) -> Result<()> {
        match incoming.field_type.as_ref() {
            Type::Struct(nested) => {
                let path: Vec<String> = full_name.split('.').map(str::to_string).collect();
                self.union_struct(nested, &path)?;
            }
            Type::List(list) => {
                // The element may itself be (or contain) a struct that gained fields; recurse into it as a
                // single-field struct under `<path>.element`.
                let element = &list.element_field;
                self.union_nested_member(full_name, element)?;
            }
            Type::Map(map) => {
                // Recurse into key then value (Java updates both partner fields), under `<path>.key` /
                // `<path>.value`. A key type change is rejected by `update_column` on the map-key path.
                let key = &map.key_field;
                let value = &map.value_field;
                self.union_nested_member(full_name, key)?;
                self.union_nested_member(full_name, value)?;
            }
            Type::Primitive(_) => {}
            // Leaf, like a primitive — variant has no nested fields to merge into (Java 1.10.0
            // `UnionByNameVisitor.variant` only partner-compares; it never recurses).
            Type::Variant => {}
        }
        Ok(())
    }

    /// Apply the column-level update + recursion for a single synthetic nested member (a list element or
    /// a map key/value) of the field at `parent_full_name`. The member is matched to the existing field
    /// by its Iceberg member name (`element` / `key` / `value`) under the parent's dotted path.
    fn union_nested_member(
        &mut self,
        parent_full_name: &str,
        incoming_member: &NestedField,
    ) -> Result<()> {
        let member_full_name = format!("{parent_full_name}.{}", incoming_member.name);
        // The member must exist on the existing side (a list-present/map-present invariant in Java); if
        // it does not resolve, there is nothing to merge against, so skip rather than fabricate a column.
        if let Some(existing_member) = self.find_for_update(&member_full_name) {
            self.union_update_existing(&member_full_name, &existing_member, incoming_member)?;
            self.union_recurse_into(&member_full_name, incoming_member)?;
        }
        Ok(())
    }
}

/// Validate that `default` is a legal default value for a column of type `field_type`, mirroring Java's
/// `Types.NestedField` `castDefault`: a non-null default for a nested type is rejected, and a default for
/// a primitive must be convertible to that type.
///
/// The Rust `NestedField::with_initial_default` / `with_write_default` setters do not validate, but the
/// serde path (`Literal::try_into_json`) does — and panics if the value is incompatible. We therefore run
/// `try_into_json` here as the canonical compatibility check: passing it guarantees the written field can
/// be serialized without a later panic, and matches the set of `(literal, type)` pairings Java accepts.
fn validate_default(default: &Literal, field_type: &Type) -> Result<()> {
    // Java rejects a non-null default on a nested type (`type.isNestedType()`); everything that is not a
    // primitive (struct/list/map) is nested.
    if !field_type.is_primitive() {
        return Err(data_invalid(format!(
            "Invalid default value for {field_type}: {default:?} (must be null)"
        )));
    }
    default
        .clone()
        .try_into_json(field_type)
        .map_err(|error| {
            data_invalid(format!(
                "Cannot cast default value to {field_type}: {default:?} ({error})"
            ))
        })
        .map(|_| ())
}

/// Java `UnionByNameVisitor.isIgnorableTypeUpdate`: decide whether the change from `existing_type` to
/// `incoming_type` can be skipped during a union merge. A change is ignorable when:
///
/// - `existing` is primitive and `incoming` is a primitive that is a *narrowing* of `existing` (Java
///   reuses `isPromotionAllowed(incoming, existing)` in reverse: `true` means incoming is narrower, so the
///   wider existing type is kept and the change ignored); or
/// - `existing` is complex (struct/list/map) and `incoming` is also complex (the recursion handles any
///   inner change).
///
/// A widening primitive change, or any mismatch between a primitive and a complex shape, is NOT ignorable
/// — the caller then routes it through `update_column`, which applies a legal promotion or errors.
fn is_ignorable_type_update(existing_type: &Type, incoming_type: &Type) -> bool {
    match existing_type {
        Type::Primitive(existing_primitive) => match incoming_type {
            Type::Primitive(incoming_primitive) => {
                // Identical primitive is trivially ignorable; otherwise ignorable only when the incoming
                // type is a narrowing of the existing one (the wider existing type wins).
                existing_primitive == incoming_primitive
                    || crate::spec::is_promotion_allowed(incoming_type, existing_primitive)
            }
            _ => false,
        },
        // existing is complex: ignorable iff incoming is also complex.
        _ => !matches!(incoming_type, Type::Primitive(_)),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::spec::{
        ListType, MapType, NestedField, PrimitiveType, Schema, StructType, TableMetadata, Type,
    };
    use crate::table::Table;
    use crate::transaction::{Transaction, TransactionAction};

    fn v2_table() -> Table {
        crate::transaction::tests::make_v2_table()
    }

    /// A from-scratch **V3** table with the same `x`/`y`/`z` (long, required) shape as `v2_table`
    /// (last_column_id 3). Built at format version 3 so column INITIAL defaults are legal — Java's
    /// `Schema.checkCompatibility` (mirrored by `Schema::check_compatibility`, enforced in
    /// `TableMetadataBuilder::add_schema`) rejects a non-null initial default on a v1/v2 table.
    fn v3_table() -> Table {
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "x", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "y", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(3, "z", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("build v3 base schema");

        let metadata = crate::spec::TableMetadataBuilder::new(
            schema,
            crate::spec::PartitionSpec::unpartition_spec(),
            crate::spec::SortOrder::unsorted_order(),
            "s3://bucket/v3".to_string(),
            crate::spec::FormatVersion::V3,
            std::collections::HashMap::new(),
        )
        .expect("build v3 metadata builder")
        .build()
        .expect("build v3 metadata")
        .metadata;

        v2_table().with_metadata(Arc::new(metadata))
    }

    /// Extract the rebuilt schema from the emitted `AddSchema` update.
    fn added_schema(updates: &[TableUpdate]) -> &Schema {
        updates
            .iter()
            .find_map(|u| match u {
                TableUpdate::AddSchema { schema } => Some(schema),
                _ => None,
            })
            .expect("an AddSchema update")
    }

    /// Drive the emitted updates through the table's `TableMetadataBuilder`, returning the new metadata.
    fn apply_updates(table: &Table, updates: Vec<TableUpdate>) -> TableMetadata {
        let mut builder = table.metadata().clone().into_builder(None);
        for update in updates {
            builder = update.apply(builder).expect("apply table update");
        }
        builder.build().expect("build metadata").metadata
    }

    /// Run an action against the v2 table and return its committed updates (panicking on error).
    async fn run(action: UpdateSchemaAction, table: &Table) -> Vec<TableUpdate> {
        let mut commit = Arc::new(action)
            .commit(table)
            .await
            .expect("action should commit");
        commit.take_updates()
    }

    /// A nested fixture built from scratch (so no stale sort order / partition spec / identifier set
    /// from the v2 fixture interferes): top-level `id` (long, required), `data` (string, optional),
    /// `location` (required struct of `lat`/`long` floats), `tags` (optional list<string>), `props`
    /// (optional map<string,string>). `TableMetadataBuilder::new` reassigns ids top-down in document
    /// order, which lands exactly on id=1..10 (last_column_id 10) as hand-assigned below. No identifier
    /// fields, unpartitioned, unsorted.
    fn nested_table() -> Table {
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "data", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(
                    3,
                    "location",
                    Type::Struct(StructType::new(vec![
                        NestedField::required(4, "lat", Type::Primitive(PrimitiveType::Float))
                            .into(),
                        NestedField::required(5, "long", Type::Primitive(PrimitiveType::Float))
                            .into(),
                    ])),
                )
                .into(),
                NestedField::optional(
                    6,
                    "tags",
                    Type::List(ListType::new(
                        NestedField::list_element(7, Type::Primitive(PrimitiveType::String), true)
                            .into(),
                    )),
                )
                .into(),
                NestedField::optional(
                    8,
                    "props",
                    Type::Map(MapType::new(
                        NestedField::map_key_element(9, Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::map_value_element(
                            10,
                            Type::Primitive(PrimitiveType::String),
                            true,
                        )
                        .into(),
                    )),
                )
                .into(),
            ])
            .build()
            .expect("build nested schema");

        let metadata = crate::spec::TableMetadataBuilder::new(
            schema,
            crate::spec::PartitionSpec::unpartition_spec(),
            crate::spec::SortOrder::unsorted_order(),
            "s3://bucket/nested".to_string(),
            crate::spec::FormatVersion::V2,
            std::collections::HashMap::new(),
        )
        .expect("build nested metadata builder")
        .build()
        .expect("build nested metadata")
        .metadata;

        v2_table().with_metadata(Arc::new(metadata))
    }

    // ----- Baseline / no-op -----

    // RISK: an empty update must still rebuild a schema structurally equal to the current one — a
    // spurious add/drop here would corrupt every column.
    #[tokio::test]
    async fn test_no_op_rebuilds_equal_schema() {
        let table = v2_table();
        let updates = run(UpdateSchemaAction::new(), &table).await;
        let schema = added_schema(&updates);
        assert_eq!(
            schema.as_struct(),
            table.metadata().current_schema().as_struct()
        );
    }

    // ----- addColumn: top-level -----

    // RISK: a top-level add must append an OPTIONAL column with the next field id (last_column_id+1) —
    // a wrong id or wrong nullability silently breaks readers of older data.
    #[tokio::test]
    async fn test_add_optional_top_level_column_gets_next_id() {
        let table = v2_table(); // x,y,z long; last_column_id 3
        let updates = run(
            UpdateSchemaAction::new().add_column("w", Type::Primitive(PrimitiveType::Int)),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        let field = schema.field_by_name("w").expect("new column w");
        assert_eq!(field.id, 4, "new column must get last_column_id+1");
        assert!(!field.required, "addColumn adds an optional column");
        assert_eq!(
            field.field_type.as_ref(),
            &Type::Primitive(PrimitiveType::Int)
        );
    }

    // RISK: the `.`-rejecting top-level add must reject an ambiguous name — letting it through would
    // create a column whose name collides with path syntax.
    #[tokio::test]
    async fn test_add_top_level_dotted_name_rejected() {
        let table = v2_table();
        let result = Arc::new(
            UpdateSchemaAction::new().add_column("a.b", Type::Primitive(PrimitiveType::Int)),
        )
        .commit(&table)
        .await;
        assert!(result.is_err(), "dotted top-level name must be rejected");
    }

    // RISK: adding a name that already exists must be rejected (no silent shadowing).
    #[tokio::test]
    async fn test_add_duplicate_top_level_name_rejected() {
        let table = v2_table();
        let result = Arc::new(
            UpdateSchemaAction::new().add_column("x", Type::Primitive(PrimitiveType::Int)),
        )
        .commit(&table)
        .await;
        assert!(result.is_err(), "duplicate name must be rejected");
    }

    // ----- addColumn: nested + field-id reassignment -----

    // RISK: adding a column into a nested struct must place it inside that struct with a fresh id; a
    // top-level placement or id collision corrupts the struct.
    #[tokio::test]
    async fn test_add_column_into_nested_struct() {
        let table = nested_table(); // last_column_id 10
        let updates = run(
            UpdateSchemaAction::new().add_column_to(
                Some("location"),
                "altitude",
                Type::Primitive(PrimitiveType::Float),
                Some("meters above sea level"),
            ),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        let altitude = schema
            .field_by_name("location.altitude")
            .expect("nested altitude field");
        assert_eq!(altitude.id, 11, "nested add gets last_column_id+1");
        assert_eq!(altitude.doc.as_deref(), Some("meters above sea level"));
    }

    // RISK: adding a NESTED-TYPE column must REASSIGN the inner field ids from last_column_id+1 — the
    // passed-in type's ids must be ignored, or two columns could share a field id.
    #[tokio::test]
    async fn test_add_nested_struct_reassigns_inner_ids() {
        let table = v2_table(); // last_column_id 3
        let added = Type::Struct(StructType::new(vec![
            // deliberately colliding ids 1,2 in the passed-in type
            NestedField::required(1, "a", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "b", Type::Primitive(PrimitiveType::String)).into(),
        ]));
        let updates = run(
            UpdateSchemaAction::new().add_column_to(None, "nested", added, None),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        let nested = schema.field_by_name("nested").expect("nested struct");
        assert_eq!(nested.id, 4, "the struct itself gets id 4");
        let a = schema.field_by_name("nested.a").expect("nested.a");
        let b = schema.field_by_name("nested.b").expect("nested.b");
        assert_eq!(
            a.id, 5,
            "inner ids reassigned, not reused from the passed type"
        );
        assert_eq!(b.id, 6);
    }

    // RISK: adding a map-of-struct column must reassign EVERY inner id (key, value, value-struct
    // fields) sequentially — a missed reassignment collides with existing ids.
    #[tokio::test]
    async fn test_add_map_of_struct_reassigns_all_ids() {
        let table = v2_table(); // last_column_id 3
        let value_struct = Type::Struct(StructType::new(vec![
            NestedField::required(100, "n", Type::Primitive(PrimitiveType::Int)).into(),
        ]));
        let map = Type::Map(MapType::new(
            NestedField::map_key_element(101, Type::Primitive(PrimitiveType::String)).into(),
            NestedField::map_value_element(102, value_struct, true).into(),
        ));
        let updates = run(
            UpdateSchemaAction::new().add_column_to(None, "m", map, None),
            &table,
        )
        .await;
        let metadata = apply_updates(&table, updates);
        // The map column id 4, key 5, value 6, value-struct field 7 -> last_column_id advances to 7.
        assert_eq!(
            metadata.last_column_id(),
            7,
            "all four new ids assigned sequentially"
        );
    }

    // ----- addRequiredColumn + allow_incompatible_changes gating -----

    // RISK: adding a REQUIRED column without a default and WITHOUT the flag must be rejected — letting
    // it through would break reads of pre-existing rows that lack the column.
    #[tokio::test]
    async fn test_add_required_column_without_flag_rejected() {
        let table = v2_table();
        let result = Arc::new(
            UpdateSchemaAction::new().add_required_column("w", Type::Primitive(PrimitiveType::Int)),
        )
        .commit(&table)
        .await;
        assert!(
            result.is_err(),
            "required add without the flag must be rejected"
        );
    }

    // RISK: with allow_incompatible_changes the required add must succeed and produce a REQUIRED field —
    // the gate must actually open, not just suppress the error.
    #[tokio::test]
    async fn test_add_required_column_with_flag_succeeds() {
        let table = v2_table();
        let updates = run(
            UpdateSchemaAction::new()
                .allow_incompatible_changes()
                .add_required_column("w", Type::Primitive(PrimitiveType::Int)),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        let field = schema.field_by_name("w").expect("required column w");
        assert!(field.required, "the added column must be required");
    }

    // ----- renameColumn -----

    // RISK: rename must change the name but PRESERVE the field id (so data stays addressable) and type.
    #[tokio::test]
    async fn test_rename_preserves_id_and_type() {
        let table = v2_table();
        let updates = run(
            UpdateSchemaAction::new().rename_column("y", "y_renamed"),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        let field = schema.field_by_name("y_renamed").expect("renamed column");
        assert_eq!(field.id, 2, "rename preserves the field id");
        assert_eq!(
            field.field_type.as_ref(),
            &Type::Primitive(PrimitiveType::Long)
        );
        assert!(schema.field_by_name("y").is_none(), "old name must be gone");
    }

    // RISK: renaming a missing column must fail loudly, not silently no-op.
    #[tokio::test]
    async fn test_rename_missing_column_rejected() {
        let table = v2_table();
        let result = Arc::new(UpdateSchemaAction::new().rename_column("nope", "x2"))
            .commit(&table)
            .await;
        assert!(result.is_err());
    }

    // ----- updateColumn: type promotion accept + reject -----

    // RISK: a legal widening (int->long here uses long->long no-op; use a real widening on a fresh int
    // column) must succeed. We add an int column then promote it to long in the same transaction.
    #[tokio::test]
    async fn test_update_column_promotes_int_to_long() {
        let table = v2_table();
        let updates = run(
            UpdateSchemaAction::new()
                .add_column("n", Type::Primitive(PrimitiveType::Int))
                .update_column("n", PrimitiveType::Long),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        let field = schema.field_by_name("n").expect("promoted column");
        assert_eq!(
            field.field_type.as_ref(),
            &Type::Primitive(PrimitiveType::Long)
        );
    }

    // RISK (the headline promotion risk): a FORBIDDEN type change (long->int narrowing) must be
    // rejected — even though allow_incompatible_changes is set, since promotion is never relaxed.
    #[tokio::test]
    async fn test_update_column_rejects_narrowing_even_with_flag() {
        let table = v2_table(); // x is long
        let result = Arc::new(
            UpdateSchemaAction::new()
                .allow_incompatible_changes()
                .update_column("x", PrimitiveType::Int),
        )
        .commit(&table)
        .await;
        assert!(
            result.is_err(),
            "narrowing must be rejected even with the flag"
        );
    }

    // RISK: updating a missing column must fail.
    #[tokio::test]
    async fn test_update_missing_column_rejected() {
        let table = v2_table();
        let result = Arc::new(UpdateSchemaAction::new().update_column("nope", PrimitiveType::Long))
            .commit(&table)
            .await;
        assert!(result.is_err());
    }

    // ----- updateColumnDoc -----

    // RISK: updating a doc must change ONLY the doc, preserving id/type/nullability.
    #[tokio::test]
    async fn test_update_column_doc_preserves_metadata() {
        let table = v2_table();
        let updates = run(
            UpdateSchemaAction::new().update_column_doc("y", Some("the y column")),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        let field = schema.field_by_name("y").expect("y");
        assert_eq!(field.doc.as_deref(), Some("the y column"));
        assert_eq!(field.id, 2);
        assert!(field.required);
    }

    // RISK: updating the doc of a missing column must fail.
    #[tokio::test]
    async fn test_update_doc_missing_column_rejected() {
        let table = v2_table();
        let result = Arc::new(UpdateSchemaAction::new().update_column_doc("nope", Some("d")))
            .commit(&table)
            .await;
        assert!(result.is_err());
    }

    // ----- makeColumnOptional / requireColumn -----

    // RISK: making a required column optional must always be allowed and must flip the flag. Uses the
    // nested fixture (no identifier fields) so the change is not blocked by the spec's required-identifier
    // rule — that interaction is covered separately by the identifier-field tests.
    #[tokio::test]
    async fn test_make_column_optional() {
        let table = nested_table(); // id required, not an identifier field
        let updates = run(UpdateSchemaAction::new().make_column_optional("id"), &table).await;
        let schema = added_schema(&updates);
        assert!(!schema.field_by_name("id").expect("id").required);
    }

    // RISK: requireColumn on an OPTIONAL field without the flag must be rejected (incompatible change).
    #[tokio::test]
    async fn test_require_optional_column_without_flag_rejected() {
        let table = nested_table(); // data is optional
        let result = Arc::new(UpdateSchemaAction::new().require_column("data"))
            .commit(&table)
            .await;
        assert!(
            result.is_err(),
            "optional->required without flag must be rejected"
        );
    }

    // RISK: requireColumn on an ALREADY-required field must be a no-op that succeeds WITHOUT the flag —
    // forgetting this no-op path wrongly rejects a legal operation.
    #[tokio::test]
    async fn test_require_already_required_column_is_noop() {
        let table = v2_table(); // x required
        let updates = run(UpdateSchemaAction::new().require_column("x"), &table).await;
        let schema = added_schema(&updates);
        assert!(schema.field_by_name("x").expect("x").required);
    }

    // RISK: requireColumn on an optional field WITH the flag must succeed and make it required.
    #[tokio::test]
    async fn test_require_optional_column_with_flag_succeeds() {
        let table = nested_table();
        let updates = run(
            UpdateSchemaAction::new()
                .allow_incompatible_changes()
                .require_column("data"),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        assert!(schema.field_by_name("data").expect("data").required);
    }

    // ----- deleteColumn -----

    // RISK: deleting a top-level column must remove it (and only it) from the result.
    #[tokio::test]
    async fn test_delete_column_removes_field() {
        let table = v2_table();
        let updates = run(UpdateSchemaAction::new().delete_column("z"), &table).await;
        let schema = added_schema(&updates);
        assert!(schema.field_by_name("z").is_none(), "z must be deleted");
        assert!(schema.field_by_name("x").is_some(), "x must survive");
        assert!(schema.field_by_name("y").is_some(), "y must survive");
    }

    // RISK: deleting a nested field must remove only that field, leaving siblings intact.
    #[tokio::test]
    async fn test_delete_nested_field_keeps_siblings() {
        let table = nested_table();
        let updates = run(
            UpdateSchemaAction::new().delete_column("location.lat"),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        assert!(schema.field_by_name("location.lat").is_none());
        assert!(schema.field_by_name("location.long").is_some());
    }

    // RISK: deleting a missing column must fail.
    #[tokio::test]
    async fn test_delete_missing_column_rejected() {
        let table = v2_table();
        let result = Arc::new(UpdateSchemaAction::new().delete_column("nope"))
            .commit(&table)
            .await;
        assert!(result.is_err());
    }

    // RISK: delete-then-add of the same name must succeed and give the re-added column a FRESH id (not
    // the deleted one) — reusing the old id would alias old data to the new column.
    #[tokio::test]
    async fn test_delete_then_add_same_name_gets_fresh_id() {
        let table = v2_table(); // last_column_id 3
        let updates = run(
            UpdateSchemaAction::new()
                .delete_column("z")
                .add_column("z", Type::Primitive(PrimitiveType::String)),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        let field = schema.field_by_name("z").expect("re-added z");
        assert_eq!(
            field.id, 4,
            "re-added column must get a fresh id, not the old id 3"
        );
        assert_eq!(
            field.field_type.as_ref(),
            &Type::Primitive(PrimitiveType::String)
        );
    }

    // ----- map-key immutability -----

    // RISK (absolute rule): deleting a map key must be rejected unconditionally — map keys are immutable.
    #[tokio::test]
    async fn test_delete_map_key_rejected() {
        let table = nested_table();
        let result = Arc::new(UpdateSchemaAction::new().delete_column("props.key"))
            .commit(&table)
            .await;
        assert!(result.is_err(), "deleting a map key must be rejected");
    }

    // RISK: deleting a map value type must be rejected (the value type cannot be removed entirely).
    #[tokio::test]
    async fn test_delete_map_value_rejected() {
        let table = nested_table();
        let result = Arc::new(UpdateSchemaAction::new().delete_column("props.value"))
            .commit(&table)
            .await;
        assert!(result.is_err(), "deleting a map value must be rejected");
    }

    // ----- conflict matrix -----

    // RISK: deleting a column that has a pending update must be rejected (Java conflict guard).
    #[tokio::test]
    async fn test_update_then_delete_same_field_rejected() {
        let table = v2_table();
        let result = Arc::new(
            UpdateSchemaAction::new()
                .rename_column("y", "y2")
                .delete_column("y"),
        )
        .commit(&table)
        .await;
        assert!(
            result.is_err(),
            "delete after rename on the same field must be rejected"
        );
    }

    // RISK: renaming a column that is already marked for deletion must be rejected.
    #[tokio::test]
    async fn test_delete_then_rename_same_field_rejected() {
        let table = v2_table();
        let result = Arc::new(
            UpdateSchemaAction::new()
                .delete_column("y")
                .rename_column("y", "y2"),
        )
        .commit(&table)
        .await;
        assert!(
            result.is_err(),
            "rename of a deleted field must be rejected"
        );
    }

    // ----- moves -----

    // RISK: moveFirst must place the column at position 0 of its struct.
    #[tokio::test]
    async fn test_move_first() {
        let table = v2_table(); // order x,y,z
        let updates = run(UpdateSchemaAction::new().move_first("z"), &table).await;
        let schema = added_schema(&updates);
        assert_eq!(schema.as_struct().fields()[0].name, "z", "z must be first");
    }

    // RISK: moveBefore/moveAfter must position the column relative to the reference; MULTIPLE moves must
    // apply in call order (not sorted) — the wrong order produces a different layout.
    #[tokio::test]
    async fn test_move_before_and_after_in_call_order() {
        let table = v2_table(); // x,y,z
        let updates = run(
            UpdateSchemaAction::new()
                .move_after("x", "z") // -> y,z,x
                .move_before("y", "x"), // -> z,y,x
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        let names: Vec<&str> = schema
            .as_struct()
            .fields()
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        assert_eq!(names, vec!["z", "y", "x"], "moves apply in call order");
    }

    // RISK: a self-referential move must be rejected (Java "Cannot move x before itself").
    #[tokio::test]
    async fn test_move_before_itself_rejected() {
        let table = v2_table();
        let result = Arc::new(UpdateSchemaAction::new().move_before("x", "x"))
            .commit(&table)
            .await;
        assert!(result.is_err());
    }

    // RISK: moving a missing column must be rejected.
    #[tokio::test]
    async fn test_move_missing_column_rejected() {
        let table = v2_table();
        let result = Arc::new(UpdateSchemaAction::new().move_first("nope"))
            .commit(&table)
            .await;
        assert!(result.is_err());
    }

    // RISK: moving a top-level field to reference a field in a DIFFERENT struct must be rejected — moves
    // are struct-local.
    #[tokio::test]
    async fn test_move_across_struct_boundary_rejected() {
        let table = nested_table();
        // move top-level "id" before the nested "location.lat" -> different struct
        let result = Arc::new(UpdateSchemaAction::new().move_before("id", "location.lat"))
            .commit(&table)
            .await;
        assert!(result.is_err(), "cross-struct move must be rejected");
    }

    // RISK: a newly added column can be moved in the same transaction (add must precede move).
    #[tokio::test]
    async fn test_move_newly_added_column() {
        let table = v2_table();
        let updates = run(
            UpdateSchemaAction::new()
                .add_column("w", Type::Primitive(PrimitiveType::Int))
                .move_first("w"),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        assert_eq!(
            schema.as_struct().fields()[0].name,
            "w",
            "added column moved first"
        );
    }

    // ----- setIdentifierFields -----

    // RISK: setting an identifier field on a required primitive must succeed and record the field id.
    #[tokio::test]
    async fn test_set_identifier_field_on_required_primitive() {
        let table = v2_table(); // x required long
        let updates = run(
            UpdateSchemaAction::new().set_identifier_fields(["x"]),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        let ids: Vec<i32> = schema.identifier_field_ids().collect();
        assert_eq!(ids, vec![1], "x (id 1) must be the identifier field");
    }

    // RISK: an identifier field that does not exist must be rejected.
    #[tokio::test]
    async fn test_set_identifier_field_unknown_rejected() {
        let table = v2_table();
        let result = Arc::new(UpdateSchemaAction::new().set_identifier_fields(["nope"]))
            .commit(&table)
            .await;
        assert!(result.is_err(), "unknown identifier field must be rejected");
    }

    // RISK: an OPTIONAL field cannot be an identifier field (nulls in identifiers are forbidden).
    #[tokio::test]
    async fn test_set_identifier_field_optional_rejected() {
        let table = nested_table(); // data optional
        let result = Arc::new(UpdateSchemaAction::new().set_identifier_fields(["data"]))
            .commit(&table)
            .await;
        assert!(
            result.is_err(),
            "optional identifier field must be rejected"
        );
    }

    // RISK: a FLOAT field cannot be an identifier field (float/double are forbidden).
    #[tokio::test]
    async fn test_set_identifier_field_float_rejected() {
        let table = nested_table();
        // location.lat is a required float nested in a required struct -> float rule must reject it.
        let result = Arc::new(
            UpdateSchemaAction::new()
                .require_column("location") // already required; no-op
                .set_identifier_fields(["location.lat"]),
        )
        .commit(&table)
        .await;
        assert!(result.is_err(), "float identifier field must be rejected");
    }

    // RISK: setting an identifier field on a column ADDED in the same transaction must work (order: add
    // then set). The added column must be required.
    #[tokio::test]
    async fn test_set_identifier_field_on_added_column() {
        let table = v2_table();
        let updates = run(
            UpdateSchemaAction::new()
                .allow_incompatible_changes()
                .add_required_column("key", Type::Primitive(PrimitiveType::Long))
                .set_identifier_fields(["key"]),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        let key = schema.field_by_name("key").expect("key column");
        let ids: Vec<i32> = schema.identifier_field_ids().collect();
        assert_eq!(
            ids,
            vec![key.id],
            "the added required column is the identifier field"
        );
    }

    // RISK: clearing the identifier set must allow deleting a former identifier field — the
    // delete-identifier guard must observe the NEW (empty) identifier set, not the base one.
    #[tokio::test]
    async fn test_clear_identifier_then_delete_former_identifier_field() {
        // v2 table identifier ids are [1,2] = x,y. Clear them, then delete y.
        let table = v2_table();
        let updates = run(
            UpdateSchemaAction::new()
                .set_identifier_fields(Vec::<String>::new())
                .delete_column("y"),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        assert!(
            schema.field_by_name("y").is_none(),
            "y deleted after clearing identifiers"
        );
        assert_eq!(schema.identifier_field_ids().count(), 0);
    }

    // RISK: deleting a current identifier field WITHOUT clearing it must be rejected — silently
    // dropping an identifier corrupts row-identity semantics.
    #[tokio::test]
    async fn test_delete_identifier_field_without_clearing_rejected() {
        let table = v2_table(); // identifier ids [1,2] = x,y
        let result = Arc::new(UpdateSchemaAction::new().delete_column("y"))
            .commit(&table)
            .await;
        assert!(
            result.is_err(),
            "deleting an identifier field must be rejected"
        );
    }

    // ----- unionByName -----

    // RISK: union must ADD a new field from the incoming schema and PROMOTE an existing field's type
    // when the incoming type widens it, while leaving non-widening fields untouched.
    #[tokio::test]
    async fn test_union_adds_new_and_promotes_existing() {
        let table = v2_table(); // x,y,z long
        // incoming schema: x stays long, plus a new optional "w" string. (long has no widening, so use a
        // fresh int column to prove promotion separately below.)
        let incoming = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "x", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(99, "w", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("incoming schema");
        let updates = run(
            UpdateSchemaAction::new().union_by_name_with(incoming),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        assert!(
            schema.field_by_name("w").is_some(),
            "union must add the new field w"
        );
        assert!(schema.field_by_name("x").is_some(), "x preserved");
    }

    // RISK: union must promote int->long (widening), and must NOT downgrade when the incoming type is
    // narrower than the existing one (long stays long).
    #[tokio::test]
    async fn test_union_promotes_widening_keeps_wider_on_narrowing() {
        let table = v2_table(); // x is long
        // First add an int column "n" so we can prove promotion via union.
        let with_int = run(
            UpdateSchemaAction::new().add_column("n", Type::Primitive(PrimitiveType::Int)),
            &table,
        )
        .await;
        let intermediate = apply_updates(&table, with_int);
        let table = table.with_metadata(Arc::new(intermediate));

        // incoming: n widened to long, x narrowed to int (must be IGNORED -> x stays long).
        let incoming = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "x", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(4, "n", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("incoming schema");
        let updates = run(
            UpdateSchemaAction::new().union_by_name_with(incoming),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        assert_eq!(
            schema.field_by_name("n").expect("n").field_type.as_ref(),
            &Type::Primitive(PrimitiveType::Long),
            "int->long widening must be applied"
        );
        assert_eq!(
            schema.field_by_name("x").expect("x").field_type.as_ref(),
            &Type::Primitive(PrimitiveType::Long),
            "narrower incoming type must be ignored (keep the wider long)"
        );
    }

    // ----- case sensitivity -----

    // RISK: case-sensitive (default) resolution must REJECT a differently-cased name; case-insensitive
    // must resolve it. A leak here would let "X" silently match "x" by accident.
    #[tokio::test]
    async fn test_case_sensitivity_resolution() {
        // Use the nested fixture (no identifier fields) and a non-identifier column so the test isolates
        // case-folded NAME RESOLUTION from the identifier-field-by-name interaction.
        let table = nested_table();
        // default case-sensitive: "DATA" is not a column.
        let strict = Arc::new(UpdateSchemaAction::new().rename_column("DATA", "data2"))
            .commit(&table)
            .await;
        assert!(
            strict.is_err(),
            "case-sensitive rename of wrong case must fail"
        );

        // case-insensitive: "DATA" resolves to "data" (id 2).
        let updates = run(
            UpdateSchemaAction::new()
                .case_sensitive(false)
                .rename_column("DATA", "data2"),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        let field = schema
            .field_by_name("data2")
            .expect("renamed via case-insensitive lookup");
        assert_eq!(field.id, 2, "DATA resolves to column data (id 2)");
    }

    // ----- emitted updates/requirements shape + end-to-end -----

    // RISK: the emitted updates must be exactly AddSchema + SetCurrentSchema{-1}, and the requirements
    // exactly LastAssignedFieldIdMatch(base last column id) + CurrentSchemaIdMatch(base current schema
    // id) — the optimistic-concurrency contract Java's UpdateRequirements attaches.
    #[tokio::test]
    async fn test_emitted_updates_and_requirements_shape() {
        let table = v2_table(); // last_column_id 3, current_schema_id 1
        let action = Transaction::new(&table)
            .update_schema()
            .add_column("w", Type::Primitive(PrimitiveType::Int));
        let mut commit = Arc::new(action).commit(&table).await.expect("commit");
        let updates = commit.take_updates();
        let requirements = commit.take_requirements();

        assert_eq!(updates.len(), 2);
        assert!(matches!(updates[0], TableUpdate::AddSchema { .. }));
        assert!(
            matches!(updates[1], TableUpdate::SetCurrentSchema { schema_id } if schema_id == -1)
        );

        assert_eq!(requirements.len(), 2);
        assert!(requirements.iter().any(|r| matches!(
            r,
            TableRequirement::LastAssignedFieldIdMatch { last_assigned_field_id } if *last_assigned_field_id == 3
        )));
        assert!(requirements.iter().any(|r| matches!(
            r,
            TableRequirement::CurrentSchemaIdMatch { current_schema_id } if *current_schema_id == 1
        )));
    }

    // RISK (end-to-end): the emitted updates must drive through TableMetadataBuilder to make the rebuilt
    // schema the current schema with the right last_column_id — a mismatch between what the action emits
    // and what the metadata layer does would silently leave the old schema current.
    #[tokio::test]
    async fn test_commit_updates_round_trip_to_new_current_schema() {
        let table = v2_table(); // last_column_id 3
        let action = Transaction::new(&table)
            .update_schema()
            .add_column("w", Type::Primitive(PrimitiveType::Int));
        let mut commit = Arc::new(action).commit(&table).await.expect("commit");
        let metadata = apply_updates(&table, commit.take_updates());

        let current = metadata.current_schema();
        assert!(
            current.field_by_name("w").is_some(),
            "w is in the new current schema"
        );
        assert_eq!(metadata.last_column_id(), 4, "last_column_id advanced to 4");
    }

    // ----- nested field-id assignment ORDER (Java AssignFreshIds level-order) -----

    /// Assert the error is `DataInvalid` and its message matches `expected` exactly (Java parity).
    fn assert_data_invalid(error: &Error, expected: &str) {
        assert_eq!(
            error.kind(),
            ErrorKind::DataInvalid,
            "expected DataInvalid, got {:?}: {}",
            error.kind(),
            error.message()
        );
        assert_eq!(error.message(), expected, "error message must match Java");
    }

    // RISK (the blocker): nested ids must be assigned LEVEL-ORDER like Java's AssignFreshIds, not
    // depth-first. For map<struct,struct>, the value id must come immediately after the key id (before
    // descending into the key struct). This is Java's testAddNestedMapOfStructs: with last_column_id 1,
    // locations=2, key=3, value=4, key-struct address=5..zip=8, value-struct lat=9, long=10. A
    // depth-first walk would give value=8 (after the whole key subtree) — divergent ids break interop.
    #[tokio::test]
    async fn test_add_nested_map_of_structs_assigns_level_order_ids() {
        // Fresh single-column table (id=1) so ids start exactly where Java's test does.
        let base = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .expect("base schema");
        let metadata = crate::spec::TableMetadataBuilder::new(
            base,
            crate::spec::PartitionSpec::unpartition_spec(),
            crate::spec::SortOrder::unsorted_order(),
            "s3://bucket/m".to_string(),
            crate::spec::FormatVersion::V2,
            std::collections::HashMap::new(),
        )
        .expect("builder")
        .build()
        .expect("metadata")
        .metadata;
        let table = v2_table().with_metadata(Arc::new(metadata));

        // map<struct{address,city,state,zip}, struct{lat,long}> with deliberately scrambled input ids.
        let key_struct = Type::Struct(StructType::new(vec![
            NestedField::required(20, "address", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(21, "city", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(22, "state", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(23, "zip", Type::Primitive(PrimitiveType::Int)).into(),
        ]));
        let value_struct = Type::Struct(StructType::new(vec![
            NestedField::required(9, "lat", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(8, "long", Type::Primitive(PrimitiveType::Int)).into(),
        ]));
        let map = Type::Map(MapType::new(
            NestedField::map_key_element(1, key_struct).into(),
            NestedField::map_value_element(2, value_struct, true).into(),
        ));
        let updates = run(
            UpdateSchemaAction::new().add_column_to(None, "locations", map, None),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        // Walk the rebuilt map type and assert exact ids by structure (robust to map key/value naming).
        let locations = schema.field_by_name("locations").expect("locations");
        assert_eq!(locations.id, 2, "the map column itself");
        let Type::Map(map) = locations.field_type.as_ref() else {
            panic!("locations must be a map");
        };
        // value id (4) must directly follow key id (3) — the level-order invariant.
        assert_eq!(map.key_field.id, 3, "map key id");
        assert_eq!(map.value_field.id, 4, "map value id directly after key id");
        let Type::Struct(key_struct) = map.key_field.field_type.as_ref() else {
            panic!("key must be a struct");
        };
        let key_ids: Vec<i32> = key_struct.fields().iter().map(|f| f.id).collect();
        assert_eq!(
            key_ids,
            vec![5, 6, 7, 8],
            "key-struct address/city/state/zip get 5..8 (after key+value ids)"
        );
        let Type::Struct(value_struct) = map.value_field.field_type.as_ref() else {
            panic!("value must be a struct");
        };
        let value_ids: Vec<i32> = value_struct.fields().iter().map(|f| f.id).collect();
        assert_eq!(
            value_ids,
            vec![9, 10],
            "value-struct lat/long get 9,10 (after the whole key subtree)"
        );
    }

    // RISK: a struct whose FIRST field is itself a struct followed by a sibling primitive must assign the
    // sibling's id BEFORE descending into the first struct's children (level-order). Depth-first would
    // give the sibling a later id. Adds nested = struct{ inner: struct{a}, b: int } onto last_column_id 3:
    // nested=4, then immediate fields inner=5, b=6, then inner's child a=7.
    #[tokio::test]
    async fn test_add_struct_with_leading_struct_sibling_is_level_order() {
        let table = v2_table(); // last_column_id 3
        let nested = Type::Struct(StructType::new(vec![
            NestedField::required(
                50,
                "inner",
                Type::Struct(StructType::new(vec![
                    NestedField::required(51, "a", Type::Primitive(PrimitiveType::Int)).into(),
                ])),
            )
            .into(),
            NestedField::optional(52, "b", Type::Primitive(PrimitiveType::Int)).into(),
        ]));
        let updates = run(
            UpdateSchemaAction::new().add_column_to(None, "nested", nested, None),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        let id = |name: &str| schema.field_by_name(name).expect(name).id;
        assert_eq!(id("nested"), 4);
        assert_eq!(id("nested.inner"), 5, "immediate sibling assigned first");
        assert_eq!(id("nested.b"), 6, "second immediate sibling before descent");
        assert_eq!(
            id("nested.inner.a"),
            7,
            "inner child assigned after siblings"
        );
    }

    // RISK: list<struct> must assign the element id before descending into the element struct
    // (Java testAddNestedListOfStructs). On last_column_id 3: locations=4, element=5, lat=6, long=7.
    #[tokio::test]
    async fn test_add_nested_list_of_structs_level_order() {
        let table = v2_table(); // last_column_id 3
        let element = Type::Struct(StructType::new(vec![
            NestedField::required(9, "lat", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(8, "long", Type::Primitive(PrimitiveType::Int)).into(),
        ]));
        let list = Type::List(ListType::new(
            NestedField::list_element(1, element, false).into(),
        ));
        let updates = run(
            UpdateSchemaAction::new().add_column_to(None, "locations", list, None),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        let id = |name: &str| schema.field_by_name(name).expect(name).id;
        assert_eq!(id("locations"), 4);
        assert_eq!(id("locations.lat"), 6, "element id 5 precedes its children");
        assert_eq!(id("locations.long"), 7);
    }

    // ----- identifier-field id STABILITY across rename / move -----

    // RISK: renaming an identifier field must preserve its identifier id (Java testRenameIdentifierFields)
    // — the by-name rewrite must keep the SAME field id in the identifier set, resolving to the new name.
    #[tokio::test]
    async fn test_rename_identifier_field_preserves_identifier_id() {
        let table = v2_table(); // identifier ids [1,2] = x,y
        let updates = run(
            UpdateSchemaAction::new()
                .set_identifier_fields(["x"])
                .rename_column("x", "x_renamed"),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        let ids: Vec<i32> = schema.identifier_field_ids().collect();
        assert_eq!(ids, vec![1], "identifier id unchanged after rename");
        assert_eq!(
            schema.field_by_name("x_renamed").expect("renamed").id,
            1,
            "identifier resolves to the new name"
        );
    }

    // RISK: moving an identifier field must not change its identifier id (Java testMoveIdentifierFields).
    #[tokio::test]
    async fn test_move_identifier_field_preserves_identifier_id() {
        let table = v2_table(); // order x,y,z; set x as identifier
        let updates = run(
            UpdateSchemaAction::new()
                .set_identifier_fields(["x"])
                .move_after("x", "z"),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        let ids: Vec<i32> = schema.identifier_field_ids().collect();
        assert_eq!(ids, vec![1], "identifier id stable across a move");
        assert_eq!(
            schema.as_struct().fields().last().expect("last").name,
            "x",
            "x moved to the end"
        );
    }

    // ----- moves: nested struct + delete/re-add/move -----

    // RISK: a field inside a list-element struct must be movable within that struct (Java
    // testMoveListElementField analogue). Uses a list<struct{a,b}> and moves b before a.
    #[tokio::test]
    async fn test_move_field_inside_list_element_struct() {
        let base = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(
                    2,
                    "points",
                    Type::List(ListType::new(
                        NestedField::list_element(
                            3,
                            Type::Struct(StructType::new(vec![
                                NestedField::required(4, "a", Type::Primitive(PrimitiveType::Int))
                                    .into(),
                                NestedField::optional(5, "b", Type::Primitive(PrimitiveType::Int))
                                    .into(),
                            ])),
                            true,
                        )
                        .into(),
                    )),
                )
                .into(),
            ])
            .build()
            .expect("list-of-struct schema");
        let metadata = crate::spec::TableMetadataBuilder::new(
            base,
            crate::spec::PartitionSpec::unpartition_spec(),
            crate::spec::SortOrder::unsorted_order(),
            "s3://bucket/lstruct".to_string(),
            crate::spec::FormatVersion::V2,
            std::collections::HashMap::new(),
        )
        .expect("builder")
        .build()
        .expect("metadata")
        .metadata;
        let table = v2_table().with_metadata(Arc::new(metadata));

        let updates = run(
            UpdateSchemaAction::new().move_before("points.element.b", "points.element.a"),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        // The list element struct should now order b, a.
        let element = schema
            .field_by_name("points.element.b")
            .expect("nested b still present");
        assert_eq!(element.name, "b");
        // Confirm ordering by walking the rebuilt type.
        if let Type::List(list) = schema
            .field_by_name("points")
            .expect("points")
            .field_type
            .as_ref()
            && let Type::Struct(inner) = list.element_field.field_type.as_ref()
        {
            let names: Vec<&str> = inner.fields().iter().map(|f| f.name.as_str()).collect();
            assert_eq!(names, vec!["b", "a"], "b moved before a inside the element");
        } else {
            panic!("points should remain a list of struct");
        }
    }

    // RISK: delete + re-add + move of the same name must land the FRESH-id re-added field at the moved
    // position (Java testMoveTopDeletedColumnAfterAnotherColumn). z is deleted, re-added (fresh id 4),
    // then moved first.
    #[tokio::test]
    async fn test_delete_readd_then_move_places_fresh_field() {
        let table = v2_table(); // x,y,z; last_column_id 3
        let updates = run(
            UpdateSchemaAction::new()
                .delete_column("z")
                .add_column("z", Type::Primitive(PrimitiveType::String))
                .move_first("z"),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        assert_eq!(
            schema.as_struct().fields()[0].name,
            "z",
            "re-added z moved to first"
        );
        assert_eq!(
            schema.field_by_name("z").expect("z").id,
            4,
            "the moved field is the fresh-id re-add, not the old id 3"
        );
    }

    // ----- union_by_name: nested + relax + errors + no-op -----

    // RISK: union must add a NEW field into an existing struct by name (Java testAddTopLevelDataframe-
    // style nested add). Incoming adds location.altitude — it must appear inside the location struct.
    #[tokio::test]
    async fn test_union_adds_field_into_existing_struct() {
        let table = nested_table(); // location struct{lat,long}
        let incoming = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(
                    3,
                    "location",
                    Type::Struct(StructType::new(vec![
                        NestedField::required(4, "lat", Type::Primitive(PrimitiveType::Float))
                            .into(),
                        NestedField::required(5, "long", Type::Primitive(PrimitiveType::Float))
                            .into(),
                        NestedField::optional(
                            99,
                            "altitude",
                            Type::Primitive(PrimitiveType::Float),
                        )
                        .into(),
                    ])),
                )
                .into(),
            ])
            .build()
            .expect("incoming");
        let updates = run(
            UpdateSchemaAction::new().union_by_name_with(incoming),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        assert!(
            schema.field_by_name("location.altitude").is_some(),
            "union must add altitude inside the location struct"
        );
    }

    // RISK: union must add a new field nested inside an existing list<struct> (Java
    // testAppendNestedLists / testAddPrimitiveToNestedStruct) — the recursion must descend through the
    // list element struct, which the struct-only recursion previously skipped.
    #[tokio::test]
    async fn test_union_adds_field_inside_list_element_struct() {
        let base = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(
                    2,
                    "points",
                    Type::List(ListType::new(
                        NestedField::list_element(
                            3,
                            Type::Struct(StructType::new(vec![
                                NestedField::required(4, "x", Type::Primitive(PrimitiveType::Int))
                                    .into(),
                            ])),
                            true,
                        )
                        .into(),
                    )),
                )
                .into(),
            ])
            .build()
            .expect("base");
        let metadata = crate::spec::TableMetadataBuilder::new(
            base,
            crate::spec::PartitionSpec::unpartition_spec(),
            crate::spec::SortOrder::unsorted_order(),
            "s3://bucket/u".to_string(),
            crate::spec::FormatVersion::V2,
            std::collections::HashMap::new(),
        )
        .expect("builder")
        .build()
        .expect("metadata")
        .metadata;
        let table = v2_table().with_metadata(Arc::new(metadata));

        // incoming list element struct gains "y".
        let incoming = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(
                    2,
                    "points",
                    Type::List(ListType::new(
                        NestedField::list_element(
                            3,
                            Type::Struct(StructType::new(vec![
                                NestedField::required(4, "x", Type::Primitive(PrimitiveType::Int))
                                    .into(),
                                NestedField::optional(5, "y", Type::Primitive(PrimitiveType::Int))
                                    .into(),
                            ])),
                            true,
                        )
                        .into(),
                    )),
                )
                .into(),
            ])
            .build()
            .expect("incoming");
        let updates = run(
            UpdateSchemaAction::new().union_by_name_with(incoming),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        assert!(
            schema.field_by_name("points.element.y").is_some(),
            "union must add y inside the list element struct"
        );
    }

    // RISK: union must relax an existing REQUIRED column to optional when the incoming schema marks it
    // optional (Java needsOptionalUpdate). data is required in base, optional in incoming.
    #[tokio::test]
    async fn test_union_relaxes_required_to_optional() {
        // Build a table where "code" is required.
        let base = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "code", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("base");
        let metadata = crate::spec::TableMetadataBuilder::new(
            base,
            crate::spec::PartitionSpec::unpartition_spec(),
            crate::spec::SortOrder::unsorted_order(),
            "s3://bucket/r".to_string(),
            crate::spec::FormatVersion::V2,
            std::collections::HashMap::new(),
        )
        .expect("builder")
        .build()
        .expect("metadata")
        .metadata;
        let table = v2_table().with_metadata(Arc::new(metadata));

        let incoming = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "code", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("incoming");
        let updates = run(
            UpdateSchemaAction::new().union_by_name_with(incoming),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        assert!(
            !schema.field_by_name("code").expect("code").required,
            "union must relax required code to optional"
        );
    }

    // RISK: union must REJECT an incompatible primitive type change (string -> long) with the Java-shaped
    // "Cannot change column type" message — the previous merge silently dropped it.
    #[tokio::test]
    async fn test_union_rejects_incompatible_primitive_change() {
        let table = nested_table(); // data is string
        let incoming = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::optional(2, "data", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("incoming");
        let error = Arc::new(UpdateSchemaAction::new().union_by_name_with(incoming))
            .commit(&table)
            .await
            .map(drop)
            .expect_err("string -> long must be rejected");
        assert_data_invalid(&error, "Cannot change column type: data: string -> long");
    }

    // RISK: union must REJECT replacing a list column with a primitive (Java testReplaceListWithPrimitive)
    // with the "Cannot change column type" message instead of silently keeping the list.
    #[tokio::test]
    async fn test_union_rejects_replacing_list_with_primitive() {
        let table = nested_table(); // tags is list<string>
        let incoming = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::optional(6, "tags", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("incoming");
        let error = Arc::new(UpdateSchemaAction::new().union_by_name_with(incoming))
            .commit(&table)
            .await
            .map(drop)
            .expect_err("list -> primitive must be rejected");
        assert_eq!(
            error.kind(),
            ErrorKind::DataInvalid,
            "replacing a list with a primitive must be DataInvalid"
        );
        assert!(
            error
                .message()
                .starts_with("Cannot change column type: tags:"),
            "message must name the offending column, got: {}",
            error.message()
        );
    }

    // RISK: union with a MIRRORED schema (identical to the current one) must be a no-op — it must not
    // fabricate adds or spurious updates (Java testMirroredSchemas). The rebuilt struct must equal the
    // current one.
    #[tokio::test]
    async fn test_union_mirrored_schema_is_noop() {
        let table = nested_table();
        let mirror = table.metadata().current_schema().as_ref().clone();
        let updates = run(UpdateSchemaAction::new().union_by_name_with(mirror), &table).await;
        let schema = added_schema(&updates);
        assert_eq!(
            schema.as_struct(),
            table.metadata().current_schema().as_struct(),
            "union of a schema with itself must not change it"
        );
    }

    // RISK: union must apply a doc-only change to an existing field (Java testUpdateColumnDoc) without
    // touching its type or nullability.
    #[tokio::test]
    async fn test_union_applies_doc_only_update() {
        let table = nested_table();
        let incoming = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![{
                let mut field =
                    NestedField::optional(2, "data", Type::Primitive(PrimitiveType::String));
                field.doc = Some("the data column".to_string());
                field.into()
            }])
            .build()
            .expect("incoming");
        let updates = run(
            UpdateSchemaAction::new().union_by_name_with(incoming),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        let data = schema.field_by_name("data").expect("data");
        assert_eq!(data.doc.as_deref(), Some("the data column"));
        assert_eq!(
            data.field_type.as_ref(),
            &Type::Primitive(PrimitiveType::String),
            "doc-only update must not change the type"
        );
    }

    // ----- test rigor: exact error kind + message on high-value negatives -----

    // RISK: the ambiguous-name reject must carry the exact Java-shaped message and DataInvalid kind, not
    // just any error — a duplicate-name reject masquerading as this would hide a precondition bug.
    #[tokio::test]
    async fn test_add_dotted_name_error_message_exact() {
        let table = v2_table();
        let error = Arc::new(
            UpdateSchemaAction::new().add_column("a.b", Type::Primitive(PrimitiveType::Int)),
        )
        .commit(&table)
        .await
        .map(drop)
        .expect_err("dotted name rejected");
        assert_data_invalid(
            &error,
            "Cannot add column with ambiguous name: a.b, use add_column_to(parent, name, type)",
        );
    }

    // RISK: the delete-with-updates conflict must carry the exact Java message.
    #[tokio::test]
    async fn test_delete_after_update_error_message_exact() {
        let table = v2_table();
        let error = Arc::new(
            UpdateSchemaAction::new()
                .rename_column("y", "y2")
                .delete_column("y"),
        )
        .commit(&table)
        .await
        .map(drop)
        .expect_err("delete after update rejected");
        assert_data_invalid(&error, "Cannot delete a column that has updates: y");
    }

    // RISK: the move-before-itself reject must carry the exact Java message.
    #[tokio::test]
    async fn test_move_before_itself_error_message_exact() {
        let table = v2_table();
        let error = Arc::new(UpdateSchemaAction::new().move_before("x", "x"))
            .commit(&table)
            .await
            .map(drop)
            .expect_err("move before itself rejected");
        assert_data_invalid(&error, "Cannot move x before itself");
    }

    // ----- column defaults (Java addColumn(..,Literal) / updateColumnDefault / addRequiredColumn(..,default)) -----

    // RISK: an OPTIONAL add WITH a default must set BOTH the initial default (backfills existing rows) and
    // the write default (default for future writes) — Java sets both; setting neither/one silently loses
    // the backfill or the write default.
    #[tokio::test]
    async fn test_add_optional_column_with_default_sets_both_defaults() {
        let table = v2_table(); // last_column_id 3
        let updates = run(
            UpdateSchemaAction::new().add_column_with_default(
                "w",
                Type::Primitive(PrimitiveType::Long),
                Literal::long(7),
            ),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        let field = schema.field_by_name("w").expect("new column w");
        assert!(!field.required, "addColumn adds an optional column");
        assert_eq!(field.initial_default, Some(Literal::long(7)));
        assert_eq!(field.write_default, Some(Literal::long(7)));
    }

    // RISK (the headline default rule): a REQUIRED add WITH a default must succeed WITHOUT
    // allow_incompatible_changes — the default backfills existing rows, so Java treats it as compatible.
    // The added field must be required and carry the defaults. Driven on a **V3** base (and applied
    // through the metadata builder), because the column initial default is only legal at v3+ — Java's
    // `Schema.checkCompatibility`, mirrored by `Schema::check_compatibility` in
    // `TableMetadataBuilder::add_schema`, rejects it on v1/v2. The V2 rejection is pinned by
    // `test_add_required_column_with_default_rejected_on_v2` below.
    #[tokio::test]
    async fn test_add_required_column_with_default_succeeds_without_flag_on_v3() {
        let table = v3_table();
        let action = Transaction::new(&table)
            .update_schema()
            .add_required_column_with_default(
                "w",
                Type::Primitive(PrimitiveType::Long),
                Literal::long(42),
            );
        let mut commit = Arc::new(action).commit(&table).await.expect("commit");
        // Apply the emitted updates through the metadata builder (the path that runs the V3 guard).
        let metadata = apply_updates(&table, commit.take_updates());
        let field = metadata
            .current_schema()
            .field_by_name("w")
            .expect("required column w");
        assert!(field.required, "the added column must be required");
        assert_eq!(field.initial_default, Some(Literal::long(42)));
        assert_eq!(field.write_default, Some(Literal::long(42)));
    }

    // RISK (the V3-only initial-default guard fires): the SAME required-with-default add, applied to a
    // **V2** table, must be REJECTED when the emitted schema reaches `TableMetadataBuilder::add_schema`
    // — mirroring Java `Schema.checkCompatibility` ("non-null default ... is not supported until v3").
    // A guard that only allowed V3 but silently let V2 through would emit Java-unreadable metadata. The
    // rejection must surface at apply time (the action's `commit` only emits the `AddSchema` update; the
    // builder enforces the guard).
    #[tokio::test]
    async fn test_add_required_column_with_default_rejected_on_v2() {
        let table = v2_table();
        let action = Transaction::new(&table)
            .update_schema()
            .add_required_column_with_default(
                "w",
                Type::Primitive(PrimitiveType::Long),
                Literal::long(42),
            );
        let mut commit = Arc::new(action)
            .commit(&table)
            .await
            .expect("the action itself still commits (it only emits the AddSchema update)");
        // Driving the emitted AddSchema through the V2 metadata builder must fail the V3 guard.
        let mut builder = table.metadata().clone().into_builder(None);
        let mut error = None;
        for update in commit.take_updates() {
            match update.apply(builder) {
                Ok(next) => builder = next,
                Err(application_error) => {
                    error = Some(application_error);
                    break;
                }
            }
        }
        let error = error.expect("applying a V2 default add must be rejected by the V3 guard");
        assert_eq!(
            error.kind(),
            ErrorKind::DataInvalid,
            "V2 default rejection must be DataInvalid, got: {}",
            error.message()
        );
        assert!(
            error.message().contains("is not supported until v3"),
            "message must mirror Java's not-supported-until-v3, got: {}",
            error.message()
        );
        assert!(
            error.message().contains('w'),
            "message must name the offending column, got: {}",
            error.message()
        );
    }

    // RISK: a required add WITHOUT a default and WITHOUT the flag must STILL be rejected — the default is
    // what relaxes the gate, not a behavioral regression that now lets all required adds through.
    #[tokio::test]
    async fn test_add_required_column_without_default_still_rejected() {
        let table = v2_table();
        let error = Arc::new(
            UpdateSchemaAction::new()
                .add_required_column("w", Type::Primitive(PrimitiveType::Long)),
        )
        .commit(&table)
        .await
        .map(drop)
        .expect_err("required add without default or flag must be rejected");
        assert_data_invalid(
            &error,
            "Incompatible change: cannot add required column without a default value: w",
        );
    }

    // RISK: a default whose type does not match the column type must be rejected — a string default on a
    // long column would otherwise panic later in the serde path. Java rejects it at add time.
    #[tokio::test]
    async fn test_add_column_with_type_mismatched_default_rejected() {
        let table = v2_table();
        let error = Arc::new(UpdateSchemaAction::new().add_column_with_default(
            "w",
            Type::Primitive(PrimitiveType::Long),
            Literal::string("not-a-long"),
        ))
        .commit(&table)
        .await
        .map(drop)
        .expect_err("type-mismatched default must be rejected");
        assert_eq!(
            error.kind(),
            ErrorKind::DataInvalid,
            "mismatched default must be DataInvalid, got {}",
            error.message()
        );
        assert!(
            error.message().starts_with("Cannot cast default value to"),
            "message must name the cast failure, got: {}",
            error.message()
        );
    }

    // RISK: a default on a NESTED (non-primitive) column type must be rejected (Java "must be null"). A
    // struct/list/map default is meaningless and would panic on serialize.
    #[tokio::test]
    async fn test_add_column_with_default_on_nested_type_rejected() {
        let table = v2_table();
        let struct_type = Type::Struct(StructType::new(vec![
            NestedField::required(1, "a", Type::Primitive(PrimitiveType::Int)).into(),
        ]));
        let error = Arc::new(UpdateSchemaAction::new().add_column_to_with_default(
            None,
            "w",
            struct_type,
            None,
            Literal::long(1),
        ))
        .commit(&table)
        .await
        .map(drop)
        .expect_err("default on a nested type must be rejected");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(
            error.message().starts_with("Invalid default value for"),
            "message must say the nested default must be null, got: {}",
            error.message()
        );
    }

    // RISK: a NESTED (struct child) add WITH a default must place the field inside the struct and set both
    // defaults on it — the default must follow the field down into the nested struct.
    #[tokio::test]
    async fn test_add_nested_struct_child_with_default() {
        let table = nested_table(); // location struct{lat,long}; last_column_id 10
        let updates = run(
            UpdateSchemaAction::new().add_column_to_with_default(
                Some("location"),
                "altitude",
                Type::Primitive(PrimitiveType::Float),
                Some("meters"),
                Literal::float(0.0),
            ),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        let altitude = schema
            .field_by_name("location.altitude")
            .expect("nested altitude field");
        assert_eq!(altitude.id, 11, "nested add gets last_column_id+1");
        assert_eq!(altitude.initial_default, Some(Literal::float(0.0)));
        assert_eq!(altitude.write_default, Some(Literal::float(0.0)));
    }

    // RISK: updateColumnDefault must set ONLY the write default on an existing field, leaving the initial
    // default unchanged (Java's comment: "write default is always set and initial default is only set if
    // the field requires one"). It must not touch type/nullability/id.
    #[tokio::test]
    async fn test_update_column_default_sets_only_write_default() {
        let table = v2_table(); // y is required long, id 2, no defaults
        let updates = run(
            UpdateSchemaAction::new().update_column_default("y", Literal::long(99)),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        let field = schema.field_by_name("y").expect("y");
        assert_eq!(field.id, 2, "id unchanged");
        assert_eq!(field.write_default, Some(Literal::long(99)));
        assert_eq!(
            field.initial_default, None,
            "updateColumnDefault must not set the initial default"
        );
    }

    // RISK: updateColumnDefault with a type-mismatched value must be rejected (Java's `newDefault.to(type)`
    // would be null -> the builder throws).
    #[tokio::test]
    async fn test_update_column_default_type_mismatch_rejected() {
        let table = v2_table(); // y is long
        let error =
            Arc::new(UpdateSchemaAction::new().update_column_default("y", Literal::string("nope")))
                .commit(&table)
                .await
                .map(drop)
                .expect_err("mismatched updateColumnDefault must be rejected");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(error.message().starts_with("Cannot cast default value to"));
    }

    // RISK (the `is_defaulted_add` relaxation, Java testAddColumnWithDefaultToRequiredColumn): an OPTIONAL
    // add WITH a default, then require_column, must succeed WITHOUT allow_incompatible_changes — the
    // initial default backfills existing rows, so making it required is compatible. The resulting field
    // must be required and keep both defaults. This pins the `is_added && initial_default.is_some()` branch
    // in update_column_requirement; a mutation dropping that branch would reject this legal case.
    #[tokio::test]
    async fn test_add_optional_column_with_default_then_require_succeeds_without_flag() {
        let table = v2_table();
        let updates = run(
            UpdateSchemaAction::new()
                .add_column_with_default(
                    "w",
                    Type::Primitive(PrimitiveType::Long),
                    Literal::long(7),
                )
                .require_column("w"),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        let field = schema.field_by_name("w").expect("new column w");
        assert!(
            field.required,
            "a defaulted optional add can be made required without the flag"
        );
        assert_eq!(field.initial_default, Some(Literal::long(7)));
        assert_eq!(field.write_default, Some(Literal::long(7)));
    }

    // RISK (Java testAddColumnWithUpdateColumnDefaultToRequiredColumn): an add WITHOUT a default, then
    // update_column_default (which sets ONLY the write default, NOT the initial default), then
    // require_column, must STILL be rejected — there is no initial default to backfill existing rows, so
    // `is_defaulted_add` must be false. This is the exact distinction between updateColumnDefault (write
    // only) and a defaulted add (both); a mutation checking write_default instead of initial_default in
    // `is_defaulted_add` would wrongly let this through.
    #[tokio::test]
    async fn test_add_column_then_update_default_then_require_is_rejected() {
        let table = v2_table();
        let error = Arc::new(
            UpdateSchemaAction::new()
                .add_column("w", Type::Primitive(PrimitiveType::Long))
                .update_column_default("w", Literal::long(7))
                .require_column("w"),
        )
        .commit(&table)
        .await
        .map(drop)
        .expect_err("update_column_default sets no initial default, so require must fail");
        assert_data_invalid(
            &error,
            "Cannot change column nullability: w: optional -> required",
        );
    }

    // RISK (end-to-end): the emitted schema must actually carry the defaults through to a rebuilt
    // current schema via TableMetadataBuilder — a default dropped between the action and the metadata
    // layer would silently lose the backfill. Uses a **V3** base because applying an initial default
    // through the builder is only legal at v3+ (the `Schema::check_compatibility` guard).
    #[tokio::test]
    async fn test_emitted_schema_round_trips_defaults() {
        let table = v3_table();
        let action = Transaction::new(&table)
            .update_schema()
            .add_required_column_with_default(
                "w",
                Type::Primitive(PrimitiveType::Long),
                Literal::long(5),
            );
        let mut commit = Arc::new(action).commit(&table).await.expect("commit");
        let metadata = apply_updates(&table, commit.take_updates());
        let field = metadata
            .current_schema()
            .field_by_name("w")
            .expect("w in new current schema");
        assert_eq!(field.initial_default, Some(Literal::long(5)));
        assert_eq!(field.write_default, Some(Literal::long(5)));
    }

    /// A from-scratch **V3** table with `id` (long, required), `v` (variant, required), and `vo`
    /// (variant, optional) — the exact schema the reviewer's live-Java probe drove through 1.10.0
    /// `SchemaUpdate` to pin the variant evolution behaviors below.
    fn v3_variant_table() -> Table {
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "v", Type::Variant).into(),
                NestedField::optional(3, "vo", Type::Variant).into(),
            ])
            .build()
            .expect("build v3 variant base schema");

        let metadata = crate::spec::TableMetadataBuilder::new(
            schema,
            crate::spec::PartitionSpec::unpartition_spec(),
            crate::spec::SortOrder::unsorted_order(),
            "s3://bucket/v3-variant".to_string(),
            crate::spec::FormatVersion::V3,
            std::collections::HashMap::new(),
        )
        .expect("build v3 variant metadata builder")
        .build()
        .expect("build v3 variant metadata")
        .metadata;

        v2_table().with_metadata(Arc::new(metadata))
    }

    // RISK (the compile-forced `ApplyChanges`/`rebuild_type` variant arms, live-Java-probed):
    // rename / make-optional / require / doc-update / move / delete on a VARIANT column must each
    // behave exactly like 1.10.0 `SchemaUpdate` (probed: every op succeeds, the column keeps
    // `variant` and its id). A wrong rebuild arm would corrupt the type or drop the column during
    // any unrelated evolution of a variant-bearing schema.
    #[tokio::test]
    async fn test_variant_column_evolution_ops_mirror_java() {
        let table = v3_variant_table();

        // rename: id 2 keeps the variant type under the new name (Java probe: rename -> v2name).
        let updates = run(
            UpdateSchemaAction::new().rename_column("v", "v2name"),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        let field = schema.field_by_name("v2name").expect("renamed variant");
        assert_eq!(field.id, 2);
        assert_eq!(field.field_type.as_ref(), &Type::Variant);

        // make optional (Java probe: makeColumnOptional succeeds on a required variant).
        let updates = run(UpdateSchemaAction::new().make_column_optional("v"), &table).await;
        assert!(
            !added_schema(&updates)
                .field_by_name("v")
                .expect("v")
                .required
        );

        // require with the incompatible-changes flag (Java probe: allowIncompatibleChanges +
        // requireColumn succeeds on an optional variant).
        let updates = run(
            UpdateSchemaAction::new()
                .allow_incompatible_changes()
                .require_column("vo"),
            &table,
        )
        .await;
        assert!(
            added_schema(&updates)
                .field_by_name("vo")
                .expect("vo")
                .required
        );

        // doc update (Java probe: updateColumnDoc stamps the doc, type untouched).
        let updates = run(
            UpdateSchemaAction::new().update_column_doc("v", Some("docs!")),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        let field = schema.field_by_name("v").expect("v");
        assert_eq!(field.doc.as_deref(), Some("docs!"));
        assert_eq!(field.field_type.as_ref(), &Type::Variant);

        // move first (Java probe: moveFirst reorders to v, id, vo).
        let updates = run(UpdateSchemaAction::new().move_first("v"), &table).await;
        let schema = added_schema(&updates);
        let names: Vec<_> = schema
            .as_struct()
            .fields()
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        assert_eq!(names, vec!["v", "id", "vo"]);

        // delete (Java probe: deleteColumn removes the variant column, siblings keep their ids).
        let updates = run(UpdateSchemaAction::new().delete_column("v"), &table).await;
        let schema = added_schema(&updates);
        assert!(schema.field_by_name("v").is_none());
        assert_eq!(schema.field_by_name("vo").expect("vo").id, 3);
    }

    // RISK (the `assign_fresh_ids` variant arm, live-Java-probed): adding a variant column — and
    // a struct that NESTS one — must assign fresh ids level-order exactly like 1.10.0
    // `SchemaUpdate.addColumn` (probed: top-level add gets id 4; a struct add gets 4 with its
    // inner variant at 5), with the variant type passing through unchanged.
    #[tokio::test]
    async fn test_add_variant_column_assigns_fresh_ids_like_java() {
        let table = v3_variant_table();

        let updates = run(
            UpdateSchemaAction::new().add_column("v_new", Type::Variant),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        let field = schema.field_by_name("v_new").expect("v_new");
        assert_eq!(field.id, 4, "fresh id after last_column_id 3");
        assert_eq!(field.field_type.as_ref(), &Type::Variant);
        assert!(!field.required, "plain add is optional");

        let updates = run(
            UpdateSchemaAction::new().add_column(
                "s",
                Type::Struct(StructType::new(vec![
                    NestedField::optional(99, "inner_v", Type::Variant).into(),
                ])),
            ),
            &table,
        )
        .await;
        let schema = added_schema(&updates);
        let s_field = schema.field_by_name("s").expect("struct s");
        assert_eq!(s_field.id, 4);
        let inner = schema.field_by_name("s.inner_v").expect("nested variant");
        assert_eq!(
            inner.id, 5,
            "level-order fresh id, the placeholder 99 discarded"
        );
        assert_eq!(inner.field_type.as_ref(), &Type::Variant);
    }

    // RISK (no type change away from variant, live-Java-probed): 1.10.0
    // `SchemaUpdate.updateColumn(v, string)` throws "Cannot change column type: v: variant ->
    // string" — `update_column` only accepts primitive→primitive promotions, and variant is not a
    // primitive. Allowing it would silently re-type every existing data file's column.
    #[tokio::test]
    async fn test_update_variant_column_type_rejected() {
        let table = v3_variant_table();
        let error = Arc::new(UpdateSchemaAction::new().update_column("v", PrimitiveType::String))
            .commit(&table)
            .await
            .map(drop)
            .expect_err("variant -> string must be rejected");
        assert_data_invalid(&error, "Cannot change column type: v: variant -> string");
    }
}
