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

use super::{AddColumn, UpdateSchemaAction};
use crate::spec::{
    ListType, MapType, NestedField, NestedFieldRef, SCHEMA_NAME_DELIMITER, Schema, StructType, Type,
};
use crate::table::Table;
use crate::transaction::action::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, Result, TableRequirement, TableUpdate};

// Default ID for a new column. This will be re-assigned to a fresh ID at commit time.
pub(super) const DEFAULT_FIELD_ID: i32 = 0;

impl AddColumn {
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
) -> Result<(i32, &'a StructType)> {
    base_schema
        .field_by_name(parent)
        .ok_or_else(|| {
            Error::new(
                ErrorKind::PreconditionFailed,
                format!("Cannot add column: parent '{parent}' not found"),
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
                format!("Cannot add column: parent '{parent}' is not a struct, map, or list"),
            )),
        })
}

// ---------------------------------------------------------------------------
// Schema tree rebuild
// ---------------------------------------------------------------------------

/// Rebuild a slice of fields, applying deletions and additions at every level,
/// plus any additions keyed by `parent_id` (`None` represents the table root).
fn rebuild_fields(
    fields: &[NestedFieldRef],
    adds: &HashMap<Option<i32>, Vec<NestedFieldRef>>,
    delete_ids: &HashSet<i32>,
    parent_id: Option<i32>,
) -> Vec<NestedFieldRef> {
    fields
        .iter()
        .filter(|f| !delete_ids.contains(&f.id))
        .map(|f| rebuild_field(f, adds, delete_ids))
        .chain(adds.get(&parent_id).into_iter().flatten().cloned())
        .collect()
}

/// Recursively rebuild a single field. If the field (or any descendant) is a struct
/// that has pending additions, those additions are appended to the struct's fields.
/// Fields whose IDs appear in `delete_ids` are filtered out at every struct level.
fn rebuild_field(
    field: &NestedFieldRef,
    adds: &HashMap<Option<i32>, Vec<NestedFieldRef>>,
    delete_ids: &HashSet<i32>,
) -> NestedFieldRef {
    match field.field_type.as_ref() {
        Type::Primitive(_) | Type::Variant(_) => field.clone(),
        Type::Struct(s) => {
            let new_fields = rebuild_fields(s.fields(), adds, delete_ids, Some(field.id));
            Arc::new(NestedField {
                id: field.id,
                name: field.name.clone(),
                required: field.required,
                field_type: Box::new(Type::Struct(StructType::new(new_fields))),
                doc: field.doc.clone(),
                initial_default: field.initial_default.clone(),
                write_default: field.write_default.clone(),
            })
        }
        Type::List(l) => {
            let new_element = rebuild_field(&l.element_field, adds, delete_ids);
            Arc::new(NestedField {
                id: field.id,
                name: field.name.clone(),
                required: field.required,
                field_type: Box::new(Type::List(ListType {
                    element_field: new_element,
                })),
                doc: field.doc.clone(),
                initial_default: field.initial_default.clone(),
                write_default: field.write_default.clone(),
            })
        }
        Type::Map(m) => {
            let new_key = rebuild_field(&m.key_field, adds, delete_ids);
            let new_value = rebuild_field(&m.value_field, adds, delete_ids);
            Arc::new(NestedField {
                id: field.id,
                name: field.name.clone(),
                required: field.required,
                field_type: Box::new(Type::Map(MapType {
                    key_field: new_key,
                    value_field: new_value,
                })),
                doc: field.doc.clone(),
                initial_default: field.initial_default.clone(),
                write_default: field.write_default.clone(),
            })
        }
    }
}

// ---------------------------------------------------------------------------
// TransactionAction implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl TransactionAction for UpdateSchemaAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let base_schema = table.metadata().current_schema();
        let mut last_column_id = table.metadata().last_column_id();

        // --- 1. Validate deletes ---
        let delete_ids = self
            .deletes
            .iter()
            .map(|name: &String| {
                base_schema
                    .field_by_name(name)
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::PreconditionFailed,
                            format!("Cannot delete missing column: {name}"),
                        )
                    })
                    .and_then(|field| {
                        match base_schema
                            .identifier_field_ids()
                            .find(|id| *id == field.id)
                        {
                            Some(_) => Err(Error::new(
                                ErrorKind::PreconditionFailed,
                                format!("Cannot delete identifier field: {name}"),
                            )),
                            None => Ok(field.id),
                        }
                    })
            })
            .collect::<Result<HashSet<i32>>>()?;

        // --- 2. Resolve parents, validate additions, assign IDs, and group by parent ID ---
        // We assign IDs inline (before grouping) to preserve the caller's insertion order,
        // since HashMap iteration order is non-deterministic.
        let mut additions_by_parent: HashMap<Option<i32>, Vec<NestedFieldRef>> = HashMap::new();

        for add in &self.additions {
            let pending_field = add.to_nested_field();

            // Check that name does not contain `SCHEMA_NAME_DELIMITER`.
            if pending_field.name.contains(SCHEMA_NAME_DELIMITER) {
                return Err(Error::new(
                    ErrorKind::PreconditionFailed,
                    format!(
                        "Cannot add column with ambiguous name: {}. Use `AddColumn::with_parent` to add a column to a nested struct.",
                        pending_field.name
                    ),
                ));
            }

            // Required columns without an initial default need allow_incompatible_changes.
            if pending_field.required && pending_field.initial_default.is_none() {
                return Err(Error::new(
                    ErrorKind::PreconditionFailed,
                    format!(
                        "Incompatible change: cannot add required column without an initial default: {}",
                        pending_field.name
                    ),
                ));
            }

            let parent_id = match &add.parent {
                None => {
                    // Root-level: check name conflict against root-level fields.
                    if let Some(existing) = base_schema.field_by_name(&pending_field.name)
                        && !delete_ids.contains(&existing.id)
                    {
                        return Err(Error::new(
                            ErrorKind::PreconditionFailed,
                            format!(
                                "Cannot add column, name already exists: {}",
                                pending_field.name
                            ),
                        ));
                    }
                    None
                }
                Some(parent_path) => {
                    // Nested: resolve parent, check name conflict within parent struct.
                    let (resolved_parent_id, parent_struct) =
                        resolve_parent_target(base_schema, parent_path)?;

                    if parent_struct.fields().iter().any(|f| {
                        f.name == pending_field.name
                            && !delete_ids.contains(&f.id)
                            && !delete_ids.contains(&resolved_parent_id)
                    }) {
                        return Err(Error::new(
                            ErrorKind::PreconditionFailed,
                            format!(
                                "Cannot add column, name already exists in '{}': {}",
                                parent_path, pending_field.name
                            ),
                        ));
                    }

                    Some(resolved_parent_id)
                }
            };

            // Assign fresh IDs immediately, preserving insertion order.
            let field = assign_fresh_ids(&pending_field, &mut last_column_id);

            additions_by_parent
                .entry(parent_id)
                .or_default()
                .push(field);
        }

        // --- 4. Rebuild the schema tree with additions and deletions ---
        let new_fields = rebuild_fields(
            base_schema.as_struct().fields(),
            &additions_by_parent,
            &delete_ids,
            None,
        );

        // --- 5. Build the new schema ---
        let schema = Schema::builder()
            .with_fields(new_fields)
            .with_identifier_field_ids(base_schema.identifier_field_ids())
            .build()?;

        let updates = vec![
            TableUpdate::AddSchema { schema },
            TableUpdate::SetCurrentSchema { schema_id: -1 },
        ];

        let requirements = vec![TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: base_schema.schema_id(),
        }];

        Ok(ActionCommit::new(updates, requirements))
    }
}
