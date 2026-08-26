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

use super::UpdateSchemaAction;
use super::apply::PendingSchemaUpdate;
use crate::spec::{
    DEFAULT_SCHEMA_NAME_MAPPING, MappedField, NameMapping, NestedFieldRef, Schema, Type,
};
use crate::table::Table;
use crate::transaction::action::{ActionCommit, TransactionAction};
use crate::{Result, TableRequirement, TableUpdate};

const COLUMN_PROPERTY_PREFIXES: [&str; 3] = [
    "write.metadata.metrics.column.",
    "write.parquet.bloom-filter-enabled.column.",
    "write.parquet.stats-enabled.column.",
];

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
