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

use crate::error::{Error, ErrorKind, Result, invalid_data};
use crate::spec::{
    FormatVersion, PartitionField, PartitionSpec, SchemaRef, Transform, UnboundPartitionSpec,
};
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{TableRequirement, TableUpdate};

enum PendingChange {
    Add {
        name: Option<String>,
        source_name: String,
        transform: Transform,
    },
    RemoveByName(String),
    RemoveByTransform {
        source_name: String,
        transform: Transform,
    },
    Rename {
        name: String,
        new_name: String,
    },
}

/// Transaction action that evolves the table's partition spec.
///
/// Changes are validated and resolved against the table metadata at commit time, so a commit
/// that is retried after a concurrent update re-applies them to the refreshed spec. A new field
/// with the same source and transform as a field of an earlier spec, and the same name if one
/// is given, reuses that field and its field id. Format version 1 tables are not supported yet.
///
/// # Example
///
/// ```ignore
/// let tx = Transaction::new(&table);
/// let action = tx
///     .update_partition_spec()
///     .add_field("id", Transform::Bucket(16))
///     .remove_field("category")
///     .rename_field("ts_day", "day");
/// let tx = action.apply(tx).unwrap();
/// let table = tx.commit(&catalog).await.unwrap();
/// ```
pub struct UpdatePartitionSpecAction {
    changes: Vec<PendingChange>,
    case_sensitive: bool,
    set_as_default: bool,
}

impl UpdatePartitionSpecAction {
    pub(crate) fn new() -> Self {
        Self {
            changes: Vec::new(),
            case_sensitive: true,
            set_as_default: true,
        }
    }

    /// Partition by the value of a source column, in a field named after the column.
    pub fn add_identity(self, source_name: impl Into<String>) -> Self {
        self.add_field(source_name, Transform::Identity)
    }

    /// Partition by `transform` applied to a source column.
    ///
    /// The field takes the name of an equivalent field from an earlier spec, if there is one.
    /// Otherwise it gets the default name for the transform, such as `id_bucket_16` or `ts_day`.
    pub fn add_field(mut self, source_name: impl Into<String>, transform: Transform) -> Self {
        self.changes.push(PendingChange::Add {
            name: None,
            source_name: source_name.into(),
            transform,
        });
        self
    }

    /// Partition by `transform` applied to a source column, in a field with the given name.
    pub fn add_field_with_name(
        mut self,
        name: impl Into<String>,
        source_name: impl Into<String>,
        transform: Transform,
    ) -> Self {
        self.changes.push(PendingChange::Add {
            name: Some(name.into()),
            source_name: source_name.into(),
            transform,
        });
        self
    }

    /// Remove the partition field with the given name.
    pub fn remove_field(mut self, name: impl Into<String>) -> Self {
        self.changes.push(PendingChange::RemoveByName(name.into()));
        self
    }

    /// Remove the partition field that applies `transform` to a source column.
    pub fn remove_field_by_transform(
        mut self,
        source_name: impl Into<String>,
        transform: Transform,
    ) -> Self {
        self.changes.push(PendingChange::RemoveByTransform {
            source_name: source_name.into(),
            transform,
        });
        self
    }

    /// Rename a partition field of the current spec.
    pub fn rename_field(mut self, name: impl Into<String>, new_name: impl Into<String>) -> Self {
        self.changes.push(PendingChange::Rename {
            name: name.into(),
            new_name: new_name.into(),
        });
        self
    }

    /// Set whether source column names match case-sensitively. Defaults to `true`.
    pub fn case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
        self
    }

    /// Add the new spec to the table without making it the default spec.
    pub fn add_non_default_spec(mut self) -> Self {
        self.set_as_default = false;
        self
    }

    fn resolve_spec<'a>(
        &self,
        schema: &'a SchemaRef,
        spec: &'a PartitionSpec,
        historical_specs: impl IntoIterator<Item = &'a PartitionSpec>,
        last_assigned_partition_id: i32,
    ) -> Result<UnboundPartitionSpec> {
        let mut update = PartitionSpecUpdate::new(
            schema,
            spec,
            historical_specs,
            last_assigned_partition_id,
            self.case_sensitive,
        )?;
        for change in &self.changes {
            match change {
                PendingChange::Add {
                    name,
                    source_name,
                    transform,
                } => update.add_field(name.as_deref(), source_name, *transform)?,
                PendingChange::RemoveByName(name) => update.remove_field(name)?,
                PendingChange::RemoveByTransform {
                    source_name,
                    transform,
                } => update.remove_field_by_transform(source_name, *transform)?,
                PendingChange::Rename { name, new_name } => update.rename_field(name, new_name)?,
            }
        }
        update.apply()
    }
}

#[async_trait]
impl TransactionAction for UpdatePartitionSpecAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let metadata = table.metadata();
        if metadata.format_version() == FormatVersion::V1 {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Updating the partition spec of a format version 1 table is not supported",
            ));
        }

        let spec = self.resolve_spec(
            metadata.current_schema(),
            metadata.default_partition_spec(),
            metadata.partition_specs_iter().map(|spec| spec.as_ref()),
            metadata.last_partition_id(),
        )?;

        let mut updates = vec![TableUpdate::AddSpec { spec }];
        let mut requirements = vec![TableRequirement::LastAssignedPartitionIdMatch {
            last_assigned_partition_id: metadata.last_partition_id(),
        }];
        if self.set_as_default {
            updates.push(TableUpdate::SetDefaultSpec { spec_id: -1 });
            requirements.push(TableRequirement::DefaultSpecIdMatch {
                default_spec_id: metadata.default_partition_spec_id(),
            });
        }

        Ok(ActionCommit::new(updates, requirements))
    }
}

/// Applies partition field changes to a base spec, following Java's `BaseUpdatePartitionSpec`
/// so that both produce the same spec for the same changes.
struct PartitionSpecUpdate<'a> {
    schema: &'a SchemaRef,
    spec: &'a PartitionSpec,
    historical_fields: Vec<&'a PartitionField>,
    case_sensitive: bool,
    last_assigned_partition_id: i32,
    name_to_field: HashMap<&'a str, &'a PartitionField>,
    transform_to_field: HashMap<(i32, Transform), &'a PartitionField>,
    adds: Vec<PartitionField>,
    added_names: HashSet<String>,
    transform_to_added_name: HashMap<(i32, Transform), String>,
    added_time_field_names: HashMap<i32, String>,
    deletes: HashSet<i32>,
    renames: HashMap<String, String>,
}

impl<'a> PartitionSpecUpdate<'a> {
    fn new(
        schema: &'a SchemaRef,
        spec: &'a PartitionSpec,
        historical_specs: impl IntoIterator<Item = &'a PartitionSpec>,
        last_assigned_partition_id: i32,
        case_sensitive: bool,
    ) -> Result<Self> {
        if let Some(field) = spec
            .fields()
            .iter()
            .find(|field| field.transform == Transform::Unknown)
        {
            return Err(invalid_data!(
                "Cannot update partition spec with unknown transform: {}",
                field.name
            ));
        }

        let mut historical_specs: Vec<_> = historical_specs.into_iter().collect();
        historical_specs.sort_by_key(|spec| spec.spec_id());

        Ok(Self {
            schema,
            spec,
            historical_fields: historical_specs
                .into_iter()
                .flat_map(|spec| spec.fields())
                .collect(),
            case_sensitive,
            last_assigned_partition_id,
            name_to_field: spec
                .fields()
                .iter()
                .map(|field| (field.name.as_str(), field))
                .collect(),
            transform_to_field: spec
                .fields()
                .iter()
                .map(|field| ((field.source_id, field.transform), field))
                .collect(),
            adds: Vec::new(),
            added_names: HashSet::new(),
            transform_to_added_name: HashMap::new(),
            added_time_field_names: HashMap::new(),
            deletes: HashSet::new(),
            renames: HashMap::new(),
        })
    }

    fn add_field(
        &mut self,
        name: Option<&str>,
        source_name: &str,
        transform: Transform,
    ) -> Result<()> {
        if let Some(name) = name
            && self.added_names.contains(name)
        {
            return Err(invalid_data!(
                "Cannot add duplicate partition field: {name}"
            ));
        }
        if matches!(transform, Transform::Void | Transform::Unknown) {
            return Err(invalid_data!(
                "Cannot add partition field with transform {transform}"
            ));
        }

        let source_id = self.source_id(source_name)?;
        let key = (source_id, transform);
        if let Some(existing) = self.transform_to_field.get(&key).copied() {
            if self.deletes.remove(&existing.field_id) {
                // Re-adding a field removed in this update restores it, renamed if asked.
                return match name {
                    Some(name) if name != existing.name => self.rename_field(&existing.name, name),
                    _ => Ok(()),
                };
            }
            return Err(invalid_data!(
                "Cannot add duplicate partition field {transform}({source_name}), conflicts with {}",
                existing.name
            ));
        }
        if let Some(added) = self.transform_to_added_name.get(&key) {
            return Err(invalid_data!(
                "Cannot add duplicate partition field {transform}({source_name}), already added: {added}"
            ));
        }

        let new_field = self.recycle_or_create_field(source_id, transform, name)?;
        if is_time_transform(transform) {
            if let Some(time_field) = self.added_time_field_names.get(&source_id) {
                return Err(invalid_data!(
                    "Cannot add redundant partition field: {time_field} conflicts with {}",
                    new_field.name
                ));
            }
            self.added_time_field_names
                .insert(source_id, new_field.name.clone());
        }
        self.transform_to_added_name
            .insert(key, new_field.name.clone());

        if let Some(existing) = self.name_to_field.get(new_field.name.as_str()).copied() {
            if self.deletes.contains(&existing.field_id) {
                self.renames
                    .insert(existing.name.clone(), freed_name(existing));
            } else if existing.transform == Transform::Void {
                self.rename_field(&existing.name, &freed_name(existing))?;
            } else {
                return Err(invalid_data!(
                    "Cannot add duplicate partition field name: {}",
                    new_field.name
                ));
            }
        }

        self.added_names.insert(new_field.name.clone());
        self.adds.push(new_field);
        Ok(())
    }

    fn remove_field(&mut self, name: &str) -> Result<()> {
        if self.added_names.contains(name) {
            return Err(invalid_data!("Cannot delete newly added field: {name}"));
        }
        if self.renames.contains_key(name) {
            return Err(invalid_data!(
                "Cannot rename and delete partition field: {name}"
            ));
        }
        let field = self
            .name_to_field
            .get(name)
            .copied()
            .ok_or_else(|| invalid_data!("Cannot find partition field to remove: {name}"))?;
        self.deletes.insert(field.field_id);
        Ok(())
    }

    fn remove_field_by_transform(&mut self, source_name: &str, transform: Transform) -> Result<()> {
        let key = (self.source_id(source_name)?, transform);
        if let Some(added) = self.transform_to_added_name.get(&key) {
            return Err(invalid_data!("Cannot delete newly added field: {added}"));
        }
        let field = self.transform_to_field.get(&key).copied().ok_or_else(|| {
            invalid_data!("Cannot find partition field to remove: {transform}({source_name})")
        })?;
        if self.renames.contains_key(&field.name) {
            return Err(invalid_data!(
                "Cannot rename and delete partition field: {}",
                field.name
            ));
        }
        self.deletes.insert(field.field_id);
        Ok(())
    }

    fn rename_field(&mut self, name: &str, new_name: &str) -> Result<()> {
        if let Some(existing) = self.name_to_field.get(new_name).copied()
            && existing.transform == Transform::Void
        {
            // A `void` field left behind by an earlier removal gives up its name.
            self.rename_field(&existing.name, &freed_name(existing))?;
        }
        if self.added_names.contains(name) {
            return Err(invalid_data!(
                "Cannot rename newly added partition field: {name}"
            ));
        }
        let field = self
            .name_to_field
            .get(name)
            .copied()
            .ok_or_else(|| invalid_data!("Cannot find partition field to rename: {name}"))?;
        if self.deletes.contains(&field.field_id) {
            return Err(invalid_data!(
                "Cannot delete and rename partition field: {name}"
            ));
        }
        self.renames.insert(name.to_string(), new_name.to_string());
        Ok(())
    }

    fn apply(self) -> Result<UnboundPartitionSpec> {
        let kept_fields = self
            .spec
            .fields()
            .iter()
            .filter(|field| !self.deletes.contains(&field.field_id))
            .map(|field| PartitionField {
                name: self
                    .renames
                    .get(&field.name)
                    .cloned()
                    .unwrap_or_else(|| field.name.clone()),
                ..field.clone()
            });
        Ok(UnboundPartitionSpec {
            spec_id: None,
            fields: kept_fields.chain(self.adds).map(Into::into).collect(),
        })
    }

    fn source_id(&self, source_name: &str) -> Result<i32> {
        let field = if self.case_sensitive {
            self.schema.field_by_name(source_name)
        } else {
            self.schema
                .field_by_name_case_insensitive_checked(source_name)?
        };
        field.map(|field| field.id).ok_or_else(|| {
            invalid_data!("Cannot find source column for partition field: {source_name}")
        })
    }

    // Of several matching fields, reuse the highest field id, as Java's `TableMetadata` does.
    fn recycle_or_create_field(
        &mut self,
        source_id: i32,
        transform: Transform,
        name: Option<&str>,
    ) -> Result<PartitionField> {
        if let Some(field) = self
            .historical_fields
            .iter()
            .filter(|field| {
                field.source_id == source_id
                    && field.transform == transform
                    && name.is_none_or(|name| field.name == name)
            })
            .max_by_key(|field| field.field_id)
        {
            return Ok((*field).clone());
        }

        self.last_assigned_partition_id = self
            .last_assigned_partition_id
            .checked_add(1)
            .ok_or_else(|| invalid_data!("Cannot assign more partition field ids. Overflow."))?;
        let name = match name {
            Some(name) => name.to_string(),
            None => self.default_field_name(source_id, transform)?,
        };
        Ok(PartitionField {
            source_id,
            field_id: self.last_assigned_partition_id,
            name,
            transform,
        })
    }

    fn default_field_name(&self, source_id: i32, transform: Transform) -> Result<String> {
        let source_name = self.schema.name_by_field_id(source_id).ok_or_else(|| {
            invalid_data!("Cannot find source column with id {source_id} in schema")
        })?;
        Ok(match transform {
            Transform::Identity => source_name.to_string(),
            Transform::Bucket(num_buckets) => format!("{source_name}_bucket_{num_buckets}"),
            Transform::Truncate(width) => format!("{source_name}_trunc_{width}"),
            Transform::Year => format!("{source_name}_year"),
            Transform::Month => format!("{source_name}_month"),
            Transform::Day => format!("{source_name}_day"),
            Transform::Hour => format!("{source_name}_hour"),
            Transform::Void => format!("{source_name}_null"),
            Transform::Unknown => format!("{source_name}_{transform}"),
        })
    }
}

/// The name a field gives up to a new or renamed field that takes its name.
fn freed_name(field: &PartitionField) -> String {
    format!("{}_{}", field.name, field.field_id)
}

fn is_time_transform(transform: Transform) -> bool {
    matches!(
        transform,
        Transform::Year | Transform::Month | Transform::Day | Transform::Hour
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::UpdatePartitionSpecAction;
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        FormatVersion, NestedField, PartitionField, PartitionSpec, PrimitiveType, Schema,
        SchemaRef, Transform, Type, UnboundPartitionSpec,
    };
    use crate::table::Table;
    use crate::transaction::tests::{make_v1_table, make_v2_table};
    use crate::transaction::{ApplyTransactionAction, Transaction, TransactionAction};
    use crate::{
        Catalog, ErrorKind, Result, TableCreation, TableIdent, TableRequirement, TableUpdate,
    };

    // Schema and specs of Java's `TestUpdatePartitionSpec`, whose cases these tests port.
    fn schema() -> SchemaRef {
        Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::required(2, "ts", Type::Primitive(PrimitiveType::Timestamptz))
                        .into(),
                    NestedField::required(3, "category", Type::Primitive(PrimitiveType::String))
                        .into(),
                    NestedField::optional(4, "data", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        )
    }

    fn field(source_id: i32, field_id: i32, name: &str, transform: Transform) -> PartitionField {
        PartitionField {
            source_id,
            field_id,
            name: name.to_string(),
            transform,
        }
    }

    fn spec_of(fields: Vec<PartitionField>) -> PartitionSpec {
        UnboundPartitionSpec {
            spec_id: Some(0),
            fields: fields.into_iter().map(Into::into).collect(),
        }
        .bind(schema())
        .unwrap()
    }

    fn unpartitioned() -> PartitionSpec {
        spec_of(vec![])
    }

    fn partitioned_fields() -> Vec<PartitionField> {
        vec![
            field(3, 1000, "category", Transform::Identity),
            field(2, 1001, "ts_day", Transform::Day),
            field(1, 1002, "shard", Transform::Bucket(16)),
        ]
    }

    fn partitioned() -> PartitionSpec {
        spec_of(partitioned_fields())
    }

    fn update() -> UpdatePartitionSpecAction {
        UpdatePartitionSpecAction::new()
    }

    /// Applies `action` to `base` alone, with no earlier specs to reuse fields from.
    fn evolve(
        base: &PartitionSpec,
        action: UpdatePartitionSpecAction,
    ) -> Result<Vec<PartitionField>> {
        let schema = schema();
        let last_assigned_partition_id = base.highest_field_id().unwrap_or(999);
        let spec = action.resolve_spec(&schema, base, [], last_assigned_partition_id)?;
        Ok(spec.bind(schema)?.fields().to_vec())
    }

    #[test]
    fn test_add_fields_get_default_names() {
        let cases = [
            ("category", Transform::Identity, "category"),
            ("ts", Transform::Year, "ts_year"),
            ("ts", Transform::Month, "ts_month"),
            ("ts", Transform::Day, "ts_day"),
            ("ts", Transform::Hour, "ts_hour"),
            ("id", Transform::Bucket(16), "id_bucket_16"),
            ("data", Transform::Truncate(4), "data_trunc_4"),
        ];
        for (source_name, transform, name) in cases {
            let source_id = schema().field_by_name(source_name).unwrap().id;
            assert_eq!(
                evolve(&unpartitioned(), update().add_field(source_name, transform)).unwrap(),
                vec![field(source_id, 1000, name, transform)]
            );
        }
        assert_eq!(
            evolve(&unpartitioned(), update().add_identity("category")).unwrap(),
            vec![field(3, 1000, "category", Transform::Identity)]
        );
    }

    #[test]
    fn test_add_fields_to_existing_spec() {
        let updated = evolve(
            &partitioned(),
            update().add_field("data", Transform::Truncate(4)),
        );
        let mut expected = partitioned_fields();
        expected.push(field(4, 1003, "data_trunc_4", Transform::Truncate(4)));
        assert_eq!(updated.unwrap(), expected);

        let updated = evolve(
            &unpartitioned(),
            update()
                .add_identity("category")
                .add_field("ts", Transform::Day)
                .add_field_with_name("shard", "id", Transform::Bucket(16))
                .add_field_with_name("prefix", "data", Transform::Truncate(4)),
        );
        assert_eq!(updated.unwrap(), vec![
            field(3, 1000, "category", Transform::Identity),
            field(2, 1001, "ts_day", Transform::Day),
            field(1, 1002, "shard", Transform::Bucket(16)),
            field(4, 1003, "prefix", Transform::Truncate(4)),
        ]);
    }

    #[test]
    fn test_add_second_bucket_on_same_source() {
        let bucket16 = spec_of(
            evolve(
                &unpartitioned(),
                update().add_field("id", Transform::Bucket(16)),
            )
            .unwrap(),
        );
        assert_eq!(
            evolve(&bucket16, update().add_field("id", Transform::Bucket(8))).unwrap(),
            vec![
                field(1, 1000, "id_bucket_16", Transform::Bucket(16)),
                field(1, 1001, "id_bucket_8", Transform::Bucket(8)),
            ]
        );
    }

    #[test]
    fn test_remove_field() {
        let without_category = vec![
            field(2, 1001, "ts_day", Transform::Day),
            field(1, 1002, "shard", Transform::Bucket(16)),
        ];
        let without_ts = vec![
            field(3, 1000, "category", Transform::Identity),
            field(1, 1002, "shard", Transform::Bucket(16)),
        ];
        let without_shard = vec![
            field(3, 1000, "category", Transform::Identity),
            field(2, 1001, "ts_day", Transform::Day),
        ];
        let cases = [
            (update().remove_field("category"), &without_category),
            (update().remove_field("shard"), &without_shard),
            (
                update().remove_field_by_transform("category", Transform::Identity),
                &without_category,
            ),
            (
                update().remove_field_by_transform("ts", Transform::Day),
                &without_ts,
            ),
            (
                update().remove_field_by_transform("id", Transform::Bucket(16)),
                &without_shard,
            ),
        ];
        for (action, expected) in cases {
            assert_eq!(&evolve(&partitioned(), action).unwrap(), expected);
        }
    }

    #[test]
    fn test_multiple_changes() {
        let updated = evolve(
            &partitioned(),
            update()
                .rename_field("shard", "id_bucket")
                .remove_field_by_transform("ts", Transform::Day)
                .add_field_with_name("prefix", "data", Transform::Truncate(4)),
        );
        assert_eq!(updated.unwrap(), vec![
            field(3, 1000, "category", Transform::Identity),
            field(1, 1002, "id_bucket", Transform::Bucket(16)),
            field(4, 1003, "prefix", Transform::Truncate(4)),
        ]);
    }

    #[test]
    fn test_readding_removed_field_restores_it() {
        for action in [
            update().remove_field("shard").add_field_with_name(
                "shard",
                "id",
                Transform::Bucket(16),
            ),
            update()
                .remove_field("shard")
                .add_field("id", Transform::Bucket(16)),
        ] {
            assert_eq!(
                evolve(&partitioned(), action).unwrap(),
                partitioned_fields()
            );
        }

        let renamed = evolve(
            &partitioned(),
            update().remove_field("shard").add_field_with_name(
                "new_shard",
                "id",
                Transform::Bucket(16),
            ),
        );
        assert_eq!(renamed.unwrap(), vec![
            field(3, 1000, "category", Transform::Identity),
            field(2, 1001, "ts_day", Transform::Day),
            field(1, 1002, "new_shard", Transform::Bucket(16)),
        ]);
    }

    #[test]
    fn test_replace_field_keeps_its_name() {
        let by_month = spec_of(vec![field(2, 1000, "ts_transformed", Transform::Month)]);
        let updated = evolve(
            &by_month,
            update().remove_field("ts_transformed").add_field_with_name(
                "ts_transformed",
                "ts",
                Transform::Day,
            ),
        );
        assert_eq!(updated.unwrap(), vec![field(
            2,
            1001,
            "ts_transformed",
            Transform::Day
        )]);
    }

    #[test]
    fn test_remove_and_add_field_over_several_updates() {
        let mut spec = unpartitioned();
        for action in [
            update().add_field_with_name("ts_date", "ts", Transform::Day),
            update().remove_field_by_transform("ts", Transform::Day),
            update().add_field_with_name("ts_date", "ts", Transform::Day),
            update().remove_field_by_transform("ts", Transform::Day),
            update().add_field("ts", Transform::Month),
            update().rename_field("ts_month", "ts_date"),
        ] {
            spec = spec_of(evolve(&spec, action).unwrap());
        }
        assert_eq!(spec.fields(), [field(2, 1000, "ts_date", Transform::Month)]);
    }

    #[test]
    fn test_void_field_gives_up_its_name() {
        // Void fields are what format version 1 keeps of removed fields.
        let base = spec_of(vec![
            field(3, 1000, "old", Transform::Void),
            field(1, 1001, "shard", Transform::Bucket(16)),
        ]);
        assert_eq!(
            evolve(
                &base,
                update().add_field_with_name("old", "data", Transform::Truncate(4))
            )
            .unwrap(),
            vec![
                field(3, 1000, "old_1000", Transform::Void),
                field(1, 1001, "shard", Transform::Bucket(16)),
                field(4, 1002, "old", Transform::Truncate(4)),
            ]
        );
        assert_eq!(
            evolve(&base, update().rename_field("shard", "old")).unwrap(),
            vec![
                field(3, 1000, "old_1000", Transform::Void),
                field(1, 1001, "old", Transform::Bucket(16)),
            ]
        );
    }

    #[test]
    fn test_add_field_reuses_equivalent_field_of_earlier_spec() {
        let schema = schema();
        let earlier = spec_of(vec![field(1, 1005, "id_bucket", Transform::Bucket(8))]);
        let base = partitioned();
        let resolve = |action: UpdatePartitionSpecAction| {
            action
                .resolve_spec(&schema, &base, [&earlier, &base], 1005)
                .unwrap()
                .bind(schema.clone())
                .unwrap()
                .fields()
                .last()
                .cloned()
                .unwrap()
        };

        assert_eq!(
            resolve(update().add_field("id", Transform::Bucket(8))),
            field(1, 1005, "id_bucket", Transform::Bucket(8))
        );
        assert_eq!(
            resolve(update().add_field_with_name("id_bucket", "id", Transform::Bucket(8))),
            field(1, 1005, "id_bucket", Transform::Bucket(8))
        );
        assert_eq!(
            resolve(update().add_field_with_name("id_b8", "id", Transform::Bucket(8))),
            field(1, 1006, "id_b8", Transform::Bucket(8))
        );

        // Of several equivalent fields, the one with the highest field id is reused.
        let later =
            spec_of(vec![field(1, 1007, "id_bucket_8", Transform::Bucket(8))]).with_spec_id(3);
        let earliest = spec_of(vec![field(1, 1006, "id_b8", Transform::Bucket(8))]);
        let spec = update()
            .add_field("id", Transform::Bucket(8))
            .resolve_spec(&schema, &base, [&earliest, &later, &earlier], 1007)
            .unwrap();
        assert_eq!(
            spec.fields().last().unwrap(),
            &field(1, 1007, "id_bucket_8", Transform::Bucket(8)).into()
        );
    }

    #[test]
    fn test_source_column_case_sensitivity() {
        let err = evolve(&unpartitioned(), update().add_identity("CATEGORY")).unwrap_err();
        assert_eq!(
            err.message(),
            "Cannot find source column for partition field: CATEGORY"
        );

        let updated = evolve(
            &unpartitioned(),
            update().case_sensitive(false).add_identity("CATEGORY"),
        );
        assert_eq!(updated.unwrap(), vec![field(
            3,
            1000,
            "category",
            Transform::Identity
        )]);
    }

    #[test]
    fn test_rejects_invalid_changes() {
        let cases = [
            (
                update()
                    .add_field_with_name("prefix", "data", Transform::Truncate(4))
                    .remove_field("prefix"),
                "Cannot delete newly added field",
            ),
            (
                update()
                    .add_field_with_name("prefix", "data", Transform::Truncate(4))
                    .remove_field_by_transform("data", Transform::Truncate(4)),
                "Cannot delete newly added field",
            ),
            (
                update()
                    .add_field_with_name("prefix", "data", Transform::Truncate(4))
                    .add_field("data", Transform::Truncate(4)),
                "Cannot add duplicate partition field",
            ),
            (
                update()
                    .add_field_with_name("prefix", "data", Transform::Truncate(4))
                    .add_field_with_name("prefix", "data", Transform::Truncate(6)),
                "Cannot add duplicate partition field",
            ),
            (
                update().add_identity("category"),
                "Cannot add duplicate partition field",
            ),
            (
                update().add_field("id", Transform::Bucket(16)),
                "Cannot add duplicate partition field",
            ),
            (
                update().add_field_with_name("b16", "id", Transform::Bucket(16)),
                "Cannot add duplicate partition field",
            ),
            (
                update().add_field_with_name("category", "id", Transform::Bucket(8)),
                "Cannot add duplicate partition field name: category",
            ),
            (
                update()
                    .add_field("ts", Transform::Hour)
                    .add_field("ts", Transform::Month),
                "Cannot add redundant partition field",
            ),
            (
                update().add_field("ts", Transform::Void),
                "Cannot add partition field with transform void",
            ),
            (
                update().add_field("ts", Transform::Unknown),
                "Cannot add partition field with transform unknown",
            ),
            (
                update().remove_field("moon"),
                "Cannot find partition field to remove: moon",
            ),
            (
                update().remove_field_by_transform("ts", Transform::Hour),
                "Cannot find partition field to remove: hour(ts)",
            ),
            (
                update().rename_field("shake", "seal"),
                "Cannot find partition field to rename: shake",
            ),
            (
                update()
                    .add_field_with_name("data_trunc", "data", Transform::Truncate(4))
                    .rename_field("data_trunc", "prefix"),
                "Cannot rename newly added partition field: data_trunc",
            ),
            (
                update()
                    .rename_field("shard", "id_bucket")
                    .remove_field_by_transform("id", Transform::Bucket(16)),
                "Cannot rename and delete partition field: shard",
            ),
            (
                update()
                    .remove_field_by_transform("id", Transform::Bucket(16))
                    .rename_field("shard", "id_bucket"),
                "Cannot delete and rename partition field: shard",
            ),
            (
                update().add_field("missing", Transform::Identity),
                "Cannot find source column for partition field: missing",
            ),
        ];
        for (action, expected) in cases {
            let err = evolve(&partitioned(), action).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::DataInvalid);
            assert!(
                err.message().starts_with(expected),
                "expected an error starting with {expected:?}, got {:?}",
                err.message()
            );
        }

        let err = evolve(
            &unpartitioned(),
            update()
                .add_field("ts", Transform::Day)
                .add_field("ts", Transform::Hour),
        )
        .unwrap_err();
        assert!(
            err.message()
                .starts_with("Cannot add redundant partition field")
        );

        let unknown = spec_of(vec![field(1, 1000, "id_unknown", Transform::Unknown)]);
        let err = evolve(&unknown, update().add_identity("category")).unwrap_err();
        assert_eq!(
            err.message(),
            "Cannot update partition spec with unknown transform: id_unknown"
        );
    }

    #[tokio::test]
    async fn test_commit_adds_spec_with_explicit_field_ids() {
        let table = make_v2_table();
        let action = update().add_field("y", Transform::Bucket(4));

        let mut commit = Arc::new(action).commit(&table).await.unwrap();

        let expected_spec = UnboundPartitionSpec {
            spec_id: None,
            fields: vec![
                field(1, 1000, "x", Transform::Identity).into(),
                field(2, 1001, "y_bucket_4", Transform::Bucket(4)).into(),
            ],
        };
        assert_eq!(commit.take_updates(), vec![
            TableUpdate::AddSpec {
                spec: expected_spec
            },
            TableUpdate::SetDefaultSpec { spec_id: -1 },
        ]);
        assert_eq!(commit.take_requirements(), vec![
            TableRequirement::LastAssignedPartitionIdMatch {
                last_assigned_partition_id: 1000
            },
            TableRequirement::DefaultSpecIdMatch { default_spec_id: 0 },
        ]);
    }

    #[tokio::test]
    async fn test_commit_non_default_spec() {
        let table = make_v2_table();
        let action = update()
            .add_field("y", Transform::Bucket(4))
            .add_non_default_spec();

        let mut commit = Arc::new(action).commit(&table).await.unwrap();

        let updates = commit.take_updates();
        assert_eq!(updates.len(), 1);
        assert!(matches!(updates[0], TableUpdate::AddSpec { .. }));
        assert_eq!(commit.take_requirements(), vec![
            TableRequirement::LastAssignedPartitionIdMatch {
                last_assigned_partition_id: 1000
            },
        ]);
    }

    #[tokio::test]
    async fn test_commit_rejects_v1_table() {
        let table = make_v1_table();
        let action = update().add_identity("x");

        let err = Arc::new(action)
            .commit(&table)
            .await
            .err()
            .expect("format version 1 tables should be rejected");

        assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);
    }

    // Table-level cases of Java's `TestTableUpdatePartitionSpec`: the table starts with
    // `bucket(data, 16)` and every update is committed through a catalog.

    async fn create_table(catalog: &impl Catalog) -> Table {
        let ident =
            TableIdent::from_strs([format!("ns-{}", uuid::Uuid::new_v4()), "t".into()]).unwrap();
        catalog
            .create_namespace(ident.namespace(), HashMap::new())
            .await
            .unwrap();
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "data", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();
        let spec = UnboundPartitionSpec {
            spec_id: None,
            fields: vec![field(2, 1000, "data_bucket", Transform::Bucket(16)).into()],
        };
        let creation = TableCreation::builder()
            .name(ident.name().to_string())
            .schema(schema)
            .partition_spec(spec)
            .format_version(FormatVersion::V2)
            .build();
        catalog
            .create_table(ident.namespace(), creation)
            .await
            .unwrap()
    }

    async fn commit(
        catalog: &impl Catalog,
        table: &Table,
        action: impl FnOnce(&Transaction) -> UpdatePartitionSpecAction,
    ) -> Table {
        let tx = Transaction::new(table);
        let tx = action(&tx).apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    fn assert_default_spec(table: &Table, spec_id: i32, fields: Vec<PartitionField>) {
        let spec = table.metadata().default_partition_spec();
        assert_eq!(spec.spec_id(), spec_id);
        assert_eq!(spec.fields(), fields);
    }

    fn data_bucket() -> PartitionField {
        field(2, 1000, "data_bucket", Transform::Bucket(16))
    }

    #[tokio::test]
    async fn test_table_commit_updated_spec() {
        let catalog = new_memory_catalog().await;
        let table = create_table(&catalog).await;

        let table = commit(&catalog, &table, |tx| {
            tx.update_partition_spec()
                .add_field("id", Transform::Bucket(8))
        })
        .await;
        assert_default_spec(&table, 1, vec![
            data_bucket(),
            field(1, 1001, "id_bucket_8", Transform::Bucket(8)),
        ]);
        assert_eq!(table.metadata().last_partition_id(), 1001);

        let table = commit(&catalog, &table, |tx| {
            tx.update_partition_spec()
                .remove_field("id_bucket_8")
                .remove_field("data_bucket")
                .add_field("data", Transform::Truncate(8))
        })
        .await;
        assert_default_spec(&table, 2, vec![field(
            2,
            1002,
            "data_trunc_8",
            Transform::Truncate(8),
        )]);
        assert_eq!(table.metadata().last_partition_id(), 1002);
    }

    #[tokio::test]
    async fn test_table_noop_commit_keeps_spec() {
        let catalog = new_memory_catalog().await;
        let table = create_table(&catalog).await;

        for action in [
            update(),
            update().rename_field("data_bucket", "data_bucket"),
        ] {
            let tx = action.apply(Transaction::new(&table)).unwrap();
            let committed = tx.commit(&catalog).await.unwrap();
            assert_default_spec(&committed, 0, vec![data_bucket()]);
            assert_eq!(committed.metadata().partition_specs_iter().len(), 1);
        }
    }

    #[tokio::test]
    async fn test_table_remove_and_add_fields() {
        let catalog = new_memory_catalog().await;

        let table = create_table(&catalog).await;
        let table = commit(&catalog, &table, |tx| {
            tx.update_partition_spec()
                .remove_field("data_bucket")
                .add_field("id", Transform::Bucket(8))
        })
        .await;
        assert_default_spec(&table, 1, vec![field(
            1,
            1001,
            "id_bucket_8",
            Transform::Bucket(8),
        )]);

        let table = create_table(&catalog).await;
        let table = commit(&catalog, &table, |tx| {
            tx.update_partition_spec()
                .add_field("data", Transform::Bucket(6))
                .remove_field("data_bucket")
        })
        .await;
        assert_default_spec(&table, 1, vec![field(
            2,
            1001,
            "data_bucket_6",
            Transform::Bucket(6),
        )]);
        assert_eq!(table.metadata().last_partition_id(), 1001);
    }

    #[tokio::test]
    async fn test_table_add_after_last_field_removed() {
        let catalog = new_memory_catalog().await;
        let table = create_table(&catalog).await;

        let table = commit(&catalog, &table, |tx| {
            tx.update_partition_spec().remove_field("data_bucket")
        })
        .await;
        assert_default_spec(&table, 1, vec![]);
        assert_eq!(table.metadata().last_partition_id(), 1000);

        let table = commit(&catalog, &table, |tx| {
            tx.update_partition_spec()
                .add_field("id", Transform::Bucket(8))
        })
        .await;
        assert_default_spec(&table, 2, vec![field(
            1,
            1001,
            "id_bucket_8",
            Transform::Bucket(8),
        )]);
        assert_eq!(table.metadata().last_partition_id(), 1001);
    }

    #[tokio::test]
    async fn test_table_readded_field_reuses_its_field_id() {
        let catalog = new_memory_catalog().await;
        let table = create_table(&catalog).await;

        let table = commit(&catalog, &table, |tx| {
            tx.update_partition_spec().remove_field("data_bucket")
        })
        .await;
        let table = commit(&catalog, &table, |tx| {
            tx.update_partition_spec()
                .add_field("data", Transform::Bucket(16))
        })
        .await;

        // The re-added field matches spec 0, so that spec becomes the default again.
        assert_default_spec(&table, 0, vec![data_bucket()]);
        assert_eq!(table.metadata().last_partition_id(), 1000);
    }

    #[tokio::test]
    async fn test_table_add_non_default_spec() {
        let catalog = new_memory_catalog().await;
        let table = create_table(&catalog).await;

        let table = commit(&catalog, &table, |tx| {
            tx.update_partition_spec()
                .add_identity("id")
                .add_non_default_spec()
        })
        .await;

        assert_default_spec(&table, 0, vec![data_bucket()]);
        assert_eq!(
            table.metadata().partition_spec_by_id(1).unwrap().fields(),
            [data_bucket(), field(1, 1001, "id", Transform::Identity)]
        );
    }
}
