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

use crate::error::Result;
use crate::spec::{
    FormatVersion, PartitionField, PartitionSpec, Schema, Transform, UnboundPartitionField,
    UnboundPartitionSpec,
};
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

/// The `spec_id` sentinel that tells `TableMetadataBuilder::set_default_partition_spec` to use the
/// spec that was just added in the same set of changes (Java's `LAST_ADDED`).
const LAST_ADDED_SPEC_ID: i32 = -1;

/// A pending partition-spec evolution operation. Builder methods record these in order; `commit`
/// replays them against `table.metadata()` (mirroring `transaction/manage_snapshots.rs`, which
/// defers all validation until a `Table` is available).
///
/// The ordering matters: Java's `BaseUpdatePartitionSpec` mutates shared state (`deletes`,
/// `renames`, added-field maps) on each call so later calls observe earlier ones (e.g. the
/// delete-then-re-add rewrite, the void-collision rename). We preserve that by replaying the ops
/// sequentially through the same state machine at commit time.
#[derive(Debug, Clone)]
enum PartitionSpecOp {
    /// Add a partition field for `source_name` with `transform`. When `name` is `None` the partition
    /// field name is auto-generated to match Java's `PartitionNameGenerator`.
    Add {
        name: Option<String>,
        source_name: String,
        transform: Transform,
    },
    /// Remove a partition field by its partition (target) name.
    RemoveByName { name: String },
    /// Remove a partition field by its `(source_name, transform)` pair.
    RemoveByTransform {
        source_name: String,
        transform: Transform,
    },
    /// Rename an existing partition field.
    Rename { name: String, new_name: String },
}

/// Transaction action for partition spec evolution — the Rust mirror of Java's
/// `BaseUpdatePartitionSpec`.
///
/// Builder methods record pending ops; [`TransactionAction::commit`] resolves them against the
/// table's current default spec and emits a [`TableUpdate::AddSpec`] for the new (unbound) spec,
/// followed (unless [`add_non_default_spec`](Self::add_non_default_spec) was called) by a
/// [`TableUpdate::SetDefaultSpec`] with the `LAST_ADDED` (`-1`) sentinel. Optimistic-concurrency
/// guards on the default spec id and last-assigned partition id are attached as requirements.
pub struct UpdatePartitionSpecAction {
    case_sensitive: bool,
    set_as_default: bool,
    ops: Vec<PartitionSpecOp>,
}

impl UpdatePartitionSpecAction {
    /// Create a new, empty partition-spec update. Source-column resolution is case-sensitive by
    /// default, matching Java.
    pub fn new() -> Self {
        UpdatePartitionSpecAction {
            case_sensitive: true,
            set_as_default: true,
            ops: vec![],
        }
    }

    /// Set whether source-column resolution against the schema is case-sensitive (default `true`).
    pub fn case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
        self
    }

    /// Do NOT set the new spec as the table's default (Java `addNonDefaultSpec`). The default is to
    /// set it.
    pub fn add_non_default_spec(mut self) -> Self {
        self.set_as_default = false;
        self
    }

    /// Add an identity partition field on `source_name`. The partition field name is the source
    /// column name (matching Java `addField(String)`).
    pub fn add_field(mut self, source_name: &str) -> Self {
        self.ops.push(PartitionSpecOp::Add {
            name: None,
            source_name: source_name.to_string(),
            transform: Transform::Identity,
        });
        self
    }

    /// Add a partition field on `source_name` with `transform`. When `name` is `None` the partition
    /// field name is auto-generated to match Java's `PartitionNameGenerator`
    /// (identity → `sourceName`, bucket[n] → `sourceName_bucket_n`, truncate[w] →
    /// `sourceName_trunc_w`, year → `sourceName_year`, month/day/hour likewise, void →
    /// `sourceName_null`).
    pub fn add_field_with_transform(
        mut self,
        name: Option<&str>,
        source_name: &str,
        transform: Transform,
    ) -> Self {
        self.ops.push(PartitionSpecOp::Add {
            name: name.map(str::to_string),
            source_name: source_name.to_string(),
            transform,
        });
        self
    }

    /// Remove a partition field by its partition (target) name.
    pub fn remove_field(mut self, name: &str) -> Self {
        self.ops.push(PartitionSpecOp::RemoveByName {
            name: name.to_string(),
        });
        self
    }

    /// Remove a partition field by its `(source_name, transform)` pair.
    pub fn remove_field_by_transform(mut self, source_name: &str, transform: Transform) -> Self {
        self.ops.push(PartitionSpecOp::RemoveByTransform {
            source_name: source_name.to_string(),
            transform,
        });
        self
    }

    /// Rename an existing partition field.
    pub fn rename_field(mut self, name: &str, new_name: &str) -> Self {
        self.ops.push(PartitionSpecOp::Rename {
            name: name.to_string(),
            new_name: new_name.to_string(),
        });
        self
    }
}

impl Default for UpdatePartitionSpecAction {
    fn default() -> Self {
        Self::new()
    }
}

fn data_invalid(message: String) -> Error {
    Error::new(ErrorKind::DataInvalid, message)
}

/// Generate the partition field name for a `(source_name, transform)` pair, matching Java's
/// `PartitionNameGenerator` exactly.
fn generate_partition_name(source_name: &str, transform: &Transform) -> String {
    match transform {
        Transform::Identity => source_name.to_string(),
        Transform::Bucket(num_buckets) => format!("{source_name}_bucket_{num_buckets}"),
        Transform::Truncate(width) => format!("{source_name}_trunc_{width}"),
        Transform::Year => format!("{source_name}_year"),
        Transform::Month => format!("{source_name}_month"),
        Transform::Day => format!("{source_name}_day"),
        Transform::Hour => format!("{source_name}_hour"),
        Transform::Void => format!("{source_name}_null"),
        // Java has no name-generation rule for unknown transforms (it rejects them up front); we
        // never reach here because `Transform::Unknown` cannot be produced by the builder API.
        Transform::Unknown => format!("{source_name}_unknown"),
    }
}

/// Returns true for the time transforms (year/month/day/hour), matching Java's `IsTimeTransform`.
fn is_time_transform(transform: &Transform) -> bool {
    matches!(
        transform,
        Transform::Year | Transform::Month | Transform::Day | Transform::Hour
    )
}

/// The in-flight resolution state — the Rust mirror of `BaseUpdatePartitionSpec`'s mutable fields.
/// Built fresh inside `commit` from the table's current default spec, then mutated as each recorded
/// op is replayed.
struct SpecEvolution<'a> {
    schema: &'a Schema,
    format_version: FormatVersion,
    case_sensitive: bool,
    /// Whether the table is at format version V2+ (controls field-id/name recycling and the
    /// alwaysNull-on-delete replacement). Mirrors Java's `formatVersion >= 2` guard.
    is_v2_or_later: bool,
    /// The base spec being evolved (the table's current default spec).
    base_fields: &'a [PartitionField],
    /// Every partition field across ALL of the table's historical specs — the recycling pool that
    /// Java's `recycleOrCreatePartitionField` walks (`base.specs()`). Used to reuse a historical
    /// field's id AND name for an equivalent `(source_id, transform)` add. Empty in the V1 case.
    historical_fields: Vec<PartitionField>,
    /// Partition (target) name → existing field in the base spec.
    name_to_field: HashMap<String, PartitionField>,
    /// `(source_id, transform.to_string())` → existing field in the base spec.
    transform_to_field: HashMap<(i32, String), PartitionField>,
    /// Newly added fields, in add order.
    adds: Vec<UnboundPartitionField>,
    /// Source id → the time field added for it (redundant-time-transform guard).
    added_time_fields: HashMap<i32, ()>,
    /// `(source_id, transform.to_string())` → added field name.
    transform_to_added_field: HashMap<(i32, String), String>,
    /// Partition (target) name → added field (duplicate-name guard).
    name_to_added_field: HashSet<String>,
    /// Field ids of base fields marked for deletion.
    deletes: HashSet<i32>,
    /// Base partition field name → its replacement name.
    renames: HashMap<String, String>,
}

impl<'a> SpecEvolution<'a> {
    fn new(
        schema: &'a Schema,
        format_version: FormatVersion,
        case_sensitive: bool,
        base_spec: &'a PartitionSpec,
        historical_fields: Vec<PartitionField>,
    ) -> Self {
        let base_fields = base_spec.fields();
        let mut name_to_field = HashMap::with_capacity(base_fields.len());
        let mut transform_to_field = HashMap::with_capacity(base_fields.len());
        for field in base_fields {
            name_to_field.insert(field.name.clone(), field.clone());
            transform_to_field.insert(
                (field.source_id, field.transform.to_string()),
                field.clone(),
            );
        }
        SpecEvolution {
            schema,
            format_version,
            case_sensitive,
            is_v2_or_later: format_version >= FormatVersion::V2,
            base_fields,
            historical_fields,
            name_to_field,
            transform_to_field,
            adds: vec![],
            added_time_fields: HashMap::new(),
            transform_to_added_field: HashMap::new(),
            name_to_added_field: HashSet::new(),
            deletes: HashSet::new(),
            renames: HashMap::new(),
        }
    }

    /// Resolve a source column name to its field id, honoring the case-sensitivity flag. Mirrors
    /// Java's `resolve(term)` schema lookup.
    fn resolve_source_id(&self, source_name: &str) -> Result<i32> {
        let field = if self.case_sensitive {
            self.schema.field_by_name(source_name)
        } else {
            self.schema.field_by_name_case_insensitive(source_name)
        };
        field.map(|f| f.id).ok_or_else(|| {
            data_invalid(format!(
                "Cannot find source column in schema: {source_name}"
            ))
        })
    }

    /// Replay `addField(name, term)` — Java `BaseUpdatePartitionSpec.addField`.
    fn add_field(
        &mut self,
        name: Option<&str>,
        source_name: &str,
        transform: Transform,
    ) -> Result<()> {
        if let Some(name) = name
            && self.name_to_added_field.contains(name)
        {
            return Err(data_invalid(format!(
                "Cannot add duplicate partition field: {name}"
            )));
        }

        let source_id = self.resolve_source_id(source_name)?;
        let validation_key = (source_id, transform.to_string());

        // delete-then-re-add (rewriteDeleteAndAddField): if an existing field with this exact
        // (source, transform) is being deleted in this action, un-delete it and optionally rename
        // it instead of adding a new field.
        if let Some(existing) = self.transform_to_field.get(&validation_key).cloned()
            && self.deletes.contains(&existing.field_id)
            && existing.transform == transform
        {
            return self.rewrite_delete_and_add_field(&existing, name);
        }

        // Reject a duplicate (source, transform) UNLESS the existing one is being deleted AND has a
        // different transform string (Java's second precondition).
        if let Some(existing) = self.transform_to_field.get(&validation_key) {
            let allowed = self.deletes.contains(&existing.field_id)
                && existing.transform.to_string() != transform.to_string();
            if !allowed {
                return Err(data_invalid(format!(
                    "Cannot add duplicate partition field, conflicts with existing field: {}",
                    existing.name
                )));
            }
        }

        if self.transform_to_added_field.contains_key(&validation_key) {
            return Err(data_invalid(
                "Cannot add duplicate partition field: already added in this action".to_string(),
            ));
        }

        // Recycle a historical field id + name (Java `recycleOrCreatePartitionField`), then resolve
        // the partition field name. Precedence (matching Java): a recycled historical name wins when
        // the add had no explicit name; otherwise the explicit name; otherwise the generated default.
        let (recycled_field_id, recycled_name) =
            self.recycle_or_create_partition_field(source_id, &transform, name);
        let partition_name = match (name, recycled_name) {
            (Some(name), _) => name.to_string(),
            (None, Some(historical_name)) => historical_name,
            (None, None) => generate_partition_name(source_name, &transform),
        };

        self.check_for_redundant_added_partition(source_id, &transform)?;
        self.transform_to_added_field
            .insert(validation_key, partition_name.clone());

        // Name-collision handling against the base spec.
        if let Some(existing_field) = self.name_to_field.get(&partition_name).cloned() {
            if !self.deletes.contains(&existing_field.field_id) {
                if existing_field.transform == Transform::Void {
                    // The new field replaces a void (effectively-dropped) field of the same name:
                    // rename the old void field out of the way.
                    self.rename_field(
                        &existing_field.name,
                        &format!("{}_{}", existing_field.name, existing_field.field_id),
                    )?;
                } else {
                    return Err(data_invalid(format!(
                        "Cannot add duplicate partition field name: {partition_name}"
                    )));
                }
            } else {
                // The colliding field is already being deleted: just rename it out of the way.
                self.renames.insert(
                    existing_field.name.clone(),
                    format!("{}_{}", existing_field.name, existing_field.field_id),
                );
            }
        }

        self.name_to_added_field.insert(partition_name.clone());
        self.adds.push(UnboundPartitionField {
            source_id,
            // When a historical equivalent `(source, transform)` field was recycled, carry its field
            // id explicitly (matching Java's `recycleOrCreatePartitionField`). Otherwise leave it
            // unset so `TableMetadataBuilder::add_partition_spec` assigns a fresh sequential id.
            field_id: recycled_field_id,
            name: partition_name,
            transform,
        });

        Ok(())
    }

    /// Java `recycleOrCreatePartitionField`: in V2+ tables, search every historical partition spec
    /// for a field with the same `(source_id, transform)`. On a match — respecting Java's rule that
    /// the target name must be unspecified (`None`) or already equal — return that historical field's
    /// `(field_id, name)` so an add reuses both. Returns `(None, None)` when nothing is recycled,
    /// which leaves the id for `TableMetadataBuilder` to assign and the name to be generated.
    ///
    /// `name` is the user-supplied name (not the auto-generated one): when it is `Some`, the recycled
    /// name is irrelevant (the user's name wins) but the id is still reused, and the match is gated on
    /// the historical name equalling it — exactly as Java does.
    fn recycle_or_create_partition_field(
        &self,
        source_id: i32,
        transform: &Transform,
        name: Option<&str>,
    ) -> (Option<i32>, Option<String>) {
        if !self.is_v2_or_later {
            return (None, None);
        }
        for field in &self.historical_fields {
            if field.source_id == source_id
                && field.transform == *transform
                && (name.is_none() || name == Some(field.name.as_str()))
            {
                return (Some(field.field_id), Some(field.name.clone()));
            }
        }
        (None, None)
    }

    /// Java `rewriteDeleteAndAddField`: un-delete an existing field, optionally renaming it.
    fn rewrite_delete_and_add_field(
        &mut self,
        existing: &PartitionField,
        name: Option<&str>,
    ) -> Result<()> {
        self.deletes.remove(&existing.field_id);
        match name {
            None => Ok(()),
            Some(name) if name == existing.name => Ok(()),
            Some(name) => self.rename_field(&existing.name, name),
        }
    }

    /// Java `removeField(String name)`.
    fn remove_field_by_name(&mut self, name: &str) -> Result<()> {
        if self.name_to_added_field.contains(name) {
            return Err(data_invalid(format!(
                "Cannot delete newly added field: {name}"
            )));
        }
        if self.renames.contains_key(name) {
            return Err(data_invalid(format!(
                "Cannot rename and delete partition field: {name}"
            )));
        }
        let field = self.name_to_field.get(name).ok_or_else(|| {
            data_invalid(format!("Cannot find partition field to remove: {name}"))
        })?;
        self.deletes.insert(field.field_id);
        Ok(())
    }

    /// Java `removeField(Term term)`.
    fn remove_field_by_transform(&mut self, source_name: &str, transform: Transform) -> Result<()> {
        let source_id = self.resolve_source_id(source_name)?;
        let key = (source_id, transform.to_string());
        if self.transform_to_added_field.contains_key(&key) {
            return Err(data_invalid("Cannot delete newly added field".to_string()));
        }
        let field = self.transform_to_field.get(&key).cloned().ok_or_else(|| {
            data_invalid(format!(
                "Cannot find partition field to remove: {source_name} {transform}"
            ))
        })?;
        if self.renames.contains_key(&field.name) {
            return Err(data_invalid(format!(
                "Cannot rename and delete partition field: {}",
                field.name
            )));
        }
        self.deletes.insert(field.field_id);
        Ok(())
    }

    /// Java `renameField(String name, String newName)`.
    fn rename_field(&mut self, name: &str, new_name: &str) -> Result<()> {
        // Void-collision: if the target name belongs to an existing void field, rename that field
        // out of the way first.
        if let Some(existing_field) = self.name_to_field.get(new_name).cloned()
            && existing_field.transform == Transform::Void
        {
            self.rename_field(
                &existing_field.name,
                &format!("{}_{}", existing_field.name, existing_field.field_id),
            )?;
        }

        if self.name_to_added_field.contains(name) {
            return Err(data_invalid(format!(
                "Cannot rename newly added partition field: {name}"
            )));
        }
        let field = self.name_to_field.get(name).ok_or_else(|| {
            data_invalid(format!("Cannot find partition field to rename: {name}"))
        })?;
        if self.deletes.contains(&field.field_id) {
            return Err(data_invalid(format!(
                "Cannot delete and rename partition field: {name}"
            )));
        }
        self.renames.insert(name.to_string(), new_name.to_string());
        Ok(())
    }

    /// Java `checkForRedundantAddedPartitions`: reject two time transforms on the same source.
    fn check_for_redundant_added_partition(
        &mut self,
        source_id: i32,
        transform: &Transform,
    ) -> Result<()> {
        if is_time_transform(transform) {
            if self.added_time_fields.contains_key(&source_id) {
                return Err(data_invalid(format!(
                    "Cannot add redundant partition field: a time transform on source id \
                     {source_id} was already added"
                )));
            }
            self.added_time_fields.insert(source_id, ());
        }
        Ok(())
    }

    /// Build the resulting unbound partition spec — the Rust mirror of Java's `apply()`. For each
    /// base field: keep it (applying any rename) unless deleted; if deleted and V1, re-add it with
    /// the Void transform (preserving its field id) to keep field ids stable; if deleted and V2,
    /// omit it. Then append the added fields (with unset field ids for recycling/assignment).
    fn apply(self) -> UnboundPartitionSpec {
        let mut fields: Vec<UnboundPartitionField> =
            Vec::with_capacity(self.base_fields.len() + self.adds.len());

        for field in self.base_fields {
            if !self.deletes.contains(&field.field_id) {
                let name = self
                    .renames
                    .get(&field.name)
                    .cloned()
                    .unwrap_or_else(|| field.name.clone());
                fields.push(UnboundPartitionField {
                    source_id: field.source_id,
                    field_id: Some(field.field_id),
                    name,
                    transform: field.transform,
                });
            } else if self.format_version < FormatVersion::V2 {
                // V1: replace a removed field with a Void transform, preserving its field id so
                // field ids stay consistent across the table's partition specs.
                let name = self
                    .renames
                    .get(&field.name)
                    .cloned()
                    .unwrap_or_else(|| field.name.clone());
                fields.push(UnboundPartitionField {
                    source_id: field.source_id,
                    field_id: Some(field.field_id),
                    name,
                    transform: Transform::Void,
                });
            }
            // V2 deleted field: omitted entirely.
        }

        fields.extend(self.adds);

        UnboundPartitionSpec {
            spec_id: None,
            fields,
        }
    }
}

#[async_trait]
impl TransactionAction for UpdatePartitionSpecAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let metadata = table.metadata();
        let schema = metadata.current_schema();
        let base_spec = metadata.default_partition_spec();

        // The recycling pool: every partition field across all of the table's historical specs
        // (Java's `base.specs()`). Cloned because `SpecEvolution` outlives the borrow on individual
        // specs and the set is small (one entry per historical partition field).
        let historical_fields: Vec<PartitionField> = metadata
            .partition_specs_iter()
            .flat_map(|spec| spec.fields().iter().cloned())
            .collect();

        let mut evolution = SpecEvolution::new(
            schema,
            metadata.format_version(),
            self.case_sensitive,
            base_spec,
            historical_fields,
        );

        for op in &self.ops {
            match op {
                PartitionSpecOp::Add {
                    name,
                    source_name,
                    transform,
                } => {
                    evolution.add_field(name.as_deref(), source_name, *transform)?;
                }
                PartitionSpecOp::RemoveByName { name } => {
                    evolution.remove_field_by_name(name)?;
                }
                PartitionSpecOp::RemoveByTransform {
                    source_name,
                    transform,
                } => {
                    evolution.remove_field_by_transform(source_name, *transform)?;
                }
                PartitionSpecOp::Rename { name, new_name } => {
                    evolution.rename_field(name, new_name)?;
                }
            }
        }

        let unbound_spec = evolution.apply();

        let mut updates = vec![TableUpdate::AddSpec { spec: unbound_spec }];
        if self.set_as_default {
            updates.push(TableUpdate::SetDefaultSpec {
                spec_id: LAST_ADDED_SPEC_ID,
            });
        }

        // Optimistic-concurrency guards. Java derives these from the emitted updates
        // (`UpdateRequirements`): the `AddPartitionSpec` update always requires the last-assigned
        // partition id to match, while the default-spec-id guard is only attached when a
        // `SetDefaultPartitionSpec` update is emitted. Under `add_non_default_spec` there is no such
        // update, so the default-spec-id guard must be omitted — emitting it would over-constrain the
        // commit relative to Java.
        let mut requirements = vec![TableRequirement::LastAssignedPartitionIdMatch {
            last_assigned_partition_id: metadata.last_partition_id(),
        }];
        if self.set_as_default {
            requirements.push(TableRequirement::DefaultSpecIdMatch {
                default_spec_id: metadata.default_partition_spec_id(),
            });
        }

        Ok(ActionCommit::new(updates, requirements))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::TableUpdate;
    use crate::spec::{PartitionSpec, Schema, Transform, UnboundPartitionSpec};
    use crate::table::Table;
    use crate::transaction::{Transaction, TransactionAction};

    fn v2_table() -> Table {
        crate::transaction::tests::make_v2_table()
    }

    fn v1_table() -> Table {
        crate::transaction::tests::make_v1_table()
    }

    /// `make_v2_table()` evolved through a second historical partition spec so that field-id
    /// recycling has something to find. Spec 0 is identity(x) at field id 1000. We add spec 1 with
    /// bucket[8](y) at field id 1001, then RESET the default back to spec 0. Now `last_partition_id`
    /// is 1001 and the table knows a historical (y, bucket[8]) → 1001 mapping that a later add of
    /// bucket[8](y) must recycle.
    fn forked_v2_table() -> Table {
        let base = v2_table();
        let metadata = base
            .metadata()
            .clone()
            .into_builder(None)
            .add_partition_spec(
                UnboundPartitionSpec::builder()
                    .add_partition_field(2, "y_bucket", Transform::Bucket(8))
                    .expect("add y bucket field")
                    .build(),
            )
            .expect("add historical spec")
            .build()
            .expect("build forked metadata")
            .metadata;
        base.with_metadata(Arc::new(metadata))
    }

    /// Extract the single `AddSpec`'s unbound spec from the updates.
    fn added_spec(updates: &[TableUpdate]) -> &UnboundPartitionSpec {
        updates
            .iter()
            .find_map(|u| match u {
                TableUpdate::AddSpec { spec } => Some(spec),
                _ => None,
            })
            .expect("an AddSpec update")
    }

    fn has_set_default(updates: &[TableUpdate]) -> bool {
        updates
            .iter()
            .any(|u| matches!(u, TableUpdate::SetDefaultSpec { .. }))
    }

    /// Bind an emitted unbound spec to a schema and return the bound fields. Field ids in the
    /// unbound spec are honored when set, else assigned from 1000 — this lets us assert on the
    /// `apply()` output shape (it does NOT route through the metadata builder's recycling).
    fn bind(spec: &UnboundPartitionSpec, schema: &Schema) -> PartitionSpec {
        spec.clone()
            .bind(Arc::new(schema.clone()))
            .expect("bind unbound spec")
    }

    /// Drive ALL emitted `TableUpdate`s (AddSpec + optional SetDefaultSpec) through the table's
    /// `TableMetadataBuilder`, returning the resulting metadata. This proves the end-to-end commit
    /// path — not just the `apply()` unbound shape — including the metadata layer's spec dedup and
    /// `LAST_ADDED` resolution. Mirrors what a catalog does on commit.
    fn apply_updates(table: &Table, updates: Vec<TableUpdate>) -> crate::spec::TableMetadata {
        let mut builder = table.metadata().clone().into_builder(None);
        for update in updates {
            builder = update.apply(builder).expect("apply table update");
        }
        builder.build().expect("build metadata").metadata
    }

    // RISK: an identity add must produce an unbound field named for the source column with the
    // Identity transform, source id resolved from the schema — the base case of partition evolution.
    #[tokio::test]
    async fn test_add_identity_field_uses_source_name() {
        let table = v2_table();
        let action = Transaction::new(&table)
            .update_partition_spec()
            .add_field("y");
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        let spec = added_spec(&updates);

        // base identity(x) preserved + new identity(y)
        assert_eq!(spec.fields().len(), 2);
        let added = &spec.fields()[1];
        assert_eq!(added.name, "y");
        assert_eq!(added.source_id, 2);
        assert_eq!(added.transform, Transform::Identity);
        assert_eq!(added.field_id, None, "added field id must be left unset");
    }

    // RISK: auto-generated names must EXACTLY match Java's PartitionNameGenerator for every
    // transform — a name drift silently produces incompatible specs vs Java-written tables.
    #[tokio::test]
    async fn test_add_with_transform_auto_names_match_java() {
        let table = v2_table();
        let cases = [
            (Transform::Identity, "y"),
            (Transform::Bucket(8), "y_bucket_8"),
            (Transform::Truncate(4), "y_trunc_4"),
            (Transform::Year, "y_year"),
            (Transform::Month, "y_month"),
            (Transform::Day, "y_day"),
            (Transform::Hour, "y_hour"),
            (Transform::Void, "y_null"),
        ];
        for (transform, expected_name) in cases {
            let action = Transaction::new(&table)
                .update_partition_spec()
                .add_field_with_transform(None, "y", transform);
            let mut commit = Arc::new(action).commit(&table).await.unwrap();
            let updates = commit.take_updates();
            let spec = added_spec(&updates);
            let added = spec
                .fields()
                .iter()
                .find(|f| f.transform == transform && f.source_id == 2)
                .unwrap_or_else(|| panic!("added field for {transform} missing"));
            assert_eq!(
                added.name, expected_name,
                "auto-name for {transform} must match Java"
            );
        }
    }

    // RISK: an explicit name must override auto-generation.
    #[tokio::test]
    async fn test_add_with_explicit_name() {
        let table = v2_table();
        let action = Transaction::new(&table)
            .update_partition_spec()
            .add_field_with_transform(Some("my_bucket"), "y", Transform::Bucket(8));
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let spec = added_spec(&commit.take_updates()).clone();
        assert!(spec.fields().iter().any(|f| f.name == "my_bucket"));
    }

    // RISK: removing the only base field on a V2 table must OMIT it entirely (no void replacement),
    // leaving an unpartitioned spec — V1/V2 divergence is the core apply() behavior.
    #[tokio::test]
    async fn test_remove_by_name_v2_omits_field() {
        let table = v2_table();
        let action = Transaction::new(&table)
            .update_partition_spec()
            .remove_field("x");
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let spec = added_spec(&commit.take_updates()).clone();
        assert!(spec.fields().is_empty(), "V2 remove must omit the field");
    }

    // RISK: remove-by-(source,transform) must target the same field as remove-by-name.
    #[tokio::test]
    async fn test_remove_by_transform_v2_omits_field() {
        let table = v2_table();
        let action = Transaction::new(&table)
            .update_partition_spec()
            .remove_field_by_transform("x", Transform::Identity);
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let spec = added_spec(&commit.take_updates()).clone();
        assert!(spec.fields().is_empty());
    }

    // RISK: rename must change the partition field name while preserving its field id (so existing
    // partition data stays addressable).
    #[tokio::test]
    async fn test_rename_preserves_field_id() {
        let table = v2_table();
        let action = Transaction::new(&table)
            .update_partition_spec()
            .rename_field("x", "x_renamed");
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let spec = added_spec(&commit.take_updates()).clone();
        let field = &spec.fields()[0];
        assert_eq!(field.name, "x_renamed");
        assert_eq!(field.field_id, Some(1000));
        assert_eq!(field.transform, Transform::Identity);
    }

    // RISK: add_non_default_spec must emit AddSpec WITHOUT a SetDefaultSpec — Java addNonDefaultSpec.
    #[tokio::test]
    async fn test_add_non_default_spec_emits_no_set_default() {
        let table = v2_table();
        let action = Transaction::new(&table)
            .update_partition_spec()
            .add_field("y")
            .add_non_default_spec();
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        assert!(matches!(updates[0], TableUpdate::AddSpec { .. }));
        assert!(
            !has_set_default(&updates),
            "add_non_default_spec must not set the default"
        );
    }

    // RISK: a default update MUST emit SetDefaultSpec with the LAST_ADDED (-1) sentinel.
    #[tokio::test]
    async fn test_default_update_emits_set_default_minus_one() {
        let table = v2_table();
        let action = Transaction::new(&table)
            .update_partition_spec()
            .add_field("y");
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        assert!(
            updates
                .iter()
                .any(|u| matches!(u, TableUpdate::SetDefaultSpec { spec_id } if *spec_id == -1))
        );
    }

    // RISK: two adds with the SAME partition field name must be rejected (Java nameToAddedField).
    #[tokio::test]
    async fn test_duplicate_added_name_fails() {
        let table = v2_table();
        let action = Transaction::new(&table)
            .update_partition_spec()
            .add_field_with_transform(Some("dup"), "y", Transform::Bucket(8))
            .add_field_with_transform(Some("dup"), "z", Transform::Bucket(8));
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    // RISK: two TIME transforms on the same source column must be rejected
    // (checkForRedundantAddedPartitions) — they would produce overlapping partitions.
    #[tokio::test]
    async fn test_redundant_time_transform_fails() {
        let table = v2_table();
        let action = Transaction::new(&table)
            .update_partition_spec()
            .add_field_with_transform(None, "y", Transform::Year)
            .add_field_with_transform(None, "y", Transform::Month);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    // RISK: removing a field added in the same action must be rejected — Java forbids it.
    #[tokio::test]
    async fn test_remove_newly_added_field_fails() {
        let table = v2_table();
        let action = Transaction::new(&table)
            .update_partition_spec()
            .add_field_with_transform(Some("y_b"), "y", Transform::Bucket(8))
            .remove_field("y_b");
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    // RISK: renaming a field then deleting it (or vice versa) in one action must be rejected.
    #[tokio::test]
    async fn test_rename_then_delete_same_field_fails() {
        let table = v2_table();
        let action = Transaction::new(&table)
            .update_partition_spec()
            .rename_field("x", "x2")
            .remove_field("x");
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    // RISK: delete-then-re-add of the SAME (source, transform) must UN-delete the field rather than
    // error or duplicate it (Java rewriteDeleteAndAddField).
    #[tokio::test]
    async fn test_delete_then_readd_undeletes() {
        let table = v2_table();
        let action = Transaction::new(&table)
            .update_partition_spec()
            .remove_field("x")
            .add_field("x"); // identity(x) again
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let spec = added_spec(&commit.take_updates()).clone();
        // The field is restored (un-deleted), keeping its original field id, not duplicated.
        assert_eq!(spec.fields().len(), 1);
        let field = &spec.fields()[0];
        assert_eq!(field.name, "x");
        assert_eq!(field.field_id, Some(1000));
        assert_eq!(field.transform, Transform::Identity);
    }

    // RISK: delete-then-re-add WITH a new name must un-delete AND rename (Java
    // rewriteDeleteAndAddField rename branch).
    #[tokio::test]
    async fn test_delete_then_readd_with_new_name_renames() {
        let table = v2_table();
        let action = Transaction::new(&table)
            .update_partition_spec()
            .remove_field("x")
            .add_field_with_transform(Some("x_new"), "x", Transform::Identity);
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let spec = added_spec(&commit.take_updates()).clone();
        assert_eq!(spec.fields().len(), 1);
        assert_eq!(spec.fields()[0].name, "x_new");
        assert_eq!(spec.fields()[0].field_id, Some(1000));
    }

    // RISK: on a V1 table, removing a field must REPLACE it with a Void transform preserving the
    // field id (V1 alwaysNull replacement) — dropping it would shift field ids and corrupt the
    // table for older readers.
    #[tokio::test]
    async fn test_remove_v1_replaces_with_void() {
        let table = v1_table();
        let action = Transaction::new(&table)
            .update_partition_spec()
            .remove_field("x");
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let spec = added_spec(&commit.take_updates()).clone();
        assert_eq!(spec.fields().len(), 1, "V1 must keep the field slot");
        let field = &spec.fields()[0];
        assert_eq!(field.name, "x");
        assert_eq!(field.field_id, Some(1000));
        assert_eq!(field.transform, Transform::Void);
    }

    // RISK: on V1, when a deleted field's NAME is reused by a newly added field, the old field must
    // be re-added as Void under a uniquified name (`oldName_fieldId`) so its field id stays stable
    // AND the new field can take the name (Java's delete-collision rename branch through V1 apply()).
    #[tokio::test]
    async fn test_remove_then_readd_same_name_v1_renames_void() {
        let table = v1_table();
        // delete identity(x) then add bucket(x) under the SAME name "x".
        let action = Transaction::new(&table)
            .update_partition_spec()
            .remove_field("x")
            .add_field_with_transform(Some("x"), "x", Transform::Bucket(8));
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let spec = added_spec(&commit.take_updates()).clone();
        // Old void-replaced field keeps id 1000 under the uniquified name; new bucket field is added.
        let void_field = spec
            .fields()
            .iter()
            .find(|f| f.transform == Transform::Void)
            .expect("V1 void replacement for the dropped field");
        assert_eq!(void_field.name, "x_1000");
        assert_eq!(void_field.field_id, Some(1000));
        let bucket_field = spec
            .fields()
            .iter()
            .find(|f| f.transform == Transform::Bucket(8))
            .expect("new bucket field");
        assert_eq!(bucket_field.name, "x");
    }

    // RISK: field-id recycling — an add of a (source, transform) that exists in a HISTORICAL spec
    // must reuse that historical field id, not allocate a fresh one. This is the Rust-native
    // equivalent of Java recycleOrCreatePartitionField and is proven only with a multi-spec fixture.
    #[tokio::test]
    async fn test_field_id_recycled_across_historical_specs() {
        let table = forked_v2_table();
        // Spec 0 (identity x @1000) is the default; spec 1 had bucket[8](y) @1001 historically.
        // Adding bucket[8](y) must recycle field id 1001 via the metadata builder.
        let action = Transaction::new(&table)
            .update_partition_spec()
            .add_field_with_transform(None, "y", Transform::Bucket(8));
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();

        // Drive the emitted updates through the metadata builder, which performs the recycling, and
        // make the evolved spec the default so we can inspect its bound field ids.
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .add_default_partition_spec(added_spec(&updates).clone())
            .expect("add evolved spec")
            .build()
            .expect("build evolved metadata")
            .metadata;
        let new_spec = metadata.default_partition_spec();
        let recycled = new_spec
            .fields()
            .iter()
            .find(|f| f.source_id == 2 && f.transform == Transform::Bucket(8))
            .expect("recycled bucket field");
        assert_eq!(
            recycled.field_id, 1001,
            "must recycle the historical field id, not assign a fresh one"
        );
        // PARITY (recycleOrCreatePartitionField): with no explicit name, the recycled HISTORICAL
        // name ("y_bucket") must be reused — NOT the generated default ("y_bucket_8"). The builder's
        // id-only recycling cannot do this; the action must carry the historical name forward.
        assert_eq!(
            recycled.name, "y_bucket",
            "must reuse the historical field name when recycling without an explicit name"
        );
        // last_partition_id must NOT grow, since no new id was assigned.
        assert_eq!(metadata.last_partition_id(), 1001);
    }

    // RISK: a fresh (source, transform) with no historical match must get a NEW id from
    // last_partition_id+1, and last_partition_id must advance.
    #[tokio::test]
    async fn test_new_field_id_assigned_when_no_recycle() {
        let table = v2_table(); // last_partition_id = 1000
        let action = Transaction::new(&table)
            .update_partition_spec()
            .add_field_with_transform(None, "y", Transform::Bucket(8));
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .add_default_partition_spec(added_spec(&updates).clone())
            .expect("add evolved spec")
            .build()
            .expect("build evolved metadata")
            .metadata;
        assert_eq!(metadata.last_partition_id(), 1001);
    }

    // RISK: case-insensitive source resolution must locate a column by a differently-cased name;
    // the default (case-sensitive) must reject it.
    #[tokio::test]
    async fn test_case_insensitive_source_resolution() {
        let table = v2_table();
        // case-sensitive default: "Y" is not a column -> error.
        let strict = Transaction::new(&table)
            .update_partition_spec()
            .add_field("Y");
        assert!(Arc::new(strict).commit(&table).await.is_err());

        // case-insensitive: "Y" resolves to "y".
        let lenient = Transaction::new(&table)
            .update_partition_spec()
            .case_sensitive(false)
            .add_field_with_transform(Some("y_part"), "Y", Transform::Identity);
        let mut commit = Arc::new(lenient).commit(&table).await.unwrap();
        let spec = added_spec(&commit.take_updates()).clone();
        let added = spec.fields().iter().find(|f| f.name == "y_part").unwrap();
        assert_eq!(added.source_id, 2, "Y must resolve to column y (id 2)");
    }

    // RISK: an unknown source column must be rejected with a clear error.
    #[tokio::test]
    async fn test_unknown_source_column_fails() {
        let table = v2_table();
        let action = Transaction::new(&table)
            .update_partition_spec()
            .add_field("does_not_exist");
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    // RISK: the emitted updates/requirements shape must be exactly AddSpec + SetDefaultSpec{-1} plus
    // the two optimistic-concurrency guards (DefaultSpecIdMatch + LastAssignedPartitionIdMatch).
    #[tokio::test]
    async fn test_emitted_updates_and_requirements_shape() {
        let table = v2_table();
        let action = Transaction::new(&table)
            .update_partition_spec()
            .add_field("y");
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        let requirements = commit.take_requirements();

        assert_eq!(updates.len(), 2);
        assert!(matches!(updates[0], TableUpdate::AddSpec { .. }));
        assert!(matches!(updates[1], TableUpdate::SetDefaultSpec { spec_id } if spec_id == -1));

        assert_eq!(requirements.len(), 2);
        assert!(requirements.iter().any(|r| matches!(
            r,
            crate::TableRequirement::DefaultSpecIdMatch { default_spec_id } if *default_spec_id == 0
        )));
        assert!(requirements.iter().any(|r| matches!(
            r,
            crate::TableRequirement::LastAssignedPartitionIdMatch {
                last_assigned_partition_id
            } if *last_assigned_partition_id == 1000
        )));
    }

    // RISK: adding a field whose generated name equals an existing NON-void field's name must be
    // rejected (Java duplicate partition field name).
    #[tokio::test]
    async fn test_add_colliding_name_with_non_void_existing_fails() {
        let table = v2_table();
        // existing identity(x) is named "x"; adding bucket(x) explicitly named "x" collides.
        let action = Transaction::new(&table)
            .update_partition_spec()
            .add_field_with_transform(Some("x"), "y", Transform::Bucket(8));
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    // RISK: the unbound spec must bind cleanly to the schema (proves apply() output is structurally
    // valid, not just field-shaped).
    #[tokio::test]
    async fn test_applied_spec_binds_to_schema() {
        let table = v2_table();
        let action = Transaction::new(&table)
            .update_partition_spec()
            .add_field_with_transform(None, "y", Transform::Bucket(8));
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let spec = added_spec(&commit.take_updates()).clone();
        let bound = bind(&spec, table.metadata().current_schema());
        assert_eq!(bound.fields().len(), 2);
        assert!(
            bound
                .fields()
                .iter()
                .any(|f| f.transform == Transform::Bucket(8))
        );
    }

    // RISK (end-to-end, review pt 9): the emitted AddSpec + SetDefaultSpec{-1} must actually drive
    // through TableMetadataBuilder to make the NEW spec the default. A mismatch between what the
    // action emits and what the metadata layer does would silently leave the old spec as default.
    #[tokio::test]
    async fn test_commit_updates_round_trip_to_new_default_spec() {
        let table = v2_table();
        let action = Transaction::new(&table)
            .update_partition_spec()
            .add_field("y");
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let metadata = apply_updates(&table, commit.take_updates());

        // The new default spec must be spec id 1 (a fresh spec), carrying identity(x) + identity(y).
        let default = metadata.default_partition_spec();
        assert_eq!(metadata.default_partition_spec_id(), 1);
        assert_eq!(default.fields().len(), 2);
        assert!(default.fields().iter().any(|f| f.name == "x"));
        let added = default
            .fields()
            .iter()
            .find(|f| f.name == "y")
            .expect("new identity(y) field in default spec");
        assert_eq!(added.source_id, 2);
        assert_eq!(added.transform, Transform::Identity);
        assert_eq!(
            added.field_id, 1001,
            "fresh id assigned past last_partition_id"
        );
        assert_eq!(metadata.last_partition_id(), 1001);
    }

    // RISK (review pt 10, idempotency): an action whose result equals the CURRENT default spec must
    // not create a spurious new spec — the metadata layer must dedup back to the existing spec id.
    // Matches Java testNoEffectAddDeletedSameFieldWithSameName (remove then re-add the same field).
    #[tokio::test]
    async fn test_remove_then_readd_same_field_is_noop_dedups_to_existing_spec() {
        let table = v2_table(); // default spec 0 = identity(x) @1000
        let action = Transaction::new(&table)
            .update_partition_spec()
            .remove_field("x")
            .add_field("x"); // identity(x) again -> result equals spec 0
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let metadata = apply_updates(&table, commit.take_updates());

        // No new spec: the default must remain spec 0, and last_partition_id must not advance.
        assert_eq!(
            metadata.default_partition_spec_id(),
            0,
            "no-op evolution must dedup to the existing spec id, not mint a new one"
        );
        assert_eq!(metadata.partition_specs_iter().count(), 1);
        assert_eq!(metadata.last_partition_id(), 1000);
        assert_eq!(metadata.default_partition_spec().fields().len(), 1);
    }

    // RISK (Java testRemoveAndUpdateWithDifferentTransformation, V2): removing a field then adding a
    // DIFFERENT transform on the same source under the SAME name must, on V2, OMIT the deleted field
    // (yielding a single new field) — not keep a void placeholder. This exercises the name-collision
    // branch where the colliding field is being deleted.
    #[tokio::test]
    async fn test_remove_then_readd_different_transform_same_name_v2_omits() {
        let table = v2_table(); // default spec 0 = identity(x) @1000, named "x"
        let action = Transaction::new(&table)
            .update_partition_spec()
            .remove_field("x")
            .add_field_with_transform(Some("x"), "x", Transform::Bucket(8));
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let spec = added_spec(&commit.take_updates()).clone();
        // V2: identity(x) omitted; only the new bucket field remains, taking the name "x".
        assert_eq!(spec.fields().len(), 1, "V2 must omit the deleted field");
        assert_eq!(spec.fields()[0].name, "x");
        assert_eq!(spec.fields()[0].transform, Transform::Bucket(8));
    }

    // RISK (review pt 8): under add_non_default_spec, Java emits NO SetDefaultPartitionSpec update, so
    // UpdateRequirements attaches only AssertLastAssignedPartitionId — NOT AssertDefaultSpecID.
    // Emitting the default-spec guard anyway would over-constrain the commit vs Java.
    #[tokio::test]
    async fn test_add_non_default_spec_omits_default_spec_id_requirement() {
        let table = v2_table();
        let action = Transaction::new(&table)
            .update_partition_spec()
            .add_field("y")
            .add_non_default_spec();
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let requirements = commit.take_requirements();
        assert_eq!(requirements.len(), 1, "only the last-assigned-id guard");
        assert!(requirements.iter().any(|r| matches!(
            r,
            crate::TableRequirement::LastAssignedPartitionIdMatch { .. }
        )));
        assert!(
            !requirements
                .iter()
                .any(|r| matches!(r, crate::TableRequirement::DefaultSpecIdMatch { .. })),
            "add_non_default_spec must not emit a default-spec-id guard"
        );
    }

    // RISK (review pt 3): recycling with an EXPLICIT name must reuse the historical field id while
    // keeping the user's chosen name. Java's recycleOrCreatePartitionField only recycles when the
    // historical name equals the requested name; here they match, so id 1001 is reused.
    #[tokio::test]
    async fn test_explicit_name_recycles_historical_id() {
        let table = forked_v2_table(); // historical bucket[8](y) @1001 named "y_bucket"
        let action = Transaction::new(&table)
            .update_partition_spec()
            .add_field_with_transform(Some("y_bucket"), "y", Transform::Bucket(8));
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let metadata = apply_updates(&table, commit.take_updates());
        let recycled = metadata
            .default_partition_spec()
            .fields()
            .iter()
            .find(|f| f.source_id == 2 && f.transform == Transform::Bucket(8))
            .expect("recycled bucket field");
        assert_eq!(recycled.field_id, 1001);
        assert_eq!(recycled.name, "y_bucket");
        assert_eq!(metadata.last_partition_id(), 1001);
    }
}
