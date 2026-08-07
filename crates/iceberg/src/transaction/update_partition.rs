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

use crate::spec::{
    FormatVersion, PartitionField, PartitionSpecRef, SchemaRef, TableMetadata, Transform,
    UnboundPartitionField, UnboundPartitionSpec,
};
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, Result, TableRequirement, TableUpdate, ensure_precondition};

/// Represents a source column and transform for partition field additions or removals.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Term {
    transform: Transform,
    source_name: String,
}

impl Term {
    /// Creates a new term with the given source column name and transform.    
    pub fn new(source_name: impl Into<String>, transform: Transform) -> Self {
        Self {
            transform,
            source_name: source_name.into(),
        }
    }

    /// Creates a new term with an identity transform.    
    pub fn identity(source_name: impl Into<String>) -> Self {
        Self::new(source_name, Transform::Identity)
    }

    /// Creates a new term with a year transform.    
    pub fn year(source_name: impl Into<String>) -> Self {
        Self::new(source_name, Transform::Year)
    }

    /// Creates a new term with a month transform.    
    pub fn month(source_name: impl Into<String>) -> Self {
        Self::new(source_name, Transform::Month)
    }

    /// Creates a new term with a day transform.    
    pub fn day(source_name: impl Into<String>) -> Self {
        Self::new(source_name, Transform::Day)
    }

    /// Creates a new term with an hour transform.
    pub fn hour(source_name: impl Into<String>) -> Self {
        Self::new(source_name, Transform::Hour)
    }

    /// Creates a new term with a bucket transform.
    pub fn bucket(source_name: impl Into<String>, num_buckets: u32) -> Self {
        Self::new(source_name, Transform::Bucket(num_buckets))
    }

    /// Creates a new term with a truncate transform.
    pub fn truncate(source_name: impl Into<String>, width: u32) -> Self {
        Self::new(source_name, Transform::Truncate(width))
    }
}

fn default_partition_name(source_name: &str, transform: Transform) -> String {
    match transform {
        Transform::Identity => source_name.to_string(),
        Transform::Bucket(bucket_count) => format!("{source_name}_bucket_{bucket_count}"),
        Transform::Truncate(width) => format!("{source_name}_trunc_{width}"),
        Transform::Year => format!("{source_name}_year"),
        Transform::Month => format!("{source_name}_month"),
        Transform::Day => format!("{source_name}_day"),
        Transform::Hour => format!("{source_name}_hour"),
        Transform::Void => format!("{source_name}_void"),
        Transform::Unknown => format!("{source_name}_unknown"),
    }
}

/// Transaction action for updating the default partition spec.
pub struct UpdatePartitionSpecAction {
    adds: Vec<PartitionField>,
    deletes: HashSet<i32>,
    renames: HashMap<String, String>,
    case_sensitive: bool,
    set_as_default: bool,

    // Current spec info for eager validation (mirrors Java's BaseUpdatePartitionSpec)
    schema: SchemaRef,
    current_spec: PartitionSpecRef,
    format_version: FormatVersion,
    last_assigned_partition_id: i32,
    partition_specs: Vec<PartitionSpecRef>,

    // Indices of current spec fields
    name_to_field: HashMap<String, PartitionField>,
    transform_to_field: HashMap<(i32, String), PartitionField>,

    // Indices of newly added fields
    name_to_added_field: HashMap<String, PartitionField>,
    transform_to_added_field: HashMap<(i32, String), PartitionField>,
    added_time_fields: HashMap<i32, PartitionField>,

    original_last_assigned_partition_id: i32,
    current_schema_id: i32,
    default_spec_id: i32,
}

impl UpdatePartitionSpecAction {
    /// Creates a new update partition spec action.
    ///
    /// Takes the current partition spec, schema, format version, and last assigned
    /// partition id for eager validation (mirrors Java's BaseUpdatePartitionSpec).
    pub fn new(metadata: &TableMetadata) -> Self {
        let schema = metadata.current_schema().clone();
        let current_spec = metadata.default_partition_spec().clone();
        let format_version = metadata.format_version();
        let last_assigned_partition_id = metadata.last_partition_id();
        let partition_specs = metadata.partition_specs.values().cloned().collect();
        let current_schema_id = metadata.current_schema_id();
        let default_spec_id = metadata.default_partition_spec_id();
        let name_to_field: HashMap<_, _> = current_spec
            .fields()
            .iter()
            .map(|f| (f.name.clone(), f.clone()))
            .collect();
        let transform_to_field: HashMap<_, _> = current_spec
            .fields()
            .iter()
            .map(|f| ((f.source_id, f.transform.to_string()), f.clone()))
            .collect();
        Self {
            adds: vec![],
            deletes: HashSet::new(),
            renames: HashMap::new(),
            case_sensitive: true,
            set_as_default: true,
            schema,
            current_spec,
            format_version,
            last_assigned_partition_id,
            name_to_field,
            transform_to_field,
            name_to_added_field: HashMap::new(),
            transform_to_added_field: HashMap::new(),
            added_time_fields: HashMap::new(),
            partition_specs,
            original_last_assigned_partition_id: last_assigned_partition_id,
            current_schema_id,
            default_spec_id,
        }
    }

    /// Set whether source column resolution is case-sensitive.
    pub fn case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
        self
    }

    /// Do not set the newly added partition spec as the table default.
    pub fn add_non_default_spec(mut self) -> Self {
        self.set_as_default = false;
        self
    }

    /// Add a partition field to the new spec.
    pub fn add_field(mut self, name: Option<String>, term: Term) -> Result<Self> {
        if let Some(ref name) = name {
            let already_added = self.name_to_added_field.get(name);
            if let Some(added) = already_added {
                return Err(Error::new(
                    ErrorKind::PreconditionFailed,
                    format!("Cannot add duplicate partition field: {:?}", added),
                ));
            }
        }

        let source_transform = self.resolve_term(&term)?;
        let validation_key = (source_transform.0, source_transform.1.to_string());

        let existing = self.transform_to_field.get(&validation_key).cloned();
        if let Some(existing) = existing {
            if self.deletes.contains(&existing.field_id) && existing.transform == source_transform.1
            {
                return self.rewrite_delete_and_add_field(&existing, name.clone());
            }
            ensure_precondition!(
                self.deletes.contains(&existing.field_id)
                    && existing.transform != source_transform.1,
                "Cannot add duplicate partition field {:?}={:?}, conflicts with {:?}",
                &name,
                &term,
                &existing
            );
        }

        let added = self.transform_to_added_field.get(&validation_key);
        if let Some(added) = added {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!(
                    "Cannot add duplicate partition field {:?}={:?}, already added: {:?}",
                    name, term, added
                ),
            ));
        }

        let new_field = self.recycle_or_create_partition_field(&source_transform, &name);
        self.check_for_redundant_added_partitions(&new_field)?;
        self.transform_to_added_field
            .insert(validation_key, new_field.clone());

        let existing_field = self.name_to_field.get(&new_field.name).cloned();
        if let Some(existing_field) = existing_field {
            if !self.deletes.contains(&existing_field.field_id) {
                if existing_field.transform == Transform::Void {
                    // rename the old deleted field that is being replaced by the new field
                    self = self.rename_field(
                        existing_field.name.clone(),
                        format!("{}_{}", existing_field.name, existing_field.field_id),
                    )?;
                } else {
                    return Err(Error::new(
                        ErrorKind::PreconditionFailed,
                        format!(
                            "Cannot add duplicate partition field name: {}",
                            new_field.name
                        ),
                    ));
                }
            } else {
                self.renames.insert(
                    existing_field.name.clone(),
                    format!("{}_{}", existing_field.name, existing_field.field_id),
                );
            }
        }

        self.name_to_added_field
            .insert(new_field.name.clone(), new_field.clone());
        self.adds.push(new_field);

        Ok(self)
    }

    fn rewrite_delete_and_add_field(
        mut self,
        existing: &PartitionField,
        name: Option<String>,
    ) -> Result<Self> {
        self.deletes.remove(&existing.field_id);
        if let Some(name) = name {
            if existing.name == name {
                Ok(self)
            } else {
                self.rename_field(existing.name.clone(), name)
            }
        } else {
            Ok(self)
        }
    }

    /// Remove a partition field by name from the new spec.    
    pub fn remove_field_by_name(mut self, name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        let already_added = self.name_to_added_field.get(&name);
        if let Some(added) = already_added {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!("Cannot delete newly added field: {:?}", added),
            ));
        }
        if self.renames.contains_key(&name) {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!("Cannot rename and delete partition field: {}", name),
            ));
        }
        let field = self.name_to_field.get(&name);
        if let Some(field) = field {
            self.deletes.insert(field.field_id);
        } else {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!("Cannot find partition field to remove: {}", name),
            ));
        }
        Ok(self)
    }

    /// Remove a partition field by source column name and transform from the new spec.
    pub fn remove_field_by_term(mut self, term: Term) -> Result<Self> {
        let (source_id, transform) = self.resolve_term(&term)?;
        let key = (source_id, transform.to_string());
        let added = self.transform_to_added_field.get(&key);
        if let Some(added) = added {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!("Cannot delete newly added field: {:?}", added),
            ));
        }
        let field = self.transform_to_field.get(&key);
        if let Some(field) = field {
            ensure_precondition!(
                !self.renames.contains_key(&field.name),
                "Cannot rename and delete partition field: {}",
                field.name
            );
            self.deletes.insert(field.field_id);
        } else {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!("Cannot find partition field to remove: {:?}", key),
            ));
        }
        Ok(self)
    }

    /// Rename a partition field in the new spec.
    pub fn rename_field(
        mut self,
        name: impl Into<String>,
        new_name: impl Into<String>,
    ) -> Result<Self> {
        let name = name.into();
        let new_name = new_name.into();
        let existing_field = self.name_to_field.get(&new_name).cloned();
        if let Some(existing_field) = existing_field
            && existing_field.transform == Transform::Void
        {
            self = self.rename_field(
                existing_field.name.clone(),
                format!("{}_{}", existing_field.name, existing_field.field_id),
            )?;
        }

        ensure_precondition!(
            !self.name_to_added_field.contains_key(&name),
            "Cannot rename newly added partition field: {}",
            name
        );

        let field = self.name_to_field.get(&name);
        if let Some(field) = field {
            ensure_precondition!(
                !self.deletes.contains(&field.field_id),
                "Cannot delete and rename partition field: {}",
                name
            );
        } else {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!("Cannot find partition field to rename: {}", name),
            ));
        }

        self.renames.insert(name, new_name);
        Ok(self)
    }

    fn apply(&self) -> Result<UnboundPartitionSpec> {
        let mut fields = vec![];
        for field in self.current_spec.fields() {
            if !self.deletes.contains(&field.field_id) {
                let new_name = self.renames.get(&field.name);
                fields.push(UnboundPartitionField {
                    source_id: field.source_id,
                    field_id: Some(field.field_id),
                    name: new_name.unwrap_or(&field.name).to_string(),
                    transform: field.transform,
                });
            } else if self.format_version == FormatVersion::V1 {
                // field IDs were not required for v1 and were assigned sequentially in each partition spec
                // starting at 1,000.
                // to maintain consistent field ids across partition specs in v1 tables, any partition field
                // that is removed
                // must be replaced with a null transform. null values are always allowed in partition data.
                let new_name = self.renames.get(&field.name);
                fields.push(UnboundPartitionField {
                    source_id: field.source_id,
                    field_id: Some(field.field_id),
                    name: new_name.unwrap_or(&field.name).to_string(),
                    transform: Transform::Void,
                });
            }
        }

        for new_field in &self.adds {
            fields.push(UnboundPartitionField {
                source_id: new_field.source_id,
                field_id: Some(new_field.field_id),
                name: new_field.name.clone(),
                transform: new_field.transform,
            });
        }
        Ok(UnboundPartitionSpec {
            spec_id: None,
            fields,
        })
    }

    // --- Private helpers (mirror Java internal methods) ---

    /// Assign a new partition field id (Java assignFieldId).
    fn assign_field_id(&mut self) -> i32 {
        self.last_assigned_partition_id += 1;
        self.last_assigned_partition_id
    }

    fn resolve_term(&self, term: &Term) -> Result<(i32, Transform)> {
        let Term {
            source_name,
            transform,
        } = term;
        let source_id = if self.case_sensitive {
            self.schema.field_by_name(source_name)
        } else {
            self.schema.field_by_name_case_insensitive(source_name)
        };
        if let Some(source_field) = source_id {
            Ok((source_field.id, *transform))
        } else {
            Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!("Cannot find source column with name: {source_name} in schema"),
            ))
        }
    }

    /// In V2, searches for a similar partition field in historical partition specs.
    ///
    /// It tries to match on source field ID, transform type, and target name (optional).
    /// If not found, or in V1 cases, it creates a new `PartitionField`.
    ///
    /// # Arguments
    ///
    /// * `source_transform` - Pair of source ID and transform for this `PartitionField` addition.
    /// * `name` - Target partition field name, if specified.
    ///
    /// # Returns
    ///
    /// The recycled or newly created partition field.
    fn recycle_or_create_partition_field(
        &mut self,
        source_transform: &(i32, Transform),
        name: &Option<String>,
    ) -> PartitionField {
        let source_id = source_transform.0;
        let transform = source_transform.1;
        if self.format_version >= FormatVersion::V2 {
            let all_historical_fields = self.partition_specs.iter().flat_map(|spec| spec.fields());
            for field in all_historical_fields {
                if field.source_id == source_id && field.transform == transform {
                    if let Some(name) = name {
                        if field.name == *name {
                            return field.clone();
                        }
                    } else {
                        return field.clone();
                    }
                }
            }
        }
        let partition_name = name.clone().unwrap_or_else(|| {
            default_partition_name(&self.schema.field_by_id(source_id).unwrap().name, transform)
        });
        PartitionField::new(source_id, self.assign_field_id(), partition_name, transform)
    }

    fn check_for_redundant_added_partitions(&mut self, field: &PartitionField) -> Result<()> {
        if field.transform.is_time_transform() {
            let time_field = self.added_time_fields.get(&field.source_id);
            if let Some(time_field) = time_field {
                return Err(Error::new(
                    ErrorKind::PreconditionFailed,
                    format!(
                        "Cannot add redundant partition field: {:?} conflicts with {:?}",
                        time_field, field,
                    ),
                ));
            }
            self.added_time_fields
                .insert(field.source_id, field.clone());
        }
        Ok(())
    }
}

#[async_trait]
impl TransactionAction for UpdatePartitionSpecAction {
    async fn commit(self: Arc<Self>, _table: &Table) -> Result<ActionCommit> {
        let spec = self.apply()?;
        let mut updates = vec![TableUpdate::AddSpec { spec }];
        if self.set_as_default {
            updates.push(TableUpdate::SetDefaultSpec { spec_id: -1 });
        }

        let requirements = vec![
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: self.current_schema_id,
            },
            TableRequirement::LastAssignedPartitionIdMatch {
                last_assigned_partition_id: self.original_last_assigned_partition_id,
            },
            TableRequirement::DefaultSpecIdMatch {
                default_spec_id: self.default_spec_id,
            },
        ];

        Ok(ActionCommit::new(updates, requirements))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, LazyLock};

    use rstest::rstest;

    use crate::io::FileIO;
    use crate::spec::{
        FormatVersion, NestedField, PartitionField, PartitionSpec, PrimitiveType, Schema,
        SchemaRef, SortOrder, TableMetadataBuilder, Transform,
    };
    use crate::table::Table;
    use crate::transaction::Transaction;
    use crate::transaction::update_partition::{Term, UpdatePartitionSpecAction};
    use crate::{Result, Runtime, TableIdent};

    static UPDATE_PARTITION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
        Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", PrimitiveType::Long.into()).into(),
                    NestedField::required(2, "ts", PrimitiveType::Timestamptz.into()).into(),
                    NestedField::required(3, "category", PrimitiveType::String.into()).into(),
                    NestedField::optional(4, "data", PrimitiveType::String.into()).into(),
                ])
                .build()
                .unwrap(),
        )
    });

    fn unpartitioned_spec() -> PartitionSpec {
        PartitionSpec::unpartition_spec()
    }

    fn partitioned_spec() -> PartitionSpec {
        PartitionSpec::builder(UPDATE_PARTITION_SCHEMA.clone())
            .add_partition_field("category", "category", Transform::Identity)
            .unwrap()
            .add_partition_field("ts", "ts_day", Transform::Day)
            .unwrap()
            .add_partition_field("id", "shard", Transform::Bucket(16))
            .unwrap()
            .build()
            .unwrap()
    }

    fn table_for_spec(format_version: FormatVersion, spec: PartitionSpec) -> Table {
        let metadata = TableMetadataBuilder::new(
            (**UPDATE_PARTITION_SCHEMA).clone(),
            spec,
            SortOrder::unsorted_order(),
            "s3://bucket/test/table".to_string(),
            format_version,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;

        Table::builder()
            .metadata(metadata)
            .metadata_location(
                "s3://bucket/test/table/metadata/1-00000000-0000-0000-0000-000000000000.metadata.json",
            )
            .identifier(TableIdent::from_strs(["ns", "partition_test"]).unwrap())
            .file_io(FileIO::new_with_memory())
            .runtime(Runtime::new(&tokio::runtime::Runtime::new().unwrap()))
            .build()
            .unwrap()
    }

    fn apply_update(
        format_version: FormatVersion,
        spec: PartitionSpec,
        build_action: impl FnOnce(UpdatePartitionSpecAction) -> Result<UpdatePartitionSpecAction>,
    ) -> Result<PartitionSpec> {
        let table = table_for_spec(format_version, spec);
        let action = build_action(Transaction::new(&table).update_partition_spec())?;
        let unbound_spec = action.apply()?;
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .add_partition_spec(unbound_spec)?
            .set_default_partition_spec(TableMetadataBuilder::LAST_ADDED)?
            .build()?
            .metadata;
        Ok((**metadata.default_partition_spec()).clone())
    }

    fn assert_fields(spec: &PartitionSpec, expected: &[PartitionField]) {
        assert_eq!(spec.fields(), expected);
    }

    fn assert_update_err_contains(
        format_version: FormatVersion,
        spec: PartitionSpec,
        build_action: impl FnOnce(UpdatePartitionSpecAction) -> Result<UpdatePartitionSpecAction>,
        expected: &str,
    ) {
        let err = apply_update(format_version, spec, build_action).unwrap_err();
        assert!(
            err.to_string().contains(expected),
            "expected error to contain {expected:?}, got {err}"
        );
    }

    #[rstest]
    fn add_identity(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        let updated = apply_update(format_version, unpartitioned_spec(), |action| {
            action.add_field(None, Term::identity("category"))
        })
        .unwrap();
        assert_fields(&updated, &[PartitionField::new(
            3,
            1000,
            "category",
            Transform::Identity,
        )]);
    }

    #[rstest]
    fn add_year(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        let updated = apply_update(format_version, unpartitioned_spec(), |action| {
            action.add_field(None, Term::year("ts"))
        })
        .unwrap();
        assert_fields(&updated, &[PartitionField::new(
            2,
            1000,
            "ts_year",
            Transform::Year,
        )]);
    }

    #[rstest]
    fn add_month(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        let updated = apply_update(format_version, unpartitioned_spec(), |action| {
            action.add_field(None, Term::month("ts"))
        })
        .unwrap();
        assert_fields(&updated, &[PartitionField::new(
            2,
            1000,
            "ts_month",
            Transform::Month,
        )]);
    }

    #[rstest]
    fn add_day(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        let updated = apply_update(format_version, unpartitioned_spec(), |action| {
            action.add_field(None, Term::day("ts"))
        })
        .unwrap();
        assert_fields(&updated, &[PartitionField::new(
            2,
            1000,
            "ts_day",
            Transform::Day,
        )]);
    }

    #[rstest]
    fn add_hour(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        let updated = apply_update(format_version, unpartitioned_spec(), |action| {
            action.add_field(None, Term::hour("ts"))
        })
        .unwrap();
        assert_fields(&updated, &[PartitionField::new(
            2,
            1000,
            "ts_hour",
            Transform::Hour,
        )]);
    }

    #[rstest]
    fn add_bucket(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        let updated = apply_update(format_version, unpartitioned_spec(), |action| {
            action.add_field(None, Term::bucket("id", 16))
        })
        .unwrap();
        assert_fields(&updated, &[PartitionField::new(
            1,
            1000,
            "id_bucket_16",
            Transform::Bucket(16),
        )]);
    }

    #[rstest]
    fn add_truncate(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        let updated = apply_update(format_version, unpartitioned_spec(), |action| {
            action.add_field(None, Term::truncate("data", 4))
        })
        .unwrap();
        assert_fields(&updated, &[PartitionField::new(
            4,
            1000,
            "data_trunc_4",
            Transform::Truncate(4),
        )]);
    }

    #[rstest]
    fn add_named_partition(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        let updated = apply_update(format_version, unpartitioned_spec(), |action| {
            action.add_field(Some("shard".to_string()), Term::bucket("id", 16))
        })
        .unwrap();
        assert_fields(&updated, &[PartitionField::new(
            1,
            1000,
            "shard",
            Transform::Bucket(16),
        )]);
    }

    #[rstest]
    fn add_to_existing(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        let updated = apply_update(format_version, partitioned_spec(), |action| {
            action.add_field(None, Term::truncate("data", 4))
        })
        .unwrap();
        assert_fields(&updated, &[
            PartitionField::new(3, 1000, "category", Transform::Identity),
            PartitionField::new(2, 1001, "ts_day", Transform::Day),
            PartitionField::new(1, 1002, "shard", Transform::Bucket(16)),
            PartitionField::new(4, 1003, "data_trunc_4", Transform::Truncate(4)),
        ]);
    }

    #[rstest]
    fn multiple_adds(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        let updated = apply_update(format_version, unpartitioned_spec(), |action| {
            action
                .add_field(None, Term::identity("category"))?
                .add_field(None, Term::day("ts"))?
                .add_field(Some("shard".to_string()), Term::bucket("id", 16))?
                .add_field(Some("prefix".to_string()), Term::truncate("data", 4))
        })
        .unwrap();
        assert_fields(&updated, &[
            PartitionField::new(3, 1000, "category", Transform::Identity),
            PartitionField::new(2, 1001, "ts_day", Transform::Day),
            PartitionField::new(1, 1002, "shard", Transform::Bucket(16)),
            PartitionField::new(4, 1003, "prefix", Transform::Truncate(4)),
        ]);
    }

    #[rstest]
    fn add_hour_to_day(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        let by_day = apply_update(format_version, unpartitioned_spec(), |action| {
            action.add_field(None, Term::day("ts"))
        })
        .unwrap();
        let by_hour = apply_update(format_version, by_day, |action| {
            action.add_field(None, Term::hour("ts"))
        })
        .unwrap();
        assert_fields(&by_hour, &[
            PartitionField::new(2, 1000, "ts_day", Transform::Day),
            PartitionField::new(2, 1001, "ts_hour", Transform::Hour),
        ]);
    }

    #[rstest]
    fn add_multiple_buckets(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        let bucket16 = apply_update(format_version, unpartitioned_spec(), |action| {
            action.add_field(None, Term::bucket("id", 16))
        })
        .unwrap();
        let bucket8 = apply_update(format_version, bucket16, |action| {
            action.add_field(None, Term::bucket("id", 8))
        })
        .unwrap();
        assert_fields(&bucket8, &[
            PartitionField::new(1, 1000, "id_bucket_16", Transform::Bucket(16)),
            PartitionField::new(1, 1001, "id_bucket_8", Transform::Bucket(8)),
        ]);
    }

    #[rstest]
    fn remove_identity_by_name(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        let updated = apply_update(format_version, partitioned_spec(), |action| {
            action.remove_field_by_name("category")
        })
        .unwrap();
        if format_version == FormatVersion::V1 {
            assert_fields(&updated, &[
                PartitionField::new(3, 1000, "category", Transform::Void),
                PartitionField::new(2, 1001, "ts_day", Transform::Day),
                PartitionField::new(1, 1002, "shard", Transform::Bucket(16)),
            ]);
        } else {
            assert_fields(&updated, &[
                PartitionField::new(2, 1001, "ts_day", Transform::Day),
                PartitionField::new(1, 1002, "shard", Transform::Bucket(16)),
            ]);
        }
    }

    #[rstest]
    fn remove_bucket_by_name(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        let updated = apply_update(format_version, partitioned_spec(), |action| {
            action.remove_field_by_name("shard")
        })
        .unwrap();
        if format_version == FormatVersion::V1 {
            assert_fields(&updated, &[
                PartitionField::new(3, 1000, "category", Transform::Identity),
                PartitionField::new(2, 1001, "ts_day", Transform::Day),
                PartitionField::new(1, 1002, "shard", Transform::Void),
            ]);
        } else {
            assert_fields(&updated, &[
                PartitionField::new(3, 1000, "category", Transform::Identity),
                PartitionField::new(2, 1001, "ts_day", Transform::Day),
            ]);
        }
    }

    #[rstest]
    fn remove_identity_by_equivalent(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        let updated = apply_update(format_version, partitioned_spec(), |action| {
            action.remove_field_by_term(Term::identity("category"))
        })
        .unwrap();
        if format_version == FormatVersion::V1 {
            assert_fields(&updated, &[
                PartitionField::new(3, 1000, "category", Transform::Void),
                PartitionField::new(2, 1001, "ts_day", Transform::Day),
                PartitionField::new(1, 1002, "shard", Transform::Bucket(16)),
            ]);
        } else {
            assert_fields(&updated, &[
                PartitionField::new(2, 1001, "ts_day", Transform::Day),
                PartitionField::new(1, 1002, "shard", Transform::Bucket(16)),
            ]);
        }
    }

    #[rstest]
    fn remove_day_by_equivalent(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        let updated = apply_update(format_version, partitioned_spec(), |action| {
            action.remove_field_by_term(Term::day("ts"))
        })
        .unwrap();
        if format_version == FormatVersion::V1 {
            assert_fields(&updated, &[
                PartitionField::new(3, 1000, "category", Transform::Identity),
                PartitionField::new(2, 1001, "ts_day", Transform::Void),
                PartitionField::new(1, 1002, "shard", Transform::Bucket(16)),
            ]);
        } else {
            assert_fields(&updated, &[
                PartitionField::new(3, 1000, "category", Transform::Identity),
                PartitionField::new(1, 1002, "shard", Transform::Bucket(16)),
            ]);
        }
    }

    #[rstest]
    fn remove_bucket_by_equivalent(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        let updated = apply_update(format_version, partitioned_spec(), |action| {
            action.remove_field_by_term(Term::bucket("id", 16))
        })
        .unwrap();
        if format_version == FormatVersion::V1 {
            assert_fields(&updated, &[
                PartitionField::new(3, 1000, "category", Transform::Identity),
                PartitionField::new(2, 1001, "ts_day", Transform::Day),
                PartitionField::new(1, 1002, "shard", Transform::Void),
            ]);
        } else {
            assert_fields(&updated, &[
                PartitionField::new(3, 1000, "category", Transform::Identity),
                PartitionField::new(2, 1001, "ts_day", Transform::Day),
            ]);
        }
    }

    #[rstest]
    fn rename(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        let updated = apply_update(format_version, partitioned_spec(), |action| {
            action.rename_field("shard", "id_bucket_16")
        })
        .unwrap();
        assert_fields(&updated, &[
            PartitionField::new(3, 1000, "category", Transform::Identity),
            PartitionField::new(2, 1001, "ts_day", Transform::Day),
            PartitionField::new(1, 1002, "id_bucket_16", Transform::Bucket(16)),
        ]);
    }

    #[rstest]
    fn multiple_changes(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        let updated = apply_update(format_version, partitioned_spec(), |action| {
            action
                .rename_field("shard", "id_bucket_16")?
                .remove_field_by_term(Term::day("ts"))?
                .add_field(Some("prefix".to_string()), Term::truncate("data", 4))
        })
        .unwrap();
        if format_version == FormatVersion::V1 {
            assert_fields(&updated, &[
                PartitionField::new(3, 1000, "category", Transform::Identity),
                PartitionField::new(2, 1001, "ts_day", Transform::Void),
                PartitionField::new(1, 1002, "id_bucket_16", Transform::Bucket(16)),
                PartitionField::new(4, 1003, "prefix", Transform::Truncate(4)),
            ]);
        } else {
            assert_fields(&updated, &[
                PartitionField::new(3, 1000, "category", Transform::Identity),
                PartitionField::new(1, 1002, "id_bucket_16", Transform::Bucket(16)),
                PartitionField::new(4, 1003, "prefix", Transform::Truncate(4)),
            ]);
        }
    }

    #[rstest]
    fn add_deleted_name(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        let updated = apply_update(format_version, partitioned_spec(), |action| {
            action.remove_field_by_term(Term::bucket("id", 16))
        })
        .unwrap();
        if format_version == FormatVersion::V1 {
            assert_fields(&updated, &[
                PartitionField::new(3, 1000, "category", Transform::Identity),
                PartitionField::new(2, 1001, "ts_day", Transform::Day),
                PartitionField::new(1, 1002, "shard", Transform::Void),
            ]);
        } else {
            assert_fields(&updated, &[
                PartitionField::new(3, 1000, "category", Transform::Identity),
                PartitionField::new(2, 1001, "ts_day", Transform::Day),
            ]);
        }
    }

    #[rstest]
    fn no_effect_add_deleted_same_field_with_same_name(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        let updated = apply_update(format_version, partitioned_spec(), |action| {
            action
                .remove_field_by_name("shard")?
                .add_field(Some("shard".to_string()), Term::bucket("id", 16))
        })
        .unwrap();
        assert_fields(&updated, &[
            PartitionField::new(3, 1000, "category", Transform::Identity),
            PartitionField::new(2, 1001, "ts_day", Transform::Day),
            PartitionField::new(1, 1002, "shard", Transform::Bucket(16)),
        ]);

        let updated = apply_update(format_version, partitioned_spec(), |action| {
            action
                .remove_field_by_name("shard")?
                .add_field(None, Term::bucket("id", 16))
        })
        .unwrap();
        assert_fields(&updated, &[
            PartitionField::new(3, 1000, "category", Transform::Identity),
            PartitionField::new(2, 1001, "ts_day", Transform::Day),
            PartitionField::new(1, 1002, "shard", Transform::Bucket(16)),
        ]);
    }

    #[rstest]
    fn generate_new_spec_add_deleted_same_field_with_different_name(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        let updated = apply_update(format_version, partitioned_spec(), |action| {
            action
                .remove_field_by_name("shard")?
                .add_field(Some("new_shard".to_string()), Term::bucket("id", 16))
        })
        .unwrap();
        assert_fields(&updated, &[
            PartitionField::new(3, 1000, "category", Transform::Identity),
            PartitionField::new(2, 1001, "ts_day", Transform::Day),
            PartitionField::new(1, 1002, "new_shard", Transform::Bucket(16)),
        ]);
    }

    #[rstest]
    fn remove_newly_added_field_by_name(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        assert!(
            Transaction::new(&table_for_spec(format_version, partitioned_spec()))
                .update_partition_spec()
                .add_field(Some("prefix".to_string()), Term::truncate("data", 4))
                .unwrap()
                .remove_field_by_name("prefix")
                .is_err()
        );
    }

    #[rstest]
    fn remove_newly_added_field_by_transform(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        assert!(
            Transaction::new(&table_for_spec(format_version, partitioned_spec()))
                .update_partition_spec()
                .add_field(Some("prefix".to_string()), Term::truncate("data", 4))
                .unwrap()
                .remove_field_by_term(Term::truncate("data", 4))
                .is_err()
        );
    }

    #[rstest]
    fn add_already_added_field_by_transform(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        assert!(
            Transaction::new(&table_for_spec(format_version, partitioned_spec()))
                .update_partition_spec()
                .add_field(Some("prefix".to_string()), Term::truncate("data", 4))
                .unwrap()
                .add_field(None, Term::truncate("data", 4))
                .is_err()
        );
    }

    #[rstest]
    fn add_already_added_field_by_name(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        assert!(
            Transaction::new(&table_for_spec(format_version, partitioned_spec()))
                .update_partition_spec()
                .add_field(Some("prefix".to_string()), Term::truncate("data", 4))
                .unwrap()
                .add_field(Some("prefix".to_string()), Term::truncate("data", 6))
                .is_err()
        );
    }

    #[rstest]
    fn add_redundant_time_partition(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        assert!(
            Transaction::new(&table_for_spec(format_version, unpartitioned_spec()))
                .update_partition_spec()
                .add_field(None, Term::day("ts"))
                .unwrap()
                .add_field(None, Term::hour("ts"))
                .is_err()
        );
        assert!(
            Transaction::new(&table_for_spec(format_version, partitioned_spec()))
                .update_partition_spec()
                .add_field(None, Term::hour("ts"))
                .unwrap()
                .add_field(None, Term::month("ts"))
                .is_err()
        );
    }

    #[rstest]
    fn add_duplicate_by(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        assert_update_err_contains(
            format_version,
            partitioned_spec(),
            |action| action.add_field(None, Term::identity("category")),
            "Cannot add duplicate partition field",
        );
    }

    #[rstest]
    fn add_duplicate_transform(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        assert_update_err_contains(
            format_version,
            partitioned_spec(),
            |action| action.add_field(None, Term::bucket("id", 16)),
            "Cannot add duplicate partition field",
        );
    }

    #[rstest]
    fn add_named_duplicate(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        assert_update_err_contains(
            format_version,
            partitioned_spec(),
            |action| action.add_field(Some("b16".to_string()), Term::bucket("id", 16)),
            "Cannot add duplicate partition field",
        );
    }

    #[rstest]
    fn remove_unknown_field_by_name(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        assert_update_err_contains(
            format_version,
            partitioned_spec(),
            |action| action.remove_field_by_name("moon"),
            "Cannot find partition field to remove",
        );
    }

    #[rstest]
    fn remove_unknown_field_by_equivalent(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        assert_update_err_contains(
            format_version,
            partitioned_spec(),
            |action| action.remove_field_by_term(Term::hour("ts")),
            "Cannot find partition field to remove",
        );
    }

    #[rstest]
    fn rename_unknown_field(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        assert_update_err_contains(
            format_version,
            partitioned_spec(),
            |action| action.rename_field("shake", "seal"),
            "Cannot find partition field to rename",
        );
    }

    #[rstest]
    fn rename_after_add(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        assert!(
            Transaction::new(&table_for_spec(format_version, partitioned_spec()))
                .update_partition_spec()
                .add_field(Some("data_trunc".to_string()), Term::truncate("data", 4))
                .unwrap()
                .rename_field("data_trunc", "prefix")
                .is_err()
        );
    }

    #[rstest]
    fn rename_and_delete(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        assert_update_err_contains(
            format_version,
            partitioned_spec(),
            |action| {
                action
                    .rename_field("shard", "id_bucket_16")?
                    .remove_field_by_term(Term::bucket("id", 16))
            },
            "Cannot rename and delete partition field",
        );
    }

    #[rstest]
    fn delete_and_rename(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        assert_update_err_contains(
            format_version,
            partitioned_spec(),
            |action| {
                action
                    .remove_field_by_term(Term::bucket("id", 16))?
                    .rename_field("shard", "id_bucket_16")
            },
            "Cannot delete and rename partition field",
        );
    }

    #[rstest]
    fn remove_and_add_multi_times(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        let add_first = apply_update(format_version, unpartitioned_spec(), |action| {
            action.add_field(Some("ts_date".to_string()), Term::day("ts"))
        })
        .unwrap();
        let remove_first = apply_update(format_version, add_first, |action| {
            action.remove_field_by_term(Term::day("ts"))
        })
        .unwrap();
        let add_second = apply_update(format_version, remove_first, |action| {
            action.add_field(Some("ts_date".to_string()), Term::day("ts"))
        })
        .unwrap();
        let remove_second = apply_update(format_version, add_second, |action| {
            action.remove_field_by_term(Term::day("ts"))
        })
        .unwrap();
        let add_third = apply_update(format_version, remove_second, |action| {
            action.add_field(None, Term::month("ts"))
        })
        .unwrap();
        let updated = apply_update(format_version, add_third, |action| {
            action.rename_field("ts_month", "ts_date")
        })
        .unwrap();

        if format_version == FormatVersion::V1 {
            assert_eq!(updated.fields().len(), 3);
            assert!(updated.fields()[0].name.starts_with("ts_date_"));
            assert!(updated.fields()[1].name.starts_with("ts_date_"));
            assert_eq!(updated.fields()[2].name, "ts_date");
            assert_eq!(updated.fields()[0].transform, Transform::Void);
            assert_eq!(updated.fields()[1].transform, Transform::Void);
            assert_eq!(updated.fields()[2].transform, Transform::Month);
        } else {
            assert_fields(&updated, &[PartitionField::new(
                2,
                1000,
                "ts_date",
                Transform::Month,
            )]);
        }
    }

    #[rstest]
    fn remove_and_update_with_different_transformation(
        #[values(FormatVersion::V1, FormatVersion::V2, FormatVersion::V3)]
        format_version: FormatVersion,
    ) {
        let expected = apply_update(format_version, unpartitioned_spec(), |action| {
            action.add_field(Some("ts_transformed".to_string()), Term::month("ts"))
        })
        .unwrap();
        let updated = apply_update(format_version, expected, |action| {
            action
                .remove_field_by_name("ts_transformed")?
                .add_field(Some("ts_transformed".to_string()), Term::day("ts"))
        })
        .unwrap();

        if format_version == FormatVersion::V1 {
            assert_fields(&updated, &[
                PartitionField::new(2, 1000, "ts_transformed_1000", Transform::Void),
                PartitionField::new(2, 1001, "ts_transformed", Transform::Day),
            ]);
        } else {
            assert_fields(&updated, &[PartitionField::new(
                2,
                1001,
                "ts_transformed",
                Transform::Day,
            )]);
        }
    }
}
