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

//! This module contains transaction api.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::mem::discriminant;
use std::ops::RangeFrom;

use uuid::Uuid;

use crate::error::Result;
use crate::expr::{Bind, Reference};
use crate::io::OutputFile;
use crate::spec::{
    DataFile, DataFileFormat, FormatVersion, ManifestEntry, ManifestFile, ManifestListWriter,
    ManifestWriterBuilder, NullOrder, Operation, PartitionField, PartitionSpec, Schema, Snapshot,
    SnapshotReference, SnapshotRetention, SortDirection, SortField, SortOrder, Struct, StructType,
    Summary, Transform, MAIN_BRANCH,
};
use crate::table::Table;
use crate::transaction::FormatVersion::V2;
use crate::TableUpdate::UpgradeFormatVersion;
use crate::{Catalog, Error, ErrorKind, TableCommit, TableRequirement, TableUpdate};

const META_ROOT_PATH: &str = "metadata";
#[allow(dead_code)]
const PARTITION_FIELD_ID_START: u32 = 1000;

/// Table transaction.
pub struct Transaction<'a> {
    table: &'a Table,
    updates: Vec<TableUpdate>,
    requirements: Vec<TableRequirement>,
}

impl<'a> Transaction<'a> {
    /// Creates a new transaction.
    pub fn new(table: &'a Table) -> Self {
        Self {
            table,
            updates: vec![],
            requirements: vec![],
        }
    }

    fn append_updates(&mut self, updates: Vec<TableUpdate>) -> Result<()> {
        for update in &updates {
            for up in &self.updates {
                if discriminant(up) == discriminant(update) {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Cannot apply update with same type at same time: {:?}",
                            update
                        ),
                    ));
                }
            }
        }
        self.updates.extend(updates);
        Ok(())
    }

    fn append_requirements(&mut self, requirements: Vec<TableRequirement>) -> Result<()> {
        self.requirements.extend(requirements);
        Ok(())
    }

    /// Sets table to a new version.
    pub fn upgrade_table_version(mut self, format_version: FormatVersion) -> Result<Self> {
        let current_version = self.table.metadata().format_version();
        match current_version.cmp(&format_version) {
            Ordering::Greater => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot downgrade table version from {} to {}",
                        current_version, format_version
                    ),
                ));
            }
            Ordering::Less => {
                self.append_updates(vec![UpgradeFormatVersion { format_version }])?;
            }
            Ordering::Equal => {
                // Do nothing.
            }
        }
        Ok(self)
    }

    /// Update table's property.
    pub fn set_properties(mut self, props: HashMap<String, String>) -> Result<Self> {
        self.append_updates(vec![TableUpdate::SetProperties { updates: props }])?;
        Ok(self)
    }

    fn generate_unique_snapshot_id(&self) -> i64 {
        let generate_random_id = || -> i64 {
            let (lhs, rhs) = Uuid::new_v4().as_u64_pair();
            let snapshot_id = (lhs ^ rhs) as i64;
            if snapshot_id < 0 {
                -snapshot_id
            } else {
                snapshot_id
            }
        };
        let mut snapshot_id = generate_random_id();
        while self
            .table
            .metadata()
            .snapshots()
            .any(|s| s.snapshot_id() == snapshot_id)
        {
            snapshot_id = generate_random_id();
        }
        snapshot_id
    }

    /// Creates a fast append action.
    pub fn fast_append(
        self,
        commit_uuid: Option<Uuid>,
        key_metadata: Vec<u8>,
    ) -> Result<FastAppendAction<'a>> {
        let snapshot_id = self.generate_unique_snapshot_id();
        FastAppendAction::new(
            self,
            snapshot_id,
            commit_uuid.unwrap_or_else(Uuid::now_v7),
            key_metadata,
            HashMap::new(),
        )
    }

    /// Creates replace sort order action.
    pub fn replace_sort_order(self) -> ReplaceSortOrderAction<'a> {
        ReplaceSortOrderAction {
            tx: self,
            sort_fields: vec![],
        }
    }

    /// Remove properties in table.
    pub fn remove_properties(mut self, keys: Vec<String>) -> Result<Self> {
        self.append_updates(vec![TableUpdate::RemoveProperties { removals: keys }])?;
        Ok(self)
    }

    /// Commit transaction.
    pub async fn commit(self, catalog: &impl Catalog) -> Result<Table> {
        let table_commit = TableCommit::builder()
            .ident(self.table.identifier().clone())
            .updates(self.updates)
            .requirements(self.requirements)
            .build();

        catalog.update_table(table_commit).await
    }
}

/// FastAppendAction is a transaction action for fast append data files to the table.
pub struct FastAppendAction<'a> {
    snapshot_produce_action: SnapshotProduceAction<'a>,
}

impl<'a> FastAppendAction<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        tx: Transaction<'a>,
        snapshot_id: i64,
        commit_uuid: Uuid,
        key_metadata: Vec<u8>,
        snapshot_properties: HashMap<String, String>,
    ) -> Result<Self> {
        Ok(Self {
            snapshot_produce_action: SnapshotProduceAction::new(
                tx,
                snapshot_id,
                key_metadata,
                commit_uuid,
                snapshot_properties,
            )?,
        })
    }

    /// Add data files to the snapshot.
    pub fn add_data_files(
        &mut self,
        data_files: impl IntoIterator<Item = DataFile>,
    ) -> Result<&mut Self> {
        self.snapshot_produce_action.add_data_files(data_files)?;
        Ok(self)
    }

    /// Finished building the action and apply it to the transaction.
    pub async fn apply(self) -> Result<Transaction<'a>> {
        self.snapshot_produce_action
            .apply(FastAppendOperation, DefaultManifestProcess)
            .await
    }
}

struct FastAppendOperation;

impl SnapshotProduceOperation for FastAppendOperation {
    fn operation(&self) -> Operation {
        Operation::Append
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProduceAction<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProduceAction<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let Some(snapshot) = snapshot_produce.tx.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.tx.table.file_io(),
                &snapshot_produce.tx.table.metadata_ref(),
            )
            .await?;

        Ok(manifest_list
            .entries()
            .iter()
            .filter(|entry| entry.has_added_files() || entry.has_existing_files())
            .cloned()
            .collect())
    }
}

trait SnapshotProduceOperation: Send + Sync {
    fn operation(&self) -> Operation;
    #[allow(unused)]
    fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProduceAction,
    ) -> impl Future<Output = Result<Vec<ManifestEntry>>> + Send;
    fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProduceAction,
    ) -> impl Future<Output = Result<Vec<ManifestFile>>> + Send;
}

struct DefaultManifestProcess;

impl ManifestProcess for DefaultManifestProcess {
    fn process_manifeset(&self, manifests: Vec<ManifestFile>) -> Vec<ManifestFile> {
        manifests
    }
}

trait ManifestProcess: Send + Sync {
    fn process_manifeset(&self, manifests: Vec<ManifestFile>) -> Vec<ManifestFile>;
}

struct SnapshotProduceAction<'a> {
    tx: Transaction<'a>,
    snapshot_id: i64,
    key_metadata: Vec<u8>,
    commit_uuid: Uuid,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    // A counter used to generate unique manifest file names.
    // It starts from 0 and increments for each new manifest file.
    // Note: This counter is limited to the range of (0..u64::MAX).
    manifest_counter: RangeFrom<u64>,
}

impl<'a> SnapshotProduceAction<'a> {
    pub(crate) fn new(
        tx: Transaction<'a>,
        snapshot_id: i64,
        key_metadata: Vec<u8>,
        commit_uuid: Uuid,
        snapshot_properties: HashMap<String, String>,
    ) -> Result<Self> {
        Ok(Self {
            tx,
            snapshot_id,
            commit_uuid,
            snapshot_properties,
            added_data_files: vec![],
            manifest_counter: (0..),
            key_metadata,
        })
    }

    // Check if the partition value is compatible with the partition type.
    fn validate_partition_value(
        partition_value: &Struct,
        partition_type: &StructType,
    ) -> Result<()> {
        if partition_value.fields().len() != partition_type.fields().len() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Partition value is not compatible with partition type",
            ));
        }
        for (value, field) in partition_value.fields().iter().zip(partition_type.fields()) {
            if !field
                .field_type
                .as_primitive_type()
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "Partition field should only be primitive type.",
                    )
                })?
                .compatible(&value.as_primitive_literal().unwrap())
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Partition value is not compatible partition type",
                ));
            }
        }
        Ok(())
    }

    /// Add data files to the snapshot.
    pub fn add_data_files(
        &mut self,
        data_files: impl IntoIterator<Item = DataFile>,
    ) -> Result<&mut Self> {
        let data_files: Vec<DataFile> = data_files.into_iter().collect();
        for data_file in &data_files {
            if data_file.content_type() != crate::spec::DataContentType::Data {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Only data content type is allowed for fast append",
                ));
            }
            Self::validate_partition_value(
                data_file.partition(),
                self.tx.table.metadata().default_partition_type(),
            )?;
        }
        self.added_data_files.extend(data_files);
        Ok(self)
    }

    fn new_manifest_output(&mut self) -> Result<OutputFile> {
        let new_manifest_path = format!(
            "{}/{}/{}-m{}.{}",
            self.tx.table.metadata().location(),
            META_ROOT_PATH,
            self.commit_uuid,
            self.manifest_counter.next().unwrap(),
            DataFileFormat::Avro
        );
        self.tx.table.file_io().new_output(new_manifest_path)
    }

    // Write manifest file for added data files and return the ManifestFile for ManifestList.
    async fn write_added_manifest(&mut self) -> Result<ManifestFile> {
        let added_data_files = std::mem::take(&mut self.added_data_files);
        let snapshot_id = self.snapshot_id;
        let manifest_entries = added_data_files.into_iter().map(|data_file| {
            let builder = ManifestEntry::builder()
                .status(crate::spec::ManifestStatus::Added)
                .data_file(data_file);
            if self.tx.table.metadata().format_version() == FormatVersion::V1 {
                builder.snapshot_id(snapshot_id).build()
            } else {
                // For format version > 1, we set the snapshot id at the inherited time to avoid rewrite the manifest file when
                // commit failed.
                builder.build()
            }
        });
        let mut writer = {
            let builder = ManifestWriterBuilder::new(
                self.new_manifest_output()?,
                Some(self.snapshot_id),
                self.key_metadata.clone(),
                self.tx.table.metadata().current_schema().clone(),
                self.tx
                    .table
                    .metadata()
                    .default_partition_spec()
                    .as_ref()
                    .clone(),
            );
            if self.tx.table.metadata().format_version() == FormatVersion::V1 {
                builder.build_v1()
            } else {
                builder.build_v2_data()
            }
        };
        for entry in manifest_entries {
            writer.add_entry(entry)?;
        }
        writer.write_manifest_file().await
    }

    async fn manifest_file<OP: SnapshotProduceOperation, MP: ManifestProcess>(
        &mut self,
        snapshot_produce_operation: &OP,
        manifest_process: &MP,
    ) -> Result<Vec<ManifestFile>> {
        let added_manifest = self.write_added_manifest().await?;
        let existing_manifests = snapshot_produce_operation.existing_manifest(self).await?;
        // # TODO
        // Support process delete entries.

        let mut manifest_files = vec![added_manifest];
        manifest_files.extend(existing_manifests);
        let manifest_files = manifest_process.process_manifeset(manifest_files);
        Ok(manifest_files)
    }

    // # TODO
    // Fulfill this function
    fn summary<OP: SnapshotProduceOperation>(&self, snapshot_produce_operation: &OP) -> Summary {
        Summary {
            operation: snapshot_produce_operation.operation(),
            additional_properties: self.snapshot_properties.clone(),
        }
    }

    fn generate_manifest_list_file_path(&self, attempt: i64) -> String {
        format!(
            "{}/{}/snap-{}-{}-{}.{}",
            self.tx.table.metadata().location(),
            META_ROOT_PATH,
            self.snapshot_id,
            attempt,
            self.commit_uuid,
            DataFileFormat::Avro
        )
    }

    /// Finished building the action and apply it to the transaction.
    pub async fn apply<OP: SnapshotProduceOperation, MP: ManifestProcess>(
        mut self,
        snapshot_produce_operation: OP,
        process: MP,
    ) -> Result<Transaction<'a>> {
        let new_manifests = self
            .manifest_file(&snapshot_produce_operation, &process)
            .await?;
        let next_seq_num = self.tx.table.metadata().next_sequence_number();

        let summary = self.summary(&snapshot_produce_operation);

        let manifest_list_path = self.generate_manifest_list_file_path(0);

        let mut manifest_list_writer = match self.tx.table.metadata().format_version() {
            FormatVersion::V1 => ManifestListWriter::v1(
                self.tx
                    .table
                    .file_io()
                    .new_output(manifest_list_path.clone())?,
                self.snapshot_id,
                self.tx.table.metadata().current_snapshot_id(),
            ),
            FormatVersion::V2 => ManifestListWriter::v2(
                self.tx
                    .table
                    .file_io()
                    .new_output(manifest_list_path.clone())?,
                self.snapshot_id,
                self.tx.table.metadata().current_snapshot_id(),
                next_seq_num,
            ),
        };
        manifest_list_writer.add_manifests(new_manifests.into_iter())?;
        manifest_list_writer.close().await?;

        let commit_ts = chrono::Utc::now().timestamp_millis();
        let new_snapshot = Snapshot::builder()
            .with_manifest_list(manifest_list_path)
            .with_snapshot_id(self.snapshot_id)
            .with_parent_snapshot_id(self.tx.table.metadata().current_snapshot_id())
            .with_sequence_number(next_seq_num)
            .with_summary(summary)
            .with_schema_id(self.tx.table.metadata().current_schema_id())
            .with_timestamp_ms(commit_ts)
            .build();

        self.tx.append_updates(vec![
            TableUpdate::AddSnapshot {
                snapshot: new_snapshot,
            },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: SnapshotReference::new(
                    self.snapshot_id,
                    SnapshotRetention::branch(None, None, None),
                ),
            },
        ])?;
        self.tx.append_requirements(vec![
            TableRequirement::UuidMatch {
                uuid: self.tx.table.metadata().uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: self.tx.table.metadata().current_snapshot_id(),
            },
        ])?;
        Ok(self.tx)
    }
}

/// Transaction action for replacing sort order.
pub struct ReplaceSortOrderAction<'a> {
    tx: Transaction<'a>,
    sort_fields: Vec<SortField>,
}

impl<'a> ReplaceSortOrderAction<'a> {
    /// Adds a field for sorting in ascending order.
    pub fn asc(self, name: &str, null_order: NullOrder) -> Result<Self> {
        self.add_sort_field(name, SortDirection::Ascending, null_order)
    }

    /// Adds a field for sorting in descending order.
    pub fn desc(self, name: &str, null_order: NullOrder) -> Result<Self> {
        self.add_sort_field(name, SortDirection::Descending, null_order)
    }

    /// Finished building the action and apply it to the transaction.
    pub fn apply(mut self) -> Result<Transaction<'a>> {
        let unbound_sort_order = SortOrder::builder()
            .with_fields(self.sort_fields)
            .build_unbound()?;

        let updates = vec![
            TableUpdate::AddSortOrder {
                sort_order: unbound_sort_order,
            },
            TableUpdate::SetDefaultSortOrder { sort_order_id: -1 },
        ];

        let requirements = vec![
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: self.tx.table.metadata().current_schema().schema_id(),
            },
            TableRequirement::DefaultSortOrderIdMatch {
                default_sort_order_id: self.tx.table.metadata().default_sort_order().order_id,
            },
        ];

        self.tx.append_requirements(requirements)?;
        self.tx.append_updates(updates)?;
        Ok(self.tx)
    }

    fn add_sort_field(
        mut self,
        name: &str,
        sort_direction: SortDirection,
        null_order: NullOrder,
    ) -> Result<Self> {
        let field_id = self
            .tx
            .table
            .metadata()
            .current_schema()
            .field_id_by_name(name)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Cannot find field {} in table schema", name),
                )
            })?;

        let sort_field = SortField::builder()
            .source_id(field_id)
            .transform(Transform::Identity)
            .direction(sort_direction)
            .null_order(null_order)
            .build();

        self.sort_fields.push(sort_field);
        Ok(self)
    }
}

/// Only used for v2 table metadata
#[allow(dead_code)]
struct UpdatePartitionSpec<'a> {
    transaction: &'a Transaction<'a>,
    name_to_field: HashMap<String, PartitionField>,
    name_to_added_field: HashMap<String, PartitionField>,
    transform_to_field: HashMap<(i32, Transform), PartitionField>,
    transform_to_added_field: HashMap<(i32, Transform), PartitionField>,
    renames: HashMap<String, String>,
    added_time_fields: HashMap<i32, PartitionField>,
    case_sensitive: bool,
    adds: Vec<PartitionField>,
    deletes: HashSet<i32>,
    last_assigned_partition_id: i32,
}

#[allow(dead_code)]
impl<'a> UpdatePartitionSpec<'a> {
    pub fn new(transaction: &'a Transaction<'a>, case_sensitive: bool) -> Self {
        let name_to_field = transaction
            .table
            .metadata()
            .partition_specs
            .values()
            .flat_map(|spec| spec.fields().iter())
            .map(|field| (field.name.clone(), field.clone()))
            .collect();

        let transform_to_field = transaction
            .table
            .metadata()
            .partition_specs
            .values()
            .flat_map(|spec| spec.fields().iter())
            .map(|field| {
                let key = (field.source_id, field.transform);
                (key, field.clone())
            })
            .collect();

        Self {
            transaction,
            name_to_field,
            name_to_added_field: HashMap::new(),
            transform_to_field,
            transform_to_added_field: HashMap::new(),
            renames: HashMap::new(),
            added_time_fields: HashMap::new(),
            case_sensitive,
            adds: Vec::new(),
            deletes: HashSet::new(),
            last_assigned_partition_id: transaction.table.metadata().last_partition_id,
        }
    }

    pub fn add_field(
        &mut self,
        source_column: String,
        transform: Transform,
        partition_field_name: Option<String>,
    ) -> Result<&mut Self> {
        let reference = Reference::new(source_column);
        let bound_reference = reference.bind(
            self.transaction.table.metadata().current_schema().clone(),
            self.case_sensitive,
        )?;

        let primitive_type = bound_reference
            .field()
            .field_type
            .as_primitive_type()
            .unwrap();
        if !transform.can_transform_from_primitive_type(primitive_type) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Cannot apply transform output on {:?}", primitive_type),
            ));
        }

        // Check if field already exists with the same transformation
        let transform_key = (bound_reference.field().id, transform);

        if let Some(existing_partition_field) = self.transform_to_field.get(&transform_key) {
            if !self.deletes.contains(&existing_partition_field.field_id) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Duplicate partition field for {}, {} already exists",
                        reference.name(),
                        existing_partition_field.name
                    ),
                ));
            }
        }

        if let Some(added) = self.transform_to_added_field.get(&transform_key) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Already added partition: {}", added.name),
            ));
        }

        let new_field = self.create_partition_field(
            (bound_reference.field().id, transform),
            partition_field_name,
        )?;

        if self.name_to_added_field.contains_key(&new_field.name) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Already added partition field with name: {}",
                    new_field.name
                ),
            ));
        }

        if new_field.transform.is_time_transform() {
            if let Some(existing_time_field) = self.added_time_fields.get(&new_field.source_id) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot add time partition field: {} conflicts with {}",
                        new_field.name, existing_time_field.name
                    ),
                ));
            }
            self.added_time_fields
                .insert(new_field.source_id, new_field.clone());
        }

        // Record the new field in the "added" map
        self.transform_to_added_field
            .insert(transform_key, new_field.clone());

        if let Some(existing_partition_field) = self.name_to_field.get(&new_field.name) {
            if !self.deletes.contains(&new_field.field_id) {
                if existing_partition_field.transform.is_void_transform() {
                    let new_name = format!(
                        "{}_{}",
                        existing_partition_field.name, existing_partition_field.field_id
                    );
                    let _ = self.rename_field(existing_partition_field.name.clone(), new_name);
                } else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Cannot add duplicate partition field name: {}",
                            existing_partition_field.name
                        ),
                    ));
                }
            }
        }

        // Add new fields
        self.name_to_added_field
            .insert(new_field.name.clone(), new_field.clone());
        self.adds.push(new_field);

        Ok(self)
    }

    pub fn add_identity(&mut self, source_column_name: String) -> Result<&mut Self> {
        self.add_field(source_column_name, Transform::Identity, None)
    }

    pub fn remove_field(&mut self, name: &str) -> Result<&mut Self> {
        // Cannot remove name that was added in the update
        if self.name_to_added_field.contains_key(name) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Cannot delete newly added field {}", name),
            ));
        }

        // Cannot remove a field that has been renamed
        if self.renames.contains_key(name) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Cannot rename and delete field {}", name),
            ));
        }

        let field = self.name_to_field.get(name).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("No such partition field: {}", name),
            )
        })?;

        self.deletes.insert(field.field_id);

        Ok(self)
    }

    pub fn rename_field(&mut self, name: String, new_name: String) -> Result<&mut Self> {
        if let Some(existing_field) = self.name_to_field.get(&new_name) {
            if existing_field.transform.is_void_transform() {
                let new_new_name = format!("{}_{}", name, existing_field.field_id);
                return self.rename_field(name, new_new_name);
            }
        }

        // do not allow renaming of recently added fields.
        if self.name_to_added_field.contains_key(&name) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot rename recently added partitions".to_string(),
            ));
        }

        let field = self.name_to_field.get(&name).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Cannot find partition field {}", name),
            )
        })?;

        if self.deletes.contains(&field.field_id) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Cannot delete and rename partition field {}", name),
            ));
        }

        self.renames.insert(name, new_name);
        Ok(self)
    }

    /// Commit the changes and produce updates and requirements
    pub fn commit(&mut self) -> (Vec<TableUpdate>, Vec<TableRequirement>) {
        let new_spec = self.apply();
        let mut updates = Vec::new();
        let mut requirements = Vec::new();

        // if default spec id changed
        if self
            .transaction
            .table
            .metadata()
            .default_partition_spec()
            .spec_id()
            != new_spec.spec_id()
        {
            if !self
                .transaction
                .table
                .metadata()
                .partition_specs
                .contains_key(&new_spec.spec_id())
            {
                updates.push(TableUpdate::AddSpec {
                    spec: new_spec.clone().into_unbound(),
                });
                updates.push(TableUpdate::SetDefaultSpec { spec_id: -1 });
            } else {
                // Otherwise, simply update the default spec to the new spec's id
                updates.push(TableUpdate::SetDefaultSpec {
                    spec_id: new_spec.spec_id(),
                });
            }
            let required_last_assigned_partition_id =
                self.transaction.table.metadata().last_partition_id;
            requirements.push(TableRequirement::LastAssignedPartitionIdMatch {
                last_assigned_partition_id: required_last_assigned_partition_id,
            });
        }
        (updates, requirements)
    }

    /// Apply the updates and produce a new PartitionSpec.
    pub fn apply(&self) -> PartitionSpec {
        let mut partition_fields = Vec::new();
        let mut partition_names = HashSet::new();
        let schema = self.transaction.table.metadata().current_schema();

        // Iterate over existing partition fields from the metadata spec.
        for spec in self.transaction.table.metadata().partition_specs.values() {
            for field in spec.fields().iter() {
                if !self.deletes.contains(&field.field_id) {
                    if let Some(renamed) = self.renames.get(&field.name) {
                        // Create a new field with the renamed name.
                        let new_field = self
                            .add_new_field(
                                schema,
                                field.source_id,
                                field.field_id,
                                renamed.clone(),
                                field.transform,
                                &mut partition_names,
                            )
                            .expect("Error adding renamed field");
                        partition_fields.push(new_field);
                    } else {
                        partition_fields.push(field.clone());
                    }
                }
            }
        }

        // Append newly added fields.
        for field in &self.adds {
            partition_fields.push(field.clone());
        }

        let mut builder = PartitionSpec::builder(schema.clone())
            .with_spec_id(self.transaction.table.metadata().default_spec.spec_id());

        for field in partition_fields {
            // Retrieve the source column name from the schema
            let source_name = schema
                .field_by_id(field.source_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Source id {} not found in schema", field.source_id),
                    )
                })
                .unwrap()
                .name
                .clone();

            // Add the field using the builder.
            builder = builder
                .add_partition_field(source_name, field.name, field.transform)
                .unwrap();
        }

        builder.build().unwrap()
    }

    pub fn create_partition_field(
        &mut self,
        transform_key: (i32, Transform),
        partition_field_name: Option<String>,
    ) -> Result<PartitionField> {
        let (source_id, transform) = transform_key;

        if self.transaction.table.metadata().format_version == V2 {
            let historical_fields: Vec<&PartitionField> = self
                .transaction
                .table
                .metadata()
                .partition_specs
                .values()
                .flat_map(|spec| spec.fields().iter())
                .collect();

            for field in historical_fields {
                if field.source_id == source_id
                    && field.transform == transform
                    && partition_field_name
                        .as_ref()
                        .map_or(true, |name| name == &field.name)
                {
                    return Ok(field.clone());
                }
            }
        }

        let new_field_id = self.new_field_id();
        let field_name = if let Some(name) = partition_field_name {
            name
        } else {
            // TODO: create partition visitor for naming
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Partition Visitor not implemented",
            ));
        };

        Ok(PartitionField::new(
            source_id,
            new_field_id,
            field_name,
            transform,
        ))
    }

    fn check_and_add_partition_name(
        &self,
        schema: &Schema,
        name: &str,
        source_id: i32,
        partition_names: &mut HashSet<String>,
    ) -> Result<()> {
        // Attempt to find a field by name in the schema.
        let field = schema.field_by_name(name).unwrap();
        if field.id != source_id {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot create identity partition from a different field in the schema {}",
                    name
                ),
            ));
        }

        if name.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Undefined name".to_string(),
            ));
        }
        if partition_names.contains(name) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Partition name has to be unique: {}", name),
            ));
        }
        partition_names.insert(name.to_string());
        Ok(())
    }

    // Create partition field helper
    fn add_new_field(
        &self,
        schema: &Schema,
        source_id: i32,
        field_id: i32,
        name: String,
        transform: Transform,
        partition_names: &mut std::collections::HashSet<String>,
    ) -> Result<PartitionField> {
        self.check_and_add_partition_name(schema, &name, source_id, partition_names)?;
        Ok(PartitionField::new(source_id, field_id, name, transform))
    }

    fn new_field_id(&mut self) -> i32 {
        self.last_assigned_partition_id += 1;
        self.last_assigned_partition_id
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::BufReader;

    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, FormatVersion, Literal, Struct,
        TableMetadata,
    };
    use crate::table::Table;
    use crate::transaction::{Transaction, MAIN_BRANCH};
    use crate::{TableIdent, TableRequirement, TableUpdate};

    fn make_v1_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV1Valid.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap()
    }

    fn make_v2_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2Valid.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap()
    }

    fn make_v2_minimal_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2ValidMinimal.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap()
    }

    #[test]
    fn test_upgrade_table_version_v1_to_v2() {
        let table = make_v1_table();
        let tx = Transaction::new(&table);
        let tx = tx.upgrade_table_version(FormatVersion::V2).unwrap();

        assert_eq!(
            vec![TableUpdate::UpgradeFormatVersion {
                format_version: FormatVersion::V2
            }],
            tx.updates
        );
    }

    #[test]
    fn test_upgrade_table_version_v2_to_v2() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let tx = tx.upgrade_table_version(FormatVersion::V2).unwrap();

        assert!(
            tx.updates.is_empty(),
            "Upgrade table to same version should not generate any updates"
        );
        assert!(
            tx.requirements.is_empty(),
            "Upgrade table to same version should not generate any requirements"
        );
    }

    #[test]
    fn test_downgrade_table_version() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let tx = tx.upgrade_table_version(FormatVersion::V1);

        assert!(tx.is_err(), "Downgrade table version should fail!");
    }

    #[test]
    fn test_set_table_property() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let tx = tx
            .set_properties(HashMap::from([("a".to_string(), "b".to_string())]))
            .unwrap();

        assert_eq!(
            vec![TableUpdate::SetProperties {
                updates: HashMap::from([("a".to_string(), "b".to_string())])
            }],
            tx.updates
        );
    }

    #[test]
    fn test_remove_property() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let tx = tx
            .remove_properties(vec!["a".to_string(), "b".to_string()])
            .unwrap();

        assert_eq!(
            vec![TableUpdate::RemoveProperties {
                removals: vec!["a".to_string(), "b".to_string()]
            }],
            tx.updates
        );
    }

    #[test]
    fn test_replace_sort_order() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let tx = tx.replace_sort_order().apply().unwrap();

        assert_eq!(
            vec![
                TableUpdate::AddSortOrder {
                    sort_order: Default::default()
                },
                TableUpdate::SetDefaultSortOrder { sort_order_id: -1 }
            ],
            tx.updates
        );

        assert_eq!(
            vec![
                TableRequirement::CurrentSchemaIdMatch {
                    current_schema_id: 1
                },
                TableRequirement::DefaultSortOrderIdMatch {
                    default_sort_order_id: 3
                }
            ],
            tx.requirements
        );
    }

    #[tokio::test]
    async fn test_fast_append_action() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let mut action = tx.fast_append(None, vec![]).unwrap();

        // check add data file with incompatible partition value
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/3.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition(Struct::from_iter([Some(Literal::string("test"))]))
            .build()
            .unwrap();
        assert!(action.add_data_files(vec![data_file.clone()]).is_err());

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/3.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();
        action.add_data_files(vec![data_file.clone()]).unwrap();
        let tx = action.apply().await.unwrap();

        // check updates and requirements
        assert!(
            matches!((&tx.updates[0],&tx.updates[1]), (TableUpdate::AddSnapshot { snapshot },TableUpdate::SetSnapshotRef { reference,ref_name }) if snapshot.snapshot_id() == reference.snapshot_id && ref_name == MAIN_BRANCH)
        );
        assert_eq!(
            vec![
                TableRequirement::UuidMatch {
                    uuid: tx.table.metadata().uuid()
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: MAIN_BRANCH.to_string(),
                    snapshot_id: tx.table.metadata().current_snapshot_id
                }
            ],
            tx.requirements
        );

        // check manifest list
        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &tx.updates[0] {
            snapshot
        } else {
            unreachable!()
        };
        let manifest_list = new_snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert_eq!(1, manifest_list.entries().len());
        assert_eq!(
            manifest_list.entries()[0].sequence_number,
            new_snapshot.sequence_number()
        );

        // check manifset
        let manifest = manifest_list.entries()[0]
            .load_manifest(table.file_io())
            .await
            .unwrap();
        assert_eq!(1, manifest.entries().len());
        assert_eq!(
            new_snapshot.sequence_number(),
            manifest.entries()[0]
                .sequence_number()
                .expect("Inherit sequence number by load manifest")
        );
        assert_eq!(
            new_snapshot.snapshot_id(),
            manifest.entries()[0].snapshot_id().unwrap()
        );
        assert_eq!(data_file, *manifest.entries()[0].data_file());
    }

    #[test]
    fn test_do_same_update_in_same_transaction() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let tx = tx
            .remove_properties(vec!["a".to_string(), "b".to_string()])
            .unwrap();

        let tx = tx.remove_properties(vec!["c".to_string(), "d".to_string()]);

        assert!(
            tx.is_err(),
            "Should not allow to do same kinds update in same transaction"
        );
    }

    fn find_unused_column(schema: &Schema, ups: &UpdatePartitionSpec) -> Option<String> {
        schema.id_to_field().values().find_map(|f| {
            if ups
                .name_to_field
                .values()
                .any(|pf| pf.source_id == f.id && pf.transform == Transform::Identity)
            {
                None
            } else {
                Some(f.name.clone())
            }
        })
    }

    #[test]
    fn test_add_field_success() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let mut ups = UpdatePartitionSpec::new(&tx, true);

        let schema = tx.table.metadata().current_schema();

        let new_col = find_unused_column(schema, &ups)
            .expect("No available column for a new partition field");

        // Attempt to add a new identity partition field
        let res = ups.add_field(
            new_col.clone(),
            Transform::Identity,
            Some("new_identity".to_string()),
        );
        assert!(res.is_ok());

        // Check that the new field was recorded in the 'added' maps
        assert!(ups.name_to_added_field.contains_key("new_identity"));
        assert!(ups
            .transform_to_added_field
            .values()
            .any(|pf| pf.name == "new_identity"));
        assert!(ups.adds.iter().any(|pf| pf.name == "new_identity"));
    }

    #[test]
    fn test_add_field_duplicate() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let mut ups = UpdatePartitionSpec::new(&tx, true);

        let existing_field = ups
            .name_to_field
            .values()
            .next()
            .expect("No partition field in metadata")
            .clone();

        let source_name = tx
            .table
            .metadata()
            .current_schema()
            .field_by_id(existing_field.source_id)
            .unwrap()
            .name
            .clone();

        // Try to add the same partition field (same source and transform).
        let res = ups.add_field(
            source_name,
            existing_field.transform,
            Some(existing_field.name.clone()),
        );

        // Shouldn't allow duplicate
        assert!(res.is_err());
    }

    #[test]
    fn test_remove_field_success() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let mut ups = UpdatePartitionSpec::new(&tx, true);

        let field_name = ups.name_to_field.keys().next().unwrap().clone();
        let field_id = ups.name_to_field.get(&field_name).unwrap().field_id;

        // removing existing field should succeed
        let res = ups.remove_field(&field_name);
        assert!(res.is_ok());
        assert!(ups.deletes.contains(&field_id));
    }

    #[test]
    fn test_remove_field_newly_added_failure() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let mut ups = UpdatePartitionSpec::new(&tx, true);

        let schema = tx.table.metadata().current_schema();
        let new_col = find_unused_column(schema, &ups).expect("No available column");
        let res = ups.add_field(new_col, Transform::Identity, Some("new_field".to_string()));
        assert!(res.is_ok());

        // Attempting to remove a field that was just added should fail.
        let res_remove = ups.remove_field("new_field");
        assert!(res_remove.is_err());
    }

    #[test]
    fn test_rename_field_success() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let mut ups = UpdatePartitionSpec::new(&tx, true);

        // Rename an existing partition field.
        let field_name = ups.name_to_field.keys().next().unwrap().clone();
        let new_name = format!("renamed_{}", field_name);
        let res = ups.rename_field(field_name.clone(), new_name.clone());

        assert!(res.is_ok());
        assert_eq!(ups.renames.get(&field_name).unwrap(), &new_name);
    }

    #[test]
    fn test_rename_field_newly_added_failure() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let mut ups = UpdatePartitionSpec::new(&tx, true);

        let schema = tx.table.metadata().current_schema();
        let new_col = find_unused_column(schema, &ups).expect("No available column");
        let res = ups.add_field(new_col, Transform::Identity, Some("new_field".to_string()));
        assert!(res.is_ok());

        // Renaming a newly added field is not allowed.
        let res_rename = ups.rename_field("new_field".to_string(), "renamed_new_field".to_string());
        assert!(res_rename.is_err());
    }
}
