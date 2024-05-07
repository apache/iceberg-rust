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
use std::collections::HashMap;
use std::mem::discriminant;

use crate::error::Result;
use crate::spec::{
    DataFile, DataFileFormat, FormatVersion, Manifest, ManifestEntry, ManifestFile,
    ManifestListWriter, ManifestMetadata, ManifestWriter, NullOrder, PartitionSpec, Schema,
    Snapshot, SnapshotReference, SnapshotRetention, SortDirection, SortField, SortOrder, Struct,
    StructType, Summary, Transform,
};
use crate::table::Table;
use crate::TableUpdate::UpgradeFormatVersion;
use crate::{Catalog, Error, ErrorKind, TableCommit, TableRequirement, TableUpdate};

const INITIAL_SEQUENCE_NUMBER: i64 = 0;
const META_ROOT_PATH: &str = "metadata";
const MAIN_BRANCH: &str = "main";

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
        commit_uuid: Option<String>,
        key_metadata: Vec<u8>,
    ) -> Result<FastAppendAction<'a>> {
        let parent_snapshot_id = self
            .table
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id());
        let snapshot_id = self.generate_unique_snapshot_id();
        let schema = self.table.metadata().current_schema().as_ref().clone();
        let schema_id = schema.schema_id();
        let format_version = self.table.metadata().format_version();
        let partition_spec = self
            .table
            .metadata()
            .default_partition_spec()
            .map(|spec| spec.as_ref().clone())
            .unwrap_or_default();
        let commit_uuid = commit_uuid.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        FastAppendAction::new(
            self,
            parent_snapshot_id,
            snapshot_id,
            schema,
            schema_id,
            format_version,
            partition_spec,
            key_metadata,
            commit_uuid,
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
    tx: Transaction<'a>,

    parent_snapshot_id: Option<i64>,
    snapshot_id: i64,
    schema: Schema,
    schema_id: i32,
    format_version: FormatVersion,
    partition_spec: PartitionSpec,
    key_metadata: Vec<u8>,

    commit_uuid: String,
    manifest_id: i64,

    appended_data_files: Vec<DataFile>,
}

impl<'a> FastAppendAction<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        tx: Transaction<'a>,
        parent_snapshot_id: Option<i64>,
        snapshot_id: i64,
        schema: Schema,
        schema_id: i32,
        format_version: FormatVersion,
        partition_spec: PartitionSpec,
        key_metadata: Vec<u8>,
        commit_uuid: String,
    ) -> Result<Self> {
        Ok(Self {
            tx,
            parent_snapshot_id,
            snapshot_id,
            schema,
            schema_id,
            format_version,
            partition_spec,
            key_metadata,
            commit_uuid,
            manifest_id: 0,
            appended_data_files: vec![],
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
                "Partition value is not compatitable with partition type",
            ));
        }
        if partition_value
            .fields()
            .iter()
            .zip(partition_type.fields())
            .any(|(field, field_type)| !field_type.field_type.compatible(field))
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Partition value is not compatitable partition type",
            ));
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
                &self.partition_spec.partition_type(&self.schema)?,
            )?;
        }
        self.appended_data_files.extend(data_files);
        Ok(self)
    }

    fn generate_manifest_file_path(&mut self) -> String {
        let manifest_id = self.manifest_id;
        self.manifest_id += 1;
        format!(
            "{}/{}/{}-m{}.{}",
            self.tx.table.metadata().location(),
            META_ROOT_PATH,
            &self.commit_uuid,
            manifest_id,
            DataFileFormat::Avro
        )
    }

    async fn manifest_from_parent_snapshot(&self) -> Result<Vec<ManifestFile>> {
        if let Some(snapshot) = self.tx.table.metadata().current_snapshot() {
            let manifest_list = snapshot
                .load_manifest_list(self.tx.table.file_io(), &self.tx.table.metadata_ref())
                .await?;
            let mut manifest_files = Vec::with_capacity(manifest_list.entries().len());
            for entry in manifest_list.entries() {
                // From: https://github.com/apache/iceberg-python/blob/659a951d6397ab280cae80206fe6e8e4be2d3738/pyiceberg/table/__init__.py#L2921
                // Why we need this?
                if entry.added_snapshot_id == self.snapshot_id {
                    continue;
                }
                let manifest = entry.load_manifest(self.tx.table.file_io()).await?;
                // Skip manifest with all delete entries.
                if manifest.entries().iter().all(|entry| !entry.is_alive()) {
                    continue;
                }
                manifest_files.push(entry.clone());
            }
            Ok(manifest_files)
        } else {
            Ok(vec![])
        }
    }

    // Write manifest file for added data files and return the ManifestFile for ManifestList.
    async fn manifest_for_data_file(&mut self) -> Result<ManifestFile> {
        let appended_data_files = std::mem::take(&mut self.appended_data_files);
        let manifest_entries = appended_data_files
            .into_iter()
            .map(|data_file| {
                let builder = ManifestEntry::builder()
                    .status(crate::spec::ManifestStatus::Added)
                    .data_file(data_file);
                if self.format_version as u8 == 1u8 {
                    builder.snapshot_id(self.snapshot_id).build()
                } else {
                    // For format version > 1, we set the snapshot id at the inherited time to avoid rewrite the manifest file when
                    // commit failed.
                    builder.build()
                }
            })
            .collect();
        let manifest_meta = ManifestMetadata::builder()
            .schema(self.schema.clone())
            .schema_id(self.schema_id)
            .format_version(self.format_version)
            .partition_spec(self.partition_spec.clone())
            .content(crate::spec::ManifestContentType::Data)
            .build();
        let manifest = Manifest::new(manifest_meta, manifest_entries);
        let writer = ManifestWriter::new(
            self.tx
                .table
                .file_io()
                .new_output(self.generate_manifest_file_path())?,
            self.snapshot_id,
            self.key_metadata.clone(),
        );
        writer.write(manifest).await
    }

    fn summary(&self) -> Summary {
        Summary {
            operation: crate::spec::Operation::Append,
            other: HashMap::new(),
        }
    }

    /// Finished building the action and apply it to the transaction.
    pub async fn apply(mut self) -> Result<Transaction<'a>> {
        let summary = self.summary();
        let manifest = self.manifest_for_data_file().await?;
        let existing_manifest_files = self.manifest_from_parent_snapshot().await?;

        let snapshot_produce_action = SnapshotProduceAction::new(
            self.tx,
            self.snapshot_id,
            self.parent_snapshot_id,
            self.schema_id,
            self.format_version,
            self.commit_uuid,
        )?;

        snapshot_produce_action
            .apply(
                vec![manifest]
                    .into_iter()
                    .chain(existing_manifest_files.into_iter()),
                summary,
            )
            .await
    }
}

struct SnapshotProduceAction<'a> {
    tx: Transaction<'a>,

    parent_snapshot_id: Option<i64>,
    snapshot_id: i64,
    schema_id: i32,
    format_version: FormatVersion,

    commit_uuid: String,
}

impl<'a> SnapshotProduceAction<'a> {
    pub(crate) fn new(
        tx: Transaction<'a>,
        snapshot_id: i64,
        parent_snapshot_id: Option<i64>,
        schema_id: i32,
        format_version: FormatVersion,
        commit_uuid: String,
    ) -> Result<Self> {
        Ok(Self {
            tx,
            parent_snapshot_id,
            snapshot_id,
            schema_id,
            format_version,
            commit_uuid,
        })
    }

    fn generate_manifest_list_file_path(&self, next_seq_num: i64) -> String {
        format!(
            "{}/{}/snap-{}-{}-{}.{}",
            self.tx.table.metadata().location(),
            META_ROOT_PATH,
            self.snapshot_id,
            next_seq_num,
            self.commit_uuid,
            DataFileFormat::Avro
        )
    }

    /// Finished building the action and apply it to the transaction.
    pub async fn apply(
        mut self,
        manifest_files: impl IntoIterator<Item = ManifestFile>,
        summary: Summary,
    ) -> Result<Transaction<'a>> {
        let next_seq_num = if self.format_version as u8 > 1u8 {
            self.tx.table.metadata().last_sequence_number() + 1
        } else {
            INITIAL_SEQUENCE_NUMBER
        };
        let commit_ts = chrono::Utc::now().timestamp_millis();
        let manifest_list_path = self.generate_manifest_list_file_path(next_seq_num);

        let mut manifest_list_writer = ManifestListWriter::v2(
            self.tx
                .table
                .file_io()
                .new_output(manifest_list_path.clone())?,
            self.snapshot_id,
            // # TODO
            // Should we use `0` here for default parent snapshot id?
            self.parent_snapshot_id.unwrap_or_default(),
            next_seq_num,
        );
        manifest_list_writer.add_manifests(manifest_files.into_iter())?;
        manifest_list_writer.close().await?;

        let new_snapshot = Snapshot::builder()
            .with_manifest_list(manifest_list_path)
            .with_snapshot_id(self.snapshot_id)
            .with_parent_snapshot_id(self.parent_snapshot_id)
            .with_sequence_number(next_seq_num)
            .with_summary(summary)
            .with_schema_id(self.schema_id)
            .with_timestamp_ms(commit_ts)
            .build();

        let new_snapshot_id = new_snapshot.snapshot_id();
        self.tx.append_updates(vec![
            TableUpdate::AddSnapshot {
                snapshot: new_snapshot,
            },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: SnapshotReference::new(
                    new_snapshot_id,
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
                snapshot_id: self.parent_snapshot_id,
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::BufReader;

    use crate::io::FileIO;
    use crate::spec::{FormatVersion, TableMetadata};
    use crate::table::Table;
    use crate::transaction::{Transaction, MAIN_BRACNH};
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
    async fn test_merge_snapshot_actio() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/3.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();
        let mut action = tx.fast_append(None, vec![]).unwrap();
        action.add_data_files(vec![data_file.clone()]).unwrap();
        let tx = action.apply().await.unwrap();

        // check updates and requirements
        let new_snapshot_id = tx
            .table
            .metadata()
            .current_snapshot_id
            .map(|id| id + 1)
            .unwrap_or(0);
        assert!(
            matches!(&tx.updates[0], TableUpdate::AddSnapshot { snapshot } if snapshot.snapshot_id() == new_snapshot_id)
                && matches!(&tx.updates[1], TableUpdate::SetSnapshotRef { reference,ref_name } if reference.snapshot_id == new_snapshot_id && ref_name == MAIN_BRANCH),
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
}
