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

mod append;
mod overwrite_files;
mod remove_snapshots;
mod rewrite_files;
mod snapshot;
mod sort_order;
mod utils;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::mem::discriminant;
use std::sync::Arc;

use remove_snapshots::RemoveSnapshotAction;
use rewrite_files::RewriteFilesAction;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::FormatVersion;
use crate::table::Table;
use crate::transaction::append::{FastAppendAction, MergeAppendAction};
use crate::transaction::overwrite_files::OverwriteFilesAction;
use crate::transaction::sort_order::ReplaceSortOrderAction;
use crate::TableUpdate::UpgradeFormatVersion;
use crate::{Catalog, Error, ErrorKind, TableCommit, TableRequirement, TableUpdate};

/// Target size of manifest file when merging manifests.
pub const MANIFEST_TARGET_SIZE_BYTES: &str = "commit.manifest.target-size-bytes";
/// This is the default value for `MANIFEST_TARGET_SIZE_BYTES`.
pub const MANIFEST_TARGET_SIZE_BYTES_DEFAULT: u32 = 8 * 1024 * 1024; // 8 MB
/// Minimum number of manifests to merge.
pub const MANIFEST_MIN_MERGE_COUNT: &str = "commit.manifest.min-count-to-merge";
/// This is the default value for `MANIFEST_MIN_MERGE_COUNT`.
pub const MANIFEST_MIN_MERGE_COUNT_DEFAULT: u32 = 100;
/// Whether allow to merge manifests.
pub const MANIFEST_MERGE_ENABLED: &str = "commit.manifest-merge.enabled";
/// This is the default value for `MANIFEST_MERGE_ENABLED`.
pub const MANIFEST_MERGE_ENABLED_DEFAULT: bool = false;

/// Table transaction.
pub struct Transaction<'a> {
    base_table: &'a Table,
    current_table: Table,
    updates: Vec<TableUpdate>,
    requirements: Vec<TableRequirement>,
}

impl<'a> Transaction<'a> {
    /// Creates a new transaction.
    pub fn new(table: &'a Table) -> Self {
        Self {
            base_table: table,
            current_table: table.clone(),
            updates: vec![],
            requirements: vec![],
        }
    }

    fn update_table_metadata(&mut self, updates: &[TableUpdate]) -> Result<()> {
        let mut metadata_builder = self.current_table.metadata().clone().into_builder(None);
        for update in updates {
            metadata_builder = update.clone().apply(metadata_builder)?;
        }

        self.current_table
            .with_metadata(Arc::new(metadata_builder.build()?.metadata));

        Ok(())
    }

    fn apply(
        &mut self,
        updates: Vec<TableUpdate>,
        requirements: Vec<TableRequirement>,
    ) -> Result<()> {
        for requirement in &requirements {
            requirement.check(Some(self.current_table.metadata()))?;
        }

        self.update_table_metadata(&updates)?;

        self.updates.extend(updates);

        // For the requirements, it does not make sense to add a requirement more than once
        // For example, you cannot assert that the current schema has two different IDs
        for new_requirement in requirements {
            if self
                .requirements
                .iter()
                .map(discriminant)
                .all(|d| d != discriminant(&new_requirement))
            {
                self.requirements.push(new_requirement);
            }
        }

        // # TODO
        // Support auto commit later.

        Ok(())
    }

    /// Sets table to a new version.
    pub fn upgrade_table_version(mut self, format_version: FormatVersion) -> Result<Self> {
        let current_version = self.current_table.metadata().format_version();
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
                self.apply(vec![UpgradeFormatVersion { format_version }], vec![])?;
            }
            Ordering::Equal => {
                // Do nothing.
            }
        }
        Ok(self)
    }

    /// Update table's property.
    pub fn set_properties(mut self, props: HashMap<String, String>) -> Result<Self> {
        self.apply(vec![TableUpdate::SetProperties { updates: props }], vec![])?;
        Ok(self)
    }

    /// Generate a new `snapshot_id`.
    pub fn generate_unique_snapshot_id(&self) -> i64 {
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
            .current_table
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
        snapshot_id: Option<i64>,
        commit_uuid: Option<Uuid>,
        key_metadata: Vec<u8>,
    ) -> Result<FastAppendAction<'a>> {
        let snapshot_id = if let Some(snapshot_id) = snapshot_id {
            if self
                .current_table
                .metadata()
                .snapshots()
                .any(|s| s.snapshot_id() == snapshot_id)
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Snapshot id {} already exists", snapshot_id),
                ));
            }
            snapshot_id
        } else {
            self.generate_unique_snapshot_id()
        };
        FastAppendAction::new(
            self,
            snapshot_id,
            commit_uuid.unwrap_or_else(Uuid::now_v7),
            key_metadata,
            HashMap::new(),
        )
    }

    /// Creates a merge append action.
    pub fn merge_append(
        self,
        commit_uuid: Option<Uuid>,
        key_metadata: Vec<u8>,
    ) -> Result<MergeAppendAction<'a>> {
        let snapshot_id = self.generate_unique_snapshot_id();
        MergeAppendAction::new(
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

    /// Creates remove snapshot action.
    pub fn expire_snapshot(self) -> RemoveSnapshotAction<'a> {
        RemoveSnapshotAction::new(self)
    }

    /// Creates rewrite files action.
    pub fn rewrite_files(
        self,
        commit_uuid: Option<Uuid>,
        key_metadata: Vec<u8>,
    ) -> Result<RewriteFilesAction<'a>> {
        let snapshot_id = self.generate_unique_snapshot_id();
        RewriteFilesAction::new(
            self,
            snapshot_id,
            commit_uuid.unwrap_or_else(Uuid::now_v7),
            key_metadata,
            HashMap::new(),
        )
    }

    /// Remove properties in table.
    pub fn remove_properties(mut self, keys: Vec<String>) -> Result<Self> {
        self.apply(
            vec![TableUpdate::RemoveProperties { removals: keys }],
            vec![],
        )?;
        Ok(self)
    }

    /// Creates an overwrite files action.
    pub fn overwrite_files(
        self,
        commit_uuid: Option<Uuid>,
        key_metadata: Vec<u8>,
    ) -> Result<OverwriteFilesAction<'a>> {
        let snapshot_id = self.generate_unique_snapshot_id();
        OverwriteFilesAction::new(
            self,
            snapshot_id,
            commit_uuid.unwrap_or_else(Uuid::now_v7),
            key_metadata,
            HashMap::new(),
        )
    }

    /// Commit transaction.
    pub async fn commit(self, catalog: &dyn Catalog) -> Result<Table> {
        let table_commit = TableCommit::builder()
            .ident(self.base_table.identifier().clone())
            .updates(self.updates)
            .requirements(self.requirements)
            .build();

        catalog.update_table(table_commit).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::BufReader;

    use crate::io::FileIOBuilder;
    use crate::spec::{FormatVersion, TableMetadata};
    use crate::table::Table;
    use crate::transaction::Transaction;
    use crate::{TableIdent, TableUpdate};

    fn make_v1_table() -> Table {
        let file: File = File::open(format!(
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

    pub fn make_v2_table() -> Table {
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

    pub fn make_v2_minimal_table() -> Table {
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

    #[tokio::test]
    async fn test_transaction_apply_upgrade() {
        let table = make_v1_table();
        let tx = Transaction::new(&table);
        // Upgrade v1 to v1, do nothing.
        let tx = tx.upgrade_table_version(FormatVersion::V1).unwrap();
        // Upgrade v1 to v2, success.
        let tx = tx.upgrade_table_version(FormatVersion::V2).unwrap();
        assert_eq!(
            vec![TableUpdate::UpgradeFormatVersion {
                format_version: FormatVersion::V2
            }],
            tx.updates
        );
        // Upgrade v2 to v1, return error.
        assert!(tx.upgrade_table_version(FormatVersion::V1).is_err());
    }
}
