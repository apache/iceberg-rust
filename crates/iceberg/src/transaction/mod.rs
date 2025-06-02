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

mod action;
mod append;
mod snapshot;
mod sort_order;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::mem::discriminant;
use std::sync::Arc;

use uuid::Uuid;

use crate::TableUpdate::UpgradeFormatVersion;
use crate::error::Result;
use crate::spec::FormatVersion;
use crate::table::Table;
use crate::transaction::action::{PendingAction, SetLocation, TransactionAction, TransactionActionCommitResult};
use crate::transaction::append::FastAppendAction;
use crate::transaction::sort_order::ReplaceSortOrderAction;
use crate::{Catalog, Error, ErrorKind, TableCommit, TableRequirement, TableUpdate};

/// Table transaction.
pub struct Transaction<'a> {
    base_table: &'a Table,
    current_table: Table,
    actions: Vec<PendingAction>,
    updates: Vec<TableUpdate>,
    requirements: Vec<TableRequirement>,
}

impl<'a> Transaction<'a> {
    /// Creates a new transaction.
    pub fn new(table: &'a Table) -> Self {
        Self {
            base_table: table,
            current_table: table.clone(),
            actions: vec![],
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
    
    fn apply_commit_result(&mut self,
                           mut tx_commit_res: TransactionActionCommitResult) -> Result<()> {
        self.actions.push(tx_commit_res.take_action().unwrap());
        self.apply(tx_commit_res.take_updates(), tx_commit_res.take_requirements())
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
        self.apply(
            vec![TableUpdate::RemoveProperties { removals: keys }],
            vec![],
        )?;
        Ok(self)
    }

    /// Set the location of table
    pub fn set_location(mut self, location: String) -> Result<Self> {
        let set_location = SetLocation::new().set_location(location);
        self.apply_commit_result(Box::new(set_location).commit()?).expect("Some error msg");
        Ok(self)
    }

    /// Commit transaction.
    pub async fn commit(&mut self, catalog: &dyn Catalog) -> Result<Table> {
        let base_table_identifier = self.base_table.identifier().to_owned();

        if self.actions.is_empty()
            || (self.base_table.metadata() == self.current_table.metadata()
                && self.base_table.metadata_location() == self.current_table.metadata_location())
        {
            // nothing to commit
            return Ok(self.current_table.clone());
        }

        // TODO use an actual retry
        for _attempt in 0..3 {
            let refreshed = catalog
                .load_table(&base_table_identifier.clone())
                .await
                .expect(format!("Failed to refresh table {}", base_table_identifier).as_str());

            if self.base_table.metadata() != refreshed.metadata()
                || self.base_table.metadata_location() != refreshed.metadata_location()
            {
                // current base is stale, use refreshed as base and re-apply transaction actions
                self.base_table = &refreshed.clone();
                self.current_table = refreshed.clone();

                // let pending_actions = self.actions.clone();

                for action in self.actions {
                    action.commit()?;
                }
            }

            let table_commit = TableCommit::builder()
                .ident(base_table_identifier.clone())
                .updates(self.updates.clone())
                .requirements(self.requirements.clone())
                .build();

            return catalog.update_table(table_commit).await;
        }

        Err(Error::new(ErrorKind::DataInvalid, "Failed to commit!"))
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

    #[test]
    fn test_set_location() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let tx = tx
            .set_location(String::from("s3://bucket/prefix/new_table"))
            .unwrap();

        assert_eq!(
            vec![TableUpdate::SetLocation {
                location: String::from("s3://bucket/prefix/new_table")
            }],
            tx.updates
        )
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
