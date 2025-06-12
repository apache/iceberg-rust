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

/// TODO doc
pub mod action;
mod append;
mod snapshot;
mod sort_order;

use std::collections::HashMap;
use std::mem::discriminant;
use std::sync::Arc;
use std::time::Duration;

use backon::{BackoffBuilder, ExponentialBuilder, RetryableWithContext};
use uuid::Uuid;

use crate::error::Result;
use crate::table::Table;
use crate::transaction::action::{
    ApplyTransactionAction, BoxedTransactionAction, UpdateLocationAction, UpdatePropertiesAction,
    UpgradeFormatVersionAction,
};
use crate::transaction::append::FastAppendAction;
use crate::transaction::sort_order::ReplaceSortOrderAction;
use crate::{Catalog, Error, ErrorKind, TableCommit, TableRequirement, TableUpdate};

/// Table transaction.
#[derive(Clone)]
pub struct Transaction {
    base_table: Table,
    actions: Vec<BoxedTransactionAction>,
}

impl Transaction {
    /// Creates a new transaction.
    pub fn new(table: Table) -> Self {
        Self {
            base_table: table.clone(),
            actions: vec![],
        }
    }

    fn update_table_metadata(&self, table: &mut Table, updates: &[TableUpdate]) -> Result<()> {
        let mut metadata_builder = table.metadata().clone().into_builder(None);
        for update in updates {
            metadata_builder = update.clone().apply(metadata_builder)?;
        }

        table.with_metadata(Arc::new(metadata_builder.build()?.metadata));

        Ok(())
    }

    /// TODO documentation
    pub fn apply(
        &self,
        table: &mut Table,
        updates: Vec<TableUpdate>,
        requirements: Vec<TableRequirement>,
        existing_updates: &mut Vec<TableUpdate>,
        existing_requirements: &mut Vec<TableRequirement>,
    ) -> Result<()> {
        for requirement in &requirements {
            requirement.check(Some(table.metadata()))?;
        }

        self.update_table_metadata(table, &updates)?;

        existing_updates.extend(updates);

        // For the requirements, it does not make sense to add a requirement more than once
        // For example, you cannot assert that the current schema has two different IDs
        for new_requirement in requirements {
            if existing_requirements
                .iter()
                .map(discriminant)
                .all(|d| d != discriminant(&new_requirement))
            {
                existing_requirements.push(new_requirement);
            }
        }

        // # TODO
        // Support auto commit later.

        Ok(())
    }

    /// Sets table to a new version.
    pub fn upgrade_table_version(&self) -> UpgradeFormatVersionAction {
        UpgradeFormatVersionAction::new()
    }

    /// Update table's property.
    pub fn update_properties(&self) -> UpdatePropertiesAction {
        UpdatePropertiesAction::new()
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
            .base_table
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
        &mut self,
        commit_uuid: Option<Uuid>,
        key_metadata: Vec<u8>,
    ) -> Result<FastAppendAction> {
        let snapshot_id = self.generate_unique_snapshot_id();
        FastAppendAction::new(
            snapshot_id,
            commit_uuid.unwrap_or_else(Uuid::now_v7),
            key_metadata,
            HashMap::new(),
        )
    }

    /// Creates replace sort order action.
    pub fn replace_sort_order(self) -> ReplaceSortOrderAction {
        ReplaceSortOrderAction {
            sort_fields: vec![],
        }
    }

    /// Set the location of table
    pub fn update_location(&mut self) -> UpdateLocationAction {
        UpdateLocationAction::new()
    }

    /// Commit transaction.
    pub async fn commit(&mut self, catalog: Arc<&dyn Catalog>) -> Result<Table> {
        if self.actions.is_empty() {
            // nothing to commit
            return Ok(self.base_table.clone());
        }

        let tx = self.clone();
        (|mut tx: Transaction| async {
            let result = tx.do_commit(catalog.clone()).await;
            (tx, result)
        })
        .retry(
            ExponentialBuilder::new()
                // TODO retry strategy should be configurable
                .with_min_delay(Duration::from_millis(100))
                .with_max_delay(Duration::from_millis(60 * 1000))
                .with_total_delay(Some(Duration::from_millis(30 * 60 * 1000)))
                .with_max_times(4)
                .with_factor(2.0)
                .build(),
        )
        .context(tx)
        .sleep(tokio::time::sleep)
        // todo use a specific commit failure
        .when(|e| e.kind() == ErrorKind::DataInvalid)
        .await
        .1
    }

    async fn do_commit(&mut self, catalog: Arc<&dyn Catalog>) -> Result<Table> {
        let base_table_identifier = self.base_table.identifier().to_owned();

        let refreshed = catalog
            .load_table(&base_table_identifier.clone())
            .await
            .expect(format!("Failed to refresh table {}", base_table_identifier).as_str());

        let mut existing_updates: Vec<TableUpdate> = vec![];
        let mut existing_requirements: Vec<TableRequirement> = vec![];

        if self.base_table.metadata() != refreshed.metadata()
            || self.base_table.metadata_location() != refreshed.metadata_location()
        {
            // current base is stale, use refreshed as base and re-apply transaction actions
            self.base_table = refreshed.clone();
        }

        let mut current_table = self.base_table.clone();

        for action in self.actions.clone() {
            let mut action_commit = action.commit(&current_table).await?;
            // apply changes to current_table
            self.apply(
                &mut current_table,
                action_commit.take_updates(),
                action_commit.take_requirements(),
                &mut existing_updates,
                &mut existing_requirements,
            )?;
        }

        let table_commit = TableCommit::builder()
            .ident(base_table_identifier.clone())
            .updates(existing_updates)
            .requirements(existing_requirements)
            .build();

        catalog.update_table(table_commit).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::Arc;

    use crate::io::FileIOBuilder;
    use crate::spec::{FormatVersion, TableMetadata};
    use crate::table::Table;
    use crate::transaction::Transaction;
    use crate::transaction::action::TransactionAction;
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
        let tx = Transaction::new(table);
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
        let tx = Transaction::new(table);
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
        let tx = Transaction::new(table);
        let tx = tx.upgrade_table_version(FormatVersion::V1);

        assert!(tx.is_err(), "Downgrade table version should fail!");
    }

    #[test]
    fn test_set_table_property() {
        let table = make_v2_table();
        let tx = Transaction::new(table);
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
        let tx = Transaction::new(table);
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
    async fn test_set_location() {
        let table = make_v2_table();
        let mut tx = Transaction::new(table);
        let update_location_action = tx
            .update_location()
            .unwrap()
            .set_location(String::from("s3://bucket/prefix/new_table"));

        let _res = Arc::new(update_location_action).commit(&mut tx).await;

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
        let tx = Transaction::new(table);
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
