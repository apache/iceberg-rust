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

use uuid::Uuid;

use crate::error::Result;
use crate::spec::{DataFile, ManifestEntry, ManifestFile, Operation};
use crate::transaction::Transaction;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceAction, SnapshotProduceOperation,
};
use crate::transaction::validate::SnapshotValidator;
use crate::writer::file_writer::ParquetWriter;
use crate::{Error, ErrorKind};

/// FastAppendAction is a transaction action for fast append data files to the table.
pub struct FastAppendAction {
    snapshot_produce_action: SnapshotProduceAction,
    check_duplicate: bool,
}

impl FastAppendAction {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        tx: Transaction,
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
            check_duplicate: true,
        })
    }

    /// Set whether to check duplicate files
    pub fn with_check_duplicate(mut self, v: bool) -> Self {
        self.check_duplicate = v;
        self
    }

    /// Add data files to the snapshot.
    pub fn add_data_files(
        &mut self,
        data_files: impl IntoIterator<Item = DataFile>,
    ) -> Result<&mut Self> {
        self.snapshot_produce_action.add_data_files(data_files)?;
        Ok(self)
    }

    /// Set snapshot summary properties.
    pub fn set_snapshot_properties(
        &mut self,
        snapshot_properties: HashMap<String, String>,
    ) -> Result<&mut Self> {
        self.snapshot_produce_action
            .set_snapshot_properties(snapshot_properties)?;
        Ok(self)
    }

    /// Adds existing parquet files
    ///
    /// Note: This API is not yet fully supported in version 0.5.x.  
    /// It is currently incomplete and should not be used in production.
    /// Specifically, schema compatibility checks and support for adding to partitioned tables
    /// have not yet been implemented.
    #[allow(dead_code)]
    async fn add_parquet_files(mut self, file_path: Vec<String>) -> Result<Transaction> {
        if !self
            .snapshot_produce_action
            .tx
            .current_table
            .metadata()
            .default_spec
            .is_unpartitioned()
        {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Appending to partitioned tables is not supported",
            ));
        }

        let table_metadata = self.snapshot_produce_action.tx.current_table.metadata();

        let data_files = ParquetWriter::parquet_files_to_data_files(
            self.snapshot_produce_action.tx.current_table.file_io(),
            file_path,
            table_metadata,
        )
        .await?;

        self.add_data_files(data_files)?;

        self.apply().await
    }

    /// Finished building the action and apply it to the transaction.
    pub async fn apply(self) -> Result<Transaction> {
        // Checks duplicate files
        if self.check_duplicate {
            let new_files: HashSet<&str> = self
                .snapshot_produce_action
                .added_data_files
                .iter()
                .map(|df| df.file_path.as_str())
                .collect();

            let mut referenced_files = Vec::new();
            let table = &self.snapshot_produce_action.tx.current_table;
            if let Some(current_snapshot) = table.metadata().current_snapshot() {
                let manifest_list = current_snapshot
                    .load_manifest_list(table.file_io(), &table.metadata_ref())
                    .await?;
                for manifest_list_entry in manifest_list.entries() {
                    let manifest = manifest_list_entry.load_manifest(table.file_io()).await?;
                    for entry in manifest.entries() {
                        let file_path = entry.file_path();
                        if new_files.contains(file_path) && entry.is_alive() {
                            referenced_files.push(file_path.to_string());
                        }
                    }
                }
            }

            if !referenced_files.is_empty() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot add files that are already referenced by table, files: {}",
                        referenced_files.join(", ")
                    ),
                ));
            }
        }

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
        _snapshot_produce: &SnapshotProduceAction,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProduceAction,
    ) -> Result<Vec<ManifestFile>> {
        let Some(snapshot) = snapshot_produce
            .tx
            .current_table
            .metadata()
            .current_snapshot()
        else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.tx.current_table.file_io(),
                &snapshot_produce.tx.current_table.metadata_ref(),
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

impl SnapshotValidator for FastAppendOperation {}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::scan::tests::TableTestFixture;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, MAIN_BRANCH, Struct,
    };
    use crate::transaction::Transaction;
    use crate::transaction::tests::make_v2_minimal_table;
    use crate::{TableRequirement, TableUpdate};

    #[tokio::test]
    async fn test_empty_data_append_action() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let mut action = tx.fast_append(None, vec![]).unwrap();
        action.add_data_files(vec![]).unwrap();
        assert!(action.apply().await.is_err());
    }

    #[tokio::test]
    async fn test_set_snapshot_properties() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let mut action = tx.fast_append(None, vec![]).unwrap();

        let mut snapshot_properties = HashMap::new();
        snapshot_properties.insert("key".to_string(), "val".to_string());
        action.set_snapshot_properties(snapshot_properties).unwrap();
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();
        action.add_data_files(vec![data_file]).unwrap();
        let tx = action.apply().await.unwrap();

        // Check customized properties is contained in snapshot summary properties.
        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &tx.updates[0] {
            snapshot
        } else {
            unreachable!()
        };
        assert_eq!(
            new_snapshot
                .summary()
                .additional_properties
                .get("key")
                .unwrap(),
            "val"
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
            .partition_spec_id(table.metadata().default_partition_spec_id())
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
            .partition_spec_id(table.metadata().default_partition_spec_id())
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
                    uuid: table.metadata().uuid()
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: MAIN_BRANCH.to_string(),
                    snapshot_id: table.metadata().current_snapshot_id
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

        // check manifest
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

    #[tokio::test]
    async fn test_add_duplicated_parquet_files_to_unpartitioned_table() {
        let mut fixture = TableTestFixture::new_unpartitioned();
        fixture.setup_unpartitioned_manifest_files().await;
        let tx = crate::transaction::Transaction::new(&fixture.table);

        let file_paths = vec![
            format!("{}/1.parquet", &fixture.table_location),
            format!("{}/3.parquet", &fixture.table_location),
        ];

        let fast_append_action = tx.fast_append(None, vec![]).unwrap();

        // Attempt to add duplicated Parquet files with fast append.
        assert!(
            fast_append_action
                .add_parquet_files(file_paths.clone())
                .await
                .is_err(),
            "file already in table"
        );

        let file_paths = vec![format!("{}/2.parquet", &fixture.table_location)];

        let tx = crate::transaction::Transaction::new(&fixture.table);
        let fast_append_action = tx.fast_append(None, vec![]).unwrap();

        // Attempt to add Parquet file which was deleted from table.
        assert!(
            fast_append_action
                .add_parquet_files(file_paths.clone())
                .await
                .is_ok(),
            "file not in table"
        );
    }
}
