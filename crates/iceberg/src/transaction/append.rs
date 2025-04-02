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

use arrow_array::StringArray;
use futures::TryStreamExt;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{DataFile, ManifestEntry, ManifestFile, Operation};
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceAction, SnapshotProduceOperation,
};
use crate::transaction::Transaction;
use crate::writer::file_writer::ParquetWriter;
use crate::{Error, ErrorKind};

/// FastAppendAction is a transaction action for fast append data files to the table.
pub struct FastAppendAction<'a> {
    snapshot_produce_action: SnapshotProduceAction<'a>,
    check_duplicate: bool,
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

    /// Adds existing parquet files
    #[allow(dead_code)]
    async fn add_parquet_files(mut self, file_path: Vec<String>) -> Result<Transaction<'a>> {
        if !self
            .snapshot_produce_action
            .tx
            .table
            .metadata()
            .default_spec
            .is_unpartitioned()
        {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Appending to partitioned tables is not supported",
            ));
        }

        let table_metadata = self.snapshot_produce_action.tx.table.metadata();

        let data_files = ParquetWriter::parquet_files_to_data_files(
            self.snapshot_produce_action.tx.table.file_io(),
            file_path,
            table_metadata,
        )
        .await?;

        self.add_data_files(data_files)?;

        self.apply().await
    }

    /// Finished building the action and apply it to the transaction.
    pub async fn apply(self) -> Result<Transaction<'a>> {
        // Checks duplicate files
        if self.check_duplicate {
            let new_files: HashSet<&str> = self
                .snapshot_produce_action
                .added_data_files
                .iter()
                .map(|df| df.file_path.as_str())
                .collect();

            let mut manifest_stream = self
                .snapshot_produce_action
                .tx
                .table
                .inspect()
                .manifests()
                .scan()
                .await?;
            let mut referenced_files = Vec::new();

            while let Some(batch) = manifest_stream.try_next().await? {
                let file_path_array = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "Failed to downcast file_path column to StringArray",
                        )
                    })?;

                for i in 0..batch.num_rows() {
                    let file_path = file_path_array.value(i);
                    if new_files.contains(file_path) {
                        referenced_files.push(file_path.to_string());
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

#[cfg(test)]
mod tests {
    use crate::scan::tests::TableTestFixture;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, Struct, MAIN_BRANCH,
    };
    use crate::transaction::tests::make_v2_minimal_table;
    use crate::transaction::Transaction;
    use crate::{TableRequirement, TableUpdate};

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

    #[tokio::test]
    async fn test_add_existing_parquet_files_to_unpartitioned_table() {
        let mut fixture = TableTestFixture::new_unpartitioned();
        fixture.setup_unpartitioned_manifest_files().await;
        let tx = crate::transaction::Transaction::new(&fixture.table);

        let file_paths = vec![
            format!("{}/1.parquet", &fixture.table_location),
            format!("{}/2.parquet", &fixture.table_location),
            format!("{}/3.parquet", &fixture.table_location),
        ];

        let fast_append_action = tx.fast_append(None, vec![]).unwrap();

        // Attempt to add the existing Parquet files with fast append.
        let new_tx = fast_append_action
            .add_parquet_files(file_paths.clone())
            .await
            .expect("Adding existing Parquet files should succeed");

        let mut found_add_snapshot = false;
        let mut found_set_snapshot_ref = false;
        for update in new_tx.updates.iter() {
            match update {
                TableUpdate::AddSnapshot { .. } => {
                    found_add_snapshot = true;
                }
                TableUpdate::SetSnapshotRef {
                    ref_name,
                    reference,
                } => {
                    found_set_snapshot_ref = true;
                    assert_eq!(ref_name, MAIN_BRANCH);
                    assert!(reference.snapshot_id > 0);
                }
                _ => {}
            }
        }
        assert!(found_add_snapshot);
        assert!(found_set_snapshot_ref);

        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &new_tx.updates[0] {
            snapshot
        } else {
            panic!("Expected the first update to be an AddSnapshot update");
        };

        let manifest_list = new_snapshot
            .load_manifest_list(fixture.table.file_io(), fixture.table.metadata())
            .await
            .expect("Failed to load manifest list");

        assert_eq!(
            manifest_list.entries().len(),
            2,
            "Expected 2 manifest list entries, got {}",
            manifest_list.entries().len()
        );

        // Load the manifest from the manifest list
        let manifest = manifest_list.entries()[0]
            .load_manifest(fixture.table.file_io())
            .await
            .expect("Failed to load manifest");

        // Check that the manifest contains three entries.
        assert_eq!(manifest.entries().len(), 3);

        // Verify each file path appears in manifest.
        let manifest_paths: Vec<String> = manifest
            .entries()
            .iter()
            .map(|entry| entry.data_file().file_path.clone())
            .collect();
        for path in file_paths {
            assert!(manifest_paths.contains(&path));
        }
    }
}
