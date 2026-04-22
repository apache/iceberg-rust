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

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use super::{
    MANIFEST_MERGE_ENABLED, MANIFEST_MERGE_ENABLED_DEFAULT, MANIFEST_MIN_MERGE_COUNT,
    MANIFEST_MIN_MERGE_COUNT_DEFAULT, MANIFEST_TARGET_SIZE_BYTES,
    MANIFEST_TARGET_SIZE_BYTES_DEFAULT,
};
use crate::error::Result;
use crate::spec::{DataFile, ManifestEntry, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, MergeManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind};

/// FastAppendAction is a transaction action for fast append data files to the table.
pub struct FastAppendAction {
    check_duplicate: bool,
    // below are properties used to create SnapshotProducer when commit
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_id: Option<i64>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    target_branch: Option<String>,
}

impl FastAppendAction {
    pub(crate) fn new() -> Self {
        Self {
            check_duplicate: true,
            commit_uuid: None,
            key_metadata: None,
            snapshot_id: None,
            snapshot_properties: HashMap::default(),
            added_data_files: vec![],
            added_delete_files: vec![],
            target_branch: None,
        }
    }

    /// Set whether to check duplicate files
    pub fn set_check_duplicate(mut self, v: bool) -> Self {
        self.check_duplicate = v;
        self
    }

    /// Set target branch for the snapshot.
    pub fn set_target_branch(mut self, target_branch: String) -> Self {
        self.target_branch = Some(target_branch);
        self
    }

    /// Add data files to the snapshot.
    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        for file in data_files {
            match file.content_type() {
                crate::spec::DataContentType::Data => self.added_data_files.push(file),
                crate::spec::DataContentType::PositionDeletes
                | crate::spec::DataContentType::EqualityDeletes => {
                    self.added_delete_files.push(file)
                }
            }
        }

        self
    }

    /// Set commit UUID for the snapshot.
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set key metadata for manifest files.
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    /// Set snapshot summary properties.
    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }

    /// Set snapshot id for the snapshot.
    pub fn set_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    /// Generate snapshot id ahead which is used by exactly once delivery.
    pub fn generate_snapshot_id(table: &Table) -> i64 {
        SnapshotProducer::generate_unique_snapshot_id(table)
    }
}

#[async_trait]
impl TransactionAction for FastAppendAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        if let Some(snapshot_id) = self.snapshot_id
            && table
                .metadata()
                .snapshots()
                .any(|s| s.snapshot_id() == snapshot_id)
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Snapshot id {snapshot_id} already exists"),
            ));
        }

        let mut snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_id,
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
            self.added_delete_files.clone(),
            vec![],
            vec![],
        );

        if let Some(target_branch) = &self.target_branch {
            snapshot_producer.set_target_branch(target_branch.clone());
        }

        // validate added files
        snapshot_producer.validate_added_data_files(&self.added_data_files)?;
        snapshot_producer.validate_added_data_files(&self.added_delete_files)?;

        // Checks duplicate files
        if self.check_duplicate {
            snapshot_producer.validate_data_file_changes().await?;
        }

        snapshot_producer
            .commit(FastAppendOperation, DefaultManifestProcess)
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
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &mut SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let Some(snapshot) = snapshot_produce
            .table
            .metadata()
            .snapshot_for_ref(snapshot_produce.target_branch())
        else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.table.file_io(),
                &snapshot_produce.table.metadata_ref(),
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

/// MergeAppendAction is a transaction action similar to fast append except that it will merge manifests
/// based on the target size.
pub struct MergeAppendAction {
    // snapshot_produce_action: SnapshotProducer<'_>,
    target_size_bytes: u32,
    min_count_to_merge: u32,
    merge_enabled: bool,

    check_duplicate: bool,
    // below are properties used to create SnapshotProducer when commit
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    snapshot_id: Option<i64>,
    target_branch: Option<String>,
}

impl MergeAppendAction {
    pub(crate) fn new() -> Self {
        Self {
            target_size_bytes: MANIFEST_TARGET_SIZE_BYTES_DEFAULT,
            min_count_to_merge: MANIFEST_MIN_MERGE_COUNT_DEFAULT,
            merge_enabled: MANIFEST_MERGE_ENABLED_DEFAULT,
            check_duplicate: true,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            added_data_files: vec![],
            added_delete_files: vec![],
            snapshot_id: None,
            target_branch: None,
        }
    }

    pub fn set_target_size_bytes(mut self, v: u32) -> Self {
        self.target_size_bytes = v;
        self
    }

    pub fn set_min_count_to_merge(mut self, v: u32) -> Self {
        self.min_count_to_merge = v;
        self
    }

    pub fn set_merge_enabled(mut self, v: bool) -> Self {
        self.merge_enabled = v;
        self
    }

    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        let target_size_bytes: u32 = snapshot_properties
            .get(MANIFEST_TARGET_SIZE_BYTES)
            .and_then(|s| s.parse().ok())
            .unwrap_or(MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
        let min_count_to_merge: u32 = snapshot_properties
            .get(MANIFEST_MIN_MERGE_COUNT)
            .and_then(|s| s.parse().ok())
            .unwrap_or(MANIFEST_MIN_MERGE_COUNT_DEFAULT);
        let merge_enabled = snapshot_properties
            .get(MANIFEST_MERGE_ENABLED)
            .and_then(|s| s.parse().ok())
            .unwrap_or(MANIFEST_MERGE_ENABLED_DEFAULT);

        self.snapshot_properties = snapshot_properties;
        self.target_size_bytes = target_size_bytes;
        self.min_count_to_merge = min_count_to_merge;
        self.merge_enabled = merge_enabled;

        self
    }

    pub fn set_target_branch(mut self, target_branch: String) -> Self {
        self.target_branch = Some(target_branch);
        self
    }

    /// Add data files to the snapshot.
    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(data_files);
        self
    }
}

#[async_trait]
impl TransactionAction for MergeAppendAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let mut snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_id,
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
            self.added_delete_files.clone(),
            vec![],
            vec![],
        );

        if let Some(target_branch) = &self.target_branch {
            snapshot_producer.set_target_branch(target_branch.clone());
        }

        // validate added files
        snapshot_producer.validate_added_data_files(&self.added_data_files)?;
        snapshot_producer.validate_added_data_files(&self.added_delete_files)?;

        // Checks duplicate files
        if self.check_duplicate {
            snapshot_producer.validate_data_file_changes().await?;
        }

        if self.merge_enabled {
            let process =
                MergeManifestProcess::new(self.target_size_bytes, self.min_count_to_merge);
            snapshot_producer.commit(FastAppendOperation, process).await
        } else {
            snapshot_producer
                .commit(FastAppendOperation, DefaultManifestProcess)
                .await
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, MAIN_BRANCH, Struct,
    };
    use crate::transaction::tests::make_v2_minimal_table;
    use crate::transaction::{Transaction, TransactionAction};
    use crate::{TableRequirement, TableUpdate};

    #[tokio::test]
    async fn test_empty_data_append_action() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![]);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_set_snapshot_properties() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let mut snapshot_properties = HashMap::new();
        snapshot_properties.insert("key".to_string(), "val".to_string());

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

        let action = tx
            .fast_append()
            .set_snapshot_properties(snapshot_properties)
            .add_data_files(vec![data_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        // Check customized properties is contained in snapshot summary properties.
        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
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
    async fn test_append_snapshot_properties() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let mut snapshot_properties = HashMap::new();
        snapshot_properties.insert("key".to_string(), "val".to_string());

        let action = tx
            .fast_append()
            .set_snapshot_properties(snapshot_properties);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        // Check customized properties is contained in snapshot summary properties.
        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
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
    async fn test_fast_append_file_with_incompatible_partition_value() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.fast_append();

        // check add data file with incompatible partition value
        let data_file = DataFileBuilder::default()
            .partition_spec_id(0)
            .content(DataContentType::Data)
            .file_path("test/3.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::string("test"))]))
            .build()
            .unwrap();

        let action = action.add_data_files(vec![data_file.clone()]);

        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_fast_append() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.fast_append();

        let data_file = DataFileBuilder::default()
            .partition_spec_id(0)
            .content(DataContentType::Data)
            .file_path("test/3.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = action.add_data_files(vec![data_file.clone()]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        // check updates and requirements
        assert!(
            matches!((&updates[0],&updates[1]), (TableUpdate::AddSnapshot { snapshot },TableUpdate::SetSnapshotRef { reference,ref_name }) if snapshot.snapshot_id() == reference.snapshot_id && ref_name == MAIN_BRANCH)
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
            requirements
        );

        // check manifest list
        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
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
    async fn test_fast_append_with_branch() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        // Test creating new branch
        let branch_name = "test_branch";

        let data_file = DataFileBuilder::default()
            .partition_spec_id(0)
            .content(DataContentType::Data)
            .file_path("test/3.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();
        let action = tx
            .fast_append()
            .set_target_branch(branch_name.to_string())
            .add_data_files(vec![data_file.clone()]);

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        // Check branch reference was created
        assert!(
            matches!(&updates[1], TableUpdate::SetSnapshotRef { ref_name, .. }
                if ref_name == branch_name)
        );

        // Test updating existing branch
        let action2 = tx
            .fast_append()
            .set_target_branch(branch_name.to_string())
            .add_data_files(vec![data_file.clone()]);
        let mut action_commit2 = Arc::new(action2).commit(&table).await.unwrap();
        let requirements = action_commit2.take_requirements();

        // Check requirements contain branch validation
        assert!(requirements.iter().any(
            |r| matches!(r, TableRequirement::RefSnapshotIdMatch { r#ref, .. }
                if r#ref == branch_name)
        ));
    }

    #[tokio::test]
    async fn test_branch_operations() {
        let table = make_v2_minimal_table();

        // Test creating new branch
        let branch_name = "test_branch";
        let tx = Transaction::new(&table);

        let data_file = DataFileBuilder::default()
            .partition_spec_id(0)
            .content(DataContentType::Data)
            .file_path("test/3.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = tx
            .fast_append()
            .set_target_branch(branch_name.to_string())
            .add_data_files(vec![data_file.clone()]);

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        // Verify branch was created
        assert!(matches!(
            &updates[1],
            TableUpdate::SetSnapshotRef { ref_name, .. } if ref_name == branch_name
        ));

        // Verify requirements
        assert!(requirements.iter().any(
            |r| matches!(r, TableRequirement::RefSnapshotIdMatch { r#ref, .. }
                if r#ref == branch_name)
        ));
    }

    /// Regression test: when a table already has a snapshot on `main` with
    /// cumulative totals set in its summary, the totals on the newly produced
    /// snapshot must be rolled forward (`previous_total + added`), not recomputed
    /// from zero.
    ///
    /// Before the fix, `SnapshotProducer::summary` looked up the previous
    /// snapshot via `snapshot_by_id(self.snapshot_id)` — i.e. the new snapshot
    /// that is not yet in `table_metadata` — so the lookup returned `None` and
    /// `update_totals` fell back to `previous_total = 0`. That caused the new
    /// snapshot to report only the files/records from the current commit,
    /// losing everything already in the table. This test fails on the buggy
    /// code path (`total-records = 10`, `total-data-files = 1`) and passes
    /// with the fix (`total-records = 110`, `total-data-files = 6`).
    #[tokio::test]
    async fn test_fast_append_rolls_previous_totals_forward() {
        use std::sync::Arc;

        use crate::spec::{
            ManifestListWriter, Operation, Snapshot, SnapshotReference, SnapshotRetention, Summary,
        };

        // Spec-stable summary keys — kept inline because the equivalent
        // constants in `spec::snapshot_summary` are private.
        const TOTAL_DATA_FILES_KEY: &str = "total-data-files";
        const TOTAL_RECORDS_KEY: &str = "total-records";

        // Build a V2 table whose current snapshot on `main` already reports
        // 5 data files / 100 records. The summary totals are what the rollup
        // must carry forward.
        let base = make_v2_minimal_table();

        const PARENT_SNAPSHOT_ID: i64 = 42;
        const PARENT_TOTAL_DATA_FILES: u64 = 5;
        const PARENT_TOTAL_RECORDS: u64 = 100;
        const PARENT_MANIFEST_LIST: &str = "memory:///snap-parent-manifest-list.avro";

        // FastAppend reads the parent snapshot's manifest list during commit
        // (to carry forward existing manifests), so we must stage a real file
        // at the path we reference on the parent snapshot. An empty V2
        // manifest list is sufficient — the commit just needs it to deserialize.
        let parent_manifest_list_writer = ManifestListWriter::v2(
            base.file_io().new_output(PARENT_MANIFEST_LIST).unwrap(),
            PARENT_SNAPSHOT_ID,
            None,
            1,
        );
        parent_manifest_list_writer.close().await.unwrap();

        let parent_summary = Summary {
            operation: Operation::Append,
            additional_properties: HashMap::from([
                (
                    TOTAL_DATA_FILES_KEY.to_string(),
                    PARENT_TOTAL_DATA_FILES.to_string(),
                ),
                (
                    TOTAL_RECORDS_KEY.to_string(),
                    PARENT_TOTAL_RECORDS.to_string(),
                ),
            ]),
        };

        let parent_snapshot = Snapshot::builder()
            .with_snapshot_id(PARENT_SNAPSHOT_ID)
            .with_timestamp_ms(base.metadata().last_updated_ms() + 1)
            .with_sequence_number(1)
            .with_schema_id(0)
            .with_manifest_list(PARENT_MANIFEST_LIST)
            .with_summary(parent_summary)
            .build();

        let metadata_with_parent = base
            .metadata()
            .clone()
            .into_builder(Some("s3://bucket/test/location/metadata/v1.json".into()))
            .add_snapshot(parent_snapshot)
            .unwrap()
            .set_ref(MAIN_BRANCH, SnapshotReference {
                snapshot_id: PARENT_SNAPSHOT_ID,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            })
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let table = base.with_metadata(Arc::new(metadata_with_parent));

        // Append 1 new data file with 10 records.
        const ADDED_RECORDS: u64 = 10;
        let data_file = DataFileBuilder::default()
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .content(DataContentType::Data)
            .file_path("test/new.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(ADDED_RECORDS)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![data_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_snapshot = match &updates[0] {
            TableUpdate::AddSnapshot { snapshot } => snapshot,
            _ => unreachable!("first update must be AddSnapshot"),
        };

        let props = &new_snapshot.summary().additional_properties;
        let total_records: u64 = props.get(TOTAL_RECORDS_KEY).unwrap().parse().unwrap();
        let total_data_files: u64 = props.get(TOTAL_DATA_FILES_KEY).unwrap().parse().unwrap();

        // The key assertion: totals are rolled forward from the parent, not
        // reset. On the buggy path these would be 10 and 1.
        assert_eq!(
            total_records,
            PARENT_TOTAL_RECORDS + ADDED_RECORDS,
            "total-records must roll forward (previous={PARENT_TOTAL_RECORDS} + added={ADDED_RECORDS})",
        );
        assert_eq!(
            total_data_files,
            PARENT_TOTAL_DATA_FILES + 1,
            "total-data-files must roll forward (previous={PARENT_TOTAL_DATA_FILES} + added=1)",
        );
    }
}
