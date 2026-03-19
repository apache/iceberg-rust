use std::cmp::{Ordering, PartialOrd};
use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;
use uuid::Uuid;

use crate::spec::{DataFile, FormatVersion, ManifestContentType, ManifestEntry, ManifestEntryRef, ManifestFile, ManifestStatus, ManifestWriter, ManifestWriterBuilder, Operation};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};

#[derive(Hash, Eq, PartialEq)]
struct ManifestGroupKey{
    partition_spec_id: i32,
    content: ManifestContentType
}
impl ManifestGroupKey {
    fn new(partition_spec_id: i32, content: ManifestContentType) -> Self {
        Self{
            partition_spec_id, content
        }
    }
}
struct RewriteManifestAction {
    check_duplicate: bool,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    deleted_data_files: Vec<DataFile>,
}
impl RewriteManifestAction {
    pub(crate) fn new() -> RewriteManifestAction {
        Self {
            check_duplicate: true,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::new(),
            added_data_files: Vec::new(),
            deleted_data_files: Vec::new(),
        }
    }

    /// Set whether to check duplicate files.
    pub fn with_check_duplicate(mut self, v: bool) -> Self {
        self.check_duplicate = v;
        self
    }

    /// Add data files to the snapshot.
    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(data_files);
        self
    }

    /// Specify data files to be removed from the table in this overwrite.
    pub fn delete_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.deleted_data_files.extend(data_files);
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
}

impl TransactionAction for RewriteManifestAction {
    async fn commit(self: Arc<Self>, table: &Table) -> crate::Result<ActionCommit> {
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
            self.deleted_data_files.clone(),
        );

        snapshot_producer.validate_added_data_files()?;

        if self.check_duplicate {
            snapshot_producer.validate_duplicate_files().await?;
        }
        let snapshot_id = snapshot_producer.snapshot_id();
        snapshot_producer
            .commit(
                RewriteManifestOperation { snapshot_id },
                DefaultManifestProcess,
            )
            .await
    }
}

struct RewriteManifestOperation {
    snapshot_id: i64,
}


impl RewriteManifestOperation {
    async fn merge_manifest_groups(
        &self,
        manifest_group: HashMap<ManifestGroupKey, Vec<&ManifestFile>>,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>,Err> {
        let mut compacted_manifest_list = Vec::new();

        let table = snapshot_produce.table;
        for (group_key,mut group_files) in &manifest_group {
            let mut writer = self.get_manifest_writer(&table,&group_key);
            let mut data_file_entries: Vec<&ManifestEntryRef> = Vec::new();
            for manifest_file in group_files{
                let manifest_file = manifest_file.load_manifest(table.file_io()).await?;
                let mut entries: Vec<&ManifestEntryRef> =  manifest_file.entries().iter().filter(|m| m.status != ManifestStatus::Deleted)
                    .collect();
                data_file_entries.append(&mut entries);
            }
            data_file_entries.sort_by(|entry1, entry2| {
                Self::compare_data_file(entry1.data_file,entry2.data_file)
            });
            for entry in data_file_entries {
                // TODO : Make it rolling writer
               writer.add_existing_file(
                    entry.data_file,
                    entry.snapshot_id.unwrap(),
                    entry.sequence_number.unwrap(),
                    entry.file_sequence_number
                ).expect("Failed to add entry to manifest file");
            }
            compacted_manifest_list.push(
                writer.unwrap().write_manifest_file().await.unwrap()
            )
        }
        Ok(compacted_manifest_list)
    }
    fn compare_data_file(data_file1: DataFile, data_file2: DataFile) -> Ordering {
        let partition_fields1 = data_file1.partition().fields();
        let partition_fields2 = data_file2.partition().fields();
        for i in 0..partition_fields1.len() {
            let field1 = partition_fields1[i].clone().unwrap().as_primitive_literal().unwrap();
            let field2 = partition_fields2[i].clone().unwrap().as_primitive_literal().unwrap();
            if field1 < field2 {
                return Ordering::Less;
            }else if field1 > field2 {
               return  Ordering::Greater;
            }else {
                continue
            }
        }
        Ordering::Equal
    }

    fn get_manifest_writer(&self, table: &Table,group_key: &ManifestGroupKey) -> Result<ManifestWriter,Err> {
        let new_manifest_path = format!(
            "{}/metadata/{}-m-compact.avro",
            table.metadata().location(),
            Uuid::now_v7(),
        );
        let output_file = table.file_io().new_output(&new_manifest_path)?;
        let builder = ManifestWriterBuilder::new(
            output_file,
            Some(self.snapshot_id),
            None,
            table.metadata().current_schema().clone(),
            table.metadata().default_partition_spec().as_ref().clone(),
        );

        let mut writer = match table.metadata().format_version() {
            FormatVersion::V1 => builder.build_v1(),
            FormatVersion::V2 => match group_key.content {
                ManifestContentType::Data => builder.build_v2_data(),
                ManifestContentType::Deletes => builder.build_v2_deletes(),
            },
            FormatVersion::V3 => match group_key.content {
                ManifestContentType::Data => builder.build_v3_data(),
                ManifestContentType::Deletes => builder.build_v3_deletes(),
            },
        };
        Ok(writer)
    }
}

impl SnapshotProduceOperation for RewriteManifestOperation {
    fn operation(&self) -> Operation {
        Operation::Replace
    }

    fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProducer,
    ) -> impl Future<Output = crate::Result<Vec<ManifestEntry>>> + Send {
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> crate::Result<Vec<ManifestFile>> {
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.table.file_io(),
                &snapshot_produce.table.metadata_ref(),
            )
            .await?;
        // group before merging
        let groups: HashMap<ManifestGroupKey, Vec<&ManifestFile>> = manifest_list
            .entries()
            .into_iter()
            .into_group_map_by(|m| {
                ManifestGroupKey::new(
                    m.partition_spec_id,
                    m.content
                )
            });
        Ok(self.merge_manifest_groups(groups, snapshot_produce).await.unwrap())
    }
}
