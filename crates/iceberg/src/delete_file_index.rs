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
use std::ops::Deref;
use std::sync::{Arc, RwLock};

use futures::StreamExt;
use futures::channel::mpsc::{Sender, channel};
use tokio::sync::Notify;

use crate::runtime::spawn;
use crate::scan::{DeleteFileContext, FileScanTaskDeleteFile};
use crate::spec::{DataContentType, DataFile, Struct};

/// Index of delete files
#[derive(Debug, Clone)]
pub(crate) struct DeleteFileIndex {
    state: Arc<RwLock<DeleteFileIndexState>>,
}

#[derive(Debug)]
enum DeleteFileIndexState {
    Populating(Arc<Notify>),
    Populated(PopulatedDeleteFileIndex),
}

#[derive(Debug)]
struct PopulatedDeleteFileIndex {
    #[allow(dead_code)]
    global_equality_deletes: Vec<Arc<DeleteFileContext>>,
    eq_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>>,
    pos_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>>,
    /// Deletion vectors indexed by the data file path they reference.
    /// Each deletion vector is stored in a Puffin file and references a specific data file.
    /// The key is the referenced data file path, and the value is the delete file context
    /// containing the Puffin file path and blob offset/size information.
    deletion_vectors_by_data_file: HashMap<String, Vec<Arc<DeleteFileContext>>>,
}

impl DeleteFileIndex {
    /// create a new `DeleteFileIndex` along with the sender that populates it with delete files
    pub(crate) fn new() -> (DeleteFileIndex, Sender<DeleteFileContext>) {
        // Channel buffer size of 10 provides reasonable backpressure while allowing some batching
        let (tx, rx) = channel(10);
        let notify = Arc::new(Notify::new());
        let state = Arc::new(RwLock::new(DeleteFileIndexState::Populating(
            notify.clone(),
        )));
        let delete_file_stream = rx.boxed();

        spawn({
            let state = state.clone();
            async move {
                let delete_files: Vec<DeleteFileContext> =
                    delete_file_stream.collect::<Vec<_>>().await;

                let populated_delete_file_index = PopulatedDeleteFileIndex::new(delete_files);

                {
                    let mut guard = state.write().unwrap();
                    *guard = DeleteFileIndexState::Populated(populated_delete_file_index);
                }
                notify.notify_waiters();
            }
        });

        (DeleteFileIndex { state }, tx)
    }

    /// Gets all the delete files that apply to the specified data file.
    pub(crate) async fn get_deletes_for_data_file(
        &self,
        data_file: &DataFile,
        seq_num: Option<i64>,
    ) -> Vec<FileScanTaskDeleteFile> {
        let notifier = {
            let guard = self.state.read().unwrap();
            match *guard {
                DeleteFileIndexState::Populating(ref notifier) => notifier.clone(),
                DeleteFileIndexState::Populated(ref index) => {
                    return index.get_deletes_for_data_file(data_file, seq_num);
                }
            }
        };

        notifier.notified().await;

        let guard = self.state.read().unwrap();
        match guard.deref() {
            DeleteFileIndexState::Populated(index) => {
                index.get_deletes_for_data_file(data_file, seq_num)
            }
            _ => unreachable!("Cannot be any other state than loaded"),
        }
    }
}

impl PopulatedDeleteFileIndex {
    /// Creates a new populated delete file index from a list of delete file contexts, which
    /// allows for fast lookup when determining which delete files apply to a given data file.
    ///
    /// The indexing strategy depends on the delete file type:
    /// 1. **Equality deletes**:
    ///    - Unpartitioned equality deletes are added to `global_equality_deletes`
    ///    - Partitioned equality deletes are indexed by partition in `eq_deletes_by_partition`
    /// 2. **Position deletes**:
    ///    - Regular position delete files are indexed by partition in `pos_deletes_by_partition`
    /// 3. **Deletion vectors** (position deletes with `referenced_data_file`, `content_offset`, and `content_size_in_bytes`):
    ///    - Indexed by referenced data file path in `deletion_vectors_by_data_file` for O(1) lookup
    fn new(files: Vec<DeleteFileContext>) -> PopulatedDeleteFileIndex {
        let mut eq_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>> =
            HashMap::default();
        let mut pos_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>> =
            HashMap::default();
        let mut deletion_vectors_by_data_file: HashMap<String, Vec<Arc<DeleteFileContext>>> =
            HashMap::default();

        let mut global_equality_deletes: Vec<Arc<DeleteFileContext>> = vec![];

        files.into_iter().for_each(|ctx| {
            let arc_ctx = Arc::new(ctx);

            let partition = arc_ctx.manifest_entry.data_file().partition();

            // The spec states that "Equality delete files stored with an unpartitioned spec are applied as global deletes".
            // Position deletes, however, are always partition-scoped - even unpartitioned ones only apply to
            // unpartitioned data files. They will be added to pos_deletes_by_partition with an empty partition key.
            if partition.fields().is_empty() {
                if arc_ctx.manifest_entry.content_type() != DataContentType::PositionDeletes {
                    global_equality_deletes.push(arc_ctx);
                    return;
                }
            }

            // Detect deletion vectors: position deletes with referenced_data_file and content offset/size
            // These are stored in Puffin files and reference a specific data file
            if arc_ctx.manifest_entry.content_type() == DataContentType::PositionDeletes {
                let data_file = arc_ctx.manifest_entry.data_file();
                if let (Some(referenced_file), Some(_offset), Some(_size)) = (
                    data_file.referenced_data_file(),
                    data_file.content_offset,
                    data_file.content_size_in_bytes,
                ) {
                    // This is a deletion vector - index by referenced data file path
                    deletion_vectors_by_data_file
                        .entry(referenced_file)
                        .and_modify(|entry| {
                            entry.push(arc_ctx.clone());
                        })
                        .or_insert(vec![arc_ctx.clone()]);
                    return;
                }
            }

            let destination_map = match arc_ctx.manifest_entry.content_type() {
                DataContentType::PositionDeletes => &mut pos_deletes_by_partition,
                DataContentType::EqualityDeletes => &mut eq_deletes_by_partition,
                _ => unreachable!(),
            };

            destination_map
                .entry(partition.clone())
                .and_modify(|entry| {
                    entry.push(arc_ctx.clone());
                })
                .or_insert(vec![arc_ctx.clone()]);
        });

        PopulatedDeleteFileIndex {
            global_equality_deletes,
            eq_deletes_by_partition,
            pos_deletes_by_partition,
            deletion_vectors_by_data_file,
        }
    }

    /// Determine all the delete files that apply to the provided `DataFile`.
    ///
    /// This method returns all delete files (equality deletes, position deletes, and deletion vectors)
    /// that should be applied when reading the specified data file. The returned delete files respect
    /// sequence number ordering and partition matching rules.
    fn get_deletes_for_data_file(
        &self,
        data_file: &DataFile,
        seq_num: Option<i64>,
    ) -> Vec<FileScanTaskDeleteFile> {
        let mut results = vec![];

        // Add global equality deletes (unpartitioned equality deletes apply to all data files)
        self.global_equality_deletes
            .iter()
            // filter that returns true if the provided delete file's sequence number is **greater than** `seq_num`
            .filter(|&delete| {
                seq_num
                    .map(|seq_num| delete.manifest_entry.sequence_number() > Some(seq_num))
                    .unwrap_or_else(|| true)
            })
            .for_each(|delete| results.push(delete.as_ref().into()));

        // Add partitioned equality deletes
        if let Some(deletes) = self.eq_deletes_by_partition.get(data_file.partition()) {
            deletes
                .iter()
                // filter that returns true if the provided delete file's sequence number is **greater than** `seq_num`
                .filter(|&delete| {
                    seq_num
                        .map(|seq_num| delete.manifest_entry.sequence_number() > Some(seq_num))
                        .unwrap_or_else(|| true)
                        && data_file.partition_spec_id == delete.partition_spec_id
                })
                .for_each(|delete| results.push(delete.as_ref().into()));
        }

        // Add position deletes from the same partition
        // Per the Iceberg spec:
        // "A position delete file is indexed by the `referenced_data_file` field of the manifest entry.
        // If the field is present, the delete file applies only to the data file with the same `file_path`.
        // If it's absent, the delete file must be scanned for each data file in the partition."
        if let Some(deletes) = self.pos_deletes_by_partition.get(data_file.partition()) {
            deletes
                .iter()
                // filter that returns true if the provided delete file's sequence number is **greater than or equal to** `seq_num`
                .filter(|&delete| {
                    let sequence_match = seq_num
                        .map(|seq_num| delete.manifest_entry.sequence_number() >= Some(seq_num))
                        .unwrap_or_else(|| true);

                    let spec_match = data_file.partition_spec_id == delete.partition_spec_id;

                    // Check referenced_data_file: if set, it must match the data file's path
                    let referenced_file_match =
                        match delete.manifest_entry.data_file().referenced_data_file() {
                            Some(referenced_path) => referenced_path == data_file.file_path,
                            None => true, // If not set, delete applies to all data files in partition
                        };

                    sequence_match && spec_match && referenced_file_match
                })
                .for_each(|delete| results.push(delete.as_ref().into()));
        }

        // Add deletion vectors that directly reference this data file
        // Deletion vectors are stored in Puffin files and always reference a specific data file
        if let Some(deletion_vectors) = self
            .deletion_vectors_by_data_file
            .get(data_file.file_path())
        {
            deletion_vectors
                .iter()
                // Deletion vectors follow the same sequence number rules as position deletes
                .filter(|&delete| {
                    seq_num
                        .map(|seq_num| delete.manifest_entry.sequence_number() >= Some(seq_num))
                        .unwrap_or_else(|| true)
                })
                .for_each(|delete| results.push(delete.as_ref().into()));
        }

        results
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, ManifestEntry, ManifestStatus,
        Struct,
    };

    #[test]
    fn test_delete_file_index_unpartitioned() {
        let deletes: Vec<ManifestEntry> = vec![
            build_added_manifest_entry(4, &build_unpartitioned_eq_delete()),
            build_added_manifest_entry(6, &build_unpartitioned_eq_delete()),
            build_added_manifest_entry(5, &build_unpartitioned_pos_delete()),
            build_added_manifest_entry(6, &build_unpartitioned_pos_delete()),
        ];

        let delete_file_paths: Vec<String> = deletes
            .iter()
            .map(|file| file.file_path().to_string())
            .collect();

        let delete_contexts: Vec<DeleteFileContext> = deletes
            .into_iter()
            .map(|entry| DeleteFileContext {
                manifest_entry: entry.into(),
                partition_spec_id: 0,
            })
            .collect();

        let delete_file_index = PopulatedDeleteFileIndex::new(delete_contexts);

        let data_file = build_unpartitioned_data_file();

        // All deletes apply to sequence 0
        let delete_files_to_apply_for_seq_0 =
            delete_file_index.get_deletes_for_data_file(&data_file, Some(0));
        assert_eq!(delete_files_to_apply_for_seq_0.len(), 4);

        // All deletes apply to sequence 3
        let delete_files_to_apply_for_seq_3 =
            delete_file_index.get_deletes_for_data_file(&data_file, Some(3));
        assert_eq!(delete_files_to_apply_for_seq_3.len(), 4);

        // Last 3 deletes apply to sequence 4
        let delete_files_to_apply_for_seq_4 =
            delete_file_index.get_deletes_for_data_file(&data_file, Some(4));
        let actual_paths_to_apply_for_seq_4: Vec<String> = delete_files_to_apply_for_seq_4
            .into_iter()
            .map(|file| file.file_path)
            .collect();

        assert_eq!(
            actual_paths_to_apply_for_seq_4,
            delete_file_paths[delete_file_paths.len() - 3..]
        );

        // Last 3 deletes apply to sequence 5
        let delete_files_to_apply_for_seq_5 =
            delete_file_index.get_deletes_for_data_file(&data_file, Some(5));
        let actual_paths_to_apply_for_seq_5: Vec<String> = delete_files_to_apply_for_seq_5
            .into_iter()
            .map(|file| file.file_path)
            .collect();
        assert_eq!(
            actual_paths_to_apply_for_seq_5,
            delete_file_paths[delete_file_paths.len() - 3..]
        );

        // Only the last position delete applies to sequence 6
        let delete_files_to_apply_for_seq_6 =
            delete_file_index.get_deletes_for_data_file(&data_file, Some(6));
        let actual_paths_to_apply_for_seq_6: Vec<String> = delete_files_to_apply_for_seq_6
            .into_iter()
            .map(|file| file.file_path)
            .collect();
        assert_eq!(
            actual_paths_to_apply_for_seq_6,
            delete_file_paths[delete_file_paths.len() - 1..]
        );

        // The 2 global equality deletes should match against any partitioned file
        let partitioned_file =
            build_partitioned_data_file(&Struct::from_iter([Some(Literal::long(100))]), 1);

        let delete_files_to_apply_for_partitioned_file =
            delete_file_index.get_deletes_for_data_file(&partitioned_file, Some(0));
        let actual_paths_to_apply_for_partitioned_file: Vec<String> =
            delete_files_to_apply_for_partitioned_file
                .into_iter()
                .map(|file| file.file_path)
                .collect();
        assert_eq!(
            actual_paths_to_apply_for_partitioned_file,
            delete_file_paths[..2]
        );
    }

    #[test]
    fn test_delete_file_index_partitioned() {
        let partition_one = Struct::from_iter([Some(Literal::long(100))]);
        let spec_id = 1;
        let deletes: Vec<ManifestEntry> = vec![
            build_added_manifest_entry(4, &build_partitioned_eq_delete(&partition_one, spec_id)),
            build_added_manifest_entry(6, &build_partitioned_eq_delete(&partition_one, spec_id)),
            build_added_manifest_entry(5, &build_partitioned_pos_delete(&partition_one, spec_id)),
            build_added_manifest_entry(6, &build_partitioned_pos_delete(&partition_one, spec_id)),
        ];

        let delete_file_paths: Vec<String> = deletes
            .iter()
            .map(|file| file.file_path().to_string())
            .collect();

        let delete_contexts: Vec<DeleteFileContext> = deletes
            .into_iter()
            .map(|entry| DeleteFileContext {
                manifest_entry: entry.into(),
                partition_spec_id: spec_id,
            })
            .collect();

        let delete_file_index = PopulatedDeleteFileIndex::new(delete_contexts);

        let partitioned_file =
            build_partitioned_data_file(&Struct::from_iter([Some(Literal::long(100))]), spec_id);

        // All deletes apply to sequence 0
        let delete_files_to_apply_for_seq_0 =
            delete_file_index.get_deletes_for_data_file(&partitioned_file, Some(0));
        assert_eq!(delete_files_to_apply_for_seq_0.len(), 4);

        // All deletes apply to sequence 3
        let delete_files_to_apply_for_seq_3 =
            delete_file_index.get_deletes_for_data_file(&partitioned_file, Some(3));
        assert_eq!(delete_files_to_apply_for_seq_3.len(), 4);

        // Last 3 deletes apply to sequence 4
        let delete_files_to_apply_for_seq_4 =
            delete_file_index.get_deletes_for_data_file(&partitioned_file, Some(4));
        let actual_paths_to_apply_for_seq_4: Vec<String> = delete_files_to_apply_for_seq_4
            .into_iter()
            .map(|file| file.file_path)
            .collect();

        assert_eq!(
            actual_paths_to_apply_for_seq_4,
            delete_file_paths[delete_file_paths.len() - 3..]
        );

        // Last 3 deletes apply to sequence 5
        let delete_files_to_apply_for_seq_5 =
            delete_file_index.get_deletes_for_data_file(&partitioned_file, Some(5));
        let actual_paths_to_apply_for_seq_5: Vec<String> = delete_files_to_apply_for_seq_5
            .into_iter()
            .map(|file| file.file_path)
            .collect();
        assert_eq!(
            actual_paths_to_apply_for_seq_5,
            delete_file_paths[delete_file_paths.len() - 3..]
        );

        // Only the last position delete applies to sequence 6
        let delete_files_to_apply_for_seq_6 =
            delete_file_index.get_deletes_for_data_file(&partitioned_file, Some(6));
        let actual_paths_to_apply_for_seq_6: Vec<String> = delete_files_to_apply_for_seq_6
            .into_iter()
            .map(|file| file.file_path)
            .collect();
        assert_eq!(
            actual_paths_to_apply_for_seq_6,
            delete_file_paths[delete_file_paths.len() - 1..]
        );

        // Data file with different partition tuples does not match any delete files
        let partitioned_second_file =
            build_partitioned_data_file(&Struct::from_iter([Some(Literal::long(200))]), 1);
        let delete_files_to_apply_for_different_partition =
            delete_file_index.get_deletes_for_data_file(&partitioned_second_file, Some(0));
        let actual_paths_to_apply_for_different_partition: Vec<String> =
            delete_files_to_apply_for_different_partition
                .into_iter()
                .map(|file| file.file_path)
                .collect();
        assert!(actual_paths_to_apply_for_different_partition.is_empty());

        // Data file with same tuple but different spec ID does not match any delete files
        let partitioned_different_spec = build_partitioned_data_file(&partition_one, 2);
        let delete_files_to_apply_for_different_spec =
            delete_file_index.get_deletes_for_data_file(&partitioned_different_spec, Some(0));
        let actual_paths_to_apply_for_different_spec: Vec<String> =
            delete_files_to_apply_for_different_spec
                .into_iter()
                .map(|file| file.file_path)
                .collect();
        assert!(actual_paths_to_apply_for_different_spec.is_empty());
    }

    fn build_unpartitioned_eq_delete() -> DataFile {
        build_partitioned_eq_delete(&Struct::empty(), 0)
    }

    fn build_partitioned_eq_delete(partition: &Struct, spec_id: i32) -> DataFile {
        DataFileBuilder::default()
            .file_path(format!("{}_equality_delete.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::EqualityDeletes)
            .equality_ids(Some(vec![1]))
            .record_count(1)
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    }

    fn build_unpartitioned_pos_delete() -> DataFile {
        build_partitioned_pos_delete(&Struct::empty(), 0)
    }

    fn build_partitioned_pos_delete(partition: &Struct, spec_id: i32) -> DataFile {
        DataFileBuilder::default()
            .file_path(format!("{}-pos-delete.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::PositionDeletes)
            .record_count(1)
            // No referenced_data_file - applies to all data files in partition
            .referenced_data_file(None)
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    }

    fn build_unpartitioned_data_file() -> DataFile {
        DataFileBuilder::default()
            .file_path(format!("{}-data.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::Data)
            .record_count(100)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    }

    fn build_partitioned_data_file(partition_value: &Struct, spec_id: i32) -> DataFile {
        DataFileBuilder::default()
            .file_path(format!("{}-data.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::Data)
            .record_count(100)
            .partition(partition_value.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    }

    fn build_added_manifest_entry(data_seq_number: i64, file: &DataFile) -> ManifestEntry {
        ManifestEntry::builder()
            .status(ManifestStatus::Added)
            .sequence_number(data_seq_number)
            .data_file(file.clone())
            .build()
    }

    #[test]
    fn test_referenced_data_file_matching() {
        // Test that position deletes with referenced_data_file set only apply to matching data files
        let partition = Struct::from_iter([Some(Literal::long(100))]);
        let spec_id = 1;

        let data_file_path_1 = "/table/data/file1.parquet";
        let data_file_path_2 = "/table/data/file2.parquet";

        // Create position delete files with specific referenced_data_file values
        let pos_delete_for_file1 = DataFileBuilder::default()
            .file_path(format!("{}-pos-delete.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::PositionDeletes)
            .record_count(1)
            .referenced_data_file(Some(data_file_path_1.to_string()))
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap();

        let pos_delete_for_file2 = DataFileBuilder::default()
            .file_path(format!("{}-pos-delete.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::PositionDeletes)
            .record_count(1)
            .referenced_data_file(Some(data_file_path_2.to_string()))
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap();

        // Create position delete file without referenced_data_file (applies to all files in partition)
        let pos_delete_global_in_partition = DataFileBuilder::default()
            .file_path(format!("{}-pos-delete.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::PositionDeletes)
            .record_count(1)
            .referenced_data_file(None) // No referenced_data_file means applies to all
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap();

        let deletes: Vec<ManifestEntry> = vec![
            build_added_manifest_entry(1, &pos_delete_for_file1),
            build_added_manifest_entry(1, &pos_delete_for_file2),
            build_added_manifest_entry(1, &pos_delete_global_in_partition),
        ];

        let delete_contexts: Vec<DeleteFileContext> = deletes
            .into_iter()
            .map(|entry| DeleteFileContext {
                manifest_entry: entry.into(),
                partition_spec_id: spec_id,
            })
            .collect();

        let delete_file_index = PopulatedDeleteFileIndex::new(delete_contexts);

        // Create data file 1
        let data_file_1 = DataFileBuilder::default()
            .file_path(data_file_path_1.to_string())
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::Data)
            .record_count(100)
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(1000)
            .build()
            .unwrap();

        // Create data file 2
        let data_file_2 = DataFileBuilder::default()
            .file_path(data_file_path_2.to_string())
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::Data)
            .record_count(100)
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(1000)
            .build()
            .unwrap();

        // Test data_file_1: should match pos_delete_for_file1 and pos_delete_global_in_partition
        let deletes_for_file1 = delete_file_index.get_deletes_for_data_file(&data_file_1, Some(0));
        assert_eq!(
            deletes_for_file1.len(),
            2,
            "Data file 1 should have 2 matching delete files"
        );

        let delete_paths_for_file1: Vec<String> =
            deletes_for_file1.into_iter().map(|d| d.file_path).collect();
        assert!(delete_paths_for_file1.contains(&pos_delete_for_file1.file_path));
        assert!(delete_paths_for_file1.contains(&pos_delete_global_in_partition.file_path));
        assert!(!delete_paths_for_file1.contains(&pos_delete_for_file2.file_path));

        // Test data_file_2: should match pos_delete_for_file2 and pos_delete_global_in_partition
        let deletes_for_file2 = delete_file_index.get_deletes_for_data_file(&data_file_2, Some(0));
        assert_eq!(
            deletes_for_file2.len(),
            2,
            "Data file 2 should have 2 matching delete files"
        );

        let delete_paths_for_file2: Vec<String> =
            deletes_for_file2.into_iter().map(|d| d.file_path).collect();
        assert!(delete_paths_for_file2.contains(&pos_delete_for_file2.file_path));
        assert!(delete_paths_for_file2.contains(&pos_delete_global_in_partition.file_path));
        assert!(!delete_paths_for_file2.contains(&pos_delete_for_file1.file_path));
    }

    #[test]
    fn test_referenced_data_file_no_match() {
        // Test that position delete with referenced_data_file doesn't match unrelated data files
        let partition = Struct::from_iter([Some(Literal::long(100))]);
        let spec_id = 1;

        // Create position delete for a specific file
        let pos_delete = DataFileBuilder::default()
            .file_path(format!("{}-pos-delete.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::PositionDeletes)
            .record_count(1)
            .referenced_data_file(Some("/table/data/specific-file.parquet".to_string()))
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap();

        let deletes: Vec<ManifestEntry> = vec![build_added_manifest_entry(1, &pos_delete)];

        let delete_contexts: Vec<DeleteFileContext> = deletes
            .into_iter()
            .map(|entry| DeleteFileContext {
                manifest_entry: entry.into(),
                partition_spec_id: spec_id,
            })
            .collect();

        let delete_file_index = PopulatedDeleteFileIndex::new(delete_contexts);

        // Create data file with different path
        let data_file = DataFileBuilder::default()
            .file_path("/table/data/different-file.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::Data)
            .record_count(100)
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(1000)
            .build()
            .unwrap();

        // The delete file should NOT match this data file
        let deletes = delete_file_index.get_deletes_for_data_file(&data_file, Some(0));
        assert_eq!(
            deletes.len(),
            0,
            "Position delete with different referenced_data_file should not match"
        );
    }

    #[test]
    fn test_referenced_data_file_null_matches_all() {
        // Test that position delete without referenced_data_file matches all files in partition
        let partition = Struct::from_iter([Some(Literal::long(100))]);
        let spec_id = 1;

        // Create position delete without referenced_data_file
        let pos_delete = DataFileBuilder::default()
            .file_path(format!("{}-pos-delete.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::PositionDeletes)
            .record_count(1)
            .referenced_data_file(None) // Applies to all files in partition
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap();

        let deletes: Vec<ManifestEntry> = vec![build_added_manifest_entry(1, &pos_delete)];

        let delete_contexts: Vec<DeleteFileContext> = deletes
            .into_iter()
            .map(|entry| DeleteFileContext {
                manifest_entry: entry.into(),
                partition_spec_id: spec_id,
            })
            .collect();

        let delete_file_index = PopulatedDeleteFileIndex::new(delete_contexts);

        // Create multiple data files with different paths
        let data_files = vec![
            DataFileBuilder::default()
                .file_path("/table/data/file1.parquet".to_string())
                .file_format(DataFileFormat::Parquet)
                .content(DataContentType::Data)
                .record_count(100)
                .partition(partition.clone())
                .partition_spec_id(spec_id)
                .file_size_in_bytes(1000)
                .build()
                .unwrap(),
            DataFileBuilder::default()
                .file_path("/table/data/file2.parquet".to_string())
                .file_format(DataFileFormat::Parquet)
                .content(DataContentType::Data)
                .record_count(100)
                .partition(partition.clone())
                .partition_spec_id(spec_id)
                .file_size_in_bytes(1000)
                .build()
                .unwrap(),
            DataFileBuilder::default()
                .file_path("/table/data/file3.parquet".to_string())
                .file_format(DataFileFormat::Parquet)
                .content(DataContentType::Data)
                .record_count(100)
                .partition(partition.clone())
                .partition_spec_id(spec_id)
                .file_size_in_bytes(1000)
                .build()
                .unwrap(),
        ];

        // All data files should match the delete file
        for data_file in data_files {
            let deletes = delete_file_index.get_deletes_for_data_file(&data_file, Some(0));
            assert_eq!(
                deletes.len(),
                1,
                "Position delete without referenced_data_file should match all files in partition"
            );
            assert_eq!(deletes[0].file_path, pos_delete.file_path);
        }
    }

    #[test]
    fn test_deletion_vector_indexing() {
        // Test that deletion vectors are properly indexed by referenced data file path
        let partition = Struct::from_iter([Some(Literal::long(100))]);
        let spec_id = 1;

        let data_file_path_1 = "/table/data/file1.parquet";
        let data_file_path_2 = "/table/data/file2.parquet";

        // Create deletion vector for file1 (position delete with referenced_data_file and content offset/size)
        let deletion_vector_1 = DataFileBuilder::default()
            .file_path("/table/metadata/deletion-vectors.puffin".to_string())
            .file_format(DataFileFormat::Puffin)
            .content(DataContentType::PositionDeletes)
            .record_count(5) // Cardinality of deletion vector
            .referenced_data_file(Some(data_file_path_1.to_string()))
            .content_offset(Some(4)) // Offset in Puffin file
            .content_size_in_bytes(Some(100)) // Size of deletion vector blob
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(1000)
            .build()
            .unwrap();

        // Create deletion vector for file2
        let deletion_vector_2 = DataFileBuilder::default()
            .file_path("/table/metadata/deletion-vectors.puffin".to_string())
            .file_format(DataFileFormat::Puffin)
            .content(DataContentType::PositionDeletes)
            .record_count(3)
            .referenced_data_file(Some(data_file_path_2.to_string()))
            .content_offset(Some(104))
            .content_size_in_bytes(Some(80))
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(1000)
            .build()
            .unwrap();

        let deletes: Vec<ManifestEntry> = vec![
            build_added_manifest_entry(1, &deletion_vector_1),
            build_added_manifest_entry(1, &deletion_vector_2),
        ];

        let delete_contexts: Vec<DeleteFileContext> = deletes
            .into_iter()
            .map(|entry| DeleteFileContext {
                manifest_entry: entry.into(),
                partition_spec_id: spec_id,
            })
            .collect();

        let delete_file_index = PopulatedDeleteFileIndex::new(delete_contexts);

        // Create data file 1
        let data_file_1 = DataFileBuilder::default()
            .file_path(data_file_path_1.to_string())
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::Data)
            .record_count(100)
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(1000)
            .build()
            .unwrap();

        // Create data file 2
        let data_file_2 = DataFileBuilder::default()
            .file_path(data_file_path_2.to_string())
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::Data)
            .record_count(100)
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(1000)
            .build()
            .unwrap();

        // Test data_file_1: should match only deletion_vector_1
        let deletes_for_file1 = delete_file_index.get_deletes_for_data_file(&data_file_1, Some(0));
        assert_eq!(
            deletes_for_file1.len(),
            1,
            "Data file 1 should have exactly 1 deletion vector"
        );
        assert_eq!(
            deletes_for_file1[0].file_path,
            "/table/metadata/deletion-vectors.puffin"
        );
        assert_eq!(
            deletes_for_file1[0].referenced_data_file,
            Some(data_file_path_1.to_string())
        );
        assert_eq!(deletes_for_file1[0].content_offset, Some(4));
        assert_eq!(deletes_for_file1[0].content_size_in_bytes, Some(100));

        // Test data_file_2: should match only deletion_vector_2
        let deletes_for_file2 = delete_file_index.get_deletes_for_data_file(&data_file_2, Some(0));
        assert_eq!(
            deletes_for_file2.len(),
            1,
            "Data file 2 should have exactly 1 deletion vector"
        );
        assert_eq!(
            deletes_for_file2[0].file_path,
            "/table/metadata/deletion-vectors.puffin"
        );
        assert_eq!(
            deletes_for_file2[0].referenced_data_file,
            Some(data_file_path_2.to_string())
        );
        assert_eq!(deletes_for_file2[0].content_offset, Some(104));
        assert_eq!(deletes_for_file2[0].content_size_in_bytes, Some(80));
    }

    #[test]
    fn test_deletion_vector_with_unrelated_data_file() {
        // Test that deletion vectors don't match unrelated data files
        let partition = Struct::from_iter([Some(Literal::long(100))]);
        let spec_id = 1;

        // Create deletion vector for a specific file
        let deletion_vector = DataFileBuilder::default()
            .file_path("/table/metadata/deletion-vectors.puffin".to_string())
            .file_format(DataFileFormat::Puffin)
            .content(DataContentType::PositionDeletes)
            .record_count(5)
            .referenced_data_file(Some("/table/data/specific-file.parquet".to_string()))
            .content_offset(Some(4))
            .content_size_in_bytes(Some(100))
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(1000)
            .build()
            .unwrap();

        let deletes: Vec<ManifestEntry> = vec![build_added_manifest_entry(1, &deletion_vector)];

        let delete_contexts: Vec<DeleteFileContext> = deletes
            .into_iter()
            .map(|entry| DeleteFileContext {
                manifest_entry: entry.into(),
                partition_spec_id: spec_id,
            })
            .collect();

        let delete_file_index = PopulatedDeleteFileIndex::new(delete_contexts);

        // Create data file with different path
        let unrelated_data_file = DataFileBuilder::default()
            .file_path("/table/data/different-file.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::Data)
            .record_count(100)
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(1000)
            .build()
            .unwrap();

        // The deletion vector should NOT match this data file
        let deletes = delete_file_index.get_deletes_for_data_file(&unrelated_data_file, Some(0));
        assert_eq!(
            deletes.len(),
            0,
            "Deletion vector should not match unrelated data file"
        );
    }

    #[test]
    fn test_deletion_vector_sequence_number_filtering() {
        // Test that deletion vectors respect sequence number filtering
        let partition = Struct::from_iter([Some(Literal::long(100))]);
        let spec_id = 1;
        let data_file_path = "/table/data/file1.parquet";

        // Create deletion vectors with different sequence numbers
        let dv_seq_5 = DataFileBuilder::default()
            .file_path("/table/metadata/dv1.puffin".to_string())
            .file_format(DataFileFormat::Puffin)
            .content(DataContentType::PositionDeletes)
            .record_count(5)
            .referenced_data_file(Some(data_file_path.to_string()))
            .content_offset(Some(4))
            .content_size_in_bytes(Some(100))
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(1000)
            .build()
            .unwrap();

        let dv_seq_10 = DataFileBuilder::default()
            .file_path("/table/metadata/dv2.puffin".to_string())
            .file_format(DataFileFormat::Puffin)
            .content(DataContentType::PositionDeletes)
            .record_count(3)
            .referenced_data_file(Some(data_file_path.to_string()))
            .content_offset(Some(104))
            .content_size_in_bytes(Some(80))
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(1000)
            .build()
            .unwrap();

        let deletes: Vec<ManifestEntry> = vec![
            build_added_manifest_entry(5, &dv_seq_5),
            build_added_manifest_entry(10, &dv_seq_10),
        ];

        let delete_contexts: Vec<DeleteFileContext> = deletes
            .into_iter()
            .map(|entry| DeleteFileContext {
                manifest_entry: entry.into(),
                partition_spec_id: spec_id,
            })
            .collect();

        let delete_file_index = PopulatedDeleteFileIndex::new(delete_contexts);

        let data_file = DataFileBuilder::default()
            .file_path(data_file_path.to_string())
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::Data)
            .record_count(100)
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(1000)
            .build()
            .unwrap();

        // Test with sequence number 0: both deletion vectors should apply
        let deletes_seq_0 = delete_file_index.get_deletes_for_data_file(&data_file, Some(0));
        assert_eq!(
            deletes_seq_0.len(),
            2,
            "Both deletion vectors should apply for seq 0"
        );

        // Test with sequence number 5: only dv_seq_10 should apply (seq >= 5)
        let deletes_seq_5 = delete_file_index.get_deletes_for_data_file(&data_file, Some(5));
        assert_eq!(
            deletes_seq_5.len(),
            2,
            "Both deletion vectors should apply for seq 5 (seq >= 5)"
        );

        // Test with sequence number 6: only dv_seq_10 should apply
        let deletes_seq_6 = delete_file_index.get_deletes_for_data_file(&data_file, Some(6));
        assert_eq!(
            deletes_seq_6.len(),
            1,
            "Only dv_seq_10 should apply for seq 6"
        );
        assert_eq!(deletes_seq_6[0].file_path, "/table/metadata/dv2.puffin");

        // Test with sequence number 11: no deletion vectors should apply
        let deletes_seq_11 = delete_file_index.get_deletes_for_data_file(&data_file, Some(11));
        assert_eq!(
            deletes_seq_11.len(),
            0,
            "No deletion vectors should apply for seq 11"
        );
    }

    #[test]
    fn test_multiple_deletion_vectors_same_data_file() {
        // Test that multiple deletion vectors can reference the same data file
        // This happens when there are multiple delete operations on the same file
        let partition = Struct::from_iter([Some(Literal::long(100))]);
        let spec_id = 1;
        let data_file_path = "/table/data/file1.parquet";

        // Create first deletion vector for file1
        let deletion_vector_1 = DataFileBuilder::default()
            .file_path("/table/metadata/dv1.puffin".to_string())
            .file_format(DataFileFormat::Puffin)
            .content(DataContentType::PositionDeletes)
            .record_count(5)
            .referenced_data_file(Some(data_file_path.to_string()))
            .content_offset(Some(4))
            .content_size_in_bytes(Some(100))
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(1000)
            .build()
            .unwrap();

        // Create second deletion vector for the SAME file (different Puffin file)
        let deletion_vector_2 = DataFileBuilder::default()
            .file_path("/table/metadata/dv2.puffin".to_string())
            .file_format(DataFileFormat::Puffin)
            .content(DataContentType::PositionDeletes)
            .record_count(3)
            .referenced_data_file(Some(data_file_path.to_string()))
            .content_offset(Some(4))
            .content_size_in_bytes(Some(80))
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(1000)
            .build()
            .unwrap();

        // Create third deletion vector for the SAME file (different blob in first Puffin file)
        let deletion_vector_3 = DataFileBuilder::default()
            .file_path("/table/metadata/dv1.puffin".to_string())
            .file_format(DataFileFormat::Puffin)
            .content(DataContentType::PositionDeletes)
            .record_count(2)
            .referenced_data_file(Some(data_file_path.to_string()))
            .content_offset(Some(104)) // Different offset in same Puffin file
            .content_size_in_bytes(Some(50))
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(1000)
            .build()
            .unwrap();

        let deletes: Vec<ManifestEntry> = vec![
            build_added_manifest_entry(1, &deletion_vector_1),
            build_added_manifest_entry(1, &deletion_vector_2),
            build_added_manifest_entry(1, &deletion_vector_3),
        ];

        let delete_contexts: Vec<DeleteFileContext> = deletes
            .into_iter()
            .map(|entry| DeleteFileContext {
                manifest_entry: entry.into(),
                partition_spec_id: spec_id,
            })
            .collect();

        let delete_file_index = PopulatedDeleteFileIndex::new(delete_contexts);

        let data_file = DataFileBuilder::default()
            .file_path(data_file_path.to_string())
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::Data)
            .record_count(100)
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(1000)
            .build()
            .unwrap();

        // All three deletion vectors should apply to this data file
        let deletes_for_file = delete_file_index.get_deletes_for_data_file(&data_file, Some(0));
        assert_eq!(
            deletes_for_file.len(),
            3,
            "Data file should have all 3 deletion vectors"
        );

        // Verify all three deletion vectors are present
        let dv_paths: Vec<String> = deletes_for_file
            .iter()
            .map(|d| d.file_path.clone())
            .collect();
        assert!(
            dv_paths.contains(&"/table/metadata/dv1.puffin".to_string()),
            "Should have deletion vector from dv1.puffin"
        );
        assert!(
            dv_paths.contains(&"/table/metadata/dv2.puffin".to_string()),
            "Should have deletion vector from dv2.puffin"
        );

        // Count how many times dv1.puffin appears (should be 2 - different blobs)
        let dv1_count = dv_paths
            .iter()
            .filter(|p| p.as_str() == "/table/metadata/dv1.puffin")
            .count();
        assert_eq!(
            dv1_count, 2,
            "Should have 2 deletion vectors from dv1.puffin (different blobs)"
        );

        // Verify different offsets for the two blobs in dv1.puffin
        let dv1_offsets: Vec<Option<i64>> = deletes_for_file
            .iter()
            .filter(|d| d.file_path == "/table/metadata/dv1.puffin")
            .map(|d| d.content_offset)
            .collect();
        assert!(dv1_offsets.contains(&Some(4)));
        assert!(dv1_offsets.contains(&Some(104)));
    }

    #[test]
    fn test_mixed_position_deletes_and_deletion_vectors() {
        // Test that both regular position deletes and deletion vectors work together
        let partition = Struct::from_iter([Some(Literal::long(100))]);
        let spec_id = 1;
        let data_file_path = "/table/data/file1.parquet";

        // Create regular position delete (no content_offset/size)
        let pos_delete = DataFileBuilder::default()
            .file_path("/table/deletes/pos-del-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::PositionDeletes)
            .record_count(10)
            .referenced_data_file(Some(data_file_path.to_string()))
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(500)
            .build()
            .unwrap();

        // Create deletion vector (with content_offset/size)
        let deletion_vector = DataFileBuilder::default()
            .file_path("/table/metadata/deletion-vectors.puffin".to_string())
            .file_format(DataFileFormat::Puffin)
            .content(DataContentType::PositionDeletes)
            .record_count(5)
            .referenced_data_file(Some(data_file_path.to_string()))
            .content_offset(Some(4))
            .content_size_in_bytes(Some(100))
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(1000)
            .build()
            .unwrap();

        let deletes: Vec<ManifestEntry> = vec![
            build_added_manifest_entry(1, &pos_delete),
            build_added_manifest_entry(1, &deletion_vector),
        ];

        let delete_contexts: Vec<DeleteFileContext> = deletes
            .into_iter()
            .map(|entry| DeleteFileContext {
                manifest_entry: entry.into(),
                partition_spec_id: spec_id,
            })
            .collect();

        let delete_file_index = PopulatedDeleteFileIndex::new(delete_contexts);

        let data_file = DataFileBuilder::default()
            .file_path(data_file_path.to_string())
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::Data)
            .record_count(100)
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(1000)
            .build()
            .unwrap();

        // Both the position delete and deletion vector should apply
        let deletes_for_file = delete_file_index.get_deletes_for_data_file(&data_file, Some(0));
        assert_eq!(
            deletes_for_file.len(),
            2,
            "Both position delete and deletion vector should apply"
        );

        // Verify we have both types
        let has_pos_delete = deletes_for_file
            .iter()
            .any(|d| d.file_path == "/table/deletes/pos-del-1.parquet");
        let has_deletion_vector = deletes_for_file.iter().any(|d| {
            d.file_path == "/table/metadata/deletion-vectors.puffin"
                && d.content_offset.is_some()
                && d.content_size_in_bytes.is_some()
        });

        assert!(has_pos_delete, "Should have regular position delete");
        assert!(has_deletion_vector, "Should have deletion vector");
    }
}
