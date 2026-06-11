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
use crate::spec::{DataContentType, DataFile, DataFileFormat, Struct};

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
    // TODO: do we need this?
    // pos_deletes_by_path: HashMap<String, Vec<Arc<DeleteFileContext>>>,
    /// Deletion vectors keyed by the data file they apply to (the DV's
    /// `referenced_data_file`), mirroring Java `DeleteFileIndex.dvByPath`
    /// (`DeleteFileIndex.Builder.build` L500/L505-506: a POSITION_DELETES file with
    /// `ContentFileUtil.isDV` — format == PUFFIN — is indexed by `referencedDataFile()`).
    ///
    /// A valid table has AT MOST ONE DV per data file; Java's `add(dvByPath, dv)` (L528-535)
    /// raises `ValidationException` ("Can't index multiple DVs for %s") on a duplicate. This
    /// index's lookup signature is infallible, so duplicates are kept HERE and rejected
    /// fail-loud at the load door instead (`CachingDeleteFileLoader::load_deletes`).
    dv_by_path: HashMap<String, Vec<Arc<DeleteFileContext>>>,
}

/// Whether a delete file is a deletion vector. Java `ContentFileUtil.isDV` (L142-144):
/// `deleteFile.format() == FileFormat.PUFFIN`.
pub(crate) fn is_deletion_vector(data_file: &DataFile) -> bool {
    data_file.file_format() == DataFileFormat::Puffin
}

impl DeleteFileIndex {
    /// create a new `DeleteFileIndex` along with the sender that populates it with delete files
    pub(crate) fn new() -> (DeleteFileIndex, Sender<DeleteFileContext>) {
        // TODO: what should the channel limit be?
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
    /// 1. The partition information is extracted from each delete file's manifest entry.
    /// 2. If the partition is empty and the delete file is not a positional delete,
    ///    it is added to the `global_equality_deletes` vector
    /// 3. Otherwise, the delete file is added to one of two hash maps based on its content type.
    fn new(files: Vec<DeleteFileContext>) -> PopulatedDeleteFileIndex {
        let mut eq_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>> =
            HashMap::default();
        let mut pos_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>> =
            HashMap::default();
        let mut dv_by_path: HashMap<String, Vec<Arc<DeleteFileContext>>> = HashMap::default();

        let mut global_equality_deletes: Vec<Arc<DeleteFileContext>> = vec![];

        files.into_iter().for_each(|ctx| {
            let arc_ctx = Arc::new(ctx);

            // A deletion vector is FILE-scoped: it indexes by the data file it references, never
            // by partition (Java `DeleteFileIndex.Builder.build` L505-506 routes
            // POSITION_DELETES + `isDV` to `dvByPath` keyed by `referencedDataFile()`). A Puffin
            // position delete WITHOUT a referenced data file is invalid per the Puffin spec
            // (`referenced-data-file` is mandatory for `deletion-vector-v1`); it falls through to
            // the partition map so the loader's DV dispatch rejects it loudly by name instead of
            // it being silently dropped here.
            if arc_ctx.manifest_entry.content_type() == DataContentType::PositionDeletes
                && is_deletion_vector(arc_ctx.manifest_entry.data_file())
                && let Some(referenced_data_file) =
                    arc_ctx.manifest_entry.data_file().referenced_data_file()
            {
                dv_by_path
                    .entry(referenced_data_file)
                    .or_default()
                    .push(arc_ctx);
                return;
            }

            let partition = arc_ctx.manifest_entry.data_file().partition();

            // The spec states that "Equality delete files stored with an unpartitioned spec are applied as global deletes".
            if partition.fields().is_empty() {
                // TODO: confirm we're good to skip here if we encounter a pos del
                if arc_ctx.manifest_entry.content_type() != DataContentType::PositionDeletes {
                    global_equality_deletes.push(arc_ctx);
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
            dv_by_path,
        }
    }

    /// Determine all the delete files that apply to the provided `DataFile`.
    fn get_deletes_for_data_file(
        &self,
        data_file: &DataFile,
        seq_num: Option<i64>,
    ) -> Vec<FileScanTaskDeleteFile> {
        let mut results = vec![];

        self.global_equality_deletes
            .iter()
            // filter that returns true if the provided delete file's sequence number is **greater than** `seq_num`
            .filter(|&delete| {
                seq_num
                    .map(|seq_num| delete.manifest_entry.sequence_number() > Some(seq_num))
                    .unwrap_or_else(|| true)
            })
            .for_each(|delete| results.push(delete.as_ref().into()));

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

        // A data file with a DELETION VECTOR uses the DV INSTEAD of any parquet position
        // deletes: Java `DeleteFileIndex.forDataFile` (L156-167) returns
        // {global eq, partition eq, dv} when `findDV` hits and only consults the
        // position-delete maps when it does not. The DV lookup is by the data file's PATH
        // (Java `findDV` L202-216: `dvByPath.get(dataFile.location())`) and is NOT
        // sequence-filtered — Java instead VALIDATES `dv.dataSequenceNumber() >= seq`
        // (ValidationException, L209-213). That validation is DEFERRED here: this lookup is
        // infallible by signature, and the invalid state (a DV older than the data file at the
        // same path) cannot be produced by a correctly-written table — flagged as residue in
        // docs/parity/GAP_MATRIX.md. Duplicate DVs for one path (Java's other
        // ValidationException, L528-535) are all returned so the loader rejects them loudly.
        if let Some(dvs) = self.dv_by_path.get(data_file.file_path()) {
            dvs.iter()
                .for_each(|delete| results.push(delete.as_ref().into()));
            return results;
        }

        // TODO: the spec states that:
        //     "The data file's file_path is equal to the delete file's referenced_data_file if it is non-null".
        //     we're not yet doing that here. The referenced data file's name will also be present in the positional
        //     delete file's file path column.
        if let Some(deletes) = self.pos_deletes_by_partition.get(data_file.partition()) {
            deletes
                .iter()
                // filter that returns true if the provided delete file's sequence number is **greater than or equal to** `seq_num`
                .filter(|&delete| {
                    seq_num
                        .map(|seq_num| delete.manifest_entry.sequence_number() >= Some(seq_num))
                        .unwrap_or_else(|| true)
                        && data_file.partition_spec_id == delete.partition_spec_id
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
            .referenced_data_file(Some("/some-data-file.parquet".to_string()))
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    }

    /// Build a deletion vector `DataFile`: a POSITION_DELETES entry in PUFFIN format referencing
    /// `referenced_data_file`, with blob coordinates (the discriminator Java uses is the format
    /// — `ContentFileUtil.isDV`).
    fn build_partitioned_deletion_vector(
        referenced_data_file: &str,
        partition: &Struct,
        spec_id: i32,
    ) -> DataFile {
        DataFileBuilder::default()
            .file_path(format!("{}-deletes.puffin", Uuid::new_v4()))
            .file_format(DataFileFormat::Puffin)
            .content(DataContentType::PositionDeletes)
            .record_count(2)
            .referenced_data_file(Some(referenced_data_file.to_string()))
            .content_offset(Some(4))
            .content_size_in_bytes(Some(40))
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    }

    /// Risk pinned: Java `DeleteFileIndex.forDataFile` (L156-167) — a data file with a DV gets
    /// the DV INSTEAD of any parquet position deletes (the DV is the complete position-delete
    /// state for that file; also returning the parquet deletes would re-apply superseded
    /// deletes), while equality deletes still apply alongside it.
    #[test]
    fn test_dv_supersedes_position_deletes_and_keeps_equality_deletes() {
        let partition = Struct::from_iter([Some(Literal::long(100))]);
        let spec_id = 1;
        let data_file = build_partitioned_data_file(&partition, spec_id);

        let deletes: Vec<ManifestEntry> = vec![
            build_added_manifest_entry(2, &build_partitioned_eq_delete(&partition, spec_id)),
            build_added_manifest_entry(2, &build_partitioned_pos_delete(&partition, spec_id)),
            build_added_manifest_entry(
                2,
                &build_partitioned_deletion_vector(data_file.file_path(), &partition, spec_id),
            ),
        ];
        let eq_delete_path = deletes[0].file_path().to_string();
        let dv_path = deletes[2].file_path().to_string();

        let delete_contexts: Vec<DeleteFileContext> = deletes
            .into_iter()
            .map(|entry| DeleteFileContext {
                manifest_entry: entry.into(),
                partition_spec_id: spec_id,
            })
            .collect();
        let index = PopulatedDeleteFileIndex::new(delete_contexts);

        let results = index.get_deletes_for_data_file(&data_file, Some(1));
        let result_paths: Vec<&str> = results.iter().map(|f| f.file_path.as_str()).collect();

        assert_eq!(
            result_paths,
            vec![eq_delete_path.as_str(), dv_path.as_str()],
            "expected the equality delete + the DV, and NO parquet position delete"
        );
        assert_eq!(
            results[1].file_format,
            DataFileFormat::Puffin,
            "the DV entry must carry the Puffin format discriminator to the loader"
        );
        assert_eq!(
            results[1].referenced_data_file.as_deref(),
            Some(data_file.file_path()),
            "the DV entry must carry the referenced data file for keying"
        );
    }

    /// Risk pinned: a DV is FILE-scoped (keyed by `referenced_data_file`, Java `findDV`
    /// L202-216) — a SIBLING data file in the SAME partition must NOT receive it, and still
    /// receives the partition-scoped parquet position deletes.
    #[test]
    fn test_dv_does_not_apply_to_sibling_file_in_same_partition() {
        let partition = Struct::from_iter([Some(Literal::long(100))]);
        let spec_id = 1;
        let data_file_with_dv = build_partitioned_data_file(&partition, spec_id);
        let sibling_data_file = build_partitioned_data_file(&partition, spec_id);

        let deletes: Vec<ManifestEntry> = vec![
            build_added_manifest_entry(2, &build_partitioned_pos_delete(&partition, spec_id)),
            build_added_manifest_entry(
                2,
                &build_partitioned_deletion_vector(
                    data_file_with_dv.file_path(),
                    &partition,
                    spec_id,
                ),
            ),
        ];
        let pos_delete_path = deletes[0].file_path().to_string();
        let dv_path = deletes[1].file_path().to_string();

        let delete_contexts: Vec<DeleteFileContext> = deletes
            .into_iter()
            .map(|entry| DeleteFileContext {
                manifest_entry: entry.into(),
                partition_spec_id: spec_id,
            })
            .collect();
        let index = PopulatedDeleteFileIndex::new(delete_contexts);

        let sibling_results = index.get_deletes_for_data_file(&sibling_data_file, Some(1));
        let sibling_paths: Vec<&str> = sibling_results
            .iter()
            .map(|f| f.file_path.as_str())
            .collect();
        assert_eq!(
            sibling_paths,
            vec![pos_delete_path.as_str()],
            "the sibling file gets the partition-scoped parquet position delete, never the DV"
        );

        let dv_file_results = index.get_deletes_for_data_file(&data_file_with_dv, Some(1));
        let dv_file_paths: Vec<&str> = dv_file_results
            .iter()
            .map(|f| f.file_path.as_str())
            .collect();
        assert_eq!(dv_file_paths, vec![dv_path.as_str()]);
    }

    /// Risk pinned (documented deferral): Java VALIDATES `dv.dataSequenceNumber() >= seq`
    /// (ValidationException, `findDV` L209-213) instead of seq-FILTERING the DV. This index is
    /// infallible by signature, so the DV is returned regardless of sequence numbers — this test
    /// pins that the DV is NOT silently dropped by a sequence filter (dropping it would
    /// resurrect deleted rows on valid tables where dv_seq >= data_seq always holds; the
    /// fail-loud half of Java's contract is the deferred residue, see GAP_MATRIX).
    #[test]
    fn test_dv_is_not_sequence_filtered() {
        let partition = Struct::from_iter([Some(Literal::long(100))]);
        let spec_id = 1;
        let data_file = build_partitioned_data_file(&partition, spec_id);

        let dv_entry = build_added_manifest_entry(
            5,
            &build_partitioned_deletion_vector(data_file.file_path(), &partition, spec_id),
        );
        let index = PopulatedDeleteFileIndex::new(vec![DeleteFileContext {
            manifest_entry: dv_entry.into(),
            partition_spec_id: spec_id,
        }]);

        // Same seq (the row-delta case: DV committed in the same snapshot family) and a HIGHER
        // data seq (the invalid state Java rejects loudly): the DV is returned in both.
        assert_eq!(
            index.get_deletes_for_data_file(&data_file, Some(5)).len(),
            1
        );
        assert_eq!(
            index.get_deletes_for_data_file(&data_file, Some(9)).len(),
            1
        );
    }

    /// Risk pinned: a PUFFIN position delete WITHOUT `referenced_data_file` is invalid (the
    /// Puffin spec makes the property mandatory for DVs). It must NOT be silently dropped by the
    /// index — it falls through to the partition map so the loader rejects it by name.
    #[test]
    fn test_puffin_delete_without_referenced_file_reaches_loader_for_rejection() {
        let partition = Struct::from_iter([Some(Literal::long(100))]);
        let spec_id = 1;
        let data_file = build_partitioned_data_file(&partition, spec_id);

        let invalid_dv = DataFileBuilder::default()
            .file_path("orphan-deletes.puffin".to_string())
            .file_format(DataFileFormat::Puffin)
            .content(DataContentType::PositionDeletes)
            .record_count(2)
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap();

        let index = PopulatedDeleteFileIndex::new(vec![DeleteFileContext {
            manifest_entry: build_added_manifest_entry(2, &invalid_dv).into(),
            partition_spec_id: spec_id,
        }]);

        let results = index.get_deletes_for_data_file(&data_file, Some(1));
        assert_eq!(results.len(), 1, "the invalid DV must reach the loader");
        assert_eq!(results[0].file_format, DataFileFormat::Puffin);
        assert_eq!(results[0].referenced_data_file, None);
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
}
