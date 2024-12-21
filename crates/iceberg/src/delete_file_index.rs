use std::collections::HashMap;
use std::sync::Arc;

use futures::channel::mpsc;
use futures::{StreamExt, TryStreamExt};
use tokio::sync::watch;

use crate::scan::{DeleteFileContext, FileScanTaskDeleteFile};
use crate::spec::{DataContentType, DataFile, Struct};
use crate::Result;

type DeleteFileIndexRef = Arc<Result<DeleteFileIndex>>;
pub(crate) type DeleteFileIndexRefReceiver = watch::Receiver<Option<DeleteFileIndexRef>>;

/// Index of delete files
#[derive(Debug)]
pub(crate) struct DeleteFileIndex {
    #[allow(dead_code)]
    global_deletes: Vec<Arc<DeleteFileContext>>,
    eq_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>>,
    pos_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>>,
    pos_deletes_by_path: HashMap<String, Vec<Arc<DeleteFileContext>>>,
}

impl DeleteFileIndex {
    pub(crate) fn from_del_file_chan(
        receiver: mpsc::Receiver<Result<DeleteFileContext>>,
    ) -> watch::Receiver<Option<DeleteFileIndexRef>> {
        let (tx, rx) = watch::channel(None);

        let delete_file_stream = receiver.boxed();
        tokio::spawn(async move {
            let delete_files = delete_file_stream.try_collect::<Vec<_>>().await;
            let delete_file_index = delete_files.map(DeleteFileIndex::from_delete_files);
            let delete_file_index = Arc::new(delete_file_index);
            tx.send(Some(delete_file_index))
        });

        rx
    }

    fn from_delete_files(files: Vec<DeleteFileContext>) -> Self {
        let mut eq_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>> =
            HashMap::default();
        let mut pos_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>> =
            HashMap::default();
        let mut pos_deletes_by_path: HashMap<String, Vec<Arc<DeleteFileContext>>> =
            HashMap::default();

        files.into_iter().for_each(|del_file_ctx| {
            let arc_del_file_ctx = Arc::new(del_file_ctx);
            match arc_del_file_ctx.manifest_entry.content_type() {
                DataContentType::PositionDeletes => {
                    // TODO: implement logic from ContentFileUtil.referencedDataFile
                    // see https://github.com/apache/iceberg/blob/cdf748e8e5537f13d861aa4c617a51f3e11dc97c/core/src/main/java/org/apache/iceberg/util/ContentFileUtil.java#L54
                    let referenced_data_file_path = "TODO".to_string();

                    pos_deletes_by_path
                        .entry(referenced_data_file_path)
                        .and_modify(|entry| {
                            entry.push(arc_del_file_ctx.clone());
                        })
                        .or_insert(vec![arc_del_file_ctx.clone()]);

                    pos_deletes_by_partition
                        .entry(
                            arc_del_file_ctx
                                .manifest_entry
                                .data_file()
                                .partition()
                                .clone(),
                        )
                        .and_modify(|entry| {
                            entry.push(arc_del_file_ctx.clone());
                        })
                        .or_insert(vec![arc_del_file_ctx.clone()]);
                }
                DataContentType::EqualityDeletes => {
                    eq_deletes_by_partition
                        .entry(
                            arc_del_file_ctx
                                .manifest_entry
                                .data_file()
                                .partition()
                                .clone(),
                        )
                        .and_modify(|entry| {
                            entry.push(arc_del_file_ctx.clone());
                        })
                        .or_insert(vec![arc_del_file_ctx.clone()]);
                }
                _ => unreachable!(),
            }
        });

        DeleteFileIndex {
            global_deletes: vec![],
            eq_deletes_by_partition,
            pos_deletes_by_partition,
            pos_deletes_by_path,
        }
    }

    /// Determine all the delete files that apply to the provided `DataFile`.
    pub(crate) fn get_deletes_for_data_file(
        &self,
        data_file: &DataFile,
        seq_num: Option<i64>,
    ) -> Vec<FileScanTaskDeleteFile> {
        let mut results = vec![];

        if let Some(deletes) = self.pos_deletes_by_path.get(data_file.file_path()) {
            deletes
                .iter()
                .filter(|&delete| {
                    seq_num
                        .map(|seq_num| delete.manifest_entry.sequence_number() > Some(seq_num))
                        .unwrap_or_else(|| true)
                })
                .for_each(|delete| {
                    results.push(FileScanTaskDeleteFile {
                        file_path: delete.manifest_entry.file_path().to_string(),
                        file_type: delete.manifest_entry.content_type(),
                        partition_spec_id: delete.partition_spec_id,
                    })
                });
        }

        if let Some(deletes) = self.pos_deletes_by_partition.get(data_file.partition()) {
            deletes
                .iter()
                .filter(|&delete| {
                    seq_num
                        .map(|seq_num| delete.manifest_entry.sequence_number() > Some(seq_num))
                        .unwrap_or_else(|| true)
                })
                .for_each(|delete| {
                    results.push(FileScanTaskDeleteFile {
                        file_path: delete.manifest_entry.file_path().to_string(),
                        file_type: delete.manifest_entry.content_type(),
                        partition_spec_id: delete.partition_spec_id,
                    })
                });
        }

        if let Some(deletes) = self.eq_deletes_by_partition.get(data_file.partition()) {
            deletes
                .iter()
                .filter(|&delete| {
                    seq_num
                        .map(|seq_num| delete.manifest_entry.sequence_number() > Some(seq_num))
                        .unwrap_or_else(|| true)
                })
                .for_each(|delete| {
                    results.push(FileScanTaskDeleteFile {
                        file_path: delete.manifest_entry.file_path().to_string(),
                        file_type: delete.manifest_entry.content_type(),
                        partition_spec_id: delete.partition_spec_id,
                    })
                });
        }

        results
    }
}
