use std::collections::HashMap;
use std::sync::Arc;

use futures::channel::mpsc;
use futures::{StreamExt, TryStreamExt};
use tokio::sync::watch;

use crate::scan::FileScanTaskDeleteFile;
use crate::spec::{DataContentType, DataFile};
use crate::Result;

type DeleteFileIndexRef = Arc<Result<DeleteFileIndex>>;
pub(crate) type DeleteFileIndexRefReceiver = watch::Receiver<Option<DeleteFileIndexRef>>;

/// Index of delete files
#[derive(Debug)]
pub(crate) struct DeleteFileIndex {
    #[allow(dead_code)]
    global_deletes: Vec<Arc<FileScanTaskDeleteFile>>,
    #[allow(dead_code)]
    equality_deletes_by_partition: HashMap<i32, Vec<Arc<FileScanTaskDeleteFile>>>,
    #[allow(dead_code)]
    positional_deletes_by_partition: HashMap<i32, Vec<Arc<FileScanTaskDeleteFile>>>,
    positional_deletes_by_path: HashMap<String, Vec<Arc<FileScanTaskDeleteFile>>>,
}

impl DeleteFileIndex {
    pub(crate) fn from_receiver(
        receiver: mpsc::Receiver<Result<FileScanTaskDeleteFile>>,
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

    fn from_delete_files(files: Vec<FileScanTaskDeleteFile>) -> Self {
        let mut equality_deletes_by_partition: HashMap<i32, Vec<Arc<FileScanTaskDeleteFile>>> =
            HashMap::default();
        let mut positional_deletes_by_partition: HashMap<i32, Vec<Arc<FileScanTaskDeleteFile>>> =
            HashMap::default();
        let mut positional_deletes_by_path: HashMap<String, Vec<Arc<FileScanTaskDeleteFile>>> =
            HashMap::default();

        files.into_iter().for_each(|file| {
            let arc_file = Arc::new(file);
            match arc_file.file_type {
                DataContentType::PositionDeletes => {
                    // TODO: implement logic from ContentFileUtil.referencedDataFile
                    // see https://github.com/apache/iceberg/blob/cdf748e8e5537f13d861aa4c617a51f3e11dc97c/core/src/main/java/org/apache/iceberg/util/ContentFileUtil.java#L54
                    let referenced_data_file_path = "TODO".to_string();

                    positional_deletes_by_path
                        .entry(referenced_data_file_path)
                        .and_modify(|entry| {
                            entry.push(arc_file.clone());
                        })
                        .or_insert(vec![arc_file.clone()]);

                    positional_deletes_by_partition
                        .entry(arc_file.partition_spec_id)
                        .and_modify(|entry| {
                            entry.push(arc_file.clone());
                        })
                        .or_insert(vec![arc_file.clone()]);
                }
                DataContentType::EqualityDeletes => {
                    equality_deletes_by_partition
                        .entry(arc_file.partition_spec_id)
                        .and_modify(|entry| {
                            entry.push(arc_file.clone());
                        })
                        .or_insert(vec![arc_file.clone()]);
                }
                _ => unreachable!(),
            }
        });

        DeleteFileIndex {
            global_deletes: vec![],
            equality_deletes_by_partition,
            positional_deletes_by_partition,
            positional_deletes_by_path,
        }
    }

    /// Determine all the delete files that apply to the provided `DataFile`.
    pub(crate) fn get_deletes_for_data_file(
        &self,
        data_file: &DataFile,
    ) -> Vec<FileScanTaskDeleteFile> {
        let mut results = vec![];

        if let Some(positional_deletes) = self.positional_deletes_by_path.get(data_file.file_path())
        {
            results.extend(positional_deletes.iter().map(|i| i.as_ref().clone()))
        }

        // TODO: equality deletes

        results
    }
}
