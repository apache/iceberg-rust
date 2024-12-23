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
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};

use futures::channel::mpsc::{channel, Sender};
use futures::StreamExt;

use crate::runtime::spawn;
use crate::scan::{DeleteFileContext, FileScanTaskDeleteFile};
use crate::spec::{DataContentType, DataFile, Struct};

/// Index of delete files
#[derive(Clone, Debug)]
pub(crate) struct DeleteFileIndex {
    state: Arc<RwLock<DeleteFileIndexState>>,
}

#[derive(Debug)]
enum DeleteFileIndexState {
    Populating,
    Populated(PopulatedDeleteFileIndex),
}

#[derive(Debug)]
struct PopulatedDeleteFileIndex {
    #[allow(dead_code)]
    global_deletes: Vec<Arc<DeleteFileContext>>,
    eq_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>>,
    pos_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>>,
    pos_deletes_by_path: HashMap<String, Vec<Arc<DeleteFileContext>>>,
}

impl DeleteFileIndex {
    /// create a new `DeleteFileIndex` along with the sender that populates it with delete files
    pub(crate) fn new() -> (DeleteFileIndex, Sender<DeleteFileContext>) {
        // TODO: what should the channel limit be?
        let (tx, rx) = channel(10);
        let state = Arc::new(RwLock::new(DeleteFileIndexState::Populating));
        let delete_file_stream = rx.boxed();

        spawn({
            let state = state.clone();
            async move {
                let delete_files = delete_file_stream.collect::<Vec<_>>().await;

                let populated_delete_file_index = PopulatedDeleteFileIndex::new(delete_files);

                let mut guard = state.write().unwrap();
                *guard = DeleteFileIndexState::Populated(populated_delete_file_index);
            }
        });

        (DeleteFileIndex { state }, tx)
    }

    /// Gets all the delete files that apply to the specified data file.
    ///
    /// Returns a future that resolves to a Vec<FileScanTaskDeleteFile>
    pub(crate) fn get_deletes_for_data_file<'a>(
        &self,
        data_file: &'a DataFile,
        seq_num: Option<i64>,
    ) -> DeletesForDataFile<'a> {
        DeletesForDataFile {
            state: self.state.clone(),
            data_file,
            seq_num,
        }
    }
}

impl PopulatedDeleteFileIndex {
    fn new(files: Vec<DeleteFileContext>) -> PopulatedDeleteFileIndex {
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

        PopulatedDeleteFileIndex {
            global_deletes: vec![],
            eq_deletes_by_partition,
            pos_deletes_by_partition,
            pos_deletes_by_path,
        }
    }

    /// Determine all the delete files that apply to the provided `DataFile`.
    fn get_deletes_for_data_file(
        &self,
        data_file: &DataFile,
        seq_num: Option<i64>,
    ) -> Vec<FileScanTaskDeleteFile> {
        let mut deletes_queue = vec![];

        if let Some(deletes) = self.pos_deletes_by_path.get(data_file.file_path()) {
            deletes_queue.extend(deletes.iter());
        }

        if let Some(deletes) = self.pos_deletes_by_partition.get(data_file.partition()) {
            deletes_queue.extend(deletes.iter());
        }

        if let Some(deletes) = self.eq_deletes_by_partition.get(data_file.partition()) {
            deletes_queue.extend(deletes.iter());
        }

        deletes_queue
            .iter()
            .filter(|&delete| {
                seq_num
                    .map(|seq_num| delete.manifest_entry.sequence_number() > Some(seq_num))
                    .unwrap_or_else(|| true)
            })
            .map(|delete| FileScanTaskDeleteFile {
                file_path: delete.manifest_entry.file_path().to_string(),
                file_type: delete.manifest_entry.content_type(),
                partition_spec_id: delete.partition_spec_id,
            })
            .collect()
    }
}

/// Future for the `DeleteFileIndex::get_deletes_for_data_file` method
pub(crate) struct DeletesForDataFile<'a> {
    state: Arc<RwLock<DeleteFileIndexState>>,
    data_file: &'a DataFile,
    seq_num: Option<i64>,
}

impl Future for DeletesForDataFile<'_> {
    type Output = Vec<FileScanTaskDeleteFile>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Ok(guard) = self.state.try_read() else {
            return Poll::Pending;
        };

        match guard.deref() {
            DeleteFileIndexState::Populated(idx) => {
                Poll::Ready(idx.get_deletes_for_data_file(self.data_file, self.seq_num))
            }
            _ => Poll::Pending,
        }
    }
}
