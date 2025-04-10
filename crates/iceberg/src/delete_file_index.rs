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
use crate::{Error, ErrorKind, Result};

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
    // TODO: do we need this?
    // pos_deletes_by_path: HashMap<String, Vec<Arc<DeleteFileContext>>>,

    // TODO: Deletion Vector support
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
    /// Returns a future that resolves to a Result<Vec<FileScanTaskDeleteFile>>
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
    /// Creates a new populated delete file index from a list of delete file contexts, which
    /// allows for fast lookup when determining which delete files apply to a given data file.
    ///
    /// 1. The partition information is extracted from each delete file's manifest entry.
    /// 2. If the partition is empty and the delete file is not a positional delete,
    ///    it is added to the `global_delees` vector
    /// 3. Otherwise, the delete file is added to one of two hash maps based on its content type.
    fn new(files: Vec<DeleteFileContext>) -> PopulatedDeleteFileIndex {
        let mut eq_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>> =
            HashMap::default();
        let mut pos_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>> =
            HashMap::default();

        let mut global_deletes: Vec<Arc<DeleteFileContext>> = vec![];

        files.into_iter().for_each(|ctx| {
            let arc_ctx = Arc::new(ctx);

            let partition = arc_ctx.manifest_entry.data_file().partition();

            // The spec states that "Equality delete files stored with an unpartitioned spec are applied as global deletes".
            if partition.fields().is_empty() {
                // TODO: confirm we're good to skip here if we encounter a pos del
                if arc_ctx.manifest_entry.content_type() != DataContentType::PositionDeletes {
                    global_deletes.push(arc_ctx);
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
            global_deletes,
            eq_deletes_by_partition,
            pos_deletes_by_partition,
        }
    }

    /// Determine all the delete files that apply to the provided `DataFile`.
    fn get_deletes_for_data_file(
        &self,
        data_file: &DataFile,
        seq_num: Option<i64>,
    ) -> Vec<FileScanTaskDeleteFile> {
        let mut results = vec![];

        self.global_deletes
            .iter()
            // filter that returns true if the provided delete file's sequence number is **greater than or equal to** `seq_num`
            .filter(|&delete| {
                seq_num
                    .map(|seq_num| delete.manifest_entry.sequence_number() >= Some(seq_num))
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
                })
                .for_each(|delete| results.push(delete.as_ref().into()));
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
                })
                .for_each(|delete| results.push(delete.as_ref().into()));
        }

        results
    }
}

/// Future for the `DeleteFileIndex::get_deletes_for_data_file` method
pub(crate) struct DeletesForDataFile<'a> {
    state: Arc<RwLock<DeleteFileIndexState>>,
    data_file: &'a DataFile,
    seq_num: Option<i64>,
}

impl Future for DeletesForDataFile<'_> {
    type Output = Result<Vec<FileScanTaskDeleteFile>>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.state.try_read() {
            Ok(guard) => match guard.deref() {
                DeleteFileIndexState::Populated(idx) => Poll::Ready(Ok(
                    idx.get_deletes_for_data_file(self.data_file, self.seq_num)
                )),
                _ => Poll::Pending,
            },
            Err(err) => Poll::Ready(Err(Error::new(ErrorKind::Unexpected, err.to_string()))),
        }
    }
}
