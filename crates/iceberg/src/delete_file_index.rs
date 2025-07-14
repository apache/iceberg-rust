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

use crate::scan::{DeleteFileContext, FileScanTaskDeleteFile};
use crate::spec::{DataContentType, DataFile, Struct};

/// Index of delete files
#[derive(Debug, Default)]
pub(crate) struct DeleteFileIndex {
    #[allow(dead_code)]
    global_deletes: Vec<Arc<DeleteFileContext>>,
    eq_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>>,
    pos_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>>,
    // TODO: Deletion Vector support
}

impl Extend<DeleteFileContext> for DeleteFileIndex {
    fn extend<T: IntoIterator<Item = DeleteFileContext>>(&mut self, iter: T) {
        // 1. The partition information is extracted from each delete file's manifest entry.
        // 2. If the partition is empty and the delete file is not a positional delete,
        //    it is added to the `global_deletes` vector
        // 3. Otherwise, the delete file is added to one of two hash maps based on its content type.
        for ctx in iter {
            let arc_ctx = Arc::new(ctx);

            let partition = arc_ctx.manifest_entry.data_file().partition();

            // The spec states that "Equality delete files stored with an unpartitioned spec
            // are applied as global deletes".
            if partition.fields().is_empty() {
                // TODO: confirm we're good to skip here if we encounter a pos del
                if arc_ctx.manifest_entry.content_type() != DataContentType::PositionDeletes {
                    self.global_deletes.push(arc_ctx);
                    continue;
                }
            }

            let destination_map = match arc_ctx.manifest_entry.content_type() {
                DataContentType::PositionDeletes => &mut self.pos_deletes_by_partition,
                DataContentType::EqualityDeletes => &mut self.eq_deletes_by_partition,
                _ => unreachable!(),
            };

            destination_map
                .entry(partition.clone())
                .and_modify(|entry| {
                    entry.push(arc_ctx.clone());
                })
                .or_insert(vec![arc_ctx.clone()]);
        }
    }
}

impl DeleteFileIndex {
    /// Determine all the delete files that apply to the provided `DataFile`.
    pub(crate) fn get_deletes_for_data_file(
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
