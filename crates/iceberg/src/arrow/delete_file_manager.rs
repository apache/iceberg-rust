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

use crate::delete_vector::DeleteVector;
use crate::expr::BoundPredicate;
use crate::io::FileIO;
use crate::scan::{ArrowRecordBatchStream, FileScanTaskDeleteFile};
use crate::spec::SchemaRef;
use crate::{Error, ErrorKind, Result};

#[allow(unused)]
pub trait DeleteFileManager {
    /// Read the delete file referred to in the task
    ///
    /// Returns the raw contents of the delete file as a RecordBatch stream
    fn read_delete_file(task: &FileScanTaskDeleteFile) -> Result<ArrowRecordBatchStream>;
}

#[allow(unused)]
#[derive(Clone, Debug)]
pub(crate) struct CachingDeleteFileManager {
    file_io: FileIO,
    concurrency_limit_data_files: usize,
}

impl DeleteFileManager for CachingDeleteFileManager {
    fn read_delete_file(_task: &FileScanTaskDeleteFile) -> Result<ArrowRecordBatchStream> {
        // TODO, implementation in https://github.com/apache/iceberg-rust/pull/982

        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Reading delete files is not yet supported",
        ))
    }
}

#[allow(unused_variables)]
impl CachingDeleteFileManager {
    pub fn new(file_io: FileIO, concurrency_limit_data_files: usize) -> CachingDeleteFileManager {
        Self {
            file_io,
            concurrency_limit_data_files,
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn load_deletes(
        &self,
        delete_file_entries: Vec<FileScanTaskDeleteFile>,
    ) -> Result<()> {
        // TODO

        if !delete_file_entries.is_empty() {
            Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Reading delete files is not yet supported",
            ))
        } else {
            Ok(())
        }
    }

    pub(crate) fn build_delete_predicate(
        &self,
        snapshot_schema: SchemaRef,
    ) -> Result<Option<BoundPredicate>> {
        // TODO

        Ok(None)
    }

    pub(crate) fn get_positional_delete_indexes_for_data_file(
        &self,
        data_file_path: &str,
    ) -> Option<DeleteVector> {
        // TODO

        None
    }
}
