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

use roaring::RoaringTreemap;

use crate::expr::BoundPredicate;
use crate::io::FileIO;
use crate::scan::FileScanTaskDeleteFile;
use crate::spec::SchemaRef;
use crate::{Error, ErrorKind, Result};

pub(crate) struct DeleteFileManager {}

#[allow(unused_variables)]
impl DeleteFileManager {
    pub(crate) async fn load_deletes(
        delete_file_entries: Vec<FileScanTaskDeleteFile>,
        file_io: FileIO,
        concurrency_limit_data_files: usize,
    ) -> Result<DeleteFileManager> {
        // TODO

        if !delete_file_entries.is_empty() {
            Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Reading delete files is not yet supported",
            ))
        } else {
            Ok(DeleteFileManager {})
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
    ) -> Option<RoaringTreemap> {
        // TODO

        None
    }
}
