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

use super::ManifestList;
use crate::error::Result;
use crate::io::FileIO;
use crate::spec::{SnapshotRef, TableMetadataRef};

/// A manifest list reader that encapsulates the logic for loading and parsing a [`ManifestList`]
/// from a snapshot.
pub struct ManifestListReader {
    snapshot: SnapshotRef,
    file_io: FileIO,
    table_metadata: TableMetadataRef,
}

impl ManifestListReader {
    pub(crate) fn new(
        snapshot: SnapshotRef,
        file_io: FileIO,
        table_metadata: TableMetadataRef,
    ) -> Self {
        Self {
            snapshot,
            file_io,
            table_metadata,
        }
    }

    /// Loads and returns the [`ManifestList`] for this snapshot.
    pub async fn load(&self) -> Result<ManifestList> {
        let manifest_list_content = self
            .file_io
            .new_input(self.snapshot.manifest_list())?
            .read()
            .await?;
        ManifestList::parse_with_version(
            &manifest_list_content,
            self.table_metadata.format_version(),
        )
    }
}
