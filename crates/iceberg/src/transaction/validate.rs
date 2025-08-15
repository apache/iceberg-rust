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

use std::collections::HashSet;

use crate::error::Result;
use crate::spec::{ManifestContentType, ManifestFile, Operation, SnapshotRef};
use crate::table::Table;

pub(crate) trait SnapshotValidator {
    fn validate(&self, _table: &Table, _snapshot: Option<&SnapshotRef>) -> Result<()> {
        // todo: add default implementation
        Ok(())
    }

    #[allow(dead_code)]
    async fn validation_history(
        &self,
        _base: &Table,
        _from_snapshot: Option<&SnapshotRef>,
        _to_snapshot: &SnapshotRef,
        _matching_operations: HashSet<Operation>,
        _manifest_content_type: ManifestContentType,
    ) -> Result<(Vec<ManifestFile>, HashSet<i64>)> {
        // todo: add default implementation
        Ok((vec![], HashSet::new()))
    }
}
