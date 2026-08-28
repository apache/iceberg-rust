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

//! Where a scan's manifests come from.
//!
//! A [`PlanContext`](crate::scan::PlanContext) says how to project, filter and
//! evaluate a scan; a [`ManifestSource`] says which manifests it reads. Keeping
//! the two apart lets one planning path serve a single snapshot and, later, a
//! range of them.

use std::fmt::Debug;

use futures::future::BoxFuture;

use crate::Result;
use crate::io::object_cache::ObjectCache;
use crate::spec::{ManifestFile, SnapshotRef, TableMetadataRef};

/// Resolves the manifests a scan should read.
///
/// The returned future borrows both the source and the objects it reads
/// through, so a source is reusable: [`TableScan::plan_files`] takes `&self`
/// and may be called more than once.
///
/// [`TableScan::plan_files`]: crate::scan::TableScan::plan_files
pub(crate) trait ManifestSource: Debug + Send + Sync {
    fn manifest_files<'a>(
        &'a self,
        object_cache: &'a ObjectCache,
        table_metadata: &'a TableMetadataRef,
    ) -> BoxFuture<'a, Result<Vec<ManifestFile>>>;
}

/// No manifests at all, for a table that has no snapshots yet.
#[derive(Debug)]
pub(crate) struct EmptySource;

impl ManifestSource for EmptySource {
    fn manifest_files<'a>(
        &'a self,
        _object_cache: &'a ObjectCache,
        _table_metadata: &'a TableMetadataRef,
    ) -> BoxFuture<'a, Result<Vec<ManifestFile>>> {
        Box::pin(async { Ok(vec![]) })
    }
}

/// Every manifest listed by a single snapshot.
#[derive(Debug)]
pub(crate) struct SnapshotSource {
    snapshot: SnapshotRef,
}

impl SnapshotSource {
    pub(crate) fn new(snapshot: SnapshotRef) -> Self {
        Self { snapshot }
    }
}

impl ManifestSource for SnapshotSource {
    fn manifest_files<'a>(
        &'a self,
        object_cache: &'a ObjectCache,
        table_metadata: &'a TableMetadataRef,
    ) -> BoxFuture<'a, Result<Vec<ManifestFile>>> {
        Box::pin(async move {
            let manifest_list = object_cache
                .get_manifest_list(&self.snapshot, table_metadata)
                .await?;

            Ok(manifest_list.entries().to_vec())
        })
    }
}
