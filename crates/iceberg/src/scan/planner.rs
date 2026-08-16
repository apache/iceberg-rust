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

//! Optional remote scan-planning seam.
//!
//! A catalog that can plan scans server-side implements [`ScanPlanner`] and
//! injects it onto the [`Table`](crate::table::Table)s it loads. The core
//! [`Catalog`](crate::Catalog) trait is left untouched. [`TableScan::plan_files`]
//! (crate::scan::TableScan::plan_files) routes on [`ScanPlanningMode`].

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;

use crate::expr::BoundPredicate;
use crate::scan::FileScanTask;
use crate::spec::{NameMapping, SchemaRef, StructType, TableMetadataRef};
use crate::{Result, TableIdent};

/// How [`TableScan::plan_files`](crate::scan::TableScan::plan_files) chooses
/// between local manifest planning and a catalog-provided [`ScanPlanner`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ScanPlanningMode {
    /// Use remote planning when the table's planner advertises it, otherwise
    /// plan locally. This is the default.
    #[default]
    Auto,
    /// Always plan by reading manifests through the table's FileIO.
    Local,
    /// Require a planner that supports remote planning; error otherwise.
    Remote,
}

/// Input a [`TableScan`](crate::scan::TableScan) hands to a [`ScanPlanner`].
#[derive(Debug, Clone)]
pub struct ScanPlanningRequest {
    /// Identifier of the table being scanned.
    pub table_ident: TableIdent,
    /// Snapshot to scan. `None` lets the server use the current snapshot.
    pub snapshot_id: Option<i64>,
    /// Projected column names, or `None` to select all top-level columns.
    pub select: Option<Vec<String>>,
    /// Case sensitivity for filter and projection binding.
    pub case_sensitive: bool,
    /// Resolved field ids to stamp onto every produced [`FileScanTask`].
    pub project_field_ids: Vec<i32>,
    /// Table metadata at scan time, used to resolve partition specs.
    pub metadata: TableMetadataRef,
    /// Schema of the scanned snapshot.
    pub snapshot_schema: SchemaRef,
    /// The scan's bound filter, applied as the per-task row predicate.
    pub bound_filter: Option<BoundPredicate>,
    /// Optional name mapping from table properties.
    pub name_mapping: Option<Arc<NameMapping>>,
    /// Unified partition type when `_partition` is projected.
    pub unified_partition_type: Option<Arc<StructType>>,
}

/// Tasks produced by a [`ScanPlanner`].
#[derive(Debug, Clone)]
pub struct ScanPlanningResult {
    /// Planned file scan tasks.
    pub tasks: Vec<FileScanTask>,
}

/// Catalog capability for planning scans remotely.
///
/// REST catalogs implement this; other catalogs leave it unset and planning
/// stays local. [`ScanPlanner::supports_remote_scan_planning`] must only be
/// true when the implementation can decode the server's task payload into
/// [`FileScanTask`]s.
#[async_trait]
pub trait ScanPlanner: Debug + Send + Sync {
    /// Whether this planner can complete a remote plan end-to-end.
    ///
    /// Auto mode routes on this flag. Returning true without a working decoder
    /// would fail auto-mode scans instead of falling back to local planning.
    async fn supports_remote_scan_planning(&self) -> Result<bool>;

    /// Plan a scan server-side and return [`FileScanTask`]s.
    async fn plan_files(&self, request: ScanPlanningRequest) -> Result<ScanPlanningResult>;
}
