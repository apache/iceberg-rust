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

//! Server-side scan planning abstraction.
//!
//! Iceberg's REST protocol allows a catalog server to perform scan planning
//! (file pruning, delete-file resolution) on the client's behalf and return a
//! ready-made list of [`FileScanTask`](crate::scan::FileScanTask)s. The
//! [`ScanPlanner`] trait is the seam through which a catalog injects that
//! capability into a [`Table`](crate::table::Table): when a planner is present,
//! [`TableScan::plan_files`](crate::scan::TableScan::plan_files) delegates to it
//! instead of reading manifests locally.
//!
//! This is the narrow, purpose-built capability trait used by the "Variant B"
//! injection design — only catalogs that actually support remote planning
//! implement it, and the core [`Catalog`](crate::Catalog) trait is left
//! untouched.

use std::fmt::Debug;

use async_trait::async_trait;

use crate::expr::Predicate;
use crate::io::FileIO;
use crate::scan::FileScanTaskStream;
use crate::spec::{SchemaRef, TableMetadataRef};
use crate::{Error, ErrorKind, Result, TableIdent};

/// The result of a server-side scan plan: the file scan tasks plus an optional
/// plan-scoped [`FileIO`] built from the credentials the server vended for this
/// scan. When present, the scan engine reads data files through this `FileIO`
/// instead of the table's default one.
pub struct ServerScanPlan {
    /// The planned file scan tasks.
    pub tasks: FileScanTaskStream,
    /// A `FileIO` carrying the plan's vended storage credentials, if any.
    pub file_io: Option<FileIO>,
}

/// A neutral, catalog-agnostic description of the scan to be planned remotely.
///
/// This is the contract between the core scan engine (which assembles it from a
/// [`TableScan`](crate::scan::TableScan)) and a catalog-specific
/// [`ScanPlanner`] implementation (which maps it onto its wire protocol).
#[derive(Debug, Clone)]
pub struct ScanPlanRequest {
    /// Identifier of the table being scanned.
    pub table_ident: TableIdent,

    /// The snapshot to scan. Mutually exclusive with
    /// [`start_snapshot_id`](Self::start_snapshot_id)/[`end_snapshot_id`](Self::end_snapshot_id).
    pub snapshot_id: Option<i64>,

    /// Exclusive start snapshot for an incremental scan.
    pub start_snapshot_id: Option<i64>,

    /// Inclusive end snapshot for an incremental scan.
    pub end_snapshot_id: Option<i64>,

    /// The column names to project, or `None` to select all top-level columns.
    pub select: Option<Vec<String>>,

    /// An optional, still-unbound filter predicate to push down to the server.
    pub filter: Option<Predicate>,

    /// Whether predicate binding should treat column names case-sensitively.
    pub case_sensitive: bool,

    /// The resolved field ids being projected. These are not carried on the
    /// wire by the server, so the planner must stamp them onto every produced
    /// task.
    pub project_field_ids: Vec<i32>,

    /// The metadata of the table at scan time, used to resolve partition specs
    /// and schemas while converting the server's response.
    pub metadata: TableMetadataRef,

    /// The schema associated with the scanned snapshot.
    pub snapshot_schema: SchemaRef,
}

/// A catalog capability for planning table scans on the server.
///
/// Implementations should return an error with
/// [`ErrorKind::FeatureUnsupported`] when the server does not advertise the
/// scan-planning endpoints, which signals the scan engine to fall back to local
/// (client-side) planning. Any other error is treated as a hard failure.
#[async_trait]
pub trait ScanPlanner: Debug + Send + Sync {
    /// Plan a table scan on the server, returning the file scan tasks and an
    /// optional plan-scoped [`FileIO`] carrying any vended credentials.
    async fn plan_table_scan(&self, request: ScanPlanRequest) -> Result<ServerScanPlan>;
}

impl ScanPlanRequest {
    /// Returns an error if the snapshot selectors are inconsistent: a request
    /// must specify either a single `snapshot_id` or *both* incremental
    /// boundaries, never a mix.
    pub fn validate(&self) -> Result<()> {
        let incremental = self.start_snapshot_id.is_some() || self.end_snapshot_id.is_some();
        if incremental && self.snapshot_id.is_some() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Scan plan request cannot set both snapshot_id and an incremental snapshot range",
            ));
        }
        if incremental && (self.start_snapshot_id.is_none() || self.end_snapshot_id.is_none()) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Incremental scan plan request requires both start_snapshot_id and end_snapshot_id",
            ));
        }
        Ok(())
    }
}
