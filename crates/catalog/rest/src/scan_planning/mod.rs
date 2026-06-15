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

//! Client implementation of the Iceberg REST server-side scan-planning
//! protocol (`planTableScan` / `fetchPlanningResult` / `fetchScanTasks`).

mod convert;
pub(crate) mod endpoint;
mod expr;
mod planner;
mod stream;
mod types;

use std::collections::HashSet;
use std::sync::Arc;

use iceberg::expr::Predicate;
use iceberg::spec::{SchemaRef, TableMetadataRef};
use iceberg::{Runtime, TableIdent};
pub use planner::RestScanPlanner;
pub(crate) use stream::plan_table_scan;

use crate::RestCatalogConfig;
use crate::client::HttpClient;
use crate::scan_planning::endpoint::Endpoint;

/// All resources and parameters needed to run one server-side scan plan.
pub(crate) struct PlanScanContext {
    pub(crate) client: Arc<HttpClient>,
    pub(crate) config: RestCatalogConfig,
    pub(crate) endpoints: Arc<HashSet<Endpoint>>,
    pub(crate) runtime: Runtime,
    pub(crate) table_ident: TableIdent,
    pub(crate) metadata: TableMetadataRef,
    pub(crate) snapshot_schema: SchemaRef,
    pub(crate) project_field_ids: Vec<i32>,
    pub(crate) case_sensitive: bool,
    pub(crate) snapshot_id: Option<i64>,
    pub(crate) start_snapshot_id: Option<i64>,
    pub(crate) end_snapshot_id: Option<i64>,
    pub(crate) select: Option<Vec<String>>,
    pub(crate) filter: Option<Predicate>,
}
