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

//! [`RestScanPlanner`]: the REST catalog's [`ScanPlanner`] implementation.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::io::StorageFactory;
use iceberg::scan::{ScanPlanRequest, ScanPlanner, ServerScanPlan};
use iceberg::{Result, Runtime};

use super::endpoint::Endpoint;
use super::{PlanScanContext, plan_table_scan};
use crate::RestCatalogConfig;
use crate::client::HttpClient;

/// A [`ScanPlanner`] backed by a REST catalog's server-side planning endpoints.
///
/// Instances are created by `RestCatalog` and attached to the tables it loads;
/// each holds a handle to the shared HTTP client, the negotiated runtime config
/// and endpoint set, plus the storage factory and base props needed to build a
/// plan-scoped `FileIO` from the credentials the server vends per scan.
#[derive(Debug)]
pub struct RestScanPlanner {
    client: Arc<HttpClient>,
    config: RestCatalogConfig,
    endpoints: Arc<HashSet<Endpoint>>,
    runtime: Runtime,
    storage_factory: Option<Arc<dyn StorageFactory>>,
    base_props: HashMap<String, String>,
}

impl RestScanPlanner {
    pub(crate) fn new(
        client: Arc<HttpClient>,
        config: RestCatalogConfig,
        endpoints: Arc<HashSet<Endpoint>>,
        runtime: Runtime,
        storage_factory: Option<Arc<dyn StorageFactory>>,
        base_props: HashMap<String, String>,
    ) -> Self {
        Self {
            client,
            config,
            endpoints,
            runtime,
            storage_factory,
            base_props,
        }
    }
}

#[async_trait]
impl ScanPlanner for RestScanPlanner {
    async fn plan_table_scan(&self, request: ScanPlanRequest) -> Result<ServerScanPlan> {
        request.validate()?;
        let ctx = PlanScanContext {
            client: self.client.clone(),
            config: self.config.clone(),
            endpoints: self.endpoints.clone(),
            runtime: self.runtime.clone(),
            table_ident: request.table_ident,
            metadata: request.metadata,
            snapshot_schema: request.snapshot_schema,
            project_field_ids: request.project_field_ids,
            case_sensitive: request.case_sensitive,
            snapshot_id: request.snapshot_id,
            start_snapshot_id: request.start_snapshot_id,
            end_snapshot_id: request.end_snapshot_id,
            select: request.select,
            filter: request.filter,
            storage_factory: self.storage_factory.clone(),
            base_props: self.base_props.clone(),
        };
        plan_table_scan(ctx).await
    }
}
