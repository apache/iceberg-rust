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

//! The server-side scan-planning state machine: submit → (poll) → fetch tasks.
//!
//! Planning is driven to completion and the resulting [`FileScanTask`]s are
//! returned as a stream. If the planning future is dropped before it resolves
//! (e.g. the caller aborts the scan), a [`CancelGuard`] fires a best-effort
//! `DELETE .../plan/{plan-id}` so the server can release the in-flight plan.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use iceberg::expr::Bind;
use iceberg::scan::{FileScanTask, FileScanTaskDeleteFile, FileScanTaskStream};
use iceberg::{Error, ErrorKind, Result, TableIdent};
use reqwest::{Method, StatusCode};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::PlanScanContext;
use super::convert::{ConvertContext, to_delete_file, to_file_scan_task};
use super::endpoint::{self, Endpoint};
use super::types::{
    FetchPlanningResultResponse, FetchScanTasksRequest, FetchScanTasksResponse, PlanStatus,
    PlanTableScanRequest, PlanTableScanResponse, RestContentFile, RestFileScanTask,
};
use crate::RestCatalogConfig;
use crate::client::{
    HttpClient, deserialize_catalog_response, deserialize_unexpected_catalog_error,
};

const MIN_SLEEP: Duration = Duration::from_secs(1);
const MAX_SLEEP: Duration = Duration::from_secs(60);
const MAX_RETRIES: u32 = 10;
const MAX_WAIT: Duration = Duration::from_secs(300);

/// Plan a table scan on the server and return its file scan tasks as a stream.
///
/// Returns [`ErrorKind::FeatureUnsupported`] if the server does not advertise
/// the submit endpoint, allowing the scan engine to fall back to native
/// planning.
pub(crate) async fn plan_table_scan(ctx: PlanScanContext) -> Result<FileScanTaskStream> {
    endpoint::check(&ctx.endpoints, &Endpoint::submit_table_scan_plan())?;

    let bound_filter = match &ctx.filter {
        Some(predicate) => Some(predicate.bind(ctx.snapshot_schema.clone(), ctx.case_sensitive)?),
        None => None,
    };
    let convert_ctx = ConvertContext {
        metadata: ctx.metadata.clone(),
        snapshot_schema: ctx.snapshot_schema.clone(),
        project_field_ids: ctx.project_field_ids.clone(),
        case_sensitive: ctx.case_sensitive,
        bound_filter,
    };

    let mut guard = CancelGuard {
        client: ctx.client.clone(),
        config: ctx.config.clone(),
        endpoints: ctx.endpoints.clone(),
        table_ident: ctx.table_ident.clone(),
        runtime: ctx.runtime.clone(),
        plan_id: None,
        armed: true,
    };

    let tasks = drive(&ctx, &convert_ctx, &mut guard).await?;
    guard.disarm();

    Ok(Box::pin(futures::stream::iter(
        tasks.into_iter().map(Ok::<FileScanTask, Error>),
    )))
}

/// Run the full state machine and collect all produced tasks.
async fn drive(
    ctx: &PlanScanContext,
    convert_ctx: &ConvertContext,
    guard: &mut CancelGuard,
) -> Result<Vec<FileScanTask>> {
    let request = build_request(ctx);
    let submit_url = ctx.config.scan_plan_endpoint(&ctx.table_ident);
    let response: PlanTableScanResponse = post_json(&ctx.client, &submit_url, &request).await?;

    if let Some(plan_id) = &response.plan_id {
        guard.plan_id = Some(plan_id.clone());
    }

    let mut out = Vec::new();
    let (delete_files, file_scan_tasks, plan_tasks) = match response.status {
        PlanStatus::Completed => (
            response.delete_files,
            response.file_scan_tasks,
            response.plan_tasks,
        ),
        PlanStatus::Submitted => {
            let plan_id = response.plan_id.ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Server returned status=submitted without a plan-id",
                )
            })?;
            poll_until_complete(ctx, &plan_id).await?
        }
        PlanStatus::Failed => return Err(failure_error(response.error.as_ref())),
        PlanStatus::Cancelled => {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Server cancelled the scan plan",
            ));
        }
    };

    convert_into(delete_files, file_scan_tasks, convert_ctx, &mut out)?;

    // Fan out over plan-task tokens; fetchScanTasks may itself return more.
    let mut queue: VecDeque<String> = plan_tasks.unwrap_or_default().into_iter().collect();
    if !queue.is_empty() {
        endpoint::check(&ctx.endpoints, &Endpoint::fetch_table_scan_plan_tasks())?;
    }
    let tasks_url = ctx.config.scan_tasks_endpoint(&ctx.table_ident);
    while let Some(plan_task) = queue.pop_front() {
        let response: FetchScanTasksResponse =
            post_json(&ctx.client, &tasks_url, &FetchScanTasksRequest {
                plan_task,
            })
            .await?;
        convert_into(
            response.delete_files,
            response.file_scan_tasks,
            convert_ctx,
            &mut out,
        )?;
        if let Some(more) = response.plan_tasks {
            queue.extend(more);
        }
    }

    Ok(out)
}

/// Poll `GET .../plan/{plan-id}` with exponential backoff until the plan
/// reaches a terminal state.
async fn poll_until_complete(
    ctx: &PlanScanContext,
    plan_id: &str,
) -> Result<(
    Option<Vec<RestContentFile>>,
    Option<Vec<RestFileScanTask>>,
    Option<Vec<String>>,
)> {
    endpoint::check(&ctx.endpoints, &Endpoint::fetch_table_scan_plan())?;
    let url = ctx.config.scan_plan_id_endpoint(&ctx.table_ident, plan_id);

    let mut backoff = MIN_SLEEP;
    let mut attempts = 0u32;
    let started = Instant::now();
    loop {
        let response: FetchPlanningResultResponse = get_json(&ctx.client, &url).await?;
        match response.status {
            PlanStatus::Completed => {
                return Ok((
                    response.delete_files,
                    response.file_scan_tasks,
                    response.plan_tasks,
                ));
            }
            PlanStatus::Submitted => {
                attempts += 1;
                if attempts > MAX_RETRIES || started.elapsed() > MAX_WAIT {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "Scan plan {plan_id} did not complete within {MAX_WAIT:?} ({MAX_RETRIES} retries)"
                        ),
                    ));
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_SLEEP);
            }
            PlanStatus::Failed => return Err(failure_error(response.error.as_ref())),
            PlanStatus::Cancelled => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Scan plan {plan_id} was cancelled by the server"),
                ));
            }
        }
    }
}

fn build_request(ctx: &PlanScanContext) -> PlanTableScanRequest {
    let incremental = ctx.start_snapshot_id.is_some() || ctx.end_snapshot_id.is_some();
    PlanTableScanRequest {
        snapshot_id: if incremental { None } else { ctx.snapshot_id },
        start_snapshot_id: ctx.start_snapshot_id,
        end_snapshot_id: ctx.end_snapshot_id,
        select: ctx.select.clone(),
        filter: ctx.filter.as_ref().and_then(super::expr::predicate_to_json),
        case_sensitive: Some(ctx.case_sensitive),
        use_snapshot_schema: incremental.then_some(true),
        stats_fields: None,
        min_rows_requested: None,
    }
}

fn convert_into(
    delete_files: Option<Vec<RestContentFile>>,
    file_scan_tasks: Option<Vec<RestFileScanTask>>,
    convert_ctx: &ConvertContext,
    out: &mut Vec<FileScanTask>,
) -> Result<()> {
    let deletes: Vec<FileScanTaskDeleteFile> = delete_files
        .unwrap_or_default()
        .iter()
        .map(to_delete_file)
        .collect();
    for task in file_scan_tasks.unwrap_or_default() {
        out.push(to_file_scan_task(task, &deletes, convert_ctx)?);
    }
    Ok(())
}

fn failure_error(error: Option<&super::types::PlanError>) -> Error {
    let detail = error
        .map(|e| e.describe())
        .unwrap_or_else(|| "unknown".to_string());
    Error::new(
        ErrorKind::Unexpected,
        format!("Server-side scan planning failed: {detail}"),
    )
}

async fn post_json<B: Serialize, R: DeserializeOwned>(
    client: &HttpClient,
    url: &str,
    body: &B,
) -> Result<R> {
    let request = client.request(Method::POST, url).json(body).build()?;
    let response = client.query_catalog(request).await?;
    match response.status() {
        StatusCode::OK => deserialize_catalog_response(response).await,
        _ => Err(
            deserialize_unexpected_catalog_error(response, client.disable_header_redaction()).await,
        ),
    }
}

async fn get_json<R: DeserializeOwned>(client: &HttpClient, url: &str) -> Result<R> {
    let request = client.request(Method::GET, url).build()?;
    let response = client.query_catalog(request).await?;
    match response.status() {
        StatusCode::OK => deserialize_catalog_response(response).await,
        _ => Err(
            deserialize_unexpected_catalog_error(response, client.disable_header_redaction()).await,
        ),
    }
}

/// Fires a best-effort `DELETE .../plan/{plan-id}` if the planning future is
/// dropped while still armed (i.e. before planning completed successfully).
struct CancelGuard {
    client: Arc<HttpClient>,
    config: RestCatalogConfig,
    endpoints: Arc<HashSet<Endpoint>>,
    table_ident: TableIdent,
    runtime: iceberg::Runtime,
    plan_id: Option<String>,
    armed: bool,
}

impl CancelGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CancelGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Some(plan_id) = self.plan_id.take() else {
            return;
        };
        if endpoint::check(&self.endpoints, &Endpoint::cancel_table_scan_plan()).is_err() {
            return;
        }
        let client = self.client.clone();
        let url = self
            .config
            .scan_plan_id_endpoint(&self.table_ident, &plan_id);
        // Detached, best-effort cancellation.
        self.runtime.io().spawn(async move {
            if let Ok(request) = client.request(Method::DELETE, url).build() {
                let _ = client.execute(request).await;
            }
        });
    }
}
