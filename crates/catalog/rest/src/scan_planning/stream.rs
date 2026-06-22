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
//! Planning is driven to completion, then the resulting [`FileScanTask`]s are
//! produced as a lazy stream: tasks returned inline come first, then any
//! `plan-task` tokens are fetched on demand with bounded concurrency (a token's
//! response may carry further tokens, which are queued the same way). A
//! [`CancelGuard`] held by the stream fires a best-effort `DELETE .../plan/{id}`
//! if the stream is dropped before it is fully drained, so the server can
//! release the plan.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use iceberg::expr::Bind;
use iceberg::io::{FileIO, FileIOBuilder, StorageCredential};
use iceberg::scan::{FileScanTask, FileScanTaskDeleteFile, FileScanTaskStream, ServerScanPlan};
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
pub(crate) async fn plan_table_scan(ctx: PlanScanContext) -> Result<ServerScanPlan> {
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

    let planned = plan_initial(&ctx, &convert_ctx, &mut guard).await?;
    let file_io = build_scan_file_io(&ctx, planned.credentials);

    // `plan-task` tokens require the fetch-tasks endpoint; check once up front
    // (the same endpoint covers any further tokens that fetching returns).
    if !planned.plan_tasks.is_empty() {
        endpoint::check(&ctx.endpoints, &Endpoint::fetch_table_scan_plan_tasks())?;
    }

    let tasks = task_stream(
        &ctx,
        convert_ctx,
        planned.initial_tasks,
        planned.plan_tasks,
        guard,
    );

    Ok(ServerScanPlan { tasks, file_io })
}

/// Build a plan-scoped `FileIO` from the credentials the server vended for this
/// scan, or `None` if there were none (or no storage factory is configured).
fn build_scan_file_io(
    ctx: &PlanScanContext,
    wire_creds: Vec<crate::types::StorageCredential>,
) -> Option<FileIO> {
    if wire_creds.is_empty() {
        return None;
    }
    let factory = ctx.storage_factory.clone()?;
    let credentials: Vec<StorageCredential> = wire_creds
        .into_iter()
        .map(|c| StorageCredential::new(c.prefix, c.config))
        .collect();
    Some(
        FileIOBuilder::new(factory)
            .with_props(ctx.base_props.clone())
            .with_storage_credentials(credentials)
            .build(),
    )
}

/// Maximum number of `fetchScanTasks` requests in flight at once.
const MAX_CONCURRENT_TASK_FETCHES: usize = 8;

/// Outcome of the planning phase: tasks produced inline, the `plan-task` tokens
/// still to be fetched, and the credentials the server vended for this plan.
struct PlannedScan {
    initial_tasks: Vec<FileScanTask>,
    plan_tasks: VecDeque<String>,
    credentials: Vec<crate::types::StorageCredential>,
}

/// Submit the scan plan and drive it to completion, returning the tasks the
/// server produced inline along with any outstanding `plan-task` tokens and
/// vended credentials. Tokens are not fetched here — that happens lazily in
/// [`task_stream`].
async fn plan_initial(
    ctx: &PlanScanContext,
    convert_ctx: &ConvertContext,
    guard: &mut CancelGuard,
) -> Result<PlannedScan> {
    let request = build_request(ctx);
    let submit_url = ctx.config.scan_plan_endpoint(&ctx.table_ident);
    let response: PlanTableScanResponse = post_json(&ctx.client, &submit_url, &request).await?;

    if let Some(plan_id) = &response.plan_id {
        guard.plan_id = Some(plan_id.clone());
    }

    let mut credentials: Vec<crate::types::StorageCredential> = Vec::new();
    let (delete_files, file_scan_tasks, plan_tasks) = match response.status {
        PlanStatus::Completed => {
            credentials.extend(response.storage_credentials.unwrap_or_default());
            (
                response.delete_files,
                response.file_scan_tasks,
                response.plan_tasks,
            )
        }
        PlanStatus::Submitted => {
            let plan_id = response.plan_id.ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Server returned status=submitted without a plan-id",
                )
            })?;
            let (d, f, p, c) = poll_until_complete(ctx, &plan_id).await?;
            credentials.extend(c);
            (d, f, p)
        }
        PlanStatus::Failed => return Err(failure_error(response.error.as_ref())),
        PlanStatus::Cancelled => {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Server cancelled the scan plan",
            ));
        }
    };

    let mut initial_tasks = Vec::new();
    convert_into(
        delete_files,
        file_scan_tasks,
        convert_ctx,
        &mut initial_tasks,
    )?;

    Ok(PlannedScan {
        initial_tasks,
        plan_tasks: plan_tasks.unwrap_or_default().into_iter().collect(),
        credentials,
    })
}

/// Mutable state threaded through the lazy task stream.
struct TaskStreamState {
    client: Arc<HttpClient>,
    tasks_url: String,
    convert_ctx: ConvertContext,
    /// `plan-task` tokens still to fetch (grows as fetches return more).
    pending: VecDeque<String>,
    inflight: FuturesUnordered<BoxFuture<'static, Result<FetchScanTasksResponse>>>,
    /// Converted tasks ready to be yielded.
    buffer: VecDeque<FileScanTask>,
    /// Held until the stream is drained; dropping the stream early cancels the
    /// plan on the server.
    guard: CancelGuard,
}

/// Produce the file scan tasks as a lazy stream.
///
/// The tasks returned inline by planning are yielded first; the outstanding
/// `plan-task` tokens are then fetched on demand, up to
/// [`MAX_CONCURRENT_TASK_FETCHES`] at a time, and any further tokens a fetch
/// returns are queued and fetched the same way. Fetches only run while the
/// consumer is polling, so the bounded buffer applies natural backpressure.
fn task_stream(
    ctx: &PlanScanContext,
    convert_ctx: ConvertContext,
    initial_tasks: Vec<FileScanTask>,
    plan_tasks: VecDeque<String>,
    guard: CancelGuard,
) -> FileScanTaskStream {
    let state = TaskStreamState {
        client: ctx.client.clone(),
        tasks_url: ctx.config.scan_tasks_endpoint(&ctx.table_ident),
        convert_ctx,
        pending: plan_tasks,
        inflight: FuturesUnordered::new(),
        buffer: initial_tasks.into_iter().collect(),
        guard,
    };

    Box::pin(futures::stream::unfold(state, |mut st| async move {
        loop {
            if let Some(task) = st.buffer.pop_front() {
                return Some((Ok(task), st));
            }

            // Top up in-flight fetches from the pending token queue.
            while st.inflight.len() < MAX_CONCURRENT_TASK_FETCHES {
                let Some(plan_task) = st.pending.pop_front() else {
                    break;
                };
                let client = st.client.clone();
                let url = st.tasks_url.clone();
                let future: BoxFuture<'static, Result<FetchScanTasksResponse>> =
                    Box::pin(async move {
                        post_json(&client, &url, &FetchScanTasksRequest { plan_task }).await
                    });
                st.inflight.push(future);
            }

            // No buffered tasks, no pending tokens, nothing in flight → done.
            if st.inflight.is_empty() {
                st.guard.disarm();
                return None;
            }

            match st.inflight.next().await {
                Some(Ok(response)) => {
                    let mut produced = Vec::new();
                    if let Err(err) = convert_into(
                        response.delete_files,
                        response.file_scan_tasks,
                        &st.convert_ctx,
                        &mut produced,
                    ) {
                        return Some((Err(err), st));
                    }
                    st.buffer.extend(produced);
                    if let Some(more) = response.plan_tasks {
                        st.pending.extend(more);
                    }
                }
                Some(Err(err)) => return Some((Err(err), st)),
                None => {
                    st.guard.disarm();
                    return None;
                }
            }
        }
    }))
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
    Vec<crate::types::StorageCredential>,
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
                    response.storage_credentials.unwrap_or_default(),
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

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use futures::TryStreamExt;
    use iceberg::Runtime;
    use mockito::Matcher;

    use super::*;
    use crate::types::LoadTableResult;

    /// Build a minimal [`PlanScanContext`] pointed at a mock server. The table
    /// metadata is borrowed from the load-table fixture; it is only consulted
    /// when converting file-scan tasks, which these tests deliberately leave
    /// empty so the focus stays on the planning/fan-out control flow.
    fn test_context(server_url: &str) -> PlanScanContext {
        let load: LoadTableResult =
            serde_json::from_str(include_str!("../../testdata/load_table_response.json")).unwrap();
        let metadata = Arc::new(load.metadata);
        let snapshot_schema = metadata.current_schema().clone();
        let config = RestCatalogConfig::builder()
            .uri(server_url.to_string())
            .build();
        let endpoints: HashSet<Endpoint> = [
            Endpoint::submit_table_scan_plan(),
            Endpoint::fetch_table_scan_plan(),
            Endpoint::fetch_table_scan_plan_tasks(),
            Endpoint::cancel_table_scan_plan(),
        ]
        .into_iter()
        .collect();

        PlanScanContext {
            client: Arc::new(HttpClient::new(&config).unwrap()),
            config,
            endpoints: Arc::new(endpoints),
            runtime: Runtime::current(),
            table_ident: TableIdent::from_strs(["ns", "t"]).unwrap(),
            metadata,
            snapshot_schema,
            project_field_ids: vec![],
            case_sensitive: true,
            snapshot_id: None,
            start_snapshot_id: None,
            end_snapshot_id: None,
            select: None,
            filter: None,
            storage_factory: None,
            base_props: HashMap::new(),
        }
    }

    /// A completed plan that hands back a `plan-task` token, whose fetch in turn
    /// returns a further token, must drive both fetches and then terminate.
    #[tokio::test]
    async fn fan_out_fetches_recursive_plan_tasks() {
        let mut server = mockito::Server::new_async().await;

        let plan_mock = server
            .mock("POST", Matcher::Regex(r"/plan$".to_string()))
            .with_status(200)
            .with_body(r#"{"status":"completed","plan-tasks":["t1"]}"#)
            .create_async()
            .await;

        // Fetching "t1" yields no tasks but a further token "t2".
        let t1_mock = server
            .mock("POST", Matcher::Regex(r"/tasks$".to_string()))
            .match_body(Matcher::Regex(r#""t1""#.to_string()))
            .with_status(200)
            .with_body(r#"{"plan-tasks":["t2"]}"#)
            .create_async()
            .await;

        // Fetching "t2" returns nothing further, ending the fan-out.
        let t2_mock = server
            .mock("POST", Matcher::Regex(r"/tasks$".to_string()))
            .match_body(Matcher::Regex(r#""t2""#.to_string()))
            .with_status(200)
            .with_body(r#"{}"#)
            .create_async()
            .await;

        let plan = plan_table_scan(test_context(&server.url())).await.unwrap();
        let tasks: Vec<FileScanTask> = plan.tasks.try_collect().await.unwrap();

        assert!(tasks.is_empty());
        plan_mock.assert_async().await;
        t1_mock.assert_async().await;
        // The recursive token was fetched and the stream terminated.
        t2_mock.assert_async().await;
    }
}
