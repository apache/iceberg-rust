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

//! REST server-side scan planning client.
//!
//! Implements the plan / fetch-result / cancel / fetch-tasks endpoints, a
//! [`RestSessionCatalog::wait_for_plan`] poller, content-file decoding, and
//! [`iceberg::scan::ScanPlanner`] so a table loaded from this catalog can
//! plan remotely when the server advertises the endpoints.

use std::collections::{HashSet, VecDeque};
use std::time::Duration;

use async_trait::async_trait;
use iceberg::scan::{FileScanTask, ScanPlanner, ScanPlanningRequest, ScanPlanningResult};
use iceberg::spec::DataContentType;
use iceberg::{Error, ErrorKind, Result, TableIdent};
use rand::Rng;
use reqwest::{Method, Response, StatusCode};
use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize, Serializer};
use uuid::{Uuid, Variant, Version};

use crate::catalog::RestSessionCatalog;
use crate::client::{deserialize_catalog_response, deserialize_unexpected_catalog_error};
use crate::endpoint::{
    Endpoint, V1_CANCEL_PLANNING, V1_FETCH_PLAN_RESULT, V1_FETCH_SCAN_TASKS, V1_PLAN_TABLE_SCAN,
};
use crate::request::HttpRequest;
use crate::types::{ErrorModel, ErrorResponse, StorageCredential};

const HEADER_IDEMPOTENCY_KEY: &str = "Idempotency-Key";
const HEADER_ACCESS_DELEGATION: &str = "X-Iceberg-Access-Delegation";

const MSG_PLAN_EXPIRED: &str = "scan plan expired";
const MSG_PLAN_FAILED: &str = "scan plan failed";
const MSG_PLAN_CANCELLED: &str = "scan plan cancelled";
const MSG_NO_SUCH_PLAN_TASK: &str = "scan plan task not found";
const MSG_PLAN_POLL_EXHAUSTED: &str = "scan plan polling exhausted retries";

const ERR_TYPE_NO_SUCH_PLAN_ID: &str = "NoSuchPlanIdException";
const ERR_TYPE_NO_SUCH_PLAN_TASK: &str = "NoSuchPlanTaskException";
const ERR_TYPE_NO_SUCH_TABLE: &str = "NoSuchTableException";
const ERR_TYPE_NO_SUCH_NAMESPACE: &str = "NoSuchNamespaceException";

/// Status of a server-side scan plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PlanStatus {
    /// Planning finished and tasks (or plan-task handles) are available.
    Completed,
    /// Planning is still running; poll [`RestSessionCatalog::fetch_planning_result`].
    Submitted,
    /// The plan was cancelled. Valid on fetch-result, not on planTableScan.
    Cancelled,
    /// Planning failed. The error detail is on the failed arm.
    Failed,
}

/// Task payload shared by completed planning responses and fetchScanTasks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct ScanTasks {
    /// Opaque plan-task handles that still need [`RestSessionCatalog::fetch_scan_tasks`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plan_tasks: Vec<String>,
    /// File scan tasks.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub file_scan_tasks: Vec<RestFileScanTask>,
    /// Delete files referenced by indices in this payload's `file-scan-tasks`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub delete_files: Vec<RestContentFile>,
}

/// REST ContentFile JSON. Unknown fields such as metrics maps are ignored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct RestContentFile {
    /// Absolute file path.
    pub file_path: String,
    /// File format string (`PARQUET`, `AVRO`, ...).
    pub file_format: String,
    /// File size in bytes.
    pub file_size_in_bytes: u64,
    /// Record count when the server provided it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_count: Option<u64>,
    /// `data`, `position-deletes`, or `equality-deletes`. Avro ordinals 0/1/2 are also accepted.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_content",
        serialize_with = "serialize_content"
    )]
    pub content: Option<DataContentType>,
    /// Partition spec id. Defaults to 0.
    #[serde(default)]
    pub spec_id: i32,
    /// Partition values as a JSON array or object keyed by field id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<serde_json::Value>,
    /// Equality field ids for equality deletes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub equality_ids: Option<Vec<i32>>,
    /// Encryption key metadata. OpenAPI hex text, or a JSON array of 0..=255 integers.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_key_metadata",
        serialize_with = "serialize_key_metadata"
    )]
    pub key_metadata: Option<Box<[u8]>>,
    /// First row id when the server provided it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_row_id: Option<i64>,
}

/// REST `FileScanTask` wire payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct RestFileScanTask {
    /// REST ContentFile for the data file.
    pub data_file: RestContentFile,
    /// Indices into the sibling delete-files array.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delete_file_references: Option<Vec<i32>>,
    /// Optional residual filter in ExpressionParser JSON.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub residual_filter: Option<serde_json::Value>,
}

/// POST `.../plan` request body. Header-only fields are skipped on the wire.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct PlanTableScanRequest {
    /// `Idempotency-Key` header. `None` generates a fresh UUIDv7 per call.
    #[serde(skip)]
    pub idempotency_key: Option<String>,
    /// `X-Iceberg-Access-Delegation` header. `None` sends no such header.
    #[serde(skip)]
    pub access_delegation: Option<String>,
    /// Snapshot to scan. Omitted for the current snapshot.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<i64>,
    /// Selected schema fields.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub select: Vec<String>,
    /// Row filter as ExpressionParser JSON, not `iceberg::expr::Predicate`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<serde_json::Value>,
    /// Hint for the minimum number of rows the server should return.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_rows_requested: Option<i64>,
    /// Case-sensitive field matching for filter and select.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub case_sensitive: Option<bool>,
    /// When true, use the schema at the scanned snapshot.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_snapshot_schema: Option<bool>,
    /// Incremental scan start (exclusive). Wire-only in this PR.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_snapshot_id: Option<i64>,
    /// Incremental scan end (inclusive). Wire-only in this PR.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_snapshot_id: Option<i64>,
    /// Fields for which the server should send column stats.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stats_fields: Vec<String>,
}

/// POST `.../plan` response. `completed` and `submitted` require `plan-id`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct PlanTableScanResponse {
    /// Discriminator for the planning-result union.
    pub status: PlanStatus,
    /// Server-issued plan id. Required for completed and submitted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<String>,
    /// Failed-arm error detail.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorModel>,
    /// Task payload. Empty unless status is completed.
    #[serde(flatten)]
    pub scan_tasks: ScanTasks,
    /// Optional vended credentials for reading the returned files.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_credentials: Option<Vec<StorageCredential>>,
}

/// GET `.../plan/{plan-id}` response.
#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct FetchPlanningResultResponse {
    /// Discriminator for the planning-result union.
    pub status: PlanStatus,
    /// Failed-arm error detail.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorModel>,
    /// Task payload. Empty unless status is completed.
    #[serde(flatten)]
    pub scan_tasks: ScanTasks,
    /// Optional vended credentials for reading the returned files.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_credentials: Option<Vec<StorageCredential>>,
}

/// Completed arm of a planning result, as returned by [`RestSessionCatalog::wait_for_plan`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CompletedPlanningResult {
    /// Always [`PlanStatus::Completed`].
    pub status: PlanStatus,
    /// Task payload, which may still include plan-task handles.
    #[serde(flatten)]
    pub scan_tasks: ScanTasks,
    /// Optional vended credentials for reading the returned files.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_credentials: Option<Vec<StorageCredential>>,
}

/// Per-call options for [`RestSessionCatalog::fetch_planning_result`].
#[derive(Debug, Clone, Default)]
pub struct FetchPlanningResultOptions {
    /// `X-Iceberg-Access-Delegation` header. `None` sends no such header.
    pub access_delegation: Option<String>,
}

/// POST `.../tasks` request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct FetchScanTasksRequest {
    /// `Idempotency-Key` header. `None` generates a fresh UUIDv7 per call.
    #[serde(skip)]
    pub idempotency_key: Option<String>,
    /// Opaque plan-task handle from a completed plan.
    pub plan_task: String,
}

/// POST `.../tasks` response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct FetchScanTasksResponse {
    /// Task payload, which may itself contain further plan-task handles.
    #[serde(flatten)]
    pub scan_tasks: ScanTasks,
}

/// Polling backoff and bounds for [`RestSessionCatalog::wait_for_plan`].
#[derive(Debug, Clone)]
pub struct WaitForPlanOptions {
    /// Backoff floor. Zero uses 100ms.
    pub min_delay: Duration,
    /// Backoff cap. Zero uses 5s.
    pub max_delay: Duration,
    /// Bound on the best-effort cancel after giving up. Zero uses 5s.
    pub cancel_grace_period: Duration,
    /// Poll attempts after the first. Zero uses 10 when [`Self::timeout`] is
    /// `None`. When a timeout is set, zero means keep polling until the
    /// deadline.
    pub max_retries: u32,
    /// Optional overall deadline. `None` relies on `max_retries`.
    pub timeout: Option<Duration>,
    /// `X-Iceberg-Access-Delegation` forwarded on each poll.
    pub access_delegation: Option<String>,
}

impl Default for WaitForPlanOptions {
    fn default() -> Self {
        Self {
            min_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            cancel_grace_period: Duration::from_secs(5),
            max_retries: 10,
            timeout: None,
            access_delegation: None,
        }
    }
}

impl<'de> Deserialize<'de> for PlanTableScanResponse {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let raw = RawPlanningResponse::deserialize(deserializer)?;
        match raw.status {
            PlanStatus::Completed | PlanStatus::Submitted => {
                if raw.plan_id.is_none() {
                    return Err(de::Error::custom(format!(
                        "planTableScan response with status {:?} missing plan-id",
                        raw.status
                    )));
                }
            }
            PlanStatus::Cancelled => {
                return Err(de::Error::custom(
                    "planTableScan response has invalid status cancelled",
                ));
            }
            PlanStatus::Failed => {}
        }
        Ok(PlanTableScanResponse {
            status: raw.status,
            plan_id: raw.plan_id,
            error: decode_planning_error(raw.error),
            scan_tasks: raw.scan_tasks,
            storage_credentials: raw.storage_credentials,
        })
    }
}

impl<'de> Deserialize<'de> for FetchPlanningResultResponse {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let raw = RawPlanningResponse::deserialize(deserializer)?;
        Ok(FetchPlanningResultResponse {
            status: raw.status,
            error: decode_planning_error(raw.error),
            scan_tasks: raw.scan_tasks,
            storage_credentials: raw.storage_credentials,
        })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct RawPlanningResponse {
    status: PlanStatus,
    #[serde(default)]
    plan_id: Option<String>,
    #[serde(default)]
    error: Option<serde_json::Value>,
    #[serde(flatten)]
    scan_tasks: ScanTasks,
    #[serde(default)]
    storage_credentials: Option<Vec<StorageCredential>>,
}

fn decode_planning_error(raw: Option<serde_json::Value>) -> Option<ErrorModel> {
    let value = raw?;
    serde_json::from_value(value).ok()
}

/// True when a fetch-result 404 was a forgotten plan-id.
pub fn is_plan_expired(err: &Error) -> bool {
    err.message() == MSG_PLAN_EXPIRED
}

/// True when the server reported a failed plan.
pub fn is_plan_failed(err: &Error) -> bool {
    err.message() == MSG_PLAN_FAILED || err.message().starts_with(&format!("{MSG_PLAN_FAILED}: "))
}

/// True when a submitted plan was cancelled.
pub fn is_plan_cancelled(err: &Error) -> bool {
    err.message() == MSG_PLAN_CANCELLED
}

/// True when a plan-task handle is gone.
pub fn is_no_such_plan_task(err: &Error) -> bool {
    err.message() == MSG_NO_SUCH_PLAN_TASK
}

/// True when [`RestSessionCatalog::wait_for_plan`] hit its retry or timeout bound.
pub fn is_plan_poll_exhausted(err: &Error) -> bool {
    err.message() == MSG_PLAN_POLL_EXHAUSTED
}

fn parse_content_type(value: &serde_json::Value) -> std::result::Result<DataContentType, String> {
    match value {
        serde_json::Value::String(s) => match s.as_str() {
            "data" => Ok(DataContentType::Data),
            "position-deletes" => Ok(DataContentType::PositionDeletes),
            "equality-deletes" => Ok(DataContentType::EqualityDeletes),
            other => Err(format!("unknown REST content type {other:?}")),
        },
        serde_json::Value::Number(n) => {
            let ordinal = n
                .as_i64()
                .ok_or_else(|| format!("content ordinal is not an integer: {n}"))?;
            let ordinal = i32::try_from(ordinal)
                .map_err(|_| format!("content ordinal out of range: {ordinal}"))?;
            DataContentType::try_from(ordinal).map_err(|e| e.to_string())
        }
        other => Err(format!("unexpected REST content encoding: {other}")),
    }
}

fn deserialize_content<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<DataContentType>, D::Error>
where D: Deserializer<'de> {
    match serde_json::Value::deserialize(deserializer)? {
        serde_json::Value::Null => Ok(None),
        value => parse_content_type(&value)
            .map(Some)
            .map_err(de::Error::custom),
    }
}

fn serialize_content<S>(
    value: &Option<DataContentType>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        None => serializer.serialize_none(),
        Some(DataContentType::Data) => serializer.serialize_str("data"),
        Some(DataContentType::PositionDeletes) => serializer.serialize_str("position-deletes"),
        Some(DataContentType::EqualityDeletes) => serializer.serialize_str("equality-deletes"),
    }
}

fn decode_hex_key_metadata(value: &str) -> std::result::Result<Vec<u8>, String> {
    if !value.len().is_multiple_of(2) {
        return Err(format!(
            "key-metadata hex string must have an even number of characters: {value:?}"
        ));
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|chunk| {
            let high = decode_hex_digit(chunk[0], value)?;
            let low = decode_hex_digit(chunk[1], value)?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn decode_hex_digit(digit: u8, value: &str) -> std::result::Result<u8, String> {
    match digit {
        b'0'..=b'9' => Ok(digit - b'0'),
        b'a'..=b'f' => Ok(digit - b'a' + 10),
        b'A'..=b'F' => Ok(digit - b'A' + 10),
        _ => Err(format!("key-metadata is not valid hex: {value:?}")),
    }
}

fn deserialize_key_metadata<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Box<[u8]>>, D::Error>
where D: Deserializer<'de> {
    match serde_json::Value::deserialize(deserializer)? {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(s) => {
            if s.is_empty() {
                return Ok(None);
            }
            decode_hex_key_metadata(&s)
                .map(|bytes| Some(bytes.into_boxed_slice()))
                .map_err(de::Error::custom)
        }
        serde_json::Value::Array(values) => {
            if values.is_empty() {
                return Ok(None);
            }
            let mut bytes = Vec::with_capacity(values.len());
            for value in values {
                let n = value.as_u64().ok_or_else(|| {
                    de::Error::custom("key-metadata array element is not an integer")
                })?;
                let byte = u8::try_from(n)
                    .map_err(|_| de::Error::custom("key-metadata array element is out of range"))?;
                bytes.push(byte);
            }
            Ok(Some(bytes.into_boxed_slice()))
        }
        other => Err(de::Error::custom(format!(
            "unexpected key-metadata encoding: {other}"
        ))),
    }
}

fn serialize_key_metadata<S>(
    value: &Option<Box<[u8]>>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        None => serializer.serialize_none(),
        Some(bytes) => {
            const HEX: &[u8; 16] = b"0123456789ABCDEF";
            let mut out = String::with_capacity(bytes.len() * 2);
            for byte in bytes.iter() {
                out.push(HEX[(byte >> 4) as usize] as char);
                out.push(HEX[(byte & 0x0f) as usize] as char);
            }
            serializer.serialize_str(&out)
        }
    }
}

impl RestSessionCatalog {
    /// Whether the server advertised the synchronous plan endpoint.
    pub async fn supports_plan_table_scan(&self) -> Result<bool> {
        self.supports_endpoint(&V1_PLAN_TABLE_SCAN).await
    }

    /// Whether the server advertised all four scan-planning endpoints.
    pub async fn supports_full_remote_scan_planning(&self) -> Result<bool> {
        Ok(self.supports_plan_table_scan().await?
            && self.supports_endpoint(&V1_FETCH_PLAN_RESULT).await?
            && self.supports_endpoint(&V1_CANCEL_PLANNING).await?
            && self.supports_endpoint(&V1_FETCH_SCAN_TASKS).await?)
    }

    /// Whether this catalog can complete a remote plan end-to-end.
    ///
    /// True when the server advertises all four scan-planning endpoints. Task
    /// decoding is implemented, so auto-mode [`iceberg::scan::TableScan`]s can
    /// route here instead of falling back to local planning.
    pub async fn supports_remote_scan_planning(&self) -> Result<bool> {
        self.supports_full_remote_scan_planning().await
    }

    /// Submits a server-side scan plan.
    ///
    /// Completed and submitted plans return `Ok`. A failed plan returns
    /// [`is_plan_failed`].
    pub async fn plan_table_scan(
        &self,
        table: &TableIdent,
        request: PlanTableScanRequest,
    ) -> Result<PlanTableScanResponse> {
        self.require_endpoint(&V1_PLAN_TABLE_SCAN, "planTableScan")
            .await?;
        let client = self.client().await?;
        let url = format!("{}/plan", client.config.table_endpoint(table));
        let headers = scan_planning_headers(
            request.idempotency_key.as_deref(),
            request.access_delegation.as_deref(),
            true,
        )?;
        let mut builder = client.http_client.request(Method::POST, url).json(&request);
        for (name, value) in headers {
            builder = builder.header(name, value);
        }
        let http_response = client.query_catalog(HttpRequest::build(builder)?).await?;
        let resp: PlanTableScanResponse = match http_response.status() {
            StatusCode::OK => deserialize_catalog_response(http_response).await?,
            StatusCode::NOT_FOUND => {
                return Err(map_plan_not_found(http_response, PlanNotFoundKind::Submit).await);
            }
            _ => {
                return Err(deserialize_unexpected_catalog_error(
                    http_response,
                    client.http_client.disable_header_redaction(),
                )
                .await);
            }
        };
        match resp.status {
            PlanStatus::Completed | PlanStatus::Submitted => Ok(resp),
            PlanStatus::Failed => Err(failed_plan_error(resp.error.as_ref())),
            PlanStatus::Cancelled => Err(Error::new(
                ErrorKind::DataInvalid,
                "planTableScan response has invalid status cancelled",
            )),
        }
    }

    /// Polls a previously submitted plan.
    pub async fn fetch_planning_result(
        &self,
        table: &TableIdent,
        plan_id: &str,
        opts: FetchPlanningResultOptions,
    ) -> Result<FetchPlanningResultResponse> {
        match self.fetch_planning_result_raw(table, plan_id, opts).await {
            Ok(resp) => Ok(resp),
            Err(PollError::Retry { error, .. }) | Err(PollError::Terminal(error)) => Err(error),
        }
    }

    /// Cancels a server-side plan. Best-effort: a 404 is returned as a generic
    /// unexpected error rather than [`is_plan_expired`].
    pub async fn cancel_planning(&self, table: &TableIdent, plan_id: &str) -> Result<()> {
        require_plan_id(plan_id)?;
        self.require_endpoint(&V1_CANCEL_PLANNING, "cancelPlanning")
            .await?;
        let client = self.client().await?;
        let url = format!(
            "{}/plan/{}",
            client.config.table_endpoint(table),
            escape_opaque_path_segment(plan_id)
        );
        let request = HttpRequest::build(client.http_client.request(Method::DELETE, url))?;
        let http_response = client.query_catalog(request).await?;
        match http_response.status() {
            StatusCode::NO_CONTENT | StatusCode::OK => Ok(()),
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                client.http_client.disable_header_redaction(),
            )
            .await),
        }
    }

    /// Fetches scan tasks for a plan-task handle.
    pub async fn fetch_scan_tasks(
        &self,
        table: &TableIdent,
        request: FetchScanTasksRequest,
    ) -> Result<FetchScanTasksResponse> {
        self.require_endpoint(&V1_FETCH_SCAN_TASKS, "fetchScanTasks")
            .await?;
        let client = self.client().await?;
        let url = format!("{}/tasks", client.config.table_endpoint(table));
        let headers = scan_planning_headers(request.idempotency_key.as_deref(), None, true)?;
        let mut builder = client.http_client.request(Method::POST, url).json(&request);
        for (name, value) in headers {
            builder = builder.header(name, value);
        }
        let http_response = client.query_catalog(HttpRequest::build(builder)?).await?;
        match http_response.status() {
            StatusCode::OK => {
                let bytes = http_response.bytes().await?;
                if bytes.is_empty() {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "fetchScanTasks response was empty",
                    ));
                }
                serde_json::from_slice(&bytes).map_err(|e| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "Failed to parse response from rest catalog server",
                    )
                    .with_context("json", String::from_utf8_lossy(&bytes))
                    .with_source(e)
                })
            }
            StatusCode::NOT_FOUND => {
                Err(map_plan_not_found(http_response, PlanNotFoundKind::Tasks).await)
            }
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                client.http_client.disable_header_redaction(),
            )
            .await),
        }
    }

    /// Polls a submitted plan until it completes, fails, or the retry budget
    /// is spent. Does not expand plan-task handles.
    pub async fn wait_for_plan(
        &self,
        table: &TableIdent,
        plan_id: &str,
        opts: WaitForPlanOptions,
    ) -> Result<CompletedPlanningResult> {
        require_plan_id(plan_id)?;
        self.require_endpoint(&V1_FETCH_PLAN_RESULT, "fetchPlanningResult")
            .await?;

        let (_, _, grace, _, _) = resolve_wait_options(&opts);
        let mut guard =
            PlanAbandonGuard::new(self.clone_uninitialized(), table.clone(), plan_id, grace);

        let work = self.wait_for_plan_loop(table, plan_id, opts.clone());
        let result = if let Some(timeout) = opts.timeout {
            match tokio::time::timeout(timeout, work).await {
                Ok(result) => result,
                Err(_) => {
                    self.abandon_plan(table, plan_id, grace).await;
                    Err(Error::new(ErrorKind::Unexpected, MSG_PLAN_POLL_EXHAUSTED))
                }
            }
        } else {
            work.await
        };
        guard.disarm();
        result
    }

    async fn wait_for_plan_loop(
        &self,
        table: &TableIdent,
        plan_id: &str,
        opts: WaitForPlanOptions,
    ) -> Result<CompletedPlanningResult> {
        let (min_delay, max_delay, grace, max_retries, clamp_retry_after) =
            resolve_wait_options(&opts);
        let fetch_opts = FetchPlanningResultOptions {
            access_delegation: opts.access_delegation.clone(),
        };
        let mut sleep = min_delay;
        let mut retries = 0u32;
        loop {
            let retry_after = match self
                .fetch_planning_result_raw(table, plan_id, fetch_opts.clone())
                .await
            {
                Ok(resp) => match resp.status {
                    PlanStatus::Completed => {
                        return Ok(CompletedPlanningResult {
                            status: PlanStatus::Completed,
                            scan_tasks: resp.scan_tasks,
                            storage_credentials: resp.storage_credentials,
                        });
                    }
                    PlanStatus::Submitted => None,
                    PlanStatus::Failed => return Err(failed_plan_error(resp.error.as_ref())),
                    PlanStatus::Cancelled => {
                        return Err(Error::new(ErrorKind::Unexpected, MSG_PLAN_CANCELLED));
                    }
                },
                Err(PollError::Retry { retry_after, .. }) => retry_after,
                Err(PollError::Terminal(err)) => return Err(err),
            };

            if max_retries > 0 && retries >= max_retries {
                self.abandon_plan(table, plan_id, grace).await;
                return Err(Error::new(ErrorKind::Unexpected, MSG_PLAN_POLL_EXHAUSTED));
            }
            retries += 1;
            sleep = next_scan_plan_backoff(sleep, min_delay, max_delay);
            sleep = apply_retry_after(sleep, retry_after, min_delay, max_delay, clamp_retry_after);
            tokio::time::sleep(sleep).await;
        }
    }

    async fn fetch_planning_result_raw(
        &self,
        table: &TableIdent,
        plan_id: &str,
        opts: FetchPlanningResultOptions,
    ) -> std::result::Result<FetchPlanningResultResponse, PollError> {
        if let Err(err) = require_plan_id(plan_id) {
            return Err(PollError::Terminal(err));
        }
        if let Err(err) = self
            .require_endpoint(&V1_FETCH_PLAN_RESULT, "fetchPlanningResult")
            .await
        {
            return Err(PollError::Terminal(err));
        }
        let client = match self.client().await {
            Ok(client) => client,
            Err(err) => return Err(PollError::Terminal(err)),
        };
        let url = format!(
            "{}/plan/{}",
            client.config.table_endpoint(table),
            escape_opaque_path_segment(plan_id)
        );
        let mut builder = client.http_client.request(Method::GET, url);
        if let Some(delegation) = opts.access_delegation.as_deref() {
            builder = builder.header(HEADER_ACCESS_DELEGATION, delegation);
        }
        let http_response = match client
            .query_catalog(HttpRequest::build(builder).map_err(PollError::Terminal)?)
            .await
        {
            Ok(resp) => resp,
            Err(err) => return Err(PollError::Terminal(err)),
        };
        let status = http_response.status();
        let retry_after =
            parse_retry_after(http_response.headers().get(reqwest::header::RETRY_AFTER));
        match status {
            StatusCode::OK => {
                let resp: FetchPlanningResultResponse = deserialize_catalog_response(http_response)
                    .await
                    .map_err(PollError::Terminal)?;
                match resp.status {
                    PlanStatus::Failed => {
                        Err(PollError::Terminal(failed_plan_error(resp.error.as_ref())))
                    }
                    PlanStatus::Cancelled => Err(PollError::Terminal(Error::new(
                        ErrorKind::Unexpected,
                        MSG_PLAN_CANCELLED,
                    ))),
                    PlanStatus::Completed | PlanStatus::Submitted => Ok(resp),
                }
            }
            StatusCode::NOT_FOUND => Err(PollError::Terminal(
                map_plan_not_found(http_response, PlanNotFoundKind::Fetch).await,
            )),
            StatusCode::REQUEST_TIMEOUT
            | StatusCode::TOO_MANY_REQUESTS
            | StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT => {
                let error = deserialize_unexpected_catalog_error(
                    http_response,
                    client.http_client.disable_header_redaction(),
                )
                .await
                .with_retryable(true);
                Err(PollError::Retry { retry_after, error })
            }
            _ => Err(PollError::Terminal(
                deserialize_unexpected_catalog_error(
                    http_response,
                    client.http_client.disable_header_redaction(),
                )
                .await,
            )),
        }
    }

    async fn require_endpoint(&self, endpoint: &Endpoint, name: &str) -> Result<()> {
        if self.supports_endpoint(endpoint).await? {
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!("{name} is not advertised by the REST catalog"),
            ))
        }
    }

    async fn abandon_plan(&self, table: &TableIdent, plan_id: &str, grace: Duration) {
        let _ = tokio::time::timeout(grace, self.cancel_planning(table, plan_id)).await;
    }

    async fn collect_scan_tasks(
        &self,
        table: &TableIdent,
        tasks: ScanTasks,
        ctx: &crate::scan_decode::ConvertContext,
    ) -> Result<Vec<FileScanTask>> {
        let mut out =
            crate::scan_decode::decode_scan_tasks(tasks.file_scan_tasks, tasks.delete_files, ctx)?;
        let mut queue: VecDeque<String> = tasks.plan_tasks.into();
        let mut seen = HashSet::new();
        while let Some(handle) = queue.pop_front() {
            if !seen.insert(handle.clone()) {
                continue;
            }
            let resp = self
                .fetch_scan_tasks(table, FetchScanTasksRequest {
                    idempotency_key: None,
                    plan_task: handle,
                })
                .await?;
            out.extend(crate::scan_decode::decode_scan_tasks(
                resp.scan_tasks.file_scan_tasks,
                resp.scan_tasks.delete_files,
                ctx,
            )?);
            queue.extend(resp.scan_tasks.plan_tasks);
        }
        Ok(out)
    }

    async fn plan_scan(&self, request: ScanPlanningRequest) -> Result<ScanPlanningResult> {
        let ident = request.table_ident.clone();
        // TableScan always binds the snapshot schema. OpenAPI defaults
        // use-snapshot-schema to false (current table schema), which would
        // plan time travel against the wrong schema.
        let use_snapshot_schema = request.snapshot_id.map(|_| true);
        let resp = self
            .plan_table_scan(&ident, PlanTableScanRequest {
                snapshot_id: request.snapshot_id,
                select: request.select.clone().unwrap_or_default(),
                case_sensitive: Some(request.case_sensitive),
                use_snapshot_schema,
                ..Default::default()
            })
            .await?;

        let completed = match resp.status {
            PlanStatus::Completed => CompletedPlanningResult {
                status: PlanStatus::Completed,
                scan_tasks: resp.scan_tasks,
                storage_credentials: resp.storage_credentials,
            },
            PlanStatus::Submitted => {
                let plan_id = resp.plan_id.ok_or_else(|| {
                    Error::new(ErrorKind::DataInvalid, "submitted plan missing plan-id")
                })?;
                self.wait_for_plan(&ident, &plan_id, WaitForPlanOptions::default())
                    .await?
            }
            PlanStatus::Failed => return Err(failed_plan_error(resp.error.as_ref())),
            PlanStatus::Cancelled => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "planTableScan response has invalid status cancelled",
                ));
            }
        };

        let ctx = crate::scan_decode::ConvertContext {
            metadata: request.metadata,
            snapshot_schema: request.snapshot_schema,
            project_field_ids: request.project_field_ids,
            case_sensitive: request.case_sensitive,
            bound_filter: request.bound_filter,
            name_mapping: request.name_mapping,
            unified_partition_type: request.unified_partition_type,
        };
        Ok(ScanPlanningResult {
            tasks: self
                .collect_scan_tasks(&ident, completed.scan_tasks, &ctx)
                .await?,
        })
    }
}

#[async_trait]
impl ScanPlanner for RestSessionCatalog {
    async fn supports_remote_scan_planning(&self) -> Result<bool> {
        self.supports_full_remote_scan_planning().await
    }

    async fn plan_files(&self, request: ScanPlanningRequest) -> Result<ScanPlanningResult> {
        self.plan_scan(request).await
    }
}

/// Best-effort DELETE of a submitted plan if [`RestSessionCatalog::wait_for_plan`] is
/// dropped (caller timeout, cancellation) before it returns.
struct PlanAbandonGuard {
    catalog: Option<RestSessionCatalog>,
    table: TableIdent,
    plan_id: String,
    grace: Duration,
}

impl PlanAbandonGuard {
    fn new(catalog: RestSessionCatalog, table: TableIdent, plan_id: &str, grace: Duration) -> Self {
        Self {
            catalog: Some(catalog),
            table,
            plan_id: plan_id.to_string(),
            grace,
        }
    }

    fn disarm(&mut self) {
        self.catalog = None;
    }
}

impl Drop for PlanAbandonGuard {
    fn drop(&mut self) {
        let Some(catalog) = self.catalog.take() else {
            return;
        };
        let table = self.table.clone();
        let plan_id = self.plan_id.clone();
        let grace = self.grace;
        let runtime = catalog.runtime().clone();
        drop(runtime.io().spawn(async move {
            let _ = tokio::time::timeout(grace, catalog.cancel_planning(&table, &plan_id)).await;
        }));
    }
}

enum PollError {
    Retry {
        retry_after: Option<Duration>,
        error: Error,
    },
    Terminal(Error),
}

enum PlanNotFoundKind {
    Submit,
    Fetch,
    Tasks,
}

async fn map_plan_not_found(response: Response, kind: PlanNotFoundKind) -> Error {
    let bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(err) => return err.into(),
    };
    let err_type = serde_json::from_slice::<ErrorResponse>(&bytes)
        .ok()
        .map(|parsed| parsed.error_type().to_string());
    match (kind, err_type.as_deref()) {
        (_, Some(ERR_TYPE_NO_SUCH_TABLE)) => Error::new(
            ErrorKind::TableNotFound,
            "Tried to plan a table that does not exist",
        ),
        (_, Some(ERR_TYPE_NO_SUCH_NAMESPACE)) => Error::new(
            ErrorKind::NamespaceNotFound,
            "Tried to plan a table in a namespace that does not exist",
        ),
        (PlanNotFoundKind::Fetch, Some(ERR_TYPE_NO_SUCH_PLAN_ID)) => {
            Error::new(ErrorKind::Unexpected, MSG_PLAN_EXPIRED)
        }
        (PlanNotFoundKind::Tasks, Some(ERR_TYPE_NO_SUCH_PLAN_TASK)) => {
            Error::new(ErrorKind::Unexpected, MSG_NO_SUCH_PLAN_TASK)
        }
        _ => Error::new(
            ErrorKind::Unexpected,
            "Received response with unexpected status code",
        )
        .with_context("status", "404")
        .with_context("json", String::from_utf8_lossy(&bytes)),
    }
}

fn failed_plan_error(detail: Option<&ErrorModel>) -> Error {
    match detail {
        Some(detail) if !detail.message.is_empty() => Error::new(
            ErrorKind::Unexpected,
            format!("{}: {}", MSG_PLAN_FAILED, detail.message),
        )
        .with_context("type", detail.r#type.clone()),
        _ => Error::new(ErrorKind::Unexpected, MSG_PLAN_FAILED),
    }
}

fn scan_planning_headers(
    idempotency_key: Option<&str>,
    access_delegation: Option<&str>,
    include_idempotency: bool,
) -> Result<Vec<(&'static str, String)>> {
    let mut headers = Vec::with_capacity(2);
    if include_idempotency {
        headers.push((
            HEADER_IDEMPOTENCY_KEY,
            idempotency_header_value(idempotency_key)?,
        ));
    }
    if let Some(delegation) = access_delegation {
        headers.push((HEADER_ACCESS_DELEGATION, delegation.to_string()));
    }
    Ok(headers)
}

fn idempotency_header_value(idempotency_key: Option<&str>) -> Result<String> {
    match idempotency_key {
        None => Ok(Uuid::now_v7().to_string()),
        Some(key) => {
            let parsed = Uuid::parse_str(key).map_err(|_| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("invalid idempotency key {key:?}"),
                )
            })?;
            if !parsed.to_string().eq_ignore_ascii_case(key) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("idempotency key {key:?} must be a canonical hyphenated UUID"),
                ));
            }
            if parsed.get_version() != Some(Version::SortRand)
                || parsed.get_variant() != Variant::RFC4122
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("idempotency key {key:?} must be an RFC 4122 UUIDv7"),
                ));
            }
            Ok(key.to_string())
        }
    }
}

fn escape_opaque_path_segment(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    match out.as_str() {
        "." => "%2E".to_string(),
        ".." => "%2E%2E".to_string(),
        _ => out,
    }
}

fn resolve_wait_options(opts: &WaitForPlanOptions) -> (Duration, Duration, Duration, u32, bool) {
    let mut min_delay = opts.min_delay;
    let mut max_delay = opts.max_delay;
    let mut grace = opts.cancel_grace_period;
    let mut max_retries = opts.max_retries;
    if min_delay.is_zero() {
        min_delay = Duration::from_millis(100);
    }
    if max_delay.is_zero() {
        max_delay = Duration::from_secs(5);
    }
    if max_delay < min_delay {
        max_delay = min_delay;
    }
    if grace.is_zero() {
        grace = Duration::from_secs(5);
    }
    if max_retries == 0 && opts.timeout.is_none() {
        max_retries = 10;
    }
    let clamp_retry_after = opts.timeout.is_none();
    (min_delay, max_delay, grace, max_retries, clamp_retry_after)
}

fn next_scan_plan_backoff(prev: Duration, min_delay: Duration, max_delay: Duration) -> Duration {
    if min_delay >= max_delay {
        return min_delay;
    }
    let mut ceiling = max_delay;
    if prev <= max_delay / 3 {
        ceiling = prev * 3;
        if ceiling < min_delay {
            ceiling = min_delay;
        }
        if ceiling > max_delay {
            ceiling = max_delay;
        }
    }
    let lo = min_delay.as_nanos().min(u64::MAX as u128) as u64;
    let hi = ceiling.as_nanos().min(u64::MAX as u128) as u64;
    Duration::from_nanos(rand::rng().random_range(lo..=hi))
}

fn apply_retry_after(
    backoff: Duration,
    retry_after: Option<Duration>,
    min_delay: Duration,
    max_delay: Duration,
    clamp_to_max: bool,
) -> Duration {
    let Some(retry_after) = retry_after.filter(|d| !d.is_zero()) else {
        return backoff;
    };
    let delay = retry_after.max(min_delay);
    if clamp_to_max {
        delay.min(max_delay)
    } else {
        delay
    }
}

fn parse_retry_after(value: Option<&reqwest::header::HeaderValue>) -> Option<Duration> {
    let value = value?.to_str().ok()?.trim();
    if let Ok(seconds) = value.parse::<u64>() {
        if seconds == 0 {
            return None;
        }
        // Duration::from_secs panics when secs * 1e9 overflows u64.
        const MAX_SECS: u64 = u64::MAX / 1_000_000_000;
        if seconds > MAX_SECS {
            return None;
        }
        return Some(Duration::from_secs(seconds));
    }
    // RFC 9110 HTTP-date, IMF-fixdate form (RFC 2822). Obsolete RFC 850 /
    // asctime values are ignored rather than adding another parser.
    let retry_at = chrono::DateTime::parse_from_rfc2822(value).ok()?;
    retry_at
        .signed_duration_since(chrono::Utc::now())
        .to_std()
        .ok()
        .filter(|d| !d.is_zero())
}

fn require_plan_id(plan_id: &str) -> Result<()> {
    if plan_id.is_empty() {
        Err(Error::new(ErrorKind::DataInvalid, "empty plan-id"))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::TryStreamExt;
    use iceberg::io::LocalFsStorageFactory;
    use iceberg::scan::ScanPlanningMode;
    use iceberg::{NamespaceIdent, Runtime, SessionCatalog, SessionContext};
    use mockito::Server;
    use serde_json::json;
    use uuid::Uuid;

    use super::*;
    use crate::catalog::RestCatalogConfig;

    fn table() -> TableIdent {
        TableIdent::new(NamespaceIdent::new("ns".into()), "tbl".into())
    }

    fn catalog(uri: &str) -> RestSessionCatalog {
        RestSessionCatalog::new(
            RestCatalogConfig::builder().uri(uri.to_string()).build(),
            None,
            None,
            Runtime::current(),
            None,
        )
    }

    async fn config_with_endpoints(
        server: &mut mockito::ServerGuard,
        endpoints: &[&str],
    ) -> mockito::Mock {
        let body = json!({
            "overrides": {},
            "defaults": {},
            "endpoints": endpoints,
        });
        server
            .mock("GET", "/v1/config")
            .with_status(200)
            .with_body(body.to_string())
            .expect_at_least(1)
            .create_async()
            .await
    }

    const ALL_PLAN: &[&str] = &[
        "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan",
        "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}",
        "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}",
        "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks",
    ];

    #[test]
    fn plan_table_scan_requires_plan_id_on_completed_and_submitted() {
        let completed = json!({"status": "completed"});
        assert!(serde_json::from_value::<PlanTableScanResponse>(completed).is_err());
        let submitted = json!({"status": "submitted", "plan-id": "p1"});
        let parsed: PlanTableScanResponse = serde_json::from_value(submitted).unwrap();
        assert_eq!(parsed.status, PlanStatus::Submitted);
        assert_eq!(parsed.plan_id.as_deref(), Some("p1"));
    }

    #[test]
    fn plan_table_scan_rejects_cancelled() {
        let body = json!({"status": "cancelled", "plan-id": "p1"});
        assert!(serde_json::from_value::<PlanTableScanResponse>(body).is_err());
    }

    #[test]
    fn fetch_result_accepts_cancelled_and_failed_without_error() {
        let cancelled: FetchPlanningResultResponse =
            serde_json::from_value(json!({"status": "cancelled"})).unwrap();
        assert_eq!(cancelled.status, PlanStatus::Cancelled);
        let failed: FetchPlanningResultResponse =
            serde_json::from_value(json!({"status": "failed", "error": "not-an-object"})).unwrap();
        assert_eq!(failed.status, PlanStatus::Failed);
        assert!(failed.error.is_none());
    }

    #[test]
    fn completed_plan_round_trips_opaque_tasks_and_credentials() {
        let body = json!({
            "status": "completed",
            "plan-id": "p1",
            "plan-tasks": ["t1"],
            "file-scan-tasks": [{"data-file": {
                "file-path": "s3://b/f.parquet",
                "file-format": "PARQUET",
                "file-size-in-bytes": 1
            }}],
            "storage-credentials": [{"prefix": "s3://b/", "config": {"s3.access-key-id": "k"}}]
        });
        let parsed: PlanTableScanResponse = serde_json::from_value(body.clone()).unwrap();
        assert_eq!(parsed.scan_tasks.plan_tasks, ["t1"]);
        assert_eq!(
            parsed.scan_tasks.file_scan_tasks[0].data_file.file_path,
            "s3://b/f.parquet"
        );
        assert_eq!(
            parsed.storage_credentials.as_ref().unwrap()[0].prefix,
            "s3://b/"
        );
        let encoded = serde_json::to_value(&parsed).unwrap();
        assert_eq!(encoded["plan-id"], "p1");
        assert_eq!(encoded["plan-tasks"][0], "t1");
    }

    #[test]
    fn plan_request_omits_header_fields_from_json() {
        let req = PlanTableScanRequest {
            idempotency_key: Some("not-on-wire".into()),
            access_delegation: Some("vended-credentials".into()),
            snapshot_id: Some(7),
            ..Default::default()
        };
        let value = serde_json::to_value(&req).unwrap();
        assert_eq!(value["snapshot-id"], 7);
        assert!(value.get("idempotency-key").is_none());
        assert!(value.get("access-delegation").is_none());
    }

    #[test]
    fn idempotency_key_must_be_canonical_uuidv7() {
        let v7 = Uuid::now_v7().to_string();
        assert!(idempotency_header_value(Some(&v7)).is_ok());
        assert!(idempotency_header_value(Some(&v7.to_uppercase())).is_ok());
        let v4 = Uuid::new_v4().to_string();
        assert!(idempotency_header_value(Some(&v4)).is_err());
        assert!(idempotency_header_value(Some("not-a-uuid")).is_err());
        let unhyphenated: String = v7.chars().filter(|c| *c != '-').collect();
        assert!(idempotency_header_value(Some(&unhyphenated)).is_err());
    }

    #[test]
    fn opaque_plan_id_is_a_single_path_segment() {
        assert_eq!(escape_opaque_path_segment("a/b"), "a%2Fb");
        assert_eq!(escape_opaque_path_segment("."), "%2E");
        assert_eq!(escape_opaque_path_segment(".."), "%2E%2E");
        assert_eq!(escape_opaque_path_segment("plain"), "plain");
    }

    #[test]
    fn parse_retry_after_ignores_zero_and_overflow() {
        let zero = reqwest::header::HeaderValue::from_static("0");
        assert!(parse_retry_after(Some(&zero)).is_none());
        let huge = reqwest::header::HeaderValue::from_static("99999999999");
        assert!(parse_retry_after(Some(&huge)).is_none());
        let ok = reqwest::header::HeaderValue::from_static("2");
        assert_eq!(parse_retry_after(Some(&ok)), Some(Duration::from_secs(2)));
        let padded = reqwest::header::HeaderValue::from_static(" 2 ");
        assert_eq!(
            parse_retry_after(Some(&padded)),
            Some(Duration::from_secs(2))
        );
        let invalid = reqwest::header::HeaderValue::from_static("not-a-date");
        assert!(parse_retry_after(Some(&invalid)).is_none());
    }

    #[test]
    fn parse_retry_after_accepts_http_date() {
        let past = reqwest::header::HeaderValue::from_static("Wed, 21 Oct 2015 07:28:00 GMT");
        assert!(parse_retry_after(Some(&past)).is_none());
        let future = reqwest::header::HeaderValue::from_static("Sun, 16 Aug 2099 12:00:00 GMT");
        let delay = parse_retry_after(Some(&future)).unwrap();
        assert!(delay > Duration::from_secs(60 * 60 * 24 * 365));
    }

    #[test]
    fn apply_retry_after_skips_max_clamp_when_a_deadline_is_set() {
        let retry = Some(Duration::from_secs(30));
        let min = Duration::from_millis(100);
        let max = Duration::from_secs(5);
        assert_eq!(apply_retry_after(min, retry, min, max, true), max);
        assert_eq!(
            apply_retry_after(min, retry, min, max, false),
            Duration::from_secs(30)
        );
    }

    #[tokio::test]
    async fn default_config_does_not_advertise_plan() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, &[]).await;
        // Empty endpoints list falls back to DEFAULT_ENDPOINTS.
        let catalog = catalog(&server.url());
        assert!(!catalog.supports_plan_table_scan().await.unwrap());
        assert!(!catalog.supports_full_remote_scan_planning().await.unwrap());
        assert!(!catalog.supports_remote_scan_planning().await.unwrap());
        config.assert_async().await;
    }

    #[tokio::test]
    async fn remote_scan_planning_is_true_when_all_endpoints_are_advertised() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let catalog = catalog(&server.url());
        assert!(catalog.supports_plan_table_scan().await.unwrap());
        assert!(catalog.supports_full_remote_scan_planning().await.unwrap());
        assert!(catalog.supports_remote_scan_planning().await.unwrap());
        config.assert_async().await;
    }

    #[tokio::test]
    async fn plan_table_scan_is_feature_unsupported_when_not_advertised() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, &["GET /v1/{prefix}/namespaces"]).await;
        let catalog = catalog(&server.url());
        let err = catalog
            .plan_table_scan(&table(), PlanTableScanRequest::default())
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);
        config.assert_async().await;
    }

    #[tokio::test]
    async fn plan_table_scan_completed_sends_uuidv7_idempotency_key() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let plan = server
            .mock("POST", "/v1/namespaces/ns/tables/tbl/plan")
            .match_header(
                "idempotency-key",
                mockito::Matcher::Regex(
                    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-7[0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
                        .into(),
                ),
            )
            .with_status(200)
            .with_body(r#"{"status":"completed","plan-id":"p1","file-scan-tasks":[]}"#)
            .create_async()
            .await;
        let catalog = catalog(&server.url());
        let resp = catalog
            .plan_table_scan(&table(), PlanTableScanRequest::default())
            .await
            .unwrap();
        assert_eq!(resp.status, PlanStatus::Completed);
        assert_eq!(resp.plan_id.as_deref(), Some("p1"));
        config.assert_async().await;
        plan.assert_async().await;
    }

    #[tokio::test]
    async fn plan_table_scan_failed_is_an_error() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let plan = server
            .mock("POST", "/v1/namespaces/ns/tables/tbl/plan")
            .with_status(200)
            .with_body(r#"{"status":"failed","error":{"message":"boom","type":"IcebergException","code":500}}"#)
            .create_async()
            .await;
        let catalog = catalog(&server.url());
        let err = catalog
            .plan_table_scan(&table(), PlanTableScanRequest::default())
            .await
            .unwrap_err();
        assert!(is_plan_failed(&err));
        assert!(err.message().contains("boom"));
        config.assert_async().await;
        plan.assert_async().await;
    }

    #[tokio::test]
    async fn plan_404_splits_table_from_unrecognized() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let missing = server
            .mock("POST", "/v1/namespaces/ns/tables/tbl/plan")
            .with_status(404)
            .with_body(r#"{"error":{"message":"gone","type":"NoSuchTableException","code":404}}"#)
            .create_async()
            .await;
        let catalog = catalog(&server.url());
        let err = catalog
            .plan_table_scan(&table(), PlanTableScanRequest::default())
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::TableNotFound);
        config.assert_async().await;
        missing.assert_async().await;
    }

    #[tokio::test]
    async fn plan_404_splits_namespace() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let missing = server
            .mock("POST", "/v1/namespaces/ns/tables/tbl/plan")
            .with_status(404)
            .with_body(
                r#"{"error":{"message":"gone","type":"NoSuchNamespaceException","code":404}}"#,
            )
            .create_async()
            .await;
        let catalog = catalog(&server.url());
        let err = catalog
            .plan_table_scan(&table(), PlanTableScanRequest::default())
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NamespaceNotFound);
        config.assert_async().await;
        missing.assert_async().await;
    }

    #[tokio::test]
    async fn fetch_planning_result_cancelled_is_an_error() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let fetch = server
            .mock("GET", "/v1/namespaces/ns/tables/tbl/plan/p1")
            .with_status(200)
            .with_body(r#"{"status":"cancelled"}"#)
            .create_async()
            .await;
        let catalog = catalog(&server.url());
        let err = catalog
            .fetch_planning_result(&table(), "p1", FetchPlanningResultOptions::default())
            .await
            .unwrap_err();
        assert!(is_plan_cancelled(&err));
        config.assert_async().await;
        fetch.assert_async().await;
    }

    #[tokio::test]
    async fn fetch_planning_result_failed_is_an_error() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let fetch = server
            .mock("GET", "/v1/namespaces/ns/tables/tbl/plan/p1")
            .with_status(200)
            .with_body(r#"{"status":"failed","error":{"message":"boom","type":"IcebergException","code":500}}"#)
            .create_async()
            .await;
        let catalog = catalog(&server.url());
        let err = catalog
            .fetch_planning_result(&table(), "p1", FetchPlanningResultOptions::default())
            .await
            .unwrap_err();
        assert!(is_plan_failed(&err));
        config.assert_async().await;
        fetch.assert_async().await;
    }

    #[tokio::test]
    async fn fetch_result_expired_plan_id() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let fetch = server
            .mock("GET", "/v1/namespaces/ns/tables/tbl/plan/p1")
            .with_status(404)
            .with_body(
                r#"{"error":{"message":"expired","type":"NoSuchPlanIdException","code":404}}"#,
            )
            .create_async()
            .await;
        let catalog = catalog(&server.url());
        let err = catalog
            .fetch_planning_result(&table(), "p1", FetchPlanningResultOptions::default())
            .await
            .unwrap_err();
        assert!(is_plan_expired(&err));
        config.assert_async().await;
        fetch.assert_async().await;
    }

    #[tokio::test]
    async fn fetch_scan_tasks_and_empty_body() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let tasks = server
            .mock("POST", "/v1/namespaces/ns/tables/tbl/tasks")
            .match_body(r#"{"plan-task":"h1"}"#)
            .with_status(200)
            .with_body(
                r#"{"plan-tasks":["h2"],"file-scan-tasks":[{"data-file":{"file-path":"f","file-format":"PARQUET","file-size-in-bytes":1}}]}"#,
            )
            .create_async()
            .await;
        let catalog = catalog(&server.url());
        let resp = catalog
            .fetch_scan_tasks(&table(), FetchScanTasksRequest {
                idempotency_key: None,
                plan_task: "h1".into(),
            })
            .await
            .unwrap();
        assert_eq!(resp.scan_tasks.plan_tasks, ["h2"]);
        config.assert_async().await;
        tasks.assert_async().await;
    }

    #[tokio::test]
    async fn fetch_scan_tasks_rejects_empty_200() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let tasks = server
            .mock("POST", "/v1/namespaces/ns/tables/tbl/tasks")
            .with_status(200)
            .with_body("")
            .create_async()
            .await;
        let catalog = catalog(&server.url());
        let err = catalog
            .fetch_scan_tasks(&table(), FetchScanTasksRequest {
                idempotency_key: None,
                plan_task: "h1".into(),
            })
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        config.assert_async().await;
        tasks.assert_async().await;
    }

    #[tokio::test]
    async fn fetch_scan_tasks_no_such_plan_task() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let tasks = server
            .mock("POST", "/v1/namespaces/ns/tables/tbl/tasks")
            .with_status(404)
            .with_body(
                r#"{"error":{"message":"gone","type":"NoSuchPlanTaskException","code":404}}"#,
            )
            .create_async()
            .await;
        let catalog = catalog(&server.url());
        let err = catalog
            .fetch_scan_tasks(&table(), FetchScanTasksRequest {
                idempotency_key: None,
                plan_task: "h1".into(),
            })
            .await
            .unwrap_err();
        assert!(is_no_such_plan_task(&err));
        config.assert_async().await;
        tasks.assert_async().await;
    }

    #[tokio::test]
    async fn wait_for_plan_polls_submitted_then_completes() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let first = server
            .mock("GET", "/v1/namespaces/ns/tables/tbl/plan/p1")
            .with_status(200)
            .with_body(r#"{"status":"submitted"}"#)
            .expect(1)
            .create_async()
            .await;
        let second = server
            .mock("GET", "/v1/namespaces/ns/tables/tbl/plan/p1")
            .with_status(200)
            .with_body(r#"{"status":"completed","plan-tasks":["h1"]}"#)
            .expect(1)
            .create_async()
            .await;
        let catalog = catalog(&server.url());
        let result = catalog
            .wait_for_plan(&table(), "p1", WaitForPlanOptions {
                min_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(result.scan_tasks.plan_tasks, ["h1"]);
        config.assert_async().await;
        first.assert_async().await;
        second.assert_async().await;
    }

    #[tokio::test]
    async fn wait_for_plan_propagates_cancelled() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let poll = server
            .mock("GET", "/v1/namespaces/ns/tables/tbl/plan/p1")
            .with_status(200)
            .with_body(r#"{"status":"cancelled"}"#)
            .expect(1)
            .create_async()
            .await;
        let catalog = catalog(&server.url());
        let err = catalog
            .wait_for_plan(&table(), "p1", WaitForPlanOptions {
                min_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(is_plan_cancelled(&err));
        config.assert_async().await;
        poll.assert_async().await;
    }

    #[tokio::test]
    async fn wait_for_plan_propagates_expired() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let poll = server
            .mock("GET", "/v1/namespaces/ns/tables/tbl/plan/p1")
            .with_status(404)
            .with_body(
                r#"{"error":{"message":"expired","type":"NoSuchPlanIdException","code":404}}"#,
            )
            .expect(1)
            .create_async()
            .await;
        let catalog = catalog(&server.url());
        let err = catalog
            .wait_for_plan(&table(), "p1", WaitForPlanOptions {
                min_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(is_plan_expired(&err));
        config.assert_async().await;
        poll.assert_async().await;
    }

    #[tokio::test]
    async fn wait_for_plan_propagates_failed() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let poll = server
            .mock("GET", "/v1/namespaces/ns/tables/tbl/plan/p1")
            .with_status(200)
            .with_body(r#"{"status":"failed","error":{"message":"boom","type":"IcebergException","code":500}}"#)
            .expect(1)
            .create_async()
            .await;
        let catalog = catalog(&server.url());
        let err = catalog
            .wait_for_plan(&table(), "p1", WaitForPlanOptions {
                min_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(is_plan_failed(&err));
        config.assert_async().await;
        poll.assert_async().await;
    }

    #[tokio::test]
    async fn wait_for_plan_retries_java_idempotent_get_statuses() {
        for status in [408, 429, 500, 502, 503, 504] {
            let mut server = Server::new_async().await;
            let config = config_with_endpoints(&mut server, ALL_PLAN).await;
            let busy = server
                .mock("GET", "/v1/namespaces/ns/tables/tbl/plan/p1")
                .with_status(status)
                .expect(1)
                .create_async()
                .await;
            let done = server
                .mock("GET", "/v1/namespaces/ns/tables/tbl/plan/p1")
                .with_status(200)
                .with_body(r#"{"status":"completed"}"#)
                .expect(1)
                .create_async()
                .await;
            let catalog = catalog(&server.url());
            let result = catalog
                .wait_for_plan(&table(), "p1", WaitForPlanOptions {
                    min_delay: Duration::from_millis(1),
                    max_delay: Duration::from_millis(1),
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(result.status, PlanStatus::Completed, "status {status}");
            config.assert_async().await;
            busy.assert_async().await;
            done.assert_async().await;
        }
    }

    #[tokio::test]
    async fn wait_for_plan_retries_503_then_completes() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let busy = server
            .mock("GET", "/v1/namespaces/ns/tables/tbl/plan/p1")
            .with_status(503)
            .with_header("Retry-After", "0")
            .expect(1)
            .create_async()
            .await;
        let done = server
            .mock("GET", "/v1/namespaces/ns/tables/tbl/plan/p1")
            .with_status(200)
            .with_body(r#"{"status":"completed"}"#)
            .expect(1)
            .create_async()
            .await;
        let catalog = catalog(&server.url());
        let result = catalog
            .wait_for_plan(&table(), "p1", WaitForPlanOptions {
                min_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(result.status, PlanStatus::Completed);
        config.assert_async().await;
        busy.assert_async().await;
        done.assert_async().await;
    }

    #[tokio::test]
    async fn wait_for_plan_cancels_after_max_retries() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let poll = server
            .mock("GET", "/v1/namespaces/ns/tables/tbl/plan/p1")
            .with_status(200)
            .with_body(r#"{"status":"submitted"}"#)
            .expect(2)
            .create_async()
            .await;
        let cancel = server
            .mock("DELETE", "/v1/namespaces/ns/tables/tbl/plan/p1")
            .with_status(204)
            .expect(1)
            .create_async()
            .await;
        let catalog = catalog(&server.url());
        let err = catalog
            .wait_for_plan(&table(), "p1", WaitForPlanOptions {
                min_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
                max_retries: 1,
                cancel_grace_period: Duration::from_secs(1),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(is_plan_poll_exhausted(&err));
        config.assert_async().await;
        poll.assert_async().await;
        cancel.assert_async().await;
    }

    #[tokio::test]
    async fn wait_for_plan_cancels_after_timeout() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let poll = server
            .mock("GET", "/v1/namespaces/ns/tables/tbl/plan/p1")
            .with_status(200)
            .with_body(r#"{"status":"submitted"}"#)
            .expect_at_least(1)
            .create_async()
            .await;
        let cancel = server
            .mock("DELETE", "/v1/namespaces/ns/tables/tbl/plan/p1")
            .with_status(204)
            .expect(1)
            .create_async()
            .await;
        let catalog = catalog(&server.url());
        let err = catalog
            .wait_for_plan(&table(), "p1", WaitForPlanOptions {
                min_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
                timeout: Some(Duration::from_millis(80)),
                max_retries: 0,
                cancel_grace_period: Duration::from_secs(1),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(is_plan_poll_exhausted(&err));
        config.assert_async().await;
        poll.assert_async().await;
        cancel.assert_async().await;
    }

    #[tokio::test]
    async fn wait_for_plan_cancels_when_future_is_dropped() {
        let mut server = Server::new_async().await;
        let body = json!({
            "overrides": {},
            "defaults": {},
            "endpoints": ALL_PLAN,
        });
        let config = server
            .mock("GET", "/v1/config")
            .with_status(200)
            .with_body(body.to_string())
            .expect_at_least(1)
            .create_async()
            .await;
        let poll = server
            .mock("GET", "/v1/namespaces/ns/tables/tbl/plan/p1")
            .with_status(200)
            .with_body(r#"{"status":"submitted"}"#)
            .expect_at_least(1)
            .create_async()
            .await;
        let cancel = server
            .mock("DELETE", "/v1/namespaces/ns/tables/tbl/plan/p1")
            .with_status(204)
            .expect(1)
            .create_async()
            .await;
        let catalog = catalog(&server.url());
        let table = table();
        let wait = catalog.wait_for_plan(&table, "p1", WaitForPlanOptions {
            min_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
            max_retries: 50,
            cancel_grace_period: Duration::from_secs(1),
            ..Default::default()
        });
        let _ = tokio::time::timeout(Duration::from_millis(80), wait).await;
        tokio::time::sleep(Duration::from_millis(250)).await;
        config.assert_async().await;
        poll.assert_async().await;
        cancel.assert_async().await;
    }

    #[tokio::test]
    async fn wait_for_plan_timeout_zero_retries_keeps_polling() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let pending = server
            .mock("GET", "/v1/namespaces/ns/tables/tbl/plan/p1")
            .with_status(200)
            .with_body(r#"{"status":"submitted"}"#)
            .expect(11)
            .create_async()
            .await;
        let done = server
            .mock("GET", "/v1/namespaces/ns/tables/tbl/plan/p1")
            .with_status(200)
            .with_body(r#"{"status":"completed"}"#)
            .expect(1)
            .create_async()
            .await;
        let catalog = catalog(&server.url());
        let result = catalog
            .wait_for_plan(&table(), "p1", WaitForPlanOptions {
                min_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
                max_retries: 0,
                timeout: Some(Duration::from_secs(5)),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(result.status, PlanStatus::Completed);
        config.assert_async().await;
        pending.assert_async().await;
        done.assert_async().await;
    }

    #[tokio::test]
    async fn wait_for_plan_huge_retry_after_does_not_panic() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let busy = server
            .mock("GET", "/v1/namespaces/ns/tables/tbl/plan/p1")
            .with_status(503)
            .with_header("Retry-After", "99999999999")
            .expect(1)
            .create_async()
            .await;
        let done = server
            .mock("GET", "/v1/namespaces/ns/tables/tbl/plan/p1")
            .with_status(200)
            .with_body(r#"{"status":"completed"}"#)
            .expect(1)
            .create_async()
            .await;
        let catalog = catalog(&server.url());
        let result = catalog
            .wait_for_plan(&table(), "p1", WaitForPlanOptions {
                min_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(result.status, PlanStatus::Completed);
        config.assert_async().await;
        busy.assert_async().await;
        done.assert_async().await;
    }

    #[tokio::test]
    async fn wait_for_plan_rejects_empty_plan_id() {
        let catalog = catalog("http://127.0.0.1:1");
        let err = catalog
            .wait_for_plan(&table(), "", WaitForPlanOptions::default())
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[tokio::test]
    async fn plan_id_with_slash_stays_one_path_segment() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let fetch = server
            .mock("GET", "/v1/namespaces/ns/tables/tbl/plan/a%2Fb")
            .with_status(200)
            .with_body(r#"{"status":"submitted"}"#)
            .create_async()
            .await;
        let catalog = catalog(&server.url());
        let resp = catalog
            .fetch_planning_result(&table(), "a/b", FetchPlanningResultOptions::default())
            .await
            .unwrap();
        assert_eq!(resp.status, PlanStatus::Submitted);
        config.assert_async().await;
        fetch.assert_async().await;
    }

    #[tokio::test]
    async fn cancel_planning_accepts_204() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let cancel = server
            .mock("DELETE", "/v1/namespaces/ns/tables/tbl/plan/p1")
            .with_status(204)
            .create_async()
            .await;
        let catalog = catalog(&server.url());
        catalog.cancel_planning(&table(), "p1").await.unwrap();
        config.assert_async().await;
        cancel.assert_async().await;
    }

    fn load_catalog(uri: &str) -> RestSessionCatalog {
        RestSessionCatalog::new(
            RestCatalogConfig::builder().uri(uri.to_string()).build(),
            None,
            Some(Arc::new(LocalFsStorageFactory)),
            Runtime::current(),
            None,
        )
    }

    fn empty_context() -> SessionContext {
        SessionContext::empty()
    }

    fn test1() -> TableIdent {
        TableIdent::new(NamespaceIdent::new("ns1".into()), "test1".into())
    }

    fn data_file_json() -> serde_json::Value {
        json!({
            "content": 0,
            "file-path": "s3://warehouse/database/table/data/00000.parquet",
            "file-format": "PARQUET",
            "spec-id": 0,
            "partition": {},
            "record-count": 1,
            "file-size-in-bytes": 697
        })
    }

    async fn mock_load_table(server: &mut mockito::ServerGuard) -> mockito::Mock {
        server
            .mock("GET", "/v1/namespaces/ns1/tables/test1")
            .with_status(200)
            .with_body_from_file(format!(
                "{}/testdata/load_table_response.json",
                env!("CARGO_MANIFEST_DIR")
            ))
            .create_async()
            .await
    }

    async fn mock_load_table_with_config(
        server: &mut mockito::ServerGuard,
        config: serde_json::Value,
    ) -> mockito::Mock {
        let mut body: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(format!(
                "{}/testdata/load_table_response.json",
                env!("CARGO_MANIFEST_DIR")
            ))
            .unwrap(),
        )
        .unwrap();
        body["config"] = config;
        server
            .mock("GET", "/v1/namespaces/ns1/tables/test1")
            .with_status(200)
            .with_body(body.to_string())
            .create_async()
            .await
    }

    fn empty_table_body() -> String {
        json!({
            "metadata-location": "s3://warehouse/ns1/test1/metadata.json",
            "metadata": {
                "format-version": 2,
                "table-uuid": "b55d9dda-6561-423a-8bfc-787980ce421f",
                "location": "s3://warehouse/ns1/test1",
                "last-sequence-number": 0,
                "last-updated-ms": 1,
                "last-column-id": 2,
                "current-schema-id": 0,
                "schemas": [{
                    "type": "struct",
                    "schema-id": 0,
                    "fields": [
                        {"id": 1, "name": "id", "required": false, "type": "int"},
                        {"id": 2, "name": "data", "required": false, "type": "string"}
                    ]
                }],
                "default-spec-id": 0,
                "partition-specs": [{"spec-id": 0, "fields": []}],
                "last-partition-id": 999,
                "default-sort-order-id": 0,
                "sort-orders": [{"order-id": 0, "fields": []}],
                "current-snapshot-id": -1,
                "snapshots": []
            }
        })
        .to_string()
    }

    #[tokio::test]
    async fn table_scan_completed_plan_yields_file_scan_task() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let load = mock_load_table(&mut server).await;
        let plan = server
            .mock("POST", "/v1/namespaces/ns1/tables/test1/plan")
            .match_body(mockito::Matcher::PartialJson(json!({
                "snapshot-id": 3497810964824022504i64,
                "use-snapshot-schema": true
            })))
            .with_status(200)
            .with_body(
                json!({
                    "status": "completed",
                    "plan-id": "p1",
                    "file-scan-tasks": [{"data-file": data_file_json()}]
                })
                .to_string(),
            )
            .expect(1)
            .create_async()
            .await;
        let catalog = load_catalog(&server.url());
        let table = catalog.load_table(&empty_context(), &test1()).await.unwrap();
        let tasks: Vec<_> = table
            .scan()
            .with_scan_planning_mode(ScanPlanningMode::Remote)
            .build()
            .unwrap()
            .plan_files()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(
            tasks[0].data_file_path,
            "s3://warehouse/database/table/data/00000.parquet"
        );
        assert_eq!(tasks[0].file_size_in_bytes, 697);
        assert_eq!(
            tasks[0].data_file_format,
            iceberg::spec::DataFileFormat::Parquet
        );
        config.assert_async().await;
        load.assert_async().await;
        plan.assert_async().await;
    }

    #[tokio::test]
    async fn table_scan_submitted_plan_waits_then_completes() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let load = mock_load_table(&mut server).await;
        let plan = server
            .mock("POST", "/v1/namespaces/ns1/tables/test1/plan")
            .with_status(200)
            .with_body(r#"{"status":"submitted","plan-id":"p1"}"#)
            .expect(1)
            .create_async()
            .await;
        let pending = server
            .mock("GET", "/v1/namespaces/ns1/tables/test1/plan/p1")
            .with_status(200)
            .with_body(r#"{"status":"submitted"}"#)
            .expect(1)
            .create_async()
            .await;
        let done = server
            .mock("GET", "/v1/namespaces/ns1/tables/test1/plan/p1")
            .with_status(200)
            .with_body(
                json!({
                    "status": "completed",
                    "file-scan-tasks": [{"data-file": data_file_json()}]
                })
                .to_string(),
            )
            .expect(1)
            .create_async()
            .await;
        let catalog = load_catalog(&server.url());
        let table = catalog.load_table(&empty_context(), &test1()).await.unwrap();
        let tasks: Vec<_> = table
            .scan()
            .with_scan_planning_mode(ScanPlanningMode::Remote)
            .build()
            .unwrap()
            .plan_files()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].file_size_in_bytes, 697);
        config.assert_async().await;
        load.assert_async().await;
        plan.assert_async().await;
        pending.assert_async().await;
        done.assert_async().await;
    }

    #[tokio::test]
    async fn table_scan_expands_plan_task_handles() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let load = mock_load_table(&mut server).await;
        let plan = server
            .mock("POST", "/v1/namespaces/ns1/tables/test1/plan")
            .with_status(200)
            .with_body(r#"{"status":"completed","plan-id":"p1","plan-tasks":["h1"]}"#)
            .expect(1)
            .create_async()
            .await;
        let tasks_ep = server
            .mock("POST", "/v1/namespaces/ns1/tables/test1/tasks")
            .match_body(r#"{"plan-task":"h1"}"#)
            .with_status(200)
            .with_body(
                json!({
                    "file-scan-tasks": [{"data-file": data_file_json()}]
                })
                .to_string(),
            )
            .expect(1)
            .create_async()
            .await;
        let catalog = load_catalog(&server.url());
        let table = catalog.load_table(&empty_context(), &test1()).await.unwrap();
        let tasks: Vec<_> = table
            .scan()
            .with_scan_planning_mode(ScanPlanningMode::Remote)
            .build()
            .unwrap()
            .plan_files()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(
            tasks[0].data_file_path,
            "s3://warehouse/database/table/data/00000.parquet"
        );
        config.assert_async().await;
        load.assert_async().await;
        plan.assert_async().await;
        tasks_ep.assert_async().await;
    }

    #[tokio::test]
    async fn table_scan_reissued_plan_task_does_not_loop() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let load = mock_load_table(&mut server).await;
        let plan = server
            .mock("POST", "/v1/namespaces/ns1/tables/test1/plan")
            .with_status(200)
            .with_body(r#"{"status":"completed","plan-id":"p1","plan-tasks":["h1"]}"#)
            .expect(1)
            .create_async()
            .await;
        let tasks_ep = server
            .mock("POST", "/v1/namespaces/ns1/tables/test1/tasks")
            .with_status(200)
            .with_body(r#"{"plan-tasks":["h1"]}"#)
            .expect(1)
            .create_async()
            .await;
        let catalog = load_catalog(&server.url());
        let table = catalog.load_table(&empty_context(), &test1()).await.unwrap();
        let tasks: Vec<_> = table
            .scan()
            .with_scan_planning_mode(ScanPlanningMode::Remote)
            .build()
            .unwrap()
            .plan_files()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert!(tasks.is_empty());
        config.assert_async().await;
        load.assert_async().await;
        plan.assert_async().await;
        tasks_ep.assert_async().await;
    }

    #[tokio::test]
    async fn table_scan_empty_plan_is_empty_not_error() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let load = mock_load_table(&mut server).await;
        let plan = server
            .mock("POST", "/v1/namespaces/ns1/tables/test1/plan")
            .with_status(200)
            .with_body(r#"{"status":"completed","plan-id":"p1"}"#)
            .expect(1)
            .create_async()
            .await;
        let catalog = load_catalog(&server.url());
        let table = catalog.load_table(&empty_context(), &test1()).await.unwrap();
        let tasks: Vec<_> = table
            .scan()
            .with_scan_planning_mode(ScanPlanningMode::Remote)
            .build()
            .unwrap()
            .plan_files()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert!(tasks.is_empty());
        config.assert_async().await;
        load.assert_async().await;
        plan.assert_async().await;
    }

    #[tokio::test]
    async fn auto_mode_without_plan_endpoints_does_not_post_plan() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, &[]).await;
        let load = server
            .mock("GET", "/v1/namespaces/ns1/tables/test1")
            .with_status(200)
            .with_body(empty_table_body())
            .create_async()
            .await;
        let plan = server
            .mock("POST", "/v1/namespaces/ns1/tables/test1/plan")
            .expect(0)
            .create_async()
            .await;
        let catalog = load_catalog(&server.url());
        let table = catalog.load_table(&empty_context(), &test1()).await.unwrap();
        let tasks: Vec<_> = table
            .scan()
            .build()
            .unwrap()
            .plan_files()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert!(tasks.is_empty());
        config.assert_async().await;
        load.assert_async().await;
        plan.assert_async().await;
    }

    #[tokio::test]
    async fn remote_mode_without_endpoints_is_feature_unsupported() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, &[]).await;
        let load = server
            .mock("GET", "/v1/namespaces/ns1/tables/test1")
            .with_status(200)
            .with_body(empty_table_body())
            .create_async()
            .await;
        let catalog = load_catalog(&server.url());
        let table = catalog.load_table(&empty_context(), &test1()).await.unwrap();
        let err = match table
            .scan()
            .with_scan_planning_mode(ScanPlanningMode::Remote)
            .build()
            .unwrap()
            .plan_files()
            .await
        {
            Ok(_) => panic!("expected FeatureUnsupported"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);
        config.assert_async().await;
        load.assert_async().await;
    }

    #[tokio::test]
    async fn default_plan_files_does_not_post_plan_when_endpoints_are_advertised() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let load = mock_load_table(&mut server).await;
        let plan = server
            .mock("POST", "/v1/namespaces/ns1/tables/test1/plan")
            .expect(0)
            .create_async()
            .await;
        let catalog = load_catalog(&server.url());
        let table = catalog.load_table(&empty_context(), &test1()).await.unwrap();
        let _ = table.scan().build().unwrap().plan_files().await;
        config.assert_async().await;
        load.assert_async().await;
        plan.assert_async().await;
    }

    #[tokio::test]
    async fn auto_mode_with_client_config_does_not_post_plan() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let load =
            mock_load_table_with_config(&mut server, json!({"scan-planning-mode": "client"})).await;
        let plan = server
            .mock("POST", "/v1/namespaces/ns1/tables/test1/plan")
            .expect(0)
            .create_async()
            .await;
        let catalog = load_catalog(&server.url());
        let table = catalog.load_table(&empty_context(), &test1()).await.unwrap();
        let _ = table
            .scan()
            .with_scan_planning_mode(ScanPlanningMode::Auto)
            .build()
            .unwrap()
            .plan_files()
            .await;
        config.assert_async().await;
        load.assert_async().await;
        plan.assert_async().await;
    }

    #[tokio::test]
    async fn auto_mode_with_server_config_posts_plan() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let load =
            mock_load_table_with_config(&mut server, json!({"scan-planning-mode": "server"})).await;
        let plan = server
            .mock("POST", "/v1/namespaces/ns1/tables/test1/plan")
            .with_status(200)
            .with_body(
                json!({
                    "status": "completed",
                    "plan-id": "p1",
                    "file-scan-tasks": [{"data-file": data_file_json()}]
                })
                .to_string(),
            )
            .expect(1)
            .create_async()
            .await;
        let catalog = load_catalog(&server.url());
        let table = catalog.load_table(&empty_context(), &test1()).await.unwrap();
        let tasks: Vec<_> = table
            .scan()
            .with_scan_planning_mode(ScanPlanningMode::Auto)
            .build()
            .unwrap()
            .plan_files()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(tasks.len(), 1);
        config.assert_async().await;
        load.assert_async().await;
        plan.assert_async().await;
    }

    #[tokio::test]
    async fn delete_file_references_are_resolved_per_payload() {
        let mut server = Server::new_async().await;
        let config = config_with_endpoints(&mut server, ALL_PLAN).await;
        let load = mock_load_table(&mut server).await;
        let plan = server
            .mock("POST", "/v1/namespaces/ns1/tables/test1/plan")
            .with_status(200)
            .with_body(
                json!({
                    "status": "completed",
                    "plan-id": "p1",
                    "plan-tasks": ["h1"],
                    "delete-files": [{
                        "content": "position-deletes",
                        "file-path": "s3://warehouse/decoy.parquet",
                        "file-format": "PARQUET",
                        "spec-id": 0,
                        "file-size-in-bytes": 3
                    }]
                })
                .to_string(),
            )
            .expect(1)
            .create_async()
            .await;
        let tasks_ep = server
            .mock("POST", "/v1/namespaces/ns1/tables/test1/tasks")
            .match_body(r#"{"plan-task":"h1"}"#)
            .with_status(200)
            .with_body(
                json!({
                    "file-scan-tasks": [{
                        "data-file": data_file_json(),
                        "delete-file-references": [0]
                    }],
                    "delete-files": [{
                        "content": "position-deletes",
                        "file-path": "s3://warehouse/d1.parquet",
                        "file-format": "PARQUET",
                        "spec-id": 0,
                        "file-size-in-bytes": 3
                    }]
                })
                .to_string(),
            )
            .expect(1)
            .create_async()
            .await;
        let catalog = load_catalog(&server.url());
        let table = catalog.load_table(&empty_context(), &test1()).await.unwrap();
        let tasks: Vec<_> = table
            .scan()
            .with_scan_planning_mode(ScanPlanningMode::Remote)
            .build()
            .unwrap()
            .plan_files()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].deletes.len(), 1);
        assert_eq!(tasks[0].deletes[0].file_path, "s3://warehouse/d1.parquet");
        config.assert_async().await;
        load.assert_async().await;
        plan.assert_async().await;
        tasks_ep.assert_async().await;
    }

    #[tokio::test]
    async fn load_table_then_auto_plan_hits_config_once() {
        let mut server = Server::new_async().await;
        let config = server
            .mock("GET", "/v1/config")
            .with_status(200)
            .with_body(
                json!({
                    "overrides": {},
                    "defaults": {},
                    "endpoints": []
                })
                .to_string(),
            )
            .expect(1)
            .create_async()
            .await;
        let load = mock_load_table(&mut server).await;
        let catalog = load_catalog(&server.url());
        let table = catalog.load_table(&empty_context(), &test1()).await.unwrap();
        let _ = table
            .scan()
            .with_scan_planning_mode(ScanPlanningMode::Auto)
            .build()
            .unwrap()
            .plan_files()
            .await;
        config.assert_async().await;
        load.assert_async().await;
    }
}
