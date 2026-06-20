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

//! Wire types for the REST scan-planning protocol, matching the Iceberg
//! OpenAPI spec (`planTableScan` / `fetchPlanningResult` / `fetchScanTasks`).
//!
//! Only the fields needed to construct an [`iceberg::scan::FileScanTask`] are
//! modelled on the content-file objects; unknown fields (column stats, bounds,
//! key metadata, …) are ignored on deserialization since a `FileScanTask`
//! carries no statistics.

use iceberg::spec::DataContentType;
use serde_derive::{Deserialize, Serialize};

use crate::types::StorageCredential;

/// Status of a server-side scan plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum PlanStatus {
    /// Planning finished; tasks (and/or plan-task tokens) are available.
    #[serde(alias = "COMPLETED")]
    Completed,
    /// Planning is in progress; poll the plan id until it completes.
    #[serde(alias = "SUBMITTED")]
    Submitted,
    /// Planning was cancelled.
    #[serde(alias = "CANCELLED")]
    Cancelled,
    /// Planning failed.
    #[serde(alias = "FAILED")]
    Failed,
}

/// Body of `POST .../plan`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct PlanTableScanRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_snapshot_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_snapshot_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub select: Option<Vec<String>>,
    /// Filter predicate as Iceberg expression JSON (see `expr.rs`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub case_sensitive: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_snapshot_schema: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats_fields: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_rows_requested: Option<i64>,
}

/// Error payload embedded in a `failed` plan response.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct PlanError {
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default, rename = "type")]
    pub r#type: Option<String>,
    #[serde(default)]
    pub code: Option<i32>,
}

impl PlanError {
    pub(crate) fn describe(&self) -> String {
        format!(
            "{} (type={}, code={})",
            self.message.as_deref().unwrap_or("unknown"),
            self.r#type.as_deref().unwrap_or("unknown"),
            self.code.unwrap_or(0)
        )
    }
}

/// Response of `POST .../plan`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct PlanTableScanResponse {
    pub status: PlanStatus,
    #[serde(default)]
    pub error: Option<PlanError>,
    #[serde(default)]
    pub plan_id: Option<String>,
    #[serde(default)]
    pub plan_tasks: Option<Vec<String>>,
    /// Vended credentials. Parsed for protocol completeness; reads currently use
    /// the table's existing `FileIO` (see the scan-planning module docs).
    #[serde(default)]
    #[allow(dead_code)]
    pub storage_credentials: Option<Vec<StorageCredential>>,
    #[serde(default)]
    pub delete_files: Option<Vec<RestContentFile>>,
    #[serde(default)]
    pub file_scan_tasks: Option<Vec<RestFileScanTask>>,
}

/// Response of `GET .../plan/{plan-id}` (no `plan-id` — it is in the URL).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct FetchPlanningResultResponse {
    pub status: PlanStatus,
    #[serde(default)]
    pub error: Option<PlanError>,
    #[serde(default)]
    pub plan_tasks: Option<Vec<String>>,
    /// Vended credentials. Parsed for protocol completeness; reads currently use
    /// the table's existing `FileIO` (see the scan-planning module docs).
    #[serde(default)]
    #[allow(dead_code)]
    pub storage_credentials: Option<Vec<StorageCredential>>,
    #[serde(default)]
    pub delete_files: Option<Vec<RestContentFile>>,
    #[serde(default)]
    pub file_scan_tasks: Option<Vec<RestFileScanTask>>,
}

/// Body of `POST .../tasks`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct FetchScanTasksRequest {
    pub plan_task: String,
}

/// Response of `POST .../tasks`. May return further `plan-tasks` (recursive).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct FetchScanTasksResponse {
    #[serde(default)]
    pub plan_tasks: Option<Vec<String>>,
    #[serde(default)]
    pub delete_files: Option<Vec<RestContentFile>>,
    #[serde(default)]
    pub file_scan_tasks: Option<Vec<RestFileScanTask>>,
}

/// One entry in a `file-scan-tasks` array.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct RestFileScanTask {
    pub data_file: RestContentFile,
    /// Indices into the response's shared `delete-files` array.
    #[serde(default)]
    pub delete_file_references: Option<Vec<usize>>,
    /// Residual filter as Iceberg expression JSON. Parsed for protocol
    /// completeness; the client's own scan filter is authoritative for row
    /// filtering, so this is not consumed.
    #[serde(default)]
    #[allow(dead_code)]
    pub residual_filter: Option<serde_json::Value>,
}

/// The `data-file` / `delete-files[]` object (a subset of `ContentFileParser`).
///
/// Statistics/bounds/key-metadata are intentionally omitted: a
/// [`FileScanTask`](iceberg::scan::FileScanTask) does not carry them, and serde
/// ignores the unmodelled fields.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct RestContentFile {
    pub spec_id: i32,
    pub content: ContentTypeStr,
    pub file_path: String,
    /// Lowercase format string (`"parquet"`/`"avro"`/`"orc"`); parsed case-insensitively.
    pub file_format: String,
    /// Positional array (one element per partition field) or field-id-keyed object.
    #[serde(default)]
    pub partition: Option<serde_json::Value>,
    pub file_size_in_bytes: u64,
    pub record_count: u64,
    #[serde(default)]
    pub equality_ids: Option<Vec<i32>>,
}

/// Content type as a kebab-case string, tolerating legacy SCREAMING_CASE names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub(crate) enum ContentTypeStr {
    #[serde(rename = "data", alias = "DATA")]
    Data,
    #[serde(rename = "position-deletes", alias = "POSITION_DELETES")]
    PositionDeletes,
    #[serde(rename = "equality-deletes", alias = "EQUALITY_DELETES")]
    EqualityDeletes,
}

impl From<ContentTypeStr> for DataContentType {
    fn from(value: ContentTypeStr) -> Self {
        match value {
            ContentTypeStr::Data => DataContentType::Data,
            ContentTypeStr::PositionDeletes => DataContentType::PositionDeletes,
            ContentTypeStr::EqualityDeletes => DataContentType::EqualityDeletes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_status_kebab() {
        assert_eq!(
            serde_json::from_value::<PlanStatus>(serde_json::json!("completed")).unwrap(),
            PlanStatus::Completed
        );
        assert_eq!(
            serde_json::from_value::<PlanStatus>(serde_json::json!("position-deletes")).is_err(),
            true
        );
    }

    #[test]
    fn request_omits_none_fields() {
        let req = PlanTableScanRequest {
            snapshot_id: Some(42),
            case_sensitive: Some(true),
            ..Default::default()
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(v["snapshot-id"], 42);
        assert_eq!(v["case-sensitive"], true);
        assert!(v.get("select").is_none());
        assert!(v.get("filter").is_none());
    }

    #[test]
    fn content_type_kebab_and_legacy() {
        let kebab: ContentTypeStr =
            serde_json::from_value(serde_json::json!("equality-deletes")).unwrap();
        assert_eq!(kebab, ContentTypeStr::EqualityDeletes);
        let legacy: ContentTypeStr =
            serde_json::from_value(serde_json::json!("POSITION_DELETES")).unwrap();
        assert_eq!(legacy, ContentTypeStr::PositionDeletes);
    }

    #[test]
    fn content_file_ignores_unmodelled_fields() {
        let json = serde_json::json!({
            "spec-id": 0,
            "content": "data",
            "file-path": "s3://b/f.parquet",
            "file-format": "parquet",
            "file-size-in-bytes": 1024,
            "record-count": 10,
            "column-sizes": {"keys": [1], "values": [100]},
            "lower-bounds": {"keys": [1], "values": ["00000000"]},
            "key-metadata": "abcd"
        });
        let cf: RestContentFile = serde_json::from_value(json).unwrap();
        assert_eq!(cf.file_path, "s3://b/f.parquet");
        assert_eq!(cf.record_count, 10);
        assert_eq!(cf.content, ContentTypeStr::Data);
    }
}
