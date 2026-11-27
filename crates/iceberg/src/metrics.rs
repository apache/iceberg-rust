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

//! Metrics reporting API for Iceberg table operations.

use std::collections::HashMap;
use std::fmt::Debug;
use std::time::Duration;

use async_trait::async_trait;

use crate::TableIdent;
use crate::expr::Predicate;
use crate::spec::SchemaId;

/// Receives metrics reports for completed table operations.
///
/// Implementations must handle reporting failures internally so they do not affect the operation
/// that produced the report.
#[async_trait]
pub trait MetricsReporter: Debug + Send + Sync {
    /// Reports the metrics for a completed table operation.
    async fn report(&self, report: &MetricsReport);
}

/// A metrics report for an Iceberg table operation.
#[derive(Debug)]
#[non_exhaustive]
pub enum MetricsReport {
    /// A report produced by planning a table scan.
    Scan(ScanReport),
}

/// Information and metrics collected while planning a table scan.
#[derive(Debug)]
pub struct ScanReport {
    pub(crate) table_ident: TableIdent,
    pub(crate) snapshot_id: i64,
    pub(crate) schema_id: SchemaId,
    pub(crate) filter: Option<Predicate>,
    pub(crate) projected_field_names: Vec<String>,
    pub(crate) projected_field_ids: Vec<i32>,
    pub(crate) metrics: ScanMetrics,
    pub(crate) metadata: HashMap<String, String>,
}

impl ScanReport {
    /// Returns the identifier of the scanned table.
    pub fn table_ident(&self) -> &TableIdent {
        &self.table_ident
    }

    /// Returns the ID of the scanned snapshot.
    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    /// Returns the ID of the schema used for the scan.
    pub fn schema_id(&self) -> SchemaId {
        self.schema_id
    }

    /// Returns the scan filter, or `None` for an unfiltered scan.
    pub fn filter(&self) -> Option<&Predicate> {
        self.filter.as_ref()
    }

    /// Returns the names of the projected fields.
    pub fn projected_field_names(&self) -> &[String] {
        &self.projected_field_names
    }

    /// Returns the IDs of the projected fields.
    pub fn projected_field_ids(&self) -> &[i32] {
        &self.projected_field_ids
    }

    /// Returns the metrics collected while planning the scan.
    pub fn metrics(&self) -> &ScanMetrics {
        &self.metrics
    }

    /// Returns additional metadata attached to the report.
    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }
}

/// Metrics collected while planning a table scan.
#[derive(Debug)]
pub struct ScanMetrics {
    pub(crate) total_planning_duration: Duration,
    pub(crate) total_data_manifests: u64,
    pub(crate) total_delete_manifests: u64,
    pub(crate) skipped_data_manifests: u64,
    pub(crate) skipped_delete_manifests: u64,
    pub(crate) scanned_data_manifests: u64,
    pub(crate) scanned_delete_manifests: u64,
    pub(crate) result_data_files: u64,
    pub(crate) skipped_data_files: u64,
    pub(crate) total_file_size_in_bytes: u64,
    pub(crate) result_delete_files: u64,
    pub(crate) skipped_delete_files: u64,
    pub(crate) total_delete_file_size_in_bytes: u64,
    pub(crate) indexed_delete_files: u64,
    pub(crate) equality_delete_files: u64,
    pub(crate) positional_delete_files: u64,
    pub(crate) dvs: u64,
}

impl ScanMetrics {
    /// Returns the total duration of scan planning.
    pub fn total_planning_duration(&self) -> Duration {
        self.total_planning_duration
    }

    /// Returns the number of data manifests in the snapshot.
    pub fn total_data_manifests(&self) -> u64 {
        self.total_data_manifests
    }

    /// Returns the number of delete manifests in the snapshot.
    pub fn total_delete_manifests(&self) -> u64 {
        self.total_delete_manifests
    }

    /// Returns the number of data manifests skipped by manifest pruning.
    pub fn skipped_data_manifests(&self) -> u64 {
        self.skipped_data_manifests
    }

    /// Returns the number of delete manifests skipped by manifest pruning.
    pub fn skipped_delete_manifests(&self) -> u64 {
        self.skipped_delete_manifests
    }

    /// Returns the number of data manifests scanned.
    pub fn scanned_data_manifests(&self) -> u64 {
        self.scanned_data_manifests
    }

    /// Returns the number of delete manifests scanned.
    pub fn scanned_delete_manifests(&self) -> u64 {
        self.scanned_delete_manifests
    }

    /// Returns the number of data files selected by the scan.
    pub fn result_data_files(&self) -> u64 {
        self.result_data_files
    }

    /// Returns the number of data files skipped by file pruning.
    pub fn skipped_data_files(&self) -> u64 {
        self.skipped_data_files
    }

    /// Returns the total size of selected data files in bytes.
    pub fn total_file_size_in_bytes(&self) -> u64 {
        self.total_file_size_in_bytes
    }

    /// Returns the number of delete-file attachments across planned data files.
    pub fn result_delete_files(&self) -> u64 {
        self.result_delete_files
    }

    /// Returns the number of delete files skipped by file pruning.
    pub fn skipped_delete_files(&self) -> u64 {
        self.skipped_delete_files
    }

    /// Returns attached delete bytes, repeated per attachment.
    ///
    /// Physical file size is used for delete files and content size for deletion vectors.
    pub fn total_delete_file_size_in_bytes(&self) -> u64 {
        self.total_delete_file_size_in_bytes
    }

    /// Returns the number of delete files added to the delete index.
    pub fn indexed_delete_files(&self) -> u64 {
        self.indexed_delete_files
    }

    /// Returns the number of indexed equality delete files.
    pub fn equality_delete_files(&self) -> u64 {
        self.equality_delete_files
    }

    /// Returns the number of indexed position delete files, excluding deletion vectors.
    pub fn positional_delete_files(&self) -> u64 {
        self.positional_delete_files
    }

    /// Returns the number of indexed deletion vectors.
    pub fn dvs(&self) -> u64 {
        self.dvs
    }
}
