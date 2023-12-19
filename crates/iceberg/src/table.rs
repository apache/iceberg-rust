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

//! Table API for Apache Iceberg

use crate::error::Result;
use crate::io::FileIO;
use crate::spec::{
    DataContentType, ManifestContentType, ManifestEntry, ManifestEntryRef, SchemaRef, SnapshotRef,
    TableMetadata, TableMetadataRef, INITIAL_SEQUENCE_NUMBER,
};
use crate::{Error, ErrorKind, TableIdent};
use arrow_array::RecordBatch;
use futures::stream::{iter, BoxStream};
use futures::StreamExt;
use typed_builder::TypedBuilder;

/// Table represents a table in the catalog.
#[derive(TypedBuilder, Debug)]
pub struct Table {
    file_io: FileIO,
    #[builder(default, setter(strip_option))]
    metadata_location: Option<String>,
    #[builder(setter(into))]
    metadata: TableMetadataRef,
    identifier: TableIdent,
}

impl Table {
    /// Returns table identifier.
    pub fn identifier(&self) -> &TableIdent {
        &self.identifier
    }
    /// Returns current metadata.
    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    /// Returns current metadata ref.
    pub fn metadata_ref(&self) -> TableMetadataRef {
        self.metadata.clone()
    }

    /// Returns current metadata location.
    pub fn metadata_location(&self) -> Option<&str> {
        self.metadata_location.as_deref()
    }

    /// Creates a table scan.
    pub fn scan(&self) -> TableScanBuilder<'_> {
        TableScanBuilder::new(self)
    }
}

/// Builder to create table scan.
pub struct TableScanBuilder<'a> {
    table: &'a Table,
    // Empty column names means to select all columns
    column_names: Vec<String>,
    limit: Option<usize>,
    case_sensitive: bool,
    snapshot_id: Option<i64>,
}

impl<'a> TableScanBuilder<'a> {
    fn new(table: &'a Table) -> Self {
        Self {
            table,
            column_names: vec![],
            case_sensitive: false,
            snapshot_id: None,
            limit: None,
        }
    }

    /// Select all columns.
    pub fn select_all(mut self) -> Self {
        self.column_names.clear();
        self
    }

    /// Select some columns of the table.
    pub fn select(mut self, column_names: impl IntoIterator<Item = impl ToString>) -> Self {
        self.column_names = column_names
            .into_iter()
            .map(|item| item.to_string())
            .collect();
        self
    }

    /// Limit the number of rows returned.
    ///
    /// If not set, all rows will be returned.
    /// If set, the value must be greater than 0.
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Whether the column names are case sensitive.
    pub fn case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
        self
    }

    /// Set the snapshot to scan. When not set, it uses current snapshot.
    pub fn snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    /// Build the table scan.
    pub async fn build(self) -> Result<TableScan> {
        let snapshot = match self.snapshot_id {
            Some(snapshot_id) => self
                .table
                .metadata()
                .snapshot_by_id(snapshot_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Snapshot with id {} not found", snapshot_id),
                    )
                })?
                .clone(),
            None => self
                .table
                .metadata()
                .current_snapshot()
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::FeatureUnsupported,
                        "Can't scan table without snapshots",
                    )
                })?
                .clone(),
        };
        let schema = match snapshot.schema_id() {
            Some(schema_id) => self
                .table
                .metadata()
                .schema_by_id(schema_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Schema with id {} not found", schema_id),
                    )
                })?
                .clone(),
            None => self.table.metadata.current_schema().clone(),
        };

        Ok(TableScan {
            column_names: self.column_names.clone(),
            limit: None,
            case_sensitive: self.case_sensitive,
            snapshot,
            schema,
            file_io: self.table.file_io.clone(),
            table_metadata: self.table.metadata_ref(),
        })
    }
}

/// Table scan.
pub struct TableScan {
    column_names: Vec<String>,
    limit: Option<usize>,
    case_sensitive: bool,
    snapshot: SnapshotRef,
    schema: SchemaRef,
    table_metadata: TableMetadataRef,
    file_io: FileIO,
}

/// A stream of [`FileScanTask`].
pub type FileScanTaskStream = BoxStream<'static, Result<FileScanTask>>;
impl TableScan {
    /// Returns a stream of file scan tasks.
    pub async fn plan_files(&self) -> Result<FileScanTaskStream> {
        let manifest_list = self
            .snapshot
            .load_manifest_list(&self.file_io, &self.table_metadata)
            .await?;

        // Get minimum sequence number of data files.
        let min_data_file_seq_num = manifest_list
            .entries()
            .iter()
            .filter(|e| e.content == ManifestContentType::Data)
            .map(|e| e.min_sequence_number)
            .min()
            .unwrap_or(INITIAL_SEQUENCE_NUMBER);

        // Collect deletion files first.
        let mut position_delete_files = Vec::with_capacity(manifest_list.entries().len());
        let mut eq_delete_files = Vec::with_capacity(manifest_list.entries().len());

        // TODO: We should introduce runtime api to enable parallel scan.
        for manifest_list_entry in manifest_list.entries().iter().filter(|e| {
            e.content == ManifestContentType::Deletes && e.sequence_number >= min_data_file_seq_num
        }) {
            let manifest_file = manifest_list_entry.load_manifest(&self.file_io).await?;

            for manifest_entry in manifest_file.entries().iter().filter(|e| e.is_alive()) {
                match manifest_entry.content_type() {
                    DataContentType::PositionDeletes => {
                        position_delete_files.push(manifest_entry.clone());
                    }
                    DataContentType::EqualityDeletes => {
                        eq_delete_files.push(manifest_entry.clone());
                    }
                    DataContentType::Data => {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "Data file entry({}) found in delete manifest file({})",
                                manifest_entry.file_path(),
                                manifest_list_entry.manifest_path
                            ),
                        ));
                    }
                }
            }
        }

        // Sort delete files by sequence number.
        position_delete_files
            .sort_by_key(|f| f.sequence_number().unwrap_or(INITIAL_SEQUENCE_NUMBER));
        eq_delete_files.sort_by_key(|f| f.sequence_number().unwrap_or(INITIAL_SEQUENCE_NUMBER));

        // Generate data file stream
        let mut file_scan_tasks = Vec::with_capacity(manifest_list.entries().len());
        for manifest_list_entry in manifest_list
            .entries()
            .iter()
            .filter(|e| e.content == ManifestContentType::Data)
        {
            // Data file
            let manifest = manifest_list_entry.load_manifest(&self.file_io).await?;

            for manifest_entry in manifest.entries() {
                if manifest_entry.is_alive() {
                    file_scan_tasks.push(Ok(FileScanTask {
                        data_file: manifest_entry.clone(),
                        position_delete_files: TableScan::filter_position_delete_files(
                            manifest_entry,
                            &position_delete_files,
                        ),
                        eq_delete_files: TableScan::filter_eq_delete_files(
                            manifest_entry,
                            &eq_delete_files,
                        ),
                        start: 0,
                        length: manifest_entry.file_size_in_bytes(),
                    }));
                }
            }
        }

        Ok(iter(file_scan_tasks).boxed())
    }

    /// Return the position delete files that should be applied to the data file.
    ///
    /// Here we assume that the position delete files are sorted by sequence number in ascending order.
    fn filter_position_delete_files(
        data_file: &ManifestEntry,
        position_deletes: &[ManifestEntryRef],
    ) -> Vec<ManifestEntryRef> {
        let data_seq_num = data_file
            .sequence_number()
            .unwrap_or(INITIAL_SEQUENCE_NUMBER);

        // Find the first position delete file whose sequence number is greater than or equal to the data file.
        let first_entry = position_deletes.partition_point(|e| {
            e.sequence_number().unwrap_or(INITIAL_SEQUENCE_NUMBER) >= data_seq_num
        });

        // TODO: We should further filter the position delete files by `file_path` column.
        position_deletes.iter().skip(first_entry).cloned().collect()
    }

    /// Return the equality delete files that should be applied to the data file.
    ///
    /// Here we assume that the equality delete files are sorted by sequence number in ascending order.
    fn filter_eq_delete_files(
        data_file: &ManifestEntry,
        eq_deletes: &[ManifestEntryRef],
    ) -> Vec<ManifestEntryRef> {
        let data_seq_num = data_file
            .sequence_number()
            .unwrap_or(INITIAL_SEQUENCE_NUMBER);

        // Find the first position delete file whose sequence number is greater than or equal to the data file.
        let first_entry = eq_deletes.partition_point(|e| {
            e.sequence_number().unwrap_or(INITIAL_SEQUENCE_NUMBER) > data_seq_num
        });

        // TODO: We should further filter the position delete files statistics
        eq_deletes.iter().skip(first_entry).cloned().collect()
    }
}

/// A task to scan part of file.
pub struct FileScanTask {
    data_file: ManifestEntryRef,
    position_delete_files: Vec<ManifestEntryRef>,
    eq_delete_files: Vec<ManifestEntryRef>,
    start: u64,
    length: u64,
}

/// A stream of arrow record batches.
pub type ArrowRecordBatchStream = BoxStream<'static, Result<RecordBatch>>;
impl FileScanTask {
    /// Returns a stream of arrow record batches.
    pub async fn execute(&self) -> Result<ArrowRecordBatchStream> {
        todo!()
    }
}
