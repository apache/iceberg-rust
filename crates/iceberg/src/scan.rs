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

//! Table scan api.

use std::ops::Range;
use std::sync::Arc;

use crate::io::{FileIO, FileRead};
use crate::spec::{
    DataContentType, DataFile, ManifestEntryRef, SchemaRef, SnapshotRef, TableMetadataRef,
};
use crate::table::Table;
use crate::{Error, ErrorKind};
use arrow_array::RecordBatch;
use bytes::{Bytes, BytesMut};
use futures::future::BoxFuture;
use futures::stream::{iter, BoxStream, SelectAll};
use futures::{AsyncReadExt, AsyncSeekExt, StreamExt, TryFutureExt, TryStreamExt};
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::file::metadata::ParquetMetaData;

/// Builder to create table scan.
pub struct TableScanBuilder<'a> {
    table: &'a Table,
    // Empty column names means to select all columns
    column_names: Vec<String>,
    snapshot_id: Option<i64>,
}

impl<'a> TableScanBuilder<'a> {
    pub fn new(table: &'a Table) -> Self {
        Self {
            table,
            column_names: vec![],
            snapshot_id: None,
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

    /// Set the snapshot to scan. When not set, it uses current snapshot.
    pub fn snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    /// Build the table scan.
    pub fn build(self) -> crate::Result<TableScan> {
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

        let schema = snapshot.schema(self.table.metadata())?;

        // Check that all column names exist in the schema.
        if !self.column_names.is_empty() {
            for column_name in &self.column_names {
                if schema.field_by_name(column_name).is_none() {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Column {} not found in table.", column_name),
                    ));
                }
            }
        }

        Ok(TableScan {
            snapshot,
            file_io: self.table.file_io().clone(),
            table_metadata: self.table.metadata_ref(),
            column_names: self.column_names,
            schema,
        })
    }
}

/// Table scan.
#[derive(Debug)]
#[allow(dead_code)]
pub struct TableScan {
    snapshot: SnapshotRef,
    table_metadata: TableMetadataRef,
    file_io: FileIO,
    column_names: Vec<String>,
    schema: SchemaRef,
}

// /// A stream of [`FileScanTask`].
// pub type FileScanTaskStream = BoxStream<'static, crate::Result<FileScanTask>>;

impl TableScan {
    /// Returns a stream of file scan tasks.
    pub async fn plan_files(&self) -> crate::Result<Vec<PlanFile>> {
        let manifest_list = self
            .snapshot
            .load_manifest_list(&self.file_io, &self.table_metadata)
            .await?;

        // Generate data file
        let mut plan_files = Vec::with_capacity(manifest_list.entries().len());
        for manifest_list_entry in manifest_list.entries().iter() {
            // Data file
            let manifest = manifest_list_entry.load_manifest(&self.file_io).await?;

            for manifest_entry in manifest.entries().iter().filter(|e| e.is_alive()) {
                match manifest_entry.content_type() {
                    DataContentType::EqualityDeletes | DataContentType::PositionDeletes => {
                        return Err(Error::new(
                            ErrorKind::FeatureUnsupported,
                            "Delete files are not supported yet.",
                        ));
                    }
                    DataContentType::Data => {
                        plan_files.push(PlanFile {
                            data_files: manifest_entry.data_file().clone(),
                            start: 0,
                            len: manifest_entry.file_size_in_bytes(),
                        });
                    }
                }
            }
        }

        Ok(plan_files)
    }
}

/// PlanFile is a file to be scanned. It's the smallest unit of scan.
#[derive(Debug)]
pub struct PlanFile {
    data_files: DataFile,
    start: u64,
    len: u64,
}

/// A task to scan part of file.
#[derive(Debug)]
#[allow(dead_code)]
pub struct FileScanTask {
    plan_files: Vec<PlanFile>,
    file_io: FileIO,
}

/// A stream of arrow record batches.
pub type ArrowRecordBatchStream = BoxStream<'static, crate::Result<RecordBatch>>;

impl FileScanTask {
    /// Create a new file scan task from `PlanFiles`.
    pub fn new(plan_files: Vec<PlanFile>, file_io: FileIO) -> Self {
        Self {
            plan_files,
            file_io,
        }
    }

    async fn build_stream(
        plan_file: &PlanFile,
        file_io: &FileIO,
    ) -> crate::Result<ArrowRecordBatchStream> {
        match plan_file.data_files.file_format {
            crate::spec::DataFileFormat::Parquet => {
                let parquet_reader = ParquetFileReader {
                    reader: file_io
                        .new_input(&plan_file.data_files.file_path)
                        .unwrap()
                        .reader()
                        .await
                        .unwrap(),
                };
                let stream = ParquetRecordBatchStreamBuilder::new(parquet_reader)
                    .await
                    .unwrap()
                    .build()
                    .unwrap()
                    .map_err(|err| {
                        Error::new(ErrorKind::Unexpected, "Fail to read data").with_source(err)
                    })
                    .boxed();

                Ok(stream)
            }
            crate::spec::DataFileFormat::Orc | crate::spec::DataFileFormat::Avro => {
                Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    "Unsupported data file format",
                ))
            }
        }
    }

    /// Returns a stream of arrow record batches.
    pub async fn execute(&self) -> crate::Result<ArrowRecordBatchStream> {
        let mut streams = SelectAll::new();
        for plan_file in &self.plan_files {
            let stream = Self::build_stream(plan_file, &self.file_io).await?;
            streams.push(stream);
        }
        Ok(streams.boxed())
    }
}

struct ParquetFileReader<R: FileRead> {
    reader: R,
}

impl<R: FileRead> AsyncFileReader for ParquetFileReader<R> {
    fn get_bytes(
        &mut self,
        range: Range<usize>,
    ) -> BoxFuture<'_, parquet::errors::Result<bytes::Bytes>> {
        todo!()
    }

    /// Get the metadata of the parquet file.
    ///
    /// Inspired by https://docs.rs/parquet/latest/parquet/file/footer/fn.parse_metadata.html
    fn get_metadata(&mut self) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::io::{FileIO, OutputFile};
    use crate::scan::FileScanTask;
    use crate::spec::{
        DataContentType, DataFile, DataFileFormat, FormatVersion, Literal, Manifest,
        ManifestContentType, ManifestEntry, ManifestListWriter, ManifestMetadata, ManifestStatus,
        ManifestWriter, Struct, TableMetadata, EMPTY_SNAPSHOT_ID,
    };
    use crate::table::Table;
    use crate::TableIdent;
    use futures::TryStreamExt;
    use itertools::Itertools;
    use std::fs;
    use tempfile::TempDir;
    use tera::{Context, Tera};
    use uuid::Uuid;

    struct TableTestFixture {
        table_location: String,
        table: Table,
    }

    impl TableTestFixture {
        fn new() -> Self {
            let tmp_dir = TempDir::new().unwrap();
            let table_location = tmp_dir.path().join("table1");
            let manifest_list1_location = table_location.join("metadata/manifests_list_1.avro");
            let manifest_list2_location = table_location.join("metadata/manifests_list_2.avro");
            let table_metadata1_location = table_location.join("metadata/v1.json");

            let file_io = FileIO::from_path(table_location.as_os_str().to_str().unwrap())
                .unwrap()
                .build()
                .unwrap();

            let table_metadata = {
                let template_json_str = fs::read_to_string(format!(
                    "{}/testdata/example_table_metadata_v2.json",
                    env!("CARGO_MANIFEST_DIR")
                ))
                .unwrap();
                let mut context = Context::new();
                context.insert("table_location", &table_location);
                context.insert("manifest_list_1_location", &manifest_list1_location);
                context.insert("manifest_list_2_location", &manifest_list2_location);
                context.insert("table_metadata_1_location", &table_metadata1_location);

                let metadata_json = Tera::one_off(&template_json_str, &context, false).unwrap();
                serde_json::from_str::<TableMetadata>(&metadata_json).unwrap()
            };

            let table = Table::builder()
                .metadata(table_metadata)
                .identifier(TableIdent::from_strs(["db", "table1"]).unwrap())
                .file_io(file_io)
                .metadata_location(table_metadata1_location.as_os_str().to_str().unwrap())
                .build();

            Self {
                table_location: table_location.to_str().unwrap().to_string(),
                table,
            }
        }

        fn next_manifest_file(&self) -> OutputFile {
            self.table
                .file_io()
                .new_output(format!(
                    "{}/metadata/manifest_{}.avro",
                    self.table_location,
                    Uuid::new_v4()
                ))
                .unwrap()
        }
    }

    #[test]
    fn test_table_scan_columns() {
        let table = TableTestFixture::new().table;

        let table_scan = table.scan().select(["x", "y"]).build().unwrap();
        assert_eq!(vec!["x", "y"], table_scan.column_names);

        let table_scan = table
            .scan()
            .select(["x", "y"])
            .select(["z"])
            .build()
            .unwrap();
        assert_eq!(vec!["z"], table_scan.column_names);
    }

    #[test]
    fn test_select_all() {
        let table = TableTestFixture::new().table;

        let table_scan = table.scan().select_all().build().unwrap();
        assert!(table_scan.column_names.is_empty());
    }

    #[test]
    fn test_select_no_exist_column() {
        let table = TableTestFixture::new().table;

        let table_scan = table.scan().select(["x", "y", "z", "a"]).build();
        assert!(table_scan.is_err());
    }

    #[test]
    fn test_table_scan_default_snapshot_id() {
        let table = TableTestFixture::new().table;

        let table_scan = table.scan().build().unwrap();
        assert_eq!(
            table.metadata().current_snapshot().unwrap().snapshot_id(),
            table_scan.snapshot.snapshot_id()
        );
    }

    #[test]
    fn test_table_scan_non_exist_snapshot_id() {
        let table = TableTestFixture::new().table;

        let table_scan = table.scan().snapshot_id(1024).build();
        assert!(table_scan.is_err());
    }

    #[test]
    fn test_table_scan_with_snapshot_id() {
        let table = TableTestFixture::new().table;

        let table_scan = table
            .scan()
            .snapshot_id(3051729675574597004)
            .build()
            .unwrap();
        assert_eq!(table_scan.snapshot.snapshot_id(), 3051729675574597004);
    }

    // #[tokio::test]
    // async fn test_plan_files_no_deletions() {
    //     let fixture = TableTestFixture::new();

    //     let current_snapshot = fixture.table.metadata().current_snapshot().unwrap();
    //     let parent_snapshot = current_snapshot
    //         .parent_snapshot(fixture.table.metadata())
    //         .unwrap();
    //     let current_schema = current_snapshot.schema(fixture.table.metadata()).unwrap();
    //     let current_partition_spec = fixture.table.metadata().default_partition_spec().unwrap();

    //     // Write data files
    //     let data_file_manifest = ManifestWriter::new(
    //         fixture.next_manifest_file(),
    //         current_snapshot.snapshot_id(),
    //         vec![],
    //     )
    //     .write(Manifest::new(
    //         ManifestMetadata::builder()
    //             .schema((*current_schema).clone())
    //             .content(ManifestContentType::Data)
    //             .format_version(FormatVersion::V2)
    //             .partition_spec((**current_partition_spec).clone())
    //             .schema_id(current_schema.schema_id())
    //             .build(),
    //         vec![
    //             ManifestEntry::builder()
    //                 .status(ManifestStatus::Added)
    //                 .data_file(
    //                     DataFile::builder()
    //                         .content(DataContentType::Data)
    //                         .file_path(format!("{}/1.parquet", &fixture.table_location))
    //                         .file_format(DataFileFormat::Parquet)
    //                         .file_size_in_bytes(100)
    //                         .record_count(1)
    //                         .partition(Struct::from_iter([Some(Literal::long(100))]))
    //                         .build(),
    //                 )
    //                 .build(),
    //             ManifestEntry::builder()
    //                 .status(ManifestStatus::Deleted)
    //                 .snapshot_id(parent_snapshot.snapshot_id())
    //                 .sequence_number(parent_snapshot.sequence_number())
    //                 .file_sequence_number(parent_snapshot.sequence_number())
    //                 .data_file(
    //                     DataFile::builder()
    //                         .content(DataContentType::Data)
    //                         .file_path(format!("{}/2.parquet", &fixture.table_location))
    //                         .file_format(DataFileFormat::Parquet)
    //                         .file_size_in_bytes(100)
    //                         .record_count(1)
    //                         .partition(Struct::from_iter([Some(Literal::long(200))]))
    //                         .build(),
    //                 )
    //                 .build(),
    //             ManifestEntry::builder()
    //                 .status(ManifestStatus::Existing)
    //                 .snapshot_id(parent_snapshot.snapshot_id())
    //                 .sequence_number(parent_snapshot.sequence_number())
    //                 .file_sequence_number(parent_snapshot.sequence_number())
    //                 .data_file(
    //                     DataFile::builder()
    //                         .content(DataContentType::Data)
    //                         .file_path(format!("{}/3.parquet", &fixture.table_location))
    //                         .file_format(DataFileFormat::Parquet)
    //                         .file_size_in_bytes(100)
    //                         .record_count(1)
    //                         .partition(Struct::from_iter([Some(Literal::long(300))]))
    //                         .build(),
    //                 )
    //                 .build(),
    //         ],
    //     ))
    //     .await
    //     .unwrap();

    //     // Write to manifest list
    //     let mut manifest_list_write = ManifestListWriter::v2(
    //         fixture
    //             .table
    //             .file_io()
    //             .new_output(current_snapshot.manifest_list_file_path().unwrap())
    //             .unwrap(),
    //         current_snapshot.snapshot_id(),
    //         current_snapshot
    //             .parent_snapshot_id()
    //             .unwrap_or(EMPTY_SNAPSHOT_ID),
    //         current_snapshot.sequence_number(),
    //     );
    //     manifest_list_write
    //         .add_manifest_entries(vec![data_file_manifest].into_iter())
    //         .unwrap();
    //     manifest_list_write.close().await.unwrap();

    //     // Create table scan for current snapshot and plan files
    //     let table_scan = fixture.table.scan().build().unwrap();
    //     let plan_files = table_scan
    //         .plan_files()
    //         .await
    //         .unwrap()
    //         .split_by_chunk_size(1)
    //         .unwrap();
    //     let mut tasks = plan_files
    //         .into_iter()
    //         .map(|plan_files| FileScanTask::new(plan_files))
    //         .collect_vec();

    //     assert_eq!(tasks.len(), 2);

    //     tasks.sort_by_key(|t| t.plan_files.data_files[0].0.to_string());

    //     // Check first task is added data file
    //     assert_eq!(
    //         tasks[0].data_file.file_path(),
    //         format!("{}/1.parquet", &fixture.table_location)
    //     );

    //     // Check second task is existing data file
    //     assert_eq!(
    //         tasks[1].data_file.file_path(),
    //         format!("{}/3.parquet", &fixture.table_location)
    //     );
    // }
}
