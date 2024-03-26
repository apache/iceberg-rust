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

use crate::arrow::ArrowReaderBuilder;
use crate::io::FileIO;
use crate::spec::{DataContentType, ManifestEntryRef, SchemaRef, SnapshotRef, TableMetadataRef};
use crate::table::Table;
use crate::{Error, ErrorKind};
use arrow_array::RecordBatch;
use async_stream::try_stream;
use futures::stream::{iter, BoxStream};
use futures::StreamExt;

/// Builder to create table scan.
pub struct TableScanBuilder<'a> {
    table: &'a Table,
    // Empty column names means to select all columns
    column_names: Vec<String>,
    snapshot_id: Option<i64>,
    batch_size: Option<usize>,
}

impl<'a> TableScanBuilder<'a> {
    pub fn new(table: &'a Table) -> Self {
        Self {
            table,
            column_names: vec![],
            snapshot_id: None,
            batch_size: None,
        }
    }

    /// Sets the desired size of batches in the response
    /// to something other than the default
    pub fn with_batch_size(mut self, batch_size: Option<usize>) -> Self {
        self.batch_size = batch_size;
        self
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
            batch_size: self.batch_size,
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
    batch_size: Option<usize>,
}

/// A stream of [`FileScanTask`].
pub type FileScanTaskStream = BoxStream<'static, crate::Result<FileScanTask>>;

impl TableScan {
    /// Returns a stream of file scan tasks.
    pub async fn plan_files(&self) -> crate::Result<FileScanTaskStream> {
        let snapshot = self.snapshot.clone();
        let table_metadata = self.table_metadata.clone();
        let file_io = self.file_io.clone();

        Ok(try_stream! {
            let manifest_list = snapshot
            .clone()
            .load_manifest_list(&file_io, &table_metadata)
            .await?;

            // Generate data file stream
            let mut entries = iter(manifest_list.entries());
            while let Some(entry) = entries.next().await {
                let manifest = entry.load_manifest(&file_io).await?;

                let mut manifest_entries = iter(manifest.entries().iter().filter(|e| e.is_alive()));
                while let Some(manifest_entry) = manifest_entries.next().await {
                    match manifest_entry.content_type() {
                        DataContentType::EqualityDeletes | DataContentType::PositionDeletes => {
                            yield Err(Error::new(
                                ErrorKind::FeatureUnsupported,
                                "Delete files are not supported yet.",
                            ))?;
                        }
                        DataContentType::Data => {
                            let scan_task: crate::Result<FileScanTask> = Ok(FileScanTask {
                                data_manifest_entry: manifest_entry.clone(),
                                start: 0,
                                length: manifest_entry.file_size_in_bytes(),
                            });
                            yield scan_task?;
                        }
                    }
                }
            }
        }
        .boxed())
    }

    pub async fn to_arrow(&self) -> crate::Result<ArrowRecordBatchStream> {
        let mut arrow_reader_builder =
            ArrowReaderBuilder::new(self.file_io.clone(), self.schema.clone());

        if let Some(batch_size) = self.batch_size {
            arrow_reader_builder = arrow_reader_builder.with_batch_size(batch_size);
        }

        arrow_reader_builder.build().read(self.plan_files().await?)
    }
}

/// A task to scan part of file.
#[derive(Debug)]
pub struct FileScanTask {
    data_manifest_entry: ManifestEntryRef,
    #[allow(dead_code)]
    start: u64,
    #[allow(dead_code)]
    length: u64,
}

/// A stream of arrow record batches.
pub type ArrowRecordBatchStream = BoxStream<'static, crate::Result<RecordBatch>>;

impl FileScanTask {
    pub fn data(&self) -> ManifestEntryRef {
        self.data_manifest_entry.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::io::{FileIO, OutputFile};
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, FormatVersion, Literal, Manifest,
        ManifestContentType, ManifestEntry, ManifestListWriter, ManifestMetadata, ManifestStatus,
        ManifestWriter, Struct, TableMetadata, EMPTY_SNAPSHOT_ID,
    };
    use crate::table::Table;
    use crate::TableIdent;
    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    use futures::TryStreamExt;
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use std::collections::HashMap;
    use std::fs;
    use std::fs::File;
    use std::sync::Arc;
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

        async fn setup_manifest_files(&mut self) {
            let current_snapshot = self.table.metadata().current_snapshot().unwrap();
            let parent_snapshot = current_snapshot
                .parent_snapshot(self.table.metadata())
                .unwrap();
            let current_schema = current_snapshot.schema(self.table.metadata()).unwrap();
            let current_partition_spec = self.table.metadata().default_partition_spec().unwrap();

            // Write data files
            let data_file_manifest = ManifestWriter::new(
                self.next_manifest_file(),
                current_snapshot.snapshot_id(),
                vec![],
            )
            .write(Manifest::new(
                ManifestMetadata::builder()
                    .schema((*current_schema).clone())
                    .content(ManifestContentType::Data)
                    .format_version(FormatVersion::V2)
                    .partition_spec((**current_partition_spec).clone())
                    .schema_id(current_schema.schema_id())
                    .build(),
                vec![
                    ManifestEntry::builder()
                        .status(ManifestStatus::Added)
                        .data_file(
                            DataFileBuilder::default()
                                .content(DataContentType::Data)
                                .file_path(format!("{}/1.parquet", &self.table_location))
                                .file_format(DataFileFormat::Parquet)
                                .file_size_in_bytes(100)
                                .record_count(1)
                                .partition(Struct::from_iter([Some(Literal::long(100))]))
                                .build()
                                .unwrap(),
                        )
                        .build(),
                    ManifestEntry::builder()
                        .status(ManifestStatus::Deleted)
                        .snapshot_id(parent_snapshot.snapshot_id())
                        .sequence_number(parent_snapshot.sequence_number())
                        .file_sequence_number(parent_snapshot.sequence_number())
                        .data_file(
                            DataFileBuilder::default()
                                .content(DataContentType::Data)
                                .file_path(format!("{}/2.parquet", &self.table_location))
                                .file_format(DataFileFormat::Parquet)
                                .file_size_in_bytes(100)
                                .record_count(1)
                                .partition(Struct::from_iter([Some(Literal::long(200))]))
                                .build()
                                .unwrap(),
                        )
                        .build(),
                    ManifestEntry::builder()
                        .status(ManifestStatus::Existing)
                        .snapshot_id(parent_snapshot.snapshot_id())
                        .sequence_number(parent_snapshot.sequence_number())
                        .file_sequence_number(parent_snapshot.sequence_number())
                        .data_file(
                            DataFileBuilder::default()
                                .content(DataContentType::Data)
                                .file_path(format!("{}/3.parquet", &self.table_location))
                                .file_format(DataFileFormat::Parquet)
                                .file_size_in_bytes(100)
                                .record_count(1)
                                .partition(Struct::from_iter([Some(Literal::long(300))]))
                                .build()
                                .unwrap(),
                        )
                        .build(),
                ],
            ))
            .await
            .unwrap();

            // Write to manifest list
            let mut manifest_list_write = ManifestListWriter::v2(
                self.table
                    .file_io()
                    .new_output(current_snapshot.manifest_list())
                    .unwrap(),
                current_snapshot.snapshot_id(),
                current_snapshot
                    .parent_snapshot_id()
                    .unwrap_or(EMPTY_SNAPSHOT_ID),
                current_snapshot.sequence_number(),
            );
            manifest_list_write
                .add_manifests(vec![data_file_manifest].into_iter())
                .unwrap();
            manifest_list_write.close().await.unwrap();

            // prepare data
            let schema = {
                let fields =
                    vec![
                        arrow_schema::Field::new("col", arrow_schema::DataType::Int64, true)
                            .with_metadata(HashMap::from([(
                                PARQUET_FIELD_ID_META_KEY.to_string(),
                                "0".to_string(),
                            )])),
                    ];
                Arc::new(arrow_schema::Schema::new(fields))
            };
            let col = Arc::new(Int64Array::from_iter_values(vec![1; 1024])) as ArrayRef;
            let to_write = RecordBatch::try_new(schema.clone(), vec![col]).unwrap();

            // Write the Parquet files
            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build();

            for n in 1..=3 {
                let file = File::create(format!("{}/{}.parquet", &self.table_location, n)).unwrap();
                let mut writer =
                    ArrowWriter::try_new(file, to_write.schema(), Some(props.clone())).unwrap();

                writer.write(&to_write).expect("Writing batch");

                // writer must be closed to write footer
                writer.close().unwrap();
            }
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

    #[tokio::test]
    async fn test_plan_files_no_deletions() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        // Create table scan for current snapshot and plan files
        let table_scan = fixture.table.scan().build().unwrap();
        let mut tasks = table_scan
            .plan_files()
            .await
            .unwrap()
            .try_fold(vec![], |mut acc, task| async move {
                acc.push(task);
                Ok(acc)
            })
            .await
            .unwrap();

        assert_eq!(tasks.len(), 2);

        tasks.sort_by_key(|t| t.data().data_file().file_path().to_string());

        // Check first task is added data file
        assert_eq!(
            tasks[0].data().data_file().file_path(),
            format!("{}/1.parquet", &fixture.table_location)
        );

        // Check second task is existing data file
        assert_eq!(
            tasks[1].data().data_file().file_path(),
            format!("{}/3.parquet", &fixture.table_location)
        );
    }

    #[tokio::test]
    async fn test_open_parquet_no_deletions() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        // Create table scan for current snapshot and plan files
        let table_scan = fixture.table.scan().build().unwrap();

        let batch_stream = table_scan.to_arrow().await.unwrap();

        let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

        let col = batches[0].column_by_name("col").unwrap();

        let int64_arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int64_arr.value(0), 1);
    }
}
