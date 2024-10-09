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

use std::collections::HashMap;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::SchemaRef;
use futures::StreamExt;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::scan::ArrowRecordBatchStream;
use crate::spec::{NestedField, PrimitiveType, Schema, Type};
use crate::{Error, ErrorKind, Result};

pub(crate) const FIELD_ID_DELETE_FILE_PATH: i32 = i32::MAX - 101;
pub(crate) const FIELD_ID_DELETE_POS: i32 = i32::MAX - 102;
// pub(crate) const FIELD_ID_DELETE_ROW: i32 = i32::MAX - 103;

pub(crate) const FIELD_NAME_DELETE_FILE_PATH: &str = "file_path";
pub(crate) const FIELD_NAME_DELETE_POS: &str = "pos";
// pub(crate) const FIELD_NAME_DELETE_ROW: &str = "row";

// Represents a parsed Delete file that can be safely stored
// in the Object Cache.
#[allow(dead_code)]
pub(crate) enum Deletes {
    // Positional delete files are parsed into a map of
    // filename to a sorted list of row indices.
    // TODO: Ignoring the stored rows that are present in
    //   positional deletes for now. I think they only used for statistics?
    Positional(HashMap<String, Vec<u64>>),

    // Equality delete files are initially parsed solely as an
    // unprocessed list of `RecordBatch`es from the equality
    // delete files.
    // I don't think we can do better than this by
    // storing a Predicate (because the equality deletes use the
    // field_id rather than the field name, so if we keep this as
    // a Predicate then a field name change would break it).
    // Similarly, I don't think we can store this as a BoundPredicate
    // as the column order could be different across different data
    // files and so the accessor in the bound predicate could be invalid).
    Equality(Vec<RecordBatch>),
}

enum PosDelSchema {
    WithRow,
    WithoutRow,
}

#[allow(dead_code)]
fn positional_delete_file_without_row_schema() -> Schema {
    Schema::builder()
        .with_fields([
            NestedField::required(
                FIELD_ID_DELETE_FILE_PATH,
                FIELD_NAME_DELETE_FILE_PATH,
                Type::Primitive(PrimitiveType::String),
            )
            .into(),
            NestedField::required(
                FIELD_ID_DELETE_POS,
                FIELD_NAME_DELETE_POS,
                Type::Primitive(PrimitiveType::Long),
            )
            .into(),
        ])
        .build()
        .unwrap()
}

fn validate_schema(schema: SchemaRef) -> Result<PosDelSchema> {
    let fields = schema.flattened_fields();
    match fields.len() {
        2 | 3 => {
            let path_field = fields[0];
            let pos_field = fields[1];
            if path_field
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .unwrap()
                .parse::<i32>()
                .unwrap()
                != FIELD_ID_DELETE_FILE_PATH
                || pos_field
                    .metadata()
                    .get(PARQUET_FIELD_ID_META_KEY)
                    .unwrap()
                    .parse::<i32>()
                    .unwrap()
                    != FIELD_ID_DELETE_POS
            {
                Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Positional Delete file did not have the expected schema",
                ))
            } else if fields.len() == 2 {
                Ok(PosDelSchema::WithoutRow)
            } else {
                // TODO: should check that col 3 is of type Struct
                //   and that it contains a subset of the table schema
                Ok(PosDelSchema::WithRow)
            }
        }
        _ => Err(Error::new(
            ErrorKind::DataInvalid,
            "Positional Delete file did not have the expected schema",
        )),
    }
}

pub(crate) async fn parse_positional_delete_file(
    mut record_batch_stream: ArrowRecordBatchStream,
) -> Result<Deletes> {
    let mut result: HashMap<String, Vec<u64>> = HashMap::new();

    while let Some(batch) = record_batch_stream.next().await {
        let batch = batch?;
        let schema = batch.schema();

        // Don't care about what schema type it is at
        // present as we're ignoring the "row" column from files
        // with 3-column schemas. We only care if it is valid
        let _schema_type = validate_schema(schema)?;

        let columns = batch.columns();

        let file_paths = columns[0].as_any().downcast_ref::<StringArray>().unwrap();
        let positions = columns[1].as_any().downcast_ref::<Int64Array>().unwrap();

        for (file_path, pos) in file_paths.iter().zip(positions.iter()) {
            let (Some(file_path), Some(pos)) = (file_path, pos) else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "null values in delete file",
                ));
            };

            result
                .entry(file_path.to_string())
                .and_modify(|entry| {
                    (*entry).push(pos as u64);
                })
                .or_insert(vec![pos as u64]);
        }
    }

    Ok(Deletes::Positional(result))
}

pub(crate) async fn parse_equality_delete_file(
    mut record_batch_stream: ArrowRecordBatchStream,
) -> Result<Deletes> {
    let mut result: Vec<RecordBatch> = Vec::new();

    while let Some(batch) = record_batch_stream.next().await {
        result.push(batch?);
    }

    Ok(Deletes::Equality(result))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::fs::File;
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use futures::TryStreamExt;
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;
    use tera::{Context, Tera};
    use uuid::Uuid;

    use crate::expr::Reference;
    use crate::io::{FileIO, OutputFile};
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Datum, FormatVersion, Literal, Manifest,
        ManifestContentType, ManifestEntry, ManifestListWriter, ManifestMetadata, ManifestStatus,
        ManifestWriter, Struct, TableMetadata, EMPTY_SNAPSHOT_ID,
    };
    use crate::table::Table;
    use crate::TableIdent;

    struct TableDeleteTestFixture {
        table_location: String,
        table: Table,
    }

    impl TableDeleteTestFixture {
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
                .file_io(file_io.clone())
                .metadata_location(table_metadata1_location.as_os_str().to_str().unwrap())
                .build()
                .unwrap();

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
            let current_partition_spec = self.table.metadata().default_partition_spec();

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

            // Write delete files
            let delete_file_manifest = ManifestWriter::new(
                self.next_manifest_file(),
                current_snapshot.snapshot_id(),
                vec![],
            )
            .write(Manifest::new(
                ManifestMetadata::builder()
                    .schema((*current_schema).clone())
                    .content(ManifestContentType::Deletes)
                    .format_version(FormatVersion::V2)
                    .partition_spec((**current_partition_spec).clone())
                    .schema_id(current_schema.schema_id())
                    .build(),
                vec![
                    ManifestEntry::builder()
                        .status(ManifestStatus::Added)
                        .data_file(
                            DataFileBuilder::default()
                                .content(DataContentType::PositionDeletes)
                                .file_path(format!("{}/deletes-1.parquet", &self.table_location))
                                .file_format(DataFileFormat::Parquet)
                                .file_size_in_bytes(100)
                                .record_count(8)
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
                                .content(DataContentType::PositionDeletes)
                                .file_path(format!("{}/deletes-2.parquet", &self.table_location))
                                .file_format(DataFileFormat::Parquet)
                                .file_size_in_bytes(100)
                                .record_count(8)
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
                                .content(DataContentType::PositionDeletes)
                                .file_path(format!("{}/deletes-3.parquet", &self.table_location))
                                .file_format(DataFileFormat::Parquet)
                                .file_size_in_bytes(100)
                                .record_count(8)
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
                .add_manifests(vec![data_file_manifest, delete_file_manifest].into_iter())
                .unwrap();
            manifest_list_write.close().await.unwrap();

            // prepare data
            let schema = {
                let fields = vec![
                    arrow_schema::Field::new("x", arrow_schema::DataType::Int64, false)
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "1".to_string(),
                        )])),
                    arrow_schema::Field::new("y", arrow_schema::DataType::Int64, false)
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "2".to_string(),
                        )])),
                    arrow_schema::Field::new("z", arrow_schema::DataType::Int64, false)
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "3".to_string(),
                        )])),
                    arrow_schema::Field::new("a", arrow_schema::DataType::Utf8, false)
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "4".to_string(),
                        )])),
                ];
                Arc::new(arrow_schema::Schema::new(fields))
            };
            // 4 columns:
            // x & y: [1000, 1001, 1002, 1003, ...]
            let col1 = Arc::new(Int64Array::from_iter_values(1000i64..2024i64)) as ArrayRef;

            // a: ["Apache", "Apache", "Apache", ..., "Iceberg", "Iceberg", "Iceberg"]
            let mut values = vec!["Apache"; 512];
            values.append(vec!["Iceberg"; 512].as_mut());
            let col4 = Arc::new(StringArray::from_iter_values(values)) as ArrayRef;

            // Write the Parquet files
            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build();

            for n in 1..=3 {
                // z: [n, n, n, n, ....]
                let values = vec![n; 1024];
                let col3 = Arc::new(Int64Array::from_iter_values(values)) as ArrayRef;

                let to_write = RecordBatch::try_new(schema.clone(), vec![
                    col1.clone(),
                    col1.clone(),
                    col3,
                    col4.clone(),
                ])
                .unwrap();

                let file = File::create(format!("{}/{}.parquet", &self.table_location, n)).unwrap();
                let mut writer =
                    ArrowWriter::try_new(file, to_write.schema(), Some(props.clone())).unwrap();

                writer.write(&to_write).expect("Writing batch");

                // writer must be closed to write footer
                writer.close().unwrap();
            }

            // prepare data for positional delete files
            let mut file_path_cols = vec![];
            let mut pos_cols = vec![];

            let file_path_values = vec![format!("{}/1.parquet", &self.table_location); 8];
            file_path_cols
                .push(Arc::new(StringArray::from_iter_values(file_path_values)) as ArrayRef);

            let file_path_values = vec![format!("{}/2.parquet", &self.table_location); 8];
            file_path_cols
                .push(Arc::new(StringArray::from_iter_values(file_path_values)) as ArrayRef);

            let file_path_values = vec![format!("{}/3.parquet", &self.table_location); 8];
            file_path_cols
                .push(Arc::new(StringArray::from_iter_values(file_path_values)) as ArrayRef);

            let pos_values = vec![0, 1, 3, 5, 6, 8, 1022, 1023];
            pos_cols.push(Arc::new(Int64Array::from_iter_values(pos_values)) as ArrayRef);

            let pos_values = vec![1, 2, 3, 5, 6, 8, 1022, 1023];
            pos_cols.push(Arc::new(Int64Array::from_iter_values(pos_values)) as ArrayRef);

            let pos_values = vec![1, 2, 3, 5, 6, 8, 1021, 1022];
            pos_cols.push(Arc::new(Int64Array::from_iter_values(pos_values)) as ArrayRef);

            let positional_delete_schema = {
                let fields = vec![
                    arrow_schema::Field::new("file_path", arrow_schema::DataType::Utf8, false)
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "2147483546".to_string(),
                        )])),
                    arrow_schema::Field::new("pos", arrow_schema::DataType::Int64, false)
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "2147483545".to_string(),
                        )])),
                ];
                Arc::new(arrow_schema::Schema::new(fields))
            };

            for n in 1..=3 {
                let positional_deletes_to_write =
                    RecordBatch::try_new(positional_delete_schema.clone(), vec![
                        file_path_cols.pop().unwrap(),
                        pos_cols.pop().unwrap(),
                    ])
                    .unwrap();

                let file = File::create(format!("{}/deletes-{}.parquet", &self.table_location, n))
                    .unwrap();
                let mut writer = ArrowWriter::try_new(
                    file,
                    positional_deletes_to_write.schema(),
                    Some(props.clone()),
                )
                .unwrap();

                writer
                    .write(&positional_deletes_to_write)
                    .expect("Writing batch");

                // writer must be closed to write footer
                writer.close().unwrap();
            }
        }
    }

    #[tokio::test]
    async fn test_plan_files_with_deletions() {
        let mut fixture = TableDeleteTestFixture::new();
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

        tasks.sort_by_key(|t| t.data_file_path.to_string());

        // Check first task is added data file
        assert_eq!(
            tasks[0].data_file_path,
            format!("{}/1.parquet", &fixture.table_location)
        );

        // Check second task is existing data file
        assert_eq!(
            tasks[1].data_file_path,
            format!("{}/3.parquet", &fixture.table_location)
        );
    }

    #[tokio::test]
    async fn test_scan_with_positional_deletes() {
        let mut fixture = TableDeleteTestFixture::new();
        fixture.setup_manifest_files().await;

        // Create table scan for current snapshot and plan files
        let table_scan = fixture.table.scan().select(["x", "z"]).build().unwrap();

        let batch_stream = table_scan.to_arrow().await.unwrap();

        let mut batches: Vec<_> = batch_stream.try_collect().await.unwrap();

        // The default batch size is 1024 and all of our parquet files have 1024 rows.
        // 2.parquet is in a deleted manifest, so only 1.parquet and 3.parquet should
        // be included, so we should expect two batches to come back.
        assert_eq!(batches.len(), 2);

        // ensure we have the expected number of columns
        assert_eq!(batches[0].num_columns(), 2);

        // the batches can come back in arbitrary order. Sort them by
        // the value of the first value in their z column to ensure that
        // we see them in the order we expect
        batches.sort_by(|a, b| {
            let z_col_a = a.column_by_name("z").unwrap();
            let z_arr_a = z_col_a.as_any().downcast_ref::<Int64Array>().unwrap();
            let a = z_arr_a.value(0);

            let z_col_b = b.column_by_name("z").unwrap();
            let z_arr_b = z_col_b.as_any().downcast_ref::<Int64Array>().unwrap();
            let b = z_arr_b.value(0);

            a.cmp(&b)
        });

        let col1 = batches[0].column_by_name("x").unwrap();
        let int64_arr = col1.as_any().downcast_ref::<Int64Array>().unwrap();

        // the delete file should have deleted the rows with these indices from the
        // first batch:
        // 0, 1, 3, 5, 6, 8, 1022, 1023
        // The x column originally contained vales monotonically increasing from 1000,
        // so if the delete file applied successfully the first 5 values should be:
        // 1002, 1004, 1007, 1009, 1010
        assert_eq!(int64_arr.value(0), 1002);
        assert_eq!(int64_arr.value(1), 1004);
        assert_eq!(int64_arr.value(2), 1007);
        assert_eq!(int64_arr.value(3), 1009);
        assert_eq!(int64_arr.value(4), 1010);

        let col1 = batches[1].column_by_name("x").unwrap();
        let int64_arr = col1.as_any().downcast_ref::<Int64Array>().unwrap();

        // the delete file should have deleted the rows with these indices from
        // the second batch:
        // 1, 2, 3, 5, 6, 8, 1022, 1023
        // The x column originally contained vales monotonically increasing from 1000,
        // so if the delete file applied successfully the first 5 values should be:
        // 1000, 1004, 1007, 1009, 1010
        assert_eq!(int64_arr.value(0), 1000);
        assert_eq!(int64_arr.value(1), 1004);
        assert_eq!(int64_arr.value(2), 1007);
        assert_eq!(int64_arr.value(3), 1009);
        assert_eq!(int64_arr.value(4), 1010);

        let col2 = batches[0].column_by_name("z").unwrap();
        let int64_arr = col2.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int64_arr.value(0), 1);

        let col2 = batches[1].column_by_name("z").unwrap();
        let int64_arr = col2.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int64_arr.value(0), 3);
    }

    #[tokio::test]
    async fn test_scan_with_positional_deletes_and_filter() {
        let mut fixture = TableDeleteTestFixture::new();
        fixture.setup_manifest_files().await;

        let mut builder = fixture.table.scan();
        let predicate = Reference::new("y")
            .greater_than_or_equal_to(Datum::long(2020))
            .and(Reference::new("z").less_than(Datum::long(3)));
        builder = builder.with_filter(predicate);

        // Create table scan for current snapshot and plan files
        let table_scan = builder.select(["y", "z"]).build().unwrap();

        let batch_stream = table_scan.to_arrow().await.unwrap();

        let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

        // The default batch size is 1024 and all of our parquet files have 1024 rows.
        // 2.parquet is in a deleted manifest, so only 1.parquet and 3.parquet should
        // be included. The filter should reject 3.parquet due to it not matching the predicate,
        // so we should expect one batch to come back.
        println!("{:?}", &batches);
        assert_eq!(batches.len(), 1);

        // ensure we have the expected number of columns
        assert_eq!(batches[0].num_columns(), 2);

        let col1 = batches[0].column_by_name("y").unwrap();
        let int64_arr = col1.as_any().downcast_ref::<Int64Array>().unwrap();

        // the delete file should have deleted the rows with these indices from the
        // first batch:
        // 0, 1, 3, 5, 6, 8, 1022, 1023
        // The predicate filter should have deleted all rows with x < 1019
        // The x column originally contained vales monotonically increasing from 1000,
        // so the remaining rows should be:
        // 1020, 1021
        assert_eq!(int64_arr.len(), 2);
        assert_eq!(int64_arr.value(0), 2020);
        assert_eq!(int64_arr.value(1), 2021);

        let col2 = batches[0].column_by_name("z").unwrap();
        let int64_arr = col2.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int64_arr.value(0), 1);
    }
}
