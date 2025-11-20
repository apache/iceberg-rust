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

//! End-to-end integration tests for deletion vectors with Puffin files

use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;

use iceberg::arrow::ArrowReaderBuilder;
use iceberg::io::FileIO;
use iceberg::puffin::DeletionVectorWriter;
use iceberg::scan::{FileScanTask, FileScanTaskDeleteFile};
use iceberg::spec::{DataContentType, DataFileFormat, Schema};

/// Test reading a data file with deletion vectors applied
#[tokio::test]
async fn test_read_with_deletion_vectors() {
    let tmp_dir = TempDir::new().unwrap();
    let table_location = tmp_dir.path();
    let file_io = FileIO::from_path(table_location.as_os_str().to_str().unwrap())
        .unwrap()
        .build()
        .unwrap();

    // Step 1: Create a Parquet data file with some test data
    let data_file_path = format!("{}/data.parquet", table_location.to_str().unwrap());
    create_test_parquet_file(&data_file_path, 100);

    // Step 2: Create deletion vector marking rows to delete
    // Delete rows: 0, 5, 10, 25, 50, 75, 99
    let positions_to_delete = vec![0u64, 5, 10, 25, 50, 75, 99];
    let delete_vector =
        DeletionVectorWriter::create_deletion_vector(positions_to_delete.clone()).unwrap();

    // Step 3: Write deletion vector to Puffin file
    let puffin_path = format!("{}/deletes.puffin", table_location.to_str().unwrap());
    let writer = DeletionVectorWriter::new(file_io.clone(), 1, 1);
    let dv_metadata = writer
        .write_single_deletion_vector(&puffin_path, &data_file_path, delete_vector)
        .await
        .unwrap();

    // Step 4: Create FileScanTask with deletion vector
    let table_schema = create_test_schema();
    let delete_task = FileScanTaskDeleteFile {
        file_path: puffin_path.clone(),
        file_type: DataContentType::PositionDeletes,
        partition_spec_id: 0,
        equality_ids: None,
        referenced_data_file: Some(data_file_path.clone()),
        content_offset: Some(dv_metadata.offset),
        content_size_in_bytes: Some(dv_metadata.length),
    };

    let scan_task = FileScanTask {
        start: 0,
        length: 0,
        record_count: Some(100),
        data_file_path: data_file_path.clone(),
        data_file_format: DataFileFormat::Parquet,
        schema: table_schema.clone(),
        project_field_ids: vec![1, 2],
        predicate: None,
        deletes: vec![delete_task],
        partition: None,
        partition_spec: None,
        name_mapping: None,
    };

    // Step 5: Read the data with deletion vector applied
    let reader = ArrowReaderBuilder::new(file_io.clone())
        .with_row_selection_enabled(true)
        .build();

    let stream = reader
        .read(Box::pin(futures::stream::once(async { Ok(scan_task) })))
        .unwrap();

    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

    // Step 6: Verify results
    // We started with 100 rows and deleted 7, so we should have 93 rows
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 93,
        "Expected 93 rows after deletion vector application"
    );

    // Verify that deleted rows are not present
    let mut all_ids = Vec::new();
    for batch in &batches {
        let id_array = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..id_array.len() {
            all_ids.push(id_array.value(i));
        }
    }

    // Deleted row IDs should not be in the results
    for deleted_pos in &positions_to_delete {
        assert!(
            !all_ids.contains(&(*deleted_pos as i64)),
            "Deleted row {} should not be present",
            deleted_pos
        );
    }

    // Non-deleted rows should be present
    for id in 0..100i64 {
        let is_deleted = positions_to_delete.contains(&(id as u64));
        let is_present = all_ids.contains(&id);
        assert_eq!(
            is_present, !is_deleted,
            "Row {} presence mismatch",
            id
        );
    }
}

/// Test reading with multiple deletion vectors for different data files
#[tokio::test]
async fn test_read_multiple_files_with_deletion_vectors() {
    let tmp_dir = TempDir::new().unwrap();
    let table_location = tmp_dir.path();
    let file_io = FileIO::from_path(table_location.as_os_str().to_str().unwrap())
        .unwrap()
        .build()
        .unwrap();

    // Create two data files
    let data_file_1 = format!("{}/data1.parquet", table_location.to_str().unwrap());
    let data_file_2 = format!("{}/data2.parquet", table_location.to_str().unwrap());
    create_test_parquet_file(&data_file_1, 50);
    create_test_parquet_file_with_offset(&data_file_2, 50, 50); // IDs 50-99

    // Create deletion vectors for both files
    let mut deletion_vectors = HashMap::new();

    let dv1 = DeletionVectorWriter::create_deletion_vector(vec![0, 10, 20]).unwrap();
    deletion_vectors.insert(data_file_1.clone(), dv1);

    let dv2 = DeletionVectorWriter::create_deletion_vector(vec![5, 15, 25]).unwrap();
    deletion_vectors.insert(data_file_2.clone(), dv2);

    // Write to Puffin file
    let puffin_path = format!("{}/multi-deletes.puffin", table_location.to_str().unwrap());
    let writer = DeletionVectorWriter::new(file_io.clone(), 1, 1);
    let metadata_map = writer
        .write_deletion_vectors(&puffin_path, deletion_vectors)
        .await
        .unwrap();

    // Create scan tasks for both files
    let table_schema = create_test_schema();

    let dv_metadata_1 = metadata_map.get(&data_file_1).unwrap();
    let delete_task_1 = FileScanTaskDeleteFile {
        file_path: puffin_path.clone(),
        file_type: DataContentType::PositionDeletes,
        partition_spec_id: 0,
        equality_ids: None,
        referenced_data_file: Some(data_file_1.clone()),
        content_offset: Some(dv_metadata_1.offset),
        content_size_in_bytes: Some(dv_metadata_1.length),
    };

    let scan_task_1 = FileScanTask {
        start: 0,
        length: 0,
        record_count: Some(50),
        data_file_path: data_file_1.clone(),
        data_file_format: DataFileFormat::Parquet,
        schema: table_schema.clone(),
        project_field_ids: vec![1, 2],
        predicate: None,
        deletes: vec![delete_task_1],
        partition: None,
        partition_spec: None,
        name_mapping: None,
    };

    let dv_metadata_2 = metadata_map.get(&data_file_2).unwrap();
    let delete_task_2 = FileScanTaskDeleteFile {
        file_path: puffin_path.clone(),
        file_type: DataContentType::PositionDeletes,
        partition_spec_id: 0,
        equality_ids: None,
        referenced_data_file: Some(data_file_2.clone()),
        content_offset: Some(dv_metadata_2.offset),
        content_size_in_bytes: Some(dv_metadata_2.length),
    };

    let scan_task_2 = FileScanTask {
        start: 0,
        length: 0,
        record_count: Some(50),
        data_file_path: data_file_2.clone(),
        data_file_format: DataFileFormat::Parquet,
        schema: table_schema.clone(),
        project_field_ids: vec![1, 2],
        predicate: None,
        deletes: vec![delete_task_2],
        partition: None,
        partition_spec: None,
        name_mapping: None,
    };

    // Read both files
    let reader = ArrowReaderBuilder::new(file_io.clone())
        .with_row_selection_enabled(true)
        .build();

    let stream = reader
        .read(Box::pin(futures::stream::iter(vec![
            Ok(scan_task_1),
            Ok(scan_task_2),
        ])))
        .unwrap();

    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

    // Verify: 100 total rows - 6 deleted = 94 rows
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 94, "Expected 94 rows after deletions");
}

/// Test reading with 64-bit deletion vector positions
#[tokio::test]
async fn test_read_with_64bit_deletion_positions() {
    let tmp_dir = TempDir::new().unwrap();
    let table_location = tmp_dir.path();
    let file_io = FileIO::from_path(table_location.as_os_str().to_str().unwrap())
        .unwrap()
        .build()
        .unwrap();

    // Create a data file
    let data_file_path = format!("{}/large-data.parquet", table_location.to_str().unwrap());
    create_test_parquet_file(&data_file_path, 100);

    // Create deletion vector with 64-bit positions (simulating very large files)
    let positions_to_delete = vec![
        0u64,
        99u64,
        (1u64 << 32) + 42, // This would be beyond the actual file, but tests 64-bit support
    ];

    let delete_vector =
        DeletionVectorWriter::create_deletion_vector(positions_to_delete.clone()).unwrap();

    // Write to Puffin file
    let puffin_path = format!("{}/64bit-deletes.puffin", table_location.to_str().unwrap());
    let writer = DeletionVectorWriter::new(file_io.clone(), 1, 1);
    let dv_metadata = writer
        .write_single_deletion_vector(&puffin_path, &data_file_path, delete_vector)
        .await
        .unwrap();

    // The deletion vector should serialize and deserialize correctly
    // Even though some positions are beyond the file size, the format supports them
    assert!(dv_metadata.length > 0);
    assert_eq!(dv_metadata.offset, 4);

    // Verify the deletion vector was written correctly by reading it back
    use iceberg::puffin::{PuffinReader, deserialize_deletion_vector};

    let input_file = file_io.new_input(&puffin_path).unwrap();
    let puffin_reader = PuffinReader::new(input_file);
    let file_metadata = puffin_reader.file_metadata().await.unwrap();
    let blob = puffin_reader.blob(&file_metadata.blobs()[0]).await.unwrap();

    let loaded_treemap = deserialize_deletion_vector(blob.data()).unwrap();
    let loaded_dv = iceberg::delete_vector::DeleteVector::new(loaded_treemap);

    assert_eq!(loaded_dv.len(), 3);
    for pos in positions_to_delete {
        assert!(loaded_dv.iter().any(|p| p == pos));
    }
}

// Helper functions

fn create_test_schema() -> Arc<Schema> {
    Arc::new(
        Schema::builder()
            .with_fields(vec![
                iceberg::spec::NestedField::optional(1, "id", iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long)).into(),
                iceberg::spec::NestedField::optional(2, "name", iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap(),
    )
}

fn create_test_parquet_file(path: &str, num_rows: usize) {
    create_test_parquet_file_with_offset(path, num_rows, 0);
}

fn create_test_parquet_file_with_offset(path: &str, num_rows: usize, offset: i64) {
    let ids: Vec<i64> = (offset..offset + num_rows as i64).collect();
    let names: Vec<String> = (0..num_rows).map(|i| format!("name_{}", i)).collect();

    let id_array = Arc::new(Int64Array::from(ids));
    let name_array = Arc::new(StringArray::from(names));

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, true).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "1".to_string(),
        )])),
        Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "2".to_string(),
        )])),
    ]));

    let batch = RecordBatch::try_new(schema.clone(), vec![id_array, name_array]).unwrap();

    let file = File::create(path).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}
