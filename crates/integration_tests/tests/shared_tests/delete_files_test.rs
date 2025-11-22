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
//   Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Integration tests for delete files (position and equality deletes) with REST catalog.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use iceberg::puffin::DeletionVectorWriter;
use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::base_writer::equality_delete_writer::{
    EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
};
use iceberg::writer::base_writer::position_delete_writer::PositionDeleteFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, TableCreation};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_integration_tests::ContainerRuntime;
use parquet::file::properties::WriterProperties;

use crate::get_shared_containers;
use crate::shared_tests::{random_ns, test_schema};

// Constants from Iceberg spec for position delete files
const PARQUET_FIELD_ID_META_KEY: &str = "PARQUET:field_id";
const FIELD_ID_POSITION_DELETE_FILE_PATH: &str = "2147483546";
const FIELD_ID_POSITION_DELETE_POS: &str = "2147483545";

/// Helper function to create test data
fn create_test_data(schema: Arc<arrow_schema::Schema>) -> RecordBatch {
    let col1 = StringArray::from(vec![Some("foo"), Some("bar"), None, Some("baz")]);
    let col2 = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
    let col3 = BooleanArray::from(vec![Some(true), Some(false), None, Some(false)]);

    RecordBatch::try_new(schema, vec![
        Arc::new(col1) as ArrayRef,
        Arc::new(col2) as ArrayRef,
        Arc::new(col3) as ArrayRef,
    ])
    .unwrap()
}

#[tokio::test]
async fn test_position_deletes_with_append_action() {
    let fixture = get_shared_containers(ContainerRuntime::Podman);
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns(ContainerRuntime::Podman).await;
    let schema = test_schema();

    let table_creation = TableCreation::builder()
        .name("test_position_deletes".to_string())
        .schema(schema.clone())
        .build();

    let table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    let arrow_schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );

    // Step 1: Write and commit initial data file
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        "data".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
    let rolling_file_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_file_writer_builder);
    let mut data_file_writer = data_file_writer_builder.build(None).await.unwrap();

    let batch = create_test_data(arrow_schema.clone());
    data_file_writer.write(batch.clone()).await.unwrap();
    let data_files = data_file_writer.close().await.unwrap();

    // Commit initial data
    let tx = Transaction::new(&table);
    let append_action = tx.fast_append().add_data_files(data_files.clone());
    let tx = append_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Step 2: Write position delete file to delete rows 0 and 2 (foo and null)
    let data_file_path = data_files[0].file_path();

    // Create position delete schema with required field IDs
    let position_delete_arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("file_path", DataType::Utf8, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            FIELD_ID_POSITION_DELETE_FILE_PATH.to_string(),
        )])),
        Field::new("pos", DataType::Int64, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            FIELD_ID_POSITION_DELETE_POS.to_string(),
        )])),
    ]));

    // Convert Arrow schema to Iceberg schema
    let position_delete_schema: Arc<iceberg::spec::Schema> =
        Arc::new((&*position_delete_arrow_schema).try_into().unwrap());

    let delete_file_name_generator = DefaultFileNameGenerator::new(
        "deletes".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let delete_parquet_writer_builder =
        ParquetWriterBuilder::new(WriterProperties::default(), position_delete_schema);
    let delete_rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        delete_parquet_writer_builder,
        table.file_io().clone(),
        location_generator.clone(),
        delete_file_name_generator,
    );
    let position_delete_writer_builder =
        PositionDeleteFileWriterBuilder::new(delete_rolling_writer_builder);
    let mut position_delete_writer = position_delete_writer_builder.build(None).await.unwrap();

    // Create position delete batch - Delete row 0 ("foo") and row 2 (null)
    let delete_batch = RecordBatch::try_new(position_delete_arrow_schema.clone(), vec![
        Arc::new(StringArray::from(vec![data_file_path, data_file_path])) as ArrayRef,
        Arc::new(Int64Array::from(vec![0, 2])) as ArrayRef,
    ])
    .unwrap();

    position_delete_writer.write(delete_batch).await.unwrap();
    let delete_files = position_delete_writer.close().await.unwrap();

    // Step 3: Commit delete files using AppendDeleteFilesAction
    let tx = Transaction::new(&table);
    let append_deletes_action = tx.append_delete_files().add_files(delete_files);
    let tx = append_deletes_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Step 4: Verify that only 2 rows remain (bar and baz)
    let batch_stream = table
        .scan()
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();
    let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 2,
        "Should have 2 rows after deleting rows 0 and 2"
    );

    // Verify the remaining data - should be "bar" and "baz"
    assert_eq!(batches.len(), 1);
    let result_batch = &batches[0];

    let col1 = result_batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let col2 = result_batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    assert_eq!(col1.value(0), "bar");
    assert_eq!(col1.value(1), "baz");
    assert_eq!(col2.value(0), 2);
    assert_eq!(col2.value(1), 4);
}

#[tokio::test]
async fn test_equality_deletes_with_append_action() {
    let fixture = get_shared_containers(ContainerRuntime::Podman);
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns(ContainerRuntime::Podman).await;
    let schema = test_schema();

    let table_creation = TableCreation::builder()
        .name("test_equality_deletes".to_string())
        .schema(schema.clone())
        .build();

    let table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    let arrow_schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );

    // Step 1: Write and commit initial data file
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        "data".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
    let rolling_file_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_file_writer_builder);
    let mut data_file_writer = data_file_writer_builder.build(None).await.unwrap();

    let batch = create_test_data(arrow_schema.clone());
    data_file_writer.write(batch.clone()).await.unwrap();
    let data_files = data_file_writer.close().await.unwrap();

    // Commit initial data
    let tx = Transaction::new(&table);
    let append_action = tx.fast_append().add_data_files(data_files.clone());
    let tx = append_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Step 2: Write equality delete file to delete rows where col2 (bar) = 2
    // Create equality delete writer using field ID 2 (bar column)
    let equality_ids = vec![2i32]; // Field ID for "bar" column

    let equality_delete_config =
        EqualityDeleteWriterConfig::new(equality_ids, table.metadata().current_schema().clone())
            .unwrap();

    // Use the projected schema from the equality config for the delete file
    let delete_schema: Arc<iceberg::spec::Schema> = Arc::new(
        iceberg::arrow::arrow_schema_to_schema(equality_delete_config.projected_arrow_schema_ref())
            .unwrap(),
    );

    let delete_file_name_generator = DefaultFileNameGenerator::new(
        "eq_deletes".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let delete_parquet_writer_builder =
        ParquetWriterBuilder::new(WriterProperties::default(), delete_schema);
    let delete_rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        delete_parquet_writer_builder,
        table.file_io().clone(),
        location_generator.clone(),
        delete_file_name_generator,
    );
    let equality_delete_writer_builder =
        EqualityDeleteFileWriterBuilder::new(delete_rolling_writer_builder, equality_delete_config);
    let mut equality_delete_writer = equality_delete_writer_builder.build(None).await.unwrap();

    // Create equality delete batch - Delete row where id=2 (which is "bar")
    let equality_delete_batch = RecordBatch::try_new(arrow_schema.clone(), vec![
        Arc::new(StringArray::from(vec![Some("bar")])) as ArrayRef,
        Arc::new(Int32Array::from(vec![Some(2)])) as ArrayRef,
        Arc::new(BooleanArray::from(vec![Some(false)])) as ArrayRef,
    ])
    .unwrap();

    equality_delete_writer
        .write(equality_delete_batch)
        .await
        .unwrap();
    let delete_files = equality_delete_writer.close().await.unwrap();

    // Step 3: Commit delete files using AppendDeleteFilesAction
    let tx = Transaction::new(&table);
    let append_deletes_action = tx.append_delete_files().add_files(delete_files);
    let tx = append_deletes_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Step 4: Verify that only 3 rows remain (foo, null, and baz - bar was deleted)
    let batch_stream = table
        .scan()
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();
    let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "Should have 3 rows after equality delete");

    // Verify "bar" (id=2) was deleted
    assert_eq!(batches.len(), 1);
    let result_batch = &batches[0];

    let col2 = result_batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    // Verify remaining rows have id 1, 3, 4 (not 2)
    let mut ids: Vec<i32> = (0..result_batch.num_rows())
        .map(|i| col2.value(i))
        .collect();
    ids.sort();
    assert_eq!(ids, vec![1, 3, 4], "Row with id=2 should be deleted");
}

#[tokio::test]
async fn test_multiple_delete_files() {
    let fixture = get_shared_containers(ContainerRuntime::Podman);
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns(ContainerRuntime::Podman).await;
    let schema = test_schema();

    let table_creation = TableCreation::builder()
        .name("test_multiple_deletes".to_string())
        .schema(schema.clone())
        .build();

    let table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    let arrow_schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );

    // Step 1: Write and commit initial data file with 4 rows
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        "data".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
    let rolling_file_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_file_writer_builder);
    let mut data_file_writer = data_file_writer_builder.build(None).await.unwrap();

    let batch = create_test_data(arrow_schema.clone());
    data_file_writer.write(batch.clone()).await.unwrap();
    let data_files = data_file_writer.close().await.unwrap();

    // Commit initial data
    let tx = Transaction::new(&table);
    let append_action = tx.fast_append().add_data_files(data_files.clone());
    let tx = append_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    let data_file_path = data_files[0].file_path();

    // Step 2: Create first position delete file to delete row 0 ("foo")
    let position_delete_arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("file_path", DataType::Utf8, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            FIELD_ID_POSITION_DELETE_FILE_PATH.to_string(),
        )])),
        Field::new("pos", DataType::Int64, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            FIELD_ID_POSITION_DELETE_POS.to_string(),
        )])),
    ]));

    let position_delete_schema: Arc<iceberg::spec::Schema> =
        Arc::new((&*position_delete_arrow_schema).try_into().unwrap());

    let delete_file_name_generator = DefaultFileNameGenerator::new(
        "deletes".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let delete_parquet_writer_builder =
        ParquetWriterBuilder::new(WriterProperties::default(), position_delete_schema);
    let delete_rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        delete_parquet_writer_builder,
        table.file_io().clone(),
        location_generator.clone(),
        delete_file_name_generator,
    );
    let position_delete_writer_builder =
        PositionDeleteFileWriterBuilder::new(delete_rolling_writer_builder);
    let mut position_delete_writer = position_delete_writer_builder.build(None).await.unwrap();

    let delete_batch1 = RecordBatch::try_new(position_delete_arrow_schema.clone(), vec![
        Arc::new(StringArray::from(vec![data_file_path])) as ArrayRef,
        Arc::new(Int64Array::from(vec![0])) as ArrayRef, // Delete row 0 ("foo")
    ])
    .unwrap();

    position_delete_writer.write(delete_batch1).await.unwrap();
    let mut all_delete_files = position_delete_writer.close().await.unwrap();

    // Step 3: Create second position delete file to delete row 2 (null)
    let delete_file_name_generator2 = DefaultFileNameGenerator::new(
        "deletes2".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );

    let position_delete_schema2: Arc<iceberg::spec::Schema> =
        Arc::new((&*position_delete_arrow_schema).try_into().unwrap());
    let delete_parquet_writer_builder2 =
        ParquetWriterBuilder::new(WriterProperties::default(), position_delete_schema2);
    let delete_rolling_writer_builder2 = RollingFileWriterBuilder::new_with_default_file_size(
        delete_parquet_writer_builder2,
        table.file_io().clone(),
        location_generator.clone(),
        delete_file_name_generator2,
    );
    let position_delete_writer_builder2 =
        PositionDeleteFileWriterBuilder::new(delete_rolling_writer_builder2);
    let mut position_delete_writer2 = position_delete_writer_builder2.build(None).await.unwrap();

    let delete_batch2 = RecordBatch::try_new(position_delete_arrow_schema.clone(), vec![
        Arc::new(StringArray::from(vec![data_file_path])) as ArrayRef,
        Arc::new(Int64Array::from(vec![2])) as ArrayRef, // Delete row 2 (null)
    ])
    .unwrap();

    position_delete_writer2.write(delete_batch2).await.unwrap();
    let delete_files2 = position_delete_writer2.close().await.unwrap();
    all_delete_files.extend(delete_files2);

    // Step 4: Commit both delete files in a single transaction
    let tx = Transaction::new(&table);
    let append_deletes_action = tx.append_delete_files().add_files(all_delete_files);
    let tx = append_deletes_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Step 5: Verify that only 2 rows remain (bar and baz)
    let batch_stream = table
        .scan()
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();
    let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 2,
        "Should have 2 rows after deleting with 2 delete files"
    );

    // Verify the remaining data - should be "bar" (id=2) and "baz" (id=4)
    assert_eq!(batches.len(), 1);
    let result_batch = &batches[0];

    let col2 = result_batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    let mut ids: Vec<i32> = (0..result_batch.num_rows())
        .map(|i| col2.value(i))
        .collect();
    ids.sort();
    assert_eq!(ids, vec![2, 4], "Should have id=2 (bar) and id=4 (baz)");
}

#[tokio::test]
async fn test_deletion_vectors_with_puffin() {
    let fixture = get_shared_containers(ContainerRuntime::Podman);
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns(ContainerRuntime::Podman).await;
    let schema = test_schema();

    let table_creation = TableCreation::builder()
        .name("test_deletion_vectors".to_string())
        .schema(schema.clone())
        .build();

    let table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    let arrow_schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );

    // Step 1: Write and commit initial data file
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        "data".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
    let rolling_file_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_file_writer_builder);
    let mut data_file_writer = data_file_writer_builder.build(None).await.unwrap();

    let batch = create_test_data(arrow_schema.clone());
    data_file_writer.write(batch.clone()).await.unwrap();
    let data_files = data_file_writer.close().await.unwrap();

    // Commit initial data
    let tx = Transaction::new(&table);
    let append_action = tx.fast_append().add_data_files(data_files.clone());
    let tx = append_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Get the data file path for the deletion vector reference
    let data_file_path = data_files[0].file_path().to_string();

    // Step 2: Create deletion vector (Puffin format) to delete rows 0 and 2 (foo and null)
    let deletion_vector = DeletionVectorWriter::create_deletion_vector(vec![0u64, 2u64]).unwrap();

    // Generate a unique path for the Puffin file
    let puffin_path = format!(
        "{}/metadata/deletion-vector-{}.puffin",
        table.metadata().location(),
        uuid::Uuid::new_v4()
    );

    // Write the deletion vector to a Puffin file
    let dv_writer = DeletionVectorWriter::new(
        table.file_io().clone(),
        table.metadata().current_snapshot().unwrap().snapshot_id(),
        table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number(),
    );

    let dv_metadata = dv_writer
        .write_single_deletion_vector(&puffin_path, &data_file_path, deletion_vector)
        .await
        .unwrap();

    // Step 3: Create a DataFile entry for the deletion vector
    // Deletion vectors are identified by having referenced_data_file, content_offset, and content_size_in_bytes
    let deletion_vector_file = DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_path(puffin_path.clone())
        .file_format(DataFileFormat::Puffin)
        .partition(Struct::empty())
        .record_count(2) // 2 positions deleted
        .file_size_in_bytes(0) // Will be updated by the catalog
        .referenced_data_file(Some(data_file_path.clone()))
        .content_offset(Some(dv_metadata.offset))
        .content_size_in_bytes(Some(dv_metadata.length))
        .build()
        .unwrap();

    // Step 4: Commit the deletion vector file using AppendDeleteFilesAction
    let tx = Transaction::new(&table);
    let append_deletes_action = tx
        .append_delete_files()
        .add_files(vec![deletion_vector_file]);
    let tx = append_deletes_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Step 5: Verify that only 2 rows remain (bar and baz - foo and null were deleted)
    let batch_stream = table
        .scan()
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();

    let batches: Vec<RecordBatch> = batch_stream.try_collect().await.unwrap();
    assert_eq!(batches.len(), 1, "Should have exactly one batch");
    let result_batch = &batches[0];
    assert_eq!(
        result_batch.num_rows(),
        2,
        "Should have 2 rows remaining after deletion vector applied"
    );

    // Verify the remaining rows are "bar" (id=2) and "baz" (id=4)
    let col1 = result_batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let col2 = result_batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    let mut ids: Vec<i32> = (0..result_batch.num_rows())
        .map(|i| col2.value(i))
        .collect();
    ids.sort();
    assert_eq!(
        ids,
        vec![2, 4],
        "Should have id=2 (bar) and id=4 (baz) after deletion vector applied"
    );

    // Also verify the string values
    let mut names: Vec<Option<&str>> = (0..result_batch.num_rows())
        .map(|i| {
            if col1.is_null(i) {
                None
            } else {
                Some(col1.value(i))
            }
        })
        .collect();
    names.sort();
    assert_eq!(
        names,
        vec![Some("bar"), Some("baz")],
        "Should have bar and baz after deletion vector applied"
    );
}
