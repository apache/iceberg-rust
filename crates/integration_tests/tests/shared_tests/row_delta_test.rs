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

//! Integration tests for RowDelta transaction action with REST catalog.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::base_writer::position_delete_writer::PositionDeleteFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, TableCreation};
use iceberg_catalog_rest::RestCatalogBuilder;
use parquet::file::properties::WriterProperties;

use iceberg_integration_tests::ContainerRuntime;

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
async fn test_row_delta_add_delete_files() {
    let fixture = get_shared_containers(ContainerRuntime::Podman);
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns(ContainerRuntime::Podman).await;
    let schema = test_schema();

    let table_creation = TableCreation::builder()
        .name("test_row_delta_add_delete_files".to_string())
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

    // Step 1: Write initial data file
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

    // Step 2: Write position delete file to delete rows 1 and 3 (bar and baz)
    let data_file_path = data_files[0].file_path();

    // Create position delete schema
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

    // Convert Arrow schema to Iceberg schema for the writer
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

    // Create position delete batch - Delete row 1 (index 1, "bar") and row 3 (index 3, "baz")
    let delete_batch = RecordBatch::try_new(position_delete_arrow_schema.clone(), vec![
        Arc::new(StringArray::from(vec![
            data_file_path,
            data_file_path,
        ])) as ArrayRef,
        Arc::new(Int64Array::from(vec![1, 3])) as ArrayRef,
    ])
    .unwrap();

    position_delete_writer.write(delete_batch).await.unwrap();
    let delete_files = position_delete_writer.close().await.unwrap();

    // Step 3: Commit delete files using RowDelta
    let tx = Transaction::new(&table);
    let row_delta = tx.row_delta().add_deletes(delete_files);
    let tx = row_delta.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Step 4: Verify that only 2 rows remain (foo and null)
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
    assert_eq!(total_rows, 2, "Should have 2 rows after deleting 2 rows");

    // Verify the remaining data
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

    // Rows 0 and 2 should remain ("foo" with 1, and null with 3)
    assert_eq!(col1.value(0), "foo");
    assert!(col1.is_null(1));
    assert_eq!(col2.value(0), 1);
    assert_eq!(col2.value(1), 3);
}

#[tokio::test]
async fn test_row_delta_add_data_and_deletes() {
    let fixture = get_shared_containers(ContainerRuntime::Podman);
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns(ContainerRuntime::Podman).await;
    let schema = test_schema();

    let table_creation = TableCreation::builder()
        .name("test_row_delta_add_data_and_deletes".to_string())
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

    // Step 1: Write initial data file
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
        parquet_writer_builder.clone(),
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_file_writer_builder);
    let mut data_file_writer = data_file_writer_builder.build(None).await.unwrap();

    let batch = create_test_data(arrow_schema.clone());
    data_file_writer.write(batch.clone()).await.unwrap();
    let old_data_files = data_file_writer.close().await.unwrap();

    // Commit initial data
    let tx = Transaction::new(&table);
    let append_action = tx.fast_append().add_data_files(old_data_files.clone());
    let tx = append_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Step 2: Write new data file with updated rows
    let rolling_file_writer_builder_2 = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder.clone(),
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );
    let data_file_writer_builder_2 = DataFileWriterBuilder::new(rolling_file_writer_builder_2);
    let mut data_file_writer_2 = data_file_writer_builder_2.build(None).await.unwrap();

    // New data with updated values
    let new_col1 = StringArray::from(vec![Some("updated_bar"), Some("updated_baz")]);
    let new_col2 = Int32Array::from(vec![Some(20), Some(40)]);
    let new_col3 = BooleanArray::from(vec![Some(true), Some(true)]);
    let new_batch = RecordBatch::try_new(arrow_schema.clone(), vec![
        Arc::new(new_col1) as ArrayRef,
        Arc::new(new_col2) as ArrayRef,
        Arc::new(new_col3) as ArrayRef,
    ])
    .unwrap();

    data_file_writer_2.write(new_batch.clone()).await.unwrap();
    let new_data_files = data_file_writer_2.close().await.unwrap();

    // Step 3: Write position delete file to mark old rows as deleted
    let old_data_file_path = old_data_files[0].file_path();

    // Create position delete schema
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

    // Convert Arrow schema to Iceberg schema for the writer
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

    // Create position delete batch - Delete the old "bar" and "baz" rows (rows 1 and 3)
    let delete_batch = RecordBatch::try_new(position_delete_arrow_schema.clone(), vec![
        Arc::new(StringArray::from(vec![
            old_data_file_path,
            old_data_file_path,
        ])) as ArrayRef,
        Arc::new(Int64Array::from(vec![1, 3])) as ArrayRef,
    ])
    .unwrap();

    position_delete_writer.write(delete_batch).await.unwrap();
    let delete_files = position_delete_writer.close().await.unwrap();

    // Step 4: Use RowDelta to add new data and delete files in one transaction (simulates UPDATE)
    let tx = Transaction::new(&table);
    let row_delta = tx
        .row_delta()
        .add_rows(new_data_files)
        .add_deletes(delete_files);
    let tx = row_delta.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Step 5: Verify final state - should have 4 rows total
    // 2 from original data (foo and null), 2 from new data (updated_bar and updated_baz)
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
        total_rows, 4,
        "Should have 4 rows total after UPDATE simulation"
    );

    // Verify snapshot operation type
    let current_snapshot = table.metadata().current_snapshot().unwrap();
    assert_eq!(
        current_snapshot.summary().operation,
        iceberg::spec::Operation::Overwrite,
        "Operation should be Overwrite when adding both data and deletes"
    );
}

#[tokio::test]
async fn test_row_delta_with_validation() {
    let fixture = get_shared_containers(ContainerRuntime::Podman);
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns(ContainerRuntime::Podman).await;
    let schema = test_schema();

    let table_creation = TableCreation::builder()
        .name("test_row_delta_with_validation".to_string())
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

    // Step 1: Write and commit initial data
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
        parquet_writer_builder.clone(),
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_file_writer_builder);
    let mut data_file_writer = data_file_writer_builder.build(None).await.unwrap();

    let batch = create_test_data(arrow_schema.clone());
    data_file_writer.write(batch.clone()).await.unwrap();
    let data_files = data_file_writer.close().await.unwrap();

    let tx = Transaction::new(&table);
    let append_action = tx.fast_append().add_data_files(data_files.clone());
    let tx = append_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Step 2: Snapshot the current state for validation
    let base_snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();

    // Step 3: Create delete files
    let data_file_path = data_files[0].file_path();

    // Create position delete schema
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

    // Convert Arrow schema to Iceberg schema for the writer
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

    // Create position delete batch
    let delete_batch = RecordBatch::try_new(position_delete_arrow_schema.clone(), vec![
        Arc::new(StringArray::from(vec![data_file_path])) as ArrayRef,
        Arc::new(Int64Array::from(vec![1])) as ArrayRef,
    ])
    .unwrap();

    position_delete_writer.write(delete_batch).await.unwrap();
    let delete_files = position_delete_writer.close().await.unwrap();

    // Step 4: Commit with validation enabled
    let tx = Transaction::new(&table);
    let row_delta = tx
        .row_delta()
        .add_deletes(delete_files)
        .validate_from_snapshot(base_snapshot_id)
        .validate_data_files_exist(vec![data_file_path.to_string()]);
    let tx = row_delta.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Verify commit succeeded
    assert!(table.metadata().current_snapshot().is_some());
    assert_ne!(
        table.metadata().current_snapshot().unwrap().snapshot_id(),
        base_snapshot_id,
        "Should have created a new snapshot"
    );

    // Verify data is correct
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
    assert_eq!(total_rows, 3, "Should have 3 rows after deleting 1 row");
}
