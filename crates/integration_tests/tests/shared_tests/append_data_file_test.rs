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

//! Integration tests for rest catalog.

use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use iceberg::transaction::Transaction;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, TableCreation};
use iceberg_catalog_rest::RestCatalog;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::file::properties::WriterProperties;

use crate::get_shared_containers;
use crate::shared_tests::{random_ns, test_schema};

#[tokio::test]
async fn test_append_data_file() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalog::new(fixture.catalog_config.clone());
    let ns = random_ns().await;
    let schema = test_schema();

    let table_creation = TableCreation::builder()
        .name("t1".to_string())
        .schema(schema.clone())
        .build();

    let table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    // Create the writer and write the data
    let schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        "test".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(parquet_writer_builder, None, 0);
    let mut data_file_writer = data_file_writer_builder.build().await.unwrap();
    let col1 = StringArray::from(vec![Some("foo"), Some("bar"), None, Some("baz")]);
    let col2 = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
    let col3 = BooleanArray::from(vec![Some(true), Some(false), None, Some(false)]);
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(col1) as ArrayRef,
        Arc::new(col2) as ArrayRef,
        Arc::new(col3) as ArrayRef,
    ])
    .unwrap();
    data_file_writer.write(batch.clone()).await.unwrap();
    let data_file = data_file_writer.close().await.unwrap();

    // check parquet file schema
    let content = table
        .file_io()
        .new_input(data_file[0].file_path())
        .unwrap()
        .read()
        .await
        .unwrap();
    let parquet_reader = parquet::arrow::arrow_reader::ArrowReaderMetadata::load(
        &content,
        ArrowReaderOptions::default(),
    )
    .unwrap();
    let field_ids: Vec<i32> = parquet_reader
        .parquet_schema()
        .columns()
        .iter()
        .map(|col| col.self_type().get_basic_info().id())
        .collect();
    assert_eq!(field_ids, vec![1, 2, 3]);

    // commit result
    let tx = Transaction::new(&table);
    let mut append_action = tx.fast_append(None, None, vec![]).unwrap();
    append_action.add_data_files(data_file.clone()).unwrap();
    let tx = append_action.apply().await.unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // check result
    let batch_stream = table
        .scan()
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();
    let batches: Vec<_> = batch_stream.try_collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0], batch);

    // commit result again
    let tx = Transaction::new(&table);
    let mut append_action = tx.fast_append(None, None, vec![]).unwrap();
    append_action.add_data_files(data_file.clone()).unwrap();
    let tx = append_action.apply().await.unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // check result again
    let batch_stream = table
        .scan()
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();
    let batches: Vec<_> = batch_stream.try_collect().await.unwrap();
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0], batch);
    assert_eq!(batches[1], batch);
}

#[tokio::test]
async fn test_append_data_file_to_branch() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalog::new(fixture.catalog_config.clone());
    let ns = random_ns().await;
    let schema = test_schema();

    let table_creation = TableCreation::builder()
        .name("t1".to_string())
        .schema(schema.clone())
        .build();

    let table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    // Create the writer and write the data
    let schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        "test".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(parquet_writer_builder, None, 0);
    let mut data_file_writer = data_file_writer_builder.build().await.unwrap();
    let col1 = StringArray::from(vec![Some("foo"), Some("bar"), None, Some("baz")]);
    let col2 = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
    let col3 = BooleanArray::from(vec![Some(true), Some(false), None, Some(false)]);
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(col1) as ArrayRef,
        Arc::new(col2) as ArrayRef,
        Arc::new(col3) as ArrayRef,
    ])
    .unwrap();
    data_file_writer.write(batch.clone()).await.unwrap();
    let data_file = data_file_writer.close().await.unwrap();

    // Test 1: Append to main branch (default behavior)
    let tx = Transaction::new(&table);
    let mut append_action = tx.fast_append(None, None, vec![]).unwrap();
    append_action.add_data_files(data_file.clone()).unwrap();
    let tx = append_action.apply().await.unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Verify main branch has the data
    assert!(table.metadata().current_snapshot().is_some());
    let main_snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();

    // Verify main branch ref points to the snapshot using snapshot_for_ref
    let main_snapshot = table.metadata().snapshot_for_ref("main").unwrap();
    // First commit should have no parent snapshot
    assert_eq!(main_snapshot.parent_snapshot_id(), None);

    let main_snapshot = table.metadata().snapshot_for_ref("main").unwrap();

    // Verify main branch data
    let main_batch_stream = table
        .scan()
        .snapshot_id(main_snapshot.snapshot_id())
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();
    let main_batches: Vec<_> = main_batch_stream.try_collect().await.unwrap();
    assert_eq!(main_batches.len(), 1);
    assert_eq!(main_batches[0], batch);
    assert_eq!(
        main_batches[0].schema(),
        batch.schema(),
        "Main branch schema mismatch"
    );

    // Test 2: Append to a custom branch
    let branch_name = "test-branch";
    let tx = Transaction::new(&table);
    let mut append_action = tx
        .fast_append(None, None, vec![])
        .unwrap()
        .with_to_branch(branch_name.to_string());
    append_action.add_data_files(data_file.clone()).unwrap();
    let tx = append_action.apply().await.unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Verify the custom branch was created and points to a new snapshot
    let branch_snapshot = table.metadata().snapshot_for_ref(branch_name).unwrap();
    assert_ne!(branch_snapshot.snapshot_id(), main_snapshot_id);
    // New branch should have no parent snapshot
    assert_eq!(
        table
            .metadata()
            .snapshot_for_ref(branch_name)
            .unwrap()
            .parent_snapshot_id(),
        None
    );

    // Verify test-branch data
    let branch_batch_stream = table
        .scan()
        .snapshot_id(branch_snapshot.snapshot_id())
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();
    let branch_batches: Vec<_> = branch_batch_stream.try_collect().await.unwrap();
    assert_eq!(branch_batches.len(), 1);
    assert_eq!(branch_batches[0], batch);
    assert_eq!(
        branch_batches[0].schema(),
        batch.schema(),
        "Test branch schema mismatch"
    );

    // Verify the main branch is unchanged
    let main_snapshot_after = table.metadata().snapshot_for_ref("main").unwrap();
    assert_eq!(main_snapshot_after.snapshot_id(), main_snapshot_id);

    // Test 3: Append to the same custom branch again
    let tx = Transaction::new(&table);
    let mut append_action = tx
        .fast_append(None, None, vec![])
        .unwrap()
        .with_to_branch(branch_name.to_string());
    append_action.add_data_files(data_file.clone()).unwrap();
    let tx = append_action.apply().await.unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Verify the custom branch now points to a newer snapshot
    let branch_snapshot_final = table.metadata().snapshot_for_ref(branch_name).unwrap();
    assert_ne!(
        branch_snapshot_final.snapshot_id(),
        branch_snapshot.snapshot_id()
    );
    assert_ne!(branch_snapshot_final.snapshot_id(), main_snapshot_id);
    // Second append should have previous branch snapshot as parent
    assert_eq!(
        table
            .metadata()
            .snapshot_for_ref(branch_name)
            .unwrap()
            .parent_snapshot_id(),
        Some(branch_snapshot.snapshot_id())
    );

    // Verify test-branch data after second append
    let branch_batch_stream = table
        .scan()
        .snapshot_id(branch_snapshot_final.snapshot_id())
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();
    let branch_batches: Vec<_> = branch_batch_stream.try_collect().await.unwrap();
    assert_eq!(branch_batches.len(), 2);
    assert_eq!(branch_batches[0], batch);
    assert_eq!(branch_batches[1], batch);
    assert_eq!(
        branch_batches[0].schema(),
        batch.schema(),
        "Test branch schema mismatch after second append"
    );

    // Verify we have 3 snapshots total (1 main + 2 branch)
    assert_eq!(table.metadata().snapshots().count(), 3);

    // Test 4: Test merge append to branch
    let another_branch = "merge-branch";
    let tx = Transaction::new(&table);
    let mut merge_append_action = tx
        .merge_append(None, vec![])
        .unwrap()
        .with_to_branch(another_branch.to_string())
        .unwrap();
    merge_append_action
        .add_data_files(data_file.clone())
        .unwrap();
    let tx = merge_append_action.apply().await.unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Verify the merge branch was created
    let merge_branch_snapshot = table.metadata().snapshot_for_ref(another_branch).unwrap();
    assert_ne!(merge_branch_snapshot.snapshot_id(), main_snapshot_id);
    assert_ne!(
        merge_branch_snapshot.snapshot_id(),
        branch_snapshot_final.snapshot_id()
    );
    // Merge branch should have no parent snapshot
    assert_eq!(
        table
            .metadata()
            .snapshot_for_ref(another_branch)
            .unwrap()
            .parent_snapshot_id(),
        None
    );

    // Verify we now have 4 snapshots total
    assert_eq!(table.metadata().snapshots().count(), 4);

    // Verify merge-branch data
    let merge_batch_stream = table
        .scan()
        .snapshot_id(merge_branch_snapshot.snapshot_id())
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();
    let merge_batches: Vec<_> = merge_batch_stream.try_collect().await.unwrap();
    assert_eq!(merge_batches.len(), 1);
    assert_eq!(merge_batches[0], batch);
    assert_eq!(
        merge_batches[0].schema(),
        batch.schema(),
        "Merge branch schema mismatch"
    );

    // Verify all branches exist and can be accessed via snapshot_for_ref
    assert!(table.metadata().snapshot_for_ref("main").is_some());
    assert!(table.metadata().snapshot_for_ref(branch_name).is_some());
    assert!(table.metadata().snapshot_for_ref(another_branch).is_some());

    // Verify each branch points to different snapshots
    let final_main_snapshot = table.metadata().snapshot_for_ref("main").unwrap();
    let final_branch_snapshot = table.metadata().snapshot_for_ref(branch_name).unwrap();
    let final_merge_snapshot = table.metadata().snapshot_for_ref(another_branch).unwrap();

    assert_ne!(
        final_main_snapshot.snapshot_id(),
        final_branch_snapshot.snapshot_id()
    );
    assert_ne!(
        final_main_snapshot.snapshot_id(),
        final_merge_snapshot.snapshot_id()
    );
    assert_ne!(
        final_branch_snapshot.snapshot_id(),
        final_merge_snapshot.snapshot_id()
    );
}
