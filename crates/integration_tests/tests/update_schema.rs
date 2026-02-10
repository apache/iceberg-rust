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

//! Integration tests for the `UpdateSchemaAction`.

mod common;

use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray, StructArray};
use common::{random_ns, test_schema};
use futures::TryStreamExt;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, TableCreation};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_integration_tests::get_test_fixture;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::file::properties::WriterProperties;

/// Creates a table, appends data, adds a new field to the schema,
/// verifies existing data is still readable, then appends data with the new schema.
#[tokio::test]
async fn test_add_field() {
    let fixture = get_test_fixture();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
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

    // Create the writer and write initial data
    let arrow_schema: Arc<arrow_schema::Schema> = Arc::new(
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
    );
    let rolling_file_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_file_writer_builder);
    let mut data_file_writer = data_file_writer_builder.build(None).await.unwrap();
    let col1 = StringArray::from(vec![Some("foo"), Some("bar"), None, Some("baz")]);
    let col2 = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
    let col3 = BooleanArray::from(vec![Some(true), Some(false), None, Some(false)]);
    let batch = RecordBatch::try_new(arrow_schema.clone(), vec![
        Arc::new(col1) as ArrayRef,
        Arc::new(col2) as ArrayRef,
        Arc::new(col3) as ArrayRef,
    ])
    .unwrap();
    data_file_writer.write(batch.clone()).await.unwrap();
    let data_file = data_file_writer.close().await.unwrap();

    // Check parquet file schema has the expected field IDs
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

    // Commit the initial data
    let tx = Transaction::new(&table);
    let append_action = tx.fast_append().add_data_files(data_file.clone());
    let tx = append_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Verify the initial data is readable
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

    // Add a new optional primitive field to the table
    let tx = Transaction::new(&table);
    let add_action = tx.update_schema().add_column(
        "a",
        iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
    );
    let tx = add_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Verify existing data is still readable after schema evolution
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

    // Add a struct column, then add a nested column inside it
    let tx = Transaction::new(&table);
    let add_action = tx.update_schema().add_column(
        "info",
        iceberg::spec::Type::Struct(iceberg::spec::StructType::new(vec![Arc::new(
            iceberg::spec::NestedField::optional(
                0,
                "city",
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String),
            ),
        )])),
    );
    let tx = add_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Verify the struct column was added
    let schema = table.metadata().current_schema();
    let info_field = schema
        .field_by_name("info")
        .expect("info field should exist");
    assert!(matches!(
        info_field.field_type.as_ref(),
        iceberg::spec::Type::Struct(_)
    ));
    let city_field = schema
        .field_by_name("info.city")
        .expect("info.city field should exist");
    assert!(matches!(
        city_field.field_type.as_ref(),
        iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String)
    ));

    // Add a nested column to the struct
    let tx = Transaction::new(&table);
    let add_action = tx.update_schema().add_column_to(
        "info",
        "zip",
        iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String),
    );
    let tx = add_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Verify the nested column was added
    let schema = table.metadata().current_schema();
    let zip_field = schema
        .field_by_name("info.zip")
        .expect("info.zip field should exist");
    assert!(matches!(
        zip_field.field_type.as_ref(),
        iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String)
    ));

    // Verify existing data is still readable
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

    // Create a new writer with the evolved schema and write data including the new field
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
    let col1 = StringArray::from(vec![Some("foo"), Some("bar"), None, Some("baz")]);
    let col2 = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
    let col3 = BooleanArray::from(vec![Some(true), Some(false), None, Some(false)]);
    let col4 = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
    let evolved_arrow_schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );
    // Build a struct array for the "info" column: {city, zip}
    let city_array = StringArray::from(vec![Some("NYC"), Some("LA"), None, Some("SF")]);
    let zip_array = StringArray::from(vec![Some("10001"), None, Some("90001"), Some("94101")]);
    let info_fields = evolved_arrow_schema
        .field_with_name("info")
        .unwrap()
        .data_type()
        .clone();
    let struct_fields = match &info_fields {
        arrow_schema::DataType::Struct(fields) => fields.clone(),
        _ => panic!("expected struct type for info"),
    };
    let info_array = StructArray::try_new(
        struct_fields,
        vec![
            Arc::new(city_array) as ArrayRef,
            Arc::new(zip_array) as ArrayRef,
        ],
        None,
    )
    .unwrap();
    let batch_with_new_field = RecordBatch::try_new(evolved_arrow_schema.clone(), vec![
        Arc::new(col1) as ArrayRef,
        Arc::new(col2) as ArrayRef,
        Arc::new(col3) as ArrayRef,
        Arc::new(col4) as ArrayRef,
        Arc::new(info_array) as ArrayRef,
    ])
    .unwrap();
    data_file_writer
        .write(batch_with_new_field.clone())
        .await
        .unwrap();
    let data_file = data_file_writer.close().await.unwrap();

    // Commit the new data with evolved schema
    let tx = Transaction::new(&table);
    let append_action = tx.fast_append().add_data_files(data_file.clone());
    let tx = append_action.apply(tx).unwrap();
    let _table = tx.commit(&rest_catalog).await.unwrap();
}

/// Creates a table, adds data, deletes a non-identifier column,
/// and verifies the schema was updated and existing data is still readable.
#[tokio::test]
async fn test_delete_field() {
    let fixture = get_test_fixture();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;
    let schema = test_schema();

    let table_creation = TableCreation::builder()
        .name("t_delete".to_string())
        .schema(schema.clone())
        .build();

    let table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    // Write initial data with all three columns
    let arrow_schema: Arc<arrow_schema::Schema> = Arc::new(
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
    );
    let rolling_file_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_file_writer_builder);
    let mut data_file_writer = data_file_writer_builder.build(None).await.unwrap();
    let col1 = StringArray::from(vec![Some("foo"), Some("bar")]);
    let col2 = Int32Array::from(vec![Some(1), Some(2)]);
    let col3 = BooleanArray::from(vec![Some(true), Some(false)]);
    let batch = RecordBatch::try_new(arrow_schema.clone(), vec![
        Arc::new(col1) as ArrayRef,
        Arc::new(col2) as ArrayRef,
        Arc::new(col3) as ArrayRef,
    ])
    .unwrap();
    data_file_writer.write(batch.clone()).await.unwrap();
    let data_file = data_file_writer.close().await.unwrap();

    // Commit the initial data
    let tx = Transaction::new(&table);
    let append_action = tx.fast_append().add_data_files(data_file.clone());
    let tx = append_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Delete the optional "baz" column (field 3, not an identifier)
    let tx = Transaction::new(&table);
    let delete_action = tx.update_schema().delete_column("baz");
    let tx = delete_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Verify the schema no longer contains "baz"
    let schema = table.metadata().current_schema();
    assert!(
        schema.field_by_name("baz").is_none(),
        "baz should have been deleted"
    );
    assert!(
        schema.field_by_name("foo").is_some(),
        "foo should still exist"
    );
    assert!(
        schema.field_by_name("bar").is_some(),
        "bar should still exist"
    );

    // Verify existing data is still readable after the column deletion
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

    // Deleting an identifier field should fail
    let tx = Transaction::new(&table);
    let delete_action = tx.update_schema().delete_column("bar");
    let tx = delete_action.apply(tx).unwrap();
    let result = tx.commit(&rest_catalog).await;
    assert!(result.is_err(), "deleting an identifier field should fail");

    // Deleting a non-existent column should fail
    let tx = Transaction::new(&table);
    let delete_action = tx.update_schema().delete_column("nonexistent");
    let tx = delete_action.apply(tx).unwrap();
    let result = tx.commit(&rest_catalog).await;
    assert!(
        result.is_err(),
        "deleting a non-existent column should fail"
    );
}
