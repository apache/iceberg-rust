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

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::Transaction;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, Namespace, NamespaceIdent, TableCreation};
use iceberg_integration_tests::set_test_fixture;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::file::properties::WriterProperties;

#[tokio::test]
async fn test_append_data_file() {
    let fixture = set_test_fixture("test_create_table").await;

    let ns = Namespace::with_properties(
        NamespaceIdent::from_strs(["apple", "ios"]).unwrap(),
        HashMap::from([
            ("owner".to_string(), "ray".to_string()),
            ("community".to_string(), "apache".to_string()),
        ]),
    );

    fixture
        .rest_catalog
        .create_namespace(ns.name(), ns.properties().clone())
        .await
        .unwrap();

    let schema = Schema::builder()
        .with_schema_id(1)
        .with_identifier_field_ids(vec![2])
        .with_fields(vec![
            NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
        ])
        .build()
        .unwrap();

    let table_creation = TableCreation::builder()
        .name("t1".to_string())
        .schema(schema.clone())
        .build();

    let table = fixture
        .rest_catalog
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
    let data_file_writer_builder = DataFileWriterBuilder::new(parquet_writer_builder, None);
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
    let mut append_action = tx.fast_append(None, vec![]).unwrap();
    append_action.add_data_files(data_file.clone()).unwrap();
    let tx = append_action.apply().await.unwrap();
    let table = tx.commit(&fixture.rest_catalog).await.unwrap();

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
    let mut append_action = tx.fast_append(None, vec![]).unwrap();
    append_action.add_data_files(data_file.clone()).unwrap();
    let tx = append_action.apply().await.unwrap();
    let table = tx.commit(&fixture.rest_catalog).await.unwrap();

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
