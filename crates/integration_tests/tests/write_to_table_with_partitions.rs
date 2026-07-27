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

mod common;

use std::sync::Arc;

use arrow_array::{ArrayRef, FixedSizeBinaryArray, RecordBatch};
use common::random_ns;
use futures::TryStreamExt;
use iceberg::spec::{
    Literal, NestedField, PartitionKey, PrimitiveType, Schema, Struct, Transform, Type,
    UnboundPartitionField, UnboundPartitionSpec,
};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, TableCreation};
use iceberg_integration_tests::get_test_fixture;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

#[tokio::test]
async fn test_writing_to_a_table_with_uuid_partition() {
    let fixture = get_test_fixture();
    let rest_catalog = fixture.rest_catalog().await;

    let schema = Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            NestedField::required(1, "uuid", Type::Primitive(PrimitiveType::Uuid)).into(),
        ])
        .build()
        .unwrap();

    let table_creation = TableCreation::builder()
        .name("t1".to_string())
        .partition_spec(
            UnboundPartitionSpec::builder()
                .with_spec_id(0)
                .add_partition_fields([UnboundPartitionField::builder()
                    .source_id(1)
                    .field_id(1)
                    .name("uuid".to_string())
                    .transform(Transform::Identity)
                    .build()])
                .unwrap()
                .build(),
        )
        .schema(schema.clone())
        .build();

    let ns = random_ns().await;
    let table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    // Create the writer and write the data
    let arrow_schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );
    let location_generator = DefaultLocationGenerator::new(table.metadata()).unwrap();
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

    let table_metadata = table.metadata();
    let spec = table_metadata.default_partition_spec().as_ref().clone();
    let table_schema = table_metadata.current_schema().clone();

    let uuid_value = Uuid::from_u128(0xa1a2a3a4b1b2c1c2d1d2d3d4d5d6d7d8u128);
    let uuid_key = Some(Literal::uuid(uuid_value));
    let data = Struct::from_iter([uuid_key]);
    let partition_key = PartitionKey::new(spec, table_schema.clone(), data);

    let mut data_file_writer = data_file_writer_builder
        .build(Some(partition_key))
        .await
        .unwrap();
    let col = FixedSizeBinaryArray::try_from_iter(std::iter::once(uuid_value.as_bytes().to_vec()))
        .unwrap();
    let batch =
        RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(col) as ArrayRef]).unwrap();
    data_file_writer.write(batch.clone()).await.unwrap();
    let data_file = data_file_writer.close().await.unwrap();

    let tx = Transaction::new(&table);
    let append_action = tx.fast_append().add_data_files(data_file.clone());
    let tx = append_action.apply(tx).unwrap();

    let table = tx
        .commit(&rest_catalog)
        .await
        .expect("The first commit should not fail.");

    // check results
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
}
