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

use arrow_array::{
    ArrayRef, BooleanArray, FixedSizeBinaryArray, Int32Array, RecordBatch, StringArray,
};
use common::{random_ns, test_schema};
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
use iceberg::{Catalog, CatalogBuilder, TableCreation};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_integration_tests::get_test_fixture;
use iceberg_storage_opendal::OpenDalStorageFactory;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

#[tokio::test]
async fn it_should_write_a_table_with_uuid_fields() {
    let fixture = get_test_fixture();
    let rest_catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let schema = Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            NestedField::required(1, "uuid", Type::Primitive(PrimitiveType::Uuid)).into(),
        ])
        .build()
        .unwrap();

    let table_creation = TableCreation::builder()
        .name("t1".to_string())
        .schema(schema.clone())
        .build();

    let ns = random_ns().await;
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
    let mut data_file_writer = data_file_writer_builder.build(None).await.unwrap();
    let uuid_value = Uuid::from_u128(0x01234567_0000_0000_0000_000000000000);
    let col = FixedSizeBinaryArray::try_from_iter(std::iter::once(uuid_value.as_bytes().to_vec()))
        .unwrap();
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(col) as ArrayRef]).unwrap();
    data_file_writer.write(batch.clone()).await.unwrap();
    let data_file = data_file_writer.close().await.unwrap();

    // start two transaction and commit one of them
    let tx1 = Transaction::new(&table);
    let append_action = tx1.fast_append().add_data_files(data_file.clone());
    let tx1 = append_action.apply(tx1).unwrap();

    let table = tx1
        .commit(&rest_catalog)
        .await
        .expect("The first commit should not fail.");

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
}

#[tokio::test]
async fn it_should_write_a_table_with_uuid_fields_and_uuid_partition() {
    let fixture = get_test_fixture();
    let rest_catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

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
    let uuid_value = Uuid::from_u128(0x01234567_0000_0000_0000_000000000000);

    let table_metadata = table.metadata();
    let spec = table_metadata.default_partition_spec().as_ref().clone();
    let table_schema = table_metadata.current_schema().clone();

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

    // start two transaction and commit one of them
    let tx1 = Transaction::new(&table);
    let append_action = tx1.fast_append().add_data_files(data_file.clone());
    let tx1 = append_action.apply(tx1).unwrap();

    let table = tx1
        .commit(&rest_catalog)
        .await
        .expect("The first commit should not fail.");

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
}
