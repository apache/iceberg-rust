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

//! Integration test for partition data file

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use iceberg::spec::{
    Literal, NestedField, PrimitiveLiteral, PrimitiveType, Schema, Struct, Transform, Type,
    UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::transaction::Transaction;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, Namespace, NamespaceIdent, TableCreation};
use iceberg_integration_tests::set_test_fixture;
use parquet::file::properties::WriterProperties;

#[tokio::test]
async fn test_append_partition_data_file() {
    let fixture = set_test_fixture("test_partition_data_file").await;

    let ns = Namespace::with_properties(
        NamespaceIdent::from_strs(["iceberg", "rust"]).unwrap(),
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

    let unbound_partition_spec = UnboundPartitionSpec::builder()
        .add_partition_field(2, "id", Transform::Identity)
        .expect("could not add partition field")
        .build();

    let partition_spec = unbound_partition_spec
        .bind(schema.clone())
        .expect("could not bind to schema");

    let table_creation = TableCreation::builder()
        .name("t1".to_string())
        .schema(schema.clone())
        .partition_spec(partition_spec)
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

    let first_partition_id_value = 100;

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

    let mut data_file_writer_valid = DataFileWriterBuilder::new(
        parquet_writer_builder.clone(),
        Some(Struct::from_iter([Some(Literal::Primitive(
            PrimitiveLiteral::Int(first_partition_id_value),
        ))])),
    )
    .build()
    .await
    .unwrap();

    let col1 = StringArray::from(vec![Some("foo1"), Some("foo2")]);
    let col2 = Int32Array::from(vec![
        Some(first_partition_id_value),
        Some(first_partition_id_value),
    ]);
    let col3 = BooleanArray::from(vec![Some(true), Some(false)]);
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(col1) as ArrayRef,
        Arc::new(col2) as ArrayRef,
        Arc::new(col3) as ArrayRef,
    ])
    .unwrap();

    data_file_writer_valid.write(batch.clone()).await.unwrap();
    let data_file_valid = data_file_writer_valid.close().await.unwrap();

    // commit result
    let tx = Transaction::new(&table);
    let mut append_action = tx.fast_append(None, vec![]).unwrap();
    append_action
        .add_data_files(data_file_valid.clone())
        .unwrap();
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

    test_schema_incompatible_partition_type(
        parquet_writer_builder.clone(),
        batch.clone(),
        table.clone(),
    )
    .await;

    test_schema_incompatible_partition_fields(
        parquet_writer_builder,
        batch,
        table,
        first_partition_id_value,
    )
    .await;
}

async fn test_schema_incompatible_partition_type(
    parquet_writer_builder: ParquetWriterBuilder<
        DefaultLocationGenerator,
        DefaultFileNameGenerator,
    >,
    batch: RecordBatch,
    table: Table,
) {
    // test writing different "type" of partition than mentioned in schema
    let mut data_file_writer_invalid = DataFileWriterBuilder::new(
        parquet_writer_builder.clone(),
        Some(Struct::from_iter([Some(Literal::Primitive(
            PrimitiveLiteral::Boolean(true),
        ))])),
    )
    .build()
    .await
    .unwrap();

    data_file_writer_invalid.write(batch.clone()).await.unwrap();
    let data_file_invalid = data_file_writer_invalid.close().await.unwrap();

    let tx = Transaction::new(&table);
    let mut append_action = tx.fast_append(None, vec![]).unwrap();
    if append_action
        .add_data_files(data_file_invalid.clone())
        .is_ok()
    {
        panic!("diverging partition info should have returned error");
    }
}

async fn test_schema_incompatible_partition_fields(
    parquet_writer_builder: ParquetWriterBuilder<
        DefaultLocationGenerator,
        DefaultFileNameGenerator,
    >,
    batch: RecordBatch,
    table: Table,
    first_partition_id_value: i32,
) {
    // test writing different number of partition fields than mentioned in schema

    let mut data_file_writer_invalid = DataFileWriterBuilder::new(
        parquet_writer_builder,
        Some(Struct::from_iter([
            Some(Literal::Primitive(PrimitiveLiteral::Int(
                first_partition_id_value,
            ))),
            Some(Literal::Primitive(PrimitiveLiteral::Int(
                first_partition_id_value,
            ))),
        ])),
    )
    .build()
    .await
    .unwrap();

    data_file_writer_invalid.write(batch.clone()).await.unwrap();
    let data_file_invalid = data_file_writer_invalid.close().await.unwrap();

    let tx = Transaction::new(&table);
    let mut append_action = tx.fast_append(None, vec![]).unwrap();
    if append_action
        .add_data_files(data_file_invalid.clone())
        .is_ok()
    {
        panic!("passing different number of partition fields should have returned error");
    }
}
