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

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray, Float32Array,
    Float64Array, Int32Array, Int64Array, LargeBinaryArray, RecordBatch, StringArray,
    Time64MicrosecondArray, TimestampMicrosecondArray, TimestampNanosecondArray,
};
use common::random_ns;
use futures::TryStreamExt;
use iceberg::spec::{NestedField, PrimitiveType, Schema, TableProperties, Type};
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
async fn test_writing_to_a_table_with_all_primitive_types() {
    let fixture = get_test_fixture();
    let rest_catalog = fixture.rest_catalog().await;

    let schema = Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            NestedField::required(1, "boolean", Type::Primitive(PrimitiveType::Boolean)).into(),
            NestedField::required(2, "int", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(3, "long", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(4, "float", Type::Primitive(PrimitiveType::Float)).into(),
            NestedField::required(5, "double", Type::Primitive(PrimitiveType::Double)).into(),
            NestedField::required(
                6,
                "decimal",
                Type::Primitive(PrimitiveType::Decimal {
                    precision: 38,
                    scale: 10,
                }),
            )
            .into(),
            NestedField::required(7, "date", Type::Primitive(PrimitiveType::Date)).into(),
            NestedField::required(8, "time", Type::Primitive(PrimitiveType::Time)).into(),
            NestedField::required(9, "timestamp", Type::Primitive(PrimitiveType::Timestamp)).into(),
            NestedField::required(
                10,
                "timestamptz",
                Type::Primitive(PrimitiveType::Timestamptz),
            )
            .into(),
            NestedField::required(
                11,
                "timestamp_ns",
                Type::Primitive(PrimitiveType::TimestampNs),
            )
            .into(),
            NestedField::required(
                12,
                "timestamptz_ns",
                Type::Primitive(PrimitiveType::TimestamptzNs),
            )
            .into(),
            NestedField::required(13, "string", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(14, "uuid", Type::Primitive(PrimitiveType::Uuid)).into(),
            NestedField::required(15, "fixed", Type::Primitive(PrimitiveType::Fixed(16))).into(),
            NestedField::required(16, "binary", Type::Primitive(PrimitiveType::Binary)).into(),
        ])
        .build()
        .unwrap();

    let table_creation = TableCreation::builder()
        .name("t1".to_string())
        .schema(schema.clone())
        // for timestamptz_ns support
        .properties(HashMap::<String, String>::from_iter([(
            TableProperties::PROPERTY_FORMAT_VERSION.to_string(),
            "3".to_string(),
        )]))
        .build();

    let ns = random_ns().await;
    let table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    // Create the writer and write the data
    let schema = Arc::<arrow_schema::Schema>::new(
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
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
        Arc::new(Int32Array::from(vec![42])) as ArrayRef,
        Arc::new(Int64Array::from(vec![42i64])) as ArrayRef,
        Arc::new(Float32Array::from(vec![1.5f32])) as ArrayRef,
        Arc::new(Float64Array::from(vec![2.5f64])) as ArrayRef,
        Arc::new(
            Decimal128Array::from(vec![12345678901234567890i128])
                .with_precision_and_scale(38, 10)
                .unwrap(),
        ) as ArrayRef,
        Arc::new(Date32Array::from(vec![19000])) as ArrayRef,
        Arc::new(Time64MicrosecondArray::from(vec![123456789i64])) as ArrayRef,
        Arc::new(TimestampMicrosecondArray::from(vec![
            1_600_000_000_000_000i64,
        ])) as ArrayRef,
        Arc::new(
            TimestampMicrosecondArray::from(vec![1_600_000_000_000_000i64]).with_timezone("+00:00"),
        ) as ArrayRef,
        Arc::new(TimestampNanosecondArray::from(vec![
            1_600_000_000_000_000_000i64,
        ])) as ArrayRef,
        Arc::new(
            TimestampNanosecondArray::from(vec![1_600_000_000_000_000_000i64])
                .with_timezone("+00:00"),
        ) as ArrayRef,
        Arc::new(StringArray::from(vec!["🦊"])) as ArrayRef,
        Arc::new(
            FixedSizeBinaryArray::try_from_iter(std::iter::once(
                Uuid::from_u128(0xa1a2a3a4b1b2c1c2d1d2d3d4d5d6d7d8u128)
                    .as_bytes()
                    .to_vec(),
            ))
            .unwrap(),
        ) as ArrayRef,
        Arc::new(FixedSizeBinaryArray::try_from_iter(std::iter::once(vec![0u8; 16])).unwrap())
            as ArrayRef,
        Arc::new(LargeBinaryArray::from_iter_values(std::iter::once(
            b"binary".as_slice(),
        ))) as ArrayRef,
    ])
    .unwrap();
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
