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

//! Integration test for unsigned integer type write/read roundtrip.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch, UInt8Array, UInt16Array, UInt32Array};
use arrow_schema::{DataType, Field};
use futures::TryStreamExt;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, TableCreation};
use iceberg_catalog_rest::RestCatalogBuilder;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::properties::WriterProperties;

use crate::get_shared_containers;
use crate::shared_tests::random_ns;

#[tokio::test]
async fn test_unsigned_type_write_read_roundtrip() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;

    // Create Iceberg schema with safe casting types (uint8/16→int32, uint32→int64)
    let schema = Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            NestedField::required(1, "uint8_col", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "uint16_col", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(3, "uint32_col", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .unwrap();

    let table_creation = TableCreation::builder()
        .name("unsigned_test".to_string())
        .schema(schema.clone())
        .build();

    let table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    // Create Arrow data with unsigned values including edge cases
    let uint8_data = UInt8Array::from(vec![255u8, 128u8, 0u8]); // Max, mid, min
    let uint16_data = UInt16Array::from(vec![65535u16, 32768u16, 0u16]); // Max, mid, min  
    let uint32_data = UInt32Array::from(vec![4_294_967_295u32, 2_147_483_648u32, 0u32]); // Max, > i32::MAX, min

    // Create Arrow schema with unsigned types and field IDs
    let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![
        Field::new("uint8_col", DataType::UInt8, false).with_metadata(
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string())])
        ),
        Field::new("uint16_col", DataType::UInt16, false).with_metadata(
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string())])
        ),
        Field::new("uint32_col", DataType::UInt32, false).with_metadata(
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "3".to_string())])
        ),
    ]));

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(uint8_data) as ArrayRef,
            Arc::new(uint16_data) as ArrayRef,
            Arc::new(uint32_data) as ArrayRef,
        ],
    ).unwrap();

    // Write the data
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        "unsigned_test".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let mut data_file_writer = DataFileWriterBuilder::new(parquet_writer_builder, None, 0)
        .build()
        .await
        .unwrap();

    data_file_writer.write(batch).await.unwrap();
    let data_file = data_file_writer.close().await.unwrap();

    // Commit the data
    let tx = Transaction::new(&table);
    let append_action = tx.fast_append().add_data_files(data_file);
    let tx = append_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Read back and verify data integrity
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
    let read_batch = &batches[0];

    // Verify schema conversion: unsigned → signed types
    assert_eq!(read_batch.column(0).data_type(), &DataType::Int32); // uint8 → int32
    assert_eq!(read_batch.column(1).data_type(), &DataType::Int32); // uint16 → int32
    assert_eq!(read_batch.column(2).data_type(), &DataType::Int64); // uint32 → int64

    // Verify data integrity: values preserved correctly
    let read_uint8 = read_batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    let read_uint16 = read_batch.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
    let read_uint32 = read_batch.column(2).as_any().downcast_ref::<Int64Array>().unwrap();

    // uint8 values preserved in int32
    assert_eq!(read_uint8.value(0), 255i32);
    assert_eq!(read_uint8.value(1), 128i32);
    assert_eq!(read_uint8.value(2), 0i32);

    // uint16 values preserved in int32
    assert_eq!(read_uint16.value(0), 65535i32);
    assert_eq!(read_uint16.value(1), 32768i32);
    assert_eq!(read_uint16.value(2), 0i32);

    // uint32 values preserved in int64 (including values > i32::MAX)
    assert_eq!(read_uint32.value(0), 4_294_967_295i64);
    assert_eq!(read_uint32.value(1), 2_147_483_648i64); // This would overflow in int32
    assert_eq!(read_uint32.value(2), 0i64);
}