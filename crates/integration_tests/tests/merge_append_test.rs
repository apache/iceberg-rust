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
use iceberg::spec::{
    DataFile, ManifestEntry, ManifestStatus, NestedField, PrimitiveType, Schema, Type,
};
use iceberg::table::Table;
use iceberg::transaction::{Transaction, MANIFEST_MERGE_ENABLED, MANIFEST_MIN_MERGE_COUNT};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, Namespace, NamespaceIdent, TableCreation};
use iceberg_integration_tests::set_test_fixture;
use parquet::file::properties::WriterProperties;

async fn write_new_data_file(table: &Table) -> Vec<DataFile> {
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
    data_file_writer.close().await.unwrap()
}

#[tokio::test]
async fn test_append_data_file() {
    let fixture = set_test_fixture("test_create_table").await;

    // Create table
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
    let mut table = fixture
        .rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    // Enable merge append for table
    let tx = Transaction::new(&table);
    table = tx
        .set_properties(HashMap::from([
            (MANIFEST_MERGE_ENABLED.to_string(), "true".to_string()),
            (MANIFEST_MIN_MERGE_COUNT.to_string(), "4".to_string()),
        ]))
        .unwrap()
        .commit(&fixture.rest_catalog)
        .await
        .unwrap();

    // fast append data file 3 time to create 3 manifest
    let mut original_manifest_entries = vec![];
    for _ in 0..3 {
        let data_file = write_new_data_file(&table).await;
        let tx = Transaction::new(&table);
        let mut append_action = tx.fast_append(None, vec![]).unwrap();
        append_action.add_data_files(data_file.clone()).unwrap();
        let tx = append_action.apply().await.unwrap();
        table = tx.commit(&fixture.rest_catalog).await.unwrap()
    }
    let manifest_list = table
        .metadata()
        .current_snapshot()
        .unwrap()
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert_eq!(manifest_list.entries().len(), 3);
    for entry in manifest_list.entries() {
        let manifest = entry.load_manifest(table.file_io()).await.unwrap();
        assert!(manifest.entries().len() == 1);

        original_manifest_entries.push(Arc::new(
            ManifestEntry::builder()
                .status(ManifestStatus::Existing)
                .snapshot_id(manifest.entries()[0].snapshot_id().unwrap())
                .sequence_number(manifest.entries()[0].sequence_number().unwrap())
                .file_sequence_number(manifest.entries()[0].file_sequence_number().unwrap())
                .data_file(manifest.entries()[0].data_file().clone())
                .build(),
        ));
    }

    // append data file with merge append, 4 data file will be merged to one manifest
    let data_file = write_new_data_file(&table).await;
    let tx = Transaction::new(&table);
    let mut merge_append_action = tx.merge_append(None, vec![]).unwrap();
    merge_append_action
        .add_data_files(data_file.clone())
        .unwrap();
    let tx = merge_append_action.apply().await.unwrap();
    table = tx.commit(&fixture.rest_catalog).await.unwrap();
    let manifest_list = table
        .metadata()
        .current_snapshot()
        .unwrap()
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert_eq!(manifest_list.entries().len(), 1);
    let manifest = manifest_list.entries()[0]
        .load_manifest(table.file_io())
        .await
        .unwrap();
    assert!(manifest.entries().len() == 4);
    for original_entry in original_manifest_entries.iter() {
        assert!(manifest.entries().contains(original_entry));
    }
}
