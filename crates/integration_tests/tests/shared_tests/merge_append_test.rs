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

use std::sync::{Arc, OnceLock};

use arrow_array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use iceberg::spec::{
    DataFile, ManifestEntry, ManifestStatus, NestedField, PrimitiveType, Schema, Type,
};
use iceberg::table::Table;
use iceberg::transaction::{
    ApplyTransactionAction, MANIFEST_MERGE_ENABLED, MANIFEST_MIN_MERGE_COUNT,
    MANIFEST_TARGET_SIZE_BYTES, Transaction,
};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, TableCreation};
use iceberg_catalog_rest::RestCatalog;
use parquet::file::properties::WriterProperties;

use crate::get_shared_containers;
use crate::shared_tests::random_ns;

static FILE_NAME_GENERATOR: OnceLock<DefaultFileNameGenerator> = OnceLock::new();

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
    let file_name_generator = FILE_NAME_GENERATOR.get_or_init(|| {
        DefaultFileNameGenerator::new(
            "test".to_string(),
            None,
            iceberg::spec::DataFileFormat::Parquet,
        )
    });
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(
        parquet_writer_builder,
        None,
        table.metadata().default_partition_spec_id(),
    );
    let mut data_file_writer = data_file_writer_builder.build().await.unwrap();
    let col1 = StringArray::from(vec![Some("foo"); 100]);
    let col2 = Int32Array::from(vec![Some(1); 100]);
    let col3 = BooleanArray::from(vec![Some(true); 100]);
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
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalog::new(fixture.catalog_config.clone());
    let ns = random_ns().await;

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
    let mut table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    // fast append data file 3 time to create 3 manifest
    for _ in 0..3 {
        let data_file = write_new_data_file(&table).await;
        let tx = Transaction::new(&table);
        let append_action = tx.fast_append().add_data_files(data_file.clone());
        let tx = append_action.apply(tx).unwrap();
        table = tx.commit(&rest_catalog).await.unwrap()
    }
    let manifest_list = table
        .metadata()
        .current_snapshot()
        .unwrap()
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert_eq!(manifest_list.entries().len(), 3);
    
    // Set the merge size to make sure per two manifest will be packed together.
    let manifest_file_len = manifest_list.entries().iter().map(|entry| entry.manifest_length).max().unwrap();
    let tx = Transaction::new(&table);
    let update_properties_action = tx
        .update_table_properties()
        .set(MANIFEST_MERGE_ENABLED.to_string(), "true".to_string())
        .set(MANIFEST_MIN_MERGE_COUNT.to_string(), "4".to_string())
        .set(MANIFEST_TARGET_SIZE_BYTES.to_string(), (manifest_file_len * 2 + 2).to_string());
    let tx = update_properties_action.apply(tx).unwrap();
    table = tx.commit(&rest_catalog).await.unwrap();

    // Test case:
    // There are 3 manifset, and we append one manifest with merge append. 
    // Target size is 2 * manifest_file_len + 1, so each two manifest will be packed together.
    // The first bin contains the first manifest and the first additional manifest, but it's count is less than min merge count, so it will not be merged.
    // Other two manifests will be merged into one manifest.
    // Expect three manifest in the new snapshot:
    // 1. The new manifest with new added data file.
    // 2. Original manifest with one original data file.
    // 3. The merged manifest with two original data files.
    let mut original_manifest_entries = vec![];
    for (idx, entry) in manifest_list.entries().iter().enumerate() {
        let manifest = entry.load_manifest(table.file_io()).await.unwrap();
        assert!(manifest.entries().len() == 1);
        if idx == 0 {
            original_manifest_entries.push(Arc::new(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .snapshot_id(manifest.entries()[0].snapshot_id().unwrap())
                    .sequence_number(manifest.entries()[0].sequence_number().unwrap())
                    .file_sequence_number(manifest.entries()[0].file_sequence_number().unwrap())
                    .data_file(manifest.entries()[0].data_file().clone())
                    .build(),
            ));
        } else {
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
    }

    // append data file with merge append, 4 data file will be merged to two manifest
    let data_file = write_new_data_file(&table).await;
    let tx = Transaction::new(&table);
    let merge_append_action = tx.merge_append().unwrap().add_data_files(data_file.clone());
    let tx = merge_append_action.apply(tx).unwrap();
    table = tx.commit(&rest_catalog).await.unwrap();
    // Check manifest file
    let manifest_list = table
        .metadata()
        .current_snapshot()
        .unwrap()
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert_eq!(manifest_list.entries().len(), 3);
    {
        let manifest = manifest_list.entries()[1]
            .load_manifest(table.file_io())
            .await
            .unwrap();
        assert!(manifest.entries().len() == 1);
        original_manifest_entries.retain(|entry| !manifest.entries().contains(entry));
    }
    {
        let manifest = manifest_list.entries()[2]
            .load_manifest(table.file_io())
            .await
            .unwrap();
        assert!(manifest.entries().len() == 2);
        for original_entry in original_manifest_entries.iter() {
            assert!(manifest.entries().contains(original_entry));
        }
    }
}
