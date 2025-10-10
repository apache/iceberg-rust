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

//! This module provides the `FanoutDataWriter` implementation.

use std::collections::HashMap;

use arrow_array::RecordBatch;
use async_trait::async_trait;

use crate::spec::{DataFile, PartitionKey, Struct};
use crate::writer::partitioning::PartitioningWriter;
use crate::writer::{IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// A writer that can write data to multiple partitions simultaneously.
///
/// Unlike `ClusteredDataWriter` which expects sorted input and maintains only one active writer,
/// `FanoutDataWriter` can handle unsorted data by maintaining multiple active writers in a map.
/// This allows writing to any partition at any time, but uses more memory as all writers
/// remain active until the writer is closed.
#[derive(Clone)]
pub struct FanoutDataWriter<B: IcebergWriterBuilder> {
    inner_builder: B,
    partition_writers: HashMap<Struct, B::R>,
    unpartitioned_writer: Option<B::R>,
    output: Vec<DataFile>,
}

impl<B: IcebergWriterBuilder> FanoutDataWriter<B> {
    /// Create a new `FanoutDataWriter`.
    pub fn new(inner_builder: B) -> Self {
        Self {
            inner_builder,
            partition_writers: HashMap::new(),
            unpartitioned_writer: None,
            output: Vec::new(),
        }
    }

    /// Get or create a writer for the specified partition.
    async fn get_or_create_partition_writer(
        &mut self,
        partition_key: &PartitionKey,
    ) -> Result<&mut B::R> {
        if !self.partition_writers.contains_key(partition_key.data()) {
            let writer = self
                .inner_builder
                .clone()
                .build_with_partition(Some(partition_key.clone()))
                .await?;
            self.partition_writers
                .insert(partition_key.data().clone(), writer);
        }

        self.partition_writers
            .get_mut(partition_key.data())
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to get partition writer after creation",
                )
            })
    }

    /// Get or create the unpartitioned writer.
    async fn get_or_create_unpartitioned_writer(&mut self) -> Result<&mut B::R> {
        if self.unpartitioned_writer.is_none() {
            self.unpartitioned_writer = Some(
                self.inner_builder
                    .clone()
                    .build_with_partition(None)
                    .await?,
            );
        }

        self.unpartitioned_writer.as_mut().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "Failed to get unpartitioned writer after creation",
            )
        })
    }
}

#[async_trait]
impl<B: IcebergWriterBuilder> PartitioningWriter for FanoutDataWriter<B> {
    async fn write(
        &mut self,
        partition_key: Option<PartitionKey>,
        input: RecordBatch,
    ) -> Result<()> {
        if let Some(ref partition_key) = partition_key {
            let writer = self.get_or_create_partition_writer(&partition_key).await?;
            writer.write(input).await
        } else {
            let writer = self.get_or_create_unpartitioned_writer().await?;
            writer.write(input).await
        }
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        // Close all partition writers
        for (_, mut writer) in std::mem::take(&mut self.partition_writers) {
            self.output.extend(writer.close().await?);
        }

        // Close unpartitioned writer if it exists
        if let Some(mut writer) = self.unpartitioned_writer.take() {
            self.output.extend(writer.close().await?);
        }

        // Return all collected data files
        Ok(std::mem::take(&mut self.output))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::{
        DataFileFormat, Literal, NestedField, PartitionKey, PartitionSpec, PrimitiveType, Struct,
        Type,
    };
    use crate::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;

    #[tokio::test]
    async fn test_fanout_writer_unpartitioned() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_io = FileIOBuilder::new_fs_io().build()?;
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // Create schema
        let schema = Arc::new(
            crate::spec::Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()?,
        );

        // Create writer builder
        let parquet_writer_builder =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());

        // Create rolling file writer builder
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );

        // Create data file writer builder
        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

        // Create fanout writer
        let mut writer = FanoutDataWriter::new(data_file_writer_builder);

        // Create test data with proper field ID metadata
        let arrow_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                1.to_string(),
            )])),
            Field::new("name", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                2.to_string(),
            )])),
        ]);

        let batch1 = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ])?;

        let batch2 = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![4, 5])),
            Arc::new(StringArray::from(vec!["Dave", "Eve"])),
        ])?;

        // Write data without partitioning (pass None for partition_key)
        writer.write(None, batch1).await?;
        writer.write(None, batch2).await?;

        // Close writer and get data files
        let data_files = writer.close().await?;

        // Verify at least one file was created
        assert!(
            !data_files.is_empty(),
            "Expected at least one data file to be created"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_fanout_writer_multiple_writes() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_io = FileIOBuilder::new_fs_io().build()?;
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // Create schema
        let schema = Arc::new(
            crate::spec::Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()?,
        );

        // Create writer builder
        let parquet_writer_builder =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());

        // Create rolling file writer builder
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );

        // Create data file writer builder
        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

        // Create fanout writer
        let mut writer = FanoutDataWriter::new(data_file_writer_builder);

        // Create test data with proper field ID metadata
        let arrow_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                1.to_string(),
            )])),
            Field::new("name", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                2.to_string(),
            )])),
        ]);

        let batch1 = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
        ])?;

        let batch2 = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![3, 4])),
            Arc::new(StringArray::from(vec!["Charlie", "Dave"])),
        ])?;

        let batch3 = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![5])),
            Arc::new(StringArray::from(vec!["Eve"])),
        ])?;

        // Write multiple batches to demonstrate fanout capability
        // (all unpartitioned for simplicity)
        writer.write(None, batch1).await?;
        writer.write(None, batch2).await?;
        writer.write(None, batch3).await?;

        // Close writer and get data files
        let data_files = writer.close().await?;

        // Verify at least one file was created
        assert!(
            !data_files.is_empty(),
            "Expected at least one data file to be created"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_fanout_writer_single_partition() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_io = FileIOBuilder::new_fs_io().build()?;
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // Create schema with partition field
        let schema = Arc::new(
            crate::spec::Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(3, "region", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()?,
        );

        // Create partition spec - using the same pattern as data_file_writer tests
        let partition_spec = PartitionSpec::builder(schema.clone()).build()?;
        let partition_value = Struct::from_iter([Some(Literal::string("US"))]);
        let partition_key =
            PartitionKey::new(partition_spec, schema.clone(), partition_value.clone());

        // Create writer builder
        let parquet_writer_builder =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());

        // Create rolling file writer builder
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );

        // Create data file writer builder
        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

        // Create fanout writer
        let mut writer = FanoutDataWriter::new(data_file_writer_builder);

        // Create test data with proper field ID metadata
        let arrow_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                1.to_string(),
            )])),
            Field::new("name", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                2.to_string(),
            )])),
            Field::new("region", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                3.to_string(),
            )])),
        ]);

        let batch1 = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            Arc::new(StringArray::from(vec!["US", "US"])),
        ])?;

        let batch2 = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![3, 4])),
            Arc::new(StringArray::from(vec!["Charlie", "Dave"])),
            Arc::new(StringArray::from(vec!["US", "US"])),
        ])?;

        // Write data to the same partition
        writer.write(Some(partition_key.clone()), batch1).await?;
        writer.write(Some(partition_key.clone()), batch2).await?;

        // Close writer and get data files
        let data_files = writer.close().await?;

        // Verify at least one file was created
        assert!(
            !data_files.is_empty(),
            "Expected at least one data file to be created"
        );

        // Verify that all data files have the correct partition value
        for data_file in &data_files {
            assert_eq!(data_file.partition, partition_value);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_fanout_writer_multiple_partitions() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_io = FileIOBuilder::new_fs_io().build()?;
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // Create schema with partition field
        let schema = Arc::new(
            crate::spec::Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(3, "region", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()?,
        );

        // Create partition spec
        let partition_spec = PartitionSpec::builder(schema.clone()).build()?;

        // Create partition keys for different regions
        let partition_value_us = Struct::from_iter([Some(Literal::string("US"))]);
        let partition_key_us = PartitionKey::new(
            partition_spec.clone(),
            schema.clone(),
            partition_value_us.clone(),
        );

        let partition_value_eu = Struct::from_iter([Some(Literal::string("EU"))]);
        let partition_key_eu = PartitionKey::new(
            partition_spec.clone(),
            schema.clone(),
            partition_value_eu.clone(),
        );

        let partition_value_asia = Struct::from_iter([Some(Literal::string("ASIA"))]);
        let partition_key_asia = PartitionKey::new(
            partition_spec.clone(),
            schema.clone(),
            partition_value_asia.clone(),
        );

        // Create writer builder
        let parquet_writer_builder =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());

        // Create rolling file writer builder
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );

        // Create data file writer builder
        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

        // Create fanout writer
        let mut writer = FanoutDataWriter::new(data_file_writer_builder);

        // Create test data with proper field ID metadata
        let arrow_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                1.to_string(),
            )])),
            Field::new("name", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                2.to_string(),
            )])),
            Field::new("region", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                3.to_string(),
            )])),
        ]);

        // Create batches for different partitions
        let batch_us1 = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            Arc::new(StringArray::from(vec!["US", "US"])),
        ])?;

        let batch_eu1 = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![3, 4])),
            Arc::new(StringArray::from(vec!["Charlie", "Dave"])),
            Arc::new(StringArray::from(vec!["EU", "EU"])),
        ])?;

        let batch_us2 = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![5])),
            Arc::new(StringArray::from(vec!["Eve"])),
            Arc::new(StringArray::from(vec!["US"])),
        ])?;

        let batch_asia1 = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![6, 7])),
            Arc::new(StringArray::from(vec!["Frank", "Grace"])),
            Arc::new(StringArray::from(vec!["ASIA", "ASIA"])),
        ])?;

        // Write data in mixed partition order to demonstrate fanout capability
        // This is the key difference from ClusteredWriter - we can write to any partition at any time
        writer
            .write(Some(partition_key_us.clone()), batch_us1)
            .await?;
        writer
            .write(Some(partition_key_eu.clone()), batch_eu1)
            .await?;
        writer
            .write(Some(partition_key_us.clone()), batch_us2)
            .await?; // Back to US partition
        writer
            .write(Some(partition_key_asia.clone()), batch_asia1)
            .await?;

        // Close writer and get data files
        let data_files = writer.close().await?;

        // Verify files were created for all partitions
        assert!(
            data_files.len() >= 3,
            "Expected at least 3 data files (one per partition), got {}",
            data_files.len()
        );

        // Verify that we have files for each partition
        let mut partitions_found = std::collections::HashSet::new();
        for data_file in &data_files {
            partitions_found.insert(data_file.partition.clone());
        }

        assert!(
            partitions_found.contains(&partition_value_us),
            "Missing US partition"
        );
        assert!(
            partitions_found.contains(&partition_value_eu),
            "Missing EU partition"
        );
        assert!(
            partitions_found.contains(&partition_value_asia),
            "Missing ASIA partition"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_fanout_writer_mixed_partitioned_unpartitioned() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_io = FileIOBuilder::new_fs_io().build()?;
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // Create schema
        let schema = Arc::new(
            crate::spec::Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(3, "region", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()?,
        );

        // Create partition spec and key
        let partition_spec = PartitionSpec::builder(schema.clone()).build()?;
        let partition_value_us = Struct::from_iter([Some(Literal::string("US"))]);
        let partition_key_us =
            PartitionKey::new(partition_spec, schema.clone(), partition_value_us.clone());

        // Create writer builder
        let parquet_writer_builder =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());

        // Create rolling file writer builder
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );

        // Create data file writer builder
        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

        // Create fanout writer
        let mut writer = FanoutDataWriter::new(data_file_writer_builder);

        // Create test data with proper field ID metadata
        let arrow_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                1.to_string(),
            )])),
            Field::new("name", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                2.to_string(),
            )])),
            Field::new("region", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                3.to_string(),
            )])),
        ]);

        // Create batches
        let batch_partitioned = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            Arc::new(StringArray::from(vec!["US", "US"])),
        ])?;

        let batch_unpartitioned = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![3, 4])),
            Arc::new(StringArray::from(vec!["Charlie", "Dave"])),
            Arc::new(StringArray::from(vec!["UNKNOWN", "UNKNOWN"])),
        ])?;

        // Write both partitioned and unpartitioned data
        writer
            .write(Some(partition_key_us), batch_partitioned)
            .await?;
        writer.write(None, batch_unpartitioned).await?;

        // Close writer and get data files
        let data_files = writer.close().await?;

        // Verify files were created for both partitioned and unpartitioned data
        assert!(
            data_files.len() >= 2,
            "Expected at least 2 data files (partitioned + unpartitioned), got {}",
            data_files.len()
        );

        // Verify we have both partitioned and unpartitioned files
        let mut has_partitioned = false;
        let mut has_unpartitioned = false;

        for data_file in &data_files {
            if data_file.partition == partition_value_us {
                has_partitioned = true;
            } else if data_file.partition == Struct::empty() {
                has_unpartitioned = true;
            }
        }

        assert!(has_partitioned, "Missing partitioned data file");
        assert!(has_unpartitioned, "Missing unpartitioned data file");

        Ok(())
    }
}
