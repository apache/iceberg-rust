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

//! Task writer module for high-level Iceberg table writing operations.
//!
//! This module provides the [`TaskWriter`] trait and [`DefaultTaskWriter`] implementation
//! for writing data to Iceberg tables with automatic partition handling.

use crate::Result;
use crate::arrow::RecordBatchPartitionSplitter;
use crate::spec::{PartitionKey, PartitionSpecRef, SchemaRef, Struct};
use crate::writer::partitioning::PartitioningWriter;
use crate::writer::{DefaultInput, DefaultOutput};

/// High-level async trait for writing tasks to Iceberg tables.
///
/// The `TaskWriter` trait provides a simplified interface for writing data and retrieving
/// results, abstracting away the complexity of partition handling and writer selection.
///
/// # Type Parameters
///
/// * `I` - Input type (defaults to `DefaultInput` which is `RecordBatch`)
/// * `O` - Output type (defaults to `DefaultOutput` which is `Vec<DataFile>`)
#[async_trait::async_trait]
pub trait TaskWriter<I = DefaultInput, O = DefaultOutput>: Send + 'static {
    /// Write input data to the task writer.
    ///
    /// # Arguments
    ///
    /// * `input` - The input data to write
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an error if the write operation fails.
    async fn write(&mut self, input: I) -> Result<()>;

    /// Close the writer and return the accumulated output.
    ///
    /// # Returns
    ///
    /// Returns the accumulated output (e.g., `Vec<DataFile>`) on success,
    /// or an error if the close operation fails.
    async fn close(self) -> Result<O>;
}

/// The default implementation of [`TaskWriter`] for writing data to Iceberg tables.
///
/// `DefaultTaskWriter` handles both partitioned and non-partitioned tables by composing
/// a [`PartitioningWriter`] with an optional [`RecordBatchPartitionSplitter`].
///
/// # Type Parameters
///
/// * `W` - The underlying [`PartitioningWriter`] implementation
pub struct DefaultTaskWriter<W: PartitioningWriter> {
    writer: W,
    partition_splitter: Option<RecordBatchPartitionSplitter>,
    schema: SchemaRef,
    partition_spec: PartitionSpecRef,
}

impl<W: PartitioningWriter> DefaultTaskWriter<W> {
    /// Create a new DefaultTaskWriter.
    ///
    /// # Parameters
    ///
    /// * `writer` - The underlying [`PartitioningWriter`] implementation
    /// * `partition_splitter` - Optional partition splitter for partitioned tables.
    ///   Should be `None` for unpartitioned tables and `Some` for partitioned tables.
    /// * `schema` - The Iceberg schema reference
    /// * `partition_spec` - The partition specification reference
    ///
    /// # Returns
    ///
    /// Returns a new `DefaultTaskWriter` instance, or an error if the partition splitter
    /// configuration is invalid (e.g., missing splitter for a partitioned table).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A partitioned table is provided without a partition splitter
    pub fn new(
        writer: W,
        partition_splitter: Option<RecordBatchPartitionSplitter>,
        schema: SchemaRef,
        partition_spec: PartitionSpecRef,
    ) -> Result<Self> {
        // Validate that partitioned tables have a splitter
        if !partition_spec.is_unpartitioned() && partition_splitter.is_none() {
            return Err(crate::Error::new(
                crate::ErrorKind::DataInvalid,
                "Partition splitter is required for partitioned tables",
            ));
        }

        Ok(Self {
            writer,
            partition_splitter,
            schema,
            partition_spec,
        })
    }
}

#[async_trait::async_trait]
impl<W: PartitioningWriter> TaskWriter for DefaultTaskWriter<W> {
    async fn write(&mut self, input: DefaultInput) -> Result<()> {
        if self.partition_spec.is_unpartitioned() {
            // Unpartitioned table: create empty PartitionKey and write directly
            let partition_key = PartitionKey::new(
                self.partition_spec.as_ref().clone(),
                self.schema.clone(),
                Struct::empty(),
            );
            self.writer.write(partition_key, input).await?;
        } else {
            // Partitioned table: must have a splitter
            let splitter = self.partition_splitter.as_ref().ok_or_else(|| {
                crate::Error::new(
                    crate::ErrorKind::DataInvalid,
                    "Partition splitter is required for partitioned tables",
                )
            })?;

            // Split batch and write each partition
            let partitioned_batches = splitter.split(&input)?;

            for (partition_key, batch) in partitioned_batches {
                self.writer.write(partition_key, batch).await?;
            }
        }

        Ok(())
    }

    async fn close(self) -> Result<DefaultOutput> {
        self.writer.close().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use super::*;
    use crate::arrow::{RecordBatchPartitionSplitter, schema_to_arrow_schema};
    use crate::io::FileIOBuilder;
    use crate::spec::{
        DataFileFormat, Literal, NestedField, PartitionSpec, PartitionSpecBuilder, PrimitiveType,
        Transform, Type,
    };
    use crate::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use crate::writer::partitioning::clustered_writer::ClusteredWriter;
    use crate::writer::partitioning::fanout_writer::FanoutWriter;
    use crate::writer::partitioning::unpartitioned_writer::UnpartitionedWriter;

    /// Helper function to create a test writer setup with common configuration
    fn create_test_writer_builder(
        temp_dir: &TempDir,
        schema: Arc<crate::spec::Schema>,
    ) -> Result<impl crate::writer::IcebergWriterBuilder> {
        let file_io = FileIOBuilder::new_fs_io().build()?;
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let parquet_writer_builder =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), schema);
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            file_io,
            location_gen,
            file_name_gen,
        );

        Ok(DataFileWriterBuilder::new(rolling_writer_builder))
    }

    /// Helper function to create a simple test schema
    fn create_simple_schema() -> Result<Arc<crate::spec::Schema>> {
        Ok(Arc::new(
            crate::spec::Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()?,
        ))
    }

    /// Helper function to create a schema with a region partition field
    fn create_schema_with_region() -> Result<Arc<crate::spec::Schema>> {
        Ok(Arc::new(
            crate::spec::Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(3, "region", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()?,
        ))
    }

    /// Helper function to create Arrow schema with field IDs for simple schema
    fn create_arrow_schema_simple() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                1.to_string(),
            )])),
            Field::new("name", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                2.to_string(),
            )])),
        ])
    }

    /// Helper function to create Arrow schema with field IDs including region
    fn create_arrow_schema_with_region() -> Schema {
        Schema::new(vec![
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
        ])
    }

    #[tokio::test]
    async fn test_default_task_writer_with_unpartitioned_table() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let schema = create_simple_schema()?;
        let data_file_writer_builder = create_test_writer_builder(&temp_dir, schema.clone())?;

        // Create unpartitioned spec
        let partition_spec = Arc::new(PartitionSpec::builder(schema.clone()).build()?);

        // Create UnpartitionedWriter
        let unpartitioned_writer = UnpartitionedWriter::new(data_file_writer_builder);

        // Create DefaultTaskWriter with None splitter (unpartitioned)
        let mut task_writer = DefaultTaskWriter::new(
            unpartitioned_writer,
            None, // No splitter for unpartitioned table
            schema.clone(),
            partition_spec.clone(),
        )?;

        // Create test data
        let arrow_schema = create_arrow_schema_simple();
        let batch1 = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
        ])?;

        let batch2 = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![3, 4])),
            Arc::new(StringArray::from(vec!["Charlie", "Dave"])),
        ])?;

        let batch3 = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![5, 6])),
            Arc::new(StringArray::from(vec!["Eve", "Frank"])),
        ])?;

        // Write multiple batches
        task_writer.write(batch1).await?;
        task_writer.write(batch2).await?;
        task_writer.write(batch3).await?;

        // Close and get data files
        let data_files = task_writer.close().await?;

        // Verify at least one file was created
        assert!(
            !data_files.is_empty(),
            "Expected at least one data file to be created"
        );

        // Verify all data files have empty partition (unpartitioned)
        for data_file in &data_files {
            assert_eq!(
                data_file.partition,
                Struct::empty(),
                "Expected empty partition for unpartitioned table"
            );
            assert_eq!(data_file.file_format, DataFileFormat::Parquet);
            assert!(
                data_file.record_count > 0,
                "Record count should be positive"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_default_task_writer_with_fanout_writer() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let schema = create_schema_with_region()?;
        let data_file_writer_builder = create_test_writer_builder(&temp_dir, schema.clone())?;

        // Create partitioned spec with id field using Identity transform
        let partition_spec = Arc::new(
            PartitionSpecBuilder::new(schema.clone())
                .with_spec_id(1)
                .add_partition_field("id", "id", Transform::Identity)?
                .build()?,
        );

        // Create FanoutWriter
        let fanout_writer = FanoutWriter::new(data_file_writer_builder);

        // Create partition splitter
        let arrow_schema = Arc::new(schema_to_arrow_schema(&schema)?);
        let partition_splitter = RecordBatchPartitionSplitter::new(
            arrow_schema.clone(),
            schema.clone(),
            partition_spec.clone(),
            false,
        )?;

        // Create DefaultTaskWriter with FanoutWriter and splitter
        let mut task_writer = DefaultTaskWriter::new(
            fanout_writer,
            Some(partition_splitter),
            schema.clone(),
            partition_spec.clone(),
        )?;

        // Create test data with mixed partition values (partitioned by id)
        let arrow_schema_with_region = create_arrow_schema_with_region();

        // Batch with mixed id values (1, 2, 1)
        let batch1 = RecordBatch::try_new(Arc::new(arrow_schema_with_region.clone()), vec![
            Arc::new(Int32Array::from(vec![1, 2, 1])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(StringArray::from(vec!["US", "EU", "US"])),
        ])?;

        // Batch with id value 3
        let batch2 = RecordBatch::try_new(Arc::new(arrow_schema_with_region.clone()), vec![
            Arc::new(Int32Array::from(vec![3, 3])),
            Arc::new(StringArray::from(vec!["Dave", "Eve"])),
            Arc::new(StringArray::from(vec!["ASIA", "ASIA"])),
        ])?;

        // Batch with mixed id values again (back to 2 and 1)
        let batch3 = RecordBatch::try_new(Arc::new(arrow_schema_with_region.clone()), vec![
            Arc::new(Int32Array::from(vec![2, 1])),
            Arc::new(StringArray::from(vec!["Frank", "Grace"])),
            Arc::new(StringArray::from(vec!["EU", "US"])),
        ])?;

        // Write batches with mixed partition values
        task_writer.write(batch1).await?;
        task_writer.write(batch2).await?;
        task_writer.write(batch3).await?;

        // Close and get data files
        let data_files = task_writer.close().await?;

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

        let partition_value_1 = Struct::from_iter([Some(Literal::int(1))]);
        let partition_value_2 = Struct::from_iter([Some(Literal::int(2))]);
        let partition_value_3 = Struct::from_iter([Some(Literal::int(3))]);

        assert!(
            partitions_found.contains(&partition_value_1),
            "Missing partition 1"
        );
        assert!(
            partitions_found.contains(&partition_value_2),
            "Missing partition 2"
        );
        assert!(
            partitions_found.contains(&partition_value_3),
            "Missing partition 3"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_default_task_writer_with_clustered_writer() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let schema = create_schema_with_region()?;
        let data_file_writer_builder = create_test_writer_builder(&temp_dir, schema.clone())?;

        // Create partitioned spec with id field using Identity transform
        let partition_spec = Arc::new(
            PartitionSpecBuilder::new(schema.clone())
                .with_spec_id(1)
                .add_partition_field("id", "id", Transform::Identity)?
                .build()?,
        );

        // Create ClusteredWriter
        let clustered_writer = ClusteredWriter::new(data_file_writer_builder);

        // Create partition splitter
        let arrow_schema = Arc::new(schema_to_arrow_schema(&schema)?);
        let partition_splitter = RecordBatchPartitionSplitter::new(
            arrow_schema.clone(),
            schema.clone(),
            partition_spec.clone(),
            false,
        )?;

        // Create DefaultTaskWriter with ClusteredWriter and splitter
        let mut task_writer = DefaultTaskWriter::new(
            clustered_writer,
            Some(partition_splitter),
            schema.clone(),
            partition_spec.clone(),
        )?;

        // Create test data with sorted partition values (id: 1, 2, 3)
        let arrow_schema_with_region = create_arrow_schema_with_region();

        // Batch with id=1
        let batch1 = RecordBatch::try_new(Arc::new(arrow_schema_with_region.clone()), vec![
            Arc::new(Int32Array::from(vec![1, 1])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            Arc::new(StringArray::from(vec!["US", "US"])),
        ])?;

        // Batch with id=2
        let batch2 = RecordBatch::try_new(Arc::new(arrow_schema_with_region.clone()), vec![
            Arc::new(Int32Array::from(vec![2, 2])),
            Arc::new(StringArray::from(vec!["Charlie", "Dave"])),
            Arc::new(StringArray::from(vec!["EU", "EU"])),
        ])?;

        // Batch with id=3
        let batch3 = RecordBatch::try_new(Arc::new(arrow_schema_with_region.clone()), vec![
            Arc::new(Int32Array::from(vec![3, 3])),
            Arc::new(StringArray::from(vec!["Eve", "Frank"])),
            Arc::new(StringArray::from(vec!["ASIA", "ASIA"])),
        ])?;

        // Write batches in sorted partition order
        task_writer.write(batch1).await?;
        task_writer.write(batch2).await?;
        task_writer.write(batch3).await?;

        // Close and get data files
        let data_files = task_writer.close().await?;

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

        let partition_value_1 = Struct::from_iter([Some(Literal::int(1))]);
        let partition_value_2 = Struct::from_iter([Some(Literal::int(2))]);
        let partition_value_3 = Struct::from_iter([Some(Literal::int(3))]);

        assert!(
            partitions_found.contains(&partition_value_1),
            "Missing partition 1"
        );
        assert!(
            partitions_found.contains(&partition_value_2),
            "Missing partition 2"
        );
        assert!(
            partitions_found.contains(&partition_value_3),
            "Missing partition 3"
        );

        Ok(())
    }
}
