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

//! This module provides the `FanoutWriter` implementation.

use std::marker::PhantomData;
use std::num::NonZeroUsize;

use async_trait::async_trait;
use hashlink::LinkedHashMap;

use crate::spec::{PartitionKey, Struct};
use crate::writer::partitioning::PartitioningWriter;
use crate::writer::{DefaultInput, DefaultOutput, IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// A writer that can write data to multiple partitions simultaneously.
///
/// Unlike `ClusteredWriter` which expects sorted input and maintains only one active writer,
/// `FanoutWriter` can handle unsorted data by maintaining multiple active writers in a map.
/// By default, all writers remain active until the writer is closed. Use
/// [`Self::new_with_max_open_partitions`] to limit the number of active writers.
/// When the limit is reached, the least recently used writer is closed before a new
/// one is opened. Writing to a closed partition opens a new writer for that partition.
/// This reduces memory used by active writers, but may produce more, smaller files.
/// Metadata for completed files is retained until this writer is closed.
///
/// # Type Parameters
///
/// * `B` - The inner writer builder type
/// * `I` - Input type (defaults to `RecordBatch`)
/// * `O` - Output collection type (defaults to `Vec<DataFile>`)
pub struct FanoutWriter<B, I = DefaultInput, O = DefaultOutput>
where
    B: IcebergWriterBuilder<I, O>,
    O: IntoIterator + FromIterator<<O as IntoIterator>::Item>,
    <O as IntoIterator>::Item: Clone,
{
    inner_builder: B,
    partition_writers: LinkedHashMap<Struct, B::R>,
    max_open_partitions: Option<NonZeroUsize>,
    output: Vec<<O as IntoIterator>::Item>,
    _phantom: PhantomData<I>,
}

impl<B, I, O> FanoutWriter<B, I, O>
where
    B: IcebergWriterBuilder<I, O>,
    I: Send + 'static,
    O: IntoIterator + FromIterator<<O as IntoIterator>::Item>,
    <O as IntoIterator>::Item: Send + Clone,
{
    /// Create a new `FanoutWriter` with no limit on the number of active partitions.
    pub fn new(inner_builder: B) -> Self {
        Self {
            inner_builder,
            partition_writers: LinkedHashMap::new(),
            max_open_partitions: None,
            output: Vec::new(),
            _phantom: PhantomData,
        }
    }

    /// Create a `FanoutWriter` with at most `max_open_partitions` active writers.
    ///
    /// Before opening a writer that would exceed the limit, the least recently used
    /// writer is closed and its output is retained. A later write to that partition
    /// opens a new writer, which may result in more, smaller files.
    pub fn new_with_max_open_partitions(
        inner_builder: B,
        max_open_partitions: NonZeroUsize,
    ) -> Self {
        Self {
            max_open_partitions: Some(max_open_partitions),
            ..Self::new(inner_builder)
        }
    }

    /// Get or create a writer for the specified partition.
    async fn get_or_create_writer(&mut self, partition_key: &PartitionKey) -> Result<&mut B::R> {
        if !self.partition_writers.contains_key(partition_key.data()) {
            if let Some(limit) = self.max_open_partitions
                && self.partition_writers.len() >= limit.get()
                && let Some((_, mut writer)) = self.partition_writers.pop_front()
            {
                self.output.extend(writer.close().await?);
            }

            let writer = self
                .inner_builder
                .build(Some(partition_key.clone()))
                .await?;
            self.partition_writers
                .insert(partition_key.data().clone(), writer);
        }

        self.partition_writers
            .to_back(partition_key.data())
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to get partition writer after creation",
                )
            })
    }
}

#[async_trait]
impl<B, I, O> PartitioningWriter<I, O> for FanoutWriter<B, I, O>
where
    B: IcebergWriterBuilder<I, O>,
    I: Send + 'static,
    O: IntoIterator + FromIterator<<O as IntoIterator>::Item> + Send + 'static,
    <O as IntoIterator>::Item: Send + Clone,
{
    async fn write(&mut self, partition_key: PartitionKey, input: I) -> Result<()> {
        let writer = self.get_or_create_writer(&partition_key).await?;
        writer.write(input).await
    }

    async fn close(mut self) -> Result<O> {
        // Close all partition writers
        for (_, mut writer) in self.partition_writers {
            self.output.extend(writer.close().await?);
        }

        // Collect all output items into the output collection type
        Ok(O::from_iter(self.output))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use super::*;
    use crate::io::FileIO;
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

    type TestOutput = Vec<(Struct, Vec<i32>)>;

    #[derive(Default)]
    struct WriterState {
        active: usize,
        peak_active: usize,
        built: usize,
        closed: Vec<Struct>,
        fail_build: bool,
        fail_close: bool,
    }

    #[derive(Clone, Default)]
    struct TestWriterBuilder(Arc<Mutex<WriterState>>);

    struct TestWriter {
        state: Arc<Mutex<WriterState>>,
        partition: Struct,
        rows: Vec<i32>,
    }

    #[async_trait]
    impl IcebergWriterBuilder<i32, TestOutput> for TestWriterBuilder {
        type R = TestWriter;

        async fn build(&self, partition_key: Option<PartitionKey>) -> Result<Self::R> {
            let mut state = self.0.lock().unwrap();
            if state.fail_build {
                return Err(Error::new(ErrorKind::Unexpected, "build failed"));
            }
            state.active += 1;
            state.peak_active = state.peak_active.max(state.active);
            state.built += 1;
            Ok(TestWriter {
                state: self.0.clone(),
                partition: partition_key.unwrap().data().clone(),
                rows: Vec::new(),
            })
        }
    }

    #[async_trait]
    impl IcebergWriter<i32, TestOutput> for TestWriter {
        async fn write(&mut self, input: i32) -> Result<()> {
            self.rows.push(input);
            Ok(())
        }

        async fn close(&mut self) -> Result<TestOutput> {
            let mut state = self.state.lock().unwrap();
            state.closed.push(self.partition.clone());
            if state.fail_close {
                return Err(Error::new(ErrorKind::Unexpected, "close failed"));
            }
            Ok(vec![(
                self.partition.clone(),
                std::mem::take(&mut self.rows),
            )])
        }
    }

    impl Drop for TestWriter {
        fn drop(&mut self) {
            self.state.lock().unwrap().active -= 1;
        }
    }

    fn test_partition(value: i32) -> PartitionKey {
        let schema = Arc::new(crate::spec::Schema::builder().build().unwrap());
        let spec = PartitionSpec::builder(schema.clone()).build().unwrap();
        PartitionKey::new(spec, schema, Struct::from_iter([Some(Literal::int(value))]))
    }

    #[tokio::test]
    async fn test_fanout_writer_limits_and_lru() -> Result<()> {
        for limit in [
            None,
            NonZeroUsize::new(1),
            NonZeroUsize::new(2),
            NonZeroUsize::new(3),
        ] {
            let builder = TestWriterBuilder::default();
            let mut writer = match limit {
                Some(limit) => FanoutWriter::new_with_max_open_partitions(builder.clone(), limit),
                None => FanoutWriter::new(builder.clone()),
            };
            for (row, partition) in [0, 1, 0, 2, 1, 0].into_iter().enumerate() {
                writer.write(test_partition(partition), row as i32).await?;
            }
            if limit == NonZeroUsize::new(2) {
                assert_eq!(builder.0.lock().unwrap().closed, vec![
                    test_partition(1).data().clone(),
                    test_partition(0).data().clone(),
                    test_partition(2).data().clone(),
                ]);
            }
            let output = writer.close().await?;
            let state = builder.0.lock().unwrap();
            assert_eq!(state.active, 0);
            assert_eq!(state.closed.len(), state.built);
            assert_eq!(output.len(), state.built);
            assert!(state.peak_active <= limit.map_or(3, NonZeroUsize::get));
            assert_eq!(state.built, match limit.map(NonZeroUsize::get) {
                Some(1) => 6,
                Some(2) => 5,
                _ => 3,
            });
            let mut rows_by_partition: HashMap<Struct, Vec<i32>> = HashMap::new();
            for (partition, rows) in output {
                rows_by_partition.entry(partition).or_default().extend(rows);
            }
            assert_eq!(
                rows_by_partition,
                HashMap::from([
                    (test_partition(0).data().clone(), vec![0, 2, 5]),
                    (test_partition(1).data().clone(), vec![1, 4]),
                    (test_partition(2).data().clone(), vec![3]),
                ])
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_fanout_writer_empty_with_limit() -> Result<()> {
        let builder = TestWriterBuilder::default();
        let writer = FanoutWriter::new_with_max_open_partitions(
            builder.clone(),
            NonZeroUsize::new(1).unwrap(),
        );
        assert!(writer.close().await?.is_empty());
        assert_eq!(builder.0.lock().unwrap().built, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_fanout_writer_eviction_close_failure() -> Result<()> {
        let builder = TestWriterBuilder::default();
        let mut writer = FanoutWriter::new_with_max_open_partitions(
            builder.clone(),
            NonZeroUsize::new(1).unwrap(),
        );
        writer.write(test_partition(0), 0).await?;
        builder.0.lock().unwrap().fail_close = true;
        let error = writer.write(test_partition(1), 1).await.unwrap_err();
        assert!(error.to_string().contains("close failed"));
        let state = builder.0.lock().unwrap();
        assert_eq!(state.built, 1);
        assert_eq!(state.active, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_fanout_writer_build_failure_after_eviction() -> Result<()> {
        let builder = TestWriterBuilder::default();
        let mut writer = FanoutWriter::new_with_max_open_partitions(
            builder.clone(),
            NonZeroUsize::new(1).unwrap(),
        );
        writer.write(test_partition(0), 0).await?;
        builder.0.lock().unwrap().fail_build = true;
        let error = writer.write(test_partition(1), 1).await.unwrap_err();
        assert!(error.to_string().contains("build failed"));
        assert_eq!(builder.0.lock().unwrap().active, 0);
        builder.0.lock().unwrap().fail_build = false;
        writer.write(test_partition(1), 1).await?;
        assert_eq!(writer.close().await?, vec![
            (test_partition(0).data().clone(), vec![0]),
            (test_partition(1).data().clone(), vec![1]),
        ]);
        Ok(())
    }

    #[tokio::test]
    async fn test_fanout_writer_single_partition() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_io = FileIO::new_with_fs();
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
        let mut writer = FanoutWriter::new(data_file_writer_builder);

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
        writer.write(partition_key.clone(), batch1).await?;
        writer.write(partition_key.clone(), batch2).await?;

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
        for limit in [None, NonZeroUsize::new(1), NonZeroUsize::new(2)] {
            check_fanout_writer_multiple_partitions(limit).await?;
        }
        Ok(())
    }

    async fn check_fanout_writer_multiple_partitions(
        max_open_partitions: Option<NonZeroUsize>,
    ) -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_io = FileIO::new_with_fs();
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
        let mut writer = match max_open_partitions {
            Some(limit) => {
                FanoutWriter::new_with_max_open_partitions(data_file_writer_builder, limit)
            }
            None => FanoutWriter::new(data_file_writer_builder),
        };

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
        writer.write(partition_key_us.clone(), batch_us1).await?;
        writer.write(partition_key_eu.clone(), batch_eu1).await?;
        writer.write(partition_key_us.clone(), batch_us2).await?; // Back to US partition
        writer
            .write(partition_key_asia.clone(), batch_asia1)
            .await?;

        // Close writer and get data files
        let data_files = writer.close().await?;

        let expected_files = if max_open_partitions == NonZeroUsize::new(1) {
            4
        } else {
            3
        };
        assert_eq!(data_files.len(), expected_files);
        let mut rows_by_partition = HashMap::new();
        for data_file in &data_files {
            *rows_by_partition
                .entry(data_file.partition.clone())
                .or_insert(0) += data_file.record_count;
        }
        assert_eq!(
            rows_by_partition,
            HashMap::from([
                (partition_value_us.clone(), 3),
                (partition_value_eu.clone(), 2),
                (partition_value_asia.clone(), 2),
            ])
        );

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
}
