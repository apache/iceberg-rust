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

use std::collections::HashMap;
use std::marker::PhantomData;
use std::num::NonZeroUsize;

use async_trait::async_trait;
use tracing::warn;

use crate::spec::{PartitionKey, Struct, TableProperties};
use crate::writer::partitioning::PartitioningWriter;
use crate::writer::{DefaultInput, DefaultOutput, IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// A writer for unsorted data across multiple partitions.
///
/// By default, the number of active writers is unlimited. A configured limit
/// closes the least recently used partition when necessary. Interleaved input
/// exceeding the limit can produce many small files; sort by partition or raise
/// the limit to reduce this overhead. Completed file metadata stays in memory.
/// The limit bounds writer count, not memory usage in bytes.
///
/// If an eviction close fails or is cancelled, subsequent writes and final close
/// fail. Discard this writer and rewrite all input before committing any files.
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
    partition_writers: HashMap<Struct, PartitionWriter<B::R>>,
    sequence: u64,
    evicting_partition: Option<Struct>,
    warned_about_eviction: bool,
    max_open_partitions: Option<NonZeroUsize>,
    output: Vec<<O as IntoIterator>::Item>,
    _phantom: PhantomData<I>,
}

struct PartitionWriter<W> {
    writer: W,
    last_used: u64,
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
            partition_writers: HashMap::new(),
            sequence: 0,
            evicting_partition: None,
            warned_about_eviction: false,
            max_open_partitions: None,
            output: Vec::new(),
            _phantom: PhantomData,
        }
    }

    /// Create a `FanoutWriter` with at most `max_open_partitions` active writers.
    pub fn new_with_max_open_partitions(
        inner_builder: B,
        max_open_partitions: NonZeroUsize,
    ) -> Self {
        Self {
            max_open_partitions: Some(max_open_partitions),
            ..Self::new(inner_builder)
        }
    }

    /// Create a `FanoutWriter` using the table's configured partition limit.
    ///
    /// # Errors
    ///
    /// Returns an error if `write.datafusion.fanout.max-open-partitions` is not a
    /// positive integer.
    pub fn new_with_table_properties(
        inner_builder: B,
        properties: &TableProperties<'_>,
    ) -> Result<Self> {
        Ok(Self {
            max_open_partitions: properties.write_datafusion_fanout_max_open_partitions()?,
            ..Self::new(inner_builder)
        })
    }

    fn ensure_healthy(&self) -> Result<()> {
        if let Some(partition) = &self.evicting_partition {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Fanout writer aborted during eviction; discard the writer and rewrite all input",
            )
            .with_context("evicted_partition", format!("{partition:?}")));
        }
        Ok(())
    }

    async fn get_or_create_writer(
        &mut self,
        partition_key: &PartitionKey,
    ) -> Result<&mut PartitionWriter<B::R>> {
        if !self.partition_writers.contains_key(partition_key.data()) {
            if let Some(limit) = self.max_open_partitions
                && self.partition_writers.len() >= limit.get()
            {
                let oldest = self
                    .partition_writers
                    .iter()
                    .min_by_key(|(_, writer)| writer.last_used)
                    .map(|(key, _)| key.clone())
                    .expect("a nonzero limit requires an active writer before eviction");
                if !self.warned_about_eviction {
                    warn!(
                        max_open_partitions = limit.get(),
                        "Fanout writer partition limit reached; interleaved input may produce \
                         small files and accumulate file metadata. Consider sorting by partition \
                         or increasing the limit"
                    );
                    self.warned_about_eviction = true;
                }
                // close() may consume inner state even if its future fails or is dropped.
                self.evicting_partition = Some(oldest.clone());
                let output = self
                    .partition_writers
                    .get_mut(&oldest)
                    .unwrap()
                    .writer
                    .close()
                    .await
                    .map_err(|error| {
                        Error::new(
                            error.kind(),
                            "Failed to close evicted partition; fanout writer aborted",
                        )
                        .with_context("evicted_partition", format!("{oldest:?}"))
                        .with_source(error)
                    })?;
                self.output.extend(output);
                self.partition_writers.remove(&oldest);
                self.evicting_partition = None;
            }

            let writer = self
                .inner_builder
                .build(Some(partition_key.clone()))
                .await?;
            self.partition_writers
                .insert(partition_key.data().clone(), PartitionWriter {
                    writer,
                    last_used: 0,
                });
        }

        Ok(self
            .partition_writers
            .get_mut(partition_key.data())
            .unwrap())
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
        self.ensure_healthy()?;
        let sequence = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Fanout writer sequence exhausted"))?;
        let writer = self.get_or_create_writer(&partition_key).await?;
        writer.writer.write(input).await?;
        writer.last_used = sequence;
        self.sequence = sequence;
        Ok(())
    }

    async fn close(mut self) -> Result<O> {
        self.ensure_healthy()?;
        // Close all partition writers
        for (_, mut writer) in self.partition_writers {
            self.output.extend(writer.writer.close().await?);
        }

        // Collect all output items into the output collection type
        Ok(O::from_iter(self.output))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    use arrow_array::{Float64Array, Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use super::*;
    use crate::arrow::schema_to_arrow_schema;
    use crate::io::FileIO;
    use crate::spec::{
        DataFileFormat, Literal, NestedField, PartitionKey, PartitionSpec, PrimitiveType, Struct,
        Transform, Type,
    };
    use crate::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator, FileNameGenerator,
    };
    use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;

    type TestOutput = Vec<(Struct, Vec<i32>)>;

    #[derive(Debug, Default, PartialEq, Eq)]
    struct WriterState {
        active: usize,
        peak_active: usize,
        built: usize,
        closed: Vec<Struct>,
        fail_build: bool,
        pending_build: bool,
        fail_close: bool,
        pending_close: bool,
        fail_write: bool,
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
            let pending_build = self.0.lock().unwrap().pending_build;
            if pending_build {
                std::future::pending::<()>().await;
            }
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
            if self.state.lock().unwrap().fail_write {
                return Err(Error::new(ErrorKind::Unexpected, "write failed"));
            }
            self.rows.push(input);
            Ok(())
        }

        async fn close(&mut self) -> Result<TestOutput> {
            let pending_close = {
                let mut state = self.state.lock().unwrap();
                state.closed.push(self.partition.clone());
                if state.fail_close {
                    return Err(Error::new(ErrorKind::Unexpected, "close failed"));
                }
                state.pending_close
            };
            let rows = std::mem::take(&mut self.rows);
            if pending_close {
                std::future::pending::<()>().await;
            }
            Ok(vec![(self.partition.clone(), rows)])
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

    fn limit_properties(limit: Option<NonZeroUsize>) -> HashMap<String, String> {
        limit
            .map(|limit| {
                (
                    TableProperties::PROPERTY_DATAFUSION_WRITE_FANOUT_MAX_OPEN_PARTITIONS
                        .to_string(),
                    limit.to_string(),
                )
            })
            .into_iter()
            .collect()
    }

    #[tokio::test]
    async fn test_fanout_writer_limits_and_lru() -> Result<()> {
        for (limit, peak_active, built, evicted) in [
            (None, 3, 3, vec![]),
            (NonZeroUsize::new(1), 1, 6, vec![0, 1, 0, 2, 1]),
            (NonZeroUsize::new(2), 2, 5, vec![1, 0, 2]),
            (NonZeroUsize::new(3), 3, 3, vec![]),
        ] {
            let builder = TestWriterBuilder::default();
            let properties = limit_properties(limit);
            let mut writer = FanoutWriter::new_with_table_properties(
                builder.clone(),
                &TableProperties::new(&properties),
            )?;
            for (row, partition) in [0, 1, 0, 2, 1, 0].into_iter().enumerate() {
                writer.write(test_partition(partition), row as i32).await?;
            }
            let expected = WriterState {
                active: peak_active,
                peak_active,
                built,
                closed: evicted
                    .into_iter()
                    .map(|p| test_partition(p).data().clone())
                    .collect(),
                ..Default::default()
            };
            assert_eq!(*builder.0.lock().unwrap(), expected);
            let output = writer.close().await?;
            assert_eq!((builder.0.lock().unwrap().active, output.len()), (0, built));
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

    fn assert_aborted<T>(result: Result<T>) {
        assert_eq!(
            result.err().unwrap().message(),
            "Fanout writer aborted during eviction; discard the writer and rewrite all input"
        );
    }

    #[tokio::test]
    async fn test_fanout_writer_eviction_close_failure_aborts() -> Result<()> {
        let builder = TestWriterBuilder::default();
        let mut writer = FanoutWriter::new_with_max_open_partitions(
            builder.clone(),
            NonZeroUsize::new(1).unwrap(),
        );
        writer.write(test_partition(0), 0).await?;
        writer.write(test_partition(1), 1).await?;
        builder.0.lock().unwrap().fail_close = true;
        let error = writer.write(test_partition(2), 2).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Failed to close evicted partition")
        );
        assert!(error.to_string().contains("close failed"));
        assert!(
            error
                .to_string()
                .contains(&format!("{:?}", test_partition(1).data()))
        );
        builder.0.lock().unwrap().fail_close = false;
        assert_aborted(writer.write(test_partition(2), 2).await);
        assert_aborted(writer.write(test_partition(1), 3).await);
        assert_aborted(writer.close().await);
        let expected = WriterState {
            active: 0,
            peak_active: 1,
            built: 2,
            closed: vec![
                test_partition(0).data().clone(),
                test_partition(1).data().clone(),
            ],
            ..Default::default()
        };
        assert_eq!(*builder.0.lock().unwrap(), expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_fanout_writer_cancelled_eviction_aborts() -> Result<()> {
        let builder = TestWriterBuilder::default();
        let mut writer = FanoutWriter::new_with_max_open_partitions(
            builder.clone(),
            NonZeroUsize::new(1).unwrap(),
        );
        writer.write(test_partition(0), 0).await?;
        builder.0.lock().unwrap().pending_close = true;
        let mut write = Box::pin(writer.write(test_partition(1), 1));
        assert!(futures::poll!(&mut write).is_pending());
        drop(write);
        builder.0.lock().unwrap().pending_close = false;
        assert_aborted(writer.write(test_partition(1), 1).await);
        assert_aborted(writer.close().await);
        let expected = WriterState {
            active: 0,
            peak_active: 1,
            built: 1,
            closed: vec![test_partition(0).data().clone()],
            ..Default::default()
        };
        assert_eq!(*builder.0.lock().unwrap(), expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_fanout_writer_cancelled_build_preserves_evicted_output() -> Result<()> {
        let builder = TestWriterBuilder::default();
        let mut writer = FanoutWriter::new_with_max_open_partitions(
            builder.clone(),
            NonZeroUsize::new(1).unwrap(),
        );
        writer.write(test_partition(0), 0).await?;
        builder.0.lock().unwrap().pending_build = true;
        let mut write = Box::pin(writer.write(test_partition(1), 1));
        assert!(futures::poll!(&mut write).is_pending());
        drop(write);
        builder.0.lock().unwrap().pending_build = false;
        writer.write(test_partition(1), 1).await?;
        assert_eq!(writer.close().await?, vec![
            (test_partition(0).data().clone(), vec![0]),
            (test_partition(1).data().clone(), vec![1]),
        ]);
        Ok(())
    }

    #[tokio::test]
    async fn test_fanout_writer_failed_write_preserves_recency() -> Result<()> {
        for (failed_partition, built, evicted) in [(0, 3, vec![0]), (2, 4, vec![0, 2])] {
            let builder = TestWriterBuilder::default();
            let mut writer = FanoutWriter::new_with_max_open_partitions(
                builder.clone(),
                NonZeroUsize::new(2).unwrap(),
            );
            writer.write(test_partition(0), 0).await?;
            writer.write(test_partition(1), 1).await?;
            builder.0.lock().unwrap().fail_write = true;
            assert!(
                writer
                    .write(test_partition(failed_partition), 2)
                    .await
                    .is_err()
            );
            builder.0.lock().unwrap().fail_write = false;
            writer.write(test_partition(3), 3).await?;
            let expected = WriterState {
                active: 2,
                peak_active: 2,
                built,
                closed: evicted
                    .into_iter()
                    .map(|p| test_partition(p).data().clone())
                    .collect(),
                ..Default::default()
            };
            assert_eq!(*builder.0.lock().unwrap(), expected);
            let output = writer.close().await?;
            let rows: HashMap<_, _> = output
                .into_iter()
                .filter(|(_, rows)| !rows.is_empty())
                .collect();
            assert_eq!(
                rows,
                HashMap::from([
                    (test_partition(0).data().clone(), vec![0]),
                    (test_partition(1).data().clone(), vec![1]),
                    (test_partition(3).data().clone(), vec![3]),
                ])
            );
        }
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

    #[derive(Debug, Default)]
    struct PerInstanceFileNameGenerator(AtomicU64);

    impl Clone for PerInstanceFileNameGenerator {
        fn clone(&self) -> Self {
            Self(AtomicU64::new(self.0.load(Ordering::Relaxed)))
        }
    }

    impl FileNameGenerator for PerInstanceFileNameGenerator {
        fn generate_file_name(&self) -> String {
            format!("part-{:05}.parquet", self.0.fetch_add(1, Ordering::Relaxed))
        }
    }

    #[tokio::test]
    async fn test_fanout_writer_multiple_partitions() -> Result<()> {
        for (limit, expected_files) in [
            (None, 3),
            (NonZeroUsize::new(1), 4),
            (NonZeroUsize::new(2), 3),
        ] {
            check_fanout_writer_multiple_partitions(limit, expected_files).await?;
        }
        Ok(())
    }

    async fn check_fanout_writer_multiple_partitions(
        max_open_partitions: Option<NonZeroUsize>,
        expected_files: usize,
    ) -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen = PerInstanceFileNameGenerator::default();

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
        let properties = limit_properties(max_open_partitions);
        let mut writer = FanoutWriter::new_with_table_properties(
            data_file_writer_builder,
            &TableProperties::new(&properties),
        )?;

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

        let paths: std::collections::HashSet<_> =
            data_files.iter().map(|file| &file.file_path).collect();
        assert_eq!(paths.len(), expected_files);
        let mut actual_ids = Vec::new();
        for file in &data_files {
            let contents = file_io.new_input(&file.file_path)?.read().await?;
            for batch in ParquetRecordBatchReaderBuilder::try_new(contents)?.build()? {
                let batch = batch?;
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                actual_ids.extend(ids.values().iter().copied());
            }
        }
        actual_ids.sort_unstable();
        assert_eq!(actual_ids, vec![1, 2, 3, 4, 5, 6, 7]);

        Ok(())
    }

    #[tokio::test]
    async fn test_fanout_writer_signed_zero_partitions() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let schema = Arc::new(
            crate::spec::Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "d", Type::Primitive(PrimitiveType::Double)).into(),
                ])
                .build()?,
        );
        let partition_spec = PartitionSpec::builder(schema.clone())
            .add_partition_field("d", "d", Transform::Identity)?
            .build()?;

        let partition_value_neg_zero = Struct::from_iter([Some(Literal::double(-0.0))]);
        let partition_key_neg_zero = PartitionKey::new(
            partition_spec.clone(),
            schema.clone(),
            partition_value_neg_zero.clone(),
        );

        let partition_value_pos_zero = Struct::from_iter([Some(Literal::double(0.0))]);
        let partition_key_pos_zero = PartitionKey::new(
            partition_spec.clone(),
            schema.clone(),
            partition_value_pos_zero.clone(),
        );

        let parquet_writer_builder =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );
        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

        let mut writer = FanoutWriter::new(data_file_writer_builder);

        let arrow_schema = Arc::new(schema_to_arrow_schema(&schema)?);
        let batch_neg_zero = RecordBatch::try_new(arrow_schema.clone(), vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Float64Array::from(vec![-0.0])),
        ])?;
        let batch_pos_zero = RecordBatch::try_new(arrow_schema.clone(), vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(Float64Array::from(vec![0.0])),
        ])?;

        writer.write(partition_key_neg_zero, batch_neg_zero).await?;
        writer.write(partition_key_pos_zero, batch_pos_zero).await?;

        let data_files = writer.close().await?;

        // -0.0 and 0.0 are different partition values, as in iceberg-java, so each row
        // goes to its own data file.
        assert_eq!(data_files.len(), 2);
        for data_file in &data_files {
            assert_eq!(data_file.record_count, 1);
        }
        let partitions_found: std::collections::HashSet<Struct> = data_files
            .iter()
            .map(|data_file| data_file.partition.clone())
            .collect();
        assert_eq!(
            partitions_found,
            std::collections::HashSet::from([partition_value_neg_zero, partition_value_pos_zero])
        );

        Ok(())
    }
}
