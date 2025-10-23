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

//! TaskWriter for DataFusion integration.
//!
//! This module provides a high-level writer that handles partitioning and routing
//! of RecordBatch data to Iceberg tables.

use datafusion::arrow::array::RecordBatch;
use iceberg::Result;
use iceberg::arrow::RecordBatchPartitionSplitter;
use iceberg::spec::{DataFile, PartitionSpecRef, SchemaRef};
use iceberg::writer::IcebergWriterBuilder;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::partitioning::clustered_writer::ClusteredWriter;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::writer::partitioning::unpartitioned_writer::UnpartitionedWriter;

/// High-level writer for DataFusion that handles partitioning and routing of RecordBatch data.
///
/// TaskWriter coordinates writing data to Iceberg tables by:
/// - Selecting the appropriate partitioning strategy (unpartitioned, fanout, or clustered)
/// - Lazily initializing the partition splitter on first write
/// - Routing data to the underlying writer
/// - Collecting all written data files
///
/// # Type Parameters
///
/// * `B` - The IcebergWriterBuilder type used to create underlying writers
///
/// # Example
///
/// ```rust,ignore
/// use iceberg::spec::{PartitionSpec, Schema};
/// use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
/// use iceberg_datafusion::writer::task::TaskWriter;
///
/// // Create a TaskWriter for an unpartitioned table
/// let task_writer = TaskWriter::new(
///     data_file_writer_builder,
///     false, // fanout_enabled
///     schema,
///     partition_spec,
/// );
///
/// // Write data
/// task_writer.write(record_batch).await?;
///
/// // Close and get data files
/// let data_files = task_writer.close().await?;
/// ```
#[allow(dead_code)]
pub struct TaskWriter<B: IcebergWriterBuilder> {
    /// The underlying writer (UnpartitionedWriter, FanoutWriter, or ClusteredWriter)
    writer: SupportedWriter<B>,
    /// Lazily initialized partition splitter for partitioned tables
    partition_splitter: Option<RecordBatchPartitionSplitter>,
    /// Iceberg schema reference
    schema: SchemaRef,
    /// Partition specification reference
    partition_spec: PartitionSpecRef,
}

/// Internal enum to hold the different writer types.
///
/// This enum allows TaskWriter to work with different partitioning strategies
/// while maintaining a unified interface.
#[allow(dead_code)]
enum SupportedWriter<B: IcebergWriterBuilder> {
    /// Writer for unpartitioned tables
    Unpartitioned(UnpartitionedWriter<B>),
    /// Writer for partitioned tables with unsorted data (maintains multiple active writers)
    Fanout(FanoutWriter<B>),
    /// Writer for partitioned tables with sorted data (maintains single active writer)
    Clustered(ClusteredWriter<B>),
}

impl<B: IcebergWriterBuilder> TaskWriter<B> {
    /// Create a new TaskWriter.
    ///
    /// # Parameters
    ///
    /// * `writer_builder` - The IcebergWriterBuilder to use for creating underlying writers
    /// * `fanout_enabled` - If true, use FanoutWriter for partitioned tables; otherwise use ClusteredWriter
    /// * `schema` - The Iceberg schema reference
    /// * `partition_spec` - The partition specification reference
    ///
    /// # Returns
    ///
    /// Returns a new TaskWriter instance.
    ///
    /// # Writer Selection Logic
    ///
    /// - If partition_spec is unpartitioned: creates UnpartitionedWriter
    /// - If partition_spec is partitioned AND fanout_enabled is true: creates FanoutWriter
    /// - If partition_spec is partitioned AND fanout_enabled is false: creates ClusteredWriter
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use iceberg::spec::{PartitionSpec, Schema};
    /// use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    /// use iceberg_datafusion::writer::task::TaskWriter;
    ///
    /// // Create a TaskWriter for an unpartitioned table
    /// let task_writer = TaskWriter::new(
    ///     data_file_writer_builder,
    ///     false, // fanout_enabled
    ///     schema,
    ///     partition_spec,
    /// );
    /// ```
    pub fn new(
        writer_builder: B,
        fanout_enabled: bool,
        schema: SchemaRef,
        partition_spec: PartitionSpecRef,
    ) -> Self {
        let writer = if partition_spec.is_unpartitioned() {
            SupportedWriter::Unpartitioned(UnpartitionedWriter::new(writer_builder))
        } else if fanout_enabled {
            SupportedWriter::Fanout(FanoutWriter::new(writer_builder))
        } else {
            SupportedWriter::Clustered(ClusteredWriter::new(writer_builder))
        };

        Self {
            writer,
            partition_splitter: None,
            schema,
            partition_spec,
        }
    }

    /// Write a RecordBatch to the TaskWriter.
    ///
    /// For the first write to a partitioned table, this method initializes the partition splitter.
    /// For unpartitioned tables, data is written directly without splitting.
    ///
    /// # Parameters
    ///
    /// * `batch` - The RecordBatch to write
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an error if the write fails.
    ///
    /// # Errors
    ///
    /// This method will return an error if:
    /// - Partition splitter initialization fails
    /// - Splitting the batch by partition fails
    /// - Writing to the underlying writer fails
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use arrow_array::RecordBatch;
    /// use iceberg_datafusion::writer::task::TaskWriter;
    ///
    /// // Write a RecordBatch
    /// task_writer.write(record_batch).await?;
    /// ```
    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        match &mut self.writer {
            SupportedWriter::Unpartitioned(writer) => {
                // Unpartitioned: write directly without splitting
                writer.write(batch).await
            }
            SupportedWriter::Fanout(writer) => {
                // Partitioned with fanout: initialize splitter on first write if needed
                if self.partition_splitter.is_none() {
                    let arrow_schema = batch.schema();
                    self.partition_splitter = Some(RecordBatchPartitionSplitter::new(
                        arrow_schema,
                        self.schema.clone(),
                        self.partition_spec.clone(),
                        true, // use_projected_partition_value
                    )?);
                }

                // Split batch by partition
                let splitter = self
                    .partition_splitter
                    .as_ref()
                    .expect("Partition splitter should be initialized");
                let partitioned_batches = splitter.split(&batch)?;

                // Write each partition
                for (partition_key, partition_batch) in partitioned_batches {
                    writer.write(partition_key, partition_batch).await?;
                }

                Ok(())
            }
            SupportedWriter::Clustered(writer) => {
                // Partitioned with clustered: initialize splitter on first write if needed
                if self.partition_splitter.is_none() {
                    let arrow_schema = batch.schema();
                    self.partition_splitter = Some(RecordBatchPartitionSplitter::new(
                        arrow_schema,
                        self.schema.clone(),
                        self.partition_spec.clone(),
                        true, // use_projected_partition_value
                    )?);
                }

                // Split batch by partition
                let splitter = self
                    .partition_splitter
                    .as_ref()
                    .expect("Partition splitter should be initialized");
                let partitioned_batches = splitter.split(&batch)?;

                // Write each partition
                for (partition_key, partition_batch) in partitioned_batches {
                    writer.write(partition_key, partition_batch).await?;
                }

                Ok(())
            }
        }
    }

    /// Close the TaskWriter and return all written data files.
    ///
    /// This method consumes the TaskWriter to prevent further use.
    ///
    /// # Returns
    ///
    /// Returns a `Vec<DataFile>` containing all written files, or an error if closing fails.
    ///
    /// # Errors
    ///
    /// This method will return an error if:
    /// - Closing the underlying writer fails
    /// - Any I/O operation fails during the close process
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use iceberg_datafusion::writer::task::TaskWriter;
    ///
    /// // Close the writer and get all data files
    /// let data_files = task_writer.close().await?;
    /// ```
    pub async fn close(self) -> Result<Vec<DataFile>> {
        match self.writer {
            SupportedWriter::Unpartitioned(writer) => writer.close().await,
            SupportedWriter::Fanout(writer) => writer.close().await,
            SupportedWriter::Clustered(writer) => writer.close().await,
        }
    }
}
