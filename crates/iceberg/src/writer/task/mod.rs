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
//! This module provides the [`TaskWriter`] trait and [`BaseTaskWriter`] implementation
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

/// A high-level writer implementation for writing data to Iceberg tables.
///
/// `BaseTaskWriter` handles both partitioned and non-partitioned tables by composing
/// a [`PartitioningWriter`] with an optional [`RecordBatchPartitionSplitter`].
///
/// # Type Parameters
///
/// * `W` - The underlying [`PartitioningWriter`] implementation
pub struct BaseTaskWriter<W: PartitioningWriter> {
    writer: W,
    partition_splitter: Option<RecordBatchPartitionSplitter>,
    schema: SchemaRef,
    partition_spec: PartitionSpecRef,
}

impl<W: PartitioningWriter> BaseTaskWriter<W> {
    /// Create a new BaseTaskWriter.
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
    /// Returns a new `BaseTaskWriter` instance, or an error if the partition splitter
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
impl<W: PartitioningWriter> TaskWriter for BaseTaskWriter<W> {
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
