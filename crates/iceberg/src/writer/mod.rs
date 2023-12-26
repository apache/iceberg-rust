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

//! Iceberg writer module.
//!
//! The writer API is designed to be extensible and flexible. Each writer is decoupled and can be create and config independently. User can:
//! 1.Combine different writer builder to build a writer which have complex write logic. Such as FanoutPartition + DataFileWrite or FanoutPartition + PosititionDeleteFileWrite.
//! 2.Customize the writer and combine it with original writer builder to build a writer which
//! can process the data in a specific way.  
//!
//! There are two kinds of writer and related builder:
//! 1. `IcebergWriter` and `IcebergWriterBuilder`, they are focus on the data process logical.
//!     If you want to support a new data process logical, you need to implement a new `IcebergWriter` and `IcebergWriterBuilder`.
//! 2. `FileWriter` and `FileWriterBuilder`, they are focus on the physical file write.
//!     If you want to support a new physical file format, you need to implement a new `FileWriter` and `FileWriterBuilder`.
//!
//! The create process of iceberg writer is:
//! 1. Create a `FileWriterBuilder`.
//!     1a. Combine it with other `FileWriterBuilder` to get a new `FileWriterBuilder`.
//! 2. Use FileWriterBuilder to create a `IcebergWriterBuilder`.
//!     2a. Combine it with other `IcebergWriterBuilder` to get a new `IcebergWriterBuilder`.
//! 3. Use `build` function in `IcebergWriterBuilder` to create a `IcebergWriter`.
//!
//! # Simple Case 1: Create a data file writer using parquet file format.
//! # TODO(Implement this example)
//! ```
//! // 1. Create a parquet file writer builder.
//! let parquet_writer_builder = ParquetFileWriterBuilder::new(parquet_file_writer_config);
//! // 2. Create a data file writer builder.
//! let DataFileWriterBuilder = DataFileWriterBuilder::new(parquet_writer_builder,data_file_writer_config);
//! // 3. Create a iceberg writer.
//! let iceberg_writer = DataFileWriterBuilder.build(schema).await?;
//!
//! iceberg_writer.write(input).await?;
//!
//! let write_result = iceberg_writer.flush().await?;
//! ```
//!
//! # Complex Case 2: Create a fanout partition data file writer using parquet file format.
//! # TODO (Implement this example)
//! ```
//! // 1. Create a parquet file writer builder.
//! let parquet_writer_builder = ParquetFileWriterBuilder::new(parquet_file_writer_config);
//! // 2. Create a data file writer builder.
//! let DataFileWriterBuilder = DataFileWriterBuilder::new(parquet_writer_builder,data_file_writer_config);
//! // 3. Create a fanout partition writer builder.
//! let fanout_partition_writer_builder = FanoutPartitionWriterBuilder::new(DataFileWriterBuilder, partition_config);
//! // 4. Create a iceberg writer.
//! let iceberg_writer = fanout_partition_writer_builder.build(schema).await?;
//!
//! iceberg_writer.write(input).await?;
//!
//! let write_result = iceberg_writer.flush().await?;
//! ```

use crate::{
    spec::{DataContentType, Struct},
    Result,
};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

pub mod file_writer;

type DefaultInput = RecordBatch;

/// The builder for iceberg writer.
#[async_trait::async_trait]
pub trait IcebergWriterBuilder<I = DefaultInput>: Send + Clone + 'static {
    /// The associated writer type.
    type R: IcebergWriter<I>;
    /// Build the iceberg writer.
    async fn build(self, schema: &SchemaRef) -> Result<Self::R>;
}

/// The iceberg writer used to write data to iceberg table.
#[async_trait::async_trait]
pub trait IcebergWriter<I = DefaultInput>: Send + 'static {
    /// The associated write result type.
    type R: IcebergWriteResult;
    /// Write data to iceberg table.
    async fn write(&mut self, input: I) -> Result<()>;
    /// Flush the writer and return the write result.
    async fn flush(&mut self) -> Result<Vec<Self::R>>;
}

/// The write result of iceberg writer.
pub trait IcebergWriteResult: Send + Sync + 'static {
    /// Set the content type of the write result.
    fn set_content(&mut self, content: DataContentType) -> &mut Self;
    /// Set the equality ids of the write result.
    fn set_equality_ids(&mut self, equality_ids: Vec<i32>) -> &mut Self;
    /// Set the partition of the write result.
    fn set_partition(&mut self, partition_value: Struct) -> &mut Self;
}

/// The current file status of iceberg writer. It implement for the writer which write a single
/// file.
pub trait CurrentFileStatus {
    /// Get the current file path.
    fn current_file_path(&self) -> String;
    /// Get the current file row number.
    fn current_row_num(&self) -> usize;
    /// Get the current file written size.
    fn current_written_size(&self) -> usize;
}
