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
//! This module contains the generic writer trait and specific writer implementation. We categorize the writer into two types:
//! 1. FileWriter: writer for physical file format (Such as parquet, orc).
//! 2. IcebergWriter: writer for logical format provided by iceberg table (Such as data file, equality delete file, position delete file)
//!    or other function (Such as partition writer, delta writer).
//!
//! The IcebergWriter will use the inner FileWriter to write physical files.
//!
//! The writer interface is designed to be extensible and flexible. Writers can be independently configured
//! and composed to support complex write logic. E.g. By combining `FanoutPartitionWriter`, `DataFileWriter`, and `ParquetWriter`,
//! you can build a writer that automatically partitions the data and writes it in the Parquet format.
//!
//! For this purpose, there are four trait corresponding to these writer:
//! - IcebergWriterBuilder
//! - IcebergWriter
//! - FileWriterBuilder
//! - FileWriter
//!
//! Users can create specific writer builders, combine them, and build the final writer.
//! They can also define custom writers by implementing the `Writer` trait,
//! allowing seamless integration with existing writers. (See the example below.)
//!
//! # Simple example for the data file writer used parquet physical format:
//! ```rust, no_run
//! use std::collections::HashMap;
//! use std::sync::Arc;
//!
//! use arrow_array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
//! use async_trait::async_trait;
//! use iceberg::io::{FileIO, FileIOBuilder};
//! use iceberg::spec::DataFile;
//! use iceberg::transaction::Transaction;
//! use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
//! use iceberg::writer::file_writer::ParquetWriterBuilder;
//! use iceberg::writer::file_writer::location_generator::{
//!     DefaultFileNameGenerator, DefaultLocationGenerator,
//! };
//! use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
//! use iceberg::{Catalog, CatalogBuilder, MemoryCatalog, Result, TableIdent};
//! use parquet::file::properties::WriterProperties;
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Connect to a catalog.
//!     use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
//!     let catalog = MemoryCatalogBuilder::default()
//!         .load(
//!             "memory",
//!             HashMap::from([(
//!                 MEMORY_CATALOG_WAREHOUSE.to_string(),
//!                 "file:///path/to/warehouse".to_string(),
//!             )]),
//!         )
//!         .await?;
//!     // Add customized code to create a table first.
//!
//!     // Load table from catalog.
//!     let table = catalog
//!         .load_table(&TableIdent::from_strs(["hello", "world"])?)
//!         .await?;
//!     let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
//!     let file_name_generator = DefaultFileNameGenerator::new(
//!         "test".to_string(),
//!         None,
//!         iceberg::spec::DataFileFormat::Parquet,
//!     );
//!
//!     // Create a parquet file writer builder. The parameter can get from table.
//!     let parquet_writer_builder = ParquetWriterBuilder::new(
//!         WriterProperties::default(),
//!         table.metadata().current_schema().clone(),
//!         table.file_io().clone(),
//!         location_generator.clone(),
//!         file_name_generator.clone(),
//!     );
//!     // Create a data file writer using parquet file writer builder.
//!     let data_file_writer_builder = DataFileWriterBuilder::new(parquet_writer_builder, None, 0);
//!     // Build the data file writer
//!     let mut data_file_writer = data_file_writer_builder.build().await.unwrap();
//!
//!     // Write the data using data_file_writer...
//!
//!     // Close the write and it will return data files back
//!     let data_files = data_file_writer.close().await.unwrap();
//!
//!     Ok(())
//! }
//! ```
//!
//! # Custom writer to record latency
//! ```rust, no_run
//! use std::collections::HashMap;
//! use std::time::Instant;
//!
//! use arrow_array::RecordBatch;
//! use iceberg::io::FileIOBuilder;
//! use iceberg::memory::MemoryCatalogBuilder;
//! use iceberg::spec::DataFile;
//! use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
//! use iceberg::writer::file_writer::ParquetWriterBuilder;
//! use iceberg::writer::file_writer::location_generator::{
//!     DefaultFileNameGenerator, DefaultLocationGenerator,
//! };
//! use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
//! use iceberg::{Catalog, CatalogBuilder, MemoryCatalog, Result, TableIdent};
//! use parquet::file::properties::WriterProperties;
//!
//! #[derive(Clone)]
//! struct LatencyRecordWriterBuilder<B> {
//!     inner_writer_builder: B,
//! }
//!
//! impl<B: IcebergWriterBuilder> LatencyRecordWriterBuilder<B> {
//!     pub fn new(inner_writer_builder: B) -> Self {
//!         Self {
//!             inner_writer_builder,
//!         }
//!     }
//! }
//!
//! #[async_trait::async_trait]
//! impl<B: IcebergWriterBuilder> IcebergWriterBuilder for LatencyRecordWriterBuilder<B> {
//!     type R = LatencyRecordWriter<B::R>;
//!
//!     async fn build(self) -> Result<Self::R> {
//!         Ok(LatencyRecordWriter {
//!             inner_writer: self.inner_writer_builder.build().await?,
//!         })
//!     }
//! }
//! struct LatencyRecordWriter<W> {
//!     inner_writer: W,
//! }
//!
//! #[async_trait::async_trait]
//! impl<W: IcebergWriter> IcebergWriter for LatencyRecordWriter<W> {
//!     async fn write(&mut self, input: RecordBatch) -> Result<()> {
//!         let start = Instant::now();
//!         self.inner_writer.write(input).await?;
//!         let _latency = start.elapsed();
//!         // record latency...
//!         Ok(())
//!     }
//!
//!     async fn close(&mut self) -> Result<Vec<DataFile>> {
//!         let start = Instant::now();
//!         let res = self.inner_writer.close().await?;
//!         let _latency = start.elapsed();
//!         // record latency...
//!         Ok(res)
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Connect to a catalog.
//!     use iceberg::memory::MEMORY_CATALOG_WAREHOUSE;
//!     let catalog = MemoryCatalogBuilder::default()
//!         .load(
//!             "memory",
//!             HashMap::from([(
//!                 MEMORY_CATALOG_WAREHOUSE.to_string(),
//!                 "file:///path/to/warehouse".to_string(),
//!             )]),
//!         )
//!         .await?;
//!
//!     // Add customized code to create a table first.
//!
//!     // Load table from catalog.
//!     let table = catalog
//!         .load_table(&TableIdent::from_strs(["hello", "world"])?)
//!         .await?;
//!     let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
//!     let file_name_generator = DefaultFileNameGenerator::new(
//!         "test".to_string(),
//!         None,
//!         iceberg::spec::DataFileFormat::Parquet,
//!     );
//!
//!     // Create a parquet file writer builder. The parameter can get from table.
//!     let parquet_writer_builder = ParquetWriterBuilder::new(
//!         WriterProperties::default(),
//!         table.metadata().current_schema().clone(),
//!         table.file_io().clone(),
//!         location_generator.clone(),
//!         file_name_generator.clone(),
//!     );
//!     // Create a data file writer builder using parquet file writer builder.
//!     let data_file_writer_builder = DataFileWriterBuilder::new(parquet_writer_builder, None, 0);
//!     // Create latency record writer using data file writer builder.
//!     let latency_record_builder = LatencyRecordWriterBuilder::new(data_file_writer_builder);
//!     // Build the final writer
//!     let mut latency_record_data_file_writer = latency_record_builder.build().await.unwrap();
//!
//!     Ok(())
//! }
//! ```

pub mod base_writer;
pub mod file_writer;

use arrow_array::RecordBatch;

use crate::Result;
use crate::spec::DataFile;

type DefaultInput = RecordBatch;
type DefaultOutput = Vec<DataFile>;

/// The builder for iceberg writer.
#[async_trait::async_trait]
pub trait IcebergWriterBuilder<I = DefaultInput, O = DefaultOutput>:
    Send + Clone + 'static
{
    /// The associated writer type.
    type R: IcebergWriter<I, O>;
    /// Build the iceberg writer.
    async fn build(self) -> Result<Self::R>;
}

/// The iceberg writer used to write data to iceberg table.
#[async_trait::async_trait]
pub trait IcebergWriter<I = DefaultInput, O = DefaultOutput>: Send + 'static {
    /// Write data to iceberg table.
    async fn write(&mut self, input: I) -> Result<()>;
    /// Close the writer and return the written data files.
    /// If close failed, the data written before maybe be lost. User may need to recreate the writer and rewrite the data again.
    /// # NOTE
    /// After close, regardless of success or failure, the writer should never be used again, otherwise the writer will panic.
    async fn close(&mut self) -> Result<O>;
}

/// The current file status of the Iceberg writer.
/// This is implemented for writers that write a single file at a time.
pub trait CurrentFileStatus {
    /// Get the current file path.
    fn current_file_path(&self) -> String;
    /// Get the current file row number.
    fn current_row_num(&self) -> usize;
    /// Get the current file written size.
    fn current_written_size(&self) -> usize;
}

#[cfg(test)]
mod tests {
    use arrow_array::RecordBatch;
    use arrow_schema::Schema;
    use arrow_select::concat::concat_batches;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::IcebergWriter;
    use crate::io::FileIO;
    use crate::spec::{DataFile, DataFileFormat};

    // This function is used to guarantee the trait can be used as a object safe trait.
    async fn _guarantee_object_safe(mut w: Box<dyn IcebergWriter>) {
        let _ = w
            .write(RecordBatch::new_empty(Schema::empty().into()))
            .await;
        let _ = w.close().await;
    }

    // This function check:
    // The data of the written parquet file is correct.
    // The metadata of the data file is consistent with the written parquet file.
    pub(crate) async fn check_parquet_data_file(
        file_io: &FileIO,
        data_file: &DataFile,
        batch: &RecordBatch,
    ) {
        assert_eq!(data_file.file_format, DataFileFormat::Parquet);

        let input_file = file_io.new_input(data_file.file_path.clone()).unwrap();
        // read the written file
        let input_content = input_file.read().await.unwrap();
        let reader_builder =
            ParquetRecordBatchReaderBuilder::try_new(input_content.clone()).unwrap();

        // check data
        let reader = reader_builder.build().unwrap();
        let batches = reader.map(|batch| batch.unwrap()).collect::<Vec<_>>();
        let res = concat_batches(&batch.schema(), &batches).unwrap();
        assert_eq!(*batch, res);
    }
}
