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
//! 1.Customize the writer using the writer trait.
//! 2.Combine different writer to build a writer which have complex write logic.
//!
//! There are two kinds of writer:
//! 1. FileWriter: Focus on writing record batch to different physical file format.(Such as parquet. orc)
//! 2. IcebergWriter: Focus on the logical format of iceberg table. It will write the data using the FileWriter finally.
//!
//! # Simple example for data file writer:
//! ```ignore
//! // Create a parquet file writer builder. The parameter can get from table.
//! let file_writer_builder = ParquetWriterBuilder::new(
//!    0,
//!    WriterProperties::builder().build(),
//!    schema,
//!    file_io.clone(),
//!    loccation_gen,
//!    file_name_gen,
//! )
//! // Create a data file writer using parquet file writer builder.
//! let data_file_builder = DataFileBuilder::new(file_writer_builder);
//! // Build the data file writer.
//! let data_file_writer = data_file_builder.build().await.unwrap();
//!
//! data_file_writer.write(&record_batch).await.unwrap();
//! let data_files = data_file_writer.flush().await.unwrap();
//! ```

pub mod base_writer;
pub mod file_writer;

use crate::{spec::DataFile, Result};
use arrow_array::RecordBatch;

type DefaultInput = RecordBatch;
type DefaultOutput = Vec<DataFile>;

/// The builder for iceberg writer.
#[async_trait::async_trait]
pub trait IcebergWriterBuilder<I = DefaultInput, O = DefaultOutput>:
    Send + Clone + 'static
{
    /// The associated writer type.
    type R: IcebergWriter<I, O>;
    /// The associated writer config type used to build the writer.
    type C;
    /// Build the iceberg writer.
    async fn build(self, config: Self::C) -> Result<Self::R>;
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

#[cfg(test)]
mod tests {
    use arrow_array::RecordBatch;
    use arrow_schema::Schema;
    use arrow_select::concat::concat_batches;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use crate::{
        io::FileIO,
        spec::{DataFile, DataFileFormat},
    };

    use super::IcebergWriter;

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
        let metadata = reader_builder.metadata().clone();

        // check data
        let reader = reader_builder.build().unwrap();
        let batches = reader.map(|batch| batch.unwrap()).collect::<Vec<_>>();
        let res = concat_batches(&batch.schema(), &batches).unwrap();
        assert_eq!(*batch, res);

        // check metadata
        let expect_column_num = batch.num_columns();

        assert_eq!(
            data_file.record_count,
            metadata
                .row_groups()
                .iter()
                .map(|group| group.num_rows())
                .sum::<i64>() as u64
        );

        assert_eq!(data_file.file_size_in_bytes, input_content.len() as u64);

        assert_eq!(data_file.column_sizes.len(), expect_column_num);
        data_file.column_sizes.iter().for_each(|(&k, &v)| {
            let expect = metadata
                .row_groups()
                .iter()
                .map(|group| group.column(k as usize).compressed_size())
                .sum::<i64>() as u64;
            assert_eq!(v, expect);
        });

        assert_eq!(data_file.value_counts.len(), expect_column_num);
        data_file.value_counts.iter().for_each(|(_, &v)| {
            let expect = metadata
                .row_groups()
                .iter()
                .map(|group| group.num_rows())
                .sum::<i64>() as u64;
            assert_eq!(v, expect);
        });

        assert_eq!(data_file.null_value_counts.len(), expect_column_num);
        data_file.null_value_counts.iter().for_each(|(&k, &v)| {
            let expect = batch.column(k as usize).null_count() as u64;
            assert_eq!(v, expect);
        });

        assert_eq!(data_file.split_offsets.len(), metadata.num_row_groups());
        data_file
            .split_offsets
            .iter()
            .enumerate()
            .for_each(|(i, &v)| {
                let expect = metadata.row_groups()[i].file_offset().unwrap();
                assert_eq!(v, expect);
            });
    }
}
