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

use std::future::Future;
use arrow_array::RecordBatch;
use crate::spec::DataFile;
use crate::Result;

type DefaultInput = RecordBatch;
type DefaultOutput = Vec<DataFile>;

/// The builder for iceberg writer.
pub trait IcebergWriterBuilder<I = DefaultInput, O = DefaultOutput>:
Send + Clone + 'static
{
    /// The associated writer type.
    type R: IcebergWriter<I, O>;
    /// The associated writer config type used to build the writer.
    type C: Send + 'static;
    /// Build the iceberg writer.
    fn build(self, config: Self::C) -> impl Future<Output=Result<Self::R>> + Send;
}

/// The iceberg writer used to write data to iceberg table.
pub trait IcebergWriter<I = DefaultInput, O = DefaultOutput>: Send + 'static {
    /// Write data to iceberg table.
    fn write(&mut self, input: I) -> impl Future<Output=Result<()>> + Send + '_;
    /// Close the writer and return the written data files.
    /// If close failed, the data written before maybe be lost. User may need to recreate the writer and rewrite the data again.
    /// # NOTE
    /// After close, regardless of success or failure, the writer should never be used again, otherwise the writer will panic.
    fn close(&mut self) -> impl Future<Output=Result<O>> + Send + '_;
}

mod dyn_trait {
    use crate::writer::{DefaultInput, DefaultOutput, IcebergWriter, IcebergWriterBuilder};
    use super::Result;
    use dyn_clone::{clone_trait_object, DynClone};

    #[async_trait::async_trait]
    pub trait DynIcebergWriterBuilder<C, I, O>: Send + DynClone + 'static {
        async fn build(self, config: C) -> Result<BoxedIcebergWriter<I, O>>;
    }

    clone_trait_object!(<C, I, O> DynIcebergWriterBuilder<C, I, O>);

    #[async_trait::async_trait]
    impl<I: 'static + Send, O: 'static + Send, B: IcebergWriterBuilder<I, O>> DynIcebergWriterBuilder<B::C, I, O> for B
    where
        B::C: Send,
    {
        async fn build(self, config: B::C) -> Result<BoxedIcebergWriter<I, O>> {
            Ok(Box::new(self.build(config).await?) as _)
        }
    }

    /// Type alias for `Box<dyn DynIcebergWriterBuilder>`
    pub type BoxedIcebergWriterBuilder<C, I = DefaultInput, O = DefaultOutput> = Box<dyn DynIcebergWriterBuilder<C, I, O>>;

    impl<C: Send + 'static, I: Send + 'static, O: Send + 'static> IcebergWriterBuilder<I, O> for BoxedIcebergWriterBuilder<C, I, O> {
        type R = BoxedIcebergWriter<I, O>;
        type C = C;

        async fn build(self, config: Self::C) -> Result<Self::R> {
            DynIcebergWriterBuilder::build(self, config).await
        }
    }

    /// Extension methods for `IcebergWriterBuilder`
    pub trait IcebergWriterBuilderDynExt<I, O>: IcebergWriterBuilder<I, O> {
        /// Create a type erased `IcebergWriterBuilder` wrapped with `Box`.
        fn boxed(self) -> BoxedIcebergWriterBuilder<Self::C, I, O>
        where
            Self: Sized,
            I: Send + 'static,
            O: Send + 'static,
        {
            Box::new(self) as _
        }
    }


    /// The dyn iceberg writer used to write data to iceberg table.
    #[async_trait::async_trait]
    pub trait DynIcebergWriter<I, O>: Send + 'static {
        /// `write` of trait `IcebergWriter`
        async fn write(&mut self, input: I) -> Result<()>;
        /// `close` of trait `IcebergWriter`
        async fn close(&mut self) -> Result<O>;
    }

    #[async_trait::async_trait]
    impl<I: 'static + Send, O: 'static + Send, W: IcebergWriter<I, O>> DynIcebergWriter<I, O> for W
    {
        async fn write(&mut self, input: I) -> Result<()> {
            self.write(input).await
        }

        async fn close(&mut self) -> Result<O> {
            self.close().await
        }
    }

    /// Type alias for `Box<dyn DynIcebergWriter>`
    pub type BoxedIcebergWriter<I = DefaultInput, O = DefaultOutput> = Box<dyn DynIcebergWriter<I, O>>;

    impl<I: 'static + Send, O: 'static + Send> IcebergWriter<I, O> for BoxedIcebergWriter<I, O> {
        async fn write(&mut self, input: I) -> Result<()> {
            (**self).write(input).await
        }

        async fn close(&mut self) -> Result<O> {
            (**self).close().await
        }
    }

    /// Extension methods for `IcebergWriter`
    pub trait IcebergWriterDynExt<I, O>: IcebergWriter<I, O> {
        /// Create a type erased `IcebergWriter` wrapped with `Box`.
        fn boxed(self) -> BoxedIcebergWriter<I, O>
        where
            Self: Sized,
            I: Send + 'static,
            O: Send + 'static,
        {
            Box::new(self) as _
        }
    }
}

pub use dyn_trait::{BoxedIcebergWriter, IcebergWriterDynExt, BoxedIcebergWriterBuilder, IcebergWriterBuilderDynExt};


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
