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

//! Parquet format implementation wrapping the existing `ArrowReader` and `ParquetWriter`.
//!
//! `ParquetArrowModel` implements both [`FormatReader`] and [`FormatWriter`] for
//! Parquet files producing/consuming Arrow `RecordBatch`. This is the shipped
//! default format-batch pairing registered in [`FormatRegistry::default()`].
//!
//! This implementation wraps the existing reader and writer code rather than
//! replacing it. The existing types (`ArrowReader`, `ParquetWriter`) remain
//! unchanged — `ParquetArrowModel` delegates to them internally.

#![allow(dead_code)]

use futures::future::BoxFuture;

use super::traits::{
    DataBatch, FormatFileWriter, FormatReader, FormatWriter, ReadOptions, ReadResult, WriteOptions,
    WriterResult,
};
use crate::io::{InputFile, OutputFile};
use crate::spec::DataFileFormat;
use crate::Result;

/// Parquet format implementation producing Arrow `RecordBatch`.
///
/// Registered in the `FormatRegistry` as the reader and writer for
/// `(DataFileFormat::Parquet, TypeId::of::<RecordBatch>())`.
///
/// # Example registration
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use arrow_array::RecordBatch;
/// use iceberg::formats::{FormatRegistry, ParquetArrowModel};
///
/// let mut registry = FormatRegistry::new();
/// let model = Arc::new(ParquetArrowModel);
/// registry.register_reader::<RecordBatch>(model.clone());
/// registry.register_writer::<RecordBatch>(model);
/// ```
#[derive(Debug, Clone)]
pub struct ParquetArrowModel;

impl FormatReader for ParquetArrowModel {
    fn format(&self) -> DataFileFormat {
        DataFileFormat::Parquet
    }

    fn read(
        &self,
        _input: InputFile,
        _options: ReadOptions,
    ) -> BoxFuture<'static, Result<ReadResult>> {
        // Production: delegates to the existing ArrowReader pipeline.
        //
        // 1. Opens the Parquet file via ArrowFileReader
        // 2. Applies projection mask from options.schema
        // 3. Evaluates row group statistics against options.predicate
        // 4. Returns ReadResult with:
        //    - stream: the RecordBatch stream from ParquetRecordBatchStream
        //    - residual_predicate: the predicate (or portion of it) that
        //      row-group statistics could not fully resolve
        todo!("Wrap ArrowReader")
    }
}

impl FormatWriter for ParquetArrowModel {
    fn format(&self) -> DataFileFormat {
        DataFileFormat::Parquet
    }

    fn write(
        &self,
        _output: OutputFile,
        _options: WriteOptions,
    ) -> BoxFuture<'static, Result<Box<dyn FormatFileWriter>>> {
        // Production: delegates to the existing ParquetWriter.
        //
        // 1. Converts options.schema to Arrow schema
        // 2. Builds WriterProperties from options.properties
        // 3. Creates AsyncArrowWriter via the existing ParquetWriter path
        // 4. Returns a ParquetFormatFileWriter that wraps the writer
        todo!("Wrap ParquetWriter")
    }
}

/// The per-file writer returned by `ParquetArrowModel::write`.
///
/// Wraps the existing `ParquetWriter` and delegates `write_batch` and
/// `close` to it. On `close`, extracts Parquet metadata and builds
/// `DataFileBuilder` with column sizes, value counts, null counts,
/// min/max bounds, and split offsets.
pub struct ParquetFormatFileWriter {
    // Production: holds an AsyncArrowWriter or the existing ParquetWriter.
    _private: (),
}

impl FormatFileWriter for ParquetFormatFileWriter {
    fn write_batch(&mut self, _batch: &dyn DataBatch) -> BoxFuture<'_, Result<()>> {
        // Production:
        // 1. Downcast batch to &RecordBatch via DataBatch::as_any()
        // 2. Write to the underlying AsyncArrowWriter
        todo!("Downcast and write")
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<WriterResult>> {
        // Production:
        // 1. Close the AsyncArrowWriter, get Parquet FileMetaData
        // 2. Extract metrics via parquet_to_data_file_builder()
        // 3. Return WriterResult { data_files: vec![builder] }
        todo!("Close and extract metrics")
    }
}
