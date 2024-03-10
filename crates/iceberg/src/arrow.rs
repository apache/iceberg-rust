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

//! Parquet file data reader

use crate::{Error, ErrorKind};
use arrow_schema::SchemaRef as ArrowSchemaRef;
use async_stream::try_stream;
use futures::stream::StreamExt;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::metadata::ParquetMetaData;
use std::sync::Arc;

use crate::io::FileIO;
use crate::scan::{ArrowRecordBatchStream, FileScanTaskStream};
use crate::spec::SchemaRef;

/// Builder to create ArrowReader
pub struct ArrowReaderBuilder {
    batch_size: Option<usize>,
    columns: Vec<usize>,
    file_io: FileIO,
    schema: SchemaRef,
}

impl ArrowReaderBuilder {
    /// Create a new ArrowReaderBuilder
    pub fn new(file_io: FileIO, schema: SchemaRef) -> Self {
        ArrowReaderBuilder {
            batch_size: None,
            columns: vec![],
            file_io,
            schema,
        }
    }

    /// Sets the desired size of batches in the response
    /// to something other than the default
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    /// Sets the desired column projection.
    pub fn with_column_projection(mut self, columns: Vec<usize>) -> Self {
        self.columns = columns;
        self
    }

    /// Build the ArrowReader.
    pub fn build(self) -> ArrowReader {
        ArrowReader {
            batch_size: self.batch_size,
            columns: self.columns,
            schema: self.schema,
            file_io: self.file_io,
        }
    }
}

/// Reads data from Parquet files
pub struct ArrowReader {
    batch_size: Option<usize>,
    columns: Vec<usize>,
    #[allow(dead_code)]
    schema: SchemaRef,
    file_io: FileIO,
}

impl ArrowReader {
    /// Take a stream of FileScanTasks and reads all the files.
    /// Returns a stream of Arrow RecordBatches containing the data from the files
    pub fn read(self, mut tasks: FileScanTaskStream) -> crate::Result<ArrowRecordBatchStream> {
        let file_io = self.file_io.clone();

        Ok(try_stream! {
            while let Some(Ok(task)) = tasks.next().await {
                let parquet_reader = file_io
                    .new_input(task.data_file().file_path())?
                    .reader()
                    .await?;

                let mut batch_stream_builder = ParquetRecordBatchStreamBuilder::new(parquet_reader)
                    .await?;

                let metadata = batch_stream_builder.metadata();
                let parquet_schema = batch_stream_builder.schema();
                let projection_mask = self.get_arrow_projection_mask(metadata, parquet_schema)?;
                batch_stream_builder = batch_stream_builder.with_projection(projection_mask);

                if let Some(batch_size) = self.batch_size {
                    batch_stream_builder = batch_stream_builder.with_batch_size(batch_size);
                }

                let mut batch_stream = batch_stream_builder.build()?;

                while let Some(batch) = batch_stream.next().await {
                    yield batch?;
                }
            }
        }
        .boxed())
    }

    fn get_arrow_projection_mask(
        &self,
        metadata: &Arc<ParquetMetaData>,
        parquet_schema: &ArrowSchemaRef,
    ) -> crate::Result<ProjectionMask> {
        if self.columns.is_empty() {
            Ok(ProjectionMask::all())
        } else {
            let mut indices = vec![];
            for col in &self.columns {
                if *col > parquet_schema.fields().len() {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Column index {} out of range. Schema: {}",
                            col, parquet_schema
                        ),
                    ));
                }
                indices.push(*col - 1);
            }
            Ok(ProjectionMask::roots(
                metadata.file_metadata().schema_descr(),
                indices,
            ))
        }
    }
}
