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

use async_stream::try_stream;
use futures::stream::StreamExt;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};

use crate::io::FileIO;
use crate::scan::{ArrowRecordBatchStream, FileScanTask, FileScanTaskStream};
use crate::spec::SchemaRef;

/// Default arrow record batch size
const DEFAULT_BATCH_SIZE: usize = 1024;

/// Reads data from Parquet files
pub struct FileRecordBatchReader {
    batch_size: Option<usize>,
    #[allow(dead_code)]
    schema: SchemaRef,
    file_io: FileIO,
}

impl FileRecordBatchReader {
    /// Constructs a new FileRecordBatchReader
    pub fn new(file_io: FileIO, schema: SchemaRef, batch_size: Option<usize>) -> Self {
        FileRecordBatchReader {
            batch_size,
            file_io,
            schema,
        }
    }

    /// Take a stream of FileScanTasks and reads all the files.
    /// Returns a stream of Arrow RecordBatches containing the data from the files
    pub fn read(self, mut tasks: FileScanTaskStream) -> crate::Result<ArrowRecordBatchStream> {
        let file_io = self.file_io.clone();
        let batch_size = self.batch_size.unwrap_or(DEFAULT_BATCH_SIZE);

        Ok(try_stream! {
            while let Some(Ok(task)) = tasks.next().await {

                let projection_mask = self.get_arrow_projection_mask(&task);

                let parquet_reader = file_io
                    .new_input(task.data_file().file_path())?
                    .reader()
                    .await?;

                let mut batch_stream = ParquetRecordBatchStreamBuilder::new(parquet_reader)
                    .await?
                    .with_batch_size(batch_size)
                    .with_projection(projection_mask)
                    .build()?;

                while let Some(batch) = batch_stream.next().await {
                    yield batch?;
                }
            }
        }
        .boxed())
    }

    fn get_arrow_projection_mask(&self, _task: &FileScanTask) -> ProjectionMask {
        // TODO: full implementation
        ProjectionMask::all()
    }
}
