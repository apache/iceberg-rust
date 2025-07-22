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

use std::mem::take;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use futures::future::try_join_all;

use crate::runtime::{JoinHandle, spawn};
use crate::spec::DataFile;
use crate::writer::{IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// A writer that can roll over to a new file when certain conditions are met.
///
/// This trait extends `IcebergWriter` with the ability to determine when to start
/// writing to a new file based on the size of incoming data.
#[async_trait]
pub trait RollingFileWriter: IcebergWriter {
    /// Determines if the writer should roll over to a new file.
    ///
    /// # Arguments
    ///
    /// * `input_size` - The size in bytes of the incoming data
    ///
    /// # Returns
    ///
    /// `true` if a new file should be started, `false` otherwise
    fn should_roll(&mut self, input_size: u64) -> bool;
}

/// Builder for creating a `RollingDataFileWriter` that rolls over to a new file
/// when the data size exceeds a target threshold.
#[derive(Clone)]
pub struct RollingDataFileWriterBuilder<B: IcebergWriterBuilder> {
    inner_builder: B,
    target_size: u64,
}

impl<B: IcebergWriterBuilder> RollingDataFileWriterBuilder<B> {
    /// Creates a new `RollingDataFileWriterBuilder` with the specified inner builder and target size.
    ///
    /// # Arguments
    ///
    /// * `inner_builder` - The builder for the underlying file writer
    /// * `target_size` - The target size in bytes before rolling over to a new file
    pub fn new(inner_builder: B, target_size: u64) -> Self {
        Self {
            inner_builder,
            target_size,
        }
    }
}

#[async_trait]
impl<B: IcebergWriterBuilder> IcebergWriterBuilder for RollingDataFileWriterBuilder<B> {
    type R = RollingDataFileWriter<B>;

    async fn build(self) -> Result<Self::R> {
        Ok(RollingDataFileWriter {
            inner: None,
            inner_builder: self.inner_builder,
            target_size: self.target_size,
            written_size: 0,
            close_handles: vec![],
        })
    }
}

/// A writer that automatically rolls over to a new file when the data size
/// exceeds a target threshold.
///
/// This writer wraps another file writer and tracks the amount of data written.
/// When the data size exceeds the target size, it closes the current file and
/// starts writing to a new one.
pub struct RollingDataFileWriter<B: IcebergWriterBuilder> {
    inner: Option<B::R>,
    inner_builder: B,
    target_size: u64,
    written_size: u64,
    close_handles: Vec<JoinHandle<Result<Vec<DataFile>>>>,
}

#[async_trait]
impl<B: IcebergWriterBuilder> IcebergWriter for RollingDataFileWriter<B> {
    async fn write(&mut self, input: RecordBatch) -> Result<()> {
        let input_size = input.get_array_memory_size() as u64;
        if self.should_roll(input_size) {
            if let Some(mut inner) = self.inner.take() {
                // close the current writer, roll to a new file
                let handle = spawn(async move { inner.close().await });
                self.close_handles.push(handle)
            }

            // clear bytes written
            self.written_size = 0;
        }

        if self.inner.is_none() {
            // start a new writer
            self.inner = Some(self.inner_builder.clone().build().await?);
        }

        // write the input and count bytes written
        let Some(writer) = self.inner.as_mut() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Writer is not initialized!",
            ));
        };
        writer.write(input).await?;
        self.written_size += input_size;
        Ok(())
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        let mut data_files = try_join_all(take(&mut self.close_handles))
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<DataFile>>();

        // close the current writer and merge the output
        if let Some(mut current_writer) = take(&mut self.inner) {
            data_files.extend(current_writer.close().await?);
        }

        Ok(data_files)
    }
}

impl<B: IcebergWriterBuilder> RollingFileWriter for RollingDataFileWriter<B> {
    fn should_roll(&mut self, input_size: u64) -> bool {
        self.written_size + input_size > self.target_size
    }
}
