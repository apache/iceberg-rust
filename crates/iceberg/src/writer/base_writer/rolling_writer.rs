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

use crate::spec::DataFile;
use crate::writer::base_writer::data_file_writer::DataFileWriter;
use crate::writer::file_writer::FileWriterBuilder;
use crate::writer::{IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

#[async_trait]
pub trait RollingFileWriter: IcebergWriter {
    fn should_roll(&mut self, input_size: u64) -> bool;
}

#[derive(Clone)]
pub struct RollingDataFileWriterBuilder<B: FileWriterBuilder> {
    inner_builder: B,
    target_size: u64,
}

impl<B: FileWriterBuilder> RollingDataFileWriterBuilder<B> {
    pub fn new(inner_builder: B, target_size: u64) -> Self {
        Self {
            inner_builder,
            target_size,
        }
    }
}

#[async_trait]
impl<B: FileWriterBuilder> IcebergWriterBuilder for RollingDataFileWriterBuilder<B> {
    type R = RollingDataFileWriter<B>;

    async fn build(self) -> Result<Self::R> {
        Ok(RollingDataFileWriter {
            inner: None,
            inner_builder: self.inner_builder,
            target_size: self.target_size,
            written_size: 0,
            data_files: vec![],
        })
    }
}

pub struct RollingDataFileWriter<B: FileWriterBuilder> {
    inner: Option<DataFileWriter<B::R>>,
    inner_builder: B,
    target_size: u64,
    written_size: u64,
    data_files: Vec<DataFile>,
}

#[async_trait]
impl<B: FileWriterBuilder> IcebergWriter for RollingDataFileWriter<B> {
    async fn write(&mut self, input: RecordBatch) -> Result<()> {
        let input_size = input.get_array_memory_size() as u64;
        if self.should_roll(input_size) {
            if let Some(mut inner) = self.inner.take() {
                // close the current writer, roll to a new file
                self.data_files.extend(inner.close().await?);
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
        Ok(take(&mut self.data_files))
    }
}

impl<B: FileWriterBuilder> RollingFileWriter for RollingDataFileWriter<B> {
    fn should_roll(&mut self, input_size: u64) -> bool {
        self.written_size + input_size > self.target_size
    }
}
