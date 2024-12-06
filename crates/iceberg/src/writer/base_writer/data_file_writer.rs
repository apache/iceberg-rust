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

//! This module provide `DataFileWriter`.

use arrow_array::RecordBatch;
use itertools::Itertools;

use crate::spec::{DataContentType, DataFile, Struct};
use crate::writer::file_writer::{FileWriter, FileWriterBuilder};
use crate::writer::{CurrentFileStatus, IcebergWriter, IcebergWriterBuilder};
use crate::Result;

/// Builder for `DataFileWriter`.
#[derive(Clone)]
pub struct DataFileWriterBuilder<B: FileWriterBuilder> {
    inner: B,
}

impl<B: FileWriterBuilder> DataFileWriterBuilder<B> {
    /// Create a new `DataFileWriterBuilder` using a `FileWriterBuilder`.
    pub fn new(inner: B) -> Self {
        Self { inner }
    }
}

/// Config for `DataFileWriter`.
pub struct DataFileWriterConfig {
    partition_value: Struct,
}

impl DataFileWriterConfig {
    /// Create a new `DataFileWriterConfig` with partition value.
    pub fn new(partition_value: Option<Struct>) -> Self {
        Self {
            partition_value: partition_value.unwrap_or(Struct::empty()),
        }
    }
}

impl<B: FileWriterBuilder> IcebergWriterBuilder for DataFileWriterBuilder<B> {
    type R = DataFileWriter<B>;
    type C = DataFileWriterConfig;

    async fn build(self, config: Self::C) -> Result<Self::R> {
        Ok(DataFileWriter {
            inner_writer: Some(self.inner.clone().build().await?),
            partition_value: config.partition_value,
        })
    }
}

/// A writer write data is within one spec/partition.
pub struct DataFileWriter<B: FileWriterBuilder> {
    inner_writer: Option<B::R>,
    partition_value: Struct,
}

impl<B: FileWriterBuilder> IcebergWriter for DataFileWriter<B> {
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.inner_writer.as_mut().unwrap().write(&batch).await
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        let writer = self.inner_writer.take().unwrap();
        Ok(writer
            .close()
            .await?
            .into_iter()
            .map(|mut res| {
                res.content(DataContentType::Data);
                res.partition(self.partition_value.clone());
                res.build().expect("Guaranteed to be valid")
            })
            .collect_vec())
    }
}

impl<B: FileWriterBuilder> CurrentFileStatus for DataFileWriter<B> {
    fn current_file_path(&self) -> String {
        self.inner_writer.as_ref().unwrap().current_file_path()
    }

    fn current_row_num(&self) -> usize {
        self.inner_writer.as_ref().unwrap().current_row_num()
    }

    fn current_written_size(&self) -> usize {
        self.inner_writer.as_ref().unwrap().current_written_size()
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use crate::io::FileIOBuilder;
    use crate::spec::{DataContentType, DataFileFormat, Schema, Struct};
    use crate::writer::base_writer::data_file_writer::{
        DataFileWriterBuilder, DataFileWriterConfig,
    };
    use crate::writer::file_writer::location_generator::test::MockLocationGenerator;
    use crate::writer::file_writer::location_generator::DefaultFileNameGenerator;
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::{IcebergWriter, IcebergWriterBuilder};
    use crate::Result;

    #[tokio::test]
    async fn test_parquet_writer() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen =
            MockLocationGenerator::new(temp_dir.path().to_str().unwrap().to_string());
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(Schema::builder().build().unwrap()),
            file_io.clone(),
            location_gen,
            file_name_gen,
        );
        let mut data_file_writer = DataFileWriterBuilder::new(pw)
            .build(DataFileWriterConfig::new(None))
            .await?;

        let data_file = data_file_writer.close().await.unwrap();
        assert_eq!(data_file.len(), 1);
        assert_eq!(data_file[0].file_format, DataFileFormat::Parquet);
        assert_eq!(data_file[0].content, DataContentType::Data);
        assert_eq!(data_file[0].partition, Struct::empty());

        Ok(())
    }
}
