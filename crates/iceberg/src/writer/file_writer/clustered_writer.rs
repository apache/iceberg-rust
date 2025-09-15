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

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef as ArrowSchemaRef;

use crate::arrow::record_batch_partition_splitter::RecordBatchPartitionSplitter;
use crate::spec::{DataFileBuilder, PartitionKey, PartitionSpecRef, SchemaRef};
use crate::writer::CurrentFileStatus;
use crate::writer::file_writer::{FileWriter, FileWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// todo doc
#[derive(Clone)]
pub struct ClusteredWriterBuilder<B: FileWriterBuilder> {
    inner_builder: B,
    partition_spec: PartitionSpecRef,
    table_schema: SchemaRef,
    arrow_schema: ArrowSchemaRef,
}

impl<B: FileWriterBuilder> ClusteredWriterBuilder<B> {
    #[allow(dead_code)]
    pub fn new(
        inner_builder: B,
        partition_spec: PartitionSpecRef,
        table_schema: SchemaRef,
        arrow_schema: ArrowSchemaRef,
    ) -> Self {
        Self {
            inner_builder,
            partition_spec,
            table_schema,
            arrow_schema,
        }
    }
}

impl<B: FileWriterBuilder> FileWriterBuilder for ClusteredWriterBuilder<B> {
    type R = ClusteredWriter<B>;

    async fn build(self) -> Result<Self::R> {
        let splitter = RecordBatchPartitionSplitter::new(
            self.arrow_schema,
            self.table_schema.clone(),
            self.partition_spec.clone(),
        )?;

        Ok(ClusteredWriter {
            inner: None,
            inner_builder: self.inner_builder,
            splitter,
            table_schema: self.table_schema,
            partition_spec: self.partition_spec,
            current_partition_key: None,
            data_file_builders: vec![],
        })
    }
}

pub struct ClusteredWriter<B: FileWriterBuilder> {
    inner: Option<B::R>,
    inner_builder: B,
    splitter: RecordBatchPartitionSplitter,
    table_schema: SchemaRef,
    partition_spec: PartitionSpecRef,
    current_partition_key: Option<PartitionKey>,
    data_file_builders: Vec<DataFileBuilder>,
}

impl<B: FileWriterBuilder> FileWriter for ClusteredWriter<B> {
    async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let splits = self.splitter.split(batch)?;
        if splits.len() > 1 {
            // todo revisit this, should we assume one batch can contain at most one partition's data?
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Records from multiple partitions found in one record batch!",
            ));
        }

        if let Some((partition_value, record_batch)) = splits.first() {
            let partition_key = PartitionKey::new(
                self.partition_spec.as_ref().clone(),
                self.table_schema.clone(),
                partition_value.clone(),
            );

            if self
                .current_partition_key
                .as_ref()
                .is_none_or(|pk| pk != &partition_key)
            {
                if let Some(inner) = self.inner.take() {
                    // Close the current writer, roll to a new file
                    self.data_file_builders.extend(inner.close().await?);

                    // Start a new writer
                    // TODO how to pass partition key to inner builder??
                    self.inner = Some(self.inner_builder.clone().build().await?);
                }
            }

            if let Some(writer) = self.inner.as_mut() {
                Ok(writer.write(record_batch).await?)
            } else {
                Err(Error::new(
                    ErrorKind::Unexpected,
                    "Writer is not initialized!",
                ))
            }
        } else {
            // Input is empty
            // todo should we fail?
            Ok(())
        }
    }

    async fn close(mut self) -> Result<Vec<DataFileBuilder>> {
        // Close the current writer and merge the output
        if let Some(current_writer) = self.inner {
            self.data_file_builders
                .extend(current_writer.close().await?);
        }
        Ok(self.data_file_builders)
    }
}

impl<B: FileWriterBuilder> CurrentFileStatus for ClusteredWriter<B> {
    fn current_file_path(&self) -> String {
        self.inner.as_ref().unwrap().current_file_path()
    }

    fn current_row_num(&self) -> usize {
        self.inner.as_ref().unwrap().current_row_num()
    }

    fn current_written_size(&self) -> usize {
        self.inner.as_ref().unwrap().current_written_size()
    }
}
