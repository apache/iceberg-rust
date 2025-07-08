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

//! Position delete file writer.
//!
//! This writer does not keep track of seen deletes and assumes all incoming records are ordered
//! by file and position as required by the spec. If there is no external process to order the
//! records, consider using SortingPositionDeleteWriter(WIP) instead.

use std::sync::Arc;

use arrow_array::builder::{PrimitiveBuilder, StringBuilder};
use arrow_array::types::Int64Type;
use arrow_array::RecordBatch;
use once_cell::sync::Lazy;

use crate::arrow::schema_to_arrow_schema;
use crate::spec::{
    DataContentType, DataFile, NestedField, NestedFieldRef, PrimitiveType, Schema, Struct, Type,
};
use crate::writer::file_writer::{FileWriter, FileWriterBuilder};
use crate::writer::{IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

static DELETE_FILE_PATH: Lazy<NestedFieldRef> = Lazy::new(|| {
    Arc::new(NestedField::required(
        2147483546,
        "file_path",
        Type::Primitive(PrimitiveType::String),
    ))
});
static DELETE_FILE_POS: Lazy<NestedFieldRef> = Lazy::new(|| {
    Arc::new(NestedField::required(
        2147483545,
        "pos",
        Type::Primitive(PrimitiveType::Long),
    ))
});
static POSITION_DELETE_SCHEMA: Lazy<Schema> = Lazy::new(|| {
    Schema::builder()
        .with_fields(vec![DELETE_FILE_PATH.clone(), DELETE_FILE_POS.clone()])
        .build()
        .unwrap()
});

/// Position delete input.
#[derive(Clone, PartialEq, Eq, Ord, PartialOrd, Debug)]
pub struct PositionDeleteInput {
    /// The path of the file.
    pub path: Arc<str>,
    /// The row number in data file
    pub pos: i64,
}

impl PositionDeleteInput {
    /// Create a new `PositionDeleteInput`.
    pub fn new(path: Arc<str>, row: i64) -> Self {
        Self { path, pos: row }
    }
}
/// Builder for `MemoryPositionDeleteWriter`.
#[derive(Clone)]
pub struct PositionDeleteWriterBuilder<B: FileWriterBuilder> {
    inner: B,
    partition_value: Option<Struct>,
}

impl<B: FileWriterBuilder> PositionDeleteWriterBuilder<B> {
    /// Create a new `MemoryPositionDeleteWriterBuilder` using a `FileWriterBuilder`.
    pub fn new(inner: B, partition_value: Option<Struct>) -> Self {
        Self {
            inner,
            partition_value,
        }
    }
}

#[async_trait::async_trait]
impl<B: FileWriterBuilder> IcebergWriterBuilder<Vec<PositionDeleteInput>>
    for PositionDeleteWriterBuilder<B>
{
    type R = PositionDeleteWriter<B>;

    async fn build(self) -> Result<Self::R> {
        Ok(PositionDeleteWriter {
            inner_writer: Some(self.inner.build().await?),
            partition_value: self.partition_value.unwrap_or(Struct::empty()),
        })
    }
}

/// Position delete writer.
pub struct PositionDeleteWriter<B: FileWriterBuilder> {
    inner_writer: Option<B::R>,
    partition_value: Struct,
}

#[async_trait::async_trait]
impl<B: FileWriterBuilder> IcebergWriter<Vec<PositionDeleteInput>> for PositionDeleteWriter<B> {
    async fn write(&mut self, input: Vec<PositionDeleteInput>) -> Result<()> {
        let mut path_column_builder = StringBuilder::new();
        let mut offset_column_builder = PrimitiveBuilder::<Int64Type>::new();
        for pd_input in input.into_iter() {
            path_column_builder.append_value(pd_input.path);
            offset_column_builder.append_value(pd_input.pos);
        }
        let record_batch = RecordBatch::try_new(
            Arc::new(schema_to_arrow_schema(&POSITION_DELETE_SCHEMA).unwrap()),
            vec![
                Arc::new(path_column_builder.finish()),
                Arc::new(offset_column_builder.finish()),
            ],
        )
        .map_err(|e| Error::new(ErrorKind::DataInvalid, e.to_string()));

        if let Some(inner_writer) = &mut self.inner_writer {
            inner_writer.write(&record_batch?).await?;
        } else {
            return Err(Error::new(ErrorKind::Unexpected, "write has been closed"));
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        let writer = self.inner_writer.take().unwrap();
        Ok(writer
            .close()
            .await?
            .into_iter()
            .map(|mut res| {
                res.content(DataContentType::PositionDeletes);
                res.partition(self.partition_value.clone());
                res.build().expect("Guaranteed to be valid")
            })
            .collect())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::{Int64Array, StringArray};
    use itertools::Itertools;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use crate::io::FileIOBuilder;
    use crate::spec::{DataContentType, DataFileFormat, Struct};
    use crate::writer::base_writer::position_delete_file_writer::{
        PositionDeleteInput, PositionDeleteWriterBuilder, POSITION_DELETE_SCHEMA,
    };
    use crate::writer::file_writer::location_generator::test::MockLocationGenerator;
    use crate::writer::file_writer::location_generator::DefaultFileNameGenerator;
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::{IcebergWriter, IcebergWriterBuilder};
    use crate::Result;

    #[tokio::test]
    async fn test_position_delete_writer() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let location_gen =
            MockLocationGenerator::new(temp_dir.path().to_str().unwrap().to_string());
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(POSITION_DELETE_SCHEMA.clone()),
            file_io.clone(),
            location_gen,
            file_name_gen,
        );
        let mut position_delete_writer = PositionDeleteWriterBuilder::new(pw, None).build().await?;

        // Write some position delete inputs
        let inputs: Vec<PositionDeleteInput> = vec![
            PositionDeleteInput {
                path: "file2.parquet".into(),
                pos: 2,
            },
            PositionDeleteInput {
                path: "file2.parquet".into(),
                pos: 1,
            },
            PositionDeleteInput {
                path: "file2.parquet".into(),
                pos: 3,
            },
            PositionDeleteInput {
                path: "file3.parquet".into(),
                pos: 2,
            },
            PositionDeleteInput {
                path: "file1.parquet".into(),
                pos: 5,
            },
            PositionDeleteInput {
                path: "file1.parquet".into(),
                pos: 4,
            },
            PositionDeleteInput {
                path: "file1.parquet".into(),
                pos: 1,
            },
        ];
        let expect_inputs = inputs
            .clone()
            .into_iter()
            .map(|input| (input.path.to_string(), input.pos))
            .collect_vec();
        position_delete_writer.write(inputs.clone()).await?;

        let data_files = position_delete_writer.close().await.unwrap();
        assert_eq!(data_files.len(), 1);
        assert_eq!(data_files[0].file_format, DataFileFormat::Parquet);
        assert_eq!(data_files[0].content, DataContentType::PositionDeletes);
        assert_eq!(data_files[0].partition, Struct::empty());

        let parquet_file = file_io
            .new_input(&data_files[0].file_path)?
            .read()
            .await
            .unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(parquet_file).unwrap();
        let reader = builder.build().unwrap();
        let batches = reader.map(|x| x.unwrap()).collect::<Vec<_>>();

        let path_column = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let offset_column = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        for (i, (path, offset)) in expect_inputs.into_iter().enumerate() {
            assert_eq!(path_column.value(i), path);
            assert_eq!(offset_column.value(i), offset);
        }

        Ok(())
    }
}
