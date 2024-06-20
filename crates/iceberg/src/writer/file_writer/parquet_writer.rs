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

//! The module contains the file writer for parquet file format.

use std::pin::Pin;
use std::task::{Context, Poll};
use std::{
    collections::HashMap,
    sync::{atomic::AtomicI64, Arc},
};

use crate::{io::FileIO, io::FileWrite, Result};
use crate::{
    io::OutputFile,
    spec::{DataFileBuilder, DataFileFormat},
    writer::CurrentFileStatus,
    Error,
};
use arrow_schema::SchemaRef as ArrowSchemaRef;
use bytes::Bytes;
use futures::future::BoxFuture;
use parquet::{arrow::AsyncArrowWriter, format::FileMetaData};
use parquet::{arrow::PARQUET_FIELD_ID_META_KEY, file::properties::WriterProperties};

use super::{
    location_generator::{FileNameGenerator, LocationGenerator},
    track_writer::TrackWriter,
    FileWriter, FileWriterBuilder,
};

/// ParquetWriterBuilder is used to builder a [`ParquetWriter`]
#[derive(Clone)]
pub struct ParquetWriterBuilder<T: LocationGenerator, F: FileNameGenerator> {
    props: WriterProperties,
    schema: ArrowSchemaRef,

    file_io: FileIO,
    location_generator: T,
    file_name_generator: F,
}

impl<T: LocationGenerator, F: FileNameGenerator> ParquetWriterBuilder<T, F> {
    /// Create a new `ParquetWriterBuilder`
    /// To construct the write result, the schema should contain the `PARQUET_FIELD_ID_META_KEY` metadata for each field.
    pub fn new(
        props: WriterProperties,
        schema: ArrowSchemaRef,
        file_io: FileIO,
        location_generator: T,
        file_name_generator: F,
    ) -> Self {
        Self {
            props,
            schema,
            file_io,
            location_generator,
            file_name_generator,
        }
    }
}

impl<T: LocationGenerator, F: FileNameGenerator> FileWriterBuilder for ParquetWriterBuilder<T, F> {
    type R = ParquetWriter;

    async fn build(self) -> crate::Result<Self::R> {
        // Fetch field id from schema
        let field_ids = self
            .schema
            .fields()
            .iter()
            .map(|field| {
                field
                    .metadata()
                    .get(PARQUET_FIELD_ID_META_KEY)
                    .ok_or_else(|| {
                        Error::new(
                            crate::ErrorKind::Unexpected,
                            "Field id not found in arrow schema metadata.",
                        )
                    })?
                    .parse::<i32>()
                    .map_err(|err| {
                        Error::new(crate::ErrorKind::Unexpected, "Failed to parse field id.")
                            .with_source(err)
                    })
            })
            .collect::<crate::Result<Vec<_>>>()?;

        let written_size = Arc::new(AtomicI64::new(0));
        let out_file = self.file_io.new_output(
            self.location_generator
                .generate_location(&self.file_name_generator.generate_file_name()),
        )?;
        let inner_writer = TrackWriter::new(out_file.writer().await?, written_size.clone());
        let async_writer = AsyncFileWriter::new(inner_writer);
        let writer = AsyncArrowWriter::try_new(async_writer, self.schema.clone(), Some(self.props))
            .map_err(|err| {
                Error::new(
                    crate::ErrorKind::Unexpected,
                    "Failed to build parquet writer.",
                )
                .with_source(err)
            })?;

        Ok(ParquetWriter {
            writer,
            written_size,
            current_row_num: 0,
            out_file,
            field_ids,
        })
    }
}

/// `ParquetWriter`` is used to write arrow data into parquet file on storage.
pub struct ParquetWriter {
    out_file: OutputFile,
    writer: AsyncArrowWriter<AsyncFileWriter<TrackWriter>>,
    written_size: Arc<AtomicI64>,
    current_row_num: usize,
    field_ids: Vec<i32>,
}

impl ParquetWriter {
    fn to_data_file_builder(
        field_ids: &[i32],
        metadata: FileMetaData,
        written_size: usize,
        file_path: String,
    ) -> Result<DataFileBuilder> {
        // Only enter here when the file is not empty.
        assert!(!metadata.row_groups.is_empty());
        if field_ids.len() != metadata.row_groups[0].columns.len() {
            return Err(Error::new(
                crate::ErrorKind::Unexpected,
                "Len of field id is not match with len of columns in parquet metadata.",
            ));
        }

        let (column_sizes, value_counts, null_value_counts) =
            {
                let mut per_col_size: HashMap<i32, u64> = HashMap::new();
                let mut per_col_val_num: HashMap<i32, u64> = HashMap::new();
                let mut per_col_null_val_num: HashMap<i32, u64> = HashMap::new();
                metadata.row_groups.iter().for_each(|group| {
                    group.columns.iter().zip(field_ids.iter()).for_each(
                        |(column_chunk, &field_id)| {
                            if let Some(column_chunk_metadata) = &column_chunk.meta_data {
                                *per_col_size.entry(field_id).or_insert(0) +=
                                    column_chunk_metadata.total_compressed_size as u64;
                                *per_col_val_num.entry(field_id).or_insert(0) +=
                                    column_chunk_metadata.num_values as u64;
                                *per_col_null_val_num.entry(field_id).or_insert(0_u64) +=
                                    column_chunk_metadata
                                        .statistics
                                        .as_ref()
                                        .map(|s| s.null_count)
                                        .unwrap_or(None)
                                        .unwrap_or(0) as u64;
                            }
                        },
                    )
                });
                (per_col_size, per_col_val_num, per_col_null_val_num)
            };

        let mut builder = DataFileBuilder::default();
        builder
            .file_path(file_path)
            .file_format(DataFileFormat::Parquet)
            .record_count(metadata.num_rows as u64)
            .file_size_in_bytes(written_size as u64)
            .column_sizes(column_sizes)
            .value_counts(value_counts)
            .null_value_counts(null_value_counts)
            // # TODO
            // - nan_value_counts
            // - lower_bounds
            // - upper_bounds
            .key_metadata(metadata.footer_signing_key_metadata.unwrap_or_default())
            .split_offsets(
                metadata
                    .row_groups
                    .iter()
                    .filter_map(|group| group.file_offset)
                    .collect(),
            );
        Ok(builder)
    }
}

impl FileWriter for ParquetWriter {
    async fn write(&mut self, batch: &arrow_array::RecordBatch) -> crate::Result<()> {
        self.current_row_num += batch.num_rows();
        self.writer.write(batch).await.map_err(|err| {
            Error::new(
                crate::ErrorKind::Unexpected,
                "Failed to write using parquet writer.",
            )
            .with_source(err)
        })?;
        Ok(())
    }

    async fn close(self) -> crate::Result<Vec<crate::spec::DataFileBuilder>> {
        let metadata = self.writer.close().await.map_err(|err| {
            Error::new(
                crate::ErrorKind::Unexpected,
                "Failed to close parquet writer.",
            )
            .with_source(err)
        })?;

        let written_size = self.written_size.load(std::sync::atomic::Ordering::Relaxed);

        Ok(vec![Self::to_data_file_builder(
            &self.field_ids,
            metadata,
            written_size as usize,
            self.out_file.location().to_string(),
        )?])
    }
}

impl CurrentFileStatus for ParquetWriter {
    fn current_file_path(&self) -> String {
        self.out_file.location().to_string()
    }

    fn current_row_num(&self) -> usize {
        self.current_row_num
    }

    fn current_written_size(&self) -> usize {
        self.written_size.load(std::sync::atomic::Ordering::Relaxed) as usize
    }
}

/// AsyncFileWriter is a wrapper of FileWrite to make it compatible with tokio::io::AsyncWrite.
///
/// # NOTES
///
/// We keep this wrapper been used inside only.
///
/// # TODO
///
/// Maybe we can use the buffer from ArrowWriter directly.
struct AsyncFileWriter<W: FileWrite>(State<W>);

enum State<W: FileWrite> {
    Idle(Option<W>),
    Write(BoxFuture<'static, (W, Result<()>)>),
    Close(BoxFuture<'static, (W, Result<()>)>),
}

impl<W: FileWrite> AsyncFileWriter<W> {
    /// Create a new `AsyncFileWriter` with the given writer.
    pub fn new(writer: W) -> Self {
        Self(State::Idle(Some(writer)))
    }
}

impl<W: FileWrite> tokio::io::AsyncWrite for AsyncFileWriter<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, std::io::Error>> {
        let this = self.get_mut();
        loop {
            match &mut this.0 {
                State::Idle(w) => {
                    let mut writer = w.take().unwrap();
                    let bs = Bytes::copy_from_slice(buf);
                    let fut = async move {
                        let res = writer.write(bs).await;
                        (writer, res)
                    };
                    this.0 = State::Write(Box::pin(fut));
                }
                State::Write(fut) => {
                    let (writer, res) = futures::ready!(fut.as_mut().poll(cx));
                    this.0 = State::Idle(Some(writer));
                    return Poll::Ready(res.map(|_| buf.len()).map_err(|err| {
                        std::io::Error::new(std::io::ErrorKind::Other, Box::new(err))
                    }));
                }
                State::Close(_) => {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "file is closed",
                    )));
                }
            }
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), std::io::Error>> {
        let this = self.get_mut();
        loop {
            match &mut this.0 {
                State::Idle(w) => {
                    let mut writer = w.take().unwrap();
                    let fut = async move {
                        let res = writer.close().await;
                        (writer, res)
                    };
                    this.0 = State::Close(Box::pin(fut));
                }
                State::Write(_) => {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "file is writing",
                    )));
                }
                State::Close(fut) => {
                    let (writer, res) = futures::ready!(fut.as_mut().poll(cx));
                    this.0 = State::Idle(Some(writer));
                    return Poll::Ready(res.map_err(|err| {
                        std::io::Error::new(std::io::ErrorKind::Other, Box::new(err))
                    }));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use arrow_array::types::Int64Type;
    use arrow_array::ArrayRef;
    use arrow_array::Int64Array;
    use arrow_array::RecordBatch;
    use arrow_array::StructArray;
    use arrow_select::concat::concat_batches;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use tempfile::TempDir;

    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::Struct;
    use crate::writer::file_writer::location_generator::test::MockLocationGenerator;
    use crate::writer::file_writer::location_generator::DefaultFileNameGenerator;
    use crate::writer::tests::check_parquet_data_file;

    #[derive(Clone)]
    struct TestLocationGen;

    #[tokio::test]
    async fn test_parquet_writer() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let loccation_gen =
            MockLocationGenerator::new(temp_dir.path().to_str().unwrap().to_string());
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // prepare data
        let schema = {
            let fields = vec![
                arrow_schema::Field::new("col", arrow_schema::DataType::Int64, true).with_metadata(
                    HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "0".to_string())]),
                ),
            ];
            Arc::new(arrow_schema::Schema::new(fields))
        };
        let col = Arc::new(Int64Array::from_iter_values(vec![1; 1024])) as ArrayRef;
        let null_col = Arc::new(Int64Array::new_null(1024)) as ArrayRef;
        let to_write = RecordBatch::try_new(schema.clone(), vec![col]).unwrap();
        let to_write_null = RecordBatch::try_new(schema.clone(), vec![null_col]).unwrap();

        // write data
        let mut pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            to_write.schema(),
            file_io.clone(),
            loccation_gen,
            file_name_gen,
        )
        .build()
        .await?;
        pw.write(&to_write).await?;
        pw.write(&to_write_null).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            // Put dummy field for build successfully.
            .content(crate::spec::DataContentType::Data)
            .partition(Struct::empty())
            .build()
            .unwrap();

        // check the written file
        let expect_batch = concat_batches(&schema, vec![&to_write, &to_write_null]).unwrap();
        check_parquet_data_file(&file_io, &data_file, &expect_batch).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_parquet_writer_with_complex_schema() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen =
            MockLocationGenerator::new(temp_dir.path().to_str().unwrap().to_string());
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // prepare data
        // Int, Struct(Int), String, List(Int), Struct(Struct(Int))
        let schema = {
            let fields = vec![
                arrow_schema::Field::new("col0", arrow_schema::DataType::Int64, true)
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "0".to_string(),
                    )])),
                arrow_schema::Field::new(
                    "col1",
                    arrow_schema::DataType::Struct(
                        vec![arrow_schema::Field::new(
                            "sub_col",
                            arrow_schema::DataType::Int64,
                            true,
                        )
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "5".to_string(),
                        )]))]
                        .into(),
                    ),
                    true,
                )
                .with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "1".to_string(),
                )])),
                arrow_schema::Field::new("col2", arrow_schema::DataType::Utf8, true).with_metadata(
                    HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string())]),
                ),
                arrow_schema::Field::new(
                    "col3",
                    arrow_schema::DataType::List(Arc::new(
                        arrow_schema::Field::new("item", arrow_schema::DataType::Int64, true)
                            .with_metadata(HashMap::from([(
                                PARQUET_FIELD_ID_META_KEY.to_string(),
                                "6".to_string(),
                            )])),
                    )),
                    true,
                )
                .with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "3".to_string(),
                )])),
                arrow_schema::Field::new(
                    "col4",
                    arrow_schema::DataType::Struct(
                        vec![arrow_schema::Field::new(
                            "sub_col",
                            arrow_schema::DataType::Struct(
                                vec![arrow_schema::Field::new(
                                    "sub_sub_col",
                                    arrow_schema::DataType::Int64,
                                    true,
                                )
                                .with_metadata(HashMap::from([(
                                    PARQUET_FIELD_ID_META_KEY.to_string(),
                                    "7".to_string(),
                                )]))]
                                .into(),
                            ),
                            true,
                        )
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "8".to_string(),
                        )]))]
                        .into(),
                    ),
                    true,
                )
                .with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "4".to_string(),
                )])),
            ];
            Arc::new(arrow_schema::Schema::new(fields))
        };
        let col0 = Arc::new(Int64Array::from_iter_values(vec![1; 1024])) as ArrayRef;
        let col1 = Arc::new(StructArray::new(
            vec![
                arrow_schema::Field::new("sub_col", arrow_schema::DataType::Int64, true)
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "5".to_string(),
                    )])),
            ]
            .into(),
            vec![Arc::new(Int64Array::from_iter_values(vec![1; 1024]))],
            None,
        ));
        let col2 = Arc::new(arrow_array::StringArray::from_iter_values(vec![
            "test";
            1024
        ])) as ArrayRef;
        let col3 = Arc::new({
            let list_parts = arrow_array::ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                Some(
                    vec![Some(1),]
                );
                1024
            ])
            .into_parts();
            arrow_array::ListArray::new(
                Arc::new(list_parts.0.as_ref().clone().with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "6".to_string(),
                )]))),
                list_parts.1,
                list_parts.2,
                list_parts.3,
            )
        }) as ArrayRef;
        let col4 = Arc::new(StructArray::new(
            vec![arrow_schema::Field::new(
                "sub_col",
                arrow_schema::DataType::Struct(
                    vec![arrow_schema::Field::new(
                        "sub_sub_col",
                        arrow_schema::DataType::Int64,
                        true,
                    )
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "7".to_string(),
                    )]))]
                    .into(),
                ),
                true,
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "8".to_string(),
            )]))]
            .into(),
            vec![Arc::new(StructArray::new(
                vec![
                    arrow_schema::Field::new("sub_sub_col", arrow_schema::DataType::Int64, true)
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "7".to_string(),
                        )])),
                ]
                .into(),
                vec![Arc::new(Int64Array::from_iter_values(vec![1; 1024]))],
                None,
            ))],
            None,
        ));
        let to_write =
            RecordBatch::try_new(schema.clone(), vec![col0, col1, col2, col3, col4]).unwrap();

        // write data
        let mut pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            to_write.schema(),
            file_io.clone(),
            location_gen,
            file_name_gen,
        )
        .build()
        .await?;
        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            // Put dummy field for build successfully.
            .content(crate::spec::DataContentType::Data)
            .partition(Struct::empty())
            .build()
            .unwrap();

        // check the written file
        check_parquet_data_file(&file_io, &data_file, &to_write).await;

        Ok(())
    }
}
