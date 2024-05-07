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

use arrow_schema::SchemaRef as ArrowSchemaRef;
use async_stream::try_stream;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::StreamExt;
use futures::TryFutureExt;
use parquet::arrow::async_reader::{AsyncFileReader, MetadataLoader};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask, PARQUET_FIELD_ID_META_KEY};
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::SchemaDescriptor;
use std::collections::HashMap;
use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;

use crate::arrow::arrow_schema_to_schema;
use crate::io::{FileIO, FileMetadata, FileRead};
use crate::scan::{ArrowRecordBatchStream, FileScanTaskStream};
use crate::spec::SchemaRef;
use crate::{Error, ErrorKind};

/// Builder to create ArrowReader
pub struct ArrowReaderBuilder {
    batch_size: Option<usize>,
    field_ids: Vec<usize>,
    file_io: FileIO,
    schema: SchemaRef,
}

impl ArrowReaderBuilder {
    /// Create a new ArrowReaderBuilder
    pub fn new(file_io: FileIO, schema: SchemaRef) -> Self {
        ArrowReaderBuilder {
            batch_size: None,
            field_ids: vec![],
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

    /// Sets the desired column projection with a list of field ids.
    pub fn with_field_ids(mut self, field_ids: impl IntoIterator<Item = usize>) -> Self {
        self.field_ids = field_ids.into_iter().collect();
        self
    }

    /// Build the ArrowReader.
    pub fn build(self) -> ArrowReader {
        ArrowReader {
            batch_size: self.batch_size,
            field_ids: self.field_ids,
            schema: self.schema,
            file_io: self.file_io,
        }
    }
}

/// Reads data from Parquet files
pub struct ArrowReader {
    batch_size: Option<usize>,
    field_ids: Vec<usize>,
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
                let parquet_file = file_io
                    .new_input(task.data().data_file().file_path())?;
                let parquet_metadata = parquet_file.metadata().await?;
                let parquet_reader =parquet_file
                    .reader()
                    .await?;
                let arrow_file_reader = ArrowFileReader::new(parquet_metadata, parquet_reader);

                let mut batch_stream_builder = ParquetRecordBatchStreamBuilder::new(arrow_file_reader)
                    .await?;

                let parquet_schema = batch_stream_builder.parquet_schema();
                let arrow_schema = batch_stream_builder.schema();
                let projection_mask = self.get_arrow_projection_mask(parquet_schema, arrow_schema)?;
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
        parquet_schema: &SchemaDescriptor,
        arrow_schema: &ArrowSchemaRef,
    ) -> crate::Result<ProjectionMask> {
        if self.field_ids.is_empty() {
            Ok(ProjectionMask::all())
        } else {
            // Build the map between field id and column index in Parquet schema.
            let mut column_map = HashMap::new();

            let fields = arrow_schema.fields();
            let iceberg_schema = arrow_schema_to_schema(arrow_schema)?;
            fields.filter_leaves(|idx, field| {
                let field_id = field.metadata().get(PARQUET_FIELD_ID_META_KEY);
                if field_id.is_none() {
                    return false;
                }

                let field_id = i32::from_str(field_id.unwrap());
                if field_id.is_err() {
                    return false;
                }
                let field_id = field_id.unwrap();

                if !self.field_ids.contains(&(field_id as usize)) {
                    return false;
                }

                let iceberg_field = self.schema.field_by_id(field_id);
                let parquet_iceberg_field = iceberg_schema.field_by_id(field_id);

                if iceberg_field.is_none() || parquet_iceberg_field.is_none() {
                    return false;
                }

                if iceberg_field.unwrap().field_type != parquet_iceberg_field.unwrap().field_type {
                    return false;
                }

                column_map.insert(field_id, idx);
                true
            });

            if column_map.len() != self.field_ids.len() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Parquet schema {} and Iceberg schema {} do not match.",
                        iceberg_schema, self.schema
                    ),
                ));
            }

            let mut indices = vec![];
            for field_id in &self.field_ids {
                if let Some(col_idx) = column_map.get(&(*field_id as i32)) {
                    indices.push(*col_idx);
                } else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Field {} is not found in Parquet schema.", field_id),
                    ));
                }
            }
            Ok(ProjectionMask::leaves(parquet_schema, indices))
        }
    }
}

/// ArrowFileReader is a wrapper around a FileRead that impls parquets AsyncFileReader.
///
/// # TODO
///
/// [ParquetObjectReader](https://docs.rs/parquet/latest/src/parquet/arrow/async_reader/store.rs.html#64) contains the following hints to speed up metadata loading, we can consider adding them to this struct:
///
/// - `metadata_size_hint`: Provide a hint as to the size of the parquet file's footer.
/// - `preload_column_index`: Load the Column Index  as part of [`Self::get_metadata`].
/// - `preload_offset_index`: Load the Offset Index as part of [`Self::get_metadata`].
struct ArrowFileReader<R: FileRead> {
    meta: FileMetadata,
    r: R,
}

impl<R: FileRead> ArrowFileReader<R> {
    /// Create a new ArrowFileReader
    fn new(meta: FileMetadata, r: R) -> Self {
        Self { meta, r }
    }
}

impl<R: FileRead> AsyncFileReader for ArrowFileReader<R> {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        Box::pin(
            self.r
                .read(range.start as _..range.end as _)
                .map_err(|err| parquet::errors::ParquetError::External(Box::new(err))),
        )
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let file_size = self.meta.size;
            let mut loader = MetadataLoader::load(self, file_size as usize, None).await?;
            loader.load_page_index(false, false).await?;
            Ok(Arc::new(loader.finish()))
        })
    }
}
