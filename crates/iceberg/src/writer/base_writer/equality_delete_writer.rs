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

//! This module provide `EqualityDeleteWriter`.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StructArray};
use arrow_schema::{DataType, FieldRef, Fields, Schema, SchemaRef};
use itertools::Itertools;
use parquet::arrow::ProjectionMask;

use crate::spec::DataFile;
use crate::writer::file_writer::FileWriter;
use crate::writer::{file_writer::FileWriterBuilder, IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// Builder for `EqualityDeleteWriter`.
#[derive(Clone)]
pub struct EqualityDeleteFileWriterBuilder<B: FileWriterBuilder> {
    inner: B,
}

impl<B: FileWriterBuilder> EqualityDeleteFileWriterBuilder<B> {
    /// Create a new `EqualityDeleteFileWriterBuilder` using a `FileWriterBuilder`.
    pub fn new(inner: B) -> Self {
        Self { inner }
    }
}

/// Config for `EqualityDeleteWriter`.
pub struct EqualityDeleteWriterConfig {
    equality_ids: Vec<usize>,
    schema: SchemaRef,
    column_id_meta_key: String,
}

impl EqualityDeleteWriterConfig {
    /// Create a new `DataFileWriterConfig` with equality ids.
    pub fn new(equality_ids: Vec<usize>, schema: Schema, column_id_meta_key: &str) -> Self {
        Self {
            equality_ids,
            schema: schema.into(),
            column_id_meta_key: column_id_meta_key.to_string(),
        }
    }
}

#[async_trait::async_trait]
impl<B: FileWriterBuilder> IcebergWriterBuilder for EqualityDeleteFileWriterBuilder<B> {
    type R = EqualityDeleteFileWriter<B>;
    type C = EqualityDeleteWriterConfig;

    async fn build(self, config: Self::C) -> Result<Self::R> {
        let (projector, fields) = FieldProjector::new(
            config.schema.fields(),
            &config.equality_ids,
            &config.column_id_meta_key,
        )?;
        let delete_schema = Arc::new(arrow_schema::Schema::new(fields));
        Ok(EqualityDeleteFileWriter {
            inner_writer: Some(self.inner.clone().build().await?),
            projector,
            delete_schema: delete_schema,
            equality_ids: config.equality_ids,
        })
    }
}

/// A writer write data
pub struct EqualityDeleteFileWriter<B: FileWriterBuilder> {
    inner_writer: Option<B::R>,
    projector: FieldProjector,
    delete_schema: SchemaRef,
    equality_ids: Vec<usize>,
}

#[async_trait::async_trait]
impl<B: FileWriterBuilder> IcebergWriter for EqualityDeleteFileWriter<B> {
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let batch = RecordBatch::try_new(
            self.delete_schema.clone(),
            self.projector.project(batch.columns()),
        )
        .map_err(|err| Error::new(ErrorKind::DataInvalid, format!("{err}")))?;
        self.inner_writer.as_mut().unwrap().write(&batch).await
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        let writer = self.inner_writer.take().unwrap();
        Ok(writer
            .close()
            .await?
            .into_iter()
            .map(|mut res| {
                res.content(crate::spec::DataContentType::EqualityDeletes);
                res.equality_ids(self.equality_ids.iter().map(|id| *id as i32).collect_vec());
                res.build().expect("msg")
            })
            .collect_vec())
    }
}

/// Help to project specific field from `RecordBatch`` according to the column id.
pub struct FieldProjector {
    index_vec_vec: Vec<Vec<usize>>,
}

impl FieldProjector {
    /// Init FieldProjector
    pub fn new(
        batch_fields: &Fields,
        column_ids: &[usize],
        column_id_meta_key: &str,
    ) -> Result<(Self, Fields)> {
        let mut index_vec_vec = Vec::with_capacity(column_ids.len());
        let mut fields = Vec::with_capacity(column_ids.len());
        for &id in column_ids {
            let mut index_vec = vec![];
            if let Some(field) = Self::fetch_column_index(
                batch_fields,
                &mut index_vec,
                id as i64,
                column_id_meta_key,
            ) {
                fields.push(field.clone());
                index_vec_vec.push(index_vec);
            } else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Can't find source column id: {}", id),
                ));
            }
        }
        Ok((Self { index_vec_vec }, Fields::from_iter(fields)))
    }

    fn fetch_column_index(
        fields: &Fields,
        index_vec: &mut Vec<usize>,
        col_id: i64,
        column_id_meta_key: &str,
    ) -> Option<FieldRef> {
        for (pos, field) in fields.iter().enumerate() {
            let id: i64 = field
                .metadata()
                .get(column_id_meta_key)
                .expect("column_id must be set")
                .parse()
                .expect("column_id must can be parse as i64");
            if col_id == id {
                index_vec.push(pos);
                return Some(field.clone());
            }
            if let DataType::Struct(inner) = field.data_type() {
                let res = Self::fetch_column_index(inner, index_vec, col_id, column_id_meta_key);
                if !index_vec.is_empty() {
                    index_vec.push(pos);
                    return res;
                }
            }
        }
        None
    }
    /// Do projection with batch
    pub fn project(&self, batch: &[ArrayRef]) -> Vec<ArrayRef> {
        self.index_vec_vec
            .iter()
            .map(|index_vec| Self::get_column_by_index_vec(batch, index_vec))
            .collect_vec()
    }

    fn get_column_by_index_vec(batch: &[ArrayRef], index_vec: &[usize]) -> ArrayRef {
        let mut rev_iterator = index_vec.iter().rev();
        let mut array = batch[*rev_iterator.next().unwrap()].clone();
        for idx in rev_iterator {
            array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .column(*idx)
                .clone();
        }
        array
    }
}
