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

use crate::error::Result;
use arrow_arith::boolean::{and, is_not_null, is_null, not, or};
use arrow_array::{ArrayRef, BooleanArray, RecordBatch};
use arrow_ord::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow_schema::{ArrowError, DataType, SchemaRef as ArrowSchemaRef};
use async_stream::try_stream;
use bytes::Bytes;
use fnv::FnvHashSet;
use futures::future::BoxFuture;
use futures::stream::StreamExt;
use futures::{try_join, TryFutureExt};
use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter};
use parquet::arrow::async_reader::{AsyncFileReader, MetadataLoader};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask, PARQUET_FIELD_ID_META_KEY};
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::{SchemaDescriptor, Type as ParquetType};
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;

use crate::arrow::{arrow_schema_to_schema, get_arrow_datum};
use crate::expr::visitors::bound_predicate_visitor::{visit, BoundPredicateVisitor};
use crate::expr::{BoundPredicate, BoundReference};
use crate::io::{FileIO, FileMetadata, FileRead};
use crate::scan::{ArrowRecordBatchStream, FileScanTaskStream};
use crate::spec::{Datum, SchemaRef};
use crate::{Error, ErrorKind};

/// Builder to create ArrowReader
pub struct ArrowReaderBuilder {
    batch_size: Option<usize>,
    field_ids: Vec<usize>,
    file_io: FileIO,
    schema: SchemaRef,
    predicates: Option<BoundPredicate>,
}

impl ArrowReaderBuilder {
    /// Create a new ArrowReaderBuilder
    pub fn new(file_io: FileIO, schema: SchemaRef) -> Self {
        ArrowReaderBuilder {
            batch_size: None,
            field_ids: vec![],
            file_io,
            schema,
            predicates: None,
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

    /// Sets the predicates to apply to the scan.
    pub fn with_predicates(mut self, predicates: BoundPredicate) -> Self {
        self.predicates = Some(predicates);
        self
    }

    /// Build the ArrowReader.
    pub fn build(self) -> ArrowReader {
        ArrowReader {
            batch_size: self.batch_size,
            field_ids: self.field_ids,
            schema: self.schema,
            file_io: self.file_io,
            predicates: self.predicates,
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
    predicates: Option<BoundPredicate>,
}

impl ArrowReader {
    /// Take a stream of FileScanTasks and reads all the files.
    /// Returns a stream of Arrow RecordBatches containing the data from the files
    pub fn read(self, mut tasks: FileScanTaskStream) -> crate::Result<ArrowRecordBatchStream> {
        let file_io = self.file_io.clone();

        // Collect Parquet column indices from field ids
        let mut collector = CollectFieldIdVisitor {
            field_ids: HashSet::default(),
        };
        if let Some(predicates) = &self.predicates {
            visit(&mut collector, predicates)?;
        }

        Ok(try_stream! {
            while let Some(Ok(task)) = tasks.next().await {
                let parquet_file = file_io
                    .new_input(task.data_file_path())?;
                let (parquet_metadata, parquet_reader) = try_join!(parquet_file.metadata(), parquet_file.reader())?;
                let arrow_file_reader = ArrowFileReader::new(parquet_metadata, parquet_reader);

                let mut batch_stream_builder = ParquetRecordBatchStreamBuilder::new(arrow_file_reader)
                    .await?;

                let parquet_schema = batch_stream_builder.parquet_schema();
                let arrow_schema = batch_stream_builder.schema();
                let projection_mask = self.get_arrow_projection_mask(parquet_schema, arrow_schema)?;
                batch_stream_builder = batch_stream_builder.with_projection(projection_mask);

                let parquet_schema = batch_stream_builder.parquet_schema();
                let row_filter = self.get_row_filter(parquet_schema, &collector)?;

                if let Some(row_filter) = row_filter {
                    batch_stream_builder = batch_stream_builder.with_row_filter(row_filter);
                }

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

    fn get_row_filter(
        &self,
        parquet_schema: &SchemaDescriptor,
        collector: &CollectFieldIdVisitor,
    ) -> Result<Option<RowFilter>> {
        if let Some(predicates) = &self.predicates {
            let field_id_map = build_field_id_map(parquet_schema)?;

            // Collect Parquet column indices from field ids.
            // If the field id is not found in Parquet schema, it will be ignored due to schema evolution.
            let mut column_indices = collector
                .field_ids
                .iter()
                .filter_map(|field_id| field_id_map.get(field_id).cloned())
                .collect::<Vec<_>>();

            column_indices.sort();

            // The converter that converts `BoundPredicates` to `ArrowPredicates`
            let mut converter = PredicateConverter {
                parquet_schema,
                column_map: &field_id_map,
                column_indices: &column_indices,
            };

            // After collecting required leaf column indices used in the predicate,
            // creates the projection mask for the Arrow predicates.
            let projection_mask = ProjectionMask::leaves(parquet_schema, column_indices.clone());
            let predicate_func = visit(&mut converter, predicates)?;
            let arrow_predicate = ArrowPredicateFn::new(projection_mask, predicate_func);
            Ok(Some(RowFilter::new(vec![Box::new(arrow_predicate)])))
        } else {
            Ok(None)
        }
    }
}

/// Build the map of field id to Parquet column index in the schema.
fn build_field_id_map(parquet_schema: &SchemaDescriptor) -> Result<HashMap<i32, usize>> {
    let mut column_map = HashMap::new();
    for (idx, field) in parquet_schema.columns().iter().enumerate() {
        let field_type = field.self_type();
        match field_type {
            ParquetType::PrimitiveType { basic_info, .. } => {
                if !basic_info.has_id() {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Leave column idx: {}, name: {}, type {:?} in schema doesn't have field id",
                            idx,
                            basic_info.name(),
                            field_type
                        ),
                    ));
                }
                column_map.insert(basic_info.id(), idx);
            }
            ParquetType::GroupType { .. } => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Leave column in schema should be primitive type but got {:?}",
                        field_type
                    ),
                ));
            }
        };
    }

    Ok(column_map)
}

/// A visitor to collect field ids from bound predicates.
struct CollectFieldIdVisitor {
    field_ids: HashSet<i32>,
}

impl BoundPredicateVisitor for CollectFieldIdVisitor {
    type T = ();

    fn always_true(&mut self) -> Result<()> {
        Ok(())
    }

    fn always_false(&mut self) -> Result<()> {
        Ok(())
    }

    fn and(&mut self, _lhs: (), _rhs: ()) -> Result<()> {
        Ok(())
    }

    fn or(&mut self, _lhs: (), _rhs: ()) -> Result<()> {
        Ok(())
    }

    fn not(&mut self, _inner: ()) -> Result<()> {
        Ok(())
    }

    fn is_null(&mut self, reference: &BoundReference, _predicate: &BoundPredicate) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn not_null(&mut self, reference: &BoundReference, _predicate: &BoundPredicate) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn is_nan(&mut self, reference: &BoundReference, _predicate: &BoundPredicate) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn not_nan(&mut self, reference: &BoundReference, _predicate: &BoundPredicate) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn less_than(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn less_than_or_eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn greater_than(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn greater_than_or_eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn not_eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn starts_with(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn not_starts_with(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn r#in(
        &mut self,
        reference: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn not_in(
        &mut self,
        reference: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }
}

/// A visitor to convert Iceberg bound predicates to Arrow predicates.
struct PredicateConverter<'a> {
    /// The Parquet schema descriptor.
    pub parquet_schema: &'a SchemaDescriptor,
    /// The map between field id and leaf column index in Parquet schema.
    pub column_map: &'a HashMap<i32, usize>,
    /// The required column indices in Parquet schema for the predicates.
    pub column_indices: &'a Vec<usize>,
}

impl PredicateConverter<'_> {
    /// When visiting a bound reference, we return index of the leaf column in the
    /// required column indices which is used to project the column in the record batch.
    /// Return None if the field id is not found in the column map, which is possible
    /// due to schema evolution.
    fn bound_reference(&mut self, reference: &BoundReference) -> Result<Option<usize>> {
        // The leaf column's index in Parquet schema.
        if let Some(column_idx) = self.column_map.get(&reference.field().id) {
            if self.parquet_schema.get_column_root_idx(*column_idx) != *column_idx {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Leave column `{}` in predicates isn't a root column in Parquet schema.",
                        reference.field().name
                    ),
                ));
            }

            // The leaf column's index in the required column indices.
            let index = self
                .column_indices
                .iter()
                .position(|&idx| idx == *column_idx).ok_or(Error::new(ErrorKind::DataInvalid, format!(
                    "Leave column `{}` in predicates cannot be found in the required column indices.",
                    reference.field().name
                )))?;

            Ok(Some(index))
        } else {
            Ok(None)
        }
    }

    /// Build an Arrow predicate that always returns true.
    fn build_always_true(&self) -> Result<Box<PredicateResult>> {
        Ok(Box::new(|batch| {
            Ok(BooleanArray::from(vec![true; batch.num_rows()]))
        }))
    }

    /// Build an Arrow predicate that always returns false.
    fn build_always_false(&self) -> Result<Box<PredicateResult>> {
        Ok(Box::new(|batch| {
            Ok(BooleanArray::from(vec![false; batch.num_rows()]))
        }))
    }
}

/// Gets the leaf column from the record batch for the required column index. Only
/// supports top-level columns for now.
fn project_column(
    batch: &RecordBatch,
    column_idx: usize,
) -> std::result::Result<ArrayRef, ArrowError> {
    let column = batch.column(column_idx);

    match column.data_type() {
        DataType::Struct(_) => Err(ArrowError::SchemaError(
            "Does not support struct column yet.".to_string(),
        )),
        _ => Ok(column.clone()),
    }
}

type PredicateResult =
    dyn FnMut(RecordBatch) -> std::result::Result<BooleanArray, ArrowError> + Send + 'static;

impl<'a> BoundPredicateVisitor for PredicateConverter<'a> {
    type T = Box<PredicateResult>;

    fn always_true(&mut self) -> Result<Box<PredicateResult>> {
        self.build_always_true()
    }

    fn always_false(&mut self) -> Result<Box<PredicateResult>> {
        self.build_always_false()
    }

    fn and(
        &mut self,
        mut lhs: Box<PredicateResult>,
        mut rhs: Box<PredicateResult>,
    ) -> Result<Box<PredicateResult>> {
        Ok(Box::new(move |batch| {
            let left = lhs(batch.clone())?;
            let right = rhs(batch)?;
            and(&left, &right)
        }))
    }

    fn or(
        &mut self,
        mut lhs: Box<PredicateResult>,
        mut rhs: Box<PredicateResult>,
    ) -> Result<Box<PredicateResult>> {
        Ok(Box::new(move |batch| {
            let left = lhs(batch.clone())?;
            let right = rhs(batch)?;
            or(&left, &right)
        }))
    }

    fn not(&mut self, mut inner: Box<PredicateResult>) -> Result<Box<PredicateResult>> {
        Ok(Box::new(move |batch| {
            let pred_ret = inner(batch)?;
            not(&pred_ret)
        }))
    }

    fn is_null(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            Ok(Box::new(move |batch| {
                let column = project_column(&batch, idx)?;
                is_null(&column)
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_true()
        }
    }

    fn not_null(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            Ok(Box::new(move |batch| {
                let column = project_column(&batch, idx)?;
                is_not_null(&column)
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn is_nan(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if self.bound_reference(reference)?.is_some() {
            self.build_always_true()
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn not_nan(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if self.bound_reference(reference)?.is_some() {
            self.build_always_false()
        } else {
            // A missing column, treating it as null.
            self.build_always_true()
        }
    }

    fn less_than(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, idx)?;
                lt(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_true()
        }
    }

    fn less_than_or_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, idx)?;
                lt_eq(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_true()
        }
    }

    fn greater_than(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, idx)?;
                gt(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn greater_than_or_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, idx)?;
                gt_eq(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, idx)?;
                eq(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn not_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, idx)?;
                neq(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn starts_with(
        &mut self,
        _reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        // TODO: Implement starts_with
        self.build_always_true()
    }

    fn not_starts_with(
        &mut self,
        _reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        // TODO: Implement not_starts_with
        self.build_always_true()
    }

    fn r#in(
        &mut self,
        _reference: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        // TODO: Implement in
        self.build_always_true()
    }

    fn not_in(
        &mut self,
        _reference: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        // TODO: Implement not_in
        self.build_always_true()
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

#[cfg(test)]
mod tests {
    use crate::arrow::reader::CollectFieldIdVisitor;
    use crate::expr::visitors::bound_predicate_visitor::visit;
    use crate::expr::{Bind, Reference};
    use crate::spec::{NestedField, PrimitiveType, Schema, SchemaRef, Type};
    use std::collections::HashSet;
    use std::sync::Arc;

    fn table_schema_simple() -> SchemaRef {
        Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_identifier_field_ids(vec![2])
                .with_fields(vec![
                    NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
                    NestedField::optional(4, "qux", Type::Primitive(PrimitiveType::Float)).into(),
                ])
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn test_collect_field_id() {
        let schema = table_schema_simple();
        let expr = Reference::new("qux").is_null();
        let bound_expr = expr.bind(schema, true).unwrap();

        let mut visitor = CollectFieldIdVisitor {
            field_ids: HashSet::default(),
        };
        visit(&mut visitor, &bound_expr).unwrap();

        let mut expected = HashSet::default();
        expected.insert(4_i32);

        assert_eq!(visitor.field_ids, expected);
    }

    #[test]
    fn test_collect_field_id_with_and() {
        let schema = table_schema_simple();
        let expr = Reference::new("qux")
            .is_null()
            .and(Reference::new("baz").is_null());
        let bound_expr = expr.bind(schema, true).unwrap();

        let mut visitor = CollectFieldIdVisitor {
            field_ids: HashSet::default(),
        };
        visit(&mut visitor, &bound_expr).unwrap();

        let mut expected = HashSet::default();
        expected.insert(4_i32);
        expected.insert(3);

        assert_eq!(visitor.field_ids, expected);
    }

    #[test]
    fn test_collect_field_id_with_or() {
        let schema = table_schema_simple();
        let expr = Reference::new("qux")
            .is_null()
            .or(Reference::new("baz").is_null());
        let bound_expr = expr.bind(schema, true).unwrap();

        let mut visitor = CollectFieldIdVisitor {
            field_ids: HashSet::default(),
        };
        visit(&mut visitor, &bound_expr).unwrap();

        let mut expected = HashSet::default();
        expected.insert(4_i32);
        expected.insert(3);

        assert_eq!(visitor.field_ids, expected);
    }
}
