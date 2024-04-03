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
use arrow_array::{
    BooleanArray, Datum as ArrowDatum, Float32Array, Float64Array, Int32Array, Int64Array,
};
use arrow_ord::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow_schema::SchemaRef as ArrowSchemaRef;
use async_stream::try_stream;
use bitvec::macros::internal::funty::Fundamental;
use futures::stream::StreamExt;
use parquet::arrow::arrow_reader::{ArrowPredicate, ArrowPredicateFn, RowFilter};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask, PARQUET_FIELD_ID_META_KEY};
use parquet::schema::types::{SchemaDescriptor, Type as ParquetType};
use std::collections::HashMap;
use std::str::FromStr;

use crate::arrow::arrow_schema_to_schema;
use crate::expr::{
    BinaryExpression, BoundPredicate, BoundReference, PredicateOperator, SetExpression,
    UnaryExpression,
};
use crate::io::FileIO;
use crate::scan::{ArrowRecordBatchStream, FileScanTaskStream};
use crate::spec::{Datum, PrimitiveLiteral, SchemaRef};
use crate::{Error, ErrorKind};

/// Builder to create ArrowReader
pub struct ArrowReaderBuilder {
    batch_size: Option<usize>,
    field_ids: Vec<usize>,
    file_io: FileIO,
    schema: SchemaRef,
    predicates: Option<Vec<BoundPredicate>>,
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
    pub fn with_predicates(mut self, predicates: Vec<BoundPredicate>) -> Self {
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
    predicates: Option<Vec<BoundPredicate>>,
}

impl ArrowReader {
    /// Take a stream of FileScanTasks and reads all the files.
    /// Returns a stream of Arrow RecordBatches containing the data from the files
    pub fn read(self, mut tasks: FileScanTaskStream) -> crate::Result<ArrowRecordBatchStream> {
        let file_io = self.file_io.clone();

        Ok(try_stream! {
            while let Some(Ok(task)) = tasks.next().await {
                let parquet_reader = file_io
                    .new_input(task.data().data_file().file_path())?
                    .reader()
                    .await?;

                let mut batch_stream_builder = ParquetRecordBatchStreamBuilder::new(parquet_reader)
                    .await?;

                let parquet_schema = batch_stream_builder.parquet_schema();
                let arrow_schema = batch_stream_builder.schema();
                let projection_mask = self.get_arrow_projection_mask(parquet_schema, arrow_schema)?;
                batch_stream_builder = batch_stream_builder.with_projection(projection_mask);

                let parquet_schema = batch_stream_builder.parquet_schema();
                let row_filter = self.get_row_filter(parquet_schema)?;

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

    fn get_row_filter(&self, parquet_schema: &SchemaDescriptor) -> Result<Option<RowFilter>> {
        if let Some(predicates) = &self.predicates {
            let field_id_map = self.build_field_id_map(parquet_schema)?;

            // Collect Parquet column indices from field ids
            let column_indices = predicates
                .iter()
                .map(|predicate| {
                    let mut collector = CollectFieldIdVisitor { field_ids: vec![] };
                    collector.visit_predicate(predicate).unwrap();
                    collector
                        .field_ids
                        .iter()
                        .map(|field_id| {
                            field_id_map.get(field_id).cloned().ok_or_else(|| {
                                Error::new(ErrorKind::DataInvalid, "Field id not found in schema")
                            })
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;

            // Convert BoundPredicates to ArrowPredicates
            let mut arrow_predicates = vec![];
            for (predicate, columns) in predicates.iter().zip(column_indices.iter()) {
                let mut converter = PredicateConverter {
                    columns,
                    projection_mask: ProjectionMask::leaves(parquet_schema, columns.clone()),
                    parquet_schema,
                    column_map: &field_id_map,
                };
                let arrow_predicate = converter.visit_predicate(predicate)?;
                arrow_predicates.push(arrow_predicate);
            }
            Ok(Some(RowFilter::new(arrow_predicates)))
        } else {
            Ok(None)
        }
    }

    /// Build the map of field id to Parquet column index in the schema.
    fn build_field_id_map(&self, parquet_schema: &SchemaDescriptor) -> Result<HashMap<i32, usize>> {
        let mut column_map = HashMap::new();
        for (idx, field) in parquet_schema.columns().iter().enumerate() {
            let field_type = field.self_type();
            match field_type {
                ParquetType::PrimitiveType { basic_info, .. } => {
                    if !basic_info.has_id() {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "Leave column {:?} in schema doesn't have field id",
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
}

/// A visitor to collect field ids from bound predicates.
struct CollectFieldIdVisitor {
    field_ids: Vec<i32>,
}

impl BoundPredicateVisitor for CollectFieldIdVisitor {
    type T = ();
    type U = ();

    fn and(&mut self, _predicates: Vec<Self::T>) -> Result<Self::T> {
        Ok(())
    }

    fn or(&mut self, _predicates: Vec<Self::T>) -> Result<Self::T> {
        Ok(())
    }

    fn not(&mut self, _predicate: Self::T) -> Result<Self::T> {
        Ok(())
    }

    fn visit_always_true(&mut self) -> Result<Self::T> {
        Ok(())
    }

    fn visit_always_false(&mut self) -> Result<Self::T> {
        Ok(())
    }

    fn visit_unary(&mut self, predicate: &UnaryExpression<BoundReference>) -> Result<Self::T> {
        self.bound_reference(predicate.term())?;
        Ok(())
    }

    fn visit_binary(&mut self, predicate: &BinaryExpression<BoundReference>) -> Result<Self::T> {
        self.bound_reference(predicate.term())?;
        Ok(())
    }

    fn visit_set(&mut self, predicate: &SetExpression<BoundReference>) -> Result<Self::T> {
        self.bound_reference(predicate.term())?;
        Ok(())
    }

    fn bound_reference(&mut self, reference: &BoundReference) -> Result<Self::T> {
        self.field_ids.push(reference.field().id);
        Ok(())
    }
}

struct PredicateConverter<'a> {
    pub columns: &'a Vec<usize>,
    pub projection_mask: ProjectionMask,
    pub parquet_schema: &'a SchemaDescriptor,
    pub column_map: &'a HashMap<i32, usize>,
}

fn get_arrow_datum(datum: &Datum) -> Box<dyn ArrowDatum> {
    match datum.literal() {
        PrimitiveLiteral::Boolean(value) => Box::new(BooleanArray::new_scalar(*value)),
        PrimitiveLiteral::Int(value) => Box::new(Int32Array::new_scalar(*value)),
        PrimitiveLiteral::Long(value) => Box::new(Int64Array::new_scalar(*value)),
        PrimitiveLiteral::Float(value) => Box::new(Float32Array::new_scalar(value.as_f32())),
        PrimitiveLiteral::Double(value) => Box::new(Float64Array::new_scalar(value.as_f64())),
        _ => todo!("Unsupported literal type"),
    }
}

impl<'a> BoundPredicateVisitor for PredicateConverter<'a> {
    type T = Box<dyn ArrowPredicate>;
    type U = usize;

    fn visit_always_true(&mut self) -> Result<Self::T> {
        Ok(Box::new(ArrowPredicateFn::new(
            self.projection_mask.clone(),
            |batch| Ok(BooleanArray::from(vec![true; batch.num_rows()])),
        )))
    }

    fn visit_always_false(&mut self) -> Result<Self::T> {
        Ok(Box::new(ArrowPredicateFn::new(
            self.projection_mask.clone(),
            |batch| Ok(BooleanArray::from(vec![false; batch.num_rows()])),
        )))
    }

    fn visit_unary(&mut self, predicate: &UnaryExpression<BoundReference>) -> Result<Self::T> {
        let term_index = self.bound_reference(predicate.term())?;

        match predicate.op() {
            PredicateOperator::IsNull => Ok(Box::new(ArrowPredicateFn::new(
                self.projection_mask.clone(),
                move |batch| {
                    let column = batch.column(term_index);
                    is_null(column)
                },
            ))),
            PredicateOperator::NotNull => Ok(Box::new(ArrowPredicateFn::new(
                self.projection_mask.clone(),
                move |batch| {
                    let column = batch.column(term_index);
                    is_not_null(column)
                },
            ))),
            PredicateOperator::IsNan => {
                todo!("IsNan is not supported yet")
            }
            PredicateOperator::NotNan => {
                todo!("NotNan is not supported yet")
            }
            op => Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Unsupported unary operator: {op}"),
            )),
        }
    }

    fn visit_binary(&mut self, predicate: &BinaryExpression<BoundReference>) -> Result<Self::T> {
        let term_index = self.bound_reference(predicate.term())?;
        let literal = predicate.literal().clone();

        match predicate.op() {
            PredicateOperator::LessThan => Ok(Box::new(ArrowPredicateFn::new(
                self.projection_mask.clone(),
                move |batch| {
                    let left = batch.column(term_index);
                    let literal = get_arrow_datum(&literal);
                    lt(left, literal.as_ref())
                },
            ))),
            PredicateOperator::LessThanOrEq => Ok(Box::new(ArrowPredicateFn::new(
                self.projection_mask.clone(),
                move |batch| {
                    let left = batch.column(term_index);
                    let literal = get_arrow_datum(&literal);
                    lt_eq(left, literal.as_ref())
                },
            ))),
            PredicateOperator::GreaterThan => Ok(Box::new(ArrowPredicateFn::new(
                self.projection_mask.clone(),
                move |batch| {
                    let left = batch.column(term_index);
                    let literal = get_arrow_datum(&literal);
                    gt(left, literal.as_ref())
                },
            ))),
            PredicateOperator::GreaterThanOrEq => Ok(Box::new(ArrowPredicateFn::new(
                self.projection_mask.clone(),
                move |batch| {
                    let left = batch.column(term_index);
                    let literal = get_arrow_datum(&literal);
                    gt_eq(left, literal.as_ref())
                },
            ))),
            PredicateOperator::Eq => Ok(Box::new(ArrowPredicateFn::new(
                self.projection_mask.clone(),
                move |batch| {
                    let left = batch.column(term_index);
                    let literal = get_arrow_datum(&literal);
                    eq(left, literal.as_ref())
                },
            ))),
            PredicateOperator::NotEq => Ok(Box::new(ArrowPredicateFn::new(
                self.projection_mask.clone(),
                move |batch| {
                    let left = batch.column(term_index);
                    let literal = get_arrow_datum(&literal);
                    neq(left, literal.as_ref())
                },
            ))),
            op => Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Unsupported binary operator: {op}"),
            )),
        }
    }

    fn visit_set(&mut self, predicate: &SetExpression<BoundReference>) -> Result<Self::T> {
        match predicate.op() {
            PredicateOperator::In => {
                todo!("In is not supported yet")
            }
            PredicateOperator::NotIn => {
                todo!("NotIn is not supported yet")
            }
            op => Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Unsupported set operator: {op}"),
            )),
        }
    }

    fn and(&mut self, mut predicates: Vec<Self::T>) -> Result<Self::T> {
        Ok(Box::new(ArrowPredicateFn::new(
            self.projection_mask.clone(),
            move |batch| {
                let left = predicates.get_mut(0).unwrap().evaluate(batch.clone())?;
                let right = predicates.get_mut(1).unwrap().evaluate(batch)?;
                and(&left, &right)
            },
        )))
    }

    fn or(&mut self, mut predicates: Vec<Self::T>) -> Result<Self::T> {
        Ok(Box::new(ArrowPredicateFn::new(
            self.projection_mask.clone(),
            move |batch| {
                let left = predicates.get_mut(0).unwrap().evaluate(batch.clone())?;
                let right = predicates.get_mut(1).unwrap().evaluate(batch)?;
                or(&left, &right)
            },
        )))
    }

    fn not(&mut self, mut predicate: Self::T) -> Result<Self::T> {
        Ok(Box::new(ArrowPredicateFn::new(
            self.projection_mask.clone(),
            move |batch| {
                let evaluated = predicate.evaluate(batch.clone())?;
                not(&evaluated)
            },
        )))
    }

    fn bound_reference(&mut self, reference: &BoundReference) -> Result<Self::U> {
        let column_idx = self.column_map.get(&reference.field().id).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Field id {} not found in schema", reference.field().id),
            )
        })?;

        let root_col_index = self.parquet_schema.get_column_root_idx(*column_idx);

        // Find the column index in projection mask.
        let column_idx = self
            .columns
            .iter()
            .position(|&x| x == root_col_index)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Column index {} not found in schema", root_col_index),
                )
            })?;

        Ok(column_idx)
    }
}

/// A visitor for bound predicates.
pub trait BoundPredicateVisitor {
    /// Return type of this visitor on bound predicate.
    type T;

    /// Return type of this visitor on bound reference.
    type U;

    /// Visit a bound predicate.
    fn visit_predicate(&mut self, predicate: &BoundPredicate) -> Result<Self::T> {
        match predicate {
            BoundPredicate::And(predicates) => self.visit_and(predicates.inputs()),
            BoundPredicate::Or(predicates) => self.visit_or(predicates.inputs()),
            BoundPredicate::Not(predicate) => self.visit_not(predicate.inputs()),
            BoundPredicate::AlwaysTrue => self.visit_always_true(),
            BoundPredicate::AlwaysFalse => self.visit_always_false(),
            BoundPredicate::Unary(unary) => self.visit_unary(unary),
            BoundPredicate::Binary(binary) => self.visit_binary(binary),
            BoundPredicate::Set(set) => self.visit_set(set),
        }
    }

    /// Visit an AND predicate.
    fn visit_and(&mut self, predicates: [&BoundPredicate; 2]) -> Result<Self::T> {
        let mut results = Vec::with_capacity(predicates.len());
        for predicate in predicates {
            let result = self.visit_predicate(predicate)?;
            results.push(result);
        }
        self.and(results)
    }

    /// Visit an OR predicate.
    fn visit_or(&mut self, predicates: [&BoundPredicate; 2]) -> Result<Self::T> {
        let mut results = Vec::with_capacity(predicates.len());
        for predicate in predicates {
            let result = self.visit_predicate(predicate)?;
            results.push(result);
        }
        self.or(results)
    }

    /// Visit a NOT predicate.
    fn visit_not(&mut self, predicate: [&BoundPredicate; 1]) -> Result<Self::T> {
        let result = self.visit_predicate(predicate.first().unwrap())?;
        self.not(result)
    }

    /// Visit an always true predicate.
    fn visit_always_true(&mut self) -> Result<Self::T>;

    /// Visit an always false predicate.
    fn visit_always_false(&mut self) -> Result<Self::T>;

    /// Visit a unary predicate.
    fn visit_unary(&mut self, predicate: &UnaryExpression<BoundReference>) -> Result<Self::T>;

    /// Visit a binary predicate.
    fn visit_binary(&mut self, predicate: &BinaryExpression<BoundReference>) -> Result<Self::T>;

    /// Visit a set predicate.
    fn visit_set(&mut self, predicate: &SetExpression<BoundReference>) -> Result<Self::T>;

    /// Called after visiting predicates of AND.
    fn and(&mut self, predicates: Vec<Self::T>) -> Result<Self::T>;

    /// Called after visiting predicates of OR.
    fn or(&mut self, predicates: Vec<Self::T>) -> Result<Self::T>;

    /// Called after visiting predicates of NOT.
    fn not(&mut self, predicate: Self::T) -> Result<Self::T>;

    /// Visit a bound reference.
    fn bound_reference(&mut self, reference: &BoundReference) -> Result<Self::U>;
}
