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
    ArrayRef, BooleanArray, Datum as ArrowDatum, Float32Array, Float64Array, Int32Array,
    Int64Array, StructArray,
};
use arrow_ord::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow_schema::{ArrowError, DataType, SchemaRef as ArrowSchemaRef};
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
    visit_predicate, BinaryExpression, BoundPredicate, BoundPredicateVisitor, BoundReference,
    PredicateOperator, SetExpression, UnaryExpression,
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
            let mut collector = CollectFieldIdVisitor { field_ids: vec![] };
            visit_predicate(&mut collector, predicates).unwrap();
            let column_indices = collector
                .field_ids
                .iter()
                .map(|field_id| {
                    field_id_map.get(field_id).cloned().ok_or_else(|| {
                        Error::new(ErrorKind::DataInvalid, "Field id not found in schema")
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            // Convert BoundPredicates to ArrowPredicates
            let mut converter = PredicateConverter {
                columns: &column_indices,
                projection_mask: ProjectionMask::leaves(parquet_schema, column_indices.clone()),
                parquet_schema,
                column_map: &field_id_map,
            };
            let arrow_predicate = visit_predicate(&mut converter, predicates)?;
            Ok(Some(RowFilter::new(vec![arrow_predicate])))
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

/// A visitor to convert Iceberg bound predicates to Arrow predicates.
struct PredicateConverter<'a> {
    /// The leaf column indices used in the predicates.
    pub columns: &'a Vec<usize>,
    /// The projection mask for the Arrow predicates.
    pub projection_mask: ProjectionMask,
    /// The Parquet schema descriptor.
    pub parquet_schema: &'a SchemaDescriptor,
    /// The map between field id and leaf column index in Parquet schema.
    pub column_map: &'a HashMap<i32, usize>,
}

fn get_arrow_datum(datum: &Datum) -> Result<Box<dyn ArrowDatum + Send>> {
    match datum.literal() {
        PrimitiveLiteral::Boolean(value) => Ok(Box::new(BooleanArray::new_scalar(*value))),
        PrimitiveLiteral::Int(value) => Ok(Box::new(Int32Array::new_scalar(*value))),
        PrimitiveLiteral::Long(value) => Ok(Box::new(Int64Array::new_scalar(*value))),
        PrimitiveLiteral::Float(value) => Ok(Box::new(Float32Array::new_scalar(value.as_f32()))),
        PrimitiveLiteral::Double(value) => Ok(Box::new(Float64Array::new_scalar(value.as_f64()))),
        l => Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Unsupported literal type: {:?}", l),
        )),
    }
}

/// Recursively get the leaf column from the record batch. Assume that the nested columns in
/// struct is projected to a single column.
fn get_leaf_column(column: &ArrayRef) -> std::result::Result<ArrayRef, ArrowError> {
    match column.data_type() {
        DataType::Struct(fields) => {
            if fields.len() != 1 {
                return Err(ArrowError::SchemaError(
                    "Struct column should have only one field after projection"
                        .parse()
                        .unwrap(),
                ));
            }
            let struct_array = column.as_any().downcast_ref::<StructArray>().unwrap();
            get_leaf_column(struct_array.column(0))
        }
        _ => Ok(column.clone()),
    }
}

impl<'a> BoundPredicateVisitor for PredicateConverter<'a> {
    type T = Box<dyn ArrowPredicate>;
    type U = ProjectionMask;

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
        let projected_mask = self.bound_reference(predicate.term())?;

        match predicate.op() {
            PredicateOperator::IsNull => Ok(Box::new(ArrowPredicateFn::new(
                projected_mask,
                move |batch| {
                    let column = get_leaf_column(batch.column(0))?;
                    is_null(&column)
                },
            ))),
            PredicateOperator::NotNull => Ok(Box::new(ArrowPredicateFn::new(
                projected_mask,
                move |batch| {
                    let column = get_leaf_column(batch.column(0))?;
                    is_not_null(&column)
                },
            ))),
            // Unsupported operators, return always true.
            _ => Ok(Box::new(ArrowPredicateFn::new(projected_mask, |batch| {
                Ok(BooleanArray::from(vec![true; batch.num_rows()]))
            }))),
        }
    }

    fn visit_binary(&mut self, predicate: &BinaryExpression<BoundReference>) -> Result<Self::T> {
        let projected_mask = self.bound_reference(predicate.term())?;
        let literal = predicate.literal().clone();
        let literal = get_arrow_datum(&literal)?;

        match predicate.op() {
            PredicateOperator::LessThan => Ok(Box::new(ArrowPredicateFn::new(
                projected_mask,
                move |batch| {
                    let left = get_leaf_column(batch.column(0))?;
                    lt(&left, literal.as_ref())
                },
            ))),
            PredicateOperator::LessThanOrEq => Ok(Box::new(ArrowPredicateFn::new(
                projected_mask,
                move |batch| {
                    let left = get_leaf_column(batch.column(0))?;
                    lt_eq(&left, literal.as_ref())
                },
            ))),
            PredicateOperator::GreaterThan => Ok(Box::new(ArrowPredicateFn::new(
                projected_mask,
                move |batch| {
                    let left = get_leaf_column(batch.column(0))?;
                    gt(&left, literal.as_ref())
                },
            ))),
            PredicateOperator::GreaterThanOrEq => Ok(Box::new(ArrowPredicateFn::new(
                projected_mask,
                move |batch| {
                    let left = get_leaf_column(batch.column(0))?;
                    gt_eq(&left, literal.as_ref())
                },
            ))),
            PredicateOperator::Eq => Ok(Box::new(ArrowPredicateFn::new(
                projected_mask,
                move |batch| {
                    let left = get_leaf_column(batch.column(0))?;
                    eq(&left, literal.as_ref())
                },
            ))),
            PredicateOperator::NotEq => Ok(Box::new(ArrowPredicateFn::new(
                projected_mask,
                move |batch| {
                    let left = get_leaf_column(batch.column(0))?;
                    neq(&left, literal.as_ref())
                },
            ))),
            // Unsupported operators, return always true.
            _ => Ok(Box::new(ArrowPredicateFn::new(
                self.projection_mask.clone(),
                |batch| Ok(BooleanArray::from(vec![true; batch.num_rows()])),
            ))),
        }
    }

    fn visit_set(&mut self, predicate: &SetExpression<BoundReference>) -> Result<Self::T> {
        #[allow(clippy::match_single_binding)]
        match predicate.op() {
            // Unsupported operators, return always true.
            _ => Ok(Box::new(ArrowPredicateFn::new(
                self.projection_mask.clone(),
                |batch| Ok(BooleanArray::from(vec![true; batch.num_rows()])),
            ))),
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

    /// When visiting a bound reference, we return the projection mask for the leaf column
    /// which is used to project the column in the record batch.
    fn bound_reference(&mut self, reference: &BoundReference) -> Result<Self::U> {
        // The leaf column's index in Parquet schema.
        let column_idx = self.column_map.get(&reference.field().id).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Field id {} not found in schema", reference.field().id),
            )
        })?;

        // Find the column index in projection mask.
        let column_idx = self
            .columns
            .iter()
            .position(|&x| x == *column_idx)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Column index {} not found in schema", *column_idx),
                )
            })?;

        Ok(ProjectionMask::leaves(
            self.parquet_schema,
            vec![self.columns[column_idx]],
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::arrow::reader::CollectFieldIdVisitor;
    use crate::expr::{visit_predicate, Bind, Reference};
    use crate::spec::{NestedField, PrimitiveType, Schema, SchemaRef, Type};
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

        let mut visitor = CollectFieldIdVisitor { field_ids: vec![] };
        visit_predicate(&mut visitor, &bound_expr).unwrap();

        assert_eq!(visitor.field_ids, vec![4]);
    }

    #[test]
    fn test_collect_field_id_with_and() {
        let schema = table_schema_simple();
        let expr = Reference::new("qux")
            .is_null()
            .and(Reference::new("baz").is_null());
        let bound_expr = expr.bind(schema, true).unwrap();

        let mut visitor = CollectFieldIdVisitor { field_ids: vec![] };
        visit_predicate(&mut visitor, &bound_expr).unwrap();

        assert_eq!(visitor.field_ids, vec![4, 3]);
    }

    #[test]
    fn test_collect_field_id_with_or() {
        let schema = table_schema_simple();
        let expr = Reference::new("qux")
            .is_null()
            .or(Reference::new("baz").is_null());
        let bound_expr = expr.bind(schema, true).unwrap();

        let mut visitor = CollectFieldIdVisitor { field_ids: vec![] };
        visit_predicate(&mut visitor, &bound_expr).unwrap();

        assert_eq!(visitor.field_ids, vec![4, 3]);
    }
}
