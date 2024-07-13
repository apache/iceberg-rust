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

use fnv::FnvHashSet;

use super::bound_predicate_visitor::{visit, BoundPredicateVisitor};
use crate::expr::PartitionBoundPredicate;
use crate::{
    expr::{BoundPredicate, BoundReference},
    spec::{DataFile, Datum, PrimitiveLiteral, Struct},
    Error, ErrorKind, Result,
};

/// Evaluates a [`DataFile`]'s partition [`Struct`] to check
/// if the partition tuples match the given [`BoundPredicate`].
///
/// Use within [`TableScan`] to prune the list of [`DataFile`]s
/// that could potentially match the TableScan's filter.
#[derive(Debug)]
pub(crate) struct ExpressionEvaluator {
    /// The provided partition filter.
    partition_filter: PartitionBoundPredicate,
}

impl ExpressionEvaluator {
    /// Creates a new [`ExpressionEvaluator`].
    pub(crate) fn new(partition_filter: PartitionBoundPredicate) -> Self {
        Self { partition_filter }
    }

    /// Evaluate this [`ExpressionEvaluator`]'s partition filter against
    /// the provided [`DataFile`]'s partition [`Struct`]. Used by [`TableScan`]
    /// to see if this [`DataFile`] could possibly contain data that matches
    /// the scan's filter.
    pub(crate) fn eval(&self, data_file: &DataFile) -> Result<bool> {
        let mut visitor = ExpressionEvaluatorVisitor::new(data_file.partition());

        visit(&mut visitor, &self.partition_filter.0)
    }
}

/// Acts as a visitor for [`ExpressionEvaluator`] to apply
/// evaluation logic to different parts of a data structure,
/// specifically for data file partitions.
#[derive(Debug)]
struct ExpressionEvaluatorVisitor<'a> {
    /// Reference to a [`DataFile`]'s partition [`Struct`].
    partition: &'a Struct,
}

impl<'a> ExpressionEvaluatorVisitor<'a> {
    /// Creates a new [`ExpressionEvaluatorVisitor`].
    fn new(partition: &'a Struct) -> Self {
        Self { partition }
    }
}

impl BoundPredicateVisitor for ExpressionEvaluatorVisitor<'_> {
    type T = bool;

    fn always_true(&mut self) -> Result<bool> {
        Ok(true)
    }

    fn always_false(&mut self) -> Result<bool> {
        Ok(false)
    }

    fn and(&mut self, lhs: bool, rhs: bool) -> Result<bool> {
        Ok(lhs && rhs)
    }

    fn or(&mut self, lhs: bool, rhs: bool) -> Result<bool> {
        Ok(lhs || rhs)
    }

    fn not(&mut self, _inner: bool) -> Result<bool> {
        Err(Error::new(ErrorKind::Unexpected, "The evaluation of expressions should not be performed against Predicates that contain a Not operator. Ensure that \"Rewrite Not\" gets applied to the originating Predicate before binding it."))
    }

    fn is_null(&mut self, reference: &BoundReference, _predicate: &BoundPredicate) -> Result<bool> {
        match reference.accessor().get(self.partition)? {
            Some(_) => Ok(false),
            None => Ok(true),
        }
    }

    fn not_null(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        match reference.accessor().get(self.partition)? {
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }

    fn is_nan(&mut self, reference: &BoundReference, _predicate: &BoundPredicate) -> Result<bool> {
        match reference.accessor().get(self.partition)? {
            Some(datum) => Ok(datum.is_nan()),
            None => Ok(false),
        }
    }

    fn not_nan(&mut self, reference: &BoundReference, _predicate: &BoundPredicate) -> Result<bool> {
        match reference.accessor().get(self.partition)? {
            Some(datum) => Ok(!datum.is_nan()),
            None => Ok(true),
        }
    }

    fn less_than(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        match reference.accessor().get(self.partition)? {
            Some(datum) => Ok(&datum < literal),
            None => Ok(false),
        }
    }

    fn less_than_or_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        match reference.accessor().get(self.partition)? {
            Some(datum) => Ok(&datum <= literal),
            None => Ok(false),
        }
    }

    fn greater_than(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        match reference.accessor().get(self.partition)? {
            Some(datum) => Ok(&datum > literal),
            None => Ok(false),
        }
    }

    fn greater_than_or_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        match reference.accessor().get(self.partition)? {
            Some(datum) => Ok(&datum >= literal),
            None => Ok(false),
        }
    }

    fn eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        match reference.accessor().get(self.partition)? {
            Some(datum) => Ok(&datum == literal),
            None => Ok(false),
        }
    }

    fn not_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        match reference.accessor().get(self.partition)? {
            Some(datum) => Ok(&datum != literal),
            None => Ok(true),
        }
    }

    fn starts_with(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        let Some(datum) = reference.accessor().get(self.partition)? else {
            return Ok(false);
        };

        match (datum.literal(), literal.literal()) {
            (PrimitiveLiteral::String(d), PrimitiveLiteral::String(l)) => Ok(d.starts_with(l)),
            _ => Ok(false),
        }
    }

    fn not_starts_with(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        Ok(!self.starts_with(reference, literal, _predicate)?)
    }

    fn r#in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        match reference.accessor().get(self.partition)? {
            Some(datum) => Ok(literals.contains(&datum)),
            None => Ok(false),
        }
    }

    fn not_in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        match reference.accessor().get(self.partition)? {
            Some(datum) => Ok(!literals.contains(&datum)),
            None => Ok(true),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use fnv::FnvHashSet;
    use predicate::SetExpression;

    use super::ExpressionEvaluator;
    use crate::expr::PartitionBoundPredicate;
    use crate::{
        expr::{
            predicate, visitors::inclusive_projection::InclusiveProjection, BinaryExpression, Bind,
            BoundPredicate, Predicate, PredicateOperator, Reference, UnaryExpression,
        },
        spec::{
            DataContentType, DataFile, DataFileFormat, Datum, Literal, NestedField, PartitionField,
            PartitionSpec, PartitionSpecRef, PrimitiveType, Schema, SchemaRef, Struct, Transform,
            Type,
        },
        Result,
    };

    fn create_schema_and_partition_spec(
        r#type: PrimitiveType,
    ) -> Result<(SchemaRef, PartitionSpecRef)> {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "a",
                Type::Primitive(r#type),
            ))])
            .build()?;

        let spec = PartitionSpec::builder()
            .with_spec_id(1)
            .with_fields(vec![PartitionField::builder()
                .source_id(1)
                .name("a".to_string())
                .field_id(1)
                .transform(Transform::Identity)
                .build()])
            .build()
            .unwrap();

        Ok((Arc::new(schema), Arc::new(spec)))
    }

    fn create_partition_filter(
        schema: &Schema,
        partition_spec: PartitionSpecRef,
        predicate: &BoundPredicate,
        case_sensitive: bool,
    ) -> Result<PartitionBoundPredicate> {
        let partition_type = partition_spec.partition_type(schema)?;
        let partition_fields = partition_type.fields().to_owned();

        let partition_schema = Schema::builder()
            .with_schema_id(partition_spec.spec_id)
            .with_fields(partition_fields)
            .build()?;

        let mut inclusive_projection = InclusiveProjection::new(partition_spec);

        let partition_filter = inclusive_projection
            .project(predicate)?
            .rewrite_not()
            .bind(Arc::new(partition_schema), case_sensitive)?;

        Ok(PartitionBoundPredicate(partition_filter))
    }

    fn create_expression_evaluator(
        schema: &Schema,
        partition_spec: PartitionSpecRef,
        predicate: &BoundPredicate,
        case_sensitive: bool,
    ) -> Result<ExpressionEvaluator> {
        let partition_filter =
            create_partition_filter(schema, partition_spec, predicate, case_sensitive)?;

        Ok(ExpressionEvaluator::new(partition_filter))
    }

    fn create_data_file_float() -> DataFile {
        let partition = Struct::from_iter([Some(Literal::float(1.0))]);

        DataFile {
            content: DataContentType::Data,
            file_path: "/test/path".to_string(),
            file_format: DataFileFormat::Parquet,
            partition,
            record_count: 1,
            file_size_in_bytes: 1,
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            key_metadata: vec![],
            split_offsets: vec![],
            equality_ids: vec![],
            sort_order_id: None,
        }
    }

    fn create_data_file_string() -> DataFile {
        let partition = Struct::from_iter([Some(Literal::string("test str"))]);

        DataFile {
            content: DataContentType::Data,
            file_path: "/test/path".to_string(),
            file_format: DataFileFormat::Parquet,
            partition,
            record_count: 1,
            file_size_in_bytes: 1,
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            key_metadata: vec![],
            split_offsets: vec![],
            equality_ids: vec![],
            sort_order_id: None,
        }
    }

    #[test]
    fn test_expr_or() -> Result<()> {
        let case_sensitive = true;
        let (schema, partition_spec) = create_schema_and_partition_spec(PrimitiveType::Float)?;

        let predicate = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::LessThan,
            Reference::new("a"),
            Datum::float(1.0),
        ))
        .or(Predicate::Binary(BinaryExpression::new(
            PredicateOperator::GreaterThanOrEq,
            Reference::new("a"),
            Datum::float(0.4),
        )))
        .bind(schema.clone(), case_sensitive)?;

        let expression_evaluator =
            create_expression_evaluator(&schema, partition_spec, &predicate, case_sensitive)?;

        let data_file = create_data_file_float();

        let result = expression_evaluator.eval(&data_file)?;

        assert!(result);

        Ok(())
    }

    #[test]
    fn test_expr_and() -> Result<()> {
        let case_sensitive = true;
        let (schema, partition_spec) = create_schema_and_partition_spec(PrimitiveType::Float)?;

        let predicate = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::LessThan,
            Reference::new("a"),
            Datum::float(1.1),
        ))
        .and(Predicate::Binary(BinaryExpression::new(
            PredicateOperator::GreaterThanOrEq,
            Reference::new("a"),
            Datum::float(0.4),
        )))
        .bind(schema.clone(), case_sensitive)?;

        let expression_evaluator =
            create_expression_evaluator(&schema, partition_spec, &predicate, case_sensitive)?;

        let data_file = create_data_file_float();

        let result = expression_evaluator.eval(&data_file)?;

        assert!(result);

        Ok(())
    }

    #[test]
    fn test_expr_not_in() -> Result<()> {
        let case_sensitive = true;
        let (schema, partition_spec) = create_schema_and_partition_spec(PrimitiveType::Float)?;

        let predicate = Predicate::Set(SetExpression::new(
            PredicateOperator::NotIn,
            Reference::new("a"),
            FnvHashSet::from_iter([Datum::float(0.9), Datum::float(1.2), Datum::float(2.4)]),
        ))
        .bind(schema.clone(), case_sensitive)?;

        let expression_evaluator =
            create_expression_evaluator(&schema, partition_spec, &predicate, case_sensitive)?;

        let data_file = create_data_file_float();

        let result = expression_evaluator.eval(&data_file)?;

        assert!(result);

        Ok(())
    }

    #[test]
    fn test_expr_in() -> Result<()> {
        let case_sensitive = true;
        let (schema, partition_spec) = create_schema_and_partition_spec(PrimitiveType::Float)?;

        let predicate = Predicate::Set(SetExpression::new(
            PredicateOperator::In,
            Reference::new("a"),
            FnvHashSet::from_iter([Datum::float(1.0), Datum::float(1.2), Datum::float(2.4)]),
        ))
        .bind(schema.clone(), case_sensitive)?;

        let expression_evaluator =
            create_expression_evaluator(&schema, partition_spec, &predicate, case_sensitive)?;

        let data_file = create_data_file_float();

        let result = expression_evaluator.eval(&data_file)?;

        assert!(result);

        Ok(())
    }

    #[test]
    fn test_expr_not_starts_with() -> Result<()> {
        let case_sensitive = true;
        let (schema, partition_spec) = create_schema_and_partition_spec(PrimitiveType::String)?;

        let predicate = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::NotStartsWith,
            Reference::new("a"),
            Datum::string("not"),
        ))
        .bind(schema.clone(), case_sensitive)?;

        let expression_evaluator =
            create_expression_evaluator(&schema, partition_spec, &predicate, case_sensitive)?;

        let data_file = create_data_file_string();

        let result = expression_evaluator.eval(&data_file)?;

        assert!(result);

        Ok(())
    }

    #[test]
    fn test_expr_starts_with() -> Result<()> {
        let case_sensitive = true;
        let (schema, partition_spec) = create_schema_and_partition_spec(PrimitiveType::String)?;

        let predicate = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::StartsWith,
            Reference::new("a"),
            Datum::string("test"),
        ))
        .bind(schema.clone(), case_sensitive)?;

        let expression_evaluator =
            create_expression_evaluator(&schema, partition_spec, &predicate, case_sensitive)?;

        let data_file = create_data_file_string();

        let result = expression_evaluator.eval(&data_file)?;

        assert!(result);

        Ok(())
    }

    #[test]
    fn test_expr_not_eq() -> Result<()> {
        let case_sensitive = true;
        let (schema, partition_spec) = create_schema_and_partition_spec(PrimitiveType::Float)?;

        let predicate = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::NotEq,
            Reference::new("a"),
            Datum::float(0.9),
        ))
        .bind(schema.clone(), case_sensitive)?;

        let expression_evaluator =
            create_expression_evaluator(&schema, partition_spec, &predicate, case_sensitive)?;

        let data_file = create_data_file_float();

        let result = expression_evaluator.eval(&data_file)?;

        assert!(result);

        Ok(())
    }

    #[test]
    fn test_expr_eq() -> Result<()> {
        let case_sensitive = true;
        let (schema, partition_spec) = create_schema_and_partition_spec(PrimitiveType::Float)?;

        let predicate = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::Eq,
            Reference::new("a"),
            Datum::float(1.0),
        ))
        .bind(schema.clone(), case_sensitive)?;

        let expression_evaluator =
            create_expression_evaluator(&schema, partition_spec, &predicate, case_sensitive)?;

        let data_file = create_data_file_float();

        let result = expression_evaluator.eval(&data_file)?;

        assert!(result);

        Ok(())
    }

    #[test]
    fn test_expr_greater_than_or_eq() -> Result<()> {
        let case_sensitive = true;
        let (schema, partition_spec) = create_schema_and_partition_spec(PrimitiveType::Float)?;

        let predicate = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::GreaterThanOrEq,
            Reference::new("a"),
            Datum::float(1.0),
        ))
        .bind(schema.clone(), case_sensitive)?;

        let expression_evaluator =
            create_expression_evaluator(&schema, partition_spec, &predicate, case_sensitive)?;

        let data_file = create_data_file_float();

        let result = expression_evaluator.eval(&data_file)?;

        assert!(result);

        Ok(())
    }

    #[test]
    fn test_expr_greater_than() -> Result<()> {
        let case_sensitive = true;
        let (schema, partition_spec) = create_schema_and_partition_spec(PrimitiveType::Float)?;

        let predicate = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::GreaterThan,
            Reference::new("a"),
            Datum::float(0.9),
        ))
        .bind(schema.clone(), case_sensitive)?;

        let expression_evaluator =
            create_expression_evaluator(&schema, partition_spec, &predicate, case_sensitive)?;

        let data_file = create_data_file_float();

        let result = expression_evaluator.eval(&data_file)?;

        assert!(result);

        Ok(())
    }

    #[test]
    fn test_expr_less_than_or_eq() -> Result<()> {
        let case_sensitive = true;
        let (schema, partition_spec) = create_schema_and_partition_spec(PrimitiveType::Float)?;

        let predicate = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::LessThanOrEq,
            Reference::new("a"),
            Datum::float(1.0),
        ))
        .bind(schema.clone(), case_sensitive)?;

        let expression_evaluator =
            create_expression_evaluator(&schema, partition_spec, &predicate, case_sensitive)?;

        let data_file = create_data_file_float();

        let result = expression_evaluator.eval(&data_file)?;

        assert!(result);

        Ok(())
    }

    #[test]
    fn test_expr_less_than() -> Result<()> {
        let case_sensitive = true;
        let (schema, partition_spec) = create_schema_and_partition_spec(PrimitiveType::Float)?;

        let predicate = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::LessThan,
            Reference::new("a"),
            Datum::float(1.1),
        ))
        .bind(schema.clone(), case_sensitive)?;

        let expression_evaluator =
            create_expression_evaluator(&schema, partition_spec, &predicate, case_sensitive)?;

        let data_file = create_data_file_float();

        let result = expression_evaluator.eval(&data_file)?;

        assert!(result);

        Ok(())
    }

    #[test]
    fn test_expr_is_not_nan() -> Result<()> {
        let case_sensitive = true;
        let (schema, partition_spec) = create_schema_and_partition_spec(PrimitiveType::Float)?;
        let predicate = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::NotNan,
            Reference::new("a"),
        ))
        .bind(schema.clone(), case_sensitive)?;

        let expression_evaluator =
            create_expression_evaluator(&schema, partition_spec, &predicate, case_sensitive)?;

        let data_file = create_data_file_float();

        let result = expression_evaluator.eval(&data_file)?;

        assert!(result);

        Ok(())
    }

    #[test]
    fn test_expr_is_nan() -> Result<()> {
        let case_sensitive = true;
        let (schema, partition_spec) = create_schema_and_partition_spec(PrimitiveType::Float)?;
        let predicate = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNan,
            Reference::new("a"),
        ))
        .bind(schema.clone(), case_sensitive)?;

        let expression_evaluator =
            create_expression_evaluator(&schema, partition_spec, &predicate, case_sensitive)?;

        let data_file = create_data_file_float();

        let result = expression_evaluator.eval(&data_file)?;

        assert!(!result);

        Ok(())
    }

    #[test]
    fn test_expr_is_not_null() -> Result<()> {
        let case_sensitive = true;
        let (schema, partition_spec) = create_schema_and_partition_spec(PrimitiveType::Float)?;
        let predicate = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::NotNull,
            Reference::new("a"),
        ))
        .bind(schema.clone(), case_sensitive)?;

        let expression_evaluator =
            create_expression_evaluator(&schema, partition_spec, &predicate, case_sensitive)?;

        let data_file = create_data_file_float();

        let result = expression_evaluator.eval(&data_file)?;

        assert!(result);

        Ok(())
    }

    #[test]
    fn test_expr_is_null() -> Result<()> {
        let case_sensitive = true;
        let (schema, partition_spec) = create_schema_and_partition_spec(PrimitiveType::Float)?;
        let predicate = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNull,
            Reference::new("a"),
        ))
        .bind(schema.clone(), case_sensitive)?;

        let expression_evaluator =
            create_expression_evaluator(&schema, partition_spec, &predicate, case_sensitive)?;

        let data_file = create_data_file_float();

        let result = expression_evaluator.eval(&data_file)?;

        assert!(!result);

        Ok(())
    }

    #[test]
    fn test_expr_always_false() -> Result<()> {
        let case_sensitive = true;
        let (schema, partition_spec) = create_schema_and_partition_spec(PrimitiveType::Float)?;
        let predicate = Predicate::AlwaysFalse.bind(schema.clone(), case_sensitive)?;

        let expression_evaluator =
            create_expression_evaluator(&schema, partition_spec, &predicate, case_sensitive)?;

        let data_file = create_data_file_float();

        let result = expression_evaluator.eval(&data_file)?;

        assert!(!result);

        Ok(())
    }

    #[test]
    fn test_expr_always_true() -> Result<()> {
        let case_sensitive = true;
        let (schema, partition_spec) = create_schema_and_partition_spec(PrimitiveType::Float)?;
        let predicate = Predicate::AlwaysTrue.bind(schema.clone(), case_sensitive)?;

        let expression_evaluator =
            create_expression_evaluator(&schema, partition_spec, &predicate, case_sensitive)?;

        let data_file = create_data_file_float();

        let result = expression_evaluator.eval(&data_file)?;

        assert!(result);

        Ok(())
    }
}
