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

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{BinaryExpr, Operator};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use futures::{Stream, TryStreamExt};
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::Datum;
use iceberg::table::Table;

use crate::to_datafusion_error;

/// Manages the scanning process of an Iceberg [`Table`], encapsulating the
/// necessary details and computed properties required for execution planning.
#[derive(Debug)]
pub(crate) struct IcebergTableScan {
    /// A table in the catalog.
    table: Table,
    /// A reference-counted arrow `Schema`.
    schema: ArrowSchemaRef,
    /// Stores certain, often expensive to compute,
    /// plan properties used in query optimization.
    plan_properties: PlanProperties,
    predicates: Option<Predicate>,
}

impl IcebergTableScan {
    /// Creates a new [`IcebergTableScan`] object.
    pub(crate) fn new(table: Table, schema: ArrowSchemaRef, filters: &[Expr]) -> Self {
        let plan_properties = Self::compute_properties(schema.clone());
        let predicates = convert_filters_to_predicate(filters);
        Self {
            table,
            schema,
            plan_properties,
            predicates,
        }
    }

    /// Computes [`PlanProperties`] used in query optimization.
    fn compute_properties(schema: ArrowSchemaRef) -> PlanProperties {
        // TODO:
        // This is more or less a placeholder, to be replaced
        // once we support output-partitioning
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        )
    }
}

impl ExecutionPlan for IcebergTableScan {
    fn name(&self) -> &str {
        "IcebergTableScan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<(dyn ExecutionPlan + 'static)>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let fut = get_batch_stream(self.table.clone(), self.predicates.clone());
        let stream = futures::stream::once(fut).try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

impl DisplayAs for IcebergTableScan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "IcebergTableScan")
    }
}

/// Asynchronously retrieves a stream of [`RecordBatch`] instances
/// from a given table.
///
/// This function initializes a [`TableScan`], builds it,
/// and then converts it into a stream of Arrow [`RecordBatch`]es.
async fn get_batch_stream(
    table: Table,
    predicates: Option<Predicate>,
) -> DFResult<Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>> {
    let mut scan_builder = table.scan();
    if let Some(pred) = predicates {
        scan_builder = scan_builder.with_filter(pred);
    }
    let table_scan = scan_builder.build().map_err(to_datafusion_error)?;

    let stream = table_scan
        .to_arrow()
        .await
        .map_err(to_datafusion_error)?
        .map_err(to_datafusion_error);

    Ok(Box::pin(stream))
}

/// convert DataFusion filters ([`Expr`]) to an iceberg [`Predicate`]
/// if none of the filters could be converted, return `None`
/// if the conversion was successful, return the converted predicates combined with an AND operator
fn convert_filters_to_predicate(filters: &[Expr]) -> Option<Predicate> {
    filters
        .iter()
        .filter_map(expr_to_predicate)
        .reduce(Predicate::and)
}

/// Recuresivly converting DataFusion filters ( in a [`Expr`]) to an Iceberg [`Predicate`].
///
/// This function currently handles the conversion of DataFusion expression of the following types:
///
/// 1. Simple binary expressions (e.g., "column < value")
/// 2. Compound AND expressions (e.g., "x < 1 AND y > 10")
/// 3. Compound OR expressions (e.g., "x < 1 OR y > 10")
///
/// For AND expressions, if one part of the expression can't be converted,
/// the function will still return a predicate for the part that can be converted.
/// For OR expressions, if any part can't be converted, the entire expression
/// will fail to convert.
///
/// # Arguments
///
/// * `expr` - A reference to a DataFusion [`Expr`] to be converted.
///
/// # Returns
///
/// * `Some(Predicate)` if the expression could be successfully converted.
/// * `None` if the expression couldn't be converted to an Iceberg predicate.
fn expr_to_predicate(expr: &Expr) -> Option<Predicate> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            match (left.as_ref(), op, right.as_ref()) {
                // First option arm (simple case), e.g. x < 1
                (Expr::Column(col), op, Expr::Literal(lit)) => {
                    let reference = Reference::new(col.name.clone());
                    let datum = scalar_value_to_datum(lit)?;
                    Some(binary_op_to_predicate(reference, op, datum))
                }
                // Second option arm (inner AND), e.g. x < 1 AND y > 10
                // if its an AND expression and one predicate fails, we can still go with the other one
                (left_expr, Operator::And, right_expr) => {
                    let left_pred = expr_to_predicate(&left_expr.clone());
                    let right_pred = expr_to_predicate(&right_expr.clone());
                    match (left_pred, right_pred) {
                        (Some(left), Some(right)) => Some(Predicate::and(left, right)),
                        (Some(left), None) => Some(left),
                        (None, Some(right)) => Some(right),
                        (None, None) => None,
                    }
                }
                // Third option arm (inner OR), e.g. x < 1 OR y > 10
                // if one is unsupported, we fail the predicate
                (Expr::BinaryExpr(left_expr), Operator::Or, Expr::BinaryExpr(right_expr)) => {
                    let left_pred = expr_to_predicate(&Expr::BinaryExpr(left_expr.clone()))?;
                    let right_pred = expr_to_predicate(&Expr::BinaryExpr(right_expr.clone()))?;
                    Some(Predicate::or(left_pred, right_pred))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// convert the data fusion Exp to an iceberg [`Predicate`]
fn binary_op_to_predicate(reference: Reference, op: &Operator, datum: Datum) -> Predicate {
    match op {
        Operator::Eq => reference.equal_to(datum),
        Operator::NotEq => reference.not_equal_to(datum),
        Operator::Lt => reference.less_than(datum),
        Operator::LtEq => reference.less_than_or_equal_to(datum),
        Operator::Gt => reference.greater_than(datum),
        Operator::GtEq => reference.greater_than_or_equal_to(datum),
        _ => Predicate::AlwaysTrue,
    }
}
/// convert a DataFusion scalar value to an iceberg [`Datum`]
fn scalar_value_to_datum(value: &ScalarValue) -> Option<Datum> {
    match value {
        ScalarValue::Int8(Some(v)) => Some(Datum::int(*v)),
        ScalarValue::Int16(Some(v)) => Some(Datum::int(*v)),
        ScalarValue::Int32(Some(v)) => Some(Datum::int(*v)),
        ScalarValue::Int64(Some(v)) => Some(Datum::long(*v)),
        ScalarValue::Float32(Some(v)) => Some(Datum::double(*v as f64)),
        ScalarValue::Float64(Some(v)) => Some(Datum::double(*v)),
        ScalarValue::Utf8(Some(v)) => Some(Datum::string(v.clone())),
        ScalarValue::LargeUtf8(Some(v)) => Some(Datum::string(v.clone())),
        // Add more cases as needed
        _ => {
            println!("unsupported scalar value: {:?}", value);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::DFSchema;
    use datafusion::prelude::SessionContext;

    use super::*;

    fn create_test_schema() -> DFSchema {
        let arrow_schema = Schema::new(vec![
            Field::new("foo", DataType::Int32, false),
            Field::new("bar", DataType::Utf8, false),
        ]);
        DFSchema::try_from_qualified_schema("my_table", &arrow_schema).unwrap()
    }
    fn create_test_schema_b() -> DFSchema {
        let arrow_schema = Schema::new(vec![
            Field::new("xxx", DataType::Int32, false),
            Field::new("yyy", DataType::Utf8, false),
            Field::new("zzz", DataType::Int32, false),
        ]);
        DFSchema::try_from_qualified_schema("my_table", &arrow_schema).unwrap()
    }

    #[test]
    fn test_predicate_conversion_with_single_condition() {
        let sql = "foo > 1";
        let df_schema = create_test_schema();
        let expr = SessionContext::new()
            .parse_sql_expr(sql, &df_schema)
            .unwrap();
        let predicate = convert_filters_to_predicate(&[expr]).unwrap();
        assert_eq!(
            predicate,
            Reference::new("foo").greater_than(Datum::long(1))
        );
    }

    #[test]
    fn test_predicate_conversion_with_multiple_conditions() {
        let sql = "foo > 1 and bar = 'test'";
        let df_schema = create_test_schema();
        let expr = SessionContext::new()
            .parse_sql_expr(sql, &df_schema)
            .unwrap();
        let predicate = convert_filters_to_predicate(&[expr]).unwrap();
        let expected_predicate = Predicate::and(
            Reference::new("foo").greater_than(Datum::long(1)),
            Reference::new("bar").equal_to(Datum::string("test")),
        );
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_multiple_binary_expr() {
        let sql = "(foo > 1 and bar = 'test') or foo < 0 ";
        let df_schema = create_test_schema();
        let expr = SessionContext::new()
            .parse_sql_expr(sql, &df_schema)
            .unwrap();
        let predicate = convert_filters_to_predicate(&[expr]).unwrap();
        let inner_predicate = Predicate::and(
            Reference::new("foo").greater_than(Datum::long(1)),
            Reference::new("bar").equal_to(Datum::string("test")),
        );
        let expected_predicate = Predicate::or(
            inner_predicate,
            Reference::new("foo").less_than(Datum::long(0)),
        );
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_unsupported_condition_not() {
        let sql = "xxx > 1 and yyy is not null and zzz < 0 ";
        let df_schema = create_test_schema_b();
        let expr = SessionContext::new()
            .parse_sql_expr(sql, &df_schema)
            .unwrap();
        let predicate = convert_filters_to_predicate(&[expr]).unwrap();
        let expected_predicate = Predicate::and(
            Reference::new("xxx").greater_than(Datum::long(1)),
            Reference::new("zzz").less_than(Datum::long(0)),
        );
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_unsupported_condition_and() {
        let sql = "(xxx > 1 and yyy in ('test', 'test2')) and zzz < 0 ";
        let df_schema = create_test_schema_b();
        let expr = SessionContext::new()
            .parse_sql_expr(sql, &df_schema)
            .unwrap();
        let predicate = convert_filters_to_predicate(&[expr]).unwrap();
        let expected_predicate = Predicate::and(
            Reference::new("xxx").greater_than(Datum::long(1)),
            Reference::new("zzz").less_than(Datum::long(0)),
        );
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_unsupported_condition_or() {
        let sql = "(foo > 1 and bar in ('test', 'test2')) or foo < 0 ";
        let df_schema = create_test_schema();
        let expr = SessionContext::new()
            .parse_sql_expr(sql, &df_schema)
            .unwrap();
        let predicate = convert_filters_to_predicate(&[expr]).unwrap();
        let expected_predicate = Predicate::or(
            Reference::new("foo").greater_than(Datum::long(1)),
            Reference::new("foo").less_than(Datum::long(0)),
        );
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_unsupported_expr() {
        let sql = "(xxx > 1 or yyy in ('test', 'test2')) and zzz < 0 ";
        let df_schema = create_test_schema_b();
        let expr = SessionContext::new()
            .parse_sql_expr(sql, &df_schema)
            .unwrap();
        let predicate = convert_filters_to_predicate(&[expr]).unwrap();
        let expected_predicate = Reference::new("zzz").less_than(Datum::long(0));
        assert_eq!(predicate, expected_predicate);
    }
}
