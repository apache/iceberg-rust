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

use datafusion::common::tree_node::{TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::Column;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, Operator};
use datafusion::scalar::ScalarValue;
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::Datum;
use std::collections::VecDeque;

pub struct ExprToPredicateVisitor {
    stack: VecDeque<Option<Predicate>>,
}
impl ExprToPredicateVisitor {
    /// Create a new predicate conversion visitor.
    pub fn new() -> Self {
        Self {
            stack: VecDeque::new(),
        }
    }
    /// Get the predicate from the stack.
    pub fn get_predicate(&self) -> Option<Predicate> {
        self.stack
            .iter()
            .filter_map(|opt| opt.clone())
            .reduce(Predicate::and)
    }

    /// Convert a column expression to an iceberg predicate.
    fn convert_column_expr(
        &self,
        col: &Column,
        op: &Operator,
        lit: &ScalarValue,
    ) -> Option<Predicate> {
        let reference = Reference::new(col.name.clone());
        let datum = scalar_value_to_datum(lit)?;
        Some(binary_op_to_predicate(reference, op, datum))
    }

    /// Convert a compound expression to an iceberg predicate.
    ///
    /// The strategy is to support the following cases:
    /// - if its an AND expression then the result will be the valid predicates, whether there are 2 or just 1
    /// - if its an OR expression then a predicate will be returned only if there are 2 valid predicates on both sides
    fn convert_compound_expr(&self, valid_preds: &[Predicate], op: &Operator) -> Option<Predicate> {
        let valid_preds_count = valid_preds.len();
        match (op, valid_preds_count) {
            (Operator::And, 1) => valid_preds.first().cloned(),
            (Operator::And, 2) => Some(Predicate::and(
                valid_preds[0].clone(),
                valid_preds[1].clone(),
            )),
            (Operator::Or, 2) => Some(Predicate::or(
                valid_preds[0].clone(),
                valid_preds[1].clone(),
            )),
            _ => None,
        }
    }
}

// Implement TreeNodeVisitor for ExprToPredicateVisitor
impl<'n> TreeNodeVisitor<'n> for ExprToPredicateVisitor {
    type Node = Expr;

    fn f_down(&mut self, _node: &'n Self::Node) -> Result<TreeNodeRecursion, DataFusionError> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, node: &'n Self::Node) -> Result<TreeNodeRecursion, DataFusionError> {
        if let Expr::BinaryExpr(binary) = node {
            match (&*binary.left, &binary.op, &*binary.right) {
                (Expr::Column(col), op, Expr::Literal(lit)) => {
                    let col_pred = self.convert_column_expr(col, op, lit);
                    self.stack.push_back(col_pred);
                }
                (_left, op, _right) if matches!(op, Operator::And | Operator::Or) => {
                    let right_pred = self.stack.pop_back().flatten();
                    let left_pred = self.stack.pop_back().flatten();
                    let valid_preds = [left_pred, right_pred]
                        .into_iter()
                        .flatten()
                        .collect::<Vec<_>>();
                    let compound_pred = self.convert_compound_expr(&valid_preds, op);
                    self.stack.push_back(compound_pred);
                }
                _ => {}
            }
        };
        Ok(TreeNodeRecursion::Continue)
    }
}

const MILLIS_PER_DAY: i64 = 24 * 60 * 60 * 1000;
/// Convert a scalar value to an iceberg datum.
fn scalar_value_to_datum(value: &ScalarValue) -> Option<Datum> {
    match value {
        ScalarValue::Int8(Some(v)) => Some(Datum::int(*v as i32)),
        ScalarValue::Int16(Some(v)) => Some(Datum::int(*v as i32)),
        ScalarValue::Int32(Some(v)) => Some(Datum::int(*v)),
        ScalarValue::Int64(Some(v)) => Some(Datum::long(*v)),
        ScalarValue::Float32(Some(v)) => Some(Datum::double(*v as f64)),
        ScalarValue::Float64(Some(v)) => Some(Datum::double(*v)),
        ScalarValue::Utf8(Some(v)) => Some(Datum::string(v.clone())),
        ScalarValue::LargeUtf8(Some(v)) => Some(Datum::string(v.clone())),
        ScalarValue::Date32(Some(v)) => Some(Datum::date(*v)),
        ScalarValue::Date64(Some(v)) => Some(Datum::date((*v / MILLIS_PER_DAY) as i32)),
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

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::tree_node::TreeNode;
    use datafusion::common::DFSchema;
    use datafusion::prelude::SessionContext;
    use iceberg::expr::{Predicate, Reference};
    use iceberg::spec::Datum;

    use super::ExprToPredicateVisitor;

    fn create_test_schema() -> DFSchema {
        let arrow_schema = Schema::new(vec![
            Field::new("foo", DataType::Int32, false),
            Field::new("bar", DataType::Utf8, false),
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
        let mut visitor = ExprToPredicateVisitor::new();
        expr.visit(&mut visitor).unwrap();
        let predicate = visitor.get_predicate().unwrap();
        assert_eq!(
            predicate,
            Reference::new("foo").greater_than(Datum::long(1))
        );
    }
    #[test]
    fn test_predicate_conversion_with_single_unsupported_condition() {
        let sql = "foo is null";
        let df_schema = create_test_schema();
        let expr = SessionContext::new()
            .parse_sql_expr(sql, &df_schema)
            .unwrap();
        let mut visitor = ExprToPredicateVisitor::new();
        expr.visit(&mut visitor).unwrap();
        let predicate = visitor.get_predicate();
        assert_eq!(predicate, None);
    }
    #[test]
    fn test_predicate_conversion_with_and_condition() {
        let sql = "foo > 1 and bar = 'test'";
        let df_schema = create_test_schema();
        let expr = SessionContext::new()
            .parse_sql_expr(sql, &df_schema)
            .unwrap();
        let mut visitor = ExprToPredicateVisitor::new();
        expr.visit(&mut visitor).unwrap();
        let predicate = visitor.get_predicate().unwrap();
        let expected_predicate = Predicate::and(
            Reference::new("foo").greater_than(Datum::long(1)),
            Reference::new("bar").equal_to(Datum::string("test")),
        );
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_and_condition_unsupported() {
        let sql = "foo > 1 and bar is not null";
        let df_schema = create_test_schema();
        let expr = SessionContext::new()
            .parse_sql_expr(sql, &df_schema)
            .unwrap();
        let mut visitor = ExprToPredicateVisitor::new();
        expr.visit(&mut visitor).unwrap();
        let predicate = visitor.get_predicate().unwrap();
        let expected_predicate = Reference::new("foo").greater_than(Datum::long(1));
        assert_eq!(predicate, expected_predicate);
    }
    #[test]
    fn test_predicate_conversion_with_and_condition_both_unsupported() {
        let sql = "foo in (1, 2, 3) and bar is not null";
        let df_schema = create_test_schema();
        let expr = SessionContext::new()
            .parse_sql_expr(sql, &df_schema)
            .unwrap();
        let mut visitor = ExprToPredicateVisitor::new();
        expr.visit(&mut visitor).unwrap();
        let predicate = visitor.get_predicate();
        let expected_predicate = None;
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_or_condition_unsupported() {
        let sql = "foo > 1 or bar is not null";
        let df_schema = create_test_schema();
        let expr = SessionContext::new()
            .parse_sql_expr(sql, &df_schema)
            .unwrap();
        let mut visitor = ExprToPredicateVisitor::new();
        expr.visit(&mut visitor).unwrap();
        let predicate = visitor.get_predicate();
        let expected_predicate = None;
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_complex_binary_expr() {
        let sql = "(foo > 1 and bar = 'test') or foo < 0 ";
        let df_schema = create_test_schema();
        let expr = SessionContext::new()
            .parse_sql_expr(sql, &df_schema)
            .unwrap();
        let mut visitor = ExprToPredicateVisitor::new();
        expr.visit(&mut visitor).unwrap();
        let predicate = visitor.get_predicate().unwrap();
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
    fn test_predicate_conversion_with_complex_binary_expr_unsupported() {
        let sql = "(foo > 1 or bar in ('test', 'test2')) and foo < 0 ";
        let df_schema = create_test_schema();
        let expr = SessionContext::new()
            .parse_sql_expr(sql, &df_schema)
            .unwrap();
        let mut visitor = ExprToPredicateVisitor::new();
        expr.visit(&mut visitor).unwrap();
        let predicate = visitor.get_predicate().unwrap();
        let expected_predicate = Reference::new("foo").less_than(Datum::long(0));
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    // test the get result method
    fn test_get_result_multiple() {
        let predicates = vec![
            Some(Reference::new("foo").greater_than(Datum::long(1))),
            None,
            Some(Reference::new("bar").equal_to(Datum::string("test"))),
        ];
        let stack = VecDeque::from(predicates);
        let visitor = ExprToPredicateVisitor { stack };
        assert_eq!(
            visitor.get_predicate(),
            Some(Predicate::and(
                Reference::new("foo").greater_than(Datum::long(1)),
                Reference::new("bar").equal_to(Datum::string("test")),
            ))
        );
    }

    #[test]
    fn test_get_result_single() {
        let predicates = vec![Some(Reference::new("foo").greater_than(Datum::long(1)))];
        let stack = VecDeque::from(predicates);
        let visitor = ExprToPredicateVisitor { stack };
        assert_eq!(
            visitor.get_predicate(),
            Some(Reference::new("foo").greater_than(Datum::long(1)))
        );
    }
}
