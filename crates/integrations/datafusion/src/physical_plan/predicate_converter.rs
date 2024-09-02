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

use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{BinaryExpr, Cast, Expr, Operator};
use datafusion::scalar::ScalarValue;
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::Datum;
#[derive(Default)]
pub struct PredicateConverter;

impl PredicateConverter {
    /// Convert a list of DataFusion expressions to an iceberg predicate.
    pub fn visit_many(&self, exprs: &[Expr]) -> Option<Predicate> {
        exprs
            .iter()
            .filter_map(|expr| self.visit(expr))
            .reduce(Predicate::and)
    }

    /// Convert a single DataFusion expression to an iceberg predicate.
    /// currently only supports binary (simple) expressions
    pub fn visit(&self, expr: &Expr) -> Option<Predicate> {
        match expr {
            Expr::BinaryExpr(binary) => self.visit_binary_expr(binary),
            _ => None,
        }
    }

    /// Convert a binary expression to an iceberg predicate.
    ///
    /// currently supports:
    /// - column, basic op, and literal, e.g. `a = 1`
    /// - column and casted literal, e.g. `a = cast(1 as bigint)`
    /// - binary conditional (and, or), e.g. `a = 1 and b = 2`
    fn visit_binary_expr(&self, binary: &BinaryExpr) -> Option<Predicate> {
        match (&*binary.left, &binary.op, &*binary.right) {
            // column, op, literal
            (Expr::Column(col), op, Expr::Literal(lit)) => self.visit_column_literal(col, op, lit),
            // column, op, casted literal
            (Expr::Column(col), op, Expr::Cast(Cast { expr, data_type })) => {
                self.visit_column_cast(col, op, expr, data_type)
            }
            // binary conditional (and, or)
            (left, op, right) if matches!(op, Operator::And | Operator::Or) => {
                self.visit_binary_conditional(left, op, right)
            }
            _ => None,
        }
    }

    /// Convert a column and casted literal to an iceberg predicate.
    /// The purpose of this function is to handle the common case in which there is a filter based on a casted literal.
    /// These kinds of expressions are often not pushed down by query engines though its an important case to handle
    /// for iceberg scan pushdown.
    fn visit_column_cast(
        &self,
        col: &datafusion::common::Column,
        op: &Operator,
        expr: &Expr,
        data_type: &DataType,
    ) -> Option<Predicate> {
        if let (Expr::Literal(ScalarValue::Utf8(lit)), DataType::Date32) = (expr, data_type) {
            let reference = Reference::new(col.name.clone());
            let datum = lit
                .clone()
                .and_then(|date_str| Datum::date_from_str(date_str).ok())?;
            return Some(binary_op_to_predicate(reference, op, datum));
        }
        None
    }

    /// Convert a binary conditional expression, i.e., (and, or), to an iceberg predicate.
    ///
    /// When processing an AND expression:
    /// - if both expressions are valid predicates then an AND predicate is returned
    /// - if either expression is None then the valid one is returned
    ///
    /// When processing an OR expression:
    /// - only if both expressions are valid predicates then an OR predicate is returned
    fn visit_binary_conditional(
        &self,
        left: &Expr,
        op: &Operator,
        right: &Expr,
    ) -> Option<Predicate> {
        let preds: Vec<Predicate> = vec![self.visit(left), self.visit(right)]
            .into_iter()
            .flatten()
            .collect();
        let num_valid_preds = preds.len();
        match (op, num_valid_preds) {
            (Operator::And, 1) => preds.first().cloned(),
            (Operator::And, 2) => Some(Predicate::and(preds[0].clone(), preds[1].clone())),
            (Operator::Or, 2) => Some(Predicate::or(preds[0].clone(), preds[1].clone())),
            _ => None,
        }
    }

    /// Convert a simple expression based on column and literal (x > 1) to an iceberg predicate.
    fn visit_column_literal(
        &self,
        col: &datafusion::common::Column,
        op: &Operator,
        lit: &ScalarValue,
    ) -> Option<Predicate> {
        let reference = Reference::new(col.name.clone());
        let datum = scalar_value_to_datum(lit)?;
        Some(binary_op_to_predicate(reference, op, datum))
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
