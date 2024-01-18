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

//! This module contains predicate expressions.
//! Predicate expressions are used to filter data, and evaluates to a boolean value. For example,
//! `a > 10` is a predicate expression, and it evaluates to `true` if `a` is greater than `10`,

use crate::expr::{BoundReference, PredicateOperator, UnboundReference};
use crate::spec::Datum;
use std::collections::HashSet;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Not;

/// Logical expression, such as `AND`, `OR`, `NOT`.
#[derive(Debug)]
pub struct LogicalExpression<T: Debug, const N: usize> {
    inputs: [Box<T>; N],
}

impl<T: Debug, const N: usize> LogicalExpression<T, N> {
    fn new(inputs: [Box<T>; N]) -> Self {
        Self { inputs }
    }

    /// Return inputs of this logical expression.
    pub fn inputs(&self) -> [&T; N] {
        let mut ret: [&T; N] = [self.inputs[0].as_ref(); N];
        for (i, item) in ret.iter_mut().enumerate() {
            *item = &self.inputs[i];
        }
        ret
    }
}

/// Unary predicate, for example, `a IS NULL`.
#[derive(Debug)]
pub struct UnaryExpression<T: Debug> {
    /// Operator of this predicate, must be single operand operator.
    op: PredicateOperator,
    /// Term of this predicate, for example, `a` in `a IS NULL`.
    term: T,
}

impl<T: Display + Debug> Display for UnaryExpression<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.term, self.op)
    }
}

impl<T: Debug> UnaryExpression<T> {
    pub(crate) fn new(op: PredicateOperator, term: T) -> Self {
        Self { op, term }
    }
}

/// Binary predicate, for example, `a > 10`.
#[derive(Debug)]
pub struct BinaryExpression<T: Debug> {
    /// Operator of this predicate, must be binary operator, such as `=`, `>`, `<`, etc.
    op: PredicateOperator,
    /// Term of this predicate, for example, `a` in `a > 10`.
    term: T,
    /// Literal of this predicate, for example, `10` in `a > 10`.
    literal: Datum,
}

impl<T: Debug> BinaryExpression<T> {
    pub(crate) fn new(op: PredicateOperator, term: T, literal: Datum) -> Self {
        Self { op, term, literal }
    }
}

impl<T: Display + Debug> Display for BinaryExpression<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.term, self.op, self.literal)
    }
}

/// Set predicates, for example, `a in (1, 2, 3)`.
#[derive(Debug)]
pub struct SetExpression<T: Debug> {
    /// Operator of this predicate, must be set operator, such as `IN`, `NOT IN`, etc.
    op: PredicateOperator,
    /// Term of this predicate, for example, `a` in `a in (1, 2, 3)`.
    term: T,
    /// Literals of this predicate, for example, `(1, 2, 3)` in `a in (1, 2, 3)`.
    literals: HashSet<Datum>,
}

/// Unbound predicate expression before binding to a schema.
#[derive(Debug)]
pub enum UnboundPredicate {
    /// And predicate, for example, `a > 10 AND b < 20`.
    And(LogicalExpression<UnboundPredicate, 2>),
    /// Or predicate, for example, `a > 10 OR b < 20`.
    Or(LogicalExpression<UnboundPredicate, 2>),
    /// Not predicate, for example, `NOT (a > 10)`.
    Not(LogicalExpression<UnboundPredicate, 1>),
    /// Unary expression, for example, `a IS NULL`.
    Unary(UnaryExpression<UnboundReference>),
    /// Binary expression, for example, `a > 10`.
    Binary(BinaryExpression<UnboundReference>),
    /// Set predicates, for example, `a in (1, 2, 3)`.
    Set(SetExpression<UnboundReference>),
}

impl Display for UnboundPredicate {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            UnboundPredicate::And(expr) => {
                write!(f, "({}) AND ({})", expr.inputs()[0], expr.inputs()[1])
            }
            UnboundPredicate::Or(expr) => {
                write!(f, "({}) OR ({})", expr.inputs()[0], expr.inputs()[1])
            }
            UnboundPredicate::Not(expr) => {
                write!(f, "NOT ({})", expr.inputs()[0])
            }
            UnboundPredicate::Unary(expr) => {
                write!(f, "{}", expr.term)
            }
            UnboundPredicate::Binary(expr) => {
                write!(f, "{} {} {}", expr.term, expr.op, expr.literal)
            }
            UnboundPredicate::Set(expr) => {
                write!(
                    f,
                    "{} {} ({})",
                    expr.term,
                    expr.op,
                    expr.literals
                        .iter()
                        .map(|l| format!("{:?}", l))
                        .collect::<Vec<String>>()
                        .join(", ")
                )
            }
        }
    }
}

impl UnboundPredicate {
    /// Combines two predicates with `AND`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::ops::Bound::Unbounded;
    /// use iceberg::expr::BoundPredicate::Unary;
    /// use iceberg::expr::UnboundReference;
    /// use iceberg::spec::Datum;
    /// let expr1 = UnboundReference::new("a").less_than(Datum::long(10));
    ///
    /// let expr2 = UnboundReference::new("b").less_than(Datum::long(20));
    ///
    /// let expr = expr1.and(expr2);
    ///
    /// assert_eq!(&format!("{expr}"), "(a < 10) AND (b < 20)");
    /// ```
    pub fn and(self, other: UnboundPredicate) -> UnboundPredicate {
        UnboundPredicate::And(LogicalExpression::new([Box::new(self), Box::new(other)]))
    }

    /// Combines two predicates with `OR`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::ops::Bound::Unbounded;
    /// use iceberg::expr::BoundPredicate::Unary;
    /// use iceberg::expr::UnboundReference;
    /// use iceberg::spec::Datum;
    /// let expr1 = UnboundReference::new("a").less_than(Datum::long(10));
    ///
    /// let expr2 = UnboundReference::new("b").less_than(Datum::long(20));
    ///
    /// let expr = expr1.or(expr2);
    ///
    /// assert_eq!(&format!("{expr}"), "(a < 10) OR (b < 20)");
    /// ```
    pub fn or(self, other: UnboundPredicate) -> UnboundPredicate {
        UnboundPredicate::Or(LogicalExpression::new([Box::new(self), Box::new(other)]))
    }
}

impl Not for UnboundPredicate {
    type Output = UnboundPredicate;

    /// Create a predicate which is the reverse of this predicate. For example: `NOT (a > 10)`
    /// # Example
    ///     
    ///```rust
    ///use std::ops::Bound::Unbounded;
    ///use iceberg::expr::BoundPredicate::Unary;
    ///use iceberg::expr::UnboundReference;
    ///use iceberg::spec::Datum;
    ///let expr1 = UnboundReference::new("a").less_than(Datum::long(10));
    ///     
    ///let expr = !expr1;
    ///     
    ///assert_eq!(&format!("{expr}"), "NOT (a < 10)");
    ///```
    fn not(self) -> Self::Output {
        UnboundPredicate::Not(LogicalExpression::new([Box::new(self)]))
    }
}

/// Bound predicate expression after binding to a schema.
#[derive(Debug)]
pub enum BoundPredicate {
    /// An expression always evaluates to true.
    AlwaysTrue,
    /// An expression always evaluates to false.
    AlwaysFalse,
    /// An expression combined by `AND`, for example, `a > 10 AND b < 20`.
    And(LogicalExpression<BoundPredicate, 2>),
    /// An expression combined by `OR`, for example, `a > 10 OR b < 20`.
    Or(LogicalExpression<BoundPredicate, 2>),
    /// An expression combined by `NOT`, for example, `NOT (a > 10)`.
    Not(LogicalExpression<BoundPredicate, 1>),
    /// Unary expression, for example, `a IS NULL`.
    Unary(UnaryExpression<BoundReference>),
    /// Binary expression, for example, `a > 10`.
    Binary(BinaryExpression<BoundReference>),
    /// Set predicates, for example, `a in (1, 2, 3)`.
    Set(SetExpression<BoundReference>),
}
