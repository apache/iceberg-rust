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

use crate::expr::{BoundReference, PredicateOperator, Reference};
use crate::spec::Datum;
use std::collections::HashSet;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Not;

/// Logical expression, such as `AND`, `OR`, `NOT`.
#[derive(PartialEq)]
pub struct LogicalExpression<T, const N: usize> {
    inputs: [Box<T>; N],
}

impl<T: Debug, const N: usize> Debug for LogicalExpression<T, N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogicalExpression")
            .field("inputs", &self.inputs)
            .finish()
    }
}

impl<T, const N: usize> LogicalExpression<T, N> {
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
#[derive(PartialEq)]
pub struct UnaryExpression<T> {
    /// Operator of this predicate, must be single operand operator.
    op: PredicateOperator,
    /// Term of this predicate, for example, `a` in `a IS NULL`.
    term: T,
}

impl<T: Debug> Debug for UnaryExpression<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnaryExpression")
            .field("op", &self.op)
            .field("term", &self.term)
            .finish()
    }
}

impl<T: Display> Display for UnaryExpression<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.term, self.op)
    }
}

impl<T> UnaryExpression<T> {
    pub(crate) fn new(op: PredicateOperator, term: T) -> Self {
        debug_assert!(op.is_unary());
        Self { op, term }
    }
}

/// Binary predicate, for example, `a > 10`.
#[derive(PartialEq)]
pub struct BinaryExpression<T> {
    /// Operator of this predicate, must be binary operator, such as `=`, `>`, `<`, etc.
    op: PredicateOperator,
    /// Term of this predicate, for example, `a` in `a > 10`.
    term: T,
    /// Literal of this predicate, for example, `10` in `a > 10`.
    literal: Datum,
}

impl<T: Debug> Debug for BinaryExpression<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BinaryExpression")
            .field("op", &self.op)
            .field("term", &self.term)
            .field("literal", &self.literal)
            .finish()
    }
}

impl<T> BinaryExpression<T> {
    pub(crate) fn new(op: PredicateOperator, term: T, literal: Datum) -> Self {
        debug_assert!(op.is_binary());
        Self { op, term, literal }
    }
}

impl<T: Display> Display for BinaryExpression<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.term, self.op, self.literal)
    }
}

/// Set predicates, for example, `a in (1, 2, 3)`.
#[derive(PartialEq)]
pub struct SetExpression<T> {
    /// Operator of this predicate, must be set operator, such as `IN`, `NOT IN`, etc.
    op: PredicateOperator,
    /// Term of this predicate, for example, `a` in `a in (1, 2, 3)`.
    term: T,
    /// Literals of this predicate, for example, `(1, 2, 3)` in `a in (1, 2, 3)`.
    literals: HashSet<Datum>,
}

impl<T: Debug> Debug for SetExpression<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SetExpression")
            .field("op", &self.op)
            .field("term", &self.term)
            .field("literal", &self.literals)
            .finish()
    }
}

/// Unbound predicate expression before binding to a schema.
#[derive(Debug, PartialEq)]
pub enum Predicate {
    /// An expression always evaluates to true.
    AlwaysTrue,
    /// An expression always evaluates to false.
    AlwaysFalse,
    /// And predicate, for example, `a > 10 AND b < 20`.
    And(LogicalExpression<Predicate, 2>),
    /// Or predicate, for example, `a > 10 OR b < 20`.
    Or(LogicalExpression<Predicate, 2>),
    /// Not predicate, for example, `NOT (a > 10)`.
    Not(LogicalExpression<Predicate, 1>),
    /// Unary expression, for example, `a IS NULL`.
    Unary(UnaryExpression<Reference>),
    /// Binary expression, for example, `a > 10`.
    Binary(BinaryExpression<Reference>),
    /// Set predicates, for example, `a in (1, 2, 3)`.
    Set(SetExpression<Reference>),
}

impl Display for Predicate {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Predicate::And(expr) => {
                write!(f, "({}) AND ({})", expr.inputs()[0], expr.inputs()[1])
            }
            Predicate::Or(expr) => {
                write!(f, "({}) OR ({})", expr.inputs()[0], expr.inputs()[1])
            }
            Predicate::Not(expr) => {
                write!(f, "NOT ({})", expr.inputs()[0])
            }
            Predicate::Unary(expr) => {
                write!(f, "{}", expr.term)
            }
            Predicate::Binary(expr) => {
                write!(f, "{} {} {}", expr.term, expr.op, expr.literal)
            }
            Predicate::Set(expr) => {
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
            Predicate::AlwaysTrue => {
                write!(f, "ALWAYS TRUE")
            }
            Predicate::AlwaysFalse => {
                write!(f, "ALWAYS FALSE")
            }
        }
    }
}

impl Predicate {
    /// Combines two predicates with `AND`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::ops::Bound::Unbounded;
    /// use iceberg::expr::BoundPredicate::Unary;
    /// use iceberg::expr::Predicate::{AlwaysTrue, AlwaysFalse};
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr1 = Reference::new("a").less_than(Datum::long(10));
    ///
    /// let expr2 = Reference::new("b").less_than(Datum::long(20));
    ///
    /// let expr3 = expr1.and(expr2);
    ///
    /// assert_eq!(&format!("{expr3}"), "(a < 10) AND (b < 20)");
    ///
    /// let expr4 = AlwaysTrue;
    ///
    /// let expr5 = AlwaysFalse;
    ///
    /// let expr6 = expr4.and(expr5);
    ///
    /// assert_eq!(&format!("{expr6}"), "ALWAYS FALSE");
    /// ```
    pub fn and(self, other: Predicate) -> Predicate {
        if self == Predicate::AlwaysFalse || other == Predicate::AlwaysFalse {
            return Predicate::AlwaysFalse;
        }
        Predicate::And(LogicalExpression::new([Box::new(self), Box::new(other)]))
    }

    /// Combines two predicates with `OR`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::ops::Bound::Unbounded;
    /// use iceberg::expr::BoundPredicate::Unary;
    /// use iceberg::expr::Predicate::{AlwaysTrue, AlwaysFalse};
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr1 = Reference::new("a").less_than(Datum::long(10));
    ///
    /// let expr2 = Reference::new("b").less_than(Datum::long(20));
    ///
    /// let expr3 = expr1.or(expr2);
    ///
    /// assert_eq!(&format!("{expr3}"), "(a < 10) OR (b < 20)");
    ///
    /// let expr4 = AlwaysTrue;
    ///
    /// let expr5 = AlwaysFalse;
    ///
    /// let expr6 = expr4.or(expr5);
    ///
    /// assert_eq!(&format!("{expr6}"), "ALWAYS TRUE");
    /// ```
    pub fn or(self, other: Predicate) -> Predicate {
        if self == Predicate::AlwaysTrue || other == Predicate::AlwaysTrue {
            return Predicate::AlwaysTrue;
        }
        Predicate::Or(LogicalExpression::new([Box::new(self), Box::new(other)]))
    }
}

impl Not for Predicate {
    type Output = Predicate;

    /// Create a predicate which is the reverse of this predicate. For example: `NOT (a > 10)`
    /// # Example
    ///     
    ///```rust
    /// use std::ops::Bound::Unbounded;
    /// use iceberg::expr::BoundPredicate::Unary;
    /// use iceberg::expr::Predicate::{AlwaysTrue, AlwaysFalse};
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr1 = Reference::new("a").less_than(Datum::long(10));
    ///     
    /// let expr2 = !expr1;
    ///     
    /// assert_eq!(&format!("{expr2}"), "NOT (a < 10)");
    ///
    /// let expr3 = AlwaysTrue;
    ///
    /// let expr4 = !expr3;
    ///
    /// assert_eq!(&format!("{expr4}"), "ALWAYS FALSE");
    ///```
    fn not(self) -> Self::Output {
        if self == Predicate::AlwaysFalse {
            return Predicate::AlwaysTrue;
        } else if self == Predicate::AlwaysTrue {
            return Predicate::AlwaysFalse;
        }
        Predicate::Not(LogicalExpression::new([Box::new(self)]))
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
