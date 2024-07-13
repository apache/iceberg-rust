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

use std::fmt::{Debug, Display, Formatter};
use std::ops::Not;

use array_init::array_init;
use fnv::FnvHashSet;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::expr::{Bind, BoundReference, PredicateOperator, Reference};
use crate::spec::{Datum, SchemaRef};
use crate::{Error, ErrorKind};

/// Logical expression, such as `AND`, `OR`, `NOT`.
#[derive(PartialEq, Clone)]
pub struct LogicalExpression<T, const N: usize> {
    inputs: [Box<T>; N],
}

impl<T: Serialize, const N: usize> Serialize for LogicalExpression<T, N> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.inputs.serialize(serializer)
    }
}

impl<'de, T: Deserialize<'de>, const N: usize> Deserialize<'de> for LogicalExpression<T, N> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let inputs = Vec::<Box<T>>::deserialize(deserializer)?;
        Ok(LogicalExpression::new(
            array_init::from_iter(inputs.into_iter()).ok_or_else(|| {
                serde::de::Error::custom(format!("Failed to deserialize LogicalExpression: the len of inputs is not match with the len of LogicalExpression {}",N))
            })?,
        ))
    }
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

impl<T: Bind, const N: usize> Bind for LogicalExpression<T, N>
where
    T::Bound: Sized,
{
    type Bound = LogicalExpression<T::Bound, N>;

    fn bind(&self, schema: SchemaRef, case_sensitive: bool) -> Result<Self::Bound> {
        let mut outputs: [Option<Box<T::Bound>>; N] = array_init(|_| None);
        for (i, input) in self.inputs.iter().enumerate() {
            outputs[i] = Some(Box::new(input.bind(schema.clone(), case_sensitive)?));
        }

        // It's safe to use `unwrap` here since they are all `Some`.
        let bound_inputs = array_init::from_iter(outputs.into_iter().map(Option::unwrap)).unwrap();
        Ok(LogicalExpression::new(bound_inputs))
    }
}

/// Unary predicate, for example, `a IS NULL`.
#[derive(PartialEq, Clone, Serialize, Deserialize)]
pub struct UnaryExpression<T> {
    /// Operator of this predicate, must be single operand operator.
    op: PredicateOperator,
    /// Term of this predicate, for example, `a` in `a IS NULL`.
    #[serde(bound(serialize = "T: Serialize", deserialize = "T: Deserialize<'de>"))]
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

impl<T: Bind> Bind for UnaryExpression<T> {
    type Bound = UnaryExpression<T::Bound>;

    fn bind(&self, schema: SchemaRef, case_sensitive: bool) -> Result<Self::Bound> {
        let bound_term = self.term.bind(schema, case_sensitive)?;
        Ok(UnaryExpression::new(self.op, bound_term))
    }
}

impl<T> UnaryExpression<T> {
    pub(crate) fn new(op: PredicateOperator, term: T) -> Self {
        debug_assert!(op.is_unary());
        Self { op, term }
    }

    /// Return the operator of this predicate.
    pub(crate) fn op(&self) -> PredicateOperator {
        self.op
    }

    /// Return the term of this predicate.
    pub(crate) fn term(&self) -> &T {
        &self.term
    }
}

/// Binary predicate, for example, `a > 10`.
#[derive(PartialEq, Clone, Serialize, Deserialize)]
pub struct BinaryExpression<T> {
    /// Operator of this predicate, must be binary operator, such as `=`, `>`, `<`, etc.
    op: PredicateOperator,
    /// Term of this predicate, for example, `a` in `a > 10`.
    #[serde(bound(serialize = "T: Serialize", deserialize = "T: Deserialize<'de>"))]
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

    pub(crate) fn op(&self) -> PredicateOperator {
        self.op
    }

    /// Return the literal of this predicate.
    pub(crate) fn literal(&self) -> &Datum {
        &self.literal
    }

    /// Return the term of this predicate.
    pub(crate) fn term(&self) -> &T {
        &self.term
    }
}

impl<T: Display> Display for BinaryExpression<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.term, self.op, self.literal)
    }
}

impl<T: Bind> Bind for BinaryExpression<T> {
    type Bound = BinaryExpression<T::Bound>;

    fn bind(&self, schema: SchemaRef, case_sensitive: bool) -> Result<Self::Bound> {
        let bound_term = self.term.bind(schema.clone(), case_sensitive)?;
        Ok(BinaryExpression::new(
            self.op,
            bound_term,
            self.literal.clone(),
        ))
    }
}

/// Set predicates, for example, `a in (1, 2, 3)`.
#[derive(PartialEq, Clone, Serialize, Deserialize)]
pub struct SetExpression<T> {
    /// Operator of this predicate, must be set operator, such as `IN`, `NOT IN`, etc.
    op: PredicateOperator,
    /// Term of this predicate, for example, `a` in `a in (1, 2, 3)`.
    term: T,
    /// Literals of this predicate, for example, `(1, 2, 3)` in `a in (1, 2, 3)`.
    literals: FnvHashSet<Datum>,
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

impl<T> SetExpression<T> {
    pub(crate) fn new(op: PredicateOperator, term: T, literals: FnvHashSet<Datum>) -> Self {
        debug_assert!(op.is_set());
        Self { op, term, literals }
    }

    /// Return the operator of this predicate.
    pub(crate) fn op(&self) -> PredicateOperator {
        self.op
    }

    pub(crate) fn literals(&self) -> &FnvHashSet<Datum> {
        &self.literals
    }

    /// Return the term of this predicate.
    pub(crate) fn term(&self) -> &T {
        &self.term
    }
}

impl<T: Bind> Bind for SetExpression<T> {
    type Bound = SetExpression<T::Bound>;

    fn bind(&self, schema: SchemaRef, case_sensitive: bool) -> Result<Self::Bound> {
        let bound_term = self.term.bind(schema.clone(), case_sensitive)?;
        Ok(SetExpression::new(
            self.op,
            bound_term,
            self.literals.clone(),
        ))
    }
}

impl<T: Display + Debug> Display for SetExpression<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut literal_strs = self.literals.iter().map(|l| format!("{}", l));

        write!(f, "{} {} ({})", self.term, self.op, literal_strs.join(", "))
    }
}

/// Unbound predicate expression before binding to a schema.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum Predicate {
    /// AlwaysTrue predicate, for example, `TRUE`.
    AlwaysTrue,
    /// AlwaysFalse predicate, for example, `FALSE`.
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

impl Bind for Predicate {
    type Bound = BoundPredicate;

    fn bind(&self, schema: SchemaRef, case_sensitive: bool) -> Result<BoundPredicate> {
        match self {
            Predicate::And(expr) => {
                let bound_expr = expr.bind(schema, case_sensitive)?;

                let [left, right] = bound_expr.inputs;
                Ok(match (left, right) {
                    (_, r) if matches!(&*r, &BoundPredicate::AlwaysFalse) => {
                        BoundPredicate::AlwaysFalse
                    }
                    (l, _) if matches!(&*l, &BoundPredicate::AlwaysFalse) => {
                        BoundPredicate::AlwaysFalse
                    }
                    (left, r) if matches!(&*r, &BoundPredicate::AlwaysTrue) => *left,
                    (l, right) if matches!(&*l, &BoundPredicate::AlwaysTrue) => *right,
                    (left, right) => BoundPredicate::And(LogicalExpression::new([left, right])),
                })
            }
            Predicate::Not(expr) => {
                let bound_expr = expr.bind(schema, case_sensitive)?;
                let [inner] = bound_expr.inputs;
                Ok(match inner {
                    e if matches!(&*e, &BoundPredicate::AlwaysTrue) => BoundPredicate::AlwaysFalse,
                    e if matches!(&*e, &BoundPredicate::AlwaysFalse) => BoundPredicate::AlwaysTrue,
                    e => BoundPredicate::Not(LogicalExpression::new([e])),
                })
            }
            Predicate::Or(expr) => {
                let bound_expr = expr.bind(schema, case_sensitive)?;
                let [left, right] = bound_expr.inputs;
                Ok(match (left, right) {
                    (l, r)
                        if matches!(&*r, &BoundPredicate::AlwaysTrue)
                            || matches!(&*l, &BoundPredicate::AlwaysTrue) =>
                    {
                        BoundPredicate::AlwaysTrue
                    }
                    (left, r) if matches!(&*r, &BoundPredicate::AlwaysFalse) => *left,
                    (l, right) if matches!(&*l, &BoundPredicate::AlwaysFalse) => *right,
                    (left, right) => BoundPredicate::Or(LogicalExpression::new([left, right])),
                })
            }
            Predicate::Unary(expr) => {
                let bound_expr = expr.bind(schema, case_sensitive)?;

                match &bound_expr.op {
                    &PredicateOperator::IsNull => {
                        if bound_expr.term.field().required {
                            return Ok(BoundPredicate::AlwaysFalse);
                        }
                    }
                    &PredicateOperator::NotNull => {
                        if bound_expr.term.field().required {
                            return Ok(BoundPredicate::AlwaysTrue);
                        }
                    }
                    &PredicateOperator::IsNan | &PredicateOperator::NotNan => {
                        if !bound_expr.term.field().field_type.is_floating_type() {
                            return Err(Error::new(
                                ErrorKind::DataInvalid,
                                format!(
                                    "Expecting floating point type, but found {}",
                                    bound_expr.term.field().field_type
                                ),
                            ));
                        }
                    }
                    op => {
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            format!("Expecting unary operator, but found {op}"),
                        ))
                    }
                }

                Ok(BoundPredicate::Unary(bound_expr))
            }
            Predicate::Binary(expr) => {
                let bound_expr = expr.bind(schema, case_sensitive)?;
                let bound_literal = bound_expr.literal.to(&bound_expr.term.field().field_type)?;
                Ok(BoundPredicate::Binary(BinaryExpression::new(
                    bound_expr.op,
                    bound_expr.term,
                    bound_literal,
                )))
            }
            Predicate::Set(expr) => {
                let bound_expr = expr.bind(schema, case_sensitive)?;
                let bound_literals = bound_expr
                    .literals
                    .into_iter()
                    .map(|l| l.to(&bound_expr.term.field().field_type))
                    .collect::<Result<FnvHashSet<Datum>>>()?;

                match &bound_expr.op {
                    &PredicateOperator::In => {
                        if bound_literals.is_empty() {
                            return Ok(BoundPredicate::AlwaysFalse);
                        }
                        if bound_literals.len() == 1 {
                            return Ok(BoundPredicate::Binary(BinaryExpression::new(
                                PredicateOperator::Eq,
                                bound_expr.term,
                                bound_literals.into_iter().next().unwrap(),
                            )));
                        }
                    }
                    &PredicateOperator::NotIn => {
                        if bound_literals.is_empty() {
                            return Ok(BoundPredicate::AlwaysTrue);
                        }
                        if bound_literals.len() == 1 {
                            return Ok(BoundPredicate::Binary(BinaryExpression::new(
                                PredicateOperator::NotEq,
                                bound_expr.term,
                                bound_literals.into_iter().next().unwrap(),
                            )));
                        }
                    }
                    op => {
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            format!("Expecting unary operator,but found {op}"),
                        ))
                    }
                }

                Ok(BoundPredicate::Set(SetExpression::new(
                    bound_expr.op,
                    bound_expr.term,
                    bound_literals,
                )))
            }
            Predicate::AlwaysTrue => Ok(BoundPredicate::AlwaysTrue),
            Predicate::AlwaysFalse => Ok(BoundPredicate::AlwaysFalse),
        }
    }
}

impl Display for Predicate {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Predicate::AlwaysTrue => {
                write!(f, "TRUE")
            }
            Predicate::AlwaysFalse => {
                write!(f, "FALSE")
            }
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
                write!(f, "{}", expr)
            }
            Predicate::Binary(expr) => {
                write!(f, "{}", expr)
            }
            Predicate::Set(expr) => {
                write!(f, "{}", expr)
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
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr1 = Reference::new("a").less_than(Datum::long(10));
    ///
    /// let expr2 = Reference::new("b").less_than(Datum::long(20));
    ///
    /// let expr = expr1.and(expr2);
    ///
    /// assert_eq!(&format!("{expr}"), "(a < 10) AND (b < 20)");
    /// ```
    pub fn and(self, other: Predicate) -> Predicate {
        match (self, other) {
            (Predicate::AlwaysFalse, _) => Predicate::AlwaysFalse,
            (_, Predicate::AlwaysFalse) => Predicate::AlwaysFalse,
            (Predicate::AlwaysTrue, rhs) => rhs,
            (lhs, Predicate::AlwaysTrue) => lhs,
            (lhs, rhs) => Predicate::And(LogicalExpression::new([Box::new(lhs), Box::new(rhs)])),
        }
    }

    /// Combines two predicates with `OR`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::ops::Bound::Unbounded;
    /// use iceberg::expr::BoundPredicate::Unary;
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr1 = Reference::new("a").less_than(Datum::long(10));
    ///
    /// let expr2 = Reference::new("b").less_than(Datum::long(20));
    ///
    /// let expr = expr1.or(expr2);
    ///
    /// assert_eq!(&format!("{expr}"), "(a < 10) OR (b < 20)");
    /// ```
    pub fn or(self, other: Predicate) -> Predicate {
        match (self, other) {
            (Predicate::AlwaysTrue, _) => Predicate::AlwaysTrue,
            (_, Predicate::AlwaysTrue) => Predicate::AlwaysTrue,
            (Predicate::AlwaysFalse, rhs) => rhs,
            (lhs, Predicate::AlwaysFalse) => lhs,
            (lhs, rhs) => Predicate::Or(LogicalExpression::new([Box::new(lhs), Box::new(rhs)])),
        }
    }

    /// Returns a predicate representing the negation ('NOT') of this one,
    /// by using inverse predicates rather than wrapping in a `NOT`.
    /// Used for `NOT` elimination.
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::ops::Bound::Unbounded;
    /// use iceberg::expr::BoundPredicate::Unary;
    /// use iceberg::expr::{LogicalExpression, Predicate, Reference};
    /// use iceberg::spec::Datum;
    /// let expr1 = Reference::new("a").less_than(Datum::long(10));
    /// let expr2 = Reference::new("b").less_than(Datum::long(5)).and(Reference::new("c").less_than(Datum::long(10)));
    ///
    /// let result = expr1.negate();
    /// assert_eq!(&format!("{result}"), "a >= 10");
    ///
    /// let result = expr2.negate();
    /// assert_eq!(&format!("{result}"), "(b >= 5) OR (c >= 10)");
    /// ```
    pub fn negate(self) -> Predicate {
        match self {
            Predicate::AlwaysTrue => Predicate::AlwaysFalse,
            Predicate::AlwaysFalse => Predicate::AlwaysTrue,
            Predicate::And(expr) => Predicate::Or(LogicalExpression::new(
                expr.inputs.map(|expr| Box::new(expr.negate())),
            )),
            Predicate::Or(expr) => Predicate::And(LogicalExpression::new(
                expr.inputs.map(|expr| Box::new(expr.negate())),
            )),
            Predicate::Not(expr) => {
                let LogicalExpression { inputs: [input_0] } = expr;
                *input_0
            }
            Predicate::Unary(expr) => {
                Predicate::Unary(UnaryExpression::new(expr.op.negate(), expr.term))
            }
            Predicate::Binary(expr) => Predicate::Binary(BinaryExpression::new(
                expr.op.negate(),
                expr.term,
                expr.literal,
            )),
            Predicate::Set(expr) => Predicate::Set(SetExpression::new(
                expr.op.negate(),
                expr.term,
                expr.literals,
            )),
        }
    }
    /// Simplifies the expression by removing `NOT` predicates,
    /// directly negating the inner expressions instead. The transformation
    /// applies logical laws (such as De Morgan's laws) to
    /// recursively negate and simplify inner expressions within `NOT`
    /// predicates.
    ///
    /// # Example
    ///
    /// ```rust
    /// use iceberg::expr::{LogicalExpression, Predicate, Reference};
    /// use iceberg::spec::Datum;
    /// use std::ops::Not;
    ///
    /// let expression = Reference::new("a").less_than(Datum::long(5)).not();
    /// let result = expression.rewrite_not();
    ///
    /// assert_eq!(&format!("{result}"), "a >= 5");
    /// ```
    pub fn rewrite_not(self) -> Predicate {
        match self {
            Predicate::And(expr) => {
                let [left, right] = expr.inputs;
                let new_left = Box::new(left.rewrite_not());
                let new_right = Box::new(right.rewrite_not());
                Predicate::And(LogicalExpression::new([new_left, new_right]))
            }
            Predicate::Or(expr) => {
                let [left, right] = expr.inputs;
                let new_left = Box::new(left.rewrite_not());
                let new_right = Box::new(right.rewrite_not());
                Predicate::Or(LogicalExpression::new([new_left, new_right]))
            }
            Predicate::Not(expr) => {
                let [inner] = expr.inputs;
                inner.negate()
            }
            Predicate::Unary(expr) => Predicate::Unary(expr),
            Predicate::Binary(expr) => Predicate::Binary(expr),
            Predicate::Set(expr) => Predicate::Set(expr),
            Predicate::AlwaysTrue => Predicate::AlwaysTrue,
            Predicate::AlwaysFalse => Predicate::AlwaysFalse,
        }
    }
}

impl Not for Predicate {
    type Output = Predicate;

    /// Create a predicate which is the reverse of this predicate. For example: `NOT (a > 10)`.
    ///
    /// This is different from [`Predicate::negate()`] since it doesn't rewrite expression, but
    /// just adds a `NOT` operator.
    ///
    /// # Example
    ///     
    ///```rust
    ///use std::ops::Bound::Unbounded;
    ///use iceberg::expr::BoundPredicate::Unary;
    ///use iceberg::expr::Reference;
    ///use iceberg::spec::Datum;
    ///let expr1 = Reference::new("a").less_than(Datum::long(10));
    ///     
    ///let expr = !expr1;
    ///     
    ///assert_eq!(&format!("{expr}"), "NOT (a < 10)");
    ///```
    fn not(self) -> Self::Output {
        Predicate::Not(LogicalExpression::new([Box::new(self)]))
    }
}

/// Bound predicate expression after binding to a schema.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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
    /// Set predicates, for example, `a IN (1, 2, 3)`.
    Set(SetExpression<BoundReference>),
}

/// Newtype to prevent accidentally confusing predicates that are bound to a partition with ones that are bound to a schema.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PartitionBoundPredicate(pub(crate) BoundPredicate);

impl Display for BoundPredicate {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BoundPredicate::AlwaysTrue => {
                write!(f, "True")
            }
            BoundPredicate::AlwaysFalse => {
                write!(f, "False")
            }
            BoundPredicate::And(expr) => {
                write!(f, "({}) AND ({})", expr.inputs()[0], expr.inputs()[1])
            }
            BoundPredicate::Or(expr) => {
                write!(f, "({}) OR ({})", expr.inputs()[0], expr.inputs()[1])
            }
            BoundPredicate::Not(expr) => {
                write!(f, "NOT ({})", expr.inputs()[0])
            }
            BoundPredicate::Unary(expr) => {
                write!(f, "{}", expr)
            }
            BoundPredicate::Binary(expr) => {
                write!(f, "{}", expr)
            }
            BoundPredicate::Set(expr) => {
                write!(f, "{}", expr)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Not;
    use std::sync::Arc;

    use crate::expr::Predicate::{AlwaysFalse, AlwaysTrue};
    use crate::expr::Reference;
    use crate::expr::{Bind, BoundPredicate};
    use crate::spec::Datum;
    use crate::spec::{NestedField, PrimitiveType, Schema, SchemaRef, Type};

    #[test]
    fn test_logical_or_rewrite_not() {
        let expression = Reference::new("b")
            .less_than(Datum::long(5))
            .or(Reference::new("c").less_than(Datum::long(10)))
            .not();

        let expected = Reference::new("b")
            .greater_than_or_equal_to(Datum::long(5))
            .and(Reference::new("c").greater_than_or_equal_to(Datum::long(10)));

        let result = expression.rewrite_not();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_logical_and_rewrite_not() {
        let expression = Reference::new("b")
            .less_than(Datum::long(5))
            .and(Reference::new("c").less_than(Datum::long(10)))
            .not();

        let expected = Reference::new("b")
            .greater_than_or_equal_to(Datum::long(5))
            .or(Reference::new("c").greater_than_or_equal_to(Datum::long(10)));

        let result = expression.rewrite_not();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_set_rewrite_not() {
        let expression = Reference::new("a")
            .is_in([Datum::int(5), Datum::int(6)])
            .not();

        let expected = Reference::new("a").is_not_in([Datum::int(5), Datum::int(6)]);

        let result = expression.rewrite_not();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_binary_rewrite_not() {
        let expression = Reference::new("a").less_than(Datum::long(5)).not();

        let expected = Reference::new("a").greater_than_or_equal_to(Datum::long(5));

        let result = expression.rewrite_not();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_unary_rewrite_not() {
        let expression = Reference::new("a").is_null().not();

        let expected = Reference::new("a").is_not_null();

        let result = expression.rewrite_not();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_predicate_and_reduce_always_true_false() {
        let true_or_expr = AlwaysTrue.and(Reference::new("b").less_than(Datum::long(5)));
        assert_eq!(&format!("{true_or_expr}"), "b < 5");

        let expr_or_true = Reference::new("b")
            .less_than(Datum::long(5))
            .and(AlwaysTrue);
        assert_eq!(&format!("{expr_or_true}"), "b < 5");

        let false_or_expr = AlwaysFalse.and(Reference::new("b").less_than(Datum::long(5)));
        assert_eq!(&format!("{false_or_expr}"), "FALSE");

        let expr_or_false = Reference::new("b")
            .less_than(Datum::long(5))
            .and(AlwaysFalse);
        assert_eq!(&format!("{expr_or_false}"), "FALSE");
    }

    #[test]
    fn test_predicate_or_reduce_always_true_false() {
        let true_or_expr = AlwaysTrue.or(Reference::new("b").less_than(Datum::long(5)));
        assert_eq!(&format!("{true_or_expr}"), "TRUE");

        let expr_or_true = Reference::new("b").less_than(Datum::long(5)).or(AlwaysTrue);
        assert_eq!(&format!("{expr_or_true}"), "TRUE");

        let false_or_expr = AlwaysFalse.or(Reference::new("b").less_than(Datum::long(5)));
        assert_eq!(&format!("{false_or_expr}"), "b < 5");

        let expr_or_false = Reference::new("b")
            .less_than(Datum::long(5))
            .or(AlwaysFalse);
        assert_eq!(&format!("{expr_or_false}"), "b < 5");
    }

    #[test]
    fn test_predicate_negate_and() {
        let expression = Reference::new("b")
            .less_than(Datum::long(5))
            .and(Reference::new("c").less_than(Datum::long(10)));

        let expected = Reference::new("b")
            .greater_than_or_equal_to(Datum::long(5))
            .or(Reference::new("c").greater_than_or_equal_to(Datum::long(10)));

        let result = expression.negate();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_predicate_negate_or() {
        let expression = Reference::new("b")
            .greater_than_or_equal_to(Datum::long(5))
            .or(Reference::new("c").greater_than_or_equal_to(Datum::long(10)));

        let expected = Reference::new("b")
            .less_than(Datum::long(5))
            .and(Reference::new("c").less_than(Datum::long(10)));

        let result = expression.negate();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_predicate_negate_not() {
        let expression = Reference::new("b")
            .greater_than_or_equal_to(Datum::long(5))
            .not();

        let expected = Reference::new("b").greater_than_or_equal_to(Datum::long(5));

        let result = expression.negate();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_predicate_negate_unary() {
        let expression = Reference::new("b").is_not_null();

        let expected = Reference::new("b").is_null();

        let result = expression.negate();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_predicate_negate_binary() {
        let expression = Reference::new("a").less_than(Datum::long(5));

        let expected = Reference::new("a").greater_than_or_equal_to(Datum::long(5));

        let result = expression.negate();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_predicate_negate_set() {
        let expression = Reference::new("a").is_in([Datum::long(5), Datum::long(6)]);

        let expected = Reference::new("a").is_not_in([Datum::long(5), Datum::long(6)]);

        let result = expression.negate();

        assert_eq!(result, expected);
    }

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

    fn test_bound_predicate_serialize_diserialize(bound_predicate: BoundPredicate) {
        let serialized = serde_json::to_string(&bound_predicate).unwrap();
        let deserialized: BoundPredicate = serde_json::from_str(&serialized).unwrap();
        assert_eq!(bound_predicate, deserialized);
    }

    #[test]
    fn test_bind_is_null() {
        let schema = table_schema_simple();
        let expr = Reference::new("foo").is_null();
        let bound_expr = expr.bind(schema, true).unwrap();
        assert_eq!(&format!("{bound_expr}"), "foo IS NULL");
        test_bound_predicate_serialize_diserialize(bound_expr);
    }

    #[test]
    fn test_bind_is_null_required() {
        let schema = table_schema_simple();
        let expr = Reference::new("bar").is_null();
        let bound_expr = expr.bind(schema, true).unwrap();
        assert_eq!(&format!("{bound_expr}"), "False");
        test_bound_predicate_serialize_diserialize(bound_expr);
    }

    #[test]
    fn test_bind_is_not_null() {
        let schema = table_schema_simple();
        let expr = Reference::new("foo").is_not_null();
        let bound_expr = expr.bind(schema, true).unwrap();
        assert_eq!(&format!("{bound_expr}"), "foo IS NOT NULL");
        test_bound_predicate_serialize_diserialize(bound_expr);
    }

    #[test]
    fn test_bind_is_not_null_required() {
        let schema = table_schema_simple();
        let expr = Reference::new("bar").is_not_null();
        let bound_expr = expr.bind(schema, true).unwrap();
        assert_eq!(&format!("{bound_expr}"), "True");
        test_bound_predicate_serialize_diserialize(bound_expr);
    }

    #[test]
    fn test_bind_is_nan() {
        let schema = table_schema_simple();
        let expr = Reference::new("qux").is_nan();
        let bound_expr = expr.bind(schema, true).unwrap();
        assert_eq!(&format!("{bound_expr}"), "qux IS NAN");

        let schema_string = table_schema_simple();
        let expr_string = Reference::new("foo").is_nan();
        let bound_expr_string = expr_string.bind(schema_string, true);
        assert!(bound_expr_string.is_err());
        test_bound_predicate_serialize_diserialize(bound_expr);
    }

    #[test]
    fn test_bind_is_nan_wrong_type() {
        let schema = table_schema_simple();
        let expr = Reference::new("foo").is_nan();
        let bound_expr = expr.bind(schema, true);
        assert!(bound_expr.is_err());
    }

    #[test]
    fn test_bind_is_not_nan() {
        let schema = table_schema_simple();
        let expr = Reference::new("qux").is_not_nan();
        let bound_expr = expr.bind(schema, true).unwrap();
        assert_eq!(&format!("{bound_expr}"), "qux IS NOT NAN");
        test_bound_predicate_serialize_diserialize(bound_expr);
    }

    #[test]
    fn test_bind_is_not_nan_wrong_type() {
        let schema = table_schema_simple();
        let expr = Reference::new("foo").is_not_nan();
        let bound_expr = expr.bind(schema, true);
        assert!(bound_expr.is_err());
    }

    #[test]
    fn test_bind_less_than() {
        let schema = table_schema_simple();
        let expr = Reference::new("bar").less_than(Datum::int(10));
        let bound_expr = expr.bind(schema, true).unwrap();
        assert_eq!(&format!("{bound_expr}"), "bar < 10");
        test_bound_predicate_serialize_diserialize(bound_expr);
    }

    #[test]
    fn test_bind_less_than_wrong_type() {
        let schema = table_schema_simple();
        let expr = Reference::new("bar").less_than(Datum::string("abcd"));
        let bound_expr = expr.bind(schema, true);
        assert!(bound_expr.is_err());
    }

    #[test]
    fn test_bind_less_than_or_eq() {
        let schema = table_schema_simple();
        let expr = Reference::new("bar").less_than_or_equal_to(Datum::int(10));
        let bound_expr = expr.bind(schema, true).unwrap();
        assert_eq!(&format!("{bound_expr}"), "bar <= 10");
        test_bound_predicate_serialize_diserialize(bound_expr);
    }

    #[test]
    fn test_bind_less_than_or_eq_wrong_type() {
        let schema = table_schema_simple();
        let expr = Reference::new("bar").less_than_or_equal_to(Datum::string("abcd"));
        let bound_expr = expr.bind(schema, true);
        assert!(bound_expr.is_err());
    }

    #[test]
    fn test_bind_greater_than() {
        let schema = table_schema_simple();
        let expr = Reference::new("bar").greater_than(Datum::int(10));
        let bound_expr = expr.bind(schema, true).unwrap();
        assert_eq!(&format!("{bound_expr}"), "bar > 10");
        test_bound_predicate_serialize_diserialize(bound_expr);
    }

    #[test]
    fn test_bind_greater_than_wrong_type() {
        let schema = table_schema_simple();
        let expr = Reference::new("bar").greater_than(Datum::string("abcd"));
        let bound_expr = expr.bind(schema, true);
        assert!(bound_expr.is_err());
    }

    #[test]
    fn test_bind_greater_than_or_eq() {
        let schema = table_schema_simple();
        let expr = Reference::new("bar").greater_than_or_equal_to(Datum::int(10));
        let bound_expr = expr.bind(schema, true).unwrap();
        assert_eq!(&format!("{bound_expr}"), "bar >= 10");
        test_bound_predicate_serialize_diserialize(bound_expr);
    }

    #[test]
    fn test_bind_greater_than_or_eq_wrong_type() {
        let schema = table_schema_simple();
        let expr = Reference::new("bar").greater_than_or_equal_to(Datum::string("abcd"));
        let bound_expr = expr.bind(schema, true);
        assert!(bound_expr.is_err());
    }

    #[test]
    fn test_bind_equal_to() {
        let schema = table_schema_simple();
        let expr = Reference::new("bar").equal_to(Datum::int(10));
        let bound_expr = expr.bind(schema, true).unwrap();
        assert_eq!(&format!("{bound_expr}"), "bar = 10");
        test_bound_predicate_serialize_diserialize(bound_expr);
    }

    #[test]
    fn test_bind_equal_to_wrong_type() {
        let schema = table_schema_simple();
        let expr = Reference::new("bar").equal_to(Datum::string("abcd"));
        let bound_expr = expr.bind(schema, true);
        assert!(bound_expr.is_err());
    }

    #[test]
    fn test_bind_not_equal_to() {
        let schema = table_schema_simple();
        let expr = Reference::new("bar").not_equal_to(Datum::int(10));
        let bound_expr = expr.bind(schema, true).unwrap();
        assert_eq!(&format!("{bound_expr}"), "bar != 10");
        test_bound_predicate_serialize_diserialize(bound_expr);
    }

    #[test]
    fn test_bind_not_equal_to_wrong_type() {
        let schema = table_schema_simple();
        let expr = Reference::new("bar").not_equal_to(Datum::string("abcd"));
        let bound_expr = expr.bind(schema, true);
        assert!(bound_expr.is_err());
    }

    #[test]
    fn test_bind_starts_with() {
        let schema = table_schema_simple();
        let expr = Reference::new("foo").starts_with(Datum::string("abcd"));
        let bound_expr = expr.bind(schema, true).unwrap();
        assert_eq!(&format!("{bound_expr}"), r#"foo STARTS WITH "abcd""#);
        test_bound_predicate_serialize_diserialize(bound_expr);
    }

    #[test]
    fn test_bind_starts_with_wrong_type() {
        let schema = table_schema_simple();
        let expr = Reference::new("bar").starts_with(Datum::string("abcd"));
        let bound_expr = expr.bind(schema, true);
        assert!(bound_expr.is_err());
    }

    #[test]
    fn test_bind_not_starts_with() {
        let schema = table_schema_simple();
        let expr = Reference::new("foo").not_starts_with(Datum::string("abcd"));
        let bound_expr = expr.bind(schema, true).unwrap();
        assert_eq!(&format!("{bound_expr}"), r#"foo NOT STARTS WITH "abcd""#);
        test_bound_predicate_serialize_diserialize(bound_expr);
    }

    #[test]
    fn test_bind_not_starts_with_wrong_type() {
        let schema = table_schema_simple();
        let expr = Reference::new("bar").not_starts_with(Datum::string("abcd"));
        let bound_expr = expr.bind(schema, true);
        assert!(bound_expr.is_err());
    }

    #[test]
    fn test_bind_in() {
        let schema = table_schema_simple();
        let expr = Reference::new("bar").is_in([Datum::int(10), Datum::int(20)]);
        let bound_expr = expr.bind(schema, true).unwrap();
        assert_eq!(&format!("{bound_expr}"), "bar IN (20, 10)");
        test_bound_predicate_serialize_diserialize(bound_expr);
    }

    #[test]
    fn test_bind_in_empty() {
        let schema = table_schema_simple();
        let expr = Reference::new("bar").is_in(vec![]);
        let bound_expr = expr.bind(schema, true).unwrap();
        assert_eq!(&format!("{bound_expr}"), "False");
        test_bound_predicate_serialize_diserialize(bound_expr);
    }

    #[test]
    fn test_bind_in_one_literal() {
        let schema = table_schema_simple();
        let expr = Reference::new("bar").is_in(vec![Datum::int(10)]);
        let bound_expr = expr.bind(schema, true).unwrap();
        assert_eq!(&format!("{bound_expr}"), "bar = 10");
        test_bound_predicate_serialize_diserialize(bound_expr);
    }

    #[test]
    fn test_bind_in_wrong_type() {
        let schema = table_schema_simple();
        let expr = Reference::new("bar").is_in(vec![Datum::int(10), Datum::string("abcd")]);
        let bound_expr = expr.bind(schema, true);
        assert!(bound_expr.is_err());
    }

    #[test]
    fn test_bind_not_in() {
        let schema = table_schema_simple();
        let expr = Reference::new("bar").is_not_in([Datum::int(10), Datum::int(20)]);
        let bound_expr = expr.bind(schema, true).unwrap();
        assert_eq!(&format!("{bound_expr}"), "bar NOT IN (20, 10)");
        test_bound_predicate_serialize_diserialize(bound_expr);
    }

    #[test]
    fn test_bind_not_in_empty() {
        let schema = table_schema_simple();
        let expr = Reference::new("bar").is_not_in(vec![]);
        let bound_expr = expr.bind(schema, true).unwrap();
        assert_eq!(&format!("{bound_expr}"), "True");
        test_bound_predicate_serialize_diserialize(bound_expr);
    }

    #[test]
    fn test_bind_not_in_one_literal() {
        let schema = table_schema_simple();
        let expr = Reference::new("bar").is_not_in(vec![Datum::int(10)]);
        let bound_expr = expr.bind(schema, true).unwrap();
        assert_eq!(&format!("{bound_expr}"), "bar != 10");
        test_bound_predicate_serialize_diserialize(bound_expr);
    }

    #[test]
    fn test_bind_not_in_wrong_type() {
        let schema = table_schema_simple();
        let expr = Reference::new("bar").is_not_in([Datum::int(10), Datum::string("abcd")]);
        let bound_expr = expr.bind(schema, true);
        assert!(bound_expr.is_err());
    }

    #[test]
    fn test_bind_and() {
        let schema = table_schema_simple();
        let expr = Reference::new("bar")
            .less_than(Datum::int(10))
            .and(Reference::new("foo").is_null());
        let bound_expr = expr.bind(schema, true).unwrap();
        assert_eq!(&format!("{bound_expr}"), "(bar < 10) AND (foo IS NULL)");
        test_bound_predicate_serialize_diserialize(bound_expr);
    }

    #[test]
    fn test_bind_and_always_false() {
        let schema = table_schema_simple();
        let expr = Reference::new("foo")
            .less_than(Datum::string("abcd"))
            .and(Reference::new("bar").is_null());
        let bound_expr = expr.bind(schema, true).unwrap();
        assert_eq!(&format!("{bound_expr}"), "False");
        test_bound_predicate_serialize_diserialize(bound_expr);
    }

    #[test]
    fn test_bind_and_always_true() {
        let schema = table_schema_simple();
        let expr = Reference::new("foo")
            .less_than(Datum::string("abcd"))
            .and(Reference::new("bar").is_not_null());
        let bound_expr = expr.bind(schema, true).unwrap();
        assert_eq!(&format!("{bound_expr}"), r#"foo < "abcd""#);
        test_bound_predicate_serialize_diserialize(bound_expr);
    }

    #[test]
    fn test_bind_or() {
        let schema = table_schema_simple();
        let expr = Reference::new("bar")
            .less_than(Datum::int(10))
            .or(Reference::new("foo").is_null());
        let bound_expr = expr.bind(schema, true).unwrap();
        assert_eq!(&format!("{bound_expr}"), "(bar < 10) OR (foo IS NULL)");
        test_bound_predicate_serialize_diserialize(bound_expr);
    }

    #[test]
    fn test_bind_or_always_true() {
        let schema = table_schema_simple();
        let expr = Reference::new("foo")
            .less_than(Datum::string("abcd"))
            .or(Reference::new("bar").is_not_null());
        let bound_expr = expr.bind(schema, true).unwrap();
        assert_eq!(&format!("{bound_expr}"), "True");
        test_bound_predicate_serialize_diserialize(bound_expr);
    }

    #[test]
    fn test_bind_or_always_false() {
        let schema = table_schema_simple();
        let expr = Reference::new("foo")
            .less_than(Datum::string("abcd"))
            .or(Reference::new("bar").is_null());
        let bound_expr = expr.bind(schema, true).unwrap();
        assert_eq!(&format!("{bound_expr}"), r#"foo < "abcd""#);
        test_bound_predicate_serialize_diserialize(bound_expr);
    }

    #[test]
    fn test_bind_not() {
        let schema = table_schema_simple();
        let expr = !Reference::new("bar").less_than(Datum::int(10));
        let bound_expr = expr.bind(schema, true).unwrap();
        assert_eq!(&format!("{bound_expr}"), "NOT (bar < 10)");
        test_bound_predicate_serialize_diserialize(bound_expr);
    }

    #[test]
    fn test_bind_not_always_true() {
        let schema = table_schema_simple();
        let expr = !Reference::new("bar").is_not_null();
        let bound_expr = expr.bind(schema, true).unwrap();
        assert_eq!(&format!("{bound_expr}"), "False");
        test_bound_predicate_serialize_diserialize(bound_expr);
    }

    #[test]
    fn test_bind_not_always_false() {
        let schema = table_schema_simple();
        let expr = !Reference::new("bar").is_null();
        let bound_expr = expr.bind(schema, true).unwrap();
        assert_eq!(&format!("{bound_expr}"), r#"True"#);
        test_bound_predicate_serialize_diserialize(bound_expr);
    }
}
