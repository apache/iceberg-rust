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

//! Term definition.

use std::fmt::{Display, Formatter};

use fnv::FnvHashSet;
use serde::{Deserialize, Serialize};

use crate::expr::accessor::{StructAccessor, StructAccessorRef};
use crate::expr::{
    BinaryExpression, Bind, Predicate, PredicateOperator, SetExpression, UnaryExpression,
};
use crate::spec::{Datum, NestedField, NestedFieldRef, SchemaRef, Transform, Type};
use crate::{Error, ErrorKind, Result};

/// Unbound term before binding to a schema.
/// Either a bare column reference or a transform applied to a reference.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Term {
    /// A bare column reference, e.g., `a` in `a > 10`.
    Reference(Reference),
    /// A transform applied to a reference, e.g., `bucket(10, col_a)`.
    Transform(TransformTerm),
}

impl Term {
    /// Returns the underlying [`Reference`] regardless of variant.
    pub fn reference(&self) -> &Reference {
        match self {
            Term::Reference(r) => r,
            Term::Transform(t) => t.reference(),
        }
    }

    /// Returns true if this term is a transform term.
    pub fn is_transform(&self) -> bool {
        matches!(self, Term::Transform(_))
    }
}

impl From<Reference> for Term {
    fn from(r: Reference) -> Self {
        Term::Reference(r)
    }
}

impl From<TransformTerm> for Term {
    fn from(t: TransformTerm) -> Self {
        Term::Transform(t)
    }
}

impl Display for Term {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Term::Reference(r) => write!(f, "{}", r),
            Term::Transform(t) => write!(f, "{}", t),
        }
    }
}

impl Bind for Term {
    type Bound = BoundTerm;

    fn bind(&self, schema: SchemaRef, case_sensitive: bool) -> crate::Result<Self::Bound> {
        match self {
            Term::Reference(r) => Ok(BoundTerm::Reference(r.bind(schema, case_sensitive)?)),
            Term::Transform(t) => Ok(BoundTerm::Transform(t.bind(schema, case_sensitive)?)),
        }
    }
}

/// Bound term after binding to a schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BoundTerm {
    /// A bound column reference.
    Reference(BoundReference),
    /// A bound transform term.
    Transform(BoundTransformTerm),
}

impl BoundTerm {
    /// Returns the underlying [`BoundReference`] regardless of variant.
    pub fn reference(&self) -> &BoundReference {
        match self {
            BoundTerm::Reference(r) => r,
            BoundTerm::Transform(t) => t.reference(),
        }
    }

    /// Returns the underlying [`StructAccessor`] of the reference.
    pub fn accessor(&self) -> &StructAccessor {
        self.reference().accessor()
    }

    /// Returns the underlying field of the reference.
    pub fn field(&self) -> &NestedField {
        self.reference().field()
    }

    /// Returns the result type of this term.
    pub fn result_type(&self) -> Result<Type> {
        match self {
            BoundTerm::Reference(r) => Ok(r.field().field_type.as_ref().clone()),
            BoundTerm::Transform(t) => t
                .transform()
                .result_type(t.reference().field().field_type.as_ref()),
        }
    }
}

impl From<BoundReference> for BoundTerm {
    fn from(r: BoundReference) -> Self {
        BoundTerm::Reference(r)
    }
}

impl From<BoundTransformTerm> for BoundTerm {
    fn from(t: BoundTransformTerm) -> Self {
        BoundTerm::Transform(t)
    }
}

impl Display for BoundTerm {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BoundTerm::Reference(r) => write!(f, "{}", r),
            BoundTerm::Transform(t) => write!(f, "{}", t),
        }
    }
}

/// A named reference in an unbound expression.
/// For example, `a` in `a > 10`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Reference {
    name: String,
}

impl Reference {
    /// Create a new unbound reference.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    /// Return the name of this reference.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Reference {
    /// Creates an less than expression. For example, `a < 10`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr = Reference::new("a").less_than(Datum::long(10));
    ///
    /// assert_eq!(&format!("{expr}"), "a < 10");
    /// ```
    pub fn less_than(self, datum: Datum) -> Predicate {
        Predicate::Binary(BinaryExpression::new(
            PredicateOperator::LessThan,
            self.into(),
            datum,
        ))
    }

    /// Creates an less than or equal to expression. For example, `a <= 10`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr = Reference::new("a").less_than_or_equal_to(Datum::long(10));
    ///
    /// assert_eq!(&format!("{expr}"), "a <= 10");
    /// ```
    pub fn less_than_or_equal_to(self, datum: Datum) -> Predicate {
        Predicate::Binary(BinaryExpression::new(
            PredicateOperator::LessThanOrEq,
            self.into(),
            datum,
        ))
    }

    /// Creates an greater than expression. For example, `a > 10`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr = Reference::new("a").greater_than(Datum::long(10));
    ///
    /// assert_eq!(&format!("{expr}"), "a > 10");
    /// ```
    pub fn greater_than(self, datum: Datum) -> Predicate {
        Predicate::Binary(BinaryExpression::new(
            PredicateOperator::GreaterThan,
            self.into(),
            datum,
        ))
    }

    /// Creates a greater-than-or-equal-to than expression. For example, `a >= 10`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr = Reference::new("a").greater_than_or_equal_to(Datum::long(10));
    ///
    /// assert_eq!(&format!("{expr}"), "a >= 10");
    /// ```
    pub fn greater_than_or_equal_to(self, datum: Datum) -> Predicate {
        Predicate::Binary(BinaryExpression::new(
            PredicateOperator::GreaterThanOrEq,
            self.into(),
            datum,
        ))
    }

    /// Creates an equal-to expression. For example, `a = 10`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr = Reference::new("a").equal_to(Datum::long(10));
    ///
    /// assert_eq!(&format!("{expr}"), "a = 10");
    /// ```
    pub fn equal_to(self, datum: Datum) -> Predicate {
        Predicate::Binary(BinaryExpression::new(
            PredicateOperator::Eq,
            self.into(),
            datum,
        ))
    }

    /// Creates a not equal-to expression. For example, `a!= 10`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr = Reference::new("a").not_equal_to(Datum::long(10));
    ///
    /// assert_eq!(&format!("{expr}"), "a != 10");
    /// ```
    pub fn not_equal_to(self, datum: Datum) -> Predicate {
        Predicate::Binary(BinaryExpression::new(
            PredicateOperator::NotEq,
            self.into(),
            datum,
        ))
    }

    /// Creates a start-with expression. For example, `a STARTS WITH "foo"`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr = Reference::new("a").starts_with(Datum::string("foo"));
    ///
    /// assert_eq!(&format!("{expr}"), r#"a STARTS WITH "foo""#);
    /// ```
    pub fn starts_with(self, datum: Datum) -> Predicate {
        Predicate::Binary(BinaryExpression::new(
            PredicateOperator::StartsWith,
            self.into(),
            datum,
        ))
    }

    /// Creates a not start-with expression. For example, `a NOT STARTS WITH 'foo'`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    ///
    /// let expr = Reference::new("a").not_starts_with(Datum::string("foo"));
    ///
    /// assert_eq!(&format!("{expr}"), r#"a NOT STARTS WITH "foo""#);
    /// ```
    pub fn not_starts_with(self, datum: Datum) -> Predicate {
        Predicate::Binary(BinaryExpression::new(
            PredicateOperator::NotStartsWith,
            self.into(),
            datum,
        ))
    }

    /// Creates an is-nan expression. For example, `a IS NAN`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr = Reference::new("a").is_nan();
    ///
    /// assert_eq!(&format!("{expr}"), "a IS NAN");
    /// ```
    pub fn is_nan(self) -> Predicate {
        Predicate::Unary(UnaryExpression::new(PredicateOperator::IsNan, self.into()))
    }

    /// Creates an is-not-nan expression. For example, `a IS NOT NAN`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr = Reference::new("a").is_not_nan();
    ///
    /// assert_eq!(&format!("{expr}"), "a IS NOT NAN");
    /// ```
    pub fn is_not_nan(self) -> Predicate {
        Predicate::Unary(UnaryExpression::new(PredicateOperator::NotNan, self.into()))
    }

    /// Creates an is-null expression. For example, `a IS NULL`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr = Reference::new("a").is_null();
    ///
    /// assert_eq!(&format!("{expr}"), "a IS NULL");
    /// ```
    pub fn is_null(self) -> Predicate {
        Predicate::Unary(UnaryExpression::new(PredicateOperator::IsNull, self.into()))
    }

    /// Creates an is-not-null expression. For example, `a IS NOT NULL`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr = Reference::new("a").is_not_null();
    ///
    /// assert_eq!(&format!("{expr}"), "a IS NOT NULL");
    /// ```
    pub fn is_not_null(self) -> Predicate {
        Predicate::Unary(UnaryExpression::new(
            PredicateOperator::NotNull,
            self.into(),
        ))
    }

    /// Creates an is-in expression. For example, `a IN (5, 6)`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use fnv::FnvHashSet;
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr = Reference::new("a").is_in([Datum::long(5), Datum::long(6)]);
    ///
    /// let as_string = format!("{expr}");
    /// assert!(&as_string == "a IN (5, 6)" || &as_string == "a IN (6, 5)");
    /// ```
    pub fn is_in(self, literals: impl IntoIterator<Item = Datum>) -> Predicate {
        Predicate::Set(SetExpression::new(
            PredicateOperator::In,
            self.into(),
            FnvHashSet::from_iter(literals),
        ))
    }

    /// Creates an is-not-in expression. For example, `a NOT IN (5, 6)`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use fnv::FnvHashSet;
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr = Reference::new("a").is_not_in([Datum::long(5), Datum::long(6)]);
    ///
    /// let as_string = format!("{expr}");
    /// assert!(&as_string == "a NOT IN (5, 6)" || &as_string == "a NOT IN (6, 5)");
    /// ```
    pub fn is_not_in(self, literals: impl IntoIterator<Item = Datum>) -> Predicate {
        Predicate::Set(SetExpression::new(
            PredicateOperator::NotIn,
            self.into(),
            FnvHashSet::from_iter(literals),
        ))
    }
}

impl Display for Reference {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl Bind for Reference {
    type Bound = BoundReference;

    fn bind(&self, schema: SchemaRef, case_sensitive: bool) -> crate::Result<Self::Bound> {
        let field = if case_sensitive {
            schema.field_by_name(&self.name)
        } else {
            schema.field_by_name_case_insensitive(&self.name)
        };

        let field = field.ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Field {} not found in schema", self.name),
            )
        })?;

        let accessor = schema.accessor_by_field_id(field.id).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Accessor for Field {} not found", self.name),
            )
        })?;

        Ok(BoundReference::new(
            self.name.clone(),
            field.clone(),
            accessor.clone(),
        ))
    }
}

/// A named reference in a bound expression after binding to a schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundReference {
    // This maybe different from [`name`] filed in [`NestedField`] since this contains full path.
    // For example, if the field is `a.b.c`, then `field.name` is `c`, but `original_name` is `a.b.c`.
    column_name: String,
    field: NestedFieldRef,
    accessor: StructAccessorRef,
}

impl BoundReference {
    /// Creates a new bound reference.
    pub fn new(
        name: impl Into<String>,
        field: NestedFieldRef,
        accessor: StructAccessorRef,
    ) -> Self {
        Self {
            column_name: name.into(),
            field,
            accessor,
        }
    }

    /// Return the field of this reference.
    pub fn field(&self) -> &NestedField {
        &self.field
    }

    /// Get this BoundReference's Accessor
    pub fn accessor(&self) -> &StructAccessor {
        &self.accessor
    }
}

impl Display for BoundReference {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.column_name)
    }
}

/// An unbound term consisting of a transform applied to a reference.
/// For example, `bucket(10, col_a)` in `bucket(10, col_a) = 3`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransformTerm {
    transform: Transform,
    reference: Reference,
}

impl TransformTerm {
    /// Create a new transform term.
    pub fn new(transform: Transform, reference: Reference) -> Self {
        Self {
            transform,
            reference,
        }
    }

    /// Create a bucket transform term. For example, `bucket(10, col_a)`.
    pub fn bucket(name: impl Into<String>, n: u32) -> Self {
        Self::new(Transform::Bucket(n), Reference::new(name))
    }

    /// Create a truncate transform term. For example, `truncate(4, name)`.
    pub fn truncate(name: impl Into<String>, width: u32) -> Self {
        Self::new(Transform::Truncate(width), Reference::new(name))
    }

    /// Create a year transform term. For example, `year(ts)`.
    pub fn year(name: impl Into<String>) -> Self {
        Self::new(Transform::Year, Reference::new(name))
    }

    /// Create a month transform term. For example, `month(ts)`.
    pub fn month(name: impl Into<String>) -> Self {
        Self::new(Transform::Month, Reference::new(name))
    }

    /// Create a day transform term. For example, `day(ts)`.
    pub fn day(name: impl Into<String>) -> Self {
        Self::new(Transform::Day, Reference::new(name))
    }

    /// Create an hour transform term. For example, `hour(ts)`.
    pub fn hour(name: impl Into<String>) -> Self {
        Self::new(Transform::Hour, Reference::new(name))
    }

    /// Returns the transform of this term.
    pub fn transform(&self) -> &Transform {
        &self.transform
    }

    /// Returns the reference of this term.
    pub fn reference(&self) -> &Reference {
        &self.reference
    }
}

impl TransformTerm {
    /// Creates an equal-to expression. For example, `bucket(10, col_a) = 3`.
    pub fn equal_to(self, datum: Datum) -> Predicate {
        Predicate::Binary(BinaryExpression::new(
            PredicateOperator::Eq,
            Term::Transform(self),
            datum,
        ))
    }

    /// Creates a not-equal-to expression.
    pub fn not_equal_to(self, datum: Datum) -> Predicate {
        Predicate::Binary(BinaryExpression::new(
            PredicateOperator::NotEq,
            Term::Transform(self),
            datum,
        ))
    }

    /// Creates a less-than expression.
    pub fn less_than(self, datum: Datum) -> Predicate {
        Predicate::Binary(BinaryExpression::new(
            PredicateOperator::LessThan,
            Term::Transform(self),
            datum,
        ))
    }

    /// Creates a less-than-or-equal-to expression.
    pub fn less_than_or_equal_to(self, datum: Datum) -> Predicate {
        Predicate::Binary(BinaryExpression::new(
            PredicateOperator::LessThanOrEq,
            Term::Transform(self),
            datum,
        ))
    }

    /// Creates a greater-than expression.
    pub fn greater_than(self, datum: Datum) -> Predicate {
        Predicate::Binary(BinaryExpression::new(
            PredicateOperator::GreaterThan,
            Term::Transform(self),
            datum,
        ))
    }

    /// Creates a greater-than-or-equal-to expression.
    pub fn greater_than_or_equal_to(self, datum: Datum) -> Predicate {
        Predicate::Binary(BinaryExpression::new(
            PredicateOperator::GreaterThanOrEq,
            Term::Transform(self),
            datum,
        ))
    }

    /// Creates an is-in expression.
    pub fn is_in(self, literals: impl IntoIterator<Item = Datum>) -> Predicate {
        Predicate::Set(SetExpression::new(
            PredicateOperator::In,
            Term::Transform(self),
            FnvHashSet::from_iter(literals),
        ))
    }

    /// Creates a not-in expression.
    pub fn is_not_in(self, literals: impl IntoIterator<Item = Datum>) -> Predicate {
        Predicate::Set(SetExpression::new(
            PredicateOperator::NotIn,
            Term::Transform(self),
            FnvHashSet::from_iter(literals),
        ))
    }
}

impl Display for TransformTerm {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}(ref(name=\"{}\"))",
            self.transform, self.reference.name
        )
    }
}

impl Bind for TransformTerm {
    type Bound = BoundTransformTerm;

    fn bind(&self, schema: SchemaRef, case_sensitive: bool) -> crate::Result<Self::Bound> {
        let bound_ref = self.reference.bind(schema, case_sensitive)?;
        // Validate the transform is compatible with the field type
        self.transform
            .result_type(bound_ref.field().field_type.as_ref())
            .map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Transform {} is not compatible with field type {}: {}",
                        self.transform,
                        bound_ref.field().field_type,
                        e
                    ),
                )
            })?;
        Ok(BoundTransformTerm::new(self.transform, bound_ref))
    }
}

/// A bound term consisting of a transform applied to a bound reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundTransformTerm {
    transform: Transform,
    reference: BoundReference,
}

impl BoundTransformTerm {
    /// Creates a new bound transform term.
    pub fn new(transform: Transform, reference: BoundReference) -> Self {
        Self {
            transform,
            reference,
        }
    }

    /// Returns the transform of this term.
    pub fn transform(&self) -> &Transform {
        &self.transform
    }

    /// Returns the bound reference of this term.
    pub fn reference(&self) -> &BoundReference {
        &self.reference
    }
}

impl Display for BoundTransformTerm {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}(ref(id={}, accessor-type={}))",
            self.transform,
            self.reference.field().id,
            self.reference.accessor().r#type()
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::expr::accessor::StructAccessor;
    use crate::expr::{Bind, BoundReference, Reference, TransformTerm};
    use crate::spec::{Datum, NestedField, PrimitiveType, Schema, SchemaRef, Transform, Type};

    fn table_schema_simple() -> SchemaRef {
        Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_identifier_field_ids(vec![2])
                .with_fields(vec![
                    NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
                ])
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn test_bind_reference() {
        let schema = table_schema_simple();
        let reference = Reference::new("bar").bind(schema, true).unwrap();

        let accessor_ref = Arc::new(StructAccessor::new(1, PrimitiveType::Int));
        let expected_ref = BoundReference::new(
            "bar",
            NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
            accessor_ref.clone(),
        );

        assert_eq!(expected_ref, reference);
    }

    #[test]
    fn test_bind_reference_case_insensitive() {
        let schema = table_schema_simple();
        let reference = Reference::new("BAR").bind(schema, false).unwrap();

        let accessor_ref = Arc::new(StructAccessor::new(1, PrimitiveType::Int));
        let expected_ref = BoundReference::new(
            "BAR",
            NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
            accessor_ref.clone(),
        );

        assert_eq!(expected_ref, reference);
    }

    #[test]
    fn test_bind_reference_failure() {
        let schema = table_schema_simple();
        let result = Reference::new("bar_not_eix").bind(schema, true);

        assert!(result.is_err());
    }

    #[test]
    fn test_bind_reference_case_insensitive_failure() {
        let schema = table_schema_simple();
        let result = Reference::new("bar_non_exist").bind(schema, false);
        assert!(result.is_err());
    }

    #[test]
    fn test_transform_term_bucket_display() {
        let term = TransformTerm::bucket("col_a", 10);
        assert_eq!(format!("{}", term), "bucket[10](ref(name=\"col_a\"))");
    }

    #[test]
    fn test_transform_term_truncate_display() {
        let term = TransformTerm::truncate("name", 4);
        assert_eq!(format!("{}", term), "truncate[4](ref(name=\"name\"))");
    }

    #[test]
    fn test_transform_term_bind() {
        let schema = table_schema_simple();
        let term = TransformTerm::bucket("bar", 10);
        let bound = term.bind(schema, true).unwrap();
        assert_eq!(bound.transform(), &Transform::Bucket(10));
        assert_eq!(bound.reference().field().name, "bar");
        // bound display matches Java BoundTransform.toString()
        assert_eq!(
            format!("{}", bound),
            "bucket[10](ref(id=2, accessor-type=int))"
        );
    }

    #[test]
    fn test_transform_term_bind_incompatible_type() {
        let schema = table_schema_simple();
        // boolean field is not compatible with bucket transform
        let term = TransformTerm::bucket("baz", 10);
        let result = term.bind(schema, true);
        assert!(result.is_err());
    }

    #[test]
    fn test_transform_term_build_predicate() {
        let predicate = TransformTerm::bucket("bar", 10).equal_to(Datum::int(3));
        assert_eq!(
            format!("{}", predicate),
            "bucket[10](ref(name=\"bar\")) = 3"
        );
    }

    #[test]
    fn test_transform_term_range_predicate() {
        let predicate = TransformTerm::bucket("bar", 10)
            .greater_than_or_equal_to(Datum::int(0))
            .and(TransformTerm::bucket("bar", 10).less_than(Datum::int(5)));
        assert_eq!(
            format!("{}", predicate),
            "(bucket[10](ref(name=\"bar\")) >= 0) AND (bucket[10](ref(name=\"bar\")) < 5)"
        );
    }
}
