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

use crate::expr::{BinaryExpression, Predicate, PredicateOperator};
use crate::spec::{Datum, NestedField, NestedFieldRef};
use std::fmt::{Display, Formatter};

/// Unbound term before binding to a schema.
pub type Term = Reference;

/// A named reference in an unbound expression.
/// For example, `a` in `a > 10`.
#[derive(Debug, Clone, PartialEq)]
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
    ///
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr = Reference::new("a").less_than(Datum::long(10));
    ///
    /// assert_eq!(&format!("{expr}"), "a < 10");
    /// ```
    pub fn less_than(self, datum: Datum) -> Predicate {
        Predicate::Binary(BinaryExpression::new(
            PredicateOperator::LessThan,
            self,
            datum,
        ))
    }
}

impl Display for Reference {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

/// A named reference in a bound expression after binding to a schema.
#[derive(Debug, Clone)]
pub struct BoundReference {
    // This maybe different from [`name`] filed in [`NestedField`] since this contains full path.
    // For example, if the field is `a.b.c`, then `field.name` is `c`, but `original_name` is `a.b.c`.
    column_name: String,
    field: NestedFieldRef,
}

impl BoundReference {
    /// Creates a new bound reference.
    pub fn new(name: impl Into<String>, field: NestedFieldRef) -> Self {
        Self {
            column_name: name.into(),
            field,
        }
    }

    /// Return the field of this reference.
    pub fn field(&self) -> &NestedField {
        &self.field
    }
}

impl Display for BoundReference {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.column_name)
    }
}

/// Bound term after binding to a schema.
pub type BoundTerm = BoundReference;
