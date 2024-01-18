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

//! This module contains expressions.

mod term;

use std::fmt::{Display, Formatter};
pub use term::*;
mod predicate;
pub use predicate::*;

/// Predicate operators used in expressions.
#[allow(missing_docs)]
#[derive(Debug, Clone, Copy)]
pub enum PredicateOperator {
    IsNull,
    NotNull,
    IsNan,
    NotNan,
    LessThan,
    LessThanOrEq,
    GreaterThan,
    GreaterThanOrEq,
    Eq,
    NotEq,
    In,
    NotIn,
    StartsWith,
    NotStartsWith,
}

impl Display for PredicateOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PredicateOperator::IsNull => write!(f, "IS NULL"),
            PredicateOperator::NotNull => write!(f, "IS NOT NULL"),
            PredicateOperator::IsNan => write!(f, "IS NAN"),
            PredicateOperator::NotNan => write!(f, "IS NOT NAN"),
            PredicateOperator::LessThan => write!(f, "<"),
            PredicateOperator::LessThanOrEq => write!(f, "<="),
            PredicateOperator::GreaterThan => write!(f, ">"),
            PredicateOperator::GreaterThanOrEq => write!(f, ">="),
            PredicateOperator::Eq => write!(f, "="),
            PredicateOperator::NotEq => write!(f, "!="),
            PredicateOperator::In => write!(f, "IN"),
            PredicateOperator::NotIn => write!(f, "NOT IN"),
            PredicateOperator::StartsWith => write!(f, "STARTS WITH"),
            PredicateOperator::NotStartsWith => write!(f, "NOT STARTS WITH"),
        }
    }
}
