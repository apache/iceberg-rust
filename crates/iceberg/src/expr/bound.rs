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

//! This module defines bound expressions.

use crate::expr::Operator;
use crate::spec::{Literal, NestedFieldRef};

/// Bound expression.
pub enum Bound {
    /// Constants such as 1, 'a', true, false, null.
    Literal(Literal),
    /// Reference to some field in schema
    Reference {
        /// Nested field found in schema.
        field: NestedFieldRef,
    },
    /// Expressions
    Expr {
        /// Operator for this expression, such as `And`, `Or`, `IsNull`, `Eq`, `LessThan`.
        op: Operator,
        /// Arguments for this expression. For example, `a > b` would have `a` and `b` as [`inputs`]
        inputs: Vec<Bound>,
    },
}
