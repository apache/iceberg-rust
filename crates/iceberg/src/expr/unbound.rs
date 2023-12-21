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

//! This module defines unbound expressions.

use crate::error::Result;
use crate::expr::bound::Bound;
use crate::expr::Operator;
use crate::spec::{Literal, Schema};

/// Unbound expressions.
pub enum Unbound {
    /// Constants, such as 1, 'a', true, false, null.
    Literal(Literal),
    /// References to fields, such as `col` or `tbl.col`.
    Reference(String),
    /// Expressions
    Expr {
        /// Operator for this expression, such as `AND` or `OR`.
        operator: Operator,
        /// Arguments of this express.
        inputs: Vec<Unbound>,
    },
}

impl Unbound {
    /// Bind expression to schema.
    pub fn bind(&self, _schema: &Schema, _case_sensitive: bool) -> Result<Bound> {
        todo!()
    }
}
