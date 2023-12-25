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

use crate::spec::NestedFieldRef;

/// Unbound term before binding to a schema.
pub type UnboundTerm = UnboundReference;

/// A named reference in an unbound expression.
/// For example, `a` in `a > 10`.
pub struct UnboundReference {
    name: String,
}

/// A named reference in a bound expression after binding to a schema.
pub struct BoundReference {
    field: NestedFieldRef,
}

/// Bound term after binding to a schema.
pub type BoundTerm = BoundReference;
