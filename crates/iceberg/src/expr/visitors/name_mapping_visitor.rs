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

use crate::spec::MappedField;

/// A trait for visiting and transforming a name mapping
pub trait NameMappingVisitor {
    /// Aggregated result of `MappedField`s
    type S;
    /// Result type for processing one `MappedField`
    type T;

    /// Handles entire `NameMapping` field
    fn mapping(&self, field_result: Self::S) -> Self::S;

    /// Takes all visited child maps and merges into one map
    fn fields(&self, field_results: Vec<Self::T>) -> Self::S;

    /// Handles a single `MappedField`
    fn field(&self, field: &MappedField, field_result: Self::S) -> Self::T;
}
