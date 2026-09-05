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

use fnv::FnvHashSet;

use crate::spec::Datum;

pub(crate) mod bound_predicate_visitor;
pub(crate) mod expression_evaluator;
pub(crate) mod inclusive_metrics_evaluator;
pub(crate) mod inclusive_projection;
pub(crate) mod manifest_evaluator;
pub(crate) mod page_index_evaluator;
pub(crate) mod predicate_visitor;
pub(crate) mod rewrite_not;
pub(crate) mod row_group_metrics_evaluator;
pub(crate) mod strict_metrics_evaluator;
pub(crate) mod strict_projection;

/// Returns true if any literal could match the inclusive `[lower, upper]` range.
/// Missing bounds are treated as unbounded on that side.
pub(crate) fn any_literal_in_bounds(
    lower: Option<&Datum>,
    upper: Option<&Datum>,
    literals: &FnvHashSet<Datum>,
) -> bool {
    match (lower, upper) {
        (Some(lower), Some(upper)) => literals
            .iter()
            .any(|datum| datum.ge(lower) && datum.le(upper)),
        (Some(lower), None) => literals.iter().any(|datum| datum.ge(lower)),
        (None, Some(upper)) => literals.iter().any(|datum| datum.le(upper)),
        (None, None) => true,
    }
}
