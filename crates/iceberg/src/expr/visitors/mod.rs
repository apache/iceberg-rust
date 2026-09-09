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
///
/// `(None, None)` returns true because no bound is available to prune against.
/// Manifest evaluation must not reach this helper when the partition summary
/// has no lower bound: that case is all-null and `IN` prunes before calling
/// here. Metrics evaluators use `(None, None)` for a missing min/max pair.
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

/// Drops a NaN bound so that side is treated as unbounded.
///
/// A NaN min or max is unreliable, but the other bound may still prune.
pub(crate) fn finite_bound(bound: Option<&Datum>) -> Option<&Datum> {
    bound.filter(|datum| !datum.is_nan())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn floats(vals: &[f32]) -> FnvHashSet<Datum> {
        vals.iter().copied().map(Datum::float).collect()
    }

    #[test]
    fn both_bounds_require_a_literal_inside_the_range() {
        let lower = Datum::float(4.0_f32);
        let upper = Datum::float(6.0_f32);
        assert!(!any_literal_in_bounds(
            Some(&lower),
            Some(&upper),
            &floats(&[2.0, 8.0])
        ));
        assert!(any_literal_in_bounds(
            Some(&lower),
            Some(&upper),
            &floats(&[2.0, 5.0])
        ));
    }

    #[test]
    fn lower_only_prunes_literals_below_the_bound() {
        let lower = Datum::float(4.0_f32);
        assert!(!any_literal_in_bounds(
            Some(&lower),
            None,
            &floats(&[2.0, 3.0])
        ));
        assert!(any_literal_in_bounds(
            Some(&lower),
            None,
            &floats(&[2.0, 4.0])
        ));
    }

    #[test]
    fn upper_only_prunes_literals_above_the_bound() {
        let upper = Datum::float(1.0_f32);
        assert!(!any_literal_in_bounds(
            None,
            Some(&upper),
            &floats(&[2.0, 3.0])
        ));
        assert!(any_literal_in_bounds(
            None,
            Some(&upper),
            &floats(&[0.5, 3.0])
        ));
    }

    #[test]
    fn neither_bound_cannot_prune() {
        assert!(any_literal_in_bounds(None, None, &floats(&[2.0, 3.0])));
    }

    #[test]
    fn finite_bound_drops_nan() {
        let nan = Datum::float(f32::NAN);
        let finite = Datum::float(4.0_f32);
        assert!(finite_bound(Some(&nan)).is_none());
        assert_eq!(finite_bound(Some(&finite)), Some(&finite));
        assert!(finite_bound(None).is_none());
    }
}
