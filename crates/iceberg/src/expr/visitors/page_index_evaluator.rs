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

//! Evaluates predicates against a Parquet Page Index

use std::collections::HashMap;

use fnv::FnvHashSet;
use ordered_float::OrderedFloat;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::page_index::index::Index;
use parquet::format::PageLocation;

use crate::expr::visitors::bound_predicate_visitor::{visit, BoundPredicateVisitor};
use crate::expr::{BoundPredicate, BoundReference};
use crate::spec::{Datum, PrimitiveLiteral, PrimitiveType, Schema};
use crate::{Error, ErrorKind, Result};

type OffsetIndex = Vec<Vec<PageLocation>>;

const IN_PREDICATE_LIMIT: usize = 200;

enum MissingColBehavior {
    CantMatch,
    MightMatch,
}

enum PageNullCount {
    AllNull,
    NoneNull,
    SomeNull,
    Unknown,
}

impl PageNullCount {
    fn from_row_and_null_counts(num_rows: usize, null_count: Option<i64>) -> Self {
        match (num_rows, null_count) {
            (x, Some(y)) if x == y as usize => PageNullCount::AllNull,
            (_, Some(0)) => PageNullCount::NoneNull,
            (_, Some(_)) => PageNullCount::SomeNull,
            _ => PageNullCount::Unknown,
        }
    }
}

pub(crate) struct PageIndexEvaluator<'a> {
    column_index: &'a [Index],
    offset_index: &'a OffsetIndex,
    row_group_metadata: &'a RowGroupMetaData,
    iceberg_field_id_to_parquet_column_index: &'a HashMap<i32, usize>,
    snapshot_schema: &'a Schema,
}

impl<'a> PageIndexEvaluator<'a> {
    pub(crate) fn new(
        column_index: &'a [Index],
        offset_index: &'a OffsetIndex,
        row_group_metadata: &'a RowGroupMetaData,
        field_id_map: &'a HashMap<i32, usize>,
        snapshot_schema: &'a Schema,
    ) -> Self {
        Self {
            column_index,
            offset_index,
            row_group_metadata,
            iceberg_field_id_to_parquet_column_index: field_id_map,
            snapshot_schema,
        }
    }

    /// Evaluate this `PageIndexEvaluator`'s filter predicate against a
    /// specific page's column index entry in a parquet file's page index.
    /// [`ArrowReader`] uses the resulting [`RowSelection`] to reject
    /// pages within a parquet file's row group that cannot contain rows
    /// matching the filter predicate.
    pub(crate) fn eval(
        filter: &'a BoundPredicate,
        column_index: &'a [Index],
        offset_index: &'a OffsetIndex,
        row_group_metadata: &'a RowGroupMetaData,
        field_id_map: &'a HashMap<i32, usize>,
        snapshot_schema: &'a Schema,
    ) -> Result<Vec<RowSelector>> {
        if row_group_metadata.num_rows() == 0 {
            return Ok(vec![]);
        }

        let mut evaluator = Self::new(
            column_index,
            offset_index,
            row_group_metadata,
            field_id_map,
            snapshot_schema,
        );

        Ok(visit(&mut evaluator, filter)?.iter().copied().collect())
    }

    fn select_all_rows(&self) -> Result<RowSelection> {
        Ok(vec![RowSelector::select(
            self.row_group_metadata.num_rows() as usize
        )]
        .into())
    }

    fn skip_all_rows(&self) -> Result<RowSelection> {
        Ok(vec![].into())
    }

    fn calc_row_selection<F>(
        &self,
        field_id: i32,
        predicate: F,
        missing_col_behavior: MissingColBehavior,
    ) -> Result<RowSelection>
    where
        F: Fn(Option<Datum>, Option<Datum>, PageNullCount) -> Result<bool>,
    {
        let Some(&parquet_column_index) =
            self.iceberg_field_id_to_parquet_column_index.get(&field_id)
        else {
            // if the snapshot's column is not present in the row group,
            // exit early
            return match missing_col_behavior {
                MissingColBehavior::CantMatch => self.skip_all_rows(),
                MissingColBehavior::MightMatch => self.select_all_rows(),
            };
        };

        let Some(field) = self.snapshot_schema.field_by_id(field_id) else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("Field with id {} missing from snapshot schema", field_id),
            ));
        };

        let Some(field_type) = field.field_type.as_primitive_type() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!(
                    "Field with id {} not convertible to primitive type",
                    field_id
                ),
            ));
        };

        let Some(column_index) = self.column_index.get(parquet_column_index) else {
            // This should not happen, but we fail soft anyway so that the scan is still
            // successful, just a bit slower
            return self.select_all_rows();
        };

        let Some(offset_index) = self.offset_index.get(parquet_column_index) else {
            // if we have a column index, we should always have an offset index.
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("Missing offset index for field id {}", field_id),
            ));
        };

        let row_counts = self.calc_row_counts(offset_index);

        let Some(page_filter) = Self::apply_predicate_to_column_index(
            predicate,
            field_type,
            column_index,
            &row_counts,
        )?
        else {
            return self.select_all_rows();
        };

        let row_selectors: Vec<_> = row_counts
            .iter()
            .zip(page_filter.iter())
            .map(|(&row_count, &is_selected)| {
                if is_selected {
                    RowSelector::select(row_count)
                } else {
                    RowSelector::skip(row_count)
                }
            })
            .collect();

        Ok(row_selectors.into())
    }

    fn calc_row_counts(&self, offset_index: &[PageLocation]) -> Vec<usize> {
        let mut remaining_rows = self.row_group_metadata.num_rows() as usize;

        let mut row_counts = Vec::with_capacity(self.column_index.len());
        for (idx, page_location) in offset_index.iter().enumerate() {
            let row_count = if idx < offset_index.len() - 1 {
                let row_count = (offset_index[idx + 1].first_row_index
                    - page_location.first_row_index) as usize;
                remaining_rows -= row_count;
                row_count
            } else {
                remaining_rows
            };
            row_counts.push(row_count);
        }

        row_counts
    }

    fn apply_predicate_to_column_index<F>(
        predicate: F,
        field_type: &PrimitiveType,
        column_index: &Index,
        row_counts: &[usize],
    ) -> Result<Option<Vec<bool>>>
    where
        F: Fn(Option<Datum>, Option<Datum>, PageNullCount) -> Result<bool>,
    {
        let result: Result<Vec<bool>> = match column_index {
            Index::NONE => {
                return Ok(None);
            }
            Index::BOOLEAN(idx) => idx
                .indexes
                .iter()
                .zip(row_counts.iter())
                .map(|(item, &row_count)| {
                    predicate(
                        item.max.map(|val| {
                            Datum::new(field_type.clone(), PrimitiveLiteral::Boolean(val))
                        }),
                        item.min.map(|val| {
                            Datum::new(field_type.clone(), PrimitiveLiteral::Boolean(val))
                        }),
                        PageNullCount::from_row_and_null_counts(row_count, item.null_count),
                    )
                })
                .collect(),
            Index::INT32(idx) => idx
                .indexes
                .iter()
                .zip(row_counts.iter())
                .map(|(item, &row_count)| {
                    predicate(
                        item.max
                            .map(|val| Datum::new(field_type.clone(), PrimitiveLiteral::Int(val))),
                        item.min
                            .map(|val| Datum::new(field_type.clone(), PrimitiveLiteral::Int(val))),
                        PageNullCount::from_row_and_null_counts(row_count, item.null_count),
                    )
                })
                .collect(),
            Index::INT64(idx) => idx
                .indexes
                .iter()
                .zip(row_counts.iter())
                .map(|(item, &row_count)| {
                    predicate(
                        item.max
                            .map(|val| Datum::new(field_type.clone(), PrimitiveLiteral::Long(val))),
                        item.min
                            .map(|val| Datum::new(field_type.clone(), PrimitiveLiteral::Long(val))),
                        PageNullCount::from_row_and_null_counts(row_count, item.null_count),
                    )
                })
                .collect(),
            Index::FLOAT(idx) => idx
                .indexes
                .iter()
                .zip(row_counts.iter())
                .map(|(item, &row_count)| {
                    predicate(
                        item.max.map(|val| {
                            Datum::new(
                                field_type.clone(),
                                PrimitiveLiteral::Float(OrderedFloat::from(val)),
                            )
                        }),
                        item.min.map(|val| {
                            Datum::new(
                                field_type.clone(),
                                PrimitiveLiteral::Float(OrderedFloat::from(val)),
                            )
                        }),
                        PageNullCount::from_row_and_null_counts(row_count, item.null_count),
                    )
                })
                .collect(),
            Index::DOUBLE(idx) => idx
                .indexes
                .iter()
                .zip(row_counts.iter())
                .map(|(item, &row_count)| {
                    predicate(
                        item.max.map(|val| {
                            Datum::new(
                                field_type.clone(),
                                PrimitiveLiteral::Double(OrderedFloat::from(val)),
                            )
                        }),
                        item.min.map(|val| {
                            Datum::new(
                                field_type.clone(),
                                PrimitiveLiteral::Double(OrderedFloat::from(val)),
                            )
                        }),
                        PageNullCount::from_row_and_null_counts(row_count, item.null_count),
                    )
                })
                .collect(),
            Index::BYTE_ARRAY(idx) => idx
                .indexes
                .iter()
                .zip(row_counts.iter())
                .map(|(item, &row_count)| {
                    predicate(
                        item.min.clone().map(|val| {
                            Datum::new(
                                field_type.clone(),
                                PrimitiveLiteral::String(
                                    String::from_utf8(val.data().to_vec()).unwrap(),
                                ),
                            )
                        }),
                        item.max.clone().map(|val| {
                            Datum::new(
                                field_type.clone(),
                                PrimitiveLiteral::String(
                                    String::from_utf8(val.data().to_vec()).unwrap(),
                                ),
                            )
                        }),
                        PageNullCount::from_row_and_null_counts(row_count, item.null_count),
                    )
                })
                .collect(),
            Index::FIXED_LEN_BYTE_ARRAY(_) => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    "unsupported 'FIXED_LEN_BYTE_ARRAY' index type in column_index",
                ))
            }
            Index::INT96(_) => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    "unsupported 'INT96' index type in column_index",
                ))
            }
        };

        Ok(Some(result?))
    }

    fn visit_inequality(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        cmp_fn: fn(&Datum, &Datum) -> bool,
        use_lower_bound: bool,
    ) -> Result<RowSelection> {
        let field_id = reference.field().id;

        self.calc_row_selection(
            field_id,
            |max, min, null_count| {
                if matches!(null_count, PageNullCount::AllNull) {
                    return Ok(false);
                }

                if datum.is_nan() {
                    // NaN indicates unreliable bounds.
                    return Ok(true);
                }

                let bound = if use_lower_bound { min } else { max };

                if let Some(bound) = bound {
                    if cmp_fn(&bound, datum) {
                        return Ok(true);
                    }

                    return Ok(false);
                }

                Ok(true)
            },
            MissingColBehavior::MightMatch,
        )
    }
}

impl BoundPredicateVisitor for PageIndexEvaluator<'_> {
    type T = RowSelection;

    fn always_true(&mut self) -> Result<RowSelection> {
        self.select_all_rows()
    }

    fn always_false(&mut self) -> Result<RowSelection> {
        self.skip_all_rows()
    }

    fn and(&mut self, lhs: RowSelection, rhs: RowSelection) -> Result<RowSelection> {
        Ok(lhs.intersection(&rhs))
    }

    fn or(&mut self, lhs: RowSelection, rhs: RowSelection) -> Result<RowSelection> {
        Ok(union_row_selections(&lhs, &rhs))
    }

    fn not(&mut self, _: RowSelection) -> Result<RowSelection> {
        Err(Error::new(
            ErrorKind::Unexpected,
            "NOT unsupported at this point. NOT-rewrite should be performed first",
        ))
    }

    fn is_null(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<RowSelection> {
        let field_id = reference.field().id;

        self.calc_row_selection(
            field_id,
            |_max, _min, null_count| Ok(!matches!(null_count, PageNullCount::NoneNull)),
            MissingColBehavior::MightMatch,
        )
    }

    fn not_null(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<RowSelection> {
        let field_id = reference.field().id;

        self.calc_row_selection(
            field_id,
            |_max, _min, null_count| Ok(!matches!(null_count, PageNullCount::AllNull)),
            MissingColBehavior::CantMatch,
        )
    }

    fn is_nan(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<RowSelection> {
        // NaN counts not present in ColumnChunkMetadata Statistics.
        // Only float columns can be NaN.
        if reference.field().field_type.is_floating_type() {
            self.select_all_rows()
        } else {
            self.skip_all_rows()
        }
    }

    fn not_nan(
        &mut self,
        _reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<RowSelection> {
        // NaN counts not present in ColumnChunkMetadata Statistics
        self.select_all_rows()
    }

    fn less_than(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<RowSelection> {
        self.visit_inequality(reference, datum, PartialOrd::lt, true)
    }

    fn less_than_or_eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<RowSelection> {
        self.visit_inequality(reference, datum, PartialOrd::le, true)
    }

    fn greater_than(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<RowSelection> {
        self.visit_inequality(reference, datum, PartialOrd::gt, false)
    }

    fn greater_than_or_eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<RowSelection> {
        self.visit_inequality(reference, datum, PartialOrd::ge, false)
    }

    fn eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<RowSelection> {
        let field_id = reference.field().id;

        self.calc_row_selection(
            field_id,
            |min, max, nulls| {
                if matches!(nulls, PageNullCount::AllNull) {
                    return Ok(false);
                }

                if let Some(min) = min {
                    if min.gt(datum) {
                        return Ok(false);
                    }
                }

                if let Some(max) = max {
                    if max.lt(datum) {
                        return Ok(false);
                    }
                }

                Ok(true)
            },
            MissingColBehavior::CantMatch,
        )
    }

    fn not_eq(
        &mut self,
        _reference: &BoundReference,
        _datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<RowSelection> {
        // Because the bounds are not necessarily a min or max value,
        // this cannot be answered using them. notEq(col, X) with (X, Y)
        // doesn't guarantee that X is a value in col.
        self.select_all_rows()
    }

    fn starts_with(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<RowSelection> {
        let field_id = reference.field().id;

        let PrimitiveLiteral::String(datum) = datum.literal() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Cannot use StartsWith operator on non-string values",
            ));
        };

        self.calc_row_selection(
            field_id,
            |min, max, nulls| {
                if matches!(nulls, PageNullCount::AllNull) {
                    return Ok(false);
                }

                if let Some(lower_bound) = min {
                    let PrimitiveLiteral::String(lower_bound) = lower_bound.literal() else {
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            "Cannot use StartsWith operator on non-string lower_bound value",
                        ));
                    };

                    let prefix_length = lower_bound.chars().count().min(datum.chars().count());

                    // truncate lower bound so that its length
                    // is not greater than the length of prefix
                    let truncated_lower_bound =
                        lower_bound.chars().take(prefix_length).collect::<String>();
                    if datum < &truncated_lower_bound {
                        return Ok(false);
                    }
                }

                if let Some(upper_bound) = max {
                    let PrimitiveLiteral::String(upper_bound) = upper_bound.literal() else {
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            "Cannot use StartsWith operator on non-string upper_bound value",
                        ));
                    };

                    let prefix_length = upper_bound.chars().count().min(datum.chars().count());

                    // truncate upper bound so that its length
                    // is not greater than the length of prefix
                    let truncated_upper_bound =
                        upper_bound.chars().take(prefix_length).collect::<String>();
                    if datum > &truncated_upper_bound {
                        return Ok(false);
                    }
                }

                Ok(true)
            },
            MissingColBehavior::CantMatch,
        )
    }

    fn not_starts_with(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<RowSelection> {
        let field_id = reference.field().id;

        // notStartsWith will match unless all values must start with the prefix.
        // This happens when the lower and upper bounds both start with the prefix.

        let PrimitiveLiteral::String(prefix) = datum.literal() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Cannot use StartsWith operator on non-string values",
            ));
        };

        self.calc_row_selection(
            field_id,
            |min, max, nulls| {
                if !matches!(nulls, PageNullCount::NoneNull) {
                    return Ok(true);
                }

                let Some(lower_bound) = min else {
                    return Ok(true);
                };

                let PrimitiveLiteral::String(lower_bound_str) = lower_bound.literal() else {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "Cannot use NotStartsWith operator on non-string lower_bound value",
                    ));
                };

                if lower_bound_str < prefix {
                    // if lower is shorter than the prefix then lower doesn't start with the prefix
                    return Ok(true);
                }

                let prefix_len = prefix.chars().count();

                if lower_bound_str.chars().take(prefix_len).collect::<String>() == *prefix {
                    // lower bound matches the prefix

                    let Some(upper_bound) = max else {
                        return Ok(true);
                    };

                    let PrimitiveLiteral::String(upper_bound) = upper_bound.literal() else {
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            "Cannot use NotStartsWith operator on non-string upper_bound value",
                        ));
                    };

                    // if upper is shorter than the prefix then upper can't start with the prefix
                    if upper_bound.chars().count() < prefix_len {
                        return Ok(true);
                    }

                    if upper_bound.chars().take(prefix_len).collect::<String>() == *prefix {
                        // both bounds match the prefix, so all rows must match the
                        // prefix and therefore do not satisfy the predicate
                        return Ok(false);
                    }
                }

                Ok(true)
            },
            MissingColBehavior::MightMatch,
        )
    }

    fn r#in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<RowSelection> {
        let field_id = reference.field().id;

        if literals.len() > IN_PREDICATE_LIMIT {
            // skip evaluating the predicate if the number of values is too big
            return self.select_all_rows();
        }
        self.calc_row_selection(
            field_id,
            |min, max, nulls| {
                if matches!(nulls, PageNullCount::AllNull) {
                    return Ok(false);
                }

                if let Some(lower_bound) = min {
                    if !literals.iter().any(|datum| datum.ge(&lower_bound)) {
                        // if all values are less than lower bound, rows cannot match.
                        return Ok(false);
                    }
                }

                if let Some(upper_bound) = max {
                    if !literals.iter().any(|datum| datum.le(&upper_bound)) {
                        // if all values are greater than upper bound, rows cannot match.
                        return Ok(false);
                    }
                }

                Ok(true)
            },
            MissingColBehavior::CantMatch,
        )
    }

    fn not_in(
        &mut self,
        _reference: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<RowSelection> {
        // Because the bounds are not necessarily a min or max value,
        // this cannot be answered using them. notIn(col, {X, ...})
        // with (X, Y) doesn't guarantee that X is a value in col.
        self.select_all_rows()
    }
}

/// Combine two lists of `RowSelection` return the union of them
/// For example:
/// self:      NNYYYYNNYYNYN
/// other:     NYNNNNNNY
///
/// returned:  NYYYYYNNYYNYN
///
/// This can be removed from here once RowSelection::union is in parquet::arrow
fn union_row_selections(left: &RowSelection, right: &RowSelection) -> RowSelection {
    let mut l_iter = left.iter().copied().peekable();
    let mut r_iter = right.iter().copied().peekable();

    let iter = std::iter::from_fn(move || {
        loop {
            let l = l_iter.peek_mut();
            let r = r_iter.peek_mut();

            match (l, r) {
                (Some(a), _) if a.row_count == 0 => {
                    l_iter.next().unwrap();
                }
                (_, Some(b)) if b.row_count == 0 => {
                    r_iter.next().unwrap();
                }
                (Some(l), Some(r)) => {
                    return match (l.skip, r.skip) {
                        // Skip both ranges
                        (true, true) => {
                            if l.row_count < r.row_count {
                                let skip = l.row_count;
                                r.row_count -= l.row_count;
                                l_iter.next();
                                Some(RowSelector::skip(skip))
                            } else {
                                let skip = r.row_count;
                                l.row_count -= skip;
                                r_iter.next();
                                Some(RowSelector::skip(skip))
                            }
                        }
                        // Keep rows from left
                        (false, true) => {
                            if l.row_count < r.row_count {
                                r.row_count -= l.row_count;
                                l_iter.next()
                            } else {
                                let r_row_count = r.row_count;
                                l.row_count -= r_row_count;
                                r_iter.next();
                                Some(RowSelector::select(r_row_count))
                            }
                        }
                        // Keep rows from right
                        (true, false) => {
                            if l.row_count < r.row_count {
                                let l_row_count = l.row_count;
                                r.row_count -= l_row_count;
                                l_iter.next();
                                Some(RowSelector::select(l_row_count))
                            } else {
                                l.row_count -= r.row_count;
                                r_iter.next()
                            }
                        }
                        // Keep at least one
                        _ => {
                            if l.row_count < r.row_count {
                                r.row_count -= l.row_count;
                                l_iter.next()
                            } else {
                                l.row_count -= r.row_count;
                                r_iter.next()
                            }
                        }
                    };
                }
                (Some(_), None) => return l_iter.next(),
                (None, Some(_)) => return r_iter.next(),
                (None, None) => return None,
            }
        }
    });

    iter.collect()
}

#[cfg(test)]
mod tests {
    use parquet::arrow::arrow_reader::{RowSelection, RowSelector};

    use crate::expr::visitors::page_index_evaluator::union_row_selections;

    #[test]
    fn test_union_row_selections() {
        let selection = RowSelection::from(vec![RowSelector::select(1048576)]);
        let result = union_row_selections(&selection, &selection);
        assert_eq!(result, selection);

        // NYNYY
        let a = RowSelection::from(vec![
            RowSelector::skip(10),
            RowSelector::select(10),
            RowSelector::skip(10),
            RowSelector::select(20),
        ]);

        // NNYYN
        let b = RowSelection::from(vec![
            RowSelector::skip(20),
            RowSelector::select(20),
            RowSelector::skip(10),
        ]);

        let result = union_row_selections(&a, &b);

        // NYYYY
        assert_eq!(result.iter().collect::<Vec<_>>(), vec![
            &RowSelector::skip(10),
            &RowSelector::select(40)
        ]);
    }
}
