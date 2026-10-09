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

//! Execution-time predicates published to the Arrow reader.

use std::sync::{Arc, Mutex};

use arrow_schema::SchemaRef as ArrowSchemaRef;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::schema::types::SchemaDescriptor;

use super::ArrowReader;
use crate::arrow::record_batch_transformer::column_needs_type_promotion;
use crate::arrow::type_to_arrow_type;
use crate::expr::{Bind, BoundPredicate, Predicate};
use crate::spec::{Schema, SchemaRef};
use crate::{Error, ErrorKind, Result};

/// An immutable view of a runtime predicate at one point in execution.
#[derive(Debug)]
pub struct RuntimePredicateSnapshot {
    predicate: Option<Predicate>,
    generation: u64,
}

impl RuntimePredicateSnapshot {
    /// Creates a runtime predicate snapshot. A `None` predicate adds no
    /// restriction to tasks that start with it. It does not undo pruning
    /// already applied.
    pub fn new(predicate: Option<Predicate>, generation: u64) -> Self {
        Self {
            predicate,
            generation,
        }
    }

    /// Returns the publication generation this predicate belongs to.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Consumes the snapshot, returning its predicate without cloning it.
    pub fn into_predicate(self) -> Option<Predicate> {
        self.predicate
    }
}

/// Supplies execution-time predicates, such as a completed join build's key
/// range or a TopK/MIN/MAX bound, to the Arrow reader.
///
/// Contract:
/// * Data skipped under a published predicate must never be required by a
///   later publication. Once published, a predicate stays safe to combine with
///   the planned predicate for every row read after it, including by tasks still
///   using that generation. A later publication may therefore tighten it, but
///   not loosen it. The one exception is `None`, which stops runtime pruning
///   for tasks that start afterwards. It does not revoke predicates already
///   captured by earlier tasks or restore data they skipped.
/// * Generations increase monotonically on every change, are never reused,
///   and become visible only after their predicate.
/// * Provider, snapshot, binding, planning and page-pruning failures are
///   advisory: the reader skips the runtime predicate and does not retry them.
///   Errors while decoding its columns or evaluating it on rows are not
///   skipped.
///
/// Pass a provider to `ArrowReaderBuilder::with_runtime_predicate_provider`.
/// This is the reader layer only: `TableScan::to_arrow` does not accept a
/// provider yet, so engines that plan through `TableScan` need that wiring as
/// a follow-up.
///
/// # Example
///
/// A provider that publishes an upper bound on `id`. The predicate and its
/// generation change together under a lock, and the cheap generation is
/// stored last so it is never visible before its predicate.
///
/// ```
/// use std::sync::Mutex;
/// use std::sync::atomic::{AtomicU64, Ordering};
///
/// use iceberg::Result;
/// use iceberg::arrow::{RuntimePredicateProvider, RuntimePredicateSnapshot};
/// use iceberg::expr::{Predicate, Reference};
/// use iceberg::spec::Datum;
///
/// struct UpperBound {
///     published: Mutex<(u64, Option<Predicate>)>,
///     generation: AtomicU64,
/// }
///
/// impl UpperBound {
///     /// Publishes a bound that must be no looser than the previous one.
///     fn publish(&self, bound: i64) {
///         let mut published = self.published.lock().unwrap();
///         let generation = published.0 + 1;
///         let predicate = Reference::new("id").less_than_or_equal_to(Datum::long(bound));
///         *published = (generation, Some(predicate));
///         self.generation.store(generation, Ordering::Release);
///     }
/// }
///
/// impl RuntimePredicateProvider for UpperBound {
///     fn generation(&self) -> u64 {
///         self.generation.load(Ordering::Acquire)
///     }
///
///     fn snapshot(&self) -> Result<RuntimePredicateSnapshot> {
///         let (generation, predicate) = self.published.lock().unwrap().clone();
///         Ok(RuntimePredicateSnapshot::new(predicate, generation))
///     }
/// }
/// ```
pub trait RuntimePredicateProvider: Send + Sync {
    /// Returns the current publication generation. Called for every task, so
    /// keep it cheap, for example an atomic load with acquire ordering.
    fn generation(&self) -> u64;

    /// Returns the current runtime predicate snapshot. The predicate and
    /// generation must be a self-consistent pair, and the generation must not
    /// be older than one `generation()` has already returned; the reader
    /// ignores an older snapshot.
    ///
    /// Implementations should use in-memory, non-blocking work because refresh
    /// serializes data-file tasks while taking the snapshot and binding it.
    fn snapshot(&self) -> Result<RuntimePredicateSnapshot>;
}

/// Retains one bound predicate across a scan's tasks, keyed by generation,
/// schema and case policy. A different schema or case policy replaces the
/// binding; bindings for multiple schemas are not retained simultaneously.
/// Failures are cached as `None` for that key.
pub(super) struct RuntimePredicates {
    provider: Arc<dyn RuntimePredicateProvider>,
    cached: Mutex<Option<CachedPredicate>>,
}

struct CachedPredicate {
    generation: u64,
    schema: SchemaRef,
    case_sensitive: bool,
    predicate: Option<Arc<BoundPredicate>>,
}

impl CachedPredicate {
    fn matches(&self, generation: u64, schema: &SchemaRef, case_sensitive: bool) -> bool {
        // A snapshot can observe a publication newer than the cheap check.
        self.generation >= generation
            && self.case_sensitive == case_sensitive
            && (Arc::ptr_eq(&self.schema, schema) || *self.schema == **schema)
    }
}

impl RuntimePredicates {
    pub(super) fn new(provider: Arc<dyn RuntimePredicateProvider>) -> Self {
        Self {
            provider,
            cached: Mutex::new(None),
        }
    }

    /// Returns the current predicate bound to `schema`, with NOT pushed to the
    /// leaves because statistics evaluators cannot negate a "might match".
    pub(super) fn current(
        &self,
        schema: &SchemaRef,
        case_sensitive: bool,
        data_file_path: &str,
    ) -> Option<Arc<BoundPredicate>> {
        // Hold the lock for the whole refresh so concurrent misses wait for it
        // instead of repeating the snapshot and binding, and a slow refresh
        // cannot overwrite a newer one. Read the generation after locking so
        // waiters reuse a completed refresh, including a failed one.
        let mut cache = self.cached.lock().unwrap();
        let generation = self.provider.generation();
        if let Some(cached) = cache.as_ref()
            && cached.matches(generation, schema, case_sensitive)
        {
            return cached.predicate.clone();
        }

        // Never replace a newer cached generation, even if the binding context
        // changes or the provider returns an outdated snapshot. Such a snapshot
        // is advisory and is cached as a failure at the observed generation.
        let generation = cache
            .as_ref()
            .map_or(generation, |cached| generation.max(cached.generation));
        let (generation, predicate) = match self.provider.snapshot() {
            Ok(snapshot) if snapshot.generation() < generation => {
                tracing::debug!("Skipping stale runtime predicate for {data_file_path}");
                (generation, None)
            }
            Ok(snapshot) => {
                let generation = snapshot.generation();
                let bound = snapshot
                    .into_predicate()
                    .map(|predicate| predicate.rewrite_not().bind(schema.clone(), case_sensitive))
                    .transpose();
                match bound {
                    Ok(bound) => (generation, bound.map(Arc::new)),
                    Err(error) => {
                        tracing::debug!("Skipping runtime predicate for {data_file_path}: {error}");
                        (generation, None)
                    }
                }
            }
            Err(error) => {
                tracing::debug!("Skipping runtime predicate for {data_file_path}: {error}");
                (generation, None)
            }
        };
        let result = predicate.clone();
        *cache = Some(CachedPredicate {
            generation,
            schema: schema.clone(),
            case_sensitive,
            predicate,
        });
        result
    }
}

/// Fails if `predicate` references a column the file lacks (its default is
/// invisible to physical filters) or stores with a promoted type (a literal
/// cast before promotion can round or overflow and reject valid rows).
pub(super) fn check_runtime_predicate_columns(
    predicate: &BoundPredicate,
    parquet_schema: &SchemaDescriptor,
    arrow_schema: &ArrowSchemaRef,
    table_schema: &Schema,
    use_position_fallback: bool,
) -> Result<()> {
    let (field_ids, field_id_map) = ArrowReader::build_field_id_set_and_map(
        parquet_schema,
        arrow_schema,
        predicate,
        use_position_fallback,
    )?;
    // Arrow and Parquet use the same leaf-column order, including for nested
    // fields. Resolve by column index so name mapping and position fallback
    // follow the same field resolution as predicate planning.
    let mut source_fields = Vec::new();
    arrow_schema.fields().filter_leaves(|_, field| {
        source_fields.push(field.clone());
        false
    });
    for field_id in field_ids {
        let unusable = |reason: &str| {
            Error::new(
                ErrorKind::FeatureUnsupported,
                format!("Runtime predicate field {field_id} {reason}"),
            )
        };
        let column = *field_id_map
            .get(&field_id)
            .ok_or_else(|| unusable("is not stored in this file"))?;
        let field = table_schema
            .field_by_id(field_id)
            .ok_or_else(|| unusable("is not in the table schema"))?;
        field
            .field_type
            .as_primitive_type()
            .ok_or_else(|| unusable("is not a primitive column"))?;
        let source_field = source_fields
            .get(column)
            .ok_or_else(|| unusable("has no Arrow leaf column"))?;
        let target_type = type_to_arrow_type(&field.field_type)?;
        if column_needs_type_promotion(source_field.data_type(), &target_type) {
            return Err(unusable("is stored with a promoted physical type"));
        }
    }
    Ok(())
}

/// Intersects two ascending row-group index lists in linear time.
pub(super) fn intersect_sorted(left: &[usize], right: &[usize]) -> Vec<usize> {
    let (mut left, mut right) = (left.iter().peekable(), right.iter().peekable());
    let mut intersection = Vec::new();
    while let (Some(&&l), Some(&&r)) = (left.peek(), right.peek()) {
        match l.cmp(&r) {
            std::cmp::Ordering::Less => {
                left.next();
            }
            std::cmp::Ordering::Greater => {
                right.next();
            }
            std::cmp::Ordering::Equal => {
                intersection.push(l);
                left.next();
                right.next();
            }
        }
    }
    intersection
}

/// Intersects a predicate's page-index selection with `current`, where `None`
/// selects every row. An advisory predicate's failure leaves `current` as is.
pub(super) fn intersect_page_selection(
    current: Option<RowSelection>,
    selection: Result<Option<RowSelection>>,
    advisory: bool,
    data_file_path: &str,
) -> Result<Option<RowSelection>> {
    let selection = match selection {
        Ok(selection) => selection,
        Err(error) if advisory => {
            tracing::debug!("Skipping runtime page pruning for {data_file_path}: {error}");
            None
        }
        Err(error) => return Err(error),
    };
    Ok(match (current, selection) {
        (Some(current), Some(selection)) => Some(current.intersection(&selection)),
        (current, selection) => current.or(selection),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::mpsc::{self, Receiver, Sender};
    use std::sync::{Barrier, OnceLock, Weak};
    use std::time::Duration;

    use super::*;
    use crate::expr::Reference;
    use crate::spec::{Datum, NestedField, PrimitiveType, Type};

    /// Holds the first snapshot open so concurrent callers overlap a refresh.
    struct GatedProvider {
        generation: AtomicU64,
        snapshots: AtomicU64,
        started: Sender<()>,
        release: Mutex<Receiver<()>>,
        fail_first_generation: bool,
        predicates: OnceLock<Weak<RuntimePredicates>>,
    }

    impl RuntimePredicateProvider for GatedProvider {
        fn generation(&self) -> u64 {
            self.generation.load(Ordering::Acquire)
        }

        fn snapshot(&self) -> Result<RuntimePredicateSnapshot> {
            let generation = self.generation();
            let first = self.snapshots.fetch_add(1, Ordering::Relaxed) == 0;
            self.started.send(()).unwrap();
            if first {
                // The cache stays locked for the whole refresh, so concurrent
                // misses wait for this snapshot instead of starting their own.
                let predicates = self.predicates.get().unwrap().upgrade().unwrap();
                assert!(predicates.cached.try_lock().is_err());
                self.release
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(10))
                    .unwrap();
            }
            if self.fail_first_generation && generation == 1 {
                return Err(Error::new(ErrorKind::Unexpected, "snapshot failed"));
            }
            Ok(RuntimePredicateSnapshot::new(
                Some(
                    Reference::new("id")
                        .greater_than_or_equal_to(Datum::long((generation * 100) as i64)),
                ),
                generation,
            ))
        }
    }

    fn concurrent_refresh(fail_first_generation: bool, publish_during_snapshot: bool) {
        const CALLERS: usize = 8;
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let provider = Arc::new(GatedProvider {
            generation: AtomicU64::new(1),
            snapshots: AtomicU64::new(0),
            started: started_tx,
            release: Mutex::new(release_rx),
            fail_first_generation,
            predicates: OnceLock::new(),
        });
        let predicates = Arc::new(RuntimePredicates::new(provider.clone()));
        provider
            .predicates
            .set(Arc::downgrade(&predicates))
            .unwrap();
        let barrier = Arc::new(Barrier::new(CALLERS + 1));
        let callers: Vec<_> = (0..CALLERS)
            .map(|_| {
                let predicates = predicates.clone();
                let schema = schema.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    predicates.current(&schema, false, "concurrent.parquet")
                })
            })
            .collect();
        barrier.wait();
        started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        if publish_during_snapshot {
            provider.generation.store(2, Ordering::Release);
        }
        release_tx.send(()).unwrap();
        let results: Vec<_> = callers.into_iter().map(|t| t.join().unwrap()).collect();

        let current = predicates.current(&schema, false, "later.parquet");
        let expected_generation = if publish_during_snapshot { 2 } else { 1 };
        assert_eq!(
            predicates
                .cached
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .generation,
            expected_generation
        );
        assert_eq!(
            provider.snapshots.load(Ordering::Relaxed),
            expected_generation,
            "one snapshot and binding attempt per generation"
        );
        if fail_first_generation {
            assert!(results.iter().all(Option::is_none));
            assert!(current.is_none());
            // A failed generation is retained, but does not suppress a later
            // successful publication.
            provider.generation.store(2, Ordering::Release);
            let recovered = predicates
                .current(&schema, false, "recovered.parquet")
                .unwrap();
            assert!(Arc::ptr_eq(
                &recovered,
                &predicates
                    .current(&schema, false, "reused.parquet")
                    .unwrap()
            ));
            assert_eq!(provider.snapshots.load(Ordering::Relaxed), 2);
        } else {
            let current = current.unwrap();
            let reused = results
                .iter()
                .filter(|result| Arc::ptr_eq(result.as_ref().unwrap(), &current))
                .count();
            assert_eq!(reused, CALLERS - usize::from(publish_during_snapshot));
            // Arc identity verifies that waiters reuse the bound predicate,
            // rather than just deduplicating snapshots and binding separately.
            assert!(Arc::ptr_eq(
                &current,
                &predicates
                    .current(&schema, false, "reused.parquet")
                    .unwrap()
            ));
        }
    }

    #[test]
    fn intersect_sorted_keeps_common_indices_in_order() {
        assert_eq!(
            intersect_sorted(&[0, 2, 3, 7, 9], &[1, 2, 3, 8, 9, 10]),
            vec![2, 3, 9]
        );
        assert_eq!(intersect_sorted(&[1, 2], &[]), Vec::<usize>::new());
        assert_eq!(intersect_sorted(&[], &[1, 2]), Vec::<usize>::new());
        assert_eq!(intersect_sorted(&[4, 5], &[4, 5]), vec![4, 5]);
        assert_eq!(intersect_sorted(&[0, 1], &[2, 3]), Vec::<usize>::new());
    }

    #[test]
    fn concurrent_runtime_predicate_refresh_snapshots_and_binds_once() {
        concurrent_refresh(false, false);
    }

    #[test]
    fn concurrent_runtime_predicate_publication_does_not_regress_cache() {
        concurrent_refresh(false, true);
    }

    #[test]
    fn concurrent_runtime_predicate_failure_is_cached_until_next_generation() {
        concurrent_refresh(true, false);
    }

    #[test]
    fn runtime_predicate_stale_snapshot_is_cached_without_regression() {
        struct Provider {
            generation: AtomicU64,
            snapshot_generation: AtomicU64,
            snapshots: AtomicU64,
        }
        impl RuntimePredicateProvider for Provider {
            fn generation(&self) -> u64 {
                self.generation.load(Ordering::Acquire)
            }

            fn snapshot(&self) -> Result<RuntimePredicateSnapshot> {
                self.snapshots.fetch_add(1, Ordering::Relaxed);
                Ok(RuntimePredicateSnapshot::new(
                    Some(Reference::new("id").greater_than(Datum::long(0))),
                    self.snapshot_generation.load(Ordering::Acquire),
                ))
            }
        }
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );
        let provider = Arc::new(Provider {
            generation: AtomicU64::new(1),
            snapshot_generation: AtomicU64::new(2),
            snapshots: AtomicU64::new(0),
        });
        let predicates = RuntimePredicates::new(provider.clone());
        let bound = predicates.current(&schema, false, "ahead.parquet").unwrap();
        // Snapshot publication can precede the cheap generation publication.
        assert!(Arc::ptr_eq(
            &bound,
            &predicates
                .current(&schema, false, "reused.parquet")
                .unwrap()
        ));
        assert_eq!(provider.snapshots.load(Ordering::Relaxed), 1);

        // Rebinding under another case policy must not lower the high-water
        // generation even if a provider supplies an outdated snapshot.
        provider.snapshot_generation.store(1, Ordering::Release);
        assert!(predicates.current(&schema, true, "stale.parquet").is_none());
        assert_eq!(
            predicates
                .cached
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .generation,
            2
        );
        assert!(
            predicates
                .current(&schema, true, "cached-failure.parquet")
                .is_none()
        );
        assert_eq!(provider.snapshots.load(Ordering::Relaxed), 2);

        // A stale snapshot following a newer cheap check is also a cached
        // failure, rather than work to retry for every data-file task.
        provider.generation.store(3, Ordering::Release);
        assert!(predicates.current(&schema, true, "newer.parquet").is_none());
        assert!(
            predicates
                .current(&schema, true, "no-storm.parquet")
                .is_none()
        );
        assert_eq!(provider.snapshots.load(Ordering::Relaxed), 3);
        assert_eq!(
            predicates
                .cached
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .generation,
            3
        );
    }
}
