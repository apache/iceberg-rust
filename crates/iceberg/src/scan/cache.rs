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

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::expr::visitors::expression_evaluator::ExpressionEvaluator;
use crate::expr::visitors::inclusive_projection::InclusiveProjection;
use crate::expr::visitors::manifest_evaluator::ManifestEvaluator;
use crate::expr::{Bind, BoundPredicate};
use crate::spec::{Schema, TableMetadataRef};
use crate::{Error, ErrorKind, Result};

/// Manages the caching of [`BoundPredicate`] objects
/// for [`PartitionSpec`]s based on partition spec id.
#[derive(Debug)]
pub(crate) struct PartitionFilterCache(RwLock<HashMap<i32, Arc<BoundPredicate>>>);

impl PartitionFilterCache {
    /// Creates a new [`PartitionFilterCache`]
    /// with an empty internal HashMap.
    pub(crate) fn new() -> Self {
        Self(RwLock::new(HashMap::new()))
    }

    /// Retrieves a [`BoundPredicate`] from the cache
    /// or computes it if not present.
    pub(crate) fn get(
        &self,
        spec_id: i32,
        table_metadata: &TableMetadataRef,
        schema: &Schema,
        case_sensitive: bool,
        filter: BoundPredicate,
    ) -> Result<Arc<BoundPredicate>> {
        // we need a block here to ensure that the `read()` gets dropped before we hit the `write()`
        // below, otherwise we hit deadlock
        {
            let read = self.0.read().map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "PartitionFilterCache RwLock was poisoned",
                )
            })?;

            if read.contains_key(&spec_id) {
                return Ok(read.get(&spec_id).unwrap().clone());
            }
        }

        let partition_spec = table_metadata
            .partition_spec_by_id(spec_id)
            .ok_or(Error::new(
                ErrorKind::Unexpected,
                format!("Could not find partition spec for id {spec_id}"),
            ))?;

        // A historical spec may reference a source column that was later dropped from the
        // schema, which is a legitimate v2+ state. Such a spec cannot be resolved to a
        // partition type, so it falls back to an always-true filter: files under the spec
        // are not partition-pruned but still receive the row filter. Any other resolution
        // failure is unexpected and propagates. The fallback is cached by spec id like any
        // other filter; this is safe only because the cache lives per-scan in `PlanContext`
        // with a fixed schema and predicate. Hoisting it to table or catalog scope would
        // pin a spec to always-true even for a later scan whose schema could resolve it.
        // TODO(https://github.com/apache/iceberg-rust/issues/2844): derive partition types from
        // transforms where possible to restore pruning on a historical spec's still-live fields,
        // which also makes the fallback per-term (dropped fields only) instead of per-spec.
        let dropped_source_column = partition_spec
            .fields()
            .iter()
            .any(|field| schema.field_by_id(field.source_id).is_none());

        let partition_filter = if dropped_source_column {
            tracing::warn!(
                spec_id,
                "Partition spec references a source column not in the scan schema; \
                 skipping partition pruning for its manifests"
            );
            BoundPredicate::AlwaysTrue
        } else {
            let partition_type = partition_spec.partition_type(schema)?;
            let partition_fields = partition_type.fields().to_owned();
            let partition_schema = Arc::new(
                Schema::builder()
                    .with_schema_id(partition_spec.spec_id())
                    .with_fields(partition_fields)
                    .build()?,
            );

            let mut inclusive_projection = InclusiveProjection::new(partition_spec.clone());

            inclusive_projection
                .project(&filter)?
                .rewrite_not()
                .bind(partition_schema.clone(), case_sensitive)?
        };

        self.0
            .write()
            .map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "PartitionFilterCache RwLock was poisoned",
                )
            })?
            .insert(spec_id, Arc::new(partition_filter));

        let read = self.0.read().map_err(|_| {
            Error::new(
                ErrorKind::Unexpected,
                "PartitionFilterCache RwLock was poisoned",
            )
        })?;

        Ok(read.get(&spec_id).unwrap().clone())
    }
}

/// Manages the caching of [`ManifestEvaluator`] objects
/// for [`PartitionSpec`]s based on partition spec id.
#[derive(Debug)]
pub(crate) struct ManifestEvaluatorCache(RwLock<HashMap<i32, Arc<ManifestEvaluator>>>);

impl ManifestEvaluatorCache {
    /// Creates a new [`ManifestEvaluatorCache`]
    /// with an empty internal HashMap.
    pub(crate) fn new() -> Self {
        Self(RwLock::new(HashMap::new()))
    }

    /// Retrieves a [`ManifestEvaluator`] from the cache
    /// or computes it if not present.
    pub(crate) fn get(
        &self,
        spec_id: i32,
        partition_filter: Arc<BoundPredicate>,
    ) -> Arc<ManifestEvaluator> {
        // we need a block here to ensure that the `read()` gets dropped before we hit the `write()`
        // below, otherwise we hit deadlock
        {
            let read = self
                .0
                .read()
                .map_err(|_| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "ManifestEvaluatorCache RwLock was poisoned",
                    )
                })
                .unwrap();

            if read.contains_key(&spec_id) {
                return read.get(&spec_id).unwrap().clone();
            }
        }

        self.0
            .write()
            .map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "ManifestEvaluatorCache RwLock was poisoned",
                )
            })
            .unwrap()
            .insert(
                spec_id,
                Arc::new(ManifestEvaluator::builder(partition_filter.as_ref().clone()).build()),
            );

        let read = self
            .0
            .read()
            .map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "ManifestEvaluatorCache RwLock was poisoned",
                )
            })
            .unwrap();

        read.get(&spec_id).unwrap().clone()
    }
}

/// Manages the caching of [`ExpressionEvaluator`] objects
/// for [`PartitionSpec`]s based on partition spec id.
#[derive(Debug)]
pub(crate) struct ExpressionEvaluatorCache(RwLock<HashMap<i32, Arc<ExpressionEvaluator>>>);

impl ExpressionEvaluatorCache {
    /// Creates a new [`ExpressionEvaluatorCache`]
    /// with an empty internal HashMap.
    pub(crate) fn new() -> Self {
        Self(RwLock::new(HashMap::new()))
    }

    /// Retrieves a [`ExpressionEvaluator`] from the cache
    /// or computes it if not present.
    pub(crate) fn get(
        &self,
        spec_id: i32,
        partition_filter: &BoundPredicate,
    ) -> Result<Arc<ExpressionEvaluator>> {
        // we need a block here to ensure that the `read()` gets dropped before we hit the `write()`
        // below, otherwise we hit deadlock
        {
            let read = self.0.read().map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "PartitionFilterCache RwLock was poisoned",
                )
            })?;

            if read.contains_key(&spec_id) {
                return Ok(read.get(&spec_id).unwrap().clone());
            }
        }

        self.0
            .write()
            .map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "ManifestEvaluatorCache RwLock was poisoned",
                )
            })
            .unwrap()
            .insert(
                spec_id,
                Arc::new(ExpressionEvaluator::new(partition_filter.clone())),
            );

        let read = self
            .0
            .read()
            .map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "ManifestEvaluatorCache RwLock was poisoned",
                )
            })
            .unwrap();

        Ok(read.get(&spec_id).unwrap().clone())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::PartitionFilterCache;
    use crate::expr::{Bind, BoundPredicate, Reference};
    use crate::spec::{
        Datum, FormatVersion, NestedField, PrimitiveType, Schema, SortOrder, TableMetadataBuilder,
        Transform, Type, UnboundPartitionSpec,
    };

    /// A historical spec whose source column was dropped from the current schema resolves to an
    /// always-true filter, and the fallback is cached under the spec id.
    #[test]
    fn dropped_partition_source_column_falls_back_to_always_true() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "part", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();

        // Spec 0 partitions on `part` (id 2).
        let spec = UnboundPartitionSpec::builder()
            .with_spec_id(0)
            .add_partition_field(2, "part", Transform::Identity)
            .unwrap()
            .build();

        // Evolve the schema so that `part` is no longer present, leaving spec 0 unresolvable.
        let evolved_schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();
        // Make an unpartitioned spec the default before dropping `part`, so spec 0 survives
        // only as a historical spec that can no longer be resolved against the schema.
        let table_metadata = Arc::new(
            TableMetadataBuilder::new(
                schema,
                spec,
                SortOrder::unsorted_order(),
                "s3://bucket/test".to_string(),
                FormatVersion::V2,
                HashMap::new(),
            )
            .unwrap()
            .add_default_partition_spec(UnboundPartitionSpec::builder().build())
            .unwrap()
            .add_current_schema(evolved_schema.clone())
            .unwrap()
            .build()
            .unwrap()
            .metadata,
        );

        let filter = Reference::new("id")
            .greater_than_or_equal_to(Datum::long(5))
            .bind(Arc::new(evolved_schema.clone()), true)
            .unwrap();

        let cache = PartitionFilterCache::new();
        let partition_filter = cache
            .get(0, &table_metadata, &evolved_schema, true, filter.clone())
            .unwrap();
        assert!(matches!(*partition_filter, BoundPredicate::AlwaysTrue));

        // The fallback is cached, so a second lookup returns the same instance.
        let cached = cache
            .get(0, &table_metadata, &evolved_schema, true, filter)
            .unwrap();
        assert!(Arc::ptr_eq(&partition_filter, &cached));
    }
}
