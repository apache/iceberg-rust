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

use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array, Int64Array,
    StringArray,
};
use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use datafusion::catalog::Session;
use datafusion::common::hash_utils::create_hashes;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::repartition::REPARTITION_RANDOM_STATE;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use futures::TryStreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::scan::FileScanTask;
use iceberg::spec::{Literal, PartitionSpec, PrimitiveLiteral, Transform};
use iceberg::table::Table;
use iceberg::{Catalog, Error, ErrorKind, NamespaceIdent, Result, TableIdent};

use crate::error::to_datafusion_error;
use crate::physical_plan::expr_to_predicate::{
    convert_filter_to_predicate, convert_filters_to_predicate,
};
use crate::physical_plan::partitioned_scan::IcebergPartitionedScan;

/// Catalog-backed table provider that scans each data file in a separate DataFusion partition.
///
/// This provider reloads table metadata from the catalog on every [`scan`][Self::scan] call
/// to guarantee freshness, then issues one DataFusion partition per data file so that
/// DataFusion's scheduler can execute file reads in parallel.
///
/// Write operations are not supported. Use [`IcebergTableProvider`] for write access.
///
/// For consistent read-only access to a fixed snapshot without per-scan catalog overhead,
/// use [`IcebergStaticTableProvider`] instead.
#[derive(Debug, Clone)]
pub struct IcebergPartitionedTableProvider {
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    schema: ArrowSchemaRef,
    /// Source-column names that are identity-partitioned in *every* historical
    /// partition spec of the table, captured at construction. Used by
    /// `supports_filters_pushdown` to mark filters as `Exact` when they only
    /// reference these columns: Iceberg evaluates partition predicates against
    /// each manifest's own spec, so a column that is identity-partitioned in
    /// every spec is fully prunable across the full table regardless of which
    /// spec a given file was written under.
    ///
    /// Columns that appear in some specs with non-identity transforms
    /// (`bucket`, `truncate`, `year`/`month`/etc.), or that are missing from
    /// any spec entirely, are dropped from the set — those files cannot be
    /// pruned exactly, so DataFusion must keep its FilterExec.
    ///
    /// This is a snapshot: if the table's specs change between `try_new` and
    /// a later scan, the cached set may be stale. The next `try_new` refreshes.
    identity_partition_cols: HashSet<String>,
}

impl IcebergPartitionedTableProvider {
    pub async fn try_new(
        catalog: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
        name: impl Into<String>,
    ) -> Result<Self> {
        let table_ident = TableIdent::new(namespace, name.into());
        // First load: used only to snapshot the Arrow schema for DataFusion planning.
        // A second load_table is issued at scan time to guarantee the freshest snapshot.
        let table = catalog.load_table(&table_ident).await?;
        let schema = Arc::new(schema_to_arrow_schema(table.metadata().current_schema())?);
        let identity_partition_cols = identity_partition_col_names(&table);
        Ok(Self {
            catalog,
            table_ident,
            schema,
            identity_partition_cols,
        })
    }
}

#[async_trait]
impl TableProvider for IcebergPartitionedTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Per-partition row limits are not yet implemented for IcebergPartitionedScan.
        // DataFusion will apply a GlobalLimitExec on top of this node when needed.

        // Second load: fetch the latest snapshot so scans always reflect current table state.
        let table = self
            .catalog
            .load_table(&self.table_ident)
            .await
            .map_err(to_datafusion_error)?;

        // Build a TableScan mirroring the inputs we'll hand to IcebergPartitionedScan,
        // so plan_files() uses the same projection/filters the scan will replay in execute().
        let col_names = projection.map(|indices| {
            indices
                .iter()
                .map(|&i| self.schema.field(i).name().clone())
                .collect::<Vec<_>>()
        });

        let predicate = convert_filters_to_predicate(filters);

        let mut builder = table.scan();
        builder = match col_names {
            Some(names) => builder.select(names),
            None => builder.select_all(),
        };
        if let Some(pred) = predicate {
            builder = builder.with_filter(pred);
        }

        let tasks: Vec<FileScanTask> = builder
            .build()
            .map_err(to_datafusion_error)?
            .plan_files()
            .await
            .map_err(to_datafusion_error)?
            .try_collect::<Vec<_>>()
            .await
            .map_err(to_datafusion_error)?;

        // Output schema after projection: column indices in `Hash` exprs and any
        // Arrow array we hash must reference this schema, not the full table schema.
        let output_schema = match projection {
            None => self.schema.clone(),
            Some(p) => Arc::new(self.schema.project(p).map_err(|e| {
                to_datafusion_error(Error::new(ErrorKind::DataInvalid, e.to_string()))
            })?),
        };

        let target_partitions = state.config().target_partitions();
        let n_partitions = if tasks.is_empty() {
            0
        } else {
            target_partitions.min(tasks.len()).max(1)
        };

        // identity_cols is Some(non-empty) iff every condition for declaring
        // Partitioning::Hash is met: the table's default spec has identity-transform
        // fields, every such source column is present in the output projection, and
        // every column type is supported by literal_to_array. Any miss collapses to
        // None, which forces UnknownPartitioning regardless of bucketing strategy.
        let identity_cols = compute_identity_cols(&table, &output_schema);

        let (buckets, all_had_full_key) =
            bucket_tasks(tasks, n_partitions, identity_cols.as_deref());

        let partitioning = match identity_cols {
            Some(cols) if !cols.is_empty() && all_had_full_key && n_partitions > 0 => {
                let exprs: Vec<Arc<dyn PhysicalExpr>> = cols
                    .iter()
                    .map(|c| Arc::new(Column::new(&c.name, c.output_idx)) as Arc<dyn PhysicalExpr>)
                    .collect();
                Partitioning::Hash(exprs, n_partitions)
            }
            _ => Partitioning::UnknownPartitioning(n_partitions),
        };

        Ok(Arc::new(IcebergPartitionedScan::new(
            table,
            None, // Always use current snapshot for catalog-backed provider
            self.schema.clone(),
            projection,
            filters,
            buckets,
            partitioning,
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                // `Exact` is only safe when (1) the filter touches nothing but
                // identity-partition columns and operators preserved by the
                // identity transform, and (2) the iceberg conversion can
                // actually represent the filter, so manifest pruning will
                // remove every row that fails it. Either miss falls back to
                // `Inexact` and DataFusion adds a FilterExec on top.
                if convert_filter_to_predicate(f).is_some()
                    && is_exact_on_identity(f, &self.identity_partition_cols)
                {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Inexact
                }
            })
            .collect())
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _insert_op: datafusion::logical_expr::dml::InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Err(to_datafusion_error(Error::new(
            ErrorKind::FeatureUnsupported,
            "IcebergPartitionedTableProvider does not support writes; \
             use IcebergTableProvider instead",
        )))
    }
}

/// Identity-partitioned column that is also present in the output projection
/// and whose Arrow type can be reconstructed from a `Literal` for hashing.
struct IdentityCol {
    name: String,
    /// Position of this column in the *output* schema (after projection).
    output_idx: usize,
    /// Position of this column inside the partition spec's `fields()` slice,
    /// matching the slot order of `FileScanTask::partition`.
    spec_field_idx: usize,
    output_dtype: DataType,
}

/// Inspect the table's default partition spec and return the list of identity
/// columns that can support a [`Partitioning::Hash`] declaration. Returns
/// `None` if any condition is violated:
///   - the source column for an identity field is not in the output projection
///   - the source column's Arrow type is not currently supported by
///     [`literal_to_array`]
///   - the table has spec evolution (>1 historical specs), since older files
///     may carry a partition tuple that does not align with the default spec
///
/// Returning `None` forces the scan to declare `UnknownPartitioning` even if
/// bucketing succeeds.
fn compute_identity_cols(table: &Table, output_schema: &ArrowSchema) -> Option<Vec<IdentityCol>> {
    let metadata = table.metadata();
    if metadata.partition_specs_iter().len() > 1 {
        return None;
    }
    let spec = metadata.default_partition_spec();
    let table_schema = metadata.current_schema();

    let mut cols = Vec::new();
    for (spec_field_idx, pf) in spec.fields().iter().enumerate() {
        if pf.transform != Transform::Identity {
            continue;
        }
        let source_field = table_schema.field_by_id(pf.source_id)?;
        let output_idx = output_schema.index_of(source_field.name.as_str()).ok()?;
        let output_dtype = output_schema.field(output_idx).data_type().clone();
        if !is_supported_dtype(&output_dtype) {
            return None;
        }
        cols.push(IdentityCol {
            name: source_field.name.clone(),
            output_idx,
            spec_field_idx,
            output_dtype,
        });
    }
    Some(cols)
}

fn is_supported_dtype(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Boolean
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::Date32
    )
}

/// Distribute `tasks` across `n_partitions` buckets. When `identity_cols`
/// describes a non-empty, hashable identity key, each task is hashed on
/// that key using DataFusion's repartition hash so the resulting partitioning
/// matches what `RepartitionExec` would produce on the same data. Tasks
/// missing partition data fall back to hashing `data_file_path`, which still
/// distributes evenly but breaks the `Hash` contract — the second tuple
/// element flags whether every task supplied a full identity key.
fn bucket_tasks(
    tasks: Vec<FileScanTask>,
    n_partitions: usize,
    identity_cols: Option<&[IdentityCol]>,
) -> (Vec<Vec<FileScanTask>>, bool) {
    if n_partitions == 0 {
        return (Vec::new(), tasks.is_empty());
    }
    let mut buckets: Vec<Vec<FileScanTask>> = (0..n_partitions).map(|_| Vec::new()).collect();
    let mut all_full_key = true;
    let cols = identity_cols.unwrap_or(&[]);

    for task in tasks {
        let bucket_idx = match identity_hash(&task, cols) {
            Some(h) => (h % n_partitions as u64) as usize,
            None => {
                all_full_key = false;
                fallback_hash(&task) as usize % n_partitions
            }
        };
        buckets[bucket_idx].push(task);
    }
    (buckets, all_full_key)
}

/// Hash the identity-partition values of `task` using
/// [`REPARTITION_RANDOM_STATE`] so the bucket assignment matches DataFusion's
/// hash-repartition convention. Returns `None` if the task lacks partition
/// data or any required slot is null/unsupported.
fn identity_hash(task: &FileScanTask, cols: &[IdentityCol]) -> Option<u64> {
    if cols.is_empty() {
        return None;
    }
    let partition = task.partition.as_ref()?;
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(cols.len());
    for col in cols {
        let lit = partition.fields().get(col.spec_field_idx)?.as_ref()?;
        arrays.push(literal_to_array(lit, &col.output_dtype)?);
    }
    let mut hashes = vec![0u64; 1];
    create_hashes(
        &arrays,
        REPARTITION_RANDOM_STATE.random_state(),
        &mut hashes,
    )
    .ok()?;
    Some(hashes[0])
}

/// Deterministic per-file fallback used when `identity_hash` cannot produce a
/// bucket. The hash function does not need to match DataFusion's because any
/// task taking this path causes the scan to drop to `UnknownPartitioning`.
fn fallback_hash(task: &FileScanTask) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    task.data_file_path.hash(&mut hasher);
    hasher.finish()
}

/// Intersection of identity-partitioned source-column names across every
/// historical partition spec. A column is included only if every spec
/// includes that column with `Transform::Identity`; any spec where the column
/// is absent or has a non-identity transform drops it from the result.
///
/// Why intersection: Iceberg evaluates partition predicates against each
/// manifest's own spec. A file written under spec A can only be exactly
/// pruned by columns identity-partitioned in spec A. To guarantee Exact
/// pushdown for *every* file in the table, the column must be identity in
/// *every* spec. Otherwise some surviving files would still need DataFusion's
/// FilterExec to enforce the predicate.
fn identity_partition_col_names(table: &Table) -> HashSet<String> {
    let metadata = table.metadata();
    let table_schema = metadata.current_schema();
    let identity_set = |spec: &PartitionSpec| -> HashSet<String> {
        spec.fields()
            .iter()
            .filter(|pf| pf.transform == Transform::Identity)
            .filter_map(|pf| {
                table_schema
                    .field_by_id(pf.source_id)
                    .map(|f| f.name.clone())
            })
            .collect()
    };

    let mut iter = metadata.partition_specs_iter();
    let Some(first) = iter.next() else {
        return HashSet::new();
    };
    let mut acc = identity_set(first);
    for spec in iter {
        if acc.is_empty() {
            break;
        }
        acc = acc.intersection(&identity_set(spec)).cloned().collect();
    }
    acc
}

/// Returns `true` when every leaf of `expr` is a comparison or null check
/// against an identity-partition column. Such filters are fully resolvable
/// by manifest-level partition pruning, so DataFusion does not need to
/// re-apply them post-scan.
///
/// Safe operators: `=`, `!=`, `<`, `<=`, `>`, `>=`, `IS NULL`, `IS NOT NULL`,
/// `IN (..)`, `NOT IN (..)`, plus `AND` / `OR` / `NOT` of any of those. Every
/// other shape returns `false` (caller falls back to `Inexact`).
fn is_exact_on_identity(expr: &Expr, cols: &HashSet<String>) -> bool {
    if cols.is_empty() {
        return false;
    }
    match expr {
        Expr::BinaryExpr(b) => match b.op {
            Operator::And | Operator::Or => {
                is_exact_on_identity(&b.left, cols) && is_exact_on_identity(&b.right, cols)
            }
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => is_simple_compare_on_identity(&b.left, &b.right, cols),
            _ => false,
        },
        Expr::Not(inner) => is_exact_on_identity(inner, cols),
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => is_identity_col(inner, cols),
        Expr::InList(l) => {
            is_identity_col(&l.expr, cols) && l.list.iter().all(|e| matches!(e, Expr::Literal(..)))
        }
        _ => false,
    }
}

fn is_simple_compare_on_identity(l: &Expr, r: &Expr, cols: &HashSet<String>) -> bool {
    let l_col = is_identity_col(l, cols);
    let r_col = is_identity_col(r, cols);
    let l_lit = matches!(l, Expr::Literal(..));
    let r_lit = matches!(r, Expr::Literal(..));
    (l_col && r_lit) || (r_col && l_lit)
}

fn is_identity_col(e: &Expr, cols: &HashSet<String>) -> bool {
    matches!(e, Expr::Column(c) if cols.contains(&c.name))
}

/// Materialize a single-element Arrow array of `dt` holding the value of
/// `lit`. The Arrow type must match what DataFusion will see for this column
/// at scan time, otherwise `create_hashes` would dispatch on a different type
/// and produce a hash that disagrees with DataFusion's row-wise hashing.
fn literal_to_array(lit: &Literal, dt: &DataType) -> Option<ArrayRef> {
    let prim = match lit {
        Literal::Primitive(p) => p,
        _ => return None,
    };
    Some(match (prim, dt) {
        (PrimitiveLiteral::Boolean(v), DataType::Boolean) => Arc::new(BooleanArray::from(vec![*v])),
        (PrimitiveLiteral::Int(v), DataType::Int32) => Arc::new(Int32Array::from(vec![*v])),
        (PrimitiveLiteral::Int(v), DataType::Date32) => Arc::new(Date32Array::from(vec![*v])),
        (PrimitiveLiteral::Long(v), DataType::Int64) => Arc::new(Int64Array::from(vec![*v])),
        (PrimitiveLiteral::Float(v), DataType::Float32) => Arc::new(Float32Array::from(vec![v.0])),
        (PrimitiveLiteral::Double(v), DataType::Float64) => Arc::new(Float64Array::from(vec![v.0])),
        (PrimitiveLiteral::String(v), DataType::Utf8) => {
            Arc::new(StringArray::from(vec![v.as_str()]))
        }
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::logical_expr::{col, lit};
    use datafusion::prelude::{SessionConfig, SessionContext};
    use iceberg::io::FileIO;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, FormatVersion, NestedField,
        PrimitiveType, Schema, SortOrder, TableMetadataBuilder, Transform, Type,
        UnboundPartitionSpec,
    };
    use iceberg::table::Table;
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
    use tempfile::TempDir;

    use super::*;

    async fn make_catalog_and_partitioned_table(
        partition_spec: Option<UnboundPartitionSpec>,
    ) -> (Arc<dyn Catalog>, NamespaceIdent, String, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let warehouse = temp_dir.path().to_str().unwrap().to_string();

        let catalog = Arc::new(
            MemoryCatalogBuilder::default()
                .load(
                    "memory",
                    HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.clone())]),
                )
                .await
                .unwrap(),
        );

        let namespace = NamespaceIdent::new("ns".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();

        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let creation = match partition_spec {
            Some(spec) => TableCreation::builder()
                .name("t".to_string())
                .location(format!("{warehouse}/t"))
                .schema(schema)
                .partition_spec(spec)
                .properties(HashMap::new())
                .build(),
            None => TableCreation::builder()
                .name("t".to_string())
                .location(format!("{warehouse}/t"))
                .schema(schema)
                .properties(HashMap::new())
                .build(),
        };

        catalog.create_table(&namespace, creation).await.unwrap();

        (catalog, namespace, "t".to_string(), temp_dir)
    }

    async fn make_catalog_and_table() -> (Arc<dyn Catalog>, NamespaceIdent, String, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let warehouse = temp_dir.path().to_str().unwrap().to_string();

        let catalog = Arc::new(
            MemoryCatalogBuilder::default()
                .load(
                    "memory",
                    HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.clone())]),
                )
                .await
                .unwrap(),
        );

        let namespace = NamespaceIdent::new("ns".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();

        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("t".to_string())
                    .location(format!("{warehouse}/t"))
                    .schema(schema)
                    .properties(HashMap::new())
                    .build(),
            )
            .await
            .unwrap();

        (catalog, namespace, "t".to_string(), temp_dir)
    }

    /// Registers `n` synthetic data files in the table metadata via the iceberg
    /// transaction API. No actual parquet files are written, only the metadata
    /// entries that `plan_files()` reads are created.
    async fn append_fake_data_files(
        catalog: &Arc<dyn Catalog>,
        namespace: &NamespaceIdent,
        table_name: &str,
        n: usize,
    ) {
        let table = catalog
            .load_table(&TableIdent::new(namespace.clone(), table_name.to_string()))
            .await
            .unwrap();

        let data_files = (0..n)
            .map(|i| {
                DataFileBuilder::default()
                    .content(DataContentType::Data)
                    .file_path(format!(
                        "{}/data/fake_{i}.parquet",
                        table.metadata().location()
                    ))
                    .file_format(DataFileFormat::Parquet)
                    .file_size_in_bytes(128)
                    .record_count(1)
                    .partition_spec_id(table.metadata().default_partition_spec_id())
                    .build()
                    .unwrap()
            })
            .collect::<Vec<_>>();

        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(data_files);
        action
            .apply(tx)
            .unwrap()
            .commit(catalog.as_ref())
            .await
            .unwrap();
    }

    fn ctx_with_target_partitions(n: usize) -> SessionContext {
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(n))
    }

    /// Build a `Table` carrying `specs.len()` historical partition specs. The
    /// first spec is the table's initial spec; each subsequent spec is added
    /// via `into_builder().add_partition_spec(...)`. No catalog round-trip,
    /// no real I/O — `FileIO::new_with_memory()` is sufficient because the
    /// helper under test only reads metadata.
    fn build_table_with_specs(specs: Vec<UnboundPartitionSpec>) -> Table {
        assert!(!specs.is_empty(), "need at least one spec");
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let mut iter = specs.into_iter();
        let first = iter.next().unwrap();
        let mut metadata = TableMetadataBuilder::new(
            schema,
            first,
            SortOrder::unsorted_order(),
            "memory:///t".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;

        for spec in iter {
            metadata = metadata
                .into_builder(None)
                .add_partition_spec(spec)
                .unwrap()
                .build()
                .unwrap()
                .metadata;
        }

        Table::builder()
            .file_io(FileIO::new_with_memory())
            .metadata(Arc::new(metadata))
            .identifier(TableIdent::new(
                NamespaceIdent::new("ns".to_string()),
                "t".to_string(),
            ))
            .build()
            .unwrap()
    }

    /// Multi-spec table where every historical spec keeps `id` as identity:
    /// the column survives the intersection and remains Exact-pushdown safe.
    /// `name` is never identity-partitioned, so it is excluded.
    #[test]
    fn test_identity_cols_preserved_across_compatible_specs() {
        let spec_v0 = UnboundPartitionSpec::builder()
            .add_partition_field(1, "id_part", Transform::Identity)
            .unwrap()
            .build();
        // Evolved spec: still identity on `id`, plus a non-identity transform
        // on `name`. The latter must not pollute the result.
        let spec_v1 = UnboundPartitionSpec::builder()
            .add_partition_field(1, "id_part", Transform::Identity)
            .unwrap()
            .add_partition_field(2, "name_bucket", Transform::Bucket(8))
            .unwrap()
            .build();
        let table = build_table_with_specs(vec![spec_v0, spec_v1]);

        let cols = identity_partition_col_names(&table);
        assert_eq!(cols, HashSet::from(["id".to_string()]));
    }

    /// Multi-spec table where the evolved spec replaces `identity(id)` with
    /// `bucket(id)`. Files written under the evolved spec cannot be exactly
    /// pruned on `id`, so `id` must be dropped from the Exact-safe set.
    #[test]
    fn test_identity_cols_dropped_when_transform_changes() {
        let spec_v0 = UnboundPartitionSpec::builder()
            .add_partition_field(1, "id_part", Transform::Identity)
            .unwrap()
            .build();
        let spec_v1 = UnboundPartitionSpec::builder()
            .add_partition_field(1, "id_bucket", Transform::Bucket(8))
            .unwrap()
            .build();
        let table = build_table_with_specs(vec![spec_v0, spec_v1]);

        let cols = identity_partition_col_names(&table);
        assert!(
            cols.is_empty(),
            "expected empty set after non-identity replacement, got {cols:?}"
        );
    }

    /// Multi-spec table where the second spec omits `id` from partitioning
    /// entirely. Files under that spec carry no `id` partition tuple, so
    /// pruning is a no-op for them — `id` must be dropped from the
    /// Exact-safe set.
    #[test]
    fn test_identity_cols_dropped_when_column_missing_from_some_spec() {
        let spec_v0 = UnboundPartitionSpec::builder()
            .add_partition_field(1, "id_part", Transform::Identity)
            .unwrap()
            .build();
        // Evolved spec only partitions on `name`, omitting `id`.
        let spec_v1 = UnboundPartitionSpec::builder()
            .add_partition_field(2, "name_part", Transform::Identity)
            .unwrap()
            .build();
        let table = build_table_with_specs(vec![spec_v0, spec_v1]);

        let cols = identity_partition_col_names(&table);
        // Neither column survives the intersection: `id` missing from v1,
        // `name` missing from v0.
        assert!(cols.is_empty(), "got {cols:?}");
    }

    /// An empty table must produce a zero-partition scan so DataFusion never calls
    /// execute(0), which would otherwise return an out-of-bounds error.
    #[tokio::test]
    async fn test_empty_table_zero_partitions() {
        let (catalog, namespace, table_name, _temp_dir) = make_catalog_and_table().await;
        // no files appended
        let provider = IcebergPartitionedTableProvider::try_new(catalog, namespace, table_name)
            .await
            .unwrap();
        let plan = provider
            .scan(&ctx_with_target_partitions(8).state(), None, &[], None)
            .await
            .unwrap();
        let scan = plan
            .as_any()
            .downcast_ref::<IcebergPartitionedScan>()
            .unwrap();

        assert_eq!(scan.buckets().len(), 0);
        assert_eq!(scan.properties().partitioning.partition_count(), 0);
    }

    /// When the table has no identity-partition columns, every task takes the
    /// fallback (file_path) bucket path, so the declaration must drop to
    /// `UnknownPartitioning`. The bucket count should still equal
    /// min(target_partitions, num_files).
    #[tokio::test]
    async fn test_unpartitioned_falls_back_to_unknown() {
        let (catalog, namespace, table_name, _temp_dir) = make_catalog_and_table().await;
        append_fake_data_files(&catalog, &namespace, &table_name, 5).await;

        let provider = IcebergPartitionedTableProvider::try_new(catalog, namespace, table_name)
            .await
            .unwrap();
        let plan = provider
            .scan(&ctx_with_target_partitions(3).state(), None, &[], None)
            .await
            .unwrap();
        let scan = plan
            .as_any()
            .downcast_ref::<IcebergPartitionedScan>()
            .unwrap();

        let total_files: usize = scan.buckets().iter().map(|b| b.len()).sum();
        assert_eq!(total_files, 5);
        assert_eq!(scan.buckets().len(), 3);
        assert!(matches!(
            scan.properties().partitioning,
            Partitioning::UnknownPartitioning(3)
        ));
    }

    /// Bucket count must be capped at the number of files: spinning up more
    /// DataFusion partitions than there are tasks would just leave empty
    /// streams, wasting scheduler slots.
    #[tokio::test]
    async fn test_bucket_count_capped_at_file_count() {
        let (catalog, namespace, table_name, _temp_dir) = make_catalog_and_table().await;
        append_fake_data_files(&catalog, &namespace, &table_name, 2).await;

        let provider = IcebergPartitionedTableProvider::try_new(catalog, namespace, table_name)
            .await
            .unwrap();
        let plan = provider
            .scan(&ctx_with_target_partitions(16).state(), None, &[], None)
            .await
            .unwrap();
        let scan = plan
            .as_any()
            .downcast_ref::<IcebergPartitionedScan>()
            .unwrap();

        assert_eq!(scan.buckets().len(), 2);
    }

    /// Filters that only touch identity-partition columns with literal RHS
    /// can be marked `Exact` because Iceberg's manifest-level pruning already
    /// removes every file whose partition value fails the predicate.
    #[tokio::test]
    async fn test_pushdown_exact_on_identity_column() {
        let spec = UnboundPartitionSpec::builder()
            .add_partition_field(1, "id_part", Transform::Identity)
            .unwrap()
            .build();
        let (catalog, namespace, table_name, _temp_dir) =
            make_catalog_and_partitioned_table(Some(spec)).await;
        let provider = IcebergPartitionedTableProvider::try_new(catalog, namespace, table_name)
            .await
            .unwrap();

        let f_eq = col("id").eq(lit(5_i32));
        let f_neq = col("id").not_eq(lit(5_i32));
        let f_isnull = col("id").is_null();
        let f_and = col("id").eq(lit(5_i32)).and(col("id").lt(lit(10_i32)));

        let supports = provider
            .supports_filters_pushdown(&[&f_eq, &f_neq, &f_isnull, &f_and])
            .unwrap();
        for (i, s) in supports.iter().enumerate() {
            assert!(
                matches!(s, TableProviderFilterPushDown::Exact),
                "filter index {i} should be Exact, got {s:?}"
            );
        }
    }

    /// Filters touching non-partition columns or columns with non-identity
    /// transforms must remain `Inexact`: the partition value is either
    /// missing or lossy (bucket/truncate/etc.), so DataFusion still needs to
    /// re-apply the filter against actual row values.
    #[tokio::test]
    async fn test_pushdown_inexact_on_non_identity_column() {
        let spec = UnboundPartitionSpec::builder()
            .add_partition_field(1, "id_part", Transform::Identity)
            .unwrap()
            .build();
        let (catalog, namespace, table_name, _temp_dir) =
            make_catalog_and_partitioned_table(Some(spec)).await;
        let provider = IcebergPartitionedTableProvider::try_new(catalog, namespace, table_name)
            .await
            .unwrap();

        // `name` is not partitioned — manifest pruning cannot eliminate files
        // by it, so the filter must re-execute post-scan.
        let f_name = col("name").eq(lit("alice"));
        // Mixed AND: even though `id` is identity-partitioned, the `name` arm
        // is not exact, so the whole expression is Inexact.
        let f_mixed = col("id").eq(lit(5_i32)).and(col("name").eq(lit("alice")));

        let supports = provider
            .supports_filters_pushdown(&[&f_name, &f_mixed])
            .unwrap();
        for (i, s) in supports.iter().enumerate() {
            assert!(
                matches!(s, TableProviderFilterPushDown::Inexact),
                "filter index {i} should be Inexact, got {s:?}"
            );
        }
    }

    /// Unpartitioned tables must mark every filter `Inexact` regardless of
    /// shape: there is no partition pruning that could make the scan
    /// authoritative.
    #[tokio::test]
    async fn test_pushdown_unpartitioned_table_all_inexact() {
        let (catalog, namespace, table_name, _temp_dir) = make_catalog_and_table().await;
        let provider = IcebergPartitionedTableProvider::try_new(catalog, namespace, table_name)
            .await
            .unwrap();

        let f_id = col("id").eq(lit(5_i32));
        let f_name = col("name").eq(lit("alice"));
        let supports = provider
            .supports_filters_pushdown(&[&f_id, &f_name])
            .unwrap();
        assert!(matches!(supports[0], TableProviderFilterPushDown::Inexact));
        assert!(matches!(supports[1], TableProviderFilterPushDown::Inexact));
    }

    /// target_partitions = 1 collapses every task into a single bucket, giving
    /// the same execution profile as `IcebergTableScan`.
    #[tokio::test]
    async fn test_single_target_partition_single_bucket() {
        let (catalog, namespace, table_name, _temp_dir) = make_catalog_and_table().await;
        append_fake_data_files(&catalog, &namespace, &table_name, 4).await;

        let provider = IcebergPartitionedTableProvider::try_new(catalog, namespace, table_name)
            .await
            .unwrap();
        let plan = provider
            .scan(&ctx_with_target_partitions(1).state(), None, &[], None)
            .await
            .unwrap();
        let scan = plan
            .as_any()
            .downcast_ref::<IcebergPartitionedScan>()
            .unwrap();

        assert_eq!(scan.buckets().len(), 1);
        assert_eq!(scan.buckets()[0].len(), 4);
    }
}
