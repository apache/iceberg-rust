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
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::repartition::REPARTITION_RANDOM_STATE;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use futures::TryStreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::scan::FileScanTask;
use iceberg::spec::{Literal, PrimitiveLiteral, Transform};
use iceberg::table::Table;
use iceberg::{Catalog, Error, ErrorKind, NamespaceIdent, Result, TableIdent};

use crate::error::to_datafusion_error;
use crate::physical_plan::expr_to_predicate::convert_filters_to_predicate;
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
        Ok(Self {
            catalog,
            table_ident,
            schema,
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
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
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

    use datafusion::prelude::{SessionConfig, SessionContext};
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, NestedField, PrimitiveType, Schema, Type,
    };
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
    use tempfile::TempDir;

    use super::*;

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
