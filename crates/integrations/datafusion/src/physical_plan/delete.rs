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

//! Execution plan for partition-aligned DELETE on an Iceberg table.
//!
//! Only deletes whose filter aligns with partition boundaries are accepted:
//! every data file must be either wholly covered by the predicate (and is
//! dropped) or wholly untouched by it. A file that the predicate straddles
//! produces a hard error — the caller must use a column/row-level delete
//! strategy for that shape of predicate, which this plan does not implement.

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::StreamExt;
use iceberg::expr::{PartitionCoverage, PartitionCoverageFilter, Predicate};
use iceberg::spec::{DataContentType, DataFile, PartitionSpecRef};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, Error, ErrorKind, TableIdent};

use crate::physical_plan::expr_to_predicate::convert_filters_to_predicate_strict;
use crate::to_datafusion_error;

/// Execution plan for partition-aligned DELETE. Has no child plan — all work
/// happens inside [`execute`](ExecutionPlan::execute): load fresh table state,
/// classify files, commit an [`OverwriteAction`](iceberg::transaction::OverwriteAction)
/// that drops the fully-covered files, and emit a single `count` row.
#[derive(Debug)]
pub(crate) struct IcebergDeleteExec {
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    filters: Vec<Expr>,
    count_schema: ArrowSchemaRef,
    plan_properties: Arc<PlanProperties>,
}

impl IcebergDeleteExec {
    pub fn new(catalog: Arc<dyn Catalog>, table_ident: TableIdent, filters: Vec<Expr>) -> Self {
        let count_schema = Self::make_count_schema();
        let plan_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(count_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            catalog,
            table_ident,
            filters,
            count_schema,
            plan_properties,
        }
    }

    fn make_count_schema() -> ArrowSchemaRef {
        Arc::new(ArrowSchema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]))
    }

    fn make_count_batch(count: u64) -> DFResult<RecordBatch> {
        let array = Arc::new(UInt64Array::from(vec![count])) as ArrayRef;
        RecordBatch::try_from_iter_with_nullable(vec![("count", array, false)]).map_err(|e| {
            DataFusionError::ArrowError(
                Box::new(e),
                Some("Failed to build DELETE count batch".to_string()),
            )
        })
    }
}

impl DisplayAs for IcebergDeleteExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::TreeRender => {
                write!(f, "IcebergDeleteExec: table={}", self.table_ident)
            }
            DisplayFormatType::Verbose => write!(
                f,
                "IcebergDeleteExec: table={}, filters={:?}",
                self.table_ident, self.filters
            ),
        }
    }
}

impl ExecutionPlan for IcebergDeleteExec {
    fn name(&self) -> &str {
        "IcebergDeleteExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(format!(
                "IcebergDeleteExec expects no children, got {}",
                children.len()
            )));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "IcebergDeleteExec only has one partition, got partition {partition}"
            )));
        }

        let catalog = self.catalog.clone();
        let table_ident = self.table_ident.clone();
        let filters = self.filters.clone();

        let stream = futures::stream::once(async move {
            let deleted = run_partition_aligned_delete(catalog.as_ref(), &table_ident, &filters)
                .await
                .map_err(to_datafusion_error)?;
            Self::make_count_batch(deleted)
        })
        .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.count_schema.clone(),
            stream,
        )))
    }
}

/// Run a partition-aligned DELETE against the table identified by `table_ident`.
/// Returns the number of rows affected.
async fn run_partition_aligned_delete(
    catalog: &dyn Catalog,
    table_ident: &TableIdent,
    filters: &[Expr],
) -> iceberg::Result<u64> {
    // 1. Convert DataFusion filters to a single Iceberg predicate. Empty filters
    //    means "DELETE everything" (effectively TRUNCATE); any unconvertible
    //    filter errors out.
    let predicate = convert_filters_to_predicate_strict(filters).map_err(|msg| {
        Error::new(
            ErrorKind::FeatureUnsupported,
            format!(
                "DELETE filter cannot be pushed into Iceberg: {msg}. \
                 This DELETE would have to be evaluated row-by-row, which \
                 partition-aligned delete does not support."
            ),
        )
    })?;

    // 2. Load fresh table state.
    let table = catalog.load_table(table_ident).await?;
    let Some(snapshot) = table.metadata().current_snapshot() else {
        // Empty table — nothing to delete.
        return Ok(0);
    };
    let schema = snapshot.schema(table.metadata())?;

    // 3. Classify each data file in the current snapshot.
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await?;

    let mut filters_by_spec: std::collections::HashMap<i32, SpecFilter> =
        std::collections::HashMap::new();
    let mut to_delete: Vec<DataFile> = Vec::new();
    let mut total_record_count: u64 = 0;

    for manifest_file in manifest_list.entries() {
        if manifest_file.content != iceberg::spec::ManifestContentType::Data {
            // Skip delete-file manifests — we only touch data files here.
            continue;
        }

        let manifest = manifest_file.load_manifest(table.file_io()).await?;
        for entry in manifest.entries() {
            if !entry.is_alive() {
                continue;
            }
            if entry.data_file().content_type() != DataContentType::Data {
                continue;
            }

            let data_file = entry.data_file();
            let spec_id = data_file.partition_spec_id();

            let spec_filter = match filters_by_spec.get(&spec_id) {
                Some(f) => f,
                None => {
                    let partition_spec = table
                        .metadata()
                        .partition_spec_by_id(spec_id)
                        .ok_or_else(|| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                format!(
                                    "Data file {} references partition spec {} which is not in table metadata",
                                    data_file.file_path(),
                                    spec_id
                                ),
                            )
                        })?
                        .clone();
                    let filter = build_spec_filter(predicate.as_ref(), &schema, partition_spec)?;
                    filters_by_spec.entry(spec_id).or_insert(filter)
                }
            };

            match spec_filter.classify(data_file)? {
                PartitionCoverage::AllRowsMatch => {
                    total_record_count += data_file.record_count();
                    to_delete.push(data_file.clone());
                }
                PartitionCoverage::NoRowsMatch => {}
                PartitionCoverage::Straddle => {
                    return Err(Error::new(
                        ErrorKind::FeatureUnsupported,
                        format!(
                            "DELETE predicate does not align with partition boundaries: \
                             data file {} would need row-level filtering. \
                             Partition-aligned delete only supports predicates that \
                             drop whole files.",
                            data_file.file_path()
                        ),
                    ));
                }
            }
        }
    }

    if to_delete.is_empty() {
        return Ok(0);
    }

    // 4. Commit the overwrite (with no added files) via the catalog.
    let tx = Transaction::new(&table);
    let action = tx.overwrite().delete_data_files(to_delete);
    action.apply(tx)?.commit(catalog).await?;

    Ok(total_record_count)
}

/// A [`PartitionCoverageFilter`] paired with metadata that the caller can use
/// to route files to the right instance (tables with evolved partition specs
/// have more than one).
struct SpecFilter {
    filter: PartitionCoverageFilter,
}

impl SpecFilter {
    fn classify(&self, data_file: &DataFile) -> iceberg::Result<PartitionCoverage> {
        self.filter.classify(data_file)
    }
}

fn build_spec_filter(
    predicate: Option<&Predicate>,
    schema: &iceberg::spec::SchemaRef,
    partition_spec: PartitionSpecRef,
) -> iceberg::Result<SpecFilter> {
    match predicate {
        // Empty filter: every file is fully covered regardless of spec.
        None => Ok(SpecFilter {
            filter: PartitionCoverageFilter::try_new(
                &Predicate::AlwaysTrue,
                schema.clone(),
                partition_spec,
                true,
            )?,
        }),
        Some(p) => Ok(SpecFilter {
            filter: PartitionCoverageFilter::try_new(p, schema.clone(), partition_spec, true)?,
        }),
    }
}
