use std::{any::Any, pin::Pin, sync::Arc};

use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef as ArrowSchemaRef},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, ExecutionMode, ExecutionPlan, Partitioning,
        PlanProperties,
    },
};
use futures::{Stream, TryStreamExt};
use iceberg::table::Table;

use crate::to_datafusion_error;

#[derive(Debug)]
pub(crate) struct IcebergTableScan {
    table: Table,
    schema: ArrowSchemaRef,
    plan_properties: PlanProperties,
}

impl IcebergTableScan {
    pub(crate) fn new(table: Table, schema: ArrowSchemaRef) -> Self {
        let plan_properties = Self::compute_properties(schema.clone());

        Self {
            table,
            schema,
            plan_properties,
        }
    }

    fn compute_properties(schema: ArrowSchemaRef) -> PlanProperties {
        // TODO:
        // This is more or less a placeholder, to be replaced
        // once we support output-partitioning
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        )
    }
}

impl ExecutionPlan for IcebergTableScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let fut = get_batch_stream(self.table.clone());
        let stream = futures::stream::once(fut).try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

impl DisplayAs for IcebergTableScan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "IcebergTableScan")
    }
}

async fn get_batch_stream(
    table: Table,
) -> datafusion::error::Result<
    Pin<Box<dyn Stream<Item = datafusion::error::Result<RecordBatch>> + Send>>,
> {
    let table_scan = table.scan().build().map_err(to_datafusion_error)?;

    let stream = table_scan
        .to_arrow()
        .await
        .map_err(to_datafusion_error)?
        .map_err(to_datafusion_error);

    Ok(Box::pin(stream))
}
