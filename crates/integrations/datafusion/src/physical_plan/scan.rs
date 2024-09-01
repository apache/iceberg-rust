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
use std::pin::Pin;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::prelude::Expr;
use futures::{Stream, TryStreamExt};
use iceberg::expr::Predicate;
use iceberg::table::Table;

use super::predicate_converter::PredicateConverter;
use crate::to_datafusion_error;

/// Manages the scanning process of an Iceberg [`Table`], encapsulating the
/// necessary details and computed properties required for execution planning.
#[derive(Debug)]
pub(crate) struct IcebergTableScan {
    /// A table in the catalog.
    table: Table,
    /// A reference-counted arrow `Schema`.
    schema: ArrowSchemaRef,
    /// Stores certain, often expensive to compute,
    /// plan properties used in query optimization.
    plan_properties: PlanProperties,
    predicates: Option<Predicate>,
}

impl IcebergTableScan {
    /// Creates a new [`IcebergTableScan`] object.
    pub(crate) fn new(table: Table, schema: ArrowSchemaRef, filters: &[Expr]) -> Self {
        let plan_properties = Self::compute_properties(schema.clone());
        let predicates = convert_filters_to_predicate(filters);
        Self {
            table,
            schema,
            plan_properties,
            predicates,
        }
    }

    /// Computes [`PlanProperties`] used in query optimization.
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
    fn name(&self) -> &str {
        "IcebergTableScan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<(dyn ExecutionPlan + 'static)>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let fut = get_batch_stream(self.table.clone(), self.predicates.clone());
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

/// Asynchronously retrieves a stream of [`RecordBatch`] instances
/// from a given table.
///
/// This function initializes a [`TableScan`], builds it,
/// and then converts it into a stream of Arrow [`RecordBatch`]es.
async fn get_batch_stream(
    table: Table,
    predicates: Option<Predicate>,
) -> DFResult<Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>> {
    let mut scan_builder = table.scan();
    if let Some(pred) = predicates {
        scan_builder = scan_builder.with_filter(pred);
    }
    let table_scan = scan_builder.build().map_err(to_datafusion_error)?;
    let stream = table_scan
        .to_arrow()
        .await
        .map_err(to_datafusion_error)?
        .map_err(to_datafusion_error);
    Ok(Box::pin(stream))
}

/// Converts DataFusion filters ([`Expr`]) to an iceberg [`Predicate`].
/// If none of the filters could be converted, return `None` which adds no predicates to the scan operation.
/// If the conversion was successful, return the converted predicates combined with an AND operator.
fn convert_filters_to_predicate(filters: &[Expr]) -> Option<Predicate> {
    PredicateConverter.visit_many(filters)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::DFSchema;
    use datafusion::prelude::SessionContext;
    use iceberg::expr::Reference;
    use iceberg::spec::Datum;

    use super::*;

    fn create_test_schema() -> DFSchema {
        let arrow_schema = Schema::new(vec![
            Field::new("foo", DataType::Int32, false),
            Field::new("bar", DataType::Utf8, false),
        ]);
        DFSchema::try_from_qualified_schema("my_table", &arrow_schema).unwrap()
    }

    #[test]
    fn test_predicate_conversion_with_single_condition() {
        let sql = "foo > 1";
        let df_schema = create_test_schema();
        let expr = SessionContext::new()
            .parse_sql_expr(sql, &df_schema)
            .unwrap();
        let predicate = convert_filters_to_predicate(&[expr]).unwrap();
        assert_eq!(
            predicate,
            Reference::new("foo").greater_than(Datum::long(1))
        );
    }
    #[test]
    fn test_predicate_conversion_with_single_unsupported_condition() {
        let sql = "foo is null";
        let df_schema = create_test_schema();
        let expr = SessionContext::new()
            .parse_sql_expr(sql, &df_schema)
            .unwrap();
        let predicate = convert_filters_to_predicate(&[expr]);
        assert_eq!(predicate, None);
    }

    #[test]
    fn test_predicate_conversion_with_and_condition() {
        let sql = "foo > 1 and bar = 'test'";
        let df_schema = create_test_schema();
        let expr = SessionContext::new()
            .parse_sql_expr(sql, &df_schema)
            .unwrap();
        let predicate = convert_filters_to_predicate(&[expr]).unwrap();
        let expected_predicate = Predicate::and(
            Reference::new("foo").greater_than(Datum::long(1)),
            Reference::new("bar").equal_to(Datum::string("test")),
        );
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_and_condition_unsupported() {
        let sql = "foo > 1 and bar is not null";
        let df_schema = create_test_schema();
        let expr = SessionContext::new()
            .parse_sql_expr(sql, &df_schema)
            .unwrap();
        let predicate = convert_filters_to_predicate(&[expr]).unwrap();
        let expected_predicate = Reference::new("foo").greater_than(Datum::long(1));
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_or_condition_unsupported() {
        let sql = "foo > 1 or bar is not null";
        let df_schema = create_test_schema();
        let expr = SessionContext::new()
            .parse_sql_expr(sql, &df_schema)
            .unwrap();
        let predicate = convert_filters_to_predicate(&[expr]);
        let expected_predicate = None;
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_complex_binary_expr() {
        let sql = "(foo > 1 and bar = 'test') or foo < 0 ";
        let df_schema = create_test_schema();
        let expr = SessionContext::new()
            .parse_sql_expr(sql, &df_schema)
            .unwrap();
        let predicate = convert_filters_to_predicate(&[expr]).unwrap();
        let inner_predicate = Predicate::and(
            Reference::new("foo").greater_than(Datum::long(1)),
            Reference::new("bar").equal_to(Datum::string("test")),
        );
        let expected_predicate = Predicate::or(
            inner_predicate,
            Reference::new("foo").less_than(Datum::long(0)),
        );
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_complex_binary_expr_unsupported() {
        let sql = "(foo > 1 or bar in ('test', 'test2')) and foo < 0 ";
        let df_schema = create_test_schema();
        let expr = SessionContext::new()
            .parse_sql_expr(sql, &df_schema)
            .unwrap();
        let predicate = convert_filters_to_predicate(&[expr]).unwrap();
        let expected_predicate = Reference::new("foo").less_than(Datum::long(0));
        assert_eq!(predicate, expected_predicate);
    }
}
