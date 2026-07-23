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

use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use futures::{StreamExt, TryStreamExt};

use crate::metadata_table::IcebergMetadataTableProvider;

#[derive(Debug)]
pub struct IcebergMetadataScan {
    provider: IcebergMetadataTableProvider,
    properties: Arc<PlanProperties>,
    /// Column indices to project, `None` means all columns.
    projection: Option<Vec<usize>>,
}

impl IcebergMetadataScan {
    pub fn new(provider: IcebergMetadataTableProvider, projection: Option<&Vec<usize>>) -> Self {
        let output_schema = match projection {
            None => provider.schema(),
            Some(projection) => Arc::new(provider.schema().project(projection).unwrap()),
        };
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            provider,
            properties,
            projection: projection.cloned(),
        }
    }
}

impl DisplayAs for IcebergMetadataScan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "IcebergMetadataScan")
    }
}

impl ExecutionPlan for IcebergMetadataScan {
    fn name(&self) -> &str {
        "IcebergMetadataScan"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        let projection = self.projection.clone();
        let fut = self.provider.clone().scan();
        let stream = futures::stream::once(fut).try_flatten();
        let stream = stream.map(move |batch| {
            let batch = batch?;
            match &projection {
                Some(projection) => Ok(batch.project(projection)?),
                None => Ok(batch),
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}
