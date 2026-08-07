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
use futures::TryStreamExt;

use crate::metadata_table::IcebergMetadataTableProvider;

#[derive(Debug)]
pub struct IcebergMetadataScan {
    provider: IcebergMetadataTableProvider,
    properties: Arc<PlanProperties>,
}

impl IcebergMetadataScan {
    pub fn new(provider: IcebergMetadataTableProvider) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(provider.schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            provider,
            properties,
        }
    }

    /// Returns the metadata-table provider this node scans, so a distributed
    /// engine can serialize the catalog config + table identifier + metadata type
    /// it carries and rebuild it on a remote node.
    pub fn provider(&self) -> &IcebergMetadataTableProvider {
        &self.provider
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
        let fut = self.provider.clone().scan();
        let stream = futures::stream::once(fut).try_flatten();
        let schema = self.provider.schema();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
