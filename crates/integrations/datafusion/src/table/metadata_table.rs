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

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::inspect::MetadataTableType;
use iceberg::table::Table;

use crate::physical_plan::metadata_scan::IcebergMetadataScan;
use crate::to_datafusion_error;

/// Represents a [`TableProvider`] for the Iceberg [`Catalog`],
/// managing access to a [`MetadataTable`].
#[derive(Debug, Clone)]
pub struct IcebergMetadataTableProvider {
    pub(crate) table: Table,
    pub(crate) r#type: MetadataTableType,
    /// Optional serializable catalog/storage config, populated when this provider
    /// is built through a config-backed table provider so that a distributed
    /// engine can reconstruct it (reload the table from the catalog) on a remote
    /// node.
    catalog_config: Option<crate::IcebergCatalogConfig>,
}

impl IcebergMetadataTableProvider {
    /// Creates a metadata-table provider over an already-loaded table.
    pub fn new(table: Table, r#type: MetadataTableType) -> Self {
        Self {
            table,
            r#type,
            catalog_config: None,
        }
    }

    /// Attaches a serializable catalog/storage config so that a distributed engine
    /// can reconstruct this provider on a remote node.
    pub fn with_catalog_config(
        mut self,
        catalog_config: Option<crate::IcebergCatalogConfig>,
    ) -> Self {
        self.catalog_config = catalog_config;
        self
    }

    /// Returns the serializable catalog/storage config, if any.
    pub fn catalog_config(&self) -> Option<&crate::IcebergCatalogConfig> {
        self.catalog_config.as_ref()
    }

    /// Returns the table this provider inspects.
    pub fn table(&self) -> &Table {
        &self.table
    }

    /// Returns which metadata table this provider serves.
    pub fn metadata_type(&self) -> &MetadataTableType {
        &self.r#type
    }
}

#[async_trait]
impl TableProvider for IcebergMetadataTableProvider {
    fn schema(&self) -> ArrowSchemaRef {
        let metadata_table = self.table.inspect();
        let schema = match self.r#type {
            MetadataTableType::Snapshots => metadata_table.snapshots().schema(),
            MetadataTableType::Manifests => metadata_table.manifests().schema(),
        };
        schema_to_arrow_schema(&schema).unwrap().into()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(IcebergMetadataScan::new(self.clone())))
    }
}

impl IcebergMetadataTableProvider {
    pub async fn scan(self) -> DFResult<BoxStream<'static, DFResult<RecordBatch>>> {
        let metadata_table = self.table.inspect();
        let stream = match self.r#type {
            MetadataTableType::Snapshots => metadata_table.snapshots().scan().await,
            MetadataTableType::Manifests => metadata_table.manifests().scan().await,
        }
        .map_err(to_datafusion_error)?;
        let stream = stream.map_err(to_datafusion_error);
        Ok(Box::pin(stream))
    }
}
