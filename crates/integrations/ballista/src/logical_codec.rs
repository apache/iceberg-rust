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

//! Logical extension codec that serializes the catalog-backed
//! [`IcebergTableProvider`] (its [`IcebergCatalogConfig`] + table identifier) so
//! that the Ballista scheduler can rebuild the provider from a logical plan and
//! perform physical planning (including `insert_into`) for Iceberg tables.
//!
//! All other logical-plan serialization (extension nodes, file formats, other
//! table providers) is delegated to an inner codec (by default Ballista's
//! [`BallistaLogicalExtensionCodec`]).

use std::fmt::Debug;
use std::sync::Arc;

use ballista_core::serde::BallistaLogicalExtensionCodec;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::DataFusionError;
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::sql::TableReference;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use iceberg::TableIdent;
use iceberg::inspect::MetadataTableType;
use iceberg_datafusion::{IcebergMetadataTableProvider, IcebergTableProvider};
use serde::{Deserialize, Serialize};

use crate::serde::{
    CatalogConfigProto, TAG_DELEGATED, TAG_ICEBERG, block_on, get_catalog, load_table, to_df_err,
};

/// Wire representation of an Iceberg table provider. Carries enough to rebuild
/// either the catalog-backed data provider or a metadata-table provider on a
/// remote node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
enum IcebergProviderProto {
    /// The catalog-backed [`IcebergTableProvider`].
    Table {
        catalog: CatalogConfigProto,
        table: TableIdent,
        /// Pinned snapshot for time-travel reads, if any.
        #[serde(default)]
        snapshot_id: Option<i64>,
    },
    /// An [`IcebergMetadataTableProvider`] (e.g. `tbl$snapshots`).
    Metadata {
        catalog: CatalogConfigProto,
        table: TableIdent,
        /// The metadata table kind, as its lowercase string name.
        metadata_type: String,
    },
}

/// A [`LogicalExtensionCodec`] that understands the catalog-backed
/// [`IcebergTableProvider`] and delegates everything else to an inner codec.
#[derive(Debug)]
pub struct IcebergLogicalCodec {
    inner: Arc<dyn LogicalExtensionCodec>,
}

impl Default for IcebergLogicalCodec {
    fn default() -> Self {
        Self {
            inner: Arc::new(BallistaLogicalExtensionCodec::default()),
        }
    }
}

impl IcebergLogicalCodec {
    /// Creates a codec that delegates non-Iceberg work to `inner`.
    pub fn new(inner: Arc<dyn LogicalExtensionCodec>) -> Self {
        Self { inner }
    }
}

impl LogicalExtensionCodec for IcebergLogicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[LogicalPlan],
        ctx: &TaskContext,
    ) -> Result<Extension, DataFusionError> {
        self.inner.try_decode(buf, inputs, ctx)
    }

    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> Result<(), DataFusionError> {
        self.inner.try_encode(node, buf)
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &TableReference,
        schema: SchemaRef,
        ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        let Some((&tag, rest)) = buf.split_first() else {
            return Err(DataFusionError::Internal(
                "empty iceberg logical table-provider buffer".to_string(),
            ));
        };
        match tag {
            TAG_DELEGATED => self
                .inner
                .try_decode_table_provider(rest, table_ref, schema, ctx),
            TAG_ICEBERG => {
                let proto: IcebergProviderProto =
                    serde_json::from_slice(rest).map_err(to_df_err)?;
                match proto {
                    IcebergProviderProto::Table {
                        catalog,
                        table,
                        snapshot_id,
                    } => {
                        let config = catalog.into();
                        let cat = get_catalog(&config)?;
                        let TableIdent { namespace, name } = table;
                        let provider = block_on(IcebergTableProvider::try_new_with_config(
                            cat, config, namespace, name,
                        ))
                        .map_err(to_df_err)?
                        .with_snapshot_id(snapshot_id);
                        Ok(Arc::new(provider))
                    }
                    IcebergProviderProto::Metadata {
                        catalog,
                        table,
                        metadata_type,
                    } => {
                        let config = catalog.into();
                        let table_obj = load_table(&config, &table)?;
                        let kind = MetadataTableType::try_from(metadata_type.as_str())
                            .map_err(DataFusionError::Internal)?;
                        let provider = IcebergMetadataTableProvider::new(table_obj, kind)
                            .with_catalog_config(Some(config));
                        Ok(Arc::new(provider))
                    }
                }
            }
            other => Err(DataFusionError::Internal(format!(
                "unknown iceberg logical table-provider tag {other}"
            ))),
        }
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError> {
        if let Some(provider) = node.as_any().downcast_ref::<IcebergTableProvider>() {
            let config = provider.config().ok_or_else(|| {
                DataFusionError::Internal(
                    "IcebergTableProvider has no IcebergCatalogConfig and cannot be \
                     distributed; register it with \
                     IcebergTableProvider::try_new_with_config (see \
                     iceberg_ballista::register_iceberg_table)."
                        .to_string(),
                )
            })?;
            let proto = IcebergProviderProto::Table {
                catalog: config.into(),
                table: provider.table_ident().clone(),
                snapshot_id: provider.snapshot_id(),
            };
            buf.push(TAG_ICEBERG);
            buf.extend_from_slice(&serde_json::to_vec(&proto).map_err(to_df_err)?);
            return Ok(());
        }
        if let Some(provider) = node.as_any().downcast_ref::<IcebergMetadataTableProvider>() {
            let config = provider.catalog_config().ok_or_else(|| {
                DataFusionError::Internal(
                    "IcebergMetadataTableProvider has no IcebergCatalogConfig and cannot be \
                     distributed; register the catalog with \
                     IcebergCatalogProvider::try_new_with_config so its tables carry it."
                        .to_string(),
                )
            })?;
            let proto = IcebergProviderProto::Metadata {
                catalog: config.into(),
                table: provider.table().identifier().clone(),
                metadata_type: provider.metadata_type().as_str().to_string(),
            };
            buf.push(TAG_ICEBERG);
            buf.extend_from_slice(&serde_json::to_vec(&proto).map_err(to_df_err)?);
            return Ok(());
        }
        buf.push(TAG_DELEGATED);
        self.inner.try_encode_table_provider(table_ref, node, buf)
    }

    fn try_decode_file_format(
        &self,
        buf: &[u8],
        ctx: &TaskContext,
    ) -> Result<Arc<dyn FileFormatFactory>, DataFusionError> {
        self.inner.try_decode_file_format(buf, ctx)
    }

    fn try_encode_file_format(
        &self,
        buf: &mut Vec<u8>,
        node: Arc<dyn FileFormatFactory>,
    ) -> Result<(), DataFusionError> {
        self.inner.try_encode_file_format(buf, node)
    }
}
