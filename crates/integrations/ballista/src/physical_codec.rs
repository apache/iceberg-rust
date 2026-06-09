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

//! Physical extension codec for the Iceberg execution plan nodes.
//!
//! Encodes/decodes [`IcebergTableScan`], [`IcebergWriteExec`], and
//! [`IcebergCommitExec`] so Ballista can ship them to remote executors. Any
//! node that is not an Iceberg node is delegated to an inner codec (by default
//! Ballista's own [`BallistaPhysicalExtensionCodec`]), so shuffle and other
//! Ballista plan nodes keep working.

use std::fmt::Debug;
use std::sync::Arc;

use ballista_core::serde::BallistaPhysicalExtensionCodec;
use datafusion::common::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use iceberg::TableIdent;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::expr::Predicate;
use iceberg::inspect::MetadataTableType;
use iceberg::spec::{PartitionSpec, Schema};
use iceberg_datafusion::IcebergMetadataTableProvider;
use iceberg_datafusion::physical_plan::{
    IcebergCommitExec, IcebergMetadataScan, IcebergTableScan, IcebergWriteExec, PartitionExpr,
};
use serde::{Deserialize, Serialize};

use crate::serde::{
    CatalogConfigProto, TAG_DELEGATED, TAG_ICEBERG, get_catalog, load_table, to_df_err,
};

/// Wire representation of an Iceberg physical plan node.
// `Predicate` is not `Eq` (it can hold float literals), so this derives only
// `PartialEq`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum IcebergPhysicalNode {
    Scan {
        catalog: CatalogConfigProto,
        table: TableIdent,
        snapshot_id: Option<i64>,
        projection: Option<Vec<String>>,
        limit: Option<usize>,
        /// Pushed-down filter, restored on the remote node so Iceberg file
        /// pruning is preserved (DataFusion still re-applies it above the scan).
        #[serde(default)]
        predicates: Option<Predicate>,
    },
    Write {
        catalog: CatalogConfigProto,
        table: TableIdent,
    },
    Commit {
        catalog: CatalogConfigProto,
        table: TableIdent,
    },
    Metadata {
        catalog: CatalogConfigProto,
        table: TableIdent,
        /// The metadata table kind, as its lowercase string name.
        metadata_type: String,
    },
}

/// Wire representation of an [`IcebergDataFusion`](iceberg_datafusion) partition
/// expression. The live `PartitionValueCalculator` it wraps is not serializable,
/// but it can be rebuilt on the far node from the (self-contained) partition spec
/// and table schema, so those are all that travels on the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PartitionExprProto {
    partition_spec: PartitionSpec,
    schema: Schema,
}

/// A [`PhysicalExtensionCodec`] that understands the Iceberg plan nodes and
/// delegates everything else to an inner codec.
#[derive(Debug)]
pub struct IcebergPhysicalCodec {
    inner: Arc<dyn PhysicalExtensionCodec>,
}

impl Default for IcebergPhysicalCodec {
    fn default() -> Self {
        Self {
            inner: Arc::new(BallistaPhysicalExtensionCodec::default()),
        }
    }
}

impl IcebergPhysicalCodec {
    /// Creates a codec that delegates non-Iceberg nodes to `inner`.
    pub fn new(inner: Arc<dyn PhysicalExtensionCodec>) -> Self {
        Self { inner }
    }
}

fn missing_config_err(node: &str) -> DataFusionError {
    DataFusionError::Internal(format!(
        "{node} has no IcebergCatalogConfig and cannot be distributed. Register the \
         table with IcebergTableProvider::try_new_with_config (see \
         iceberg_ballista::register_iceberg_table)."
    ))
}

fn write_blob(buf: &mut Vec<u8>, node: &IcebergPhysicalNode) -> Result<(), DataFusionError> {
    buf.push(TAG_ICEBERG);
    let json = serde_json::to_vec(node).map_err(to_df_err)?;
    buf.extend_from_slice(&json);
    Ok(())
}

impl PhysicalExtensionCodec for IcebergPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let Some((&tag, rest)) = buf.split_first() else {
            return Err(DataFusionError::Internal(
                "empty iceberg physical codec buffer".to_string(),
            ));
        };
        // Not one of ours? Hand the inner payload back to the inner codec.
        if tag == TAG_DELEGATED {
            return self.inner.try_decode(rest, inputs, ctx);
        }
        if tag != TAG_ICEBERG {
            return Err(DataFusionError::Internal(format!(
                "unknown iceberg physical codec tag {tag}"
            )));
        }

        let node: IcebergPhysicalNode = serde_json::from_slice(rest).map_err(to_df_err)?;

        match node {
            IcebergPhysicalNode::Scan {
                catalog,
                table,
                snapshot_id,
                projection,
                limit,
                predicates,
            } => {
                let config = catalog.into();
                let table_obj = load_table(&config, &table)?;
                let arrow_schema = Arc::new(
                    schema_to_arrow_schema(table_obj.metadata().current_schema())
                        .map_err(to_df_err)?,
                );
                // Map the projected column names back to indices in the table
                // schema. A name that doesn't resolve is a hard error: the
                // executor reloads table metadata independently of the scheduler,
                // so silently dropping it would rebuild the scan with fewer
                // columns than the plan expects and surface later as a confusing
                // column-count mismatch instead of a clear failure here.
                let proj_indices: Option<Vec<usize>> = projection
                    .as_ref()
                    .map(|names| {
                        names
                            .iter()
                            .map(|n| {
                                arrow_schema.index_of(n).map_err(|_| {
                                    DataFusionError::Internal(format!(
                                        "projected column {n:?} not found in table {} schema; \
                                         scheduler and executor table metadata may be out of sync",
                                        table
                                    ))
                                })
                            })
                            .collect::<Result<Vec<usize>, _>>()
                    })
                    .transpose()?;
                // Restore the pushed-down predicate so Iceberg file pruning runs on
                // the remote node too. DataFusion still re-applies it above the scan
                // (pushdown is reported Inexact), so correctness holds regardless.
                let scan = IcebergTableScan::new(
                    table_obj,
                    snapshot_id,
                    arrow_schema,
                    proj_indices.as_ref(),
                    &[],
                    limit,
                )
                .with_predicates(predicates)
                .with_catalog_config(Some(config));
                Ok(Arc::new(scan))
            }
            IcebergPhysicalNode::Write { catalog, table } => {
                let config = catalog.into();
                let table_obj = load_table(&config, &table)?;
                let arrow_schema = Arc::new(
                    schema_to_arrow_schema(table_obj.metadata().current_schema())
                        .map_err(to_df_err)?,
                );
                let input = single_input(inputs, "IcebergWriteExec")?;
                let write = IcebergWriteExec::new(table_obj, input, arrow_schema)
                    .with_catalog_config(Some(config));
                Ok(Arc::new(write))
            }
            IcebergPhysicalNode::Commit { catalog, table } => {
                let config = catalog.into();
                let cat = get_catalog(&config)?;
                let table_obj =
                    crate::serde::block_on(cat.load_table(&table)).map_err(to_df_err)?;
                let arrow_schema = Arc::new(
                    schema_to_arrow_schema(table_obj.metadata().current_schema())
                        .map_err(to_df_err)?,
                );
                let input = single_input(inputs, "IcebergCommitExec")?;
                let commit = IcebergCommitExec::new(table_obj, cat, input, arrow_schema)
                    .with_catalog_config(Some(config));
                Ok(Arc::new(commit))
            }
            IcebergPhysicalNode::Metadata {
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
                Ok(Arc::new(IcebergMetadataScan::new(provider)))
            }
        }
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError> {
        if let Some(scan) = node.as_any().downcast_ref::<IcebergTableScan>() {
            let config = scan
                .catalog_config()
                .ok_or_else(|| missing_config_err("IcebergTableScan"))?;
            let proto = IcebergPhysicalNode::Scan {
                catalog: config.into(),
                table: scan.table().identifier().clone(),
                snapshot_id: scan.snapshot_id(),
                projection: scan.projection().map(|s| s.to_vec()),
                limit: scan.limit(),
                predicates: scan.predicates().cloned(),
            };
            return write_blob(buf, &proto);
        }

        if let Some(write) = node.as_any().downcast_ref::<IcebergWriteExec>() {
            let config = write
                .catalog_config()
                .ok_or_else(|| missing_config_err("IcebergWriteExec"))?;
            let proto = IcebergPhysicalNode::Write {
                catalog: config.into(),
                table: write.table().identifier().clone(),
            };
            return write_blob(buf, &proto);
        }

        if let Some(commit) = node.as_any().downcast_ref::<IcebergCommitExec>() {
            let config = commit
                .catalog_config()
                .ok_or_else(|| missing_config_err("IcebergCommitExec"))?;
            let proto = IcebergPhysicalNode::Commit {
                catalog: config.into(),
                table: commit.table().identifier().clone(),
            };
            return write_blob(buf, &proto);
        }

        if let Some(meta) = node.as_any().downcast_ref::<IcebergMetadataScan>() {
            let provider = meta.provider();
            let config = provider
                .catalog_config()
                .ok_or_else(|| missing_config_err("IcebergMetadataScan"))?;
            let proto = IcebergPhysicalNode::Metadata {
                catalog: config.into(),
                table: provider.table().identifier().clone(),
                metadata_type: provider.metadata_type().as_str().to_string(),
            };
            return write_blob(buf, &proto);
        }

        buf.push(TAG_DELEGATED);
        self.inner.try_encode(node, buf)
    }

    fn try_encode_expr(
        &self,
        node: &Arc<dyn PhysicalExpr>,
        buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError> {
        // The partition-value expression a partitioned write injects holds a
        // live calculator; serialize the spec + schema it can be rebuilt from.
        if let Some(expr) = node.as_any().downcast_ref::<PartitionExpr>() {
            let proto = PartitionExprProto {
                partition_spec: expr.partition_spec().as_ref().clone(),
                schema: expr.table_schema().as_ref().clone(),
            };
            buf.push(TAG_ICEBERG);
            buf.extend_from_slice(&serde_json::to_vec(&proto).map_err(to_df_err)?);
            return Ok(());
        }
        buf.push(TAG_DELEGATED);
        self.inner.try_encode_expr(node, buf)
    }

    fn try_decode_expr(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn PhysicalExpr>],
    ) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
        let Some((&tag, rest)) = buf.split_first() else {
            return Err(DataFusionError::Internal(
                "empty iceberg physical expr buffer".to_string(),
            ));
        };
        match tag {
            TAG_DELEGATED => self.inner.try_decode_expr(rest, inputs),
            TAG_ICEBERG => {
                let proto: PartitionExprProto =
                    serde_json::from_slice(rest).map_err(to_df_err)?;
                let expr =
                    PartitionExpr::try_new(Arc::new(proto.partition_spec), Arc::new(proto.schema))?;
                Ok(Arc::new(expr))
            }
            other => Err(DataFusionError::Internal(format!(
                "unknown iceberg physical expr tag {other}"
            ))),
        }
    }
}

fn single_input(
    inputs: &[Arc<dyn ExecutionPlan>],
    node: &str,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    if inputs.len() != 1 {
        return Err(DataFusionError::Internal(format!(
            "{node} expects exactly one input, got {}",
            inputs.len()
        )));
    }
    Ok(inputs[0].clone())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn sample_catalog() -> CatalogConfigProto {
        CatalogConfigProto {
            r#type: "rest".to_string(),
            name: "rest".to_string(),
            props: BTreeMap::from([
                ("uri".to_string(), "http://localhost:8181".to_string()),
                ("warehouse".to_string(), "s3://bucket/wh".to_string()),
            ]),
        }
    }

    fn roundtrip(node: &IcebergPhysicalNode) -> IcebergPhysicalNode {
        let mut buf = Vec::new();
        super::write_blob(&mut buf, node).expect("encode");
        assert_eq!(buf[0], TAG_ICEBERG, "blob must carry the iceberg tag");
        serde_json::from_slice(&buf[1..]).expect("decode")
    }

    #[test]
    fn scan_node_roundtrips() {
        let node = IcebergPhysicalNode::Scan {
            catalog: sample_catalog(),
            table: TableIdent::from_strs(["ns", "tbl"]).unwrap(),
            snapshot_id: Some(42),
            projection: Some(vec!["a".to_string(), "b".to_string()]),
            limit: Some(10),
            predicates: None,
        };
        assert_eq!(node, roundtrip(&node));
    }

    #[test]
    fn scan_node_with_predicate_roundtrips() {
        use iceberg::expr::Reference;
        use iceberg::spec::Datum;

        let node = IcebergPhysicalNode::Scan {
            catalog: sample_catalog(),
            table: TableIdent::from_strs(["ns", "tbl"]).unwrap(),
            snapshot_id: None,
            projection: None,
            limit: None,
            predicates: Some(Reference::new("a").less_than(Datum::long(5))),
        };
        assert_eq!(node, roundtrip(&node));
    }

    #[test]
    fn metadata_node_roundtrips() {
        let node = IcebergPhysicalNode::Metadata {
            catalog: sample_catalog(),
            table: TableIdent::from_strs(["ns", "tbl"]).unwrap(),
            metadata_type: "snapshots".to_string(),
        };
        assert_eq!(node, roundtrip(&node));
    }

    #[test]
    fn write_node_roundtrips() {
        let node = IcebergPhysicalNode::Write {
            catalog: sample_catalog(),
            table: TableIdent::from_strs(["ns", "tbl"]).unwrap(),
        };
        assert_eq!(node, roundtrip(&node));
    }

    #[test]
    fn commit_node_roundtrips() {
        let node = IcebergPhysicalNode::Commit {
            catalog: sample_catalog(),
            table: TableIdent::from_strs(["a", "b", "tbl"]).unwrap(),
        };
        assert_eq!(node, roundtrip(&node));
    }

    #[test]
    fn delegated_blob_carries_distinct_tag() {
        // Payloads we hand to the inner codec are framed with TAG_DELEGATED, so
        // decode routes them to the inner codec rather than parsing them as
        // Iceberg nodes. The two tags must stay distinct for that to hold.
        assert_ne!(TAG_ICEBERG, TAG_DELEGATED);
        let mut buf = vec![TAG_DELEGATED];
        buf.extend_from_slice(b"inner codec payload");
        assert_ne!(buf[0], TAG_ICEBERG);
    }
}
