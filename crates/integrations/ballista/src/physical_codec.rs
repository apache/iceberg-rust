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
use datafusion::arrow::datatypes::SchemaRef;
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
use iceberg::table::Table;
use iceberg_datafusion::IcebergMetadataTableProvider;
use iceberg_datafusion::physical_plan::{
    IcebergCommitExec, IcebergMetadataScan, IcebergTableScan, IcebergWriteExec, PartitionExpr,
};
use serde::{Deserialize, Serialize};

use crate::bridge::{
    CatalogConfigWire, TAG_DELEGATED, TAG_ICEBERG, encode_blob, get_catalog, load_table,
    split_tagged, to_df_err,
};

/// Wire representation of an Iceberg physical plan node.
// `Predicate` is not `Eq` (it can hold float literals), so this derives only
// `PartialEq`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum IcebergPhysicalNode {
    Scan {
        catalog: CatalogConfigWire,
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
        catalog: CatalogConfigWire,
        table: TableIdent,
    },
    Commit {
        catalog: CatalogConfigWire,
        table: TableIdent,
    },
    Metadata {
        catalog: CatalogConfigWire,
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
struct PartitionExprWire {
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

/// Arrow schema of the table's current Iceberg schema.
fn arrow_schema_of(table: &Table) -> Result<SchemaRef, DataFusionError> {
    Ok(Arc::new(
        schema_to_arrow_schema(table.metadata().current_schema()).map_err(to_df_err)?,
    ))
}

impl PhysicalExtensionCodec for IcebergPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let (tag, rest) = split_tagged(buf, "iceberg physical codec")?;
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
                let arrow_schema = arrow_schema_of(&table_obj)?;
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
                let arrow_schema = arrow_schema_of(&table_obj)?;
                let input = single_input(inputs, "IcebergWriteExec")?;
                let write = IcebergWriteExec::new(table_obj, input, arrow_schema)
                    .with_catalog_config(Some(config));
                Ok(Arc::new(write))
            }
            IcebergPhysicalNode::Commit { catalog, table } => {
                let config = catalog.into();
                let cat = get_catalog(&config)?;
                let table_obj = load_table(&config, &table)?;
                let arrow_schema = arrow_schema_of(&table_obj)?;
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
        if let Some(scan) = node.downcast_ref::<IcebergTableScan>() {
            let config = scan
                .catalog_config()
                .ok_or_else(|| missing_config_err("IcebergTableScan"))?;
            // Pin the snapshot at encode (planning) time. The executor reloads
            // table metadata independently, so an unpinned scan would read
            // whatever snapshot is current when each task decodes — concurrent
            // commits could then give two tasks of one query different
            // snapshots. `scan.table()` is the table as loaded at planning, so
            // its current snapshot is the consistent choice for every task.
            let snapshot_id = scan
                .snapshot_id()
                .or_else(|| scan.table().metadata().current_snapshot_id());
            let node = IcebergPhysicalNode::Scan {
                catalog: config.into(),
                table: scan.table().identifier().clone(),
                snapshot_id,
                projection: scan.projection().map(|s| s.to_vec()),
                limit: scan.limit(),
                predicates: scan.predicates().cloned(),
            };
            return encode_blob(buf, &node);
        }

        if let Some(write) = node.downcast_ref::<IcebergWriteExec>() {
            let config = write
                .catalog_config()
                .ok_or_else(|| missing_config_err("IcebergWriteExec"))?;
            let node = IcebergPhysicalNode::Write {
                catalog: config.into(),
                table: write.table().identifier().clone(),
            };
            return encode_blob(buf, &node);
        }

        if let Some(commit) = node.downcast_ref::<IcebergCommitExec>() {
            let config = commit
                .catalog_config()
                .ok_or_else(|| missing_config_err("IcebergCommitExec"))?;
            let node = IcebergPhysicalNode::Commit {
                catalog: config.into(),
                table: commit.table().identifier().clone(),
            };
            return encode_blob(buf, &node);
        }

        if let Some(meta) = node.downcast_ref::<IcebergMetadataScan>() {
            let provider = meta.provider();
            let config = provider
                .catalog_config()
                .ok_or_else(|| missing_config_err("IcebergMetadataScan"))?;
            let node = IcebergPhysicalNode::Metadata {
                catalog: config.into(),
                table: provider.table().identifier().clone(),
                metadata_type: provider.metadata_type().as_str().to_string(),
            };
            return encode_blob(buf, &node);
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
        if let Some(expr) = node.downcast_ref::<PartitionExpr>() {
            let wire = PartitionExprWire {
                partition_spec: expr.partition_spec().as_ref().clone(),
                schema: expr.table_schema().as_ref().clone(),
            };
            return encode_blob(buf, &wire);
        }
        buf.push(TAG_DELEGATED);
        self.inner.try_encode_expr(node, buf)
    }

    fn try_decode_expr(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn PhysicalExpr>],
    ) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
        let (tag, rest) = split_tagged(buf, "iceberg physical expr")?;
        match tag {
            TAG_DELEGATED => self.inner.try_decode_expr(rest, inputs),
            TAG_ICEBERG => {
                let wire: PartitionExprWire = serde_json::from_slice(rest).map_err(to_df_err)?;
                let expr =
                    PartitionExpr::try_new(Arc::new(wire.partition_spec), Arc::new(wire.schema))?;
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

    fn sample_catalog() -> CatalogConfigWire {
        CatalogConfigWire {
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
        encode_blob(&mut buf, node).expect("encode");
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
    fn scan_node_with_compound_predicate_roundtrips() {
        use iceberg::expr::Reference;
        use iceberg::spec::Datum;

        // Exercise AND / OR / IN / IS NULL together — Predicate is the trickiest type.
        let predicate = Reference::new("a")
            .less_than(Datum::long(5))
            .and(Reference::new("b").is_null())
            .or(Reference::new("c").is_in([Datum::string("x"), Datum::string("y")]));
        let node = IcebergPhysicalNode::Scan {
            catalog: sample_catalog(),
            table: TableIdent::from_strs(["ns", "tbl"]).unwrap(),
            snapshot_id: None,
            projection: None,
            limit: None,
            predicates: Some(predicate),
        };
        assert_eq!(node, roundtrip(&node));
    }

    #[test]
    fn scan_node_without_predicates_field_decodes_to_none() {
        use iceberg::expr::Reference;
        use iceberg::spec::Datum;

        // `predicates` is `#[serde(default)]`: a payload missing the key still decodes.
        let node = IcebergPhysicalNode::Scan {
            catalog: sample_catalog(),
            table: TableIdent::from_strs(["ns", "tbl"]).unwrap(),
            snapshot_id: Some(7),
            projection: None,
            limit: None,
            predicates: Some(Reference::new("a").less_than(Datum::long(5))),
        };
        let mut value = serde_json::to_value(&node).unwrap();
        value["Scan"].as_object_mut().unwrap().remove("predicates");

        let decoded: IcebergPhysicalNode = serde_json::from_value(value).expect("decode");
        assert!(matches!(decoded, IcebergPhysicalNode::Scan {
            predicates: None,
            snapshot_id: Some(7),
            ..
        }));
    }

    #[test]
    fn partition_expr_wire_roundtrips() {
        use iceberg::spec::{NestedField, PrimitiveType, Transform, Type};

        // Schema + PartitionSpec are the heaviest serde types in the crate.
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "region", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .add_partition_field("region", "region", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        let wire = PartitionExprWire {
            partition_spec,
            schema,
        };

        let mut buf = Vec::new();
        encode_blob(&mut buf, &wire).expect("encode");
        assert_eq!(buf[0], TAG_ICEBERG, "blob must carry the iceberg tag");
        let decoded: PartitionExprWire = serde_json::from_slice(&buf[1..]).expect("decode");

        assert_eq!(decoded.partition_spec, wire.partition_spec);
        assert_eq!(decoded.schema, wire.schema);
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
    fn non_iceberg_node_roundtrips_through_inner_codec() {
        use ballista_core::execution_plans::ShuffleWriterExec;
        use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use datafusion::physical_plan::empty::EmptyExec;
        use datafusion::prelude::SessionContext;

        // A Ballista shuffle node is not an Iceberg node, so the codec must
        // frame it with TAG_DELEGATED and hand it to the inner Ballista codec —
        // and decode must route it back there, reconstructing the same node.
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "a",
            DataType::Int32,
            false,
        )]));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        let shuffle = ShuffleWriterExec::try_new(
            "job-1".to_string().into(),
            7,
            input.clone(),
            "/tmp/work".to_string(),
            None,
        )
        .expect("build shuffle writer");

        let codec = IcebergPhysicalCodec::default();
        let mut buf = Vec::new();
        codec
            .try_encode(Arc::new(shuffle), &mut buf)
            .expect("encode delegated node");
        assert_eq!(buf[0], TAG_DELEGATED, "non-Iceberg node must be delegated");

        let ctx = SessionContext::new();
        let decoded = codec
            .try_decode(&buf, &[input], &ctx.task_ctx())
            .expect("decode delegated node");
        let decoded = decoded
            .downcast_ref::<ShuffleWriterExec>()
            .expect("decoded plan should be a ShuffleWriterExec");
        assert_eq!(decoded.job_id().as_str(), "job-1");
        assert_eq!(decoded.stage_id(), 7);
    }
}
