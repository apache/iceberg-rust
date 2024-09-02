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

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider, TableProviderFactory};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::CreateExternalTable;
use datafusion::sql::TableReference;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::FileIO;
use iceberg::table::StaticTable;
use iceberg::{Error, ErrorKind, Result, TableIdent};

use super::IcebergTableProvider;
use crate::to_datafusion_error;

#[derive(Default)]
#[non_exhaustive]
pub struct IcebergTableProviderFactory {}

impl IcebergTableProviderFactory {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl TableProviderFactory for IcebergTableProviderFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> DFResult<Arc<dyn TableProvider>> {
        check_cmd(cmd).map_err(to_datafusion_error)?;

        let table_name = &cmd.name;
        let metadata_file_path = &cmd.location;
        let options = &cmd.options;

        let table = create_static_table(table_name, metadata_file_path, options)
            .await
            .map_err(to_datafusion_error)?
            .into_table();

        let schema = schema_to_arrow_schema(table.metadata().current_schema())
            .map_err(to_datafusion_error)?;

        Ok(Arc::new(IcebergTableProvider::new(table, Arc::new(schema))))
    }
}

fn check_cmd(cmd: &CreateExternalTable) -> Result<()> {
    let CreateExternalTable {
        schema,
        table_partition_cols,
        order_exprs,
        constraints,
        column_defaults,
        ..
    } = cmd;

    if !schema.fields().is_empty() {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Schema specification is not supported",
        ));
    }

    if !table_partition_cols.is_empty() {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Partition columns cannot be specified",
        ));
    }

    if !order_exprs.is_empty() {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Ordering clause is not supported",
        ));
    }

    if !constraints.is_empty() {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Constraints are not supported",
        ));
    }

    if !column_defaults.is_empty() {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Default values for columns are not supported",
        ));
    }

    Ok(())
}

async fn create_static_table(
    table_name: &TableReference,
    metadata_file_path: &str,
    props: &HashMap<String, String>,
) -> Result<StaticTable> {
    let table_ident = TableIdent::from_strs(table_name.to_vec())?;
    let file_io = FileIO::from_path(metadata_file_path)?
        .with_props(props)
        .build()?;
    StaticTable::from_metadata_file(metadata_file_path, table_ident, file_io).await
}

#[cfg(test)]
mod tests {

    use datafusion::catalog::TableProviderFactory;
    use datafusion::common::{Constraints, DFSchema};
    use datafusion::logical_expr::CreateExternalTable;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::TableReference;

    use super::*;

    #[tokio::test]
    async fn test_schema_of_created_table() {
        let factory = IcebergTableProviderFactory::new();

        let metadata_file_path = format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2.json"
        );

        let cmd = CreateExternalTable {
            name: TableReference::partial("static_ns", "static_table"),
            location: metadata_file_path,
            schema: Arc::new(DFSchema::empty()),
            file_type: "iceberg".to_string(),
            options: Default::default(),
            table_partition_cols: Default::default(),
            order_exprs: Default::default(),
            constraints: Constraints::empty(),
            column_defaults: Default::default(),
            if_not_exists: Default::default(),
            definition: Default::default(),
            unbounded: Default::default(),
        };

        let ctx = SessionContext::new();
        let state = ctx.state();
        let table_provider = factory
            .create(&state, &cmd)
            .await
            .expect("create table failed");

        // Field { name: "x", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1"} },
        // Field { name: "y", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "2"} },
        // Field { name: "z", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "3"} }
        let actual_schema = table_provider.schema();
        let expected_schema_str = "Field { name: \"x\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {\"PARQUET:field_id\": \"1\"} }, Field { name: \"y\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {\"PARQUET:field_id\": \"2\"} }, Field { name: \"z\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {\"PARQUET:field_id\": \"3\"} }";

        assert_eq!(actual_schema.to_string(), expected_schema_str);
    }
}
