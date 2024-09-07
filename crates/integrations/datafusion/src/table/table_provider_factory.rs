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

/// A factory that implements DataFusion's `TableProviderFactory` to create `IcebergTableProvider` instances.
///
/// # Example
///
/// ```
/// use datafusion::catalog::TableProviderFactory;
/// use iceberg_datafusion::IcebergTableProviderFactory;
///
/// let factory = IcebergTableProviderFactory::new();
/// // Use the factory to create a table provider for an Iceberg table
/// ```
///
/// # Note
/// This factory is designed to work with the DataFusion query engine,
/// specifically for handling Iceberg tables in external table commands.
/// Currently, this implementation supports only reading Iceberg tables, with
/// the creation of new tables not yet available.
///
/// # Errors
/// An error will be returned if any unsupported feature, such as partition columns,
/// order expressions, constraints, or column defaults, is detected in the table creation command.
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

    // Check if any of the fields violate the constraints in a single condition
    let is_invalid = !schema.fields().is_empty()
        || !table_partition_cols.is_empty()
        || !order_exprs.is_empty()
        || !constraints.is_empty()
        || !column_defaults.is_empty();

    if is_invalid {
        return Err(Error::new(ErrorKind::FeatureUnsupported, "Currently we only support reading existing icebergs tables in external table command. To create new table, please use catalog provider."));
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

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::TableProviderFactory;
    use datafusion::common::{Constraints, DFSchema};
    use datafusion::logical_expr::CreateExternalTable;
    use datafusion::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::TableReference;

    use super::*;

    fn table_metadata_v2_schema() -> Schema {
        Schema::new(vec![
            Field::new("x", DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("y", DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
            Field::new("z", DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "3".to_string(),
            )])),
        ])
    }

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

        let expected_schema = table_metadata_v2_schema();
        let actual_schema = table_provider.schema();

        assert_eq!(actual_schema.as_ref(), &expected_schema);
    }
}
