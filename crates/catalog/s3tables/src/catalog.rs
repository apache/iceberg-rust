use std::collections::HashMap;

use anyhow::anyhow;
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};

/// S3Tables catalog implementation.
#[derive(Debug)]
pub struct S3TablesCatalog {
    table_bucket_arn: String,
    client: aws_sdk_s3tables::Client,
}

impl S3TablesCatalog {
    pub async fn new(table_bucket_arn: String) -> Self {
        let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let client = aws_sdk_s3tables::Client::new(&config);
        Self {
            table_bucket_arn,
            client,
        }
    }
}

#[async_trait]
impl Catalog for S3TablesCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        let mut req = self
            .client
            .list_namespaces()
            .table_bucket_arn(self.table_bucket_arn.clone());
        if let Some(parent) = parent {
            req = req.prefix(parent.to_url_string());
        }
        let resp = req.send().await.map_err(from_aws_sdk_error)?;
        let mut result = Vec::new();
        for ns in resp.namespaces() {
            let ns_names = ns.namespace();
            result.extend(
                ns_names
                    .into_iter()
                    .map(|name| NamespaceIdent::new(name.to_string())),
            );
        }
        Ok(result)
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        todo!()
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        todo!()
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        todo!()
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        todo!()
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        todo!()
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        todo!()
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        todo!()
    }

    async fn load_table(&self, table: &TableIdent) -> Result<Table> {
        todo!()
    }

    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        todo!()
    }

    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        todo!()
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        todo!()
    }

    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        todo!()
    }
}

/// Format AWS SDK error into iceberg error
pub(crate) fn from_aws_sdk_error<T>(error: aws_sdk_s3tables::error::SdkError<T>) -> Error
where T: std::fmt::Debug {
    Error::new(
        ErrorKind::Unexpected,
        "Operation failed for hitting aws skd error".to_string(),
    )
    .with_source(anyhow!("aws sdk error: {:?}", error))
}
