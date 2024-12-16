use std::collections::HashMap;

use anyhow::anyhow;
use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};

use crate::utils::create_sdk_config;

#[derive(Debug)]
pub struct S3TablesCatalogConfig {
    table_bucket_arn: String,
    properties: HashMap<String, String>,
    endpoint_url: Option<String>,
}

/// S3Tables catalog implementation.
#[derive(Debug)]
pub struct S3TablesCatalog {
    config: S3TablesCatalogConfig,
    s3tables_client: aws_sdk_s3tables::Client,
}

impl S3TablesCatalog {
    pub async fn new(config: S3TablesCatalogConfig) -> Self {
        let aws_config = create_sdk_config(&config.properties, config.endpoint_url.clone()).await;
        let s3tables_client = aws_sdk_s3tables::Client::new(&aws_config);
        Self {
            config,
            s3tables_client,
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
            .s3tables_client
            .list_namespaces()
            .table_bucket_arn(self.config.table_bucket_arn.clone());
        if let Some(parent) = parent {
            req = req.prefix(parent.to_url_string());
        }
        let resp = req.send().await.map_err(from_aws_sdk_error)?;
        let mut result = Vec::new();
        for ns in resp.namespaces() {
            result.push(NamespaceIdent::from_vec(ns.namespace().to_vec())?);
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
