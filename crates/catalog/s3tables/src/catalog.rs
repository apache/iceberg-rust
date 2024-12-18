use std::collections::HashMap;

use anyhow::anyhow;
use async_trait::async_trait;
use aws_sdk_s3tables::operation::get_table::GetTableOutput;
use aws_sdk_s3tables::operation::list_tables::ListTablesOutput;
use iceberg::io::FileIO;
use iceberg::spec::TableMetadata;
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
    file_io: FileIO,
}

impl S3TablesCatalog {
    pub async fn new(config: S3TablesCatalogConfig) -> Result<Self> {
        let aws_config = create_sdk_config(&config.properties, config.endpoint_url.clone()).await;
        let s3tables_client = aws_sdk_s3tables::Client::new(&aws_config);

        // parse bucket name from ARN format like: arn:aws:s3:<region>:<account>:bucket/<bucket_name>
        let bucket_name = config
            .table_bucket_arn
            .rsplit(":bucket/")
            .next()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Invalid bucket ARN format: {}", config.table_bucket_arn),
                )
            })?;

        let file_io = FileIO::from_path(&format!("s3://{}", bucket_name))?
            .with_props(&config.properties)
            .build()?;

        Ok(Self {
            config,
            s3tables_client,
            file_io,
        })
    }
}

#[async_trait]
impl Catalog for S3TablesCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        let mut result = Vec::new();
        let mut continuation_token = None;
        loop {
            let mut req = self
                .s3tables_client
                .list_namespaces()
                .table_bucket_arn(self.config.table_bucket_arn.clone());
            if let Some(parent) = parent {
                req = req.prefix(parent.to_url_string());
            }
            if let Some(token) = continuation_token {
                req = req.continuation_token(token);
            }
            let resp = req.send().await.map_err(from_aws_sdk_error)?;
            for ns in resp.namespaces() {
                result.push(NamespaceIdent::from_vec(ns.namespace().to_vec())?);
            }
            continuation_token = resp.continuation_token().map(|s| s.to_string());
            if continuation_token.is_none() {
                break;
            }
        }
        Ok(result)
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        let req = self
            .s3tables_client
            .create_namespace()
            .table_bucket_arn(self.config.table_bucket_arn.clone())
            .namespace(namespace.to_url_string());
        req.send().await.map_err(from_aws_sdk_error)?;
        Ok(Namespace::with_properties(
            namespace.clone(),
            HashMap::new(),
        ))
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        let req = self
            .s3tables_client
            .get_namespace()
            .table_bucket_arn(self.config.table_bucket_arn.clone())
            .namespace(namespace.to_url_string());
        let resp = req.send().await.map_err(from_aws_sdk_error)?;
        let properties = HashMap::new();
        Ok(Namespace::with_properties(
            NamespaceIdent::from_vec(resp.namespace().to_vec())?,
            properties,
        ))
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        let req = self
            .s3tables_client
            .get_namespace()
            .table_bucket_arn(self.config.table_bucket_arn.clone())
            .namespace(namespace.to_url_string());
        match req.send().await {
            Ok(_) => Ok(true),
            Err(err) => {
                if err.as_service_error().map(|e| e.is_not_found_exception()) == Some(true) {
                    Ok(false)
                } else {
                    Err(from_aws_sdk_error(err))
                }
            }
        }
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Update namespace is not supported for s3tables catalog",
        ))
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        let req = self
            .s3tables_client
            .delete_namespace()
            .table_bucket_arn(self.config.table_bucket_arn.clone())
            .namespace(namespace.to_url_string());
        req.send().await.map_err(from_aws_sdk_error)?;
        Ok(())
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        let mut result = Vec::new();
        let mut continuation_token = None;
        loop {
            let mut req = self
                .s3tables_client
                .list_tables()
                .table_bucket_arn(self.config.table_bucket_arn.clone())
                .namespace(namespace.to_url_string());
            if let Some(token) = continuation_token {
                req = req.continuation_token(token);
            }
            let resp: ListTablesOutput = req.send().await.map_err(from_aws_sdk_error)?;
            for table in resp.tables() {
                result.push(TableIdent::new(
                    NamespaceIdent::from_vec(table.namespace().to_vec())?,
                    table.name().to_string(),
                ));
            }
            continuation_token = resp.continuation_token().map(|s| s.to_string());
            if continuation_token.is_none() {
                break;
            }
        }
        Ok(result)
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        todo!()
    }

    async fn load_table(&self, table_ident: &TableIdent) -> Result<Table> {
        let req = self
            .s3tables_client
            .get_table()
            .table_bucket_arn(self.config.table_bucket_arn.clone())
            .namespace(table_ident.namespace().to_url_string())
            .name(table_ident.name());
        let resp: GetTableOutput = req.send().await.map_err(from_aws_sdk_error)?;

        let metadata_location = resp.metadata_location().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                format!(
                    "Table {} does not have metadata location",
                    table_ident.name()
                ),
            )
        })?;
        let input_file = self.file_io.new_input(&metadata_location)?;
        let metadata_content = input_file.read().await?;
        let metadata = serde_json::from_slice::<TableMetadata>(&metadata_content)?;

        let table = Table::builder()
            .identifier(table_ident.clone())
            .metadata(metadata)
            .metadata_location(metadata_location)
            .file_io(self.file_io.clone())
            .build()?;
        Ok(table)
    }

    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        let req = self
            .s3tables_client
            .delete_table()
            .table_bucket_arn(self.config.table_bucket_arn.clone())
            .namespace(table.namespace().to_url_string())
            .name(table.name());
        req.send().await.map_err(from_aws_sdk_error)?;
        Ok(())
    }

    async fn table_exists(&self, table_ident: &TableIdent) -> Result<bool> {
        let req = self
            .s3tables_client
            .get_table()
            .table_bucket_arn(self.config.table_bucket_arn.clone())
            .namespace(table_ident.namespace().to_url_string())
            .name(table_ident.name());
        match req.send().await {
            Ok(_) => Ok(true),
            Err(err) => {
                if err.as_service_error().map(|e| e.is_not_found_exception()) == Some(true) {
                    Ok(false)
                } else {
                    Err(from_aws_sdk_error(err))
                }
            }
        }
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        let req = self
            .s3tables_client
            .rename_table()
            .table_bucket_arn(self.config.table_bucket_arn.clone())
            .namespace(src.namespace().to_url_string())
            .name(src.name())
            .new_namespace_name(dest.namespace().to_url_string())
            .new_name(dest.name());
        req.send().await.map_err(from_aws_sdk_error)?;
        Ok(())
    }

    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Updating a table is not supported yet",
        ))
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

#[cfg(test)]
mod tests {
    use super::*;

    async fn load_s3tables_catalog_from_env() -> Result<Option<S3TablesCatalog>> {
        let table_bucket_arn = match std::env::var("TABLE_BUCKET_ARN").ok() {
            Some(table_bucket_arn) => table_bucket_arn,
            None => return Ok(None),
        };

        let properties = HashMap::new();
        let config = S3TablesCatalogConfig {
            table_bucket_arn,
            properties,
            endpoint_url: None,
        };

        Ok(Some(S3TablesCatalog::new(config).await?))
    }

    #[tokio::test]
    async fn test_s3tables_list_namespace() {
        let catalog = match load_s3tables_catalog_from_env().await {
            Ok(Some(catalog)) => catalog,
            Ok(None) => return,
            Err(e) => panic!("Error loading catalog: {}", e),
        };

        let namespaces = catalog.list_namespaces(None).await.unwrap();
        assert!(namespaces.len() > 0);
    }

    #[tokio::test]
    async fn test_s3tables_list_tables() {
        let catalog = match load_s3tables_catalog_from_env().await {
            Ok(Some(catalog)) => catalog,
            Ok(None) => return,
            Err(e) => panic!("Error loading catalog: {}", e),
        };

        let tables = catalog
            .list_tables(&NamespaceIdent::new("aws_s3_metadata".to_string()))
            .await
            .unwrap();
        println!("{:?}", tables);
        assert!(tables.len() > 0);
    }

    #[tokio::test]
    async fn test_s3tables_load_table() {
        let catalog = match load_s3tables_catalog_from_env().await {
            Ok(Some(catalog)) => catalog,
            Ok(None) => return,
            Err(e) => panic!("Error loading catalog: {}", e),
        };

        let table = catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new("aws_s3_metadata".to_string()),
                "query_storage_metadata".to_string(),
            ))
            .await
            .unwrap();
        println!("{:?}", table);
    }
}
