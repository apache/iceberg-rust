use std::{collections::HashMap, time::SystemTime};

use aws_config::{AppName, Region, SdkConfig};
use aws_sdk_dynamodb::operation::describe_table::DescribeTableError::ResourceNotFoundException;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::{
    config::{Credentials, SharedCredentialsProvider},
    error::SdkError,
    types::{AttributeDefinition, KeySchemaElement, KeyType},
    Client,
};
use iceberg::{
    table::Table, Catalog, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};

use crate::utils::{from_sdk_error, SdkResultExt};

static APP_NAME: &str = "iceberg_dynamodb";
static PROVIDER_NAME: &str = "iceberg_dynamodb";

// These attributes should be consistent with pyiceberg
static DEFAULT_DYNAMODB_TABLE_NAME: &str = "iceberg";
static DYNAMODB_COL_IDENTIFIER: &str = "identifier";
static DYNAMODB_COL_NAMESPACE: &str = "namespace";
static _DYNAMODB_COL_VERSION: &str = "v";
static _DYNAMODB_COL_UPDATED_AT: &str = "updated_at";
static _DYNAMODB_COL_CREATED_AT: &str = "created_at";

#[derive(Debug)]
pub struct DynamoDBCatalog {
    client: Client,
    dynamodb_table_name: String,
}

pub struct DynamoDBConfig {
    region: String,
    endpoint_url: String,
    access_key_id: String,
    secret_access_key: String,
    aws_session_token: Option<String>,
    dynamodb_table_name: Option<String>,
    expires_after: Option<SystemTime>,
}

impl DynamoDBCatalog {
    pub async fn new(config: DynamoDBConfig) -> Result<Self> {
        let sdk_config = SdkConfig::builder()
            .app_name(AppName::new(APP_NAME.to_string()).unwrap())
            .region(Region::new(config.region))
            .endpoint_url(config.endpoint_url)
            .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                config.access_key_id,
                config.secret_access_key,
                config.aws_session_token,
                config.expires_after,
                PROVIDER_NAME,
            )))
            .build();

        let client = Client::new(&sdk_config);
        let dynamodb_table_name = config
            .dynamodb_table_name
            .unwrap_or(DEFAULT_DYNAMODB_TABLE_NAME.to_string());

        let catalog = Self {
            client,
            dynamodb_table_name,
        };

        catalog.create_dynamodb_table_if_not_exist().await?;

        Ok(catalog)
    }

    async fn create_dynamodb_table_if_not_exist(&self) -> Result<()> {
        if self.table_exists(&self.dynamodb_table_name).await? {
            return Ok(());
        }

        let col_identifier_ks = KeySchemaElement::builder()
            .set_attribute_name(Some(DYNAMODB_COL_IDENTIFIER.to_string()))
            .set_key_type(Some(KeyType::Hash))
            .build()
            .wrap_err()?;
        let namespace_ks = KeySchemaElement::builder()
            .set_attribute_name(Some(DYNAMODB_COL_NAMESPACE.to_string()))
            .set_key_type(Some(KeyType::Range))
            .build()
            .wrap_err()?;
        let col_attribute = AttributeDefinition::builder()
            .attribute_name(DYNAMODB_COL_IDENTIFIER.to_string())
            .attribute_type("S".into())
            .build()
            .wrap_err()?;
        let namespace_attribute = AttributeDefinition::builder()
            .attribute_name(DYNAMODB_COL_NAMESPACE.to_string())
            .attribute_type("S".into())
            .build()
            .wrap_err()?;

        self.client
            .create_table()
            .table_name(&self.dynamodb_table_name)
            .attribute_definitions(col_attribute)
            .attribute_definitions(namespace_attribute)
            .key_schema(col_identifier_ks)
            .key_schema(namespace_ks)
            .send()
            .await
            .wrap_err()?;

        Ok(())
    }

    async fn table_exists(&self, table_name: &str) -> Result<bool> {
        let res = self
            .client
            .describe_table()
            .table_name(table_name)
            .send()
            .await;

        match res {
            Ok(_) => Ok(true),
            Err(e) => match e {
                SdkError::ServiceError(se) => match se.err() {
                    ResourceNotFoundException(_) => Ok(false),
                    _ => Err(from_sdk_error(SdkError::ServiceError(se))),
                },
                _ => Err(from_sdk_error(e)),
            },
        }
    }
}

#[async_trait::async_trait]
impl Catalog for DynamoDBCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        // does not support nested namespace
        if parent.is_some() {
            return Ok(vec![]);
        }

        let output = self
            .client
            .query()
            .table_name(&self.dynamodb_table_name)
            .consistent_read(true)
            .key_condition_expression(format!("{} = :identifier", DYNAMODB_COL_IDENTIFIER))
            .expression_attribute_values(
                ":identifier",
                AttributeValue::S(DYNAMODB_COL_NAMESPACE.to_string()),
            )
            .send()
            .await
            .wrap_err()?;

        let mut namespace_idents = vec![];
        if let Some(items) = output.items {
            for item in items {
                let namespace = item.get(DYNAMODB_COL_NAMESPACE).unwrap();
                namespace_idents.push(NamespaceIdent::new(namespace.as_s().unwrap().into()));
            }
        }
        Ok(namespace_idents)
    }

    async fn create_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        todo!()
    }

    async fn get_namespace(&self, _namespace: &NamespaceIdent) -> Result<Namespace> {
        todo!()
    }

    async fn namespace_exists(&self, _namesace: &NamespaceIdent) -> Result<bool> {
        todo!()
    }

    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<()> {
        todo!()
    }

    async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> Result<()> {
        todo!()
    }

    async fn list_tables(&self, _namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        todo!()
    }

    async fn create_table(
        &self,
        _namespace: &NamespaceIdent,
        _creation: TableCreation,
    ) -> Result<Table> {
        todo!()
    }

    async fn load_table(&self, _table: &TableIdent) -> Result<Table> {
        todo!()
    }

    async fn drop_table(&self, _table: &TableIdent) -> Result<()> {
        todo!()
    }

    async fn stat_table(&self, _table: &TableIdent) -> Result<bool> {
        todo!()
    }

    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> Result<()> {
        todo!()
    }

    async fn update_table(&self, _commit: TableCommit) -> Result<Table> {
        todo!()
    }
}
