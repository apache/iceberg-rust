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

use anyhow::anyhow;
use async_trait::async_trait;
use aws_sdk_s3tables::operation::create_table::CreateTableOutput;
use aws_sdk_s3tables::operation::get_namespace::GetNamespaceOutput;
use aws_sdk_s3tables::operation::get_table::GetTableOutput;
use aws_sdk_s3tables::operation::list_tables::ListTablesOutput;
use aws_sdk_s3tables::types::OpenTableFormat;
use iceberg::io::FileIO;
use iceberg::spec::{TableMetadata, TableMetadataBuilder};
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};
use typed_builder::TypedBuilder;

use crate::utils::{create_metadata_location, create_sdk_config};

/// S3Tables catalog configuration.
#[derive(Debug, TypedBuilder)]
pub struct S3TablesCatalogConfig {
    /// Unlike other buckets, S3Tables bucket is not a physical bucket, but a virtual bucket
    /// that is managed by s3tables. We can't directly access the bucket with path like
    /// s3://{bucket_name}/{file_path}, all the operations are done with respect of the bucket
    /// ARN.
    table_bucket_arn: String,
    /// Properties for the catalog. The available properties are:
    /// - `profile_name`: The name of the AWS profile to use.
    /// - `region_name`: The AWS region to use.
    /// - `aws_access_key_id`: The AWS access key ID to use.
    /// - `aws_secret_access_key`: The AWS secret access key to use.
    /// - `aws_session_token`: The AWS session token to use.
    #[builder(default)]
    properties: HashMap<String, String>,
    /// Endpoint URL for the catalog.
    #[builder(default, setter(strip_option(fallback = endpoint_url_opt)))]
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
    /// Creates a new S3Tables catalog.
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

        let file_io = FileIO::from_path(format!("s3://{}", bucket_name))?
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
    /// List namespaces from s3tables catalog.
    ///
    /// S3Tables doesn't support nested namespaces. If parent is provided, it will
    /// return an empty list.
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        if parent.is_some() {
            return Ok(vec![]);
        }

        let mut result = Vec::new();
        let mut continuation_token = None;
        loop {
            let mut req = self
                .s3tables_client
                .list_namespaces()
                .table_bucket_arn(self.config.table_bucket_arn.clone());
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

    /// Creates a new namespace with the given identifier and properties.
    ///
    /// Attempts to create a namespace defined by the `namespace`. The `properties`
    /// parameter is ignored.
    ///
    /// The following naming rules apply to namespaces:
    ///
    /// - Names must be between 3 (min) and 63 (max) characters long.
    /// - Names can consist only of lowercase letters, numbers, and underscores (_).
    /// - Names must begin and end with a letter or number.
    /// - Names must not contain hyphens (-) or periods (.).
    ///
    /// This function can return an error in the following situations:
    ///
    /// - Errors from the underlying database creation process, converted using
    /// `from_aws_sdk_error`.
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

    /// Retrieves a namespace by its identifier.
    ///
    /// Validates the given namespace identifier and then queries the
    /// underlying database client to fetch the corresponding namespace data.
    /// Constructs a `Namespace` object with the retrieved data and returns it.
    ///
    /// This function can return an error in any of the following situations:
    /// - If there is an error querying the database, returned by
    /// `from_aws_sdk_error`.
    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        let req = self
            .s3tables_client
            .get_namespace()
            .table_bucket_arn(self.config.table_bucket_arn.clone())
            .namespace(namespace.to_url_string());
        let resp: GetNamespaceOutput = req.send().await.map_err(from_aws_sdk_error)?;
        let properties = HashMap::new();
        Ok(Namespace::with_properties(
            NamespaceIdent::from_vec(resp.namespace().to_vec())?,
            properties,
        ))
    }

    /// Checks if a namespace exists within the s3tables catalog.
    ///
    /// Validates the namespace identifier by querying the s3tables catalog
    /// to determine if the specified namespace exists.
    ///
    /// # Returns
    /// A `Result<bool>` indicating the outcome of the check:
    /// - `Ok(true)` if the namespace exists.
    /// - `Ok(false)` if the namespace does not exist, identified by a specific
    /// `IsNotFoundException` variant.
    /// - `Err(...)` if an error occurs during validation or the s3tables catalog
    /// query, with the error encapsulating the issue.
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

    /// Updates the properties of an existing namespace.
    ///
    /// S3Tables doesn't support updating namespace properties, so this function
    /// will always return an error.
    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Update namespace is not supported for s3tables catalog",
        ))
    }

    /// Drops an existing namespace from the s3tables catalog.
    ///
    /// Validates the namespace identifier and then deletes the corresponding
    /// namespace from the s3tables catalog.
    ///
    /// This function can return an error in the following situations:
    /// - Errors from the underlying database deletion process, converted using
    /// `from_aws_sdk_error`.
    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        let req = self
            .s3tables_client
            .delete_namespace()
            .table_bucket_arn(self.config.table_bucket_arn.clone())
            .namespace(namespace.to_url_string());
        req.send().await.map_err(from_aws_sdk_error)?;
        Ok(())
    }

    /// Lists all tables within a given namespace.
    ///
    /// Retrieves all tables associated with the specified namespace and returns
    /// their identifiers.
    ///
    /// This function can return an error in the following situations:
    /// - Errors from the underlying database query process, converted using
    /// `from_aws_sdk_error`.
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

    /// Creates a new table within a specified namespace.
    ///
    /// Attempts to create a table defined by the `creation` parameter. The metadata
    /// location is generated by the s3tables catalog, looks like:
    ///
    /// s3://{RANDOM WAREHOUSE LOCATION}/metadata/{VERSION}-{UUID}.metadata.json
    ///
    /// We have to get this random warehouse location after the table is created.
    ///
    /// This function can return an error in the following situations:
    /// - If the location of the table is set by user, identified by a specific
    /// `DataInvalid` variant.
    /// - Errors from the underlying database creation process, converted using
    /// `from_aws_sdk_error`.
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        mut creation: TableCreation,
    ) -> Result<Table> {
        let table_ident = TableIdent::new(namespace.clone(), creation.name.clone());

        // create table
        let create_resp: CreateTableOutput = self
            .s3tables_client
            .create_table()
            .table_bucket_arn(self.config.table_bucket_arn.clone())
            .namespace(namespace.to_url_string())
            .format(OpenTableFormat::Iceberg)
            .name(table_ident.name())
            .send()
            .await
            .map_err(from_aws_sdk_error)?;

        // prepare metadata location. the warehouse location is generated by s3tables catalog,
        // which looks like: s3://e6c9bf20-991a-46fb-kni5xs1q2yxi3xxdyxzjzigdeop1quse2b--table-s3
        let metadata_location = match &creation.location {
            Some(_) => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "The location of the table is generated by s3tables catalog, can't be set by user.",
                ));
            }
            None => {
                let get_resp: GetTableOutput = self
                    .s3tables_client
                    .get_table()
                    .table_bucket_arn(self.config.table_bucket_arn.clone())
                    .namespace(namespace.to_url_string())
                    .name(table_ident.name())
                    .send()
                    .await
                    .map_err(from_aws_sdk_error)?;
                let warehouse_location = get_resp.warehouse_location().to_string();
                create_metadata_location(warehouse_location, 0)?
            }
        };

        // write metadata to file
        creation.location = Some(metadata_location.clone());
        let metadata = TableMetadataBuilder::from_table_creation(creation)?
            .build()?
            .metadata;
        self.file_io
            .new_output(&metadata_location)?
            .write(serde_json::to_vec(&metadata)?.into())
            .await?;

        // update metadata location
        self.s3tables_client
            .update_table_metadata_location()
            .table_bucket_arn(self.config.table_bucket_arn.clone())
            .namespace(namespace.to_url_string())
            .name(table_ident.name())
            .metadata_location(metadata_location.clone())
            .version_token(create_resp.version_token())
            .send()
            .await
            .map_err(from_aws_sdk_error)?;

        let table = Table::builder()
            .identifier(table_ident)
            .metadata_location(metadata_location)
            .metadata(metadata)
            .file_io(self.file_io.clone())
            .build()?;
        Ok(table)
    }

    /// Loads an existing table from the s3tables catalog.
    ///
    /// Retrieves the metadata location of the specified table and constructs a
    /// `Table` object with the retrieved metadata.
    ///
    /// This function can return an error in the following situations:
    /// - If the table does not have a metadata location, identified by a specific
    /// `Unexpected` variant.
    /// - Errors from the underlying database query process, converted using
    /// `from_aws_sdk_error`.
    async fn load_table(&self, table_ident: &TableIdent) -> Result<Table> {
        let req = self
            .s3tables_client
            .get_table()
            .table_bucket_arn(self.config.table_bucket_arn.clone())
            .namespace(table_ident.namespace().to_url_string())
            .name(table_ident.name());
        let resp: GetTableOutput = req.send().await.map_err(from_aws_sdk_error)?;

        // when a table is created, it's possible that the metadata location is not set.
        let metadata_location = resp.metadata_location().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                format!(
                    "Table {} does not have metadata location",
                    table_ident.name()
                ),
            )
        })?;
        let input_file = self.file_io.new_input(metadata_location)?;
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

    /// Drops an existing table from the s3tables catalog.
    ///
    /// Validates the table identifier and then deletes the corresponding
    /// table from the s3tables catalog.
    ///
    /// This function can return an error in the following situations:
    /// - Errors from the underlying database deletion process, converted using
    /// `from_aws_sdk_error`.
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

    /// Checks if a table exists within the s3tables catalog.
    ///
    /// Validates the table identifier by querying the s3tables catalog
    /// to determine if the specified table exists.
    ///
    /// # Returns
    /// A `Result<bool>` indicating the outcome of the check:
    /// - `Ok(true)` if the table exists.
    /// - `Ok(false)` if the table does not exist, identified by a specific
    /// `IsNotFoundException` variant.
    /// - `Err(...)` if an error occurs during validation or the s3tables catalog
    /// query, with the error encapsulating the issue.
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

    /// Renames an existing table within the s3tables catalog.
    ///
    /// Validates the source and destination table identifiers and then renames
    /// the source table to the destination table.
    ///
    /// This function can return an error in the following situations:
    /// - Errors from the underlying database renaming process, converted using
    /// `from_aws_sdk_error`.
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

    /// Updates an existing table within the s3tables catalog.
    ///
    /// This function is still in development and will always return an error.
    async fn update_table(&self, _commit: TableCommit) -> Result<Table> {
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
        "Operation failed for hitting aws sdk error".to_string(),
    )
    .with_source(anyhow!("aws sdk error: {:?}", error))
}

#[cfg(test)]
mod tests {
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

    use super::*;

    async fn load_s3tables_catalog_from_env() -> Result<Option<S3TablesCatalog>> {
        let table_bucket_arn = match std::env::var("TABLE_BUCKET_ARN").ok() {
            Some(table_bucket_arn) => table_bucket_arn,
            None => return Ok(None),
        };

        let config = S3TablesCatalogConfig::builder()
            .table_bucket_arn(table_bucket_arn)
            .build();

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
        assert!(!namespaces.is_empty());
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
        assert!(!tables.is_empty());
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

    #[tokio::test]
    async fn test_s3tables_create_delete_namespace() {
        let catalog = match load_s3tables_catalog_from_env().await {
            Ok(Some(catalog)) => catalog,
            Ok(None) => return,
            Err(e) => panic!("Error loading catalog: {}", e),
        };

        let namespace = NamespaceIdent::new("test_s3tables_create_delete_namespace".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        assert!(catalog.namespace_exists(&namespace).await.unwrap());
        catalog.drop_namespace(&namespace).await.unwrap();
        assert!(!catalog.namespace_exists(&namespace).await.unwrap());
    }

    #[tokio::test]
    async fn test_s3tables_create_delete_table() {
        let catalog = match load_s3tables_catalog_from_env().await {
            Ok(Some(catalog)) => catalog,
            Ok(None) => return,
            Err(e) => panic!("Error loading catalog: {}", e),
        };

        let creation = {
            let schema = Schema::builder()
                .with_schema_id(0)
                .with_fields(vec![
                    NestedField::required(1, "foo", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "bar", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap();
            TableCreation::builder()
                .name("test_s3tables_create_delete_table".to_string())
                .properties(HashMap::new())
                .schema(schema)
                .build()
        };

        let namespace = NamespaceIdent::new("test_s3tables_create_delete_table".to_string());
        let table_ident = TableIdent::new(
            namespace.clone(),
            "test_s3tables_create_delete_table".to_string(),
        );
        catalog.drop_namespace(&namespace).await.ok();
        catalog.drop_table(&table_ident).await.ok();

        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        catalog.create_table(&namespace, creation).await.unwrap();
        assert!(catalog.table_exists(&table_ident).await.unwrap());
        catalog.drop_table(&table_ident).await.unwrap();
        assert!(!catalog.table_exists(&table_ident).await.unwrap());
        catalog.drop_namespace(&namespace).await.unwrap();
    }
}
