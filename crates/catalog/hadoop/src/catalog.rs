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

use async_trait::async_trait;
use iceberg::io::{FileIO, S3_ENDPOINT};
use iceberg::spec::{TableMetadata, TableMetadataBuilder};
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};
use tokio::io::AsyncReadExt;
use typed_builder::TypedBuilder;

use crate::utils::{
    create_metadata_location, create_sdk_config, get_default_table_location, valid_s3_namespaces,
    FS_DEFAULTFS,
};

/// Hadoop catalog configuration.
#[derive(Debug, TypedBuilder)]
pub struct HadoopCatalogConfig {
    /// Properties for the catalog. The available properties are:
    /// when using s3 filesystem
    /// - `profile_name`: The name of the AWS profile to use.
    /// - `region_name`: The AWS region to use.
    /// - `aws_access_key_id`: The AWS access key ID to use.
    /// - `aws_secret_access_key`: The AWS secret access key to use.
    /// - `aws_session_token`: The AWS session token to use.
    /// when using hdfs filesystem (like the properties in hdfs-site.xml)
    #[builder(default)]
    properties: HashMap<String, String>,
    /// Endpoint URL for the catalog.
    #[builder(default, setter(strip_option(fallback = endpoint_url_opt)))]
    endpoint_url: Option<String>,
    /// warehouse for the catalog.
    /// This is the root directory for the catalog.
    /// when using s3 filesystem  like s3://<bucket>/<path>.
    /// when using hdfs filesystem like hdfs://<namenode>:<port>/<path>.
    #[builder(default, setter(strip_option(fallback = warehouse_opt)))]
    warehouse: Option<String>,
}

/// Hadoop catalog implementation.
#[derive(Debug)]
pub struct HadoopCatalog {
    config: HadoopCatalogConfig,
    file_io: FileIO,
    s3_client: Option<aws_sdk_s3::Client>,
    hdfs_native_client: Option<hdfs_native::Client>,
}

impl HadoopCatalog {
    /// Creates a new Hadoop catalog.
    pub async fn new(config: HadoopCatalogConfig) -> Result<Self> {
        if let Some(warehouse_url) = &config.warehouse {
            if warehouse_url.starts_with("s3://") || warehouse_url.starts_with("s3a://") {
                let mut io_props = config.properties.clone();
                if config.endpoint_url.is_some() {
                    io_props.insert(
                        S3_ENDPOINT.to_string(),
                        config.endpoint_url.clone().unwrap_or_default(),
                    );
                }
                let file_io = FileIO::from_path(&warehouse_url)?
                    .with_props(&io_props)
                    .build()?;
                let aws_config = create_sdk_config(&config.properties, config.endpoint_url.clone());
                let s3_client = Some(aws_sdk_s3::Client::from_conf(aws_config));
                return Ok(Self {
                    config: config,
                    file_io,
                    s3_client: s3_client,
                    hdfs_native_client: None,
                });
            } else if warehouse_url.starts_with("hdfs://") {
                //todo hdfs native client
                let file_io = FileIO::from_path(&warehouse_url)?
                    .with_props(&config.properties)
                    .build()?;
                let default_fs = config
                    .properties
                    .get(FS_DEFAULTFS)
                    .ok_or(iceberg::Error::new(
                        ErrorKind::DataInvalid,
                        " fs.defaultFS is null",
                    ))?;
                let hdfs_native_client =
                    hdfs_native::Client::new_with_config(&default_fs, config.properties.clone())
                        .map_err(|e| iceberg::Error::new(ErrorKind::Unexpected, e.to_string()))?;
                return Ok(Self {
                    config: config,
                    file_io: file_io,
                    s3_client: None,
                    hdfs_native_client: Some(hdfs_native_client),
                });
            }

            return Err(Error::new(
                ErrorKind::DataInvalid,
                "warehouse_url is not supported",
            ));
        } else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "warehouse_url is required",
            ));
        }
    }
}

#[async_trait]
impl Catalog for HadoopCatalog {
    /// List namespaces from s3tables catalog.
    ///
    /// S3Tables doesn't support nested namespaces. If parent is provided, it will
    /// return an empty list.
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        let mut result = Vec::new();
        if self.s3_client.is_some() {
            let s3_client = self.s3_client.as_ref().unwrap();
            match self.config.warehouse.clone() {
                Some(warehouse_url) => {
                    let bucket = warehouse_url.split("/").nth(2).unwrap_or("");
                    let warehouse_prefix_origin = warehouse_url
                        .split("/")
                        .skip(3)
                        .collect::<Vec<_>>()
                        .join("/");
                    let mut warehouse_prefix = warehouse_prefix_origin.clone();
                    let mut prefix = format!("{}/", &warehouse_prefix);

                    if let Some(parent) = parent {
                        warehouse_prefix = format!("{}/{}", &warehouse_prefix, parent.join("/"));
                        prefix = format!("{}/", &warehouse_prefix);
                    }

                    let list = s3_client
                        .list_objects_v2()
                        .bucket(bucket)
                        .prefix(&prefix)
                        .delimiter("/")
                        .send()
                        .await
                        .map_err(|e| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                format!("Failed to list objects: {}", e),
                            )
                        })?;
                    for object in list.common_prefixes.unwrap_or_default() {
                        let key = object.prefix.unwrap_or_default();
                        // Only add namespace if it's a first level directory (has only one /)

                        if key.ends_with("/") && key.starts_with(&warehouse_prefix) {
                            let warehouse_relative_path = &key[warehouse_prefix_origin.len()..];

                            let namespaces = warehouse_relative_path.split("/").collect::<Vec<_>>();

                            let table_version_hint_path = format!(
                                "{}{}metadata/version-hint.text",
                                &warehouse_url
                                    .split("/")
                                    .skip(3)
                                    .collect::<Vec<_>>()
                                    .join("/"),
                                namespaces.join("/"),
                            );

                            //check It's not table
                            let table_version_hint = s3_client
                                .get_object()
                                .bucket(bucket)
                                .key(table_version_hint_path)
                                .send()
                                .await
                                .map_err(|e| {
                                    Error::new(
                                        ErrorKind::DataInvalid,
                                        format!("Failed to get table version hint: {}", e),
                                    )
                                });

                            if !table_version_hint.is_ok() && !namespaces.is_empty() {
                                result.push(NamespaceIdent::from_vec(
                                    namespaces
                                        .iter()
                                        .filter(|e| !e.is_empty())
                                        .map(|e| e.to_string())
                                        .collect(),
                                )?);
                            }
                        }
                    }
                }
                None => {
                    return Err(Error::new(ErrorKind::DataInvalid, "warehouse is required"));
                }
            }
        } else if self.hdfs_native_client.is_some() {
            let hdfs_native_client = self.hdfs_native_client.as_ref().unwrap();
            let default_fs =
                self.config
                    .properties
                    .get(FS_DEFAULTFS)
                    .ok_or(iceberg::Error::new(
                        ErrorKind::DataInvalid,
                        " fs.defaultFS is null",
                    ))?;

            match self.config.warehouse.clone() {
                Some(warehouse_url) => {
                    let mut prefix = warehouse_url[default_fs.len()..].to_string();
                    if let Some(parent) = parent {
                        prefix = format!("{}/{}", &prefix, parent.join("/"));
                    }
                    let list = hdfs_native_client
                        .list_status(&prefix, false)
                        .await
                        .map_err(|e| iceberg::Error::new(ErrorKind::Unexpected, e.to_string()))?;
                    for f in list {
                        let table_version_hint_path =
                            format!("{}/metadata/version-hint.text", f.path.clone(),);
                        if hdfs_native_client
                            .get_file_info(&table_version_hint_path)
                            .await
                            .is_err()
                        {
                            let file_relative_path = f.path[prefix.len()..].to_string();
                            let namespaces = file_relative_path.split("/").collect::<Vec<_>>();
                            result.push(NamespaceIdent::from_vec(
                                namespaces
                                    .iter()
                                    .filter(|e| !e.is_empty())
                                    .map(|e| e.to_string())
                                    .collect(),
                            )?);
                        }
                    }
                }
                None => {
                    return Err(Error::new(ErrorKind::DataInvalid, "warehouse is required"));
                }
            }
        } else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "s3 client or hdfs native client is not initialized",
            ));
        }

        return Ok(result);
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
        if self.s3_client.is_some() {
            if valid_s3_namespaces(&namespace).is_err() {
                return Err(Error::new(ErrorKind::DataInvalid, "Invalid namespace name"));
            }
            let s3_client = self.s3_client.as_ref().unwrap();
            match self.config.warehouse.clone() {
                Some(warehouse_url) => {
                    let bucket = warehouse_url.split("/").nth(2).unwrap_or("");
                    let prefix = format!(
                        "{}/{}/",
                        &warehouse_url
                            .split("/")
                            .skip(3)
                            .collect::<Vec<_>>()
                            .join("/"),
                        namespace.join("/")
                    );
                    s3_client
                        .put_object()
                        .bucket(bucket)
                        .key(prefix)
                        .content_length(0)
                        .send()
                        .await
                        .map_err(|e| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                format!("Failed to create namespace: {}", e),
                            )
                        })?;
                }
                None => {
                    return Err(Error::new(ErrorKind::DataInvalid, "warehouse is required"));
                }
            }
        } else if self.hdfs_native_client.is_some() {
            let hdfs_native_client = self.hdfs_native_client.as_ref().unwrap();
            let default_fs =
                self.config
                    .properties
                    .get(FS_DEFAULTFS)
                    .ok_or(iceberg::Error::new(
                        ErrorKind::DataInvalid,
                        " fs.defaultFS is null",
                    ))?;

            match self.config.warehouse.clone() {
                Some(warehouse_url) => {
                    let prefix = format!(
                        "{}/{}",
                        &warehouse_url[default_fs.len()..].to_string(),
                        namespace.join("/")
                    );

                    hdfs_native_client
                        .mkdirs(&prefix, 0o755, true)
                        .await
                        .map_err(|e| iceberg::Error::new(ErrorKind::Unexpected, e.to_string()))?;
                }
                None => {
                    return Err(Error::new(ErrorKind::DataInvalid, "warehouse is required"));
                }
            }
        } else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "s3 client or hdfs native client is not initialized",
            ));
        }

        Ok(Namespace::new(namespace.clone()))
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
        if self.s3_client.is_some() {
            if valid_s3_namespaces(&namespace).is_err() {
                return Err(Error::new(ErrorKind::DataInvalid, "Invalid namespace name"));
            }
            let s3_client = self.s3_client.as_ref().unwrap();
            match self.config.warehouse.clone() {
                Some(warehouse_url) => {
                    let bucket = warehouse_url.split("/").nth(2).unwrap_or("");
                    let prefix = format!(
                        "{}/{}/",
                        &warehouse_url
                            .split("/")
                            .skip(3)
                            .collect::<Vec<_>>()
                            .join("/"),
                        namespace.join("/")
                    );
                    s3_client
                        .get_object()
                        .bucket(bucket)
                        .key(&prefix)
                        .send()
                        .await
                        .map_err(|e| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                format!("Failed to get namespace: {}", e),
                            )
                        })?;
                    let table_version_hint_path = format!("{}metadata/version-hint.text", &prefix,);

                    //check It's not table
                    let table_version_hint = s3_client
                        .get_object()
                        .bucket(bucket)
                        .key(table_version_hint_path)
                        .send()
                        .await
                        .map_err(|e| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                format!("Failed to get table version hint: {}", e),
                            )
                        });
                    if table_version_hint.is_ok() {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            "It's a table not namespace",
                        ));
                    }
                }
                None => {
                    return Err(Error::new(ErrorKind::DataInvalid, "warehouse is required"));
                }
            }
        } else if self.hdfs_native_client.is_some() {
            let hdfs_native_client = self.hdfs_native_client.as_ref().unwrap();
            let default_fs =
                self.config
                    .properties
                    .get(FS_DEFAULTFS)
                    .ok_or(iceberg::Error::new(
                        ErrorKind::DataInvalid,
                        " fs.defaultFS is null",
                    ))?;

            match self.config.warehouse.clone() {
                Some(warehouse_url) => {
                    let prefix = format!(
                        "{}/{}",
                        &warehouse_url[default_fs.len()..].to_string(),
                        namespace.join("/")
                    );

                    hdfs_native_client
                        .get_file_info(&prefix)
                        .await
                        .map_err(|e| iceberg::Error::new(ErrorKind::Unexpected, e.to_string()))?;

                    let table_version_hint_path =
                        format!("{}/metadata/version-hint.text", &prefix,);
                    //check It's not table
                    let table_version_hint = hdfs_native_client
                        .get_file_info(&table_version_hint_path)
                        .await
                        .map_err(|e| iceberg::Error::new(ErrorKind::Unexpected, e.to_string()));
                    if table_version_hint.is_ok() {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            "It's a table not namespace",
                        ));
                    }
                }
                None => {
                    return Err(Error::new(ErrorKind::DataInvalid, "warehouse is required"));
                }
            }
        } else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "s3 client or hdfs native client is not initialized",
            ));
        }

        Ok(Namespace::new(namespace.clone()))
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
        if self.s3_client.is_some() {
            if valid_s3_namespaces(&namespace).is_err() {
                return Err(Error::new(ErrorKind::DataInvalid, "Invalid namespace name"));
            }
            let s3_client = self.s3_client.as_ref().unwrap();
            match self.config.warehouse.clone() {
                Some(warehouse_url) => {
                    let bucket = warehouse_url.split("/").nth(2).unwrap_or("");
                    let prefix = format!(
                        "{}/{}/",
                        &warehouse_url
                            .split("/")
                            .skip(3)
                            .collect::<Vec<_>>()
                            .join("/"),
                        namespace.join("/")
                    );
                    match s3_client
                        .head_object()
                        .bucket(bucket)
                        .key(&prefix)
                        .send()
                        .await
                    {
                        Ok(_) => (),
                        Err(_e) => {
                            return Ok(false);
                        }
                    };
                    let table_version_hint_path = format!("{}metadata/version-hint.text", &prefix,);

                    //check It's not table
                    let table_version_hint = s3_client
                        .get_object()
                        .bucket(bucket)
                        .key(table_version_hint_path)
                        .send()
                        .await
                        .map_err(|e| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                format!("Failed to get table version hint: {}", e),
                            )
                        });
                    if table_version_hint.is_ok() {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            "It's a table not namespace",
                        ));
                    }
                }
                None => {
                    return Err(Error::new(ErrorKind::DataInvalid, "warehouse is required"));
                }
            }
        } else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "s3 client is not initialized",
            ));
        }

        Ok(true)
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
            "Update namespace is not supported for hadoop catalog",
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
        if self.s3_client.is_some() {
            let s3_client = self.s3_client.as_ref().unwrap();
            match self.config.warehouse.clone() {
                Some(warehouse_url) => {
                    let bucket = warehouse_url.split("/").nth(2).unwrap_or("");
                    let prefix = format!(
                        "{}/{}/",
                        &warehouse_url
                            .split("/")
                            .skip(3)
                            .collect::<Vec<_>>()
                            .join("/"),
                        namespace.join("/")
                    );
                    let list = s3_client
                        .list_objects_v2()
                        .bucket(bucket)
                        .prefix(&prefix)
                        .send()
                        .await
                        .map_err(|e| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                format!("Failed to list objects: {}", e),
                            )
                        })?;

                    for object in list.contents.unwrap_or_default() {
                        s3_client
                            .delete_object()
                            .bucket(bucket)
                            .key(object.key.unwrap_or_default())
                            .send()
                            .await
                            .map_err(|e| {
                                Error::new(
                                    ErrorKind::DataInvalid,
                                    format!("Failed to delete object: {}", e),
                                )
                            })?;
                    }
                }
                None => {
                    return Err(Error::new(ErrorKind::DataInvalid, "warehouse is required"));
                }
            }
        } else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "s3 client is not initialized",
            ));
        }

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
        if self.s3_client.is_some() {
            let s3_client = self.s3_client.as_ref().unwrap();
            match self.config.warehouse.clone() {
                Some(warehouse_url) => {
                    let bucket = warehouse_url.split("/").nth(2).unwrap_or("");
                    let prefix = format!(
                        "{}/{}/",
                        &warehouse_url
                            .split("/")
                            .skip(3)
                            .collect::<Vec<_>>()
                            .join("/"),
                        namespace.join("/")
                    );
                    let list = s3_client
                        .list_objects_v2()
                        .bucket(bucket)
                        .prefix(&prefix)
                        .delimiter("/")
                        .send()
                        .await
                        .map_err(|e| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                format!("Failed to list tables: {}", e),
                            )
                        })?;
                    let mut result = Vec::new();
                    for object in list.common_prefixes.unwrap_or_default() {
                        let key = object.prefix.unwrap_or_default();
                        if key.ends_with("/") {
                            let mut table_name = if key.ends_with("/") {
                                key[..key.len() - 1].to_string()
                            } else {
                                key.clone()
                            };
                            table_name = table_name.split("/").last().unwrap_or(&"").to_string();

                            let table_version_hint_path =
                                format!("{}metadata/version-hint.text", &key);
                            let table_version_hint = s3_client
                                .get_object()
                                .bucket(bucket)
                                .key(table_version_hint_path)
                                .send()
                                .await
                                .map_err(|e| {
                                    Error::new(
                                        ErrorKind::DataInvalid,
                                        format!("Failed to get table version hint: {}", e),
                                    )
                                });
                            if table_version_hint.is_ok() {
                                result.push(TableIdent::new(
                                    namespace.clone(),
                                    table_name.to_string(),
                                ));
                            }
                        }
                    }
                    return Ok(result);
                }
                None => {
                    return Err(Error::new(ErrorKind::DataInvalid, "warehouse is required"));
                }
            }
        } else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "s3 client is not initialized",
            ));
        }
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
        creation: TableCreation,
    ) -> Result<Table> {
        let table_name = creation.name.clone();
        let location = match self.config.warehouse.clone() {
            Some(warehouse_url) => {
                get_default_table_location(&namespace, &table_name, &warehouse_url)
            }
            None => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "warehouse_url is required",
                ));
            }
        };

        let table_version_hint_path = format!("{}/metadata/version-hint.text", &location,);

        if self.s3_client.is_some() {
            let s3_client = self.s3_client.as_ref().unwrap();
            let table_version_hint_relative_path = format!(
                "{}/metadata/version-hint.text",
                &location.split("/").skip(3).collect::<Vec<_>>().join("/")
            );

            match self.config.warehouse.clone() {
                Some(warehouse_url) => {
                    let bucket = warehouse_url.split("/").nth(2).unwrap_or("");
                    let table_version_hint = s3_client
                        .get_object()
                        .bucket(bucket)
                        .key(&table_version_hint_relative_path)
                        .send()
                        .await
                        .map_err(|e| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                format!("Failed to get table version hint: {}", e),
                            )
                        });
                    if table_version_hint.is_ok() {
                        return Err(Error::new(ErrorKind::DataInvalid, "Table already exists"));
                    }

                    if valid_s3_namespaces(&namespace).is_err() {
                        return Err(Error::new(ErrorKind::DataInvalid, "Invalid namespace name"));
                    }
                }
                None => {
                    return Err(Error::new(ErrorKind::DataInvalid, "warehouse is required"));
                }
            }
        } else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "s3 client is not initialized",
            ));
        }

        let metadata = TableMetadataBuilder::from_table_creation(creation)?
            .build()?
            .metadata;

        let metadata_location = create_metadata_location(&location, 1)?;

        self.file_io
            .new_output(&metadata_location)?
            .write(serde_json::to_vec(&metadata)?.into())
            .await?;

        self.file_io
            .new_output(&table_version_hint_path)?
            .write("1".into())
            .await?;
        Table::builder()
            .file_io(self.file_io.clone())
            .metadata_location(metadata_location)
            .metadata(metadata)
            .identifier(TableIdent::new(namespace.clone(), table_name))
            .build()
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
        if self.s3_client.is_some() {
            let s3_client = self.s3_client.as_ref().unwrap();
            match self.config.warehouse.clone() {
                Some(warehouse_url) => {
                    let bucket = warehouse_url.split("/").nth(2).unwrap_or("");
                    let table_name = table_ident.name.clone();
                    let table_version_hint_path = format!(
                        "{}/{}/{}/metadata/version-hint.text",
                        &warehouse_url
                            .split("/")
                            .skip(3)
                            .collect::<Vec<_>>()
                            .join("/"),
                        table_ident.namespace.join("/"),
                        &table_name
                    );

                    let table_version_hint_result = s3_client
                        .get_object()
                        .bucket(bucket)
                        .key(table_version_hint_path)
                        .send()
                        .await;

                    match table_version_hint_result {
                        Ok(table_version_hint_result_output) => {
                            let mut buf = Vec::new();
                            table_version_hint_result_output
                                .body
                                .into_async_read()
                                .read_to_end(&mut buf)
                                .await?;
                            let table_version_hint = String::from_utf8_lossy(&buf);

                            let metadata_location = format!(
                                "{}/{}/{}/metadata/v{}.metadata.json",
                                &warehouse_url,
                                table_ident.namespace.join("/"),
                                &table_name,
                                &table_version_hint
                            );
                            let metadata_content =
                                self.file_io.new_input(&metadata_location)?.read().await?;
                            let metadata =
                                serde_json::from_slice::<TableMetadata>(&metadata_content)?;

                            return Ok(Table::builder()
                                .file_io(self.file_io.clone())
                                .metadata_location(metadata_location)
                                .metadata(metadata)
                                .identifier(table_ident.clone())
                                .build()?);
                        }

                        Err(e) => {
                            return Err(Error::new(
                                ErrorKind::DataInvalid,
                                format!("Failed to get table version hint: {}", e),
                            ));
                        }
                    }
                }
                None => {
                    return Err(Error::new(ErrorKind::DataInvalid, "warehouse is required"));
                }
            }
        } else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "s3 client is not initialized",
            ));
        }
    }

    /// Asynchronously drops a table from the database.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The namespace provided in `table` cannot be validated
    /// or does not exist.
    /// - The underlying database client encounters an error while
    /// attempting to drop the table. This includes scenarios where
    /// the table does not exist.
    /// - Any network or communication error occurs with the database backend.
    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        if self.s3_client.is_some() {
            let s3_client = self.s3_client.as_ref().unwrap();
            match self.config.warehouse.clone() {
                Some(warehouse_url) => {
                    let bucket = warehouse_url.split("/").nth(2).unwrap_or("");
                    let table_name = table.name.clone();
                    let table_path = format!(
                        "{}/{}/{}/",
                        &warehouse_url
                            .split("/")
                            .skip(3)
                            .collect::<Vec<_>>()
                            .join("/"),
                        table.namespace.join("/"),
                        &table_name
                    );
                    let list = s3_client
                        .list_objects_v2()
                        .bucket(bucket)
                        .prefix(&table_path)
                        .send()
                        .await
                        .map_err(|e| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                format!("Failed to list objects: {}", e),
                            )
                        })?;

                    for object in list.contents.unwrap_or_default() {
                        s3_client
                            .delete_object()
                            .bucket(bucket)
                            .key(object.key.unwrap_or_default())
                            .send()
                            .await
                            .map_err(|e| {
                                Error::new(
                                    ErrorKind::DataInvalid,
                                    format!("Failed to delete object: {}", e),
                                )
                            })?;
                    }
                }
                None => {
                    return Err(Error::new(ErrorKind::DataInvalid, "warehouse is required"));
                }
            }
        } else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "s3 client is not initialized",
            ));
        }

        Ok(())
    }

    /// Asynchronously checks the existence of a specified table
    /// in the database.
    ///
    /// # Returns
    /// - `Ok(true)` if the table exists in the database.
    /// - `Ok(false)` if the table does not exist in the database.
    /// - `Err(...)` if an error occurs during the process
    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        if self.s3_client.is_some() {
            let s3_client = self.s3_client.as_ref().unwrap();
            match self.config.warehouse.clone() {
                Some(warehouse_url) => {
                    let bucket = warehouse_url.split("/").nth(2).unwrap_or("");
                    let table_name = table.name.clone();
                    let table_version_hint_path = format!(
                        "{}/{}/{}/metadata/version-hint.text",
                        &warehouse_url
                            .split("/")
                            .skip(3)
                            .collect::<Vec<_>>()
                            .join("/"),
                        table.namespace.join("/"),
                        &table_name
                    );
                    s3_client
                        .head_object()
                        .bucket(bucket)
                        .key(table_version_hint_path)
                        .send()
                        .await
                        .map_err(|e| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                format!("Failed to check table: {}", e),
                            )
                        })?;
                }
                None => {
                    return Err(Error::new(ErrorKind::DataInvalid, "warehouse is required"));
                }
            }
        } else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "s3 client is not initialized",
            ));
        }

        Ok(true)
    }

    async fn update_table(&self, _commit: TableCommit) -> Result<Table> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Updating a table is not supported yet",
        ))
    }
    /// Asynchronously renames a table within the database
    /// or moves it between namespaces (databases).
    ///
    /// # Returns
    /// - `Ok(())` on successful rename or move of the table.
    /// - `Err(...)` if an error occurs during the process.
    async fn rename_table(&self, _src: &TableIdent, _destt: &TableIdent) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Updating a table is not supported yet",
        ))
    }
}
