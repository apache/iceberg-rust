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

//! This module contains in-memory catalog implementation.

use async_lock::Mutex;
use iceberg::io::FileIO;
use iceberg::spec::{TableMetadata, TableMetadataBuilder};
use itertools::Itertools;
use std::collections::HashMap;
use uuid::Uuid;

use async_trait::async_trait;

use iceberg::table::Table;
use iceberg::Result;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent,
};
use tempfile::TempDir;

use crate::namespace_state::NamespaceState;

/// In-memory catalog implementation.
#[derive(Debug)]
pub struct InMemoryCatalog {
    root_namespace_state: Mutex<NamespaceState>,
    file_io: FileIO,
}

impl InMemoryCatalog {
    /// Creates an in-memory catalog.
    pub fn new(file_io: FileIO) -> Self {
        Self {
            root_namespace_state: Mutex::new(NamespaceState::new()),
            file_io,
        }
    }
}

/// Create metadata location from `location` and `version`
fn create_metadata_location(location: impl AsRef<str>, version: i32) -> Result<String> {
    if version < 0 {
        Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Table metadata version: '{}' must be a non-negative integer",
                version
            ),
        ))
    } else {
        let version = format!("{:0>5}", version);
        let id = Uuid::new_v4();
        let metadata_location = format!(
            "{}/metadata/{}-{}.metadata.json",
            location.as_ref(),
            version,
            id
        );

        Ok(metadata_location)
    }
}

#[async_trait]
impl Catalog for InMemoryCatalog {
    /// List namespaces inside the Catalog.
    async fn list_namespaces(
        &self,
        maybe_parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        let root_namespace_state = self.root_namespace_state.lock().await;

        match maybe_parent {
            None => Ok(root_namespace_state
                .list_top_level_namespaces()
                .into_iter()
                .map(|str| NamespaceIdent::new(str.to_string()))
                .collect_vec()),
            Some(parent_namespace_ident) => {
                let namespaces = root_namespace_state
                    .list_namespaces_under(parent_namespace_ident)?
                    .into_iter()
                    .map(|name| NamespaceIdent::new(name.to_string()))
                    .collect_vec();

                Ok(namespaces)
            }
        }
    }

    /// Create a new namespace inside the catalog.
    async fn create_namespace(
        &self,
        namespace_ident: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        let mut root_namespace_state = self.root_namespace_state.lock().await;

        root_namespace_state.insert_new_namespace(namespace_ident, properties.clone())?;
        let namespace = Namespace::with_properties(namespace_ident.clone(), properties);

        Ok(namespace)
    }

    /// Get a namespace information from the catalog.
    async fn get_namespace(&self, namespace_ident: &NamespaceIdent) -> Result<Namespace> {
        let root_namespace_state = self.root_namespace_state.lock().await;

        let namespace = Namespace::with_properties(
            namespace_ident.clone(),
            root_namespace_state
                .get_properties(namespace_ident)?
                .clone(),
        );

        Ok(namespace)
    }

    /// Check if namespace exists in catalog.
    async fn namespace_exists(&self, namespace_ident: &NamespaceIdent) -> Result<bool> {
        let guarded_namespaces = self.root_namespace_state.lock().await;

        Ok(guarded_namespaces.namespace_exists(namespace_ident))
    }

    /// Update a namespace inside the catalog.
    ///
    /// # Behavior
    ///
    /// The properties must be the full set of namespace.
    async fn update_namespace(
        &self,
        namespace_ident: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        let mut root_namespace_state = self.root_namespace_state.lock().await;

        root_namespace_state.replace_properties(namespace_ident, properties)
    }

    /// Drop a namespace from the catalog.
    async fn drop_namespace(&self, namespace_ident: &NamespaceIdent) -> Result<()> {
        let mut root_namespace_state = self.root_namespace_state.lock().await;

        root_namespace_state.remove_existing_namespace(namespace_ident)
    }

    /// List tables from namespace.
    async fn list_tables(&self, namespace_ident: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        let root_namespace_state = self.root_namespace_state.lock().await;

        let table_names = root_namespace_state.list_tables(namespace_ident)?;
        let table_idents = table_names
            .into_iter()
            .map(|table_name| TableIdent::new(namespace_ident.clone(), table_name.clone()))
            .collect_vec();

        Ok(table_idents)
    }

    /// Create a new table inside the namespace.
    async fn create_table(
        &self,
        namespace_ident: &NamespaceIdent,
        table_creation: TableCreation,
    ) -> Result<Table> {
        let mut root_namespace_state = self.root_namespace_state.lock().await;

        let table_name = table_creation.name.clone();
        let table_ident = TableIdent::new(namespace_ident.clone(), table_name);
        let table_exists = root_namespace_state.table_exists(&table_ident)?;

        if table_exists {
            Err(Error::new(
                ErrorKind::Unexpected,
                format!(
                    "Cannot create table {:?}. Table already exists",
                    &table_ident
                ),
            ))
        } else {
            let (table_creation, location) = match table_creation.location.clone() {
                Some(location) => (table_creation, location),
                None => {
                    let tmp_dir = TempDir::new()?;
                    let location = tmp_dir.path().to_str().unwrap().to_string();
                    let new_table_creation = TableCreation {
                        location: Some(location.clone()),
                        ..table_creation
                    };
                    (new_table_creation, location)
                }
            };
            let metadata = TableMetadataBuilder::from_table_creation(table_creation)?.build()?;
            let metadata_location = create_metadata_location(&location, 0)?;

            self.file_io
                .new_output(&metadata_location)?
                .write(serde_json::to_vec(&metadata)?.into())
                .await?;

            root_namespace_state.insert_new_table(&table_ident, metadata_location.clone())?;

            let table = Table::builder()
                .file_io(self.file_io.clone())
                .metadata_location(metadata_location)
                .metadata(metadata)
                .identifier(table_ident)
                .build();

            Ok(table)
        }
    }

    /// Load table from the catalog.
    async fn load_table(&self, table_ident: &TableIdent) -> Result<Table> {
        let root_namespace_state = self.root_namespace_state.lock().await;

        let metadata_location = root_namespace_state.get_existing_table_location(table_ident)?;
        let input_file = self.file_io.new_input(metadata_location)?;
        let metadata_content = input_file.read().await?;
        let metadata = serde_json::from_slice::<TableMetadata>(&metadata_content)?;
        let table = Table::builder()
            .file_io(self.file_io.clone())
            .metadata_location(metadata_location.clone())
            .metadata(metadata)
            .identifier(table_ident.clone())
            .build();

        Ok(table)
    }

    /// Drop a table from the catalog.
    async fn drop_table(&self, table_ident: &TableIdent) -> Result<()> {
        let mut root_namespace_state = self.root_namespace_state.lock().await;

        root_namespace_state.remove_existing_table(table_ident)
    }

    /// Check if a table exists in the catalog.
    async fn table_exists(&self, table_ident: &TableIdent) -> Result<bool> {
        let root_namespace_state = self.root_namespace_state.lock().await;

        root_namespace_state.table_exists(table_ident)
    }

    /// Rename a table in the catalog.
    async fn rename_table(
        &self,
        src_table_ident: &TableIdent,
        dst_table_ident: &TableIdent,
    ) -> Result<()> {
        let mut root_namespace_state = self.root_namespace_state.lock().await;

        let mut new_root_namespace_state = root_namespace_state.clone();
        let metadata_location = new_root_namespace_state
            .get_existing_table_location(src_table_ident)?
            .clone();
        new_root_namespace_state.remove_existing_table(src_table_ident)?;
        new_root_namespace_state.insert_new_table(dst_table_ident, metadata_location)?;
        *root_namespace_state = new_root_namespace_state;
        Ok(())
    }

    /// Update a table to the catalog.
    async fn update_table(&self, _commit: TableCommit) -> Result<Table> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "In-memory catalog does not currently support updating tables.",
        ))
    }
}

#[cfg(test)]
mod tests {
    use iceberg::io::FileIOBuilder;
    use std::collections::HashSet;
    use std::hash::Hash;
    use std::iter::FromIterator;

    use iceberg::spec::{NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder, Type};

    use super::*;

    fn new_inmemory_catalog() -> impl Catalog {
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        InMemoryCatalog::new(file_io)
    }

    async fn create_namespace<C: Catalog>(catalog: &C, namespace_ident: &NamespaceIdent) {
        let _ = catalog
            .create_namespace(namespace_ident, HashMap::new())
            .await
            .unwrap();
    }

    async fn create_namespaces<C: Catalog>(catalog: &C, namespace_idents: &Vec<&NamespaceIdent>) {
        for namespace_ident in namespace_idents {
            let _ = create_namespace(catalog, namespace_ident).await;
        }
    }

    fn to_set<T: std::cmp::Eq + Hash>(vec: Vec<T>) -> HashSet<T> {
        HashSet::from_iter(vec)
    }

    fn simple_table_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![NestedField::required(
                1,
                "foo",
                Type::Primitive(PrimitiveType::Int),
            )
            .into()])
            .build()
            .unwrap()
    }

    async fn create_table<C: Catalog>(catalog: &C, table_ident: &TableIdent) {
        let _ = catalog
            .create_table(
                &table_ident.namespace,
                TableCreation::builder()
                    .name(table_ident.name().into())
                    .schema(simple_table_schema())
                    .build(),
            )
            .await
            .unwrap();
    }

    async fn create_tables<C: Catalog>(catalog: &C, table_idents: Vec<&TableIdent>) {
        for table_ident in table_idents {
            create_table(catalog, table_ident).await;
        }
    }

    fn assert_table_eq(table: &Table, expected_table_ident: &TableIdent, expected_schema: &Schema) {
        assert_eq!(table.identifier(), expected_table_ident);

        let metadata = table.metadata();

        assert_eq!(metadata.current_schema().as_ref(), expected_schema);

        let expected_partition_spec = PartitionSpec::builder()
            .with_spec_id(0)
            .with_fields(vec![])
            .build()
            .unwrap();

        assert_eq!(
            metadata
                .partition_specs_iter()
                .map(|p| p.as_ref())
                .collect_vec(),
            vec![&expected_partition_spec]
        );

        let expected_sorted_order = SortOrder::builder()
            .with_order_id(0)
            .with_fields(vec![])
            .build(expected_schema.clone())
            .unwrap();

        assert_eq!(
            metadata
                .sort_orders_iter()
                .map(|s| s.as_ref())
                .collect_vec(),
            vec![&expected_sorted_order]
        );

        assert_eq!(metadata.properties(), &HashMap::new());

        assert!(!table.readonly());
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_empty_vector() {
        let catalog = new_inmemory_catalog();

        assert_eq!(catalog.list_namespaces(None).await.unwrap(), vec![]);
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_single_namespace() {
        let catalog = new_inmemory_catalog();
        let namespace_ident = NamespaceIdent::new("abc".into());
        create_namespace(&catalog, &namespace_ident).await;

        assert_eq!(
            catalog.list_namespaces(None).await.unwrap(),
            vec![namespace_ident]
        );
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_multiple_namespaces() {
        let catalog = new_inmemory_catalog();
        let namespace_ident_1 = NamespaceIdent::new("a".into());
        let namespace_ident_2 = NamespaceIdent::new("b".into());
        create_namespaces(&catalog, &vec![&namespace_ident_1, &namespace_ident_2]).await;

        assert_eq!(
            to_set(catalog.list_namespaces(None).await.unwrap()),
            to_set(vec![namespace_ident_1, namespace_ident_2])
        );
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_only_top_level_namespaces() {
        let catalog = new_inmemory_catalog();
        let namespace_ident_1 = NamespaceIdent::new("a".into());
        let namespace_ident_2 = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_3 = NamespaceIdent::new("b".into());
        create_namespaces(
            &catalog,
            &vec![&namespace_ident_1, &namespace_ident_2, &namespace_ident_3],
        )
        .await;

        assert_eq!(
            to_set(catalog.list_namespaces(None).await.unwrap()),
            to_set(vec![namespace_ident_1, namespace_ident_3])
        );
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_no_namespaces_under_parent() {
        let catalog = new_inmemory_catalog();
        let namespace_ident_1 = NamespaceIdent::new("a".into());
        let namespace_ident_2 = NamespaceIdent::new("b".into());
        create_namespaces(&catalog, &vec![&namespace_ident_1, &namespace_ident_2]).await;

        assert_eq!(
            catalog
                .list_namespaces(Some(&namespace_ident_1))
                .await
                .unwrap(),
            vec![]
        );
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_namespace_under_parent() {
        let catalog = new_inmemory_catalog();
        let namespace_ident_1 = NamespaceIdent::new("a".into());
        let namespace_ident_2 = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_3 = NamespaceIdent::new("c".into());
        create_namespaces(
            &catalog,
            &vec![&namespace_ident_1, &namespace_ident_2, &namespace_ident_3],
        )
        .await;

        assert_eq!(
            to_set(catalog.list_namespaces(None).await.unwrap()),
            to_set(vec![namespace_ident_1.clone(), namespace_ident_3])
        );

        assert_eq!(
            catalog
                .list_namespaces(Some(&namespace_ident_1))
                .await
                .unwrap(),
            vec![NamespaceIdent::new("b".into())]
        );
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_multiple_namespaces_under_parent() {
        let catalog = new_inmemory_catalog();
        let namespace_ident_1 = NamespaceIdent::new("a".to_string());
        let namespace_ident_2 = NamespaceIdent::from_strs(vec!["a", "a"]).unwrap();
        let namespace_ident_3 = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_4 = NamespaceIdent::from_strs(vec!["a", "c"]).unwrap();
        let namespace_ident_5 = NamespaceIdent::new("b".into());
        create_namespaces(
            &catalog,
            &vec![
                &namespace_ident_1,
                &namespace_ident_2,
                &namespace_ident_3,
                &namespace_ident_4,
                &namespace_ident_5,
            ],
        )
        .await;

        assert_eq!(
            to_set(
                catalog
                    .list_namespaces(Some(&namespace_ident_1))
                    .await
                    .unwrap()
            ),
            to_set(vec![
                NamespaceIdent::new("a".into()),
                NamespaceIdent::new("b".into()),
                NamespaceIdent::new("c".into()),
            ])
        );
    }

    #[tokio::test]
    async fn test_namespace_exists_returns_false() {
        let catalog = new_inmemory_catalog();
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        assert!(!catalog
            .namespace_exists(&NamespaceIdent::new("b".into()))
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_namespace_exists_returns_true() {
        let catalog = new_inmemory_catalog();
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        assert!(catalog.namespace_exists(&namespace_ident).await.unwrap());
    }

    #[tokio::test]
    async fn test_create_namespace_with_empty_properties() {
        let catalog = new_inmemory_catalog();
        let namespace_ident = NamespaceIdent::new("a".into());

        assert_eq!(
            catalog
                .create_namespace(&namespace_ident, HashMap::new())
                .await
                .unwrap(),
            Namespace::new(namespace_ident.clone())
        );

        assert_eq!(
            catalog.get_namespace(&namespace_ident).await.unwrap(),
            Namespace::with_properties(namespace_ident, HashMap::new())
        );
    }

    #[tokio::test]
    async fn test_create_namespace_with_properties() {
        let catalog = new_inmemory_catalog();
        let namespace_ident = NamespaceIdent::new("abc".into());

        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("k".into(), "v".into());

        assert_eq!(
            catalog
                .create_namespace(&namespace_ident, properties.clone())
                .await
                .unwrap(),
            Namespace::with_properties(namespace_ident.clone(), properties.clone())
        );

        assert_eq!(
            catalog.get_namespace(&namespace_ident).await.unwrap(),
            Namespace::with_properties(namespace_ident, properties)
        );
    }

    #[tokio::test]
    async fn test_create_namespace_throws_error_if_namespace_already_exists() {
        let catalog = new_inmemory_catalog();
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        assert_eq!(
            catalog
                .create_namespace(&namespace_ident, HashMap::new())
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => Cannot create namespace {:?}. Namespace already exists",
                &namespace_ident
            )
        );

        assert_eq!(
            catalog.get_namespace(&namespace_ident).await.unwrap(),
            Namespace::with_properties(namespace_ident, HashMap::new())
        );
    }

    #[tokio::test]
    async fn test_create_nested_namespace() {
        let catalog = new_inmemory_catalog();
        let parent_namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &parent_namespace_ident).await;

        let child_namespace_ident = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();

        assert_eq!(
            catalog
                .create_namespace(&child_namespace_ident, HashMap::new())
                .await
                .unwrap(),
            Namespace::new(child_namespace_ident.clone())
        );

        assert_eq!(
            catalog.get_namespace(&child_namespace_ident).await.unwrap(),
            Namespace::with_properties(child_namespace_ident, HashMap::new())
        );
    }

    #[tokio::test]
    async fn test_create_deeply_nested_namespace() {
        let catalog = new_inmemory_catalog();
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]).await;

        let namespace_ident_a_b_c = NamespaceIdent::from_strs(vec!["a", "b", "c"]).unwrap();

        assert_eq!(
            catalog
                .create_namespace(&namespace_ident_a_b_c, HashMap::new())
                .await
                .unwrap(),
            Namespace::new(namespace_ident_a_b_c.clone())
        );

        assert_eq!(
            catalog.get_namespace(&namespace_ident_a_b_c).await.unwrap(),
            Namespace::with_properties(namespace_ident_a_b_c, HashMap::new())
        );
    }

    #[tokio::test]
    async fn test_create_nested_namespace_throws_error_if_top_level_namespace_doesnt_exist() {
        let catalog = new_inmemory_catalog();

        let nested_namespace_ident = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();

        assert_eq!(
            catalog
                .create_namespace(&nested_namespace_ident, HashMap::new())
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => No such namespace: {:?}",
                NamespaceIdent::new("a".into())
            )
        );

        assert_eq!(catalog.list_namespaces(None).await.unwrap(), vec![]);
    }

    #[tokio::test]
    async fn test_create_deeply_nested_namespace_throws_error_if_intermediate_namespace_doesnt_exist(
    ) {
        let catalog = new_inmemory_catalog();

        let namespace_ident_a = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident_a).await;

        let namespace_ident_a_b_c = NamespaceIdent::from_strs(vec!["a", "b", "c"]).unwrap();

        assert_eq!(
            catalog
                .create_namespace(&namespace_ident_a_b_c, HashMap::new())
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => No such namespace: {:?}",
                NamespaceIdent::from_strs(vec!["a", "b"]).unwrap()
            )
        );

        assert_eq!(
            catalog.list_namespaces(None).await.unwrap(),
            vec![namespace_ident_a.clone()]
        );

        assert_eq!(
            catalog
                .list_namespaces(Some(&namespace_ident_a))
                .await
                .unwrap(),
            vec![]
        );
    }

    #[tokio::test]
    async fn test_get_namespace() {
        let catalog = new_inmemory_catalog();
        let namespace_ident = NamespaceIdent::new("abc".into());

        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("k".into(), "v".into());
        let _ = catalog
            .create_namespace(&namespace_ident, properties.clone())
            .await
            .unwrap();

        assert_eq!(
            catalog.get_namespace(&namespace_ident).await.unwrap(),
            Namespace::with_properties(namespace_ident, properties)
        )
    }

    #[tokio::test]
    async fn test_get_nested_namespace() {
        let catalog = new_inmemory_catalog();
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]).await;

        assert_eq!(
            catalog.get_namespace(&namespace_ident_a_b).await.unwrap(),
            Namespace::with_properties(namespace_ident_a_b, HashMap::new())
        );
    }

    #[tokio::test]
    async fn test_get_deeply_nested_namespace() {
        let catalog = new_inmemory_catalog();
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_a_b_c = NamespaceIdent::from_strs(vec!["a", "b", "c"]).unwrap();
        create_namespaces(
            &catalog,
            &vec![
                &namespace_ident_a,
                &namespace_ident_a_b,
                &namespace_ident_a_b_c,
            ],
        )
        .await;

        assert_eq!(
            catalog.get_namespace(&namespace_ident_a_b_c).await.unwrap(),
            Namespace::with_properties(namespace_ident_a_b_c, HashMap::new())
        );
    }

    #[tokio::test]
    async fn test_get_namespace_throws_error_if_namespace_doesnt_exist() {
        let catalog = new_inmemory_catalog();
        create_namespace(&catalog, &NamespaceIdent::new("a".into())).await;

        let non_existent_namespace_ident = NamespaceIdent::new("b".into());
        assert_eq!(
            catalog
                .get_namespace(&non_existent_namespace_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => No such namespace: {:?}",
                non_existent_namespace_ident
            )
        )
    }

    #[tokio::test]
    async fn test_update_namespace() {
        let catalog = new_inmemory_catalog();
        let namespace_ident = NamespaceIdent::new("abc".into());
        create_namespace(&catalog, &namespace_ident).await;

        let mut new_properties: HashMap<String, String> = HashMap::new();
        new_properties.insert("k".into(), "v".into());

        catalog
            .update_namespace(&namespace_ident, new_properties.clone())
            .await
            .unwrap();

        assert_eq!(
            catalog.get_namespace(&namespace_ident).await.unwrap(),
            Namespace::with_properties(namespace_ident, new_properties)
        )
    }

    #[tokio::test]
    async fn test_update_nested_namespace() {
        let catalog = new_inmemory_catalog();
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]).await;

        let mut new_properties = HashMap::new();
        new_properties.insert("k".into(), "v".into());

        catalog
            .update_namespace(&namespace_ident_a_b, new_properties.clone())
            .await
            .unwrap();

        assert_eq!(
            catalog.get_namespace(&namespace_ident_a_b).await.unwrap(),
            Namespace::with_properties(namespace_ident_a_b, new_properties)
        );
    }

    #[tokio::test]
    async fn test_update_deeply_nested_namespace() {
        let catalog = new_inmemory_catalog();
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_a_b_c = NamespaceIdent::from_strs(vec!["a", "b", "c"]).unwrap();
        create_namespaces(
            &catalog,
            &vec![
                &namespace_ident_a,
                &namespace_ident_a_b,
                &namespace_ident_a_b_c,
            ],
        )
        .await;

        let mut new_properties = HashMap::new();
        new_properties.insert("k".into(), "v".into());

        catalog
            .update_namespace(&namespace_ident_a_b_c, new_properties.clone())
            .await
            .unwrap();

        assert_eq!(
            catalog.get_namespace(&namespace_ident_a_b_c).await.unwrap(),
            Namespace::with_properties(namespace_ident_a_b_c, new_properties)
        );
    }

    #[tokio::test]
    async fn test_update_namespace_throws_error_if_namespace_doesnt_exist() {
        let catalog = new_inmemory_catalog();
        create_namespace(&catalog, &NamespaceIdent::new("abc".into())).await;

        let non_existent_namespace_ident = NamespaceIdent::new("def".into());
        assert_eq!(
            catalog
                .update_namespace(&non_existent_namespace_ident, HashMap::new())
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => No such namespace: {:?}",
                non_existent_namespace_ident
            )
        )
    }

    #[tokio::test]
    async fn test_drop_namespace() {
        let catalog = new_inmemory_catalog();
        let namespace_ident = NamespaceIdent::new("abc".into());
        create_namespace(&catalog, &namespace_ident).await;

        catalog.drop_namespace(&namespace_ident).await.unwrap();

        assert!(!catalog.namespace_exists(&namespace_ident).await.unwrap())
    }

    #[tokio::test]
    async fn test_drop_nested_namespace() {
        let catalog = new_inmemory_catalog();
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]).await;

        catalog.drop_namespace(&namespace_ident_a_b).await.unwrap();

        assert!(!catalog
            .namespace_exists(&namespace_ident_a_b)
            .await
            .unwrap());

        assert!(catalog.namespace_exists(&namespace_ident_a).await.unwrap());
    }

    #[tokio::test]
    async fn test_drop_deeply_nested_namespace() {
        let catalog = new_inmemory_catalog();
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_a_b_c = NamespaceIdent::from_strs(vec!["a", "b", "c"]).unwrap();
        create_namespaces(
            &catalog,
            &vec![
                &namespace_ident_a,
                &namespace_ident_a_b,
                &namespace_ident_a_b_c,
            ],
        )
        .await;

        catalog
            .drop_namespace(&namespace_ident_a_b_c)
            .await
            .unwrap();

        assert!(!catalog
            .namespace_exists(&namespace_ident_a_b_c)
            .await
            .unwrap());

        assert!(catalog
            .namespace_exists(&namespace_ident_a_b)
            .await
            .unwrap());

        assert!(catalog.namespace_exists(&namespace_ident_a).await.unwrap());
    }

    #[tokio::test]
    async fn test_drop_namespace_throws_error_if_namespace_doesnt_exist() {
        let catalog = new_inmemory_catalog();

        let non_existent_namespace_ident = NamespaceIdent::new("abc".into());
        assert_eq!(
            catalog
                .drop_namespace(&non_existent_namespace_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => No such namespace: {:?}",
                non_existent_namespace_ident
            )
        )
    }

    #[tokio::test]
    async fn test_drop_namespace_throws_error_if_nested_namespace_doesnt_exist() {
        let catalog = new_inmemory_catalog();
        create_namespace(&catalog, &NamespaceIdent::new("a".into())).await;

        let non_existent_namespace_ident =
            NamespaceIdent::from_vec(vec!["a".into(), "b".into()]).unwrap();
        assert_eq!(
            catalog
                .drop_namespace(&non_existent_namespace_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => No such namespace: {:?}",
                non_existent_namespace_ident
            )
        )
    }

    #[tokio::test]
    async fn test_dropping_a_namespace_also_drops_namespaces_nested_under_that_one() {
        let catalog = new_inmemory_catalog();
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]).await;

        catalog.drop_namespace(&namespace_ident_a).await.unwrap();

        assert!(!catalog.namespace_exists(&namespace_ident_a).await.unwrap());

        assert!(!catalog
            .namespace_exists(&namespace_ident_a_b)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_create_table_without_location() {
        let catalog = new_inmemory_catalog();
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        let table_name = "tbl1";
        let expected_table_ident = TableIdent::new(namespace_ident.clone(), table_name.into());

        assert_table_eq(
            &catalog
                .create_table(
                    &namespace_ident,
                    TableCreation::builder()
                        .name(table_name.into())
                        .schema(simple_table_schema())
                        .build(),
                )
                .await
                .unwrap(),
            &expected_table_ident,
            &simple_table_schema(),
        );

        assert_table_eq(
            &catalog.load_table(&expected_table_ident).await.unwrap(),
            &expected_table_ident,
            &simple_table_schema(),
        )
    }

    #[tokio::test]
    async fn test_create_table_with_location() {
        let tmp_dir = TempDir::new().unwrap();
        let catalog = new_inmemory_catalog();
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        let table_name = "abc";
        let location = tmp_dir.path().to_str().unwrap().to_string();
        let table_creation = TableCreation::builder()
            .name(table_name.into())
            .location(location.clone())
            .schema(simple_table_schema())
            .build();

        let expected_table_ident = TableIdent::new(namespace_ident.clone(), table_name.into());

        assert_table_eq(
            &catalog
                .create_table(&namespace_ident, table_creation)
                .await
                .unwrap(),
            &expected_table_ident,
            &simple_table_schema(),
        );

        let table = catalog.load_table(&expected_table_ident).await.unwrap();

        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());

        assert!(table
            .metadata_location()
            .unwrap()
            .to_string()
            .starts_with(&location));

        assert_table_eq(
            &catalog.load_table(&expected_table_ident).await.unwrap(),
            &expected_table_ident,
            &simple_table_schema(),
        )
    }

    #[tokio::test]
    async fn test_create_table_throws_error_if_table_with_same_name_already_exists() {
        let catalog = new_inmemory_catalog();
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;
        let table_name = "tbl1";
        let table_ident = TableIdent::new(namespace_ident.clone(), table_name.into());
        create_table(&catalog, &table_ident).await;

        assert_eq!(
            catalog
                .create_table(
                    &namespace_ident,
                    TableCreation::builder()
                        .name(table_name.into())
                        .schema(simple_table_schema())
                        .build()
                )
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => Cannot create table {:?}. Table already exists",
                &table_ident
            )
        );
    }

    #[tokio::test]
    async fn test_list_tables_returns_empty_vector() {
        let catalog = new_inmemory_catalog();
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        assert_eq!(catalog.list_tables(&namespace_ident).await.unwrap(), vec![]);
    }

    #[tokio::test]
    async fn test_list_tables_returns_a_single_table() {
        let catalog = new_inmemory_catalog();
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;

        let table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        create_table(&catalog, &table_ident).await;

        assert_eq!(
            catalog.list_tables(&namespace_ident).await.unwrap(),
            vec![table_ident]
        );
    }

    #[tokio::test]
    async fn test_list_tables_returns_multiple_tables() {
        let catalog = new_inmemory_catalog();
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;

        let table_ident_1 = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        let table_ident_2 = TableIdent::new(namespace_ident.clone(), "tbl2".into());
        let _ = create_tables(&catalog, vec![&table_ident_1, &table_ident_2]).await;

        assert_eq!(
            to_set(catalog.list_tables(&namespace_ident).await.unwrap()),
            to_set(vec![table_ident_1, table_ident_2])
        );
    }

    #[tokio::test]
    async fn test_list_tables_returns_tables_from_correct_namespace() {
        let catalog = new_inmemory_catalog();
        let namespace_ident_1 = NamespaceIdent::new("n1".into());
        let namespace_ident_2 = NamespaceIdent::new("n2".into());
        create_namespaces(&catalog, &vec![&namespace_ident_1, &namespace_ident_2]).await;

        let table_ident_1 = TableIdent::new(namespace_ident_1.clone(), "tbl1".into());
        let table_ident_2 = TableIdent::new(namespace_ident_1.clone(), "tbl2".into());
        let table_ident_3 = TableIdent::new(namespace_ident_2.clone(), "tbl1".into());
        let _ = create_tables(
            &catalog,
            vec![&table_ident_1, &table_ident_2, &table_ident_3],
        )
        .await;

        assert_eq!(
            to_set(catalog.list_tables(&namespace_ident_1).await.unwrap()),
            to_set(vec![table_ident_1, table_ident_2])
        );

        assert_eq!(
            to_set(catalog.list_tables(&namespace_ident_2).await.unwrap()),
            to_set(vec![table_ident_3])
        );
    }

    #[tokio::test]
    async fn test_list_tables_returns_table_under_nested_namespace() {
        let catalog = new_inmemory_catalog();
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]).await;

        let table_ident = TableIdent::new(namespace_ident_a_b.clone(), "tbl1".into());
        create_table(&catalog, &table_ident).await;

        assert_eq!(
            catalog.list_tables(&namespace_ident_a_b).await.unwrap(),
            vec![table_ident]
        );
    }

    #[tokio::test]
    async fn test_list_tables_throws_error_if_namespace_doesnt_exist() {
        let catalog = new_inmemory_catalog();

        let non_existent_namespace_ident = NamespaceIdent::new("n1".into());

        assert_eq!(
            catalog
                .list_tables(&non_existent_namespace_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => No such namespace: {:?}",
                non_existent_namespace_ident
            ),
        );
    }

    #[tokio::test]
    async fn test_drop_table() {
        let catalog = new_inmemory_catalog();
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;
        let table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        create_table(&catalog, &table_ident).await;

        catalog.drop_table(&table_ident).await.unwrap();
    }

    #[tokio::test]
    async fn test_drop_table_drops_table_under_nested_namespace() {
        let catalog = new_inmemory_catalog();
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]).await;

        let table_ident = TableIdent::new(namespace_ident_a_b.clone(), "tbl1".into());
        create_table(&catalog, &table_ident).await;

        catalog.drop_table(&table_ident).await.unwrap();

        assert_eq!(
            catalog.list_tables(&namespace_ident_a_b).await.unwrap(),
            vec![]
        );
    }

    #[tokio::test]
    async fn test_drop_table_throws_error_if_namespace_doesnt_exist() {
        let catalog = new_inmemory_catalog();

        let non_existent_namespace_ident = NamespaceIdent::new("n1".into());
        let non_existent_table_ident =
            TableIdent::new(non_existent_namespace_ident.clone(), "tbl1".into());

        assert_eq!(
            catalog
                .drop_table(&non_existent_table_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => No such namespace: {:?}",
                non_existent_namespace_ident
            ),
        );
    }

    #[tokio::test]
    async fn test_drop_table_throws_error_if_table_doesnt_exist() {
        let catalog = new_inmemory_catalog();
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;

        let non_existent_table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());

        assert_eq!(
            catalog
                .drop_table(&non_existent_table_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => No such table: {:?}",
                non_existent_table_ident
            ),
        );
    }

    #[tokio::test]
    async fn test_table_exists_returns_true() {
        let catalog = new_inmemory_catalog();
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;
        let table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        create_table(&catalog, &table_ident).await;

        assert!(catalog.table_exists(&table_ident).await.unwrap());
    }

    #[tokio::test]
    async fn test_table_exists_returns_false() {
        let catalog = new_inmemory_catalog();
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;
        let non_existent_table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());

        assert!(!catalog
            .table_exists(&non_existent_table_ident)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_table_exists_under_nested_namespace() {
        let catalog = new_inmemory_catalog();
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]).await;

        let table_ident = TableIdent::new(namespace_ident_a_b.clone(), "tbl1".into());
        create_table(&catalog, &table_ident).await;

        assert!(catalog.table_exists(&table_ident).await.unwrap());

        let non_existent_table_ident = TableIdent::new(namespace_ident_a_b.clone(), "tbl2".into());
        assert!(!catalog
            .table_exists(&non_existent_table_ident)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_table_exists_throws_error_if_namespace_doesnt_exist() {
        let catalog = new_inmemory_catalog();

        let non_existent_namespace_ident = NamespaceIdent::new("n1".into());
        let non_existent_table_ident =
            TableIdent::new(non_existent_namespace_ident.clone(), "tbl1".into());

        assert_eq!(
            catalog
                .table_exists(&non_existent_table_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => No such namespace: {:?}",
                non_existent_namespace_ident
            ),
        );
    }

    #[tokio::test]
    async fn test_rename_table_in_same_namespace() {
        let catalog = new_inmemory_catalog();
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;
        let src_table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        let dst_table_ident = TableIdent::new(namespace_ident.clone(), "tbl2".into());
        create_table(&catalog, &src_table_ident).await;

        catalog
            .rename_table(&src_table_ident, &dst_table_ident)
            .await
            .unwrap();

        assert_eq!(
            catalog.list_tables(&namespace_ident).await.unwrap(),
            vec![dst_table_ident],
        );
    }

    #[tokio::test]
    async fn test_rename_table_across_namespaces() {
        let catalog = new_inmemory_catalog();
        let src_namespace_ident = NamespaceIdent::new("a".into());
        let dst_namespace_ident = NamespaceIdent::new("b".into());
        create_namespaces(&catalog, &vec![&src_namespace_ident, &dst_namespace_ident]).await;
        let src_table_ident = TableIdent::new(src_namespace_ident.clone(), "tbl1".into());
        let dst_table_ident = TableIdent::new(dst_namespace_ident.clone(), "tbl2".into());
        create_table(&catalog, &src_table_ident).await;

        catalog
            .rename_table(&src_table_ident, &dst_table_ident)
            .await
            .unwrap();

        assert_eq!(
            catalog.list_tables(&src_namespace_ident).await.unwrap(),
            vec![],
        );

        assert_eq!(
            catalog.list_tables(&dst_namespace_ident).await.unwrap(),
            vec![dst_table_ident],
        );
    }

    #[tokio::test]
    async fn test_rename_table_src_table_is_same_as_dst_table() {
        let catalog = new_inmemory_catalog();
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;
        let table_ident = TableIdent::new(namespace_ident.clone(), "tbl".into());
        create_table(&catalog, &table_ident).await;

        catalog
            .rename_table(&table_ident, &table_ident)
            .await
            .unwrap();

        assert_eq!(
            catalog.list_tables(&namespace_ident).await.unwrap(),
            vec![table_ident],
        );
    }

    #[tokio::test]
    async fn test_rename_table_across_nested_namespaces() {
        let catalog = new_inmemory_catalog();
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_a_b_c = NamespaceIdent::from_strs(vec!["a", "b", "c"]).unwrap();
        create_namespaces(
            &catalog,
            &vec![
                &namespace_ident_a,
                &namespace_ident_a_b,
                &namespace_ident_a_b_c,
            ],
        )
        .await;

        let src_table_ident = TableIdent::new(namespace_ident_a_b_c.clone(), "tbl1".into());
        create_tables(&catalog, vec![&src_table_ident]).await;

        let dst_table_ident = TableIdent::new(namespace_ident_a_b.clone(), "tbl1".into());
        catalog
            .rename_table(&src_table_ident, &dst_table_ident)
            .await
            .unwrap();

        assert!(!catalog.table_exists(&src_table_ident).await.unwrap());

        assert!(catalog.table_exists(&dst_table_ident).await.unwrap());
    }

    #[tokio::test]
    async fn test_rename_table_throws_error_if_src_namespace_doesnt_exist() {
        let catalog = new_inmemory_catalog();

        let non_existent_src_namespace_ident = NamespaceIdent::new("n1".into());
        let src_table_ident =
            TableIdent::new(non_existent_src_namespace_ident.clone(), "tbl1".into());

        let dst_namespace_ident = NamespaceIdent::new("n2".into());
        create_namespace(&catalog, &dst_namespace_ident).await;
        let dst_table_ident = TableIdent::new(dst_namespace_ident.clone(), "tbl1".into());

        assert_eq!(
            catalog
                .rename_table(&src_table_ident, &dst_table_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => No such namespace: {:?}",
                non_existent_src_namespace_ident
            ),
        );
    }

    #[tokio::test]
    async fn test_rename_table_throws_error_if_dst_namespace_doesnt_exist() {
        let catalog = new_inmemory_catalog();
        let src_namespace_ident = NamespaceIdent::new("n1".into());
        let src_table_ident = TableIdent::new(src_namespace_ident.clone(), "tbl1".into());
        create_namespace(&catalog, &src_namespace_ident).await;
        create_table(&catalog, &src_table_ident).await;

        let non_existent_dst_namespace_ident = NamespaceIdent::new("n2".into());
        let dst_table_ident =
            TableIdent::new(non_existent_dst_namespace_ident.clone(), "tbl1".into());
        assert_eq!(
            catalog
                .rename_table(&src_table_ident, &dst_table_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => No such namespace: {:?}",
                non_existent_dst_namespace_ident
            ),
        );
    }

    #[tokio::test]
    async fn test_rename_table_throws_error_if_src_table_doesnt_exist() {
        let catalog = new_inmemory_catalog();
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;
        let src_table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        let dst_table_ident = TableIdent::new(namespace_ident.clone(), "tbl2".into());

        assert_eq!(
            catalog
                .rename_table(&src_table_ident, &dst_table_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!("Unexpected => No such table: {:?}", src_table_ident),
        );
    }

    #[tokio::test]
    async fn test_rename_table_throws_error_if_dst_table_already_exists() {
        let catalog = new_inmemory_catalog();
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;
        let src_table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        let dst_table_ident = TableIdent::new(namespace_ident.clone(), "tbl2".into());
        create_tables(&catalog, vec![&src_table_ident, &dst_table_ident]).await;

        assert_eq!(
            catalog
                .rename_table(&src_table_ident, &dst_table_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => Cannot insert table {:? } as it already exists.",
                &dst_table_ident
            ),
        );
    }
}
