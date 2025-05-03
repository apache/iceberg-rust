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

use std::collections::{hash_map, HashMap};
use std::fmt::Display;
use std::str::FromStr;

use iceberg::{Error, ErrorKind, NamespaceIdent, Result, TableIdent};
use itertools::Itertools;
use uuid::Uuid;

// Represents the state of a namespace
#[derive(Debug, Clone, Default)]
pub(crate) struct NamespaceState {
    // Properties of this namespace
    properties: HashMap<String, String>,
    // Namespaces nested inside this namespace
    namespaces: HashMap<String, NamespaceState>,
    // Mapping of tables to metadata locations in this namespace
    table_metadata_locations: HashMap<String, MetadataLocation>,
}

fn no_such_namespace_err<T>(namespace_ident: &NamespaceIdent) -> Result<T> {
    Err(Error::new(
        ErrorKind::NamespaceNotFound,
        format!("No such namespace: {:?}", namespace_ident),
    ))
}

fn no_such_table_err<T>(table_ident: &TableIdent) -> Result<T> {
    Err(Error::new(
        ErrorKind::TableNotFound,
        format!("No such table: {:?}", table_ident),
    ))
}

fn namespace_already_exists_err<T>(namespace_ident: &NamespaceIdent) -> Result<T> {
    Err(Error::new(
        ErrorKind::NamespaceAlreadyExists,
        format!(
            "Cannot create namespace {:?}. Namespace already exists.",
            namespace_ident
        ),
    ))
}

fn table_already_exists_err<T>(table_ident: &TableIdent) -> Result<T> {
    Err(Error::new(
        ErrorKind::TableAlreadyExists,
        format!(
            "Cannot create table {:?}. Table already exists.",
            table_ident
        ),
    ))
}

impl NamespaceState {
    // Returns the state of the given namespace or an error if doesn't exist
    fn get_namespace(&self, namespace_ident: &NamespaceIdent) -> Result<&NamespaceState> {
        let mut acc_name_parts = vec![];
        let mut namespace_state = self;
        for next_name in namespace_ident.iter() {
            acc_name_parts.push(next_name);
            match namespace_state.namespaces.get(next_name) {
                None => {
                    let namespace_ident = NamespaceIdent::from_strs(acc_name_parts)?;
                    return no_such_namespace_err(&namespace_ident);
                }
                Some(intermediate_namespace) => {
                    namespace_state = intermediate_namespace;
                }
            }
        }

        Ok(namespace_state)
    }

    // Returns the state of the given namespace or an error if doesn't exist
    fn get_mut_namespace(
        &mut self,
        namespace_ident: &NamespaceIdent,
    ) -> Result<&mut NamespaceState> {
        let mut acc_name_parts = vec![];
        let mut namespace_state = self;
        for next_name in namespace_ident.iter() {
            acc_name_parts.push(next_name);
            match namespace_state.namespaces.get_mut(next_name) {
                None => {
                    let namespace_ident = NamespaceIdent::from_strs(acc_name_parts)?;
                    return no_such_namespace_err(&namespace_ident);
                }
                Some(intermediate_namespace) => {
                    namespace_state = intermediate_namespace;
                }
            }
        }

        Ok(namespace_state)
    }

    // Returns the state of the parent of the given namespace or an error if doesn't exist
    fn get_mut_parent_namespace_of(
        &mut self,
        namespace_ident: &NamespaceIdent,
    ) -> Result<(&mut NamespaceState, String)> {
        match namespace_ident.split_last() {
            None => Err(Error::new(
                ErrorKind::DataInvalid,
                "Namespace identifier can't be empty!",
            )),
            Some((child_namespace_name, parent_name_parts)) => {
                let parent_namespace_state = if parent_name_parts.is_empty() {
                    Ok(self)
                } else {
                    let parent_namespace_ident = NamespaceIdent::from_strs(parent_name_parts)?;
                    self.get_mut_namespace(&parent_namespace_ident)
                }?;

                Ok((parent_namespace_state, child_namespace_name.clone()))
            }
        }
    }

    // Returns any top-level namespaces
    pub(crate) fn list_top_level_namespaces(&self) -> Vec<&String> {
        self.namespaces.keys().collect_vec()
    }

    // Returns any namespaces nested under the given namespace or an error if the given namespace does not exist
    pub(crate) fn list_namespaces_under(
        &self,
        namespace_ident: &NamespaceIdent,
    ) -> Result<Vec<&String>> {
        let nested_namespace_names = self
            .get_namespace(namespace_ident)?
            .namespaces
            .keys()
            .collect_vec();

        Ok(nested_namespace_names)
    }

    // Returns true if the given namespace exists, otherwise false
    pub(crate) fn namespace_exists(&self, namespace_ident: &NamespaceIdent) -> bool {
        self.get_namespace(namespace_ident).is_ok()
    }

    // Inserts the given namespace or returns an error if it already exists
    pub(crate) fn insert_new_namespace(
        &mut self,
        namespace_ident: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        let (parent_namespace_state, child_namespace_name) =
            self.get_mut_parent_namespace_of(namespace_ident)?;

        match parent_namespace_state
            .namespaces
            .entry(child_namespace_name)
        {
            hash_map::Entry::Occupied(_) => namespace_already_exists_err(namespace_ident),
            hash_map::Entry::Vacant(entry) => {
                let _ = entry.insert(NamespaceState {
                    properties,
                    namespaces: HashMap::new(),
                    table_metadata_locations: HashMap::new(),
                });

                Ok(())
            }
        }
    }

    // Removes the given namespace or returns an error if doesn't exist
    pub(crate) fn remove_existing_namespace(
        &mut self,
        namespace_ident: &NamespaceIdent,
    ) -> Result<()> {
        let (parent_namespace_state, child_namespace_name) =
            self.get_mut_parent_namespace_of(namespace_ident)?;

        match parent_namespace_state
            .namespaces
            .remove(&child_namespace_name)
        {
            None => no_such_namespace_err(namespace_ident),
            Some(_) => Ok(()),
        }
    }

    // Returns the properties of the given namespace or an error if doesn't exist
    pub(crate) fn get_properties(
        &self,
        namespace_ident: &NamespaceIdent,
    ) -> Result<&HashMap<String, String>> {
        let properties = &self.get_namespace(namespace_ident)?.properties;

        Ok(properties)
    }

    // Returns the properties of this namespace or an error if doesn't exist
    fn get_mut_properties(
        &mut self,
        namespace_ident: &NamespaceIdent,
    ) -> Result<&mut HashMap<String, String>> {
        let properties = &mut self.get_mut_namespace(namespace_ident)?.properties;

        Ok(properties)
    }

    // Replaces the properties of the given namespace or an error if doesn't exist
    pub(crate) fn replace_properties(
        &mut self,
        namespace_ident: &NamespaceIdent,
        new_properties: HashMap<String, String>,
    ) -> Result<()> {
        let properties = self.get_mut_properties(namespace_ident)?;
        *properties = new_properties;

        Ok(())
    }

    // Returns the list of table names under the given namespace
    pub(crate) fn list_tables(&self, namespace_ident: &NamespaceIdent) -> Result<Vec<&String>> {
        let table_names = self
            .get_namespace(namespace_ident)?
            .table_metadata_locations
            .keys()
            .collect_vec();

        Ok(table_names)
    }

    // Returns true if the given table exists, otherwise false
    pub(crate) fn table_exists(&self, table_ident: &TableIdent) -> Result<bool> {
        let namespace_state = self.get_namespace(table_ident.namespace())?;
        let table_exists = namespace_state
            .table_metadata_locations
            .contains_key(&table_ident.name);

        Ok(table_exists)
    }

    // Returns the metadata location of the given table or an error if doesn't exist
    pub(crate) fn get_existing_table_location(
        &self,
        table_ident: &TableIdent,
    ) -> Result<&MetadataLocation> {
        let namespace = self.get_namespace(table_ident.namespace())?;

        match namespace.table_metadata_locations.get(table_ident.name()) {
            None => no_such_table_err(table_ident),
            Some(table_metadadata_location) => Ok(table_metadadata_location),
        }
    }

    // Inserts the given table or returns an error if it already exists
    pub(crate) fn insert_new_table(
        &mut self,
        table_ident: &TableIdent,
        location: MetadataLocation,
    ) -> Result<()> {
        let namespace = self.get_mut_namespace(table_ident.namespace())?;

        match namespace
            .table_metadata_locations
            .entry(table_ident.name().to_string())
        {
            hash_map::Entry::Occupied(_) => table_already_exists_err(table_ident),
            hash_map::Entry::Vacant(entry) => {
                let _ = entry.insert(location);

                Ok(())
            }
        }
    }

    // Removes the given table or returns an error if doesn't exist
    pub(crate) fn remove_existing_table(
        &mut self,
        table_ident: &TableIdent,
    ) -> Result<MetadataLocation> {
        let namespace = self.get_mut_namespace(table_ident.namespace())?;

        match namespace
            .table_metadata_locations
            .remove(table_ident.name())
        {
            None => no_such_table_err(table_ident),
            Some(metadata_location) => Ok(metadata_location),
        }
    }

    /// Updates the metadata location of the given table or returns an error if doesn't exist
    pub(crate) fn update_table(
        &mut self,
        table_ident: &TableIdent,
        new_location: MetadataLocation,
    ) -> Result<()> {
        let namespace = self.get_mut_namespace(table_ident.namespace())?;

        let _ = namespace
            .table_metadata_locations
            .insert(table_ident.name().to_string(), new_location)
            .ok_or(Error::new(
                ErrorKind::Unexpected,
                format!("No such table: {:?}", table_ident),
            ))?;

        Ok(())
    }
}

/// Represents a location of the format: `<prefix>/metadata/<version>-<uuid>.metadata.json`
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MetadataLocation {
    prefix: String,
    version: i32,
    id: Uuid,
}

impl MetadataLocation {
    /// Creates a completely new metadata location starting at version 0.
    /// Only used for creating a new table. For updates, see `with_next_version`.
    pub(crate) fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
            version: 0,
            id: Uuid::new_v4(),
        }
    }

    /// Creates a new metadata location for an updated metadata file.
    pub(crate) fn with_next_version(&self) -> Self {
        Self {
            prefix: self.prefix.clone(),
            version: self.version + 1,
            id: Uuid::new_v4(),
        }
    }

    fn parse_metadata_path_prefix(path: &str) -> Result<String> {
        let prefix = path.strip_suffix("/metadata").ok_or(Error::new(
            ErrorKind::Unexpected,
            format!(
                "Metadata location not under \"/metadata\" subdirectory: {}",
                path
            ),
        ))?;

        Ok(prefix.to_string())
    }

    /// Parses a file name of the format `<version>-<uuid>.metadata.json`.
    fn parse_file_name(file_name: &str) -> Result<(i32, Uuid)> {
        let (version, id) = file_name
            .strip_suffix(".metadata.json")
            .ok_or(Error::new(
                ErrorKind::Unexpected,
                format!("Invalid metadata file ending: {}", file_name),
            ))?
            .split_once('-')
            .ok_or(Error::new(
                ErrorKind::Unexpected,
                format!("Invalid metadata file name format: {}", file_name),
            ))?;

        Ok((version.parse::<i32>()?, Uuid::parse_str(id)?))
    }
}

impl Display for MetadataLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/metadata/{}-{}.metadata.json",
            self.prefix, self.version, self.id
        )
    }
}

impl FromStr for MetadataLocation {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let (path, file_name) = s.rsplit_once('/').ok_or(Error::new(
            ErrorKind::Unexpected,
            format!("Invalid metadata location: {}", s),
        ))?;

        let prefix = Self::parse_metadata_path_prefix(path)?;
        let (version, id) = Self::parse_file_name(file_name)?;

        Ok(MetadataLocation {
            prefix,
            version,
            id,
        })
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use uuid::Uuid;

    use super::MetadataLocation;

    #[test]
    fn test_metadata_location_from_string() {
        let test_cases = vec![
            // No prefix
            ("/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json", Ok(MetadataLocation{
                prefix: "".to_string(),
                version: 1234567,
                id: Uuid::from_str("2cd22b57-5127-4198-92ba-e4e67c79821b").unwrap(),
            })),
            // Some prefix
            ("/abc/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json", Ok(MetadataLocation{
                prefix: "/abc".to_string(),
                version: 1234567,
                id: Uuid::from_str("2cd22b57-5127-4198-92ba-e4e67c79821b").unwrap(),
            })),
            // Longer prefix
            ("/abc/def/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json", Ok(MetadataLocation{
                prefix: "/abc/def".to_string(),
                version: 1234567,
                id: Uuid::from_str("2cd22b57-5127-4198-92ba-e4e67c79821b").unwrap(),
            })),
            // Prefix with special characters
            ("https://127.0.0.1/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json", Ok(MetadataLocation{
                prefix: "https://127.0.0.1".to_string(),
                version: 1234567,
                id: Uuid::from_str("2cd22b57-5127-4198-92ba-e4e67c79821b").unwrap(),
            })),
            // Another id
            ("/abc/metadata/1234567-81056704-ce5b-41c4-bb83-eb6408081af6.metadata.json", Ok(MetadataLocation{
                prefix: "/abc".to_string(),
                version: 1234567,
                id: Uuid::from_str("81056704-ce5b-41c4-bb83-eb6408081af6").unwrap(),
            })),
            // Version 0
            ("/abc/metadata/0-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json", Ok(MetadataLocation{
                prefix: "/abc".to_string(),
                version: 0,
                id: Uuid::from_str("2cd22b57-5127-4198-92ba-e4e67c79821b").unwrap(),
            })),
            // Negative version
            ("/metadata/-123-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json", Err("".to_string())),
            // Invalid uuid
            ("/metadata/1234567-no-valid-id.metadata.json", Err("".to_string())),
            // Non-numeric version
            ("/metadata/noversion-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json", Err("".to_string())),
            // No /metadata subdirectory
            ("/wrongsubdir/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json", Err("".to_string())),
            // No .metadata.json suffix
            ("/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata", Err("".to_string())),
            ("/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.wrong.file", Err("".to_string())),
        ];

        for (input, expected) in test_cases {
            match MetadataLocation::from_str(input) {
                Ok(metadata_location) => {
                    assert!(expected.is_ok());
                    assert_eq!(metadata_location, expected.unwrap());
                }
                Err(_) => assert!(expected.is_err()),
            }
        }
    }

    #[test]
    fn test_metadata_location_with_next_version() {
        let test_cases = vec![
            MetadataLocation::new("/abc"),
            MetadataLocation::from_str(
                "/abc/def/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
            )
            .unwrap(),
        ];

        for input in test_cases {
            let next = input.with_next_version();
            assert_eq!(next.prefix, input.prefix);
            assert_eq!(next.version, input.version + 1);
            assert_ne!(next.id, input.id);
        }
    }
}
