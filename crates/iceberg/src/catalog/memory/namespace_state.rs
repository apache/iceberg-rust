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

use std::collections::{HashMap, hash_map};

use itertools::Itertools;

use crate::table::Table;
use crate::{Error, ErrorKind, NamespaceIdent, Result, TableIdent};

// Represents the state of a namespace
#[derive(Debug, Clone, Default)]
pub(crate) struct NamespaceState {
    // Properties of this namespace
    properties: HashMap<String, String>,
    // Namespaces nested inside this namespace
    namespaces: HashMap<String, NamespaceState>,
    // Mapping of tables to metadata locations in this namespace
    table_metadata_locations: HashMap<String, String>,
    // Mapping of views to metadata locations in this namespace
    view_metadata_locations: HashMap<String, String>,
}

fn no_such_namespace_err<T>(namespace_ident: &NamespaceIdent) -> Result<T> {
    Err(Error::new(
        ErrorKind::NamespaceNotFound,
        format!("No such namespace: {namespace_ident:?}"),
    ))
}

fn no_such_table_err<T>(table_ident: &TableIdent) -> Result<T> {
    Err(Error::new(
        ErrorKind::TableNotFound,
        format!("No such table: {table_ident:?}"),
    ))
}

fn namespace_already_exists_err<T>(namespace_ident: &NamespaceIdent) -> Result<T> {
    Err(Error::new(
        ErrorKind::NamespaceAlreadyExists,
        format!("Cannot create namespace {namespace_ident:?}. Namespace already exists."),
    ))
}

fn table_already_exists_err<T>(table_ident: &TableIdent) -> Result<T> {
    Err(Error::new(
        ErrorKind::TableAlreadyExists,
        format!("Cannot create table {table_ident:?}. Table already exists."),
    ))
}

fn no_such_view_err<T>(view_ident: &TableIdent) -> Result<T> {
    Err(Error::new(
        ErrorKind::ViewNotFound,
        format!("No such view: {view_ident:?}"),
    ))
}

fn view_already_exists_err<T>(view_ident: &TableIdent) -> Result<T> {
    Err(Error::new(
        ErrorKind::ViewAlreadyExists,
        format!("Cannot create view {view_ident:?}. View already exists."),
    ))
}

// A table already occupies the name a view is trying to take. Mirrors Java
// `InMemoryViewOperations.doCommit` / `BaseViewBuilder.replace`, which throw
// `AlreadyExistsException("Table with same name already exists")` — tables and views share one
// name space in a catalog, so a view cannot shadow a table of the same name.
fn table_with_same_name_err<T>(view_ident: &TableIdent) -> Result<T> {
    Err(Error::new(
        ErrorKind::TableAlreadyExists,
        format!("Cannot create view {view_ident:?}. Table with same name already exists."),
    ))
}

// A view already occupies the name a table is trying to take. Mirrors Java
// `BaseMetastoreViewCatalog`'s table builder, which throws
// `AlreadyExistsException("View with same name already exists")`.
fn view_with_same_name_err<T>(table_ident: &TableIdent) -> Result<T> {
    Err(Error::new(
        ErrorKind::ViewAlreadyExists,
        format!("Cannot create table {table_ident:?}. View with same name already exists."),
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
                    view_metadata_locations: HashMap::new(),
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
    pub(crate) fn get_existing_table_location(&self, table_ident: &TableIdent) -> Result<&String> {
        let namespace = self.get_namespace(table_ident.namespace())?;

        match namespace.table_metadata_locations.get(table_ident.name()) {
            None => no_such_table_err(table_ident),
            Some(table_metadata_location) => Ok(table_metadata_location),
        }
    }

    // Inserts the given table or returns an error if it already exists
    pub(crate) fn insert_new_table(
        &mut self,
        table_ident: &TableIdent,
        metadata_location: String,
    ) -> Result<()> {
        let namespace = self.get_mut_namespace(table_ident.namespace())?;

        // Tables and views share one name space: a table cannot shadow a view of the same name.
        if namespace
            .view_metadata_locations
            .contains_key(table_ident.name())
        {
            return view_with_same_name_err(table_ident);
        }

        match namespace
            .table_metadata_locations
            .entry(table_ident.name().to_string())
        {
            hash_map::Entry::Occupied(_) => table_already_exists_err(table_ident),
            hash_map::Entry::Vacant(entry) => {
                let _ = entry.insert(metadata_location);

                Ok(())
            }
        }
    }

    // Removes the given table or returns an error if doesn't exist
    pub(crate) fn remove_existing_table(&mut self, table_ident: &TableIdent) -> Result<String> {
        let namespace = self.get_mut_namespace(table_ident.namespace())?;

        match namespace
            .table_metadata_locations
            .remove(table_ident.name())
        {
            None => no_such_table_err(table_ident),
            Some(metadata_location) => Ok(metadata_location),
        }
    }

    /// Updates the metadata location of the given table or returns an error if it doesn't exist
    pub(crate) fn commit_table_update(&mut self, staged_table: Table) -> Result<Table> {
        let namespace = self.get_mut_namespace(staged_table.identifier().namespace())?;

        let _ = namespace
            .table_metadata_locations
            .insert(
                staged_table.identifier().name().to_string(),
                staged_table.metadata_location_result()?.to_string(),
            )
            .ok_or(Error::new(
                ErrorKind::TableNotFound,
                format!("No such table: {:?}", staged_table.identifier()),
            ))?;

        Ok(staged_table)
    }

    // Returns the list of view names under the given namespace
    pub(crate) fn list_views(&self, namespace_ident: &NamespaceIdent) -> Result<Vec<&String>> {
        let view_names = self
            .get_namespace(namespace_ident)?
            .view_metadata_locations
            .keys()
            .collect_vec();

        Ok(view_names)
    }

    // Returns true if the given view exists, otherwise false
    pub(crate) fn view_exists(&self, view_ident: &TableIdent) -> Result<bool> {
        let namespace_state = self.get_namespace(view_ident.namespace())?;
        let view_exists = namespace_state
            .view_metadata_locations
            .contains_key(&view_ident.name);

        Ok(view_exists)
    }

    // Returns the metadata location of the given view or an error if doesn't exist
    pub(crate) fn get_existing_view_location(&self, view_ident: &TableIdent) -> Result<&String> {
        let namespace = self.get_namespace(view_ident.namespace())?;

        match namespace.view_metadata_locations.get(view_ident.name()) {
            None => no_such_view_err(view_ident),
            Some(view_metadata_location) => Ok(view_metadata_location),
        }
    }

    // Inserts the given view or returns an error if it already exists
    pub(crate) fn insert_new_view(
        &mut self,
        view_ident: &TableIdent,
        metadata_location: String,
    ) -> Result<()> {
        let namespace = self.get_mut_namespace(view_ident.namespace())?;

        // Tables and views share one name space: a view cannot shadow a table of the same name.
        if namespace
            .table_metadata_locations
            .contains_key(view_ident.name())
        {
            return table_with_same_name_err(view_ident);
        }

        match namespace
            .view_metadata_locations
            .entry(view_ident.name().to_string())
        {
            hash_map::Entry::Occupied(_) => view_already_exists_err(view_ident),
            hash_map::Entry::Vacant(entry) => {
                let _ = entry.insert(metadata_location);

                Ok(())
            }
        }
    }

    // Removes the given view or returns an error if doesn't exist
    pub(crate) fn remove_existing_view(&mut self, view_ident: &TableIdent) -> Result<String> {
        let namespace = self.get_mut_namespace(view_ident.namespace())?;

        match namespace.view_metadata_locations.remove(view_ident.name()) {
            None => no_such_view_err(view_ident),
            Some(metadata_location) => Ok(metadata_location),
        }
    }

    /// Updates the metadata location of the given view or returns an error if it doesn't exist
    pub(crate) fn commit_view_update(
        &mut self,
        view_ident: &TableIdent,
        metadata_location: String,
    ) -> Result<()> {
        let namespace = self.get_mut_namespace(view_ident.namespace())?;

        let _ = namespace
            .view_metadata_locations
            .insert(view_ident.name().to_string(), metadata_location)
            .ok_or(Error::new(
                ErrorKind::ViewNotFound,
                format!("No such view: {view_ident:?}"),
            ))?;

        Ok(())
    }
}
