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

//! This module contains transaction api.

use crate::error::Result;
use crate::spec::{FormatVersion, NullOrder, SortDirection, SortField, SortOrder, Transform};
use crate::table::Table;
use crate::TableUpdate::UpgradeFormatVersion;
use crate::{Catalog, Error, ErrorKind, TableCommit, TableRequirement, TableUpdate};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::mem::discriminant;

/// Table transaction.
pub struct Transaction<'a> {
    table: &'a Table,
    updates: Vec<TableUpdate>,
    requirements: Vec<TableRequirement>,
}

impl<'a> Transaction<'a> {
    /// Creates a new transaction.
    pub fn new(table: &'a Table) -> Self {
        Self {
            table,
            updates: vec![],
            requirements: vec![],
        }
    }

    fn append_updates(&mut self, updates: Vec<TableUpdate>) -> Result<()> {
        for update in &updates {
            for up in &self.updates {
                if discriminant(up) == discriminant(update) {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Cannot apply update with same type at same time: {:?}",
                            update
                        ),
                    ));
                }
            }
        }
        self.updates.extend(updates);
        Ok(())
    }

    fn append_requirements(&mut self, requirements: Vec<TableRequirement>) -> Result<()> {
        self.requirements.extend(requirements);
        Ok(())
    }

    /// Sets table to a new version.
    pub fn upgrade_table_version(mut self, format_version: FormatVersion) -> Result<Self> {
        let current_version = self.table.metadata().format_version();
        match current_version.cmp(&format_version) {
            Ordering::Greater => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot downgrade table version from {} to {}",
                        current_version, format_version
                    ),
                ));
            }
            Ordering::Less => {
                self.append_updates(vec![UpgradeFormatVersion { format_version }])?;
            }
            Ordering::Equal => {
                // Do nothing.
            }
        }
        Ok(self)
    }

    /// Update table's property.
    pub fn set_properties(mut self, props: HashMap<String, String>) -> Result<Self> {
        self.append_updates(vec![TableUpdate::SetProperties { updates: props }])?;
        Ok(self)
    }

    /// Creates replace sort order action.
    pub fn replace_sort_order(self) -> ReplaceSortOrderAction<'a> {
        ReplaceSortOrderAction {
            tx: self,
            sort_fields: vec![],
        }
    }

    /// Remove properties in table.
    pub fn remove_properties(mut self, keys: Vec<String>) -> Result<Self> {
        self.append_updates(vec![TableUpdate::RemoveProperties { removals: keys }])?;
        Ok(self)
    }

    /// Commit transaction.
    pub async fn commit(self, catalog: &impl Catalog) -> Result<Table> {
        let table_commit = TableCommit::builder()
            .ident(self.table.identifier().clone())
            .updates(self.updates)
            .requirements(self.requirements)
            .build();

        catalog.update_table(table_commit).await
    }
}

/// Transaction action for replacing sort order.
pub struct ReplaceSortOrderAction<'a> {
    tx: Transaction<'a>,
    sort_fields: Vec<SortField>,
}

impl<'a> ReplaceSortOrderAction<'a> {
    /// Adds a field for sorting in ascending order.
    pub fn asc(self, name: &str, null_order: NullOrder) -> Result<Self> {
        self.add_sort_field(name, SortDirection::Ascending, null_order)
    }

    /// Adds a field for sorting in descending order.
    pub fn desc(self, name: &str, null_order: NullOrder) -> Result<Self> {
        self.add_sort_field(name, SortDirection::Descending, null_order)
    }

    /// Finished building the action and apply it to the transaction.
    pub fn apply(mut self) -> Result<Transaction<'a>> {
        let unbound_sort_order = SortOrder::builder()
            .with_fields(self.sort_fields)
            .build_unbound()?;

        let updates = vec![
            TableUpdate::AddSortOrder {
                sort_order: unbound_sort_order,
            },
            TableUpdate::SetDefaultSortOrder { sort_order_id: -1 },
        ];

        let requirements = vec![
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: self.tx.table.metadata().current_schema().schema_id() as i64,
            },
            TableRequirement::DefaultSortOrderIdMatch {
                default_sort_order_id: self
                    .tx
                    .table
                    .metadata()
                    .default_sort_order()
                    .ok_or(Error::new(
                        ErrorKind::Unexpected,
                        "default sort order impossible to be none",
                    ))?
                    .order_id,
            },
        ];

        self.tx.append_requirements(requirements)?;
        self.tx.append_updates(updates)?;
        Ok(self.tx)
    }

    fn add_sort_field(
        mut self,
        name: &str,
        sort_direction: SortDirection,
        null_order: NullOrder,
    ) -> Result<Self> {
        let field_id = self
            .tx
            .table
            .metadata()
            .current_schema()
            .field_id_by_name(name)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Cannot find field {} in table schema", name),
                )
            })?;

        let sort_field = SortField::builder()
            .source_id(field_id)
            .transform(Transform::Identity)
            .direction(sort_direction)
            .null_order(null_order)
            .build();

        self.sort_fields.push(sort_field);
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use crate::io::FileIO;
    use crate::spec::{FormatVersion, TableMetadata};
    use crate::table::Table;
    use crate::transaction::Transaction;
    use crate::{TableIdent, TableRequirement, TableUpdate};
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::BufReader;

    fn make_v1_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV1Valid.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIO::from_path("/tmp").unwrap().build().unwrap())
            .build()
            .unwrap()
    }

    fn make_v2_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2Valid.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIO::from_path("/tmp").unwrap().build().unwrap())
            .build()
            .unwrap()
    }

    #[test]
    fn test_upgrade_table_version_v1_to_v2() {
        let table = make_v1_table();
        let tx = Transaction::new(&table);
        let tx = tx.upgrade_table_version(FormatVersion::V2).unwrap();

        assert_eq!(
            vec![TableUpdate::UpgradeFormatVersion {
                format_version: FormatVersion::V2
            }],
            tx.updates
        );
    }

    #[test]
    fn test_upgrade_table_version_v2_to_v2() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let tx = tx.upgrade_table_version(FormatVersion::V2).unwrap();

        assert!(
            tx.updates.is_empty(),
            "Upgrade table to same version should not generate any updates"
        );
        assert!(
            tx.requirements.is_empty(),
            "Upgrade table to same version should not generate any requirements"
        );
    }

    #[test]
    fn test_downgrade_table_version() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let tx = tx.upgrade_table_version(FormatVersion::V1);

        assert!(tx.is_err(), "Downgrade table version should fail!");
    }

    #[test]
    fn test_set_table_property() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let tx = tx
            .set_properties(HashMap::from([("a".to_string(), "b".to_string())]))
            .unwrap();

        assert_eq!(
            vec![TableUpdate::SetProperties {
                updates: HashMap::from([("a".to_string(), "b".to_string())])
            }],
            tx.updates
        );
    }

    #[test]
    fn test_remove_property() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let tx = tx
            .remove_properties(vec!["a".to_string(), "b".to_string()])
            .unwrap();

        assert_eq!(
            vec![TableUpdate::RemoveProperties {
                removals: vec!["a".to_string(), "b".to_string()]
            }],
            tx.updates
        );
    }

    #[test]
    fn test_replace_sort_order() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let tx = tx.replace_sort_order().apply().unwrap();

        assert_eq!(
            vec![
                TableUpdate::AddSortOrder {
                    sort_order: Default::default()
                },
                TableUpdate::SetDefaultSortOrder { sort_order_id: -1 }
            ],
            tx.updates
        );

        assert_eq!(
            vec![
                TableRequirement::CurrentSchemaIdMatch {
                    current_schema_id: 1
                },
                TableRequirement::DefaultSortOrderIdMatch {
                    default_sort_order_id: 3
                }
            ],
            tx.requirements
        );
    }

    #[test]
    fn test_do_same_update_in_same_transaction() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let tx = tx
            .remove_properties(vec!["a".to_string(), "b".to_string()])
            .unwrap();

        let tx = tx.remove_properties(vec!["c".to_string(), "d".to_string()]);

        assert!(
            tx.is_err(),
            "Should not allow to do same kinds update in same transaction"
        );
    }
}
