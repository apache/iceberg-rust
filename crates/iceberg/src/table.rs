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

//! Table API for Apache Iceberg
use crate::arrow::ArrowReaderBuilder;
use crate::io::FileIO;
use crate::scan::TableScanBuilder;
use crate::spec::{TableMetadata, TableMetadataRef};
use crate::Result;
use crate::TableIdent;
use typed_builder::TypedBuilder;

/// Table represents a table in the catalog.
#[derive(TypedBuilder, Debug, Clone)]
pub struct Table {
    file_io: FileIO,
    #[builder(default, setter(strip_option, into))]
    metadata_location: Option<String>,
    #[builder(setter(into))]
    metadata: TableMetadataRef,
    identifier: TableIdent,
    #[builder(default = false)]
    readonly: bool,
}

impl Table {
    /// Returns table identifier.
    pub fn identifier(&self) -> &TableIdent {
        &self.identifier
    }
    /// Returns current metadata.
    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    /// Returns current metadata ref.
    pub fn metadata_ref(&self) -> TableMetadataRef {
        self.metadata.clone()
    }

    /// Returns current metadata location.
    pub fn metadata_location(&self) -> Option<&str> {
        self.metadata_location.as_deref()
    }

    /// Returns file io used in this table.
    pub fn file_io(&self) -> &FileIO {
        &self.file_io
    }

    /// Creates a table scan.
    pub fn scan(&self) -> TableScanBuilder<'_> {
        TableScanBuilder::new(self)
    }

    /// Returns the flag indicating whether the `Table` is readonly or not
    pub fn readonly(&self) -> bool {
        self.readonly
    }

    /// Create a reader for the table.
    pub fn reader_builder(&self) -> ArrowReaderBuilder {
        ArrowReaderBuilder::new(self.file_io.clone())
    }
}

/// `StaticTable` is a read-only table struct that can be created from a metadata file or from `TableMetaData` without a catalog.
/// It can only be used to read metadata and for table scan.
/// # Examples
///
/// ```rust, no_run
/// # use iceberg::io::FileIO;
/// # use iceberg::table::StaticTable;
/// # use iceberg::TableIdent;
/// # async fn example() {
/// let metadata_file_location = "s3://bucket_name/path/to/metadata.json";
/// let file_io = FileIO::from_path(&metadata_file_location).unwrap().build().unwrap();
/// let static_identifier = TableIdent::from_strs(["static_ns", "static_table"]).unwrap();
/// let static_table = StaticTable::from_metadata_file(&metadata_file_location, static_identifier, file_io).await.unwrap();
/// let snapshot_id = static_table
/// .metadata()
/// .current_snapshot()
/// .unwrap()
/// .snapshot_id();
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct StaticTable(Table);

impl StaticTable {
    /// Creates a static table from a given `TableMetadata` and `FileIO`
    pub async fn from_metadata(
        metadata: TableMetadata,
        table_ident: TableIdent,
        file_io: FileIO,
    ) -> Result<Self> {
        let table = Table::builder()
            .metadata(metadata)
            .identifier(table_ident)
            .file_io(file_io)
            .readonly(true)
            .build();

        Ok(Self(table))
    }
    /// Creates a static table directly from metadata file and `FileIO`
    pub async fn from_metadata_file(
        metadata_file_path: &str,
        table_ident: TableIdent,
        file_io: FileIO,
    ) -> Result<Self> {
        let metadata_file = file_io.new_input(metadata_file_path)?;
        let metadata_file_content = metadata_file.read().await?;
        let table_metadata = serde_json::from_slice::<TableMetadata>(&metadata_file_content)?;
        Self::from_metadata(table_metadata, table_ident, file_io).await
    }

    /// Create a TableScanBuilder for the static table.
    pub fn scan(&self) -> TableScanBuilder<'_> {
        self.0.scan()
    }

    /// Get TableMetadataRef for the static table
    pub fn metadata(&self) -> TableMetadataRef {
        self.0.metadata_ref()
    }

    /// Consumes the `StaticTable` and return it as a `Table`
    /// Please use this method carefully as the Table it returns remains detached from a catalog
    /// and can't be used to perform modifications on the table.
    pub fn into_table(self) -> Table {
        self.0
    }

    /// Create a reader for the table.
    pub fn reader_builder(&self) -> ArrowReaderBuilder {
        ArrowReaderBuilder::new(self.0.file_io.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_static_table_from_file() {
        let metadata_file_name = "TableMetadataV2Valid.json";
        let metadata_file_path = format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            metadata_file_name
        );
        let file_io = FileIO::from_path(&metadata_file_path)
            .unwrap()
            .build()
            .unwrap();
        let static_identifier = TableIdent::from_strs(["static_ns", "static_table"]).unwrap();
        let static_table =
            StaticTable::from_metadata_file(&metadata_file_path, static_identifier, file_io)
                .await
                .unwrap();
        let snapshot_id = static_table
            .metadata()
            .current_snapshot()
            .unwrap()
            .snapshot_id();
        assert_eq!(
            snapshot_id, 3055729675574597004,
            "snapshot id from metadata don't match"
        );
    }

    #[tokio::test]
    async fn test_static_into_table() {
        let metadata_file_name = "TableMetadataV2Valid.json";
        let metadata_file_path = format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            metadata_file_name
        );
        let file_io = FileIO::from_path(&metadata_file_path)
            .unwrap()
            .build()
            .unwrap();
        let static_identifier = TableIdent::from_strs(["static_ns", "static_table"]).unwrap();
        let static_table =
            StaticTable::from_metadata_file(&metadata_file_path, static_identifier, file_io)
                .await
                .unwrap();
        let table = static_table.into_table();
        assert!(table.readonly());
        assert_eq!(table.identifier.name(), "static_table");
    }

    #[tokio::test]
    async fn test_table_readonly_flag() {
        let metadata_file_name = "TableMetadataV2Valid.json";
        let metadata_file_path = format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            metadata_file_name
        );
        let file_io = FileIO::from_path(&metadata_file_path)
            .unwrap()
            .build()
            .unwrap();
        let metadata_file = file_io.new_input(metadata_file_path).unwrap();
        let metadata_file_content = metadata_file.read().await.unwrap();
        let table_metadata =
            serde_json::from_slice::<TableMetadata>(&metadata_file_content).unwrap();
        let static_identifier = TableIdent::from_strs(["ns", "table"]).unwrap();
        let table = Table::builder()
            .metadata(table_metadata)
            .identifier(static_identifier)
            .file_io(file_io)
            .build();
        assert!(!table.readonly());
        assert_eq!(table.identifier.name(), "table");
    }
}
