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

use std::sync::Arc;

use crate::arrow::ArrowReaderBuilder;
use crate::encryption::EncryptionManager;
use crate::encryption::kms::KeyManagementClient;
use crate::inspect::MetadataTable;
use crate::io::FileIO;
use crate::io::object_cache::ObjectCache;
use crate::runtime::Runtime;
use crate::scan::TableScanBuilder;
use crate::spec::{ManifestListReader, SchemaRef, Snapshot, TableMetadata, TableMetadataRef};
use crate::{Error, ErrorKind, Result, TableIdent};

/// Builder to create table scan.
pub struct TableBuilder {
    file_io: Option<FileIO>,
    metadata_location: Option<String>,
    metadata: Option<TableMetadataRef>,
    identifier: Option<TableIdent>,
    kms_client: Option<Arc<dyn KeyManagementClient>>,
    readonly: bool,
    disable_cache: bool,
    cache_size_bytes: Option<u64>,
    runtime: Option<Runtime>,
}

impl TableBuilder {
    pub(crate) fn new() -> Self {
        Self {
            file_io: None,
            metadata_location: None,
            metadata: None,
            identifier: None,
            kms_client: None,
            readonly: false,
            disable_cache: false,
            cache_size_bytes: None,
            runtime: None,
        }
    }

    /// required - sets the necessary FileIO to use for the table
    pub fn file_io(mut self, file_io: FileIO) -> Self {
        self.file_io = Some(file_io);
        self
    }

    /// optional - sets the tables metadata location
    pub fn metadata_location<T: Into<String>>(mut self, metadata_location: T) -> Self {
        self.metadata_location = Some(metadata_location.into());
        self
    }

    /// required - passes in the TableMetadata to use for the Table
    pub fn metadata<T: Into<TableMetadataRef>>(mut self, metadata: T) -> Self {
        self.metadata = Some(metadata.into());
        self
    }

    /// required - passes in the TableIdent to use for the Table
    pub fn identifier(mut self, identifier: TableIdent) -> Self {
        self.identifier = Some(identifier);
        self
    }

    /// specifies if the Table is readonly or not (default not)
    pub fn readonly(mut self, readonly: bool) -> Self {
        self.readonly = readonly;
        self
    }

    /// specifies if the Table's metadata cache will be disabled,
    /// so that reads of Manifests and ManifestLists will never
    /// get cached.
    pub fn disable_cache(mut self) -> Self {
        self.disable_cache = true;
        self
    }

    /// optionally set a non-default metadata cache size
    pub fn cache_size_bytes(mut self, cache_size_bytes: u64) -> Self {
        self.cache_size_bytes = Some(cache_size_bytes);
        self
    }

    /// Set the Runtime for this table to use when spawning tasks.
    pub fn runtime(mut self, runtime: Runtime) -> Self {
        self.runtime = Some(runtime);
        self
    }

    /// optional - sets the KMS client used to unwrap keys for table encryption.
    ///
    /// If the table metadata has the `encryption.key-id` property set, a
    /// [`KeyManagementClient`] must be provided here so the table can build
    /// an [`EncryptionManager`]; otherwise [`Self::build`] will return an error.
    pub fn kms_client(mut self, kms_client: Arc<dyn KeyManagementClient>) -> Self {
        self.kms_client = Some(kms_client);
        self
    }

    /// build the Table
    pub fn build(self) -> Result<Table> {
        let Self {
            file_io,
            metadata_location,
            metadata,
            identifier,
            kms_client,
            readonly,
            disable_cache,
            cache_size_bytes,
            runtime,
        } = self;

        let Some(file_io) = file_io else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "FileIO must be provided with TableBuilder.file_io()",
            ));
        };

        let Some(metadata) = metadata else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "TableMetadataRef must be provided with TableBuilder.metadata()",
            ));
        };

        let Some(identifier) = identifier else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "TableIdent must be provided with TableBuilder.identifier()",
            ));
        };

        let Some(runtime) = runtime else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Runtime must be provided with TableBuilder.runtime()",
            ));
        };

        let encryption_manager = maybe_configure_encryption(kms_client.as_ref(), &metadata)?;

        let object_cache = if disable_cache {
            Arc::new(ObjectCache::with_disabled_cache(
                file_io.clone(),
                encryption_manager.clone(),
            ))
        } else if let Some(cache_size_bytes) = cache_size_bytes {
            Arc::new(ObjectCache::new_with_capacity(
                file_io.clone(),
                cache_size_bytes,
                encryption_manager.clone(),
            ))
        } else {
            Arc::new(ObjectCache::new(
                file_io.clone(),
                encryption_manager.clone(),
            ))
        };

        Ok(Table {
            file_io,
            metadata_location,
            metadata,
            identifier,
            readonly,
            object_cache,
            runtime,
            encryption_manager,
        })
    }
}

/// Table represents a table in the catalog.
#[derive(Debug, Clone)]
pub struct Table {
    file_io: FileIO,
    metadata_location: Option<String>,
    metadata: TableMetadataRef,
    identifier: TableIdent,
    readonly: bool,
    object_cache: Arc<ObjectCache>,
    runtime: Runtime,
    encryption_manager: Option<Arc<EncryptionManager>>,
}

impl Table {
    /// Sets the [`Table`] metadata and returns an updated instance with the new metadata applied.
    pub(crate) fn with_metadata(mut self, metadata: TableMetadataRef) -> Self {
        self.metadata = metadata;
        self
    }

    /// Sets the [`Table`] metadata location and returns an updated instance.
    pub(crate) fn with_metadata_location(mut self, metadata_location: String) -> Self {
        self.metadata_location = Some(metadata_location);
        self
    }

    /// Returns a TableBuilder to build a table
    pub fn builder() -> TableBuilder {
        TableBuilder::new()
    }

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

    /// Returns current metadata location in a result.
    pub fn metadata_location_result(&self) -> Result<&str> {
        self.metadata_location.as_deref().ok_or(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Metadata location does not exist for table: {}",
                self.identifier
            ),
        ))
    }

    /// Returns file io used in this table.
    pub fn file_io(&self) -> &FileIO {
        &self.file_io
    }

    /// Returns this table's object cache
    pub(crate) fn object_cache(&self) -> Arc<ObjectCache> {
        self.object_cache.clone()
    }

    /// Returns the [`EncryptionManager`] for this table, if encryption is
    /// configured.
    ///
    /// A manager is present iff the table metadata has the
    /// `encryption.key-id` property set and a [`KeyManagementClient`] was
    /// supplied to the [`TableBuilder`].
    pub fn encryption_manager(&self) -> Option<&EncryptionManager> {
        self.encryption_manager.as_deref()
    }

    /// Creates a table scan.
    pub fn scan(&self) -> TableScanBuilder<'_> {
        TableScanBuilder::new(self)
    }

    /// Creates a metadata table which provides table-like APIs for inspecting metadata.
    /// See [`MetadataTable`] for more details.
    pub fn inspect(&self) -> MetadataTable<'_> {
        MetadataTable::new(self)
    }

    /// Returns the [`Runtime`] for this table.
    pub(crate) fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    /// Returns the flag indicating whether the `Table` is readonly or not
    pub fn readonly(&self) -> bool {
        self.readonly
    }

    /// Returns the current schema as a shared reference.
    pub fn current_schema_ref(&self) -> SchemaRef {
        self.metadata.current_schema().clone()
    }

    /// Creates a [`ManifestListReader`] for the given snapshot.
    pub fn manifest_list_reader<'a>(&'a self, snapshot: &'a Snapshot) -> ManifestListReader<'a> {
        ManifestListReader::new(snapshot, &self.file_io, &self.metadata)
    }

    /// Create a reader for the table.
    pub fn reader_builder(&self) -> ArrowReaderBuilder {
        ArrowReaderBuilder::new(self.file_io.clone(), self.runtime().clone())
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
/// let file_io = FileIO::new_with_fs();
/// let static_identifier = TableIdent::from_strs(["static_ns", "static_table"]).unwrap();
/// let static_table =
///     StaticTable::from_metadata_file(&metadata_file_location, static_identifier, file_io)
///         .await
///         .unwrap();
/// let snapshot_id = static_table
///     .metadata()
///     .current_snapshot()
///     .unwrap()
///     .snapshot_id();
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
            .file_io(file_io.clone())
            .runtime(Runtime::try_current()?)
            .readonly(true)
            .build();

        Ok(Self(table?))
    }
    /// Creates a static table directly from metadata file and `FileIO`
    pub async fn from_metadata_file(
        metadata_location: &str,
        table_ident: TableIdent,
        file_io: FileIO,
    ) -> Result<Self> {
        let metadata = TableMetadata::read_from(&file_io, metadata_location).await?;

        let table = Table::builder()
            .metadata(metadata)
            .metadata_location(metadata_location)
            .identifier(table_ident)
            .file_io(file_io.clone())
            .runtime(Runtime::try_current()?)
            .readonly(true)
            .build();

        Ok(Self(table?))
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
        self.0.reader_builder()
    }
}

/// If the table metadata sets the `encryption.key-id` property, build an
/// [`EncryptionManager`] for the table.
///
/// Returns `Ok(None)` if the property is not set. Returns an error if the
/// property is set but no [`KeyManagementClient`] was provided.
fn maybe_configure_encryption(
    kms_client: Option<&Arc<dyn KeyManagementClient>>,
    metadata: &TableMetadataRef,
) -> Result<Option<Arc<EncryptionManager>>> {
    let Some(table_key_id) = metadata.table_properties()?.encryption_key_id else {
        return Ok(None);
    };

    // Encryption is a v3 feature: `encryption-keys` table metadata and the
    // snapshot `key-id` field are introduced in format version 3.
    if metadata.format_version() < FormatVersion::V3 {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            format!(
                "Table encryption requires format version 3, found {}",
                metadata.format_version()
            ),
        ));
    }

    let kms_client = kms_client.ok_or_else(|| {
        Error::new(
            ErrorKind::FeatureUnsupported,
            "Table has encryption.key-id set but no KeyManagementClient was provided to TableBuilder",
        )
    })?;

    let em = EncryptionManager::builder()
        .kms_client(Arc::clone(kms_client))
        .table_key_id(table_key_id)
        .encryption_keys(metadata.encryption_keys.clone())
        .build();
    Ok(Some(Arc::new(em)))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::encryption::StandardKeyMetadata;
    use crate::encryption::kms::MemoryKeyManagementClient;
    use crate::spec::{ManifestListWriter, Operation, Snapshot, Summary, TableProperties};

    #[tokio::test]
    async fn test_static_table_from_file() {
        let metadata_file_name = "TableMetadataV2Valid.json";
        let metadata_file_path = format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            metadata_file_name
        );
        let file_io = FileIO::new_with_fs();
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
        let file_io = FileIO::new_with_fs();
        let static_identifier = TableIdent::from_strs(["static_ns", "static_table"]).unwrap();
        let static_table =
            StaticTable::from_metadata_file(&metadata_file_path, static_identifier, file_io)
                .await
                .unwrap();
        let table = static_table.into_table();
        assert!(table.readonly());
        assert_eq!(table.identifier.name(), "static_table");
        assert_eq!(
            table.metadata_location(),
            Some(metadata_file_path).as_deref()
        );
    }

    #[tokio::test]
    async fn test_table_readonly_flag() {
        let metadata_file_name = "TableMetadataV2Valid.json";
        let metadata_file_path = format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            metadata_file_name
        );
        let file_io = FileIO::new_with_fs();
        let metadata_file = file_io.new_input(metadata_file_path).unwrap();
        let metadata_file_content = metadata_file.read().await.unwrap();
        let table_metadata =
            serde_json::from_slice::<TableMetadata>(&metadata_file_content).unwrap();
        let static_identifier = TableIdent::from_strs(["ns", "table"]).unwrap();
        let table = Table::builder()
            .metadata(table_metadata)
            .identifier(static_identifier)
            .file_io(file_io.clone())
            .runtime(Runtime::try_current().unwrap())
            .build()
            .unwrap();
        assert!(!table.readonly());
        assert_eq!(table.identifier.name(), "table");
    }

    const V3_METADATA: &str = r#"{
        "format-version": 3,
        "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
        "location": "memory:///table",
        "last-sequence-number": 0,
        "last-updated-ms": 1602638573590,
        "last-column-id": 1,
        "current-schema-id": 0,
        "schemas": [{"type": "struct", "schema-id": 0, "fields": [
            {"id": 1, "name": "x", "required": true, "type": "long"}
        ]}],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 1000,
        "default-sort-order-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "properties": {},
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": [],
        "refs": {},
        "next-row-id": 0
    }"#;

    const V2_METADATA: &str = r#"{
        "format-version": 2,
        "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
        "location": "memory:///table",
        "last-sequence-number": 0,
        "last-updated-ms": 1602638573590,
        "last-column-id": 1,
        "current-schema-id": 0,
        "schemas": [{"type": "struct", "schema-id": 0, "fields": [
            {"id": 1, "name": "x", "required": true, "type": "long"}
        ]}],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 1000,
        "default-sort-order-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "properties": {},
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": [],
        "refs": {}
    }"#;

    fn make_kms() -> Arc<dyn KeyManagementClient> {
        let kms = MemoryKeyManagementClient::new();
        kms.add_master_key("master-1").unwrap();
        Arc::new(kms)
    }

    async fn write_empty_manifest_list_bytes(io: &FileIO, path: &str) -> bytes::Bytes {
        let output = io.new_output(path).unwrap();
        let mut writer = ManifestListWriter::v3(output, 1, None, 0, Some(0));
        writer.add_manifests(std::iter::empty()).unwrap();
        writer.close().await.unwrap();
        io.new_input(path).unwrap().read().await.unwrap()
    }

    #[tokio::test]
    async fn table_decrypts_manifest_list_via_object_cache() {
        let io = FileIO::new_with_memory();
        let plain_path = "memory:///table/metadata/manifest-list-plain.avro";
        let encrypted_path = "memory:///table/metadata/manifest-list-enc.avro";

        // Encrypt a real manifest list onto the encrypted path.
        let raw = write_empty_manifest_list_bytes(&io, plain_path).await;
        let kms = make_kms();
        let mgr = EncryptionManager::builder()
            .kms_client(Arc::clone(&kms))
            .table_key_id("master-1")
            .build();
        let encrypted_output = mgr.encrypt(io.new_output(encrypted_path).unwrap());
        let std_km: StandardKeyMetadata = encrypted_output.key_metadata().clone();
        encrypted_output.write(raw).await.unwrap();
        let key_id = mgr
            .encrypt_manifest_list_key_metadata(&std_km)
            .await
            .unwrap();

        // Snapshot the wrapped keys (manifest-list entry + KEK) the manager produced.
        let encryption_keys = mgr.with_encryption_keys(|keys| keys.clone());

        // Build a TableMetadata with those keys, the encryption.key-id property,
        // and a snapshot whose encryption_key_id points at the wrapped entry.
        let mut metadata: TableMetadata = serde_json::from_str(V3_METADATA).unwrap();
        metadata.properties.insert(
            TableProperties::PROPERTY_ENCRYPTION_KEY_ID.to_string(),
            "master-1".to_string(),
        );
        metadata.encryption_keys = encryption_keys;

        let snapshot = Snapshot::builder()
            .with_snapshot_id(1)
            .with_sequence_number(0)
            .with_timestamp_ms(0)
            .with_manifest_list(encrypted_path.to_string())
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(0)
            .with_encryption_key_id(Some(key_id))
            .build();
        let snapshot_ref = Arc::new(snapshot);
        metadata
            .snapshots
            .insert(snapshot_ref.snapshot_id(), snapshot_ref.clone());
        metadata.current_snapshot_id = Some(snapshot_ref.snapshot_id());

        // Build the table with the KMS client, then read via the object cache.
        let table = Table::builder()
            .file_io(io)
            .metadata(metadata)
            .identifier(TableIdent::from_strs(["ns", "enc"]).unwrap())
            .kms_client(kms)
            .build()
            .unwrap();
        assert!(table.encryption_manager().is_some());

        let manifest_list = table
            .object_cache()
            .get_manifest_list(&snapshot_ref, &table.metadata_ref())
            .await
            .unwrap();
        assert_eq!(manifest_list.entries().len(), 0);
    }

    #[tokio::test]
    async fn table_builder_errors_when_encryption_key_id_set_but_no_kms() {
        let mut metadata: TableMetadata = serde_json::from_str(V3_METADATA).unwrap();
        metadata.properties.insert(
            TableProperties::PROPERTY_ENCRYPTION_KEY_ID.to_string(),
            "master-1".to_string(),
        );

        let err = Table::builder()
            .file_io(FileIO::new_with_memory())
            .metadata(metadata)
            .identifier(TableIdent::from_strs(["ns", "enc"]).unwrap())
            .build()
            .unwrap_err();
        assert_eq!(err.kind(), crate::ErrorKind::FeatureUnsupported);
    }

    #[tokio::test]
    async fn table_builder_errors_when_encryption_set_on_pre_v3_table() {
        // Encryption is a v3 spec feature; setting encryption.key-id on a v2 table
        // must be rejected even when a KMS client is available.
        let mut metadata: TableMetadata = serde_json::from_str(V2_METADATA).unwrap();
        metadata.properties.insert(
            TableProperties::PROPERTY_ENCRYPTION_KEY_ID.to_string(),
            "master-1".to_string(),
        );

        let err = Table::builder()
            .file_io(FileIO::new_with_memory())
            .metadata(metadata)
            .identifier(TableIdent::from_strs(["ns", "enc"]).unwrap())
            .kms_client(make_kms())
            .build()
            .unwrap_err();
        assert_eq!(err.kind(), crate::ErrorKind::FeatureUnsupported);
    }

    #[tokio::test]
    async fn table_builder_skips_encryption_when_property_absent() {
        let metadata: TableMetadata = serde_json::from_str(V2_METADATA).unwrap();
        let table = Table::builder()
            .file_io(FileIO::new_with_memory())
            .metadata(metadata)
            .identifier(TableIdent::from_strs(["ns", "plain"]).unwrap())
            .kms_client(make_kms())
            .build()
            .unwrap();
        assert!(table.encryption_manager().is_none());
    }
}
