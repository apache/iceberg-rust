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

//! View API for Apache Iceberg.
//!
//! This module is the Rust analogue of Java's `org.apache.iceberg.view` package: the [`View`]
//! handle (mirroring `org.apache.iceberg.view.View` / `BaseView`), the [`ViewCommit`] commit
//! object (mirroring how `BaseViewOperations.commit(base, metadata)` is fed by
//! `UpdateRequirements.forReplaceView`), and the two pending-update builders
//! [`ReplaceViewVersionAction`] (Java `ReplaceViewVersion` / `ViewVersionReplace`) and
//! [`UpdateViewPropertiesAction`] (Java `UpdateViewProperties` / `PropertiesUpdate`).
//!
//! The metadata machinery itself — version-id assignment, the identical-version REUSE, schema
//! interning, version-log append/expiry, and the dialect rules — lives in
//! [`crate::spec::ViewMetadataBuilder`], a 1:1 port of Java `ViewMetadata.Builder`. The types
//! here are the catalog-facing seam over that builder.

use std::collections::{HashMap, HashSet};
use std::mem::take;
use std::str::FromStr;
use std::sync::Arc;

use chrono::Utc;

use crate::catalog::{ViewRequirement, ViewUpdate};
use crate::io::FileIO;
use crate::spec::{
    Schema, ViewMetadata, ViewMetadataRef, ViewRepresentation, ViewRepresentations, ViewVersion,
};
use crate::{Error, ErrorKind, MetadataLocation, NamespaceIdent, Result, TableIdent};

/// Builder for a [`View`].
#[derive(Debug)]
pub struct ViewBuilder {
    file_io: Option<FileIO>,
    metadata_location: Option<String>,
    metadata: Option<ViewMetadataRef>,
    identifier: Option<TableIdent>,
}

impl ViewBuilder {
    pub(crate) fn new() -> Self {
        Self {
            file_io: None,
            metadata_location: None,
            metadata: None,
            identifier: None,
        }
    }

    /// Required — the [`FileIO`] used to read/write the view's metadata files.
    pub fn file_io(mut self, file_io: FileIO) -> Self {
        self.file_io = Some(file_io);
        self
    }

    /// Optional — the location of the view's current metadata JSON file.
    pub fn metadata_location<T: Into<String>>(mut self, metadata_location: T) -> Self {
        self.metadata_location = Some(metadata_location.into());
        self
    }

    /// Required — the [`ViewMetadata`] for the view.
    pub fn metadata<T: Into<ViewMetadataRef>>(mut self, metadata: T) -> Self {
        self.metadata = Some(metadata.into());
        self
    }

    /// Required — the [`TableIdent`] (namespace + name) identifying the view.
    pub fn identifier(mut self, identifier: TableIdent) -> Self {
        self.identifier = Some(identifier);
        self
    }

    /// Build the [`View`].
    pub fn build(self) -> Result<View> {
        let Self {
            file_io,
            metadata_location,
            metadata,
            identifier,
        } = self;

        let Some(file_io) = file_io else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "FileIO must be provided with ViewBuilder.file_io()",
            ));
        };

        let Some(metadata) = metadata else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "ViewMetadataRef must be provided with ViewBuilder.metadata()",
            ));
        };

        let Some(identifier) = identifier else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "TableIdent must be provided with ViewBuilder.identifier()",
            ));
        };

        Ok(View {
            file_io,
            metadata_location,
            metadata,
            identifier,
        })
    }
}

/// `View` represents a view in the catalog — the Rust analogue of Java
/// `org.apache.iceberg.view.View` / `BaseView`.
#[derive(Debug, Clone)]
pub struct View {
    file_io: FileIO,
    metadata_location: Option<String>,
    metadata: ViewMetadataRef,
    identifier: TableIdent,
}

impl View {
    /// Returns a [`ViewBuilder`] to construct a view.
    pub fn builder() -> ViewBuilder {
        ViewBuilder::new()
    }

    /// Returns the view identifier.
    pub fn identifier(&self) -> &TableIdent {
        &self.identifier
    }

    /// Returns the current view metadata.
    pub fn metadata(&self) -> &ViewMetadata {
        &self.metadata
    }

    /// Returns the current view metadata as a shared reference.
    pub fn metadata_ref(&self) -> ViewMetadataRef {
        self.metadata.clone()
    }

    /// Returns the location of the view's current metadata JSON file, if known.
    pub fn metadata_location(&self) -> Option<&str> {
        self.metadata_location.as_deref()
    }

    /// Returns the location of the view's current metadata JSON file, or an error if absent.
    pub fn metadata_location_result(&self) -> Result<&str> {
        self.metadata_location.as_deref().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Metadata location does not exist for view: {}",
                    self.identifier
                ),
            )
        })
    }

    /// Returns the [`FileIO`] used by this view.
    pub fn file_io(&self) -> &FileIO {
        &self.file_io
    }

    /// Sets the view metadata, returning the updated handle. Used when applying a commit.
    pub(crate) fn with_metadata(mut self, metadata: ViewMetadataRef) -> Self {
        self.metadata = metadata;
        self
    }

    /// Sets the view metadata location, returning the updated handle.
    pub(crate) fn with_metadata_location(mut self, metadata_location: String) -> Self {
        self.metadata_location = Some(metadata_location);
        self
    }

    /// Begin a `replaceVersion` operation (Java `View.replaceVersion()`).
    pub fn replace_version(&self) -> ReplaceViewVersionAction {
        ReplaceViewVersionAction::new(self.identifier.clone(), self.metadata.clone())
    }

    /// Begin an `updateProperties` operation (Java `View.updateProperties()`).
    pub fn update_properties(&self) -> UpdateViewPropertiesAction {
        UpdateViewPropertiesAction::new(self.identifier.clone(), self.metadata.clone())
    }
}

/// `ViewCommit` represents the commit of a view change to the catalog — the Rust analogue of
/// [`crate::TableCommit`].
///
/// It bundles the view identifier, the requirement set the commit is gated on, and the ordered
/// metadata updates. The builder is `pub(crate)` because, like `TableCommit`, a `ViewCommit`
/// should be produced through a pending-update action ([`ReplaceViewVersionAction`] /
/// [`UpdateViewPropertiesAction`]) rather than constructed directly.
#[derive(Debug, Clone)]
pub struct ViewCommit {
    identifier: TableIdent,
    requirements: Vec<ViewRequirement>,
    updates: Vec<ViewUpdate>,
}

impl ViewCommit {
    /// Returns the view identifier.
    pub fn identifier(&self) -> &TableIdent {
        &self.identifier
    }

    /// Take all requirements, leaving the commit's requirement list empty.
    pub fn take_requirements(&mut self) -> Vec<ViewRequirement> {
        take(&mut self.requirements)
    }

    /// Take all updates, leaving the commit's update list empty.
    pub fn take_updates(&mut self) -> Vec<ViewUpdate> {
        take(&mut self.updates)
    }

    /// Applies this commit to the given [`View`] as part of a catalog update.
    ///
    /// Mirrors [`crate::TableCommit::apply`]: each requirement is checked against the current
    /// metadata, then the updates are applied to a fresh [`crate::spec::ViewMetadataBuilder`],
    /// and the metadata-file version is bumped. Returns a new [`View`] with updated metadata,
    /// or an error if a requirement fails or an update is invalid.
    pub fn apply(self, view: View) -> Result<View> {
        for requirement in &self.requirements {
            requirement.check(Some(view.metadata()))?;
        }

        let current_metadata_location = view.metadata_location_result()?;

        let mut metadata_builder = view.metadata().clone().into_builder();
        for update in self.updates {
            metadata_builder = update.apply(metadata_builder)?;
        }

        let new_metadata_location = MetadataLocation::from_str(current_metadata_location)?
            .with_next_version()
            .to_string();

        Ok(view
            .with_metadata(Arc::new(metadata_builder.build()?.metadata))
            .with_metadata_location(new_metadata_location))
    }
}

/// `replaceVersion` pending update — the Rust analogue of Java `ReplaceViewVersion` /
/// `ViewVersionReplace`.
///
/// Collects representations (one per SQL dialect), a schema, and the default namespace/catalog,
/// then commits a new — or REUSED, when an identical version already exists — current view
/// version. The reuse decision is made by [`crate::spec::ViewMetadataBuilder`] inside
/// [`Self::to_commit`], exactly as Java's `ViewMetadata.buildFrom(base).setCurrentVersion(...)`
/// runs `reuseOrCreateNewViewVersionId`.
#[derive(Debug)]
pub struct ReplaceViewVersionAction {
    identifier: TableIdent,
    base: ViewMetadataRef,
    representations: Vec<ViewRepresentation>,
    schema: Option<Schema>,
    default_namespace: Option<NamespaceIdent>,
    default_catalog: Option<String>,
}

impl ReplaceViewVersionAction {
    fn new(identifier: TableIdent, base: ViewMetadataRef) -> Self {
        Self {
            identifier,
            base,
            representations: Vec::new(),
            schema: None,
            default_namespace: None,
            default_catalog: None,
        }
    }

    /// Add a SQL query for the given dialect (Java `VersionBuilder.withQuery`).
    pub fn with_query(mut self, dialect: impl Into<String>, sql: impl Into<String>) -> Self {
        self.representations.push(ViewRepresentation::Sql(
            crate::spec::SqlViewRepresentation {
                dialect: dialect.into(),
                sql: sql.into(),
            },
        ));
        self
    }

    /// Set the schema of the replacement version (Java `VersionBuilder.withSchema`).
    pub fn with_schema(mut self, schema: Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Set the default namespace (Java `VersionBuilder.withDefaultNamespace`).
    pub fn with_default_namespace(mut self, namespace: NamespaceIdent) -> Self {
        self.default_namespace = Some(namespace);
        self
    }

    /// Set the default catalog (Java `VersionBuilder.withDefaultCatalog`).
    pub fn with_default_catalog(mut self, catalog: impl Into<String>) -> Self {
        self.default_catalog = Some(catalog.into());
        self
    }

    /// Produce the [`ViewCommit`] for this replace.
    ///
    /// Mirrors Java `ViewVersionReplace.internalApply`: requires at least one representation, a
    /// schema, and a default namespace; builds the candidate version with id `max(versionId) + 1`
    /// and the current timestamp; then runs it through [`crate::spec::ViewMetadataBuilder`]
    /// (which performs the identical-version reuse). The emitted requirement set is Java
    /// `UpdateRequirements.forReplaceView` = `[AssertViewUUID]` alone.
    pub fn to_commit(self) -> Result<ViewCommit> {
        if self.representations.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot replace view without specifying a query",
            ));
        }
        let schema = self.schema.ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Cannot replace view without specifying schema",
            )
        })?;
        let default_namespace = self.default_namespace.ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Cannot replace view without specifying a default namespace",
            )
        })?;

        // Java assigns max(versionId) + 1; the builder re-checks and reuses an identical version.
        let max_version_id = self
            .base
            .versions()
            .map(|version| version.version_id())
            .max()
            .unwrap_or(self.base.current_version_id());

        let new_version = ViewVersion::builder()
            .with_version_id(max_version_id + 1)
            .with_timestamp_ms(Utc::now().timestamp_millis())
            .with_schema_id(schema.schema_id())
            .with_default_namespace(default_namespace)
            .with_default_catalog(self.default_catalog)
            .with_summary(HashMap::new())
            .with_representations(ViewRepresentations(self.representations))
            .build();

        let build_result = self
            .base
            .as_ref()
            .clone()
            .into_builder()
            .set_current_version(new_version, schema)?
            .build()?;

        Ok(ViewCommit {
            identifier: self.identifier,
            requirements: vec![ViewRequirement::UuidMatch {
                uuid: self.base.uuid(),
            }],
            updates: build_result.changes,
        })
    }
}

/// `updateProperties` pending update — the Rust analogue of Java `UpdateViewProperties` /
/// `PropertiesUpdate`.
///
/// Collects property sets and removals, enforcing Java's cross-guard that the same key cannot be
/// both set and removed in one operation, then emits a [`ViewCommit`].
#[derive(Debug)]
pub struct UpdateViewPropertiesAction {
    identifier: TableIdent,
    base: ViewMetadataRef,
    updates: HashMap<String, String>,
    removals: HashSet<String>,
}

impl UpdateViewPropertiesAction {
    fn new(identifier: TableIdent, base: ViewMetadataRef) -> Self {
        Self {
            identifier,
            base,
            updates: HashMap::new(),
            removals: HashSet::new(),
        }
    }

    /// Set a property (Java `UpdateViewProperties.set`).
    ///
    /// # Errors
    /// - The key is also queued for removal (Java "Cannot remove and update the same key").
    pub fn set(mut self, key: impl Into<String>, value: impl Into<String>) -> Result<Self> {
        let key = key.into();
        if self.removals.contains(&key) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Cannot remove and update the same key: {key}"),
            ));
        }
        self.updates.insert(key, value.into());
        Ok(self)
    }

    /// Remove a property (Java `UpdateViewProperties.remove`).
    ///
    /// # Errors
    /// - The key is also queued for update (Java "Cannot remove and update the same key").
    pub fn remove(mut self, key: impl Into<String>) -> Result<Self> {
        let key = key.into();
        if self.updates.contains_key(&key) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Cannot remove and update the same key: {key}"),
            ));
        }
        self.removals.insert(key);
        Ok(self)
    }

    /// Produce the [`ViewCommit`] for this property update.
    ///
    /// Mirrors Java `PropertiesUpdate.internalApply`:
    /// `buildFrom(base).setProperties(updates).removeProperties(removals).build()`. The
    /// requirement set is `[AssertViewUUID]` alone, matching `forReplaceView`.
    pub fn to_commit(self) -> Result<ViewCommit> {
        let build_result = self
            .base
            .as_ref()
            .clone()
            .into_builder()
            .set_properties(self.updates)?
            .remove_properties(&self.removals.into_iter().collect::<Vec<_>>())
            .build()?;

        Ok(ViewCommit {
            identifier: self.identifier,
            requirements: vec![ViewRequirement::UuidMatch {
                uuid: self.base.uuid(),
            }],
            updates: build_result.changes,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use uuid::Uuid;

    use super::*;
    use crate::spec::{
        NestedField, PrimitiveType, SqlViewRepresentation, Type, ViewMetadataBuilder,
        ViewRepresentations,
    };
    use crate::{NamespaceIdent, TableIdent, ViewCreation};

    fn test_schema() -> Schema {
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::optional(1, "event_count", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap()
    }

    /// Build a base [`ViewMetadata`] with one `spark`-dialect version.
    fn base_metadata() -> ViewMetadata {
        let representations =
            ViewRepresentations(vec![ViewRepresentation::Sql(SqlViewRepresentation {
                sql: "SELECT 1 AS event_count".to_string(),
                dialect: "spark".to_string(),
            })]);
        let creation = ViewCreation::builder()
            .location("s3://bucket/warehouse/default.db/event_agg".to_string())
            .name("event_agg".to_string())
            .schema(test_schema())
            .default_namespace(NamespaceIdent::new("default".to_string()))
            .representations(representations)
            .build();

        ViewMetadataBuilder::from_view_creation(creation)
            .unwrap()
            .build()
            .unwrap()
            .metadata
    }

    fn view_ident() -> TableIdent {
        TableIdent::new(
            NamespaceIdent::new("default".to_string()),
            "event_agg".to_string(),
        )
    }

    // RISK: a uuid-match requirement that does not actually compare uuids would let a commit land
    // against a different (recreated) view, corrupting its history.
    #[test]
    fn test_view_requirement_uuid_match_accepts_same_rejects_different() {
        let metadata = base_metadata();
        let matching = ViewRequirement::UuidMatch {
            uuid: metadata.uuid(),
        };
        assert!(matching.check(Some(&metadata)).is_ok());

        let mismatching = ViewRequirement::UuidMatch {
            uuid: Uuid::now_v7(),
        };
        let error = mismatching.check(Some(&metadata)).unwrap_err();
        assert!(error.to_string().contains("view UUID does not match"));
    }

    // RISK: a NotExist requirement that passes when the view exists would clobber an existing view.
    #[test]
    fn test_view_requirement_not_exist_gates_on_presence() {
        let metadata = base_metadata();
        assert!(ViewRequirement::NotExist.check(None).is_ok());
        let error = ViewRequirement::NotExist
            .check(Some(&metadata))
            .unwrap_err();
        assert!(error.to_string().contains("already exists"));
    }

    // RISK: the replace requirement set must be EXACTLY [AssertViewUUID] (Java forReplaceView). The
    // retry machinery would mask an over-strict requirement; pin it at the commit object source.
    #[test]
    fn test_replace_version_emits_only_uuid_requirement() {
        let metadata = base_metadata();
        let view = View::builder()
            .identifier(view_ident())
            .metadata(metadata.clone())
            .metadata_location(
                "s3://bucket/warehouse/default.db/event_agg/metadata/00000-\
                 11111111-1111-1111-1111-111111111111.metadata.json"
                    .to_string(),
            )
            .file_io(
                crate::io::FileIOBuilder::new(Arc::new(crate::io::MemoryStorageFactory)).build(),
            )
            .build()
            .unwrap();

        let mut commit = view
            .replace_version()
            .with_query("spark", "SELECT 2 AS event_count")
            .with_schema(test_schema())
            .with_default_namespace(NamespaceIdent::new("default".to_string()))
            .to_commit()
            .unwrap();

        assert_eq!(commit.take_requirements(), vec![
            ViewRequirement::UuidMatch {
                uuid: metadata.uuid()
            }
        ]);
    }

    // RISK: replace without a query/schema/namespace must fail loudly (Java Preconditions.checkState).
    #[test]
    fn test_replace_version_requires_query_schema_namespace() {
        let metadata = base_metadata();
        let action = ReplaceViewVersionAction::new(view_ident(), Arc::new(metadata.clone()));
        assert!(
            action
                .to_commit()
                .unwrap_err()
                .to_string()
                .contains("without specifying a query")
        );

        let action = ReplaceViewVersionAction::new(view_ident(), Arc::new(metadata.clone()))
            .with_query("spark", "SELECT 1");
        assert!(
            action
                .to_commit()
                .unwrap_err()
                .to_string()
                .contains("without specifying schema")
        );

        let action = ReplaceViewVersionAction::new(view_ident(), Arc::new(metadata))
            .with_query("spark", "SELECT 1")
            .with_schema(test_schema());
        assert!(
            action
                .to_commit()
                .unwrap_err()
                .to_string()
                .contains("without specifying a default namespace")
        );
    }

    // RISK: committing the SAME representations twice must REUSE the existing version (Java reuses
    // rather than minting a new id) — a new version each time bloats history forever.
    #[test]
    fn test_replace_version_reuses_identical_version() {
        let metadata = base_metadata();
        // The base already carries the `spark` "SELECT 1 AS event_count" version as version 1.
        let commit = ReplaceViewVersionAction::new(view_ident(), Arc::new(metadata.clone()))
            .with_query("spark", "SELECT 1 AS event_count")
            .with_schema(test_schema())
            .with_default_namespace(NamespaceIdent::new("default".to_string()))
            .to_commit()
            .unwrap();

        // Apply the commit's updates against a fresh builder from the same base and confirm the
        // version count did NOT grow (the identical version was reused at id 1).
        let mut builder = metadata.clone().into_builder();
        for update in commit.updates.clone() {
            builder = update.apply(builder).unwrap();
        }
        let new_metadata = builder.build().unwrap().metadata;
        assert_eq!(new_metadata.versions().count(), 1);
        assert_eq!(new_metadata.current_version_id(), 1);
    }

    // RISK: a non-identical replace must MINT a new version and flip the current pointer + log.
    #[test]
    fn test_replace_version_mints_new_version_and_flips_current() {
        let metadata = base_metadata();
        let commit = ReplaceViewVersionAction::new(view_ident(), Arc::new(metadata.clone()))
            .with_query("spark", "SELECT 99 AS event_count")
            .with_schema(test_schema())
            .with_default_namespace(NamespaceIdent::new("default".to_string()))
            .to_commit()
            .unwrap();

        let mut builder = metadata.clone().into_builder();
        for update in commit.updates.clone() {
            builder = update.apply(builder).unwrap();
        }
        let new_metadata = builder.build().unwrap().metadata;
        assert_eq!(new_metadata.versions().count(), 2);
        assert_eq!(new_metadata.current_version_id(), 2);
        // Old version is still present.
        assert!(new_metadata.version_by_id(1).is_some());
        // Version log records both transitions.
        assert_eq!(
            new_metadata
                .history()
                .iter()
                .map(|entry| entry.version_id())
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    // RISK: the same key cannot be both set and removed in one property update (Java cross-guard).
    #[test]
    fn test_update_properties_cannot_set_and_remove_same_key() {
        let metadata = base_metadata();
        let action = UpdateViewPropertiesAction::new(view_ident(), Arc::new(metadata.clone()))
            .set("comment", "hello")
            .unwrap();
        assert!(
            action
                .remove("comment")
                .unwrap_err()
                .to_string()
                .contains("Cannot remove and update the same key")
        );

        let action = UpdateViewPropertiesAction::new(view_ident(), Arc::new(metadata))
            .remove("comment")
            .unwrap();
        assert!(
            action
                .set("comment", "hello")
                .unwrap_err()
                .to_string()
                .contains("Cannot remove and update the same key")
        );
    }

    // RISK: update_properties must carry only the uuid requirement and actually update properties.
    #[test]
    fn test_update_properties_emits_uuid_requirement_and_applies() {
        let metadata = base_metadata();
        let commit = UpdateViewPropertiesAction::new(view_ident(), Arc::new(metadata.clone()))
            .set("comment", "daily counts")
            .unwrap()
            .to_commit()
            .unwrap();

        assert_eq!(commit.requirements, vec![ViewRequirement::UuidMatch {
            uuid: metadata.uuid()
        }]);

        let mut builder = metadata.into_builder();
        for update in commit.updates.clone() {
            builder = update.apply(builder).unwrap();
        }
        let new_metadata = builder.build().unwrap().metadata;
        assert_eq!(
            new_metadata.properties().get("comment"),
            Some(&"daily counts".to_string())
        );
    }
}
