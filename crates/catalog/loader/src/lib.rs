use std::sync::Arc;

use iceberg::{Catalog, CatalogBuilder, Error, ErrorKind, Result};
use iceberg_catalog_rest::RestCatalogBuilder;

pub enum CatalogBuilderDef {
    Rest(RestCatalogBuilder),
}

pub fn load(r#type: &str) -> Result<CatalogBuilderDef> {
    match r#type {
        "rest" => Ok(CatalogBuilderDef::Rest(RestCatalogBuilder::default())),
        _ => Err(Error::new(
            ErrorKind::FeatureUnsupported,
            format!("Unsupported catalog type: {}", r#type),
        )),
    }
}

impl CatalogBuilderDef {
    pub fn name(self, name: impl Into<String>) -> Self {
        match self {
            CatalogBuilderDef::Rest(builder) => CatalogBuilderDef::Rest(builder.name(name)),
        }
    }

    pub fn uri(self, uri: impl Into<String>) -> Self {
        match self {
            CatalogBuilderDef::Rest(builder) => CatalogBuilderDef::Rest(builder.uri(uri)),
        }
    }

    pub fn warehouse(self, warehouse: impl Into<String>) -> Self {
        match self {
            CatalogBuilderDef::Rest(builder) => {
                CatalogBuilderDef::Rest(builder.warehouse(warehouse))
            }
        }
    }

    pub fn with_prop(self, key: impl Into<String>, value: impl Into<String>) -> Self {
        match self {
            CatalogBuilderDef::Rest(builder) => {
                CatalogBuilderDef::Rest(builder.with_prop(key, value))
            }
        }
    }

    pub async fn build(self) -> Result<Arc<dyn Catalog>> {
        match self {
            CatalogBuilderDef::Rest(builder) => builder
                .build()
                .await
                .map(|c| Arc::new(c) as Arc<dyn Catalog>),
        }
    }
}
