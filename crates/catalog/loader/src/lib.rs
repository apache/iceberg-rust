use async_trait::async_trait;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use iceberg::{Catalog, CatalogBuilder, Error, ErrorKind, Result};
use iceberg_catalog_rest::RestCatalogBuilder;

#[async_trait]
pub trait BoxedCatalogBuilder {
    async fn load(self: Box<Self>, name: String, props: HashMap<String, String>) -> Result<Arc<dyn Catalog>>;
}

#[async_trait]
impl<T: CatalogBuilder + 'static> BoxedCatalogBuilder for T {
    async fn load(self: Box<Self>, name: String, props: HashMap<String, String>) -> Result<Arc<dyn Catalog>> {
        let builder = *self;
        Ok(Arc::new(builder.load(name, props).await.unwrap()) as Arc<dyn Catalog>) 
    }
}

pub fn load(r#type: &str) -> Result<Box<dyn BoxedCatalogBuilder>> {
    match r#type {
        "rest" => Ok(Box::new(RestCatalogBuilder::default()) as Box<dyn BoxedCatalogBuilder>),
        _ => Err(Error::new(
            ErrorKind::FeatureUnsupported,
            format!("Unsupported catalog type: {}", r#type),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use crate::load;

    #[tokio::test]
    async fn test_load() {
        let catalog = load("rest").unwrap();
        catalog.load("rest".to_string(), HashMap::from(
            [("key".to_string(), "value".to_string())]
        )).await.unwrap();
    }
}
