use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use iceberg::{Catalog, CatalogBuilder, Error, ErrorKind, Result};
use iceberg_catalog_rest::RestCatalogBuilder;

type BoxedCatalogBuilderFuture = Pin<Box<dyn Future<Output = Result<Arc<dyn Catalog>>>>>;

pub trait BoxedCatalogBuilder {
    fn load(self: Box<Self>, name: String, props: HashMap<String, String>) -> BoxedCatalogBuilderFuture;
}

impl<T: CatalogBuilder + 'static> BoxedCatalogBuilder for T {
    fn load(self: Box<Self>, name: String, props: HashMap<String, String>) -> BoxedCatalogBuilderFuture {
        let builder = *self;
        Box::pin(async move { Ok(Arc::new(builder.load(name, props).await.unwrap()) as Arc<dyn Catalog>) })
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
        let mut catalog = load("rest").unwrap();
        catalog.load("rest".to_string(), HashMap::from(
            [("key".to_string(), "value".to_string())]
        )).await.unwrap();
    }
}
