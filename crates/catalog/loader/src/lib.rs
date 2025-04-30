use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use iceberg::{Catalog, CatalogBuilder, Error, ErrorKind, Result};
use iceberg_catalog_rest::RestCatalogBuilder;

type BoxedCatalogBuilderFuture = Pin<Box<dyn Future<Output = Result<Arc<dyn Catalog>>>>>;

pub trait BoxedCatalogBuilder {
    fn name(&mut self, name: String);
    fn uri(&mut self, uri: String);
    fn warehouse(&mut self, warehouse: String);
    fn with_prop(&mut self, key: String, value: String);

    fn build(self: Box<Self>) -> BoxedCatalogBuilderFuture;
}

impl<T: CatalogBuilder + 'static> BoxedCatalogBuilder for T {
    fn name(&mut self, name: String) {
        self.name(name);
    }

    fn uri(&mut self, uri: String) {
        self.uri(uri);
    }

    fn warehouse(&mut self, warehouse: String) {
        self.warehouse(warehouse);
    }

    fn with_prop(&mut self, key: String, value: String) {
        self.with_prop(key, value);
    }

    fn build(self: Box<Self>) -> BoxedCatalogBuilderFuture {
        let builder = *self;
        Box::pin(async move { Ok(Arc::new(builder.build().await.unwrap()) as Arc<dyn Catalog>) })
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
    use crate::load;

    #[tokio::test]
    async fn test_load() {
        let mut catalog = load("rest").unwrap();
        catalog.name("rest".to_string());
        catalog.with_prop("key".to_string(), "value".to_string());

        catalog.build().await.unwrap();
    }
}
