use pyo3::prelude::*;
use pyo3::wrap_pyfunction;


use std::collections::HashMap;

use iceberg_catalog_sql::{SqlCatalog, SqlCatalogConfig, SqlBindStyle};
use iceberg::catalog::NamespaceIdent;
use iceberg::io::FileIO;



#[pyclass]
pub struct PySqlCatalog {
    inner: SqlCatalog,
}

#[pymethods]
impl PySqlCatalog {
    #[new]
    fn new(uri: String, name: String, warehouse_location: String, sql_bind_style: String) -> PyResult<Self> {
        let sql_bind_style = match sql_bind_style.as_str() {
            "DollarNumeric" => SqlBindStyle::DollarNumeric,
            "QMark" => SqlBindStyle::QMark,
            _ => return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid SqlBindStyle")),
        };

        let config = SqlCatalogConfig {
            uri,
            name,
            warehouse_location,
            file_io: FileIO::from_path(&warehouse_location).unwrap().build().unwrap(),
            sql_bind_style,
            props: HashMap::new(),
        };

        let inner = SqlCatalog::new(config).unwrap();

        Ok(PySqlCatalog { inner })
    }

    fn list_namespaces(&self, parent: Option<String>) -> PyResult<Vec<String>> {
        let parent_ident = parent.map(|p| NamespaceIdent::new(p));
        let namespaces = self.inner.list_namespaces(parent_ident.as_ref()).unwrap();
        Ok(namespaces.into_iter().map(|ns| ns.to_string()).collect())
    }

    fn create_namespace(&self, namespace: String, properties: HashMap<String, String>) -> PyResult<()> {
        let namespace_ident = NamespaceIdent::new(namespace);
        self.inner.create_namespace(&namespace_ident, properties).unwrap();
        Ok(())
    }

    // Add other methods similarly...
}
