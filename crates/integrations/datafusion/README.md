<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Apache Iceberg DataFusion Integration

This crate contains the integration of Apache DataFusion and Apache Iceberg.

## REST catalog pagination

Configure pagination on the REST catalog before passing it to
`IcebergCatalogProvider`. The provider loads every namespace and table page;
page size controls REST response sizes, not the number of schemas or tables
visible to DataFusion.

```rust
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, RestCatalogBuilder};
use iceberg_datafusion::IcebergCatalogProvider;

async fn register_catalog(context: &SessionContext) -> iceberg::Result<()> {
    let catalog = RestCatalogBuilder::default()
        .with_page_size(1000)
        // Use the storage factory appropriate for your table locations.
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "rest",
            HashMap::from([(
                REST_CATALOG_PROP_URI.to_string(),
                "http://localhost:8181".to_string(),
            )]),
        )
        .await?;
    let provider = IcebergCatalogProvider::try_new(Arc::new(catalog)).await?;
    context.register_catalog("rest", Arc::new(provider));
    Ok(())
}
```

The `rest-page-size` property in the map passed to `load` can also configure the
page size and takes precedence over `with_page_size`. Server `/v1/config`
defaults have lower priority than client configuration, while server overrides
have the highest priority. If none of these sources sets a page size, the client
omits `pageSize` rather than imposing a default. The effective value must be a
positive `u32` and is validated after the lazy server configuration handshake.
