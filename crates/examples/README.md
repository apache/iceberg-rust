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

Example usage code for `iceberg-rust`.

The [`datafusion-session-catalog` example](src/datafusion_session_catalog.rs) is
self-contained. It demonstrates how to attach application-specific request metadata to a
DataFusion session, resolve it into an Iceberg `SessionContext`, and query a
session-catalog-backed `IcebergCatalogProvider`:

```shell
cargo run -p iceberg-examples --example datafusion-session-catalog
```

The REST catalog examples require a catalog server and its supporting environment.
