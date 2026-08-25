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

# Catalog

`Catalog` is the entry point for accessing iceberg tables. You can use a catalog to:

* Create and list namespaces.
* Create, load, and drop tables

`SessionCatalog` offers the same operations, but takes a `SessionContext` with each call instead of
binding one for the lifetime of the catalog. It is currently implemented by the REST catalog only.

There is support for the following catalogs:

| Catalog | Description |
|---------|------------|
| `Rest`   | the Iceberg REST catalog              |
| `Glue`          | the AWS Glue Data Catalog             |
| `Memory` | a memory-based Catalog                |
| `HMS`           | Apache Iceberg HiveMetaStore catalog  |
| `S3Tables`      | AWS S3 Tables                         |
| `SQL`           | SQL-based catalog                     |

Not all catalog implementations are complete.

## REST catalog APIs

The REST catalog provides two interfaces:

* `RestCatalog` implements `Catalog` and binds one `SessionContext` to every operation.
* `RestSessionCatalog` implements `SessionCatalog` and accepts a context with each operation.

REST configuration, authentication, and the HTTP client are shared by the catalog instance.

### `RestCatalog`

Here is an example of how to create a `RestCatalog`:

```rust,no_run,noplayground
{{#rustdoc_include ../../crates/examples/src/rest_catalog_namespace.rs:create_catalog}}
```

You can run following code to list all root namespaces:

```rust,no_run,noplayground
{{#rustdoc_include ../../crates/examples/src/rest_catalog_namespace.rs:list_all_namespace}}
```

Then you can run following code to create namespace:

```rust,no_run,noplayground
{{#rustdoc_include ../../crates/examples/src/rest_catalog_namespace.rs:create_namespace}}
```

### `RestSessionCatalog`

Use `RestSessionCatalog` when the caller supplies a session context for each operation. Build the
catalog and the context it should run under:

```rust,no_run,noplayground
{{#rustdoc_include ../../crates/examples/src/rest_session_catalog_namespace.rs:create_catalog}}
```

Every operation then takes that context. You can run following code to list all root namespaces:

```rust,no_run,noplayground
{{#rustdoc_include ../../crates/examples/src/rest_session_catalog_namespace.rs:list_all_namespace}}
```

Then you can run following code to create namespace:

```rust,no_run,noplayground
{{#rustdoc_include ../../crates/examples/src/rest_session_catalog_namespace.rs:create_namespace}}
```

# Table

After creating `Catalog`, we can manipulate tables through `Catalog`.

You can use following code to create a table:

```rust,no_run,noplayground
{{#rustdoc_include ../../crates/examples/src/rest_catalog_table.rs:create_table}}
```

Also, you can load a table directly:

```rust,no_run,noplayground
{{#rustdoc_include ../../crates/examples/src/rest_catalog_table.rs:load_table}}
```
