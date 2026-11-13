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

Example usage code for `iceberg-rust`. Catalog examples require the corresponding
services, such as a REST catalog server.

The `scan-metrics` example captures reports in memory while planning an existing
local table, without reading data rows or requiring a catalog server:

```sh
cargo run -p iceberg-examples --example scan-metrics -- /path/to/metadata.json
```

The metadata, manifest lists, and manifests must be locally accessible at their
recorded locations. Tables without a snapshot produce no scan report. The example
uses a table-level reporter; a scan can replace it with
`.with_metrics_reporter(other_reporter)` before `.build()`. To discard reports,
install a custom no-op implementation of `MetricsReporter`. Custom reporting
replaces the default info logging consumer. Planning does not wait for delivery,
including at successful EOF. The example keeps the runtime alive and waits on its
reporter's signal, with a timeout, before reading the reports. Tables without a
snapshot skip that wait.
