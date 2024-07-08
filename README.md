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

# Apache Iceberg™ Rust



Rust implementation of [Apache Iceberg™](https://iceberg.apache.org/).

Working on [v0.3.0 Release Milestone](https://github.com/apache/iceberg-rust/milestone/2)

## Components

The Apache Iceberg Rust project is composed of the following components:

| Name                   | Release                                                    | Docs                                                 |
|------------------------|------------------------------------------------------------|------------------------------------------------------|
| [iceberg]              | [![iceberg image]][iceberg link]                           | [![docs release]][iceberg release docs]              |
| [iceberg-datafusion]   | -                                                          | -                                                    |
| [iceberg-catalog-glue] | -                                                          | -                                                    |
| [iceberg-catalog-hms]  | [![iceberg-catalog-hms image]][iceberg-catalog-hms link]   | [![docs release]][iceberg-catalog-hms release docs]  |
| [iceberg-catalog-rest] | [![iceberg-catalog-rest image]][iceberg-catalog-rest link] | [![docs release]][iceberg-catalog-rest release docs] |

[docs release]: https://img.shields.io/badge/docs-release-blue
[iceberg]: crates/iceberg/README.md
[iceberg image]: https://img.shields.io/crates/v/iceberg.svg
[iceberg link]: https://crates.io/crates/iceberg
[iceberg release docs]: https://docs.rs/iceberg

[iceberg-datafusion]: crates/integrations/datafusion/README.md

[iceberg-catalog-glue]: crates/catalog/glue/README.md

[iceberg-catalog-hms]: crates/catalog/hms/README.md
[iceberg-catalog-hms image]: https://img.shields.io/crates/v/iceberg-catalog-hms.svg
[iceberg-catalog-hms link]: https://crates.io/crates/iceberg-catalog-hms
[iceberg-catalog-hms release docs]: https://docs.rs/iceberg-catalog-hms

[iceberg-catalog-rest]: crates/catalog/rest/README.md
[iceberg-catalog-rest image]: https://img.shields.io/crates/v/iceberg-catalog-rest.svg
[iceberg-catalog-rest link]: https://crates.io/crates/iceberg-catalog-rest
[iceberg-catalog-rest release docs]: https://docs.rs/iceberg-catalog-rest

## Contribute

Apache Iceberg is an active open-source project, governed under the Apache Software Foundation (ASF). We are always open to people who want to use or contribute to it. Here are some ways to get involved.

- Start with [Contributing Guide](CONTRIBUTING.md).
- Submit [Issues](https://github.com/apache/iceberg-rust/issues/new) for bug report or feature requests.
- Discuss
  at [dev mailing list](mailto:dev@iceberg.apache.org) ([subscribe](<mailto:dev-subscribe@iceberg.apache.org?subject=(send%20this%20email%20to%20subscribe)>) / [unsubscribe](<mailto:dev-unsubscribe@iceberg.apache.org?subject=(send%20this%20email%20to%20unsubscribe)>) / [archives](https://lists.apache.org/list.html?dev@iceberg.apache.org))
- Talk to community directly
  at [Slack #rust channel](https://join.slack.com/t/apache-iceberg/shared_invite/zt-1zbov3k6e-KtJfoaxp97YfX6dPz1Bk7A).

The Apache Iceberg community is built on the principles described in the [Apache Way](https://www.apache.org/theapacheway/index.html) and all who engage with the community are expected to be respectful, open, come with the best interests of the community in mind, and abide by the Apache Foundation [Code of Conduct](https://www.apache.org/foundation/policies/conduct.html).
## Users

- [Databend](https://github.com/datafuselabs/databend/): An open-source cloud data warehouse that serves as a cost-effective alternative to Snowflake.
- [iceberg-catalog](https://github.com/hansetag/iceberg-catalog): A Rust implementation of the Iceberg REST Catalog specification.

## License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
