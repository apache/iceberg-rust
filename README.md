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

# Apache Iceberg Rust

Native Rust implementation of [Apache Iceberg](https://iceberg.apache.org/).

## Roadmap

### Catalog

| Catalog Type | Status      |
| ------------ | ----------- |
| Rest         | Done        |
| Hive         | Done        |
| Sql          | In Progress |
| Glue         | Done        |
| DynamoDB     | Not Started |

### FileIO

| FileIO Type | Status      |
| ----------- | ----------- |
| S3          | Done        |
| Local File  | Done        |
| GCS         | Not Started |
| HDFS        | Not Started |

Our `FileIO` is powered by [Apache OpenDAL](https://github.com/apache/opendal), so it would be quite easy to
expand to other service.

### Table API

#### Reader

| Feature                                                    | Status      |
| ---------------------------------------------------------- | ----------- |
| File based task planning                                   | Done        |
| Size based task planning                                   | Not started |
| Filter pushdown(manifest evaluation, partition prunning)   | In Progress |
| Apply deletions, including equality and position deletions | Not started |
| Read into arrow record batch                               | In Progress |
| Parquet file support                                       | Done        |
| ORC file support                                           | Not started |

#### Writer

| Feature                  | Status      |
| ------------------------ | ----------- |
| Data writer              | Not started |
| Equality deletion writer | Not started |
| Position deletion writer | Not started |
| Partitioned writer       | Not started |
| Upsert writer            | Not started |
| Parquet file support     | Not started |
| ORC file support         | Not started |

#### Transaction

| Feature               | Status      |
| --------------------- | ----------- |
| Schema evolution      | Not started |
| Update partition spec | Not started |
| Update properties     | Not started |
| Replace sort order    | Not started |
| Update location       | Not started |
| Append files          | Not started |
| Rewrite files         | Not started |
| Rewrite manifests     | Not started |
| Overwrite files       | Not started |
| Row level updates     | Not started |
| Replace partitions    | Not started |
| Snapshot management   | Not started |

### Integrations

We will add integrations with other rust based data systems, such as polars, datafusion, etc.

## Contribute

Iceberg is an active open-source project. We are always open to people who want to use it or contribute to it. Here are some ways to go.

- Start with [Contributing Guide](CONTRIBUTING.md).
- Submit [Issues](https://github.com/apache/iceberg-rust/issues/new) for bug report or feature requests.
- Discuss at [dev mailing list](mailto:dev@iceberg.apache.org) ([subscribe](<mailto:dev-subscribe@iceberg.apache.org?subject=(send%20this%20email%20to%20subscribe)>) / [unsubscribe](<mailto:dev-unsubscribe@iceberg.apache.org?subject=(send%20this%20email%20to%20unsubscribe)>) / [archives](https://lists.apache.org/list.html?dev@iceberg.apache.org))
- Talk to community directly at [Slack #rust channel](https://join.slack.com/t/apache-iceberg/shared_invite/zt-1zbov3k6e-KtJfoaxp97YfX6dPz1Bk7A).

## License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
