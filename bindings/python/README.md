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

# Pyiceberg Core

This project is used to build an Iceberg-rust powered core for [PyIceberg](https://py.iceberg.apache.org/).

## Setup

The repository uses [mise](https://mise.en.dev/) to install Python, uv, Rust, and the other development tools. Follow the root [contributor setup](../../CONTRIBUTING.md#setup), then run these commands from the repository root:

```shell
mise install
mise run python:verify-libpython
mise run python:install
```

The managed Python includes the shared `libpython` required to compile the Python/DataFusion bindings. The install task makes uv use that interpreter and creates the binding's development environment.

## Build

```shell
mise run python:build
```

## Test

```shell
mise run python:test
```

## Style checks

```shell
mise run python:check-format
mise run python:check-style
```
