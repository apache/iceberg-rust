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

Install Hatch:

```shell
pip install hatch==1.12.0
```

Hatch uses [uv](https://docs.astral.sh/uv/) as a backend by default, so [make sure that it is installed](https://docs.astral.sh/uv/getting-started/installation/) as well.

## Build

```shell
hatch run dev:develop
```

## Test

```shell
hatch run dev:test
```
