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


# Manifest test data

`repeated-decimal-type-definitions.avro` is a V2 data manifest written by
iceberg-rust at commit `ecba0b5`, which used `apache-avro` 0.21. Its table schema
has two optional `decimal(10, 2)` columns, `d1` (id 1) and `d2` (id 2), and its
partition spec has an identity partition on each. It holds one entry for
`s3://b/t/a.parquet` with partition values `123.45` and `-6.78`.

The Avro schema in its header defines the named fixed type `decimal_10_2` twice.
The Avro specification allows only one definition of each name, and
`apache-avro` 0.22 rejects the header, but iceberg-rust wrote manifests like
this one before it moved to 0.22. Tests use it to check that such manifests
still read.

To regenerate it, check out a commit that uses `apache-avro` 0.21, write the
manifest described above with `ManifestWriterBuilder::build_v2_data`, and copy
the file here.
