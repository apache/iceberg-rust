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

# Apache Iceberg Rust AWS Integration

This crate provides shared AWS configuration and credential utilities for Apache Iceberg Rust catalogs and integrations, including:

- Strongly typed `AwsProperties` view over catalog and client property maps.
- AWS `SdkConfig` construction with STS `AssumeRole` support and STS endpoint isolation.
- Refreshable STS credential caching (`RefreshingCredentialsProvider`) with non-blocking read-lock fast paths and failure backoff.
- Credential bridging (`AwsSdkCredentialProvider`) between AWS SDK credential providers and `reqsign` / S3 `FileIO`.
