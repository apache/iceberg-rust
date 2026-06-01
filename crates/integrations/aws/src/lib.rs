// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! AWS Integration for Apache Iceberg.

pub mod config;

pub use config::{create_sdk_config, map_aws_to_s3_properties};

/// Property aws profile name
pub const AWS_PROFILE_NAME: &str = "profile_name";
/// Property aws region
pub const AWS_REGION_NAME: &str = "region_name";
/// Property aws access key
pub const AWS_ACCESS_KEY_ID: &str = "aws_access_key_id";
/// Property aws secret access key
pub const AWS_SECRET_ACCESS_KEY: &str = "aws_secret_access_key";
/// Property aws session token
pub const AWS_SESSION_TOKEN: &str = "aws_session_token";
/// Property for the IAM role ARN to assume when accessing AWS services.
/// When set, the catalog will use AWS STS to assume the specified role,
/// replacing the default credential chain for both the AWS service client and S3 file I/O.
pub const AWS_ASSUME_ROLE_ARN: &str = "client.assume-role.arn";
/// Optional external ID used when assuming an IAM role (for cross-account access).
pub const AWS_ASSUME_ROLE_EXTERNAL_ID: &str = "client.assume-role.external-id";
/// Optional session name used when assuming an IAM role.
pub const AWS_ASSUME_ROLE_SESSION_NAME: &str = "client.assume-role.session-name";
