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

#![deny(missing_docs)]

mod credentials;
mod properties;

pub use credentials::AwsSdkCredentialProvider;
pub use properties::{
    AWS_ACCESS_KEY_ID, AWS_ASSUME_ROLE_ARN, AWS_ASSUME_ROLE_EXTERNAL_ID,
    AWS_ASSUME_ROLE_SESSION_NAME, AWS_PROFILE_NAME, AWS_REGION_NAME, AWS_SECRET_ACCESS_KEY,
    AWS_SESSION_TOKEN, AwsProperties,
};
