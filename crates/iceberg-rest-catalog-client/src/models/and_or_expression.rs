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

/*
 * Apache Iceberg REST Catalog API
 *
 * Defines the specification for the first version of the REST Catalog API. Implementations should ideally support both Iceberg table specs v1 and v2, with priority given to v2.
 *
 * The version of the OpenAPI document: 0.0.1
 *
 * Generated by: https://openapi-generator.tech
 */

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct AndOrExpression {
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(rename = "left")]
    pub left: Box<crate::models::Expression>,
    #[serde(rename = "right")]
    pub right: Box<crate::models::Expression>,
}

impl AndOrExpression {
    pub fn new(
        r#type: String,
        left: crate::models::Expression,
        right: crate::models::Expression,
    ) -> AndOrExpression {
        AndOrExpression {
            r#type,
            left: Box::new(left),
            right: Box::new(right),
        }
    }
}
