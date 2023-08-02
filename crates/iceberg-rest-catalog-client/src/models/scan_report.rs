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
pub struct ScanReport {
    #[serde(rename = "table-name")]
    pub table_name: String,
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    #[serde(rename = "filter")]
    pub filter: Box<crate::models::Expression>,
    #[serde(rename = "schema-id")]
    pub schema_id: i32,
    #[serde(rename = "projected-field-ids")]
    pub projected_field_ids: Vec<i32>,
    #[serde(rename = "projected-field-names")]
    pub projected_field_names: Vec<String>,
    #[serde(rename = "metrics")]
    pub metrics: ::std::collections::HashMap<String, crate::models::MetricResult>,
    #[serde(rename = "metadata", skip_serializing_if = "Option::is_none")]
    pub metadata: Option<::std::collections::HashMap<String, String>>,
}

impl ScanReport {
    pub fn new(
        table_name: String,
        snapshot_id: i64,
        filter: crate::models::Expression,
        schema_id: i32,
        projected_field_ids: Vec<i32>,
        projected_field_names: Vec<String>,
        metrics: ::std::collections::HashMap<String, crate::models::MetricResult>,
    ) -> ScanReport {
        ScanReport {
            table_name,
            snapshot_id,
            filter: Box::new(filter),
            schema_id,
            projected_field_ids,
            projected_field_names,
            metrics,
            metadata: None,
        }
    }
}
