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

# ReportMetricsRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**report_type** | **String** |  | 
**table_name** | **String** |  | 
**snapshot_id** | **i64** |  | 
**filter** | [**crate::models::Expression**](Expression.md) |  | 
**schema_id** | **i32** |  | 
**projected_field_ids** | **Vec<i32>** |  | 
**projected_field_names** | **Vec<String>** |  | 
**metrics** | [**::std::collections::HashMap<String, crate::models::MetricResult>**](MetricResult.md) |  | 
**metadata** | Option<**::std::collections::HashMap<String, String>**> |  | [optional]
**sequence_number** | **i64** |  | 
**operation** | **String** |  | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


