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

# TableUpdate

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**action** | **String** |  | 
**format_version** | **i32** |  | 
**schema** | [**crate::models::Schema**](Schema.md) |  | 
**schema_id** | **i32** | Schema ID to set as current, or -1 to set last added schema | 
**spec** | [**crate::models::PartitionSpec**](PartitionSpec.md) |  | 
**spec_id** | **i32** | Partition spec ID to set as the default, or -1 to set last added spec | 
**sort_order** | [**crate::models::SortOrder**](SortOrder.md) |  | 
**sort_order_id** | **i32** | Sort order ID to set as the default, or -1 to set last added sort order | 
**snapshot** | [**crate::models::Snapshot**](Snapshot.md) |  | 
**r#type** | **String** |  | 
**snapshot_id** | **i64** |  | 
**max_ref_age_ms** | Option<**i64**> |  | [optional]
**max_snapshot_age_ms** | Option<**i64**> |  | [optional]
**min_snapshots_to_keep** | Option<**i32**> |  | [optional]
**ref_name** | **String** |  | 
**snapshot_ids** | **Vec<i64>** |  | 
**location** | **String** |  | 
**updates** | **::std::collections::HashMap<String, String>** |  | 
**removals** | **Vec<String>** |  | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


