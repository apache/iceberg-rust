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


