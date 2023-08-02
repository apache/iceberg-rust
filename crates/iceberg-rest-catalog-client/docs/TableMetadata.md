# TableMetadata

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**format_version** | **i32** |  | 
**table_uuid** | **String** |  | 
**location** | Option<**String**> |  | [optional]
**last_updated_ms** | Option<**i64**> |  | [optional]
**properties** | Option<**::std::collections::HashMap<String, String>**> |  | [optional]
**schemas** | Option<[**Vec<crate::models::Schema>**](Schema.md)> |  | [optional]
**current_schema_id** | Option<**i32**> |  | [optional]
**last_column_id** | Option<**i32**> |  | [optional]
**partition_specs** | Option<[**Vec<crate::models::PartitionSpec>**](PartitionSpec.md)> |  | [optional]
**default_spec_id** | Option<**i32**> |  | [optional]
**last_partition_id** | Option<**i32**> |  | [optional]
**sort_orders** | Option<[**Vec<crate::models::SortOrder>**](SortOrder.md)> |  | [optional]
**default_sort_order_id** | Option<**i32**> |  | [optional]
**snapshots** | Option<[**Vec<crate::models::Snapshot>**](Snapshot.md)> |  | [optional]
**refs** | Option<[**::std::collections::HashMap<String, crate::models::SnapshotReference>**](SnapshotReference.md)> |  | [optional]
**current_snapshot_id** | Option<**i64**> |  | [optional]
**snapshot_log** | Option<[**Vec<crate::models::SnapshotLogInner>**](SnapshotLog_inner.md)> |  | [optional]
**metadata_log** | Option<[**Vec<crate::models::MetadataLogInner>**](MetadataLog_inner.md)> |  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


