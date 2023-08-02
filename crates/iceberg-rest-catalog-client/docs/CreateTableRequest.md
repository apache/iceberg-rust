# CreateTableRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **String** |  | 
**location** | Option<**String**> |  | [optional]
**schema** | [**crate::models::Schema**](Schema.md) |  | 
**partition_spec** | Option<[**crate::models::PartitionSpec**](PartitionSpec.md)> |  | [optional]
**write_order** | Option<[**crate::models::SortOrder**](SortOrder.md)> |  | [optional]
**stage_create** | Option<**bool**> |  | [optional]
**properties** | Option<**::std::collections::HashMap<String, String>**> |  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


