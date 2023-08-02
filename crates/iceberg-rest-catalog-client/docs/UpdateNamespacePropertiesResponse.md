# UpdateNamespacePropertiesResponse

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**updated** | **Vec<String>** | List of property keys that were added or updated | 
**removed** | **Vec<String>** | List of properties that were removed | 
**missing** | Option<**Vec<String>**> | List of properties requested for removal that were not found in the namespace's properties. Represents a partial success response. Server's do not need to implement this. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


