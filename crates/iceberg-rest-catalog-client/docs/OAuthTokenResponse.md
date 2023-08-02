# OAuthTokenResponse

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**access_token** | **String** | The access token, for client credentials or token exchange | 
**token_type** | **String** | Access token type for client credentials or token exchange  See https://datatracker.ietf.org/doc/html/rfc6749#section-7.1 | 
**expires_in** | Option<**i32**> | Lifetime of the access token in seconds for client credentials or token exchange | [optional]
**issued_token_type** | Option<[**crate::models::TokenType**](TokenType.md)> |  | [optional]
**refresh_token** | Option<**String**> | Refresh token for client credentials or token exchange | [optional]
**scope** | Option<**String**> | Authorization scope for client credentials or token exchange | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


