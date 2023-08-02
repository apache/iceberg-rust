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

# \OAuth2ApiApi

All URIs are relative to *https://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_token**](OAuth2ApiApi.md#get_token) | **POST** /v1/oauth/tokens | Get a token using an OAuth2 flow



## get_token

> crate::models::OAuthTokenResponse get_token(grant_type, client_id, client_secret, subject_token, subject_token_type, scope, requested_token_type, actor_token, actor_token_type)
Get a token using an OAuth2 flow

Exchange credentials for a token using the OAuth2 client credentials flow or token exchange.  This endpoint is used for three purposes - 1. To exchange client credentials (client ID and secret) for an access token This uses the client credentials flow. 2. To exchange a client token and an identity token for a more specific access token This uses the token exchange flow. 3. To exchange an access token for one with the same claims and a refreshed expiration period This uses the token exchange flow.  For example, a catalog client may be configured with client credentials from the OAuth2 Authorization flow. This client would exchange its client ID and secret for an access token using the client credentials request with this endpoint (1). Subsequent requests would then use that access token.  Some clients may also handle sessions that have additional user context. These clients would use the token exchange flow to exchange a user token (the \"subject\" token) from the session for a more specific access token for that user, using the catalog's access token as the \"actor\" token (2). The user ID token is the \"subject\" token and can be any token type allowed by the OAuth2 token exchange flow, including a unsecured JWT token with a sub claim. This request should use the catalog's bearer token in the \"Authorization\" header.  Clients may also use the token exchange flow to refresh a token that is about to expire by sending a token exchange request (3). The request's \"subject\" token should be the expiring token. This request should use the subject token in the \"Authorization\" header.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**grant_type** | **String** |  | [required] |
**client_id** | **String** | Client ID  This can be sent in the request body, but OAuth2 recommends sending it in a Basic Authorization header. | [required] |
**client_secret** | **String** | Client secret  This can be sent in the request body, but OAuth2 recommends sending it in a Basic Authorization header. | [required] |
**subject_token** | **String** | Subject token for token exchange request | [required] |
**subject_token_type** | [**crate::models::TokenType**](TokenType.md) |  | [required] |
**scope** | Option<**String**> |  |  |
**requested_token_type** | Option<[**crate::models::TokenType**](TokenType.md)> |  |  |
**actor_token** | Option<**String**> | Actor token for token exchange request |  |
**actor_token_type** | Option<[**crate::models::TokenType**](TokenType.md)> |  |  |

### Return type

[**crate::models::OAuthTokenResponse**](OAuthTokenResponse.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

- **Content-Type**: application/x-www-form-urlencoded
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

