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

# \CatalogApiApi

All URIs are relative to *https://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_namespace**](CatalogApiApi.md#create_namespace) | **POST** /v1/{prefix}/namespaces | Create a namespace
[**create_table**](CatalogApiApi.md#create_table) | **POST** /v1/{prefix}/namespaces/{namespace}/tables | Create a table in the given namespace
[**drop_namespace**](CatalogApiApi.md#drop_namespace) | **DELETE** /v1/{prefix}/namespaces/{namespace} | Drop a namespace from the catalog. Namespace must be empty.
[**drop_table**](CatalogApiApi.md#drop_table) | **DELETE** /v1/{prefix}/namespaces/{namespace}/tables/{table} | Drop a table from the catalog
[**list_namespaces**](CatalogApiApi.md#list_namespaces) | **GET** /v1/{prefix}/namespaces | List namespaces, optionally providing a parent namespace to list underneath
[**list_tables**](CatalogApiApi.md#list_tables) | **GET** /v1/{prefix}/namespaces/{namespace}/tables | List all table identifiers underneath a given namespace
[**load_namespace_metadata**](CatalogApiApi.md#load_namespace_metadata) | **GET** /v1/{prefix}/namespaces/{namespace} | Load the metadata properties for a namespace
[**load_table**](CatalogApiApi.md#load_table) | **GET** /v1/{prefix}/namespaces/{namespace}/tables/{table} | Load a table from the catalog
[**rename_table**](CatalogApiApi.md#rename_table) | **POST** /v1/{prefix}/tables/rename | Rename a table from its current name to a new name
[**report_metrics**](CatalogApiApi.md#report_metrics) | **POST** /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics | Send a metrics report to this endpoint to be processed by the backend
[**table_exists**](CatalogApiApi.md#table_exists) | **HEAD** /v1/{prefix}/namespaces/{namespace}/tables/{table} | Check if a table exists
[**update_properties**](CatalogApiApi.md#update_properties) | **POST** /v1/{prefix}/namespaces/{namespace}/properties | Set or remove properties on a namespace
[**update_table**](CatalogApiApi.md#update_table) | **POST** /v1/{prefix}/namespaces/{namespace}/tables/{table} | Commit updates to a table



## create_namespace

> crate::models::CreateNamespaceResponse create_namespace(prefix, create_namespace_request)
Create a namespace

Create a namespace, with an optional set of properties. The server might also add properties, such as `last_modified_time` etc.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**prefix** | **String** | An optional prefix in the path | [required] |
**create_namespace_request** | Option<[**CreateNamespaceRequest**](CreateNamespaceRequest.md)> |  |  |

### Return type

[**crate::models::CreateNamespaceResponse**](CreateNamespaceResponse.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## create_table

> crate::models::LoadTableResult create_table(prefix, namespace, create_table_request)
Create a table in the given namespace

Create a table or start a create transaction, like atomic CTAS.  If `stage-create` is false, the table is created immediately.  If `stage-create` is true, the table is not created, but table metadata is initialized and returned. The service should prepare as needed for a commit to the table commit endpoint to complete the create transaction. The client uses the returned metadata to begin a transaction. To commit the transaction, the client sends all create and subsequent changes to the table commit route. Changes from the table create operation include changes like AddSchemaUpdate and SetCurrentSchemaUpdate that set the initial table state.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**prefix** | **String** | An optional prefix in the path | [required] |
**namespace** | **String** | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte. | [required] |
**create_table_request** | Option<[**CreateTableRequest**](CreateTableRequest.md)> |  |  |

### Return type

[**crate::models::LoadTableResult**](LoadTableResult.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## drop_namespace

> drop_namespace(prefix, namespace)
Drop a namespace from the catalog. Namespace must be empty.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**prefix** | **String** | An optional prefix in the path | [required] |
**namespace** | **String** | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte. | [required] |

### Return type

 (empty response body)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## drop_table

> drop_table(prefix, namespace, table, purge_requested)
Drop a table from the catalog

Remove a table from the catalog

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**prefix** | **String** | An optional prefix in the path | [required] |
**namespace** | **String** | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte. | [required] |
**table** | **String** | A table name | [required] |
**purge_requested** | Option<**bool**> | Whether the user requested to purge the underlying table's data and metadata |  |[default to false]

### Return type

 (empty response body)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_namespaces

> crate::models::ListNamespacesResponse list_namespaces(prefix, parent)
List namespaces, optionally providing a parent namespace to list underneath

List all namespaces at a certain level, optionally starting from a given parent namespace. For example, if table accounting.tax.paid exists, using 'SELECT NAMESPACE IN accounting' would translate into `GET /namespaces?parent=accounting` and must return a namespace, [\"accounting\", \"tax\"]. If `parent` is not provided, all top-level namespaces should be listed.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**prefix** | **String** | An optional prefix in the path | [required] |
**parent** | Option<**String**> | An optional namespace, underneath which to list namespaces. If not provided or empty, all top-level namespaces should be listed. If parent is a multipart namespace, the parts must be separated by the unit separator (`0x1F`) byte. |  |

### Return type

[**crate::models::ListNamespacesResponse**](ListNamespacesResponse.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_tables

> crate::models::ListTablesResponse list_tables(prefix, namespace)
List all table identifiers underneath a given namespace

Return all table identifiers under this namespace

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**prefix** | **String** | An optional prefix in the path | [required] |
**namespace** | **String** | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte. | [required] |

### Return type

[**crate::models::ListTablesResponse**](ListTablesResponse.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## load_namespace_metadata

> crate::models::GetNamespaceResponse load_namespace_metadata(prefix, namespace)
Load the metadata properties for a namespace

Return all stored metadata properties for a given namespace

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**prefix** | **String** | An optional prefix in the path | [required] |
**namespace** | **String** | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte. | [required] |

### Return type

[**crate::models::GetNamespaceResponse**](GetNamespaceResponse.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## load_table

> crate::models::LoadTableResult load_table(prefix, namespace, table, snapshots)
Load a table from the catalog

Load a table from the catalog.  The response contains both configuration and table metadata. The configuration, if non-empty is used as additional configuration for the table that overrides catalog configuration. For example, this configuration may change the FileIO implementation to be used for the table.  The response also contains the table's full metadata, matching the table metadata JSON file.  The catalog configuration may contain credentials that should be used for subsequent requests for the table. The configuration key \"token\" is used to pass an access token to be used as a bearer token for table requests. Otherwise, a token may be passed using a RFC 8693 token type as a configuration key. For example, \"urn:ietf:params:oauth:token-type:jwt=<JWT-token>\".

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**prefix** | **String** | An optional prefix in the path | [required] |
**namespace** | **String** | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte. | [required] |
**table** | **String** | A table name | [required] |
**snapshots** | Option<**String**> | The snapshots to return in the body of the metadata. Setting the value to `all` would return the full set of snapshots currently valid for the table. Setting the value to `refs` would load all snapshots referenced by branches or tags. Default if no param is provided is `all`. |  |

### Return type

[**crate::models::LoadTableResult**](LoadTableResult.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## rename_table

> rename_table(prefix, rename_table_request)
Rename a table from its current name to a new name

Rename a table from one identifier to another. It's valid to move a table across namespaces, but the server implementation is not required to support it.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**prefix** | **String** | An optional prefix in the path | [required] |
**rename_table_request** | [**RenameTableRequest**](RenameTableRequest.md) | Current table identifier to rename and new table identifier to rename to | [required] |

### Return type

 (empty response body)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## report_metrics

> report_metrics(prefix, namespace, table, report_metrics_request)
Send a metrics report to this endpoint to be processed by the backend

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**prefix** | **String** | An optional prefix in the path | [required] |
**namespace** | **String** | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte. | [required] |
**table** | **String** | A table name | [required] |
**report_metrics_request** | [**ReportMetricsRequest**](ReportMetricsRequest.md) | The request containing the metrics report to be sent | [required] |

### Return type

 (empty response body)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## table_exists

> table_exists(prefix, namespace, table)
Check if a table exists

Check if a table exists within a given namespace. This request does not return a response body.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**prefix** | **String** | An optional prefix in the path | [required] |
**namespace** | **String** | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte. | [required] |
**table** | **String** | A table name | [required] |

### Return type

 (empty response body)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## update_properties

> crate::models::UpdateNamespacePropertiesResponse update_properties(prefix, namespace, update_namespace_properties_request)
Set or remove properties on a namespace

Set and/or remove properties on a namespace. The request body specifies a list of properties to remove and a map of key value pairs to update. Properties that are not in the request are not modified or removed by this call. Server implementations are not required to support namespace properties.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**prefix** | **String** | An optional prefix in the path | [required] |
**namespace** | **String** | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte. | [required] |
**update_namespace_properties_request** | Option<[**UpdateNamespacePropertiesRequest**](UpdateNamespacePropertiesRequest.md)> |  |  |

### Return type

[**crate::models::UpdateNamespacePropertiesResponse**](UpdateNamespacePropertiesResponse.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## update_table

> crate::models::CommitTableResponse update_table(prefix, namespace, table, commit_table_request)
Commit updates to a table

Commit updates to a table.  Commits have two parts, requirements and updates. Requirements are assertions that will be validated before attempting to make and commit changes. For example, `assert-ref-snapshot-id` will check that a named ref's snapshot ID has a certain value.  Updates are changes to make to table metadata. For example, after asserting that the current main ref is at the expected snapshot, a commit may add a new child snapshot and set the ref to the new snapshot id.  Create table transactions that are started by createTable with `stage-create` set to true are committed using this route. Transactions should include all changes to the table, including table initialization, like AddSchemaUpdate and SetCurrentSchemaUpdate. The `assert-create` requirement is used to ensure that the table was not created concurrently.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**prefix** | **String** | An optional prefix in the path | [required] |
**namespace** | **String** | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte. | [required] |
**table** | **String** | A table name | [required] |
**commit_table_request** | Option<[**CommitTableRequest**](CommitTableRequest.md)> |  |  |

### Return type

[**crate::models::CommitTableResponse**](CommitTableResponse.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

