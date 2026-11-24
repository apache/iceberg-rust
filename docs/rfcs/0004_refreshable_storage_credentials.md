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

# RFC: Refreshable Vended Storage Credentials

**Status:** Draft  
**Target:** Apache Iceberg Rust (`iceberg-rust`)  
**Extends:** [RFC 0002: Making Storage a Trait](0002_storage_trait.md)  
**Tracking issue:** [#2931](https://github.com/apache/iceberg-rust/issues/2931)  
**Reference implementation:** [#2932](https://github.com/apache/iceberg-rust/pull/2932)

## 1. Motivation and Scope

REST catalogs vend short-lived storage credentials with a table, and may
advertise an endpoint (`GET .../tables/{table}/credentials`) for refreshing
them. Iceberg Java refreshes them before they expire. `iceberg-rust` uses the
initially vended credentials for the lifetime of a `FileIO`, so jobs that
outlive the credentials fail with authorization errors from the object store.

This RFC covers refresh for the OpenDAL S3, GCS and ADLS backends, driven by
the REST catalog's `client.refresh-credentials-endpoint`,
`gcs.oauth2.refresh-credentials-endpoint` and
`adls.refresh-credentials-endpoint` table properties. Refreshing the catalog's
own OAuth2 token is out of scope.

It builds on [#2651](https://github.com/apache/iceberg-rust/pull/2651), which
passes `LoadTableResult.storage-credentials` to the storage factory in
`StorageConfig` and routes each path to a storage built for the credential
with the longest matching prefix.

## 2. What Iceberg Java Does

`S3FileIO.clientForStoragePath` routes each path to the client of the longest
matching `storageCredentials` prefix, or to the `s3` root client. The clients
are built once, when the `FileIO` first needs them (`S3FileIO.clientByPrefix`).

Each client refreshes through its own `VendedCredentialsProvider`. On refresh,
the provider keeps the `s3*` credentials of the response and requires exactly
one of them, whatever its prefix (`VendedCredentialsProvider.refreshCredential`).
GCS does the same for `gs*` (`OAuth2RefreshCredentialsHandler`). ADLS requires
one credential per storage account (`VendedAdlsCredentialProvider`), so several
accounts work. A table vended more than one S3 or GCS credential therefore
fails at its first refresh.

`AwsClientProperties.credentialsProvider` installs the refreshing provider when
the endpoint is set and `client.refresh-credentials-enabled` is absent or
`true`. It takes precedence over static keys and the default credential chain.
Static keys only seed it, and only when the access key, secret, session token
and an expiry more than five minutes away are all present.

Executors rebuild the provider from the `FileIO` properties with their own REST
client. The provider authenticates with `AuthManager.catalogSession` on those
properties, not with a table or contextual session.

## 3. Decision 1: A Core Credential Provider

Add a provider trait to `iceberg::io`. Catalogs implement it, and storage
backends call it:

```rust
#[async_trait]
pub trait StorageCredentialProvider: Debug + Send + Sync {
    /// Whether this provider supplies credentials for `path`.
    fn supports_path(&self, path: &str) -> bool;

    /// The current credential for the storage prefix `prefix`.
    async fn load_credential(&self, prefix: &str) -> Result<Arc<StorageCredential>>;

    /// A serializable recipe that rebuilds this provider (Decision 4).
    fn factory(&self) -> Result<Option<Arc<dyn StorageCredentialProviderFactory>>>;
}
```

`StorageCredential` is the `{ prefix, config }` type that #2651 adds, so vended
and refreshed credentials share one type. Backends parse their own properties
from `config`, such as `s3.session-token-expires-at-ms`, so the trait does not
change per backend.

`FileIOBuilder::with_credential_provider` attaches a provider, and
`StorageFactory::build_with_credential_provider` passes it to the backend. The
default implementation ignores the provider and warns once per factory type,
so a wrapping factory that does not forward it is visible in the logs.

The refresh state belongs to the catalog: its URI, auth manager, sessions and
endpoint. The consumer is the request signer in a backend. Refresh inside each
`StorageFactory` would require every backend crate to depend on the REST crate
or to reimplement REST authentication. Java appears to refresh in the backend,
but its properties name a provider class that builds its own REST client. A
trait object with a serializable factory is the Rust equivalent.
`CustomAwsCredentialLoader` does not generalize, because a catalog would have
to know which backend it was given.

## 4. Decision 2: Routing Stays in the Storage From #2651

The OpenDAL factory already builds a storage per vended prefix and a
props-only default storage. With a provider attached, it binds each storage to
its prefix, and the storage asks the provider for that prefix only:

- A per-prefix storage asks for its own prefix.
- The default storage asks for its scheme, such as `s3`. The provider serves the
  credential for the scheme if the response has one, and otherwise the only
  credential for that cloud, as Java does. If there are several, it fails.

The provider fetches all of a cloud's credentials in one request and caches
them by prefix, so one request refreshes every prefix of a table. Unlike Java, a table vended
several S3 or GCS credentials keeps all of them across refreshes.

Backends no longer select prefixes. A bulk delete is already batched per
storage, so the per-credential batching in the reference implementation
(`BatchCredential`, `superseded_batches`) is not needed. The set of prefixes is
fixed when the table is loaded, as in Java.

The alternative, used by the reference implementation, routes inside the
provider: backends pass the path, the provider picks the longest covering
credential, and bulk deletes flush a batch when a narrower credential appears.
It handles prefixes that change at runtime, but duplicates #2651's routing and
most of the reference implementation's complexity comes from it.

This decision depends on #2651, which is still under review. If it stalls,
#2976 can include the per-prefix storage itself.

## 5. Decision 3: The Provider Owns Refresh Timing

The provider decides when to fetch. It seeds its cache from
`StorageConfig::storage_credentials()` and the flat table properties, so the
first access needs no request. It prefetches five minutes before expiry,
capped at half the remaining lifetime, so short-lived credentials are not
refetched on every access. It runs one refresh per cloud at a time, times
requests out after 30 seconds, and backs off with jitter after failures. A
response that vends nothing for a prefix delays the next request for that
prefix without backing off the others.

Backends treat the provider as their only credential source. reqsign asks
again within two minutes of expiry for S3 and GCS and within 20 seconds for
ADLS, and each request is a cache hit in the provider. The trait documents
that providers must cache and must return credentials with more than two
minutes left.

Java also refreshes the `FileIO` credential list on a timer, so that engines
ship current credentials to executors. With Decision 4 a worker rebuilds its
own provider, so `FileIO` does not need a background task.

## 6. Decision 4: What Is Serialized to Workers

`FileIO::serialize_all` serializes the provider as its
`StorageCredentialProviderFactory`, registered with `typetag`. The REST factory
contains the catalog URI, the table identifier, the resolved auth type and the
table's authentication properties. A worker that links the REST crate rebuilds
the provider and connects to the catalog itself.

`factory()` decides what happens to a provider that cannot be rebuilt:

| `factory()` returns | `serialize_all` | Example |
|---|---|---|
| `Ok(Some(factory))` | serializes the factory | the REST provider with the built-in OAuth2 or no-auth manager |
| `Ok(None)` | omits the provider and warns | the REST provider with an injected `AuthManager` |
| `Err(error)` (the default) | fails | a provider wrapping an SDK client that exists only in the application |

A provider is therefore dropped only when its author chose that, or when the
caller asks for it with `FileIO::without_credential_provider`. The REST
provider returns `Ok(None)` for an injected `AuthManager` because workers then
behave as they do today, using the vended credentials. Failing instead would
break callers that serialize such a `FileIO` now.

`deserialize_all` drops a provider whose factory is not linked or cannot be
rebuilt, warns, and uses the credentials in the configuration. This lets a
worker without the REST crate run a short task instead of failing.
`FileIO::has_credential_provider` lets callers check the result.

Serialized bytes already contain vended credentials. A REST factory adds the
catalog authentication it needs, such as a table token, as Java's properties
do, and `serialize_all` documents that the bytes must be protected.

A registry of auth managers built from properties, as Java's
`AuthManagers.loadAuthManager` does, would let custom managers be rebuilt on
workers too. Workers would still not get the contextual identity, and managers
with state only in the application would still return `Ok(None)`, so it is
left for later.

## 7. Decision 5: Refresh Is Enabled by the Catalog

As in Java, a backend uses the provider when the catalog advertises a refresh
endpoint and the matching `*-refresh-credentials-enabled` property is absent or
`true`. For the paths it supports, the provider replaces the backend's
credential chain without a fallback to static keys or ambient credentials, so
a failed refresh cannot switch identity.

A complete temporary credential in the table properties is used until it is
due. Keys without a session token or expiry are fetched first, as in Java, but
are served while the refresh fails, so tables whose catalog advertises an
endpoint and vends static keys keep working as they do today.

Java has no opt-in switch, and the catalog has already declared that the
table's credentials are vended, so this RFC does not add one.

## 8. Decision 6: REST Authentication API

Add `AuthManager::table_session(client, table, props, parent)`, with a default
that returns `parent`. It corresponds to Java's `tableSession(TableIdentifier,
Map, AuthSession)` and does not break existing implementations. Java's second
overload, `tableSession(RESTClient, Map)`, for components other than the
catalog, is not added.

The parent is the contextual session ([#3081](https://github.com/apache/iceberg-rust/pull/3081))
of the operation that loaded the table, so refreshes keep the caller's
identity. Java's refresher uses the catalog session instead. The session is
derived only when a provider is built. Without the method, an injected manager
could not scope credential requests to a table.

## 9. Deviations From Java

- A table keeps several S3 or GCS credentials across refreshes.
- Prefetch is capped at half the remaining lifetime.
- Static seed keys are served while refresh fails.
- Refresh requests use a table session derived from the contextual session,
  not the catalog session.

## 10. Adoption Plan

1. Core and OpenDAL S3 and GCS ([#2976](https://github.com/apache/iceberg-rust/pull/2976)),
   reworked for prefix-addressed providers on top of #2651.
2. OpenDAL ADLS, with the OpenDAL 0.59 upgrade.
3. The REST provider and `AuthManager::table_session`.

## 11. Open Questions

- Should a table whose vended prefixes change between refreshes be reloaded
  automatically?
- Should later table operations use the table session, as Java does?
- Should the credential request timeout be configurable?
