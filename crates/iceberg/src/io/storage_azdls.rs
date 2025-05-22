// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;

use opendal::Configurator;
use opendal::services::AzdlsConfig;
use url::Url;

use crate::{Error, ErrorKind, Result};

/// A connection string.
///
/// This could be used to use FileIO with any adls-compatible object storage
/// service that has a different endpoint (like Azurite).
///
/// Note, this string is parsed first, and any other passed adls.* properties
/// will override values from the connection string.
const ADLS_CONNECTION_STRING: &str = "adls.connection-string";

/// The account that you want to connect to.
pub const ADLS_ACCOUNT_NAME: &str = "adls.account-name";

/// The key to authentication against the account.
pub const ADLS_ACCOUNT_KEY: &str = "adls.account-key";

/// The shared access signature.
pub const ADLS_SAS_TOKEN: &str = "adls.sas-token";

/// The tenant-id.
pub const ADLS_TENANT_ID: &str = "adls.tenant-id";

/// The client-id.
pub const ADLS_CLIENT_ID: &str = "adls.client-id";

/// The client-secret.
pub const ADLS_CLIENT_SECRET: &str = "adls.client-secret";

/// Parses adls.* prefixed configuration properties.
pub(crate) fn azdls_config_parse(mut m: HashMap<String, String>) -> Result<AzdlsConfig> {
    let mut cfg = AzdlsConfig::default();

    if let Some(_conn_str) = m.remove(ADLS_CONNECTION_STRING) {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Azdls: connection string currently not supported",
        ));
    }

    if let Some(account_name) = m.remove(ADLS_ACCOUNT_NAME) {
        cfg.account_name = Some(account_name);
    }

    if let Some(account_key) = m.remove(ADLS_ACCOUNT_KEY) {
        cfg.account_key = Some(account_key);
    }

    if let Some(sas_token) = m.remove(ADLS_SAS_TOKEN) {
        cfg.sas_token = Some(sas_token);
    }

    if let Some(tenant_id) = m.remove(ADLS_TENANT_ID) {
        cfg.tenant_id = Some(tenant_id);
    }

    if let Some(client_id) = m.remove(ADLS_CLIENT_ID) {
        cfg.client_id = Some(client_id);
    }

    if let Some(client_secret) = m.remove(ADLS_CLIENT_SECRET) {
        cfg.client_secret = Some(client_secret);
    }

    Ok(cfg)
}

pub(crate) fn azdls_config_build(cfg: &AzdlsConfig, path: &str) -> Result<opendal::Operator> {
    let url = Url::parse(path)?;

    // This is ok to be empty, OpenDAL will use the default filesystem.
    let filesystem = url.username();

    let builder = cfg.clone().into_builder().filesystem(filesystem);

    Ok(opendal::Operator::new(builder)?.finish())
}

#[cfg(test)]
mod tests {
    use opendal::services::AzdlsConfig;

    use crate::io::{azdls_config_build, azdls_config_parse};

    #[test]
    fn test_azdls_config_parse() {
        let mut m = std::collections::HashMap::new();
        m.insert(super::ADLS_ACCOUNT_NAME.to_string(), "test".to_string());
        m.insert(super::ADLS_ACCOUNT_KEY.to_string(), "secret".to_string());

        let config = azdls_config_parse(m).unwrap();

        assert_eq!(config.account_name, Some("test".to_string()));
        assert_eq!(config.account_key, Some("secret".to_string()));
    }

    #[test]
    fn test_azdls_config_build() {
        let mut config = AzdlsConfig::default();
        config.endpoint = Some("https://myaccount.dfs.core.windows.net".to_string());

        let path = "abfss://myfs@myaccount.dfs.core.windows.net/mydir/myfile.parquet";

        let op = azdls_config_build(&config, path).unwrap();
        assert_eq!(op.info().name(), "myfs");
    }
}
