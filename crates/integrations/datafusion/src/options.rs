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

use std::any::Any;
use std::collections::HashMap;

use datafusion::catalog::Session as DFSession;
use datafusion::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use datafusion::error::{DataFusionError, Result as DFResult};
use iceberg::SessionContext;
use iceberg::sensitive::SensitiveString;

/// Iceberg-specific DataFusion options.
///
/// Register these options with
/// [`SessionConfig::with_option_extension`](datafusion::execution::config::SessionConfig::with_option_extension)
/// to make them available to Iceberg's DataFusion integration.
/// String-based configuration APIs use `iceberg.identity`,
/// `iceberg.properties.<key>`, and `iceberg.credentials.<key>`.
/// [`ExtensionOptions::entries`] includes credential values so callers can
/// serialize the complete extension and must therefore be treated as sensitive.
#[derive(Clone, Debug, Default)]
pub struct IcebergOptions {
    /// Optional identity used when deriving the Iceberg session context.
    pub identity: Option<String>,

    /// Non-sensitive properties propagated to the Iceberg session context.
    pub properties: HashMap<String, String>,

    /// Sensitive credentials propagated to the Iceberg session context.
    pub credentials: HashMap<String, SensitiveString>,
}

impl ConfigExtension for IcebergOptions {
    const PREFIX: &'static str = "iceberg";
}

impl ExtensionOptions for IcebergOptions {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> DFResult<()> {
        match key.split_once('.') {
            None if key == "identity" => {
                self.identity = Some(value.to_string());
                Ok(())
            }
            Some(("properties", property)) if !property.is_empty() => {
                self.properties
                    .insert(property.to_string(), value.to_string());
                Ok(())
            }
            Some(("credentials", credential)) if !credential.is_empty() => {
                self.credentials.insert(
                    credential.to_string(),
                    SensitiveString::from(value.to_string()),
                );
                Ok(())
            }
            _ => Err(DataFusionError::Configuration(format!(
                "Config value \"{key}\" not found on IcebergOptions"
            ))),
        }
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        let mut entries = vec![ConfigEntry {
            key: "identity".to_string(),
            value: self.identity.clone(),
            description: "Optional identity used when deriving the Iceberg session context.",
        }];

        entries.extend(self.properties.iter().map(|(key, value)| ConfigEntry {
            key: format!("properties.{key}"),
            value: Some(value.clone()),
            description: "Non-sensitive property propagated to the Iceberg session context.",
        }));

        entries.extend(self.credentials.iter().map(|(key, value)| ConfigEntry {
            key: format!("credentials.{key}"),
            value: Some(value.expose().to_string()),
            description: "Sensitive credential propagated to the Iceberg session context.",
        }));

        entries
    }
}

/// Derives an Iceberg session context from a DataFusion session and its
/// configured [`IcebergOptions`], if registered.
pub(crate) fn resolve_session_context(session: &dyn DFSession) -> Option<SessionContext> {
    let options = session
        .config()
        .options()
        .extensions
        .get::<IcebergOptions>()?;

    let builder = SessionContext::builder()
        .session_id(session.session_id().to_string())
        .properties(options.properties.clone())
        .credentials(options.credentials.clone());

    let context = if let Some(identity) = &options.identity {
        builder.identity(identity.to_string()).build()
    } else {
        builder.build()
    };

    Some(context)
}

#[cfg(test)]
mod tests {
    use datafusion::execution::config::SessionConfig;
    use datafusion::prelude::SessionContext as DFSessionContext;

    use super::*;

    #[test]
    fn test_resolve_session_context_distinguishes_missing_and_default_options() {
        let session_without_options = DFSessionContext::new();
        assert!(resolve_session_context(&session_without_options.state()).is_none());

        let config = SessionConfig::new().with_option_extension(IcebergOptions::default());
        let session_with_default_options = DFSessionContext::new_with_config(config);
        let context = resolve_session_context(&session_with_default_options.state()).unwrap();

        assert_eq!(
            context.session_id(),
            session_with_default_options.session_id()
        );
        assert!(context.identity().is_none());
        assert!(context.properties().is_empty());
        assert!(context.credentials().is_empty());
    }

    #[test]
    fn test_config_extension_sets_and_lists_options() {
        let secret = "credential-listed-for-string-based-configuration";
        let mut config = SessionConfig::new().with_option_extension(IcebergOptions::default());
        config
            .options_mut()
            .set("iceberg.identity", "user123")
            .unwrap();
        config
            .options_mut()
            .set("iceberg.properties.s3.region", "us-east-1")
            .unwrap();
        config
            .options_mut()
            .set("iceberg.credentials.token", secret)
            .unwrap();

        let options = config.options().extensions.get::<IcebergOptions>().unwrap();
        assert_eq!(options.identity.as_deref(), Some("user123"));
        assert_eq!(
            options.properties.get("s3.region").map(String::as_str),
            Some("us-east-1")
        );
        assert_eq!(
            options.credentials.get("token"),
            Some(&SensitiveString::from(secret.to_string()))
        );

        let entries = options.entries();
        assert_eq!(entries, vec![
            ConfigEntry {
                key: "identity".to_string(),
                value: Some("user123".to_string()),
                description: "Optional identity used when deriving the Iceberg session context.",
            },
            ConfigEntry {
                key: "properties.s3.region".to_string(),
                value: Some("us-east-1".to_string()),
                description: "Non-sensitive property propagated to the Iceberg session context.",
            },
            ConfigEntry {
                key: "credentials.token".to_string(),
                value: Some(secret.to_string()),
                description: "Sensitive credential propagated to the Iceberg session context.",
            },
        ]);
    }

    #[test]
    fn test_debug_redacts_credentials() {
        let secret = "credential-that-must-not-be-logged";
        let options = IcebergOptions {
            credentials: HashMap::from([(
                "token".to_string(),
                SensitiveString::from(secret.to_string()),
            )]),
            ..Default::default()
        };

        let debug_output = format!("{options:?}");
        assert!(!debug_output.contains(secret));
        assert!(debug_output.contains("[REDACTED]"));
    }
}
