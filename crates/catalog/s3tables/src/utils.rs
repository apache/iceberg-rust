use std::collections::HashMap;

use aws_config::default_provider::endpoint_url;
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_s3tables::config::Credentials;

/// Property aws profile name
pub const AWS_PROFILE_NAME: &str = "profile_name";
/// Property aws region
pub const AWS_REGION_NAME: &str = "region_name";
/// Property aws access key
pub const AWS_ACCESS_KEY_ID: &str = "aws_access_key_id";
/// Property aws secret access key
pub const AWS_SECRET_ACCESS_KEY: &str = "aws_secret_access_key";
/// Property aws session token
pub const AWS_SESSION_TOKEN: &str = "aws_session_token";

/// Creates an aws sdk configuration based on
/// provided properties and an optional endpoint URL.
pub(crate) async fn create_sdk_config(
    properties: &HashMap<String, String>,
    endpoint_url: Option<String>,
) -> SdkConfig {
    let mut config = aws_config::defaults(BehaviorVersion::latest());

    if properties.is_empty() {
        return config.load().await;
    }

    if let Some(endpoint_url) = endpoint_url {
        config = config.endpoint_url(endpoint_url);
    }

    if let (Some(access_key), Some(secret_key)) = (
        properties.get(AWS_ACCESS_KEY_ID),
        properties.get(AWS_SECRET_ACCESS_KEY),
    ) {
        let session_token = properties.get(AWS_SESSION_TOKEN).cloned();
        let credentials_provider =
            Credentials::new(access_key, secret_key, session_token, None, "properties");

        config = config.credentials_provider(credentials_provider)
    };

    if let Some(profile_name) = properties.get(AWS_PROFILE_NAME) {
        config = config.profile_name(profile_name);
    }

    if let Some(region_name) = properties.get(AWS_REGION_NAME) {
        let region = Region::new(region_name.clone());
        config = config.region(region);
    }

    config.load().await
}
