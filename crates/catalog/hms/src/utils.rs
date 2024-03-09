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

use anyhow::anyhow;
use hive_metastore::{Database, PrincipalType};
use iceberg::{Error, ErrorKind, NamespaceIdent, Result};
use pilota::{AHashMap, FastStr};
use std::collections::HashMap;
use std::fmt::Debug;
use std::io;

/// hive.metastore.database.owner setting
pub const HMS_DB_OWNER: &str = "hive.metastore.database.owner";
/// hive.metastore.database.owner default setting
pub const HMS_DEFAULT_DB_OWNER: &str = "user.name";
/// hive.metastore.database.owner-type setting
pub const HMS_DB_OWNER_TYPE: &str = "hive.metastore.database.owner-type";
/// hive metatore `description` property
pub const COMMENT: &str = "comment";
/// hive metatore `location` property
pub const LOCATION: &str = "location";

/// Format a thrift error into iceberg error.
pub fn from_thrift_error<T>(error: volo_thrift::error::ResponseError<T>) -> Error
where
    T: Debug,
{
    Error::new(
        ErrorKind::Unexpected,
        "operation failed for hitting thrift error".to_string(),
    )
    .with_source(anyhow!("thrift error: {:?}", error))
}

/// Format an io error into iceberg error.
pub fn from_io_error(error: io::Error) -> Error {
    Error::new(
        ErrorKind::Unexpected,
        "operation failed for hitting io error".to_string(),
    )
    .with_source(error)
}

/// Create and extract properties from `hive_metastore::hms::Database`.
pub fn properties_from_database(database: &Database) -> HashMap<String, String> {
    let mut properties = HashMap::new();

    if let Some(description) = &database.description {
        properties.insert(COMMENT.to_string(), description.to_string());
    };

    if let Some(location) = &database.location_uri {
        properties.insert(LOCATION.to_string(), location.to_string());
    };

    if let Some(owner) = &database.owner_name {
        properties.insert(HMS_DB_OWNER.to_string(), owner.to_string());
    };

    if let Some(owner_type) = &database.owner_type {
        let value = match owner_type {
            PrincipalType::User => "User",
            PrincipalType::Group => "Group",
            PrincipalType::Role => "Role",
        };

        properties.insert(HMS_DB_OWNER_TYPE.to_string(), value.to_string());
    };

    if let Some(params) = &database.parameters {
        params.iter().for_each(|(k, v)| {
            properties.insert(k.clone().into(), v.clone().into());
        });
    };

    properties
}

/// Converts name and properties into `hive_metastore::hms::Database`
/// after validating the `namespace` and `owner-settings`.
pub fn convert_to_database(
    namespace: &NamespaceIdent,
    properties: &HashMap<String, String>,
) -> Result<Database> {
    let name = validate_namespace(namespace)?;
    validate_owner_settings(properties)?;

    let mut db = Database::default();
    let mut parameters = AHashMap::new();

    db.name = Some(name.into());

    for (k, v) in properties {
        match k.as_str() {
            COMMENT => db.description = Some(v.clone().into()),
            LOCATION => db.location_uri = Some(format_location_uri(v.clone()).into()),
            HMS_DB_OWNER => db.owner_name = Some(v.clone().into()),
            HMS_DB_OWNER_TYPE => {
                let owner_type = match v.to_lowercase().as_str() {
                    "user" => PrincipalType::User,
                    "group" => PrincipalType::Group,
                    "role" => PrincipalType::Role,
                    _ => {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!("Invalid value for setting 'owner_type': {}", v),
                        ))
                    }
                };
                db.owner_type = Some(owner_type);
            }
            _ => {
                parameters.insert(
                    FastStr::from_string(k.clone()),
                    FastStr::from_string(v.clone()),
                );
            }
        }
    }

    db.parameters = Some(parameters);

    // Set default owner, if none provided
    // https://github.com/apache/iceberg/blob/main/hive-metastore/src/main/java/org/apache/iceberg/hive/HiveHadoopUtil.java#L44
    if db.owner_name.is_none() {
        db.owner_name = Some(HMS_DEFAULT_DB_OWNER.into());
        db.owner_type = Some(PrincipalType::User);
    }

    Ok(db)
}

/// Checks if provided `NamespaceIdent` is valid.
pub fn validate_namespace(namespace: &NamespaceIdent) -> Result<String> {
    let name = namespace.as_ref();

    if name.len() != 1 {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "Invalid database, hierarchical namespaces are not supported",
        ));
    }

    let name = name[0].clone();

    if name.is_empty() {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "Invalid database, provided namespace is empty.",
        ));
    }

    Ok(name)
}

/// Formats location_uri by e.g. removing trailing slashes.
fn format_location_uri(location: String) -> String {
    let mut location = location;

    if !location.starts_with('/') {
        location = format!("/{}", location);
    }

    if location.ends_with('/') && location.len() > 1 {
        location.pop();
    }

    location
}

/// Checks if `owner-settings` are valid.
/// If `owner_type` is set, then `owner` must also be set.
fn validate_owner_settings(properties: &HashMap<String, String>) -> Result<()> {
    let owner_is_set = properties.get(HMS_DB_OWNER).is_some();
    let owner_type_is_set = properties.get(HMS_DB_OWNER_TYPE).is_some();

    if owner_type_is_set && !owner_is_set {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Setting '{}' without setting '{}' is not allowed",
                HMS_DB_OWNER_TYPE, HMS_DB_OWNER
            ),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use iceberg::{Namespace, NamespaceIdent};

    use super::*;

    #[test]
    fn test_properties_from_database() -> Result<()> {
        let ns = NamespaceIdent::new("my_namespace".into());
        let properties = HashMap::from([
            (COMMENT.to_string(), "my_description".to_string()),
            (LOCATION.to_string(), "/my_location".to_string()),
            (HMS_DB_OWNER.to_string(), "apache".to_string()),
            (HMS_DB_OWNER_TYPE.to_string(), "User".to_string()),
            ("key1".to_string(), "value1".to_string()),
        ]);

        let db = convert_to_database(&ns, &properties)?;

        let expected = properties_from_database(&db);

        assert_eq!(expected, properties);

        Ok(())
    }

    #[test]
    fn test_validate_owner_settings() {
        let valid = HashMap::from([
            (HMS_DB_OWNER.to_string(), "apache".to_string()),
            (HMS_DB_OWNER_TYPE.to_string(), "user".to_string()),
        ]);
        let invalid = HashMap::from([(HMS_DB_OWNER_TYPE.to_string(), "user".to_string())]);

        assert!(validate_owner_settings(&valid).is_ok());
        assert!(validate_owner_settings(&invalid).is_err());
    }

    #[test]
    fn test_convert_to_database() -> Result<()> {
        let ns = NamespaceIdent::new("my_namespace".into());
        let properties = HashMap::from([
            (COMMENT.to_string(), "my_description".to_string()),
            (LOCATION.to_string(), "my_location".to_string()),
            (HMS_DB_OWNER.to_string(), "apache".to_string()),
            (HMS_DB_OWNER_TYPE.to_string(), "user".to_string()),
            ("key1".to_string(), "value1".to_string()),
        ]);

        let db = convert_to_database(&ns, &properties)?;

        assert_eq!(db.name, Some(FastStr::from("my_namespace")));
        assert_eq!(db.description, Some(FastStr::from("my_description")));
        assert_eq!(db.owner_name, Some(FastStr::from("apache")));
        assert_eq!(db.owner_type, Some(PrincipalType::User));

        if let Some(params) = db.parameters {
            assert_eq!(params.get("key1"), Some(&FastStr::from("value1")));
        }

        Ok(())
    }

    #[test]
    fn test_convert_to_database_with_default_user() -> Result<()> {
        let ns = NamespaceIdent::new("my_namespace".into());
        let properties = HashMap::new();

        let db = convert_to_database(&ns, &properties)?;

        assert_eq!(db.name, Some(FastStr::from("my_namespace")));
        assert_eq!(db.owner_name, Some(FastStr::from(HMS_DEFAULT_DB_OWNER)));
        assert_eq!(db.owner_type, Some(PrincipalType::User));

        Ok(())
    }

    #[test]
    fn test_validate_namespace() {
        let valid_ns = Namespace::new(NamespaceIdent::new("ns".to_string()));
        let empty_ns = Namespace::new(NamespaceIdent::new("".to_string()));
        let hierarchical_ns = Namespace::new(
            NamespaceIdent::from_vec(vec!["level1".to_string(), "level2".to_string()]).unwrap(),
        );

        let valid = validate_namespace(valid_ns.name());
        let empty = validate_namespace(empty_ns.name());
        let hierarchical = validate_namespace(hierarchical_ns.name());

        assert!(valid.is_ok());
        assert!(empty.is_err());
        assert!(hierarchical.is_err());
    }

    #[test]
    fn test_format_location_uri() {
        let inputs = vec!["iceberg", "is/", "/nice/", "really/nice/", "/"];
        let outputs = vec!["/iceberg", "/is", "/nice", "/really/nice", "/"];

        inputs.into_iter().zip(outputs).for_each(|(inp, out)| {
            let location = format_location_uri(inp.to_string());
            assert_eq!(location, out);
        })
    }
}
