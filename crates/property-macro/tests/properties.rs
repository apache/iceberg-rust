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
use std::str::FromStr;

use iceberg_property_macro::Properties;
use serde::{Deserialize, Serialize};

const RETRIES: &str = "commit.retry.num-retries";
const OWNER: &str = "owner";
const FORMAT: &str = "write.format.default";
const FANOUT_ENABLED: &str = "write.datafusion.fanout.enabled";
const COLUMN_FPP_PREFIX: &str = "write.parquet.bloom-filter-fpp.column.";
const WIDTH: &str = "dimensions.width";
const HEIGHT: &str = "dimensions.height";
const DEPTH: &str = "dimensions.depth";

fn parse_dimensions(
    properties: &HashMap<String, String>,
    width_key: &str,
    additional_keys: &[&str],
    default: (u64, u64, u64),
) -> Result<(u64, u64, u64), String> {
    if additional_keys.len() != 2 {
        return Err("dimensions require height and depth keys".to_string());
    }
    let parse = |property_key: &str, default| {
        properties
            .get(property_key)
            .map(|value| value.parse::<u64>().map_err(|error| error.to_string()))
            .transpose()
            .map(|value| value.unwrap_or(default))
    };

    Ok((
        parse(width_key, default.0)?,
        parse(additional_keys[0], default.1)?,
        parse(additional_keys[1], default.2)?,
    ))
}

#[derive(Debug, Properties)]
struct TestProperties {
    #[property(key = RETRIES, default = 4, getter)]
    retries: u64,

    #[property(key = OWNER, default = None, getter)]
    owner: Option<String>,

    #[property(key = FORMAT, default = "parquet", getter)]
    format: String,

    #[property(key = FANOUT_ENABLED, default = true, getter)]
    fanout_enabled: bool,

    #[property(
        prefix = COLUMN_FPP_PREFIX,
        getter
    )]
    column_fpp: HashMap<String, f64>,

    #[property(
        key = WIDTH,
        additional_keys = [HEIGHT, DEPTH],
        default = (640, 480, 320),
        parse_properties_with = parse_dimensions,
        getter
    )]
    dimensions: (u64, u64, u64),
}

#[test]
fn reads_defaults_through_generated_getters() {
    let properties = TestProperties::from_properties(&HashMap::new()).unwrap();

    assert_eq!(properties.retries(), 4);
    assert_eq!(properties.owner(), &None);
    assert_eq!(properties.format(), "parquet");
    assert!(properties.fanout_enabled());
    assert!(properties.column_fpp().is_empty());
    assert_eq!(properties.dimensions(), (640, 480, 320));
}

#[test]
fn reads_overrides_and_ignores_unknown_properties() {
    let raw = HashMap::from([
        (RETRIES.to_string(), "8".to_string()),
        (OWNER.to_string(), "iceberg".to_string()),
        (FORMAT.to_string(), "orc".to_string()),
        (FANOUT_ENABLED.to_string(), "FALSE".to_string()),
        (format!("{COLUMN_FPP_PREFIX}id"), "0.01".to_string()),
        (WIDTH.to_string(), "1920".to_string()),
        (HEIGHT.to_string(), "1080".to_string()),
        (DEPTH.to_string(), "720".to_string()),
        ("unknown".to_string(), "ignored".to_string()),
    ]);
    let properties = TestProperties::from_properties(&raw).unwrap();

    assert_eq!(properties.retries(), 8);
    assert_eq!(properties.owner().as_deref(), Some("iceberg"));
    assert_eq!(properties.format(), "orc");
    assert!(!properties.fanout_enabled());
    assert_eq!(properties.column_fpp()["id"], 0.01);
    assert_eq!(properties.dimensions(), (1920, 1080, 720));
}

#[test]
fn reports_the_property_with_an_invalid_value() {
    let numeric_error = TestProperties::from_properties(&HashMap::from([(
        RETRIES.to_string(),
        "many".to_string(),
    )]))
    .unwrap_err();
    assert!(numeric_error.contains(RETRIES));

    let boolean_error = TestProperties::from_properties(&HashMap::from([(
        FANOUT_ENABLED.to_string(),
        "sometimes".to_string(),
    )]))
    .unwrap_err();
    assert!(boolean_error.contains(FANOUT_ENABLED));

    let prefixed_key = format!("{COLUMN_FPP_PREFIX}id");
    let prefix_error = TestProperties::from_properties(&HashMap::from([(
        prefixed_key.clone(),
        "low".to_string(),
    )]))
    .unwrap_err();
    assert!(prefix_error.contains(&prefixed_key));
}

#[derive(Debug, Properties)]
struct CommitProperties {
    /// Maximum number of times to retry a commit.
    #[property(key = RETRIES, default = 4, getter)]
    retries: u64,
}

#[derive(Debug, Properties)]
struct NestedProperties {
    #[property(nested, getter)]
    commit: CommitProperties,
}

#[test]
fn nested_properties_read_the_same_flat_map() {
    let raw = HashMap::from([(RETRIES.to_string(), "9".to_string())]);
    let properties = NestedProperties::from_properties(&raw).unwrap();

    assert_eq!(properties.commit().retries(), 9);
}

fn parse_non_empty(value: &str) -> Result<String, &'static str> {
    let value = value.trim();
    if value.is_empty() {
        Err("value must not be empty")
    } else {
        Ok(value.to_string())
    }
}

#[derive(Debug, Properties)]
struct ValidatedProperties {
    #[property(
        key = "location",
        default = "default",
        parse_with = parse_non_empty,
        getter
    )]
    location: String,
}

#[test]
fn custom_single_value_parser_can_validate_and_normalize() {
    let defaults = ValidatedProperties::from_properties(&HashMap::new()).unwrap();
    assert_eq!(defaults.location(), "default");

    let parsed = ValidatedProperties::from_properties(&HashMap::from([(
        "location".to_string(),
        " path ".to_string(),
    )]))
    .unwrap();
    assert_eq!(parsed.location(), "path");

    let error = ValidatedProperties::from_properties(&HashMap::from([(
        "location".to_string(),
        "  ".to_string(),
    )]))
    .unwrap_err();
    assert_eq!(error, "Invalid value for location: value must not be empty");
}

#[derive(Debug, Properties)]
struct OptionalValidatedProperties {
    #[property(
        key = "optional-location",
        default = None,
        parse_with = parse_non_empty,
        getter
    )]
    location: Option<String>,
}

#[test]
fn custom_single_value_parser_wraps_present_optional_values() {
    let defaults = OptionalValidatedProperties::from_properties(&HashMap::new()).unwrap();
    assert_eq!(defaults.location(), &None);

    let parsed = OptionalValidatedProperties::from_properties(&HashMap::from([(
        "optional-location".to_string(),
        " path ".to_string(),
    )]))
    .unwrap();
    assert_eq!(parsed.location().as_deref(), Some("path"));
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Meters(u64);

impl FromStr for Meters {
    type Err = <u64 as FromStr>::Err;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse().map(Self)
    }
}

#[derive(Debug, Properties)]
struct CopyGetterProperties {
    #[property(key = "distance", default = Meters(1), getter)]
    distance: Meters,

    #[property(key = "count", default = Some(2), getter)]
    count: Option<u64>,
}

#[test]
fn returns_only_structurally_known_copy_types_by_value() {
    let properties = CopyGetterProperties::from_properties(&HashMap::new()).unwrap();

    let _: &Meters = properties.distance();
    let _: Option<u64> = properties.count();
    assert_eq!(properties.distance(), &Meters(1));
    assert_eq!(properties.count(), Some(2));
}

#[derive(Debug, Default, Serialize, Deserialize, Properties)]
struct DerivedTraitProperties {
    #[property(key = RETRIES, default = 4, getter)]
    retries: u64,
}

#[test]
fn coexists_with_default_serialize_and_deserialize_derives() {
    let defaults = DerivedTraitProperties::default();
    assert_eq!(defaults.retries(), 0);

    let properties = DerivedTraitProperties::from_properties(&HashMap::new()).unwrap();
    assert_eq!(properties.retries(), 4);
    assert_eq!(
        serde_json::to_string(&properties).unwrap(),
        r#"{"retries":4}"#
    );

    let decoded: DerivedTraitProperties = serde_json::from_str(r#"{"retries":7}"#).unwrap();
    assert_eq!(decoded.retries(), 7);
}
