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

use iceberg_property_macro::Properties;

const RETRIES: &str = "commit.retry.num-retries";
const OWNER: &str = "owner";
const FORMAT: &str = "write.format.default";
const FANOUT_ENABLED: &str = "write.fanout.enabled";
const COLUMN_FPP_PREFIX: &str = "write.parquet.bloom-filter-fpp.column.";
const WIDTH: &str = "dimensions.width";
const HEIGHT: &str = "dimensions.height";

fn parse_dimensions(
    properties: &HashMap<String, String>,
    width_key: &str,
    height_key: &str,
    default: (u64, u64),
) -> Result<(u64, u64), String> {
    let parse = |property_key: &str, default| {
        properties
            .get(property_key)
            .map(|value| value.parse::<u64>().map_err(|error| error.to_string()))
            .transpose()
            .map(|value| value.unwrap_or(default))
    };

    Ok((parse(width_key, default.0)?, parse(height_key, default.1)?))
}

fn write_dimensions(
    dimensions: &(u64, u64),
    properties: &mut HashMap<String, String>,
    width_key: &str,
    height_key: &str,
    default: &(u64, u64),
) {
    properties.remove(width_key);
    properties.remove(height_key);
    if dimensions != default {
        properties.insert(width_key.to_string(), dimensions.0.to_string());
        properties.insert(height_key.to_string(), dimensions.1.to_string());
    }
}

#[derive(Debug, Properties)]
struct TestProperties {
    #[key(RETRIES)]
    #[default(4)]
    pub retries: u64,

    #[key(OWNER)]
    #[default(None)]
    pub owner: Option<String>,

    #[key(FORMAT)]
    #[default("parquet")]
    pub format: String,

    #[key(FANOUT_ENABLED)]
    #[default(true)]
    pub fanout_enabled: bool,

    #[prefix(COLUMN_FPP_PREFIX)]
    #[default(HashMap::new())]
    pub column_fpp: HashMap<String, f64>,

    #[key(WIDTH)]
    #[additional_key(HEIGHT)]
    #[default((640, 480))]
    #[parse_properties_with(parse_dimensions)]
    #[write_properties_with(write_dimensions)]
    pub dimensions: (u64, u64),
}

#[test]
fn reads_defaults_and_overrides() {
    let defaults = TestProperties::from_properties(&HashMap::new()).unwrap();
    assert_eq!(defaults.retries, 4);
    assert_eq!(defaults.owner, None);
    assert_eq!(defaults.format, "parquet");
    assert!(defaults.fanout_enabled);
    assert!(defaults.column_fpp.is_empty());
    assert_eq!(defaults.dimensions, (640, 480));

    let properties = HashMap::from([
        (RETRIES.to_string(), "8".to_string()),
        (OWNER.to_string(), "iceberg".to_string()),
        (FORMAT.to_string(), "orc".to_string()),
        (FANOUT_ENABLED.to_string(), "FALSE".to_string()),
        (format!("{COLUMN_FPP_PREFIX}id"), "0.01".to_string()),
        (WIDTH.to_string(), "1920".to_string()),
        (HEIGHT.to_string(), "1080".to_string()),
    ]);
    let parsed = TestProperties::from_properties(&properties).unwrap();

    assert_eq!(parsed.retries, 8);
    assert_eq!(parsed.owner.as_deref(), Some("iceberg"));
    assert_eq!(parsed.format, "orc");
    assert!(!parsed.fanout_enabled);
    assert_eq!(parsed.column_fpp["id"], 0.01);
    assert_eq!(parsed.dimensions, (1920, 1080));
}

#[test]
fn writes_overrides_and_preserves_unrelated_properties() {
    let parsed = TestProperties::from_properties(&HashMap::from([
        (RETRIES.to_string(), "8".to_string()),
        (OWNER.to_string(), "iceberg".to_string()),
        (FORMAT.to_string(), "orc".to_string()),
        (FANOUT_ENABLED.to_string(), "false".to_string()),
        (format!("{COLUMN_FPP_PREFIX}id"), "0.01".to_string()),
        (WIDTH.to_string(), "1920".to_string()),
        (HEIGHT.to_string(), "1080".to_string()),
    ]))
    .unwrap();
    let mut properties = HashMap::from([("unrelated".to_string(), "value".to_string())]);

    parsed.write_properties(&mut properties);

    assert_eq!(properties[RETRIES], "8");
    assert_eq!(properties[OWNER], "iceberg");
    assert_eq!(properties[FORMAT], "orc");
    assert_eq!(properties[FANOUT_ENABLED], "false");
    assert_eq!(properties[&format!("{COLUMN_FPP_PREFIX}id")], "0.01");
    assert_eq!(properties[WIDTH], "1920");
    assert_eq!(properties[HEIGHT], "1080");
    assert_eq!(properties["unrelated"], "value");
}

#[test]
fn writing_defaults_removes_modeled_properties() {
    let defaults = TestProperties::from_properties(&HashMap::new()).unwrap();
    let mut properties = HashMap::from([
        (RETRIES.to_string(), "8".to_string()),
        (OWNER.to_string(), "iceberg".to_string()),
        (FORMAT.to_string(), "orc".to_string()),
        (FANOUT_ENABLED.to_string(), "false".to_string()),
        (format!("{COLUMN_FPP_PREFIX}id"), "0.01".to_string()),
        (WIDTH.to_string(), "1920".to_string()),
        (HEIGHT.to_string(), "1080".to_string()),
        ("unrelated".to_string(), "value".to_string()),
    ]);

    defaults.write_properties(&mut properties);

    assert_eq!(
        properties,
        HashMap::from([("unrelated".to_string(), "value".to_string())])
    );
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

#[derive(Clone, Debug, Properties)]
struct CommitProperties {
    #[key = "commit.retry.num-retries"]
    #[default = 4]
    pub num_retries: u64,
}

#[derive(Debug, Properties)]
struct NestedProperties {
    #[nested]
    pub commit: CommitProperties,
}

#[test]
fn nested_properties_use_a_flat_property_map() {
    let mut properties = NestedProperties::from_properties(&HashMap::new()).unwrap();
    assert_eq!(properties.commit.num_retries, 4);

    properties.commit.num_retries = 9;
    let mut written = HashMap::new();
    properties.write_properties(&mut written);
    assert_eq!(
        written,
        HashMap::from([("commit.retry.num-retries".to_string(), "9".to_string())])
    );

    let decoded = NestedProperties::from_properties(&written).unwrap();
    assert_eq!(decoded.commit.num_retries, 9);
}

fn parse_non_empty(value: &str) -> Result<String, &'static str> {
    let value = value.trim();
    if value.is_empty() {
        Err("value must not be empty")
    } else {
        Ok(value.to_string())
    }
}

fn serialize_trimmed(value: &str) -> String {
    value.trim().to_string()
}

#[derive(Debug, Properties)]
struct ValidatedProperties {
    #[key = "location"]
    #[default = "default"]
    #[parse_with(parse_non_empty)]
    #[serialize_with(serialize_trimmed)]
    location: String,
}

#[test]
fn custom_single_value_hooks_can_validate_and_normalize() {
    let parsed = ValidatedProperties::from_properties(&HashMap::from([(
        "location".to_string(),
        " path ".to_string(),
    )]))
    .unwrap();
    assert_eq!(parsed.location, "path");

    let error = ValidatedProperties::from_properties(&HashMap::from([(
        "location".to_string(),
        "  ".to_string(),
    )]))
    .unwrap_err();
    assert_eq!(error, "Invalid value for location: value must not be empty");

    let properties = ValidatedProperties {
        location: " normalized ".to_string(),
    };
    let mut written = HashMap::new();
    properties.write_properties(&mut written);
    assert_eq!(written["location"], "normalized");
}

mod accessor_fixture {
    use iceberg_property_macro::Properties;

    #[derive(Debug, Default, Properties)]
    pub struct AccessorProperties {
        #[doc = "A property with public read and write access."]
        #[property(key = "public.both", default = 0, pub(getter), pub(setter))]
        both: u64,

        #[property(key = "public.getter", default = "", pub(getter))]
        getter_only: String,

        #[property(key = "public.setter", default = false, pub(setter))]
        setter_only: bool,
    }

    impl AccessorProperties {
        pub fn setter_only_for_test(&self) -> bool {
            self.setter_only
        }
    }
}

#[test]
fn coexists_with_derived_default_and_generates_opt_in_accessors() {
    let mut properties = accessor_fixture::AccessorProperties::default();

    assert_eq!(*properties.both(), 0);
    properties.set_both(2);
    assert_eq!(*properties.both(), 2);
    assert_eq!(properties.getter_only(), "");

    properties.set_setter_only(true);
    assert!(properties.setter_only_for_test());
}
