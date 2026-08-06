<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Iceberg property derive macro

`Properties` generates inherent methods for reading and writing a typed struct
from a flat `HashMap<String, String>`. It deliberately does not implement
`Default`, `Serialize`, `Deserialize`, or any other trait.

## Generated methods

For every annotated struct, `#[derive(Properties)]` generates these inherent
methods:

```text
impl MyProperties {
    pub fn from_properties(
        properties: &HashMap<String, String>,
    ) -> Result<Self, String>;

    pub fn write_properties(
        &self,
        properties: &mut HashMap<String, String>,
    ) -> Result<(), String>;
}
```

`from_properties` parses every modeled property, uses its annotated default
when absent, and returns an error containing the primary property key when a
value is invalid. Unknown keys are ignored.

`write_properties` updates an existing map. It removes modeled keys whose
values equal their annotated defaults, writes non-default values as strings,
and preserves unknown keys. It returns an error when a custom
`serialize_with` or `serialize_properties_with` hook fails.

## Complete example

This example exercises the complete generated API: exact keys and defaults,
optional values, case-insensitive booleans, prefixed maps, nested property
groups, custom single-value conversion, custom multi-key conversion, public
accessors, contextual errors, and writing into an existing property map.

```rust
use std::collections::HashMap;

use iceberg_property_macro::Properties;

const RETRIES: &str = "commit.retry.num-retries";
const OWNER: &str = "owner";
const FANOUT: &str = "write.fanout.enabled";
const COLUMN_FPP_PREFIX: &str = "write.parquet.bloom-filter-fpp.column.";
const LOCATION: &str = "write.data.path";
const WIDTH: &str = "dimensions.width";
const HEIGHT: &str = "dimensions.height";
const DEPTH: &str = "dimensions.depth";

fn parse_location(value: &str) -> Result<String, &'static str> {
    let location = value.trim().trim_end_matches('/');
    if location.is_empty() {
        Err("location must not be empty")
    } else {
        Ok(location.to_string())
    }
}

fn serialize_location(value: &str) -> Result<String, &'static str> {
    let location = value.trim().trim_end_matches('/');
    if location.is_empty() {
        Err("location must not be empty")
    } else {
        Ok(location.to_string())
    }
}

fn parse_dimensions(
    properties: &HashMap<String, String>,
    width_key: &str,
    additional_keys: &[&str],
    default: (u64, u64, u64),
) -> Result<(u64, u64, u64), String> {
    if additional_keys.len() != 2 {
        return Err("dimensions require height and depth keys".to_string());
    }
    let parse = |key: &str, default| {
        properties
            .get(key)
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

fn serialize_dimensions(
    dimensions: &(u64, u64, u64),
    properties: &mut HashMap<String, String>,
    width_key: &str,
    additional_keys: &[&str],
    default: &(u64, u64, u64),
) -> Result<(), String> {
    if additional_keys.len() != 2 {
        return Err("dimensions require height and depth keys".to_string());
    }
    if dimensions.0 == 0 || dimensions.1 == 0 || dimensions.2 == 0 {
        return Err("dimensions must be positive".to_string());
    }
    properties.remove(width_key);
    properties.remove(additional_keys[0]);
    properties.remove(additional_keys[1]);
    if dimensions != default {
        properties.insert(width_key.to_string(), dimensions.0.to_string());
        properties.insert(additional_keys[0].to_string(), dimensions.1.to_string());
        properties.insert(additional_keys[1].to_string(), dimensions.2.to_string());
    }
    Ok(())
}

#[derive(Debug, Properties)]
struct CommitProperties {
    #[property(
        key = RETRIES,
        default = 4,
        pub(getter),
        pub(setter)
    )]
    retries: usize,
}

#[derive(Debug, Properties)]
struct TableLikeProperties {
    // Nested groups still read and write the same flat property map.
    #[property(nested)]
    commit: CommitProperties,

    // Option<T> distinguishes an absent property from a present value.
    #[property(key = OWNER, default = None, pub(getter), pub(setter))]
    owner: Option<String>,

    // Boolean values are parsed case-insensitively.
    #[property(key = FANOUT, default = true, pub(getter))]
    fanout_enabled: bool,

    // A prefix captures suffix/value pairs into a typed map.
    #[property(prefix = COLUMN_FPP_PREFIX, default = HashMap::new(), pub(getter))]
    column_fpp: HashMap<String, f64>,

    // Single-key hooks provide validation and custom string conversion.
    #[property(
        key = LOCATION,
        default = "warehouse",
        parse_with = parse_location,
        serialize_with = serialize_location,
        pub(getter),
        pub(setter)
    )]
    location: String,

    // Full-map hooks can model one field with multiple property keys.
    #[property(
        key = WIDTH,
        additional_keys = [HEIGHT, DEPTH],
        default = (640, 480, 320),
        parse_properties_with = parse_dimensions,
        serialize_properties_with = serialize_dimensions,
        pub(getter)
    )]
    dimensions: (u64, u64, u64),
}

fn main() -> Result<(), String> {
    // An empty map uses every annotated property default.
    let defaults = TableLikeProperties::from_properties(&HashMap::new())?;
    assert_eq!(*defaults.commit.retries(), 4);
    assert_eq!(defaults.owner(), &None);
    assert!(*defaults.fanout_enabled());
    assert!(defaults.column_fpp().is_empty());
    assert_eq!(defaults.location(), "warehouse");
    assert_eq!(defaults.dimensions(), &(640, 480, 320));

    let mut raw = HashMap::from([
        (RETRIES.to_string(), "8".to_string()),
        (OWNER.to_string(), "iceberg".to_string()),
        (FANOUT.to_string(), "FALSE".to_string()),
        (format!("{COLUMN_FPP_PREFIX}id"), "0.01".to_string()),
        (LOCATION.to_string(), " s3://bucket/table/ ".to_string()),
        (WIDTH.to_string(), "1920".to_string()),
        (HEIGHT.to_string(), "1080".to_string()),
        (DEPTH.to_string(), "720".to_string()),
        ("unmodeled".to_string(), "preserved".to_string()),
    ]);

    let mut properties = TableLikeProperties::from_properties(&raw)?;
    assert_eq!(*properties.commit.retries(), 8);
    assert_eq!(properties.owner().as_deref(), Some("iceberg"));
    assert!(!properties.fanout_enabled());
    assert_eq!(properties.column_fpp()["id"], 0.01);
    assert_eq!(properties.location(), "s3://bucket/table");
    assert_eq!(properties.dimensions(), &(1920, 1080, 720));

    // Generated setters modify private fields. Writing removes modeled values
    // reset to their defaults and preserves properties the struct does not own.
    properties.commit.set_retries(10);
    properties.set_owner(None);
    properties.set_location("s3://bucket/new-table/".to_string());
    properties.write_properties(&mut raw)?;

    assert_eq!(raw[RETRIES], "10");
    assert!(!raw.contains_key(OWNER));
    assert_eq!(raw[FANOUT], "false");
    assert_eq!(raw[&format!("{COLUMN_FPP_PREFIX}id")], "0.01");
    assert_eq!(raw[LOCATION], "s3://bucket/new-table");
    assert_eq!(raw[WIDTH], "1920");
    assert_eq!(raw[HEIGHT], "1080");
    assert_eq!(raw[DEPTH], "720");
    assert_eq!(raw["unmodeled"], "preserved");

    // Parsing errors identify the primary property key.
    let error = TableLikeProperties::from_properties(&HashMap::from([(
        LOCATION.to_string(),
        "/".to_string(),
    )]))
    .unwrap_err();
    assert!(error.contains(LOCATION));

    Ok(())
}
```

The annotated property default is independent of the value produced by a
derived `Default` implementation. When both are used, keep them aligned.

## Using a property map with Serde

Serde's standard derives serialize a struct's fields and cannot infer the
property-map representation from `Properties` attributes. A transparent adapter
keeps that conversion explicit while allowing `Default`, `Serialize`, and
`Deserialize` to remain ordinary derives:

```rust
use std::collections::HashMap;

use iceberg_property_macro::Properties;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Default, PartialEq, Properties)]
struct WriteProperties {
    #[property(
        key = "commit.retry.num-retries",
        default = 0,
        pub(getter),
        pub(setter)
    )]
    retries: u64,

    #[property(key = "owner", default = None)]
    owner: Option<String>,
}

mod property_map {
    use super::*;

    pub fn serialize<S>(value: &WriteProperties, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut properties = HashMap::new();
        value
            .write_properties(&mut properties)
            .map_err(serde::ser::Error::custom)?;
        properties.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<WriteProperties, D::Error>
    where
        D: Deserializer<'de>,
    {
        let properties = HashMap::<String, String>::deserialize(deserializer)?;
        WriteProperties::from_properties(&properties).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(transparent)]
struct PropertyDocument(#[serde(with = "property_map")] WriteProperties);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut document = PropertyDocument::default();
    document.0.set_retries(4);

    let json = serde_json::to_string(&document)?;
    assert_eq!(json, r#"{"commit.retry.num-retries":"4"}"#);

    let decoded: PropertyDocument = serde_json::from_str(&json)?;
    assert_eq!(*decoded.0.retries(), 4);
    Ok(())
}
```

Property options may be grouped under `#[property(...)]`, which avoids a
collision between the standalone `#[default(...)]` helper and Rust's `Default`
derive. The standalone annotations from the original framework remain
supported.

`#[prefix(...)]` captures a family of properties in a `HashMap<String, T>`,
keyed by the suffix after the prefix. `#[nested]` embeds another `Properties`
struct while keeping the property map flat. `#[parse_with(...)]` and
`#[serialize_with(...)]` customize conversion for one exact-key field. The
latter name refers to conversion into a property string and does not require
Serde.

`#[parse_properties_with(...)]` and `#[serialize_properties_with(...)]` receive the
complete property map for fields represented by more than one key.
`#[additional_keys(...)]` supplies a list of secondary keys to those hooks.
Custom serialization hooks return `Result`, receive the field default, and are
responsible for removing or omitting default-valued properties.

Boolean property values are parsed case-insensitively. Other values require
`FromStr` and `ToString` unless custom conversion hooks are supplied. Leaf
fields require `PartialEq` so default values can be omitted. String-literal and
path defaults are converted into their field type with `Into`.
