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

`Properties` parses a typed struct from a flat `HashMap<String, String>` and
can generate opt-in read-only getters. It deliberately does not generate
property-map serialization or implement `Default`, `Serialize`, `Deserialize`,
or any other trait.

## Generated API

For every annotated struct, `#[derive(Properties)]` generates this inherent
constructor:

```text
impl MyProperties {
    pub fn from_properties(
        properties: &HashMap<String, String>,
    ) -> Result<Self, String>;
}
```

`from_properties` borrows the source map, parses every modeled property, and
uses its annotated default when a property is absent. Unknown keys are ignored.
An invalid value returns an error containing its primary property key.

Adding `pub(getter)` to a field generates an immutable accessor with the field
name. Structurally known `Copy` types return `T`; other types return `&T`.
Documentation attributes on the field are copied to the generated getter. The
macro generates no setters, backing fields, or conversion back to a property
map.

## Complete example

This example covers exact keys and defaults, optional values, case-insensitive
booleans, prefixed maps, nested groups, custom single-value parsing, custom
multi-key parsing, lists of additional keys, read-only getters, ignored unknown
keys, and contextual errors.

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

#[derive(Debug, Properties)]
struct CommitProperties {
    /// Maximum number of times to retry a commit.
    #[property(key = RETRIES, default = 4, pub(getter))]
    retries: usize,
}

#[derive(Debug, Properties)]
struct TableLikeProperties {
    /// Nested groups parse from the same flat property map.
    #[property(nested, pub(getter))]
    commit: CommitProperties,

    /// Option<T> distinguishes an absent property from a present value.
    #[property(key = OWNER, default = None, pub(getter))]
    owner: Option<String>,

    /// Boolean values are parsed case-insensitively.
    #[property(key = FANOUT, default = true, pub(getter))]
    fanout_enabled: bool,

    /// A prefix captures suffix/value pairs into a typed map.
    #[property(prefix = COLUMN_FPP_PREFIX, default = HashMap::new(), pub(getter))]
    column_fpp: HashMap<String, f64>,

    /// A single-key parser can validate and normalize a property value.
    #[property(
        key = LOCATION,
        default = "warehouse",
        parse_with = parse_location,
        pub(getter)
    )]
    location: String,

    /// A full-map parser can model one field with multiple property keys.
    #[property(
        key = WIDTH,
        additional_keys = [HEIGHT, DEPTH],
        default = (640, 480, 320),
        parse_properties_with = parse_dimensions,
        pub(getter)
    )]
    dimensions: (u64, u64, u64),
}

fn main() -> Result<(), String> {
    let defaults = TableLikeProperties::from_properties(&HashMap::new())?;
    assert_eq!(defaults.commit().retries(), 4);
    assert_eq!(defaults.owner(), &None);
    assert!(defaults.fanout_enabled());
    assert!(defaults.column_fpp().is_empty());
    assert_eq!(defaults.location(), "warehouse");
    assert_eq!(defaults.dimensions(), (640, 480, 320));

    let raw = HashMap::from([
        (RETRIES.to_string(), "8".to_string()),
        (OWNER.to_string(), "iceberg".to_string()),
        (FANOUT.to_string(), "FALSE".to_string()),
        (format!("{COLUMN_FPP_PREFIX}id"), "0.01".to_string()),
        (LOCATION.to_string(), " s3://bucket/table/ ".to_string()),
        (WIDTH.to_string(), "1920".to_string()),
        (HEIGHT.to_string(), "1080".to_string()),
        (DEPTH.to_string(), "720".to_string()),
        ("unmodeled".to_string(), "ignored".to_string()),
    ]);

    let properties = TableLikeProperties::from_properties(&raw)?;
    assert_eq!(properties.commit().retries(), 8);
    assert_eq!(properties.owner().as_deref(), Some("iceberg"));
    assert!(!properties.fanout_enabled());
    assert_eq!(properties.column_fpp()["id"], 0.01);
    assert_eq!(properties.location(), "s3://bucket/table");
    assert_eq!(properties.dimensions(), (1920, 1080, 720));

    let error = TableLikeProperties::from_properties(&HashMap::from([(
        LOCATION.to_string(),
        "/".to_string(),
    )]))
    .unwrap_err();
    assert!(error.contains(LOCATION));

    Ok(())
}
```

## Using ordinary derives together

`Properties` does not implicitly derive other traits, so `Default`,
`Serialize`, and `Deserialize` can be selected independently and behave like
ordinary Rust derives:

```rust
use std::collections::HashMap;

use iceberg_property_macro::Properties;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize, Properties)]
struct ReadProperties {
    #[property(
        key = "commit.retry.num-retries",
        default = 4,
        pub(getter)
    )]
    retries: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The property annotation supplies the default used by from_properties.
    let properties = ReadProperties::from_properties(&HashMap::new())?;
    assert_eq!(properties.retries(), 4);

    // The ordinary Default derive uses the field's Rust default instead.
    let defaults = ReadProperties::default();
    assert_eq!(defaults.retries(), 0);

    // Ordinary Serde derives use Rust field names, not property keys.
    let json = serde_json::to_string(&properties)?;
    assert_eq!(json, r#"{"retries":4}"#);
    let decoded: ReadProperties = serde_json::from_str(r#"{"retries":7}"#)?;
    assert_eq!(decoded.retries(), 7);
    Ok(())
}
```

Property options may be grouped under `#[property(...)]`, which avoids a
collision between the standalone `#[default(...)]` helper and Rust's `Default`
derive. The original standalone annotations remain supported.

`#[prefix(...)]` requires `HashMap<String, T>`. `#[nested]` embeds another
`Properties` struct while reading the same flat map. `#[parse_with(...)]`
customizes parsing for one exact-key field. `#[parse_properties_with(...)]`
receives the complete property map, and `#[additional_keys(...)]` supplies its
list of secondary keys.

Boolean values are parsed case-insensitively. Other values require `FromStr`
unless a custom parser is supplied. String-literal and path defaults are
converted into their field type with `Into`.
