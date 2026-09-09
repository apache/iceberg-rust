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

# Iceberg property macros

## `Properties` derive macro

`#[derive(Properties)]` parses an owned typed struct from a flat
`HashMap<String, String>` and can generate opt-in read-only getters. It
deliberately does not generate property-map serialization or implement
`Default`, `Serialize`, `Deserialize`, or any other trait.

### Generated API

For every annotated struct, `#[derive(Properties)]` generates this inherent
constructor:

```text
impl MyProperties {
    pub fn from_properties(
        properties: &HashMap<String, String>,
    ) -> iceberg::Result<Self>;
}
```

`from_properties` borrows the source map, parses every modeled property, and
uses its annotated default when a property is absent. Unknown keys are ignored.
An invalid value returns an `iceberg::Error` with `ErrorKind::DataInvalid` and a
message containing its primary property key.

Adding `getter` to a field generates a public immutable accessor with the field
name. Primitive `Copy` types, references, pointers, and compositions of those
types return `T`; other types return `&T`. Because a procedural macro cannot
resolve trait implementations, a user-defined `Copy` type returns `&T`.
Documentation attributes on the field are copied to the generated getter. The
macro generates no setters, backing fields, or conversion back to a property
map.

`Option<T>` key fields and `HashMap<String, T>` prefix fields must use the
corresponding types from `std`. The macro recognizes those field shapes
syntactically and generates code using the standard-library variants.

### Complete example

This example covers exact keys and defaults, optional values, case-insensitive
booleans, prefixed maps, nested groups, custom single-value parsing, custom
multi-key parsing, lists of additional keys, read-only getters, ignored unknown
keys, and contextual errors.

```rust
use std::collections::HashMap;

use iceberg::{Error, ErrorKind};
use iceberg_property_macro::Properties;

const RETRIES: &str = "commit.retry.num-retries";
const OWNER: &str = "owner";
const FANOUT: &str = "write.datafusion.fanout.enabled";
const COLUMN_FPP_PREFIX: &str = "write.parquet.bloom-filter-fpp.column.";
const LOCATION: &str = "write.data.path";
const WIDTH: &str = "dimensions.width";
const HEIGHT: &str = "dimensions.height";
const DEPTH: &str = "dimensions.depth";

fn parse_location(value: &str) -> iceberg::Result<String> {
    let location = value.trim().trim_end_matches('/');
    if location.is_empty() {
        Err(Error::new(
            ErrorKind::DataInvalid,
            "location must not be empty",
        ))
    } else {
        Ok(location.to_string())
    }
}

fn parse_dimensions(
    properties: &HashMap<String, String>,
    width_key: &str,
    additional_keys: &[&str],
    default: (u64, u64, u64),
) -> iceberg::Result<(u64, u64, u64)> {
    if additional_keys.len() != 2 {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "dimensions require height and depth keys",
        ));
    }
    let parse = |key: &str, default| {
        properties
            .get(key)
            .map(|value| {
                value.parse::<u64>().map_err(|error| {
                    Error::new(ErrorKind::DataInvalid, error.to_string())
                })
            })
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
    #[property(key = RETRIES, default = 4, getter)]
    retries: usize,
}

#[derive(Debug, Properties)]
struct TableLikeProperties {
    /// Nested groups parse from the same flat property map.
    #[property(nested, getter)]
    commit: CommitProperties,

    /// Option<T> distinguishes an absent property from a present value.
    #[property(key = OWNER, default = None, getter)]
    owner: Option<String>,

    /// This DataFusion-specific boolean is parsed case-insensitively.
    /// Its `true` default is engine-specific, rather than an Iceberg-wide default.
    #[property(key = FANOUT, default = true, getter)]
    fanout_enabled: bool,

    /// A prefix captures suffix/value pairs into a typed map.
    #[property(prefix = COLUMN_FPP_PREFIX, getter)]
    column_fpp: HashMap<String, f64>,

    /// An explicitly configured data path override.
    /// `None` means callers should use `<table-location>/data`.
    /// A single-key parser validates and normalizes configured values.
    #[property(
        key = LOCATION,
        default = None,
        parse_with = parse_location,
        getter
    )]
    location: Option<String>,

    /// A full-map parser can model one field with multiple property keys.
    #[property(
        key = WIDTH,
        additional_keys = [HEIGHT, DEPTH],
        default = (640, 480, 320),
        parse_properties_with = parse_dimensions,
        getter
    )]
    dimensions: (u64, u64, u64),
}

fn main() -> iceberg::Result<()> {
    let defaults = TableLikeProperties::from_properties(&HashMap::new())?;
    assert_eq!(defaults.commit().retries(), 4);
    assert_eq!(defaults.owner(), &None);
    assert!(defaults.fanout_enabled());
    assert!(defaults.column_fpp().is_empty());
    assert_eq!(defaults.location(), &None);
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
    assert_eq!(
        properties.location().as_deref(),
        Some("s3://bucket/table")
    );
    assert_eq!(properties.dimensions(), (1920, 1080, 720));

    let error = TableLikeProperties::from_properties(&HashMap::from([(
        LOCATION.to_string(),
        "/".to_string(),
    )]))
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::DataInvalid);
    assert!(format!("{error}").contains(LOCATION));

    Ok(())
}
```

### Using ordinary derives together

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
        getter
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

### Property declarations

All field settings must be grouped under `#[property(...)]`. This keeps `key`,
`default`, `prefix`, `nested`, parser hooks, additional keys, and getter
generation in one attribute and avoids collisions with ordinary Rust derives.

Exact-key fields require a `default`. The `prefix` setting requires
`HashMap<String, T>` and returns the entries matched by the prefix, or an empty
map when none match. `nested` embeds another `Properties` struct while reading
the same flat map. Neither prefix nor nested fields accept a `default`.
`parse_with` customizes parsing for one exact-key field; on an `Option<T>` field,
the parser produces `T` and the macro wraps a present value in `Some`.
`parse_properties_with` receives the complete property map, and its required
`additional_keys` setting supplies the list of secondary keys.

Boolean values are parsed case-insensitively. Other values require `FromStr`
unless a custom parser is supplied. String-literal and path defaults are
converted into their field type with `Into`.

`from_properties` and both custom parser hooks use `iceberg::Result`. Generated
`FromStr` failures use `ErrorKind::DataInvalid`. The macro preserves errors from
custom parsers and adds the primary property key as error context.

## `properties_view!` function-like macro

`properties_view!` independently defines a lightweight borrowed view over a
flat property map. Its struct-shaped fields are declarations used to generate
getters rather than stored fields. Constructing the view does not parse
anything; each generated getter parses and returns only its declared property.
Missing properties use their annotated defaults, while invalid configured
values return an error from the corresponding getter.

```rust
use std::collections::HashMap;

use iceberg_property_macro::properties_view;

properties_view! {
    #[derive(Debug)]
    pub struct WriteProperties {
        // `getter` makes a generated getter public. Without it, the getter is
        // private, regardless of the declaration's visibility modifier.

        /// Number of times to retry a commit.
        #[property(key = "commit.retry.num-retries", default = 4, getter)]
        commit_num_retries: usize,

        /// Optional configured base directory for metadata files.
        #[property(key = "write.metadata.path", default = None)]
        raw_write_metadata_path: Option<String>,
    }
}

impl WriteProperties<'_> {
    /// Returns the configured metadata path or the table's metadata directory.
    pub fn write_metadata_path(&self, table_path: &str) -> iceberg::Result<String> {
        Ok(self.raw_write_metadata_path()?.unwrap_or_else(|| {
            format!("{}/metadata", table_path.trim_end_matches('/'))
        }))
    }
}

fn read(properties: &HashMap<String, String>) -> iceberg::Result<()> {
    let view = WriteProperties::new(properties);
    let _retries = view.commit_num_retries()?;
    let _metadata_path = view.write_metadata_path("s3://bucket/table")?;
    Ok(())
}
```

For this declaration, the macro generates
`impl<'properties> WriteProperties<'properties>` with a
`pub fn new(properties: &'properties HashMap<String, String>) -> Self`
constructor. The constructor has the same visibility as the view declaration:
`pub` here, `pub(crate)` for a `pub(crate)` view, and private for a private view.
It only stores the map reference and does not parse any properties.

The generated type has one lifetime parameter and contains only a reference to
the source `HashMap`; no declared field is stored in the view. Every declaration
generates a getter, but only the `getter` option makes that method public. Field
visibility modifiers are ignored, consistent with how `Properties` uses
`getter` to control generated accessors. Omitting `getter` makes it possible to
keep a raw generated method private and expose a public method that adds
application-specific behavior, as shown by `write_metadata_path` above.

Non-`Copy` values are returned by value because the view does not retain parsed
results, so calling the same getter repeatedly reparses that field.

The view supports exact keys, optional values, case-insensitive booleans,
prefixed maps, nested views, `parse_with`, and `parse_properties_with` using the
same property declaration syntax as `Properties`. A nested field's type must be
another view type, such as `CommitProperties<'_>`. The nested getter returns a
view tied directly to the source map's lifetime, so it remains usable after the
parent view is dropped.
