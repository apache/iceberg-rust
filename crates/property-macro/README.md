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

Leaf fields declare a property key and the default used when that key is absent.
Public accessors are opt-in:

```rust
use iceberg_property_macro::Properties;

#[derive(Default, Properties)]
struct WriteProperties {
    #[property(
        key = "commit.retry.num-retries",
        default = 0,
        pub(getter),
        pub(setter)
    )]
    retries: u64,
}

let mut properties = WriteProperties::default();
properties.set_retries(4);
assert_eq!(*properties.retries(), 4);
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
        value.write_properties(&mut properties);
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

`#[parse_properties_with(...)]` and `#[write_properties_with(...)]` receive the
complete property map for fields represented by more than one key.
`#[additional_key(...)]` supplies a second key to those hooks. Custom write
hooks receive the field default and are responsible for removing or omitting
default-valued properties.

Boolean property values are parsed case-insensitively. Other values require
`FromStr` and `ToString` unless custom conversion hooks are supplied. Leaf
fields require `PartialEq` so default values can be omitted. String-literal and
path defaults are converted into their field type with `Into`.
