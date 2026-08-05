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
const COLUMN_FPP_PREFIX: &str = "write.parquet.bloom-filter-fpp.column.";
const WIDTH: &str = "dimensions.width";
const HEIGHT: &str = "dimensions.height";

fn parse_dimensions(
    properties: &HashMap<String, String>,
    key: &str,
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

    Ok((parse(key, default.0)?, parse(height_key, default.1)?))
}

fn write_dimensions(
    dimensions: &(u64, u64),
    properties: &mut HashMap<String, String>,
    key: &str,
    height_key: &str,
) {
    properties.insert(key.to_string(), dimensions.0.to_string());
    properties.insert(height_key.to_string(), dimensions.1.to_string());
}

#[derive(Debug, Properties)]
struct TestProperties {
    #[key(RETRIES)]
    #[default(4)]
    #[doc = "Number of retries."]
    pub retries: u64,

    #[key(OWNER)]
    #[default(None)]
    pub owner: Option<String>,

    #[key(FORMAT)]
    #[default("parquet")]
    pub format: String,

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
fn generates_defaults_and_serde_for_public_fields() {
    assert_eq!(TestProperties::default().format, "parquet");

    let properties = TestProperties {
        retries: 8,
        owner: Some("iceberg".to_string()),
        format: "orc".to_string(),
        column_fpp: HashMap::from([("id".to_string(), 0.01)]),
        dimensions: (1920, 1080),
    };

    assert_eq!(properties.retries, 8);
    assert_eq!(properties.owner, Some("iceberg".to_string()));
    assert_eq!(properties.format, "orc");
    assert_eq!(properties.column_fpp["id"], 0.01);
    assert_eq!(properties.dimensions, (1920, 1080));

    let json = serde_json::to_value(&properties).unwrap();
    assert_eq!(json[RETRIES], "8");
    assert_eq!(json[OWNER], "iceberg");
    assert_eq!(json[FORMAT], "orc");
    assert_eq!(json[format!("{COLUMN_FPP_PREFIX}id")], "0.01");
    assert_eq!(json[WIDTH], "1920");
    assert_eq!(json[HEIGHT], "1080");

    let decoded: TestProperties = serde_json::from_value(json).unwrap();
    assert_eq!(decoded.retries, 8);
    assert_eq!(decoded.owner, Some("iceberg".to_string()));
    assert_eq!(decoded.format, "orc");
    assert_eq!(decoded.column_fpp["id"], 0.01);
    assert_eq!(decoded.dimensions, (1920, 1080));
}

#[derive(Clone, Debug, Properties)]
struct CommitProperties {
    #[key = "commit.retry.num-retries"]
    #[default = 4]
    #[doc = "Number of times to retry a commit before failing."]
    pub num_retries: u64,
}

#[derive(Debug, Properties)]
struct NestedProperties {
    #[nested]
    #[doc = "Commit behavior properties."]
    pub commit: CommitProperties,
}

#[test]
fn nested_properties_use_a_flat_property_map() {
    let mut properties = NestedProperties::default();
    properties.commit.num_retries = 9;

    let json = serde_json::to_value(&properties).unwrap();
    assert_eq!(json["commit.retry.num-retries"], "9");

    let decoded: NestedProperties = serde_json::from_value(json).unwrap();
    assert_eq!(decoded.commit.num_retries, 9);
}
