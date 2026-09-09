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
use std::mem::size_of;

use iceberg::{Error, ErrorKind};
use iceberg_property_macro::properties_view;

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
) -> iceberg::Result<(u64, u64, u64)> {
    if additional_keys.len() != 2 {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "dimensions require height and depth keys",
        ));
    }
    let parse = |property_key: &str, default| {
        properties
            .get(property_key)
            .map(|value| {
                value
                    .parse::<u64>()
                    .map_err(|error| Error::new(ErrorKind::DataInvalid, error.to_string()))
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

fn parse_non_empty(value: &str) -> iceberg::Result<String> {
    let value = value.trim();
    if value.is_empty() {
        Err(Error::new(
            ErrorKind::DataInvalid,
            "value must not be empty",
        ))
    } else {
        Ok(value.to_string())
    }
}

properties_view! {
    #[derive(Debug)]
    struct TestPropertiesView {
        /// Maximum number of times to retry a commit.
        #[property(key = RETRIES, default = 4, getter)]
        retries: u64,

        #[property(key = OWNER, default = None, getter)]
        owner: Option<String>,

        #[property(key = FORMAT, default = "parquet", getter)]
        format: String,

        #[property(key = FANOUT_ENABLED, default = true, getter)]
        fanout_enabled: bool,

        #[property(prefix = COLUMN_FPP_PREFIX, getter)]
        column_fpp: HashMap<String, f64>,

        #[property(
            key = "location",
            default = "default",
            parse_with = parse_non_empty,
            getter
        )]
        location: String,

        #[property(
            key = WIDTH,
            additional_keys = [HEIGHT, DEPTH],
            default = (640, 480, 320),
            parse_properties_with = parse_dimensions,
            getter
        )]
        dimensions: (u64, u64, u64),
    }
}

properties_view! {
    #[derive(Debug)]
    struct CommitPropertiesView {
        #[property(key = RETRIES, default = 4, getter)]
        retries: u64,
    }
}

properties_view! {
    #[derive(Debug)]
    struct NestedPropertiesView {
        #[property(nested, getter)]
        commit: CommitPropertiesView<'_>,
    }
}

#[test]
fn property_view_is_only_a_reference_to_the_source_map() {
    assert_eq!(
        size_of::<TestPropertiesView<'_>>(),
        size_of::<&HashMap<String, String>>()
    );
}

#[test]
fn property_view_parses_only_the_requested_field() {
    let raw = HashMap::from([
        (RETRIES.to_string(), "many".to_string()),
        (OWNER.to_string(), "iceberg".to_string()),
        (FORMAT.to_string(), "orc".to_string()),
        (FANOUT_ENABLED.to_string(), "FALSE".to_string()),
        (WIDTH.to_string(), "1920".to_string()),
        (HEIGHT.to_string(), "1080".to_string()),
        (DEPTH.to_string(), "720".to_string()),
    ]);
    let properties = TestPropertiesView::new(&raw);

    let error = properties.retries().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::DataInvalid);
    assert!(error.message().contains(RETRIES));
    assert_eq!(properties.owner().unwrap().as_deref(), Some("iceberg"));
    assert_eq!(properties.format().unwrap(), "orc");
    assert!(!properties.fanout_enabled().unwrap());
    assert_eq!(properties.dimensions().unwrap(), (1920, 1080, 720));
}

#[test]
fn property_view_uses_defaults_and_supports_custom_parsers() {
    let raw = HashMap::from([("location".to_string(), " path ".to_string())]);
    let properties = TestPropertiesView::new(&raw);

    assert_eq!(properties.retries().unwrap(), 4);
    assert_eq!(properties.owner().unwrap(), None);
    assert_eq!(properties.format().unwrap(), "parquet");
    assert!(properties.fanout_enabled().unwrap());
    assert!(properties.column_fpp().unwrap().is_empty());
    assert_eq!(properties.location().unwrap(), "path");
}

#[test]
fn nested_property_views_borrow_the_same_source_map() {
    let raw = HashMap::from([(RETRIES.to_string(), "9".to_string())]);
    let commit = {
        let properties = NestedPropertiesView::new(&raw);
        properties.commit().unwrap()
    };

    assert_eq!(commit.retries().unwrap(), 9);
}

#[test]
fn property_view_reports_errors_when_the_corresponding_getter_is_called() {
    let prefixed_key = format!("{COLUMN_FPP_PREFIX}id");
    let raw = HashMap::from([
        (prefixed_key.clone(), "low".to_string()),
        (format!("{COLUMN_FPP_PREFIX}category"), "0.01".to_string()),
        ("location".to_string(), "  ".to_string()),
    ]);
    let properties = TestPropertiesView::new(&raw);

    let prefix_error = properties.column_fpp().unwrap_err();
    assert!(prefix_error.message().contains(&prefixed_key));

    let location_error = properties.location().unwrap_err();
    assert_eq!(location_error.message(), "value must not be empty");
    assert!(format!("{location_error}").contains("property: location"));
}
