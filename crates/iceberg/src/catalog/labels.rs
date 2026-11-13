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

//! Catalog-provided labels for tables.

use std::collections::HashMap;

use serde_derive::{Deserialize, Serialize};
use serde_with::{DefaultOnNull, serde_as};

/// Labels a catalog attaches to a table and its fields, such as ownership or classification.
///
/// Labels come from the catalog with each load and are not part of the table metadata.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Labels {
    /// Labels for the table as a whole.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    #[serde_as(deserialize_as = "DefaultOnNull")]
    pub object_labels: HashMap<String, String>,
    /// Labels for individual fields.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[serde_as(deserialize_as = "DefaultOnNull")]
    pub fields: Vec<FieldLabels>,
}

/// Labels for a single field.
///
/// The field ID may not match a field in the current schema, for example after the field is
/// dropped.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct FieldLabels {
    /// ID of the labeled field.
    pub field_id: i32,
    /// Labels for the field.
    pub labels: HashMap<String, String>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn labels_map(entries: &[(&str, &str)]) -> HashMap<String, String> {
        entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn empty_labels() -> Labels {
        Labels {
            object_labels: HashMap::new(),
            fields: vec![],
        }
    }

    #[test]
    fn labels_round_trip_through_json() {
        let json = json!({
            "object-labels": {"owner": "finance"},
            "fields": [
                {"field-id": 3, "labels": {"classification": "pii", "retention": "90d"}},
                {"field-id": 7, "labels": {"classification": "public"}}
            ]
        });
        let expected = Labels {
            object_labels: labels_map(&[("owner", "finance")]),
            fields: vec![
                FieldLabels {
                    field_id: 3,
                    labels: labels_map(&[("classification", "pii"), ("retention", "90d")]),
                },
                FieldLabels {
                    field_id: 7,
                    labels: labels_map(&[("classification", "public")]),
                },
            ],
        };

        let labels: Labels = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(labels, expected);
        assert_eq!(serde_json::to_value(&labels).unwrap(), json);
    }

    #[test]
    fn missing_or_null_labels_parts_deserialize_as_empty() {
        for json in [json!({}), json!({"object-labels": null, "fields": null})] {
            let labels: Labels = serde_json::from_value(json).unwrap();
            assert_eq!(labels, empty_labels());
        }
    }

    #[test]
    fn empty_labels_serialize_as_empty_object() {
        assert_eq!(serde_json::to_value(empty_labels()).unwrap(), json!({}));
    }

    #[test]
    fn field_labels_require_field_id_and_labels() {
        for json in [
            json!({"fields": [{"labels": {"classification": "pii"}}]}),
            json!({"fields": [{"field-id": 3}]}),
            json!({"fields": [{"field-id": 3, "labels": null}]}),
        ] {
            assert!(
                serde_json::from_value::<Labels>(json.clone()).is_err(),
                "expected an error for {json}"
            );
        }
    }
}
