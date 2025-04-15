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

//! Iceberg name mapping.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DefaultOnNull};

use crate::Error;

/// Property name for name mapping.
pub const DEFAULT_SCHEMA_NAME_MAPPING: &str = "schema.name-mapping.default";

/// Iceberg fallback field name to ID mapping.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(transparent)]
pub struct NameMapping {
    root: Vec<MappedField>,
    #[serde(skip)]
    name_to_id: HashMap<String, i32>,
    #[serde(skip)]
    id_to_field: HashMap<i32, MappedField>,
}

impl NameMapping {
    /// Create a new [`NameMapping`] given a collection of mapped fields.
    pub fn try_new(fields: Vec<MappedField>) -> Result<NameMapping, Error> {
        let mut name_to_id = HashMap::new();
        let mut id_to_field = HashMap::new();

        for field in &fields {
            if let Some(id) = field.field_id() {
                if id_to_field.contains_key(&id) {
                    return Err(Error::new(
                        crate::ErrorKind::DataInvalid,
                        format!("duplicate id '{id}' is not allowed"),
                    ));
                }

                id_to_field.insert(id, field.clone());
                for name in field.names() {
                    if name_to_id.contains_key(name) {
                        return Err(Error::new(
                            crate::ErrorKind::DataInvalid,
                            format!("duplicate name '{name}' is not allowed"),
                        ));
                    }
                    name_to_id.insert(name.to_string(), id);
                }
            }
        }
        Ok(Self {
            root: fields,
            name_to_id,
            id_to_field,
        })
    }

    /// Get a reference to fields which are to be mapped from name to field ID.
    pub fn root(&self) -> &[MappedField] {
        &self.root
    }

    /// Get a field, by name, returning its ID if it exists, otherwise `None`.
    pub fn id(&self, field_name: &str) -> Option<i32> {
        self.name_to_id.get(field_name).copied()
    }

    /// Get a field, by ID, returning the underlying [`MappedField`] if it exists,
    /// otherwise `None`.
    pub fn field(&self, id: i32) -> Option<&MappedField> {
        self.id_to_field.get(&id)
    }
}

/// Maps field names to IDs.
#[serde_as]
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct MappedField {
    #[serde(skip_serializing_if = "Option::is_none")]
    field_id: Option<i32>,
    names: Vec<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde_as(deserialize_as = "DefaultOnNull")]
    fields: Vec<MappedField>,
}

impl MappedField {
    /// Create a new [`MappedField`].
    pub fn new(field_id: Option<i32>, names: Vec<String>, fields: Vec<MappedField>) -> Self {
        Self {
            field_id,
            names,
            fields,
        }
    }

    /// Iceberg field ID when a field's name is present within `names`.
    pub fn field_id(&self) -> Option<i32> {
        self.field_id
    }

    /// Get a reference to names for a mapped field.
    pub fn names(&self) -> &[String] {
        &self.names
    }

    /// Get a reference to the field mapping for any child fields.
    pub fn fields(&self) -> &[MappedField] {
        &self.fields
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorKind;

    #[test]
    fn test_json_mapped_field_deserialization() {
        let expected = MappedField {
            field_id: Some(1),
            names: vec!["id".to_string(), "record_id".to_string()],
            fields: vec![],
        };
        let mapped_field = r#"
        {
            "field-id": 1,
            "names": ["id", "record_id"]
        }
        "#;

        let mapped_field: MappedField = serde_json::from_str(mapped_field).unwrap();
        assert_eq!(mapped_field, expected);

        let mapped_field_with_null_fields = r#"
        {
            "field-id": 1,
            "names": ["id", "record_id"],
            "fields": null
        }
        "#;

        let mapped_field_with_null_fields: MappedField =
            serde_json::from_str(mapped_field_with_null_fields).unwrap();
        assert_eq!(mapped_field_with_null_fields, expected);
    }

    #[test]
    fn test_json_mapped_field_no_names_deserialization() {
        let expected = MappedField {
            field_id: Some(1),
            names: vec![],
            fields: vec![],
        };
        let mapped_field = r#"
        {
            "field-id": 1,
            "names": []
        }
        "#;

        let mapped_field: MappedField = serde_json::from_str(mapped_field).unwrap();
        assert_eq!(mapped_field, expected);

        let mapped_field_with_null_fields = r#"
        {
            "field-id": 1,
            "names": [],
            "fields": null
        }
        "#;

        let mapped_field_with_null_fields: MappedField =
            serde_json::from_str(mapped_field_with_null_fields).unwrap();
        assert_eq!(mapped_field_with_null_fields, expected);
    }

    #[test]
    fn test_json_mapped_field_no_field_id_deserialization() {
        let expected = MappedField {
            field_id: None,
            names: vec!["id".to_string(), "record_id".to_string()],
            fields: vec![],
        };
        let mapped_field = r#"
        {
            "names": ["id", "record_id"]
        }
        "#;

        let mapped_field: MappedField = serde_json::from_str(mapped_field).unwrap();
        assert_eq!(mapped_field, expected);

        let mapped_field_with_null_fields = r#"
        {
            "names": ["id", "record_id"],
            "fields": null
        }
        "#;

        let mapped_field_with_null_fields: MappedField =
            serde_json::from_str(mapped_field_with_null_fields).unwrap();
        assert_eq!(mapped_field_with_null_fields, expected);
    }

    #[test]
    fn test_json_name_mapping_deserialization() {
        let name_mapping = r#"
        [
            {
                "field-id": 1,
                "names": [
                    "id",
                    "record_id"
                ]
            },
            {
                "field-id": 2,
                "names": [
                    "data"
                ]
            },
            {
                "field-id": 3,
                "names": [
                    "location"
                ],
                "fields": [
                    {
                        "field-id": 4,
                        "names": [
                            "latitude",
                            "lat"
                        ]
                    },
                    {
                        "field-id": 5,
                        "names": [
                            "longitude",
                            "long"
                        ]
                    }
                ]
            }
        ]
        "#;

        let name_mapping: NameMapping = serde_json::from_str(name_mapping).unwrap();
        assert_eq!(name_mapping, NameMapping {
            root: vec![
                MappedField {
                    field_id: Some(1),
                    names: vec!["id".to_string(), "record_id".to_string()],
                    fields: vec![]
                },
                MappedField {
                    field_id: Some(2),
                    names: vec!["data".to_string()],
                    fields: vec![]
                },
                MappedField {
                    field_id: Some(3),
                    names: vec!["location".to_string()],
                    fields: vec![
                        MappedField {
                            field_id: Some(4),
                            names: vec!["latitude".to_string(), "lat".to_string()],
                            fields: vec![]
                        },
                        MappedField {
                            field_id: Some(5),
                            names: vec!["longitude".to_string(), "long".to_string()],
                            fields: vec![]
                        },
                    ]
                }
            ],
            id_to_field: HashMap::default(),
            name_to_id: HashMap::default()
        });
    }

    #[test]
    fn test_json_name_mapping_serialization() {
        let name_mapping = NameMapping {
            root: vec![
                MappedField {
                    field_id: None,
                    names: vec!["foo".to_string()],
                    fields: vec![],
                },
                MappedField {
                    field_id: Some(2),
                    names: vec!["bar".to_string()],
                    fields: vec![],
                },
                MappedField {
                    field_id: Some(3),
                    names: vec!["baz".to_string()],
                    fields: vec![],
                },
                MappedField {
                    field_id: Some(4),
                    names: vec!["qux".to_string()],
                    fields: vec![MappedField {
                        field_id: Some(5),
                        names: vec!["element".to_string()],
                        fields: vec![],
                    }],
                },
                MappedField {
                    field_id: Some(6),
                    names: vec!["quux".to_string()],
                    fields: vec![
                        MappedField {
                            field_id: Some(7),
                            names: vec!["key".to_string()],
                            fields: vec![],
                        },
                        MappedField {
                            field_id: Some(8),
                            names: vec!["value".to_string()],
                            fields: vec![
                                MappedField {
                                    field_id: Some(9),
                                    names: vec!["key".to_string()],
                                    fields: vec![],
                                },
                                MappedField {
                                    field_id: Some(10),
                                    names: vec!["value".to_string()],
                                    fields: vec![],
                                },
                            ],
                        },
                    ],
                },
                MappedField {
                    field_id: Some(11),
                    names: vec!["location".to_string()],
                    fields: vec![MappedField {
                        field_id: Some(12),
                        names: vec!["element".to_string()],
                        fields: vec![
                            MappedField {
                                field_id: Some(13),
                                names: vec!["latitude".to_string()],
                                fields: vec![],
                            },
                            MappedField {
                                field_id: Some(14),
                                names: vec!["longitude".to_string()],
                                fields: vec![],
                            },
                        ],
                    }],
                },
                MappedField {
                    field_id: Some(15),
                    names: vec!["person".to_string()],
                    fields: vec![
                        MappedField {
                            field_id: Some(16),
                            names: vec!["name".to_string()],
                            fields: vec![],
                        },
                        MappedField {
                            field_id: Some(17),
                            names: vec!["age".to_string()],
                            fields: vec![],
                        },
                    ],
                },
            ],
            id_to_field: HashMap::default(),
            name_to_id: HashMap::default(),
        };
        let expected = r#"[{"names":["foo"]},{"field-id":2,"names":["bar"]},{"field-id":3,"names":["baz"]},{"field-id":4,"names":["qux"],"fields":[{"field-id":5,"names":["element"]}]},{"field-id":6,"names":["quux"],"fields":[{"field-id":7,"names":["key"]},{"field-id":8,"names":["value"],"fields":[{"field-id":9,"names":["key"]},{"field-id":10,"names":["value"]}]}]},{"field-id":11,"names":["location"],"fields":[{"field-id":12,"names":["element"],"fields":[{"field-id":13,"names":["latitude"]},{"field-id":14,"names":["longitude"]}]}]},{"field-id":15,"names":["person"],"fields":[{"field-id":16,"names":["name"]},{"field-id":17,"names":["age"]}]}]"#;
        assert_eq!(serde_json::to_string(&name_mapping).unwrap(), expected);
    }

    #[test]
    fn name_mapping_internal_fields() {
        let my_fields = vec![
            MappedField::new(Some(1), vec!["field_one".to_string()], Vec::new()),
            MappedField::new(
                Some(2),
                vec!["field_two".to_string(), "field_foo".to_string()],
                Vec::new(),
            ),
        ];

        let name_mapping = NameMapping::try_new(my_fields).expect("valid mapped fields for test");
        assert!(
            name_mapping.field(1000).is_none(),
            "Field with ID '1000' was not inserted to the collection"
        );
        assert_eq!(
            *name_mapping.field(1).unwrap(),
            MappedField::new(Some(1), vec!["field_one".to_string()], Vec::new()),
        );
        assert_eq!(
            *name_mapping.field(2).unwrap(),
            MappedField::new(
                Some(2),
                vec!["field_two".to_string(), "field_foo".to_string()],
                Vec::new(),
            )
        );

        assert!(
            name_mapping.id("not_exist").is_none(),
            "Field was not expected to exist in the collection"
        );
        assert_eq!(name_mapping.id("field_two"), Some(2));
        assert_eq!(
            name_mapping.id("field_foo"),
            Some(2),
            "Field with another name shares the same ID of '2'"
        );
        assert_eq!(name_mapping.id("field_one"), Some(1));
    }

    #[test]
    fn name_mapping_internal_fields_invalid() {
        let duplicate_ids = vec![
            MappedField::new(Some(1), vec!["field_one".to_string()], Vec::new()),
            MappedField::new(Some(1), vec!["field_two".to_string()], Vec::new()),
        ];

        assert_eq!(
            NameMapping::try_new(duplicate_ids).unwrap_err().kind(),
            ErrorKind::DataInvalid
        );

        let duplicate_names = vec![
            MappedField::new(Some(1), vec!["field_one".to_string()], Vec::new()),
            MappedField::new(Some(2), vec!["field_one".to_string()], Vec::new()),
        ];
        assert_eq!(
            NameMapping::try_new(duplicate_names).unwrap_err().kind(),
            ErrorKind::DataInvalid
        );
    }
}
