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

/// This is a helper module that defines the schema field of the manifest list entry.
use std::sync::Arc;

use apache_avro::Schema as AvroSchema;
use once_cell::sync::Lazy;

use crate::avro::schema_to_avro_schema;
use crate::spec::{ListType, NestedField, NestedFieldRef, PrimitiveType, Schema, StructType, Type};

static MANIFEST_PATH: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::required(
            500,
            "manifest_path",
            Type::Primitive(PrimitiveType::String),
        ))
    })
};
static MANIFEST_LENGTH: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::required(
            501,
            "manifest_length",
            Type::Primitive(PrimitiveType::Long),
        ))
    })
};
static PARTITION_SPEC_ID: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::required(
            502,
            "partition_spec_id",
            Type::Primitive(PrimitiveType::Int),
        ))
    })
};
static CONTENT: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::required(
            517,
            "content",
            Type::Primitive(PrimitiveType::Int),
        ))
    })
};
static SEQUENCE_NUMBER: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::required(
            515,
            "sequence_number",
            Type::Primitive(PrimitiveType::Long),
        ))
    })
};
static MIN_SEQUENCE_NUMBER: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::required(
            516,
            "min_sequence_number",
            Type::Primitive(PrimitiveType::Long),
        ))
    })
};
static ADDED_SNAPSHOT_ID: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::required(
            503,
            "added_snapshot_id",
            Type::Primitive(PrimitiveType::Long),
        ))
    })
};
static ADDED_FILES_COUNT_V2: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::required(
            504,
            "added_files_count",
            Type::Primitive(PrimitiveType::Int),
        ))
    })
};
static ADDED_FILES_COUNT_V1: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::optional(
            504,
            "added_data_files_count",
            Type::Primitive(PrimitiveType::Int),
        ))
    })
};
static EXISTING_FILES_COUNT_V2: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::required(
            505,
            "existing_files_count",
            Type::Primitive(PrimitiveType::Int),
        ))
    })
};
static EXISTING_FILES_COUNT_V1: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::optional(
            505,
            "existing_data_files_count",
            Type::Primitive(PrimitiveType::Int),
        ))
    })
};
static DELETED_FILES_COUNT_V2: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::required(
            506,
            "deleted_files_count",
            Type::Primitive(PrimitiveType::Int),
        ))
    })
};
static DELETED_FILES_COUNT_V1: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::optional(
            506,
            "deleted_data_files_count",
            Type::Primitive(PrimitiveType::Int),
        ))
    })
};
static ADDED_ROWS_COUNT_V2: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::required(
            512,
            "added_rows_count",
            Type::Primitive(PrimitiveType::Long),
        ))
    })
};
static ADDED_ROWS_COUNT_V1: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::optional(
            512,
            "added_rows_count",
            Type::Primitive(PrimitiveType::Long),
        ))
    })
};
static EXISTING_ROWS_COUNT_V2: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::required(
            513,
            "existing_rows_count",
            Type::Primitive(PrimitiveType::Long),
        ))
    })
};
static EXISTING_ROWS_COUNT_V1: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::optional(
            513,
            "existing_rows_count",
            Type::Primitive(PrimitiveType::Long),
        ))
    })
};
static DELETED_ROWS_COUNT_V2: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::required(
            514,
            "deleted_rows_count",
            Type::Primitive(PrimitiveType::Long),
        ))
    })
};
static DELETED_ROWS_COUNT_V1: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::optional(
            514,
            "deleted_rows_count",
            Type::Primitive(PrimitiveType::Long),
        ))
    })
};
static PARTITIONS: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        // element type
        let fields = vec![
            Arc::new(NestedField::required(
                509,
                "contains_null",
                Type::Primitive(PrimitiveType::Boolean),
            )),
            Arc::new(NestedField::optional(
                518,
                "contains_nan",
                Type::Primitive(PrimitiveType::Boolean),
            )),
            Arc::new(NestedField::optional(
                510,
                "lower_bound",
                Type::Primitive(PrimitiveType::Binary),
            )),
            Arc::new(NestedField::optional(
                511,
                "upper_bound",
                Type::Primitive(PrimitiveType::Binary),
            )),
        ];
        let element_field = Arc::new(NestedField::required(
            508,
            "r_508",
            Type::Struct(StructType::new(fields)),
        ));
        Arc::new(NestedField::optional(
            507,
            "partitions",
            Type::List(ListType { element_field }),
        ))
    })
};
static KEY_METADATA: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::optional(
            519,
            "key_metadata",
            Type::Primitive(PrimitiveType::Binary),
        ))
    })
};
static FIRST_ROW_ID: Lazy<NestedFieldRef> = {
    Lazy::new(|| {
        Arc::new(NestedField::optional(
            520,
            "first_row_id",
            Type::Primitive(PrimitiveType::Long),
        ))
    })
};

static V1_SCHEMA: Lazy<Schema> = {
    Lazy::new(|| {
        let fields = vec![
            MANIFEST_PATH.clone(),
            MANIFEST_LENGTH.clone(),
            PARTITION_SPEC_ID.clone(),
            ADDED_SNAPSHOT_ID.clone(),
            ADDED_FILES_COUNT_V1.clone().to_owned(),
            EXISTING_FILES_COUNT_V1.clone(),
            DELETED_FILES_COUNT_V1.clone(),
            ADDED_ROWS_COUNT_V1.clone(),
            EXISTING_ROWS_COUNT_V1.clone(),
            DELETED_ROWS_COUNT_V1.clone(),
            PARTITIONS.clone(),
            KEY_METADATA.clone(),
        ];
        Schema::builder().with_fields(fields).build().unwrap()
    })
};

static V2_SCHEMA: Lazy<Schema> = {
    Lazy::new(|| {
        let fields = vec![
            MANIFEST_PATH.clone(),
            MANIFEST_LENGTH.clone(),
            PARTITION_SPEC_ID.clone(),
            CONTENT.clone(),
            SEQUENCE_NUMBER.clone(),
            MIN_SEQUENCE_NUMBER.clone(),
            ADDED_SNAPSHOT_ID.clone(),
            ADDED_FILES_COUNT_V2.clone(),
            EXISTING_FILES_COUNT_V2.clone(),
            DELETED_FILES_COUNT_V2.clone(),
            ADDED_ROWS_COUNT_V2.clone(),
            EXISTING_ROWS_COUNT_V2.clone(),
            DELETED_ROWS_COUNT_V2.clone(),
            PARTITIONS.clone(),
            KEY_METADATA.clone(),
        ];
        Schema::builder().with_fields(fields).build().unwrap()
    })
};

static V3_SCHEMA: Lazy<Schema> = {
    Lazy::new(|| {
        let fields = vec![
            MANIFEST_PATH.clone(),
            MANIFEST_LENGTH.clone(),
            PARTITION_SPEC_ID.clone(),
            CONTENT.clone(),
            SEQUENCE_NUMBER.clone(),
            MIN_SEQUENCE_NUMBER.clone(),
            ADDED_SNAPSHOT_ID.clone(),
            ADDED_FILES_COUNT_V2.clone(),
            EXISTING_FILES_COUNT_V2.clone(),
            DELETED_FILES_COUNT_V2.clone(),
            ADDED_ROWS_COUNT_V2.clone(),
            EXISTING_ROWS_COUNT_V2.clone(),
            DELETED_ROWS_COUNT_V2.clone(),
            PARTITIONS.clone(),
            KEY_METADATA.clone(),
            FIRST_ROW_ID.clone(),
        ];
        Schema::builder().with_fields(fields).build().unwrap()
    })
};

pub(super) static MANIFEST_LIST_AVRO_SCHEMA_V1: Lazy<AvroSchema> =
    Lazy::new(|| schema_to_avro_schema("manifest_file", &V1_SCHEMA).unwrap());

pub(super) static MANIFEST_LIST_AVRO_SCHEMA_V2: Lazy<AvroSchema> =
    Lazy::new(|| schema_to_avro_schema("manifest_file", &V2_SCHEMA).unwrap());

pub(super) static MANIFEST_LIST_AVRO_SCHEMA_V3: Lazy<AvroSchema> =
    Lazy::new(|| schema_to_avro_schema("manifest_file", &V3_SCHEMA).unwrap());
