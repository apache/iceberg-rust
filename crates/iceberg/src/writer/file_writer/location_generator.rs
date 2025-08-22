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

//! This module contains the location generator and file name generator for generating path of data file.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use crate::Result;
use crate::spec::{DataFileFormat, PartitionSpec, Struct, TableMetadata};

/// `LocationGenerator` used to generate the location of data file.
pub trait LocationGenerator: Clone + Send + 'static {
    /// Generate an absolute path for the given file name without partition information.
    /// 
    /// # Arguments
    ///
    /// * `file_name` - The name of the file
    ///
    /// # Returns
    ///
    /// An absolute path, e.g., "/table/data/part-00000.parquet"
    fn generate_location(&self, file_name: &str) -> String;
    /// Generate an absolute path for the given file name that includes the partition path.
    ///
    /// # Arguments
    ///
    /// * `spec` - The partition specification that defines how to produce partition values
    /// * `partition_values` - The struct containing partition values
    /// * `file_name` - The name of the file
    ///
    /// # Returns
    ///
    /// An absolute path that includes the partition path, e.g.,
    /// "/table/data/id=1/name=alice/part-00000.parquet"
    fn generate_location_with_partition(
        &self,
        spec: &PartitionSpec,
        partition_values: &Struct,
        file_name: &str,
    ) -> String;
}

const WRITE_DATA_LOCATION: &str = "write.data.path";
const WRITE_FOLDER_STORAGE_LOCATION: &str = "write.folder-storage.path";
const DEFAULT_DATA_DIR: &str = "/data";

#[derive(Clone, Debug)]
/// `DefaultLocationGenerator` used to generate the data dir location of data file.
/// The location is generated based on the table location and the data location in table properties.
pub struct DefaultLocationGenerator {
    data_location: String,
}

impl DefaultLocationGenerator {
    /// Create a new `DefaultLocationGenerator`.
    pub fn new(table_metadata: TableMetadata) -> Result<Self> {
        let table_location = table_metadata.location();
        let prop = table_metadata.properties();
        let data_location_from_props = prop
            .get(WRITE_DATA_LOCATION)
            .or(prop.get(WRITE_FOLDER_STORAGE_LOCATION));
        let data_location = if let Some(data_location) = data_location_from_props {
            data_location.clone()
        } else {
            format!("{}{}", table_location, DEFAULT_DATA_DIR)
        };
        Ok(Self { data_location })
    }
}

impl LocationGenerator for DefaultLocationGenerator {
    fn generate_location(&self, file_name: &str) -> String {
        format!("{}/{}", self.data_location, file_name)
    }

    fn generate_location_with_partition(
        &self,
        spec: &PartitionSpec,
        partition_values: &Struct,
        file_name: &str,
    ) -> String {
        format!(
            "{}/{}/{}",
            self.data_location,
            spec.partition_to_path(partition_values),
            file_name
        )
    }
}

/// `FileNameGeneratorTrait` used to generate file name for data file. The file name can be passed to `LocationGenerator` to generate the location of the file.
pub trait FileNameGenerator: Clone + Send + 'static {
    /// Generate a file name.
    fn generate_file_name(&self) -> String;
}

/// `DefaultFileNameGenerator` used to generate file name for data file. The file name can be
/// passed to `LocationGenerator` to generate the location of the file.
/// The file name format is "{prefix}-{file_count}[-{suffix}].{file_format}".
#[derive(Clone, Debug)]
pub struct DefaultFileNameGenerator {
    prefix: String,
    suffix: String,
    format: String,
    file_count: Arc<AtomicU64>,
}

impl DefaultFileNameGenerator {
    /// Create a new `FileNameGenerator`.
    pub fn new(prefix: String, suffix: Option<String>, format: DataFileFormat) -> Self {
        let suffix = if let Some(suffix) = suffix {
            format!("-{}", suffix)
        } else {
            "".to_string()
        };

        Self {
            prefix,
            suffix,
            format: format.to_string(),
            file_count: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl FileNameGenerator for DefaultFileNameGenerator {
    fn generate_file_name(&self) -> String {
        let file_id = self
            .file_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!(
            "{}-{:05}{}.{}",
            self.prefix, file_id, self.suffix, self.format
        )
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::collections::HashMap;

    use uuid::Uuid;

    use super::LocationGenerator;
    use crate::spec::{FormatVersion, PartitionSpec, Struct, StructType, TableMetadata, Transform};
    use crate::writer::file_writer::location_generator::{
        FileNameGenerator, WRITE_DATA_LOCATION, WRITE_FOLDER_STORAGE_LOCATION,
    };

    #[derive(Clone)]
    pub(crate) struct MockLocationGenerator {
        root: String,
    }

    impl MockLocationGenerator {
        pub(crate) fn new(root: String) -> Self {
            Self { root }
        }
    }

    impl LocationGenerator for MockLocationGenerator {
        fn generate_location(&self, file_name: &str) -> String {
            format!("{}/{}", self.root, file_name)
        }

        fn generate_location_with_partition(
            &self,
            _spec: &PartitionSpec,
            _partition_values: &Struct,
            file_name: &str,
        ) -> String {
            format!("{}/{}", self.root, file_name)
        }
    }

    #[test]
    fn test_default_location_generate() {
        let mut table_metadata = TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: Uuid::parse_str("fb072c92-a02b-11e9-ae9c-1bb7bc9eca94").unwrap(),
            location: "s3://data.db/table".to_string(),
            last_updated_ms: 1515100955770,
            last_column_id: 1,
            schemas: HashMap::new(),
            current_schema_id: 1,
            partition_specs: HashMap::new(),
            default_spec: PartitionSpec::unpartition_spec().into(),
            default_partition_type: StructType::new(vec![]),
            last_partition_id: 1000,
            default_sort_order_id: 0,
            sort_orders: HashMap::from_iter(vec![]),
            snapshots: HashMap::default(),
            current_snapshot_id: None,
            last_sequence_number: 1,
            properties: HashMap::new(),
            snapshot_log: Vec::new(),
            metadata_log: vec![],
            refs: HashMap::new(),
            statistics: HashMap::new(),
            partition_statistics: HashMap::new(),
            encryption_keys: HashMap::new(),
        };

        let file_name_genertaor = super::DefaultFileNameGenerator::new(
            "part".to_string(),
            Some("test".to_string()),
            crate::spec::DataFileFormat::Parquet,
        );

        // test default data location
        let location_generator =
            super::DefaultLocationGenerator::new(table_metadata.clone()).unwrap();
        let location =
            location_generator.generate_location(&file_name_genertaor.generate_file_name());
        assert_eq!(location, "s3://data.db/table/data/part-00000-test.parquet");

        // test custom data location
        table_metadata.properties.insert(
            WRITE_FOLDER_STORAGE_LOCATION.to_string(),
            "s3://data.db/table/data_1".to_string(),
        );
        let location_generator =
            super::DefaultLocationGenerator::new(table_metadata.clone()).unwrap();
        let location =
            location_generator.generate_location(&file_name_genertaor.generate_file_name());
        assert_eq!(
            location,
            "s3://data.db/table/data_1/part-00001-test.parquet"
        );

        table_metadata.properties.insert(
            WRITE_DATA_LOCATION.to_string(),
            "s3://data.db/table/data_2".to_string(),
        );
        let location_generator =
            super::DefaultLocationGenerator::new(table_metadata.clone()).unwrap();
        let location =
            location_generator.generate_location(&file_name_genertaor.generate_file_name());
        assert_eq!(
            location,
            "s3://data.db/table/data_2/part-00002-test.parquet"
        );

        table_metadata.properties.insert(
            WRITE_DATA_LOCATION.to_string(),
            // invalid table location
            "s3://data.db/data_3".to_string(),
        );
        let location_generator =
            super::DefaultLocationGenerator::new(table_metadata.clone()).unwrap();
        let location =
            location_generator.generate_location(&file_name_genertaor.generate_file_name());
        assert_eq!(location, "s3://data.db/data_3/part-00003-test.parquet");
    }

    #[test]
    fn test_default_location_generate_with_partition() {
        use std::sync::Arc;

        use crate::spec::{Literal, NestedField, PrimitiveType, Schema, Transform, Type};

        // Create a schema with id and name fields
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        // Create a partition spec with identity transforms for both fields
        let partition_spec = PartitionSpec::builder(schema.clone())
            .add_partition_field("id", "id", Transform::Identity)
            .unwrap()
            .add_partition_field("name", "name", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        // Create partition values
        let partition_values =
            Struct::from_iter([Some(Literal::int(42)), Some(Literal::string("alice"))]);

        // Create table metadata
        let table_metadata = TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: Uuid::parse_str("fb072c92-a02b-11e9-ae9c-1bb7bc9eca94").unwrap(),
            location: "s3://data.db/table".to_string(),
            last_updated_ms: 1515100955770,
            last_column_id: 2,
            schemas: HashMap::from_iter(vec![(0, Arc::new(schema))]),
            current_schema_id: 0,
            partition_specs: HashMap::from_iter(vec![(0, Arc::new(partition_spec.clone()))]),
            default_spec: Arc::new(partition_spec.clone()),
            default_partition_type: partition_spec.partition_type().unwrap(),
            last_partition_id: 1001,
            default_sort_order_id: 0,
            sort_orders: HashMap::from_iter(vec![]),
            snapshots: HashMap::default(),
            current_snapshot_id: None,
            last_sequence_number: 1,
            properties: HashMap::new(),
            snapshot_log: Vec::new(),
            metadata_log: vec![],
            refs: HashMap::new(),
            statistics: HashMap::new(),
            partition_statistics: HashMap::new(),
            encryption_keys: HashMap::new(),
        };

        // Create a file name generator
        let file_name_generator = super::DefaultFileNameGenerator::new(
            "part".to_string(),
            Some("test".to_string()),
            crate::spec::DataFileFormat::Parquet,
        );
        let file_name = file_name_generator.generate_file_name();

        // Create a location generator
        let location_generator = super::DefaultLocationGenerator::new(table_metadata).unwrap();

        // Test generate_location_with_partition
        let location = location_generator.generate_location_with_partition(
            &partition_spec,
            &partition_values,
            &file_name,
        );

        // Verify the result
        assert_eq!(
            location,
            "s3://data.db/table/data/id=42/name=\"alice\"/part-00000-test.parquet"
        );
    }
}
