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

//! Partition statistics rows and their versioned stored schemas.

use typed_builder::TypedBuilder;

use super::{
    FormatVersion, Literal, NestedField, NestedFieldRef, PrimitiveType, Schema, Struct, StructType,
    Type,
};
use crate::Result;
use crate::error::invalid_data;

// Keep field IDs and names together for schema construction and future statistics codecs.
pub(crate) const PARTITION_STATS_PARTITION_FIELD: (i32, &str) = (1, "partition");
pub(crate) const PARTITION_STATS_SPEC_ID_FIELD: (i32, &str) = (2, "spec_id");
pub(crate) const PARTITION_STATS_DATA_RECORD_COUNT_FIELD: (i32, &str) = (3, "data_record_count");
pub(crate) const PARTITION_STATS_DATA_FILE_COUNT_FIELD: (i32, &str) = (4, "data_file_count");
pub(crate) const PARTITION_STATS_TOTAL_DATA_FILE_SIZE_IN_BYTES_FIELD: (i32, &str) =
    (5, "total_data_file_size_in_bytes");
pub(crate) const PARTITION_STATS_POSITION_DELETE_RECORD_COUNT_FIELD: (i32, &str) =
    (6, "position_delete_record_count");
pub(crate) const PARTITION_STATS_POSITION_DELETE_FILE_COUNT_FIELD: (i32, &str) =
    (7, "position_delete_file_count");
pub(crate) const PARTITION_STATS_EQUALITY_DELETE_RECORD_COUNT_FIELD: (i32, &str) =
    (8, "equality_delete_record_count");
pub(crate) const PARTITION_STATS_EQUALITY_DELETE_FILE_COUNT_FIELD: (i32, &str) =
    (9, "equality_delete_file_count");
pub(crate) const PARTITION_STATS_TOTAL_RECORD_COUNT_FIELD: (i32, &str) = (10, "total_record_count");
pub(crate) const PARTITION_STATS_LAST_UPDATED_AT_FIELD: (i32, &str) = (11, "last_updated_at");
pub(crate) const PARTITION_STATS_LAST_UPDATED_SNAPSHOT_ID_FIELD: (i32, &str) =
    (12, "last_updated_snapshot_id");
pub(crate) const PARTITION_STATS_DV_COUNT_FIELD: (i32, &str) = (13, "dv_count");

/// Statistics for one partition and partition spec.
///
/// All accessors are nullable to represent projected reads, even for fields that
/// are required in the stored schema. [`Default`] and the builder leave unspecified
/// fields as `None`; [`Self::new_for_computation`] instead initializes counters to zero.
/// Reading or constructing a row preserves an externally supplied exact record count.
///
/// ```rust
/// use iceberg::spec::PartitionStatistics;
///
/// let row = PartitionStatistics::builder()
///     .with_data_file_count(2)
///     .build();
/// assert_eq!(row.data_file_count(), Some(2));
/// assert_eq!(row.data_record_count(), None);
/// ```
#[derive(Debug, Default, Clone, PartialEq, Eq, TypedBuilder)]
#[builder(field_defaults(default, setter(prefix = "with_", strip_option)))]
pub struct PartitionStatistics {
    pub(crate) partition: Option<Struct>,
    pub(crate) spec_id: Option<i32>,
    pub(crate) data_record_count: Option<i64>,
    pub(crate) data_file_count: Option<i32>,
    pub(crate) total_data_file_size_in_bytes: Option<i64>,
    pub(crate) position_delete_record_count: Option<i64>,
    pub(crate) position_delete_file_count: Option<i32>,
    pub(crate) equality_delete_record_count: Option<i64>,
    pub(crate) equality_delete_file_count: Option<i32>,
    pub(crate) total_record_count: Option<i64>,
    pub(crate) last_updated_at: Option<i64>,
    pub(crate) last_updated_snapshot_id: Option<i64>,
    pub(crate) dv_count: Option<i32>,
}

impl PartitionStatistics {
    /// Creates a complete computation row with zero counters and unknown exact total and history.
    ///
    /// This initializes values only; it does not read files or compute statistics.
    /// Use the builder or [`Default`] for partial read rows instead.
    pub fn new_for_computation(partition: Struct, spec_id: i32) -> Self {
        Self {
            partition: Some(partition),
            spec_id: Some(spec_id),
            data_record_count: Some(0),
            data_file_count: Some(0),
            total_data_file_size_in_bytes: Some(0),
            position_delete_record_count: Some(0),
            position_delete_file_count: Some(0),
            equality_delete_record_count: Some(0),
            equality_delete_file_count: Some(0),
            dv_count: Some(0),
            ..Self::default()
        }
    }

    /// Builds the stored schema for a supplied unified partition type and table format version.
    ///
    /// Nested partition fields are preserved without field-ID reassignment or type discovery.
    /// Versions 1 and 2 share a schema. Version 3 requires the delete counters and adds
    /// `dv_count` with initial and write defaults of zero.
    ///
    /// Returns an error if the partition type is empty or the fields do not form a valid schema.
    pub fn schema(partition_type: StructType, format_version: FormatVersion) -> Result<Schema> {
        if partition_type.fields().is_empty() {
            return Err(invalid_data!(
                "Partition statistics require a nonempty partition type"
            ));
        }

        let delete_counters_required = format_version == FormatVersion::V3;
        let mut fields = vec![
            NestedField::required(
                PARTITION_STATS_PARTITION_FIELD.0,
                PARTITION_STATS_PARTITION_FIELD.1,
                Type::Struct(partition_type),
            )
            .into(),
            primitive_field(PARTITION_STATS_SPEC_ID_FIELD, PrimitiveType::Int, true),
            primitive_field(
                PARTITION_STATS_DATA_RECORD_COUNT_FIELD,
                PrimitiveType::Long,
                true,
            ),
            primitive_field(
                PARTITION_STATS_DATA_FILE_COUNT_FIELD,
                PrimitiveType::Int,
                true,
            ),
            primitive_field(
                PARTITION_STATS_TOTAL_DATA_FILE_SIZE_IN_BYTES_FIELD,
                PrimitiveType::Long,
                true,
            ),
            primitive_field(
                PARTITION_STATS_POSITION_DELETE_RECORD_COUNT_FIELD,
                PrimitiveType::Long,
                delete_counters_required,
            ),
            primitive_field(
                PARTITION_STATS_POSITION_DELETE_FILE_COUNT_FIELD,
                PrimitiveType::Int,
                delete_counters_required,
            ),
            primitive_field(
                PARTITION_STATS_EQUALITY_DELETE_RECORD_COUNT_FIELD,
                PrimitiveType::Long,
                delete_counters_required,
            ),
            primitive_field(
                PARTITION_STATS_EQUALITY_DELETE_FILE_COUNT_FIELD,
                PrimitiveType::Int,
                delete_counters_required,
            ),
            primitive_field(
                PARTITION_STATS_TOTAL_RECORD_COUNT_FIELD,
                PrimitiveType::Long,
                false,
            ),
            primitive_field(
                PARTITION_STATS_LAST_UPDATED_AT_FIELD,
                PrimitiveType::Long,
                false,
            ),
            primitive_field(
                PARTITION_STATS_LAST_UPDATED_SNAPSHOT_ID_FIELD,
                PrimitiveType::Long,
                false,
            ),
        ];

        if format_version == FormatVersion::V3 {
            fields.push(
                NestedField::required(
                    PARTITION_STATS_DV_COUNT_FIELD.0,
                    PARTITION_STATS_DV_COUNT_FIELD.1,
                    PrimitiveType::Int.into(),
                )
                .with_initial_default(Literal::int(0))
                .with_write_default(Literal::int(0))
                .into(),
            );
        }

        Schema::builder().with_fields(fields).build()
    }

    /// Returns the partition values, or `None` when not selected.
    pub fn partition(&self) -> Option<&Struct> {
        self.partition.as_ref()
    }

    /// Returns the partition spec ID, or `None` when not selected.
    pub fn spec_id(&self) -> Option<i32> {
        self.spec_id
    }

    /// Returns records in data files before applying deletes, or `None` when not selected.
    pub fn data_record_count(&self) -> Option<i64> {
        self.data_record_count
    }

    /// Returns the number of data files, or `None` when not selected.
    pub fn data_file_count(&self) -> Option<i32> {
        self.data_file_count
    }

    /// Returns the total data-file size in bytes, or `None` when not selected.
    pub fn total_data_file_size_in_bytes(&self) -> Option<i64> {
        self.total_data_file_size_in_bytes
    }

    /// Returns position-delete records, including DV records, or `None` when unknown or not selected.
    pub fn position_delete_record_count(&self) -> Option<i64> {
        self.position_delete_record_count
    }

    /// Returns position-delete files, excluding DVs, or `None` when unknown or not selected.
    pub fn position_delete_file_count(&self) -> Option<i32> {
        self.position_delete_file_count
    }

    /// Returns equality-delete records, or `None` when unknown or not selected.
    pub fn equality_delete_record_count(&self) -> Option<i64> {
        self.equality_delete_record_count
    }

    /// Returns equality-delete files, or `None` when unknown or not selected.
    pub fn equality_delete_file_count(&self) -> Option<i32> {
        self.equality_delete_file_count
    }

    /// Returns the externally supplied exact live-record count, or `None` when unknown or not selected.
    ///
    /// This value is not inferred by subtracting delete counts from data counts.
    pub fn total_record_count(&self) -> Option<i64> {
        self.total_record_count
    }

    /// Returns the latest known update time in epoch milliseconds, or `None` when unknown or not selected.
    pub fn last_updated_at(&self) -> Option<i64> {
        self.last_updated_at
    }

    /// Returns the latest known update's snapshot ID, or `None` when unknown or not selected.
    pub fn last_updated_snapshot_id(&self) -> Option<i64> {
        self.last_updated_snapshot_id
    }

    /// Returns the deletion-vector count, or `None` when absent from this row or not selected.
    ///
    /// The v3 stored schema defines a zero default for older files. Constructing a partial
    /// row does not apply that default.
    pub fn dv_count(&self) -> Option<i32> {
        self.dv_count
    }
}

fn primitive_field(
    (id, name): (i32, &str),
    field_type: PrimitiveType,
    required: bool,
) -> NestedFieldRef {
    NestedField::new(id, name, field_type.into(), required).into()
}

#[cfg(test)]
mod tests {
    use super::PartitionStatistics;
    use crate::ErrorKind;
    use crate::spec::{
        FormatVersion, Literal, NestedField, PrimitiveType, Schema, Struct, StructType,
    };

    fn partition_type() -> StructType {
        StructType::new(vec![
            NestedField::optional(1000, "day", PrimitiveType::Date.into()).into(),
            NestedField::optional(1001, "region", PrimitiveType::String.into()).into(),
        ])
    }

    fn partition() -> Struct {
        [Some(Literal::date(0)), Some(Literal::string("us"))]
            .into_iter()
            .collect()
    }

    #[test]
    fn test_partition_statistics_v1_v2_schema() {
        // Enumerate the contract independently of the implementation's shared constants.
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "partition", partition_type().into()).into(),
                NestedField::required(2, "spec_id", PrimitiveType::Int.into()).into(),
                NestedField::required(3, "data_record_count", PrimitiveType::Long.into()).into(),
                NestedField::required(4, "data_file_count", PrimitiveType::Int.into()).into(),
                NestedField::required(
                    5,
                    "total_data_file_size_in_bytes",
                    PrimitiveType::Long.into(),
                )
                .into(),
                NestedField::optional(
                    6,
                    "position_delete_record_count",
                    PrimitiveType::Long.into(),
                )
                .into(),
                NestedField::optional(7, "position_delete_file_count", PrimitiveType::Int.into())
                    .into(),
                NestedField::optional(
                    8,
                    "equality_delete_record_count",
                    PrimitiveType::Long.into(),
                )
                .into(),
                NestedField::optional(9, "equality_delete_file_count", PrimitiveType::Int.into())
                    .into(),
                NestedField::optional(10, "total_record_count", PrimitiveType::Long.into()).into(),
                NestedField::optional(11, "last_updated_at", PrimitiveType::Long.into()).into(),
                NestedField::optional(12, "last_updated_snapshot_id", PrimitiveType::Long.into())
                    .into(),
            ])
            .build()
            .unwrap();

        for version in [FormatVersion::V1, FormatVersion::V2] {
            let actual = PartitionStatistics::schema(partition_type(), version).unwrap();
            assert_eq!(actual, expected);
            assert_eq!(actual.as_struct().fields().len(), 12);
            assert!(actual.field_by_id(13).is_none());
        }
    }

    #[test]
    fn test_partition_statistics_v3_schema() {
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "partition", partition_type().into()).into(),
                NestedField::required(2, "spec_id", PrimitiveType::Int.into()).into(),
                NestedField::required(3, "data_record_count", PrimitiveType::Long.into()).into(),
                NestedField::required(4, "data_file_count", PrimitiveType::Int.into()).into(),
                NestedField::required(
                    5,
                    "total_data_file_size_in_bytes",
                    PrimitiveType::Long.into(),
                )
                .into(),
                NestedField::required(
                    6,
                    "position_delete_record_count",
                    PrimitiveType::Long.into(),
                )
                .into(),
                NestedField::required(7, "position_delete_file_count", PrimitiveType::Int.into())
                    .into(),
                NestedField::required(
                    8,
                    "equality_delete_record_count",
                    PrimitiveType::Long.into(),
                )
                .into(),
                NestedField::required(9, "equality_delete_file_count", PrimitiveType::Int.into())
                    .into(),
                NestedField::optional(10, "total_record_count", PrimitiveType::Long.into()).into(),
                NestedField::optional(11, "last_updated_at", PrimitiveType::Long.into()).into(),
                NestedField::optional(12, "last_updated_snapshot_id", PrimitiveType::Long.into())
                    .into(),
                NestedField::required(13, "dv_count", PrimitiveType::Int.into())
                    .with_initial_default(Literal::int(0))
                    .with_write_default(Literal::int(0))
                    .into(),
            ])
            .build()
            .unwrap();

        let actual = PartitionStatistics::schema(partition_type(), FormatVersion::V3).unwrap();
        assert_eq!(actual, expected);
        assert_eq!(actual.as_struct().fields().len(), 13);
    }

    #[test]
    fn test_dv_initial_and_write_defaults_are_zero() {
        let schema = PartitionStatistics::schema(partition_type(), FormatVersion::V3).unwrap();
        for field in schema.as_struct().fields() {
            let expected = (field.id == 13).then(|| Literal::int(0));
            assert_eq!(field.initial_default, expected);
            assert_eq!(field.write_default, expected);
        }
    }

    #[test]
    fn test_nested_partition_fields_are_preserved() {
        let supplied = StructType::new(vec![
            NestedField::required(1007, "region", PrimitiveType::String.into()).into(),
            NestedField::optional(1002, "day", PrimitiveType::Date.into()).into(),
        ]);

        for version in [FormatVersion::V1, FormatVersion::V2, FormatVersion::V3] {
            let schema = PartitionStatistics::schema(supplied.clone(), version).unwrap();
            assert_eq!(
                schema.field_by_id(1).unwrap().field_type.as_ref(),
                &supplied.clone().into()
            );
            assert_eq!(schema.field_by_id(1007).unwrap(), &supplied.fields()[0]);
            assert_eq!(schema.field_by_id(1002).unwrap(), &supplied.fields()[1]);
        }
    }

    #[test]
    fn test_empty_partition_type_is_rejected() {
        for version in [FormatVersion::V1, FormatVersion::V2, FormatVersion::V3] {
            let error = PartitionStatistics::schema(StructType::new(vec![]), version).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::DataInvalid);
            assert!(error.message().contains("nonempty partition type"));
        }
    }

    #[test]
    fn test_partition_ids_cannot_collide_with_statistics_fields() {
        let conflicting = StructType::new(vec![
            NestedField::optional(3, "day", PrimitiveType::Date.into()).into(),
        ]);
        assert!(PartitionStatistics::schema(conflicting, FormatVersion::V3).is_err());
    }

    #[test]
    fn test_unselected_fields_return_none() {
        let row = PartitionStatistics::builder().build();
        assert_eq!(row, PartitionStatistics::default());
        assert_eq!(row.partition(), None);
        assert_eq!(row.spec_id(), None);
        assert_eq!(row.data_record_count(), None);
        assert_eq!(row.data_file_count(), None);
        assert_eq!(row.total_data_file_size_in_bytes(), None);
        assert_eq!(row.position_delete_record_count(), None);
        assert_eq!(row.position_delete_file_count(), None);
        assert_eq!(row.equality_delete_record_count(), None);
        assert_eq!(row.equality_delete_file_count(), None);
        assert_eq!(row.total_record_count(), None);
        assert_eq!(row.last_updated_at(), None);
        assert_eq!(row.last_updated_snapshot_id(), None);
        assert_eq!(row.dv_count(), None);
    }

    #[test]
    fn test_partial_row_does_not_initialize_unselected_counters() {
        let row = PartitionStatistics::builder()
            .with_data_file_count(2)
            .build();
        assert_eq!(row.data_file_count(), Some(2));
        assert_eq!(row.data_record_count(), None);
        assert_eq!(row.position_delete_file_count(), None);
        assert_eq!(row.dv_count(), None);
        assert_eq!(row.partition(), None);
        assert_eq!(row.spec_id(), None);
    }

    #[test]
    fn test_computation_row_initializes_zero_counters_and_null_total() {
        let values = partition();
        let row = PartitionStatistics::new_for_computation(values.clone(), 4);
        assert_eq!(row.partition(), Some(&values));
        assert_eq!(row.spec_id(), Some(4));
        assert_eq!(row.data_record_count(), Some(0));
        assert_eq!(row.data_file_count(), Some(0));
        assert_eq!(row.total_data_file_size_in_bytes(), Some(0));
        assert_eq!(row.position_delete_record_count(), Some(0));
        assert_eq!(row.position_delete_file_count(), Some(0));
        assert_eq!(row.equality_delete_record_count(), Some(0));
        assert_eq!(row.equality_delete_file_count(), Some(0));
        assert_eq!(row.dv_count(), Some(0));
        assert_eq!(row.total_record_count(), None);
        assert_eq!(row.last_updated_at(), None);
        assert_eq!(row.last_updated_snapshot_id(), None);
    }

    #[test]
    fn test_builder_preserves_all_supplied_fields_and_imported_exact_total() {
        let values = partition();
        let row = PartitionStatistics::builder()
            .with_partition(values.clone())
            .with_spec_id(4)
            .with_data_record_count(3000)
            .with_data_file_count(2)
            .with_total_data_file_size_in_bytes(31457280)
            .with_position_delete_record_count(100)
            .with_position_delete_file_count(1)
            .with_equality_delete_record_count(20)
            .with_equality_delete_file_count(1)
            .with_total_record_count(2900)
            .with_last_updated_at(1790640000000)
            .with_last_updated_snapshot_id(8998)
            .with_dv_count(1)
            .build();

        assert_eq!(row.partition(), Some(&values));
        assert_eq!(row.spec_id(), Some(4));
        assert_eq!(row.data_record_count(), Some(3000));
        assert_eq!(row.data_file_count(), Some(2));
        assert_eq!(row.total_data_file_size_in_bytes(), Some(31457280));
        assert_eq!(row.position_delete_record_count(), Some(100));
        assert_eq!(row.position_delete_file_count(), Some(1));
        assert_eq!(row.equality_delete_record_count(), Some(20));
        assert_eq!(row.equality_delete_file_count(), Some(1));
        assert_eq!(row.total_record_count(), Some(2900));
        assert_eq!(row.last_updated_at(), Some(1790640000000));
        assert_eq!(row.last_updated_snapshot_id(), Some(8998));
        assert_eq!(row.dv_count(), Some(1));
    }

    #[test]
    fn test_signed_counters_are_preserved() {
        let row = PartitionStatistics::builder()
            .with_data_record_count(i64::MIN)
            .with_data_file_count(i32::MIN)
            .with_total_data_file_size_in_bytes(i64::MAX)
            .with_position_delete_record_count(i64::MAX)
            .with_position_delete_file_count(i32::MAX)
            .with_equality_delete_record_count(-1)
            .with_equality_delete_file_count(i32::MIN)
            .with_dv_count(i32::MAX)
            .build();

        assert_eq!(row.data_record_count(), Some(i64::MIN));
        assert_eq!(row.data_file_count(), Some(i32::MIN));
        assert_eq!(row.total_data_file_size_in_bytes(), Some(i64::MAX));
        assert_eq!(row.position_delete_record_count(), Some(i64::MAX));
        assert_eq!(row.position_delete_file_count(), Some(i32::MAX));
        assert_eq!(row.equality_delete_record_count(), Some(-1));
        assert_eq!(row.equality_delete_file_count(), Some(i32::MIN));
        assert_eq!(row.dv_count(), Some(i32::MAX));
    }
}
