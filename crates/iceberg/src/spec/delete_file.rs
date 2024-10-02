use std::collections::HashMap;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::SchemaRef;
use futures::StreamExt;

use crate::scan::ArrowRecordBatchStream;
use crate::spec::{NestedField, PrimitiveType, Schema, Type};
use crate::{Error, ErrorKind, Result};

pub(crate) const FIELD_ID_DELETE_FILE_PATH: i32 = i32::MAX - 101;
pub(crate) const FIELD_ID_DELETE_POS: i32 = i32::MAX - 102;
// pub(crate) const FIELD_ID_DELETE_ROW: i32 = i32::MAX - 103;

pub(crate) const FIELD_NAME_DELETE_FILE_PATH: &str = "file_path";
pub(crate) const FIELD_NAME_DELETE_POS: &str = "pos";
// pub(crate) const FIELD_NAME_DELETE_ROW: &str = "row";

// Represents a parsed Delete file that can be safely stored
// in the Object Cache.
#[allow(dead_code)]
pub(crate) enum Deletes {
    // Positional delete files are parsed into a map of
    // filename to a sorted list of row indices.
    // TODO: Ignoring the stored rows that are present in
    //   positional deletes for now. I think they only used for statistics?
    Positional(HashMap<String, Vec<u64>>),

    // Equality delete files are initially parsed solely as an
    // unprocessed list of `RecordBatch`es from the equality
    // delete files.
    // I don't think we can do better than this by
    // storing a Predicate (because the equality deletes use the
    // field_id rather than the field name, so if we keep this as
    // a Predicate then a field name change would break it).
    // Similarly, I don't think we can store this as a BoundPredicate
    // as the column order could be different across different data
    // files and so the accessor in the bound predicate could be invalid).
    Equality(Vec<RecordBatch>),
}

enum PosDelSchema {
    WithRow,
    WithoutRow,
}

#[allow(dead_code)]
fn positional_delete_file_without_row_schema() -> Schema {
    Schema::builder()
        .with_fields([
            NestedField::required(
                FIELD_ID_DELETE_FILE_PATH,
                FIELD_NAME_DELETE_FILE_PATH,
                Type::Primitive(PrimitiveType::String),
            )
            .into(),
            NestedField::required(
                FIELD_ID_DELETE_POS,
                FIELD_NAME_DELETE_POS,
                Type::Primitive(PrimitiveType::Long),
            )
            .into(),
        ])
        .build()
        .unwrap()
}

fn validate_schema(schema: SchemaRef) -> Result<PosDelSchema> {
    let fields = schema.flattened_fields();
    match fields.len() {
        2 | 3 => {
            let path_field = fields[0];
            let pos_field = fields[1];
            if path_field.dict_id().unwrap() as i32 != FIELD_ID_DELETE_FILE_PATH
                || path_field.name() != FIELD_NAME_DELETE_FILE_PATH
                || pos_field.dict_id().unwrap() as i32 != FIELD_ID_DELETE_POS
                || pos_field.name() != FIELD_NAME_DELETE_POS
            {
                Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Positional Delete file did not have the expected schema",
                ))
            } else if fields.len() == 2 {
                Ok(PosDelSchema::WithoutRow)
            } else {
                // TODO: should check that col 3 is of type Struct
                //   and that it contains a subset of the table schema
                Ok(PosDelSchema::WithRow)
            }
        }
        _ => Err(Error::new(
            ErrorKind::DataInvalid,
            "Positional Delete file did not have the expected schema",
        )),
    }
}

pub(crate) async fn parse_positional_delete_file(
    mut record_batch_stream: ArrowRecordBatchStream,
) -> Result<Deletes> {
    let mut result: HashMap<String, Vec<u64>> = HashMap::new();

    while let Some(batch) = record_batch_stream.next().await {
        let batch = batch?;
        let schema = batch.schema();

        // Don't care about what schema type it is at
        // present as we're ignoring the "row" column from files
        // with 3-column schemas. We only care if it is valid
        let _schema_type = validate_schema(schema)?;

        let columns = batch.columns();

        let file_paths = columns[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .values();
        let positions = columns[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values();

        for (file_path, pos) in file_paths.iter().zip(positions.iter()) {
            let pos = (*pos).try_into()?;
            result
                .entry(file_path.to_string())
                .and_modify(|entry| {
                    (*entry).push(pos);
                })
                .or_insert(vec![pos]);
        }
    }

    Ok(Deletes::Positional(result))
}

pub(crate) async fn parse_equality_delete_file(
    mut record_batch_stream: ArrowRecordBatchStream,
) -> Result<Deletes> {
    let mut result: Vec<RecordBatch> = Vec::new();

    while let Some(batch) = record_batch_stream.next().await {
        result.push(batch?);
    }

    Ok(Deletes::Equality(result))
}
