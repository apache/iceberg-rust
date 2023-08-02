/*
 * Apache Iceberg REST Catalog API
 *
 * Defines the specification for the first version of the REST Catalog API. Implementations should ideally support both Iceberg table specs v1 and v2, with priority given to v2.
 *
 * The version of the OpenAPI document: 0.0.1
 *
 * Generated by: https://openapi-generator.tech
 */

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct CommitTableResponse {
    #[serde(rename = "metadata-location")]
    pub metadata_location: String,
    #[serde(rename = "metadata")]
    pub metadata: Box<crate::models::TableMetadata>,
}

impl CommitTableResponse {
    pub fn new(
        metadata_location: String,
        metadata: crate::models::TableMetadata,
    ) -> CommitTableResponse {
        CommitTableResponse {
            metadata_location,
            metadata: Box::new(metadata),
        }
    }
}
