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
pub struct MapType {
    #[serde(rename = "type")]
    pub r#type: RHashType,
    #[serde(rename = "key-id")]
    pub key_id: i32,
    #[serde(rename = "key")]
    pub key: Box<crate::models::Type>,
    #[serde(rename = "value-id")]
    pub value_id: i32,
    #[serde(rename = "value")]
    pub value: Box<crate::models::Type>,
    #[serde(rename = "value-required")]
    pub value_required: bool,
}

impl MapType {
    pub fn new(
        r#type: RHashType,
        key_id: i32,
        key: crate::models::Type,
        value_id: i32,
        value: crate::models::Type,
        value_required: bool,
    ) -> MapType {
        MapType {
            r#type,
            key_id,
            key: Box::new(key),
            value_id,
            value: Box::new(value),
            value_required,
        }
    }
}

///
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum RHashType {
    #[serde(rename = "map")]
    Map,
}

impl Default for RHashType {
    fn default() -> RHashType {
        Self::Map
    }
}
