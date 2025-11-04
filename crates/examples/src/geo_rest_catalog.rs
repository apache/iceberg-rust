use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, BinaryArray, Float64Array, Int32Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use geo_types::{Coord, Geometry, LineString, Point, Polygon};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, RestCatalogBuilder};

static REST_URI: &str = "http://localhost:8081";
static NAMESPACE: &str = "geo_data";
static TABLE_NAME: &str = "cities";

#[derive(Debug, Clone)]
struct GeoFeature {
    id: i32,
    name: String,
    properties: HashMap<String, String>,
    geometry: Geometry,
    srid: i32,
}

impl GeoFeature {
    fn new(
        id: i32,
        name: &str,
        properties: HashMap<String, String>,
        geometry: Geometry,
        srid: i32,
    ) -> Self {
        Self {
            id,
            name: name.to_string(),
            properties,
            geometry,
            srid,
        }
    }

    fn bbox(&self) -> (f64, f64, f64, f64) {
        match &self.geometry {
            Geometry::Point(point) => {
                let coord = point.0;
                (coord.x, coord.y, coord.x, coord.y)
            }
            Geometry::LineString(line) => {
                let coords: Vec<_> = line.coords().collect();
                let xs: Vec<f64> = coords.iter().map(|p| p.x).collect();
                let ys: Vec<f64> = coords.iter().map(|p| p.y).collect();
                (
                    xs.iter().cloned().fold(f64::INFINITY, f64::min),
                    ys.iter().cloned().fold(f64::INFINITY, f64::min),
                    xs.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
                    ys.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
                )
            }
            Geometry::Polygon(poly) => {
                let exterior = poly.exterior();
                let coords: Vec<_> = exterior.coords().collect();
                let xs: Vec<f64> = coords.iter().map(|p| p.x).collect();
                let ys: Vec<f64> = coords.iter().map(|p| p.y).collect();
                (
                    xs.iter().cloned().fold(f64::INFINITY, f64::min),
                    ys.iter().cloned().fold(f64::INFINITY, f64::min),
                    xs.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
                    ys.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
                )
            }
            _ => (0.0, 0.0, 0.0, 0.0),
        }
    }

    fn geometry_type(&self) -> &str {
        match &self.geometry {
            Geometry::Point(_) => "Point",
            Geometry::LineString(_) => "LineString",
            Geometry::Polygon(_) => "Polygon",
            Geometry::MultiPoint(_) => "MultiPoint",
            Geometry::MultiLineString(_) => "MultiLineString",
            Geometry::MultiPolygon(_) => "MultiPolygon",
            _ => "Geometry",
        }
    }

    fn to_wkb(&self) -> Vec<u8> {
        use wkb::writer::{WriteOptions, write_geometry};
        let mut buf = Vec::new();
        write_geometry(&mut buf, &self.geometry, &WriteOptions::default())
            .expect("Failed to write WKB");
        buf
    }
}

fn mock_sample_features() -> Vec<GeoFeature> {
    let mut features = Vec::new();
    let salt_lake_city = GeoFeature {
        id: 1,
        name: "Salt Lake City".to_string(),
        geometry: Geometry::Point(Point::new(-111.89, 40.76)),
        srid: 4326,
        properties: HashMap::from([
            ("country".to_string(), "USA".to_string()),
            ("population".to_string(), "200000".to_string()),
        ]),
    };
    features.push(salt_lake_city);
    let denver = GeoFeature {
        id: 2,
        name: "Denver".to_string(),
        geometry: Geometry::Point(Point::new(-104.99, 39.74)),
        srid: 4326,
        properties: HashMap::from([
            ("country".to_string(), "USA".to_string()),
            ("population".to_string(), "700000".to_string()),
        ]),
    };
    features.push(denver);
    features
}

#[tokio::main]
async fn main() {
    println!("Geo Types Iceberg REST Catalog");
    let catalog = RestCatalogBuilder::default()
        .load(
            "rest",
            HashMap::from([(REST_CATALOG_PROP_URI.to_string(), REST_URI.to_string())]),
        )
        .await
        .unwrap();
    println!("Connected to REST Catalog at {}", REST_URI);

    let namespace_ident = NamespaceIdent::from_vec(vec![NAMESPACE.to_string()]).unwrap();
    let table_ident = TableIdent::new(namespace_ident, TABLE_NAME.to_string());
    if catalog.table_exists(&table_ident).await.unwrap() {
        println!("Table '{}' already exists, dropping it", TABLE_NAME);
        catalog.drop_table(&table_ident).await.unwrap();
    }
    let iceberg_schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id".to_string(), Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(
                2,
                "name".to_string(),
                Type::Primitive(PrimitiveType::String),
            )
            .into(),
            NestedField::required(
                3,
                "geometry_wkb".to_string(),
                Type::Primitive(PrimitiveType::Binary),
            )
            .into(),
            NestedField::required(
                4,
                "geometry_type".to_string(),
                Type::Primitive(PrimitiveType::String),
            )
            .into(),
            NestedField::required(5, "srid".to_string(), Type::Primitive(PrimitiveType::Int))
                .into(),
            NestedField::required(
                6,
                "bbox_min_x".to_string(),
                Type::Primitive(PrimitiveType::Double),
            )
            .into(),
            NestedField::required(
                7,
                "bbox_min_y".to_string(),
                Type::Primitive(PrimitiveType::Double),
            )
            .into(),
            NestedField::required(
                8,
                "bbox_max_x".to_string(),
                Type::Primitive(PrimitiveType::Double),
            )
            .into(),
            NestedField::required(
                9,
                "bbox_max_y".to_string(),
                Type::Primitive(PrimitiveType::Double),
            )
            .into(),
            NestedField::required(
                10,
                "country".to_string(),
                Type::Primitive(PrimitiveType::String),
            )
            .into(),
            NestedField::required(
                11,
                "population".to_string(),
                Type::Primitive(PrimitiveType::String),
            )
            .into(),
        ])
        .with_schema_id(1)
        .with_identifier_field_ids(vec![1])
        .build()
        .unwrap();
    todo!()
}
