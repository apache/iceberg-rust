use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::{DataType, Field, Schema};
use geo_types::{Coord, Geometry, LineString, Point, Polygon};
//use iceberg::spec::{NestedField, PrimitiveType, Type, Schema, StructType};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent};
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
async fn main() {}
