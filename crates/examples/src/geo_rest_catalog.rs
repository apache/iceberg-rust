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

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, Int32Array, LargeBinaryArray, StringArray};
use futures::TryStreamExt;
use geo_types::{Geometry, Point};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::writer::IcebergWriterBuilder;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, RestCatalogBuilder};
use parquet::file::properties::WriterProperties;

static REST_URI: &str = "http://localhost:8181";
static NAMESPACE: &str = "ns1";
static TABLE_NAME: &str = "cities_table2";

//This is an example of creating and loading an table using a schema with
// geo types via the Iceberg REST Catalog.
//
/// A running instance of the iceberg-rest catalog on port 8181 is required. You can find how to run
/// the iceberg-rest catalog with `docker compose` in the official
/// [quickstart documentation](https://iceberg.apache.org/spark-quickstart/).

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
        .map_err(|e| {
            eprintln!("Failed to connect to REST catalog: {:?}", e);
            eprintln!("Error: {}", e);
            e
        })
        .unwrap();
    println!("Connected to REST Catalog at {}", REST_URI);

    let namespace_ident = NamespaceIdent::from_vec(vec![NAMESPACE.to_string()]).unwrap();
    let table_ident = TableIdent::new(namespace_ident.clone(), TABLE_NAME.to_string());

    println!("Checking if table exists...");
    let table_exists = catalog
        .table_exists(&table_ident)
        .await
        .map_err(|e| {
            eprintln!("Failed to check if table exists: {:?}", e);
            eprintln!("Error: {}", e);
            e
        })
        .unwrap();

    if table_exists {
        println!("Table {TABLE_NAME} already exists, dropping now.");
        catalog
            .drop_table(&table_ident)
            .await
            .map_err(|e| {
                eprintln!("Failed to drop table: {:?}", e);
                eprintln!("Error: {}", e);
                e
            })
            .unwrap();
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
    let table_creation = TableCreation::builder()
        .name(table_ident.name.clone())
        .schema(iceberg_schema.clone())
        .properties(HashMap::from([("geo".to_string(), "geotestx".to_string())]))
        .build();

    let _created_table = catalog
        .create_table(&table_ident.namespace, table_creation)
        .await
        .map_err(|e| {
            eprintln!("\n=== FAILED TO CREATE TABLE ===");
            eprintln!("Error type: {:?}", e);
            eprintln!("Error message: {}", e);
            eprintln!("Namespace: {:?}", table_ident.namespace);
            eprintln!("Table name: {}", table_ident.name);
            eprintln!("==============================\n");
            e
        })
        .unwrap();
    println!("Table {TABLE_NAME} created.");
    assert!(
        catalog
            .list_tables(&namespace_ident)
            .await
            .unwrap()
            .contains(&table_ident)
    );
    let schema: Arc<arrow_schema::Schema> = Arc::new(
        _created_table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );
    let location_generator =
        DefaultLocationGenerator::new(_created_table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        "geo_type_example".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        _created_table.metadata().current_schema().clone(),
    );
    let rolling_file_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        _created_table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_file_writer_builder);
    let data_file_writer = data_file_writer_builder.build(None).await.unwrap();

    let features = mock_sample_features();
    let ids: ArrayRef = Arc::new(Int32Array::from_iter_values(features.iter().map(|f| f.id)));
    let names: ArrayRef = Arc::new(StringArray::from_iter_values(
        features.iter().map(|f| f.name.as_str()),
    ));
    let geometries_wkb: ArrayRef = Arc::new(LargeBinaryArray::from_iter_values(
        features.iter().map(|f| f.to_wkb()),
    ));
    let geometry_types: ArrayRef = Arc::new(StringArray::from_iter_values(
        features.iter().map(|f| f.geometry_type()),
    ));
    let srids: ArrayRef = Arc::new(Int32Array::from_iter_values(
        features.iter().map(|f| f.srid),
    ));
    let bbox_min_xs: ArrayRef = Arc::new(Float64Array::from_iter_values(
        features.iter().map(|f| f.bbox().0),
    ));
    let bbox_min_ys: ArrayRef = Arc::new(Float64Array::from_iter_values(
        features.iter().map(|f| f.bbox().1),
    ));
    let bbox_max_xs: ArrayRef = Arc::new(Float64Array::from_iter_values(
        features.iter().map(|f| f.bbox().2),
    ));
    let bbox_max_ys: ArrayRef = Arc::new(Float64Array::from_iter_values(
        features.iter().map(|f| f.bbox().3),
    ));

    let countries: ArrayRef = Arc::new(StringArray::from_iter_values(
        features
            .iter()
            .map(|f| f.properties.get("country").unwrap().as_str()),
    ));
    let populations: ArrayRef = Arc::new(StringArray::from_iter_values(
        features
            .iter()
            .map(|f| f.properties.get("population").unwrap().as_str()),
    ));
    //TODO: make write with credentials
    /*let record_batch = RecordBatch::try_new(schema.clone(), vec![
            ids,
            names,
            geometries_wkb,
            geometry_types,
            srids,
            bbox_min_xs,
            bbox_min_ys,
            bbox_max_xs,
            bbox_max_ys,
            countries,
            populations,
        ])
        .unwrap();

        data_file_writer.write(record_batch.clone()).await.unwrap();
        let data_file = data_file_writer.close().await.unwrap();
    */

    let loaded_table = catalog.load_table(&table_ident).await.unwrap();
    println!("Table {TABLE_NAME} loaded!\n\nTable: {loaded_table:?}");
}
