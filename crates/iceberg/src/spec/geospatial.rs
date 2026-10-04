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

//! Geospatial types defined by the Iceberg specification.

use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};

use super::PrimitiveType;
use crate::error::Result;

pub(crate) const DEFAULT_GEOSPATIAL_CRS: &str = "OGC:CRS84";
pub(super) const MAX_GEOSPATIAL_CRS_BYTES: usize = 128;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Crs(String);

impl Crs {
    fn new(crs: Option<String>) -> Result<Self> {
        let crs = crs.unwrap_or_else(|| DEFAULT_GEOSPATIAL_CRS.to_string());
        let crs = crs.trim().to_string();
        if crs.len() > MAX_GEOSPATIAL_CRS_BYTES {
            return Err(crate::Error::new(
                crate::ErrorKind::DataInvalid,
                format!("Geospatial CRS must be at most {MAX_GEOSPATIAL_CRS_BYTES} bytes"),
            ));
        }
        if crs.is_empty() || crs.contains([',', ')']) {
            return Err(crate::Error::new(
                crate::ErrorKind::DataInvalid,
                "Geospatial CRS must be non-empty and must not contain ',' or ')'",
            ));
        }

        Ok(Self(crs))
    }
}

impl Default for Crs {
    fn default() -> Self {
        Self(DEFAULT_GEOSPATIAL_CRS.to_string())
    }
}

impl PartialEq for Crs {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq_ignore_ascii_case(&other.0)
    }
}

impl Eq for Crs {}

impl Hash for Crs {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for byte in self.0.bytes() {
            state.write_u8(byte.to_ascii_lowercase());
        }
    }
}

/// Iceberg geometry type.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Hash, Default)]
pub struct GeometryType {
    crs: Crs,
}

impl GeometryType {
    /// Creates a geometry type with an optional coordinate reference system.
    pub fn new(crs: Option<String>) -> Result<Self> {
        Ok(Self {
            crs: Crs::new(crs)?,
        })
    }

    /// Returns the coordinate reference system.
    pub fn crs(&self) -> &str {
        &self.crs.0
    }
}

/// Iceberg geography edge interpolation algorithm.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy, Hash, Default)]
#[serde(rename_all = "lowercase")]
pub enum EdgeInterpolationAlgorithm {
    /// Spherical edge interpolation.
    #[default]
    Spherical,
    /// Vincenty edge interpolation.
    Vincenty,
    /// Thomas edge interpolation.
    Thomas,
    /// Andoyer edge interpolation.
    Andoyer,
    /// Karney edge interpolation.
    Karney,
}

impl EdgeInterpolationAlgorithm {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Spherical => "spherical",
            Self::Vincenty => "vincenty",
            Self::Thomas => "thomas",
            Self::Andoyer => "andoyer",
            Self::Karney => "karney",
        }
    }

    fn parse(value: &str) -> std::result::Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "spherical" => Ok(Self::Spherical),
            "vincenty" => Ok(Self::Vincenty),
            "thomas" => Ok(Self::Thomas),
            "andoyer" => Ok(Self::Andoyer),
            "karney" => Ok(Self::Karney),
            _ => Err(format!(
                "Unknown geography edge interpolation algorithm: {value}"
            )),
        }
    }
}

/// Iceberg geography type.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Hash)]
pub struct GeographyType {
    crs: Crs,
    algorithm: EdgeInterpolationAlgorithm,
}

impl Default for GeographyType {
    fn default() -> Self {
        Self {
            crs: Crs::default(),
            algorithm: EdgeInterpolationAlgorithm::Spherical,
        }
    }
}

impl GeographyType {
    /// Creates a geography type with an optional coordinate reference system and edge interpolation algorithm.
    pub fn new(crs: Option<String>, algorithm: EdgeInterpolationAlgorithm) -> Result<Self> {
        Ok(Self {
            crs: Crs::new(crs)?,
            algorithm,
        })
    }

    /// Returns the coordinate reference system.
    pub fn crs(&self) -> &str {
        &self.crs.0
    }

    /// Returns the edge interpolation algorithm.
    pub fn algorithm(&self) -> EdgeInterpolationAlgorithm {
        self.algorithm
    }
}

fn parse_geospatial_params<'a>(
    value: &'a str,
    type_name: &str,
) -> std::result::Result<Vec<&'a str>, String> {
    if value == type_name {
        return Ok(vec![]);
    }

    let params = value
        .strip_prefix(type_name)
        .map(str::trim_start)
        .and_then(|s| s.strip_prefix('('))
        .and_then(|s| s.strip_suffix(')'))
        .ok_or_else(|| format!("Invalid {type_name} type: {value}"))?;

    if params.trim().is_empty() {
        return Err(format!("{type_name} requires a non-empty CRS"));
    }

    Ok(params.split(',').map(str::trim).collect())
}

pub(super) fn parse_geometry(value: &str) -> std::result::Result<PrimitiveType, String> {
    let params = parse_geospatial_params(value.trim(), "geometry")?;
    let geometry = match params.as_slice() {
        [] => GeometryType::default(),
        [crs] if !crs.is_empty() => {
            GeometryType::new(Some((*crs).to_string())).map_err(|err| err.to_string())?
        }
        _ => return Err(format!("Invalid geometry type: {value}")),
    };

    Ok(PrimitiveType::Geometry(geometry))
}

pub(super) fn parse_geography(value: &str) -> std::result::Result<PrimitiveType, String> {
    let params = parse_geospatial_params(value.trim(), "geography")?;
    let geography = match params.as_slice() {
        [] => GeographyType::default(),
        [crs] if !crs.is_empty() => GeographyType::new(
            Some((*crs).to_string()),
            EdgeInterpolationAlgorithm::Spherical,
        )
        .map_err(|err| err.to_string())?,
        [crs, algorithm] if !crs.is_empty() && !algorithm.is_empty() => GeographyType::new(
            Some((*crs).to_string()),
            EdgeInterpolationAlgorithm::parse(algorithm)?,
        )
        .map_err(|err| err.to_string())?,
        _ => return Err(format!("Invalid geography type: {value}")),
    };

    Ok(PrimitiveType::Geography(geography))
}
