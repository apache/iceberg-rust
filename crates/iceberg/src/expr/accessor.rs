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

use std::sync::Arc;

use serde_derive::{Deserialize, Serialize};

use crate::spec::{Datum, Literal, PrimitiveType, Struct, Type};
use crate::{Error, ErrorKind, Result};

/// The type of field that an accessor points to.
/// Complex types (Struct, List, Map) can only be used for null checks,
/// while Primitive types can be used for value extraction.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum AccessorType {
    /// Primitive type - supports value extraction and null checks
    Primitive(PrimitiveType),
    /// Struct type - only supports null checks
    Struct,
    /// List type - only supports null checks
    List,
    /// Map type - only supports null checks
    Map,
}

impl AccessorType {
    /// Returns the primitive type if this is a primitive accessor, otherwise None
    pub fn as_primitive(&self) -> Option<&PrimitiveType> {
        match self {
            AccessorType::Primitive(p) => Some(p),
            _ => None,
        }
    }

    /// Returns true if this accessor type is complex (non-primitive)
    pub fn is_complex(&self) -> bool {
        !matches!(self, AccessorType::Primitive(_))
    }
}

impl From<&Type> for AccessorType {
    fn from(ty: &Type) -> Self {
        match ty {
            Type::Primitive(p) => AccessorType::Primitive(p.clone()),
            Type::Struct(_) => AccessorType::Struct,
            Type::List(_) => AccessorType::List,
            Type::Map(_) => AccessorType::Map,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct StructAccessor {
    position: usize,
    accessor_type: AccessorType,
    inner: Option<Box<StructAccessor>>,
}

pub(crate) type StructAccessorRef = Arc<StructAccessor>;

impl StructAccessor {
    pub(crate) fn new(position: usize, r#type: PrimitiveType) -> Self {
        StructAccessor {
            position,
            accessor_type: AccessorType::Primitive(r#type),
            inner: None,
        }
    }

    /// Create a new accessor for a complex type (struct, list, or map).
    /// Complex type accessors can only be used for null checks.
    pub(crate) fn new_complex(position: usize, ty: &Type) -> Self {
        StructAccessor {
            position,
            accessor_type: AccessorType::from(ty),
            inner: None,
        }
    }

    pub(crate) fn wrap(position: usize, inner: Box<StructAccessor>) -> Self {
        StructAccessor {
            position,
            accessor_type: inner.accessor_type().clone(),
            inner: Some(inner),
        }
    }

    pub(crate) fn position(&self) -> usize {
        self.position
    }

    /// Returns the accessor type (primitive or complex)
    pub(crate) fn accessor_type(&self) -> &AccessorType {
        &self.accessor_type
    }

    /// Returns the primitive type if this is a primitive accessor.
    /// For backward compatibility with code that expects a primitive type.
    pub(crate) fn r#type(&self) -> &PrimitiveType {
        match &self.accessor_type {
            AccessorType::Primitive(p) => p,
            // This should only be called for primitive accessors
            // Return a placeholder for complex types to avoid breaking existing code
            _ => &PrimitiveType::Boolean, // Placeholder, should not be used
        }
    }

    /// Check if the value at this accessor's position is null.
    /// This works for both primitive and complex types.
    pub(crate) fn is_null(&self, container: &Struct) -> Result<bool> {
        match &self.inner {
            None => Ok(container[self.position].is_none()),
            Some(inner) => {
                if let Some(Literal::Struct(wrapped)) = &container[self.position] {
                    inner.is_null(wrapped)
                } else if container[self.position].is_none() {
                    Ok(true)
                } else {
                    Err(Error::new(
                        ErrorKind::Unexpected,
                        "Nested accessor should only be wrapping a Struct",
                    ))
                }
            }
        }
    }

    pub(crate) fn get<'a>(&'a self, container: &'a Struct) -> Result<Option<Datum>> {
        match &self.inner {
            None => match &container[self.position] {
                None => Ok(None),
                Some(Literal::Primitive(literal)) => {
                    if let AccessorType::Primitive(prim_type) = &self.accessor_type {
                        Ok(Some(Datum::new(prim_type.clone(), literal.clone())))
                    } else {
                        Err(Error::new(
                            ErrorKind::Unexpected,
                            "Cannot extract Datum from complex type accessor",
                        ))
                    }
                }
                Some(_) => Err(Error::new(
                    ErrorKind::Unexpected,
                    "Expected Literal to be Primitive",
                )),
            },
            Some(inner) => {
                if let Some(Literal::Struct(wrapped)) = &container[self.position] {
                    inner.get(wrapped)
                } else {
                    Err(Error::new(
                        ErrorKind::Unexpected,
                        "Nested accessor should only be wrapping a Struct",
                    ))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::expr::accessor::{AccessorType, StructAccessor};
    use crate::spec::datatypes::{ListType, MapType, NestedField, StructType};
    use crate::spec::{Datum, Literal, PrimitiveType, Struct, Type};

    #[test]
    fn test_single_level_accessor() {
        let accessor = StructAccessor::new(1, PrimitiveType::Boolean);

        assert_eq!(accessor.r#type(), &PrimitiveType::Boolean);
        assert_eq!(accessor.position(), 1);

        let test_struct =
            Struct::from_iter(vec![Some(Literal::bool(false)), Some(Literal::bool(true))]);

        assert_eq!(accessor.get(&test_struct).unwrap(), Some(Datum::bool(true)));
    }

    #[test]
    fn test_single_level_accessor_null() {
        let accessor = StructAccessor::new(1, PrimitiveType::Boolean);

        assert_eq!(accessor.r#type(), &PrimitiveType::Boolean);
        assert_eq!(accessor.position(), 1);

        let test_struct = Struct::from_iter(vec![Some(Literal::bool(false)), None]);

        assert_eq!(accessor.get(&test_struct).unwrap(), None);
    }

    #[test]
    fn test_nested_accessor() {
        let nested_accessor = StructAccessor::new(1, PrimitiveType::Boolean);
        let accessor = StructAccessor::wrap(2, Box::new(nested_accessor));

        assert_eq!(accessor.r#type(), &PrimitiveType::Boolean);
        //assert_eq!(accessor.position(), 1);

        let nested_test_struct =
            Struct::from_iter(vec![Some(Literal::bool(false)), Some(Literal::bool(true))]);

        let test_struct = Struct::from_iter(vec![
            Some(Literal::bool(false)),
            Some(Literal::bool(false)),
            Some(Literal::Struct(nested_test_struct)),
        ]);

        assert_eq!(accessor.get(&test_struct).unwrap(), Some(Datum::bool(true)));
    }

    #[test]
    fn test_nested_accessor_null() {
        let nested_accessor = StructAccessor::new(0, PrimitiveType::Boolean);
        let accessor = StructAccessor::wrap(2, Box::new(nested_accessor));

        assert_eq!(accessor.r#type(), &PrimitiveType::Boolean);
        //assert_eq!(accessor.position(), 1);

        let nested_test_struct = Struct::from_iter(vec![None, Some(Literal::bool(true))]);

        let test_struct = Struct::from_iter(vec![
            Some(Literal::bool(false)),
            Some(Literal::bool(false)),
            Some(Literal::Struct(nested_test_struct)),
        ]);

        assert_eq!(accessor.get(&test_struct).unwrap(), None);
    }

    #[test]
    fn test_complex_type_accessor_struct() {
        let struct_type = Type::Struct(StructType::new(vec![Arc::new(NestedField::required(
            1,
            "inner",
            Type::Primitive(PrimitiveType::String),
        ))]));
        let accessor = StructAccessor::new_complex(0, &struct_type);

        assert!(accessor.accessor_type().is_complex());
        assert!(matches!(accessor.accessor_type(), AccessorType::Struct));

        // Test null check on non-null struct
        let inner_struct = Struct::from_iter(vec![Some(Literal::string("test".to_string()))]);
        let test_struct = Struct::from_iter(vec![Some(Literal::Struct(inner_struct))]);
        assert!(!accessor.is_null(&test_struct).unwrap());

        // Test null check on null struct
        let null_struct = Struct::from_iter(vec![None]);
        assert!(accessor.is_null(&null_struct).unwrap());
    }

    #[test]
    fn test_complex_type_accessor_list() {
        let list_type = Type::List(ListType::new(Arc::new(NestedField::list_element(
            1,
            Type::Primitive(PrimitiveType::Int),
            true,
        ))));
        let accessor = StructAccessor::new_complex(1, &list_type);

        assert!(accessor.accessor_type().is_complex());
        assert!(matches!(accessor.accessor_type(), AccessorType::List));

        // Test null check on non-null list
        let test_struct = Struct::from_iter(vec![
            Some(Literal::bool(false)),
            Some(Literal::List(vec![Some(Literal::int(1)), Some(Literal::int(2))])),
        ]);
        assert!(!accessor.is_null(&test_struct).unwrap());

        // Test null check on null list
        let null_struct = Struct::from_iter(vec![Some(Literal::bool(false)), None]);
        assert!(accessor.is_null(&null_struct).unwrap());
    }

    #[test]
    fn test_complex_type_accessor_map() {
        let map_type = Type::Map(MapType::new(
            Arc::new(NestedField::map_key_element(
                1,
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::map_value_element(
                2,
                Type::Primitive(PrimitiveType::Int),
                true,
            )),
        ));
        let accessor = StructAccessor::new_complex(0, &map_type);

        assert!(accessor.accessor_type().is_complex());
        assert!(matches!(accessor.accessor_type(), AccessorType::Map));

        // Test null check on null map
        let null_struct = Struct::from_iter(vec![None]);
        assert!(accessor.is_null(&null_struct).unwrap());
    }

    #[test]
    fn test_primitive_is_null() {
        let accessor = StructAccessor::new(0, PrimitiveType::Int);

        // Test null check on non-null primitive
        let test_struct = Struct::from_iter(vec![Some(Literal::int(42))]);
        assert!(!accessor.is_null(&test_struct).unwrap());

        // Test null check on null primitive
        let null_struct = Struct::from_iter(vec![None]);
        assert!(accessor.is_null(&null_struct).unwrap());
    }

    #[test]
    fn test_accessor_type_as_primitive() {
        let primitive = AccessorType::Primitive(PrimitiveType::Int);
        assert_eq!(primitive.as_primitive(), Some(&PrimitiveType::Int));
        assert!(!primitive.is_complex());

        let struct_type = AccessorType::Struct;
        assert_eq!(struct_type.as_primitive(), None);
        assert!(struct_type.is_complex());

        let list_type = AccessorType::List;
        assert_eq!(list_type.as_primitive(), None);
        assert!(list_type.is_complex());

        let map_type = AccessorType::Map;
        assert_eq!(map_type.as_primitive(), None);
        assert!(map_type.is_complex());
    }
}
