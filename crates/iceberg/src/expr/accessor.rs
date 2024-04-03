use crate::spec::{Literal, Struct, Type};
use serde_derive::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
enum InnerOrType {
    Inner(Arc<StructAccessor>),
    Type(Type),
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct StructAccessor {
    position: i32,
    inner_or_type: InnerOrType,
}

pub(crate) type StructAccessorRef = Arc<StructAccessor>;

impl StructAccessor {
    pub(crate) fn new(position: i32, r#type: Type) -> Self {
        StructAccessor {
            position,
            inner_or_type: InnerOrType::Type(r#type),
        }
    }

    pub(crate) fn wrap(position: i32, inner: StructAccessorRef) -> Self {
        StructAccessor {
            position,
            inner_or_type: InnerOrType::Inner(inner),
        }
    }

    pub fn position(&self) -> i32 {
        self.position
    }

    fn r#type(&self) -> &Type {
        match &self.inner_or_type {
            InnerOrType::Inner(inner) => inner.r#type(),
            InnerOrType::Type(r#type) => r#type,
        }
    }

    fn get<'a>(&'a self, container: &'a Struct) -> &Literal {
        match &self.inner_or_type {
            InnerOrType::Inner(inner) => match container.get(self.position) {
                Literal::Struct(wrapped) => inner.get(wrapped),
                _ => {
                    panic!("Nested accessor should only be wrapping a Struct")
                }
            },
            InnerOrType::Type(_) => container.get(self.position),
        }
    }
}
