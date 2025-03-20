use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::spec::MappedField;

/// TODO
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct MappedFields {
    fields: Vec<MappedField>,
    name_to_id: HashMap<String, i32>,
    id_to_field: HashMap<i32, MappedField>,
}

impl MappedFields {
    /// Create a new [`MappedFields`].
    pub fn new(fields: Vec<MappedField>) -> Self {
        let mut name_to_id = HashMap::new();
        let mut id_to_field = HashMap::new();

        for field in &fields {
            if let Some(id) = field.field_id() {
                id_to_field.insert(id, field.clone());
                for name in field.names() {
                    name_to_id.insert(name.to_string(), id);
                }
            }
        }

        Self {
            fields,
            name_to_id,
            id_to_field,
        }
    }

    /// Get a reference to the underlying fields.
    pub fn fields(&self) -> &[MappedField] {
        &self.fields
    }

    /// Get a field, by name, returning its ID if it exists, otherwise `None`.
    pub fn id(&self, field_name: String) -> Option<&i32> {
        self.name_to_id.get(&field_name)
    }

    /// Get a field, by ID, returning the underlying [`MappedField`] if it exists,
    /// otherwise `None`.
    pub fn field(&self, id: i32) -> Option<&MappedField> {
        self.id_to_field.get(&id)
    }
}

#[cfg(test)]
mod test {

    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn mapped_fields() {
        let my_fields = vec![
            MappedField::new(Some(1), vec!["field_one".to_string()], Vec::new()),
            MappedField::new(
                Some(2),
                vec!["field_two".to_string(), "field_foo".to_string()],
                Vec::new(),
            ),
        ];

        let mapped_fields = MappedFields::new(my_fields);

        assert!(
            mapped_fields.field(1000).is_none(),
            "Field with ID '1000' was not inserted to the collection"
        );
        assert_eq!(
            *mapped_fields.field(1).unwrap(),
            MappedField::new(Some(1), vec!["field_one".to_string()], Vec::new()),
        );
        assert_eq!(
            *mapped_fields.field(2).unwrap(),
            MappedField::new(
                Some(2),
                vec!["field_two".to_string(), "field_foo".to_string()],
                Vec::new(),
            )
        );

        assert!(
            mapped_fields.id("not_exist".to_string()).is_none(),
            "Field was not expected to exist in the collection"
        );
        assert_eq!(mapped_fields.id("field_two".to_string()).cloned(), Some(2));
        assert_eq!(
            mapped_fields.id("field_foo".to_string()).cloned(),
            Some(2),
            "Field with another name shares the same ID of '2'"
        );
        assert_eq!(mapped_fields.id("field_one".to_string()).cloned(), Some(1));
    }
}
