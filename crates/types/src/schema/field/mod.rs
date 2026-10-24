//! Preserve logical extension identity across Arrow schema boundaries.
use crate::VeinDataType;
use arrow::datatypes::Field;
use error::{Result, VeinError};
use std::collections::HashMap;
use std::sync::Arc;

const EXTENSION_NAME: &str = "ARROW:extension:name";

impl VeinDataType {
    /// Produces an Arrow field, retaining logical extension identity in metadata.
    pub fn to_arrow_field(&self, name: impl Into<String>, nullable: bool) -> Field {
        let field = Field::new(name, self.to_arrow(), nullable);
        match self {
            Self::Extension(name, _) => field.with_metadata(HashMap::from([
                (EXTENSION_NAME.to_owned(), name.clone()),
            ])),
            _ => field,
        }
    }

    /// Recovers logical identity without confusing a storage decimal with an integer.
    pub fn from_arrow_field(field: &Field) -> Result<Self> {
        let storage = Self::try_from(field.data_type())?;
        let Some(name) = field.metadata().get(EXTENSION_NAME) else { return Ok(storage); };
        if name.is_empty() {
            return Err(VeinError::invalid_input("Arrow extension identity cannot be empty"));
        }
        let logical = Self::Extension(name.clone(), Arc::new(storage));
        if name == crate::wide_integer::INT128_EXTENSION && !logical.is_int128() {
            return Err(VeinError::invalid_input("Int128 Arrow extension has an incompatible storage layout"));
        }
        Ok(logical)
    }

    /// Human-readable logical type for result protocols; storage remains available separately.
    pub fn arrow_field_type_name(field: &Field) -> Result<String> {
        let logical = Self::from_arrow_field(field)?;
        Ok(if logical.is_int128() {
            "Int128".to_owned()
        } else {
            match logical {
                Self::Extension(name, _) => name,
                _ => format!("{:?}", field.data_type()),
            }
        })
    }
}
