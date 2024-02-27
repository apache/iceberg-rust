use anyhow::anyhow;
use aws_sdk_dynamodb::error::{BuildError, SdkError};
use iceberg::{Error, ErrorKind, Result};
use std::fmt::Debug;

pub(crate) trait SdkResultExt<T> {
    fn wrap_err(self) -> Result<T>;
}

impl<T, E: Debug> SdkResultExt<T> for std::result::Result<T, SdkError<E>> {
    fn wrap_err(self) -> Result<T> {
        self.map_err(from_sdk_error)
    }
}

impl<T> SdkResultExt<T> for std::result::Result<T, BuildError> {
    fn wrap_err(self) -> Result<T> {
        self.map_err(from_build_error)
    }
}

pub fn from_build_error(error: BuildError) -> Error {
    Error::new(
        ErrorKind::Unexpected,
        "operation failed for dynamodb sdk error".to_string(),
    )
    .with_source(anyhow!("dynamodb sdk error: {:?}", error))
}

pub fn from_sdk_error<E>(error: SdkError<E>) -> Error
where
    E: Debug,
{
    Error::new(
        ErrorKind::Unexpected,
        "operation failed for dynamodb sdk error".to_string(),
    )
    .with_source(anyhow!("dynamodb sdk error: {:?}", error))
}
