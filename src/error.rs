use thiserror::Error;

#[derive(Error, Debug)]
pub enum IcebergError {
    #[error("The type `{0}` cannot be stored as bytes.")]
    ValueByteConversion(String),
    #[error("Failed to convert slice to array")]
    TryFromSlice(#[from] std::array::TryFromSliceError),
    #[error("Failed to convert u8 to string")]
    Utf8(#[from] std::str::Utf8Error),
}
