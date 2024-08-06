use arrow::error::ArrowError;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use delta_kernel::Error as DKError;
use thiserror::Error;
use url::{ParseError, Url};

#[derive(Debug, Error)]
pub enum DeltaError {
    #[error("An error occured with Delta Table")]
    DeltaTableError(#[from] DKError),
    #[error("An error occured with handling Arrow data")]
    ArrowError(#[from] ArrowError),
}

pub fn type_from_str(type_str: &str) -> Result<DataType, String> {
    match type_str {
        "int" => Ok(DataType::Int32),
        "bigint" => Ok(DataType::Int64),
        "float" => Ok(DataType::Float32),
        "double" => Ok(DataType::Float64),
        "string" => Ok(DataType::Utf8),
        "date" => Ok(DataType::Date32),
        "timestamp" => Ok(DataType::Timestamp(TimeUnit::Second, None)),
        "timestamp_ms" => Ok(DataType::Timestamp(TimeUnit::Millisecond, None)),
        _ => Err(String::from("Unsupported type string")),
    }
}

pub fn ensure_scheme(s: &str) -> Result<Url, ()> {
    match Url::parse(s) {
        Ok(url) => Ok(url),
        Err(ParseError::RelativeUrlWithoutBase) => {
            let local_path = std::path::Path::new(s).canonicalize().unwrap();
            if local_path.is_file() {
                Url::from_file_path(&local_path)
            } else {
                Url::from_directory_path(&local_path)
            }
        }
        Err(_) => Err(()),
    }
}
