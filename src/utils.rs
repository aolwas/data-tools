use datafusion::arrow::datatypes::{DataType, TimeUnit};

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
