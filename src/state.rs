use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::file_format::file_type::{FileType, GetExt};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::error::Result;
use datafusion::prelude::*;
use object_store::aws::AmazonS3Builder;
use std::sync::Arc;
use url::{ParseError, Url};

use crate::args::Args;

pub struct AppState {
    ctx: SessionContext,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
        }
    }

    pub async fn register_table(&self, args: &Args) -> Result<()> {
        let url = ensure_scheme(args.table_path.as_ref()).unwrap();

        let file_format = ParquetFormat::default()
            .with_enable_pruning(Some(true))
            .with_skip_metadata(Some(true));

        let listing_common_options = ListingOptions::new(Arc::new(file_format))
            .with_file_extension(FileType::PARQUET.get_ext());

        let listing_options = match get_partitions_spec(args) {
            Some(parts) => listing_common_options.with_table_partition_cols(parts),
            None => listing_common_options,
        };

        if url.scheme() == "s3" || url.scheme() == "s3a" {
            let s3 = AmazonS3Builder::from_env()
                .with_bucket_name(url.host_str().unwrap())
                .build()?;
            let s3_bucket = format!("{}://{}", url.scheme(), url.host_str().unwrap());
            let s3_url = Url::parse(&s3_bucket).unwrap();
            self.ctx
                .runtime_env()
                .register_object_store(&s3_url, Arc::new(s3));
        }

        self.ctx
            .register_listing_table("tbl", url.as_str(), listing_options, None, None)
            .await
    }

    pub async fn exec_query(&self, args: &Args) -> Result<DataFrame> {
        let full_query = format!("{} LIMIT {}", args.query, args.limit);
        println!("full query: {}", full_query);
        self.ctx.sql(full_query.as_str()).await
    }
}

fn get_partitions_spec(args: &Args) -> Option<Vec<(String, DataType)>> {
    if let Some(parts) = args.partitions.as_deref() {
        let mut vec = Vec::new();
        parts
            .split(',')
            .map(|s| s.trim())
            .map(|s| s.split(':').collect())
            .for_each(|t: Vec<&str>| {
                vec.push((t[0].to_string(), crate::utils::type_from_str(t[1]).unwrap()))
            });
        Some(vec)
    } else {
        None
    }
}

fn ensure_scheme(s: &str) -> Result<Url, ()> {
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
