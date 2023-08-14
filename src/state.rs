use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::datasource::file_format::file_type::{FileType, GetExt};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::prelude::*;
use deltalake::{DeltaTable, DeltaTableError};
use object_store::aws::AmazonS3Builder;
use std::sync::Arc;
use url::{ParseError, Url};

use crate::args::{Args, Format};

struct Config {
    path: Url,
    partition_spec: Option<Vec<(String, DataType)>>,
}

impl Config {
    fn from_args(args: &Args) -> Result<Config, ()> {
        ensure_scheme(&args.table_path).map(|url| Config {
            path: url,
            partition_spec: get_partitions_spec(args),
        })
    }
}

pub struct AppState {
    ctx: SessionContext,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
        }
    }

    fn register_store(&self, url: &Url) -> Result<()> {
        match url.scheme() {
            "s3" | "s3a" => {
                let s3 = AmazonS3Builder::from_env()
                    .with_bucket_name(
                        url.host_str()
                            .expect("failed to extract host/bucket from path"),
                    )
                    .build()
                    .expect("Unable to create S3 object store");
                let s3_url =
                    Url::parse(&url[url::Position::BeforeScheme..url::Position::AfterHost])
                        .expect("Unable to get bucket based S3 url");
                let _ = self
                    .ctx
                    .runtime_env()
                    .object_store_registry
                    .register_store(&s3_url, Arc::new(s3));
            }
            _ => (),
        }
        Ok(())
    }

    pub async fn register_table(
        &self,
        args: &Args,
    ) -> Result<Option<Arc<(dyn TableProvider)>>, DataFusionError> {
        let config = Config::from_args(args).unwrap();
        let provider: Arc<dyn TableProvider> = match args.format {
            Format::Parquet => {
                self.register_store(&config.path)?;
                let parquet_table = self.parquet_table_provider(&config).await?;
                Arc::new(parquet_table)
            }
            Format::Delta => {
                let delta_table = self.delta_table_provider(&config).await?;
                Arc::new(delta_table)
            }
        };
        self.ctx.register_table("tbl", provider)
    }

    pub async fn exec_query(&self, args: &Args) -> Result<DataFrame> {
        let full_query = format!("{} LIMIT {}", args.query, args.limit);
        println!("full query: {}", full_query);
        self.ctx.sql(full_query.as_str()).await
    }

    async fn parquet_table_provider(&self, config: &Config) -> Result<ListingTable> {
        let file_format = ParquetFormat::default()
            .with_enable_pruning(Some(true))
            .with_skip_metadata(Some(true));

        let listing_common_options = ListingOptions::new(Arc::new(file_format))
            .with_file_extension(FileType::PARQUET.get_ext());

        let listing_options = match config.partition_spec.clone() {
            Some(parts) => listing_common_options.with_table_partition_cols(parts),
            None => listing_common_options,
        };

        let path = ListingTableUrl::parse(config.path.as_str())?;
        let table_config = ListingTableConfig::new(path)
            .with_listing_options(listing_options)
            .infer_schema(&self.ctx.state())
            .await?;
        let table = ListingTable::try_new(table_config)?;
        Ok(table)
    }

    async fn delta_table_provider(&self, config: &Config) -> Result<DeltaTable, DeltaTableError> {
        deltalake::open_table(config.path.as_str()).await
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
