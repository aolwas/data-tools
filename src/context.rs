use anyhow::Result;
use datafusion::execution::context::{SessionContext, SessionState};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use datafusion::prelude::*;
// use deltalake::delta_datafusion::DeltaTableFactory;
use object_store;
use object_store::aws::AmazonS3Builder;
use std::sync::Arc;
use url::Url;

use crate::deltalake::DeltaTableFactory;
use crate::utils::ensure_scheme;

pub struct SQLContext {
    ctx: SessionContext,
}

impl SQLContext {
    pub fn new() -> Self {
        let cfg = RuntimeConfig::new();
        let env = RuntimeEnv::new(cfg).unwrap();
        let ses = SessionConfig::new().with_information_schema(true);
        let mut state = SessionState::new_with_config_rt(ses, Arc::new(env));
        state
            .table_factories_mut()
            .insert("DELTA".to_string(), Arc::new(DeltaTableFactory {}));
        Self {
            ctx: SessionContext::new_with_state(state),
        }
    }

    async fn register_object_store(&self, location: &String, file_type: &String) -> Result<()> {
        let url = ensure_scheme(location).unwrap();
        match (url.scheme(), file_type.as_str()) {
            ("s3", ft) => {
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
                // match ft {
                //     "DELTA" => deltalake::aws::register_handlers(None),
                //     _ => (),
                // }
            }
            _ => (),
        }
        Ok(())
    }

    pub async fn execute_logical_plan(&self, plan: LogicalPlan) -> Result<DataFrame> {
        if let LogicalPlan::Ddl(DdlStatement::CreateExternalTable(cmd)) = &plan {
            println!("file type: {:?}", cmd.file_type);
            self.register_object_store(&cmd.location, &cmd.file_type)
                .await?;
        }
        let df = self.ctx.execute_logical_plan(plan).await?;
        Ok(df)
    }

    pub async fn sql(&self, sql: &str) -> Result<DataFrame> {
        self.sql_with_options(sql, SQLOptions::new()).await
    }

    pub async fn sql_with_options(&self, sql: &str, options: SQLOptions) -> Result<DataFrame> {
        let plan = self.ctx.state().create_logical_plan(sql).await?;
        options.verify_plan(&plan)?;
        self.execute_logical_plan(plan).await
    }
}
