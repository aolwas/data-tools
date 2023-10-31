use std::sync::Arc;
use std::time::Instant;

use arrow::util::pretty::pretty_format_batches;
use clap::Parser;
use log::info;
use simple_logger::SimpleLogger;

mod cli;
mod table;
mod tui;
mod utils;

use crate::cli::{Cli, Commands};
use crate::table::TableContext;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let logger = SimpleLogger::new();

    match cli.get_log_level() {
        Some(level) => logger.with_level(level).init().unwrap(),
        None => {}
    }

    match &cli.command {
        Commands::View {
            table_path,
            format,
            query,
            partitions,
            limit,
            no_tui,
        } => {
            let tblctx = Arc::new(TableContext::new(
                table_path.as_str(),
                partitions,
                format.clone(),
            ));
            let req_time = Instant::now();
            tblctx
                .register_table()
                .await
                .expect("Table registration fails");
            let req_time_elapsed = req_time.elapsed();
            info!("Table registration time: {:.2?}", req_time_elapsed);
            let req_time = Instant::now();
            let records = tblctx
                .exec_query(query.clone(), limit.clone())
                .await
                .expect("Query execution fails")
                .collect()
                .await
                .expect("Records collect fails");
            let req_time_elapsed = req_time.elapsed();
            info!("Query execution time: {:.2?}", req_time_elapsed);
            if *no_tui {
                println!(
                    "{}",
                    pretty_format_batches(&records).expect("Pretty format fails")
                );
            } else {
                let _ = tui::show_in_tui(
                    pretty_format_batches(&records)
                        .unwrap()
                        .to_string()
                        .as_str(),
                );
            }
        }
        Commands::Schema {
            table_path,
            partitions,
            format,
            no_tui,
        } => {
            let tblctx = Arc::new(TableContext::new(
                table_path.as_str(),
                partitions,
                format.clone(),
            ));
            let req_time = Instant::now();
            tblctx
                .register_table()
                .await
                .expect("Table registration fails");
            let req_time_elapsed = req_time.elapsed();
            info!("Table registration time: {:.2?}", req_time_elapsed);
            let req_time = Instant::now();
            let records = tblctx
                .schema()
                .await
                .expect("Schema query fails")
                .collect()
                .await
                .expect("Schema collect fails");
            let req_time_elapsed = req_time.elapsed();
            info!("Query execution time: {:.2?}", req_time_elapsed);
            if *no_tui {
                println!(
                    "{}",
                    pretty_format_batches(&records).expect("Pretty format fails")
                );
            } else {
                let _ = tui::show_in_tui(
                    pretty_format_batches(&records)
                        .unwrap()
                        .to_string()
                        .as_str(),
                );
            }
        }
    }
}
