use arrow::util::pretty::pretty_format_batches;
use clap::Parser;
use std::sync::Arc;
use std::time::Instant;

mod app;
mod cli;
mod tui;
mod utils;

use crate::app::App;
use crate::cli::{Cli, Commands};

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Commands::View {
            table_path,
            format,
            query,
            partitions,
            limit,
            no_tui,
        } => {
            let shared_state = Arc::new(App::new(table_path.as_str(), partitions, format.clone()));
            let req_time = Instant::now();
            shared_state
                .register_table()
                .await
                .expect("Table registration fails");
            let req_time_elapsed = req_time.elapsed();
            println!("Table registration time: {:.2?}", req_time_elapsed);
            let req_time = Instant::now();
            let records = shared_state
                .exec_query(query.clone(), limit.clone())
                .await
                .expect("Query execution fails")
                .collect()
                .await
                .expect("Records collect fails");
            let req_time_elapsed = req_time.elapsed();
            println!("Query execution time: {:.2?}", req_time_elapsed);
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
            let shared_state = Arc::new(App::new(table_path.as_str(), partitions, format.clone()));
            let req_time = Instant::now();
            shared_state
                .register_table()
                .await
                .expect("Table registration fails");
            let req_time_elapsed = req_time.elapsed();
            println!("Table registration time: {:.2?}", req_time_elapsed);
            let req_time = Instant::now();
            let records = shared_state
                .schema()
                .await
                .expect("Schema query fails")
                .collect()
                .await
                .expect("Schema collect fails");
            let req_time_elapsed = req_time.elapsed();
            println!("Query execution time: {:.2?}", req_time_elapsed);
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
