use clap::Parser;
use std::sync::Arc;
use std::time::Instant;

mod args;
mod state;
mod utils;

use crate::args::Args;
use crate::state::AppState;

#[tokio::main]
async fn main() {
    let shared_state = Arc::new(AppState::new());

    let args = Args::parse();

    shared_state
        .register_table(&args)
        .await
        .expect("Table registration fails");

    let req_time = Instant::now();
    shared_state
        .exec_query(&args)
        .await
        .expect("Query execution fails")
        .show()
        .await
        .expect("Query show fails");
    let req_time_elapsed = req_time.elapsed();
    println!("Elapsed: {:.2?}", req_time_elapsed);
}
