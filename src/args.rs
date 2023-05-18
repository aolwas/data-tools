use clap::{Parser, ValueEnum};

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum Format {
    Auto,
    Parquet,
    Delta,
}

/// cli parser
#[derive(Parser)]
#[command(name = "data-tools")]
#[command(author = "Maxime 'aolwas' Cottret <maxime.cottret@gmail.com>")]
#[command(version = "0.1")]
#[command(about = "Small toy project for data with rust/dafafusion/deltars/axum/tui", long_about = None)]
pub struct Args {
    /// table path
    pub table_path: String,

    /// table format
    #[arg(value_enum, default_value_t = Format::Auto)]
    pub format: Format,

    #[arg(short, long, default_value_t = String::from("select * from tbl"))]
    pub query: String,

    #[arg(short, long, default_value_t = 50)]
    pub limit: usize,

    #[arg(short, long)]
    pub partitions: Option<String>,
}
