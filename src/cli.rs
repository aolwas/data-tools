use clap::{Parser, Subcommand, ValueEnum};

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum Format {
    Parquet,
    Delta,
}

/// cli parser
#[derive(Parser)]
#[command(name = "datatools")]
#[command(author = "Maxime 'aolwas' Cottret <maxime.cottret@gmail.com>")]
#[command(version = "0.1")]
#[command(about = "Small toy project for data processing while learning Rust", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    View {
        table_path: String,
        #[arg(short, long, value_enum, default_value_t = Format::Delta)]
        format: Format,
        #[arg(short, long, default_value_t = String::from("select * from tbl"))]
        query: String,
        #[arg(short, long, default_value_t = 50)]
        limit: usize,
        #[arg(short, long)]
        partitions: Option<String>,
    },
    Schema {
        table_path: String,
        #[arg(short, long, value_enum, default_value_t = Format::Delta)]
        format: Format,
        #[arg(short, long)]
        partitions: Option<String>,
    },
}
