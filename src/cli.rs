use clap::{Parser, Subcommand, ValueEnum};
use log;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum Format {
    Parquet,
    Delta,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum LogLevel {
    Off,
    Info,
    Debug,
}

/// cli parser
#[derive(Parser)]
#[command(name = "datatools")]
#[command(author = "Maxime 'aolwas' Cottret <maxime.cottret@gmail.com>")]
#[command(version = "0.1")]
#[command(about = "Small toy project for data processing while learning Rust", long_about = None)]
pub struct Cli {
    #[arg(short, long, value_enum, default_value_t = LogLevel::Info)]
    log_level: LogLevel,
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
        #[arg(long, default_value_t = false)]
        no_tui: bool,
    },
    Schema {
        table_path: String,
        #[arg(short, long, value_enum, default_value_t = Format::Delta)]
        format: Format,
        #[arg(short, long)]
        partitions: Option<String>,
        #[arg(long, default_value_t = false)]
        no_tui: bool,
    },
}

impl Cli {
    pub fn get_log_level(&self) -> Option<log::LevelFilter> {
        match self.log_level {
            LogLevel::Off => None,
            LogLevel::Info => Some(log::LevelFilter::Info),
            LogLevel::Debug => Some(log::LevelFilter::Debug),
        }
    }
}
