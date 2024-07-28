# Aolwas Data tools

Toy project around Rust/Arrow/Datafusion/DeltaRS/Axum/Tui

The main idea is to be able to run the tool to query a single local/S3
parquet/delta table in two ways:

- through a cli with optional query result TUI
- through a REST API built with axum

There is no plan to make a prod ready tool, it is just a personal playground.

I clearly took inspiration from those projects:

* <https://github.com/timvw/qv>
* <https://github.com/roapi/roapi>
* <https://github.com/datafusion-contrib/datafusion-tui>
* <https://github.com/andygrove/bdt>
* <https://github.com/spiceai/spiceai> from whom I've borrowed the delta kernel based table provider.
