# Parquet flamegraph

`parquet-flamegraph` is a small tool for generating flamegraphs to investigate parquet file storage.

It helps to identify which columns take up the most storage in a parquet file.

Example (click to zoom): [![Flamegraph example](./documentation/nested_maps.svg)](https://raw.githubusercontent.com/bluesheeptoken/parquet-flamegraph/refs/heads/main/documentation/nested_maps.svg)


Hovering over the fields will show the exact number of bytes used by each column. (You will need to open the file in a browser.)

# Usage

```
> parquet-flamegraph --help
A simple program to generate flamegraph and investigate parquet storage

Usage: parquet-flamegraph [OPTIONS] --input-path <INPUT_PATH>

Options:
  -i, --input-path <INPUT_PATH>    Input path to a single parquet file
  -o, --output-path <OUTPUT_PATH>  Output path file, flamegraph will be saved in. [default: tmp file]
  -u, --unit <UNIT>                Unit to display data. This will truncate columns if their compressed size is less than 1 unit [default: b] [possible values: b, kb, mb, gb]
  -h, --help                       Print help
  -V, --version                    Print version
```

# Installation

Currently, the only installation method available is through the Rust toolchain. You can install it with the following command:

`cargo install parquet-flamegraph`

