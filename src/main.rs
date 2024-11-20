use clap::Parser;
use inferno::flamegraph::{self, Options};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet_flamegraph::args::Args;
use std::fs;
use std::io::BufWriter;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let output_file = args.output_file()?;
    let mut writer = BufWriter::new(output_file);

    let mut options = Options::default();
    options.hash = true;
    options.title = format!(
        "Flamegraph parquet {}",
        args.input_file_name().unwrap_or_default()
    );
    options.count_name = args.unit.to_string();

    for path in args.input_paths()? {
        let file = fs::File::open(path)?;
        let reader = SerializedFileReader::new(file)?;

        flamegraph::from_lines(
            &mut options,
            parquet_flamegraph::parquet_column_size_to_flamegraph_format(
                reader.metadata(),
                &args.unit,
            )
            .iter()
            .map(String::as_str),
            &mut writer,
        )?;
    }

    println!("Data written successfully!");

    Ok(())
}
