use clap::Parser;
use std::env;
use std::fmt;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
/// Program to generate a flamegraph based on compressed memory used for parquet files
pub struct Args {
    /// Input path to a single parquet file
    #[arg(short, long)]
    input_path: String,

    /// Output path file, flamegraph will be saved in. [default: tmp file]
    #[arg(short, long)]
    output_path: Option<String>,

    /// Unit to display data. This will truncate columns if their compressed size is less than 1 unit.
    #[arg(short, long, default_value_t, value_enum)]
    pub unit: Unit,
}

#[derive(clap::ValueEnum, Clone, Default, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Unit {
    #[default]
    #[clap(name = "b")]
    Bytes,
    #[clap(name = "kb")]
    KiloBytes,
    #[clap(name = "mb")]
    MegaBytes,
    #[clap(name = "gb")]
    GigaBytes,
}

impl fmt::Display for Unit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Unit::Bytes => "Bytes",
            Unit::KiloBytes => "KB",
            Unit::MegaBytes => "MB",
            Unit::GigaBytes => "GB",
        };
        write!(f, "{}", name)
    }
}

impl Args {
    pub fn input_file_name(&self) -> Option<String> {
        let path = Path::new(&self.input_path);
        path.file_name()
            .and_then(|os_str| os_str.to_str())
            .map(|s| s.to_string())
    }

    pub fn input_paths(&self) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
        let path = Path::new(&self.input_path);

        if path.is_dir() {
            let mut paths = Vec::new();
            for entry in fs::read_dir(path)? {
                let path = entry?.path();
                if path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
                    paths.push(path)
                }
            }
            if paths.is_empty() {
                return Err(
                    format!("No parquet file found under {} directory", path.display()).into(),
                );
            }
            Ok(paths)
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
            Ok(vec![path.to_path_buf()])
        } else {
            Err("The file is not a .parquet file".into())
        }
    }

    pub fn output_file(&self) -> Result<fs::File, Box<dyn std::error::Error>> {
        let path = match &self.output_path {
            Some(path) => path.clone(),
            None => {
                let temp_dir = env::temp_dir();
                let unique_file_name = format!("flamegraph_parquet-{}.svg", Uuid::new_v4());

                // Join temp directory with the unique file name and ensure it's valid UTF-8
                let file_path = temp_dir.join(unique_file_name);

                file_path.to_str().ok_or("Cannot convert tmp file path to UTF-8, please retry by providing an output path")?.to_string()
            }
        };
        println!("Flamegraph will be written in {}", path);

        fs::File::create(path).map_err(|e| e.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn test_required_argument() {
        let args = Args::parse_from(&["program_name", "--input-path", "example.parquet"]);
        assert_eq!(args.input_path, "example.parquet");
        assert!(args.output_path.is_none());
    }

    #[test]
    fn test_with_optional_output_path() {
        let args = Args::parse_from(&[
            "program_name",
            "--input-path",
            "example.parquet",
            "--output-path",
            "output.svg",
        ]);
        assert_eq!(args.input_path, "example.parquet");
        assert_eq!(args.output_path, Some("output.svg".to_string()));
    }

    #[test]
    fn test_short_flags() {
        let args = Args::parse_from(&["program_name", "-i", "example.parquet", "-o", "output.svg"]);
        assert_eq!(args.input_path, "example.parquet");
        assert_eq!(args.output_path, Some("output.svg".to_string()));
    }

    #[test]
    fn test_missing_required_argument() {
        let result = Args::try_parse_from(&["program_name"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_help_flag() {
        let result = Args::try_parse_from(&["program_name", "--help"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_list_directory() {
        let args = Args::parse_from(&[
            "program_name",
            "--input-path",
            "./resources/several_files_parquet",
        ]);

        let mut result = args.input_paths().unwrap();
        result.sort();

        let mut expected = vec![
            PathBuf::from("./resources/several_files_parquet/nested_maps.snappy_002.parquet"),
            PathBuf::from("./resources/several_files_parquet/nested_maps.snappy_001.parquet"),
        ];
        expected.sort();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_list_empty_directory() {
        let args = Args::parse_from(&[
            "program_name",
            "--input-path",
            "./resources/directory_no_parquet",
        ]);
        assert!(args.input_paths().is_err());
    }

    #[test]
    fn test_provided_file_is_not_a_parquet() {
        let args = Args::parse_from(&[
            "program_name",
            "--input-path",
            "./resources/not_a_parquet.txt",
        ]);
        assert!(args.input_paths().is_err());
    }
}
