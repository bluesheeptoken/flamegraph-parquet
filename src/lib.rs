pub mod args;

use parquet::file::metadata::ParquetMetaData;

// https://parquet.apache.org/docs/file-format/metadata/
pub fn parquet_column_size_to_flamegraph_format(
    parquet_metadata: &ParquetMetaData,
    unit: &args::Unit,
) -> Vec<String> {
    let converter_value = match *unit {
        args::Unit::Bytes => 1,
        args::Unit::KiloBytes => 1024,
        args::Unit::MegaBytes => 1024 * 1024,
        args::Unit::GigaBytes => 1024 * 1024 * 1024,
    };

    parquet_metadata
        .row_groups()
        .iter()
        .flat_map(|row_group_metadata| {
            row_group_metadata.columns().iter().map(|column_metadata| {
                format!(
                    "{} {}",
                    column_metadata.column_path().string().replace(".", ";"),
                    column_metadata.compressed_size() / converter_value
                )
            })
        })
        .collect()
}

// Data comes from: https://github.com/apache/parquet-testing/tree/master/data
// It's easier to test with files than creating dummy metadata, cf: https://github.com/apache/arrow-rs/blob/3ee5048c8ea3aa531d111afe33d0a3551eabcd84/parquet/src/file/metadata/reader.rs#L891
#[cfg(test)]
mod tests {
    use std::{fs::File, path::Path};

    use parquet::file::reader::{FileReader, SerializedFileReader};

    use super::*;

    #[test]
    fn test_parquet_column_size_to_flamegraph_format_with_nested_paths() {
        let result = parquet_column_size_to_flamegraph_format(
            metadata_reader("nested_maps.snappy.parquet").metadata(),
            &args::Unit::Bytes,
        );
        let expected: Vec<String> = vec![
            "a;key_value;key 69".to_string(),
            "a;key_value;value;key_value;key 95".to_string(),
            "a;key_value;value;key_value;value 50".to_string(),
            "b 56".to_string(),
            "c 68".to_string(),
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parquet_column_size_to_flamegraph_format_with_multiple_row_groups() {
        let result = parquet_column_size_to_flamegraph_format(
            metadata_reader("sort_columns.parquet").metadata(),
            &args::Unit::Bytes,
        );
        let expected: Vec<String> = vec![
            "a 104".to_string(),
            "b 70".to_string(),
            "a 104".to_string(),
            "b 70".to_string(),
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parquet_column_size_to_flamegraph_format_with_kilobytes() {
        let result = parquet_column_size_to_flamegraph_format(
            metadata_reader("delta_encoding_required_column.parquet").metadata(),
            &args::Unit::KiloBytes,
        );
        let expected: Vec<String> = vec![
            "c_customer_sk: 0".to_string(),
            "c_current_cdemo_sk: 0".to_string(),
            "c_current_hdemo_sk: 0".to_string(),
            "c_current_addr_sk: 0".to_string(),
            "c_first_shipto_date_sk: 0".to_string(),
            "c_first_sales_date_sk: 0".to_string(),
            "c_birth_day: 0".to_string(),
            "c_birth_month: 0".to_string(),
            "c_birth_year: 0".to_string(),
            "c_customer_id: 0".to_string(),
            "c_salutation: 0".to_string(),
            "c_first_name: 0".to_string(),
            "c_last_name: 0".to_string(),
            "c_preferred_cust_flag: 0".to_string(),
            "c_birth_country: 1".to_string(),
            "c_email_address: 2".to_string(),
            "c_last_review_date: 0".to_string(),
        ];
        assert_eq!(result, expected);
    }

    fn metadata_reader(file_name: &str) -> SerializedFileReader<File> {
        let file = File::open(Path::new(&format!("./resources/{}", &file_name))).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();

        reader
    }
}
