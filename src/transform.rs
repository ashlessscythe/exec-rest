use anyhow::{Context, Result};
use encoding_rs::WINDOWS_1252;
use log::{debug, info, warn};
use std::collections::HashSet;
use std::io::Write;
use std::path::Path;
use tempfile::NamedTempFile;

use crate::config::TransformConfig;

pub struct Transformer {
    config: TransformConfig,
}

impl Transformer {
    pub fn new(config: &TransformConfig) -> Result<Self> {
        Ok(Self {
            config: config.clone(),
        })
    }

    pub async fn transform_file(&self, input_path: &Path) -> Result<NamedTempFile> {
        info!("Transforming file: {}", input_path.display());

        // Read file content
        let content = self.read_file_content(input_path).await?;
        debug!("Read {} bytes from file", content.len());

        // Parse lines
        let lines: Vec<&str> = content.lines().collect();
        debug!("File has {} lines", lines.len());

        if lines.len() <= self.config.header_rows_to_skip {
            anyhow::bail!(
                "File has too few lines ({}), cannot skip {} header rows",
                lines.len(),
                self.config.header_rows_to_skip
            );
        }

        // Find data start line
        let data_start = self.find_data_start(&lines)?;
        debug!("Data starts at line {}", data_start + 1);

        // Extract and process data rows
        let mut data_rows = Vec::new();
        let mut seen_rows = HashSet::new();

        for (i, line) in lines.iter().enumerate().skip(data_start) {
            if line.trim().is_empty() {
                continue;
            }

            let processed_line = if self.config.trim_whitespace {
                line.trim()
            } else {
                line
            };

            if processed_line.is_empty() {
                continue;
            }

            // Check for duplicates if deduplication is enabled
            if self.config.dedupe_rows {
                if seen_rows.contains(processed_line) {
                    debug!("Skipping duplicate row at line {}", i + 1);
                    continue;
                }
                seen_rows.insert(processed_line.to_string());
            }

            data_rows.push(processed_line.to_string());
        }

        debug!("Extracted {} data rows", data_rows.len());

        // Create output file
        let mut temp_file = NamedTempFile::new()?;

        // Write header
        let header = if self.config.format == "csv" {
            "Plant,Delivery,Material"
        } else {
            "Plant\tDelivery\tMaterial"
        };

        let line_ending = if self.config.output_line_ending == "crlf" {
            "\r\n"
        } else {
            "\n"
        };

        temp_file.write_all(header.as_bytes())?;
        temp_file.write_all(line_ending.as_bytes())?;

        // Write data rows
        for row in data_rows {
            let processed_row = if self.config.format == "csv" {
                // Convert tabs to commas for CSV
                row.replace('\t', ",")
            } else {
                row
            };
            temp_file.write_all(processed_row.as_bytes())?;
            temp_file.write_all(line_ending.as_bytes())?;
        }

        temp_file.flush()?;
        info!("Transformed file created: {}", temp_file.path().display());

        Ok(temp_file)
    }

    async fn read_file_content(&self, path: &Path) -> Result<String> {
        let bytes = tokio::fs::read(path)
            .await
            .with_context(|| format!("Failed to read file: {}", path.display()))?;

        // Try UTF-8 first
        if let Ok(content) = String::from_utf8(bytes.clone()) {
            return Ok(content);
        }

        // Fallback to Windows-1252
        warn!("File is not valid UTF-8, attempting Windows-1252 conversion");
        let (content, _encoding_used, had_errors) = WINDOWS_1252.decode(&bytes);

        if had_errors {
            warn!("Windows-1252 conversion had errors, proceeding with best-effort result");
        }

        Ok(content.to_string())
    }

    fn find_data_start(&self, lines: &[&str]) -> Result<usize> {
        let header_rows_to_skip = self.config.header_rows_to_skip;

        if lines.len() <= header_rows_to_skip {
            anyhow::bail!(
                "Not enough lines to skip {} header rows",
                header_rows_to_skip
            );
        }

        // Look for the header row that contains our expected header
        for (i, line) in lines.iter().enumerate().skip(header_rows_to_skip) {
            if line
                .to_lowercase()
                .contains(&self.config.header_match.to_lowercase())
            {
                debug!("Found header row at line {}: {}", i + 1, line);
                return Ok(i + 1); // Return the line after the header
            }
        }

        // If we don't find the expected header, just skip the configured number of rows
        warn!(
            "Header row '{}' not found, using configured skip count",
            self.config.header_match
        );
        Ok(header_rows_to_skip)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Write;

    fn create_test_config() -> TransformConfig {
        TransformConfig {
            enabled: true,
            format: "tsv".to_string(),
            header_rows_to_skip: 6,
            header_match: "Plant\tDelivery\tMaterial".to_string(),
            dedupe_rows: false,
            trim_whitespace: true,
            output_line_ending: "lf".to_string(),
        }
    }

    fn create_test_file(content: &str) -> Result<NamedTempFile> {
        let mut file = NamedTempFile::new()?;
        file.write_all(content.as_bytes())?;
        file.flush()?;
        Ok(file)
    }

    #[tokio::test]
    async fn test_transform_basic() {
        let config = create_test_config();
        let transformer = Transformer::new(&config).unwrap();

        let test_content = r#"In-Transfer (Push Delivery) Materials Report
Acme Manufacturing Corp

User                                   TESTUSER
Run Date   :                           2025-01-15
Run Time   :                           14:30:22

        Plant	Delivery	Material
        PLT01	9876543210	55512345
        PLT02	9876543211	55512346"#;

        let input_file = create_test_file(test_content).unwrap();
        let output_file = transformer.transform_file(input_file.path()).await.unwrap();

        let output_content = std::fs::read_to_string(output_file.path()).unwrap();
        let expected =
            "Plant\tDelivery\tMaterial\nPLT01\t9876543210\t55512345\nPLT02\t9876543211\t55512346\n";

        assert_eq!(output_content, expected);
    }

    #[tokio::test]
    async fn test_transform_csv() {
        let mut config = create_test_config();
        config.format = "csv".to_string();
        let transformer = Transformer::new(&config).unwrap();

        let test_content = r#"In-Transfer (Push Delivery) Materials Report
Acme Manufacturing Corp

User                                   TESTUSER
Run Date   :                           2025-01-15
Run Time   :                           14:30:22

        Plant	Delivery	Material
        PLT01	9876543210	55512345"#;

        let input_file = create_test_file(test_content).unwrap();
        let output_file = transformer.transform_file(input_file.path()).await.unwrap();

        let output_content = std::fs::read_to_string(output_file.path()).unwrap();
        let expected = "Plant,Delivery,Material\nPLT01,9876543210,55512345\n";

        assert_eq!(output_content, expected);
    }

    #[tokio::test]
    async fn test_transform_dedupe() {
        let mut config = create_test_config();
        config.dedupe_rows = true;
        let transformer = Transformer::new(&config).unwrap();

        let test_content = r#"In-Transfer (Push Delivery) Materials Report
Acme Manufacturing Corp

User                                   TESTUSER
Run Date   :                           2025-01-15
Run Time   :                           14:30:22

        Plant	Delivery	Material
        PLT01	9876543210	55512345
        PLT01	9876543210	55512345
        PLT02	9876543211	55512346"#;

        let input_file = create_test_file(test_content).unwrap();
        let output_file = transformer.transform_file(input_file.path()).await.unwrap();

        let output_content = std::fs::read_to_string(output_file.path()).unwrap();
        let expected =
            "Plant\tDelivery\tMaterial\nPLT01\t9876543210\t55512345\nPLT02\t9876543211\t55512346\n";

        assert_eq!(output_content, expected);
    }
}
