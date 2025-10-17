use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use toml::Value as TomlValue;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub extraction: ExtractionConfig,
    pub files: FilesConfig,
    pub transform: TransformConfig,
    pub api: ApiConfig,
    pub retry: RetryConfig,
    pub loop_config: LoopConfig,
    pub archive: ArchiveConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractionConfig {
    pub executable: String,
    pub subcommand: String,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilesConfig {
    pub output_dir: String,
    pub file_glob: String,
    pub filename_timestamp_prefix: bool,
    pub stable_size_check_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformConfig {
    pub enabled: bool,
    pub format: String,
    pub header_rows_to_skip: usize,
    pub header_match: String,
    pub dedupe_rows: bool,
    pub trim_whitespace: bool,
    pub output_line_ending: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    pub endpoint: String,
    pub mode: String,
    pub field_name: String,
    pub extra_fields: HashMap<String, String>,
    pub json_filename_key: String,
    pub json_data_key: String,
    pub auth: String,
    pub bearer_token: String,
    pub basic_username: String,
    pub basic_password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub initial_backoff_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoopConfig {
    #[serde(rename = "interval_seconds")]
    pub interval_seconds: u64,
    pub allow_nested: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveConfig {
    pub enabled: bool,
    pub path: String,
    pub append_timestamp: bool,
}

impl Config {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let content = std::fs::read_to_string(&path)
            .with_context(|| format!("Failed to read config file: {}", path_ref.display()))?;
        // Parse to TOML value to normalize legacy/misplaced fields before strict deserialization
        let mut root: TomlValue =
            toml::from_str(&content).with_context(|| "Failed to parse TOML configuration")?;

        // If [loop] exists, map it to loop_config
        if let Some(loop_table) = root.get("loop").cloned() {
            root.as_table_mut()
                .unwrap()
                .insert("loop_config".to_string(), loop_table);
            root.as_table_mut().unwrap().remove("loop");
        }

        // If [extraction].loop_config exists (misplaced), move it to root.loop_config
        if let Some(extraction) = root.get_mut("extraction") {
            if let Some(extraction_table) = extraction.as_table_mut() {
                if let Some(misplaced_loop) = extraction_table.remove("loop_config") {
                    root.as_table_mut()
                        .unwrap()
                        .insert("loop_config".to_string(), misplaced_loop);
                }
            }
        }

        let config: Config = root
            .try_into()
            .with_context(|| "Failed to map configuration to structs")?;

        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        // Validate extraction config
        if self.extraction.executable.is_empty() {
            anyhow::bail!("extraction.executable cannot be empty");
        }
        if self.extraction.subcommand.is_empty() {
            anyhow::bail!("extraction.subcommand cannot be empty");
        }

        // Validate files config
        if self.files.output_dir.is_empty() {
            anyhow::bail!("files.output_dir cannot be empty");
        }
        if self.files.file_glob.is_empty() {
            anyhow::bail!("files.file_glob cannot be empty");
        }

        // Validate transform config
        if !["tsv", "csv"].contains(&self.transform.format.as_str()) {
            anyhow::bail!("transform.format must be 'tsv' or 'csv'");
        }
        if !["crlf", "lf"].contains(&self.transform.output_line_ending.as_str()) {
            anyhow::bail!("transform.output_line_ending must be 'crlf' or 'lf'");
        }

        // Validate API config
        if self.api.endpoint.is_empty() {
            anyhow::bail!("api.endpoint cannot be empty");
        }
        if !["multipart", "json_base64"].contains(&self.api.mode.as_str()) {
            anyhow::bail!("api.mode must be 'multipart' or 'json_base64'");
        }
        if !["none", "bearer", "basic"].contains(&self.api.auth.as_str()) {
            anyhow::bail!("api.auth must be 'none', 'bearer', or 'basic'");
        }

        // Validate retry config
        if self.retry.max_attempts == 0 {
            anyhow::bail!("retry.max_attempts must be greater than 0");
        }

        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            extraction: ExtractionConfig {
                executable: "C:\\tools\\sap_auto.exe".to_string(),
                subcommand: "run-sequence".to_string(),
                args: vec![
                    "--plant".to_string(),
                    "149".to_string(),
                    "--cols".to_string(),
                    "plant,material,delivery".to_string(),
                ],
                env: HashMap::new(),
            },
            files: FilesConfig {
                output_dir: "C:\\sap\\outputs".to_string(),
                file_glob: "*_y_149-ALL.txt".to_string(),
                filename_timestamp_prefix: true,
                stable_size_check_secs: 2,
            },
            transform: TransformConfig {
                enabled: false,
                format: "tsv".to_string(),
                header_rows_to_skip: 6,
                header_match: "Plant\tDelivery\tMaterial".to_string(),
                dedupe_rows: false,
                trim_whitespace: true,
                output_line_ending: "crlf".to_string(),
            },
            api: ApiConfig {
                endpoint: "https://intranet.local/upload.php".to_string(),
                mode: "multipart".to_string(),
                field_name: "file".to_string(),
                extra_fields: HashMap::new(),
                json_filename_key: "filename".to_string(),
                json_data_key: "data".to_string(),
                auth: "none".to_string(),
                bearer_token: String::new(),
                basic_username: String::new(),
                basic_password: String::new(),
            },
            retry: RetryConfig {
                max_attempts: 3,
                initial_backoff_secs: 3,
            },
            loop_config: LoopConfig {
                interval_seconds: 300,
                allow_nested: false,
            },
            archive: ArchiveConfig {
                enabled: false,
                path: "C:\\sap\\archive".to_string(),
                append_timestamp: true,
            },
        }
    }
}
