use anyhow::Result;
use clap::Parser;
use dialoguer::{theme::ColorfulTheme, Select};
use log::{error, info, warn};
use std::path::PathBuf;
use std::process::Stdio;
use tokio::process::Command;
use tokio::time::{sleep, Duration};

mod config;
mod file_utils;
mod lookup;
mod transform;
mod upload;

use config::Config;
use file_utils::FileWatcher;
use lookup::LookupEnricher;
use transform::Transformer;
use upload::Uploader;

#[derive(Parser)]
#[command(name = "sap_auto_runner")]
#[command(about = "Windows-only Rust CLI for running SAP auto extractor and uploading results")]
struct Cli {
    /// Path to configuration file
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,

    /// Override API endpoint
    #[arg(long)]
    endpoint: Option<String>,

    /// Override upload mode (multipart or json_base64)
    #[arg(long, value_parser = ["multipart", "json_base64"])]
    mode: Option<String>,

    /// Override output directory
    #[arg(long)]
    output_dir: Option<PathBuf>,

    /// Override file glob pattern
    #[arg(long)]
    file_glob: Option<String>,

    /// Override loop interval in seconds
    #[arg(long)]
    loop_interval: Option<u64>,

    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging
    let log_level = if cli.verbose { "debug" } else { "info" };
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(log_level)).init();

    info!("Starting SAP Auto Runner");

    // Load configuration
    let mut config = Config::load(&cli.config)?;

    // Show landing menu if no CLI overrides are provided
    let no_overrides = cli.endpoint.is_none()
        && cli.mode.is_none()
        && cli.output_dir.is_none()
        && cli.file_glob.is_none()
        && cli.loop_interval.is_none();

    let mut menu_selection = None;
    if no_overrides {
        let items = vec![
            "Run once (no loop)",
            "Run loop (use configured interval)",
            "Enrich latest file only (no extraction)",
            "Open config in Notepad",
            "Exit",
        ];
        let selection = Select::with_theme(&ColorfulTheme::default())
            .with_prompt("What would you like to do?")
            .items(&items)
            .default(0)
            .interact()
            .unwrap_or(3);

        match selection {
            0 => {
                // Force single run
                config.loop_config.interval_seconds = 0;
            }
            1 => {
                // Keep configured loop interval (ensure >0)
                if config.loop_config.interval_seconds == 0 {
                    config.loop_config.interval_seconds = 300;
                }
            }
            2 => {
                // Enrich latest file only (no extraction) - handle after component creation
                menu_selection = Some(2);
            }
            3 => {
                // Open config in Notepad then exit
                let _ = std::process::Command::new("notepad")
                    .arg(&cli.config)
                    .status();
                return Ok(());
            }
            _ => return Ok(()),
        }
    }

    // Apply CLI overrides
    if let Some(endpoint) = cli.endpoint {
        config.api.endpoint = endpoint;
    }
    if let Some(mode) = cli.mode {
        config.api.mode = mode;
    }
    if let Some(output_dir) = cli.output_dir {
        config.files.output_dir = output_dir.to_string_lossy().to_string();
    }
    if let Some(file_glob) = cli.file_glob {
        config.files.file_glob = file_glob;
    }
    if let Some(loop_interval) = cli.loop_interval {
        config.loop_config.interval_seconds = loop_interval;
    }

    // Validate configuration
    config.validate()?;

    // Check for nested loop conflict
    if config.extraction.subcommand == "run-loop"
        && config.loop_config.interval_seconds > 0
        && !config.loop_config.allow_nested
    {
        anyhow::bail!("Error: subcommand is 'run-loop' and loop interval > 0, but allow_nested is false. This would create nested loops.");
    }

    // Create components
    let file_watcher = FileWatcher::new(&config.files)?.with_archive(&config.archive);
    let transformer = Transformer::new(&config.transform)?;
    let uploader = Uploader::new(&config.api, &config.retry)?;
    let lookup_enricher = if config.lookup.enabled {
        Some(LookupEnricher::new(&config.lookup)?)
    } else {
        None
    };

    // Handle special menu selections
    if let Some(selection) = menu_selection {
        match selection {
            2 => {
                // Enrich latest file only (no extraction)
                return enrich_latest_file_only(&config, &file_watcher, lookup_enricher.as_ref())
                    .await;
            }
            _ => {}
        }
    }

    // Main execution loop
    if config.loop_config.interval_seconds == 0 {
        // Run once
        run_once(
            &config,
            &file_watcher,
            &transformer,
            &uploader,
            lookup_enricher.as_ref(),
        )
        .await?;
    } else {
        // Run in loop
        loop {
            if let Err(e) = run_once(
                &config,
                &file_watcher,
                &transformer,
                &uploader,
                lookup_enricher.as_ref(),
            )
            .await
            {
                error!("Error in run cycle: {}", e);
            }

            info!(
                "Waiting {} seconds before next run",
                config.loop_config.interval_seconds
            );
            sleep(Duration::from_secs(config.loop_config.interval_seconds)).await;
        }
    }

    Ok(())
}

async fn run_once(
    config: &Config,
    file_watcher: &FileWatcher,
    transformer: &Transformer,
    uploader: &Uploader,
    lookup_enricher: Option<&LookupEnricher>,
) -> Result<()> {
    // Spawn SAP auto process
    info!(
        "Spawning SAP auto process: {} {}",
        config.extraction.executable, config.extraction.subcommand
    );

    let exe_path = std::path::Path::new(&config.extraction.executable);
    let exe_dir = exe_path.parent().unwrap_or(std::path::Path::new("."));

    let mut child = Command::new(&config.extraction.executable)
        .arg(&config.extraction.subcommand)
        .args(&config.extraction.args)
        .envs(&config.extraction.env)
        .current_dir(exe_dir)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()?;

    let exit_status = child.wait().await?;

    if !exit_status.success() {
        warn!(
            "SAP auto process exited with non-zero status: {:?}",
            exit_status.code()
        );
    } else {
        info!("SAP auto process completed successfully");
    }

    // Wait a moment for files to be written
    sleep(Duration::from_millis(500)).await;

    // Find newest file
    let newest_file = match file_watcher.find_newest_file().await? {
        Some(file) => {
            info!("Found newest file: {}", file.display());
            file
        }
        None => {
            warn!("No matching files found in output directory");
            return Ok(());
        }
    };

    // Wait for file to be stable
    file_watcher.wait_for_stable_file(&newest_file).await?;
    info!("File is stable: {}", newest_file.display());

    // Handle lookup enrichment or regular upload
    if config.lookup.enabled && config.api.mode == "lookup_enrich" {
        // Use lookup enrichment flow
        if let Some(enricher) = lookup_enricher {
            info!("Using lookup enrichment flow");
            let enriched_rows = enricher.enrich_tsv_file(&newest_file).await?;
            enricher.post_enriched_data(&enriched_rows).await?;
            info!("Lookup enrichment and upload completed successfully");
        } else {
            anyhow::bail!("Lookup enrichment is enabled but enricher is not available");
        }
    } else {
        // Use regular transform + upload flow
        let (upload_file, is_transformed) = if config.transform.enabled {
            info!("Transforming file before upload");
            let temp_file = transformer.transform_file(&newest_file).await?;
            (temp_file.path().to_path_buf(), true)
        } else {
            (newest_file.clone(), false)
        };

        // Upload file
        info!("Uploading file: {}", upload_file.display());
        uploader
            .upload_file(
                &upload_file,
                &newest_file.file_name().unwrap().to_string_lossy(),
            )
            .await?;
        info!("File uploaded successfully");

        // Clean up transformed file if it was created
        if is_transformed {
            if let Err(e) = tokio::fs::remove_file(&upload_file).await {
                warn!(
                    "Failed to clean up transformed file {}: {}",
                    upload_file.display(),
                    e
                );
            }
        }
    }

    // Archive file if enabled
    if config.archive.enabled {
        info!("Archiving file");
        file_watcher.archive_file(&newest_file).await?;
        info!("File archived");
    }

    Ok(())
}

async fn enrich_latest_file_only(
    config: &Config,
    file_watcher: &FileWatcher,
    lookup_enricher: Option<&LookupEnricher>,
) -> Result<()> {
    info!("Enriching latest file only (no extraction)");

    // Check if output directory exists
    let output_dir = std::path::Path::new(&config.files.output_dir);
    if !output_dir.exists() {
        anyhow::bail!(
            "Output directory does not exist: {}\nPlease check your configuration or run the extraction first.",
            output_dir.display()
        );
    }

    if !output_dir.is_dir() {
        anyhow::bail!(
            "Output path is not a directory: {}\nPlease check your configuration.",
            output_dir.display()
        );
    }

    // Find newest file
    let newest_file = match file_watcher.find_newest_file().await? {
        Some(file) => {
            info!("Found newest file: {}", file.display());
            file
        }
        None => {
            anyhow::bail!(
                "No matching files found in output directory: {}\nPattern: {}\n\nThis could mean:\n- No files match the pattern '{}'\n- The extraction hasn't been run yet\n- Files are in a different location\n\nTry running the full extraction first, or check your file_glob pattern.",
                config.files.output_dir,
                config.files.file_glob,
                config.files.file_glob
            );
        }
    };

    // Verify the file still exists and is readable
    if !newest_file.exists() {
        anyhow::bail!(
            "File no longer exists: {}\nThe file may have been moved or deleted.",
            newest_file.display()
        );
    }

    if !newest_file.is_file() {
        anyhow::bail!(
            "Path is not a file: {}\nExpected a file but found something else.",
            newest_file.display()
        );
    }

    // Wait for file to be stable
    file_watcher.wait_for_stable_file(&newest_file).await?;
    info!("File is stable: {}", newest_file.display());

    // Check if lookup enrichment is enabled
    if !config.lookup.enabled {
        anyhow::bail!(
            "Lookup enrichment is not enabled in configuration.\nPlease set 'lookup.enabled = true' in your config file."
        );
    }

    if config.api.mode != "lookup_enrich" {
        anyhow::bail!(
            "API mode must be 'lookup_enrich' for this operation.\nCurrent mode: '{}'\nPlease set 'api.mode = \"lookup_enrich\"' in your config file.",
            config.api.mode
        );
    }

    if let Some(enricher) = lookup_enricher {
        info!(
            "Starting lookup enrichment for file: {}",
            newest_file.display()
        );
        let enriched_rows = enricher.enrich_tsv_file(&newest_file).await?;
        enricher.post_enriched_data(&enriched_rows).await?;
        info!("Lookup enrichment and upload completed successfully");
    } else {
        anyhow::bail!(
            "Lookup enrichment is enabled but enricher is not available.\nThis is an internal error - please check your configuration."
        );
    }

    // Archive file if enabled
    if config.archive.enabled {
        info!("Archiving file");
        file_watcher.archive_file(&newest_file).await?;
        info!("File archived");
    }

    Ok(())
}
