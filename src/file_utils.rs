use anyhow::{Context, Result};
use chrono::Utc;
use glob::glob;
use log::{debug, info, warn};
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use tokio::fs;
use tokio::time::{sleep, Duration};

use crate::config::{ArchiveConfig, FilesConfig};

pub struct FileWatcher {
    config: FilesConfig,
    archive_config: ArchiveConfig,
}

impl FileWatcher {
    pub fn new(files_config: &FilesConfig) -> Result<Self> {
        Ok(Self {
            config: files_config.clone(),
            archive_config: ArchiveConfig {
                enabled: false,
                path: String::new(),
                append_timestamp: false,
            },
        })
    }

    pub fn with_archive(mut self, archive_config: &ArchiveConfig) -> Self {
        self.archive_config = archive_config.clone();
        self
    }

    pub async fn find_newest_file(&self) -> Result<Option<PathBuf>> {
        let pattern = format!("{}/{}", self.config.output_dir, self.config.file_glob);
        debug!("Searching for files matching pattern: {}", pattern);

        let mut candidates = Vec::new();

        for entry in glob(&pattern).context("Failed to read glob pattern")? {
            match entry {
                Ok(path) => {
                    if path.is_file() {
                        debug!("Found candidate file: {}", path.display());
                        candidates.push(path);
                    }
                }
                Err(e) => {
                    warn!("Error reading directory entry: {}", e);
                }
            }
        }

        if candidates.is_empty() {
            return Ok(None);
        }

        // Sort by modification time, with timestamp prefix as tiebreaker
        candidates.sort_by(|a, b| {
            let a_time = self.get_file_time(a).unwrap_or(SystemTime::UNIX_EPOCH);
            let b_time = self.get_file_time(b).unwrap_or(SystemTime::UNIX_EPOCH);
            b_time.cmp(&a_time) // Reverse order (newest first)
        });

        let newest = candidates.into_iter().next();
        if let Some(ref path) = newest {
            info!("Selected newest file: {} (mtime: {:?})", 
                  path.display(), 
                  self.get_file_time(path).unwrap_or(SystemTime::UNIX_EPOCH));
        }

        Ok(newest)
    }

    fn get_file_time(&self, path: &Path) -> Result<SystemTime> {
        let metadata = std::fs::metadata(path)?;
        let mtime = metadata.modified()?;
        
        // If timestamp prefix is enabled, try to parse timestamp from filename
        if self.config.filename_timestamp_prefix {
            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                if let Some(timestamp) = self.parse_timestamp_from_filename(filename) {
                    debug!("Parsed timestamp from filename {}: {:?}", filename, timestamp);
                    return Ok(timestamp);
                }
            }
        }
        
        Ok(mtime)
    }

    fn parse_timestamp_from_filename(&self, filename: &str) -> Option<SystemTime> {
        // Look for pattern YYYYMMDDhhmmss at the beginning
        if filename.len() < 14 {
            return None;
        }

        let timestamp_str = &filename[..14];
        if timestamp_str.chars().all(|c| c.is_ascii_digit()) {
            // Parse YYYYMMDDhhmmss
            let year: i32 = timestamp_str[0..4].parse().ok()?;
            let month: u32 = timestamp_str[4..6].parse().ok()?;
            let day: u32 = timestamp_str[6..8].parse().ok()?;
            let hour: u32 = timestamp_str[8..10].parse().ok()?;
            let minute: u32 = timestamp_str[10..12].parse().ok()?;
            let second: u32 = timestamp_str[12..14].parse().ok()?;

            if let Some(date) = chrono::NaiveDate::from_ymd_opt(year, month, day) {
                if let Some(datetime) = date.and_hms_opt(hour, minute, second) {
                    return Some(SystemTime::from(datetime.and_utc()));
                }
            }
        }

        None
    }

    pub async fn wait_for_stable_file(&self, file_path: &Path) -> Result<()> {
        let mut last_size = 0;
        let mut stable_count = 0;
        let required_stable_checks = (self.config.stable_size_check_secs * 2).max(1); // Check every 0.5 seconds
        let max_wait_secs = 10;
        let mut total_wait_secs = 0;

        loop {
            match fs::metadata(file_path).await {
                Ok(metadata) => {
                    let current_size = metadata.len();
                    debug!("File size check: {} bytes (was {} bytes)", current_size, last_size);
                    
                    if current_size == last_size {
                        stable_count += 1;
                        if stable_count >= required_stable_checks {
                            debug!("File is stable after {} checks", stable_count);
                            return Ok(());
                        }
                    } else {
                        stable_count = 0;
                        last_size = current_size;
                    }
                }
                Err(e) => {
                    warn!("Error checking file size: {}", e);
                }
            }

            sleep(Duration::from_millis(500)).await;
            total_wait_secs += 1;
            
            if total_wait_secs >= max_wait_secs * 2 { // 0.5 second intervals
                warn!("File did not stabilize within {} seconds, proceeding anyway", max_wait_secs);
                return Ok(());
            }
        }
    }

    pub async fn archive_file(&self, file_path: &Path) -> Result<()> {
        if !self.archive_config.enabled {
            return Ok(());
        }

        let filename = file_path.file_name()
            .context("File has no filename")?
            .to_string_lossy();

        let mut archive_filename = filename.to_string();
        
        if self.archive_config.append_timestamp {
            let now = Utc::now();
            let timestamp = now.format("%Y%m%d_%H%M%S");
            let stem = file_path.file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("file");
            let extension = file_path.extension()
                .and_then(|s| s.to_str())
                .map(|s| format!(".{}", s))
                .unwrap_or_default();
            
            archive_filename = format!("{}_{}{}", stem, timestamp, extension);
        }

        let archive_path = Path::new(&self.archive_config.path).join(&archive_filename);
        
        // Create archive directory if it doesn't exist
        if let Some(parent) = archive_path.parent() {
            fs::create_dir_all(parent).await
                .with_context(|| format!("Failed to create archive directory: {}", parent.display()))?;
        }

        // Move file to archive
        fs::rename(file_path, &archive_path).await
            .with_context(|| format!("Failed to move file from {} to {}", file_path.display(), archive_path.display()))?;

        info!("File archived to: {}", archive_path.display());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_find_newest_file() {
        let temp_dir = tempdir().unwrap();
        let files_config = FilesConfig {
            output_dir: temp_dir.path().to_string_lossy().to_string(),
            file_glob: "*.txt".to_string(),
            filename_timestamp_prefix: false,
            stable_size_check_secs: 1,
        };

        let watcher = FileWatcher::new(&files_config).unwrap();

        // Create test files
        let file1 = temp_dir.path().join("old_file.txt");
        let file2 = temp_dir.path().join("new_file.txt");
        
        File::create(&file1).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(100));
        File::create(&file2).unwrap();

        let newest = watcher.find_newest_file().await.unwrap();
        assert!(newest.is_some());
        assert_eq!(newest.unwrap().file_name().unwrap(), "new_file.txt");
    }

    #[tokio::test]
    async fn test_timestamp_parsing() {
        let temp_dir = tempdir().unwrap();
        let files_config = FilesConfig {
            output_dir: temp_dir.path().to_string_lossy().to_string(),
            file_glob: "*.txt".to_string(),
            filename_timestamp_prefix: true,
            stable_size_check_secs: 1,
        };

        let watcher = FileWatcher::new(&files_config).unwrap();

        // Create test files with timestamps
        let file1 = temp_dir.path().join("20251016170601_y_149-ALL.txt");
        let file2 = temp_dir.path().join("20251016170602_y_149-ALL.txt");
        
        File::create(&file1).unwrap();
        File::create(&file2).unwrap();

        let newest = watcher.find_newest_file().await.unwrap();
        assert!(newest.is_some());
        assert_eq!(newest.unwrap().file_name().unwrap(), "20251016170602_y_149-ALL.txt");
    }
}
