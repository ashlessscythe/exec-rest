use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose};
use log::{debug, error, info, warn};
use reqwest::{Client, StatusCode};
use serde_json::json;
use std::path::Path;
use tokio::fs;
use tokio::time::{sleep, Duration};

use crate::config::{ApiConfig, RetryConfig};

pub struct Uploader {
    client: Client,
    api_config: ApiConfig,
    retry_config: RetryConfig,
}

impl Uploader {
    pub fn new(api_config: &ApiConfig, retry_config: &RetryConfig) -> Result<Self> {
        let client_builder = Client::builder()
            .timeout(Duration::from_secs(30));

        // Configure authentication
        match api_config.auth.as_str() {
            "bearer" => {
                if api_config.bearer_token.is_empty() {
                    anyhow::bail!("Bearer token is required when auth is 'bearer'");
                }
                // Bearer token will be added in the request
            }
            "basic" => {
                if api_config.basic_username.is_empty() || api_config.basic_password.is_empty() {
                    anyhow::bail!("Username and password are required when auth is 'basic'");
                }
                // Basic auth will be added in the request
            }
            "none" => {
                // No authentication
            }
            _ => {
                anyhow::bail!("Invalid auth type: {}", api_config.auth);
            }
        }

        let client = client_builder.build()
            .context("Failed to create HTTP client")?;

        Ok(Self {
            client,
            api_config: api_config.clone(),
            retry_config: retry_config.clone(),
        })
    }

    pub async fn upload_file(&self, file_path: &Path, original_filename: &str) -> Result<()> {
        let mut attempt = 0;
        let mut backoff_secs = self.retry_config.initial_backoff_secs;

        loop {
            attempt += 1;
            debug!("Upload attempt {} of {}", attempt, self.retry_config.max_attempts);

            match self.try_upload(file_path, original_filename).await {
                Ok(()) => {
                    info!("File uploaded successfully on attempt {}", attempt);
                    return Ok(());
                }
                Err(e) => {
                    error!("Upload attempt {} failed: {}", attempt, e);

                    if attempt >= self.retry_config.max_attempts {
                        anyhow::bail!("Upload failed after {} attempts: {}", self.retry_config.max_attempts, e);
                    }

                    // Determine if this is a retryable error
                    if self.is_retryable_error(&e) {
                        warn!("Retryable error, waiting {} seconds before retry", backoff_secs);
                        sleep(Duration::from_secs(backoff_secs)).await;
                        
                        // Exponential backoff with cap at 30 seconds
                        backoff_secs = (backoff_secs * 2).min(30);
                    } else {
                        anyhow::bail!("Non-retryable error: {}", e);
                    }
                }
            }
        }
    }

    async fn try_upload(&self, file_path: &Path, original_filename: &str) -> Result<()> {
        match self.api_config.mode.as_str() {
            "multipart" => self.upload_multipart(file_path, original_filename).await,
            "json_base64" => self.upload_json_base64(file_path, original_filename).await,
            _ => anyhow::bail!("Invalid upload mode: {}", self.api_config.mode),
        }
    }

    async fn upload_multipart(&self, file_path: &Path, original_filename: &str) -> Result<()> {
        debug!("Uploading file as multipart: {}", file_path.display());

        // Read file content
        let file_content = fs::read(file_path).await
            .context("Failed to read file for multipart upload")?;
        
        let file_part = reqwest::multipart::Part::bytes(file_content)
            .file_name(original_filename.to_string());
        
        let field_name = self.api_config.field_name.clone();
        let mut form = reqwest::multipart::Form::new()
            .part(field_name, file_part);

        // Add extra fields
        for (key, value) in &self.api_config.extra_fields {
            form = form.text(key.clone(), value.clone());
        }

        let mut request = self.client
            .post(&self.api_config.endpoint)
            .multipart(form);

        // Add authentication
        request = self.add_auth(request);

        let response = request.send().await
            .context("Failed to send multipart request")?;

        self.handle_response(response).await
    }

    async fn upload_json_base64(&self, file_path: &Path, original_filename: &str) -> Result<()> {
        debug!("Uploading file as JSON base64: {}", file_path.display());

        // Read file content
        let file_content = fs::read(file_path).await
            .context("Failed to read file for base64 encoding")?;

        // Encode as base64
        let base64_content = general_purpose::STANDARD.encode(&file_content);

        // Create JSON payload
        let mut payload = json!({
            self.api_config.json_filename_key.clone(): original_filename,
            self.api_config.json_data_key.clone(): base64_content
        });

        // Add extra fields to JSON
        for (key, value) in &self.api_config.extra_fields {
            payload[key] = json!(value);
        }

        let mut request = self.client
            .post(&self.api_config.endpoint)
            .json(&payload);

        // Add authentication
        request = self.add_auth(request);

        let response = request.send().await
            .context("Failed to send JSON request")?;

        self.handle_response(response).await
    }

    fn add_auth(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match self.api_config.auth.as_str() {
            "bearer" => {
                request.bearer_auth(&self.api_config.bearer_token)
            }
            "basic" => {
                request.basic_auth(&self.api_config.basic_username, Some(&self.api_config.basic_password))
            }
            _ => request,
        }
    }

    async fn handle_response(&self, response: reqwest::Response) -> Result<()> {
        let status = response.status();
        let response_text = response.text().await
            .unwrap_or_else(|_| "Failed to read response body".to_string());

        debug!("Response status: {}, body: {}", status, response_text);

        match status {
            StatusCode::OK | StatusCode::CREATED | StatusCode::ACCEPTED => {
                info!("Upload successful (status: {})", status);
                Ok(())
            }
            status if status.is_client_error() => {
                anyhow::bail!("Client error ({}): {}", status, response_text);
            }
            status if status.is_server_error() => {
                anyhow::bail!("Server error ({}): {}", status, response_text);
            }
            _ => {
                anyhow::bail!("Unexpected status code: {} - {}", status, response_text);
            }
        }
    }

    fn is_retryable_error(&self, error: &anyhow::Error) -> bool {
        let error_str = error.to_string().to_lowercase();
        
        // Retry on network errors, timeouts, and 5xx server errors
        error_str.contains("timeout") ||
        error_str.contains("connection") ||
        error_str.contains("network") ||
        error_str.contains("server error") ||
        error_str.contains("5")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    
    

    fn create_test_config() -> (ApiConfig, RetryConfig) {
        let api_config = ApiConfig {
            endpoint: "http://localhost:8080/upload".to_string(),
            mode: "multipart".to_string(),
            field_name: "file".to_string(),
            extra_fields: std::collections::HashMap::new(),
            json_filename_key: "filename".to_string(),
            json_data_key: "data".to_string(),
            auth: "none".to_string(),
            bearer_token: String::new(),
            basic_username: String::new(),
            basic_password: String::new(),
        };

        let retry_config = RetryConfig {
            max_attempts: 3,
            initial_backoff_secs: 1,
        };

        (api_config, retry_config)
    }

    #[tokio::test]
    async fn test_uploader_creation() {
        let (api_config, retry_config) = create_test_config();
        let uploader = Uploader::new(&api_config, &retry_config);
        assert!(uploader.is_ok());
    }

    #[tokio::test]
    async fn test_retryable_error_detection() {
        let (api_config, retry_config) = create_test_config();
        let uploader = Uploader::new(&api_config, &retry_config).unwrap();

        // Test retryable errors
        assert!(uploader.is_retryable_error(&anyhow::anyhow!("Connection timeout")));
        assert!(uploader.is_retryable_error(&anyhow::anyhow!("Server error 500")));
        assert!(uploader.is_retryable_error(&anyhow::anyhow!("Network error")));

        // Test non-retryable errors
        assert!(!uploader.is_retryable_error(&anyhow::anyhow!("Client error 400")));
        assert!(!uploader.is_retryable_error(&anyhow::anyhow!("Invalid file format")));
    }
}
