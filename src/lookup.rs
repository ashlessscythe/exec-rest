use anyhow::{Context, Result};
use log::{debug, info, warn};
use reqwest::{header, Client};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use tokio::time::Duration;

use crate::config::LookupConfig;

#[derive(Serialize, Clone)]
pub struct EnrichedRow {
    pub plant: String,
    pub delivery: String,
    #[serde(rename = "part_no")]
    pub part_no: String,
    pub duns: String,
    pub cof: String,
    pub country: String,
    pub shipment: String,
}

#[derive(Deserialize)]
struct LookupResponse {
    duns: String,
    cof: String,
    country: String,
}

pub struct LookupEnricher {
    client: Client,
    config: LookupConfig,
}

impl LookupEnricher {
    pub fn new(config: &LookupConfig) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .build()
            .context("Failed to create HTTP client for lookup")?;

        Ok(Self {
            client,
            config: config.clone(),
        })
    }

    pub async fn enrich_tsv_file(&self, tsv_path: &Path) -> Result<Vec<EnrichedRow>> {
        info!(
            "Starting lookup enrichment for file: {}",
            tsv_path.display()
        );

        // Parse TSV file into base rows
        let base_rows = self.parse_tsv_file(tsv_path).await?;
        if base_rows.is_empty() {
            warn!("No rows found in TSV file");
            return Ok(base_rows);
        }

        info!("Parsed {} rows from TSV file", base_rows.len());
        
        // Log sample of parsed rows for debugging
        for (i, row) in base_rows.iter().take(5).enumerate() {
            info!("Sample row {}: Plant='{}', Delivery='{}', Part='{}'", 
                  i + 1, row.plant, row.delivery, row.part_no);
        }
        if base_rows.len() > 5 {
            info!("... and {} more rows", base_rows.len() - 5);
        }

        // Extract unique part numbers
        let part_numbers = self.dedupe_part_numbers(&base_rows);
        info!(
            "Found {} unique part numbers for lookup",
            part_numbers.len()
        );
        
        // Log sample part numbers for debugging
        if !part_numbers.is_empty() {
            info!("Sample part numbers: {}", part_numbers.iter().take(10).cloned().collect::<Vec<_>>().join(", "));
            if part_numbers.len() > 10 {
                info!("... and {} more part numbers", part_numbers.len() - 10);
            }
        } else {
            warn!("No part numbers found! Checking for empty part numbers in rows...");
            let empty_parts = base_rows.iter().filter(|row| row.part_no.trim().is_empty()).count();
            let non_empty_parts = base_rows.iter().filter(|row| !row.part_no.trim().is_empty()).count();
            info!("Rows with empty part numbers: {}", empty_parts);
            info!("Rows with non-empty part numbers: {}", non_empty_parts);
        }

        if part_numbers.is_empty() {
            warn!("No part numbers found for lookup");
            // Return base rows with empty lookup fields - they'll still be posted
            return Ok(base_rows);
        }

        // Perform chunked lookups
        let lookup_data = self.lookup_chunks(&part_numbers).await?;
        info!("Retrieved lookup data for {} parts", lookup_data.len());

        // Merge lookup data into rows (even if lookup_data is empty)
        let enriched_rows = self.merge_lookup_data(base_rows, &lookup_data);
        info!("Enriched {} rows with lookup data", enriched_rows.len());
        
        if lookup_data.is_empty() {
            info!("No lookup data was found - rows will be posted with original data only (empty DUNS, COF, Country fields)");
        }
        
        // Log sample of final enriched rows
        if !enriched_rows.is_empty() {
            info!("Sample final enriched rows:");
            for (i, row) in enriched_rows.iter().take(5).enumerate() {
                let lookup_status = if row.duns.is_empty() { "No lookup data" } else { "With lookup data" };
                info!("  {}: Plant='{}', Delivery='{}', Part='{}', DUNS='{}', COF='{}', Country='{}' [{}]", 
                      i + 1, row.plant, row.delivery, row.part_no, row.duns, row.cof, row.country, lookup_status);
            }
            if enriched_rows.len() > 5 {
                info!("  ... and {} more enriched rows", enriched_rows.len() - 5);
            }
        }

        Ok(enriched_rows)
    }

    async fn parse_tsv_file(&self, path: &Path) -> Result<Vec<EnrichedRow>> {
        let content = tokio::fs::read_to_string(path)
            .await
            .with_context(|| format!("Failed to read TSV file: {}", path.display()))?;

        info!("TSV file content length: {} characters", content.len());
        debug!("First 500 characters of TSV file:\n{}", 
               content.chars().take(500).collect::<String>());

        let mut rows = Vec::new();
        let mut seen_header = false;
        let mut line_count = 0;
        let mut header_found = false;

        info!("Starting to parse TSV file with {} lines", content.lines().count());

        for line in content.lines() {
            line_count += 1;
            let line = line.trim_end_matches(['\r', '\n']);
            let trimmed_line = line.trim();
            if trimmed_line.is_empty() {
                continue;
            }

            // Look for header row
            if !seen_header {
                let lc = trimmed_line.to_ascii_lowercase();
                debug!("Line {}: Checking for header: '{}'", line_count, trimmed_line);
                if lc.contains("plant") && lc.contains("delivery") && lc.contains("material") {
                    seen_header = true;
                    header_found = true;
                    info!("Found header row at line {}: '{}'", line_count, trimmed_line);
                    continue;
                }
                debug!("Line {}: Not a header, skipping", line_count);
                continue;
            }

            // Parse data row - handle mixed tab/space separators
            // The format appears to be: Plant\tDelivery\t\tMaterial or Plant\tDelivery\t\t\tMaterial
            // We'll split by tab first, then handle the material column which might have spaces
            debug!("Line {}: Raw line: '{}'", line_count, trimmed_line);
            let cols: Vec<&str> = trimmed_line.split('\t').collect();
            debug!("Line {}: Split into {} columns: {:?}", line_count, cols.len(), cols);
            
            if cols.len() < 3 {
                debug!("Skipping line with insufficient columns ({}): '{}'", cols.len(), trimmed_line);
                continue;
            }

            let plant = cols[0].trim().to_string();
            let delivery = cols[1].trim().to_string();
            
            // Find the material column - it should be the last non-empty column
            let mut part_no = String::new();
            for i in (2..cols.len()).rev() {
                let col = cols[i].trim();
                if !col.is_empty() {
                    // This might contain spaces, so split by whitespace and take the first part
                    let material_parts: Vec<&str> = col.split_whitespace().collect();
                    if !material_parts.is_empty() {
                        part_no = material_parts[0].to_string();
                        break;
                    }
                }
            }

            debug!("Parsed row - Plant: '{}', Delivery: '{}', Part: '{}'", plant, delivery, part_no);

            // Skip empty rows
            if plant.is_empty() && delivery.is_empty() && part_no.is_empty() {
                continue;
            }

            rows.push(EnrichedRow {
                plant,
                delivery,
                part_no,
                duns: String::new(),
                cof: String::new(),
                country: String::new(),
                shipment: String::new(),
            });
        }

        info!("TSV parsing complete: {} total lines processed, header found: {}, {} data rows parsed", 
              line_count, header_found, rows.len());

        Ok(rows)
    }

    fn dedupe_part_numbers(&self, rows: &[EnrichedRow]) -> Vec<String> {
        let mut seen = HashSet::new();
        let mut parts = Vec::new();
        let mut empty_count = 0;
        let mut duplicate_count = 0;

        for row in rows {
            if row.part_no.trim().is_empty() {
                empty_count += 1;
                debug!("Skipping row with empty part number: Plant='{}', Delivery='{}'", row.plant, row.delivery);
            } else if seen.insert(row.part_no.clone()) {
                parts.push(row.part_no.clone());
                debug!("Added unique part number: '{}'", row.part_no);
            } else {
                duplicate_count += 1;
                debug!("Skipping duplicate part number: '{}'", row.part_no);
            }
        }

        info!("Part number deduplication: {} unique, {} empty, {} duplicates", 
              parts.len(), empty_count, duplicate_count);
        
        parts
    }

    async fn lookup_chunks(
        &self,
        part_numbers: &[String],
    ) -> Result<HashMap<String, LookupResponse>> {
        let mut all_lookup_data = HashMap::new();

        for chunk in part_numbers.chunks(self.config.chunk_size) {
            let chunk_data = self.lookup_single_chunk(chunk).await?;
            all_lookup_data.extend(chunk_data);
        }

        Ok(all_lookup_data)
    }

    async fn lookup_single_chunk(
        &self,
        part_numbers: &[String],
    ) -> Result<HashMap<String, LookupResponse>> {
        let joined_parts = part_numbers.join(",");
        let encoded_parts = urlencoding::encode(&joined_parts);
        let url = format!("{}{}", self.config.url, encoded_parts);

        info!("Looking up chunk: {} parts", part_numbers.len());
        debug!("Lookup URL: {}", url);

        let mut request = self.client.get(&url);

        // Add cookie if configured
        if !self.config.cookie.is_empty() {
            request = request.header(header::COOKIE, &self.config.cookie);
        }

        let response = request
            .send()
            .await
            .with_context(|| format!("Failed to send lookup request to: {}", url))?;

        if !response.status().is_success() {
            anyhow::bail!(
                "Lookup request failed with status {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            );
        }

        // Get response text first for debugging
        let response_text = response.text().await
            .with_context(|| "Failed to read response body")?;
        
        info!("Lookup response length: {} characters", response_text.len());
        debug!("Lookup response content (first 1000 chars): {}", 
               response_text.chars().take(1000).collect::<String>());
        
        // Try to parse as JSON - handle both array and object responses
        let lookup_map: HashMap<String, LookupResponse> = match serde_json::from_str::<HashMap<String, LookupResponse>>(&response_text) {
            Ok(map) => map,
            Err(_) => {
                // Try parsing as array of objects
                info!("Response is not a JSON object, trying to parse as array...");
                let array_response: Vec<serde_json::Value> = serde_json::from_str(&response_text)
                    .with_context(|| {
                        format!("Failed to parse lookup response as JSON array or object. First 500 chars: {}", 
                                response_text.chars().take(500).collect::<String>())
                    })?;
                
                info!("Successfully parsed as JSON array with {} items", array_response.len());
                
                // Convert array to HashMap - assuming each item has a "part" or "part_no" field as key
                let mut map = HashMap::new();
                for item in &array_response {
                    if let (Some(part_key), Some(duns)) = (
                        item.get("part").or_else(|| item.get("part_no")).or_else(|| item.get("material")),
                        item.get("duns").and_then(|d| d.as_str())
                    ) {
                        if let Some(part_no) = part_key.as_str() {
                            let lookup_response = LookupResponse {
                                duns: duns.to_string(),
                                cof: item.get("cof").and_then(|c| c.as_str()).unwrap_or("").to_string(),
                                country: item.get("country").and_then(|c| c.as_str()).unwrap_or("").to_string(),
                            };
                            map.insert(part_no.to_string(), lookup_response);
                        }
                    }
                }
                
                if map.is_empty() {
                    if array_response.is_empty() {
                        info!("Lookup API returned empty array - no lookup data found for any parts. Proceeding with original data only.");
                    } else {
                        warn!("Could not extract part numbers from array response. Array structure: {}", 
                              serde_json::to_string_pretty(&array_response).unwrap_or_default());
                        info!("Proceeding with original data only (no lookup enrichment).");
                    }
                }
                
                map
            }
        };

        info!("Received lookup data for {} parts", lookup_map.len());
        
        // Log sample of enriched data response
        if !lookup_map.is_empty() {
            info!("Sample enriched data from GET request:");
            for (i, (part_no, lookup_data)) in lookup_map.iter().take(5).enumerate() {
                info!("  {}: Part='{}', DUNS='{}', COF='{}', Country='{}'", 
                      i + 1, part_no, lookup_data.duns, lookup_data.cof, lookup_data.country);
            }
            if lookup_map.len() > 5 {
                info!("  ... and {} more enriched records", lookup_map.len() - 5);
            }
        } else {
            warn!("No enriched data received from GET request");
        }
        
        Ok(lookup_map)
    }

    fn merge_lookup_data(
        &self,
        mut rows: Vec<EnrichedRow>,
        lookup_data: &HashMap<String, LookupResponse>,
    ) -> Vec<EnrichedRow> {
        for row in &mut rows {
            if let Some(lookup) = lookup_data.get(&row.part_no) {
                row.duns = lookup.duns.clone();
                row.cof = lookup.cof.clone();
                row.country = lookup.country.clone();
            }
        }

        rows
    }

    pub async fn post_enriched_data(&self, rows: &[EnrichedRow]) -> Result<()> {
        let json_data =
            serde_json::to_string(rows).context("Failed to serialize enriched rows to JSON")?;

        let form_data = vec![("tableData", json_data.as_str()), ("save", "")];

        debug!(
            "Posting {} enriched rows to: {}",
            rows.len(),
            self.config.post_url
        );

        let mut request = self.client.post(&self.config.post_url).form(&form_data);

        // Add cookie if configured
        if !self.config.cookie.is_empty() {
            request = request.header(header::COOKIE, &self.config.cookie);
        }

        let response = request.send().await.with_context(|| {
            format!("Failed to send enriched data to: {}", self.config.post_url)
        })?;

        if !response.status().is_success() {
            anyhow::bail!(
                "Post request failed with status {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            );
        }

        info!("Successfully posted {} enriched rows", rows.len());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_test_config() -> LookupConfig {
        LookupConfig {
            enabled: true,
            url: "http://localhost:8080/lookup?part=".to_string(),
            chunk_size: 2,
            cookie: String::new(),
            timeout_secs: 30,
            post_url: "http://localhost:8080/post".to_string(),
        }
    }

    #[test]
    fn test_dedupe_part_numbers() {
        let config = create_test_config();
        let enricher = LookupEnricher::new(&config).unwrap();

        let rows = vec![
            EnrichedRow {
                plant: "TEST01".to_string(),
                delivery: "DEL001".to_string(),
                part_no: "TEST001".to_string(),
                duns: String::new(),
                cof: String::new(),
                country: String::new(),
                shipment: String::new(),
            },
            EnrichedRow {
                plant: "TEST02".to_string(),
                delivery: "DEL002".to_string(),
                part_no: "TEST001".to_string(), // duplicate
                duns: String::new(),
                cof: String::new(),
                country: String::new(),
                shipment: String::new(),
            },
            EnrichedRow {
                plant: "TEST03".to_string(),
                delivery: "DEL003".to_string(),
                part_no: "TEST002".to_string(),
                duns: String::new(),
                cof: String::new(),
                country: String::new(),
                shipment: String::new(),
            },
        ];

        let parts = enricher.dedupe_part_numbers(&rows);
        assert_eq!(parts.len(), 2);
        assert!(parts.contains(&"TEST001".to_string()));
        assert!(parts.contains(&"TEST002".to_string()));
    }

    #[test]
    fn test_parse_tsv_with_mixed_separators() {
        use tokio::fs::write;
        use tempfile::tempdir;

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            // Create a temporary TSV file with the actual format from the user's data
            let temp_dir = tempdir().unwrap();
            let test_file = temp_dir.path().join("test.tsv");
            
            // Test data with randomized values
            let tsv_content = "Plant Delivery                Material\n\tTEST01\t1234567890\t\t987654321\n\tTEST01\t1234567890\t\t456789123\n\tTEST01\t1234567890\t\t789123456\n";
            write(&test_file, tsv_content).await.unwrap();
            
            let config = create_test_config();
            let enricher = LookupEnricher::new(&config).unwrap();
            
            let rows = enricher.parse_tsv_file(&test_file).await.unwrap();
            
            // Should parse 3 rows correctly despite mixed separators
            assert_eq!(rows.len(), 3);
            
            // First row should have correct values
            assert_eq!(rows[0].plant, "TEST01");
            assert_eq!(rows[0].delivery, "1234567890");
            assert_eq!(rows[0].part_no, "987654321");
            
            // Second row should have correct values
            assert_eq!(rows[1].plant, "TEST01");
            assert_eq!(rows[1].delivery, "1234567890");
            assert_eq!(rows[1].part_no, "456789123");
            
            // Third row should have correct values
            assert_eq!(rows[2].plant, "TEST01");
            assert_eq!(rows[2].delivery, "1234567890");
            assert_eq!(rows[2].part_no, "789123456");
        });
    }

    #[test]
    fn test_parse_tsv_with_leading_tabs() {
        use tokio::fs::write;
        use tempfile::tempdir;

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            // Create a temporary TSV file with leading tabs
            let temp_dir = tempdir().unwrap();
            let test_file = temp_dir.path().join("test.tsv");
            
            let tsv_content = "Plant\tDelivery\tMaterial\n\tTEST01\t1234567890\t987654321\n\tTEST01\t1234567890\t456789123\n";
            write(&test_file, tsv_content).await.unwrap();
            
            let config = create_test_config();
            let enricher = LookupEnricher::new(&config).unwrap();
            
            let rows = enricher.parse_tsv_file(&test_file).await.unwrap();
            
            // Should parse 2 rows correctly despite leading tabs
            assert_eq!(rows.len(), 2);
            
            // First row should have correct values
            assert_eq!(rows[0].plant, "TEST01");
            assert_eq!(rows[0].delivery, "1234567890");
            assert_eq!(rows[0].part_no, "987654321");
            
            // Second row should have correct values
            assert_eq!(rows[1].plant, "TEST01");
            assert_eq!(rows[1].delivery, "1234567890");
            assert_eq!(rows[1].part_no, "456789123");
        });
    }

    #[test]
    fn test_merge_lookup_data() {
        let config = create_test_config();
        let enricher = LookupEnricher::new(&config).unwrap();

        let rows = vec![
            EnrichedRow {
                plant: "TEST01".to_string(),
                delivery: "DEL001".to_string(),
                part_no: "TEST001".to_string(),
                duns: String::new(),
                cof: String::new(),
                country: String::new(),
                shipment: String::new(),
            },
            EnrichedRow {
                plant: "TEST02".to_string(),
                delivery: "DEL002".to_string(),
                part_no: "TEST002".to_string(),
                duns: String::new(),
                cof: String::new(),
                country: String::new(),
                shipment: String::new(),
            },
        ];

        let mut lookup_data = HashMap::new();
        lookup_data.insert(
            "TEST001".to_string(),
            LookupResponse {
                duns: "987654321".to_string(),
                cof: "TEST".to_string(),
                country: "Test Country".to_string(),
            },
        );

        let enriched = enricher.merge_lookup_data(rows, &lookup_data);

        assert_eq!(enriched[0].duns, "987654321");
        assert_eq!(enriched[0].cof, "TEST");
        assert_eq!(enriched[0].country, "Test Country");
        assert_eq!(enriched[1].duns, ""); // No lookup data for TEST002
    }
}
