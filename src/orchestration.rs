use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{info, warn};

/// Client for interacting with the aggregator's orchestration API.
///
/// Responsible for:
/// - Registering this collector instance
/// - Fetching assigned token IDs
/// - Sending periodic heartbeats
/// - Polling for assignment changes
#[derive(Debug, Clone)]
pub struct OrchestrationClient {
    base_url: String,
    collector_id: String,
    http_client: reqwest::Client,
}

/// Request body for registration.
#[derive(Debug, Serialize)]
struct RegisterRequest {
    collector_id: Option<String>,
}

/// Response from registration.
#[derive(Debug, Deserialize)]
struct RegisterResponse {
    collector_id: String,
}

/// Response from assignment endpoint.
#[derive(Debug, Deserialize)]
struct AssignmentResponse {
    token_ids: Vec<String>,
    chunk_size: usize,
}

/// Request body for S3 upload notification.
#[derive(Debug, Serialize)]
pub struct NotifyRequest {
    pub bucket: String,
    pub key: String,
    pub collector_id: String,
}

impl OrchestrationClient {
    /// Create a new orchestration client and register with the aggregator.
    ///
    /// # Arguments
    /// * `base_url` — Aggregator base URL, e.g. `"http://aggregator:8080"`
    /// * `preferred_id` — Optional preferred collector ID. If None, aggregator assigns UUID.
    ///
    /// # Returns
    /// `Ok(OrchestrationClient)` with the assigned collector ID.
    ///
    /// # Example — Input / Output
    /// ```rust,ignore
    /// let client = OrchestrationClient::register(
    ///     "http://127.0.0.1:8080".to_string(),
    ///     None,
    /// ).await.unwrap();
    ///
    /// assert!(!client.collector_id().is_empty());
    /// ```
    pub async fn register(base_url: String, preferred_id: Option<String>) -> Result<Self> {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;

        let register_url = format!("{}/register", base_url);
        let resp = http_client
            .post(&register_url)
            .json(&RegisterRequest {
                collector_id: preferred_id,
            })
            .send()
            .await
            .with_context(|| format!("Failed to register at {}", register_url))?;

        if !resp.status().is_success() {
            anyhow::bail!("Registration failed: {}", resp.status());
        }

        let body: RegisterResponse = resp
            .json()
            .await
            .context("Failed to parse register response")?;

        info!(collector_id = %body.collector_id, "Registered with aggregator");

        Ok(Self {
            base_url,
            collector_id: body.collector_id,
            http_client,
        })
    }

    /// Return the assigned collector ID.
    pub fn collector_id(&self) -> &str {
        &self.collector_id
    }

    /// Fetch the current token assignment from the aggregator.
    ///
    /// # Returns
    /// `(token_ids, chunk_size)` tuple.
    ///
    /// # Example — Input / Output
    /// ```rust,ignore
    /// let (tokens, chunk_size) = client.fetch_assignment().await.unwrap();
    ///
    /// // Output:
    /// // tokens = vec!["400737...", "647039...", ...]
    /// // chunk_size = 100
    /// ```
    pub async fn fetch_assignment(&self) -> Result<(Vec<String>, usize)> {
        let url = format!("{}/assignment/{}", self.base_url, self.collector_id);
        let resp = self
            .http_client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("Failed to fetch assignment from {}", url))?;

        if !resp.status().is_success() {
            anyhow::bail!("Assignment fetch failed: {}", resp.status());
        }

        let body: AssignmentResponse = resp
            .json()
            .await
            .context("Failed to parse assignment response")?;

        Ok((body.token_ids, body.chunk_size))
    }

    /// Send a heartbeat to the aggregator.
    ///
    /// Should be called periodically (e.g., every 10 seconds) to keep the
    /// collector marked as healthy.
    pub async fn send_heartbeat(&self) -> Result<()> {
        let url = format!("{}/heartbeat/{}", self.base_url, self.collector_id);
        let resp = self
            .http_client
            .post(&url)
            .send()
            .await
            .with_context(|| format!("Failed to send heartbeat to {}", url))?;

        if !resp.status().is_success() {
            anyhow::bail!("Heartbeat failed: {}", resp.status());
        }

        Ok(())
    }

    /// Notify the aggregator that a new S3 object is available for merging.
    ///
    /// # Arguments
    /// * `bucket` — S3 bucket name
    /// * `key` — S3 object key
    ///
    /// # Example — Input / Output
    /// ```rust,ignore
    /// client.notify_s3("my-bucket", "orderbook/2024-06-08/12/file.jsonl").await.unwrap();
    /// // Output: aggregator adds the key to pending_files queue
    /// ```
    pub async fn notify_s3(&self, bucket: &str, key: &str) -> Result<()> {
        let url = format!("{}/notify", self.base_url);
        let resp = self
            .http_client
            .post(&url)
            .json(&NotifyRequest {
                bucket: bucket.to_string(),
                key: key.to_string(),
                collector_id: self.collector_id.clone(),
            })
            .send()
            .await
            .with_context(|| format!("Failed to notify aggregator at {}", url))?;

        if !resp.status().is_success() {
            anyhow::bail!("Notify failed: {}", resp.status());
        }

        Ok(())
    }

    /// Spawn a background heartbeat task.
    ///
    /// Sends heartbeat every `interval` seconds until the returned handle is aborted.
    pub fn spawn_heartbeat_task(&self, interval: Duration) -> tokio::task::JoinHandle<()> {
        let client = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            loop {
                tick.tick().await;
                if let Err(e) = client.send_heartbeat().await {
                    warn!(error = %e, "Heartbeat failed");
                }
            }
        })
    }
}
