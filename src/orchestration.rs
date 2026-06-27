use crate::http_client::{HttpClient, ReqwestHttpClient};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

/// Client for interacting with the aggregator's orchestration API.
///
/// Responsible for:
/// - Registering this collector instance
/// - Fetching assigned token IDs
/// - Sending periodic heartbeats
/// - Polling for assignment changes
#[derive(Clone)]
pub struct OrchestrationClient {
    base_url: String,
    collector_id: String,
    http_client: Arc<dyn HttpClient>,
}

impl std::fmt::Debug for OrchestrationClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OrchestrationClient")
            .field("base_url", &self.base_url)
            .field("collector_id", &self.collector_id)
            .finish_non_exhaustive()
    }
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
    #[serde(default)]
    version: u64,
}

/// Versioned token assignment returned by the aggregator.
///
/// # Detailed Description
/// The aggregator includes a monotonically increasing `version` so collectors
/// can skip subscription diff work when their assignment has not changed. The
/// token list remains the source of truth for correctness; the version is only
/// a cheap change detector.
///
/// # Arguments
/// Values are returned by [`OrchestrationClient::fetch_assignment_snapshot`].
///
/// # Returns
/// A cloneable assignment containing token IDs, recommended chunk size, and
/// assignment version.
///
/// # Example — Input / Output
/// ```rust,ignore
/// let assignment = client.fetch_assignment_snapshot().await?;
/// assert_eq!(assignment.version, 2);
/// assert_eq!(assignment.token_ids, vec!["token-a".to_string()]);
/// # anyhow::Ok(())
/// ```
///
/// # Related
/// - [`OrchestrationClient::fetch_assignment_snapshot`]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignmentSnapshot {
    /// Token IDs assigned to this collector.
    pub token_ids: Vec<String>,
    /// Recommended number of token IDs per WebSocket worker.
    pub chunk_size: usize,
    /// Aggregator assignment version for cheap change detection.
    pub version: u64,
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
    pub async fn register(base_url: String, preferred_id: Option<String>) -> Result<Self> {
        let http_client = Arc::new(ReqwestHttpClient::new());
        Self::register_with_client(http_client, base_url, preferred_id).await
    }

    /// Register with a caller-supplied HTTP client.
    ///
    /// This constructor enables dependency injection and is the primary way
    /// tests wire in a mock [`HttpClient`].
    pub async fn register_with_client(
        http_client: Arc<dyn HttpClient>,
        base_url: String,
        preferred_id: Option<String>,
    ) -> Result<Self> {
        let register_url = format!("{}/register", base_url);
        let body = serde_json::to_string(&RegisterRequest {
            collector_id: preferred_id,
        })?;
        let response = http_client
            .post(&register_url, body)
            .await
            .with_context(|| format!("Failed to register at {}", register_url))?;

        if !response.is_success() {
            anyhow::bail!("Registration failed: {}", response.status);
        }

        let body: RegisterResponse = response
            .json()
            .context("Failed to parse register response")?;

        info!(collector_id = %body.collector_id, "Registered with aggregator");

        Ok(Self {
            base_url,
            collector_id: body.collector_id,
            http_client,
        })
    }

    /// Build a client from an existing collector ID and HTTP client.
    ///
    /// Useful in tests that need a fully-constructed client without first
    /// exercising the registration endpoint.
    pub fn with_client(
        base_url: String,
        collector_id: String,
        http_client: Arc<dyn HttpClient>,
    ) -> Self {
        Self {
            base_url,
            collector_id,
            http_client,
        }
    }

    /// Return the assigned collector ID.
    pub fn collector_id(&self) -> &str {
        &self.collector_id
    }

    /// Fetch the current versioned token assignment from the aggregator.
    ///
    /// # Detailed Description
    /// This method calls `/assignment/{collector_id}` and parses the token IDs,
    /// chunk size, and assignment `version`. Older aggregators that omit
    /// `version` deserialize as version `0`, allowing backward-compatible tests
    /// while newer collectors use the version to avoid unnecessary subscription
    /// updates.
    ///
    /// # Arguments
    /// This method has no arguments; it uses the client base URL and registered
    /// collector ID.
    ///
    /// # Returns
    /// A [`AssignmentSnapshot`] on success, or an error when the request,
    /// response status, or JSON body is invalid.
    ///
    /// # Example — Input / Output
    /// ```rust,ignore
    /// let assignment = client.fetch_assignment_snapshot().await?;
    /// assert_eq!(assignment.chunk_size, 100);
    /// assert!(assignment.version >= 1);
    /// # anyhow::Ok(())
    /// ```
    ///
    /// # Related
    /// - [`AssignmentSnapshot`]
    pub async fn fetch_assignment_snapshot(&self) -> Result<AssignmentSnapshot> {
        let url = format!("{}/assignment/{}", self.base_url, self.collector_id);
        let response = self
            .http_client
            .get(&url)
            .await
            .with_context(|| format!("Failed to fetch assignment from {}", url))?;

        if !response.is_success() {
            anyhow::bail!("Assignment fetch failed: {}", response.status);
        }

        let body: AssignmentResponse = response
            .json()
            .context("Failed to parse assignment response")?;

        Ok(AssignmentSnapshot {
            token_ids: body.token_ids,
            chunk_size: body.chunk_size,
            version: body.version,
        })
    }

    /// Fetch the current token assignment from the aggregator.
    ///
    /// # Detailed Description
    /// This compatibility wrapper preserves the older `(token_ids, chunk_size)`
    /// return shape for callers that do not yet need assignment-version polling.
    /// New dynamic collectors should prefer [`fetch_assignment_snapshot`](Self::fetch_assignment_snapshot)
    /// so they can distinguish unchanged empty responses from versioned empty
    /// assignments that require unsubscribe diffs.
    ///
    /// # Arguments
    /// This method has no arguments; it uses the client base URL and collector ID
    /// captured during registration.
    ///
    /// # Returns
    /// A tuple of assigned token IDs and recommended chunk size, or an error when
    /// the request fails, the aggregator returns a non-success status, or the JSON
    /// response cannot be parsed.
    ///
    /// # Example — Input / Output
    /// ```rust,ignore
    /// let (token_ids, chunk_size) = client.fetch_assignment().await?;
    /// assert_eq!(chunk_size, 100);
    /// assert_eq!(token_ids, vec!["token-a".to_string()]);
    /// # anyhow::Ok(())
    /// ```
    ///
    /// # Related
    /// - [`OrchestrationClient::fetch_assignment_snapshot`]
    /// - [`AssignmentSnapshot`]
    pub async fn fetch_assignment(&self) -> Result<(Vec<String>, usize)> {
        let assignment = self.fetch_assignment_snapshot().await?;
        Ok((assignment.token_ids, assignment.chunk_size))
    }

    /// Send a heartbeat to the aggregator.
    pub async fn send_heartbeat(&self) -> Result<()> {
        let url = format!("{}/heartbeat/{}", self.base_url, self.collector_id);
        let response = self
            .http_client
            .post(&url, String::new())
            .await
            .with_context(|| format!("Failed to send heartbeat to {}", url))?;

        if !response.is_success() {
            anyhow::bail!("Heartbeat failed: {}", response.status);
        }

        Ok(())
    }

    /// Notify the aggregator that a new S3 object is available for merging.
    pub async fn notify_s3(&self, bucket: &str, key: &str) -> Result<()> {
        let url = format!("{}/notify", self.base_url);
        let body = serde_json::to_string(&NotifyRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            collector_id: self.collector_id.clone(),
        })?;
        let response = self
            .http_client
            .post(&url, body)
            .await
            .with_context(|| format!("Failed to notify aggregator at {}", url))?;

        if !response.is_success() {
            anyhow::bail!("Notify failed: {}", response.status);
        }

        Ok(())
    }

    /// Spawn a background heartbeat task.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_client::{HttpResponse, InMemoryHttpClient};

    fn ok_json(body: serde_json::Value) -> HttpResponse {
        HttpResponse {
            status: 200,
            body: serde_json::to_string(&body).unwrap(),
        }
    }

    fn error_response(status: u16, body: &str) -> HttpResponse {
        HttpResponse {
            status,
            body: body.into(),
        }
    }

    #[tokio::test]
    async fn test_register_success() {
        let client = InMemoryHttpClient::new();
        let base = "http://aggregator";
        client.set_response(
            &format!("{}/register", base),
            Ok(ok_json(serde_json::json!({ "collector_id": "cid-123" }))),
        );

        let orch = OrchestrationClient::register_with_client(
            Arc::new(client),
            base.to_string(),
            Some("preferred".to_string()),
        )
        .await
        .unwrap();

        assert_eq!(orch.collector_id(), "cid-123");
    }

    #[tokio::test]
    async fn test_register_http_error() {
        let client = InMemoryHttpClient::new();
        let base = "http://aggregator";
        client.set_response(
            &format!("{}/register", base),
            Err("connection refused".to_string()),
        );

        let err =
            OrchestrationClient::register_with_client(Arc::new(client), base.to_string(), None)
                .await
                .unwrap_err();

        assert!(err.to_string().contains("Failed to register"));
    }

    #[tokio::test]
    async fn test_register_non_success_status() {
        let client = InMemoryHttpClient::new();
        let base = "http://aggregator";
        client.set_response(
            &format!("{}/register", base),
            Ok(error_response(409, "conflict")),
        );

        let err =
            OrchestrationClient::register_with_client(Arc::new(client), base.to_string(), None)
                .await
                .unwrap_err();

        assert!(err.to_string().contains("Registration failed"));
    }

    #[tokio::test]
    async fn test_register_parse_error() {
        let client = InMemoryHttpClient::new();
        let base = "http://aggregator";
        client.set_response(
            &format!("{}/register", base),
            Ok(HttpResponse {
                status: 200,
                body: "not-json".into(),
            }),
        );

        let err =
            OrchestrationClient::register_with_client(Arc::new(client), base.to_string(), None)
                .await
                .unwrap_err();

        assert!(err
            .to_string()
            .contains("Failed to parse register response"));
    }

    #[tokio::test]
    async fn test_fetch_assignment_success() {
        let client = InMemoryHttpClient::new();
        let base = "http://aggregator";
        client.set_response(
            &format!("{}/assignment/cid", base),
            Ok(ok_json(serde_json::json!({
                "token_ids": ["a", "b"],
                "chunk_size": 50,
                "version": 7,
            }))),
        );

        let orch =
            OrchestrationClient::with_client(base.to_string(), "cid".to_string(), Arc::new(client));

        let (tokens, chunk_size) = orch.fetch_assignment().await.unwrap();
        assert_eq!(tokens, vec!["a", "b"]);
        assert_eq!(chunk_size, 50);
    }

    #[tokio::test]
    async fn test_fetch_assignment_snapshot_success_with_version() {
        let client = InMemoryHttpClient::new();
        let base = "http://aggregator";
        client.set_response(
            &format!("{}/assignment/cid", base),
            Ok(ok_json(serde_json::json!({
                "token_ids": ["a", "b"],
                "chunk_size": 50,
                "version": 7,
            }))),
        );

        let orch =
            OrchestrationClient::with_client(base.to_string(), "cid".to_string(), Arc::new(client));

        let assignment = orch.fetch_assignment_snapshot().await.unwrap();
        assert_eq!(assignment.token_ids, vec!["a", "b"]);
        assert_eq!(assignment.chunk_size, 50);
        assert_eq!(assignment.version, 7);
    }

    #[tokio::test]
    async fn test_fetch_assignment_non_success_status() {
        let client = InMemoryHttpClient::new();
        let base = "http://aggregator";
        client.set_response(
            &format!("{}/assignment/cid", base),
            Ok(error_response(500, "down")),
        );

        let orch =
            OrchestrationClient::with_client(base.to_string(), "cid".to_string(), Arc::new(client));

        let err = orch.fetch_assignment().await.unwrap_err();
        assert!(err.to_string().contains("Assignment fetch failed"));
    }

    #[tokio::test]
    async fn test_fetch_assignment_http_error() {
        let client = InMemoryHttpClient::new();
        let base = "http://aggregator";
        client.set_response(
            &format!("{}/assignment/cid", base),
            Err("network unreachable".to_string()),
        );

        let orch =
            OrchestrationClient::with_client(base.to_string(), "cid".to_string(), Arc::new(client));

        let err = orch.fetch_assignment().await.unwrap_err();
        assert!(err.to_string().contains("Failed to fetch assignment"));
    }

    #[tokio::test]
    async fn test_send_heartbeat_success() {
        let client = InMemoryHttpClient::new();
        let base = "http://aggregator";
        client.set_response(
            &format!("{}/heartbeat/cid", base),
            Ok(ok_json(serde_json::json!({}))),
        );

        let orch =
            OrchestrationClient::with_client(base.to_string(), "cid".to_string(), Arc::new(client));

        orch.send_heartbeat().await.unwrap();
    }

    #[tokio::test]
    async fn test_send_heartbeat_non_success_status() {
        let client = InMemoryHttpClient::new();
        let base = "http://aggregator";
        client.set_response(
            &format!("{}/heartbeat/cid", base),
            Ok(error_response(503, "unavailable")),
        );

        let orch =
            OrchestrationClient::with_client(base.to_string(), "cid".to_string(), Arc::new(client));

        let err = orch.send_heartbeat().await.unwrap_err();
        assert!(err.to_string().contains("Heartbeat failed"));
    }

    #[tokio::test]
    async fn test_send_heartbeat_http_error() {
        let client = InMemoryHttpClient::new();
        let base = "http://aggregator";
        client.set_response(
            &format!("{}/heartbeat/cid", base),
            Err("connection reset".to_string()),
        );

        let orch =
            OrchestrationClient::with_client(base.to_string(), "cid".to_string(), Arc::new(client));

        let err = orch.send_heartbeat().await.unwrap_err();
        assert!(err.to_string().contains("Failed to send heartbeat"));
    }

    #[tokio::test]
    async fn test_notify_s3_success() {
        let client = InMemoryHttpClient::new();
        let base = "http://aggregator";
        client.set_response(
            &format!("{}/notify", base),
            Ok(ok_json(serde_json::json!({}))),
        );

        let orch =
            OrchestrationClient::with_client(base.to_string(), "cid".to_string(), Arc::new(client));

        orch.notify_s3("bucket", "orderbook/key.jsonl")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_notify_s3_non_success_status() {
        let client = InMemoryHttpClient::new();
        let base = "http://aggregator";
        client.set_response(&format!("{}/notify", base), Ok(error_response(500, "down")));

        let orch =
            OrchestrationClient::with_client(base.to_string(), "cid".to_string(), Arc::new(client));

        let err = orch.notify_s3("bucket", "key").await.unwrap_err();
        assert!(err.to_string().contains("Notify failed"));
    }

    #[tokio::test]
    async fn test_notify_s3_http_error() {
        let client = InMemoryHttpClient::new();
        let base = "http://aggregator";
        client.set_response(&format!("{}/notify", base), Err("timeout".to_string()));

        let orch =
            OrchestrationClient::with_client(base.to_string(), "cid".to_string(), Arc::new(client));

        let err = orch.notify_s3("bucket", "key").await.unwrap_err();
        assert!(err.to_string().contains("Failed to notify aggregator"));
    }

    #[tokio::test]
    async fn test_collector_id() {
        let client = InMemoryHttpClient::new();
        let base = "http://aggregator";
        let orch = OrchestrationClient::with_client(
            base.to_string(),
            "cid-abc".to_string(),
            Arc::new(client),
        );

        assert_eq!(orch.collector_id(), "cid-abc");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_spawn_heartbeat_task_logs_on_failure() {
        let mock = Arc::new(InMemoryHttpClient::new());
        let base = "http://aggregator";
        let url = format!("{}/heartbeat/cid", base);
        mock.set_response(&url, Ok(error_response(503, "unavailable")));

        let client: Arc<dyn HttpClient> = mock.clone();
        let orch = OrchestrationClient::with_client(base.to_string(), "cid".to_string(), client);

        let handle = orch.spawn_heartbeat_task(Duration::from_secs(1));
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        handle.abort();
        let _ = handle.await;

        assert!(mock.request_count(&url) >= 1);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_spawn_heartbeat_task_sends_repeatedly() {
        let mock = Arc::new(InMemoryHttpClient::new());
        let base = "http://aggregator";
        let url = format!("{}/heartbeat/cid", base);
        mock.set_response_sequence(
            &url,
            vec![
                Ok(ok_json(serde_json::json!({}))),
                Ok(ok_json(serde_json::json!({}))),
                Ok(ok_json(serde_json::json!({}))),
            ],
        );

        let client: Arc<dyn HttpClient> = mock.clone();
        let orch = OrchestrationClient::with_client(base.to_string(), "cid".to_string(), client);

        let handle = orch.spawn_heartbeat_task(Duration::from_secs(1));
        // Let the spawned task start up and fire its first immediate tick.
        tokio::task::yield_now().await;
        // Advance the clock past two more scheduled ticks.
        tokio::time::advance(Duration::from_secs(3)).await;
        tokio::task::yield_now().await;
        handle.abort();
        let _ = handle.await;

        assert!(mock.request_count(&url) >= 2);
    }

    #[test]
    fn test_debug_format_contains_struct_name() {
        let client = InMemoryHttpClient::new();
        let orch = OrchestrationClient::with_client(
            "http://aggregator".to_string(),
            "cid-debug".to_string(),
            Arc::new(client),
        );

        let debug = format!("{:?}", orch);
        assert!(debug.contains("OrchestrationClient"));
        assert!(debug.contains("http://aggregator"));
        assert!(debug.contains("cid-debug"));
    }
}
