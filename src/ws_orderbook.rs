use anyhow::{Context, Result};
use chrono::Utc;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{info, warn};

use std::path::Path;

use crate::aggregate_s3::{AwsS3Service, S3Service};
use crate::orchestration::OrchestrationClient;
use crate::storage::RotatedWriter;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderbookEvent {
    pub event_type: String,
    pub asset: String,
    pub side: Option<String>,
    pub price: Option<f64>,
    pub size: Option<f64>,
    pub timestamp: i64,
    pub received_at: i64,
    pub raw: String,
    pub worker_id: usize,
}

/// Spawns multiple parallel WebSocket workers, each responsible for a chunk of tokens.
pub struct OrderbookCollector {
    token_ids: Vec<String>,
    output_dir: PathBuf,
    chunk_size: usize,
    relay_url: Option<String>,
    rotate_interval: Duration,
    duration_secs: Option<u64>,
    shutdown: Arc<AtomicBool>,
    /// Optional aggregator URL for orchestrated mode.
    /// When set, the collector registers with the aggregator, fetches its
    /// token assignment, and sends periodic heartbeats.
    aggregator_url: Option<String>,
    /// Optional S3 configuration. When set, closed rotation files are uploaded
    /// to S3 and the aggregator is notified.
    s3_bucket: Option<String>,
    s3_prefix: Option<String>,
    aws_region: String,
    ws_url: Option<String>,
}

impl OrderbookCollector {
    pub fn new(
        token_ids: Vec<String>,
        output_dir: PathBuf,
        relay_url: Option<String>,
        chunk_size: usize,
        rotate_interval: Duration,
        duration_secs: Option<u64>,
    ) -> Self {
        Self {
            token_ids,
            output_dir,
            chunk_size,
            relay_url,
            rotate_interval,
            duration_secs,
            shutdown: Arc::new(AtomicBool::new(false)),
            aggregator_url: None,
            s3_bucket: None,
            s3_prefix: None,
            aws_region: "us-east-1".to_string(),
            ws_url: None,
        }
    }

    /// Set the aggregator URL to enable orchestrated mode.
    ///
    /// In orchestrated mode, the collector ignores any initially supplied
    /// `token_ids` and instead fetches its assignment from the aggregator.
    pub fn with_aggregator_url(mut self, url: String) -> Self {
        self.aggregator_url = Some(url);
        self
    }

    /// Enable S3 upload of closed rotation files.
    ///
    /// When set, each worker uploads completed files to
    /// `s3://{bucket}/{prefix}{collector_id}/{relative_local_path}` and notifies
    /// the aggregator via `/notify`.
    pub fn with_s3_upload(mut self, bucket: String, prefix: String, region: String) -> Self {
        self.s3_bucket = Some(bucket);
        self.s3_prefix = Some(prefix);
        self.aws_region = region;
        self
    }

    #[cfg(test)]
    fn with_ws_url(mut self, url: String) -> Self {
        self.ws_url = Some(url);
        self
    }

    pub async fn run(self) -> Result<()> {
        // ── Orchestrated mode: register with aggregator and fetch assignment ──
        let mut _heartbeat_handle: Option<tokio::task::JoinHandle<()>> = None;
        let (
            token_ids,
            orchestration_client,
            collector_id,
            s3_service,
            s3_bucket,
            s3_prefix,
        ) = if let Some(aggregator_url) = &self.aggregator_url {
            info!(url = %aggregator_url, "Entering orchestrated collector mode");
            let client = OrchestrationClient::register(aggregator_url.clone(), None)
                .await
                .context("Failed to register with aggregator")?;
            let cid = client.collector_id().to_string();

            let (assigned_tokens, assigned_chunk_size) = client
                .fetch_assignment()
                .await
                .context("Failed to fetch assignment from aggregator")?;

            info!(
                collector_id = %cid,
                assigned_tokens = assigned_tokens.len(),
                chunk_size = assigned_chunk_size,
                "Received assignment from aggregator"
            );

            // Start heartbeat task: every 10 seconds.
            _heartbeat_handle = Some(client.spawn_heartbeat_task(Duration::from_secs(10)));

            // Build S3 service if the user supplied a bucket.
            let s3_service: Option<Arc<dyn S3Service>> = if self.s3_bucket.is_some() {
                Some(Arc::new(AwsS3Service::new(&self.aws_region).await))
            } else {
                None
            };

            (
                assigned_tokens,
                Some(client),
                Some(cid),
                s3_service,
                self.s3_bucket.clone(),
                self.s3_prefix.clone(),
            )
        } else {
            (
                self.token_ids.clone(),
                None,
                None,
                None,
                self.s3_bucket.clone(),
                self.s3_prefix.clone(),
            )
        };

        let token_chunks: Vec<Vec<String>> = token_ids
            .chunks(self.chunk_size)
            .map(|c| c.to_vec())
            .collect();

        info!(
            total_tokens = token_ids.len(),
            chunks = token_chunks.len(),
            chunk_size = self.chunk_size,
            rotate_secs = self.rotate_interval.as_secs(),
            duration_secs = ?self.duration_secs,
            relay_url = ?self.relay_url,
            orchestrated = self.aggregator_url.is_some(),
            s3_upload = s3_bucket.is_some(),
            "Starting parallel WebSocket collectors"
        );

        let mut handles = Vec::new();
        for (id, chunk) in token_chunks.into_iter().enumerate() {
            let output_dir = self.output_dir.clone();
            let relay_url = self.relay_url.clone();
            let rotate_interval = self.rotate_interval;
            let shutdown = self.shutdown.clone();
            let s3_service = s3_service.clone();
            let s3_bucket = s3_bucket.clone();
            let s3_prefix = s3_prefix.clone();
            let orchestration_client = orchestration_client.clone();
            let collector_id = collector_id.clone();
            let ws_url = self.ws_url.clone();
            let handle = tokio::spawn(async move {
                let mut worker = OrderbookWorker::new(
                    id,
                    chunk,
                    output_dir,
                    relay_url,
                    rotate_interval,
                    shutdown,
                    s3_service,
                    s3_bucket,
                    s3_prefix,
                    collector_id,
                    orchestration_client,
                    ws_url,
                );
                if let Err(e) = worker.run().await {
                    warn!(worker_id = id, error = %e, "Worker failed");
                }
            });
            handles.push(handle);
            // Stagger connections to avoid IP-based rate limiting.
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        // Graceful shutdown on Ctrl+C or duration timeout
        if let Some(duration) = self.duration_secs {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("Ctrl+C received, shutting down...");
                }
                _ = tokio::time::sleep(Duration::from_secs(duration)) => {
                    info!(duration, "Duration reached, shutting down...");
                }
            }
        } else {
            tokio::signal::ctrl_c().await.ok();
            info!("Shutdown signal received, waiting for workers to flush...");
        }

        self.shutdown.store(true, Ordering::Relaxed);

        for h in handles {
            let _ = h.await;
        }

        info!("All workers stopped");
        Ok(())
    }
}

struct OrderbookWorker {
    id: usize,
    token_ids: Vec<String>,
    output_dir: PathBuf,
    relay_url: Option<String>,
    buffer: Vec<OrderbookEvent>,
    flush_interval: Duration,
    buffer_size: usize,
    http_client: reqwest::Client,
    writer: Option<RotatedWriter>,
    rotate_interval: Duration,
    shutdown: Arc<AtomicBool>,
    s3_service: Option<Arc<dyn S3Service>>,
    s3_bucket: Option<String>,
    s3_prefix: Option<String>,
    collector_id: Option<String>,
    orchestration_client: Option<OrchestrationClient>,
    ws_url: Option<String>,
}

impl OrderbookWorker {
    #[allow(clippy::too_many_arguments)]
    fn new(
        id: usize,
        token_ids: Vec<String>,
        output_dir: PathBuf,
        relay_url: Option<String>,
        rotate_interval: Duration,
        shutdown: Arc<AtomicBool>,
        s3_service: Option<Arc<dyn S3Service>>,
        s3_bucket: Option<String>,
        s3_prefix: Option<String>,
        collector_id: Option<String>,
        orchestration_client: Option<OrchestrationClient>,
        ws_url: Option<String>,
    ) -> Self {
        // Ensure the output directory exists and use an absolute path so that
        // rotated file paths can be reliably stripped to compute S3 keys.
        std::fs::create_dir_all(&output_dir).ok();
        let output_dir = std::fs::canonicalize(&output_dir).unwrap_or(output_dir);

        Self {
            id,
            token_ids,
            output_dir,
            relay_url,
            buffer: Vec::with_capacity(1_000),
            flush_interval: Duration::from_secs(10),
            buffer_size: 1_000,
            http_client: reqwest::Client::new(),
            writer: None,
            rotate_interval,
            shutdown,
            s3_service,
            s3_bucket,
            s3_prefix,
            collector_id,
            orchestration_client,
            ws_url,
        }
    }

    fn writer(&mut self) -> Result<&mut RotatedWriter> {
        if self.writer.is_none() {
            let suffix = format!("_worker_{}", self.id);
            self.writer = Some(RotatedWriter::new(
                self.output_dir.clone(),
                suffix,
                self.rotate_interval,
            ));
        }
        Ok(self.writer.as_mut().unwrap())
    }

    async fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let count = self.buffer.len();
        let id = self.id;

        if let Some(url) = &self.relay_url {
            // Relay mode: send buffered events as newline-delimited JSON
            let buffer = std::mem::take(&mut self.buffer);
            let mut body = String::new();
            for ev in &buffer {
                body.push_str(&serde_json::to_string(ev)?);
                body.push('\n');
            }
            match self.http_client.post(url).body(body).send().await {
                Ok(resp) if resp.status().is_success() => {
                    info!(worker_id = id, count, "Relayed buffer");
                }
                Ok(resp) => {
                    warn!(worker_id = id, status = %resp.status(), "Relay failed, will retry");
                    self.buffer = buffer; // restore for retry
                }
                Err(e) => {
                    warn!(worker_id = id, error = %e, "Relay request failed");
                    self.buffer = buffer; // restore for retry
                }
            }
        } else {
            // Local storage mode with time-rotated writer
            let buffer = std::mem::take(&mut self.buffer);
            let writer = self.writer()?;
            writer.append(&buffer).await?;
            info!(
                worker_id = id,
                count,
                path = ?writer.current_path(),
                "Flushed buffer"
            );

            // If a rotation happened, upload the closed file in the background.
            if let Some(rotated) = writer.take_rotated_path() {
                self.spawn_upload_task(rotated);
            }
        }
        Ok(())
    }

    /// Spawn a background task to upload a rotated file to S3 and notify the aggregator.
    fn spawn_upload_task(&self, local_path: PathBuf) {
        if let (Some(s3), Some(bucket), Some(prefix), Some(collector_id)) = (
            self.s3_service.clone(),
            self.s3_bucket.clone(),
            self.s3_prefix.clone(),
            self.collector_id.clone(),
        ) {
            let output_dir = self.output_dir.clone();
            let client = self.orchestration_client.clone();
            tokio::spawn(async move {
                if let Err(e) = upload_rotated_file(
                    &*s3,
                    &bucket,
                    &prefix,
                    &collector_id,
                    &output_dir,
                    &local_path,
                    client.as_ref(),
                )
                .await
                {
                    warn!(
                        error = %e,
                        path = %local_path.display(),
                        "Failed to upload/notify rotated file; local copy preserved"
                    );
                }
            });
        }
    }

    async fn run(&mut self) -> Result<()> {
        let mut reconnect_delay = Duration::from_secs(5);
        loop {
            match self.connect_and_collect().await {
                Ok(()) => {
                    info!(worker_id = self.id, "Worker ended gracefully");
                    break;
                }
                Err(e) => {
                    warn!(
                        worker_id = self.id,
                        error = %e,
                        "Worker error, reconnecting"
                    );
                    tokio::time::sleep(reconnect_delay).await;
                    reconnect_delay = std::cmp::min(reconnect_delay * 2, Duration::from_secs(60));
                }
            }
        }
        self.flush().await?;
        if let Some(writer) = &mut self.writer {
            writer.flush().await?;
        }
        Ok(())
    }

    async fn connect_and_collect(&mut self) -> Result<()> {
        const DEFAULT_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
        let url = self.ws_url.as_deref().unwrap_or(DEFAULT_WS_URL);
        let (ws_stream, _) = connect_async(url).await?;
        let (mut write, mut read) = ws_stream.split();

        let payload = json!({
            "type": "market",
            "assets_ids": self.token_ids,
            "custom_feature_enabled": true,
        });
        write.send(Message::Text(payload.to_string())).await?;
        info!(
            worker_id = self.id,
            count = self.token_ids.len(),
            "Subscribed"
        );

        let mut flush_tick = tokio::time::interval(self.flush_interval);
        let mut ping_tick = tokio::time::interval(Duration::from_secs(10));
        let mut shutdown_check = tokio::time::interval(Duration::from_secs(1));

        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                info!(worker_id = self.id, "Shutdown requested, exiting event loop");
                break;
            }
            tokio::select! {
                _ = shutdown_check.tick() => {
                    if self.shutdown.load(Ordering::Relaxed) {
                        info!(worker_id = self.id, "Shutdown confirmed, exiting event loop");
                        break;
                    }
                }
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if text == "PONG" {
                                continue;
                            }
                            self.handle_message(&text)?;
                            if self.buffer.len() >= self.buffer_size {
                                self.flush().await?;
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            write.send(Message::Pong(data)).await.ok();
                        }
                        Some(Ok(Message::Close(_))) => {
                            warn!(worker_id = self.id, "WebSocket closed by server");
                            break;
                        }
                        Some(Err(e)) => return Err(e.into()),
                        _ => {}
                    }
                }
                _ = flush_tick.tick() => {
                    self.flush().await?;
                }
                _ = ping_tick.tick() => {
                    write.send(Message::Text("PING".to_string())).await.ok();
                }
            }
        }
        Ok(())
    }

    /// Parse a single WebSocket text message and push an `OrderbookEvent` to the buffer.
    ///
    /// This is the core message handler for all Polymarket WebSocket events.
    /// It handles three event types:
    /// - `"book"` — Full orderbook snapshot (bids + asks)
    /// - `"price_change"` — Midpoint price update
    /// - `"last_trade_price"` — On-chain trade fill
    ///
    /// # Arguments
    /// * `text` — The raw WebSocket text frame (JSON string)
    ///
    /// # Returns
    /// `Ok(())` if the message was parsed and buffered successfully.
    /// `Err` if JSON parsing fails or required fields are missing.
    ///
    /// # Side Effects
    /// Pushes one or more `OrderbookEvent`s to `self.buffer`. When the buffer reaches
    /// `self.buffer_size`, it is automatically flushed to disk or relay.
    ///
    /// # Example — Input / Output
    /// ```rust,ignore
    /// // Input: raw WebSocket text frame
    /// let text = r#"{"event_type":"last_trade_price","asset_id":"123...","price":"0.084","size":"110.476189","side":"BUY","timestamp":"1781051970651"}"#;
    ///
    /// // Function call
    /// worker.handle_message(text).unwrap();
    ///
    /// // Output: buffer now contains an OrderbookEvent
    /// assert_eq!(worker.buffer.len(), 1);
    /// assert_eq!(worker.buffer[0].event_type, "last_trade");
    /// assert_eq!(worker.buffer[0].price, Some(0.084));
    /// assert_eq!(worker.buffer[0].side, Some("BUY".to_string()));
    /// ```
    fn handle_message(&mut self, text: &str) -> Result<()> {
        // Capture the local receive timestamp for latency tracking.
        let received_at = Utc::now().timestamp_millis();
        let msg: serde_json::Value = serde_json::from_str(text)?;

        let msg_type = msg.get("event_type").and_then(|v| v.as_str()).unwrap_or("");
        let asset = msg
            .get("asset_id")
            .or_else(|| msg.get("token_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        // Polymarket sometimes sends price/size as strings (e.g. "0.05").
        let parse_f64 = |v: Option<&serde_json::Value>| -> Option<f64> {
            v.and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
        };

        match msg_type {
            "book" => {
                for side in ["bids", "asks"] {
                    if let Some(levels) = msg.get(side).and_then(|v| v.as_array()) {
                        for level in levels {
                            self.buffer.push(OrderbookEvent {
                                event_type: "book".to_string(),
                                asset: asset.clone(),
                                side: Some(if side == "bids" {
                                    "bid".to_string()
                                } else {
                                    "ask".to_string()
                                }),
                                price: parse_f64(level.get("price")),
                                size: parse_f64(level.get("size")),
                                timestamp: msg
                                    .get("timestamp")
                                    .and_then(|v| v.as_i64())
                                    .unwrap_or(received_at),
                                received_at,
                                raw: text.to_string(),
                                worker_id: self.id,
                            });
                        }
                    }
                }
            }
            "price_change" => {
                if let Some(changes) = msg.get("price_changes").and_then(|v| v.as_array()) {
                    for change in changes {
                        let change_asset = change
                            .get("asset_id")
                            .or_else(|| change.get("token_id"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        self.buffer.push(OrderbookEvent {
                            event_type: "price_change".to_string(),
                            asset: change_asset,
                            side: change
                                .get("side")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string()),
                            price: parse_f64(change.get("price")),
                            size: parse_f64(change.get("size")),
                            timestamp: msg
                                .get("timestamp")
                                .and_then(|v| v.as_i64())
                                .unwrap_or(received_at),
                            received_at,
                            raw: text.to_string(),
                            worker_id: self.id,
                        });
                    }
                } else {
                    self.buffer.push(OrderbookEvent {
                        event_type: "price_change".to_string(),
                        asset: asset.clone(),
                        side: msg.get("side").and_then(|v| v.as_str()).map(|s| s.to_string()),
                        price: parse_f64(msg.get("price")),
                        size: parse_f64(msg.get("size")),
                        timestamp: msg
                            .get("timestamp")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(received_at),
                        received_at,
                        raw: text.to_string(),
                        worker_id: self.id,
                    });
                }
            }
            // ── Last Trade Price Event ────────────────────────────────────
            // Emitted by Polymarket's WebSocket when an on-chain fill occurs.
            // Contains the trade price, size, direction (BUY/SELL), and crucially
            // the transaction_hash which lets us look up maker/taker on Polygon.
            //
            // Example input — raw WebSocket message:
            // {
            //   "event_type": "last_trade_price",
            //   "asset_id": "400737005616952...",
            //   "price": "0.084",
            //   "size": "110.476189",
            //   "side": "BUY",
            //   "transaction_hash": "0x5e5fe7c64a30b1d23366bf508ea288b994e3b3d8d5afd5facd991af8551dae02",
            //   "timestamp": "1781051970651"
            // }
            //
            // Example output — OrderbookEvent pushed to buffer:
            // OrderbookEvent {
            //     event_type: "last_trade",
            //     asset: "40073700561695212653451049120779209383948898865772011302940523990213422296817",
            //     side: Some("BUY"),
            //     price: Some(0.084),
            //     size: Some(110.476189),
            //     timestamp: 1781051970651,
            //     received_at: 1781051970699,
            //     raw: "{...original JSON text...}",
            //     worker_id: 3,
            // }
            "last_trade_price" => {
                self.buffer.push(OrderbookEvent {
                    event_type: "last_trade".to_string(),
                    asset: asset.clone(),
                    // Parse the trade direction: "BUY" = buyer was the taker (aggressive),
                    // "SELL" = seller was the taker. The opposite side was the resting maker.
                    side: msg.get("side").and_then(|v| v.as_str()).map(|s| s.to_string()),
                    // Price and size may be sent as strings (e.g., "0.084") rather than numbers.
                    // parse_f64 handles both via as_f64() fallback to as_str().parse().
                    price: parse_f64(msg.get("price")),
                    size: parse_f64(msg.get("size")),
                    // Use the event's own timestamp if present, otherwise fall back to
                    // our local receive time. This ensures chronological ordering even
                    // if the WebSocket message is slightly delayed.
                    timestamp: msg
                        .get("timestamp")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(received_at),
                    received_at,
                    // Store the full raw JSON text. This preserves fields we don't explicitly
                    // extract (like transaction_hash) for downstream parsing by the viewer.
                    raw: text.to_string(),
                    worker_id: self.id,
                });
            }
            _ => {}
        }

        Ok(())
    }
}

/// Upload a closed rotation file to S3 and optionally notify the aggregator.
///
/// The S3 key is built as `{prefix}{collector_id}/{relative_local_path}` so that
/// multiple collectors can write to the same prefix without key collisions.
/// The local file is deleted only after both upload and notification succeed.
async fn upload_rotated_file(
    s3: &dyn S3Service,
    bucket: &str,
    prefix: &str,
    collector_id: &str,
    output_dir: &Path,
    local_path: &Path,
    client: Option<&OrchestrationClient>,
) -> Result<()> {
    let body = tokio::fs::read(local_path)
        .await
        .with_context(|| format!("Failed to read rotated file {}", local_path.display()))?;

    let rel = local_path
        .strip_prefix(output_dir)
        .with_context(|| {
            format!(
                "Rotated file {} is not under output dir {}",
                local_path.display(),
                output_dir.display()
            )
        })?
        .to_string_lossy()
        .replace('\\', "/");

    let key = format!("{}/{}/{}", prefix.trim_end_matches('/'), collector_id, rel);

    s3.put_object(bucket, &key, body)
        .await
        .with_context(|| format!("Failed to upload {} to s3://{}/{}", local_path.display(), bucket, key))?;

    if let Some(client) = client {
        client
            .notify_s3(bucket, &key)
            .await
            .with_context(|| format!("Failed to notify aggregator about s3://{}/{}", bucket, key))?;
    }

    tokio::fs::remove_file(local_path)
        .await
        .with_context(|| format!("Failed to delete local file {}", local_path.display()))?;

    info!(key = %key, "Uploaded and notified");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate_s3::{InMemoryS3Service, S3Object, S3Service};
    use crate::http_client::{HttpResponse, InMemoryHttpClient};
    use async_trait::async_trait;
    use tempfile::tempdir;

    fn test_worker(dir: &tempfile::TempDir) -> OrderbookWorker {
        OrderbookWorker::new(
            0,
            vec!["token1".to_string()],
            dir.path().to_path_buf(),
            None,
            Duration::from_secs(60),
            Arc::new(AtomicBool::new(false)),
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    #[test]
    fn test_handle_message_book() {
        let mut worker = test_worker(&tempdir().unwrap());
        let text = r#"{"event_type":"book","asset_id":"token1","bids":[["0.1","100"]],"asks":[["0.2","50"]],"timestamp":1234}"#;
        worker.handle_message(text).unwrap();
        assert_eq!(worker.buffer.len(), 2); // one bid + one ask
        assert_eq!(worker.buffer[0].event_type, "book");
        assert_eq!(worker.buffer[0].asset, "token1");
    }

    #[test]
    fn test_handle_message_price_change_nested() {
        let mut worker = test_worker(&tempdir().unwrap());
        let text = r#"{"event_type":"price_change","price_changes":[{"asset_id":"token1","price":"0.15","size":"200","side":"SELL"}],"timestamp":1234}"#;
        worker.handle_message(text).unwrap();
        assert_eq!(worker.buffer.len(), 1);
        assert_eq!(worker.buffer[0].event_type, "price_change");
        assert_eq!(worker.buffer[0].price, Some(0.15));
        assert_eq!(worker.buffer[0].side, Some("SELL".to_string()));
    }

    #[test]
    fn test_handle_message_price_change_nested_token_id() {
        let mut worker = test_worker(&tempdir().unwrap());
        let text = r#"{"event_type":"price_change","price_changes":[{"token_id":"token1","price":"0.16","size":"250"}],"timestamp":1234}"#;
        worker.handle_message(text).unwrap();
        assert_eq!(worker.buffer.len(), 1);
        assert_eq!(worker.buffer[0].asset, "token1");
        assert_eq!(worker.buffer[0].event_type, "price_change");
    }

    #[test]
    fn test_handle_message_price_change_flat() {
        let mut worker = test_worker(&tempdir().unwrap());
        let text = r#"{"event_type":"price_change","asset_id":"token1","price":"0.16","size":"300","timestamp":1234}"#;
        worker.handle_message(text).unwrap();
        assert_eq!(worker.buffer.len(), 1);
        assert_eq!(worker.buffer[0].event_type, "price_change");
    }

    #[test]
    fn test_handle_message_last_trade() {
        let mut worker = test_worker(&tempdir().unwrap());
        let text = r#"{"event_type":"last_trade_price","asset_id":"token1","price":"0.5","size":"10","side":"BUY","transaction_hash":"0xabc","timestamp":"1234"}"#;
        worker.handle_message(text).unwrap();
        assert_eq!(worker.buffer.len(), 1);
        assert_eq!(worker.buffer[0].event_type, "last_trade");
        assert_eq!(worker.buffer[0].price, Some(0.5));
        assert_eq!(worker.buffer[0].side, Some("BUY".to_string()));
    }

    #[test]
    fn test_handle_message_unknown_type() {
        let mut worker = test_worker(&tempdir().unwrap());
        let text = r#"{"event_type":"unknown","asset_id":"token1"}"#;
        worker.handle_message(text).unwrap();
        assert!(worker.buffer.is_empty());
    }

    #[tokio::test]
    async fn test_upload_rotated_file_success() {
        let dir = tempdir().unwrap();
        let s3 = InMemoryS3Service::default();
        let local_path = dir.path().join("2025-06-08").join("12").join("12_00_worker_0.jsonl");
        tokio::fs::create_dir_all(local_path.parent().unwrap()).await.unwrap();
        tokio::fs::write(&local_path, b"{\"v\":1}\n").await.unwrap();

        upload_rotated_file(
            &s3,
            "bucket",
            "orderbook/",
            "cid",
            dir.path(),
            &local_path,
            None,
        )
        .await
        .unwrap();

        assert!(!local_path.exists());
        let objects = s3.list_objects("bucket", "orderbook/").await.unwrap();
        assert_eq!(objects.len(), 1);
        assert!(objects[0].key.contains("orderbook/cid/"));
    }

    #[tokio::test]
    async fn test_upload_rotated_file_notifies() {
        let dir = tempdir().unwrap();
        let s3 = InMemoryS3Service::default();
        let http = InMemoryHttpClient::new();
        http.set_response(
            "http://aggregator/notify",
            Ok(HttpResponse {
                status: 200,
                body: "{}".to_string(),
            }),
        );
        let client = OrchestrationClient::with_client(
            "http://aggregator".to_string(),
            "cid".to_string(),
            Arc::new(http),
        );

        let local_path = dir.path().join("2025-06-08").join("12").join("12_00_worker_0.jsonl");
        tokio::fs::create_dir_all(local_path.parent().unwrap()).await.unwrap();
        tokio::fs::write(&local_path, b"{\"v\":1}\n").await.unwrap();

        upload_rotated_file(
            &s3,
            "bucket",
            "orderbook/",
            "cid",
            dir.path(),
            &local_path,
            Some(&client),
        )
        .await
        .unwrap();

        assert!(!local_path.exists());
    }

    #[tokio::test]
    async fn test_flush_local_writes_and_uploads_on_rotation() {
        let dir = tempdir().unwrap();
        let s3 = Arc::new(InMemoryS3Service::default());
        let http = InMemoryHttpClient::new();
        http.set_response(
            "http://aggregator/notify",
            Ok(HttpResponse {
                status: 200,
                body: "{}".to_string(),
            }),
        );
        let client = OrchestrationClient::with_client(
            "http://aggregator".to_string(),
            "cid".to_string(),
            Arc::new(http),
        );

        let mut worker = OrderbookWorker::new(
            0,
            vec!["token1".to_string()],
            dir.path().to_path_buf(),
            None,
            Duration::from_secs(60),
            Arc::new(AtomicBool::new(false)),
            Some(s3.clone()),
            Some("bucket".to_string()),
            Some("orderbook/".to_string()),
            Some("cid".to_string()),
            Some(client),
            None,
        );

        // Write into the first window, then rotate to the next minute.
        worker
            .handle_message(r#"{"event_type":"last_trade_price","asset_id":"token1","price":"0.5","size":"1","timestamp":1}"#)
            .unwrap();
        worker.flush().await.unwrap();
        let first_path = worker.writer.as_ref().unwrap().current_path().unwrap().clone();

        let next_window = worker.writer.as_ref().unwrap().current_window.unwrap()
            + chrono::Duration::minutes(1);
        worker.writer.as_mut().unwrap().rotate_to(next_window).await.unwrap();

        // After rotation, the previous file should be queued for upload.
        let rotated = worker.writer.as_mut().unwrap().take_rotated_path();
        assert_eq!(rotated, Some(first_path));

        // Trigger the upload synchronously for the test.
        if let Some(path) = rotated {
            upload_rotated_file(
                &*s3,
                "bucket",
                "orderbook/",
                "cid",
                &worker.output_dir,
                &path,
                worker.orchestration_client.as_ref(),
            )
            .await
            .unwrap();
        }

        let objects = s3.list_objects("bucket", "orderbook/").await.unwrap();
        assert_eq!(objects.len(), 1);
    }

    #[tokio::test]
    async fn test_flush_relay_success() {
        let dir = tempdir().unwrap();
        let mut worker = OrderbookWorker::new(
            0,
            vec!["token1".to_string()],
            dir.path().to_path_buf(),
            Some("http://relay".to_string()),
            Duration::from_secs(60),
            Arc::new(AtomicBool::new(false)),
            None,
            None,
            None,
            None,
            None,
            None,
        );
        worker
            .handle_message(r#"{"event_type":"last_trade_price","asset_id":"token1","price":"0.5","size":"1","timestamp":1}"#)
            .unwrap();
        // Without a mock relay server this will fail and restore the buffer.
        worker.flush().await.unwrap();
        assert_eq!(worker.buffer.len(), 1);
    }

    #[test]
    fn test_collector_builder_methods() {
        let collector = OrderbookCollector::new(
            vec!["t1".to_string()],
            PathBuf::from("/tmp"),
            None,
            10,
            Duration::from_secs(60),
            Some(60),
        )
        .with_aggregator_url("http://agg".to_string())
        .with_s3_upload("bucket".to_string(), "prefix/".to_string(), "us-west-2".to_string());

        assert!(collector.aggregator_url.is_some());
        assert_eq!(collector.s3_bucket, Some("bucket".to_string()));
        assert_eq!(collector.s3_prefix, Some("prefix/".to_string()));
        assert_eq!(collector.aws_region, "us-west-2");
    }

    #[test]
    fn test_handle_message_book_missing_bids_asks() {
        let mut worker = test_worker(&tempdir().unwrap());
        let text = r#"{"event_type":"book","asset_id":"token1","timestamp":1234}"#;
        worker.handle_message(text).unwrap();
        assert!(worker.buffer.is_empty());
    }

    #[test]
    fn test_handle_message_last_trade_no_side() {
        let mut worker = test_worker(&tempdir().unwrap());
        let text = r#"{"event_type":"last_trade_price","asset_id":"token1","price":"0.5","size":"10","timestamp":"1234"}"#;
        worker.handle_message(text).unwrap();
        assert_eq!(worker.buffer.len(), 1);
        assert_eq!(worker.buffer[0].side, None);
    }

    #[test]
    fn test_handle_message_invalid_json() {
        let mut worker = test_worker(&tempdir().unwrap());
        assert!(worker.handle_message("not json").is_err());
    }

    #[tokio::test]
    async fn test_flush_empty_buffer() {
        let mut worker = test_worker(&tempdir().unwrap());
        worker.flush().await.unwrap();
        assert!(worker.buffer.is_empty());
    }

    #[tokio::test]
    async fn test_upload_rotated_file_missing_local() {
        let dir = tempdir().unwrap();
        let s3 = InMemoryS3Service::default();
        let path = dir.path().join("missing.jsonl");
        assert!(upload_rotated_file(&s3, "b", "p/", "c", dir.path(), &path, None)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_upload_rotated_file_not_under_output_dir() {
        let dir = tempdir().unwrap();
        let other = tempdir().unwrap();
        let s3 = InMemoryS3Service::default();
        let path = other.path().join("file.jsonl");
        tokio::fs::write(&path, b"x").await.unwrap();
        assert!(upload_rotated_file(&s3, "b", "p/", "c", dir.path(), &path, None)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_upload_rotated_file_notify_failure_preserves_local() {
        let dir = tempdir().unwrap();
        let s3 = InMemoryS3Service::default();
        let http = InMemoryHttpClient::new();
        http.set_response(
            "http://aggregator/notify",
            Err("notify failed".to_string()),
        );
        let client = OrchestrationClient::with_client(
            "http://aggregator".to_string(),
            "cid".to_string(),
            Arc::new(http),
        );

        let local_path = dir.path().join("file.jsonl");
        tokio::fs::write(&local_path, b"x").await.unwrap();

        let result = upload_rotated_file(
            &s3,
            "bucket",
            "orderbook/",
            "cid",
            dir.path(),
            &local_path,
            Some(&client),
        )
        .await;
        assert!(result.is_err());
        assert!(local_path.exists());
    }

    // Helpers for the tests below.
    async fn run_ws_server(
        addr_tx: tokio::sync::oneshot::Sender<std::net::SocketAddr>,
        messages: Vec<Message>,
        shutdown: Arc<AtomicBool>,
    ) {
        use futures::stream::StreamExt;
        use tokio::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        addr_tx.send(listener.local_addr().unwrap()).unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        let (mut write, mut read) = tokio_tungstenite::accept_async(stream).await.unwrap().split();
        // Wait for the client's subscription message.
        let _ = read.next().await;
        // Give the client time to enter its read loop before flooding messages.
        tokio::time::sleep(Duration::from_millis(200)).await;
        for msg in messages {
            write.send(msg).await.unwrap();
        }
        // Keep the socket open until the test signals shutdown.
        while !shutdown.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let _ = write.close().await;
    }

    #[tokio::test]
    async fn test_connect_and_collect_handles_ping_pong_and_close() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let server_shutdown = Arc::new(AtomicBool::new(false));
        let ssd = server_shutdown.clone();
        let book = r#"{"event_type":"book","asset_id":"token1","bids":[["0.1","100"]],"asks":[["0.2","50"]],"timestamp":1234}"#;
        tokio::spawn(async move {
            run_ws_server(
                tx,
                vec![
                    Message::Ping(vec![]),
                    Message::Text(book.to_string()),
                    Message::Text("PONG".to_string()),
                ],
                ssd,
            )
            .await;
        });
        let addr = rx.await.unwrap();
        let url = format!("ws://{}", addr);

        let shutdown = Arc::new(AtomicBool::new(false));
        let mut worker = OrderbookWorker::new(
            0,
            vec!["token1".to_string()],
            tempdir().unwrap().path().to_path_buf(),
            None,
            Duration::from_secs(60),
            shutdown.clone(),
            None,
            None,
            None,
            None,
            None,
            Some(url),
        );

        // Let the worker run long enough to process the incoming frames.
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(800)).await;
            sd.store(true, Ordering::Relaxed);
        });

        let result = tokio::time::timeout(Duration::from_secs(5), worker.connect_and_collect()).await;
        server_shutdown.store(true, Ordering::Relaxed);
        result.unwrap().unwrap();
        assert!(!worker.buffer.is_empty());
    }

    #[tokio::test]
    async fn test_worker_run_flushes_and_exits() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let server_shutdown = Arc::new(AtomicBool::new(false));
        let ssd = server_shutdown.clone();
        tokio::spawn(async move {
            run_ws_server(tx, vec![Message::Close(None)], ssd).await;
        });
        let addr = rx.await.unwrap();
        let url = format!("ws://{}", addr);

        let mut worker = OrderbookWorker::new(
            0,
            vec!["token1".to_string()],
            tempdir().unwrap().path().to_path_buf(),
            None,
            Duration::from_secs(60),
            Arc::new(AtomicBool::new(false)),
            None,
            None,
            None,
            None,
            None,
            Some(url),
        );
        worker.handle_message(r#"{"event_type":"last_trade_price","asset_id":"token1","price":"0.5","size":"1","timestamp":1}"#).unwrap();

        tokio::time::timeout(Duration::from_secs(5), worker.run())
            .await
            .unwrap()
            .unwrap();
        assert!(worker.buffer.is_empty());
    }

    #[tokio::test]
    async fn test_collector_run_static_with_duration() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let server_shutdown = Arc::new(AtomicBool::new(false));
        let ssd = server_shutdown.clone();
        tokio::spawn(async move {
            run_ws_server(tx, vec![Message::Text("PONG".to_string())], ssd).await;
        });
        let addr = rx.await.unwrap();
        let url = format!("ws://{}", addr);

        let dir = tempdir().unwrap();
        let collector = OrderbookCollector::new(
            vec!["token1".to_string()],
            dir.path().to_path_buf(),
            None,
            100,
            Duration::from_secs(60),
            Some(1),
        )
        .with_ws_url(url);

        tokio::time::timeout(Duration::from_secs(10), collector.run())
            .await
            .unwrap()
            .unwrap();
        server_shutdown.store(true, Ordering::Relaxed);
    }

    #[tokio::test]
    async fn test_flush_relay_success_mock_server() {
        use axum::{routing::post, Router};
        let dir = tempdir().unwrap();
        let app = Router::new().route(
            "/",
            post(|body: String| async move {
                assert!(!body.is_empty());
                "ok"
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let mut worker = OrderbookWorker::new(
            0,
            vec!["token1".to_string()],
            dir.path().to_path_buf(),
            Some(format!("http://127.0.0.1:{}/", port)),
            Duration::from_secs(60),
            Arc::new(AtomicBool::new(false)),
            None,
            None,
            None,
            None,
            None,
            None,
        );
        worker.handle_message(r#"{"event_type":"last_trade_price","asset_id":"token1","price":"0.5","size":"1","timestamp":1}"#).unwrap();
        worker.flush().await.unwrap();
        assert!(worker.buffer.is_empty());
    }

    #[tokio::test]
    async fn test_flush_spawns_upload_task_on_rotation() {
        let dir = tempdir().unwrap();
        let s3 = Arc::new(InMemoryS3Service::default());
        let http = InMemoryHttpClient::new();
        http.set_response(
            "http://aggregator/notify",
            Ok(HttpResponse {
                status: 200,
                body: "{}".to_string(),
            }),
        );
        let client = OrchestrationClient::with_client(
            "http://aggregator".to_string(),
            "cid".to_string(),
            Arc::new(http),
        );

        let mut worker = OrderbookWorker::new(
            0,
            vec!["token1".to_string()],
            dir.path().to_path_buf(),
            None,
            Duration::from_secs(60),
            Arc::new(AtomicBool::new(false)),
            Some(s3.clone()),
            Some("bucket".to_string()),
            Some("orderbook/".to_string()),
            Some("cid".to_string()),
            Some(client),
            None,
        );

        worker.handle_message(r#"{"event_type":"last_trade_price","asset_id":"token1","price":"0.5","size":"1","timestamp":1}"#).unwrap();
        worker.flush().await.unwrap();
        let first_path = worker.writer.as_ref().unwrap().current_path().unwrap().clone();

        // Manually queue the current file as rotated so the next flush spawns the upload task.
        worker.writer.as_mut().unwrap().rotated_path = Some(first_path.clone());

        worker.handle_message(r#"{"event_type":"last_trade_price","asset_id":"token1","price":"0.6","size":"2","timestamp":2}"#).unwrap();
        worker.flush().await.unwrap();

        // Wait for the background upload task.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!first_path.exists());
        let objects = s3.list_objects("bucket", "orderbook/").await.unwrap();
        assert_eq!(objects.len(), 1);
    }

    #[tokio::test]
    async fn test_flush_relay_non_success_restores_buffer() {
        use axum::{http::StatusCode, routing::post, Router};
        let dir = tempdir().unwrap();
        let app = Router::new().route("/", post(|| async { StatusCode::SERVICE_UNAVAILABLE }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let mut worker = OrderbookWorker::new(
            0,
            vec!["token1".to_string()],
            dir.path().to_path_buf(),
            Some(format!("http://127.0.0.1:{}/", port)),
            Duration::from_secs(60),
            Arc::new(AtomicBool::new(false)),
            None,
            None,
            None,
            None,
            None,
            None,
        );
        worker
            .handle_message(r#"{"event_type":"last_trade_price","asset_id":"token1","price":"0.5","size":"1","timestamp":1}"#)
            .unwrap();
        worker.flush().await.unwrap();
        assert_eq!(worker.buffer.len(), 1);
    }

    /// S3 service whose `put_object` always fails, used to exercise error paths.
    struct FailingS3Service;

    #[async_trait]
    impl S3Service for FailingS3Service {
        async fn list_objects(&self, _bucket: &str, _prefix: &str) -> Result<Vec<S3Object>> {
            Ok(vec![])
        }

        async fn get_object(&self, _bucket: &str, _key: &str) -> Result<Vec<u8>> {
            anyhow::bail!("get failed")
        }

        async fn put_object(&self, _bucket: &str, _key: &str, _body: Vec<u8>) -> Result<()> {
            anyhow::bail!("put failed")
        }

        async fn delete_object(&self, _bucket: &str, _key: &str) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_upload_rotated_file_s3_put_failure() {
        let dir = tempdir().unwrap();
        let s3 = FailingS3Service;
        let local_path = dir.path().join("file.jsonl");
        tokio::fs::write(&local_path, b"x").await.unwrap();

        let result = upload_rotated_file(
            &s3,
            "bucket",
            "orderbook/",
            "cid",
            dir.path(),
            &local_path,
            None,
        )
        .await;
        assert!(result.is_err());
        assert!(local_path.exists());
    }

    #[tokio::test]
    async fn test_spawn_upload_task_logs_on_s3_failure() {
        let dir = tempdir().unwrap();
        let mut worker = OrderbookWorker::new(
            0,
            vec!["token1".to_string()],
            dir.path().to_path_buf(),
            None,
            Duration::from_secs(60),
            Arc::new(AtomicBool::new(false)),
            Some(Arc::new(FailingS3Service)),
            Some("bucket".to_string()),
            Some("orderbook/".to_string()),
            Some("cid".to_string()),
            None,
            None,
        );

        worker
            .handle_message(r#"{"event_type":"last_trade_price","asset_id":"token1","price":"0.5","size":"1","timestamp":1}"#)
            .unwrap();
        worker.flush().await.unwrap();
        let first_path = worker.writer.as_ref().unwrap().current_path().unwrap().clone();

        // Manually queue the current file as rotated so the next flush spawns the upload task.
        worker.writer.as_mut().unwrap().rotated_path = Some(first_path.clone());

        worker
            .handle_message(r#"{"event_type":"last_trade_price","asset_id":"token1","price":"0.6","size":"2","timestamp":2}"#)
            .unwrap();
        worker.flush().await.unwrap();

        // Wait for the background upload task to fail and log.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(first_path.exists());
    }

    async fn run_ws_server_error_then_close(
        addr_tx: tokio::sync::oneshot::Sender<std::net::SocketAddr>,
        shutdown: Arc<AtomicBool>,
    ) {
        use futures::stream::StreamExt;
        use tokio::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        addr_tx.send(listener.local_addr().unwrap()).unwrap();
        let mut connection = 0usize;
        loop {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
            let (stream, _) = listener.accept().await.unwrap();
            let (mut write, mut read) = tokio_tungstenite::accept_async(stream)
                .await
                .unwrap()
                .split();
            // Wait for the client's subscription message.
            let _ = read.next().await;
            if connection == 0 {
                write
                    .send(Message::Text("not json".to_string()))
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(100)).await;
                let _ = write.close().await;
            } else {
                write.send(Message::Close(None)).await.unwrap();
                while !shutdown.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                break;
            }
            connection += 1;
        }
    }

    #[tokio::test]
    async fn test_worker_run_reconnects_after_error() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let shutdown = Arc::new(AtomicBool::new(false));
        let sd = shutdown.clone();
        tokio::spawn(async move { run_ws_server_error_then_close(tx, sd).await });
        let addr = rx.await.unwrap();
        let url = format!("ws://{}", addr);

        let mut worker = OrderbookWorker::new(
            0,
            vec!["token1".to_string()],
            tempdir().unwrap().path().to_path_buf(),
            None,
            Duration::from_secs(60),
            Arc::new(AtomicBool::new(false)),
            None,
            None,
            None,
            None,
            None,
            Some(url),
        );

        tokio::time::timeout(Duration::from_secs(15), worker.run())
            .await
            .unwrap()
            .unwrap();
        shutdown.store(true, Ordering::Relaxed);
    }

    #[tokio::test]
    async fn test_binary_frame_is_ignored() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let server_shutdown = Arc::new(AtomicBool::new(false));
        let ssd = server_shutdown.clone();
        tokio::spawn(async move {
            run_ws_server(tx, vec![Message::Binary(vec![1, 2, 3])], ssd).await;
        });
        let addr = rx.await.unwrap();
        let url = format!("ws://{}", addr);

        let shutdown = Arc::new(AtomicBool::new(false));
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            sd.store(true, Ordering::Relaxed);
        });

        let mut worker = OrderbookWorker::new(
            0,
            vec!["token1".to_string()],
            tempdir().unwrap().path().to_path_buf(),
            None,
            Duration::from_secs(60),
            shutdown,
            None,
            None,
            None,
            None,
            None,
            Some(url),
        );
        tokio::time::timeout(Duration::from_secs(5), worker.connect_and_collect())
            .await
            .unwrap()
            .unwrap();
        server_shutdown.store(true, Ordering::Relaxed);
        assert!(worker.buffer.is_empty());
    }

    #[tokio::test]
    async fn test_buffer_size_triggers_flush() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let server_shutdown = Arc::new(AtomicBool::new(false));
        let ssd = server_shutdown.clone();
        let messages: Vec<Message> = (0..1000)
            .map(|i| {
                Message::Text(format!(
                    r#"{{"event_type":"last_trade_price","asset_id":"token1","price":"0.5","size":"1","timestamp":{}}}"#,
                    i
                ))
            })
            .collect();
        tokio::spawn(async move { run_ws_server(tx, messages, ssd).await });
        let addr = rx.await.unwrap();
        let url = format!("ws://{}", addr);

        let dir = tempdir().unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(1500)).await;
            sd.store(true, Ordering::Relaxed);
        });

        let mut worker = OrderbookWorker::new(
            0,
            vec!["token1".to_string()],
            dir.path().to_path_buf(),
            None,
            Duration::from_secs(60),
            shutdown,
            None,
            None,
            None,
            None,
            None,
            Some(url),
        );
        tokio::time::timeout(Duration::from_secs(10), worker.connect_and_collect())
            .await
            .unwrap()
            .unwrap();
        server_shutdown.store(true, Ordering::Relaxed);
        assert!(worker.buffer.is_empty());
    }

    async fn run_minimal_aggregator(
        addr_tx: tokio::sync::oneshot::Sender<std::net::SocketAddr>,
        shutdown: Arc<AtomicBool>,
    ) {
        use axum::{
            routing::{get, post},
            Json, Router,
        };
        let app = Router::new()
            .route(
                "/register",
                post(|| async { Json(serde_json::json!({ "collector_id": "test-cid" })) }),
            )
            .route(
                "/assignment/:collector_id",
                get(|| async {
                    Json(serde_json::json!({
                        "token_ids": ["token1"],
                        "chunk_size": 1,
                    }))
                }),
            )
            .route("/heartbeat/:collector_id", post(|| async { "ok" }))
            .route(
                "/notify",
                post(|| async { Json(serde_json::json!({ "received": true })) }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        addr_tx.send(listener.local_addr().unwrap()).unwrap();
        let shutdown_future = async move {
            while !shutdown.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_future)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_collector_run_orchestrated_without_s3() {
        let (ws_tx, ws_rx) = tokio::sync::oneshot::channel();
        let ws_shutdown = Arc::new(AtomicBool::new(false));
        let ws_shutdown_clone = ws_shutdown.clone();
        tokio::spawn(async move {
            run_ws_server(ws_tx, vec![Message::Text("PONG".to_string())], ws_shutdown_clone).await;
        });
        let ws_addr = ws_rx.await.unwrap();
        let ws_url = format!("ws://{}", ws_addr);

        let (agg_tx, agg_rx) = tokio::sync::oneshot::channel();
        let agg_shutdown = Arc::new(AtomicBool::new(false));
        let agg_shutdown_clone = agg_shutdown.clone();
        tokio::spawn(async move {
            run_minimal_aggregator(agg_tx, agg_shutdown_clone).await;
        });
        let agg_addr = agg_rx.await.unwrap();
        let aggregator_url = format!("http://{}", agg_addr);

        let dir = tempdir().unwrap();
        let collector = OrderbookCollector::new(
            vec![],
            dir.path().to_path_buf(),
            None,
            100,
            Duration::from_secs(60),
            Some(1),
        )
        .with_aggregator_url(aggregator_url)
        .with_ws_url(ws_url);

        tokio::time::timeout(Duration::from_secs(10), collector.run())
            .await
            .unwrap()
            .unwrap();
        ws_shutdown.store(true, Ordering::Relaxed);
        agg_shutdown.store(true, Ordering::Relaxed);
    }

    #[tokio::test]
    async fn test_collector_run_orchestrated_with_s3_upload_path() {
        let (ws_tx, ws_rx) = tokio::sync::oneshot::channel();
        let ws_shutdown = Arc::new(AtomicBool::new(false));
        let ws_shutdown_clone = ws_shutdown.clone();
        tokio::spawn(async move {
            run_ws_server(ws_tx, vec![Message::Text("PONG".to_string())], ws_shutdown_clone).await;
        });
        let ws_addr = ws_rx.await.unwrap();
        let ws_url = format!("ws://{}", ws_addr);

        let (agg_tx, agg_rx) = tokio::sync::oneshot::channel();
        let agg_shutdown = Arc::new(AtomicBool::new(false));
        let agg_shutdown_clone = agg_shutdown.clone();
        tokio::spawn(async move {
            run_minimal_aggregator(agg_tx, agg_shutdown_clone).await;
        });
        let agg_addr = agg_rx.await.unwrap();
        let aggregator_url = format!("http://{}", agg_addr);

        let dir = tempdir().unwrap();
        let collector = OrderbookCollector::new(
            vec![],
            dir.path().to_path_buf(),
            None,
            100,
            Duration::from_secs(60),
            Some(1),
        )
        .with_aggregator_url(aggregator_url)
        .with_s3_upload("bucket".to_string(), "orderbook/".to_string(), "us-east-1".to_string())
        .with_ws_url(ws_url);

        tokio::time::timeout(Duration::from_secs(10), collector.run())
            .await
            .unwrap()
            .unwrap();
        ws_shutdown.store(true, Ordering::Relaxed);
        agg_shutdown.store(true, Ordering::Relaxed);
    }

    #[tokio::test]
    async fn test_collector_run_static_with_sigint_shutdown() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let server_shutdown = Arc::new(AtomicBool::new(false));
        let ssd = server_shutdown.clone();
        tokio::spawn(async move {
            run_ws_server(tx, vec![Message::Text("PONG".to_string())], ssd).await;
        });
        let addr = rx.await.unwrap();
        let url = format!("ws://{}", addr);

        let dir = tempdir().unwrap();
        let collector = OrderbookCollector::new(
            vec!["token1".to_string()],
            dir.path().to_path_buf(),
            None,
            100,
            Duration::from_secs(60),
            None,
        )
        .with_ws_url(url);

        let pid = std::process::id() as libc::pid_t;
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(1200)).await;
            unsafe {
                let _ = libc::kill(pid, libc::SIGINT);
            }
        });

        tokio::time::timeout(Duration::from_secs(10), collector.run())
            .await
            .unwrap()
            .unwrap();
        server_shutdown.store(true, Ordering::Relaxed);
    }

    #[tokio::test]
    async fn test_collector_run_with_duration_and_sigint() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let server_shutdown = Arc::new(AtomicBool::new(false));
        let ssd = server_shutdown.clone();
        tokio::spawn(async move {
            run_ws_server(tx, vec![Message::Text("PONG".to_string())], ssd).await;
        });
        let addr = rx.await.unwrap();
        let url = format!("ws://{}", addr);

        let dir = tempdir().unwrap();
        let collector = OrderbookCollector::new(
            vec!["token1".to_string()],
            dir.path().to_path_buf(),
            None,
            100,
            Duration::from_secs(60),
            Some(10),
        )
        .with_ws_url(url);

        let pid = std::process::id() as libc::pid_t;
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(800)).await;
            unsafe {
                let _ = libc::kill(pid, libc::SIGINT);
            }
        });

        tokio::time::timeout(Duration::from_secs(10), collector.run())
            .await
            .unwrap()
            .unwrap();
        server_shutdown.store(true, Ordering::Relaxed);
    }
}
