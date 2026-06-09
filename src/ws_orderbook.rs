use anyhow::Result;
use chrono::Utc;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::PathBuf;
use std::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{info, warn};

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
}

impl OrderbookCollector {
    pub fn new(
        token_ids: Vec<String>,
        output_dir: PathBuf,
        relay_url: Option<String>,
        chunk_size: usize,
        rotate_interval: Duration,
    ) -> Self {
        Self {
            token_ids,
            output_dir,
            chunk_size,
            relay_url,
            rotate_interval,
        }
    }

    pub async fn run(self) -> Result<()> {
        let token_chunks: Vec<Vec<String>> = self
            .token_ids
            .chunks(self.chunk_size)
            .map(|c| c.to_vec())
            .collect();

        info!(
            total_tokens = self.token_ids.len(),
            chunks = token_chunks.len(),
            chunk_size = self.chunk_size,
            rotate_secs = self.rotate_interval.as_secs(),
            relay_url = ?self.relay_url,
            "Starting parallel WebSocket collectors"
        );

        let mut handles = Vec::new();
        for (id, chunk) in token_chunks.into_iter().enumerate() {
            let output_dir = self.output_dir.clone();
            let relay_url = self.relay_url.clone();
            let rotate_interval = self.rotate_interval;
            let handle = tokio::spawn(async move {
                let mut worker = OrderbookWorker::new(id, chunk, output_dir, relay_url, rotate_interval);
                if let Err(e) = worker.run().await {
                    warn!(worker_id = id, error = %e, "Worker failed");
                }
            });
            handles.push(handle);
            // Stagger connections to avoid IP-based rate limiting.
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        // Graceful shutdown on Ctrl+C
        tokio::signal::ctrl_c().await.ok();
        info!("Shutdown signal received, waiting for workers to flush...");

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
}

impl OrderbookWorker {
    fn new(
        id: usize,
        token_ids: Vec<String>,
        output_dir: PathBuf,
        relay_url: Option<String>,
        rotate_interval: Duration,
    ) -> Self {
        Self {
            id,
            token_ids,
            output_dir,
            relay_url,
            buffer: Vec::with_capacity(10_000),
            flush_interval: Duration::from_secs(60),
            buffer_size: 10_000,
            http_client: reqwest::Client::new(),
            writer: None,
            rotate_interval,
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
        }
        Ok(())
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
        let (ws_stream, _) =
            connect_async("wss://ws-subscriptions-clob.polymarket.com/ws/market").await?;
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

        loop {
            tokio::select! {
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

    fn handle_message(&mut self, text: &str) -> Result<()> {
        let received_at = Utc::now().timestamp_millis();
        let msg: serde_json::Value = serde_json::from_str(text)?;

        let msg_type = msg.get("event_type").and_then(|v| v.as_str()).unwrap_or("");
        let asset = msg
            .get("asset_id")
            .or_else(|| msg.get("token_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

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
                                price: level.get("price").and_then(|v| v.as_f64()),
                                size: level.get("size").and_then(|v| v.as_f64()),
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
                        self.buffer.push(OrderbookEvent {
                            event_type: "price_change".to_string(),
                            asset: asset.clone(),
                            side: change
                                .get("side")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string()),
                            price: change.get("price").and_then(|v| v.as_f64()),
                            size: change.get("size").and_then(|v| v.as_f64()),
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
                        price: msg.get("price").and_then(|v| v.as_f64()),
                        size: msg.get("size").and_then(|v| v.as_f64()),
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
            "last_trade_price" => {
                self.buffer.push(OrderbookEvent {
                    event_type: "last_trade".to_string(),
                    asset: asset.clone(),
                    side: None,
                    price: msg.get("price").and_then(|v| v.as_f64()),
                    size: msg.get("size").and_then(|v| v.as_f64()),
                    timestamp: msg
                        .get("timestamp")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(received_at),
                    received_at,
                    raw: text.to_string(),
                    worker_id: self.id,
                });
            }
            _ => {}
        }

        Ok(())
    }
}
