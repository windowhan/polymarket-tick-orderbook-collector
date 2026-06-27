use anyhow::{Context, Result};
use chrono::Utc;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{info, warn};

use std::path::Path;

use crate::aggregate_s3::{AwsS3Service, S3Service};
use crate::dynamic_markets::{
    LiveMarketSource, MarketRefreshPolicy, MarketRegistry, RefreshWarning,
};
use crate::orchestration::OrchestrationClient;
use crate::storage::RotatedWriter;

/// Normalized orderbook event persisted by WebSocket workers.
///
/// # Detailed Description
/// Workers convert heterogeneous Polymarket market-channel messages into this
/// stable event shape before buffering and rotating JSONL output. `raw`
/// preserves the original message for later debugging, while parsed fields make
/// viewer and aggregation code avoid reparsing common top-level values.
///
/// # Arguments
/// Values are produced by [`OrderbookWorker::handle_message`] rather than by a
/// constructor.
///
/// # Returns
/// A serializable orderbook event record.
///
/// # Example — Input / Output
/// ```rust,ignore
/// let event = worker.handle_message(r#"{"event_type":"book","asset_id":"token-a"}"#)?;
/// assert_eq!(event.asset, "token-a");
/// ```
///
/// # Related
/// - [`OrderbookWorker::handle_message`]
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

/// Default interval for polling aggregator assignment changes, in seconds.
pub const DEFAULT_ASSIGNMENT_POLL_INTERVAL_SECS: u64 = 60;

/// Subscription diff command sent from the collector manager to one worker.
///
/// # Detailed Description
/// `subscribe` and `unsubscribe` contain only the incremental token changes for
/// the currently connected WebSocket. `desired_full_set` is the authoritative
/// full token set for that worker and is retained so reconnect fallback can
/// resubscribe the correct complete set after a failed in-place update.
///
/// # Arguments
/// Values are created by [`plan_subscription_command`] and sent over a Tokio
/// channel to [`OrderbookWorker`].
///
/// # Returns
/// A deterministic command with sorted, deduplicated token IDs.
///
/// # Example — Input / Output
/// ```rust,ignore
/// let command = plan_subscription_command(&["a".into()], vec!["b".into()]);
/// assert_eq!(command.unwrap().subscribe, vec!["b".to_string()]);
/// assert_eq!(command.unwrap().unsubscribe, vec!["a".to_string()]);
/// ```
///
/// # Related
/// - [`market_subscription_payload`]
#[derive(Debug, Clone, PartialEq, Eq)]
struct SubscriptionCommand {
    /// Token IDs to add to this worker's WebSocket subscription.
    subscribe: Vec<String>,
    /// Token IDs to remove from this worker's WebSocket subscription.
    unsubscribe: Vec<String>,
    /// Complete desired token set used for reconnect fallback.
    desired_full_set: Vec<String>,
}

/// Runtime configuration needed to spawn or restart one WebSocket worker.
///
/// # Detailed Description
/// The collector manager owns assignment refresh and chunk reconciliation, but
/// spawning a worker requires repeated boilerplate: output paths, rotation
/// timing, shutdown flag, optional S3 upload settings, orchestration metadata,
/// and test WebSocket URL overrides. This struct keeps those stable fields
/// together so dynamic subscription updates can restart only the affected worker
/// when a control-channel send fails.
///
/// # Arguments
/// Constructed inside [`OrderbookCollector::run`] from the collector settings.
///
/// # Returns
/// A cloneable worker-spawn template.
///
/// # Example — Input / Output
/// ```rust,ignore
/// let (handle, tx) = spawn_config.spawn_worker(0, vec!["token-a".to_string()]);
/// tx.send(command).await?;
/// ```
///
/// # Related
/// - [`apply_token_update_to_workers`]
#[derive(Clone)]
struct WorkerSpawnConfig {
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
}

impl WorkerSpawnConfig {
    /// Spawn a WebSocket worker and its subscription-control channel.
    ///
    /// # Detailed Description
    /// Each worker receives a bounded Tokio channel for low-frequency
    /// subscribe/unsubscribe diffs. The bound is deliberately small because
    /// assignment and market refreshes happen on minute-scale intervals; if the
    /// channel is closed or congested enough to fail, the manager treats that as
    /// a worker-local restart signal.
    ///
    /// # Arguments
    /// * `id` — Stable worker index used in logs and output filenames.
    /// * `token_ids` — Initial sorted token chunk for the worker subscription.
    ///
    /// # Returns
    /// The worker task handle and sender side of its subscription-control
    /// channel.
    ///
    /// # Example — Input / Output
    /// ```rust,ignore
    /// let (handle, control_tx) = config.spawn_worker(1, vec!["token-b".to_string()]);
    /// assert!(!handle.is_finished());
    /// ```
    ///
    /// # Related
    /// - [`SubscriptionCommand`]
    fn spawn_worker(
        &self,
        id: usize,
        token_ids: Vec<String>,
    ) -> (
        tokio::task::JoinHandle<()>,
        mpsc::Sender<SubscriptionCommand>,
    ) {
        let (control_tx, control_rx) = mpsc::channel(16); // Small bounded queue for infrequent assignment diffs.
        let mut worker = OrderbookWorker::new(
            id,
            token_ids,
            self.output_dir.clone(),
            self.relay_url.clone(),
            self.rotate_interval,
            self.shutdown.clone(),
            self.s3_service.clone(),
            self.s3_bucket.clone(),
            self.s3_prefix.clone(),
            self.collector_id.clone(),
            self.orchestration_client.clone(),
            self.ws_url.clone(),
        )
        .with_control_rx(control_rx);

        let handle = tokio::spawn(async move {
            if let Err(e) = worker.run().await {
                warn!(worker_id = id, error = %e, "Worker failed");
            }
        });

        (handle, control_tx)
    }
}

/// Normalize token IDs into a sorted, deduplicated vector.
///
/// # Detailed Description
/// Assignment responses and Gamma market snapshots can contain duplicate tokens
/// or accidental whitespace. Sorting with a [`BTreeSet`] makes downstream chunk
/// planning deterministic, which keeps collector tests and subscribe/unsubscribe
/// diffs stable across refreshes.
///
/// # Arguments
/// * `tokens` — Raw token strings from a static file, assignment response, or
///   live-market snapshot.
///
/// # Returns
/// Trimmed non-empty token IDs sorted lexicographically and deduplicated.
///
/// # Example — Input / Output
/// ```rust,ignore
/// assert_eq!(normalized_tokens(vec![" b ".into(), "a".into(), "a".into()]), vec!["a", "b"]);
/// ```
///
/// # Related
/// - [`chunk_tokens`]
fn normalized_tokens(tokens: impl IntoIterator<Item = String>) -> Vec<String> {
    tokens
        .into_iter()
        .map(|token| token.trim().to_string())
        .filter(|token| !token.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// Split a deterministic token list into non-empty worker chunks.
///
/// # Detailed Description
/// WebSocket workers subscribe to a bounded number of token IDs per connection.
/// This helper treats a zero `chunk_size` as one to avoid panics from accidental
/// invalid CLI/test input and to preserve forward progress.
///
/// # Arguments
/// * `tokens` — Sorted token IDs to assign to workers.
/// * `chunk_size` — Desired maximum tokens per worker; values below one become
///   one.
///
/// # Returns
/// Token chunks in input order.
///
/// # Example — Input / Output
/// ```rust,ignore
/// assert_eq!(chunk_tokens(&vec!["a".into(), "b".into()], 1).len(), 2);
/// ```
///
/// # Related
/// - [`apply_token_update_to_workers`]
fn chunk_tokens(tokens: &[String], chunk_size: usize) -> Vec<Vec<String>> {
    let size = chunk_size.max(1);
    tokens.chunks(size).map(|chunk| chunk.to_vec()).collect()
}

/// Flatten worker chunks back into a normalized token set.
///
/// # Detailed Description
/// Managers keep per-worker chunk state but sometimes need the aggregate desired
/// token set for logs, warning decisions, or comparison with assignment
/// snapshots. This helper preserves the same trim/sort/dedupe semantics as
/// [`normalized_tokens`].
///
/// # Arguments
/// * `chunks` — Per-worker token chunks.
///
/// # Returns
/// Stable deduplicated token IDs across all chunks.
///
/// # Example — Input / Output
/// ```rust,ignore
/// let tokens = flatten_chunks(&vec![vec!["b".into()], vec!["a".into()]]);
/// assert_eq!(tokens, vec!["a", "b"]);
/// ```
///
/// # Related
/// - [`normalized_tokens`]
fn flatten_chunks(chunks: &[Vec<String>]) -> Vec<String> {
    normalized_tokens(chunks.iter().flat_map(|chunk| chunk.iter().cloned()))
}

/// Build the minimal subscribe/unsubscribe command for one worker.
///
/// # Detailed Description
/// The manager owns desired chunk state and workers own connected socket state.
/// This function compares the worker's current chunk with its desired chunk and
/// emits only the delta operations. A `None` return means the chunk is already
/// correct and no WebSocket message should be sent.
///
/// # Arguments
/// * `current_tokens` — Tokens currently assigned to the worker.
/// * `desired_tokens` — Desired full token set for that worker after refresh.
///
/// # Returns
/// A [`SubscriptionCommand`] with deterministic sorted deltas, or `None` for no
/// change.
///
/// # Example — Input / Output
/// ```rust,ignore
/// let command = plan_subscription_command(&["a".to_string()], vec!["a".into(), "b".into()]).unwrap();
/// assert_eq!(command.subscribe, vec!["b".to_string()]);
/// assert!(command.unsubscribe.is_empty());
/// ```
///
/// # Related
/// - [`SubscriptionCommand`]
fn plan_subscription_command(
    current_tokens: &[String],
    desired_tokens: Vec<String>,
) -> Option<SubscriptionCommand> {
    let current = normalized_tokens(current_tokens.iter().cloned());
    let desired = normalized_tokens(desired_tokens);

    let current_set: BTreeSet<_> = current.iter().cloned().collect();
    let desired_set: BTreeSet<_> = desired.iter().cloned().collect();

    let subscribe: Vec<String> = desired_set.difference(&current_set).cloned().collect();
    let unsubscribe: Vec<String> = current_set.difference(&desired_set).cloned().collect();

    if subscribe.is_empty() && unsubscribe.is_empty() {
        return None;
    }

    Some(SubscriptionCommand {
        subscribe,
        unsubscribe,
        desired_full_set: desired,
    })
}

/// Serialize a Polymarket market-channel subscription payload.
///
/// # Detailed Description
/// The market WebSocket uses `type: "market"` with `assets_ids` for the initial
/// subscription. Runtime updates use the same shape plus an `operation` field of
/// `"subscribe"` or `"unsubscribe"`. The `custom_feature_enabled` flag preserves
/// the existing collector payload shape.
///
/// # Arguments
/// * `operation` — `None` for initial subscription, or a concrete update
///   operation string.
/// * `token_ids` — Token IDs for the initial subscription or incremental update.
///
/// # Returns
/// A compact JSON string ready to send over the WebSocket.
///
/// # Example — Input / Output
/// ```rust,ignore
/// let payload = market_subscription_payload(Some("subscribe"), &["token-a".to_string()]);
/// assert!(payload.contains("\"operation\":\"subscribe\""));
/// ```
///
/// # Related
/// - [`OrderbookWorker::connect_and_collect`]
fn market_subscription_payload(operation: Option<&str>, token_ids: &[String]) -> String {
    let mut payload = json!({
        "type": "market",
        "assets_ids": token_ids,
        "custom_feature_enabled": true,
    });

    if let Some(operation) = operation {
        payload["operation"] = json!(operation);
    }

    payload.to_string()
}

/// Await the next subscription command, or remain pending when control is disabled.
///
/// # Detailed Description
/// Static workers created before dynamic control support can run without a
/// control channel. Returning a pending future in that case lets one
/// `tokio::select!` branch cover both dynamic and non-dynamic workers without
/// busy-looping.
///
/// # Arguments
/// * `control_rx` — Optional receiver owned by the worker.
///
/// # Returns
/// The next [`SubscriptionCommand`], or `None` when the channel is closed.
///
/// # Example — Input / Output
/// ```rust,ignore
/// let next = recv_subscription_command(&mut Some(rx)).await;
/// assert!(next.is_some());
/// ```
///
/// # Related
/// - [`OrderbookWorker::connect_and_collect`]
async fn recv_subscription_command(
    control_rx: &mut Option<mpsc::Receiver<SubscriptionCommand>>,
) -> Option<SubscriptionCommand> {
    match control_rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

/// Reconcile a new global token set across live worker chunks.
///
/// # Detailed Description
/// The collector manager calls this after an assignment or market-registry
/// version change. Existing workers receive in-place subscription diffs, new
/// chunks spawn new workers, and obsolete chunks receive an empty desired set so
/// they unsubscribe from expired tokens. If a worker control send fails, only
/// that worker is aborted and restarted with its desired chunk.
///
/// # Arguments
/// * `desired_tokens` — New aggregate desired token set.
/// * `chunk_size` — Maximum tokens per worker chunk.
/// * `current_chunks` — Mutable manager view of current worker chunks.
/// * `control_txs` — Per-worker command senders.
/// * `handles` — Per-worker task handles used for restart fallback.
/// * `spawn_config` — Shared worker spawn settings.
///
/// # Returns
/// Nothing; worker/chunk state is updated in place.
///
/// # Example — Input / Output
/// ```rust,ignore
/// apply_token_update_to_workers(vec!["token-b".into()], 100, &mut chunks, &mut txs, &mut handles, &config).await;
/// assert_eq!(flatten_chunks(&chunks), vec!["token-b".to_string()]);
/// ```
///
/// # Related
/// - [`plan_subscription_command`]
async fn apply_token_update_to_workers(
    desired_tokens: Vec<String>,
    chunk_size: usize,
    current_chunks: &mut Vec<Vec<String>>,
    control_txs: &mut Vec<mpsc::Sender<SubscriptionCommand>>,
    handles: &mut Vec<tokio::task::JoinHandle<()>>,
    spawn_config: &WorkerSpawnConfig,
) {
    let desired_tokens = normalized_tokens(desired_tokens);
    let desired_chunks = chunk_tokens(&desired_tokens, chunk_size);

    for (id, desired_chunk) in desired_chunks.iter().cloned().enumerate() {
        if id >= control_txs.len() {
            let (handle, tx) = spawn_config.spawn_worker(id, desired_chunk.clone());
            handles.push(handle);
            control_txs.push(tx);
            current_chunks.push(desired_chunk);
            tokio::time::sleep(Duration::from_millis(500)).await; // Stagger new WebSocket connections.
            continue;
        }

        if let Some(command) = plan_subscription_command(&current_chunks[id], desired_chunk.clone())
        {
            if let Err(error) = control_txs[id].send(command).await {
                warn!(
                    worker_id = id,
                    error = %error,
                    "Subscription control send failed; restarting worker with desired chunk"
                );
                handles[id].abort();
                let (handle, tx) = spawn_config.spawn_worker(id, desired_chunk.clone());
                handles[id] = handle;
                control_txs[id] = tx;
            }
            current_chunks[id] = desired_chunk;
        }
    }

    for id in desired_chunks.len()..current_chunks.len() {
        let desired_chunk = Vec::new();
        if let Some(command) = plan_subscription_command(&current_chunks[id], desired_chunk.clone())
        {
            if let Err(error) = control_txs[id].send(command).await {
                warn!(
                    worker_id = id,
                    error = %error,
                    "Subscription control send failed while draining obsolete chunk"
                );
            }
            current_chunks[id] = desired_chunk;
        }
    }
}

/// Restart worker tasks that exited outside the manager's shutdown path.
///
/// # Detailed Description
/// WebSocket servers can close a connection cleanly, which makes the worker task
/// finish without sending an error through the subscription-control channel.
/// The collector manager calls this small supervision pass on a periodic tick so
/// a finished worker with a non-empty desired chunk is respawned even when no
/// market/assignment update arrives. Empty obsolete chunks are intentionally not
/// restarted because they represent drained subscriptions.
///
/// # Arguments
/// * `current_chunks` — Manager-owned desired token chunks for each worker.
/// * `control_txs` — Per-worker command senders to replace when a task restarts.
/// * `handles` — Worker task handles inspected for completion.
/// * `spawn_config` — Shared worker spawn settings.
///
/// # Returns
/// Nothing; finished worker handles and senders are replaced in place.
///
/// # Example — Input / Output
/// ```rust,ignore
/// restart_finished_workers(&chunks, &mut control_txs, &mut handles, &spawn_config).await;
/// assert!(!handles[0].is_finished());
/// ```
///
/// # Related
/// - [`WorkerSpawnConfig::spawn_worker`]
/// - [`apply_token_update_to_workers`]
async fn restart_finished_workers(
    current_chunks: &[Vec<String>],
    control_txs: &mut [mpsc::Sender<SubscriptionCommand>],
    handles: &mut [tokio::task::JoinHandle<()>],
    spawn_config: &WorkerSpawnConfig,
) {
    let managed_workers = current_chunks
        .len()
        .min(control_txs.len())
        .min(handles.len());
    for id in 0..managed_workers {
        if current_chunks[id].is_empty() || !handles[id].is_finished() {
            continue;
        }

        let desired_chunk = current_chunks[id].clone();
        let (new_handle, new_tx) = spawn_config.spawn_worker(id, desired_chunk.clone());
        let old_handle = std::mem::replace(&mut handles[id], new_handle);
        control_txs[id] = new_tx;

        match old_handle.await {
            Ok(()) => {
                warn!(
                    worker_id = id,
                    tokens = desired_chunk.len(),
                    "Worker task exited; restarted with current desired chunk"
                );
            }
            Err(error) => {
                warn!(
                    worker_id = id,
                    tokens = desired_chunk.len(),
                    error = %error,
                    "Worker task join failed; restarted with current desired chunk"
                );
            }
        }

        tokio::time::sleep(Duration::from_millis(500)).await; // Stagger restarted WebSocket connections.
    }
}

/// Sleep before a reconnect attempt while polling for collector shutdown.
///
/// Worker reconnect backoff can be as long as 60 seconds, but collector shutdown
/// should not wait for that whole sleep. This helper bounds shutdown latency by
/// polling the shared atomic flag every 100 milliseconds while preserving the
/// requested reconnect delay when the collector is still running.
async fn sleep_until_reconnect_or_shutdown(shutdown: &AtomicBool, delay: Duration) {
    let deadline = tokio::time::Instant::now() + delay;
    let mut shutdown_poll = tokio::time::interval(Duration::from_millis(100)); // 100 ms max shutdown polling interval.

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => break,
            _ = shutdown_poll.tick() => {}
        }
    }
}

/// Spawns and manages parallel WebSocket workers for orderbook tokens.
///
/// # Detailed Description
/// `OrderbookCollector` owns the high-level collection mode: static token list,
/// orchestrated aggregator assignment, or standalone API-first live markets. It
/// chunks tokens across workers, starts optional S3 upload plumbing, and applies
/// runtime subscription diffs without reconnecting unless a worker-local failure
/// requires fallback.
///
/// # Arguments
/// Constructed with [`OrderbookCollector::new`] and optional builder methods.
///
/// # Returns
/// A collector that runs until duration, Ctrl+C, or shutdown.
///
/// # Example — Input / Output
/// ```rust,ignore
/// let collector = OrderbookCollector::new(
///     vec!["token-a".to_string()],
///     "data/orderbook".into(),
///     None,
///     100,
///     Duration::from_secs(300),
///     Some(60),
/// );
/// collector.run().await?;
/// ```
///
/// # Related
/// - [`OrderbookWorker`]
/// - [`MarketRegistry`]
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
    assignment_poll_interval: Duration,
    market_source: Option<Arc<dyn LiveMarketSource>>,
    market_refresh_policy: MarketRefreshPolicy,
}

impl OrderbookCollector {
    /// Create a collector from an initial token list and local output settings.
    ///
    /// # Detailed Description
    /// The constructor is mode-neutral. Passing non-empty `token_ids` and no
    /// live/aggregator builder produces explicit static collection. Calling
    /// [`with_aggregator_url`](Self::with_aggregator_url) switches to
    /// orchestrated assignment mode and ignores the initial token list. Calling
    /// [`with_market_source`](Self::with_market_source) switches standalone mode
    /// to API-first live market discovery.
    ///
    /// # Arguments
    /// * `token_ids` — Static token IDs, or an empty vector for dynamic modes.
    /// * `output_dir` — Local directory for rotated JSONL files.
    /// * `relay_url` — Legacy relay URL slot retained for compatibility; new
    ///   aggregation uses S3 upload and `/notify`.
    /// * `chunk_size` — Maximum token IDs per WebSocket worker.
    /// * `rotate_interval` — Duration for local file rotation windows.
    /// * `duration_secs` — Optional run duration for tests and bounded jobs.
    ///
    /// # Returns
    /// A collector configured with default assignment polling and market refresh
    /// policies.
    ///
    /// # Example — Input / Output
    /// ```rust,ignore
    /// let collector = OrderbookCollector::new(tokens, output_dir, None, 100, Duration::from_secs(300), None);
    /// assert!(collector.run().await.is_ok());
    /// ```
    ///
    /// # Related
    /// - [`OrderbookCollector::with_aggregator_url`]
    /// - [`OrderbookCollector::with_market_source`]
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
            assignment_poll_interval: Duration::from_secs(DEFAULT_ASSIGNMENT_POLL_INTERVAL_SECS),
            market_source: None,
            market_refresh_policy: MarketRefreshPolicy::default(),
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

    /// Set how often orchestrated collectors poll assignment changes.
    ///
    /// # Detailed Description
    /// The default is [`DEFAULT_ASSIGNMENT_POLL_INTERVAL_SECS`] (60 seconds).
    /// Tests can lower this interval with paused Tokio time while production
    /// keeps low control-plane load on the aggregator.
    ///
    /// # Arguments
    /// * `interval` — Polling interval for `/assignment/{collector_id}`.
    ///
    /// # Returns
    /// The collector with an updated assignment polling interval.
    ///
    /// # Example — Input / Output
    /// ```rust,ignore
    /// let collector = collector.with_assignment_poll_interval(Duration::from_secs(5));
    /// ```
    ///
    /// # Related
    /// - [`OrchestrationClient::fetch_assignment_snapshot`]
    pub fn with_assignment_poll_interval(mut self, interval: Duration) -> Self {
        self.assignment_poll_interval = interval;
        self
    }

    /// Enable standalone API-first live market refresh.
    ///
    /// # Detailed Description
    /// When no aggregator URL is configured, this source makes the collector
    /// fetch Gamma markets at startup and then refresh them periodically using
    /// the same market lifecycle policy as the aggregator. This is the live
    /// replacement for implicit static `markets.jsonl` loading; explicit static
    /// callers can continue passing initial `token_ids` and omitting this source.
    ///
    /// # Arguments
    /// * `source` — Live market source used for startup and periodic refreshes.
    /// * `policy` — Refresh interval and stale-market retention policy.
    ///
    /// # Returns
    /// The collector configured for standalone dynamic live markets.
    ///
    /// # Example — Input / Output
    /// ```rust,ignore
    /// let source = Arc::new(GammaMarketSource::new(client));
    /// let collector = collector.with_market_source(source, MarketRefreshPolicy::default());
    /// ```
    ///
    /// # Related
    /// - [`MarketRegistry`]
    pub fn with_market_source(
        mut self,
        source: Arc<dyn LiveMarketSource>,
        policy: MarketRefreshPolicy,
    ) -> Self {
        self.market_source = Some(source);
        self.market_refresh_policy = policy;
        self
    }

    /// Run the collector until shutdown, duration expiry, or unrecoverable startup failure.
    ///
    /// # Detailed Description
    /// Startup is fail-fast for dynamic live inputs: orchestrated collectors must
    /// register and receive a non-empty first assignment; standalone live
    /// collectors must fetch a non-empty usable market set. After startup,
    /// assignment/market refresh errors are fail-open and keep current
    /// subscriptions. Worker updates are sent as in-place subscribe/unsubscribe
    /// messages with reconnect fallback for worker-local control failures and
    /// periodic supervision for finished worker tasks.
    ///
    /// # Arguments
    /// This method consumes the configured collector.
    ///
    /// # Returns
    /// `Ok(())` after graceful shutdown, or an error when startup cannot obtain
    /// an initial live/static token set.
    ///
    /// # Example — Input / Output
    /// ```rust,ignore
    /// collector.run().await?;
    /// ```
    ///
    /// # Related
    /// - [`apply_token_update_to_workers`]
    /// - [`MarketRegistry`]
    pub async fn run(self) -> Result<()> {
        let mut heartbeat_handle: Option<tokio::task::JoinHandle<()>> = None;
        let mut orchestrated_client: Option<OrchestrationClient> = None;
        let mut orchestrated_assignment_version: u64 = 0;
        let mut standalone_registry: Option<MarketRegistry> = None;
        let mut token_ids = normalized_tokens(self.token_ids.clone());
        let mut effective_chunk_size = self.chunk_size.max(1);

        // ── Orchestrated mode: register with aggregator and fetch assignment ──
        let (collector_id, s3_service, s3_bucket, s3_prefix) =
            if let Some(aggregator_url) = &self.aggregator_url {
                info!(url = %aggregator_url, "Entering orchestrated collector mode");
                let client = OrchestrationClient::register(aggregator_url.clone(), None)
                    .await
                    .context("Failed to register with aggregator")?;
                let cid = client.collector_id().to_string();

                let assignment = client
                    .fetch_assignment_snapshot()
                    .await
                    .context("Failed to fetch assignment from aggregator")?;
                if assignment.token_ids.is_empty() {
                    anyhow::bail!("Initial assignment from aggregator contained no token IDs");
                }

                token_ids = normalized_tokens(assignment.token_ids);
                effective_chunk_size = assignment.chunk_size.max(1);
                orchestrated_assignment_version = assignment.version;

                info!(
                    collector_id = %cid,
                    assigned_tokens = token_ids.len(),
                    chunk_size = effective_chunk_size,
                    version = orchestrated_assignment_version,
                    "Received assignment from aggregator"
                );

                // Start heartbeat task: every 10 seconds.
                heartbeat_handle = Some(client.spawn_heartbeat_task(Duration::from_secs(10)));

                let s3_service: Option<Arc<dyn S3Service>> = if self.s3_bucket.is_some() {
                    Some(Arc::new(AwsS3Service::new(&self.aws_region).await))
                } else {
                    None
                };

                orchestrated_client = Some(client.clone());

                (
                    Some(cid),
                    s3_service,
                    self.s3_bucket.clone(),
                    self.s3_prefix.clone(),
                )
            } else if let Some(source) = &self.market_source {
                let markets = source
                    .fetch_usable_markets()
                    .await
                    .context("initial standalone live market fetch failed")?;
                let registry = MarketRegistry::from_initial_markets(
                    Instant::now(),
                    self.market_refresh_policy,
                    markets,
                )?;
                let snapshot = registry.snapshot();
                token_ids = snapshot.token_ids;
                standalone_registry = Some(registry);

                info!(
                    tokens = token_ids.len(),
                    version = snapshot.version,
                    refresh_interval_secs = self.market_refresh_policy.refresh_interval.as_secs(),
                    stale_ttl_secs = self.market_refresh_policy.stale_ttl.as_secs(),
                    "Received standalone live market snapshot"
                );

                (None, None, self.s3_bucket.clone(), self.s3_prefix.clone())
            } else {
                (None, None, self.s3_bucket.clone(), self.s3_prefix.clone())
            };

        let token_chunks = chunk_tokens(&token_ids, effective_chunk_size);

        info!(
            total_tokens = token_ids.len(),
            chunks = token_chunks.len(),
            chunk_size = effective_chunk_size,
            rotate_secs = self.rotate_interval.as_secs(),
            duration_secs = ?self.duration_secs,
            relay_url = ?self.relay_url,
            orchestrated = self.aggregator_url.is_some(),
            standalone_live = self.market_source.is_some() && self.aggregator_url.is_none(),
            s3_upload = s3_bucket.is_some(),
            "Starting parallel WebSocket collectors"
        );

        let spawn_config = WorkerSpawnConfig {
            output_dir: self.output_dir.clone(),
            relay_url: self.relay_url.clone(),
            rotate_interval: self.rotate_interval,
            shutdown: self.shutdown.clone(),
            s3_service,
            s3_bucket,
            s3_prefix,
            collector_id,
            orchestration_client: orchestrated_client.clone(),
            ws_url: self.ws_url.clone(),
        };

        let mut current_chunks = Vec::new();
        let mut handles = Vec::new();
        let mut control_txs = Vec::new();
        for (id, chunk) in token_chunks.into_iter().enumerate() {
            let (handle, tx) = spawn_config.spawn_worker(id, chunk.clone());
            handles.push(handle);
            control_txs.push(tx);
            current_chunks.push(chunk);
            tokio::time::sleep(Duration::from_millis(500)).await; // Stagger initial WebSocket connections.
        }

        let mut assignment_tick = tokio::time::interval(self.assignment_poll_interval);
        assignment_tick.tick().await; // Startup already fetched assignment; skip immediate poll.
        let mut market_tick = tokio::time::interval(self.market_refresh_policy.refresh_interval);
        market_tick.tick().await; // Startup already fetched live markets; skip immediate refresh.
        let mut worker_supervision_tick = tokio::time::interval(Duration::from_secs(1)); // Check worker exits once per second.
        worker_supervision_tick.tick().await; // Initial workers were just spawned; skip immediate supervision.
        let mut duration_sleep = self
            .duration_secs
            .map(|duration| Box::pin(tokio::time::sleep(Duration::from_secs(duration))));

        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("Ctrl+C received, shutting down...");
                    break;
                }
                _ = async {
                    match &mut duration_sleep {
                        Some(sleep) => sleep.as_mut().await,
                        None => std::future::pending().await,
                    }
                } => {
                    info!(duration_secs = ?self.duration_secs, "Duration reached, shutting down...");
                    break;
                }
                _ = assignment_tick.tick(), if orchestrated_client.is_some() => {
                    let client = orchestrated_client.as_ref().expect("orchestrated client present when assignment tick is enabled");
                    match client.fetch_assignment_snapshot().await {
                        Ok(assignment) => {
                            let desired_tokens = normalized_tokens(assignment.token_ids);
                            let current_tokens = flatten_chunks(&current_chunks);
                            let version_changed = assignment.version != orchestrated_assignment_version;
                            let token_changed = desired_tokens != current_tokens;

                            if desired_tokens.is_empty() && !version_changed {
                                warn!(
                                    version = assignment.version,
                                    "Ignoring empty assignment with unchanged version"
                                );
                                continue;
                            }

                            if version_changed || token_changed {
                                effective_chunk_size = assignment.chunk_size.max(1);
                                apply_token_update_to_workers(
                                    desired_tokens,
                                    effective_chunk_size,
                                    &mut current_chunks,
                                    &mut control_txs,
                                    &mut handles,
                                    &spawn_config,
                                ).await;
                                orchestrated_assignment_version = assignment.version;
                            }
                        }
                        Err(error) => {
                            warn!(error = %error, "Assignment refresh failed; keeping current subscriptions");
                        }
                    }
                }
                _ = market_tick.tick(), if standalone_registry.is_some() => {
                    let source = self.market_source.as_ref().expect("standalone source present when market tick is enabled");
                    let registry = standalone_registry.as_mut().expect("standalone registry present when market tick is enabled");
                    let outcome = registry.apply_refresh_result(
                        Instant::now(),
                        source.fetch_usable_markets().await,
                    );

                    if let Some(warning) = &outcome.warning {
                        match warning {
                            RefreshWarning::FetchFailed(error) => {
                                warn!(error = %error, version = outcome.version, tokens = outcome.token_count, "Standalone market refresh failed; keeping current subscriptions");
                            }
                            RefreshWarning::EmptyUsableRefresh => {
                                warn!(version = outcome.version, tokens = outcome.token_count, "Standalone market refresh returned no usable markets; keeping current subscriptions");
                            }
                        }
                        continue;
                    }

                    if outcome.changed {
                        let snapshot = registry.snapshot();
                        apply_token_update_to_workers(
                            snapshot.token_ids,
                            self.chunk_size,
                            &mut current_chunks,
                            &mut control_txs,
                            &mut handles,
                            &spawn_config,
                        ).await;
                    }
                }
                _ = worker_supervision_tick.tick() => {
                    restart_finished_workers(
                        &current_chunks,
                        &mut control_txs,
                        &mut handles,
                        &spawn_config,
                    ).await;
                }
            }
        }

        self.shutdown.store(true, Ordering::Relaxed);

        for h in handles {
            let _ = h.await;
        }
        if let Some(handle) = heartbeat_handle {
            handle.abort();
            let _ = handle.await;
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
    control_rx: Option<mpsc::Receiver<SubscriptionCommand>>,
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
            control_rx: None,
        }
    }

    fn with_control_rx(mut self, control_rx: mpsc::Receiver<SubscriptionCommand>) -> Self {
        self.control_rx = Some(control_rx);
        self
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
                    sleep_until_reconnect_or_shutdown(&self.shutdown, reconnect_delay).await;
                    if self.shutdown.load(Ordering::Relaxed) {
                        break;
                    }
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

        let payload = market_subscription_payload(None, &self.token_ids);
        write.send(Message::Text(payload)).await?;
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
                info!(
                    worker_id = self.id,
                    "Shutdown requested, exiting event loop"
                );
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
                command = recv_subscription_command(&mut self.control_rx) => {
                    let Some(command) = command else {
                        anyhow::bail!("subscription control channel closed");
                    };
                    if !command.subscribe.is_empty() {
                        let payload = market_subscription_payload(Some("subscribe"), &command.subscribe);
                        if let Err(error) = write.send(Message::Text(payload)).await {
                            self.token_ids = command.desired_full_set;
                            return Err(error.into());
                        }
                    }
                    if !command.unsubscribe.is_empty() {
                        let payload = market_subscription_payload(Some("unsubscribe"), &command.unsubscribe);
                        if let Err(error) = write.send(Message::Text(payload)).await {
                            self.token_ids = command.desired_full_set;
                            return Err(error.into());
                        }
                    }
                    info!(
                        worker_id = self.id,
                        subscribed = command.subscribe.len(),
                        unsubscribed = command.unsubscribe.len(),
                        desired = command.desired_full_set.len(),
                        "Applied subscription update"
                    );
                    self.token_ids = command.desired_full_set;
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
            v.and_then(|v| {
                v.as_f64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })
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
                        side: msg
                            .get("side")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
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
                    side: msg
                        .get("side")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
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

    s3.put_object(bucket, &key, body).await.with_context(|| {
        format!(
            "Failed to upload {} to s3://{}/{}",
            local_path.display(),
            bucket,
            key
        )
    })?;

    if let Some(client) = client {
        client.notify_s3(bucket, &key).await.with_context(|| {
            format!("Failed to notify aggregator about s3://{}/{}", bucket, key)
        })?;
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
    use crate::market_discovery::Market;
    use async_trait::async_trait;
    use std::collections::VecDeque;
    use std::sync::Mutex as StdMutex;
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

    fn live_market(id: &str, tokens: Vec<String>) -> Market {
        Market {
            id: id.to_string(),
            condition_id: format!("cond-{id}"),
            question: "Will it rain?".to_string(),
            slug: format!("slug-{id}"),
            description: None,
            active: true,
            closed: false,
            archived: false,
            end_date: None,
            start_date: None,
            created_at: None,
            updated_at: None,
            volume: None,
            liquidity: None,
            volume_24h: None,
            outcomes: None,
            outcome_prices: None,
            token_ids: tokens,
            enable_order_book: true,
            order_min_size: None,
            order_price_min_tick_size: None,
            neg_risk: false,
            accepting_orders: true,
            clob_rewards: Vec::new(),
            rewards_min_size: None,
            rewards_max_spread: None,
            competitive: None,
        }
    }

    struct SequenceMarketSource {
        responses: StdMutex<VecDeque<Result<Vec<Market>, String>>>,
    }

    impl SequenceMarketSource {
        fn new(responses: Vec<Result<Vec<Market>, String>>) -> Self {
            Self {
                responses: StdMutex::new(responses.into_iter().collect()),
            }
        }
    }

    #[async_trait]
    impl LiveMarketSource for SequenceMarketSource {
        async fn fetch_usable_markets(&self) -> Result<Vec<Market>> {
            let mut responses = self
                .responses
                .lock()
                // Safe in tests: a poisoned mutex means a previous assertion
                // already failed while holding the fake source.
                .expect("sequence market source mutex should not be poisoned");
            responses
                .pop_front()
                .unwrap_or_else(|| Err("no market response queued".to_string()))
                .map_err(|error| anyhow::anyhow!(error))
        }
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

    #[test]
    fn test_plan_subscription_command_is_deterministic() {
        let command = plan_subscription_command(
            &["token-c".to_string(), "token-a".to_string()],
            vec![
                "token-b".to_string(),
                "token-a".to_string(),
                "token-b".to_string(),
            ],
        )
        .unwrap();

        assert_eq!(command.subscribe, vec!["token-b".to_string()]);
        assert_eq!(command.unsubscribe, vec!["token-c".to_string()]);
        assert_eq!(
            command.desired_full_set,
            vec!["token-a".to_string(), "token-b".to_string()]
        );
        assert!(plan_subscription_command(
            &command.desired_full_set,
            command.desired_full_set.clone()
        )
        .is_none());
    }

    #[test]
    fn test_market_subscription_payload_shapes() {
        let tokens = vec!["token-a".to_string(), "token-b".to_string()];
        let initial: serde_json::Value =
            serde_json::from_str(&market_subscription_payload(None, &tokens)).unwrap();
        assert_eq!(initial["type"], "market");
        assert_eq!(initial["assets_ids"], serde_json::json!(tokens));
        assert!(initial.get("operation").is_none());

        let subscribe: serde_json::Value = serde_json::from_str(&market_subscription_payload(
            Some("subscribe"),
            &["token-c".to_string()],
        ))
        .unwrap();
        assert_eq!(subscribe["operation"], "subscribe");
        assert_eq!(subscribe["assets_ids"], serde_json::json!(["token-c"]));

        let unsubscribe: serde_json::Value = serde_json::from_str(&market_subscription_payload(
            Some("unsubscribe"),
            &["token-a".to_string()],
        ))
        .unwrap();
        assert_eq!(unsubscribe["operation"], "unsubscribe");
        assert_eq!(unsubscribe["assets_ids"], serde_json::json!(["token-a"]));
    }

    #[tokio::test]
    async fn test_upload_rotated_file_success() {
        let dir = tempdir().unwrap();
        let s3 = InMemoryS3Service::default();
        let local_path = dir
            .path()
            .join("2025-06-08")
            .join("12")
            .join("12_00_worker_0.jsonl");
        tokio::fs::create_dir_all(local_path.parent().unwrap())
            .await
            .unwrap();
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

        let local_path = dir
            .path()
            .join("2025-06-08")
            .join("12")
            .join("12_00_worker_0.jsonl");
        tokio::fs::create_dir_all(local_path.parent().unwrap())
            .await
            .unwrap();
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
        let first_path = worker
            .writer
            .as_ref()
            .unwrap()
            .current_path()
            .unwrap()
            .clone();

        let next_window =
            worker.writer.as_ref().unwrap().current_window.unwrap() + chrono::Duration::minutes(1);
        worker
            .writer
            .as_mut()
            .unwrap()
            .rotate_to(next_window)
            .await
            .unwrap();

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
        .with_s3_upload(
            "bucket".to_string(),
            "prefix/".to_string(),
            "us-west-2".to_string(),
        );

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
        assert!(
            upload_rotated_file(&s3, "b", "p/", "c", dir.path(), &path, None)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_upload_rotated_file_not_under_output_dir() {
        let dir = tempdir().unwrap();
        let other = tempdir().unwrap();
        let s3 = InMemoryS3Service::default();
        let path = other.path().join("file.jsonl");
        tokio::fs::write(&path, b"x").await.unwrap();
        assert!(
            upload_rotated_file(&s3, "b", "p/", "c", dir.path(), &path, None)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_upload_rotated_file_notify_failure_preserves_local() {
        let dir = tempdir().unwrap();
        let s3 = InMemoryS3Service::default();
        let http = InMemoryHttpClient::new();
        http.set_response("http://aggregator/notify", Err("notify failed".to_string()));
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
        let (mut write, mut read) = tokio_tungstenite::accept_async(stream)
            .await
            .unwrap()
            .split();
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

    async fn run_ws_capture_server(
        addr_tx: tokio::sync::oneshot::Sender<std::net::SocketAddr>,
        frame_tx: mpsc::Sender<String>,
        expected_frames: usize,
    ) {
        use tokio::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        addr_tx.send(listener.local_addr().unwrap()).unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        let (mut write, mut read) = tokio_tungstenite::accept_async(stream)
            .await
            .unwrap()
            .split();

        let mut captured = 0usize;
        while let Some(Ok(message)) = read.next().await {
            if let Message::Text(text) = message {
                if text == "PING" {
                    write.send(Message::Text("PONG".to_string())).await.unwrap();
                    continue;
                }
                frame_tx.send(text).await.unwrap();
                captured += 1;
                if captured >= expected_frames {
                    break;
                }
            }
        }
        let _ = write.close().await;
    }

    async fn run_ws_reconnect_capture_server(
        addr_tx: tokio::sync::oneshot::Sender<std::net::SocketAddr>,
        frame_tx: mpsc::Sender<String>,
        expected_frames: usize,
    ) {
        use tokio::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        addr_tx.send(listener.local_addr().unwrap()).unwrap();

        let mut captured = 0usize;
        while captured < expected_frames {
            let (stream, _) = listener.accept().await.unwrap();
            let (mut write, mut read) = tokio_tungstenite::accept_async(stream)
                .await
                .unwrap()
                .split();

            while let Some(Ok(message)) = read.next().await {
                if let Message::Text(text) = message {
                    if text == "PING" {
                        write.send(Message::Text("PONG".to_string())).await.unwrap();
                        continue;
                    }
                    frame_tx.send(text).await.unwrap();
                    captured += 1;
                    // Close each accepted socket after its first subscription
                    // frame so the collector manager must supervise and
                    // respawn the finished worker task.
                    let _ = write.close().await;
                    break;
                }
            }
        }
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

        let result =
            tokio::time::timeout(Duration::from_secs(5), worker.connect_and_collect()).await;
        server_shutdown.store(true, Ordering::Relaxed);
        result.unwrap().unwrap();
        assert!(!worker.buffer.is_empty());
    }

    #[tokio::test]
    async fn test_worker_applies_subscription_updates_without_reconnect() {
        let (addr_tx, addr_rx) = tokio::sync::oneshot::channel();
        let (frame_tx, mut frame_rx) = mpsc::channel(3);
        tokio::spawn(async move {
            run_ws_capture_server(addr_tx, frame_tx, 3).await;
        });
        let addr = addr_rx.await.unwrap();
        let url = format!("ws://{}", addr);

        let shutdown = Arc::new(AtomicBool::new(false));
        let (control_tx, control_rx) = mpsc::channel(4);
        let mut worker = OrderbookWorker::new(
            0,
            vec!["token-a".to_string()],
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
        )
        .with_control_rx(control_rx);

        let worker_handle = tokio::spawn(async move { worker.connect_and_collect().await });

        let initial = tokio::time::timeout(Duration::from_secs(5), frame_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let initial_json: serde_json::Value = serde_json::from_str(&initial).unwrap();
        assert_eq!(initial_json["assets_ids"], serde_json::json!(["token-a"]));
        assert!(initial_json.get("operation").is_none());

        control_tx
            .send(SubscriptionCommand {
                subscribe: vec!["token-b".to_string()],
                unsubscribe: Vec::new(),
                desired_full_set: vec!["token-a".to_string(), "token-b".to_string()],
            })
            .await
            .unwrap();
        let subscribe = tokio::time::timeout(Duration::from_secs(5), frame_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let subscribe_json: serde_json::Value = serde_json::from_str(&subscribe).unwrap();
        assert_eq!(subscribe_json["operation"], "subscribe");
        assert_eq!(subscribe_json["assets_ids"], serde_json::json!(["token-b"]));

        control_tx
            .send(SubscriptionCommand {
                subscribe: Vec::new(),
                unsubscribe: vec!["token-a".to_string()],
                desired_full_set: vec!["token-b".to_string()],
            })
            .await
            .unwrap();
        let unsubscribe = tokio::time::timeout(Duration::from_secs(5), frame_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let unsubscribe_json: serde_json::Value = serde_json::from_str(&unsubscribe).unwrap();
        assert_eq!(unsubscribe_json["operation"], "unsubscribe");
        assert_eq!(
            unsubscribe_json["assets_ids"],
            serde_json::json!(["token-a"])
        );

        shutdown.store(true, Ordering::Relaxed);
        tokio::time::timeout(Duration::from_secs(5), worker_handle)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_restart_finished_workers_respawns_closed_connection() {
        let (addr_tx, addr_rx) = tokio::sync::oneshot::channel();
        let (frame_tx, mut frame_rx) = mpsc::channel(2);
        tokio::spawn(async move {
            run_ws_reconnect_capture_server(addr_tx, frame_tx, 2).await;
        });
        let addr = addr_rx.await.unwrap();
        let url = format!("ws://{}", addr);

        let dir = tempdir().unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let spawn_config = WorkerSpawnConfig {
            output_dir: dir.path().to_path_buf(),
            relay_url: None,
            rotate_interval: Duration::from_secs(60),
            shutdown: shutdown.clone(),
            s3_service: None,
            s3_bucket: None,
            s3_prefix: None,
            collector_id: None,
            orchestration_client: None,
            ws_url: Some(url),
        };
        let current_chunks = vec![vec!["token-a".to_string()]];
        let (handle, control_tx) = spawn_config.spawn_worker(0, current_chunks[0].clone());
        let mut handles = vec![handle];
        let mut control_txs = vec![control_tx];

        let first = tokio::time::timeout(Duration::from_secs(5), frame_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let first_json: serde_json::Value = serde_json::from_str(&first).unwrap();
        assert_eq!(first_json["assets_ids"], serde_json::json!(["token-a"]));

        tokio::time::timeout(Duration::from_secs(5), async {
            while !handles[0].is_finished() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        restart_finished_workers(
            &current_chunks,
            &mut control_txs,
            &mut handles,
            &spawn_config,
        )
        .await;

        let restarted = tokio::time::timeout(Duration::from_secs(5), frame_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let restarted_json: serde_json::Value = serde_json::from_str(&restarted).unwrap();
        assert_eq!(restarted_json["assets_ids"], serde_json::json!(["token-a"]));

        shutdown.store(true, Ordering::Relaxed);
        for handle in handles {
            handle.abort();
            let _ = handle.await;
        }
    }

    #[tokio::test]
    async fn test_standalone_live_collector_refresh_subscribes_new_market() {
        let (addr_tx, addr_rx) = tokio::sync::oneshot::channel();
        let (frame_tx, mut frame_rx) = mpsc::channel(2);
        tokio::spawn(async move {
            run_ws_capture_server(addr_tx, frame_tx, 2).await;
        });
        let addr = addr_rx.await.unwrap();
        let url = format!("ws://{}", addr);

        let source = Arc::new(SequenceMarketSource::new(vec![
            Ok(vec![live_market("m1", vec!["token-a".to_string()])]),
            Ok(vec![
                live_market("m1", vec!["token-a".to_string()]),
                live_market("m2", vec!["token-b".to_string()]),
            ]),
        ]));
        let policy = MarketRefreshPolicy {
            refresh_interval: Duration::from_millis(100), // Short wall-clock test refresh interval.
            stale_ttl: Duration::from_secs(12 * 60 * 60), // 12 hours in seconds.
        };

        let dir = tempdir().unwrap();
        let collector = OrderbookCollector::new(
            Vec::new(),
            dir.path().to_path_buf(),
            None,
            100,
            Duration::from_secs(60),
            Some(1),
        )
        .with_market_source(source, policy)
        .with_ws_url(url);

        let collector_handle = tokio::spawn(async move { collector.run().await });

        let initial = tokio::time::timeout(Duration::from_secs(5), frame_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let initial_json: serde_json::Value = serde_json::from_str(&initial).unwrap();
        assert_eq!(initial_json["assets_ids"], serde_json::json!(["token-a"]));

        let subscribe = tokio::time::timeout(Duration::from_secs(5), frame_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let subscribe_json: serde_json::Value = serde_json::from_str(&subscribe).unwrap();
        assert_eq!(subscribe_json["operation"], "subscribe");
        assert_eq!(subscribe_json["assets_ids"], serde_json::json!(["token-b"]));

        tokio::time::timeout(Duration::from_secs(5), collector_handle)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_orchestrated_collector_polls_assignment_and_subscribes_new_token() {
        use axum::{routing::get, routing::post, Json, Router};
        use std::sync::atomic::AtomicUsize;

        let (addr_tx, addr_rx) = tokio::sync::oneshot::channel();
        let aggregator_shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_for_server = aggregator_shutdown.clone();
        tokio::spawn(async move {
            let calls = Arc::new(AtomicUsize::new(0));
            let assignment_calls = calls.clone();
            let app = Router::new()
                .route(
                    "/register",
                    post(|| async { Json(serde_json::json!({ "collector_id": "test-cid" })) }),
                )
                .route(
                    "/assignment/:collector_id",
                    get(move || {
                        let assignment_calls = assignment_calls.clone();
                        async move {
                            let call = assignment_calls.fetch_add(1, Ordering::Relaxed);
                            if call == 0 {
                                Json(serde_json::json!({
                                    "token_ids": ["token-a"],
                                    "chunk_size": 100,
                                    "version": 1,
                                }))
                            } else {
                                Json(serde_json::json!({
                                    "token_ids": ["token-a", "token-b"],
                                    "chunk_size": 100,
                                    "version": 2,
                                }))
                            }
                        }
                    }),
                )
                .route("/heartbeat/:collector_id", post(|| async { "ok" }));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            addr_tx.send(listener.local_addr().unwrap()).unwrap();
            let shutdown_future = async move {
                while !shutdown_for_server.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            };
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown_future)
                .await
                .unwrap();
        });
        let aggregator_url = format!("http://{}", addr_rx.await.unwrap());

        let (ws_addr_tx, ws_addr_rx) = tokio::sync::oneshot::channel();
        let (frame_tx, mut frame_rx) = mpsc::channel(2);
        tokio::spawn(async move {
            run_ws_capture_server(ws_addr_tx, frame_tx, 2).await;
        });
        let ws_url = format!("ws://{}", ws_addr_rx.await.unwrap());

        let dir = tempdir().unwrap();
        let collector = OrderbookCollector::new(
            Vec::new(),
            dir.path().to_path_buf(),
            None,
            100,
            Duration::from_secs(60),
            Some(1),
        )
        .with_aggregator_url(aggregator_url)
        .with_assignment_poll_interval(Duration::from_millis(100))
        .with_ws_url(ws_url);

        let collector_handle = tokio::spawn(async move { collector.run().await });

        let initial = tokio::time::timeout(Duration::from_secs(5), frame_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let initial_json: serde_json::Value = serde_json::from_str(&initial).unwrap();
        assert_eq!(initial_json["assets_ids"], serde_json::json!(["token-a"]));

        let subscribe = tokio::time::timeout(Duration::from_secs(5), frame_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let subscribe_json: serde_json::Value = serde_json::from_str(&subscribe).unwrap();
        assert_eq!(subscribe_json["operation"], "subscribe");
        assert_eq!(subscribe_json["assets_ids"], serde_json::json!(["token-b"]));

        tokio::time::timeout(Duration::from_secs(5), collector_handle)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        aggregator_shutdown.store(true, Ordering::Relaxed);
    }

    #[tokio::test]
    async fn test_orchestrated_collector_applies_empty_assignment_when_version_changes() {
        use axum::{routing::get, routing::post, Json, Router};
        use std::sync::atomic::AtomicUsize;

        let (addr_tx, addr_rx) = tokio::sync::oneshot::channel();
        let aggregator_shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_for_server = aggregator_shutdown.clone();
        tokio::spawn(async move {
            let calls = Arc::new(AtomicUsize::new(0));
            let assignment_calls = calls.clone();
            let app = Router::new()
                .route(
                    "/register",
                    post(|| async { Json(serde_json::json!({ "collector_id": "test-cid" })) }),
                )
                .route(
                    "/assignment/:collector_id",
                    get(move || {
                        let assignment_calls = assignment_calls.clone();
                        async move {
                            let call = assignment_calls.fetch_add(1, Ordering::Relaxed);
                            if call == 0 {
                                Json(serde_json::json!({
                                    "token_ids": ["token-a"],
                                    "chunk_size": 100,
                                    "version": 1,
                                }))
                            } else {
                                Json(serde_json::json!({
                                    "token_ids": [],
                                    "chunk_size": 100,
                                    "version": 2,
                                }))
                            }
                        }
                    }),
                )
                .route("/heartbeat/:collector_id", post(|| async { "ok" }));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            addr_tx.send(listener.local_addr().unwrap()).unwrap();
            let shutdown_future = async move {
                while !shutdown_for_server.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            };
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown_future)
                .await
                .unwrap();
        });
        let aggregator_url = format!("http://{}", addr_rx.await.unwrap());

        let (ws_addr_tx, ws_addr_rx) = tokio::sync::oneshot::channel();
        let (frame_tx, mut frame_rx) = mpsc::channel(2);
        tokio::spawn(async move {
            run_ws_capture_server(ws_addr_tx, frame_tx, 2).await;
        });
        let ws_url = format!("ws://{}", ws_addr_rx.await.unwrap());

        let dir = tempdir().unwrap();
        let collector = OrderbookCollector::new(
            Vec::new(),
            dir.path().to_path_buf(),
            None,
            100,
            Duration::from_secs(60),
            Some(1),
        )
        .with_aggregator_url(aggregator_url)
        .with_assignment_poll_interval(Duration::from_millis(100))
        .with_ws_url(ws_url);

        let collector_handle = tokio::spawn(async move { collector.run().await });

        let initial = tokio::time::timeout(Duration::from_secs(5), frame_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let initial_json: serde_json::Value = serde_json::from_str(&initial).unwrap();
        assert_eq!(initial_json["assets_ids"], serde_json::json!(["token-a"]));

        let unsubscribe = tokio::time::timeout(Duration::from_secs(5), frame_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let unsubscribe_json: serde_json::Value = serde_json::from_str(&unsubscribe).unwrap();
        assert_eq!(unsubscribe_json["operation"], "unsubscribe");
        assert_eq!(
            unsubscribe_json["assets_ids"],
            serde_json::json!(["token-a"])
        );

        tokio::time::timeout(Duration::from_secs(5), collector_handle)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        aggregator_shutdown.store(true, Ordering::Relaxed);
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
        let first_path = worker
            .writer
            .as_ref()
            .unwrap()
            .current_path()
            .unwrap()
            .clone();

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
        let first_path = worker
            .writer
            .as_ref()
            .unwrap()
            .current_path()
            .unwrap()
            .clone();

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
            run_ws_server(
                ws_tx,
                vec![Message::Text("PONG".to_string())],
                ws_shutdown_clone,
            )
            .await;
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
            run_ws_server(
                ws_tx,
                vec![Message::Text("PONG".to_string())],
                ws_shutdown_clone,
            )
            .await;
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
        .with_s3_upload(
            "bucket".to_string(),
            "orderbook/".to_string(),
            "us-east-1".to_string(),
        )
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
