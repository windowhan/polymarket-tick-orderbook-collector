use anyhow::{Context, Result};
use axum::{
    extract::{DefaultBodyLimit, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{interval, MissedTickBehavior};
use tracing::{info, warn};
use uuid::Uuid;

use crate::aggregate_s3::{AwsS3Service, S3Object, S3Service};
use crate::dynamic_markets::{
    GammaMarketSource, LiveMarketSource, MarketRefreshPolicy, MarketRegistry, RefreshOutcome,
    RefreshWarning,
};
use crate::http_client::ReqwestHttpClient;
#[cfg(test)]
use crate::market_discovery::Market;

/// State for a single registered collector.
#[derive(Debug, Clone)]
struct CollectorInfo {
    last_heartbeat: Instant,
}

/// Shared aggregator state.
#[derive(Debug, Clone)]
struct AppState {
    /// Live market registry used to produce assignment token snapshots.
    market_registry: Arc<RwLock<MarketRegistry>>,
    /// Registered collectors keyed by collector ID.
    collectors: Arc<RwLock<HashMap<String, CollectorInfo>>>,
    /// Current token assignment: collector_id -> list of token IDs.
    assignments: Arc<RwLock<HashMap<String, Vec<String>>>>,
    /// Monotonic assignment generation returned by `/assignment/:collector_id`.
    assignment_version: Arc<RwLock<u64>>,
    /// Reverse index: token_id -> list of collector IDs currently assigned.
    market_replicas: Arc<RwLock<HashMap<String, Vec<String>>>>,
    /// Files notified by collectors that are waiting to be merged.
    pending_files: Arc<Mutex<Vec<S3Object>>>,
    /// S3 keys already merged to avoid reprocessing.
    processed_keys: Arc<RwLock<HashSet<String>>>,
    /// Final merged output path.
    output_path: PathBuf,
    /// S3 bucket where collectors upload rotated files.
    s3_bucket: String,
    /// S3 prefix where collector files are stored.
    s3_prefix: String,
    /// Whether to delete S3 objects after merging.
    delete_after_merge: bool,
    /// Number of collectors each market is assigned to (default 2).
    replication_factor: usize,
    /// How long a collector can miss heartbeats before marked stale.
    heartbeat_timeout: Duration,
    /// Approximate max token IDs per collector.
    tokens_per_collector: usize,
}

/// Request body for collector registration.
#[derive(Debug, Deserialize)]
struct RegisterRequest {
    /// Optional collector ID. If omitted, aggregator assigns a UUID.
    #[serde(default)]
    collector_id: Option<String>,
}

/// Response for collector registration.
#[derive(Debug, Serialize)]
struct RegisterResponse {
    collector_id: String,
}

/// Response for assignment request.
#[derive(Debug, Serialize)]
struct AssignmentResponse {
    token_ids: Vec<String>,
    chunk_size: usize,
    version: u64,
}

/// Request body for S3 upload notification.
#[derive(Debug, Deserialize)]
struct NotifyRequest {
    key: String,
    #[serde(default)]
    collector_id: Option<String>,
}

/// Response for notify endpoint.
#[derive(Debug, Serialize)]
struct NotifyResponse {
    received: bool,
}

/// Load all markets from a JSONL file and flatten token IDs.
///
/// # Example — Input / Output
/// ```rust,ignore
/// // markets.jsonl:
/// // {"token_ids":["400737..."], ...}
///
/// let (markets, token_ids) = load_markets(Path::new("markets.jsonl")).unwrap();
///
/// // Output:
/// assert_eq!(markets.len(), 1);
/// assert_eq!(token_ids, vec!["400737..."]);
/// ```
#[cfg(test)]
fn load_markets(path: &Path) -> Result<(Vec<Market>, Vec<String>)> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read markets file: {}", path.display()))?;

    let mut markets = Vec::new();
    let mut token_ids = Vec::new();

    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let market: Market = serde_json::from_str(line)
            .with_context(|| format!("Failed to parse market line: {}", line))?;
        token_ids.extend(market.token_ids.clone());
        markets.push(market);
    }

    info!(
        markets = markets.len(),
        tokens = token_ids.len(),
        "Loaded markets"
    );
    Ok((markets, token_ids))
}

/// Allocate token IDs to registered collectors with replication.
///
/// # Allocation Strategy
/// 1. Split all token IDs into chunks of size `tokens_per_collector`.
/// 2. For each chunk, assign it to `replication_factor` different collectors.
/// 3. Use round-robin to distribute replicas across healthy collectors.
///
/// # Example — Input / Output
/// ```rust,ignore
/// let tokens = vec!["t0", "t1", "t2", "t3"];
/// let collectors = vec!["c0", "c1"];
/// let replication = 2;
/// let tokens_per_collector = 2;
///
/// let assignments = allocate_tokens(&tokens, &collectors, replication, tokens_per_collector);
///
/// // Output: both collectors get all tokens because each chunk is replicated twice
/// assert_eq!(assignments["c0"], vec!["t0", "t1", "t2", "t3"]);
/// assert_eq!(assignments["c1"], vec!["t0", "t1", "t2", "t3"]);
/// ```
fn allocate_tokens(
    tokens: &[String],
    collector_ids: &[String],
    replication_factor: usize,
    tokens_per_collector: usize,
) -> HashMap<String, Vec<String>> {
    let mut assignments: HashMap<String, Vec<String>> = collector_ids
        .iter()
        .map(|id| (id.clone(), Vec::new()))
        .collect();

    if collector_ids.is_empty() || tokens.is_empty() {
        return assignments;
    }

    // Split tokens into chunks. Each chunk will be replicated `replication_factor` times.
    let chunks: Vec<Vec<String>> = tokens
        .chunks(tokens_per_collector)
        .map(|c| c.to_vec())
        .collect();

    for (chunk_idx, chunk) in chunks.iter().enumerate() {
        for r in 0..replication_factor {
            // Pick a collector in round-robin fashion, offset by chunk and replica index
            // to spread replicas across different collectors.
            let collector_idx = (chunk_idx * replication_factor + r) % collector_ids.len();
            let collector_id = &collector_ids[collector_idx];
            assignments
                .get_mut(collector_id)
                .unwrap()
                .extend(chunk.clone());
        }
    }

    assignments
}

/// Recompute token assignments from current healthy collectors.
///
/// The live market registry has its own token-output version, but collectors
/// need an assignment generation that also changes when collector membership or
/// health changes. This function therefore increments `assignment_version` only
/// when the concrete `collector_id -> token_ids` map changes, including valid
/// transitions to an empty token list for a still-healthy collector.
async fn rebalance(state: &AppState) {
    let collectors = state.collectors.read().await;
    let now = Instant::now();
    let snapshot = state.market_registry.read().await.snapshot();

    // Filter out stale collectors.
    let mut healthy: Vec<String> = collectors
        .iter()
        .filter(|(_, info)| now.duration_since(info.last_heartbeat) <= state.heartbeat_timeout)
        .map(|(id, _)| id.clone())
        .collect();
    healthy.sort();

    drop(collectors);

    if healthy.is_empty() {
        let version = *state.assignment_version.read().await;
        warn!(
            version,
            market_version = snapshot.version,
            "No healthy collectors, preserving previous assignments"
        );
        return;
    }

    let new_assignments = allocate_tokens(
        &snapshot.token_ids,
        &healthy,
        state.replication_factor,
        state.tokens_per_collector,
    );

    // Build reverse index: token_id -> [collector_ids]
    let mut market_replicas: HashMap<String, Vec<String>> = HashMap::new();
    for (collector_id, tokens) in &new_assignments {
        for token in tokens {
            market_replicas
                .entry(token.clone())
                .or_default()
                .push(collector_id.clone());
        }
    }

    let mut assignments_guard = state.assignments.write().await;
    let assignments_changed = *assignments_guard != new_assignments;
    if assignments_changed {
        *assignments_guard = new_assignments;
    }
    drop(assignments_guard);

    if assignments_changed {
        let mut replicas_guard = state.market_replicas.write().await;
        *replicas_guard = market_replicas;
        drop(replicas_guard);

        let mut assignment_version = state.assignment_version.write().await;
        *assignment_version = assignment_version.saturating_add(1);
        let version = *assignment_version;
        drop(assignment_version);

        info!(
            healthy = healthy.len(),
            version,
            market_version = snapshot.version,
            tokens = snapshot.token_ids.len(),
            "Rebalanced assignments"
        );
    } else {
        let version = *state.assignment_version.read().await;
        info!(
            healthy = healthy.len(),
            version,
            market_version = snapshot.version,
            tokens = snapshot.token_ids.len(),
            "Assignments unchanged after rebalance"
        );
    }
}

/// Background task: monitor heartbeats and rebalance when collectors go stale.
async fn heartbeat_monitor(state: AppState) {
    let mut tick = interval(Duration::from_secs(10));
    let mut last_healthy_count: Option<usize> = None;

    loop {
        tick.tick().await;

        let collectors = state.collectors.read().await;
        let now = Instant::now();
        let mut stale_ids = Vec::new();
        let mut healthy_count = 0;

        for (id, info) in collectors.iter() {
            if now.duration_since(info.last_heartbeat) > state.heartbeat_timeout {
                stale_ids.push(id.clone());
            } else {
                healthy_count += 1;
            }
        }

        drop(collectors);

        for id in &stale_ids {
            warn!(collector_id = %id, "Collector stale, removing");
            let mut collectors = state.collectors.write().await;
            collectors.remove(id);
        }

        // Rebalance if any collector became stale or healthy count changed.
        if !stale_ids.is_empty() || last_healthy_count != Some(healthy_count) {
            rebalance(&state).await;
            last_healthy_count = Some(healthy_count);
        }
    }
}

/// Apply one live-market refresh and rebalance when assignment tokens changed.
async fn refresh_markets_once(state: &AppState, source: &dyn LiveMarketSource) -> RefreshOutcome {
    let result = source.fetch_usable_markets().await;
    let outcome = {
        let mut registry = state.market_registry.write().await;
        registry.apply_refresh_result(Instant::now(), result)
    };

    if let Some(warning) = &outcome.warning {
        match warning {
            RefreshWarning::FetchFailed(error) => {
                warn!(error = %error, version = outcome.version, tokens = outcome.token_count, "Live market refresh failed; keeping previous assignments");
            }
            RefreshWarning::EmptyUsableRefresh => {
                warn!(
                    version = outcome.version,
                    tokens = outcome.token_count,
                    "Live market refresh returned no usable markets; keeping previous assignments"
                );
            }
        }
        return outcome;
    }

    info!(
        changed = outcome.changed,
        added_markets = outcome.added_markets,
        marked_stale = outcome.marked_stale,
        removed_markets = outcome.removed_markets,
        version = outcome.version,
        tokens = outcome.token_count,
        "Applied live market refresh"
    );

    if outcome.changed {
        rebalance(state).await;
    }

    outcome
}

/// Background task: refresh live Polymarket markets and update assignments.
async fn market_refresh_task(state: AppState, source: Arc<dyn LiveMarketSource>) {
    let refresh_interval = state.market_registry.read().await.policy().refresh_interval;
    let mut tick = interval(refresh_interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

    // Tokio intervals fire immediately on the first tick. Startup already
    // performed the initial fail-fast fetch, so consume that first tick to make
    // runtime refreshes happen after the configured interval.
    tick.tick().await;

    loop {
        tick.tick().await;
        let _outcome = refresh_markets_once(&state, source.as_ref()).await;
    }
}

/// Download a single S3 object and append its valid JSONL lines to the output file.
async fn merge_object(
    service: &dyn S3Service,
    bucket: &str,
    obj: &S3Object,
    output_path: &Path,
) -> Result<u64> {
    let data = service
        .get_object(bucket, &obj.key)
        .await
        .with_context(|| format!("Failed to download {}", obj.key))?;

    let text = String::from_utf8_lossy(&data);
    let mut valid_lines: u64 = 0;

    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(output_path)
        .await?;

    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        if serde_json::from_str::<serde_json::Value>(line).is_ok() {
            file.write_all(line.as_bytes()).await?;
            file.write_all(b"\n").await?;
            valid_lines += 1;
        } else {
            warn!(key = %obj.key, "Skipping malformed JSON line");
        }
    }

    file.flush().await?;
    Ok(valid_lines)
}

/// Background task: process pending S3 files and merge them into the output.
async fn merge_task(state: AppState, s3: Arc<dyn S3Service>) {
    let mut tick = interval(Duration::from_secs(30));

    loop {
        tick.tick().await;

        // Step 1: Process files that collectors explicitly notified.
        let pending: Vec<S3Object> = {
            let mut guard = state.pending_files.lock().await;
            std::mem::take(&mut *guard)
        };

        let mut processed = Vec::new();
        for obj in pending {
            match merge_object(s3.as_ref(), &state.s3_bucket, &obj, &state.output_path).await {
                Ok(lines) => {
                    info!(key = %obj.key, lines, "Merged notified object");
                    processed.push(obj.key.clone());
                }
                Err(e) => {
                    warn!(key = %obj.key, error = %e, "Failed to merge notified object, will retry");
                    // Put it back for retry
                    let mut guard = state.pending_files.lock().await;
                    guard.push(obj);
                }
            }
        }

        // Step 2: Fallback S3 poll for any missed notifications.
        match s3.list_objects(&state.s3_bucket, &state.s3_prefix).await {
            Ok(objects) => {
                let jsonl_objects: Vec<_> = objects
                    .into_iter()
                    .filter(|o| o.key.ends_with(".jsonl"))
                    .collect();

                let processed_keys = state.processed_keys.read().await;
                let unprocessed: Vec<_> = jsonl_objects
                    .into_iter()
                    .filter(|o| !processed_keys.contains(&o.key))
                    .collect();
                drop(processed_keys);

                for obj in unprocessed {
                    match merge_object(s3.as_ref(), &state.s3_bucket, &obj, &state.output_path)
                        .await
                    {
                        Ok(lines) => {
                            info!(key = %obj.key, lines, "Merged polled object");
                            processed.push(obj.key.clone());
                        }
                        Err(e) => {
                            warn!(key = %obj.key, error = %e, "Failed to merge polled object");
                        }
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "Failed to list S3 objects");
            }
        }

        // Step 3: Mark processed and optionally delete from S3.
        if !processed.is_empty() {
            let mut keys = state.processed_keys.write().await;
            for key in &processed {
                keys.insert(key.clone());
            }
            drop(keys);

            if state.delete_after_merge {
                for key in &processed {
                    if let Err(e) = s3.delete_object(&state.s3_bucket, key).await {
                        warn!(key = %key, error = %e, "Failed to delete merged object");
                    } else {
                        info!(key = %key, "Deleted merged object");
                    }
                }
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP Handlers
// ─────────────────────────────────────────────────────────────────────────────

/// POST /register — Register a new collector and return its ID.
async fn register_handler(
    State(state): State<AppState>,
    Json(req): Json<RegisterRequest>,
) -> Json<RegisterResponse> {
    let id = req
        .collector_id
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    let mut collectors = state.collectors.write().await;
    collectors.insert(
        id.clone(),
        CollectorInfo {
            last_heartbeat: Instant::now(),
        },
    );
    drop(collectors);

    // Trigger rebalance so the new collector gets assignments.
    rebalance(&state).await;

    info!(collector_id = %id, "Collector registered");
    Json(RegisterResponse { collector_id: id })
}

/// POST /heartbeat/:collector_id — Update collector liveness.
async fn heartbeat_handler(
    State(state): State<AppState>,
    axum::extract::Path(collector_id): axum::extract::Path<String>,
) -> StatusCode {
    let mut collectors = state.collectors.write().await;
    if let Some(info) = collectors.get_mut(&collector_id) {
        info.last_heartbeat = Instant::now();
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

/// GET /assignment/:collector_id — Return token IDs assigned to this collector.
async fn assignment_handler(
    State(state): State<AppState>,
    axum::extract::Path(collector_id): axum::extract::Path<String>,
) -> Json<AssignmentResponse> {
    let assignments = state.assignments.read().await;
    let token_ids = assignments.get(&collector_id).cloned().unwrap_or_default();
    drop(assignments);
    let version = *state.assignment_version.read().await;

    Json(AssignmentResponse {
        token_ids,
        chunk_size: state.tokens_per_collector,
        version,
    })
}

/// POST /notify — Collector notifies aggregator that a new S3 object is available.
async fn notify_handler(
    State(state): State<AppState>,
    Json(req): Json<NotifyRequest>,
) -> Json<NotifyResponse> {
    let obj = S3Object {
        key: req.key,
        etag: None,
        size: 0,
    };

    // Skip if already processed.
    let already_processed = {
        let keys = state.processed_keys.read().await;
        keys.contains(&obj.key)
    };

    if !already_processed {
        info!(key = %obj.key, collector = ?req.collector_id, "Pending file notified");
        let mut pending = state.pending_files.lock().await;
        pending.push(obj);
    }

    Json(NotifyResponse { received: true })
}

// ─────────────────────────────────────────────────────────────────────────────
// Public Entry Point
// ─────────────────────────────────────────────────────────────────────────────

/// Run the orchestrated aggregator server.
///
/// # Detailed Description
/// The aggregator is API-first: startup fetches active open Polymarket Gamma
/// markets through [`GammaMarketSource`] and fails before binding the HTTP
/// socket if that initial fetch errors or produces no usable orderbook tokens.
/// After startup, a background refresh loop keeps assignments in sync with live
/// market additions/removals using [`MarketRefreshPolicy::default`] (10-minute
/// refreshes and a 12-hour stale-market grace window). The `markets_path`
/// argument is retained only for backward-compatible call sites and is not used
/// by this live path.
///
/// # Arguments
/// * `bind` — Socket address to bind, e.g. `"0.0.0.0:8080"`
/// * `output_path` — Local path for the merged JSONL output
/// * `markets_path` — Deprecated static market path retained for call-site compatibility.
/// * `s3_bucket` — S3 bucket where collectors upload rotated files
/// * `s3_prefix` — S3 prefix under collector files are stored
/// * `replication_factor` — How many collectors each market is assigned to (default 2)
/// * `heartbeat_timeout` — Seconds before a collector is considered stale (default 30)
/// * `delete_after_merge` — Whether to delete S3 objects after merging (default false)
/// * `region` — AWS region for S3 operations (default "us-east-1")
///
/// # Example — Input / Output
/// ```rust,ignore
/// aggregator::run(
///     "0.0.0.0:8080",
///     PathBuf::from("/data/aggregated.jsonl"),
///     PathBuf::from("/data/markets.jsonl"),
///     "my-polymarket-bucket".to_string(),
///     "orderbook/".to_string(),
///     2,                          // replication_factor
///     Duration::from_secs(30),    // heartbeat_timeout
///     true,                       // delete_after_merge
///     "us-east-1".to_string(),
/// ).await.unwrap();
///
/// // Output: axum server listening, background tasks running
/// // Logs: "Fetched initial live markets", "Aggregator listening"
/// ```
#[allow(clippy::too_many_arguments)]
pub async fn run(
    bind: &str,
    output_path: PathBuf,
    _markets_path: PathBuf,
    s3_bucket: String,
    s3_prefix: String,
    replication_factor: usize,
    heartbeat_timeout: Duration,
    delete_after_merge: bool,
    region: String,
) -> Result<()> {
    let source = Arc::new(GammaMarketSource::new(Arc::new(ReqwestHttpClient::new())));
    run_with_market_source(
        bind,
        output_path,
        s3_bucket,
        s3_prefix,
        replication_factor,
        heartbeat_timeout,
        delete_after_merge,
        region,
        source,
        MarketRefreshPolicy::default(),
    )
    .await
}

/// Run the orchestrated aggregator with an injected live-market source.
///
/// # Detailed Description
/// This entry point exists so the CLI and tests can provide the same HTTP
/// client or deterministic fake source while production keeps the API-first
/// lifecycle. It performs the same fail-fast startup and fail-open periodic
/// refresh behavior as [`run`].
///
/// # Arguments
/// * `bind` — Socket address to bind, e.g. `"0.0.0.0:8080"`.
/// * `output_path` — Local path for the merged JSONL output.
/// * `s3_bucket` — S3 bucket where collectors upload rotated files.
/// * `s3_prefix` — S3 prefix under which collector files are stored.
/// * `replication_factor` — Number of collectors each token chunk is assigned to.
/// * `heartbeat_timeout` — Duration before a collector is considered stale.
/// * `delete_after_merge` — Whether merged S3 objects should be deleted.
/// * `region` — AWS region used by the S3 merge service.
/// * `market_source` — Live market source used for startup and refresh fetches.
/// * `refresh_policy` — Runtime refresh interval and stale-retention policy.
///
/// # Returns
/// `Ok(())` when the server shuts down gracefully, or an error when startup,
/// binding, market discovery, or server execution fails.
///
/// # Example — Input / Output
/// ```rust,ignore
/// let source = Arc::new(GammaMarketSource::new(client));
/// aggregator::run_with_market_source(
///     "127.0.0.1:8080",
///     PathBuf::from("data/aggregated.jsonl"),
///     "bucket".to_string(),
///     "orderbook/".to_string(),
///     2,
///     Duration::from_secs(60),
///     false,
///     "us-east-1".to_string(),
///     source,
///     MarketRefreshPolicy::default(),
/// ).await?;
/// # anyhow::Ok(())
/// ```
///
/// # Related
/// - [`run`]
/// - [`MarketRefreshPolicy`]
#[allow(clippy::too_many_arguments)]
pub async fn run_with_market_source(
    bind: &str,
    output_path: PathBuf,
    s3_bucket: String,
    s3_prefix: String,
    replication_factor: usize,
    heartbeat_timeout: Duration,
    delete_after_merge: bool,
    region: String,
    market_source: Arc<dyn LiveMarketSource>,
    refresh_policy: MarketRefreshPolicy,
) -> Result<()> {
    run_with_shutdown_and_market_source(
        bind,
        output_path,
        s3_bucket,
        s3_prefix,
        replication_factor,
        heartbeat_timeout,
        delete_after_merge,
        region,
        market_source,
        refresh_policy,
        std::future::pending(),
    )
    .await
}

/// Same as [`run`], but accepts a shutdown future so tests can make
/// `axum::serve(...).await` return gracefully for line-coverage purposes.
#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(crate) async fn run_with_shutdown(
    bind: &str,
    output_path: PathBuf,
    _markets_path: PathBuf,
    s3_bucket: String,
    s3_prefix: String,
    replication_factor: usize,
    heartbeat_timeout: Duration,
    delete_after_merge: bool,
    region: String,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let source = Arc::new(GammaMarketSource::new(Arc::new(ReqwestHttpClient::new())));
    run_with_shutdown_and_market_source(
        bind,
        output_path,
        s3_bucket,
        s3_prefix,
        replication_factor,
        heartbeat_timeout,
        delete_after_merge,
        region,
        source,
        MarketRefreshPolicy::default(),
        shutdown,
    )
    .await
}

/// Same as [`run_with_market_source`], but accepts a shutdown future for tests.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_with_shutdown_and_market_source(
    bind: &str,
    output_path: PathBuf,
    s3_bucket: String,
    s3_prefix: String,
    replication_factor: usize,
    heartbeat_timeout: Duration,
    delete_after_merge: bool,
    region: String,
    market_source: Arc<dyn LiveMarketSource>,
    refresh_policy: MarketRefreshPolicy,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    // Ensure output directory exists.
    if let Some(parent) = output_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let state = prepare_app_state(
        bind,
        output_path,
        s3_bucket,
        s3_prefix,
        replication_factor,
        heartbeat_timeout,
        delete_after_merge,
        market_source.clone(),
        refresh_policy,
    )
    .await?;

    start_server(bind, state, region, Some(market_source), shutdown).await
}

/// Fetch initial live markets and build shared aggregator state.
#[allow(clippy::too_many_arguments)]
async fn prepare_app_state(
    bind: &str,
    output_path: PathBuf,
    s3_bucket: String,
    s3_prefix: String,
    replication_factor: usize,
    heartbeat_timeout: Duration,
    delete_after_merge: bool,
    market_source: Arc<dyn LiveMarketSource>,
    refresh_policy: MarketRefreshPolicy,
) -> Result<AppState> {
    let markets = market_source
        .fetch_usable_markets()
        .await
        .context("initial live market fetch failed")?;
    let registry = MarketRegistry::from_initial_markets(Instant::now(), refresh_policy, markets)?;
    let snapshot = registry.snapshot();

    info!(bind = %bind, output = %output_path.display(), active_markets = snapshot.active_markets, stale_markets = snapshot.stale_markets, tokens = snapshot.token_ids.len(), version = snapshot.version, replication_factor, ?heartbeat_timeout, refresh_interval_secs = refresh_policy.refresh_interval.as_secs(), stale_ttl_secs = refresh_policy.stale_ttl.as_secs(), "Fetched initial live markets");

    Ok(build_app_state(
        registry,
        output_path,
        s3_bucket,
        s3_prefix,
        replication_factor,
        heartbeat_timeout,
        delete_after_merge,
    ))
}

/// Start the HTTP server and background tasks.
async fn start_server(
    bind: &str,
    state: AppState,
    region: String,
    market_source: Option<Arc<dyn LiveMarketSource>>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    spawn_background_tasks(state.clone(), region, market_source).await;

    let app = build_app(state.clone());

    info!(bind = %bind, "Aggregator listening");
    let listener = tokio::net::TcpListener::bind(bind).await?;
    serve(app, listener, shutdown).await
}

/// Build the shared aggregator state.
fn build_app_state(
    market_registry: MarketRegistry,
    output_path: PathBuf,
    s3_bucket: String,
    s3_prefix: String,
    replication_factor: usize,
    heartbeat_timeout: Duration,
    delete_after_merge: bool,
) -> AppState {
    let assignment_version = market_registry.snapshot().version;
    AppState {
        market_registry: Arc::new(RwLock::new(market_registry)),
        collectors: Arc::new(RwLock::new(HashMap::new())),
        assignments: Arc::new(RwLock::new(HashMap::new())),
        assignment_version: Arc::new(RwLock::new(assignment_version)),
        market_replicas: Arc::new(RwLock::new(HashMap::new())),
        pending_files: Arc::new(Mutex::new(Vec::new())),
        processed_keys: Arc::new(RwLock::new(HashSet::new())),
        output_path,
        s3_bucket,
        s3_prefix,
        delete_after_merge,
        replication_factor,
        heartbeat_timeout,
        tokens_per_collector: 100,
    }
}

/// Spawn heartbeat, live-market refresh, and S3 merge background tasks.
async fn spawn_background_tasks(
    state: AppState,
    region: String,
    market_source: Option<Arc<dyn LiveMarketSource>>,
) {
    let monitor_state = state.clone();
    tokio::spawn(async move {
        heartbeat_monitor(monitor_state).await;
    });

    if let Some(source) = market_source {
        let refresh_state = state.clone();
        tokio::spawn(async move {
            market_refresh_task(refresh_state, source).await;
        });
    }

    let s3 = Arc::new(AwsS3Service::new(region).await);
    let merge_state = state.clone();
    tokio::spawn(async move {
        merge_task(merge_state, s3).await;
    });
}

/// Build the axum router for the aggregator.
fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/register", post(register_handler))
        .route("/heartbeat/:collector_id", post(heartbeat_handler))
        .route("/assignment/:collector_id", get(assignment_handler))
        .route("/notify", post(notify_handler))
        .layer(DefaultBodyLimit::max(50 * 1024 * 1024))
        .with_state(state)
}

/// Serve the axum app with a graceful shutdown signal.
async fn serve(
    app: Router,
    listener: tokio::net::TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate_s3::InMemoryS3Service;
    use std::collections::VecDeque;
    use std::sync::Mutex as StdMutex;
    use std::time::Duration;
    use tempfile::tempdir;

    fn sample_market_json(id: &str, tokens: &[&str]) -> String {
        serde_json::json!({
            "id": id,
            "conditionId": format!("cond-{}", id),
            "condition_id": format!("cond-{}", id),
            "question": "Will it rain?",
            "slug": "rain",
            "active": true,
            "closed": false,
            "archived": false,
            // Safe in tests: `tokens` is a slice of strings and JSON string
            // serialization cannot fail for this value shape.
            "clobTokenIds": serde_json::to_string(tokens).expect("test token ids serialize"),
            "neg_risk": false,
            "negRisk": false,
            "accepting_orders": true,
            "acceptingOrders": true,
            "enable_order_book": true,
            "enableOrderBook": true,
            "token_ids": tokens,
        })
        .to_string()
    }

    fn sample_market(id: &str, tokens: Vec<String>) -> Market {
        Market {
            id: id.to_string(),
            condition_id: format!("cond-{id}"),
            question: "Will it rain?".to_string(),
            slug: "rain".to_string(),
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

    fn test_registry(tokens: Vec<String>) -> MarketRegistry {
        let markets = if tokens.is_empty() {
            vec![sample_market(
                "test-seed",
                vec!["__test_seed_token__".to_string()],
            )]
        } else {
            vec![sample_market("test-market", tokens)]
        };
        MarketRegistry::from_initial_markets(
            Instant::now(),
            MarketRefreshPolicy::default(),
            markets,
        )
        // Safe in tests: empty token inputs are replaced with one seed
        // token above so the fail-fast startup invariant is not triggered.
        .expect("test registry should contain at least one usable token")
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

        fn single(markets: Vec<Market>) -> Self {
            Self::new(vec![Ok(markets)])
        }
    }

    #[async_trait::async_trait]
    impl LiveMarketSource for SequenceMarketSource {
        async fn fetch_usable_markets(&self) -> Result<Vec<Market>> {
            let mut responses = self
                .responses
                .lock()
                // Safe in tests: no panic is expected while the fake source is
                // held, so a poisoned mutex indicates a failed test invariant.
                .expect("sequence source mutex should not be poisoned");
            responses
                .pop_front()
                .unwrap_or_else(|| Err("no fake market response left".to_string()))
                .map_err(|error| anyhow::anyhow!(error))
        }
    }

    fn test_state(tokens: Vec<String>) -> AppState {
        AppState {
            market_registry: Arc::new(RwLock::new(test_registry(tokens))),
            collectors: Arc::new(RwLock::new(HashMap::new())),
            assignments: Arc::new(RwLock::new(HashMap::new())),
            assignment_version: Arc::new(RwLock::new(1)),
            market_replicas: Arc::new(RwLock::new(HashMap::new())),
            pending_files: Arc::new(Mutex::new(Vec::new())),
            processed_keys: Arc::new(RwLock::new(HashSet::new())),
            output_path: PathBuf::from("/tmp/aggregator_test_output.jsonl"),
            s3_bucket: "test-bucket".to_string(),
            s3_prefix: "orderbook/".to_string(),
            delete_after_merge: false,
            replication_factor: 1,
            heartbeat_timeout: Duration::from_secs(60),
            tokens_per_collector: 2,
        }
    }

    #[tokio::test]
    async fn test_prepare_app_state_fails_fast_on_initial_fetch_error() {
        let dir = tempdir().unwrap();
        let source = Arc::new(SequenceMarketSource::new(vec![Err(
            "gamma down".to_string()
        )]));

        let err = prepare_app_state(
            "127.0.0.1:0",
            dir.path().join("out.jsonl"),
            "bucket".to_string(),
            "orderbook/".to_string(),
            1,
            Duration::from_secs(60),
            false,
            source,
            MarketRefreshPolicy::default(),
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("initial live market fetch failed"));
    }

    #[tokio::test]
    async fn test_prepare_app_state_fails_fast_on_empty_initial_market_set() {
        let dir = tempdir().unwrap();
        let source = Arc::new(SequenceMarketSource::single(Vec::new()));

        let err = prepare_app_state(
            "127.0.0.1:0",
            dir.path().join("out.jsonl"),
            "bucket".to_string(),
            "orderbook/".to_string(),
            1,
            Duration::from_secs(60),
            false,
            source,
            MarketRefreshPolicy::default(),
        )
        .await
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("initial market refresh produced no usable orderbook tokens"));
    }

    #[tokio::test]
    async fn test_refresh_once_adds_new_markets_and_rebalances_assignments() {
        let state = test_state(vec!["token-a".to_string()]);
        {
            let mut collectors = state.collectors.write().await;
            collectors.insert(
                "collector-a".to_string(),
                CollectorInfo {
                    last_heartbeat: Instant::now(),
                },
            );
        }
        rebalance(&state).await;
        let before_version = *state.assignment_version.read().await;

        let source = SequenceMarketSource::single(vec![
            sample_market("test-market", vec!["token-a".to_string()]),
            sample_market("m2", vec!["token-b".to_string()]),
        ]);
        let outcome = refresh_markets_once(&state, &source).await;

        assert!(outcome.changed);
        assert_eq!(outcome.added_markets, 1);
        let assignments = state.assignments.read().await;
        assert_eq!(
            assignments["collector-a"],
            vec!["token-a".to_string(), "token-b".to_string()]
        );
        drop(assignments);
        assert!(*state.assignment_version.read().await > before_version);
    }

    #[tokio::test]
    async fn test_refresh_once_fail_open_keeps_existing_assignment() {
        let state = test_state(vec!["token-a".to_string()]);
        {
            let mut collectors = state.collectors.write().await;
            collectors.insert(
                "collector-a".to_string(),
                CollectorInfo {
                    last_heartbeat: Instant::now(),
                },
            );
        }
        rebalance(&state).await;
        let before = state.assignments.read().await.clone();
        let before_version = *state.assignment_version.read().await;

        let source = SequenceMarketSource::new(vec![Err("timeout".to_string())]);
        let outcome = refresh_markets_once(&state, &source).await;

        assert!(!outcome.changed);
        assert!(matches!(
            outcome.warning,
            Some(RefreshWarning::FetchFailed(_))
        ));
        assert_eq!(*state.assignment_version.read().await, before_version);
        assert_eq!(*state.assignments.read().await, before);
    }

    #[tokio::test]
    async fn test_refresh_once_retains_stale_market_inside_grace_window() {
        let registry = MarketRegistry::from_initial_markets(
            Instant::now(),
            MarketRefreshPolicy::default(),
            vec![
                sample_market("m1", vec!["token-a".to_string()]),
                sample_market("m2", vec!["token-b".to_string()]),
            ],
        )
        // Safe in tests: both markets are active orderbook markets with one
        // token, so the fail-fast startup invariant is satisfied.
        .expect("two-market registry should be usable");
        let state = build_app_state(
            registry,
            PathBuf::from("/tmp/stale-grace.jsonl"),
            "bucket".to_string(),
            "prefix/".to_string(),
            1,
            Duration::from_secs(60),
            false,
        );
        {
            let mut collectors = state.collectors.write().await;
            collectors.insert(
                "collector-a".to_string(),
                CollectorInfo {
                    last_heartbeat: Instant::now(),
                },
            );
        }
        rebalance(&state).await;
        let before = state.assignments.read().await.clone();
        let source =
            SequenceMarketSource::single(vec![sample_market("m1", vec!["token-a".to_string()])]);
        let outcome = refresh_markets_once(&state, &source).await;

        assert!(!outcome.changed);
        assert_eq!(outcome.marked_stale, 1);
        assert_eq!(*state.assignments.read().await, before);
    }

    #[tokio::test(start_paused = true)]
    async fn test_market_refresh_task_waits_for_configured_interval() {
        let policy = MarketRefreshPolicy {
            // Short test-only interval so paused Tokio time can advance one
            // refresh cycle without sleeping in wall-clock time.
            refresh_interval: Duration::from_secs(10),
            stale_ttl: Duration::from_secs(12 * 60 * 60), // 12 hours in seconds.
        };
        let registry = MarketRegistry::from_initial_markets(
            Instant::now(),
            policy,
            vec![sample_market("m1", vec!["token-a".to_string()])],
        )
        // Safe in tests: the seed market is active and has one token.
        .expect("seed registry should be usable");
        let state = build_app_state(
            registry,
            PathBuf::from("/tmp/refresh-task.jsonl"),
            "bucket".to_string(),
            "prefix/".to_string(),
            1,
            Duration::from_secs(60),
            false,
        );
        {
            let mut collectors = state.collectors.write().await;
            collectors.insert(
                "collector-a".to_string(),
                CollectorInfo {
                    last_heartbeat: Instant::now(),
                },
            );
        }
        rebalance(&state).await;

        let source = Arc::new(SequenceMarketSource::single(vec![
            sample_market("m1", vec!["token-a".to_string()]),
            sample_market("m2", vec!["token-b".to_string()]),
        ]));
        let task_state = state.clone();
        let handle = tokio::spawn(async move {
            market_refresh_task(task_state, source).await;
        });

        tokio::task::yield_now().await;
        assert_eq!(
            state.assignments.read().await["collector-a"],
            vec!["token-a".to_string()]
        );

        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::task::yield_now().await;

        assert_eq!(
            state.assignments.read().await["collector-a"],
            vec!["token-a".to_string(), "token-b".to_string()]
        );
        handle.abort();
        let _ = handle.await;
    }

    #[test]
    fn test_load_markets_success() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("markets.jsonl");
        std::fs::write(
            &path,
            format!("{}\n", sample_market_json("m1", &["t1", "t2"])),
        )
        .unwrap();

        let (markets, token_ids) = load_markets(&path).unwrap();
        assert_eq!(markets.len(), 1);
        assert_eq!(markets[0].id, "m1");
        assert_eq!(token_ids, vec!["t1", "t2"]);
    }

    #[test]
    fn test_load_markets_missing_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("missing.jsonl");
        let err = load_markets(&path).unwrap_err();
        assert!(err.to_string().contains("Failed to read markets file"));
    }

    #[test]
    fn test_load_markets_invalid_json() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("markets.jsonl");
        std::fs::write(&path, "not valid json\n").unwrap();
        let err = load_markets(&path).unwrap_err();
        assert!(err.to_string().contains("Failed to parse market line"));
    }

    #[test]
    fn test_load_markets_skips_blank_lines() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("markets.jsonl");
        let content = format!("\n\n{}\n\n", sample_market_json("m1", &["t1"]));
        std::fs::write(&path, content).unwrap();
        let (markets, token_ids) = load_markets(&path).unwrap();
        assert_eq!(markets.len(), 1);
        assert_eq!(token_ids, vec!["t1"]);
    }

    #[test]
    fn test_allocate_tokens_empty_tokens() {
        let map = allocate_tokens(&[], &["c1".to_string()], 1, 2);
        assert!(map["c1"].is_empty());
    }

    #[test]
    fn test_allocate_tokens_empty_collectors() {
        let map = allocate_tokens(&["t1".to_string()], &[], 1, 2);
        assert!(map.is_empty());
    }

    #[test]
    fn test_allocate_tokens_single_token() {
        let map = allocate_tokens(&["t1".to_string()], &["c1".to_string()], 1, 2);
        assert_eq!(map["c1"], vec!["t1"]);
    }

    #[test]
    fn test_allocate_tokens_multiple_chunks() {
        let tokens: Vec<String> = (0..5).map(|i| format!("t{}", i)).collect();
        let collectors = vec!["c0".to_string(), "c1".to_string()];
        let map = allocate_tokens(&tokens, &collectors, 1, 2);
        assert_eq!(map["c0"], vec!["t0", "t1", "t4"]);
        assert_eq!(map["c1"], vec!["t2", "t3"]);
    }

    #[test]
    fn test_allocate_tokens_replication_greater_than_collectors() {
        let tokens = vec!["t1".to_string()];
        let collectors = vec!["c1".to_string()];
        let map = allocate_tokens(&tokens, &collectors, 3, 10);
        assert_eq!(map["c1"], vec!["t1", "t1", "t1"]);
    }

    #[tokio::test]
    async fn test_rebalance_filters_stale() {
        let state = test_state(vec![
            "t1".to_string(),
            "t2".to_string(),
            "t3".to_string(),
            "t4".to_string(),
        ]);
        {
            let mut collectors = state.collectors.write().await;
            collectors.insert(
                "c1".to_string(),
                CollectorInfo {
                    last_heartbeat: Instant::now(),
                },
            );
            collectors.insert(
                "c2".to_string(),
                CollectorInfo {
                    last_heartbeat: Instant::now() - Duration::from_secs(120),
                },
            );
        }

        rebalance(&state).await;

        let assignments = state.assignments.read().await;
        assert!(assignments.contains_key("c1"));
        assert!(!assignments.contains_key("c2"));
        assert!(!assignments["c1"].is_empty());

        let replicas = state.market_replicas.read().await;
        assert_eq!(replicas.len(), 4);
    }

    #[tokio::test]
    async fn test_rebalance_increments_assignment_version_on_membership_changes() {
        let state = test_state(vec![
            "t1".to_string(),
            "t2".to_string(),
            "t3".to_string(),
            "t4".to_string(),
        ]);
        {
            let mut collectors = state.collectors.write().await;
            collectors.insert(
                "c1".to_string(),
                CollectorInfo {
                    last_heartbeat: Instant::now(),
                },
            );
        }

        rebalance(&state).await;
        let first_version = *state.assignment_version.read().await;
        assert_eq!(
            state.assignments.read().await["c1"],
            vec![
                "t1".to_string(),
                "t2".to_string(),
                "t3".to_string(),
                "t4".to_string()
            ]
        );

        {
            let mut collectors = state.collectors.write().await;
            collectors.insert(
                "c2".to_string(),
                CollectorInfo {
                    last_heartbeat: Instant::now(),
                },
            );
        }

        rebalance(&state).await;
        let second_version = *state.assignment_version.read().await;
        assert!(second_version > first_version);
        {
            let assignments = state.assignments.read().await;
            assert_eq!(assignments["c1"], vec!["t1".to_string(), "t2".to_string()]);
            assert_eq!(assignments["c2"], vec!["t3".to_string(), "t4".to_string()]);
        }

        state.collectors.write().await.remove("c2");
        rebalance(&state).await;
        let third_version = *state.assignment_version.read().await;
        assert!(third_version > second_version);
        let assignments = state.assignments.read().await;
        assert!(!assignments.contains_key("c2"));
        assert_eq!(
            assignments["c1"],
            vec![
                "t1".to_string(),
                "t2".to_string(),
                "t3".to_string(),
                "t4".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn test_merge_object_valid_jsonl() {
        let s3 = InMemoryS3Service::default();
        s3.put_object(
            "test-bucket",
            "key.jsonl",
            b"{\"a\":1}\n{\"b\":2}\n".to_vec(),
        )
        .await
        .unwrap();

        let dir = tempdir().unwrap();
        let output = dir.path().join("out.jsonl");
        let obj = S3Object {
            key: "key.jsonl".to_string(),
            etag: None,
            size: 0,
        };

        let lines = merge_object(&s3, "test-bucket", &obj, &output)
            .await
            .unwrap();
        assert_eq!(lines, 2);

        let content = tokio::fs::read_to_string(&output).await.unwrap();
        assert!(content.contains("\"a\":1"));
        assert!(content.contains("\"b\":2"));
    }

    #[tokio::test]
    async fn test_merge_object_malformed_lines() {
        let s3 = InMemoryS3Service::default();
        s3.put_object(
            "test-bucket",
            "key.jsonl",
            b"{\"a\":1}\nnot json\n{\"c\":3}\n".to_vec(),
        )
        .await
        .unwrap();

        let dir = tempdir().unwrap();
        let output = dir.path().join("out.jsonl");
        let obj = S3Object {
            key: "key.jsonl".to_string(),
            etag: None,
            size: 0,
        };

        let lines = merge_object(&s3, "test-bucket", &obj, &output)
            .await
            .unwrap();
        assert_eq!(lines, 2);

        let content = tokio::fs::read_to_string(&output).await.unwrap();
        assert!(content.contains("\"a\":1"));
        assert!(content.contains("\"c\":3"));
        assert!(!content.contains("not json"));
    }

    #[tokio::test]
    async fn test_merge_object_missing_object() {
        let s3 = InMemoryS3Service::default();
        let dir = tempdir().unwrap();
        let output = dir.path().join("out.jsonl");
        let obj = S3Object {
            key: "missing.jsonl".to_string(),
            etag: None,
            size: 0,
        };

        assert!(merge_object(&s3, "test-bucket", &obj, &output)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_register_handler_with_id() {
        let state = test_state(vec!["t1".to_string(), "t2".to_string()]);
        let before_version = *state.assignment_version.read().await;
        let resp = register_handler(
            State(state.clone()),
            Json(RegisterRequest {
                collector_id: Some("c1".to_string()),
            }),
        )
        .await;

        assert_eq!(resp.collector_id, "c1");
        let collectors = state.collectors.read().await;
        assert!(collectors.contains_key("c1"));
        drop(collectors);
        let assignments = state.assignments.read().await;
        assert!(assignments.contains_key("c1"));
        assert!(!assignments["c1"].is_empty());
        drop(assignments);
        assert!(*state.assignment_version.read().await > before_version);
    }

    #[tokio::test]
    async fn test_register_handler_without_id() {
        let state = test_state(vec!["t1".to_string()]);
        let resp = register_handler(
            State(state.clone()),
            Json(RegisterRequest { collector_id: None }),
        )
        .await;

        assert!(!resp.collector_id.is_empty());
        let collectors = state.collectors.read().await;
        assert!(collectors.contains_key(&resp.collector_id));
    }

    #[tokio::test]
    async fn test_heartbeat_handler_unknown() {
        let state = test_state(vec![]);
        let status =
            heartbeat_handler(State(state), axum::extract::Path("unknown".to_string())).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_heartbeat_handler_known() {
        let state = test_state(vec![]);
        {
            let mut collectors = state.collectors.write().await;
            collectors.insert(
                "c1".to_string(),
                CollectorInfo {
                    last_heartbeat: Instant::now() - Duration::from_secs(60),
                },
            );
        }
        let status =
            heartbeat_handler(State(state.clone()), axum::extract::Path("c1".to_string())).await;
        assert_eq!(status, StatusCode::OK);
        let collectors = state.collectors.read().await;
        let elapsed = Instant::now().duration_since(collectors["c1"].last_heartbeat);
        assert!(elapsed < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn test_assignment_handler_unassigned() {
        let state = test_state(vec!["t1".to_string()]);
        let resp =
            assignment_handler(State(state), axum::extract::Path("nobody".to_string())).await;
        assert!(resp.token_ids.is_empty());
        assert_eq!(resp.chunk_size, 2);
        assert_eq!(resp.version, 1);
    }

    #[tokio::test]
    async fn test_notify_handler_adds_pending_and_skips_processed() {
        let state = test_state(vec![]);
        let resp = notify_handler(
            State(state.clone()),
            Json(NotifyRequest {
                key: "k1.jsonl".to_string(),
                collector_id: Some("c1".to_string()),
            }),
        )
        .await;
        assert!(resp.received);

        {
            let pending = state.pending_files.lock().await;
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].key, "k1.jsonl");
        }

        state
            .processed_keys
            .write()
            .await
            .insert("k1.jsonl".to_string());

        let resp2 = notify_handler(
            State(state.clone()),
            Json(NotifyRequest {
                key: "k1.jsonl".to_string(),
                collector_id: None,
            }),
        )
        .await;
        assert!(resp2.received);

        let pending = state.pending_files.lock().await;
        assert_eq!(pending.len(), 1);
    }

    #[tokio::test]
    async fn test_run_spawns_server() {
        let dir = tempdir().unwrap();
        let output_path = dir.path().join("out.jsonl");
        let source = Arc::new(SequenceMarketSource::single(vec![sample_market(
            "m1",
            vec!["t1".to_string(), "t2".to_string()],
        )]));

        let handle = tokio::spawn(async move {
            let _ = run_with_market_source(
                "127.0.0.1:19090",
                output_path,
                "test-bucket".to_string(),
                "orderbook/".to_string(),
                1,
                Duration::from_secs(60),
                false,
                "us-east-1".to_string(),
                source,
                MarketRefreshPolicy::default(),
            )
            .await;
        });

        tokio::time::sleep(Duration::from_millis(500)).await;

        let client = reqwest::Client::new();
        let resp = client
            .post("http://127.0.0.1:19090/register")
            .json(&serde_json::json!({ "collector_id": "test" }))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());

        handle.abort();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn test_heartbeat_monitor_removes_stale() {
        let state = test_state(vec!["t1".to_string()]);
        {
            let mut collectors = state.collectors.write().await;
            collectors.insert(
                "c1".to_string(),
                CollectorInfo {
                    last_heartbeat: Instant::now() - Duration::from_secs(120),
                },
            );
        }

        // Spawn the monitor and let it run for one tick.
        let monitor_state = state.clone();
        let handle = tokio::spawn(async move {
            heartbeat_monitor(monitor_state).await;
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        handle.abort();
        let _ = handle.await;

        let collectors = state.collectors.read().await;
        assert!(!collectors.contains_key("c1"));
    }

    #[tokio::test]
    async fn test_merge_task_processes_pending() {
        let dir = tempdir().unwrap();
        let output_path = dir.path().join("out.jsonl");
        let s3 = Arc::new(InMemoryS3Service::default());
        s3.put_object("test-bucket", "orderbook/k1.jsonl", b"{\"a\":1}\n".to_vec())
            .await
            .unwrap();

        let state = AppState {
            market_registry: Arc::new(RwLock::new(test_registry(Vec::new()))),
            collectors: Arc::new(RwLock::new(HashMap::new())),
            assignments: Arc::new(RwLock::new(HashMap::new())),
            assignment_version: Arc::new(RwLock::new(1)),
            market_replicas: Arc::new(RwLock::new(HashMap::new())),
            pending_files: Arc::new(Mutex::new(vec![S3Object {
                key: "orderbook/k1.jsonl".to_string(),
                etag: None,
                size: 0,
            }])),
            processed_keys: Arc::new(RwLock::new(HashSet::new())),
            output_path: output_path.clone(),
            s3_bucket: "test-bucket".to_string(),
            s3_prefix: "orderbook/".to_string(),
            delete_after_merge: false,
            replication_factor: 1,
            heartbeat_timeout: Duration::from_secs(60),
            tokens_per_collector: 2,
        };

        let task_state = state.clone();
        let handle = tokio::spawn(async move {
            merge_task(task_state, s3).await;
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        handle.abort();
        let _ = handle.await;

        let content = tokio::fs::read_to_string(&output_path).await.unwrap();
        assert!(content.contains("\"a\":1"));
    }

    #[tokio::test]
    async fn test_merge_task_deletes_after_merge() {
        let dir = tempdir().unwrap();
        let output_path = dir.path().join("out.jsonl");
        let s3 = Arc::new(InMemoryS3Service::default());
        s3.put_object("test-bucket", "orderbook/k1.jsonl", b"{\"a\":1}\n".to_vec())
            .await
            .unwrap();

        let state = AppState {
            market_registry: Arc::new(RwLock::new(test_registry(Vec::new()))),
            collectors: Arc::new(RwLock::new(HashMap::new())),
            assignments: Arc::new(RwLock::new(HashMap::new())),
            assignment_version: Arc::new(RwLock::new(1)),
            market_replicas: Arc::new(RwLock::new(HashMap::new())),
            pending_files: Arc::new(Mutex::new(vec![S3Object {
                key: "orderbook/k1.jsonl".to_string(),
                etag: None,
                size: 0,
            }])),
            processed_keys: Arc::new(RwLock::new(HashSet::new())),
            output_path,
            s3_bucket: "test-bucket".to_string(),
            s3_prefix: "orderbook/".to_string(),
            delete_after_merge: true,
            replication_factor: 1,
            heartbeat_timeout: Duration::from_secs(60),
            tokens_per_collector: 2,
        };

        let task_state = state.clone();
        let handle = tokio::spawn(async move {
            merge_task(task_state, s3.clone()).await;
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        handle.abort();
        let _ = handle.await;

        let remaining = state.pending_files.lock().await;
        assert!(remaining.is_empty());
    }

    #[tokio::test]
    async fn test_heartbeat_monitor_keeps_healthy_collector() {
        let state = test_state(vec!["t1".to_string()]);
        {
            let mut collectors = state.collectors.write().await;
            collectors.insert(
                "c1".to_string(),
                CollectorInfo {
                    last_heartbeat: Instant::now(),
                },
            );
        }

        let monitor_state = state.clone();
        let handle = tokio::spawn(async move {
            heartbeat_monitor(monitor_state).await;
        });
        tokio::time::sleep(Duration::from_millis(1500)).await;
        handle.abort();
        let _ = handle.await;

        let collectors = state.collectors.read().await;
        assert!(collectors.contains_key("c1"));
    }

    #[tokio::test]
    async fn test_rebalance_no_healthy_collectors() {
        let state = test_state(vec!["t1".to_string(), "t2".to_string()]);
        {
            let mut collectors = state.collectors.write().await;
            collectors.insert(
                "c1".to_string(),
                CollectorInfo {
                    last_heartbeat: Instant::now(),
                },
            );
        }

        rebalance(&state).await;
        let before_assignments = state.assignments.read().await.clone();
        let before_replicas = state.market_replicas.read().await.clone();
        let before_version = *state.assignment_version.read().await;
        {
            let mut collectors = state.collectors.write().await;
            collectors.insert(
                "c1".to_string(),
                CollectorInfo {
                    last_heartbeat: Instant::now() - Duration::from_secs(120),
                },
            );
        }

        rebalance(&state).await;

        assert_eq!(*state.assignments.read().await, before_assignments);
        assert_eq!(*state.market_replicas.read().await, before_replicas);
        assert_eq!(*state.assignment_version.read().await, before_version);
    }

    /// S3 backend wrapper that can fail selected operations for coverage tests.
    struct FailingS3Service {
        pub inner: InMemoryS3Service,
        fail_get: Vec<String>,
        fail_list: bool,
        fail_delete: Vec<String>,
    }

    impl FailingS3Service {
        fn new(fail_get: Vec<String>, fail_list: bool, fail_delete: Vec<String>) -> Self {
            Self {
                inner: InMemoryS3Service::default(),
                fail_get,
                fail_list,
                fail_delete,
            }
        }
    }

    #[async_trait::async_trait]
    impl S3Service for FailingS3Service {
        async fn list_objects(&self, bucket: &str, prefix: &str) -> Result<Vec<S3Object>> {
            if self.fail_list {
                anyhow::bail!("list_objects failed");
            }
            self.inner.list_objects(bucket, prefix).await
        }

        async fn get_object(&self, bucket: &str, key: &str) -> Result<Vec<u8>> {
            if self.fail_get.contains(&key.to_string()) {
                anyhow::bail!("get_object failed for {}", key);
            }
            self.inner.get_object(bucket, key).await
        }

        async fn put_object(&self, bucket: &str, key: &str, body: Vec<u8>) -> Result<()> {
            self.inner.put_object(bucket, key, body).await
        }

        async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
            if self.fail_delete.contains(&key.to_string()) {
                anyhow::bail!("delete_object failed for {}", key);
            }
            self.inner.delete_object(bucket, key).await
        }
    }

    #[tokio::test]
    async fn test_merge_task_retries_failed_notified_object() {
        let dir = tempdir().unwrap();
        let output_path = dir.path().join("out.jsonl");
        let s3 = Arc::new(FailingS3Service::new(
            vec!["orderbook/k1.jsonl".to_string()],
            false,
            vec![],
        ));
        s3.inner
            .put_object("test-bucket", "orderbook/k1.jsonl", b"{\"a\":1}\n".to_vec())
            .await
            .unwrap();

        let state = AppState {
            market_registry: Arc::new(RwLock::new(test_registry(Vec::new()))),
            collectors: Arc::new(RwLock::new(HashMap::new())),
            assignments: Arc::new(RwLock::new(HashMap::new())),
            assignment_version: Arc::new(RwLock::new(1)),
            market_replicas: Arc::new(RwLock::new(HashMap::new())),
            pending_files: Arc::new(Mutex::new(vec![S3Object {
                key: "orderbook/k1.jsonl".to_string(),
                etag: None,
                size: 0,
            }])),
            processed_keys: Arc::new(RwLock::new(HashSet::new())),
            output_path,
            s3_bucket: "test-bucket".to_string(),
            s3_prefix: "orderbook/".to_string(),
            delete_after_merge: false,
            replication_factor: 1,
            heartbeat_timeout: Duration::from_secs(60),
            tokens_per_collector: 2,
        };

        let task_state = state.clone();
        let handle = tokio::spawn(async move {
            merge_task(task_state, s3).await;
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        handle.abort();
        let _ = handle.await;

        let pending = state.pending_files.lock().await;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].key, "orderbook/k1.jsonl");
    }

    #[tokio::test]
    async fn test_merge_task_polled_object_merge_failure() {
        let dir = tempdir().unwrap();
        let output_path = dir.path().join("out.jsonl");
        let s3 = Arc::new(FailingS3Service::new(
            vec!["orderbook/k1.jsonl".to_string()],
            false,
            vec![],
        ));
        s3.inner
            .put_object("test-bucket", "orderbook/k1.jsonl", b"{\"a\":1}\n".to_vec())
            .await
            .unwrap();

        let state = AppState {
            market_registry: Arc::new(RwLock::new(test_registry(Vec::new()))),
            collectors: Arc::new(RwLock::new(HashMap::new())),
            assignments: Arc::new(RwLock::new(HashMap::new())),
            assignment_version: Arc::new(RwLock::new(1)),
            market_replicas: Arc::new(RwLock::new(HashMap::new())),
            pending_files: Arc::new(Mutex::new(Vec::new())),
            processed_keys: Arc::new(RwLock::new(HashSet::new())),
            output_path,
            s3_bucket: "test-bucket".to_string(),
            s3_prefix: "orderbook/".to_string(),
            delete_after_merge: false,
            replication_factor: 1,
            heartbeat_timeout: Duration::from_secs(60),
            tokens_per_collector: 2,
        };

        let task_state = state.clone();
        let handle = tokio::spawn(async move {
            merge_task(task_state, s3).await;
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        handle.abort();
        let _ = handle.await;

        let processed = state.processed_keys.read().await;
        assert!(!processed.contains("orderbook/k1.jsonl"));
    }

    #[tokio::test]
    async fn test_merge_task_delete_failure_keeps_object() {
        let dir = tempdir().unwrap();
        let output_path = dir.path().join("out.jsonl");
        let s3 = Arc::new(FailingS3Service::new(
            vec![],
            false,
            vec!["orderbook/k1.jsonl".to_string()],
        ));
        s3.inner
            .put_object("test-bucket", "orderbook/k1.jsonl", b"{\"a\":1}\n".to_vec())
            .await
            .unwrap();

        let state = AppState {
            market_registry: Arc::new(RwLock::new(test_registry(Vec::new()))),
            collectors: Arc::new(RwLock::new(HashMap::new())),
            assignments: Arc::new(RwLock::new(HashMap::new())),
            assignment_version: Arc::new(RwLock::new(1)),
            market_replicas: Arc::new(RwLock::new(HashMap::new())),
            pending_files: Arc::new(Mutex::new(vec![S3Object {
                key: "orderbook/k1.jsonl".to_string(),
                etag: None,
                size: 0,
            }])),
            processed_keys: Arc::new(RwLock::new(HashSet::new())),
            output_path,
            s3_bucket: "test-bucket".to_string(),
            s3_prefix: "orderbook/".to_string(),
            delete_after_merge: true,
            replication_factor: 1,
            heartbeat_timeout: Duration::from_secs(60),
            tokens_per_collector: 2,
        };

        let s3_for_task = Arc::clone(&s3);
        let task_state = state.clone();
        let handle = tokio::spawn(async move {
            merge_task(task_state, s3_for_task).await;
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        handle.abort();
        let _ = handle.await;

        let processed = state.processed_keys.read().await;
        assert!(processed.contains("orderbook/k1.jsonl"));
        let remaining = s3
            .inner
            .list_objects("test-bucket", "orderbook/")
            .await
            .unwrap();
        assert_eq!(remaining.len(), 1);
    }

    #[tokio::test]
    async fn test_merge_object_skips_blank_lines() {
        let s3 = InMemoryS3Service::default();
        s3.put_object(
            "test-bucket",
            "key.jsonl",
            b"{\"a\":1}\n\n\n{\"b\":2}\n".to_vec(),
        )
        .await
        .unwrap();

        let dir = tempdir().unwrap();
        let output = dir.path().join("out.jsonl");
        let obj = S3Object {
            key: "key.jsonl".to_string(),
            etag: None,
            size: 0,
        };

        let lines = merge_object(&s3, "test-bucket", &obj, &output)
            .await
            .unwrap();
        assert_eq!(lines, 2);

        let content = tokio::fs::read_to_string(&output).await.unwrap();
        assert!(content.contains("\"a\":1"));
        assert!(content.contains("\"b\":2"));
    }

    #[tokio::test]
    async fn test_merge_object_open_error() {
        let s3 = InMemoryS3Service::default();
        s3.put_object("test-bucket", "key.jsonl", b"{\"a\":1}\n".to_vec())
            .await
            .unwrap();

        let dir = tempdir().unwrap();
        let output = dir.path().to_path_buf(); // directory, cannot open as file
        let obj = S3Object {
            key: "key.jsonl".to_string(),
            etag: None,
            size: 0,
        };

        assert!(merge_object(&s3, "test-bucket", &obj, &output)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_run_with_shutdown_creates_parent_and_serves() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .try_init();
        let dir = tempdir().unwrap();
        let output_path = dir.path().join("nested").join("out.jsonl");
        let source = Arc::new(SequenceMarketSource::single(vec![sample_market(
            "m1",
            vec!["t1".to_string(), "t2".to_string()],
        )]));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let bind = format!("127.0.0.1:{}", port);

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let request_handle = tokio::spawn({
            let bind = bind.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                let client = reqwest::Client::new();
                let resp = client
                    .post(format!("http://{}/register", bind))
                    .json(&serde_json::json!({ "collector_id": "test" }))
                    .send()
                    .await
                    .unwrap();
                assert!(resp.status().is_success());
                assert!(dir.path().join("nested").exists());
                let _ = shutdown_tx.send(());
            }
        });

        run_with_shutdown_and_market_source(
            &bind,
            output_path,
            "test-bucket".to_string(),
            "orderbook/".to_string(),
            1,
            Duration::from_secs(60),
            false,
            "us-east-1".to_string(),
            source,
            MarketRefreshPolicy::default(),
            async {
                shutdown_rx.await.ok();
            },
        )
        .await
        .unwrap();

        request_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_merge_task_list_objects_failure() {
        let dir = tempdir().unwrap();
        let output_path = dir.path().join("out.jsonl");
        let s3 = Arc::new(FailingS3Service::new(vec![], true, vec![]));

        let state = AppState {
            market_registry: Arc::new(RwLock::new(test_registry(Vec::new()))),
            collectors: Arc::new(RwLock::new(HashMap::new())),
            assignments: Arc::new(RwLock::new(HashMap::new())),
            assignment_version: Arc::new(RwLock::new(1)),
            market_replicas: Arc::new(RwLock::new(HashMap::new())),
            pending_files: Arc::new(Mutex::new(Vec::new())),
            processed_keys: Arc::new(RwLock::new(HashSet::new())),
            output_path,
            s3_bucket: "test-bucket".to_string(),
            s3_prefix: "orderbook/".to_string(),
            delete_after_merge: false,
            replication_factor: 1,
            heartbeat_timeout: Duration::from_secs(60),
            tokens_per_collector: 2,
        };

        let task_state = state.clone();
        let handle = tokio::spawn(async move {
            merge_task(task_state, s3).await;
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        handle.abort();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn test_build_app_state_returns_state() {
        let state = build_app_state(
            test_registry(vec!["t1".to_string()]),
            PathBuf::from("/tmp/out.jsonl"),
            "bucket".to_string(),
            "prefix/".to_string(),
            2,
            Duration::from_secs(30),
            true,
        );
        assert_eq!(
            state.market_registry.read().await.snapshot().token_ids,
            vec!["t1".to_string()]
        );
        assert_eq!(state.s3_bucket, "bucket");
        assert_eq!(state.replication_factor, 2);
        assert!(state.delete_after_merge);
    }

    #[tokio::test]
    async fn test_start_server_serves_and_shuts_down() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .try_init();
        let dir = tempdir().unwrap();
        let output_path = dir.path().join("out.jsonl");
        let state = build_app_state(
            test_registry(vec!["t1".to_string()]),
            output_path,
            "bucket".to_string(),
            "prefix/".to_string(),
            1,
            Duration::from_secs(60),
            false,
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let bind = format!("127.0.0.1:{}", port);

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bind_for_request = bind.clone();
        let request_handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let client = reqwest::Client::new();
            let resp = client
                .post(format!("http://{}/register", bind_for_request))
                .json(&serde_json::json!({ "collector_id": "test" }))
                .send()
                .await
                .unwrap();
            assert!(resp.status().is_success());
            let _ = shutdown_tx.send(());
        });

        start_server(&bind, state, "us-east-1".to_string(), None, async {
            shutdown_rx.await.ok();
        })
        .await
        .unwrap();

        request_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_run_with_shutdown_create_dir_all_error() {
        let dir = tempdir().unwrap();
        let markets_path = dir.path().join("markets.jsonl");
        std::fs::write(
            &markets_path,
            format!("{}\n", sample_market_json("m1", &["t1"])),
        )
        .unwrap();

        // Make the parent a file so create_dir_all fails.
        let parent_as_file = dir.path().join("parent_file");
        std::fs::write(&parent_as_file, "x").unwrap();
        let output_path = parent_as_file.join("out.jsonl");

        let err = run_with_shutdown(
            "127.0.0.1:0",
            output_path,
            markets_path,
            "test-bucket".to_string(),
            "orderbook/".to_string(),
            1,
            Duration::from_secs(60),
            false,
            "us-east-1".to_string(),
            std::future::pending(),
        )
        .await;

        assert!(err.is_err());
    }
}
