use anyhow::{Context, Result};
use axum::{
    extract::{DefaultBodyLimit, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, RwLock};
use tokio::time::interval;
use tracing::{info, warn};
use uuid::Uuid;

use crate::aggregate_s3::{AwsS3Service, S3Object, S3Service};
use crate::market_discovery::Market;

/// State for a single registered collector.
#[derive(Debug, Clone)]
struct CollectorInfo {
    last_heartbeat: Instant,
}

/// Shared aggregator state.
#[derive(Clone)]
struct AppState {
    /// Flattened list of all token IDs across all markets.
    token_ids: Vec<String>,
    /// Registered collectors keyed by collector ID.
    collectors: Arc<RwLock<HashMap<String, CollectorInfo>>>,
    /// Current token assignment: collector_id -> list of token IDs.
    assignments: Arc<RwLock<HashMap<String, Vec<String>>>>,
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

    info!(markets = markets.len(), tokens = token_ids.len(), "Loaded markets");
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
async fn rebalance(state: &AppState) {
    let collectors = state.collectors.read().await;
    let now = Instant::now();

    // Filter out stale collectors.
    let healthy: Vec<String> = collectors
        .iter()
        .filter(|(_, info)| now.duration_since(info.last_heartbeat) <= state.heartbeat_timeout)
        .map(|(id, _)| id.clone())
        .collect();

    drop(collectors);

    if healthy.is_empty() {
        warn!("No healthy collectors, skipping rebalance");
        return;
    }

    let new_assignments = allocate_tokens(
        &state.token_ids,
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
    *assignments_guard = new_assignments;
    drop(assignments_guard);

    let mut replicas_guard = state.market_replicas.write().await;
    *replicas_guard = market_replicas;
    drop(replicas_guard);

    info!(healthy = healthy.len(), "Rebalanced assignments");
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
                    match merge_object(s3.as_ref(), &state.s3_bucket, &obj, &state.output_path).await
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
    let id = req.collector_id.unwrap_or_else(|| Uuid::new_v4().to_string());

    let mut collectors = state.collectors.write().await;
    collectors.insert(id.clone(), CollectorInfo { last_heartbeat: Instant::now() });
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
    let token_ids = assignments
        .get(&collector_id)
        .cloned()
        .unwrap_or_default();

    Json(AssignmentResponse {
        token_ids,
        chunk_size: state.tokens_per_collector,
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
/// # Arguments
/// * `bind` — Socket address to bind, e.g. `"0.0.0.0:8080"`
/// * `output_path` — Local path for the merged JSONL output
/// * `markets_path` — Path to `markets.jsonl` used for market/token discovery
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
/// // Logs: "Loaded markets", "Rebalanced assignments", "Aggregator listening"
/// ```
#[allow(clippy::too_many_arguments)]
pub async fn run(
    bind: &str,
    output_path: PathBuf,
    markets_path: PathBuf,
    s3_bucket: String,
    s3_prefix: String,
    replication_factor: usize,
    heartbeat_timeout: Duration,
    delete_after_merge: bool,
    region: String,
) -> Result<()> {
    // Ensure output directory exists.
    if let Some(parent) = output_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let (markets, token_ids) = load_markets(&markets_path)?;
    let tokens_per_collector = 100; // Default max tokens per collector worker chunk.

    info!(
        bind = %bind,
        output = %output_path.display(),
        markets = markets.len(),
        tokens = token_ids.len(),
        replication_factor,
        ?heartbeat_timeout,
        "Starting orchestrated aggregator"
    );

    let state = AppState {
        token_ids,
        collectors: Arc::new(RwLock::new(HashMap::new())),
        assignments: Arc::new(RwLock::new(HashMap::new())),
        market_replicas: Arc::new(RwLock::new(HashMap::new())),
        pending_files: Arc::new(Mutex::new(Vec::new())),
        processed_keys: Arc::new(RwLock::new(HashSet::new())),
        output_path,
        s3_bucket,
        s3_prefix,
        delete_after_merge,
        replication_factor,
        heartbeat_timeout,
        tokens_per_collector,
    };

    // Spawn heartbeat monitor background task.
    let monitor_state = state.clone();
    tokio::spawn(async move {
        heartbeat_monitor(monitor_state).await;
    });

    // Spawn S3 merge background task.
    let s3 = Arc::new(AwsS3Service::new(region).await);
    let merge_state = state.clone();
    tokio::spawn(async move {
        merge_task(merge_state, s3).await;
    });

    let app = Router::new()
        .route("/register", post(register_handler))
        .route("/heartbeat/:collector_id", post(heartbeat_handler))
        .route("/assignment/:collector_id", get(assignment_handler))
        .route("/notify", post(notify_handler))
        .layer(DefaultBodyLimit::max(50 * 1024 * 1024))
        .with_state(state);

    info!(bind = %bind, "Aggregator listening");
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
