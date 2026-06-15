use crate::http_client::{HttpClient, ReqwestHttpClient};
use anyhow::Result;
use axum::{
    extract::{Path as AxumPath, State},
    response::Html,
    routing::get,
    Json, Router,
};
use rayon::prelude::*;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

// ─────────────────────────────────────────────────────────────────────────────
// Data Models
// ─────────────────────────────────────────────────────────────────────────────

/// A single price level in the orderbook (bid or ask).
///
/// # Example — Input / Output
/// ```rust,ignore
/// let level = Level { price: 0.084, size: 110.476189 };
/// // Serialized as JSON: {"price":0.084,"size":110.476189}
/// ```
#[derive(Debug, Clone, Serialize)]
struct Level {
    price: f64,
    size: f64,
}

/// A snapshot of the full orderbook at a specific moment in time.
///
/// Constructed from WebSocket `book` events by parsing the `raw` JSON field.
#[derive(Debug, Clone, Serialize, Default)]
struct BookSnapshot {
    bids: Vec<Level>,
    asks: Vec<Level>,
}

/// A single price point from a `price_change` event.
///
/// Represents a midpoint or mark price update rather than an actual trade.
#[derive(Debug, Clone, Serialize)]
struct PricePoint {
    timestamp: i64,
    price: f64,
    side: Option<String>,
}

/// A single trade (fill) from a `last_trade` event.
///
/// Contains the on-chain transaction hash so we can later look up
/// the maker/taker addresses via Polygon RPC.
#[derive(Debug, Clone, Serialize)]
struct TradePoint {
    timestamp: i64,
    price: f64,
    size: Option<f64>,
    side: Option<String>,
    /// Polygon transaction hash, if available from the WebSocket event.
    /// Example: `"0x5e5fe7c64a30b1d23366bf508ea288b994e3b3d8d5afd5facd991af8551dae02"`
    tx_hash: Option<String>,
}

/// Aggregated view of all data for a single market (token/asset).
#[derive(Debug, Clone, Serialize, Default)]
struct MarketView {
    asset: String,
    book_snapshots: Vec<(i64, BookSnapshot)>,
    price_history: Vec<PricePoint>,
    trades: Vec<TradePoint>,
}

/// The top-level data container shared across all API handlers.
#[derive(Default)]
struct ViewerData {
    markets: HashMap<String, MarketView>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Application State
// ─────────────────────────────────────────────────────────────────────────────

/// Shared application state for the Axum server.
///
/// Passed to every handler via `State` extractor. Contains:
/// - `data`: Pre-loaded orderbook/trade data (read-heavy, RwLock for async read access)
/// - `rpc_client`: HTTP client for Polygon RPC calls
/// - `rpc_url`: Polygon RPC endpoint (e.g., `"https://polygon.drpc.org"`)
/// - `tx_cache`: In-memory cache to avoid repeated RPC calls for the same tx hash
#[derive(Clone)]
struct AppState {
    data: Arc<RwLock<ViewerData>>,
    rpc_client: Arc<dyn HttpClient>,
    rpc_url: String,
    tx_cache: Arc<RwLock<HashMap<String, Option<crate::onchain::OnchainTrade>>>>,
}

/// Thread-local accumulator used during parallel data loading.
///
/// Each Rayon worker thread builds its own `LocalMarketData` map,
/// which are later merged into the global `ViewerData`.
#[derive(Default)]
struct LocalMarketData {
    book_snapshots: Vec<(i64, BookSnapshot)>,
    price_history: Vec<PricePoint>,
    trades: Vec<TradePoint>,
    /// Deduplication set for book snapshot timestamps.
    /// Prevents parsing the same snapshot multiple times if duplicate events exist.
    seen_ts: HashSet<i64>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper Functions
// ─────────────────────────────────────────────────────────────────────────────

/// Parse a `serde_json::Value` into an `f64`, handling both numeric and string representations.
///
/// Polymarket's WebSocket API is inconsistent: some fields are numbers (`0.05`),
/// others are strings (`"0.05"`). This helper normalizes both cases.
///
/// # Arguments
/// * `v` — An optional reference to a JSON value
///
/// # Returns
/// `Some(f64)` if parsing succeeds, `None` otherwise.
///
/// # Example — Input / Output
/// ```rust,ignore
/// let num = serde_json::json!(0.05);
/// let str = serde_json::json!("0.05");
///
/// assert_eq!(parse_f64(Some(&num)), Some(0.05));
/// assert_eq!(parse_f64(Some(&str)), Some(0.05));
/// assert_eq!(parse_f64(None), None);
/// ```
fn parse_f64(v: Option<&serde_json::Value>) -> Option<f64> {
    v.and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
}

// ─────────────────────────────────────────────────────────────────────────────
// Data Loading
// ─────────────────────────────────────────────────────────────────────────────

/// Load and parse the aggregated JSONL file into `ViewerData`.
///
/// This is the core data ingestion step. It reads all lines from the input file,
/// parses each `OrderbookEvent` in parallel using Rayon, groups events by asset,
/// and reconstructs orderbook snapshots, price history, and trade lists.
///
/// # Parallel Strategy
/// 1. Read all lines into a `Vec<String>` sequentially (I/O bound).
/// 2. Split into `num_cpus` chunks and process each chunk in parallel (CPU bound).
/// 3. Each thread builds a local `HashMap<String, LocalMarketData>`.
/// 4. Merge all local maps into the global `ViewerData`.
///
/// # Deduplication
/// - `book` events are deduplicated by `(timestamp, asset)` using `seen_ts` HashSet.
///   This avoids parsing the same snapshot multiple times if duplicate events exist.
/// - Final cross-thread dedup is applied after merging.
///
/// # Arguments
/// * `path` — Path to the aggregated `.jsonl` file
///
/// # Returns
/// `Ok(ViewerData)` containing all parsed markets, or an I/O / parse error.
///
/// # Example — Input / Output
/// ```rust,ignore
/// // Input file (data/aggregated.jsonl):
/// // {"event_type":"book","asset":"123...","timestamp":1781051970699,"raw":"{\"bids\":...}",...}
/// // {"event_type":"last_trade","asset":"123...","timestamp":1781051970699,"raw":"{\"price\":\"0.084\"}",...}
///
/// let data = load_data(Path::new("data/aggregated.jsonl")).unwrap();
///
/// // Output: ViewerData with markets HashMap populated
/// assert!(data.markets.contains_key("123..."));
/// let market = data.markets.get("123...").unwrap();
/// assert!(!market.book_snapshots.is_empty());
/// assert!(!market.trades.is_empty());
/// ```
fn load_data(path: &Path) -> Result<ViewerData> {
    info!(path = %path.display(), "Loading viewer data (parallel)");
    let start = std::time::Instant::now();

    // Step 1: Read all lines sequentially. I/O is not parallelizable here
    // because we're reading from a single file on a single disk.
    let file = std::fs::File::open(path)?;
    let reader = BufReader::new(file);
    let lines: Vec<String> = reader.lines().collect::<Result<Vec<_>, _>>()?;
    let _line_count = lines.len();

    // Step 2: Determine chunk size for parallel processing.
    // Each CPU core gets roughly `lines / num_cpus` lines to process independently.
    let num_cpus = std::thread::available_parallelism()?.get();
    let chunk_size = std::cmp::max(1, lines.len().div_ceil(num_cpus));

    // Step 3: Parallel map over chunks using Rayon.
    // Each chunk produces a local HashMap<asset, LocalMarketData>.
    let local_maps: Vec<HashMap<String, LocalMarketData>> = lines
        .par_chunks(chunk_size)
        .map(|chunk| {
            let mut local = HashMap::new();
            for line in chunk {
                let ev: crate::ws_orderbook::OrderbookEvent = match serde_json::from_str(line) {
                    Ok(e) => e,
                    Err(e) => {
                        warn!(error = %e, "Skipping malformed line");
                        continue;
                    }
                };
                if ev.asset.is_empty() {
                    continue;
                }

                let entry = local
                    .entry(ev.asset.clone())
                    .or_insert_with(LocalMarketData::default);

                match ev.event_type.as_str() {
                    // ── Book Snapshot Events ─────────────────────────────
                    // These contain the full orderbook state (bids[] + asks[]).
                    // We parse the `raw` JSON field to extract price/size levels.
                    // Deduplication by timestamp prevents parsing the same snapshot twice.
                    "book" => {
                        // Only process if we haven't seen this timestamp before.
                        if entry.seen_ts.insert(ev.timestamp) {
                            if let Ok(raw_msg) =
                                serde_json::from_str::<serde_json::Value>(&ev.raw)
                            {
                                let bids = raw_msg
                                    .get("bids")
                                    .and_then(|v| v.as_array())
                                    .cloned()
                                    .unwrap_or_default();
                                let asks = raw_msg
                                    .get("asks")
                                    .and_then(|v| v.as_array())
                                    .cloned()
                                    .unwrap_or_default();

                                let snapshot = BookSnapshot {
                                    bids: bids
                                        .iter()
                                        .map(|b| Level {
                                            price: parse_f64(b.get("price")).unwrap_or(0.0),
                                            size: parse_f64(b.get("size")).unwrap_or(0.0),
                                        })
                                        .collect(),
                                    asks: asks
                                        .iter()
                                        .map(|a| Level {
                                            price: parse_f64(a.get("price")).unwrap_or(0.0),
                                            size: parse_f64(a.get("size")).unwrap_or(0.0),
                                        })
                                        .collect(),
                                };
                                entry.book_snapshots.push((ev.timestamp, snapshot));
                            }
                        }
                    }
                    // ── Price Change Events ──────────────────────────────
                    // Midpoint price updates. Not actual trades.
                    "price_change" => {
                        if let Some(price) = ev.price {
                            entry.price_history.push(PricePoint {
                                timestamp: ev.timestamp,
                                price,
                                side: ev.side.clone(),
                            });
                        }
                    }
                    // ── Last Trade Events ────────────────────────────────
                    // Actual on-chain fills. We extract side and tx_hash from the `raw`
                    // field as fallback, since older data may have `side: null` due to
                    // a prior bug in the collector.
                    "last_trade" => {
                        let raw_json = serde_json::from_str::<serde_json::Value>(&ev.raw).ok();

                        // Fallback: parse side from raw if not already extracted by collector.
                        let side = ev.side.clone().or_else(|| {
                            raw_json.as_ref()
                                .and_then(|v| v.get("side").and_then(|s| s.as_str()).map(|s| s.to_string()))
                        });

                        // Extract transaction hash from raw for on-chain lookup.
                        // The WebSocket last_trade_price event includes this field.
                        let tx_hash = raw_json
                            .and_then(|v| v.get("transaction_hash").and_then(|t| t.as_str()).map(|s| s.to_string()));

                        entry.trades.push(TradePoint {
                            timestamp: ev.timestamp,
                            price: ev.price.unwrap_or(0.0),
                            size: ev.size,
                            side,
                            tx_hash,
                        });
                    }
                    _ => {}
                }
            }
            local
        })
        .collect();

    // Step 4: Merge local maps from all threads into a single ViewerData.
    let mut data = ViewerData::default();
    for local in local_maps {
        for (asset, local_view) in local {
            let entry = data
                .markets
                .entry(asset.clone())
                .or_insert_with(|| MarketView {
                    asset,
                    ..Default::default()
                });
            entry.book_snapshots.extend(local_view.book_snapshots);
            entry.price_history.extend(local_view.price_history);
            entry.trades.extend(local_view.trades);
        }
    }

    // Step 5: Sort and final dedup (cross-thread duplicates may exist).
    for mv in data.markets.values_mut() {
        mv.book_snapshots.sort_by_key(|(ts, _)| *ts);
        let mut seen = HashSet::new();
        mv.book_snapshots.retain(|(ts, _)| seen.insert(*ts));
        mv.price_history.sort_by_key(|p| p.timestamp);
        mv.trades.sort_by_key(|t| t.timestamp);
    }

    let _elapsed = start.elapsed();
    info!("Viewer data loaded");
    Ok(data)
}

// ─────────────────────────────────────────────────────────────────────────────
// API Response Types
// ─────────────────────────────────────────────────────────────────────────────

/// JSON response wrapper for `/api/book_snapshots/:asset`.
#[derive(Serialize)]
struct BookSnapshotsResponse {
    snapshots: Vec<BookSnapshotItem>,
}

/// A single book snapshot item in the API response, with explicit timestamp.
#[derive(Serialize)]
struct BookSnapshotItem {
    timestamp: i64,
    bids: Vec<Level>,
    asks: Vec<Level>,
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP Handlers
// ─────────────────────────────────────────────────────────────────────────────

/// Serve the embedded HTML frontend.
///
/// Returns the single-page application HTML/JS/CSS bundle compiled into the binary
/// via `include_str!("viewer.html")`.
///
/// # Example — Input / Output
/// ```text
/// // Request
/// GET /
///
/// // Response (Content-Type: text/html)
/// <!DOCTYPE html>
/// <html>
/// <head>...Polymarket Orderbook Viewer...</head>
/// <body>...</body>
/// </html>
/// ```
async fn index_handler() -> Html<&'static str> {
    Html(include_str!("viewer.html"))
}

/// GET /api/markets — Return the list of all available market asset IDs.
///
/// # Example — Input / Output
/// ```text
/// // Request
/// GET /api/markets
///
/// // Response (Content-Type: application/json)
/// ["40073700561695212653451049120779209383948898865772011302940523990213422296817",
///  "64703998724474008677827057135436893758254552168142785204605792475717308499827"]
/// ```
async fn markets_handler(State(state): State<AppState>) -> Json<Vec<String>> {
    let data = state.data.read().await;
    let mut markets: Vec<String> = data.markets.keys().cloned().collect();
    markets.sort();
    Json(markets)
}

/// GET /api/book_snapshots/:asset — Return all book snapshots for a given asset.
///
/// Snapshots are sorted by timestamp ascending. The frontend uses these
/// to drive the timeline slider and render the orderbook at each point.
///
/// # Example — Input / Output
/// ```text
/// // Request
/// GET /api/book_snapshots/40073700561695212653451049120779209383948898865772011302940523990213422296817
///
/// // Response
/// {
///   "snapshots": [
///     {
///       "timestamp": 1781051970699,
///       "bids": [{"price": 0.15, "size": 197542.14}, ...],
///       "asks": [{"price": 0.16, "size": 12567.06}, ...]
///     },
///     ...
///   ]
/// }
/// ```
async fn book_snapshots_handler(
    State(state): State<AppState>,
    AxumPath(asset): AxumPath<String>,
) -> Json<BookSnapshotsResponse> {
    let data = state.data.read().await;
    let snapshots = data
        .markets
        .get(&asset)
        .map(|m| {
            m.book_snapshots
                .iter()
                .map(|(ts, snap)| BookSnapshotItem {
                    timestamp: *ts,
                    bids: snap.bids.clone(),
                    asks: snap.asks.clone(),
                })
                .collect()
        })
        .unwrap_or_default();
    Json(BookSnapshotsResponse { snapshots })
}

/// GET /api/trades/:asset — Return all trades for a given asset.
///
/// Trades include timestamp, price, size, side (BUY/SELL), and optional tx_hash.
/// The frontend filters these client-side by the selected timeline position.
///
/// # Example — Input / Output
/// ```text
/// // Request
/// GET /api/trades/40073700561695212653451049120779209383948898865772011302940523990213422296817
///
/// // Response
/// [
///   {
///     "timestamp": 1781051970699,
///     "price": 0.084,
///     "size": 110.476189,
///     "side": "BUY",
///     "tx_hash": "0x5e5fe7c64a30b1d23366bf508ea288b994e3b3d8d5afd5facd991af8551dae02"
///   },
///   ...
/// ]
/// ```
async fn trades_handler(
    State(state): State<AppState>,
    AxumPath(asset): AxumPath<String>,
) -> Json<Vec<TradePoint>> {
    let data = state.data.read().await;
    Json(
        data.markets
            .get(&asset)
            .map(|m| m.trades.clone())
            .unwrap_or_default(),
    )
}

/// GET /api/price_history/:asset — Return price change history for a given asset.
///
/// # Example — Input / Output
/// ```text
/// // Request
/// GET /api/price_history/40073700561695212653451049120779209383948898865772011302940523990213422296817
///
/// // Response
/// [
///   {"timestamp": 1781051970699, "price": 0.084, "side": "BUY"},
///   {"timestamp": 1781051977889, "price": 0.084, "side": "BUY"},
///   ...
/// ]
/// ```
async fn price_history_handler(
    State(state): State<AppState>,
    AxumPath(asset): AxumPath<String>,
) -> Json<Vec<PricePoint>> {
    let data = state.data.read().await;
    Json(
        data.markets
            .get(&asset)
            .map(|m| m.price_history.clone())
            .unwrap_or_default(),
    )
}

/// GET /api/trade_detail/:tx_hash — Look up on-chain details for a specific trade.
///
/// Queries the Polygon RPC for the transaction receipt, searches for an
/// `OrderFilled` event within the receipt logs, and returns the decoded
/// maker/taker addresses, block number, and other metadata.
///
/// # Caching
/// Results are cached in an in-memory HashMap to avoid repeated RPC calls
/// for the same transaction hash. Cache lives for the duration of the server process.
///
/// # Arguments
/// * `tx_hash` — The Polygon transaction hash (0x-prefixed, 66 chars)
///
/// # Returns
/// `Json<Some(OnchainTrade)>` if an OrderFilled event is found and parsed.
/// `Json<None>` if the tx has no matching OrderFilled log or the RPC fails.
///
/// # Example — Input / Output
/// ```text
/// // Request
/// GET /api/trade_detail/0x5e5fe7c64a30b1d23366bf508ea288b994e3b3d8d5afd5facd991af8551dae02
///
/// // Response
/// {
///   "order_hash": "0xd980fee1...d473",
///   "maker": "0x448861155279dbf833d041b963e3ac854599e319",
///   "taker": "0x6f3c1ddc97c9abfb38ff0f1302a56a1946d04c6f",
///   "side": 0,
///   "token_id": "0x66fc627fc41c09ce984d8db2aa4dcc8102d201581e73ad410b142f209832e207",
///   "block_number": 88231325,
///   "transaction_hash": "0x5e5fe7c64a30b1d23366bf508ea288b994e3b3d8d5afd5facd991af8551dae02"
/// }
/// ```
async fn trade_detail_handler(
    State(state): State<AppState>,
    AxumPath(tx_hash): AxumPath<String>,
) -> Json<Option<crate::onchain::OnchainTrade>> {
    // Step 1: Check in-memory cache to avoid redundant RPC calls.
    {
        let cache = state.tx_cache.read().await;
        if let Some(cached) = cache.get(&tx_hash) {
            return Json(cached.clone());
        }
    }

    // Step 2: Fetch the transaction receipt from Polygon RPC.
    let result = match crate::onchain::get_receipt_logs_with_client(
        state.rpc_client.as_ref(),
        &state.rpc_url,
        &tx_hash,
    )
    .await
    {
        Ok(logs) => crate::onchain::find_order_filled(&logs),
        Err(e) => {
            warn!(tx = %tx_hash, error = %e, "Failed to fetch receipt");
            None
        }
    };

    // Step 3: Store result in cache (even if None, to prevent retry storms).
    {
        let mut cache = state.tx_cache.write().await;
        cache.insert(tx_hash, result.clone());
    }

    Json(result)
}

// ─────────────────────────────────────────────────────────────────────────────
// Server Entry Point
// ─────────────────────────────────────────────────────────────────────────────

/// Start the axum web viewer server.
///
/// # Arguments
/// * `input_path` — Path to the aggregated JSONL file to visualize
/// * `bind` — Socket address to bind, e.g. `"127.0.0.1:3001"`
/// * `rpc_url` — Optional Polygon RPC endpoint for on-chain trade lookups.
///   Defaults to `"https://polygon.drpc.org"` if not provided.
///
/// # Data Loading
/// The JSONL file is loaded once at startup in a blocking thread pool
/// (`tokio::task::spawn_blocking`), then served from an in-memory HashMap.
/// This allows sub-second load times for ~1M events when using parallel parsing.
///
/// # Example — Input / Output
/// ```rust,ignore
/// // Input: 800k-line aggregated file
/// viewer::run(
///     Path::new("data/longrun_aggregated.jsonl"),
///     "127.0.0.1:3001",
///     Some("https://polygon.drpc.org".to_string()),
/// ).await.unwrap();
///
/// // Output: axum server listening on 127.0.0.1:3001
/// // Logs: "Viewer data loaded" markets=8 lines=808696 elapsed=16.2s
/// ```
/// Start the axum web viewer server with a graceful shutdown signal.
///
/// # Arguments
/// * `input_path` — Path to the aggregated JSONL file to visualize
/// * `bind` — Socket address to bind, e.g. `"127.0.0.1:3001"`
/// * `rpc_url` — Optional Polygon RPC endpoint for on-chain trade lookups.
/// * `shutdown` — Future that resolves when the server should shut down.
///
/// # Example — Input / Output
/// ```rust,ignore
/// // Input: 800k-line aggregated file
/// viewer::run_with_shutdown(
///     Path::new("data/longrun_aggregated.jsonl"),
///     "127.0.0.1:3001",
///     Some("https://polygon.drpc.org".to_string()),
///     tokio::signal::ctrl_c(),
/// ).await.unwrap();
/// ```
fn build_state(data: ViewerData, rpc_url: Option<String>) -> AppState {
    AppState {
        data: Arc::new(RwLock::new(data)),
        rpc_client: Arc::new(ReqwestHttpClient::new()),
        rpc_url: rpc_url.unwrap_or_else(|| "https://polygon.drpc.org".to_string()),
        tx_cache: Arc::new(RwLock::new(HashMap::new())),
    }
}

fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/", get(index_handler))
        .route("/api/markets", get(markets_handler))
        .route("/api/book_snapshots/:asset", get(book_snapshots_handler))
        .route("/api/trades/:asset", get(trades_handler))
        .route("/api/price_history/:asset", get(price_history_handler))
        .route("/api/trade_detail/:tx_hash", get(trade_detail_handler))
        .with_state(state)
}

async fn serve(
    app: Router,
    listener: tokio::net::TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    axum::serve(listener, app).with_graceful_shutdown(shutdown).await?;
    Ok(())
}

async fn run_with_app(
    data: ViewerData,
    listener: tokio::net::TcpListener,
    rpc_url: Option<String>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let state = build_state(data, rpc_url);
    let app = build_app(state);

    info!("Viewer listening");
    serve(app, listener, shutdown).await?;
    Ok(())
}

pub async fn run_with_shutdown(
    input_path: &Path,
    bind: &str,
    rpc_url: Option<String>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let path = input_path.to_path_buf();

    // Load data off the async runtime to avoid blocking the event loop.
    // For 800k lines this takes ~16s with Rayon parallel parsing.
    let data = tokio::task::spawn_blocking(move || load_data(&path))
        .await
        .expect("spawn_blocking failed")?;

    run_with_app(data, tokio::net::TcpListener::bind(bind).await?, rpc_url, shutdown).await
}

/// Start the axum web viewer server.
///
/// This is a thin wrapper around [`run_with_shutdown`] that uses
/// `tokio::signal::ctrl_c()` as the shutdown signal.
pub async fn run(input_path: &Path, bind: &str, rpc_url: Option<String>) -> Result<()> {
    run_with_shutdown(
        input_path,
        bind,
        rpc_url,
        async {
            let _ = tokio::signal::ctrl_c().await;
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_client::{HttpResponse, InMemoryHttpClient};
    use crate::onchain::{OnchainTrade, NEG_RISK_CTF_EXCHANGE_V2, ORDER_FILLED_TOPIC};
    use crate::ws_orderbook::OrderbookEvent;
    use axum::body::{to_bytes, Body};
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::tempdir;
    use tower::ServiceExt;

    fn test_state_with_data(data: ViewerData) -> AppState {
        AppState {
            data: Arc::new(RwLock::new(data)),
            rpc_client: Arc::new(InMemoryHttpClient::new()),
            rpc_url: "https://polygon-rpc.com".to_string(),
            tx_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn sample_book_event(asset: &str, ts: i64) -> OrderbookEvent {
        OrderbookEvent {
            event_type: "book".to_string(),
            asset: asset.to_string(),
            side: None,
            price: None,
            size: None,
            timestamp: ts,
            received_at: ts,
            raw: json!({
                "bids": [{"price": "0.1", "size": 100.0}],
                "asks": [{"price": 0.2, "size": "50"}],
            })
            .to_string(),
            worker_id: 0,
        }
    }

    fn sample_price_change_event(asset: &str, ts: i64) -> OrderbookEvent {
        OrderbookEvent {
            event_type: "price_change".to_string(),
            asset: asset.to_string(),
            side: Some("BUY".to_string()),
            price: Some(0.15),
            size: None,
            timestamp: ts,
            received_at: ts,
            raw: "".to_string(),
            worker_id: 0,
        }
    }

    fn sample_last_trade_event(asset: &str, ts: i64) -> OrderbookEvent {
        OrderbookEvent {
            event_type: "last_trade".to_string(),
            asset: asset.to_string(),
            side: Some("SELL".to_string()),
            price: Some(0.15),
            size: Some(10.0),
            timestamp: ts,
            received_at: ts,
            raw: json!({
                "transaction_hash": "0x5e5fe7c64a30b1d23366bf508ea288b994e3b3d8d5afd5facd991af8551dae02"
            })
            .to_string(),
            worker_id: 0,
        }
    }

    async fn write_events(path: &std::path::Path, events: &[OrderbookEvent]) {
        let mut content = String::new();
        for ev in events {
            content.push_str(&serde_json::to_string(ev).unwrap());
            content.push('\n');
        }
        tokio::fs::write(path, content).await.unwrap();
    }

    #[test]
    fn test_parse_f64_all_branches() {
        assert_eq!(parse_f64(Some(&json!(1.5))), Some(1.5));
        assert_eq!(parse_f64(Some(&json!("2.5"))), Some(2.5));
        assert_eq!(parse_f64(Some(&json!("bad"))), None);
        assert_eq!(parse_f64(None), None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_empty() {
        let subscriber = tracing_subscriber::fmt::Subscriber::default();
        let _guard =
            tracing::dispatcher::set_default(&tracing::dispatcher::Dispatch::new(subscriber));
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.jsonl");
        tokio::fs::write(&path, "").await.unwrap();
        let data = load_data(&path).unwrap();
        assert!(data.markets.is_empty());
    }

    #[tokio::test]
    async fn test_load_data_book_snapshot() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data.jsonl");
        write_events(&path, &[sample_book_event("0xA", 1000)]).await;

        let data = load_data(&path).unwrap();
        let market = data.markets.get("0xA").unwrap();
        assert_eq!(market.book_snapshots.len(), 1);

        let snap = &market.book_snapshots[0].1;
        assert_eq!(snap.bids[0].price, 0.1);
        assert_eq!(snap.bids[0].size, 100.0);
        assert_eq!(snap.asks[0].price, 0.2);
        assert_eq!(snap.asks[0].size, 50.0);
    }

    #[tokio::test]
    async fn test_load_data_price_change() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data.jsonl");
        write_events(&path, &[sample_price_change_event("0xA", 2000)]).await;

        let data = load_data(&path).unwrap();
        let market = data.markets.get("0xA").unwrap();
        assert_eq!(market.price_history.len(), 1);
        assert_eq!(market.price_history[0].price, 0.15);
        assert_eq!(market.price_history[0].side, Some("BUY".to_string()));
    }

    #[tokio::test]
    async fn test_load_data_last_trade() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data.jsonl");
        write_events(&path, &[sample_last_trade_event("0xA", 3000)]).await;

        let data = load_data(&path).unwrap();
        let market = data.markets.get("0xA").unwrap();
        assert_eq!(market.trades.len(), 1);
        let trade = &market.trades[0];
        assert_eq!(trade.price, 0.15);
        assert_eq!(trade.size, Some(10.0));
        assert_eq!(trade.side, Some("SELL".to_string()));
        assert_eq!(
            trade.tx_hash,
            Some("0x5e5fe7c64a30b1d23366bf508ea288b994e3b3d8d5afd5facd991af8551dae02".to_string())
        );
    }

    #[tokio::test]
    async fn test_load_data_dedup() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data.jsonl");
        write_events(
            &path,
            &[
                sample_book_event("0xA", 1000),
                sample_book_event("0xA", 1000),
            ],
        )
        .await;

        let data = load_data(&path).unwrap();
        assert_eq!(data.markets["0xA"].book_snapshots.len(), 1);
    }

    #[tokio::test]
    async fn test_load_data_malformed_and_empty_asset_and_unknown_event() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data.jsonl");

        let empty_asset = OrderbookEvent {
            event_type: "book".to_string(),
            asset: "".to_string(),
            side: None,
            price: None,
            size: None,
            timestamp: 1,
            received_at: 1,
            raw: "{}".to_string(),
            worker_id: 0,
        };
        let unknown_event = OrderbookEvent {
            event_type: "unknown".to_string(),
            asset: "0xA".to_string(),
            side: None,
            price: None,
            size: None,
            timestamp: 1,
            received_at: 1,
            raw: "{}".to_string(),
            worker_id: 0,
        };

        let mut content = String::from("not json\n");
        content.push_str(&serde_json::to_string(&empty_asset).unwrap());
        content.push('\n');
        content.push_str(&serde_json::to_string(&unknown_event).unwrap());
        content.push('\n');
        tokio::fs::write(&path, content).await.unwrap();

        let data = load_data(&path).unwrap();
        // Malformed line and empty asset are skipped. An unknown event type still
        // creates a market entry, but it contains no parsed data.
        assert!(!data.markets.contains_key(""));
        let empty_market = data.markets.get("0xA").unwrap();
        assert!(empty_market.book_snapshots.is_empty());
        assert!(empty_market.price_history.is_empty());
        assert!(empty_market.trades.is_empty());
    }

    #[tokio::test]
    async fn test_markets_handler() {
        let mut data = ViewerData::default();
        data.markets.insert(
            "0xB".to_string(),
            MarketView {
                asset: "0xB".to_string(),
                ..Default::default()
            },
        );
        data.markets.insert(
            "0xA".to_string(),
            MarketView {
                asset: "0xA".to_string(),
                ..Default::default()
            },
        );
        let state = test_state_with_data(data);
        let Json(markets) = markets_handler(State(state)).await;
        assert_eq!(markets, vec!["0xA", "0xB"]);
    }

    #[tokio::test]
    async fn test_book_snapshots_handler() {
        let mut data = ViewerData::default();
        let mut mv = MarketView {
            asset: "0xA".to_string(),
            ..Default::default()
        };
        mv.book_snapshots.push((
            1000,
            BookSnapshot {
                bids: vec![Level { price: 0.1, size: 1.0 }],
                asks: vec![],
            },
        ));
        data.markets.insert("0xA".to_string(), mv);

        let state = test_state_with_data(data);
        let Json(resp) =
            book_snapshots_handler(State(state.clone()), AxumPath("0xA".to_string())).await;
        assert_eq!(resp.snapshots.len(), 1);
        assert_eq!(resp.snapshots[0].timestamp, 1000);

        let Json(resp_missing) =
            book_snapshots_handler(State(state), AxumPath("missing".to_string())).await;
        assert!(resp_missing.snapshots.is_empty());
    }

    #[tokio::test]
    async fn test_trades_handler() {
        let mut data = ViewerData::default();
        let mut mv = MarketView {
            asset: "0xA".to_string(),
            ..Default::default()
        };
        mv.trades.push(TradePoint {
            timestamp: 1000,
            price: 0.1,
            size: Some(1.0),
            side: Some("BUY".to_string()),
            tx_hash: None,
        });
        data.markets.insert("0xA".to_string(), mv);

        let state = test_state_with_data(data);
        let Json(trades) =
            trades_handler(State(state.clone()), AxumPath("0xA".to_string())).await;
        assert_eq!(trades.len(), 1);

        let Json(empty) = trades_handler(State(state), AxumPath("missing".to_string())).await;
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn test_price_history_handler() {
        let mut data = ViewerData::default();
        let mut mv = MarketView {
            asset: "0xA".to_string(),
            ..Default::default()
        };
        mv.price_history.push(PricePoint {
            timestamp: 1000,
            price: 0.1,
            side: Some("SELL".to_string()),
        });
        data.markets.insert("0xA".to_string(), mv);

        let state = test_state_with_data(data);
        let Json(history) =
            price_history_handler(State(state.clone()), AxumPath("0xA".to_string())).await;
        assert_eq!(history.len(), 1);

        let Json(empty) =
            price_history_handler(State(state), AxumPath("missing".to_string())).await;
        assert!(empty.is_empty());
    }

    fn dummy_trade(tx_hash: &str) -> OnchainTrade {
        OnchainTrade {
            order_hash: "0xorder".to_string(),
            maker: "0xmaker".to_string(),
            taker: "0xtaker".to_string(),
            side: 0,
            token_id: "0xtoken".to_string(),
            maker_amount_filled: "0x1".to_string(),
            taker_amount_filled: "0x2".to_string(),
            fee: "0x0".to_string(),
            builder: "0xbuilder".to_string(),
            metadata: "0xmeta".to_string(),
            transaction_hash: tx_hash.to_string(),
            block_number: 100,
            log_index: 0,
        }
    }

    #[tokio::test]
    async fn test_trade_detail_cached() {
        let tx_hash = "0xabc".to_string();
        let trade = dummy_trade(&tx_hash);
        let mut cache = HashMap::new();
        cache.insert(tx_hash.clone(), Some(trade.clone()));

        let state = AppState {
            data: Arc::new(RwLock::new(ViewerData::default())),
            rpc_client: Arc::new(InMemoryHttpClient::new()),
            rpc_url: "https://polygon-rpc.com".to_string(),
            tx_cache: Arc::new(RwLock::new(cache)),
        };

        let Json(result) = trade_detail_handler(State(state), AxumPath(tx_hash)).await;
        assert_eq!(result.unwrap().transaction_hash, "0xabc");
    }

    fn word(hex: &str) -> String {
        format!("{:0>64}", hex.strip_prefix("0x").unwrap_or(hex))
    }

    fn padded_address(addr: &str) -> String {
        format!(
            "0x{}000000000000000000000000{}",
            "0".repeat(24),
            addr.strip_prefix("0x").unwrap_or(addr)
        )
    }

    fn sample_data(side: u8, token_id: &str) -> String {
        let side_word = word(&format!("0x{:02x}", side));
        let token_word = word(token_id);
        let maker_amount = word("0x1234");
        let taker_amount = word("0x5678");
        let fee = word("0x0");
        let builder = word("0xabcd");
        let metadata = word("0xef01");
        format!(
            "0x{}{}{}{}{}{}{}",
            side_word, token_word, maker_amount, taker_amount, fee, builder, metadata
        )
    }

    fn sample_log_with_address(address: &str) -> serde_json::Value {
        let maker_addr = "0x448861155279dbf833d041b963e3ac854599e319";
        let taker_addr = "0x6f3c1ddc97c9abfb38ff0f1302a56a1946d04c6f";
        json!({
            "address": address,
            "topics": [
                ORDER_FILLED_TOPIC,
                "0xd980fee1cbe88b9fbca895573ec0296b5a049937556040671ca4eb90d612d473",
                padded_address(maker_addr),
                padded_address(taker_addr),
            ],
            "data": sample_data(0, "0x66fc627fc41c09ce984d8db2aa4dcc8102d201581e73ad410b142f209832e207"),
            "transactionHash": "0x5e5fe7c64a30b1d23366bf508ea288b994e3b3d8d5afd5facd991af8551dae02",
            "blockNumber": "0x5424d9d",
            "logIndex": "0x38e",
        })
    }

    fn sample_log() -> serde_json::Value {
        sample_log_with_address(NEG_RISK_CTF_EXCHANGE_V2)
    }

    fn rpc_response(result: serde_json::Value) -> HttpResponse {
        HttpResponse {
            status: 200,
            body: json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": result,
            })
            .to_string(),
        }
    }

    #[tokio::test]
    async fn test_trade_detail_mocked_http() {
        let client = Arc::new(InMemoryHttpClient::new());
        let rpc_url = "https://polygon-rpc.com";
        client.set_response(
            rpc_url,
            Ok(rpc_response(json!({ "logs": [sample_log()] }))),
        );

        let state = AppState {
            data: Arc::new(RwLock::new(ViewerData::default())),
            rpc_client: client.clone(),
            rpc_url: rpc_url.to_string(),
            tx_cache: Arc::new(RwLock::new(HashMap::new())),
        };

        let tx_hash = "0x5e5fe7c64a30b1d23366bf508ea288b994e3b3d8d5afd5facd991af8551dae02".to_string();
        let Json(result1) =
            trade_detail_handler(State(state.clone()), AxumPath(tx_hash.clone())).await;
        assert!(result1.is_some());
        assert_eq!(result1.as_ref().unwrap().maker, "0x448861155279dbf833d041b963e3ac854599e319");

        // Second call should hit the cache.
        let Json(result2) = trade_detail_handler(State(state.clone()), AxumPath(tx_hash)).await;
        assert_eq!(result2.unwrap().maker, result1.unwrap().maker);

        assert_eq!(client.request_count(rpc_url), 1);
    }

    #[tokio::test]
    async fn test_trade_detail_invalid_tx_hash() {
        let client = InMemoryHttpClient::new();
        let rpc_url = "https://polygon-rpc.com";
        // No mock response configured -> request fails.
        let state = AppState {
            data: Arc::new(RwLock::new(ViewerData::default())),
            rpc_client: Arc::new(client),
            rpc_url: rpc_url.to_string(),
            tx_cache: Arc::new(RwLock::new(HashMap::new())),
        };

        let Json(result) =
            trade_detail_handler(State(state.clone()), AxumPath("not-a-hash".to_string())).await;
        assert!(result.is_none());

        let cache = state.tx_cache.read().await;
        assert!(cache.get("not-a-hash").unwrap().is_none());
    }

    #[tokio::test]
    async fn test_app_router() {
        let mut data = ViewerData::default();
        let mut mv = MarketView {
            asset: "0xA".to_string(),
            ..Default::default()
        };
        mv.book_snapshots.push((
            1000,
            BookSnapshot {
                bids: vec![Level { price: 0.1, size: 1.0 }],
                asks: vec![],
            },
        ));
        mv.price_history.push(PricePoint {
            timestamp: 1000,
            price: 0.1,
            side: Some("BUY".to_string()),
        });
        mv.trades.push(TradePoint {
            timestamp: 1000,
            price: 0.1,
            size: Some(1.0),
            side: Some("BUY".to_string()),
            tx_hash: None,
        });
        data.markets.insert("0xA".to_string(), mv);

        let client = InMemoryHttpClient::new();
        let rpc_url = "https://polygon-rpc.com";
        client.set_response(
            rpc_url,
            Ok(rpc_response(json!({ "logs": [sample_log()] }))),
        );

        let state = AppState {
            data: Arc::new(RwLock::new(data)),
            rpc_client: Arc::new(client),
            rpc_url: rpc_url.to_string(),
            tx_cache: Arc::new(RwLock::new(HashMap::new())),
        };

        let app = Router::new()
            .route("/", get(index_handler))
            .route("/api/markets", get(markets_handler))
            .route("/api/book_snapshots/:asset", get(book_snapshots_handler))
            .route("/api/trades/:asset", get(trades_handler))
            .route("/api/price_history/:asset", get(price_history_handler))
            .route("/api/trade_detail/:tx_hash", get(trade_detail_handler))
            .with_state(state);

        // GET /
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        // GET /api/markets
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/markets")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let markets: Vec<String> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(markets, vec!["0xA"]);

        // GET /api/book_snapshots/0xA
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/book_snapshots/0xA")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        // GET /api/trades/0xA
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/trades/0xA")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        // GET /api/price_history/0xA
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/price_history/0xA")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        // GET /api/trade_detail/0xtx
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/trade_detail/0x5e5fe7c64a30b1d23366bf508ea288b994e3b3d8d5afd5facd991af8551dae02")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let detail: Option<OnchainTrade> = serde_json::from_slice(&bytes).unwrap();
        assert!(detail.is_some());
    }

    #[test]
    fn test_build_state_uses_provided_rpc_url() {
        let data = ViewerData::default();
        let state = build_state(data, Some("https://custom.rpc".to_string()));
        assert_eq!(state.rpc_url, "https://custom.rpc");
    }

    #[test]
    fn test_build_state_defaults_rpc_url() {
        let data = ViewerData::default();
        let state = build_state(data, None);
        assert_eq!(state.rpc_url, "https://polygon.drpc.org");
    }

    #[test]
    fn test_build_app_routes() {
        let data = ViewerData::default();
        let state = build_state(data, None);
        let _app = build_app(state);
    }

    #[tokio::test]
    async fn test_run_with_app_hits_listen_lines() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("data.jsonl");
        tokio::fs::write(&input_path, "").await.unwrap();

        let data = load_data(&input_path).unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let _ = shutdown_tx.send(());
        });

        let client = reqwest::Client::new();
        let request_url = format!("http://127.0.0.1:{}/api/markets", port);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let resp = client.get(&request_url).send().await.unwrap();
            assert!(resp.status().is_success());
        });

        run_with_app(
            data,
            listener,
            Some("https://polygon-rpc.com".to_string()),
            async { shutdown_rx.await.ok(); },
        )
        .await
        .unwrap();
    }

    #[test]
    fn test_run_with_shutdown_sync() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let dir = tempdir().unwrap();
            let input_path = dir.path().join("data.jsonl");
            tokio::fs::write(&input_path, "").await.unwrap();

            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            drop(listener);

            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
            let path = input_path.clone();
            let bind = format!("127.0.0.1:{}", port);

            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(500)).await;
                let _ = shutdown_tx.send(());
            });

            let client = reqwest::Client::new();
            let request_url = format!("http://127.0.0.1:{}/api/markets", port);
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let resp = client.get(&request_url).send().await.unwrap();
                assert!(resp.status().is_success());
            });

            run_with_shutdown(
                &path,
                &bind,
                Some("https://polygon-rpc.com".to_string()),
                async { shutdown_rx.await.ok(); },
            )
            .await
            .unwrap();
        });
    }

    #[tokio::test]
    async fn test_run_with_shutdown_spawns_server() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("data.jsonl");
        tokio::fs::write(&input_path, "").await.unwrap();

        // Bind a free port and release it so run_with_shutdown can use it.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let path = input_path.clone();
        let bind = format!("127.0.0.1:{}", port);

        // Send shutdown after the request is served.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let _ = shutdown_tx.send(());
        });

        // Make the request from a separate task while run_with_shutdown runs here.
        let client = reqwest::Client::new();
        let request_url = format!("http://127.0.0.1:{}/api/markets", port);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let resp = client.get(&request_url).send().await.unwrap();
            assert!(resp.status().is_success());
        });

        run_with_shutdown(
            &path,
            &bind,
            Some("https://polygon-rpc.com".to_string()),
            async { shutdown_rx.await.ok(); },
        )
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_run_wrapper_spawns_server() {
        let subscriber = tracing_subscriber::fmt::Subscriber::default();
        let _guard =
            tracing::dispatcher::set_default(&tracing::dispatcher::Dispatch::new(subscriber));

        let dir = tempdir().unwrap();
        let input_path = dir.path().join("data.jsonl");
        tokio::fs::write(&input_path, "").await.unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let path = input_path.clone();
        let bind = format!("127.0.0.1:{}", port);
        let handle = tokio::spawn(async move {
            let _ = run(&path, &bind, Some("https://polygon-rpc.com".to_string())).await;
        });

        tokio::time::sleep(Duration::from_millis(300)).await;

        let client = reqwest::Client::new();
        let resp = client
            .get(&format!("http://127.0.0.1:{}/api/markets", port))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());

        handle.abort();
        let _ = handle.await;
    }
    #[test]
    fn test_load_data_fallback_side_from_raw() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data.jsonl");
        let line = serde_json::to_string(&OrderbookEvent {
            event_type: "last_trade".to_string(),
            asset: "asset1".to_string(),
            side: None,
            price: Some(0.5),
            size: Some(10.0),
            timestamp: 1000,
            received_at: 1001,
            raw: r#"{"event_type":"last_trade_price","asset_id":"asset1","price":"0.5","size":"10","side":"SELL","transaction_hash":"0xabc","timestamp":"1000"}"#.to_string(),
            worker_id: 0,
        })
        .unwrap();
        std::fs::write(&path, format!("{}\n", line)).unwrap();

        let data = load_data(&path).unwrap();
        let trades = data.markets.get("asset1").unwrap().trades.clone();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].side, Some("SELL".to_string()));
    }
}
