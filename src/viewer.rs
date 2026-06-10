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
    rpc_client: reqwest::Client,
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
    let line_count = lines.len();

    // Step 2: Determine chunk size for parallel processing.
    // Each CPU core gets roughly `lines / num_cpus` lines to process independently.
    let num_cpus = std::thread::available_parallelism()?.get();
    let chunk_size = std::cmp::max(1, (lines.len() + num_cpus - 1) / num_cpus);

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

    let elapsed = start.elapsed();
    info!(
        markets = data.markets.len(),
        lines = line_count,
        ?elapsed,
        "Viewer data loaded"
    );
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
    let result = match crate::onchain::get_receipt_logs(
        &state.rpc_client,
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
pub async fn run(input_path: &Path, bind: &str, rpc_url: Option<String>) -> Result<()> {
    let path = input_path.to_path_buf();

    // Load data off the async runtime to avoid blocking the event loop.
    // For 800k lines this takes ~16s with Rayon parallel parsing.
    let data = tokio::task::spawn_blocking(move || load_data(&path))
        .await
        .expect("spawn_blocking failed")?;

    let state = AppState {
        data: Arc::new(RwLock::new(data)),
        rpc_client: reqwest::Client::new(),
        rpc_url: rpc_url.unwrap_or_else(|| "https://polygon.drpc.org".to_string()),
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

    info!(bind = %bind, "Viewer listening");
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
