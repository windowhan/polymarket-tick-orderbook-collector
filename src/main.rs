use anyhow::Result;
use clap::{Parser, Subcommand};
use polymarket_collector::dynamic_markets::{
    GammaMarketSource, MarketRefreshPolicy, DEFAULT_MARKET_REFRESH_INTERVAL_SECS,
    DEFAULT_STALE_MARKET_TTL_HOURS,
};
use polymarket_collector::http_client::HttpClient;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

#[derive(Parser)]
#[command(name = "polymarket-collector")]
#[command(about = "Polymarket tick-level order book and trade collector (Rust)")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Discover all markets and save metadata to data/markets/markets.jsonl
    Discover {
        #[arg(long)]
        active: Option<bool>,
        #[arg(long)]
        closed: Option<bool>,
    },
    /// Collect real-time order book via WebSocket
    ///
    /// In orchestrated mode (`--aggregator-url`), the collector receives a
    /// dynamic assignment from the aggregator. In standalone mode with no static
    /// path, it fetches live Gamma markets directly. File-based collection is an
    /// explicit offline/static mode via `--static-markets-path`.
    CollectOrderbook {
        /// Deprecated alias for --static-markets-path; hidden to avoid implying
        /// that live collection falls back to data/markets/markets.jsonl.
        #[arg(long, hide = true)]
        markets_path: Option<PathBuf>,
        /// Explicit static/offline markets.jsonl path. When omitted without an
        /// aggregator, the collector starts in API-first live mode.
        #[arg(long)]
        static_markets_path: Option<PathBuf>,
        /// Local directory for rotated orderbook files.
        #[arg(long, default_value = "data/orderbook")]
        output_dir: PathBuf,
        /// Aggregator URL for orchestrated mode (e.g. http://aggregator:8080)
        #[arg(long)]
        aggregator_url: Option<String>,
        /// S3 bucket where rotated files are uploaded (used for notify metadata).
        #[arg(long)]
        s3_bucket: Option<String>,
        /// S3 prefix where rotated files are uploaded (used for notify metadata).
        #[arg(long, default_value = "orderbook/")]
        s3_prefix: String,
        /// AWS region for S3 operations.
        #[arg(long, default_value = "us-east-1")]
        aws_region: String,
        /// Tokens per WebSocket connection (default 100)
        #[arg(long, default_value = "100")]
        chunk_size: usize,
        /// Seconds between file rotations for local storage (default 300)
        #[arg(long, default_value = "300")]
        rotate_interval_secs: u64,
        /// Seconds between standalone live Gamma market refreshes (default 600).
        #[arg(long, default_value_t = DEFAULT_MARKET_REFRESH_INTERVAL_SECS)]
        market_refresh_interval_secs: u64,
        /// Hours to keep missing markets subscribed before removal (default 12).
        #[arg(long, default_value_t = DEFAULT_STALE_MARKET_TTL_HOURS)]
        stale_market_ttl_hours: u64,
        /// Seconds between orchestrated assignment polls (default 60).
        #[arg(
            long,
            default_value_t = polymarket_collector::ws_orderbook::DEFAULT_ASSIGNMENT_POLL_INTERVAL_SECS
        )]
        assignment_poll_interval_secs: u64,
        /// Run for N seconds then graceful shutdown (for testing)
        #[arg(long)]
        duration_secs: Option<u64>,
        /// Limit total tokens for explicit static/offline load testing.
        #[arg(long)]
        limit_tokens: Option<usize>,
    },
    /// Run central orchestrated aggregator server
    ///
    /// Assigns markets to registered collectors, receives S3 upload
    /// notifications, and merges S3 files into a single local JSONL output.
    Aggregator {
        #[arg(long, default_value = "0.0.0.0:8080")]
        bind: String,
        #[arg(long, default_value = "data/aggregated_orderbook.jsonl")]
        output_path: PathBuf,
        /// Deprecated static markets path retained only as a hidden compatibility
        /// alias; live aggregator startup always uses the Gamma API.
        #[arg(long, hide = true)]
        markets_path: Option<PathBuf>,
        /// S3 bucket where collectors upload rotated files.
        #[arg(long)]
        s3_bucket: String,
        /// S3 prefix under which collector files are stored.
        #[arg(long, default_value = "orderbook/")]
        s3_prefix: String,
        /// How many collectors each market is assigned to (default 2).
        #[arg(long, default_value = "2")]
        replication_factor: usize,
        /// Seconds before a collector is considered stale (default 60).
        #[arg(long, default_value = "60")]
        heartbeat_timeout_secs: u64,
        /// Seconds between live Gamma market refreshes (default 600).
        #[arg(long, default_value_t = DEFAULT_MARKET_REFRESH_INTERVAL_SECS)]
        market_refresh_interval_secs: u64,
        /// Hours to keep missing markets assigned before removal (default 12).
        #[arg(long, default_value_t = DEFAULT_STALE_MARKET_TTL_HOURS)]
        stale_market_ttl_hours: u64,
        /// Delete S3 objects after merging.
        #[arg(long)]
        delete_after_merge: bool,
        /// AWS region for S3 operations.
        #[arg(long, default_value = "us-east-1")]
        region: String,
    },
    /// Backfill historical trades for all discovered markets
    CollectTrades {
        #[arg(long, default_value = "data/markets/markets.jsonl")]
        markets_path: PathBuf,
        #[arg(long, default_value = "data/trades")]
        output_dir: PathBuf,
    },
    /// Scrape on-chain OrderFilled events from Polygon CTF Exchange V2
    ScrapeOnchain {
        #[arg(long, default_value = "https://polygon-rpc.com")]
        rpc_url: String,
        #[arg(long, default_value = polymarket_collector::onchain::CTF_EXCHANGE_V2)]
        exchange: String,
        #[arg(long)]
        from_block: u64,
        #[arg(long)]
        to_block: u64,
        #[arg(long, default_value = "1000")]
        chunk_size: u64,
        #[arg(long, default_value = "data/onchain.jsonl")]
        output: PathBuf,
    },
    /// Analyze reward size distribution and competitive intensity
    AnalyzeRewards {
        #[arg(long, default_value = "data/markets/markets.jsonl")]
        markets_path: PathBuf,
    },
    /// Launch a local web viewer for aggregated orderbook data
    Viewer {
        #[arg(long, default_value = "data/local_test_aggregated.jsonl")]
        input_path: PathBuf,
        #[arg(long, default_value = "127.0.0.1:3000")]
        bind: String,
        #[arg(long, default_value = "https://polygon.drpc.org")]
        rpc_url: String,
    },
}

fn default_http_client() -> Arc<dyn HttpClient> {
    #[cfg(test)]
    if let Some(client) = test_helpers::try_test_http_client() {
        return client;
    }
    use polymarket_collector::http_client::ReqwestHttpClient;
    Arc::new(ReqwestHttpClient::new())
}

fn dispatch_data_dir() -> PathBuf {
    #[cfg(test)]
    if let Some(dir) = test_helpers::try_test_data_dir() {
        return dir;
    }
    polymarket_collector::storage::data_dir()
}

/// Number of seconds in one hour for converting `--stale-market-ttl-hours`.
const SECONDS_PER_HOUR: u64 = 60 * 60;

/// Resolve the explicit static market path from the new flag or deprecated alias.
///
/// # Detailed Description
/// Live orderbook collection is now API-first by default, so no local
/// `data/markets/markets.jsonl` path is assumed. This helper keeps the old
/// `--markets-path` flag as a hidden compatibility alias while requiring callers
/// to choose only one static path spelling.
///
/// # Arguments
/// * `static_markets_path` — Preferred explicit static/offline JSONL path.
/// * `markets_path` — Deprecated alias for the same static/offline JSONL path.
///
/// # Returns
/// The chosen static path, or `None` when the caller requested live API mode.
///
/// # Example — Input / Output
/// ```rust,ignore
/// let path = resolve_static_markets_path(Some("markets.jsonl".into()), None)?;
/// assert_eq!(path.unwrap(), PathBuf::from("markets.jsonl"));
/// ```
///
/// # Related
/// - `Commands::CollectOrderbook`
fn resolve_static_markets_path(
    static_markets_path: Option<PathBuf>,
    markets_path: Option<PathBuf>,
) -> Result<Option<PathBuf>> {
    match (static_markets_path, markets_path) {
        (Some(_), Some(_)) => {
            anyhow::bail!("Use only one of --static-markets-path or deprecated --markets-path")
        }
        (Some(path), None) | (None, Some(path)) => Ok(Some(path)),
        (None, None) => Ok(None),
    }
}

/// Build a live-market refresh policy from validated CLI timing flags.
///
/// # Detailed Description
/// Tokio intervals require non-zero periods. This helper validates that
/// user-supplied live refresh and stale-retention values are positive, then
/// converts hours to seconds without changing the shared registry semantics.
///
/// # Arguments
/// * `refresh_interval_secs` — Seconds between live Gamma refresh attempts.
/// * `stale_market_ttl_hours` — Hours to keep missing markets before removal.
///
/// # Returns
/// A [`MarketRefreshPolicy`] or an error for invalid zero/overflow values.
///
/// # Example — Input / Output
/// ```rust,ignore
/// let policy = market_refresh_policy_from_cli(600, 12)?;
/// assert_eq!(policy.refresh_interval, Duration::from_secs(600));
/// assert_eq!(policy.stale_ttl, Duration::from_secs(43_200));
/// ```
///
/// # Related
/// - [`MarketRefreshPolicy`]
fn market_refresh_policy_from_cli(
    refresh_interval_secs: u64,
    stale_market_ttl_hours: u64,
) -> Result<MarketRefreshPolicy> {
    if refresh_interval_secs == 0 {
        anyhow::bail!("--market-refresh-interval-secs must be greater than 0");
    }
    if stale_market_ttl_hours == 0 {
        anyhow::bail!("--stale-market-ttl-hours must be greater than 0");
    }
    let stale_ttl_secs = stale_market_ttl_hours
        .checked_mul(SECONDS_PER_HOUR)
        .ok_or_else(|| anyhow::anyhow!("--stale-market-ttl-hours is too large"))?;
    Ok(MarketRefreshPolicy {
        refresh_interval: Duration::from_secs(refresh_interval_secs),
        stale_ttl: Duration::from_secs(stale_ttl_secs),
    })
}

/// Convert a positive second count into a [`Duration`] for a named CLI flag.
///
/// # Detailed Description
/// Assignment polling and other Tokio interval-backed values must be non-zero.
/// Keeping the validation in one helper prevents panics from invalid CLI input.
///
/// # Arguments
/// * `seconds` — User-provided interval in seconds.
/// * `flag_name` — CLI flag name included in validation errors.
///
/// # Returns
/// A positive [`Duration`] when valid.
///
/// # Example — Input / Output
/// ```rust,ignore
/// let interval = positive_duration_secs(60, "--assignment-poll-interval-secs")?;
/// assert_eq!(interval, Duration::from_secs(60));
/// ```
///
/// # Related
/// - [`polymarket_collector::ws_orderbook::OrderbookCollector::with_assignment_poll_interval`]
fn positive_duration_secs(seconds: u64, flag_name: &str) -> Result<Duration> {
    if seconds == 0 {
        anyhow::bail!("{flag_name} must be greater than 0");
    }
    Ok(Duration::from_secs(seconds))
}

#[cfg(test)]
mod test_helpers {
    use polymarket_collector::http_client::InMemoryHttpClient;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    static TEST_HTTP_CLIENT: Mutex<Option<Arc<InMemoryHttpClient>>> = Mutex::new(None);
    static TEST_DATA_DIR: Mutex<Option<PathBuf>> = Mutex::new(None);

    pub fn try_test_http_client() -> Option<Arc<InMemoryHttpClient>> {
        TEST_HTTP_CLIENT.lock().unwrap().clone()
    }

    pub fn reset_test_http_client() -> Arc<InMemoryHttpClient> {
        let client = Arc::new(InMemoryHttpClient::new());
        *TEST_HTTP_CLIENT.lock().unwrap() = Some(client.clone());
        client
    }

    pub fn clear_test_http_client() {
        *TEST_HTTP_CLIENT.lock().unwrap() = None;
    }

    pub fn try_test_data_dir() -> Option<PathBuf> {
        TEST_DATA_DIR.lock().unwrap().clone()
    }

    pub fn set_test_data_dir(dir: PathBuf) {
        *TEST_DATA_DIR.lock().unwrap() = Some(dir);
    }

    pub fn clear_test_data_dir() {
        *TEST_DATA_DIR.lock().unwrap() = None;
    }
}

/// Dispatch a single CLI command to its handler.
pub async fn run_command(command: Commands) -> Result<()> {
    match command {
        Commands::Discover { active, closed } => {
            info!("Starting market discovery...");
            let client = default_http_client();
            let count = polymarket_collector::market_discovery::discover_and_save_with_client(
                client.as_ref(),
                active,
                closed,
                &dispatch_data_dir(),
            )
            .await?;
            info!(count, "Discovery complete");
        }

        Commands::CollectOrderbook {
            markets_path,
            static_markets_path,
            output_dir,
            aggregator_url,
            s3_bucket,
            s3_prefix,
            aws_region,
            chunk_size,
            rotate_interval_secs,
            market_refresh_interval_secs,
            stale_market_ttl_hours,
            assignment_poll_interval_secs,
            duration_secs,
            limit_tokens,
        } => {
            let static_markets_path =
                resolve_static_markets_path(static_markets_path, markets_path)?;
            if aggregator_url.is_some() && static_markets_path.is_some() {
                anyhow::bail!("Static market paths cannot be combined with --aggregator-url");
            }
            let market_refresh_policy = market_refresh_policy_from_cli(
                market_refresh_interval_secs,
                stale_market_ttl_hours,
            )?;
            let assignment_poll_interval = positive_duration_secs(
                assignment_poll_interval_secs,
                "--assignment-poll-interval-secs",
            )?;
            let live_mode = aggregator_url.is_none() && static_markets_path.is_none();
            if limit_tokens.is_some() && static_markets_path.is_none() {
                anyhow::bail!("--limit-tokens is only supported with --static-markets-path");
            }

            // In orchestrated mode, token IDs come from the aggregator. In
            // standalone live mode, token IDs come from the injected Gamma source
            // inside `OrderbookCollector::run`. Only explicit static mode reads a
            // local markets file.
            let token_ids = if let Some(_url) = &aggregator_url {
                // Orchestrated mode: token IDs will be fetched from aggregator at runtime.
                Vec::new()
            } else if let Some(path) = static_markets_path.as_ref() {
                if !path.exists() {
                    anyhow::bail!("Markets file not found: {}", path.display());
                }

                let content = tokio::fs::read_to_string(path).await?;
                let mut ids = Vec::new();
                for line in content.lines() {
                    if let Ok(market) =
                        serde_json::from_str::<polymarket_collector::market_discovery::Market>(line)
                    {
                        ids.extend(market.token_ids);
                    }
                }

                if let Some(limit) = limit_tokens {
                    ids.truncate(limit);
                }

                if ids.is_empty() {
                    anyhow::bail!("No token IDs found in markets file");
                }
                ids
            } else {
                // Standalone live mode: initial fetch and fail-fast validation
                // happen inside the collector using the configured source.
                Vec::new()
            };

            info!(
                token_count = token_ids.len(),
                chunk_size,
                aggregator_url = ?aggregator_url,
                static_markets_path = ?static_markets_path,
                live_mode,
                "Starting order book collection"
            );

            let mut collector = polymarket_collector::ws_orderbook::OrderbookCollector::new(
                token_ids,
                output_dir,
                None, // relay_url is no longer used
                chunk_size,
                Duration::from_secs(rotate_interval_secs),
                duration_secs,
            );

            if let Some(url) = aggregator_url {
                collector = collector
                    .with_aggregator_url(url)
                    .with_assignment_poll_interval(assignment_poll_interval);
            } else if live_mode {
                let market_source = Arc::new(GammaMarketSource::new(default_http_client()));
                collector = collector.with_market_source(market_source, market_refresh_policy);
            }

            if let Some(bucket) = s3_bucket {
                collector = collector.with_s3_upload(bucket, s3_prefix, aws_region);
            }

            collector.run().await?;
        }

        Commands::Aggregator {
            bind,
            output_path,
            markets_path,
            s3_bucket,
            s3_prefix,
            replication_factor,
            heartbeat_timeout_secs,
            market_refresh_interval_secs,
            stale_market_ttl_hours,
            delete_after_merge,
            region,
        } => {
            let market_refresh_policy = market_refresh_policy_from_cli(
                market_refresh_interval_secs,
                stale_market_ttl_hours,
            )?;
            info!(
                bind = %bind,
                path = %output_path.display(),
                legacy_markets_path = ?markets_path,
                bucket = %s3_bucket,
                prefix = %s3_prefix,
                replication_factor,
                heartbeat_timeout_secs,
                market_refresh_interval_secs,
                stale_market_ttl_hours,
                "Starting API-first orchestrated aggregator"
            );

            let market_source = Arc::new(GammaMarketSource::new(default_http_client()));
            polymarket_collector::aggregator::run_with_market_source(
                &bind,
                output_path,
                s3_bucket,
                s3_prefix,
                replication_factor,
                Duration::from_secs(heartbeat_timeout_secs),
                delete_after_merge,
                region,
                market_source,
                market_refresh_policy,
            )
            .await?;
        }

        Commands::CollectTrades {
            markets_path,
            output_dir,
        } => {
            if !markets_path.exists() {
                anyhow::bail!("Markets file not found: {}", markets_path.display());
            }

            let content = tokio::fs::read_to_string(&markets_path).await?;
            let mut assets = Vec::new();
            for line in content.lines() {
                if let Ok(market) =
                    serde_json::from_str::<polymarket_collector::market_discovery::Market>(line)
                {
                    assets.extend(market.token_ids);
                }
            }

            info!(count = assets.len(), "Starting trade backfill");
            let client = default_http_client();

            for asset in assets {
                let trades = polymarket_collector::trade_fetcher::backfill_trades_for_asset(
                    client.as_ref(),
                    &asset,
                )
                .await?;
                if !trades.is_empty() {
                    let path = output_dir.join(format!("{}.jsonl", asset));
                    std::fs::create_dir_all(&output_dir)?;
                    polymarket_collector::storage::append_jsonl(&path, &trades).await?;
                    info!(asset, count = trades.len(), "Saved trades");
                }
            }
        }

        Commands::ScrapeOnchain {
            rpc_url,
            exchange,
            from_block,
            to_block,
            chunk_size,
            output,
        } => {
            info!("Starting on-chain scrape...");
            let client = default_http_client();
            let count = polymarket_collector::onchain::scrape_exchange_with_client(
                client.as_ref(),
                &rpc_url,
                &exchange,
                from_block,
                to_block,
                chunk_size,
                &output,
            )
            .await?;
            info!(count, "On-chain scrape complete");
        }

        Commands::AnalyzeRewards { markets_path } => {
            if !markets_path.exists() {
                anyhow::bail!("Markets file not found: {}", markets_path.display());
            }
            let buckets = polymarket_collector::reward_analyzer::analyze_rewards(&markets_path)?;
            polymarket_collector::reward_analyzer::print_reward_analysis(&buckets);
        }

        Commands::Viewer {
            input_path,
            bind,
            rpc_url,
        } => {
            if !input_path.exists() {
                anyhow::bail!("Input file not found: {}", input_path.display());
            }
            info!(path = %input_path.display(), bind = %bind, rpc = %rpc_url, "Starting viewer");
            polymarket_collector::viewer::run(&input_path, &bind, Some(rpc_url)).await?;
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    run_command(Cli::parse().command).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use polymarket_collector::http_client::{HttpResponse, InMemoryHttpClient};
    use std::io::Write;
    use tempfile::tempdir;
    use tokio::time::{timeout, Duration};

    /// Serializes main::tests so the shared in-memory HTTP client and data dir
    /// are not mutated by concurrent async tests.
    static TEST_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// Locks shared CLI test state with an async-aware mutex.
    ///
    /// # Detailed Description
    /// The `main` module tests intentionally share test-only global overrides for
    /// the HTTP client and data directory. Holding a standard mutex across
    /// `await` points creates misleading lint failures and can poison later tests
    /// after one assertion panic. Tokio's mutex is designed for async test
    /// serialization and does not poison, so later tests keep reporting their own
    /// result instead of cascading a `PoisonError`.
    ///
    /// # Returns
    /// A mutex guard that serializes access to shared test globals for the
    /// duration of a test.
    ///
    /// # Example — Input / Output
    /// ```rust,ignore
    /// let _lock = lock_test_state().await;
    /// // Shared test-only HTTP client and data-dir overrides are now isolated.
    /// ```
    ///
    /// # Related
    /// - `setup` resets the shared test HTTP client and data directory.
    async fn lock_test_state() -> tokio::sync::MutexGuard<'static, ()> {
        TEST_MUTEX.lock().await
    }

    fn setup() -> (tempfile::TempDir, Arc<InMemoryHttpClient>) {
        let dir = tempdir().unwrap();
        test_helpers::set_test_data_dir(dir.path().to_path_buf());
        let client = test_helpers::reset_test_http_client();
        (dir, client)
    }

    fn market_json(token_ids: Vec<String>) -> serde_json::Value {
        serde_json::json!({
            "id": "m1",
            "conditionId": "cond1",
            "condition_id": "cond1",
            "question": "Will it rain?",
            "slug": "rain",
            "active": true,
            "closed": false,
            "archived": false,
            // Safe in tests: a vector of strings is always JSON-serializable.
            "clobTokenIds": serde_json::to_string(&token_ids).unwrap(),
            "clobRewards": [],
            "clob_rewards": [],
            "token_ids": token_ids,
            "enableOrderBook": true,
            "enable_order_book": true,
            "negRisk": false,
            "neg_risk": false,
            "acceptingOrders": true,
            "accepting_orders": true,
        })
    }

    fn set_live_markets_response(client: &InMemoryHttpClient, token_ids: Vec<String>) {
        client.set_response(
            "https://gamma-api.polymarket.com/markets?limit=100&active=true&closed=false",
            Ok(HttpResponse {
                status: 200,
                body: serde_json::to_string(&serde_json::json!([market_json(token_ids)]))
                    // Safe in tests: the helper emits a plain JSON object array.
                    .unwrap(),
            }),
        );
    }

    fn write_markets(path: &std::path::Path, token_ids: Vec<String>) {
        let mut file = std::fs::File::create(path).unwrap();
        writeln!(
            file,
            "{}",
            serde_json::to_string(&market_json(token_ids)).unwrap()
        )
        .unwrap();
    }

    fn reward_market(daily_rate: f64) -> serde_json::Value {
        serde_json::json!({
            "id": "m1",
            "condition_id": "cond1",
            "question": "Will it rain?",
            "slug": "rain",
            "active": true,
            "closed": false,
            "archived": false,
            "token_ids": ["0xtoken1"],
            "clob_rewards": [{
                "rewards_daily_rate": daily_rate,
            }],
            "volume": 100.0,
            "liquidity": 200.0,
            "volume_24h": 50.0,
            "enable_order_book": true,
            "neg_risk": false,
            "accepting_orders": true,
        })
    }

    fn find_free_port() -> u16 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    }

    #[tokio::test]
    async fn test_discover_command_writes_markets() {
        let _lock = lock_test_state().await;
        let (dir, client) = setup();
        let gamma_url = "https://gamma-api.polymarket.com/markets?limit=100";
        client.set_response(
            gamma_url,
            Ok(HttpResponse {
                status: 200,
                body: serde_json::to_string(&serde_json::json!([market_json(vec![
                    "0xtoken1".to_string()
                ])]))
                .unwrap(),
            }),
        );

        let cmd = Commands::Discover {
            active: None,
            closed: None,
        };
        run_command(cmd).await.unwrap();

        let path = dir.path().join("markets").join("markets.jsonl");
        assert!(path.exists());
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("0xtoken1"));
    }

    #[tokio::test]
    async fn test_collect_orderbook_static_missing_file() {
        let _lock = lock_test_state().await;
        let (dir, _client) = setup();
        let cmd = Commands::CollectOrderbook {
            markets_path: None,
            static_markets_path: Some(dir.path().join("nonexistent.jsonl")),
            output_dir: dir.path().join("orderbook"),
            aggregator_url: None,
            s3_bucket: None,
            s3_prefix: "orderbook/".to_string(),
            aws_region: "us-east-1".to_string(),
            chunk_size: 100,
            rotate_interval_secs: 300,
            market_refresh_interval_secs: DEFAULT_MARKET_REFRESH_INTERVAL_SECS,
            stale_market_ttl_hours: DEFAULT_STALE_MARKET_TTL_HOURS,
            assignment_poll_interval_secs:
                polymarket_collector::ws_orderbook::DEFAULT_ASSIGNMENT_POLL_INTERVAL_SECS,
            duration_secs: None,
            limit_tokens: None,
        };
        let err = run_command(cmd).await.unwrap_err();
        assert!(err.to_string().contains("Markets file not found"));
    }

    #[tokio::test]
    async fn test_collect_orderbook_static_empty_tokens() {
        let _lock = lock_test_state().await;
        let (dir, _client) = setup();
        let markets_path = dir.path().join("markets.jsonl");
        write_markets(&markets_path, vec![]);
        let cmd = Commands::CollectOrderbook {
            markets_path: None,
            static_markets_path: Some(markets_path),
            output_dir: dir.path().join("orderbook"),
            aggregator_url: None,
            s3_bucket: None,
            s3_prefix: "orderbook/".to_string(),
            aws_region: "us-east-1".to_string(),
            chunk_size: 100,
            rotate_interval_secs: 300,
            market_refresh_interval_secs: DEFAULT_MARKET_REFRESH_INTERVAL_SECS,
            stale_market_ttl_hours: DEFAULT_STALE_MARKET_TTL_HOURS,
            assignment_poll_interval_secs:
                polymarket_collector::ws_orderbook::DEFAULT_ASSIGNMENT_POLL_INTERVAL_SECS,
            duration_secs: None,
            limit_tokens: None,
        };
        let err = run_command(cmd).await.unwrap_err();
        assert!(err.to_string().contains("No token IDs found"));
    }

    #[tokio::test]
    async fn test_collect_orderbook_static_runs() {
        let _lock = lock_test_state().await;
        let (dir, _client) = setup();
        let markets_path = dir.path().join("markets.jsonl");
        write_markets(&markets_path, vec!["0xtoken1".to_string()]);
        let output_dir = dir.path().join("orderbook");
        let cmd = Commands::CollectOrderbook {
            markets_path: None,
            static_markets_path: Some(markets_path),
            output_dir,
            aggregator_url: None,
            s3_bucket: None,
            s3_prefix: "orderbook/".to_string(),
            aws_region: "us-east-1".to_string(),
            chunk_size: 100,
            rotate_interval_secs: 300,
            market_refresh_interval_secs: DEFAULT_MARKET_REFRESH_INTERVAL_SECS,
            stale_market_ttl_hours: DEFAULT_STALE_MARKET_TTL_HOURS,
            assignment_poll_interval_secs:
                polymarket_collector::ws_orderbook::DEFAULT_ASSIGNMENT_POLL_INTERVAL_SECS,
            duration_secs: Some(1),
            limit_tokens: None,
        };
        let result = timeout(Duration::from_secs(15), run_command(cmd)).await;
        assert!(
            result.is_ok(),
            "collect orderbook static should complete within timeout"
        );
    }

    #[tokio::test]
    async fn test_collect_orderbook_orchestrated_dispatches() {
        let _lock = lock_test_state().await;
        let (dir, _client) = setup();
        // Point at an unreachable aggregator so registration fails quickly.
        let cmd = Commands::CollectOrderbook {
            markets_path: None,
            static_markets_path: None,
            output_dir: dir.path().join("orderbook"),
            aggregator_url: Some("http://127.0.0.1:1".to_string()),
            s3_bucket: None,
            s3_prefix: "orderbook/".to_string(),
            aws_region: "us-east-1".to_string(),
            chunk_size: 100,
            rotate_interval_secs: 300,
            market_refresh_interval_secs: DEFAULT_MARKET_REFRESH_INTERVAL_SECS,
            stale_market_ttl_hours: DEFAULT_STALE_MARKET_TTL_HOURS,
            assignment_poll_interval_secs:
                polymarket_collector::ws_orderbook::DEFAULT_ASSIGNMENT_POLL_INTERVAL_SECS,
            duration_secs: Some(1),
            limit_tokens: None,
        };
        let err = run_command(cmd).await.unwrap_err();
        assert!(err.to_string().contains("Failed to register"));
    }

    #[tokio::test]
    async fn test_aggregator_command_starts() {
        let _lock = lock_test_state().await;
        let (dir, client) = setup();
        set_live_markets_response(&client, vec!["0xtoken1".to_string()]);
        let output_path = dir.path().join("aggregated.jsonl");
        let port = find_free_port();
        let cmd = Commands::Aggregator {
            bind: format!("127.0.0.1:{}", port),
            output_path,
            markets_path: None,
            s3_bucket: "dummy-bucket".to_string(),
            s3_prefix: "orderbook/".to_string(),
            replication_factor: 1,
            heartbeat_timeout_secs: 60,
            market_refresh_interval_secs: DEFAULT_MARKET_REFRESH_INTERVAL_SECS,
            stale_market_ttl_hours: DEFAULT_STALE_MARKET_TTL_HOURS,
            delete_after_merge: false,
            region: "us-east-1".to_string(),
        };
        let result = timeout(Duration::from_secs(5), run_command(cmd)).await;
        assert!(
            result.is_err(),
            "aggregator should keep running until timeout"
        );
    }

    #[tokio::test]
    async fn test_collect_trades_command_saves_trades() {
        let _lock = lock_test_state().await;
        let (dir, client) = setup();
        let markets_path = dir.path().join("markets.jsonl");
        write_markets(&markets_path, vec!["0xasset1".to_string()]);
        let output_dir = dir.path().join("trades");

        let url =
            "https://data-api.polymarket.com/trades?asset=0xasset1&limit=500&offset=0".to_string();
        client.set_response(
            &url,
            Ok(HttpResponse {
                status: 200,
                body: serde_json::to_string(&serde_json::json!([{
                    "proxyWallet": "0xwallet",
                    "side": "BUY",
                    "asset": "0xasset1",
                    "conditionId": "0xcond",
                    "size": 1.5,
                    "price": 0.6,
                    "timestamp": 1_700_000_000_i64,
                    "title": "Test",
                    "slug": "test",
                    "outcome": "Yes",
                    "transactionHash": "0xabc",
                }]))
                .unwrap(),
            }),
        );

        let cmd = Commands::CollectTrades {
            markets_path: markets_path.clone(),
            output_dir: output_dir.clone(),
        };
        run_command(cmd).await.unwrap();

        let path = output_dir.join("0xasset1.jsonl");
        assert!(path.exists());
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("0xasset1"));
    }

    fn sample_order_filled_log() -> serde_json::Value {
        let topic0 = polymarket_collector::onchain::ORDER_FILLED_TOPIC;
        let address = polymarket_collector::onchain::CTF_EXCHANGE_V2;
        let side_word = format!("{:0>64}", "00");
        let token_word = format!("{:0>64}", "abc");
        let maker_amount = format!("{:0>64}", "1234");
        let taker_amount = format!("{:0>64}", "5678");
        let fee = format!("{:0>64}", "0");
        let builder = format!("{:0>64}", "abcd");
        let metadata = format!("{:0>64}", "ef01");
        let data = format!(
            "0x{}{}{}{}{}{}{}",
            side_word, token_word, maker_amount, taker_amount, fee, builder, metadata
        );
        serde_json::json!({
            "address": address,
            "topics": [
                topic0,
                "0xd980fee1cbe88b9fbca895573ec0296b5a049937556040671ca4eb90d612d473",
                "0x000000000000000000000000448861155279dbf833d041b963e3ac854599e319",
                "0x0000000000000000000000006f3c1ddc97c9abfb38ff0f1302a56a1946d04c6f",
            ],
            "data": data,
            "transactionHash": "0x5e5fe7c64a30b1d23366bf508ea288b994e3b3d8d5afd5facd991af8551dae02",
            "blockNumber": "0x5424d9d",
            "logIndex": "0x38e",
        })
    }

    #[tokio::test]
    async fn test_scrape_onchain_command_writes_trades() {
        let _lock = lock_test_state().await;
        let (dir, client) = setup();
        let output = dir.path().join("onchain.jsonl");
        let rpc_url = "https://polygon-rpc.com";
        let body = serde_json::to_string(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": [sample_order_filled_log()],
        }))
        .unwrap();
        client.set_response(rpc_url, Ok(HttpResponse { status: 200, body }));

        let cmd = Commands::ScrapeOnchain {
            rpc_url: rpc_url.to_string(),
            exchange: polymarket_collector::onchain::CTF_EXCHANGE_V2.to_string(),
            from_block: 1,
            to_block: 1,
            chunk_size: 1,
            output: output.clone(),
        };
        run_command(cmd).await.unwrap();

        assert!(output.exists());
        let content = std::fs::read_to_string(&output).unwrap();
        assert!(content.contains("0x448861155279dbf833d041b963e3ac854599e319"));
    }

    #[tokio::test]
    async fn test_analyze_rewards_command_runs() {
        let _lock = lock_test_state().await;
        let (dir, _client) = setup();
        let markets_path = dir.path().join("markets.jsonl");
        let mut file = std::fs::File::create(&markets_path).unwrap();
        writeln!(
            file,
            "{}",
            serde_json::to_string(&reward_market(10.0)).unwrap()
        )
        .unwrap();

        let cmd = Commands::AnalyzeRewards { markets_path };
        run_command(cmd).await.unwrap();
    }

    #[tokio::test]
    async fn test_viewer_command_starts() {
        let _lock = lock_test_state().await;
        let (dir, _client) = setup();
        let input_path = dir.path().join("aggregated.jsonl");
        std::fs::write(&input_path, "").unwrap();
        let port = find_free_port();
        let cmd = Commands::Viewer {
            input_path,
            bind: format!("127.0.0.1:{}", port),
            rpc_url: "https://polygon.drpc.org".to_string(),
        };
        let result = timeout(Duration::from_secs(5), run_command(cmd)).await;
        assert!(result.is_err(), "viewer should keep running until timeout");
    }

    #[tokio::test]
    async fn test_default_http_client_falls_back_to_reqwest() {
        let _lock = lock_test_state().await;
        test_helpers::clear_test_http_client();
        let _client = default_http_client();
        test_helpers::reset_test_http_client();
    }

    #[tokio::test]
    async fn test_dispatch_data_dir_falls_back_to_project_data_dir() {
        let _lock = lock_test_state().await;
        test_helpers::clear_test_data_dir();
        let path = dispatch_data_dir();
        assert!(path.to_string_lossy().ends_with("data"));
        test_helpers::set_test_data_dir(tempfile::tempdir().unwrap().path().to_path_buf());
    }

    #[tokio::test]
    async fn test_collect_orderbook_limit_tokens() {
        let _lock = lock_test_state().await;
        let (dir, _client) = setup();
        let markets_path = dir.path().join("markets.jsonl");
        write_markets(
            &markets_path,
            vec!["0xtoken1".to_string(), "0xtoken2".to_string()],
        );
        let cmd = Commands::CollectOrderbook {
            markets_path: None,
            static_markets_path: Some(markets_path),
            output_dir: dir.path().join("orderbook"),
            aggregator_url: None,
            s3_bucket: None,
            s3_prefix: "orderbook/".to_string(),
            aws_region: "us-east-1".to_string(),
            chunk_size: 100,
            rotate_interval_secs: 300,
            market_refresh_interval_secs: DEFAULT_MARKET_REFRESH_INTERVAL_SECS,
            stale_market_ttl_hours: DEFAULT_STALE_MARKET_TTL_HOURS,
            assignment_poll_interval_secs:
                polymarket_collector::ws_orderbook::DEFAULT_ASSIGNMENT_POLL_INTERVAL_SECS,
            duration_secs: Some(1),
            limit_tokens: Some(1),
        };
        let result = timeout(Duration::from_secs(15), run_command(cmd)).await;
        assert!(
            result.is_ok(),
            "collect orderbook with limit should complete"
        );
    }

    #[tokio::test]
    async fn test_collect_orderbook_limit_tokens_requires_static_path() {
        let _lock = lock_test_state().await;
        let (dir, _client) = setup();
        let cmd = Commands::CollectOrderbook {
            markets_path: None,
            static_markets_path: None,
            output_dir: dir.path().join("orderbook"),
            aggregator_url: None,
            s3_bucket: None,
            s3_prefix: "orderbook/".to_string(),
            aws_region: "us-east-1".to_string(),
            chunk_size: 100,
            rotate_interval_secs: 300,
            market_refresh_interval_secs: DEFAULT_MARKET_REFRESH_INTERVAL_SECS,
            stale_market_ttl_hours: DEFAULT_STALE_MARKET_TTL_HOURS,
            assignment_poll_interval_secs:
                polymarket_collector::ws_orderbook::DEFAULT_ASSIGNMENT_POLL_INTERVAL_SECS,
            duration_secs: Some(1),
            limit_tokens: Some(1),
        };
        let err = run_command(cmd).await.unwrap_err();
        assert!(err.to_string().contains("--limit-tokens is only supported"));
    }

    #[tokio::test]
    async fn test_collect_orderbook_default_live_fails_without_api_markets() {
        let _lock = lock_test_state().await;
        let (dir, _client) = setup();
        let cmd = Commands::CollectOrderbook {
            markets_path: None,
            static_markets_path: None,
            output_dir: dir.path().join("orderbook"),
            aggregator_url: None,
            s3_bucket: None,
            s3_prefix: "orderbook/".to_string(),
            aws_region: "us-east-1".to_string(),
            chunk_size: 100,
            rotate_interval_secs: 300,
            market_refresh_interval_secs: DEFAULT_MARKET_REFRESH_INTERVAL_SECS,
            stale_market_ttl_hours: DEFAULT_STALE_MARKET_TTL_HOURS,
            assignment_poll_interval_secs:
                polymarket_collector::ws_orderbook::DEFAULT_ASSIGNMENT_POLL_INTERVAL_SECS,
            duration_secs: None,
            limit_tokens: None,
        };
        let err = run_command(cmd).await.unwrap_err();
        assert!(err
            .to_string()
            .contains("initial standalone live market fetch failed"));
    }

    #[tokio::test]
    async fn test_collect_orderbook_default_live_runs_from_api() {
        let _lock = lock_test_state().await;
        let (dir, client) = setup();
        set_live_markets_response(&client, vec!["0xtoken1".to_string()]);
        let cmd = Commands::CollectOrderbook {
            markets_path: None,
            static_markets_path: None,
            output_dir: dir.path().join("orderbook"),
            aggregator_url: None,
            s3_bucket: None,
            s3_prefix: "orderbook/".to_string(),
            aws_region: "us-east-1".to_string(),
            chunk_size: 100,
            rotate_interval_secs: 300,
            market_refresh_interval_secs: DEFAULT_MARKET_REFRESH_INTERVAL_SECS,
            stale_market_ttl_hours: DEFAULT_STALE_MARKET_TTL_HOURS,
            assignment_poll_interval_secs:
                polymarket_collector::ws_orderbook::DEFAULT_ASSIGNMENT_POLL_INTERVAL_SECS,
            duration_secs: Some(1),
            limit_tokens: None,
        };
        let result = timeout(Duration::from_secs(15), run_command(cmd)).await;
        assert!(
            result.is_ok(),
            "default live orderbook collection should complete within timeout"
        );
    }

    #[tokio::test]
    async fn test_collect_orderbook_static_path_conflicts_with_aggregator() {
        let _lock = lock_test_state().await;
        let (dir, _client) = setup();
        let markets_path = dir.path().join("markets.jsonl");
        write_markets(&markets_path, vec!["0xtoken1".to_string()]);
        let cmd = Commands::CollectOrderbook {
            markets_path: None,
            static_markets_path: Some(markets_path),
            output_dir: dir.path().join("orderbook"),
            aggregator_url: Some("http://127.0.0.1:1".to_string()),
            s3_bucket: None,
            s3_prefix: "orderbook/".to_string(),
            aws_region: "us-east-1".to_string(),
            chunk_size: 100,
            rotate_interval_secs: 300,
            market_refresh_interval_secs: DEFAULT_MARKET_REFRESH_INTERVAL_SECS,
            stale_market_ttl_hours: DEFAULT_STALE_MARKET_TTL_HOURS,
            assignment_poll_interval_secs:
                polymarket_collector::ws_orderbook::DEFAULT_ASSIGNMENT_POLL_INTERVAL_SECS,
            duration_secs: Some(1),
            limit_tokens: None,
        };
        let err = run_command(cmd).await.unwrap_err();
        assert!(err
            .to_string()
            .contains("Static market paths cannot be combined"));
    }

    #[tokio::test]
    async fn test_aggregator_command_fails_when_initial_live_fetch_fails() {
        let _lock = lock_test_state().await;
        let (dir, _client) = setup();
        let cmd = Commands::Aggregator {
            bind: "127.0.0.1:18080".to_string(),
            output_path: dir.path().join("out.jsonl"),
            markets_path: None,
            s3_bucket: "dummy".to_string(),
            s3_prefix: "orderbook/".to_string(),
            replication_factor: 1,
            heartbeat_timeout_secs: 60,
            market_refresh_interval_secs: DEFAULT_MARKET_REFRESH_INTERVAL_SECS,
            stale_market_ttl_hours: DEFAULT_STALE_MARKET_TTL_HOURS,
            delete_after_merge: false,
            region: "us-east-1".to_string(),
        };
        let err = run_command(cmd).await.unwrap_err();
        assert!(err.to_string().contains("initial live market fetch failed"));
    }

    #[tokio::test]
    async fn test_collect_trades_command_missing_markets_file() {
        let _lock = lock_test_state().await;
        let (dir, _client) = setup();
        let cmd = Commands::CollectTrades {
            markets_path: dir.path().join("missing.jsonl"),
            output_dir: dir.path().join("trades"),
        };
        let err = run_command(cmd).await.unwrap_err();
        assert!(err.to_string().contains("Markets file not found"));
    }

    #[tokio::test]
    async fn test_analyze_rewards_command_missing_markets_file() {
        let _lock = lock_test_state().await;
        let (dir, _client) = setup();
        let cmd = Commands::AnalyzeRewards {
            markets_path: dir.path().join("missing.jsonl"),
        };
        let err = run_command(cmd).await.unwrap_err();
        assert!(err.to_string().contains("Markets file not found"));
    }

    #[tokio::test]
    async fn test_viewer_command_missing_input_file() {
        let _lock = lock_test_state().await;
        let (dir, _client) = setup();
        let cmd = Commands::Viewer {
            input_path: dir.path().join("missing.jsonl"),
            bind: "127.0.0.1:13000".to_string(),
            rpc_url: "https://polygon.drpc.org".to_string(),
        };
        let err = run_command(cmd).await.unwrap_err();
        assert!(err.to_string().contains("Input file not found"));
    }

    async fn run_minimal_aggregator(
        addr_tx: tokio::sync::oneshot::Sender<std::net::SocketAddr>,
        shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>,
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
                        "version": 1,
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
            while !shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_future)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_collect_orderbook_orchestrated_with_local_aggregator() {
        let _lock = lock_test_state().await;
        let (dir, _client) = setup();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let sd = shutdown.clone();
        tokio::spawn(async move { run_minimal_aggregator(tx, sd).await });
        let addr = rx.await.unwrap();
        let aggregator_url = format!("http://{}", addr);

        let cmd = Commands::CollectOrderbook {
            markets_path: None,
            static_markets_path: None,
            output_dir: dir.path().join("orderbook"),
            aggregator_url: Some(aggregator_url),
            s3_bucket: None,
            s3_prefix: "orderbook/".to_string(),
            aws_region: "us-east-1".to_string(),
            chunk_size: 100,
            rotate_interval_secs: 300,
            market_refresh_interval_secs: DEFAULT_MARKET_REFRESH_INTERVAL_SECS,
            stale_market_ttl_hours: DEFAULT_STALE_MARKET_TTL_HOURS,
            assignment_poll_interval_secs:
                polymarket_collector::ws_orderbook::DEFAULT_ASSIGNMENT_POLL_INTERVAL_SECS,
            duration_secs: Some(1),
            limit_tokens: None,
        };
        let result = timeout(Duration::from_secs(30), run_command(cmd)).await;
        assert!(result.is_ok(), "orchestrated collect should complete");
        result
            .unwrap()
            .expect("orchestrated collect should succeed");
        shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    #[tokio::test]
    async fn test_collect_trades_command_saves_multiple_trades() {
        let _lock = lock_test_state().await;
        let (dir, client) = setup();
        let markets_path = dir.path().join("markets.jsonl");
        write_markets(
            &markets_path,
            vec!["0xasset1".to_string(), "0xasset2".to_string()],
        );
        let output_dir = dir.path().join("trades");

        for asset in ["0xasset1", "0xasset2"] {
            let url = format!(
                "https://data-api.polymarket.com/trades?asset={}&limit=500&offset=0",
                asset
            );
            client.set_response(
                &url,
                Ok(HttpResponse {
                    status: 200,
                    body: serde_json::to_string(&serde_json::json!([{
                        "proxyWallet": "0xwallet",
                        "side": "BUY",
                        "asset": asset,
                        "conditionId": "0xcond",
                        "size": 1.5,
                        "price": 0.6,
                        "timestamp": 1_700_000_000_i64,
                        "title": "Test",
                        "slug": "test",
                        "outcome": "Yes",
                        "transactionHash": "0xabc",
                    }]))
                    .unwrap(),
                }),
            );
        }

        let cmd = Commands::CollectTrades {
            markets_path,
            output_dir,
        };
        run_command(cmd).await.unwrap();

        assert!(dir.path().join("trades").join("0xasset1.jsonl").exists());
        assert!(dir.path().join("trades").join("0xasset2.jsonl").exists());
    }
}
