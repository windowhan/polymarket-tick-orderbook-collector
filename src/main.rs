use anyhow::Result;
use clap::{Parser, Subcommand};
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
    /// In orchestrated mode (--aggregator-url), the collector registers with the
    /// aggregator and receives its market assignment dynamically. Otherwise it
    /// reads token IDs from --markets-path.
    CollectOrderbook {
        /// Path to markets.jsonl. Required in static mode; optional when
        /// --aggregator-url is provided.
        #[arg(long, default_value = "data/markets/markets.jsonl")]
        markets_path: Option<PathBuf>,
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
        /// Run for N seconds then graceful shutdown (for testing)
        #[arg(long)]
        duration_secs: Option<u64>,
        /// Limit total tokens for load testing
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
        /// Path to full markets.jsonl used for market discovery and allocation.
        #[arg(long, default_value = "data/markets/markets.jsonl")]
        markets_path: PathBuf,
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
            output_dir,
            aggregator_url,
            s3_bucket,
            s3_prefix,
            aws_region,
            chunk_size,
            rotate_interval_secs,
            duration_secs,
            limit_tokens,
        } => {
            // In orchestrated mode, token IDs come from the aggregator.
            // In static mode, they come from the markets file.
            let token_ids = if let Some(_url) = &aggregator_url {
                // Orchestrated mode: token IDs will be fetched from aggregator at runtime.
                Vec::new()
            } else {
                let path = markets_path
                    .clone()
                    .unwrap_or_else(|| PathBuf::from("data/markets/markets.jsonl"));
                if !path.exists() {
                    anyhow::bail!("Markets file not found: {}", path.display());
                }

                let content = tokio::fs::read_to_string(&path).await?;
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
            };

            if aggregator_url.is_none() && markets_path.is_none() {
                anyhow::bail!("Either --markets-path or --aggregator-url must be provided");
            }

            info!(
                token_count = token_ids.len(),
                chunk_size,
                aggregator_url = ?aggregator_url,
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
                collector = collector.with_aggregator_url(url);
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
            delete_after_merge,
            region,
        } => {
            if !markets_path.exists() {
                anyhow::bail!("Markets file not found: {}", markets_path.display());
            }

            info!(
                bind = %bind,
                path = %output_path.display(),
                markets = %markets_path.display(),
                bucket = %s3_bucket,
                prefix = %s3_prefix,
                replication_factor,
                heartbeat_timeout_secs,
                "Starting orchestrated aggregator"
            );

            polymarket_collector::aggregator::run(
                &bind,
                output_path,
                markets_path,
                s3_bucket,
                s3_prefix,
                replication_factor,
                Duration::from_secs(heartbeat_timeout_secs),
                delete_after_merge,
                region,
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
                let trades =
                    polymarket_collector::trade_fetcher::backfill_trades_for_asset(
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

        Commands::Viewer { input_path, bind, rpc_url } => {
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
    /// are not mutated by concurrent tests.
    static TEST_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn setup() -> (tempfile::TempDir, Arc<InMemoryHttpClient>) {
        let dir = tempdir().unwrap();
        test_helpers::set_test_data_dir(dir.path().to_path_buf());
        let client = test_helpers::reset_test_http_client();
        (dir, client)
    }

    fn market_json(token_ids: Vec<String>) -> serde_json::Value {
        serde_json::json!({
            "id": "m1",
            "condition_id": "cond1",
            "question": "Will it rain?",
            "slug": "rain",
            "active": true,
            "closed": false,
            "archived": false,
            "clobTokenIds": serde_json::to_string(&token_ids).unwrap(),
            "clob_rewards": [],
            "token_ids": token_ids,
            "enable_order_book": true,
            "neg_risk": false,
            "accepting_orders": true,
        })
    }

    fn write_markets(path: &std::path::Path, token_ids: Vec<String>) {
        let mut file = std::fs::File::create(path).unwrap();
        writeln!(file, "{}", serde_json::to_string(&market_json(token_ids)).unwrap()).unwrap();
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
        let _lock = TEST_MUTEX.lock().unwrap();
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

        let cmd = Commands::Discover { active: None, closed: None };
        run_command(cmd).await.unwrap();

        let path = dir.path().join("markets").join("markets.jsonl");
        assert!(path.exists());
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("0xtoken1"));
    }

    #[tokio::test]
    async fn test_collect_orderbook_static_missing_file() {
        let _lock = TEST_MUTEX.lock().unwrap();
        let (dir, _client) = setup();
        let cmd = Commands::CollectOrderbook {
            markets_path: Some(dir.path().join("nonexistent.jsonl")),
            output_dir: dir.path().join("orderbook"),
            aggregator_url: None,
            s3_bucket: None,
            s3_prefix: "orderbook/".to_string(),
            aws_region: "us-east-1".to_string(),
            chunk_size: 100,
            rotate_interval_secs: 300,
            duration_secs: None,
            limit_tokens: None,
        };
        let err = run_command(cmd).await.unwrap_err();
        assert!(err.to_string().contains("Markets file not found"));
    }

    #[tokio::test]
    async fn test_collect_orderbook_static_empty_tokens() {
        let _lock = TEST_MUTEX.lock().unwrap();
        let (dir, _client) = setup();
        let markets_path = dir.path().join("markets.jsonl");
        write_markets(&markets_path, vec![]);
        let cmd = Commands::CollectOrderbook {
            markets_path: Some(markets_path),
            output_dir: dir.path().join("orderbook"),
            aggregator_url: None,
            s3_bucket: None,
            s3_prefix: "orderbook/".to_string(),
            aws_region: "us-east-1".to_string(),
            chunk_size: 100,
            rotate_interval_secs: 300,
            duration_secs: None,
            limit_tokens: None,
        };
        let err = run_command(cmd).await.unwrap_err();
        assert!(err.to_string().contains("No token IDs found"));
    }

    #[tokio::test]
    async fn test_collect_orderbook_static_runs() {
        let _lock = TEST_MUTEX.lock().unwrap();
        let (dir, _client) = setup();
        let markets_path = dir.path().join("markets.jsonl");
        write_markets(&markets_path, vec!["0xtoken1".to_string()]);
        let output_dir = dir.path().join("orderbook");
        let cmd = Commands::CollectOrderbook {
            markets_path: Some(markets_path),
            output_dir,
            aggregator_url: None,
            s3_bucket: None,
            s3_prefix: "orderbook/".to_string(),
            aws_region: "us-east-1".to_string(),
            chunk_size: 100,
            rotate_interval_secs: 300,
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
        let _lock = TEST_MUTEX.lock().unwrap();
        let (dir, _client) = setup();
        let markets_path = dir.path().join("markets.jsonl");
        write_markets(&markets_path, vec![]);
        // Point at an unreachable aggregator so registration fails quickly.
        let cmd = Commands::CollectOrderbook {
            markets_path: Some(markets_path),
            output_dir: dir.path().join("orderbook"),
            aggregator_url: Some("http://127.0.0.1:1".to_string()),
            s3_bucket: None,
            s3_prefix: "orderbook/".to_string(),
            aws_region: "us-east-1".to_string(),
            chunk_size: 100,
            rotate_interval_secs: 300,
            duration_secs: Some(1),
            limit_tokens: None,
        };
        let err = run_command(cmd).await.unwrap_err();
        assert!(err.to_string().contains("Failed to register"));
    }

    #[tokio::test]
    async fn test_aggregator_command_starts() {
        let _lock = TEST_MUTEX.lock().unwrap();
        let (dir, _client) = setup();
        let markets_path = dir.path().join("markets.jsonl");
        write_markets(&markets_path, vec!["0xtoken1".to_string()]);
        let output_path = dir.path().join("aggregated.jsonl");
        let port = find_free_port();
        let cmd = Commands::Aggregator {
            bind: format!("127.0.0.1:{}", port),
            output_path,
            markets_path,
            s3_bucket: "dummy-bucket".to_string(),
            s3_prefix: "orderbook/".to_string(),
            replication_factor: 1,
            heartbeat_timeout_secs: 60,
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
        let _lock = TEST_MUTEX.lock().unwrap();
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
        let _lock = TEST_MUTEX.lock().unwrap();
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
        let _lock = TEST_MUTEX.lock().unwrap();
        let (dir, _client) = setup();
        let markets_path = dir.path().join("markets.jsonl");
        let mut file = std::fs::File::create(&markets_path).unwrap();
        writeln!(file, "{}", serde_json::to_string(&reward_market(10.0)).unwrap()).unwrap();

        let cmd = Commands::AnalyzeRewards { markets_path };
        run_command(cmd).await.unwrap();
    }

    #[tokio::test]
    async fn test_viewer_command_starts() {
        let _lock = TEST_MUTEX.lock().unwrap();
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
        let _lock = TEST_MUTEX.lock().unwrap();
        test_helpers::clear_test_http_client();
        let _client = default_http_client();
        test_helpers::reset_test_http_client();
    }

    #[test]
    fn test_dispatch_data_dir_falls_back_to_project_data_dir() {
        let _lock = TEST_MUTEX.lock().unwrap();
        test_helpers::clear_test_data_dir();
        let path = dispatch_data_dir();
        assert!(path.to_string_lossy().ends_with("data"));
        test_helpers::set_test_data_dir(tempfile::tempdir().unwrap().path().to_path_buf());
    }

    #[tokio::test]
    async fn test_collect_orderbook_limit_tokens() {
        let _lock = TEST_MUTEX.lock().unwrap();
        let (dir, _client) = setup();
        let markets_path = dir.path().join("markets.jsonl");
        write_markets(&markets_path, vec!["0xtoken1".to_string(), "0xtoken2".to_string()]);
        let cmd = Commands::CollectOrderbook {
            markets_path: Some(markets_path),
            output_dir: dir.path().join("orderbook"),
            aggregator_url: None,
            s3_bucket: None,
            s3_prefix: "orderbook/".to_string(),
            aws_region: "us-east-1".to_string(),
            chunk_size: 100,
            rotate_interval_secs: 300,
            duration_secs: Some(1),
            limit_tokens: Some(1),
        };
        let result = timeout(Duration::from_secs(15), run_command(cmd)).await;
        assert!(result.is_ok(), "collect orderbook with limit should complete");
    }

    #[tokio::test]
    async fn test_collect_orderbook_needs_markets_path_or_aggregator() {
        let _lock = TEST_MUTEX.lock().unwrap();
        let (dir, _client) = setup();
        let cmd = Commands::CollectOrderbook {
            markets_path: None,
            output_dir: dir.path().join("orderbook"),
            aggregator_url: None,
            s3_bucket: None,
            s3_prefix: "orderbook/".to_string(),
            aws_region: "us-east-1".to_string(),
            chunk_size: 100,
            rotate_interval_secs: 300,
            duration_secs: None,
            limit_tokens: None,
        };
        let err = run_command(cmd).await.unwrap_err();
        assert!(err.to_string().contains("Either --markets-path or --aggregator-url"));
    }

    #[tokio::test]
    async fn test_aggregator_command_missing_markets_file() {
        let _lock = TEST_MUTEX.lock().unwrap();
        let (dir, _client) = setup();
        let cmd = Commands::Aggregator {
            bind: "127.0.0.1:18080".to_string(),
            output_path: dir.path().join("out.jsonl"),
            markets_path: dir.path().join("missing.jsonl"),
            s3_bucket: "dummy".to_string(),
            s3_prefix: "orderbook/".to_string(),
            replication_factor: 1,
            heartbeat_timeout_secs: 60,
            delete_after_merge: false,
            region: "us-east-1".to_string(),
        };
        let err = run_command(cmd).await.unwrap_err();
        assert!(err.to_string().contains("Markets file not found"));
    }

    #[tokio::test]
    async fn test_collect_trades_command_missing_markets_file() {
        let _lock = TEST_MUTEX.lock().unwrap();
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
        let _lock = TEST_MUTEX.lock().unwrap();
        let (dir, _client) = setup();
        let cmd = Commands::AnalyzeRewards {
            markets_path: dir.path().join("missing.jsonl"),
        };
        let err = run_command(cmd).await.unwrap_err();
        assert!(err.to_string().contains("Markets file not found"));
    }

    #[tokio::test]
    async fn test_viewer_command_missing_input_file() {
        let _lock = TEST_MUTEX.lock().unwrap();
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
                        "token_ids": [],
                        "chunk_size": 100,
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
        let _lock = TEST_MUTEX.lock().unwrap();
        let (dir, _client) = setup();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let sd = shutdown.clone();
        tokio::spawn(async move { run_minimal_aggregator(tx, sd).await });
        let addr = rx.await.unwrap();
        let aggregator_url = format!("http://{}", addr);

        let cmd = Commands::CollectOrderbook {
            markets_path: None,
            output_dir: dir.path().join("orderbook"),
            aggregator_url: Some(aggregator_url),
            s3_bucket: None,
            s3_prefix: "orderbook/".to_string(),
            aws_region: "us-east-1".to_string(),
            chunk_size: 100,
            rotate_interval_secs: 300,
            duration_secs: Some(1),
            limit_tokens: None,
        };
        let result = timeout(Duration::from_secs(30), run_command(cmd)).await;
        assert!(result.is_ok(), "orchestrated collect should complete");
        result.unwrap().expect("orchestrated collect should succeed");
        shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    #[tokio::test]
    async fn test_collect_trades_command_saves_multiple_trades() {
        let _lock = TEST_MUTEX.lock().unwrap();
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
