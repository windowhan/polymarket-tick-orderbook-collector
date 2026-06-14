use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::time::Duration;
use tracing::info;

#[derive(Parser)]
#[command(name = "polymarket-collector")]
#[command(about = "Polymarket tick-level order book and trade collector (Rust)")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Discover { active, closed } => {
            info!("Starting market discovery...");
            let count = polymarket_collector::market_discovery::discover_and_save(active, closed).await?;
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
                let path = markets_path.clone().unwrap_or_else(|| PathBuf::from("data/markets/markets.jsonl"));
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
                std::time::Duration::from_secs(rotate_interval_secs),
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
            let client = reqwest::Client::new();

            for asset in assets {
                let trades = polymarket_collector::trade_fetcher::backfill_trades_for_asset(&client, &asset)
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
            let client = reqwest::Client::new();
            let count = polymarket_collector::onchain::scrape_exchange(
                &client,
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
