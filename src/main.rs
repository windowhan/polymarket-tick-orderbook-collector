use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
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
    CollectOrderbook {
        #[arg(long, default_value = "data/markets/markets.jsonl")]
        markets_path: PathBuf,
        #[arg(long, default_value = "data/orderbook")]
        output_dir: PathBuf,
        /// Relay URL for central aggregator (e.g. http://aggregator:8080/ingest)
        #[arg(long)]
        relay_url: Option<String>,
        /// Tokens per WebSocket connection (default 30)
        #[arg(long, default_value = "30")]
        chunk_size: usize,
        /// Seconds between file rotations for local storage (default 300)
        #[arg(long, default_value = "300")]
        rotate_interval_secs: u64,
        /// Limit total tokens for load testing
        #[arg(long)]
        limit_tokens: Option<usize>,
    },
    /// Run central aggregator server that receives relayed orderbook data
    Aggregator {
        #[arg(long, default_value = "0.0.0.0:8080")]
        bind: String,
        #[arg(long, default_value = "data/aggregated_orderbook.jsonl")]
        output_path: PathBuf,
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
        #[arg(long, default_value = "https://rpc-mainnet.matic.quiknode.pro")]
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
    /// Split markets.jsonl into N shards for distributed collection
    SplitMarkets {
        #[arg(long, default_value = "data/markets/markets.jsonl")]
        markets_path: PathBuf,
        #[arg(long, default_value = "data/markets/shards")]
        output_dir: PathBuf,
        #[arg(long, default_value = "10")]
        shards: usize,
    },
    /// Analyze reward size distribution and competitive intensity
    AnalyzeRewards {
        #[arg(long, default_value = "data/markets/markets.jsonl")]
        markets_path: PathBuf,
    },
    /// Aggregate S3 orderbook shards into a single local file
    AggregateS3 {
        #[arg(long)]
        bucket: String,
        #[arg(long, default_value = "orderbook/")]
        prefix: String,
        #[arg(long, default_value = "data/aggregated_orderbook.jsonl")]
        output_path: PathBuf,
        #[arg(long, default_value = "us-east-1")]
        region: String,
        #[arg(long)]
        endpoint: Option<String>,
        #[arg(long)]
        access_key: Option<String>,
        #[arg(long)]
        secret_key: Option<String>,
        #[arg(long)]
        delete_after_merge: bool,
        #[arg(long, default_value = "data/aggregate_manifest.json")]
        manifest_path: PathBuf,
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
            relay_url,
            chunk_size,
            rotate_interval_secs,
            limit_tokens,
        } => {
            if !markets_path.exists() {
                anyhow::bail!("Markets file not found: {}", markets_path.display());
            }

            let content = tokio::fs::read_to_string(&markets_path).await?;
            let mut token_ids = Vec::new();
            for line in content.lines() {
                if let Ok(market) =
                    serde_json::from_str::<polymarket_collector::market_discovery::Market>(line)
                {
                    token_ids.extend(market.token_ids);
                }
            }

            if let Some(limit) = limit_tokens {
                token_ids.truncate(limit);
            }

            if token_ids.is_empty() {
                anyhow::bail!("No token IDs found in markets file");
            }

            info!(count = token_ids.len(), chunk_size, relay_url = ?relay_url, "Starting order book collection");
            let collector =
                polymarket_collector::ws_orderbook::OrderbookCollector::new(token_ids, output_dir, relay_url, chunk_size, std::time::Duration::from_secs(rotate_interval_secs));
            collector.run().await?;
        }

        Commands::Aggregator { bind, output_path } => {
            info!(bind = %bind, path = %output_path.display(), "Starting aggregator");
            polymarket_collector::aggregator::run(&bind, output_path).await?;
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
                let trades =
                    polymarket_collector::trade_fetcher::backfill_trades_for_asset(&client, &asset)
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

        Commands::SplitMarkets {
            markets_path,
            output_dir,
            shards,
        } => {
            if !markets_path.exists() {
                anyhow::bail!("Markets file not found: {}", markets_path.display());
            }

            std::fs::create_dir_all(&output_dir)?;
            let content = tokio::fs::read_to_string(&markets_path).await?;
            let lines: Vec<&str> = content.lines().collect();
            let chunk_size = (lines.len() + shards - 1) / shards;

            for (i, chunk) in lines.chunks(chunk_size).enumerate() {
                let path = output_dir.join(format!("markets_shard_{}.jsonl", i));
                let shard: Vec<serde_json::Value> = chunk
                    .iter()
                    .filter_map(|line| serde_json::from_str(line).ok())
                    .collect();
                polymarket_collector::storage::append_jsonl(&path, &shard).await?;
                info!(shard = i, count = shard.len(), path = %path.display(), "Wrote shard");
            }
        }

        Commands::AnalyzeRewards { markets_path } => {
            if !markets_path.exists() {
                anyhow::bail!("Markets file not found: {}", markets_path.display());
            }
            let buckets = polymarket_collector::reward_analyzer::analyze_rewards(&markets_path)?;
            polymarket_collector::reward_analyzer::print_reward_analysis(&buckets);
        }

        Commands::AggregateS3 {
            bucket,
            prefix,
            output_path,
            region,
            endpoint,
            access_key,
            secret_key,
            delete_after_merge,
            manifest_path,
        } => {
            use polymarket_collector::aggregate_s3::{aggregate_s3, write_manifest, AggregateOptions, AwsS3Service};

            let service: Box<dyn polymarket_collector::aggregate_s3::S3Service> =
                if let (Some(endpoint), Some(access_key), Some(secret_key)) =
                    (endpoint, access_key, secret_key)
                {
                    Box::new(AwsS3Service::from_endpoint(region, endpoint, access_key, secret_key))
                } else {
                    Box::new(AwsS3Service::new(region).await)
                };

            let opts = AggregateOptions {
                bucket,
                prefix,
                output_path,
                delete_after_merge,
            };

            let summary = aggregate_s3(service.as_ref(), &opts).await?;
            write_manifest(&manifest_path, summary, &opts).await?;
            info!(?summary, "S3 aggregation complete");
        }
    }

    Ok(())
}
