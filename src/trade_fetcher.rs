use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{info, warn};

/// A single trade from Polymarket Data API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub proxy_wallet: String,
    pub side: String,
    pub asset: String,
    pub condition_id: String,
    pub size: f64,
    pub price: f64,
    pub timestamp: i64,
    pub title: String,
    pub slug: String,
    pub outcome: String,
    pub transaction_hash: String,
}

/// Fetch one page of trades for a given asset.
pub async fn fetch_trades(
    client: &reqwest::Client,
    asset: &str,
    limit: usize,
    offset: usize,
) -> Result<Vec<Trade>> {
    let mut url = reqwest::Url::parse("https://data-api.polymarket.com/trades")?;
    {
        let mut q = url.query_pairs_mut();
        q.append_pair("asset", asset);
        q.append_pair("limit", &limit.to_string());
        q.append_pair("offset", &offset.to_string());
    }

    let resp = client.get(url).send().await?;
    if !resp.status().is_success() {
        let text = resp.text().await?;
        anyhow::bail!("Data API error: {}", text);
    }

    let trades: Vec<serde_json::Value> = resp.json().await?;
    let mut result = Vec::with_capacity(trades.len());
    for t in trades {
        result.push(Trade {
            proxy_wallet: t
                .get("proxyWallet")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            side: t
                .get("side")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            asset: t
                .get("asset")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            condition_id: t
                .get("conditionId")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            size: t.get("size").and_then(|v| v.as_f64()).unwrap_or(0.0),
            price: t.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0),
            timestamp: t.get("timestamp").and_then(|v| v.as_i64()).unwrap_or(0),
            title: t
                .get("title")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            slug: t
                .get("slug")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            outcome: t
                .get("outcome")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            transaction_hash: t
                .get("transactionHash")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
        });
    }
    Ok(result)
}

/// Backfill all historical trades for a single asset.
/// Data API /trades limit is 200/10s → sleep 55 ms between calls.
pub async fn backfill_trades_for_asset(
    client: &reqwest::Client,
    asset: &str,
) -> Result<Vec<Trade>> {
    let mut all_trades = Vec::new();
    let mut offset = 0usize;
    let limit = 500;

    loop {
        match fetch_trades(client, asset, limit, offset).await {
            Ok(trades) => {
                if trades.is_empty() {
                    break;
                }
                let count = trades.len();
                all_trades.extend(trades);
                info!(asset, offset, count, total = all_trades.len(), "Fetched trades");
                if count < limit {
                    break;
                }
                offset += limit;
            }
            Err(e) => {
                let msg = format!("{}", e);
                if msg.contains("max historical activity offset") {
                    info!(asset, offset, "Reached max historical offset, stopping backfill");
                    break;
                }
                warn!(asset, offset, error = %e, "Trade fetch failed, retrying after delay");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }

        // Respect rate limit: 200 req / 10s ≈ 20 req/s.
        tokio::time::sleep(Duration::from_millis(55)).await;
    }

    Ok(all_trades)
}
