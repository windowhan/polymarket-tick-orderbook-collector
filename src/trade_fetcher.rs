use crate::http_client::HttpClient;
use anyhow::{Context, Result};
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

fn trades_url(asset: &str, limit: usize, offset: usize) -> String {
    format!(
        "https://data-api.polymarket.com/trades?asset={}&limit={}&offset={}",
        asset, limit, offset
    )
}

/// Fetch one page of trades for a given asset.
pub async fn fetch_trades(
    client: &dyn HttpClient,
    asset: &str,
    limit: usize,
    offset: usize,
) -> Result<Vec<Trade>> {
    let url = trades_url(asset, limit, offset);

    let response = client
        .get(&url)
        .await
        .with_context(|| format!("Failed to fetch trades for {} from {}", asset, url))?;
    if !response.is_success() {
        anyhow::bail!("Data API error: {}", response.body);
    }

    let trades: Vec<serde_json::Value> = response.json()?;
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
    client: &dyn HttpClient,
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
                info!("Fetched trades");
                if count < limit {
                    break;
                }
                offset += limit;
            }
            Err(e) => {
                let root = e.root_cause().to_string();
                if root.contains("max historical activity offset") {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_client::{HttpResponse, InMemoryHttpClient};

    fn trade_json(asset: &str, idx: usize) -> serde_json::Value {
        serde_json::json!({
            "proxyWallet": "0xwallet",
            "side": "BUY",
            "asset": asset,
            "conditionId": "0xcond",
            "size": 1.5,
            "price": 0.6,
            "timestamp": 1_700_000_000 + idx as i64,
            "title": "Test",
            "slug": "test",
            "outcome": "Yes",
            "transactionHash": "0xabc",
        })
    }

    fn ok_trades(trades: Vec<serde_json::Value>) -> HttpResponse {
        HttpResponse {
            status: 200,
            body: serde_json::to_string(&trades).unwrap(),
        }
    }

    #[tokio::test]
    async fn test_fetch_trades_success() {
        let client = InMemoryHttpClient::new();
        let asset = "0xasset";
        client.set_response(
            &trades_url(asset, 10, 0),
            Ok(ok_trades(vec![trade_json(asset, 0)])),
        );

        let trades = fetch_trades(&client, asset, 10, 0).await.unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].asset, asset);
    }

    #[tokio::test]
    async fn test_fetch_trades_api_error() {
        let client = InMemoryHttpClient::new();
        let asset = "0xasset";
        client.set_response(
            &trades_url(asset, 10, 0),
            Ok(HttpResponse {
                status: 503,
                body: "busy".into(),
            }),
        );

        let err = fetch_trades(&client, asset, 10, 0).await.unwrap_err();
        assert!(err.to_string().contains("Data API error"));
    }

    #[tokio::test]
    async fn test_fetch_trades_malformed_defaults() {
        let client = InMemoryHttpClient::new();
        let asset = "0xasset";
        client.set_response(
            &trades_url(asset, 10, 0),
            Ok(ok_trades(vec![serde_json::json!({
                "size": "not-a-number",
                "price": null,
                "timestamp": true,
            })])),
        );

        let trades = fetch_trades(&client, asset, 10, 0).await.unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].size, 0.0);
        assert_eq!(trades[0].price, 0.0);
        assert_eq!(trades[0].timestamp, 0);
        assert_eq!(trades[0].asset, "");
    }

    #[tokio::test]
    async fn test_backfill_empty() {
        let client = InMemoryHttpClient::new();
        let asset = "0xasset";
        client.set_response(&trades_url(asset, 500, 0), Ok(ok_trades(vec![])));

        let trades = backfill_trades_for_asset(&client, asset).await.unwrap();
        assert!(trades.is_empty());
    }

    #[tokio::test]
    async fn test_backfill_pagination() {
        let client = InMemoryHttpClient::new();
        let asset = "0xasset";
        let first: Vec<serde_json::Value> =
            (0..500).map(|i| trade_json(asset, i)).collect();
        let second: Vec<serde_json::Value> =
            (500..750).map(|i| trade_json(asset, i)).collect();

        client.set_response(&trades_url(asset, 500, 0), Ok(ok_trades(first)));
        client.set_response(&trades_url(asset, 500, 500), Ok(ok_trades(second)));

        let trades = backfill_trades_for_asset(&client, asset).await.unwrap();
        assert_eq!(trades.len(), 750);
        assert_eq!(client.request_count(&trades_url(asset, 500, 0)), 1);
        assert_eq!(client.request_count(&trades_url(asset, 500, 500)), 1);
    }

    #[tokio::test]
    async fn test_backfill_stops_at_max_offset() {
        let client = InMemoryHttpClient::new();
        let asset = "0xasset";
        client.set_response(
            &trades_url(asset, 500, 0),
            Err("max historical activity offset reached".into()),
        );

        let trades = backfill_trades_for_asset(&client, asset).await.unwrap();
        assert!(trades.is_empty());
        assert_eq!(client.request_count(&trades_url(asset, 500, 0)), 1);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_backfill_retries_then_succeeds() {
        let client = InMemoryHttpClient::new();
        let asset = "0xasset";
        client.set_response_sequence(
            &trades_url(asset, 500, 0),
            vec![
                Err("transient failure".into()),
                Ok(ok_trades(vec![trade_json(asset, 0)])),
            ],
        );

        let trades = backfill_trades_for_asset(&client, asset).await.unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(client.request_count(&trades_url(asset, 500, 0)), 2);
    }

    #[tokio::test]
    async fn test_fetch_trades_network_error() {
        let client = InMemoryHttpClient::new();
        let asset = "0xasset";
        client.set_response(
            &trades_url(asset, 10, 0),
            Err("connection refused".into()),
        );

        let err = fetch_trades(&client, asset, 10, 0).await.unwrap_err();
        assert!(err.to_string().contains("Failed to fetch trades"));
    }

    #[tokio::test]
    async fn test_fetch_trades_invalid_json() {
        let client = InMemoryHttpClient::new();
        let asset = "0xasset";
        client.set_response(
            &trades_url(asset, 10, 0),
            Ok(HttpResponse {
                status: 200,
                body: "not a json array".into(),
            }),
        );

        let err = fetch_trades(&client, asset, 10, 0).await.unwrap_err();
        assert!(err.to_string().contains("Failed to parse JSON"));
    }

    #[tokio::test]
    async fn test_backfill_full_page_then_max_offset() {
        let client = InMemoryHttpClient::new();
        let asset = "0xasset";
        let first: Vec<serde_json::Value> =
            (0..500).map(|i| trade_json(asset, i)).collect();
        client.set_response(&trades_url(asset, 500, 0), Ok(ok_trades(first)));
        client.set_response(
            &trades_url(asset, 500, 500),
            Err("max historical activity offset reached".into()),
        );

        let trades = backfill_trades_for_asset(&client, asset).await.unwrap();
        assert_eq!(trades.len(), 500);
        assert_eq!(client.request_count(&trades_url(asset, 500, 0)), 1);
        assert_eq!(client.request_count(&trades_url(asset, 500, 500)), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_backfill_logs_on_non_final_page() {
        let subscriber = tracing_subscriber::fmt::Subscriber::default();
        let _guard =
            tracing::dispatcher::set_default(&tracing::dispatcher::Dispatch::new(subscriber));
        let client = InMemoryHttpClient::new();
        let asset = "0xasset";
        let first: Vec<serde_json::Value> =
            (0..500).map(|i| trade_json(asset, i)).collect();
        let second: Vec<serde_json::Value> =
            (500..750).map(|i| trade_json(asset, i)).collect();

        client.set_response(&trades_url(asset, 500, 0), Ok(ok_trades(first)));
        client.set_response(&trades_url(asset, 500, 500), Ok(ok_trades(second)));

        let trades = backfill_trades_for_asset(&client, asset).await.unwrap();
        assert_eq!(trades.len(), 750);
        assert_eq!(client.request_count(&trades_url(asset, 500, 0)), 1);
        assert_eq!(client.request_count(&trades_url(asset, 500, 500)), 1);
    }
}
