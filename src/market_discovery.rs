use crate::http_client::{HttpClient, ReqwestHttpClient};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use tracing::info;

#[cfg(test)]
use std::sync::Mutex;

/// Test-only injection point for the default HTTP client used by the
/// convenience wrappers `fetch_markets` and `discover_and_save`.
#[cfg(test)]
static DEFAULT_HTTP_CLIENT: Mutex<Option<Arc<dyn HttpClient>>> = Mutex::new(None);

/// Serialize tests that change the process current working directory so they
/// do not interfere with other tests that rely on `std::env::current_dir()`.
#[cfg(test)]
static CWD_LOCK: Mutex<()> = Mutex::new(());

/// Return the default HTTP client for the current compilation mode.
///
/// In tests, this returns an injected mock client if one has been set via
/// `set_default_http_client`; otherwise it falls back to a real reqwest client.
fn default_http_client() -> Arc<dyn HttpClient> {
    #[cfg(test)]
    {
        let lock = DEFAULT_HTTP_CLIENT.lock().unwrap();
        if let Some(client) = lock.as_ref() {
            return client.clone();
        }
    }
    Arc::new(ReqwestHttpClient::new())
}

/// Set the default HTTP client used by `fetch_markets` and `discover_and_save`
/// in tests.
#[cfg(test)]
fn set_default_http_client(client: Arc<dyn HttpClient>) {
    *DEFAULT_HTTP_CLIENT.lock().unwrap() = Some(client);
}

/// Clear any injected default HTTP client.
#[cfg(test)]
fn clear_default_http_client() {
    *DEFAULT_HTTP_CLIENT.lock().unwrap() = None;
}

/// A single CLOB reward configuration entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClobReward {
    pub id: Option<String>,
    #[serde(rename = "conditionId")]
    pub condition_id: Option<String>,
    #[serde(rename = "assetAddress")]
    pub asset_address: Option<String>,
    #[serde(rename = "rewardsAmount")]
    pub rewards_amount: Option<f64>,
    #[serde(rename = "rewardsDailyRate")]
    pub rewards_daily_rate: Option<f64>,
    #[serde(rename = "startDate")]
    pub start_date: Option<String>,
    #[serde(rename = "endDate")]
    pub end_date: Option<String>,
}

/// A Polymarket market with the fields we care about.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Market {
    pub id: String,
    pub condition_id: String,
    pub question: String,
    pub slug: String,
    pub description: Option<String>,
    pub active: bool,
    pub closed: bool,
    pub archived: bool,
    pub end_date: Option<String>,
    pub start_date: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub volume: Option<f64>,
    pub liquidity: Option<f64>,
    pub volume_24h: Option<f64>,
    pub outcomes: Option<String>,
    pub outcome_prices: Option<String>,
    pub token_ids: Vec<String>,
    pub enable_order_book: bool,
    pub order_min_size: Option<f64>,
    pub order_price_min_tick_size: Option<f64>,
    pub neg_risk: bool,
    pub accepting_orders: bool,
    #[serde(rename = "clobRewards")]
    #[serde(default)]
    pub clob_rewards: Vec<ClobReward>,
    #[serde(rename = "rewardsMinSize")]
    pub rewards_min_size: Option<f64>,
    #[serde(rename = "rewardsMaxSpread")]
    pub rewards_max_spread: Option<f64>,
    pub competitive: Option<f64>,
}

/// Convenience wrapper that fetches markets using the production reqwest client.
///
/// Kept for backward compatibility with `main.rs` and other callers.
pub async fn fetch_markets(
    active: Option<bool>,
    closed: Option<bool>,
) -> Result<Vec<Market>> {
    fetch_markets_with_client(default_http_client().as_ref(), active, closed).await
}

/// Fetch all markets from Gamma API `/markets` with offset pagination.
pub async fn fetch_markets_with_client(
    client: &dyn HttpClient,
    active: Option<bool>,
    closed: Option<bool>,
) -> Result<Vec<Market>> {
    let mut markets = Vec::new();
    let mut offset = 0;
    let limit = 100;

    loop {
        let mut url = format!(
            "https://gamma-api.polymarket.com/markets?limit={}",
            limit
        );
        if offset > 0 {
            url.push_str(&format!("&offset={}", offset));
        }
        if let Some(v) = active {
            url.push_str(if v { "&active=true" } else { "&active=false" });
        }
        if let Some(v) = closed {
            url.push_str(if v { "&closed=true" } else { "&closed=false" });
        }

        let response = client
            .get(&url)
            .await
            .with_context(|| format!("Failed to fetch markets from {}", url))?;
        if !response.is_success() {
            anyhow::bail!("Gamma API error: {}", response.body);
        }

        let body: serde_json::Value = response
            .json()
            .context("Failed to parse Gamma markets response")?;
        let page = body.as_array().cloned().unwrap_or_default();

        if page.is_empty() {
            break;
        }

        for item in &page {
            let token_ids: Vec<String> = item
                .get("clobTokenIds")
                .and_then(|v| v.as_str())
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or_default();

            let clob_rewards: Vec<ClobReward> = item
                .get("clobRewards")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();

            markets.push(Market {
                id: item.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                condition_id: item
                    .get("conditionId")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                question: item
                    .get("question")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                slug: item.get("slug").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                description: item
                    .get("description")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                active: item.get("active").and_then(|v| v.as_bool()).unwrap_or(false),
                closed: item.get("closed").and_then(|v| v.as_bool()).unwrap_or(false),
                archived: item.get("archived").and_then(|v| v.as_bool()).unwrap_or(false),
                end_date: item
                    .get("endDate")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                start_date: item
                    .get("startDate")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                created_at: item
                    .get("createdAt")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                updated_at: item
                    .get("updatedAt")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                volume: item.get("volumeNum").and_then(|v| v.as_f64()),
                liquidity: item.get("liquidityNum").and_then(|v| v.as_f64()),
                volume_24h: item.get("volume24hr").and_then(|v| v.as_f64()),
                outcomes: item
                    .get("outcomes")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                outcome_prices: item
                    .get("outcomePrices")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                token_ids,
                enable_order_book: item
                    .get("enableOrderBook")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
                order_min_size: item.get("orderMinSize").and_then(|v| v.as_f64()),
                order_price_min_tick_size: item
                    .get("orderPriceMinTickSize")
                    .and_then(|v| v.as_f64()),
                neg_risk: item.get("negRisk").and_then(|v| v.as_bool()).unwrap_or(false),
                accepting_orders: item
                    .get("acceptingOrders")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
                clob_rewards,
                rewards_min_size: item.get("rewardsMinSize").and_then(|v| v.as_f64()),
                rewards_max_spread: item.get("rewardsMaxSpread").and_then(|v| v.as_f64()),
                competitive: item.get("competitive").and_then(|v| v.as_f64()),
            });
        }

        let _fetched = markets.len();
        info!("Fetched markets page");

        if page.len() < limit || offset >= 9900 {
            break;
        }
        offset += limit;
    }

    Ok(markets)
}

/// Discover markets and persist to `data/markets/markets.jsonl`.
pub async fn discover_and_save(
    active: Option<bool>,
    closed: Option<bool>,
) -> Result<usize> {
    discover_and_save_with_client(
        default_http_client().as_ref(),
        active,
        closed,
        &crate::storage::data_dir(),
    )
    .await
}

/// Discover markets with a provided HTTP client and persist to
/// `{data_dir}/markets/markets.jsonl`.
pub async fn discover_and_save_with_client(
    client: &dyn HttpClient,
    active: Option<bool>,
    closed: Option<bool>,
    data_dir: &Path,
) -> Result<usize> {
    let markets = fetch_markets_with_client(client, active, closed).await?;
    let path = data_dir.join("markets").join("markets.jsonl");
    crate::storage::write_jsonl(&path, &markets).await?;
    info!(count = markets.len(), path = %path.display(), "Discovery complete");
    Ok(markets.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_client::{HttpResponse, InMemoryHttpClient};
    use tempfile::tempdir;

    fn gamma_url(active: Option<bool>, closed: Option<bool>, offset: usize) -> String {
        let mut url = "https://gamma-api.polymarket.com/markets?limit=100".to_string();
        if offset > 0 {
            url.push_str(&format!("&offset={}", offset));
        }
        if let Some(v) = active {
            url.push_str(if v { "&active=true" } else { "&active=false" });
        }
        if let Some(v) = closed {
            url.push_str(if v { "&closed=true" } else { "&closed=false" });
        }
        url
    }

    fn market_item(id: &str) -> serde_json::Value {
        serde_json::json!({
            "id": id,
            "conditionId": format!("cond-{}", id),
            "question": "Will it rain?",
            "slug": "rain",
            "active": true,
            "closed": false,
            "archived": false,
            "clobTokenIds": serde_json::to_string(&vec![format!("0x{}", id)]).unwrap(),
            "clobRewards": [],
            "token_ids": vec![format!("0x{}", id)],
            "enableOrderBook": true,
            "negRisk": false,
            "acceptingOrders": true,
        })
    }

    fn ok_response(body: serde_json::Value) -> HttpResponse {
        HttpResponse {
            status: 200,
            body: serde_json::to_string(&body).unwrap(),
        }
    }

    #[tokio::test]
    async fn test_fetch_markets_empty() {
        let client = InMemoryHttpClient::new();
        client.set_response(
            &gamma_url(None, None, 0),
            Ok(ok_response(serde_json::json!([]))),
        );

        let markets = fetch_markets_with_client(&client, None, None).await.unwrap();
        assert!(markets.is_empty());
        assert_eq!(client.request_count(&gamma_url(None, None, 0)), 1);
    }

    #[tokio::test]
    async fn test_fetch_markets_pagination() {
        let client = InMemoryHttpClient::new();
        let first_page: Vec<serde_json::Value> =
            (0..100).map(|i| market_item(&format!("m{}", i))).collect();
        let second_page: Vec<serde_json::Value> =
            (100..150).map(|i| market_item(&format!("m{}", i))).collect();

        client.set_response(
            &gamma_url(None, None, 0),
            Ok(ok_response(serde_json::json!(first_page))),
        );
        client.set_response(
            &gamma_url(None, None, 100),
            Ok(ok_response(serde_json::json!(second_page))),
        );

        let markets = fetch_markets_with_client(&client, None, None).await.unwrap();
        assert_eq!(markets.len(), 150);
        assert_eq!(markets[0].id, "m0");
        assert_eq!(markets[149].id, "m149");
        assert_eq!(client.request_count(&gamma_url(None, None, 0)), 1);
        assert_eq!(client.request_count(&gamma_url(None, None, 100)), 1);
    }

    #[tokio::test]
    async fn test_fetch_markets_active_closed_flags() {
        let client = InMemoryHttpClient::new();
        client.set_response(
            &gamma_url(Some(true), Some(false), 0),
            Ok(ok_response(serde_json::json!([market_item("flagged")]))),
        );

        let markets = fetch_markets_with_client(&client, Some(true), Some(false))
            .await
            .unwrap();
        assert_eq!(markets.len(), 1);
        assert_eq!(markets[0].id, "flagged");
    }

    #[tokio::test]
    async fn test_fetch_markets_api_error() {
        let client = InMemoryHttpClient::new();
        client.set_response(
            &gamma_url(None, None, 0),
            Ok(HttpResponse {
                status: 500,
                body: "internal error".into(),
            }),
        );

        let err = fetch_markets_with_client(&client, None, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Gamma API error"));
    }

    #[tokio::test]
    async fn test_fetch_markets_malformed_defaults() {
        let client = InMemoryHttpClient::new();
        let body = serde_json::json!([{
            "id": null,
            "conditionId": 123,
            "active": "not-a-bool",
            "closed": 1,
            "clobTokenIds": ["not", "a", "string"],
            "clobRewards": "not-an-array",
        }]);
        client.set_response(&gamma_url(None, None, 0), Ok(ok_response(body)));

        let markets = fetch_markets_with_client(&client, None, None).await.unwrap();
        assert_eq!(markets.len(), 1);
        let m = &markets[0];
        assert_eq!(m.id, "");
        assert_eq!(m.condition_id, "");
        assert!(!m.active);
        assert!(!m.closed);
        assert!(m.token_ids.is_empty());
        assert!(m.clob_rewards.is_empty());
    }

    #[tokio::test]
    async fn test_fetch_markets_single_all_fields() {
        let client = InMemoryHttpClient::new();
        let body = serde_json::json!([{
            "id": "m1",
            "conditionId": "cond1",
            "question": "Will it rain?",
            "slug": "rain",
            "description": "A market about rain",
            "active": true,
            "closed": false,
            "archived": false,
            "endDate": "2025-12-31",
            "startDate": "2025-01-01",
            "createdAt": "2025-01-01T00:00:00Z",
            "updatedAt": "2025-06-01T00:00:00Z",
            "volumeNum": 1234.5,
            "liquidityNum": 567.8,
            "volume24hr": 90.1,
            "outcomes": "Yes,No",
            "outcomePrices": "0.6,0.4",
            "clobTokenIds": serde_json::to_string(&vec!["0xabc", "0xdef"]).unwrap(),
            "clobRewards": [{
                "id": "r1",
                "conditionId": "cond1",
                "assetAddress": "0xreward",
                "rewardsAmount": 100.0,
                "rewardsDailyRate": 10.0,
                "startDate": "2025-01-01",
                "endDate": "2025-12-31"
            }],
            "enableOrderBook": true,
            "orderMinSize": 1.0,
            "orderPriceMinTickSize": 0.01,
            "negRisk": false,
            "acceptingOrders": true,
            "rewardsMinSize": 10.0,
            "rewardsMaxSpread": 0.05,
            "competitive": 0.9,
        }]);
        client.set_response(&gamma_url(None, None, 0), Ok(ok_response(body)));

        let markets = fetch_markets_with_client(&client, None, None).await.unwrap();
        assert_eq!(markets.len(), 1);
        let m = &markets[0];
        assert_eq!(m.id, "m1");
        assert_eq!(m.condition_id, "cond1");
        assert_eq!(m.question, "Will it rain?");
        assert_eq!(m.slug, "rain");
        assert_eq!(m.description, Some("A market about rain".to_string()));
        assert!(m.active);
        assert!(!m.closed);
        assert!(!m.archived);
        assert_eq!(m.end_date, Some("2025-12-31".to_string()));
        assert_eq!(m.start_date, Some("2025-01-01".to_string()));
        assert_eq!(m.created_at, Some("2025-01-01T00:00:00Z".to_string()));
        assert_eq!(m.updated_at, Some("2025-06-01T00:00:00Z".to_string()));
        assert_eq!(m.volume, Some(1234.5));
        assert_eq!(m.liquidity, Some(567.8));
        assert_eq!(m.volume_24h, Some(90.1));
        assert_eq!(m.outcomes, Some("Yes,No".to_string()));
        assert_eq!(m.outcome_prices, Some("0.6,0.4".to_string()));
        assert_eq!(m.token_ids, vec!["0xabc", "0xdef"]);
        assert!(m.enable_order_book);
        assert_eq!(m.order_min_size, Some(1.0));
        assert_eq!(m.order_price_min_tick_size, Some(0.01));
        assert!(!m.neg_risk);
        assert!(m.accepting_orders);
        assert_eq!(m.rewards_min_size, Some(10.0));
        assert_eq!(m.rewards_max_spread, Some(0.05));
        assert_eq!(m.competitive, Some(0.9));
        assert_eq!(m.clob_rewards.len(), 1);
        let reward = &m.clob_rewards[0];
        assert_eq!(reward.id, Some("r1".to_string()));
        assert_eq!(reward.condition_id, Some("cond1".to_string()));
        assert_eq!(reward.asset_address, Some("0xreward".to_string()));
        assert_eq!(reward.rewards_amount, Some(100.0));
        assert_eq!(reward.rewards_daily_rate, Some(10.0));
        assert_eq!(reward.start_date, Some("2025-01-01".to_string()));
        assert_eq!(reward.end_date, Some("2025-12-31".to_string()));
    }

    #[tokio::test]
    async fn test_fetch_markets_invalid_json() {
        let client = InMemoryHttpClient::new();
        client.set_response(
            &gamma_url(None, None, 0),
            Ok(HttpResponse {
                status: 200,
                body: "not json".into(),
            }),
        );

        let err = fetch_markets_with_client(&client, None, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Failed to parse Gamma markets response"));
    }

    #[tokio::test]
    async fn test_fetch_markets_active_closed_false() {
        let client = InMemoryHttpClient::new();
        client.set_response(
            &gamma_url(Some(false), Some(false), 0),
            Ok(ok_response(serde_json::json!([market_item("false-flag")]))),
        );

        let markets = fetch_markets_with_client(&client, Some(false), Some(false))
            .await
            .unwrap();
        assert_eq!(markets.len(), 1);
        assert_eq!(markets[0].id, "false-flag");
    }

    #[tokio::test]
    async fn test_fetch_markets_stops_at_max_offset() {
        let client = InMemoryHttpClient::new();
        // Generate 100 full pages so the crawler reaches offset 9900 and stops
        // there even though the page is full.
        for offset in (0..=9900).step_by(100) {
            let page: Vec<serde_json::Value> = (0..100)
                .map(|i| market_item(&format!("m-{}", offset + i)))
                .collect();
            client.set_response(&gamma_url(None, None, offset), Ok(ok_response(serde_json::json!(page))));
        }

        let markets = fetch_markets_with_client(&client, None, None).await.unwrap();
        assert_eq!(markets.len(), 10_000);
        assert_eq!(markets[0].id, "m-0");
        assert_eq!(markets[9999].id, "m-9999");
        assert_eq!(client.request_count(&gamma_url(None, None, 9900)), 1);
        assert_eq!(client.request_count(&gamma_url(None, None, 10000)), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_discover_and_save_with_client_writes_file() {
        let subscriber = tracing_subscriber::fmt::Subscriber::default();
        let _guard =
            tracing::dispatcher::set_default(&tracing::dispatcher::Dispatch::new(subscriber));
        let client = InMemoryHttpClient::new();
        client.set_response(
            &gamma_url(None, None, 0),
            Ok(ok_response(serde_json::json!([market_item("saved")]))),
        );

        let dir = tempdir().unwrap();
        let count = discover_and_save_with_client(&client, None, None, dir.path())
            .await
            .unwrap();
        assert_eq!(count, 1);

        let path = dir.path().join("markets").join("markets.jsonl");
        assert!(path.exists());
        let content = std::fs::read_to_string(&path).unwrap();
        let parsed: Market = serde_json::from_str(content.lines().next().unwrap()).unwrap();
        assert_eq!(parsed.id, "saved");
    }

    #[tokio::test]
    async fn test_fetch_markets_network_error() {
        let client = InMemoryHttpClient::new();
        client.set_response(
            &gamma_url(None, None, 0),
            Err("connection refused".into()),
        );

        let err = fetch_markets_with_client(&client, None, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Failed to fetch markets"));
    }

    #[tokio::test]
    async fn test_discover_and_save_with_client_write_error() {
        let client = InMemoryHttpClient::new();
        client.set_response(
            &gamma_url(None, None, 0),
            Ok(ok_response(serde_json::json!([market_item("m1")]))),
        );

        // Create a file where the data_dir should be so directory creation fails.
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("markets");
        std::fs::write(&file_path, "not a dir").unwrap();

        let err = discover_and_save_with_client(&client, None, None, dir.path())
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Failed to create file")
                || msg.contains("Not a directory")
                || msg.contains("File exists"),
            "unexpected error: {}",
            msg
        );
    }

    #[tokio::test]
    async fn test_discover_and_save_with_client_empty() {
        let client = InMemoryHttpClient::new();
        client.set_response(&gamma_url(None, None, 0), Ok(ok_response(serde_json::json!([]))));

        let dir = tempdir().unwrap();
        let count = discover_and_save_with_client(&client, None, None, dir.path())
            .await
            .unwrap();
        assert_eq!(count, 0);

        let path = dir.path().join("markets").join("markets.jsonl");
        assert!(path.exists());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_default_http_client_fallback() {
        clear_default_http_client();
        let _client = default_http_client();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_fetch_markets_wrapper() {
        let subscriber = tracing_subscriber::fmt::Subscriber::default();
        let _guard =
            tracing::dispatcher::set_default(&tracing::dispatcher::Dispatch::new(subscriber));
        let client = InMemoryHttpClient::new();
        client.set_response(
            &gamma_url(None, None, 0),
            Ok(ok_response(serde_json::json!([market_item("wrapped")]))),
        );
        set_default_http_client(Arc::new(client));

        let markets = fetch_markets(None, None).await.unwrap();
        assert_eq!(markets.len(), 1);
        assert_eq!(markets[0].id, "wrapped");

        clear_default_http_client();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_discover_and_save_wrapper() {
        let _cwd_guard = CWD_LOCK.lock().unwrap();

        let subscriber = tracing_subscriber::fmt::Subscriber::default();
        let _guard =
            tracing::dispatcher::set_default(&tracing::dispatcher::Dispatch::new(subscriber));
        let client = InMemoryHttpClient::new();
        client.set_response(
            &gamma_url(None, None, 0),
            Ok(ok_response(serde_json::json!([market_item("saved")]))),
        );
        set_default_http_client(Arc::new(client));

        let original_dir = std::env::current_dir().unwrap();
        let dir = tempdir().unwrap();
        std::env::set_current_dir(dir.path()).unwrap();

        let count = discover_and_save(None, None).await.unwrap();

        std::env::set_current_dir(original_dir).unwrap();
        clear_default_http_client();

        assert_eq!(count, 1);
        let path = dir.path().join("data").join("markets").join("markets.jsonl");
        assert!(path.exists());
        let content = std::fs::read_to_string(&path).unwrap();
        let parsed: Market = serde_json::from_str(content.lines().next().unwrap()).unwrap();
        assert_eq!(parsed.id, "saved");
    }
}
