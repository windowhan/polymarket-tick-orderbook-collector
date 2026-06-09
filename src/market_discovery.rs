use anyhow::Result;
use serde::{Deserialize, Serialize};
use tracing::info;

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

/// Fetch all markets from Gamma API `/markets` with offset pagination.
pub async fn fetch_markets(
    active: Option<bool>,
    closed: Option<bool>,
) -> Result<Vec<Market>> {
    let mut markets = Vec::new();
    let mut offset = 0;
    let limit = 100;
    let client = reqwest::Client::new();

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

        let resp = client
            .get(&url)
            .header("User-Agent", "polymarket-collector/0.1")
            .send()
            .await?;
        if !resp.status().is_success() {
            let text = resp.text().await?;
            anyhow::bail!("Gamma API error: {}", text);
        }

        let body: serde_json::Value = resp.json().await?;
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

        let fetched = markets.len();
        info!(page_size = page.len(), fetched, "Fetched markets page");

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
    let markets = fetch_markets(active, closed).await?;
    let path = crate::storage::data_dir().join("markets").join("markets.jsonl");
    crate::storage::write_jsonl(&path, &markets).await?;
    info!(count = markets.len(), path = %path.display(), "Discovery complete");
    Ok(markets.len())
}
