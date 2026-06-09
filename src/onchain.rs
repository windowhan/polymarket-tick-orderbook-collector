use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::Path;
use tracing::{info, warn};

/// CTF Exchange V2 (standard) on Polygon.
pub const CTF_EXCHANGE_V2: &str = "0xE111180000d2663C0091e4f400237545B87B996B";
/// NegRisk CTF Exchange V2 on Polygon.
pub const NEG_RISK_CTF_EXCHANGE_V2: &str = "0xe2222d279d744050d28e00520010520000310F59";
/// Topic0 for OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32).
pub const ORDER_FILLED_TOPIC: &str =
    "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnchainTrade {
    pub order_hash: String,
    pub maker: String,
    pub taker: String,
    pub side: u8,
    pub token_id: String,
    pub maker_amount_filled: String,
    pub taker_amount_filled: String,
    pub fee: String,
    pub builder: String,
    pub metadata: String,
    pub transaction_hash: String,
    pub block_number: u64,
    pub log_index: u64,
}

fn parse_log(log: &serde_json::Value) -> Result<OnchainTrade> {
    let topics = log
        .get("topics")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let data = log.get("data").and_then(|v| v.as_str()).unwrap_or("");
    let data = data.strip_prefix("0x").unwrap_or(data);

    if data.len() < 448 {
        anyhow::bail!("Data too short: {} chars", data.len());
    }

    let order_hash = topics
        .get(1)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let maker = topics
        .get(2)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let taker = topics
        .get(3)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let side = u8::from_str_radix(&data[62..64], 16)?;

    let token_id = format!("0x{}", &data[64..128]);
    let maker_amount_filled = format!("0x{}", &data[128..192]);
    let taker_amount_filled = format!("0x{}", &data[192..256]);
    let fee = format!("0x{}", &data[256..320]);
    let builder = format!("0x{}", &data[320..384]);
    let metadata = format!("0x{}", &data[384..448]);

    let tx_hash = log
        .get("transactionHash")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let block_number = u64::from_str_radix(
        log.get("blockNumber")
            .and_then(|v| v.as_str())
            .unwrap_or("0x0")
            .strip_prefix("0x")
            .unwrap_or("0"),
        16,
    )?;
    let log_index = u64::from_str_radix(
        log.get("logIndex")
            .and_then(|v| v.as_str())
            .unwrap_or("0x0")
            .strip_prefix("0x")
            .unwrap_or("0"),
        16,
    )?;

    Ok(OnchainTrade {
        order_hash,
        maker,
        taker,
        side,
        token_id,
        maker_amount_filled,
        taker_amount_filled,
        fee,
        builder,
        metadata,
        transaction_hash: tx_hash,
        block_number,
        log_index,
    })
}

async fn get_logs(
    client: &reqwest::Client,
    rpc_url: &str,
    from_block: u64,
    to_block: u64,
    address: &str,
    topic0: &str,
) -> Result<Vec<serde_json::Value>> {
    let payload = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "eth_getLogs",
        "params": [{
            "fromBlock": format!("0x{:x}", from_block),
            "toBlock": format!("0x{:x}", to_block),
            "address": address,
            "topics": [topic0]
        }],
        "id": 1
    });

    let resp = client.post(rpc_url).json(&payload).send().await?;
    if !resp.status().is_success() {
        anyhow::bail!("RPC HTTP error: {}", resp.text().await?);
    }

    let body: serde_json::Value = resp.json().await?;
    if let Some(err) = body.get("error") {
        anyhow::bail!("RPC error: {}", err);
    }

    let logs = body
        .get("result")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    Ok(logs)
}

/// Scrape OrderFilled events from a CTF Exchange V2 contract.
pub async fn scrape_exchange(
    client: &reqwest::Client,
    rpc_url: &str,
    exchange_address: &str,
    from_block: u64,
    to_block: u64,
    chunk_size: u64,
    output_path: &Path,
) -> Result<usize> {
    let mut total = 0usize;
    let mut current = from_block;

    while current <= to_block {
        let end = std::cmp::min(current + chunk_size - 1, to_block);
        match get_logs(
            client,
            rpc_url,
            current,
            end,
            exchange_address,
            ORDER_FILLED_TOPIC,
        )
        .await
        {
            Ok(logs) => {
                let mut trades = Vec::with_capacity(logs.len());
                for log in &logs {
                    match parse_log(log) {
                        Ok(trade) => trades.push(trade),
                        Err(e) => {
                            warn!(log = %log, error = %e, "Failed to parse log");
                        }
                    }
                }
                if !trades.is_empty() {
                    crate::storage::append_jsonl(output_path, &trades).await?;
                    total += trades.len();
                    info!(
                        from = current,
                        to = end,
                        count = trades.len(),
                        total,
                        "Scraped logs"
                    );
                }
            }
            Err(e) => {
                warn!(from = current, to = end, error = %e, "Log fetch failed, retrying");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
        }
        current = end + 1;
    }

    info!(
        total,
        path = %output_path.display(),
        "Onchain scrape complete"
    );
    Ok(total)
}
