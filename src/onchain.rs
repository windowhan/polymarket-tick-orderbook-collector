use crate::http_client::{HttpClient, ReqwestHttpClient};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tracing::{info, warn};

/// CTF Exchange V2 (standard markets) deployed on Polygon PoS.
///
/// This contract emits `OrderFilled` events whenever a standard (non-NegRisk)
/// market order is settled on-chain.
pub const CTF_EXCHANGE_V2: &str = "0xE111180000d2663C0091e4f400237545B87B996B";

/// NegRisk CTF Exchange V2 deployed on Polygon PoS.
///
/// This contract emits `OrderFilled` events for NegRisk (multi-outcome) markets.
pub const NEG_RISK_CTF_EXCHANGE_V2: &str = "0xe2222d279d744050d28e00520010520000310F59";

/// Keccak-256 topic0 signature for the `OrderFilled` event.
///
/// Event signature: `OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)`
///
/// # How the hash is computed
/// ```text
/// keccak256("OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)")
/// = 0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee
/// ```
pub const ORDER_FILLED_TOPIC: &str =
    "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee";

/// Decoded on-chain trade from a Polymarket CLOB `OrderFilled` event log.
///
/// # Field mapping from event log
/// The `OrderFilled` event has the following indexed (topics) and non-indexed (data) fields:
///
/// | Field               | Source        | Solidity Type | Notes                              |
/// |---------------------|---------------|---------------|------------------------------------|
/// | `order_hash`        | topics[1]     | bytes32       | Unique identifier of the order     |
/// | `maker`             | topics[2]     | address       | Address that placed the limit order|
/// | `taker`             | topics[3]     | address       | Address that initiated the trade   |
/// | `side`              | data[0..32]   | uint8         | 0 = BUY, 1 = SELL                  |
/// | `token_id`          | data[32..64]  | uint256       | Polymarket asset/token identifier  |
/// | `maker_amount`      | data[64..96]  | uint256       | Amount filled for maker side       |
/// | `taker_amount`      | data[96..128] | uint256       | Amount filled for taker side       |
/// | `fee`               | data[128..160]| uint256       | Fee charged (in token decimals)    |
/// | `builder`           | data[160..192]| bytes32       | Builder identifier                 |
/// | `metadata`          | data[192..224]| bytes32       | Additional metadata                |
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

/// Normalize a 32-byte zero-padded Ethereum address (from event topic) to a standard 20-byte address.
///
/// Ethereum event topics store `address` types as 32-byte words with leading zeros.
/// This strips the `0x` prefix, removes all leading zero bytes, then re-adds `0x`.
fn normalize_address(addr: &str) -> String {
    let stripped = addr.strip_prefix("0x").unwrap_or(addr);
    let trimmed = stripped.trim_start_matches('0');
    format!("0x{}", trimmed)
}

/// Parse a single Ethereum event log JSON into an `OnchainTrade` struct.
fn parse_log(log: &serde_json::Value) -> Result<OnchainTrade> {
    // Extract the topics array. topics[0] is the event signature hash,
    // topics[1..=3] are the indexed arguments (order_hash, maker, taker).
    let topics = log
        .get("topics")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    // Extract the non-indexed data payload and strip the "0x" prefix.
    let data = log.get("data").and_then(|v| v.as_str()).unwrap_or("");
    let data = data.strip_prefix("0x").unwrap_or(data);

    // The OrderFilled event data contains 7 uint256/bytes32 words = 224 bytes = 448 hex chars.
    // Anything shorter means the log format is unexpected and we cannot safely slice.
    if data.len() < 448 {
        anyhow::bail!("Data too short: {} chars (expected ≥ 448)", data.len());
    }

    // topics[1] = orderHash (bytes32, indexed)
    let order_hash = topics
        .get(1)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // topics[2] = maker address (indexed, but stored as 32-byte padded word)
    // Use normalize_address to strip the 12 bytes of leading zeros.
    let maker = topics
        .get(2)
        .and_then(|v| v.as_str())
        .map(normalize_address)
        .unwrap_or_default();

    // topics[3] = taker address (indexed, same 32-byte padding as maker)
    let taker = topics
        .get(3)
        .and_then(|v| v.as_str())
        .map(normalize_address)
        .unwrap_or_default();

    // data[62..64] = side byte (uint8 stored in the last byte of the first 32-byte word).
    let side = u8::from_str_radix(&data[62..64], 16)?;

    // data[64..128]  = token_id (uint256, 32 bytes)
    // data[128..192] = maker_amount_filled (uint256, 32 bytes)
    // data[192..256] = taker_amount_filled (uint256, 32 bytes)
    // data[256..320] = fee (uint256, 32 bytes)
    // data[320..384] = builder (bytes32, 32 bytes)
    // data[384..448] = metadata (bytes32, 32 bytes)
    let token_id = format!("0x{}", &data[64..128]);
    let maker_amount_filled = format!("0x{}", &data[128..192]);
    let taker_amount_filled = format!("0x{}", &data[192..256]);
    let fee = format!("0x{}", &data[256..320]);
    let builder = format!("0x{}", &data[320..384]);
    let metadata = format!("0x{}", &data[384..448]);

    // transactionHash is a direct hex string in the log object (not padded).
    let tx_hash = log
        .get("transactionHash")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // blockNumber and logIndex are hex-encoded strings — parse from base-16.
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

/// Fetch Ethereum event logs from a Polygon RPC node via `eth_getLogs`.
///
/// Accepts any [`HttpClient`] implementation so tests can inject a mock client.
async fn get_logs_with_client(
    client: &dyn HttpClient,
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
    let body = serde_json::to_string(&payload)?;

    let response = client
        .post(rpc_url, body)
        .await
        .with_context(|| format!("Failed to fetch logs from {}", rpc_url))?;
    if !response.is_success() {
        anyhow::bail!("RPC HTTP error: {}", response.body);
    }

    let body: serde_json::Value = response.json()?;
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

/// Query a single transaction receipt by its hash and return the event logs within it.
///
/// Backward-compatible wrapper that creates a production HTTP client.
pub async fn get_receipt_logs(
    _client: &reqwest::Client,
    rpc_url: &str,
    tx_hash: &str,
) -> Result<Vec<serde_json::Value>> {
    get_receipt_logs_with_client(&ReqwestHttpClient::new(), rpc_url, tx_hash).await
}

/// Query a single transaction receipt by its hash and return the event logs within it.
///
/// Accepts any [`HttpClient`] implementation so tests can inject a mock client.
pub async fn get_receipt_logs_with_client(
    client: &dyn HttpClient,
    rpc_url: &str,
    tx_hash: &str,
) -> Result<Vec<serde_json::Value>> {
    let payload = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "eth_getTransactionReceipt",
        "params": [tx_hash],
        "id": 1
    });
    let body = serde_json::to_string(&payload)?;

    let response = client
        .post(rpc_url, body)
        .await
        .with_context(|| format!("Failed to fetch receipt from {}", rpc_url))?;
    if !response.is_success() {
        anyhow::bail!("RPC HTTP error: {}", response.body);
    }

    let body: serde_json::Value = response.json()?;
    if let Some(err) = body.get("error") {
        anyhow::bail!("RPC error: {}", err);
    }

    // result.logs is the array of event logs emitted during this transaction.
    let logs = body
        .get("result")
        .and_then(|v| v.get("logs"))
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    Ok(logs)
}

/// Search through a list of receipt logs and find the first `OrderFilled` event
/// emitted by a known Polymarket CTF Exchange V2 contract.
pub fn find_order_filled(logs: &[serde_json::Value]) -> Option<OnchainTrade> {
    // Build a HashSet of known exchange addresses in lowercase for case-insensitive matching.
    let valid_addresses: std::collections::HashSet<String> =
        [CTF_EXCHANGE_V2, NEG_RISK_CTF_EXCHANGE_V2]
            .iter()
            .map(|a| a.to_lowercase())
            .collect();

    for log in logs {
        let addr = log.get("address").and_then(|v| v.as_str()).unwrap_or("");
        let topics = log
            .get("topics")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let topic0 = topics.first().and_then(|v| v.as_str()).unwrap_or("");

        // Match only logs from our known exchange contracts with the OrderFilled topic.
        if valid_addresses.contains(addr.to_lowercase().as_str())
            && topic0.eq_ignore_ascii_case(ORDER_FILLED_TOPIC)
        {
            // Attempt to decode. If parsing fails (malformed log), silently skip.
            if let Ok(trade) = parse_log(log) {
                return Some(trade);
            }
        }
    }
    None
}

/// Scrape `OrderFilled` events from a Polymarket CTF Exchange V2 contract across a block range.
///
/// Backward-compatible wrapper that creates a production HTTP client.
pub async fn scrape_exchange(
    _client: &reqwest::Client,
    rpc_url: &str,
    exchange_address: &str,
    from_block: u64,
    to_block: u64,
    chunk_size: u64,
    output_path: &Path,
) -> Result<usize> {
    scrape_exchange_with_client(
        &ReqwestHttpClient::new(),
        rpc_url,
        exchange_address,
        from_block,
        to_block,
        chunk_size,
        output_path,
    )
    .await
}

/// Scrape `OrderFilled` events from a Polymarket CTF Exchange V2 contract across a block range.
///
/// Accepts any [`HttpClient`] implementation so tests can inject a mock client.
pub async fn scrape_exchange_with_client(
    client: &dyn HttpClient,
    rpc_url: &str,
    exchange_address: &str,
    from_block: u64,
    to_block: u64,
    chunk_size: u64,
    output_path: &Path,
) -> Result<usize> {
    let mut total = 0usize;
    let mut current = from_block;

    // Iterate through the block range in chunks to avoid RPC timeouts.
    while current <= to_block {
        let end = std::cmp::min(current + chunk_size - 1, to_block);
        match get_logs_with_client(
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
                    let count = trades.len();
                    total += count;
                    info!(from = current, to = end, count, total, "Scraped logs");
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

    info!(total, path = %output_path.display(), "Onchain scrape complete");
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_client::{HttpResponse, InMemoryHttpClient};
    use std::collections::HashSet;
    use tempfile::tempdir;

    fn word(hex: &str) -> String {
        format!("{:0>64}", hex.strip_prefix("0x").unwrap_or(hex))
    }

    fn padded_address(addr: &str) -> String {
        format!(
            "0x{}000000000000000000000000{}",
            "0".repeat(24),
            addr.strip_prefix("0x").unwrap_or(addr)
        )
    }

    fn sample_data(side: u8, token_id: &str) -> String {
        let side_word = word(&format!("0x{:02x}", side));
        let token_word = word(token_id);
        let maker_amount = word("0x1234");
        let taker_amount = word("0x5678");
        let fee = word("0x0");
        let builder = word("0xabcd");
        let metadata = word("0xef01");
        format!(
            "0x{}{}{}{}{}{}{}",
            side_word, token_word, maker_amount, taker_amount, fee, builder, metadata
        )
    }

    fn sample_log_with_address(address: &str) -> serde_json::Value {
        let maker_addr = "0x448861155279dbf833d041b963e3ac854599e319";
        let taker_addr = "0x6f3c1ddc97c9abfb38ff0f1302a56a1946d04c6f";
        serde_json::json!({
            "address": address,
            "topics": [
                ORDER_FILLED_TOPIC,
                "0xd980fee1cbe88b9fbca895573ec0296b5a049937556040671ca4eb90d612d473",
                padded_address(maker_addr),
                padded_address(taker_addr),
            ],
            "data": sample_data(0, "0x66fc627fc41c09ce984d8db2aa4dcc8102d201581e73ad410b142f209832e207"),
            "transactionHash": "0x5e5fe7c64a30b1d23366bf508ea288b994e3b3d8d5afd5facd991af8551dae02",
            "blockNumber": "0x5424d9d",
            "logIndex": "0x38e",
        })
    }

    fn sample_log() -> serde_json::Value {
        sample_log_with_address(NEG_RISK_CTF_EXCHANGE_V2)
    }

    #[test]
    fn test_normalize_address_with_prefix() {
        assert_eq!(
            normalize_address("0x000000000000000000000000448861155279dbf833d041b963e3ac854599e319"),
            "0x448861155279dbf833d041b963e3ac854599e319"
        );
    }

    #[test]
    fn test_normalize_address_without_prefix() {
        assert_eq!(
            normalize_address("000000000000000000000000448861155279dbf833d041b963e3ac854599e319"),
            "0x448861155279dbf833d041b963e3ac854599e319"
        );
    }

    #[test]
    fn test_normalize_address_all_zeros() {
        assert_eq!(normalize_address("0x0000000000000000000000000000000000000000"), "0x");
    }

    #[test]
    fn test_normalize_address_already_normalized() {
        assert_eq!(
            normalize_address("0x448861155279dbf833d041b963e3ac854599e319"),
            "0x448861155279dbf833d041b963e3ac854599e319"
        );
    }

    #[test]
    fn test_normalize_address_uppercase() {
        assert_eq!(
            normalize_address("0x000000000000000000000000448861155279DBF833D041B963E3AC854599E319"),
            "0x448861155279DBF833D041B963E3AC854599E319"
        );
    }

    #[test]
    fn test_parse_log_success() {
        let log = sample_log();
        let trade = parse_log(&log).unwrap();
        assert_eq!(trade.maker, "0x448861155279dbf833d041b963e3ac854599e319");
        assert_eq!(trade.taker, "0x6f3c1ddc97c9abfb38ff0f1302a56a1946d04c6f");
        assert_eq!(trade.side, 0);
        assert_eq!(trade.token_id, "0x66fc627fc41c09ce984d8db2aa4dcc8102d201581e73ad410b142f209832e207");
        assert_eq!(trade.maker_amount_filled, "0x0000000000000000000000000000000000000000000000000000000000001234");
        assert_eq!(trade.taker_amount_filled, "0x0000000000000000000000000000000000000000000000000000000000005678");
        assert_eq!(trade.fee, "0x0000000000000000000000000000000000000000000000000000000000000000");
        assert_eq!(trade.builder, "0x000000000000000000000000000000000000000000000000000000000000abcd");
        assert_eq!(trade.metadata, "0x000000000000000000000000000000000000000000000000000000000000ef01");
        assert_eq!(trade.transaction_hash, "0x5e5fe7c64a30b1d23366bf508ea288b994e3b3d8d5afd5facd991af8551dae02");
        assert_eq!(trade.block_number, 0x5424d9d);
        assert_eq!(trade.log_index, 0x38e);
    }

    #[test]
    fn test_parse_log_sell_side() {
        let mut log = sample_log();
        log["data"] = sample_data(1, "0x66fc627fc41c09ce984d8db2aa4dcc8102d201581e73ad410b142f209832e207").into();
        let trade = parse_log(&log).unwrap();
        assert_eq!(trade.side, 1);
    }

    #[test]
    fn test_parse_log_data_too_short() {
        let log = serde_json::json!({
            "topics": [ORDER_FILLED_TOPIC, "0x0", "0x0", "0x0"],
            "data": "0x1234",
        });
        assert!(parse_log(&log).is_err());
    }

    #[test]
    fn test_parse_log_missing_topics() {
        let log = serde_json::json!({
            "topics": [],
            "data": sample_data(0, "0xabc"),
        });
        // Missing topics still allow parsing because we use empty defaults.
        let trade = parse_log(&log).unwrap();
        assert!(trade.order_hash.is_empty());
        assert!(trade.maker.is_empty());
        assert!(trade.taker.is_empty());
    }

    #[test]
    fn test_parse_log_missing_data() {
        let log = serde_json::json!({
            "topics": [ORDER_FILLED_TOPIC, "0x0", "0x0", "0x0"],
        });
        assert!(parse_log(&log).is_err());
    }

    #[test]
    fn test_parse_log_invalid_side_hex() {
        let mut log = sample_log();
        let data = log["data"].as_str().unwrap().to_string();
        // Replace the last byte of the side word with invalid hex chars.
        // The side word occupies string indices 2..66 (after the "0x" prefix),
        // so the last byte is at indices 64..66.
        let mut chars: Vec<char> = data.chars().collect();
        chars[64] = 'g';
        chars[65] = 'g';
        log["data"] = chars.into_iter().collect::<String>().into();
        assert!(parse_log(&log).is_err());
    }

    #[test]
    fn test_find_order_filled_matches_neg_risk() {
        let logs = vec![sample_log()];
        let trade = find_order_filled(&logs).unwrap();
        assert_eq!(trade.maker, "0x448861155279dbf833d041b963e3ac854599e319");
    }

    #[test]
    fn test_find_order_filled_matches_ctf_exchange() {
        let log = sample_log_with_address(CTF_EXCHANGE_V2);
        let trade = find_order_filled(&[log]).unwrap();
        assert_eq!(trade.maker, "0x448861155279dbf833d041b963e3ac854599e319");
    }

    #[test]
    fn test_find_order_filled_case_insensitive_address() {
        let mut log = sample_log();
        log["address"] = NEG_RISK_CTF_EXCHANGE_V2.to_lowercase().into();
        let trade = find_order_filled(&[log]).unwrap();
        assert_eq!(trade.taker, "0x6f3c1ddc97c9abfb38ff0f1302a56a1946d04c6f");
    }

    #[test]
    fn test_find_order_filled_case_insensitive_topic() {
        let mut log = sample_log();
        log["topics"] = serde_json::json!([
            ORDER_FILLED_TOPIC.to_lowercase(),
            "0xd980fee1cbe88b9fbca895573ec0296b5a049937556040671ca4eb90d612d473",
            padded_address("0x448861155279dbf833d041b963e3ac854599e319"),
            padded_address("0x6f3c1ddc97c9abfb38ff0f1302a56a1946d04c6f"),
        ]);
        let trade = find_order_filled(&[log]).unwrap();
        assert_eq!(trade.maker, "0x448861155279dbf833d041b963e3ac854599e319");
    }

    #[test]
    fn test_find_order_filled_skips_unknown_address() {
        let mut log = sample_log();
        log["address"] = "0x0000000000000000000000000000000000000000".into();
        assert!(find_order_filled(&[log]).is_none());
    }

    #[test]
    fn test_find_order_filled_skips_unknown_topic() {
        let mut log = sample_log();
        log["topics"] = serde_json::json!([
            "0x0000000000000000000000000000000000000000000000000000000000000000",
            "0x0",
            "0x0",
            "0x0"
        ]);
        assert!(find_order_filled(&[log]).is_none());
    }

    #[test]
    fn test_find_order_filled_skips_malformed_log() {
        let mut log = sample_log();
        log["data"] = "0x1234".into();
        assert!(find_order_filled(&[log]).is_none());
    }

    #[test]
    fn test_find_order_filled_empty_logs() {
        assert!(find_order_filled(&[]).is_none());
    }

    #[test]
    fn test_parse_log_invalid_block_number() {
        let mut log = sample_log();
        log["blockNumber"] = "0xzz".into();
        assert!(parse_log(&log).is_err());
    }

    #[test]
    fn test_parse_log_invalid_log_index() {
        let mut log = sample_log();
        log["logIndex"] = "0xzz".into();
        assert!(parse_log(&log).is_err());
    }

    #[test]
    fn test_find_order_filled_non_matching_topic() {
        let mut log = sample_log();
        let mut topics = log["topics"].as_array().unwrap().clone();
        topics[0] = "0x1111111111111111111111111111111111111111111111111111111111111111".into();
        log["topics"] = topics.into();
        assert!(find_order_filled(&[log]).is_none());
    }

    fn rpc_response(result: serde_json::Value) -> HttpResponse {
        HttpResponse {
            status: 200,
            body: serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": result,
            }))
            .unwrap(),
        }
    }

    #[tokio::test]
    async fn test_get_logs_success() {
        let client = InMemoryHttpClient::new();
        let rpc_url = "https://polygon-rpc.com";
        client.set_response(
            rpc_url,
            Ok(rpc_response(serde_json::json!([sample_log()]))),
        );

        let logs = get_logs_with_client(
            &client,
            rpc_url,
            1,
            10,
            CTF_EXCHANGE_V2,
            ORDER_FILLED_TOPIC,
        )
        .await
        .unwrap();

        assert_eq!(logs.len(), 1);
        assert_eq!(client.request_count(rpc_url), 1);
    }

    #[tokio::test]
    async fn test_get_logs_rpc_error() {
        let client = InMemoryHttpClient::new();
        let rpc_url = "https://polygon-rpc.com";
        client.set_response(
            rpc_url,
            Ok(HttpResponse {
                status: 200,
                body: serde_json::to_string(&serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "error": { "code": -32000, "message": "limit exceeded" },
                }))
                .unwrap(),
            }),
        );

        let err = get_logs_with_client(
            &client,
            rpc_url,
            1,
            10,
            CTF_EXCHANGE_V2,
            ORDER_FILLED_TOPIC,
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("RPC error"));
    }

    #[tokio::test]
    async fn test_get_logs_http_error() {
        let client = InMemoryHttpClient::new();
        let rpc_url = "https://polygon-rpc.com";
        client.set_response(
            rpc_url,
            Ok(HttpResponse {
                status: 429,
                body: "rate limited".into(),
            }),
        );

        let err = get_logs_with_client(
            &client,
            rpc_url,
            1,
            10,
            CTF_EXCHANGE_V2,
            ORDER_FILLED_TOPIC,
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("RPC HTTP error"));
    }

    #[tokio::test]
    async fn test_get_logs_empty_result() {
        let client = InMemoryHttpClient::new();
        let rpc_url = "https://polygon-rpc.com";
        client.set_response(rpc_url, Ok(rpc_response(serde_json::json!([]))));

        let logs = get_logs_with_client(
            &client,
            rpc_url,
            1,
            10,
            CTF_EXCHANGE_V2,
            ORDER_FILLED_TOPIC,
        )
        .await
        .unwrap();

        assert!(logs.is_empty());
    }

    #[tokio::test]
    async fn test_get_receipt_logs_success() {
        let client = InMemoryHttpClient::new();
        let rpc_url = "https://polygon-rpc.com";
        client.set_response(
            rpc_url,
            Ok(rpc_response(serde_json::json!({
                "logs": [sample_log()],
            }))),
        );

        let logs = get_receipt_logs_with_client(&client, rpc_url, "0xtx")
            .await
            .unwrap();
        assert_eq!(logs.len(), 1);
    }

    #[tokio::test]
    async fn test_get_receipt_logs_rpc_error() {
        let client = InMemoryHttpClient::new();
        let rpc_url = "https://polygon-rpc.com";
        client.set_response(
            rpc_url,
            Ok(HttpResponse {
                status: 200,
                body: serde_json::to_string(&serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "error": { "code": -32000, "message": "oops" },
                }))
                .unwrap(),
            }),
        );

        let err = get_receipt_logs_with_client(&client, rpc_url, "0xtx")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("RPC error"));
    }

    #[tokio::test]
    async fn test_get_receipt_logs_http_error() {
        let client = InMemoryHttpClient::new();
        let rpc_url = "https://polygon-rpc.com";
        client.set_response(
            rpc_url,
            Ok(HttpResponse {
                status: 429,
                body: "rate limited".into(),
            }),
        );

        let err = get_receipt_logs_with_client(&client, rpc_url, "0xtx")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("RPC HTTP error"));
    }

    #[tokio::test]
    async fn test_get_receipt_logs_missing_logs() {
        let client = InMemoryHttpClient::new();
        let rpc_url = "https://polygon-rpc.com";
        client.set_response(
            rpc_url,
            Ok(rpc_response(serde_json::json!({"status": "0x1"}))),
        );

        let logs = get_receipt_logs_with_client(&client, rpc_url, "0xtx")
            .await
            .unwrap();
        assert!(logs.is_empty());
    }

    #[tokio::test]
    async fn test_scrape_exchange_success() {
        let client = InMemoryHttpClient::new();
        let dir = tempdir().unwrap();
        let path = dir.path().join("onchain.jsonl");
        let rpc_url = "https://polygon-rpc.com";

        client.set_response(
            rpc_url,
            Ok(rpc_response(serde_json::json!([sample_log()]))),
        );

        let count = scrape_exchange_with_client(
            &client,
            rpc_url,
            CTF_EXCHANGE_V2,
            1,
            1,
            1,
            &path,
        )
        .await
        .unwrap();

        assert_eq!(count, 1);
        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(content.contains("0x448861155279dbf833d041b963e3ac854599e319"));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_scrape_exchange_retries_then_succeeds() {
        let client = InMemoryHttpClient::new();
        let dir = tempdir().unwrap();
        let path = dir.path().join("onchain.jsonl");
        let rpc_url = "https://polygon-rpc.com";

        client.set_response_sequence(
            rpc_url,
            vec![
                Err("timeout".into()),
                Ok(rpc_response(serde_json::json!([sample_log()]))),
            ],
        );

        let count = scrape_exchange_with_client(
            &client,
            rpc_url,
            CTF_EXCHANGE_V2,
            1,
            1,
            1,
            &path,
        )
        .await
        .unwrap();

        assert_eq!(count, 1);
        assert_eq!(client.request_count(rpc_url), 2);
    }

    #[tokio::test]
    async fn test_scrape_exchange_skips_malformed_log() {
        let client = InMemoryHttpClient::new();
        let dir = tempdir().unwrap();
        let path = dir.path().join("onchain.jsonl");
        let rpc_url = "https://polygon-rpc.com";

        let mut log = sample_log();
        log["data"] = "0x1234".into();
        client.set_response(rpc_url, Ok(rpc_response(serde_json::json!([log]))));

        let count = scrape_exchange_with_client(
            &client,
            rpc_url,
            CTF_EXCHANGE_V2,
            1,
            1,
            1,
            &path,
        )
        .await
        .unwrap();

        assert_eq!(count, 0);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn test_scrape_exchange_multiple_blocks() {
        let client = InMemoryHttpClient::new();
        let dir = tempdir().unwrap();
        let path = dir.path().join("onchain.jsonl");
        let rpc_url = "https://polygon-rpc.com";

        client.set_response(
            rpc_url,
            Ok(rpc_response(serde_json::json!([sample_log()]))),
        );

        let count = scrape_exchange_with_client(
            &client,
            rpc_url,
            CTF_EXCHANGE_V2,
            1,
            5,
            2,
            &path,
        )
        .await
        .unwrap();

        assert_eq!(count, 3);
        assert_eq!(client.request_count(rpc_url), 3);
        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 3);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_scrape_exchange_empty_logs_across_blocks() {
        let client = InMemoryHttpClient::new();
        let dir = tempdir().unwrap();
        let path = dir.path().join("onchain.jsonl");
        let rpc_url = "https://polygon-rpc.com";

        client.set_response(rpc_url, Ok(rpc_response(serde_json::json!([]))));

        let count = scrape_exchange_with_client(
            &client,
            rpc_url,
            CTF_EXCHANGE_V2,
            10,
            20,
            5,
            &path,
        )
        .await
        .unwrap();

        assert_eq!(count, 0);
        assert!(!path.exists());
        assert_eq!(client.request_count(rpc_url), 3);
    }

    #[test]
    fn test_constants_cover_known_addresses() {
        let set: HashSet<&str> = [CTF_EXCHANGE_V2, NEG_RISK_CTF_EXCHANGE_V2]
            .into_iter()
            .collect();
        assert_eq!(set.len(), 2);
        assert!(ORDER_FILLED_TOPIC.starts_with("0x"));
    }

    async fn spawn_rpc_server(result: serde_json::Value) -> (u16, tokio::task::JoinHandle<()>) {
        use axum::{extract::Json as AxumJson, routing::post, Router};

        let app = Router::new().route(
            "/",
            post(move |AxumJson(req): AxumJson<serde_json::Value>| async move {
                AxumJson(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": req.get("id").cloned().unwrap_or(1.into()),
                    "result": result,
                }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (port, handle)
    }

    #[tokio::test]
    async fn test_get_receipt_logs_wrapper() {
        let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();
        let (port, handle) = spawn_rpc_server(serde_json::json!({
            "logs": [sample_log()],
        }))
        .await;

        let client = reqwest::Client::new();
        let logs = get_receipt_logs(&client, &format!("http://127.0.0.1:{}/", port), "0xtx")
            .await
            .unwrap();
        assert_eq!(logs.len(), 1);

        handle.abort();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn test_scrape_exchange_wrapper_full() {
        let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();
        let (port, handle) = spawn_rpc_server(serde_json::json!([sample_log()])).await;

        let dir = tempdir().unwrap();
        let path = dir.path().join("onchain.jsonl");
        let client = reqwest::Client::new();

        let count = scrape_exchange(
            &client,
            &format!("http://127.0.0.1:{}/", port),
            CTF_EXCHANGE_V2,
            1,
            1,
            1,
            &path,
        )
        .await
        .unwrap();

        assert_eq!(count, 1);
        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(content.contains("0x448861155279dbf833d041b963e3ac854599e319"));

        handle.abort();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn test_scrape_exchange_wrapper_empty() {
        let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();
        let (port, handle) = spawn_rpc_server(serde_json::json!([])).await;

        let dir = tempdir().unwrap();
        let path = dir.path().join("onchain.jsonl");
        let client = reqwest::Client::new();

        let count = scrape_exchange(
            &client,
            &format!("http://127.0.0.1:{}/", port),
            CTF_EXCHANGE_V2,
            1,
            1,
            1,
            &path,
        )
        .await
        .unwrap();

        assert_eq!(count, 0);
        assert!(!path.exists());

        handle.abort();
        let _ = handle.await;
    }
}
