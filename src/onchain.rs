use anyhow::Result;
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
///
/// # Arguments
/// * `addr` — A hex string, e.g. `"0x000000000000000000000000448861155279dbf833d041b963e3ac854599e319"`
///
/// # Returns
/// A normalized Ethereum address, e.g. `"0x448861155279dbf833d041b963e3ac854599e319"`
///
/// # Example — Input / Output
/// ```rust,ignore
/// // Input: 32-byte padded address from event topic
/// let padded = "0x000000000000000000000000448861155279dbf833d041b963e3ac854599e319";
///
/// // Function call
/// let normalized = normalize_address(padded);
///
/// // Output: standard 20-byte address
/// assert_eq!(normalized, "0x448861155279dbf833d041b963e3ac854599e319");
/// ```
fn normalize_address(addr: &str) -> String {
    let stripped = addr.strip_prefix("0x").unwrap_or(addr);
    let trimmed = stripped.trim_start_matches('0');
    format!("0x{}", trimmed)
}

/// Parse a single Ethereum event log JSON into an `OnchainTrade` struct.
///
/// Expects a log from the Polymarket CTF Exchange V2 `OrderFilled` event.
/// The log must contain 4 topics (event signature + 3 indexed args) and
/// at least 224 bytes (7 × 32-byte words) of non-indexed data.
///
/// # Arguments
/// * `log` — A `serde_json::Value` representing an Ethereum event log with fields:
///   - `"topics"`: array of 4 hex strings
///   - `"data"`: hex string of encoded event data (≥ 448 hex chars after 0x)
///   - `"transactionHash"`: the tx hash
///   - `"blockNumber"`: hex-encoded block number
///   - `"logIndex"`: hex-encoded log index
///
/// # Returns
/// `Ok(OnchainTrade)` if parsing succeeds, `Err` if data is malformed or too short.
///
/// # Example — Input / Output
/// ```rust,ignore
/// // Input: raw event log from Polygon RPC eth_getLogs response
/// let log = serde_json::json!({
///     "address": "0xe2222d279d744050d28e00520010520000310f59",
///     "topics": [
///         "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee",
///         "0xd980fee1cbe88b9fbca895573ec0296b5a049937556040671ca4eb90d612d473",
///         "0x000000000000000000000000448861155279dbf833d041b963e3ac854599e319",
///         "0x0000000000000000000000006f3c1ddc97c9abfb38ff0f1302a56a1946d04c6f"
///     ],
///     "data": "0x0000...000066fc627fc41c09ce984d8db2aa4dcc8102d201581e73ad410b142f209832e207000000000000000000000000000000000000000000000000000000000608219e000000000000000000000000000000000000000000000000000000000695bb9d000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
///     "transactionHash": "0x5e5fe7c64a30b1d23366bf508ea288b994e3b3d8d5afd5facd991af8551dae02",
///     "blockNumber": "0x5424d9d",
///     "logIndex": "0x38e"
/// });
///
/// // Function call
/// let trade = parse_log(&log).unwrap();
///
/// // Output: decoded fields
/// assert_eq!(trade.maker, "0x448861155279dbf833d041b963e3ac854599e319");
/// assert_eq!(trade.taker, "0x6f3c1ddc97c9abfb38ff0f1302a56a1946d04c6f");
/// assert_eq!(trade.side, 0);  // BUY
/// assert_eq!(trade.block_number, 88231325);
/// ```
///
/// # Data Layout
/// The `data` field is a concatenation of 7 ABI-encoded uint256/bytes32 values:
///
/// ```text
/// Offset (hex chars) | Bytes | Field
/// -------------------+-------+------------------
/// 0..64              | 0..32 | side (uint8, padded)
/// 64..128            | 32..64| token_id (uint256)
/// 128..192           | 64..96| maker_amount_filled (uint256)
/// 192..256           | 96..128| taker_amount_filled (uint256)
/// 256..320           | 128..160| fee (uint256)
/// 320..384           | 160..192| builder (bytes32)
/// 384..448           | 192..224| metadata (bytes32)
/// ```
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
        .map(|s| normalize_address(s))
        .unwrap_or_default();

    // topics[3] = taker address (indexed, same 32-byte padding as maker)
    let taker = topics
        .get(3)
        .and_then(|v| v.as_str())
        .map(|s| normalize_address(s))
        .unwrap_or_default();

    // data[62..64] = side byte (uint8 stored in the last byte of the first 32-byte word).
    // The first 62 hex chars are zeros (31 bytes of padding), char 62..64 is the actual u8 value.
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
/// This is the low-level JSON-RPC call used by `scrape_exchange()` to bulk-query
/// historical `OrderFilled` events across a block range.
///
/// # Arguments
/// * `client` — `reqwest::Client` for making the HTTP POST
/// * `rpc_url` — Polygon RPC endpoint, e.g. `"https://polygon.drpc.org"`
/// * `from_block` — Starting block number (inclusive)
/// * `to_block` — Ending block number (inclusive)
/// * `address` — Contract address to filter on
/// * `topic0` — Event signature hash to filter on
///
/// # Returns
/// A vector of `serde_json::Value` log objects, or an error if the RPC fails.
///
/// # Example — Input / Output
/// ```rust,ignore
/// // Input
/// let client = reqwest::Client::new();
/// let logs = get_logs(
///     &client,
///     "https://polygon.drpc.org",
///     88231300,  // from_block
///     88231330,  // to_block
///     "0xe2222d279d744050d28e00520010520000310F59",
///     "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee",
/// ).await.unwrap();
///
/// // Output: vector of log JSON objects
/// assert!(!logs.is_empty());
/// assert_eq!(logs[0]["address"], "0xe2222d279d744050d28e00520010520000310f59");
/// ```
async fn get_logs(
    client: &reqwest::Client,
    rpc_url: &str,
    from_block: u64,
    to_block: u64,
    address: &str,
    topic0: &str,
) -> Result<Vec<serde_json::Value>> {
    // Build the eth_getLogs JSON-RPC payload.
    // We filter by: contract address + topic0 (event signature).
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

/// Query a single transaction receipt by its hash and return the event logs within it.
///
/// Unlike `get_logs()` which scans a block range, this function looks up a specific
/// transaction that we already know about (e.g., from a WebSocket `last_trade` event's
/// `transaction_hash` field). The receipt contains all event logs emitted by that tx.
///
/// # Arguments
/// * `client` — `reqwest::Client` for the HTTP POST
/// * `rpc_url` — Polygon RPC endpoint
/// * `tx_hash` — The transaction hash as a `0x`-prefixed hex string
///
/// # Returns
/// A vector of `serde_json::Value` log objects from the receipt, or an RPC error.
///
/// # Example — Input / Output
/// ```rust,ignore
/// // Input: tx hash from a WebSocket last_trade event
/// let tx = "0x5e5fe7c64a30b1d23366bf508ea288b994e3b3d8d5afd5facd991af8551dae02";
///
/// // Function call
/// let logs = get_receipt_logs(&client, "https://polygon.drpc.org", tx).await.unwrap();
///
/// // Output: logs from this specific transaction
/// assert!(!logs.is_empty());
/// // The OrderFilled event log will be one of these
/// ```
pub async fn get_receipt_logs(
    client: &reqwest::Client,
    rpc_url: &str,
    tx_hash: &str,
) -> Result<Vec<serde_json::Value>> {
    // Build the eth_getTransactionReceipt JSON-RPC payload.
    // This returns the full receipt including status, gas used, and all event logs.
    let payload = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "eth_getTransactionReceipt",
        "params": [tx_hash],
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
///
/// This is the bridge between "we have a tx hash from WebSocket" and
/// "we know who the maker and taker were."
///
/// # Arguments
/// * `logs` — Event logs from `eth_getTransactionReceipt` or `eth_getLogs`
///
/// # Returns
/// `Some(OnchainTrade)` if a matching OrderFilled log is found and parsed successfully.
/// `None` if no matching log exists or if parsing fails.
///
/// # Matching Logic
/// 1. The log's `address` must be either `CTF_EXCHANGE_V2` or `NEG_RISK_CTF_EXCHANGE_V2`
///    (case-insensitive comparison since RPCs may return lowercase addresses).
/// 2. The log's `topics[0]` must equal `ORDER_FILLED_TOPIC` (the event signature hash).
/// 3. If both match, attempt to `parse_log()` the raw log into an `OnchainTrade`.
///
/// # Example — Input / Output
/// ```rust,ignore
/// // Input: receipt logs from a transaction that settled on NegRisk exchange
/// let logs = vec![
///     // ... ERC-20 Transfer logs ...
///     serde_json::json!({
///         "address": "0xe2222d279d744050d28e00520010520000310f59",
///         "topics": [ORDER_FILLED_TOPIC, "0xabc...", "0x000...maker", "0x000...taker"],
///         "data": "0x0000...",
///         // ...
///     })
/// ];
///
/// // Function call
/// let result = find_order_filled(&logs);
///
/// // Output: the decoded OrderFilled event
/// assert!(result.is_some());
/// let trade = result.unwrap();
/// assert_eq!(trade.maker, "0x448861155279dbf833d041b963e3ac854599e319");
/// assert_eq!(trade.taker, "0x6f3c1ddc97c9abfb38ff0f1302a56a1946d04c6f");
/// ```
pub fn find_order_filled(logs: &[serde_json::Value]) -> Option<OnchainTrade> {
    // Build a HashSet of known exchange addresses in lowercase for case-insensitive matching.
    // Polygon RPCs may return addresses in lowercase even if the constant has mixed case.
    let valid_addresses: std::collections::HashSet<String> =
        [CTF_EXCHANGE_V2, NEG_RISK_CTF_EXCHANGE_V2]
            .iter()
            .map(|a| a.to_lowercase())
            .collect();

    for log in logs {
        // Each log has an "address" field (the contract that emitted it)
        // and a "topics" array (indexed event arguments).
        let addr = log.get("address").and_then(|v| v.as_str()).unwrap_or("");
        let topics = log
            .get("topics")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let topic0 = topics.get(0).and_then(|v| v.as_str()).unwrap_or("");

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
/// This is the bulk historical scraping mode. It iterates through the block range in chunks,
/// queries `eth_getLogs` for each chunk, parses the results, and appends them to a JSONL file.
///
/// # Arguments
/// * `client` — `reqwest::Client`
/// * `rpc_url` — Polygon RPC endpoint
/// * `exchange_address` — The CTF Exchange contract to query
/// * `from_block` — Starting block (inclusive)
/// * `to_block` — Ending block (inclusive)
/// * `chunk_size` — Number of blocks per RPC request (reduce if hitting timeout/rate limits)
/// * `output_path` — Path to append JSONL output
///
/// # Returns
/// Total number of `OnchainTrade` records written to the output file.
///
/// # Chunking Strategy
/// Polygon RPC nodes often have limits on the block range per `eth_getLogs` call.
/// We use `chunk_size` (default 1000) to paginate through large ranges safely.
/// On failure, we retry after a 5-second delay.
///
/// # Example — Input / Output
/// ```rust,ignore
/// // Input: scrape blocks 88_231_000 through 88_231_500
/// let count = scrape_exchange(
///     &client,
///     "https://polygon.drpc.org",
///     CTF_EXCHANGE_V2,
///     88231000,
///     88231500,
///     1000,
///     Path::new("data/onchain.jsonl"),
/// ).await.unwrap();
///
/// // Output: number of trades scraped
/// println!("Scraped {} trades", count);
/// ```
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

    // Iterate through the block range in chunks to avoid RPC timeouts.
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
