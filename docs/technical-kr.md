# Polymarket Tick Orderbook Collector — 기술 문서

> 본 문서는 개발자/기여자를 대상으로 한국어로 작성된 코드 레벨 기술 문서입니다.

---

## 1. 프로젝트 개요

본 프로젝트는 Polymarket CLOB(Central Limit Order Book)의 실시간 호가창(orderbook) 데이터를 수집·가공·시각화하는 Rust 기반 도구입니다.

### 핵심 기능

| 기능 | 설명 |
|------|------|
| **시장 발굴** | Gamma API에서 시장/토큰 목록 수집 |
| **WebSocket 수집** | Polymarket CLOB WebSocket에 병렬 연결하여 book/price_change/last_trade 이벤트 수집 |
| **HTTP Relay** | WebSocket 데이터를 HTTP POST로 중앙 Aggregator에 전달 |
| **파일 저장** | 시간 기반 회전(rotation)으로 로컬 JSONL 저장 |
| **S3 집계** | 분산 수집된 S3 객체를 병합하여 단일 JSONL 생성 |
| **On-chain 조회** | `transaction_hash`로 Polygon에서 실제 체결자(maker/taker) 조회 |
| **Web Viewer** | axum 기반 웹 UI로 호가창/체결/타임라인 시각화 |

---

## 2. 시스템 아키텍처

```
┌─────────────────────────────────────────────────────────────────────┐
│                         데이터 수집 단계                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   Gamma API          Polymarket WS           Polygon RPC            │
│       │                    │                     │                  │
│       ▼                    ▼                     ▼                  │
│  ┌──────────┐      ┌──────────────┐      ┌─────────────┐           │
│  │ discover │      │ ws_orderbook │      │  onchain.rs │           │
│  │  (REST)  │      │  (WebSocket) │      │(eth_getLogs)│           │
│  └────┬─────┘      └──────┬───────┘      └──────┬──────┘           │
│       │                   │                     │                   │
│       ▼                   ▼                     ▼                   │
│  data/markets/       data/orderbook/      data/onchain.jsonl        │
│  markets.jsonl       *.jsonl                                        │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         데이터 집계 단계                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   aggregator (HTTP)  ──►  data/aggregated.jsonl                      │
│        or                                                           │
│   aggregate-s3  ──►  S3 shards 병합 ──►  *.jsonl                    │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         시각화 단계                                   │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   viewer (axum)  ──►  http://127.0.0.1:3001                        │
│   - /api/markets                                                    │
│   - /api/book_snapshots/:asset                                      │
│   - /api/trades/:asset                                              │
│   - /api/trade_detail/:tx_hash  ──► Polygon RPC                    │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 3. 핵심 모듈 상세 설명

### 3.1 `src/ws_orderbook.rs` — WebSocket 수집기

#### 책임
- 여러 WebSocket 연결을 병렬로 관리
- `book`, `price_change`, `last_trade_price` 이벤트 파싱
- 버퍼링 및 주기적 플러시 (파일 또는 relay)

#### 주요 구조체

```rust
pub struct OrderbookCollector {
    token_ids: Vec<String>,
    output_dir: Option<PathBuf>,
    relay_url: Option<String>,
    chunk_size: usize,          // 한 워커가 담당할 토큰 수
    rotate_interval: Duration,  // 파일 회전 주기
    duration_secs: Option<u64>, // graceful shutdown 타이머
}
```

#### Worker 동작 방식

```rust
pub struct OrderbookWorker {
    id: usize,
    tokens: Vec<String>,
    buffer: Vec<OrderbookEvent>,
    buffer_size: usize,     // 기본 1000
    flush_interval: Duration, // 기본 10초
    writer: Option<RotatedWriter>,
    relay_url: Option<String>,
}
```

1. **토큰 분할**: `chunk_size`만큼 토큰을 나눠 각 Worker에 할당
2. **연결**: 각 Worker는 `wss://ws-subscriptions-clob.polymarket.com/ws/market`
3. **구독**: `{"asset_ids": ["..."], "type": "market"}` JSON 전송
4. **수신 루프**: `tokio::select!`로 메시지 수신 / PING 전송 / shutdown 체크
5. **버퍼링**: 이벤트를 `buffer`에 쌓고, `buffer_size` 도달 또는 `flush_interval` 경과 시 flush

#### 메시지 처리 흐름

```
WebSocket text frame
        │
        ▼
handle_message(text)
        │
        ├─ event_type == "book" ──► OrderbookEvent { event_type="book", raw=... }
        ├─ event_type == "price_change" ──► asset_id는 change["asset_id"]에서 추출
        └─ event_type == "last_trade_price" ──► side/tx_hash 파싱
```

#### 주의사항: `price_change` asset_id 처리

Polymarket의 `price_change` 이벤트는 다음과 같은 중첩 구조를 가집니다:

```json
{
  "event_type": "price_change",
  "price_changes": [
    {
      "asset_id": "400737005616952...",
      "price": "0.084",
      "side": "SELL"
    }
  ]
}
```

**함정**: 최상위 `asset_id`는 condition ID이며 실제 토큰 ID가 아닙니다. 실제 asset_id는 `price_changes[].asset_id`에서 추출해야 합니다.

또한 `price`와 `size`는 문자열로 전송되므로 `as_str()` 후 `parse::<f64>()`로 변환해야 합니다.

```rust
let change_asset = change
    .get("asset_id")
    .and_then(|v| v.as_str())
    .map(|s| s.to_string())
    .unwrap_or_else(|| asset.clone());
```

---

### 3.2 `src/onchain.rs` — On-chain 데이터 조회

#### 책임
- Polygon RPC를 통해 `OrderFilled` 이벤트 로그 조회
- 로그 hex 데이터 파싱하여 maker/taker/amount 등 추출
- 블록 범위 기반 대량 스크래핑 (`scrape-onchain` 명령)

#### 컨트랙트 주소

| 컨트랙트 | 주소 | 용도 |
|---------|------|------|
| CTF Exchange V2 | `0xE111180000d2663C0091e4f400237545B87B996B` | 일반 시장 체결 |
| NegRisk CTF Exchange V2 | `0xe2222d279d744050d28e00520010520000310F59` | NegRisk(다중결과) 시장 체결 |

#### OrderFilled 이벤트 시그니처

```solidity
event OrderFilled(
    bytes32 orderHash,      // topics[1]
    address maker,          // topics[2] (32바이트 패딩됨)
    address taker,          // topics[3] (32바이트 패딩됨)
    uint8 side,             // data[0..32] 마지막 바이트 (0=BUY, 1=SELL)
    uint256 makerAssetId,   // data[32..64]
    uint256 takerAssetId,   // data[64..96]
    uint256 makerAmount,    // data[96..128]
    uint256 takerAmount,    // data[128..160]
    uint256 fee,            // data[160..192]
    bytes32 builder,        // data[192..224]
    bytes32 metadata        // data[224..256]
);
```

Topic0 해시:
```
keccak256("OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)")
= 0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee
```

#### 데이터 파싱 상세

`parse_log()` 함수는 다음과 같이 동작합니다:

```rust
let data = log["data"].as_str().strip_prefix("0x").unwrap();

// data 길이: 7개 × 32바이트 = 224바이트 = 448 hex 문자
if data.len() < 448 {
    bail!("Data too short");
}

let side = u8::from_str_radix(&data[62..64], 16)?;   // 마지막 1바이트
let token_id      = format!("0x{}", &data[64..128]);  // 32바이트
let maker_amount  = format!("0x{}", &data[128..192]); // 32바이트
let taker_amount  = format!("0x{}", &data[192..256]); // 32바이트
let fee           = format!("0x{}", &data[256..320]); // 32바이트
let builder       = format!("0x{}", &data[320..384]); // 32바이트
let metadata      = format!("0x{}", &data[384..448]); // 32바이트
```

**주의**: 이벤트 토픽의 `maker`/`taker`는 32바이트로 패딩되어 있습니다. `normalize_address()`를 통해 표준 20바이트 주소로 변환해야 합니다.

```rust
fn normalize_address(addr: &str) -> String {
    let stripped = addr.strip_prefix("0x").unwrap_or(addr);
    let trimmed = stripped.trim_start_matches('0');
    format!("0x{}", trimmed)
}

// Input:  "0x000000000000000000000000448861155279dbf833d041b963e3ac854599e319"
// Output: "0x448861155279dbf833d041b963e3ac854599e319"
```

#### 단일 트랜잭션 조회 (`get_receipt_logs`)

Viewer의 `/api/trade_detail/:tx_hash`는 다음 흐름으로 동작합니다:

```
Client ──GET /api/trade_detail/0xabc...──► Server
                                              │
                                              ▼
                                    eth_getTransactionReceipt
                                              │
                                              ▼
                                    find_order_filled(logs)
                                              │
                                              ▼
                                    parse_log(order_filled_log)
                                              │
                                              ▼
                                    Json<OnchainTrade>
```

결과 캐싱: `HashMap<String, Option<OnchainTrade>>`에 저장하여 동일 tx_hash 반복 조회 방지

---

### 3.3 `src/viewer.rs` — 데이터 시각화 서버

#### 책임
- JSONL 파일 로드 및 병렬 파싱
- API 엔드포인트 제공
- WebSocket/On-chain 데이터 통합

#### 데이터 로드 파이프라인

```
data/longrun_aggregated.jsonl
        │
        ▼
BufReader.lines() → Vec<String>
        │
        ▼
par_chunks(num_cpus) → Rayon 병렬 처리
        │
        ▼
HashMap<String, LocalMarketData> (per thread)
        │
        ▼
merge into ViewerData
        │
        ▼
sort + dedup
```

#### 병렬 처리 전략

```rust
let num_cpus = std::thread::available_parallelism()?.get();
let chunk_size = std::cmp::max(1, (lines.len() + num_cpus - 1) / num_cpus);

let local_maps: Vec<HashMap<String, LocalMarketData>> = lines
    .par_chunks(chunk_size)
    .map(|chunk| { /* 각 스레드가 독립적으로 파싱 */ })
    .collect();
```

**이유**: 800k 라인을 싱글스레드로 파싱하면 60초+ 소요되지만, Rayon 병렬 처리 시 약 16초로 단축됩니다.

#### Book Snapshot 재구성

`book` 이벤트의 `raw` 필드에는 전체 호가창 상태가 포함됩니다:

```json
{
  "bids": [["0.15", "197542.14"], ["0.14", "211667.40"], ...],
  "asks": [["0.16", "12567.06"], ["0.17", "47380.50"], ...]
}
```

**중복 제거**: `(timestamp, asset)` 단위로 중복 제거합니다. 같은 초에 여러 번 도착한 book 이벤트 중 첫 번째만 사용합니다.

```rust
if entry.seen_ts.insert(ev.timestamp) {
    // raw 파싱 및 snapshot 저장
}
```

#### API 엔드포인트

| 엔드포인트 | 설명 | 예시 응답 |
|-----------|------|----------|
| `GET /api/markets` | 시장(asset) 목록 | `["400737...", "647039..."]` |
| `GET /api/book_snapshots/:asset` | 호가창 스냅샷 배열 | `{ "snapshots": [{"timestamp": ..., "bids": [...], "asks": [...]}] }` |
| `GET /api/trades/:asset` | 체결 기록 | `[{"timestamp": ..., "price": 0.084, "side": "BUY", "tx_hash": "0x..."}]` |
| `GET /api/price_history/:asset` | 가격 변동 이력 | `[{"timestamp": ..., "price": 0.084, "side": "BUY"}]` |
| `GET /api/trade_detail/:tx_hash` | On-chain 상세 | `OnchainTrade` JSON |

---

### 3.4 `src/storage.rs` — 파일 저장소

#### 책임
- JSONL 파일 입출력
- 시간 기반 파일 회전 (`RotatedWriter`)
- 파티션된 디렉토리 구조 생성

#### `RotatedWriter`

```rust
pub struct RotatedWriter {
    output_dir: PathBuf,
    suffix: String,
    rotate_interval: Duration,
    current_window: Option<DateTime<Utc>>,
    current_file: Option<tokio::fs::File>,
    current_path: Option<PathBuf>,
}
```

**파일 경로 규칙**:
```
data/orderbook/YYYY-MM-DD/HH/<filename>.jsonl
```

**회전 조건**: 현재 시간이 `rotate_interval` 단위의 새로운 time window에 진입하면 새 파일 생성. 파일명은 immutable(시작 시간 기준)이므로 S3 업로드 시 덮어쓰기 위험 없음.

---

### 3.5 `src/aggregator.rs` — HTTP Relay 수신 서버

#### 책임
- `POST /ingest`로 WebSocket Worker로부터 데이터 수신
- 버퍼링 후 JSONL 파일에 플러시

#### 플러시 전략

```rust
// 5초마다 백그라운드 태스크로 버퍼 플러시
tokio::spawn(async move {
    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;
        flush_buffer(&state).await;
    }
});
```

**주의**: Aggregator는 newline-delimited JSON 형식으로 저장해야 합니다. 각 HTTP body가 여러 개의 JSON 객체를 포함할 수 있으므로, `
`로 분리하여 한 줄씩 저장해야 합니다.

---

### 3.6 `src/market_discovery.rs` — 시장 발굴

#### 책임
- Gamma API에서 시장 목록 수집
- `clobTokenIds` 추출
- 보상/스프레드/사이즈 메타데이터 파싱

#### API 엔드포인트

```
https://gamma-api.polymarket.com/markets?offset=<offset>&limit=100
```

**주의**: `/markets/keyset` 엔드포인트는 cursor 동결 버그가 있어, offset 기반 페이징을 사용하고 최대 10,000개까지만 수집합니다.

---

## 4. 데이터 모델

### 4.1 `OrderbookEvent`

```rust
pub struct OrderbookEvent {
    pub event_type: String,    // "book" | "price_change" | "last_trade"
    pub asset: String,         // 토큰/자산 ID
    pub side: Option<String>,  // BUY / SELL / bid / ask
    pub price: Option<f64>,    // 가격
    pub size: Option<f64>,     // 수량
    pub timestamp: i64,        // 이벤트 발생 시간 (ms)
    pub received_at: i64,      // 로컬 수신 시간 (ms)
    pub raw: String,           // 원본 JSON 텍스트
    pub worker_id: usize,      // WebSocket Worker ID
}
```

### 4.2 `BookSnapshot`

```rust
struct BookSnapshot {
    bids: Vec<Level>,  // [{ price, size }, ...]
    asks: Vec<Level>,  // [{ price, size }, ...]
}
```

### 4.3 `TradePoint`

```rust
struct TradePoint {
    timestamp: i64,
    price: f64,
    size: Option<f64>,
    side: Option<String>,   // "BUY" | "SELL"
    tx_hash: Option<String>, // Polygon tx hash
}
```

### 4.4 `OnchainTrade`

```rust
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
```

---

## 5. Viewer UI 레이아웃 및 동작

### 5.1 화면 구성

```
┌─────────────────────────────────────────────────────────────┐
│  Polymarket Orderbook Viewer                                │
├─────────────────────────────────────────────────────────────┤
│  [Market Select Dropdown]                                   │
├───────────────────────────┬─────────────────────────────────┤
│  Asks (Sell)              │  Trades (≤ selected time)       │
│  [scroll to bottom]       │  Time | Side | Price | Size | Tx│
│  0.17  47380              │  14:32 BUY 0.084 110.47 0x5e..  │
│  0.16  12567  ← Best Ask  │  ...                            │
├───────────────────────────┤                                 │
│  Best Ask 0.16 ←→ 0.15 BB │                                 │
│  Spread 0.0100              │                                 │
├───────────────────────────┤                                 │
│  Bids (Buy)               │                                 │
│  0.15  197542 ← Best Bid  │                                 │
│  0.14  211667             │                                 │
├───────────────────────────┴─────────────────────────────────┤
│  Timeline [================●====]  2024-06-08 14:32:01       │
└─────────────────────────────────────────────────────────────┘
```

### 5.2 Asks/Bids 정렬 규칙

| 테이블 | 정렬 | 표시 범위 | 이유 |
|--------|------|----------|------|
| Asks | 내림차순 (비싼→싼) | `slice(-20)` (최저 20개) | Spread에 가까운 저렴한 ask를 보여주기 위해 |
| Bids | 내림차순 (높은→낮은) | `slice(0, 20)` (최고 20개) | Spread에 가까운 높은 bid를 보여주기 위해 |

### 5.3 Asks 자동 스크롤

Asks 테이블은 내림차순이라 최저가가 맨 아래에 위치합니다. 따라서 렌더링 후 `scrollTop = scrollHeight`로 자동 스크롤하여 spread 근처 가격이 보이도록 합니다.

```javascript
requestAnimationFrame(() => {
    asksWrap.scrollTop = asksWrap.scrollHeight;
});
```

### 5.4 Best Ask / Best Bid / Spread

```javascript
const bestAsk = asks.length > 0 ? asks[asks.length - 1].price : null; // 최저 ask
const bestBid = bids.length > 0 ? bids[0].price : null;                // 최고 bid
const spread = bestAsk - bestBid;
```

### 5.5 Tx 클릭 시 상세 조회

Trades 테이블의 Tx 링크 클릭 → `/api/trade_detail/:tx_hash` 호출 → Polygon RPC 조회 → 팝업에 Maker/Taker/Block 표시

---

## 6. 설정 및 실행

### 6.1 빌드

```bash
cargo build --release
```

### 6.2 시장 발굴

```bash
./target/release/polymarket-collector discover
```

### 6.3 WebSocket 수집

```bash
# 로컬 파일 모드
./target/release/polymarket-collector collect-orderbook \
  --input data/markets/markets.jsonl \
  --output-dir data/orderbook \
  --chunk-size 100 \
  --duration-secs 3600

# HTTP Relay 모드
./target/release/polymarket-collector collect-orderbook \
  --input data/markets/markets.jsonl \
  --relay-url http://127.0.0.1:3000/ingest \
  --chunk-size 100
```

### 6.4 Aggregator 실행

```bash
./target/release/polymarket-collector aggregator \
  --bind 127.0.0.1:3000 \
  --output data/aggregated.jsonl
```

### 6.5 Viewer 실행

```bash
./target/release/polymarket-collector viewer \
  --input-path data/aggregated.jsonl \
  --bind 127.0.0.1:3001 \
  --rpc-url https://polygon.drpc.org
```

### 6.6 On-chain 스크래핑

```bash
./target/release/polymarket-collector scrape-onchain \
  --from-block 88231000 \
  --to-block 88232000 \
  --output data/onchain.jsonl
```

---

## 7. 주의사항 및 함정

### 7.1 `price_change`의 asset_id

최상위 `asset_id`는 condition ID입니다. 실제 토큰 ID는 `price_changes[].asset_id`에 있습니다.

### 7.2 `last_trade`의 side 필드

이전 버그로 인해 저장된 데이터의 `side`가 `null`일 수 있습니다. Viewer는 `raw` 필드에서 fallback으로 복구합니다.

### 7.3 `price`/`size` 문자열 변환

`price_change` 이벤트에서 `price`와 `size`는 문자열로 전송됩니다. `as_f64()` 대신 `as_str().parse::<f64>()`를 사용해야 합니다.

### 7.4 Polygon RPC 엔드포인트

- `https://polygon-rpc.com`은 현재 403/비활성화 상태입니다.
- 대체: `https://polygon.drpc.org`, `https://polygon.llamarpc.com`
- Public RPC는 rate limit에 주의해야 합니다.

### 7.5 S3 집계 시 덮어쓰기

`RotatedWriter`는 시간 window 기반 immutable 파일명을 사용하므로 S3 업로드 시 동일 파일 덮어쓰기로 인한 데이터 손실을 방지합니다.

### 7.6 Viewer 메모리 사용

모든 데이터를 시작 시 메모리에 로드합니다. 800k 라인 기준 약 829MB RAM 사용. 대용량 데이터 시 충분한 메모리 확보 필요.

---

## 8. 성능 지표

| 항목 | 수치 |
|------|------|
| 장기 수집 안정성 | 5h 18min, 808,696 events, 2 reconnects |
| 평균 수집 속도 | ~2,540 events/min |
| Viewer 데이터 로드 (800k 라인) | 16.2s (Rayon 병렬) |
| Viewer 메모리 사용 | 829MB |
| 고유 book 스냅샷 | 103개 (628k book events에서 dedup) |

---

## 9. 기여 가이드라인

### 9.1 코드 주석 규칙

모든 함수는 다음 형식의 doc comment를 포함해야 합니다:

```rust
/// 함수 한 줄 설명
///
/// # Arguments
/// * `arg` — 인자 설명
///
/// # Returns
/// 반환값 설명
///
/// # Example — Input / Output
/// ```rust,ignore
/// // Input
/// let x = ...;
///
/// // Function call
/// let result = func(x);
///
/// // Output
/// assert_eq!(result, ...);
/// ```
fn func(arg: T) -> R { ... }
```

자세한 규칙은 `AGENTS.md`의 "Code Documentation Style Guide" 섹션을 참조하세요.

### 9.2 테스트

```bash
# 단위 테스트
cargo test

# 빌드
cargo build --release

# 로컬 전체 흐름 테스트
./scripts/test_local_relay.sh
```
