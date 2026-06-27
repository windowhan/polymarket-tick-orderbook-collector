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
| **Orchestrated Distribution** | Aggregator가 Collector들에게 동적으로 마켓 할당 |
| **파일 저장** | 시간 기반 회전(rotation)으로 로컬 JSONL 저장 |
| **S3-Notify Aggregation** | Collector가 S3 업로드 후 Aggregator에 알림, Aggregator가 병합 |
| **On-chain 조회** | `transaction_hash`로 Polygon에서 실제 체결자(maker/taker) 조회 |
| **Web Viewer** | axum 기반 웹 UI로 호가창/체결/타임라인 시각화 |

---

## 2. 시스템 아키텍처

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              제어 흐름 (Orchestration)                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Gamma API ──► Aggregator live market refresh                              │
│                  (startup fail-fast, 10분 주기 refresh, 12시간 stale 유지)    │
│                                    │                                        │
│                                    ▼                                        │
│                           Aggregator (orchestrator)                         │
│                           - /register                                       │
│                           - /assignment/:id  ──────┐                        │
│                           - /heartbeat/:id         │                        │
│                           - /notify ◄──────────────┘                        │
│                                    │                                        │
│                                    ▼                                        │
│                           S3 download + merge                               │
│                                    │                                        │
│                                    ▼                                        │
│                           data/aggregated_orderbook.jsonl                   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        │ /assignment
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              데이터 흐름 (Data Flow)                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Collector 1 ──► WebSocket ──► 로컬 회전 파일 ──► S3 업로드 ──┐           │
│   Collector 2 ──► WebSocket ──► 로컬 회전 파일 ──► S3 업로드 ──┤           │
│   Collector N ──► WebSocket ──► 로컬 회전 파일 ──► S3 업로드 ──┤           │
│                                                                │           │
│   업로드 완료 후 각 Collector가 Aggregator에게 /notify 호출  ──┘           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              시각화 단계                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   viewer (axum)  ──►  http://127.0.0.1:3001                                │
│   - /api/markets                                                            │
│   - /api/book_snapshots/:asset                                              │
│   - /api/trades/:asset                                                      │
│   - /api/trade_detail/:tx_hash  ──► Polygon RPC                            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
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
    output_dir: PathBuf,
    relay_url: Option<String>,      // 레거시 relay 모드 (거의 사용 안 함)
    aggregator_url: Option<String>, // Orchestrator 주소
    chunk_size: usize,              // 한 워커가 담당할 토큰 수
    rotate_interval: Duration,      // 파일 회전 주기
    duration_secs: Option<u64>,     // graceful shutdown 타이머
    s3_bucket: Option<String>,      // S3 업로드 버킷
    s3_prefix: Option<String>,      // S3 업로드 prefix
    aws_region: String,             // S3 리전
}
```

#### Worker 동작 방식

```rust
pub struct OrderbookWorker {
    id: usize,
    tokens: Vec<String>,
    buffer: Vec<OrderbookEvent>,
    buffer_size: usize,         // 기본 1000
    flush_interval: Duration,   // 기본 10초
    writer: Option<RotatedWriter>,
    relay_url: Option<String>,
    s3_service: Option<Arc<dyn S3Service>>,
    s3_bucket: Option<String>,
    s3_prefix: Option<String>,
    collector_id: Option<String>,
    orchestration_client: Option<OrchestrationClient>,
}
```

5. **회전 파일 업로드**: `RotatedWriter`가 새 time window로 전환되면 이전 파일 경로를 `take_rotated_path()`로 꺼내 백그라운드 `tokio::task`에서 S3로 업로드하고 Aggregator의 `/notify`를 호출합니다. 업로드와 알림이 모두 성공하면 로컬 파일을 삭제합니다.

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

### 3.5 `src/aggregator.rs` — Orchestrated Aggregator + S3 Merger

#### 책임
- Collector 등록 및 마켓 할당 (orchestrator)
- Heartbeat 기반 Collector 생존 감시 및 자동 재할당
- Collector로부터 S3 업로드 notify 수신
- S3 파일 다운로드 및 단일 JSONL로 병합

#### API 엔드포인트

| 엔드포인트 | 메서드 | 설명 |
|-----------|--------|------|
| `/register` | POST | Collector 등록, `collector_id` 반환 |
| `/assignment/:collector_id` | GET | 해당 Collector에 할당된 token IDs 조회 |
| `/heartbeat/:collector_id` | POST | 생존 보고 |
| `/notify` | POST | S3 업로드 완료 알림 (`bucket`, `key`) |

#### 상태 구조

```rust
struct AppState {
    token_ids: Vec<String>,                 // 전체 토큰 리스트
    collectors: HashMap<String, CollectorInfo>,
    assignments: HashMap<String, Vec<String>>, // collector_id -> token_ids
    market_replicas: HashMap<String, Vec<String>>, // token_id -> collector_ids
    pending_files: Vec<S3Object>,           // notify로 들어온 파일
    processed_keys: HashSet<String>,        // 이미 병합한 S3 key
    replication_factor: usize,              // 기본 2
    heartbeat_timeout: Duration,            // 기본 60초
}
```

#### 할당 알고리즘

1. 전체 token 리스트를 `tokens_per_collector`(기본 100) 단위로 chunk 분할
2. 각 chunk를 `replication_factor`(기본 2)만큼의 Collector에 할당
3. Round-robin으로 replica를 서로 다른 Collector에 배치

```rust
// 예시: 4대 Collector, chunk_size=100, replication=2
// Chunk 0 (token 0..99)   → Collector 0, Collector 1
// Chunk 1 (token 100..199) → Collector 2, Collector 3
```

#### Stale Collector 처리

1. 10초마다 모든 Collector의 `last_heartbeat` 확인
2. `heartbeat_timeout`(기본 60초) 초과 시 STALE로 표시
3. STALE Collector의 assignments 해제
4. replica가 부족한 chunk를 healthy Collector에 재할당
5. 재할당된 Collector는 다음 `/assignment` 폴린 또는 재시작 시 새 마켓 수집

#### S3 병합 흐름

```
Collector ──WebSocket──► 로컬 회전 파일
                              │
                              ▼ (rotation 발생)
                    Rust-native S3 업로드
                              │
                              ▼
                         S3 bucket
                              │
                              ▼
                    POST /notify (key, collector_id)
                              │
                              ▼
                         Aggregator
                              │
                              ├─ pending_files에 추가
                              │
                              ▼
                    백그라운드 merge_task (30초 주기)
                              │
                              ├─ pending_files 처리
                              ├─ S3 폴린 백업 (notify 유실 대비)
                              └─ output_path에 append
```

**S3 key 형식**: `{s3_prefix}{collector_id}/{relative_local_path}`
- 여러 Collector가 동일 prefix에 업로드핏 key 충돌을 피하기 위해 `collector_id`를 포함합니다.
- 예: `orderbook/uuid-123/2024-06-08/15/15_00_worker_0.jsonl`

병합 시 중복 제거:
- `book`: `(timestamp / 1000, asset)`
- `last_trade`: `(transaction_hash)` 또는 `(timestamp, asset, price, size)`
- `price_change`: `(timestamp, asset)`

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

### 3.7 `src/orchestration.rs` — Aggregator Orchestration Client

#### 책임
- Collector가 Aggregator에 등록
- 할당된 token IDs 조회
- 주기적 heartbeat 전송
- S3 업로드 완료 후 `/notify` 호출

#### 주요 메서드

```rust
impl OrchestrationClient {
    /// Aggregator에 등록하고 collector_id를 받음
    pub async fn register(base_url: String, preferred_id: Option<String>) -> Result<Self>

    /// 현재 할당된 token IDs와 chunk_size 조회
    pub async fn fetch_assignment(&self) -> Result<(Vec<String>, usize)>

    /// 생존 보고 (10초 주기 권장)
    pub async fn send_heartbeat(&self) -> Result<()>

    /// S3 업로드 완료 알림
    pub async fn notify_s3(&self, bucket: &str, key: &str) -> Result<()>

    /// 백그라운드 heartbeat 태스크 생성
    pub fn spawn_heartbeat_task(&self, interval: Duration) -> JoinHandle<()>
}
```

#### 사용 예시

```rust
let client = OrchestrationClient::register(
    "http://aggregator:8080".to_string(),
    None,
).await?;

let (tokens, chunk_size) = client.fetch_assignment().await?;
let _handle = client.spawn_heartbeat_task(Duration::from_secs(10));

// After S3 upload
client.notify_s3("my-bucket", "orderbook/2024-06-08/12/file.jsonl").await?;
```

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
# Standalone live 모드 (기본): Gamma API에서 시작/주기 refresh 후 로컬 저장
./target/release/polymarket-collector collect-orderbook \
  --output-dir data/orderbook \
  --chunk-size 100 \
  --market-refresh-interval-secs 600 \
  --stale-market-ttl-hours 12 \
  --duration-secs 3600

# 명시적 정적 모드 (오프라인/fixture 파일 기반)
./target/release/polymarket-collector collect-orderbook \
  --static-markets-path data/markets/markets.jsonl \
  --output-dir data/orderbook \
  --chunk-size 100 \
  --duration-secs 3600

# Orchestrated 모드 (Aggregator에게 등록하고 할당받은 마켓 수집, S3 업로드)
./target/release/polymarket-collector collect-orderbook \
  --aggregator-url http://127.0.0.1:8080 \
  --output-dir data/orderbook \
  --s3-bucket my-polymarket-bucket \
  --s3-prefix orderbook/ \
  --aws-region us-east-1 \
  --chunk-size 100
```

### 6.4 Aggregator 실행

```bash
./target/release/polymarket-collector aggregator \
  --bind 127.0.0.1:8080 \
  --output-path data/aggregated_orderbook.jsonl \
  --s3-bucket my-polymarket-bucket \
  --s3-prefix orderbook/ \
  --replication-factor 2 \
  --heartbeat-timeout-secs 60 \
  --market-refresh-interval-secs 600 \
  --stale-market-ttl-hours 12 \
  --delete-after-merge
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

### 7.7 제거된 레거시 명령

새 오케스트레이션 아키텍처로 전환하면서 다음 명령/옵션이 제거되었습니다:

| 제거 항목 | 대안 |
|-----------|------|
| `collect-orderbook --relay-url` | S3 업로드 + `/notify` |
| `aggregate-s3` 서브커맨드 | Aggregator 내부 S3 병합 |
| `collect-orderbook` cron 업로드 | Rust-native S3 업로드 + `/notify` |
| `split-markets` 서브커맨드 | Aggregator 동적 할당 |
| `aggregator` 단순 HTTP `/ingest` | `/register`, `/assignment`, `/notify` API |

### 7.8 고가용성 설정

- `replication_factor=2` 권장: 단일 Collector 장애 시에도 데이터 끊김 없음
- Collector 최소 대수: `ceil(total_tokens / chunk_size / desired_connections_per_machine) * replication_factor`
- `heartbeat_timeout_secs`는 네트워크 지연을 고려하여 30~60초 권장
- `delete-after-merge=true`로 S3 비용 절감 가능 (디버깅 시 false 권장)

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
