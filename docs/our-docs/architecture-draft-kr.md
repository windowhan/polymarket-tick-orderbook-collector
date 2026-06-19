# Polymarket Tick Orderbook Collector — Architecture 초안

> 상태: Draft v0.1
> 범위: 분산 Collector, Aggregator orchestration, GCS notify aggregation, Viewer, On-chain detail 조회 구조를 설명합니다.
> 관련 문서: `docs/our-docs/spec-draft-kr.md`, `docs/our-docs/technical-kr.md`

---

## 1. Architecture Goal

이 시스템은 Polymarket CLOB WebSocket 데이터를 단일 프로세스가 아니라 여러 Collector 인스턴스가 나누어 수집하도록 설계합니다. Collector는 실시간 수집과 임시 내구성을 담당하고, Aggregator는 collector orchestration과 GCS 기반 병합을 담당합니다.

핵심 설계 원칙은 다음과 같습니다.

1. **Collector는 수집에 집중**합니다.
   - WebSocket 연결 유지
   - event parsing
   - local rotated JSONL persistence
   - GCS upload
   - Aggregator notify

2. **Aggregator는 중앙 조정과 병합에 집중**합니다.
   - collector registration
   - heartbeat tracking
   - token assignment
   - GCS notify queue
   - GCS polling fallback
   - merged JSONL output

3. **GCS는 Collector와 Aggregator 사이의 durable handoff layer**입니다.
   - Collector가 Aggregator로 대용량 body를 직접 push하지 않습니다.
   - Aggregator는 GCS object name만 받아 merge합니다.
   - notify가 유실되어도 prefix polling으로 복구할 수 있습니다.

4. **Viewer는 수집 파이프라인과 분리된 read-side component**입니다.
   - Aggregated JSONL을 읽어 UI/API 제공
   - transaction_hash 기반 on-chain detail 조회는 필요 시 Polygon RPC 호출

---

## 1.1 Cloud Provider Boundary

이 아키텍처는 **Google Cloud 전용 운영**을 전제로 합니다. Durable object storage는 Google Cloud Storage(GCS), 권한은 Google Cloud IAM/service account, 인증은 Application Default Credentials(ADC)를 기본값으로 둡니다. AWS S3, EC2, AWS IAM, AWS SDK 기반 설계는 target architecture가 아니며, 현재 코드에 남아 있는 AWS/S3 구현은 GCS object storage adapter로 교체해야 할 migration surface입니다.

---

## 2. High-Level Context

```text
┌────────────────────┐
│ Polymarket Gamma   │
│ Markets API        │
└─────────┬──────────┘
          │ discover
          ▼
┌────────────────────────────────────┐
│ data/markets/markets.jsonl         │
│ full market/token universe         │
└─────────┬──────────────────────────┘
          │ load
          ▼
┌────────────────────────────────────┐
│ Aggregator                         │
│ - orchestration API                │
│ - assignment                       │
│ - heartbeat monitor                │
│ - GCS merge task                    │
└──────┬───────────────┬─────────────┘
       │ assignment    │ notify object name
       │ heartbeat     │
       ▼               │
┌──────────────────┐   │          ┌────────────────────┐
│ Collector 1      │───┼─────────►│ GCS bucket/prefix    │
│ Collector 2      │───┼─────────►│ JSONL objects       │
│ Collector N      │───┘          └─────────┬──────────┘
└────────┬─────────┘                        │ get/list
         │ WebSocket                        ▼
         ▼                         ┌────────────────────┐
┌────────────────────┐             │ aggregated JSONL   │
│ Polymarket CLOB    │             │ output             │
│ WebSocket          │             └─────────┬──────────┘
└────────────────────┘                       │ read
                                             ▼
                                    ┌────────────────────┐
                                    │ Viewer / API       │
                                    └─────────┬──────────┘
                                              │ tx detail
                                              ▼
                                    ┌────────────────────┐
                                    │ Polygon RPC        │
                                    └────────────────────┘
```

---

## 3. Component Map

| Component | Rust module | Runtime command | Responsibility |
|---|---|---|---|
| Market Discovery | `src/market_discovery.rs` | `discover` | Gamma API market fetch, JSONL persistence |
| Collector | `src/ws_orderbook.rs` | `collect-orderbook` | WebSocket collection, event normalization, local rotation, GCS upload |
| Orchestration Client | `src/orchestration.rs` | used by collector | register, assignment, heartbeat, notify |
| Aggregator | `src/aggregator.rs` | `aggregator` | registration, heartbeat, assignment, GCS merge |
| Object storage abstraction | 현재 `src/aggregate_s3.rs`, 목표 `src/object_store.rs` | used by collector/aggregator/tests | Google Cloud Storage and in-memory/emulated object storage operations |
| Trade Backfill | `src/trade_fetcher.rs` | `collect-trades` | historical trade fetch by asset |
| On-chain Scraper | `src/onchain.rs` | `scrape-onchain` | Polygon `OrderFilled` log scraping/parsing |
| Viewer | `src/viewer.rs`, `src/viewer.html` | `viewer` | aggregated JSONL query/UI, trade detail RPC lookup |
| CLI Dispatch | `src/main.rs` | all commands | command parsing and component wiring |

---

## 4. Control Plane vs Data Plane

### 4.1 Control Plane

Control plane은 Collector와 Aggregator 사이의 lightweight HTTP traffic입니다.

```text
Collector ──POST /register──────────────► Aggregator
Collector ◄─{ collector_id }───────────── Aggregator
Collector ──GET /assignment/:id─────────► Aggregator
Collector ◄─{ token_ids, chunk_size }──── Aggregator
Collector ──POST /heartbeat/:id─────────► Aggregator
Collector ──POST /notify { object_name }──► Aggregator
```

특징:

- JSON request/response 기반
- low bandwidth
- Collector lifecycle 관리 목적
- 대용량 event data는 control plane으로 보내지 않음

### 4.2 Data Plane

Data plane은 실제 market data 이동 경로입니다.

```text
Polymarket WebSocket
  → Collector memory buffer
  → Collector local rotated JSONL
  → GCS object
  → Aggregator merge task
  → aggregated JSONL
  → Viewer
```

특징:

- event volume이 큼
- local file과 GCS를 통해 durability 확보
- Aggregator가 direct ingest bottleneck이 되지 않도록 설계

---

## 5. End-to-End Sequence

### 5.1 Bootstrap

```text
Operator
  │
  ├─ run discover
  │    └─ Gamma API → data/markets/markets.jsonl
  │
  ├─ run aggregator
  │    ├─ load markets.jsonl
  │    ├─ flatten token_ids
  │    ├─ start HTTP server
  │    ├─ start heartbeat monitor
  │    └─ start GCS merge task
  │
  └─ run collectors
       ├─ register
       ├─ fetch assignment
       ├─ start heartbeat loop
       └─ start WebSocket workers
```

### 5.2 Collector Runtime

```text
Collector
  │
  ├─ receives token_ids from Aggregator
  ├─ splits tokens into chunks
  ├─ spawns worker per chunk
  │
  ├─ Worker connects to Polymarket WebSocket
  │    ├─ subscribe token chunk
  │    ├─ parse book events
  │    ├─ parse price_change events
  │    ├─ parse last_trade_price events
  │    └─ push normalized OrderbookEvent into buffer
  │
  ├─ flush buffer every interval or buffer_size
  ├─ write to local RotatedWriter
  ├─ when file rotates:
  │    ├─ upload closed file to GCS
  │    ├─ POST /notify to Aggregator
  │    └─ delete local file only after upload+notify success
  │
  └─ on shutdown:
       ├─ set shutdown flag
       ├─ workers exit loop
       ├─ flush remaining buffer
       └─ close current writer
```

### 5.3 Aggregator Runtime

```text
Aggregator
  │
  ├─ /register
  │    ├─ assign collector_id
  │    ├─ mark heartbeat now
  │    └─ rebalance assignments
  │
  ├─ /heartbeat/:collector_id
  │    └─ update last_heartbeat
  │
  ├─ heartbeat monitor every 10s
  │    ├─ find stale collectors
  │    ├─ remove stale collectors
  │    └─ rebalance assignments
  │
  ├─ /assignment/:collector_id
  │    └─ return token_ids + chunk_size
  │
  ├─ /notify
  │    └─ enqueue GCS object name into pending_files
  │
  └─ merge task every 30s
       ├─ process pending notified objects
       ├─ list GCS object prefix for missed notify fallback
       ├─ skip already processed object names
       ├─ append valid JSONL lines to output_path
       ├─ mark object name as processed
       └─ optionally delete GCS object
```

---

## 6. Deployment Topologies

### 6.1 Single-machine local development

```text
one laptop
  ├─ cargo run -- discover
  ├─ cargo run -- collect-orderbook --duration-secs 60
  └─ cargo run -- viewer
```

용도:

- parser 검증
- local output 확인
- viewer UI 확인

제약:

- Polymarket WebSocket per-IP connection limit 회피 불가
- Aggregator/GCS flow를 완전히 검증하지 않음

### 6.2 GCS emulator integration

```text
laptop
  ├─ fake-gcs-server or in-memory GCS-compatible test service
  ├─ aggregator
  └─ collector(s)
```

용도:

- GCS upload/list/get/delete 검증
- Aggregator merge flow 검증

제약:

- 실제 Google Cloud IAM/latency/throughput과 다름
- WebSocket IP limit은 여전히 동일

### 6.3 Cloud distributed collection

```text
VPC/internal network
  ├─ Aggregator instance
  ├─ GCS bucket
  ├─ Collector instance A, public IP A
  ├─ Collector instance B, public IP B
  └─ Collector instance N, public IP N
```

용도:

- 실제 분산 수집
- IP별 WebSocket connection 분산
- production-like long-running collection

권장:

- Aggregator API는 private network 또는 firewall 뒤에 배치
- Collector service account는 GCS target prefix에 대한 `storage.objects.create` 중심으로 제한
- Aggregator service account는 `storage.objects.list`, `storage.objects.get`, optional `storage.objects.delete` 중심으로 제한
- Viewer는 별도 read-only 환경에서 실행 가능

---

## 7. Storage Layout

### 7.1 Local Collector Output

RotatedWriter target layout:

```text
data/orderbook/
  2026-06-19/
    08/
      08_00_worker_0.jsonl
      08_00_worker_1.jsonl
      08_05_worker_0.jsonl
```

파일명 의미:

```text
{hour}_{minute-window}_worker_{worker_id}.jsonl
```

### 7.2 GCS Output

Collector upload target:

```text
gs://{bucket}/{prefix}/{collector_id}/{relative_local_path}
```

예시:

```text
gs://my-polymarket-bucket/orderbook/collector-a/2026-06-19/08/08_00_worker_0.jsonl
```

### 7.3 Aggregated Output

Aggregator append-only output:

```text
data/aggregated_orderbook.jsonl
```

특징:

- GCS object 단위로 append
- line 단위 JSON validation
- malformed line skip
- 현재는 file-level append order가 event timestamp order를 보장하지 않음

---

## 8. Data Model

### 8.1 Normalized OrderbookEvent

```text
OrderbookEvent
  event_type: String
  asset: String
  side: Option<String>
  price: Option<f64>
  size: Option<f64>
  timestamp: i64
  received_at: i64
  raw: String
  worker_id: usize
```

Event type mapping:

| Source event | Normalized `event_type` | Notes |
|---|---|---|
| `book` | `book` | one output row per bid/ask level |
| `price_change` | `price_change` | nested `price_changes[].asset_id` is authoritative |
| `last_trade_price` | `last_trade` | preserves `transaction_hash` in `raw` |

### 8.2 Important parsing caveats

1. Polymarket often sends `price`/`size` as strings.
   - Parser must support both JSON number and string number.

2. `price_change` may contain nested token ids.
   - Top-level `asset_id` can be condition id or otherwise not the token id needed for viewer grouping.
   - `price_changes[].asset_id` should be used when present.

3. `last_trade_price` transaction hash is currently preserved via `raw`.
   - Viewer/on-chain detail can parse it later.

---

## 9. Assignment Architecture

### 9.1 Current assignment model

```text
inputs:
  token_ids: Vec<String>
  healthy_collectors: Vec<String>
  replication_factor: usize
  tokens_per_collector: usize

algorithm:
  chunks = token_ids.chunks(tokens_per_collector)
  for each chunk:
    assign to replication_factor collectors by round-robin
```

장점:

- 단순하고 예측 가능함
- collector 수 변화 시 재계산 쉬움
- replication factor로 redundancy 확보 가능

한계:

- token별 거래량/이벤트량 차이를 고려하지 않음
- collector별 capacity 차이를 고려하지 않음
- collector가 runtime assignment 변경을 지속 polling하지 않으면 hot rebalance 반영이 제한됨

### 9.2 Target evolution

향후 assignment model은 다음을 포함할 수 있습니다.

```text
AssignmentPlan
  version: u64
  generated_at: timestamp
  collectors:
    collector_id:
      token_ids: [...]
      max_ws_connections: N
      expected_event_rate: optional
```

Collector는 주기적으로 assignment version을 확인하고, 변경이 있으면 worker set을 재구성합니다.

---

## 10. Failure Modes

### 10.1 WebSocket disconnect

Current behavior:

```text
worker error
  → log warning
  → sleep reconnect_delay
  → reconnect
  → delay doubles up to max 60s
```

Risk:

- 해당 worker chunk는 reconnect 중 event gap이 생길 수 있습니다.

Mitigation:

- replication_factor > 1로 같은 token chunk를 다른 collector에도 할당
- downstream에서 중복 event deduplication 정책 필요

### 10.2 Collector process crash

Current behavior:

```text
collector stops heartbeating
  → heartbeat monitor detects stale
  → aggregator removes collector
  → rebalance assignments
```

Risk:

- crash 직전 local unuploaded file은 해당 머신에 남을 수 있습니다.

Mitigation 후보:

- startup recovery: collector 시작 시 output_dir의 orphan rotated file upload 시도
- local disk monitoring
- graceful systemd stop hook

### 10.3 GCS upload failure

Current behavior:

```text
upload/notify task fails
  → warning log
  → local file preserved
```

Risk:

- retry가 별도 background queue로 지속되지 않으면 operator 개입이 필요할 수 있음

Mitigation 후보:

- failed upload retry queue
- startup orphan file scanner
- upload status sidecar file

### 10.4 Notify failure

Current behavior:

```text
notify fails
  → local file preserved
```

Aggregator fallback:

```text
merge task lists GCS object prefix every 30s
  → finds unprocessed .jsonl
  → merges
```

Risk:

- upload succeeded but notify failed and local delete did not happen하면 중복 local upload retry 시 같은 object name 재업로드 가능

Mitigation:

- deterministic object name means same object overwrite/generation update or idempotent reprocess via processed object-name manifest

### 10.5 Aggregator restart

Current behavior:

- in-memory `processed_keys`, collectors, assignments are lost.
- GCS object prefix polling can re-discover objects.

Risk:

- output file이 유지되고 processed_keys가 사라지면 이미 merge한 GCS object를 다시 append할 수 있음.

Mitigation 후보:

- processed object-name manifest 저장
- output checkpoint metadata
- object tagging after merge
- delete-after-merge 사용

---

## 11. Consistency / Idempotency

### Current guarantees

| Layer | Guarantee |
|---|---|
| Collector buffer | flush 전 process crash 시 손실 가능 |
| Local rotated file | flush 이후 local disk에 존재 |
| GCS upload | upload 성공 시 object-level durability |
| Notify | at-least-once 성격으로 처리 가능 |
| Aggregator processed_keys | process lifetime 내 GCS object name 중복 merge 방지 |
| Aggregated output | append-only JSONL |

### Not guaranteed yet

- global exactly-once event semantics
- event timestamp total ordering
- aggregator restart 이후 processed_keys persistence
- replica collector 간 duplicate event deduplication

### Recommended policy

1. Raw bronze dataset은 append-only로 유지합니다.
2. Dedup/sort는 별도 silver processing 단계에서 수행합니다.
3. Dedup key 후보:
   - `raw.transaction_hash + asset + price + size + timestamp`
   - `event_type + asset + side + price + size + timestamp + raw hash`

---

## 12. Viewer Architecture

Viewer는 Aggregator와 직접 연결된 live dashboard라기보다 aggregated JSONL snapshot/append file을 읽는 read-side process입니다.

```text
viewer startup
  ├─ read input JSONL
  ├─ parse events
  ├─ build in-memory indexes
  │    ├─ markets/assets
  │    ├─ book snapshots by asset
  │    └─ trades by asset
  └─ serve HTTP + HTML
```

API responsibilities:

| Endpoint shape | Responsibility |
|---|---|
| `/` | HTML viewer |
| `/api/markets` | asset/market 목록 |
| `/api/book_snapshots/:asset` | 특정 asset orderbook snapshot |
| `/api/trades/:asset` | 특정 asset trade list |
| `/api/trade_detail/:tx_hash` | Polygon RPC on-chain detail 조회 |

주의:

- 매우 큰 aggregated JSONL을 메모리에 전부 로드하면 startup time과 memory가 커질 수 있습니다.
- production viewer는 향후 SQLite/DuckDB/Parquet 또는 streaming indexer로 분리하는 것이 좋습니다.

---

## 13. On-chain Detail Architecture

`last_trade` event는 `raw` 안에 `transaction_hash`를 보존합니다. Viewer는 사용자가 tx를 클릭하면 Polygon RPC로 transaction receipt/log를 조회하고, CTF Exchange `OrderFilled` event를 파싱하여 maker/taker/detail을 보여줍니다.

```text
User clicks tx hash
  → Viewer /api/trade_detail/:tx_hash
  → Polygon RPC getTransactionReceipt
  → find OrderFilled log
  → parse topics/data
  → return maker/taker/amount/block_number
```

Relevant contracts:

| Contract | Address | Purpose |
|---|---|---|
| CTF Exchange V2 | `0xE111180000d2663C0091e4f400237545B87B996B` | 일반 시장 체결 |
| NegRisk CTF Exchange V2 | `0xe2222d279d744050d28e00520010520000310F59` | Negative risk 시장 체결 |

---

## 14. Runtime Configuration

### Environment

| Variable / Config | Purpose |
|---|---|
| `RUST_LOG=info` | structured logs 확인 |
| Google Application Default Credentials | GCS access |
| network/firewall | Aggregator API protection |

### Key runtime parameters

| Parameter | Component | Default | Architecture impact |
|---|---|---:|---|
| `chunk-size` | Collector | 100 | WebSocket connection count 결정 |
| `rotate-interval-secs` | Collector | 300 | file size, upload cadence, merge lag 결정 |
| `replication-factor` | Aggregator | 2 | redundancy와 duplicate volume 결정 |
| `heartbeat-timeout-secs` | Aggregator | 60 | stale detection sensitivity |
| merge tick | Aggregator | 30s currently hardcoded | notify-to-merge latency |
| heartbeat interval | Collector | 10s currently hardcoded | liveness traffic/sensitivity |

---

## 15. Known Architecture Gaps

1. **Script drift**
   - 일부 scripts는 과거 relay mode(`--relay-url`)를 사용합니다.
   - 현재 target architecture는 GCS notify flow입니다.

2. **Aggregator restart idempotency**
   - `processed_keys`가 in-memory라 restart 후 중복 append 가능성이 있습니다.

3. **Runtime rebalance propagation**
   - Aggregator가 rebalance해도 기존 Collector가 assignment를 지속 polling하지 않으면 새 assignment를 자동 반영하지 못합니다.

4. **Upload retry lifecycle**
   - upload/notify 실패 시 local file은 보존되지만 자동 재시도 관리가 부족할 수 있습니다.

5. **Duplicate event policy**
   - replication_factor > 1이면 의도적인 중복 수집이 발생합니다.
   - downstream dedup key와 silver dataset policy가 필요합니다.

6. **Large viewer scalability**
   - Viewer가 JSONL을 직접 로드하는 방식은 초기 개발/분석에는 적합하지만 장기 대용량 데이터에는 별도 query store가 필요합니다.

---

## 16. Recommended Next Architecture Iterations

우선순위 순서:

1. Current GCS notify flow를 기준으로 stale relay scripts 제거/수정
2. Collector startup orphan file upload recovery 추가
3. Aggregator processed object-name manifest persistence 추가
4. Collector assignment polling/versioning 추가
5. Dedup/silver processing stage 설계
6. Viewer query backend를 파일 직접 로드에서 SQLite/DuckDB/Parquet로 확장
7. Cloud deployment/IAM/Terraform 문서화

---

## 17. Minimal Production Runbook Draft

```text
1. Build release binary
2. Run discover and validate markets.jsonl line count
3. Create GCS bucket/prefix and IAM roles
4. Start Aggregator with bucket/prefix/markets path
5. Start N Collectors with aggregator-url and same bucket/prefix
6. Monitor logs:
   - collector registered
   - assignment received
   - worker subscribed
   - flushed buffer
   - uploaded and notified
   - aggregator merged object
7. Start Viewer against aggregated output
8. Periodically verify:
   - GCS object count
   - aggregated JSONL line growth
   - collector heartbeat health
   - reconnect/error rate
```
