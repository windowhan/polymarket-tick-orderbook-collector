# Polymarket Tick Orderbook Collector — 제품/기능 Spec 초안

> 상태: Draft v0.1
> 범위: `discover → aggregator → collectors → GCS notify → merge → viewer → on-chain detail` 플로우를 기준으로 한 요구사항 초안입니다.
> 목적: 구현 세부보다 “무엇을 만족해야 하는가”를 먼저 고정하고, 이후 Rust 구현/테스트/배포 문서가 이 문서를 기준으로 맞춰지도록 합니다.

---

## 1. 목표

이 프로젝트의 목표는 Polymarket CLOB의 실시간 tick-level orderbook/trade 이벤트를 안정적으로 수집하고, 여러 Collector 인스턴스가 분산 수집한 데이터를 중앙 Aggregator가 병합하여 분석/시각화 가능한 JSONL 데이터셋으로 만드는 것입니다.

핵심 Flow는 다음과 같습니다.

```text
1. discover
   Polymarket Gamma API에서 수집 대상 market/token 목록 생성

2. aggregator
   Collector 등록, collector heartbeat, token assignment, GCS notify 수신, GCS merge 담당

3. collector N개
   Aggregator에 register → assignment 수신 → WebSocket 수집 → 로컬 회전 파일 기록

4. GCS upload + notify
   Collector가 회전 완료된 JSONL 파일을 GCS에 업로드하고 Aggregator에 /notify 전송

5. aggregation
   Aggregator가 notified GCS object를 다운로드하여 단일 aggregated JSONL로 병합

6. viewer
   Aggregated JSONL을 로드하여 orderbook/trade/timeline을 웹 UI로 시각화

7. on-chain detail
   transaction_hash가 있는 trade는 Polygon RPC로 maker/taker 등 실제 체결 로그 조회
```

---

## 1.1 Cloud Provider 결정

이 문서의 목표 아키텍처는 **Google Cloud 기준**입니다. AWS는 사용할 수 없다는 제약을 전제로 하며, durable handoff layer는 Amazon S3가 아니라 **Google Cloud Storage(GCS)** 를 사용합니다.

설계상 object storage 추상화는 유지하되, production 구현/운영 문서/권한 모델/배포 예시는 모두 GCS, Google Cloud IAM, service account, Application Default Credentials(ADC)를 기준으로 작성합니다. 현재 코드에 남아 있는 `s3` 명칭과 AWS SDK 의존성은 migration 대상입니다.

---

## 2. 성공 기준

### 2.1 기능 성공 기준

| 영역 | 성공 기준 |
|---|---|
| Market discovery | 활성/비활성/종료 여부 필터를 적용해 `data/markets/markets.jsonl` 생성 |
| Collector registration | Collector가 Aggregator에 등록하고 고유 `collector_id`를 받음 |
| Dynamic assignment | Aggregator가 healthy collector 목록을 기준으로 token assignment를 생성 |
| WebSocket collection | Collector worker가 Polymarket CLOB WebSocket에 연결해 `book`, `price_change`, `last_trade_price` 이벤트 수집 |
| Local durability | Collector가 이벤트를 메모리에만 두지 않고 회전 JSONL 파일에 flush |
| GCS handoff | 회전 완료된 파일을 GCS에 업로드하고 Aggregator에 notify |
| Merge | Aggregator가 GCS JSONL object를 다운로드하고 유효 JSONL line만 output에 append |
| Viewer | Aggregated JSONL 기반으로 market, book snapshot, trade list, trade detail 조회 가능 |

### 2.2 운영 성공 기준

| 영역 | 성공 기준 |
|---|---|
| 장애 복구 | WebSocket 연결이 끊기면 worker가 backoff 후 재연결 |
| Collector 장애 감지 | heartbeat timeout 이후 stale collector를 제외하고 재할당 |
| Notify 유실 대응 | `/notify` 유실 시에도 Aggregator의 GCS polling fallback으로 병합 가능 |
| 중복 방지 | Aggregator는 이미 처리한 GCS object name을 재처리하지 않음 |
| 로컬 데이터 보존 | GCS upload 또는 notify 실패 시 collector local file을 삭제하지 않음 |
| 종료 | `duration-secs` 또는 Ctrl+C 시 worker가 남은 buffer를 flush하고 종료 |

---

## 3. 비목표 / 현재 제외 범위

이 초안의 1차 범위에서 제외하는 항목입니다.

1. Polymarket 주문 제출, 취소, 서명, trading bot 기능
2. 실시간 알파 전략 또는 매매 의사결정 엔진
3. 완전한 exactly-once end-to-end 보장
4. 장기 데이터 웨어하우스 스키마 설계
5. 복잡한 UI 권한/로그인 시스템
6. 다중 클라우드 orchestration 자동 배포
7. WebSocket 원천 데이터의 완전 무손실 보장

다만 GCS object name 단위 idempotency, local file preservation, retry 가능한 구조는 1차 범위에 포함합니다.

---

## 4. 주요 사용자/운영 시나리오

### 4.1 로컬 smoke test

목적: 전체 기능이 큰 비용 없이 동작하는지 확인합니다.

```bash
cargo build
RUST_LOG=info cargo run -- discover --active true --closed false
RUST_LOG=info cargo run -- collect-orderbook \
  --markets-path data/markets/markets.jsonl \
  --output-dir data/orderbook \
  --limit-tokens 100 \
  --duration-secs 60
```

결과:

- `data/markets/markets.jsonl` 존재
- `data/orderbook/<date>/<hour>/*.jsonl` 형태의 local rotated files 생성

### 4.2 분산 수집 운영 모드

목적: 여러 Collector가 Aggregator에게 할당받은 token을 수집하고 GCS를 통해 병합합니다.

1. `discover`로 full market universe 생성
2. Aggregator 실행
3. Collector 인스턴스 N개 실행
4. Collector가 각자 GCS 업로드 + notify
5. Aggregator가 `data/aggregated_orderbook.jsonl`로 병합
6. Viewer로 확인

예시:

```bash
RUST_LOG=info cargo run -- aggregator \
  --bind 0.0.0.0:8080 \
  --markets-path data/markets/markets.jsonl \
  --output-path data/aggregated_orderbook.jsonl \
  --gcs-bucket my-polymarket-bucket \
  --gcs-prefix orderbook/ \
  --replication-factor 2 \
  --heartbeat-timeout-secs 60 \
  --gcp-project my-gcp-project
```

```bash
RUST_LOG=info cargo run -- collect-orderbook \
  --aggregator-url http://AGGREGATOR_HOST:8080 \
  --output-dir data/orderbook \
  --gcs-bucket my-polymarket-bucket \
  --gcs-prefix orderbook/ \
  --gcp-project my-gcp-project \
  --chunk-size 100
```

### 4.3 Viewer 분석 모드

목적: Aggregator가 만든 단일 JSONL을 웹에서 확인합니다.

```bash
RUST_LOG=info cargo run -- viewer \
  --input-path data/aggregated_orderbook.jsonl \
  --bind 127.0.0.1:3001 \
  --rpc-url https://polygon.drpc.org
```

결과:

- 브라우저에서 `http://127.0.0.1:3001` 접속
- asset별 book snapshot, trade, timeline 확인
- transaction hash 클릭 시 Polygon RPC 기반 detail 조회

---

## 5. CLI 요구사항

### 5.1 `discover`

역할: Gamma API에서 market metadata를 수집하여 JSONL로 저장합니다.

입력:

| 옵션 | 필수 | 기본값 | 설명 |
|---|---:|---|---|
| `--active` | 아니오 | 없음 | active market 필터 |
| `--closed` | 아니오 | 없음 | closed market 필터 |

출력:

```text
data/markets/markets.jsonl
```

각 line은 하나의 market JSON입니다.

### 5.2 `collect-orderbook`

역할: WebSocket orderbook/trade event를 수집합니다.

운영 모드:

| 모드 | 조건 | token source | output |
|---|---|---|---|
| Static local mode | `--aggregator-url` 없음 | `--markets-path` | local rotated JSONL |
| Orchestrated GCS mode | `--aggregator-url` 있음 | Aggregator assignment | local rotated JSONL + GCS upload + notify |

주요 옵션:

| 옵션 | 필수 | 기본값 | 설명 |
|---|---:|---|---|
| `--markets-path` | static mode에서 필요 | `data/markets/markets.jsonl` | market/token JSONL path |
| `--output-dir` | 아니오 | `data/orderbook` | local rotated files root |
| `--aggregator-url` | orchestrated mode에서 필요 | 없음 | Aggregator base URL |
| `--gcs-bucket` | GCS upload 시 필요 | 없음 | upload target bucket |
| `--gcs-prefix` | 아니오 | `orderbook/` | upload object name prefix |
| `--gcp-project` | 아니오 | ADC에서 추론 | Google Cloud project id. Application Default Credentials로 추론 가능하면 생략 가능 |
| `--chunk-size` | 아니오 | `100` | worker/WebSocket connection당 token 수 |
| `--rotate-interval-secs` | 아니오 | `300` | local file rotation interval |
| `--duration-secs` | 아니오 | 없음 | 테스트용 finite run |
| `--limit-tokens` | 아니오 | 없음 | static mode load test용 token limit |

### 5.3 `aggregator`

역할: Orchestration server + GCS merge worker입니다.

주요 옵션:

| 옵션 | 필수 | 기본값 | 설명 |
|---|---:|---|---|
| `--bind` | 아니오 | `0.0.0.0:8080` | HTTP listen address |
| `--output-path` | 아니오 | `data/aggregated_orderbook.jsonl` | merged JSONL output |
| `--markets-path` | 아니오 | `data/markets/markets.jsonl` | full market universe |
| `--gcs-bucket` | 예 | 없음 | collector upload bucket |
| `--gcs-prefix` | 아니오 | `orderbook/` | collector upload prefix |
| `--replication-factor` | 아니오 | `2` | token chunk당 replica collector 수 |
| `--heartbeat-timeout-secs` | 아니오 | `60` | stale collector timeout |
| `--delete-after-merge` | 아니오 | false | merge 후 GCS object 삭제 여부 |
| `--gcp-project` | 아니오 | ADC에서 추론 | Google Cloud project id. GCS bucket location은 bucket 생성 시 결정 |

### 5.4 `viewer`

역할: aggregated JSONL을 메모리에 로드하고 web API/UI를 제공합니다.

주요 옵션:

| 옵션 | 필수 | 기본값 | 설명 |
|---|---:|---|---|
| `--input-path` | 아니오 | `data/local_test_aggregated.jsonl` | viewer input JSONL |
| `--bind` | 아니오 | `127.0.0.1:3000` | HTTP listen address |
| `--rpc-url` | 아니오 | `https://polygon.drpc.org` | trade detail용 Polygon RPC |

### 5.5 `scrape-onchain`

역할: Polygon RPC에서 CTF Exchange `OrderFilled` logs를 backfill합니다.

주요 옵션:

| 옵션 | 필수 | 기본값 | 설명 |
|---|---:|---|---|
| `--rpc-url` | 아니오 | `https://polygon-rpc.com` | Polygon RPC endpoint |
| `--exchange` | 아니오 | CTF Exchange V2 | target exchange contract |
| `--from-block` | 예 | 없음 | start block |
| `--to-block` | 예 | 없음 | end block |
| `--chunk-size` | 아니오 | `1000` | eth_getLogs block chunk size |
| `--output` | 아니오 | `data/onchain.jsonl` | output JSONL |

---

## 6. HTTP API Spec — Aggregator

### 6.1 `POST /register`

Collector가 Aggregator에 자신을 등록합니다.

Request:

```json
{
  "collector_id": "optional-preferred-id"
}
```

`collector_id`는 optional입니다. 없으면 Aggregator가 UUID를 발급합니다.

Response:

```json
{
  "collector_id": "generated-or-accepted-id"
}
```

Side effects:

- `collectors[collector_id].last_heartbeat = now`
- healthy collector set이 변경되므로 assignment rebalance 수행

### 6.2 `POST /heartbeat/:collector_id`

Collector 생존 신호를 갱신합니다.

Response:

| 조건 | Status |
|---|---:|
| collector 존재 | `200 OK` |
| collector 미등록 | `404 NOT FOUND` |

### 6.3 `GET /assignment/:collector_id`

Collector가 현재 자신에게 할당된 token list를 가져옵니다.

Response:

```json
{
  "token_ids": ["token-1", "token-2"],
  "chunk_size": 100
}
```

요구사항:

- unknown collector에 대해서는 empty assignment를 반환할 수 있습니다.
- Collector는 받은 `token_ids`를 `chunk_size` 단위로 worker/WebSocket connection에 분배합니다.

### 6.4 `POST /notify`

Collector가 GCS upload 완료를 Aggregator에 알립니다.

Request target shape:

```json
{
  "bucket": "my-polymarket-bucket",
  "object_name": "orderbook/collector-id/2026-06-19/08/08_30_worker_0.jsonl",
  "collector_id": "collector-id"
}
```

Response:

```json
{
  "received": true
}
```

요구사항:

- 이미 처리된 `object_name`은 재처리하지 않습니다.
- notify를 받은 object name은 pending queue에 들어갑니다.
- merge task는 pending queue를 우선 처리합니다.
- notify가 누락되더라도 GCS object prefix polling fallback으로 처리되어야 합니다.

주의:

- 현재 AWS/S3 기반 구현의 Aggregator handler는 `key`, `collector_id`를 사용합니다. GCS migration 후에는 request field를 `object_name`으로 바꾸고, bucket은 실행 옵션의 `--gcs-bucket`을 authoritative source로 사용합니다.
- Client notify body에 `bucket`을 포함할지 여부는 최종 contract에서 결정합니다. 권장안은 configured `--gcs-bucket`만 신뢰하고 body에는 `object_name`, `collector_id`, optional checksum/generation만 포함하는 것입니다.

---

## 7. Data Contract

### 7.1 Market JSONL

Source:

```text
data/markets/markets.jsonl
```

각 line은 market 하나를 나타냅니다. Collector/Aggregator에게 특히 중요한 필드는 다음과 같습니다.

```json
{
  "id": "market-id",
  "condition_id": "condition-id",
  "question": "Will ...?",
  "slug": "market-slug",
  "active": true,
  "closed": false,
  "archived": false,
  "enable_order_book": true,
  "neg_risk": false,
  "accepting_orders": true,
  "token_ids": ["token-id-yes", "token-id-no"],
  "clob_rewards": []
}
```

### 7.2 Orderbook Event JSONL

Collector output과 Aggregator output의 기본 line schema입니다.

```json
{
  "event_type": "book | price_change | last_trade",
  "asset": "token-id",
  "side": "bid | ask | BUY | SELL | null",
  "price": 0.084,
  "size": 110.476189,
  "timestamp": 1781051970651,
  "received_at": 1781051970699,
  "raw": "{ original websocket json }",
  "worker_id": 3
}
```

필드 의미:

| 필드 | 설명 |
|---|---|
| `event_type` | normalized event type |
| `asset` | Polymarket CLOB token id |
| `side` | book이면 `bid/ask`, trade이면 `BUY/SELL` 가능 |
| `price` | decimal price, 원천 데이터가 문자열이어도 f64로 변환 |
| `size` | decimal size, 원천 데이터가 문자열이어도 f64로 변환 |
| `timestamp` | source event timestamp 우선, 없으면 local receive time |
| `received_at` | collector local receive timestamp in milliseconds |
| `raw` | 원본 WebSocket JSON string |
| `worker_id` | collector process 내부 worker 번호 |

### 7.3 GCS Object Name

Target format:

```text
{prefix}/{collector_id}/{relative_local_path}
```

예시:

```text
orderbook/collector-abc/2026-06-19/08/08_30_worker_0.jsonl
```

요구사항:

- prefix 중복 slash는 피합니다.
- collector_id를 포함해 여러 Collector가 같은 파일명을 생성해도 object name collision이 없어야 합니다.
- Aggregator는 `.jsonl` suffix만 merge 대상으로 삼습니다.

---

## 8. Assignment / Replication 요구사항

Aggregator는 full token list와 healthy collector list를 기준으로 assignment를 생성합니다.

현재 기본 전략:

```text
1. 전체 token_ids를 tokens_per_collector 단위 chunk로 나눔
2. 각 chunk를 replication_factor번 서로 다른 collector에 round-robin 할당
3. collector가 추가/제거되면 rebalance
```

요구사항:

- `replication_factor = 1`: 각 token chunk가 collector 한 곳에만 할당됩니다.
- `replication_factor = 2`: 각 token chunk가 두 collector에 중복 할당됩니다.
- healthy collector 수가 replication factor보다 작으면 실제 replica 수는 줄어들 수 있습니다.
- collector stale 감지 이후 stale collector는 assignment에서 제거되어야 합니다.

향후 개선 후보:

- token별 volume/reward 기반 weighted assignment
- connection/IP limit을 고려한 collector capacity 설정
- assignment versioning
- collector가 주기적으로 assignment 변경을 polling하여 hot rebalance 반영

---

## 9. Reliability 요구사항

### 9.1 Collector

- WebSocket disconnect 시 exponential backoff로 재연결합니다.
- buffer는 `buffer_size` 도달 또는 `flush_interval` 도달 시 flush합니다.
- 회전 파일은 upload+notify 성공 후에만 삭제합니다.
- upload 실패 또는 notify 실패 시 local file을 보존합니다.
- Ctrl+C 또는 duration timeout 시 남은 buffer를 flush합니다.

### 9.2 Aggregator

- heartbeat monitor는 stale collector를 제거하고 rebalance합니다.
- merge task는 30초 주기로 동작합니다.
- merge는 pending notified files를 먼저 처리합니다.
- 이후 GCS object prefix list로 missed notify fallback을 수행합니다.
- malformed JSON line은 skip하고 valid line만 append합니다.
- `processed_keys`로 GCS object name 중복 merge를 방지합니다.

### 9.3 Viewer

- input JSONL이 없으면 명확히 실패해야 합니다.
- malformed line이 있더라도 전체 viewer가 죽지 않도록 skip 또는 warning 처리하는 방향이 바람직합니다.
- raw field에서 transaction_hash를 fallback 추출할 수 있어야 합니다.

---

## 10. Observability 요구사항

모든 실행 모드는 `RUST_LOG=info` 기준으로 다음 정보를 확인할 수 있어야 합니다.

| Component | 필요 로그 |
|---|---|
| discover | page fetch, total market count, output path |
| aggregator | bind address, loaded markets/tokens, register, heartbeat stale, rebalance, notify, merge result |
| collector | mode, assigned token count, worker count, WebSocket subscribe, flush count/path, upload/notify result, reconnect |
| viewer | input path, loaded event count, bind address, RPC URL |

향후 metric 후보:

- events/sec per collector
- flush latency
- GCS upload latency
- merge lag: `GCS uploaded_at → merged_at`
- WebSocket reconnect count
- assignment generation/version
- per-token event count

---

## 11. Security / Credentials 요구사항

- Google Cloud credential은 코드/문서에 하드코딩하지 않습니다.
- Google Cloud Storage client 기본 credential chain을 사용합니다.
- GCS bucket 권한은 최소한 다음으로 제한합니다.
  - Collector: `storage.objects.create` on target bucket/prefix
  - Aggregator: `storage.objects.list`, `storage.objects.get`, optional `storage.objects.delete`
- Aggregator HTTP API는 production 배포 시 내부 네트워크 또는 인증/방화벽 뒤에 둡니다.
- Viewer의 Polygon RPC URL은 환경별로 변경 가능해야 합니다.

---

## 12. Acceptance Test 초안

### 12.1 Unit / Integration

| Test | 기대 결과 |
|---|---|
| `discover` with mocked Gamma API | markets.jsonl 생성 |
| static `collect-orderbook` missing file | 명확한 error |
| assignment allocation | replication factor에 따른 token 분배 검증 |
| heartbeat stale removal | stale collector 제거 후 rebalance |
| notify handler | pending queue에 GCS object name 추가 |
| merge object | valid JSONL만 output append |
| duplicate object name | processed object name은 중복 merge 안 함 |
| upload rotated file | GCS object name 생성, notify, local delete 순서 검증 |
| viewer start with input | HTTP server 시작 |

### 12.2 Manual smoke

```bash
cargo test
cargo build
cargo run -- discover --active true --closed false
cargo run -- collect-orderbook \
  --markets-path data/markets/markets.jsonl \
  --limit-tokens 20 \
  --duration-secs 30
```

성공 기준:

- command가 panic 없이 종료
- local JSONL output 생성
- line별 JSON parse 가능

### 12.3 GCS emulator smoke

```bash
docker compose up -d fake-gcs
cargo test --test gcs_integration_test
```

성공 기준:

- GCS put/list/get/delete 동작
- merge 결과 JSONL line count가 기대값과 일치

---

## 13. 현재 구현과 Spec 차이 / 정리 필요 항목

이 초안 기준으로 현재 코드/스크립트에서 확인된 정리 후보입니다.

1. `scripts/test_local_relay.sh`, `scripts/long_running_test.sh`는 `--relay-url`을 사용하지만 현재 CLI help에는 `--relay-url`이 없습니다.
2. 현재 CLI/코드에는 AWS/S3 명칭(`--s3-bucket`, `--s3-prefix`, `--aws-region`, `src/aggregate_s3.rs`)이 남아 있습니다. 목표 CLI는 `--gcs-bucket`, `--gcs-prefix`, optional `--gcp-project`로 정리합니다.
3. 현재 Aggregator notify request handler는 `key` field와 configured bucket을 사용합니다. 목표 contract는 `object_name` 중심으로 정리하고, body `bucket`은 제거하거나 configured `--gcs-bucket`과 일치 검증합니다.
4. Assignment는 Collector 시작 시 1회 fetch입니다. runtime 중 rebalance를 collector가 자동 반영하려면 assignment polling/versioning이 필요합니다.
5. `tokens_per_collector`는 Aggregator 내부에서 100으로 고정되어 있습니다. CLI의 `--chunk-size`와의 관계를 명확히 해야 합니다.
6. Replication에 의한 중복 이벤트를 downstream에서 어떻게 deduplicate할지 정책이 필요합니다.

---

## 14. 다음 문서화 작업

1. `architecture-draft-kr.md`에서 컴포넌트/데이터 흐름/배포 구조를 더 자세히 설명합니다.
2. 이 spec을 기준으로 outdated script 정리 또는 새로운 smoke script를 작성합니다.
3. CLI/API contract가 확정되면 README quickstart를 최신화합니다.
4. Production 배포용 Terraform/Compute Engine/GCS/IAM 문서를 별도 작성합니다.
