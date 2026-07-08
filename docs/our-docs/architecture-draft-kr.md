# Polymarket Tick Orderbook Collector — 아키텍처 초안

> 상태: Draft v0.3 — Greenfield target architecture
> 범위: 동적 마켓 유니버스, Collector 용량 기반 오케스트레이션, GCS 기반 내구적 전달, 병합/조회 경로, 비용 가드, 로컬 개발/시뮬레이션 전략.
> 전제: 기존 `src/`/`infra/` 구현과 과거 AWS/S3 설계는 이 문서의 기준이 아니다. 이 문서는 새로 만드는 v2의 기준 문서다.

---

## 1. 목표와 핵심 원칙

이 시스템은 Polymarket CLOB의 실시간 orderbook/trade 이벤트를 여러 Collector 인스턴스가 나누어 수집하고, Google Cloud Storage(GCS)에 안전하게 넘긴 뒤, 병합된 데이터셋과 Viewer/API를 제공한다.

핵심 원칙은 다음과 같다.

1. **정적 `markets.jsonl`을 기준으로 삼지 않는다.**
   - 수집 대상 마켓/토큰 목록은 Gamma API refresh 결과로 계속 갱신한다.
   - 새 마켓은 런타임에 assignment 대상으로 들어와야 한다.
   - 종료/비활성/아카이브된 마켓은 lifecycle policy에 따라 drain 또는 제거한다.

2. **Collector는 수집 실행자다.**
   - WebSocket 연결 유지
   - 이벤트 파싱/정규화
   - 메모리 버퍼 및 로컬 회전 JSONL 파일 기록
   - GCS 업로드
   - Orchestrator로 heartbeat/status/object notify 전송

3. **Orchestrator는 control plane의 판단 지점이다.**
   - 최신 마켓 유니버스 유지
   - Collector 등록/상태/용량 추적
   - Collector별 assignment 생성
   - 리밸런싱 판단
   - 비용 가드 및 긴급 중단 명령
   - autoscaling metric 노출
   - GCS object notification 수신

4. **GCS는 대용량 이벤트 데이터의 전달 지점이다.**
   - Collector는 이벤트 본문을 Orchestrator로 직접 보내지 않는다.
   - Collector는 로컬 파일을 GCS object로 업로드한다.
   - Orchestrator/Compactor는 object 이름과 generation만 보고 병합 대상을 추적한다.

5. **초기 버전에서는 replication을 기본으로 두지 않는다.**
   - 하나의 token shard는 기본적으로 하나의 Collector가 담당한다.
   - 다만 리밸런싱 중 데이터 유실을 줄이기 위해 짧은 handoff overlap은 허용할 수 있다.
   - 이 overlap은 “상시 replication”이 아니라 안전한 이관 절차다.

---

## 2. 용어 정의

### 2.1 “내구적(durable)” / “내구성(durability)”

문서에서 말하는 “durable”은 다음 의미다.

> 프로세스 종료, VM/container 재시작, 네트워크 일시 장애, Orchestrator 재시작이 발생해도 이미 파일로 기록되거나 GCS에 업로드된 데이터를 잃지 않고 다시 처리할 수 있는 성질.

이 문서에서는 가능하면 “내구적”, “내구성”이라는 한국어 표현을 사용한다. 예를 들어 “GCS 기반 durable handoff”는 “GCS를 이용해 Collector와 병합 단계 사이에서 데이터를 잃지 않도록 넘기는 내구적 전달 경로”를 뜻한다.

내구성의 범위:

| 계층 | 내구성 의미 |
|---|---|
| Collector memory buffer | 내구적이지 않음. 프로세스가 죽으면 flush 전 데이터는 손실 가능 |
| Collector local rotated JSONL | 로컬 디스크에 기록된 범위에서는 재시작 후 재업로드 가능 |
| GCS object | GCS 업로드 완료 후에는 Collector 장애와 무관하게 재처리 가능 |
| processed object manifest | 어떤 GCS object를 이미 병합했는지 재시작 후에도 판단 가능 |

### 2.2 “local rotated JSONL buffering”

`local rotated JSONL buffering`은 Collector가 WebSocket 이벤트를 바로 GCS에 한 줄씩 쓰지 않고, 먼저 로컬 디스크에 일정 단위 파일로 기록하는 행동이다.

정확한 행동은 다음과 같다.

```text
WebSocket event 수신
  → 메모리 buffer에 잠시 쌓음
  → buffer size 또는 flush interval 도달
  → 현재 열려 있는 local JSONL 파일에 append
  → rotation 조건 도달
  → 현재 파일 close
  → 닫힌 파일을 upload queue에 넣음
  → GCS upload
  → object notify
  → upload + notify 성공 후 local file 삭제 또는 보존 정책 적용
```

존재 이유:

1. **GCS 비용과 요청 수 감소**
   - 이벤트마다 GCS object를 만들면 요청 수와 비용이 커진다.
   - 일정 시간/크기 단위로 묶어서 업로드한다.

2. **네트워크/GCS 일시 장애 대응**
   - GCS 업로드가 실패해도 닫힌 local file을 보존하고 나중에 재시도할 수 있다.

3. **Collector 재시작 복구**
   - Collector 시작 시 local spool directory를 스캔해 업로드되지 않은 파일을 다시 업로드/notify할 수 있다.

4. **병합 단위 안정화**
   - Compactor는 닫힌 immutable 파일만 읽는다.
   - 쓰는 중인 파일을 읽는 문제를 피한다.

5. **WebSocket 수집과 GCS 업로드 분리**
   - WebSocket 수신 루프가 GCS latency 때문에 막히지 않는다.

주의:

- 메모리 buffer에만 있는 데이터는 아직 내구적이지 않다.
- local file에 flush된 이후부터는 로컬 디스크 수준의 내구성을 가진다.
- GCS upload가 완료된 이후부터는 Collector 인스턴스 장애와 무관하게 재처리 가능하다.

### 2.3 “rebalance decision”

`rebalance decision`은 Orchestrator가 “어떤 token/market shard를 어떤 Collector에게 맡길지”를 다시 계산하고, 필요하면 assignment version을 새로 발행하는 판단이다.

v1 리밸런싱은 구조적 변화에 한정한다.

| 원인 | 예시 | 행동 |
|---|---|---|
| 새 마켓 발견 | Gamma refresh에서 새 token 발견 | 새 shard를 여유 Collector에 배정 |
| 마켓 종료 | market closed/archived | 새 assignment에서 제거 또는 drain |
| 새 Collector 등록 | autoscaler가 Collector 추가 | 기존 healthy shard를 뺏기보다 신규/미할당 shard를 우선 배정 |
| Collector 장애 | heartbeat timeout | 해당 shard를 다른 Collector로 이동 |

리밸런싱하지 않는 상황:

| 상황 | v1 행동 |
|---|---|
| 특정 market 거래량 폭증 | 기존 구독 market을 다른 Collector로 넘기지 않음. 관측/알림만 수행 |
| upload backlog 증가 | 기존 구독 market을 자동 이동하지 않음. 알림, 신규 assignment 제한, 운영자 판단 |
| 이벤트량 기반 hot shard 탐지 | v1 범위 제외. 실제 운영에서 필요성이 확인되면 별도 설계 |
| 비용 긴급 중단 | 리밸런싱하지 않고 모든 Collector에 stop/pause 명령 |

중요한 원칙:

- Orchestrator는 처음부터 Collector capacity를 넘겨서 assignment하면 안 된다.
- `Collector overload`는 “과할당해도 된다”는 뜻이 아니다.
- v1에서 overload는 리밸런싱 트리거가 아니라 관측/알림/보호 상태다.
- 특정 market이 갑자기 거래량이 폭증하더라도 기존에 구독 중인 market을 다른 Collector로 넘기지 않는다.
- 나중에 실제 운영에서 필요성이 확인되면 hot-market 이동 정책을 별도 버전에서 설계한다.
- v1에서 overload로 기록할 수 있는 신호는 다음과 같다.
  - WebSocket reconnect 반복
  - GCS upload backlog 증가
  - Collector CPU/메모리/네트워크 저하
  - 실제 subscribed count가 planned assignment와 다름

### 2.4 “GCS object notification intake”

`GCS object notification intake`는 Collector가 “GCS에 파일 업로드가 끝났다”고 Orchestrator에게 알려주는 control-plane 수신 경로다.

이 경로는 이벤트 본문을 받지 않는다. 다음 같은 metadata만 받는다.

```json
{
  "collector_id": "collector-a",
  "assignment_version": 42,
  "bucket": "polymarket-orderbook-prod",
  "object_name": "raw/orderbook/dt=2026-07-06/hour=10/collector_id=collector-a/part-000001.jsonl",
  "generation": "1710000000000000",
  "line_count": 12000,
  "checksum_crc32c": "optional"
}
```

Orchestrator가 하는 일:

1. Collector가 등록된 Collector인지 확인한다.
2. object path가 허용된 prefix인지 확인한다.
3. `collector_id`, `assignment_version`, `bucket`, `object_name`, `generation` 조합으로 중복 notify인지 확인한다.
4. 병합 대상 queue/manifest에 추가한다.
5. Compactor가 처리할 수 있게 상태를 저장한다.
6. Collector에게 “notify 수신 완료” 응답을 준다.

하지 않는 일:

- 대용량 JSONL body 수신
- 파일 내용 병합
- 이벤트별 dedup 판단

---

## 3. 비목표

초기 버전에서 제외한다.

1. Polymarket 주문 제출/취소/서명/trading bot 기능
2. 완전한 exactly-once event-level 보장
3. 복잡한 data warehouse 최종 스키마 확정
4. 실시간 전략/알파 계산
5. multi-cloud 추상화
6. Viewer 인증/권한 시스템
7. 상시 replication 기반 고가용 수집

초기 목표는 **단일 replica 기반 at-least-once 수집 + object-level idempotent merge**다.

---

## 4. Google Cloud 기준

Target cloud는 Google Cloud다.

| 영역 | 기준 |
|---|---|
| Object storage | Google Cloud Storage, GCS |
| 인증 | Google Cloud service account / Application Default Credentials |
| 권한 | Google Cloud IAM |
| 비용 모니터링 | Orchestrator 추정 비용 + Google Cloud Billing/Budget 보조 |
| 알림 | Discord webhook |
| 로컬 GCS 대체 | fake-gcs-server |
| 작은 실환경 검증 | 실제 GCS bucket + 강한 limit + 짧은 duration |

용어 설명:

| 용어 | 뜻 |
|---|---|
| GKE | Google Kubernetes Engine. Google Cloud의 관리형 Kubernetes. 여러 containerized Collector를 배포/스케일링하기 좋다. |
| MIG | Managed Instance Group. 동일 VM 템플릿으로 여러 Compute Engine VM을 운영하고 autoscaling하는 Google Cloud 기능. 고정 VM/egress 제어가 필요할 때 후보가 된다. |
| Cloud Run | container를 serverless 방식으로 실행하는 Google Cloud 서비스. 장시간 WebSocket 수집에는 제약을 검토해야 한다. |

### 4.1 GKE / MIG / Cloud Run이 왜 필요한가

이 시스템에서 scale-out 대상은 GCS가 아니라 **Collector가 실행되는 compute**다. 마켓 수가 늘어나거나 Collector capacity가 부족해지면 Collector 프로세스를 더 띄워야 한다. GKE, MIG, Cloud Run은 이 Collector 프로세스를 Google Cloud에서 어떻게 실행하고 늘릴지에 대한 후보들이다.

| 선택지 | 정확한 의미 | 왜 필요한가 | 장점 | 주의점 |
|---|---|---|---|---|
| GKE | Google Kubernetes Engine. Kubernetes cluster를 Google이 관리해주는 서비스 | Collector를 container로 여러 개 띄우고, rolling update, health check, autoscaling을 체계적으로 운영하기 위해 | 장시간 실행 workload, 여러 서비스 조합, 세밀한 network/secret/config 관리에 강함 | Kubernetes 운영 복잡도가 있음. 작은 초기 실험에는 과할 수 있음 |
| MIG | Managed Instance Group. 동일 VM template을 기반으로 Compute Engine VM 여러 대를 관리하는 기능 | Collector를 VM 단위로 늘리고 줄이기 위해. 고정 VM, 고정 egress, 긴 실행시간이 중요할 때 후보 | 구조가 단순하고 VM 단위 디버깅이 쉬움. 장시간 WebSocket에 안정적 | container orchestration 편의성은 GKE보다 낮음. 배포/롤링 업데이트를 직접 설계해야 함 |
| Cloud Run | container를 요청 기반/serverless 형태로 실행하는 서비스 | Orchestrator/API/짧은 worker처럼 HTTP 중심 서비스는 빠르게 배포하기 위해 | 운영 부담이 낮고 배포가 쉬움. 작은 API에는 좋음 | 장시간 WebSocket Collector에는 timeout, instance lifecycle, egress/IP 안정성 제약을 반드시 검토해야 함 |

초기 판단:

1. 로컬 개발과 small real GCS smoke 단계에서는 셋 다 필요 없다.
2. Orchestrator API는 Cloud Run도 후보가 될 수 있다.
3. Collector는 장시간 WebSocket 연결을 유지해야 하므로 GKE 또는 MIG를 우선 검토한다.
4. 가장 먼저 하나만 선택한다. GKE/MIG/Cloud Run을 동시에 지원하려고 하지 않는다.

---

## 5. 전체 구조

```text
┌────────────────────┐
│ Polymarket Gamma   │
│ Markets API        │
└─────────┬──────────┘
          │ small live refresh / production refresh
          ▼
┌─────────────────────────────────────────────────────────────┐
│ Orchestrator                                                │
│ - Market Universe Manager                                   │
│ - Collector Registry                                        │
│ - Assignment Planner                                        │
│ - Rebalance / Handoff Planner                               │
│ - Cost Guard                                                │
│ - GCS Object Notification Intake                            │
│ - Metrics / Discord Alert                                   │
└──────┬──────────────────────────────┬───────────────────────┘
       │ assignment / stop command     │ object notification
       │ heartbeat / status            │
       ▼                              │
┌──────────────────────┐              │      ┌──────────────────────────┐
│ Collector Fleet      │──────────────┼─────►│ GCS Raw Objects          │
│ - Collector 1        │ upload       │      │ rotated JSONL files      │
│ - Collector 2        │              │      └───────────┬──────────────┘
│ - Collector N        │              │                  │ list/get
└─────────┬────────────┘              │                  ▼
          │ WebSocket subscribe       │      ┌──────────────────────────┐
          ▼                           └─────►│ Compactor / Merger       │
┌──────────────────────┐                     │ - processed manifest     │
│ Polymarket CLOB      │                     │ - merged JSONL output    │
│ WebSocket            │                     └───────────┬──────────────┘
└──────────────────────┘                                 │ read
                                                         ▼
                                            ┌──────────────────────────┐
                                            │ Viewer / API             │
                                            └───────────┬──────────────┘
                                                        │ tx receipt/log
                                                        ▼
                                            ┌──────────────────────────┐
                                            │ Polygon RPC              │
                                            └──────────────────────────┘
```

---

## 6. 컴포넌트

| 컴포넌트 | 책임 | 상태 저장 |
|---|---|---|
| Market Universe Manager | Gamma API refresh, market lifecycle 판단, universe version 발행 | GCS checkpoint 또는 control state store |
| Orchestrator API | register, assignment, heartbeat, status, notify, stop command 제공 | Collector registry, assignment manifest |
| Assignment Planner | capacity를 넘지 않도록 token/market shard 배정 | assignment plan version |
| Rebalance / Handoff Planner | shard 이동 시 유실을 줄이는 이관 절차 생성 | handoff state |
| Cost Guard | GCS 비용 추정, Discord 경고, hard stop 명령 | budget state |
| Collector | WebSocket 수집, local rotated JSONL 기록, GCS upload, status report | local spool + GCS object |
| GCS Raw Object Store | 닫힌 JSONL 파일 저장 | GCS bucket |
| Compactor / Merger | GCS object를 읽어 병합 output 생성, processed manifest 기록 | processed manifest + merged output |
| Viewer / API | 병합 output을 읽어 market/book/trade 조회 제공 | optional local/read-side cache |
| On-chain Detail Resolver | tx hash 기반 Polygon receipt/log 조회 | optional cache |

### 6.1 구현 언어 결정

현재 greenfield 구현 방향은 다음과 같이 둔다.

| 영역 | 언어 | 이유 |
|---|---|---|
| Orchestrator / control plane | Python | assignment planning, cost guard, Discord 알림, 작은 live Gamma refresh를 빠르게 반복하기 위해 |
| Collector / data plane | Rust | WebSocket 수집, 로컬 파일 회전, GCS upload처럼 장시간 실행과 성능/안정성이 중요한 경로이기 때문 |
| Shared contract | Rust + Python mirror | Collector와 Orchestrator가 같은 JSON 계약을 쓰도록 양쪽에 타입을 둔다 |

주의:

- Python Orchestrator는 control-plane 로직을 빠르게 완성하기 위한 선택이다.
- Collector는 production data path이므로 Rust로 유지한다.
- 두 언어 사이의 불일치를 막기 위해 22.1 단계에서 계약 타입을 먼저 고정한다.

---

## 7. 개발/시뮬레이션 전략

처음부터 full Google Cloud production 환경에서 개발하지 않는다. 단계적으로 simulation fidelity를 올린다.

### 7.1 단계 0 — 순수 로컬 단위 테스트

목표:

- domain type과 planner 로직을 네트워크 없이 검증한다.

구성:

```text
in-memory MarketUniverseSnapshot
in-memory CollectorRegistry
in-memory AssignmentPlanner
in-memory ProcessedObjectManifest
```

검증:

- 새 market 추가 시 assignment version 증가
- Collector capacity 초과 방지
- hard budget stop 시 assignment가 중단 상태로 변함
- processed manifest가 중복 object를 skip

### 7.2 단계 1 — 작은 live Gamma refresh

fake Gamma API 대신 작은 live Gamma refresh를 사용한다.

구성:

```text
live Gamma API
  └─ limit markets/tokens very small
Orchestrator local process
Collector는 아직 mock status만 전송
```

안전장치:

- market limit
- token limit
- refresh interval 길게 설정
- request timeout/retry 제한

목표:

- Gamma API shape 변화에 빨리 노출된다.
- 실제 market lifecycle field를 보고 policy를 조정한다.

### 7.3 단계 2 — fake-gcs-server 기반 로컬 통합

구성:

```text
small live Gamma refresh
fake-gcs-server
Orchestrator
1-2 local Collectors
Compactor
```

목표:

- GCS upload/list/get/delete와 유사한 흐름 검증
- object notify 유실 시 polling으로 복구되는지 검증
- local rotated JSONL buffering 검증

### 7.4 단계 3 — 작은 실제 GCS smoke

구성:

```text
real GCS bucket
strict token limit
short duration
1 Collector
Orchestrator local or small cloud instance
Compactor local or small cloud instance
Discord webhook enabled
hard budget threshold very low
```

목표:

- 실제 Google credential/IAM/GCS latency 검증
- 비용 추정/Discord 경고/hard stop 동작 검증
- fake-gcs-server와 실제 GCS 차이 확인

### 7.5 단계 4 — 다중 Collector simulation

구성:

```text
small live Gamma refresh
fake-gcs-server or real GCS small bucket
3+ Collectors
capacity limits intentionally low
```

목표:

- capacity-aware assignment 검증
- 새 Collector 추가 후 신규/미할당 shard 배정 검증
- make-before-break handoff 검증
- emergency stop broadcast 검증

### 7.6 단계 5 — Google Cloud pilot

구성:

```text
GCS bucket
Orchestrator service
Collector compute fleet 후보 1개 선택
Compactor worker
Viewer/API
```

처음에는 GKE/MIG/Cloud Run 중 하나만 선택한다. 모든 배포 방식을 동시에 지원하지 않는다.

---

## 8. 제어 평면 계약

### 8.1 흐름

```text
Gamma API ──refresh────► Market Universe Manager
Market Universe Manager ──universe_version────► Assignment Planner

Collector ──POST /collectors/register────────► Orchestrator
Collector ──GET  /collectors/{id}/assignment──► Orchestrator
Collector ──POST /collectors/{id}/heartbeat───► Orchestrator
Collector ──POST /collectors/{id}/status──────► Orchestrator
Collector ──POST /objects/notify──────────────► Orchestrator

Orchestrator ──Discord webhook────────────────► operator
Orchestrator ──metrics/status─────────────────► autoscaler / dashboard
```

### 8.2 Register

Collector는 시작 시 처리 가능 용량을 선언한다.

```json
{
  "collector_name": "collector-a",
  "capacity": {
    "max_market_subscriptions": 300,
    "max_token_subscriptions": 600,
    "max_ws_connections": 6,
    "max_events_per_sec": 3000,
    "max_upload_backlog_files": 50
  },
  "labels": {
    "zone": "local-dev",
    "egress_pool": "default"
  }
}
```

### 8.3 Assignment

```json
{
  "assignment_version": 42,
  "universe_version": 105,
  "control_state": "RUNNING",
  "collector_id": "collector-a",
  "token_ids": ["token-1", "token-2"],
  "market_ids": ["market-1"],
  "limits": {
    "max_ws_connections": 6,
    "max_tokens_per_ws_connection": 100
  },
  "handoff": {
    "mode": "NONE",
    "drain_removed_tokens_after_ms": null
  }
}
```

`control_state` 값:

| 값 | 의미 |
|---|---|
| `RUNNING` | 정상 수집 |
| `PAUSED_BY_OPERATOR` | 운영자 수동 중단 |
| `PAUSED_BY_BUDGET_WARNING` | 비용 경고 후 신규 assignment 제한 가능 |
| `EMERGENCY_STOP_BY_BUDGET` | hard budget 초과. Collector는 수집 중단 |

### 8.4 Heartbeat

Heartbeat는 생존 확인에 집중한다.

```json
{
  "collector_id": "collector-a",
  "assignment_version": 42,
  "status": "ALIVE"
}
```

### 8.5 Status

Status는 실제 구독 상태와 부하를 보고한다.

```json
{
  "collector_id": "collector-a",
  "assignment_version": 42,
  "assigned_market_count": 290,
  "assigned_token_count": 580,
  "subscribed_market_count": 290,
  "subscribed_token_count": 580,
  "active_ws_connections": 6,
  "events_per_sec": 1200.5,
  "reconnects_last_minute": 0,
  "upload_backlog_files": 2,
  "local_spool_bytes": 104857600,
  "last_successful_upload_at_ms": 1781051970000
}
```

Orchestrator는 `assigned_*`와 `subscribed_*`가 계속 다르면 assignment 적용 실패로 본다.

### 8.6 Object Notify(객체 알림)

```json
{
  "collector_id": "collector-a",
  "assignment_version": 42,
  "bucket": "polymarket-orderbook-dev",
  "object_name": "raw/orderbook/dt=2026-07-06/hour=10/collector_id=collector-a/part-000001.jsonl",
  "generation": "1710000000000000",
  "line_count": 12000,
  "first_event_ts_ms": 1781051900000,
  "last_event_ts_ms": 1781051960000,
  "checksum_crc32c": "optional"
}
```

Orchestrator는 notify를 받은 뒤 다음을 수행한다.

1. 등록된 Collector인지 확인
2. object path가 허용된 prefix인지 확인
3. `bucket + object_name + generation` 중복 여부 확인
4. object metadata를 pending object manifest에 기록
5. Compactor가 처리할 수 있게 queue 상태로 둠
6. Collector에게 ack 반환

---

## 9. 비용 가드와 긴급 중단

GCS 비용이 일정 수준을 넘으면 Discord로 알리고, 더 높은 threshold를 넘으면 모든 Collector를 중단해야 한다.

중요한 제약:

- Google Cloud Billing의 실제 비용 데이터는 실시간이 아닐 수 있다.
- 따라서 Orchestrator는 자체 추정 비용을 먼저 계산하고, Cloud Billing/Budget은 보조 검증으로 사용한다.

### 9.1 비용 추정 입력

Orchestrator/Compactor는 다음 값을 누적한다.

| 항목 | 설명 |
|---|---|
| uploaded_bytes | Collector가 GCS에 업로드한 bytes |
| stored_object_bytes | 현재 보존 중인 object bytes 추정 |
| object_create_count | GCS object create 요청 수 |
| object_list_count | prefix polling/list 요청 수 |
| object_get_count | Compactor get 요청 수 |
| object_delete_count | retention/delete 요청 수 |

정확한 과금식은 GCS storage class/region/operation price에 따라 달라지므로, 설정 파일에 단가를 넣어 추정한다.

### 9.2 Budget Policy

```text
BudgetPolicy
  warning_threshold_usd: 10.0
  hard_stop_threshold_usd: 20.0
  discord_webhook_url: secret
  check_interval_secs: 60
  hard_stop_mode: STOP_WEBSOCKET_KEEP_LOCAL_SPOOL
```

### 9.3 Discord 경고

warning threshold 도달 시:

1. Discord webhook으로 알림 전송
2. Orchestrator state를 `PAUSED_BY_BUDGET_WARNING`으로 둘지, 경고만 할지 설정으로 결정
3. Dashboard/metrics에 현재 추정 비용 노출

알림 예시:

```text
[Polymarket Collector Budget Warning]
Estimated GCS cost reached 10.23 USD.
Collectors running: 3
Uploaded bytes: 128 GiB
Object count: 15420
Hard stop threshold: 20.00 USD
```

### 9.4 긴급 중단

hard stop threshold 도달 시:

1. Orchestrator global state를 `EMERGENCY_STOP_BY_BUDGET`으로 변경
2. 모든 assignment response에 stop command 포함
3. heartbeat/status response에도 stop command 포함
4. Collector는 새 WebSocket 연결을 열지 않음
5. Collector는 기존 WebSocket을 닫음
6. 메모리 buffer는 local spool에 flush
7. 설정에 따라 GCS upload는 중단하거나 제한
8. Discord webhook으로 긴급 중단 알림 전송

권장 hard stop mode:

```text
STOP_WEBSOCKET_KEEP_LOCAL_SPOOL
```

의미:

- WebSocket 수집은 즉시 멈춘다.
- memory buffer는 local file로 flush한다.
- 추가 GCS upload는 멈춰 비용 증가를 막는다.
- 나중에 운영자가 예산을 확인한 뒤 local spool upload를 재개할 수 있다.

주의:

- hard stop은 데이터 연속성보다 비용 보호를 우선한다.
- hard stop 중 local spool이 있는 VM/container가 사라지면 해당 spool은 손실될 수 있다.
- 비용 보호와 데이터 보존 사이의 우선순위는 운영 정책으로 명확히 정해야 한다.

---

## 10. 마켓 유니버스와 생명주기 정책

Market Universe Manager는 Gamma API를 주기적으로 조회해 수집 대상 universe를 만든다.

```text
refresh tick
  ├─ Gamma API page fetch
  ├─ market metadata normalize
  ├─ CLOB token id 추출
  ├─ lifecycle state 계산
  ├─ 이전 universe와 diff
  ├─ universe_version 증가
  └─ assignment 재계산 요청
```

### 10.1 Universe Snapshot

```text
MarketUniverseSnapshot
  version: u64
  generated_at_ms: i64
  markets:
    market_id:
      slug: string
      question: string
      active: bool
      closed: bool
      archived: bool
      accepting_orders: bool
      enable_order_book: bool
      token_ids: [string]
      lifecycle_state: DISCOVERED | ACTIVE | DRAINING | CLOSED | ARCHIVED | EXCLUDED
```

### 10.2 생명주기 정책 상세

| 상태 | 진입 조건 | Collector assignment 행동 | 설명 |
|---|---|---|---|
| `DISCOVERED` | Gamma에서 처음 발견 | 바로 assign하지 않고 1회 검증 대기 가능 | token id 누락/불완전 metadata 방지 |
| `ACTIVE` | active=true, closed=false, archived=false, enable_order_book=true | assignment 대상 | 정상 수집 대상 |
| `DRAINING` | accepting_orders=false 또는 closed 전환 직후 | 새 Collector에는 배정하지 않거나 drain window 동안 기존 Collector 유지 | 종료 직전 이벤트를 조금 더 받을지 결정하는 완충 상태 |
| `CLOSED` | closed=true | assignment 제거 | 더 이상 신규 수집하지 않음 |
| `ARCHIVED` | archived=true | universe active set에서 제거 | 장기 조회 metadata만 남길 수 있음 |
| `EXCLUDED` | 정책상 제외 | assignment하지 않음 | 예: token id 없음, order book 비활성, blacklist |

### 10.3 Drain Window

마켓이 `ACTIVE → CLOSED`로 바뀌는 순간 바로 구독을 끊으면 마지막 이벤트를 놓칠 수 있다. 따라서 짧은 drain window를 둘 수 있다.

```text
closed detected at T
  → lifecycle_state = DRAINING
  → 기존 Collector는 drain_window_secs 동안 유지
  → 새 Collector에는 배정하지 않음
  → drain 종료 후 assignment에서 제거
```

초기 권장값:

| 값 | 권장 |
|---|---:|
| `new_market_confirm_refreshes` | 1-2회 |
| `closed_market_drain_secs` | 60-300초 |
| `archived_remove_immediately` | true |

---

## 11. 할당, 용량, 리밸런싱

### 11.1 기본 원칙

1. Orchestrator는 Collector declared capacity를 넘겨서 assignment하면 안 된다.
2. 초기 버전은 replication 없이 `replication_factor = 1`로 설계한다.
3. 리밸런싱은 가능한 적게 수행한다.
4. 정상 동작 중인 Collector를 불필요하게 흔들지 않는다.
5. v1에서는 거래량 폭증만으로 기존 healthy shard를 다른 Collector로 이동하지 않는다.
6. shard 이동은 Collector 장애, 마켓 lifecycle 제거, 운영자 수동 지시처럼 구조적으로 필요한 경우로 제한한다.
7. 계획된 shard 이동이 불가피하면 유실을 줄이는 handoff 절차를 사용한다.

### 11.2 Planner 입력

```text
AssignmentPlannerInputs
  market_universe_snapshot
  collector_registry
  collector_declared_capacity
  collector_last_status
  cost_guard_state
  shard_weight_estimates
```

### 11.3 Planner 출력

```text
AssignmentPlan
  version: u64
  universe_version: u64
  control_state: RUNNING | PAUSED | EMERGENCY_STOP
  collectors:
    collector_id:
      market_ids: [...]
      token_ids: [...]
      expected_ws_connections: usize
      capacity_utilization: f64
      handoff_actions: [...]
```

### 11.4 Overload의 의미

`Collector overload`는 Orchestrator가 처음부터 과도한 마켓을 몰아줘도 된다는 뜻이 아니다.

정상 planner는 다음을 지켜야 한다.

```text
assigned_token_count <= max_token_subscriptions
assigned_market_count <= max_market_subscriptions
expected_ws_connections <= max_ws_connections
```

그런데 count 기준으로는 정상이어도 실제 운영 중 아래 문제가 생길 수 있다.

- 특정 market 이벤트 폭증
- upload backlog 증가
- reconnect 반복
- Collector CPU/network 문제
- subscribed count가 assignment보다 적음
- local spool이 계속 증가

이런 상황을 status report로 감지하는 것이 overload handling이다. 단, v1에서는 이 신호만으로 기존 구독 market을 다른 Collector로 넘기지 않는다. 우선순위는 관측, Discord/metric 알림, 신규 assignment 제한, 운영자 판단이다. hot-market 이관은 실제 운영에서 필요성이 확인된 뒤 별도 설계로 추가한다.

### 11.5 리밸런싱 시 유실 방지

잘 돌아가는 Collector에서 shard를 끊고 다른 Collector에 넘기는 동안 유실이 생길 수 있다. 따라서 v1은 정상 구독 중인 shard 이동을 최대한 피한다. 그래도 장애 복구나 운영자 수동 이동처럼 계획된 shard 이동이 불가피하면 `make-before-break` 방식을 사용한다.

```text
1. 새 assignment version 생성
2. 이동 대상 token을 새 Collector에도 임시로 할당
3. 새 Collector가 subscribed 상태를 status로 확인
4. 짧은 overlap window 유지
5. 기존 Collector에 해당 token drain 명령
6. 기존 Collector가 unsubscribe 완료 보고
7. assignment finalize
```

장점:

- 구독 공백을 줄인다.

단점:

- overlap 동안 중복 이벤트가 생길 수 있다.

초기 정책:

- 상시 replication은 사용하지 않는다.
- 장애 복구/운영자 수동 이동 등 불가피한 handoff에서만 짧은 overlap을 허용한다.
- Compactor는 object-level 중복은 막고, event-level 중복 제거는 이후 단계로 둔다.

유실보다 중복이 더 싫은 경우:

- overlap 없이 `break-before-make`를 사용할 수 있다.
- 하지만 이 경우 WebSocket 구독 공백으로 이벤트 유실 가능성이 커진다.

### 11.6 리밸런싱 발생 조건

| 조건 | 리밸런싱 여부 |
|---|---|
| 새 market/token 발견 | 필요 |
| market closed/archived | drain 후 제거 |
| 새 Collector 등록 | 기존 healthy shard 이동 없이 신규/미할당 shard 우선 배정 |
| Collector heartbeat timeout | 즉시 필요 |
| Collector status overload | 리밸런싱하지 않음. 관측/알림/신규 assignment 제한/운영자 판단 |
| 비용 warning | 설정에 따라 신규 assignment 제한 가능 |
| 비용 hard stop | 리밸런싱이 아니라 전체 stop |
| 단순 refresh에서 변화 없음 | 리밸런싱하지 않음 |

---

## 12. 데이터 평면과 로컬 회전 JSONL

### 12.1 Collector runtime

```text
Collector
  ├─ register capacity
  ├─ fetch/watch assignment
  ├─ open WebSocket workers
  ├─ subscribe token chunks
  ├─ receive events
  ├─ normalize events
  ├─ memory buffer append
  ├─ flush memory buffer to current local JSONL
  ├─ rotate local JSONL by time/size
  ├─ upload closed file to GCS
  ├─ notify object metadata
  ├─ delete or retain local file by policy
  └─ report status periodically
```

### 12.2 Rotation 조건

초기에는 시간 기준 rotation을 기본으로 한다.

| 조건 | 예시 |
|---|---:|
| `rotate_interval_secs` | 300초 |
| `max_file_bytes` | optional, 예: 64 MiB |
| graceful shutdown | 현재 파일 close 후 spool에 보존 |

### 12.3 Local spool directory

```text
/data/spool/orderbook/
  open/
    collector-a-worker-0-current.jsonl
  closed/
    dt=2026-07-06/hour=10/collector-a-worker-0-part-000001.jsonl
  uploaded_pending_notify/
    ...
  failed/
    ...
```

상태 전이:

```text
open file
  → closed file
  → uploading
  → uploaded_pending_notify
  → notified
  → delete or archive local copy
```

---

## 13. GCS 객체 배치

초기 layout은 단순하고 디버깅하기 쉬워야 한다.

```text
gs://{bucket}/raw/orderbook/
  dt=YYYY-MM-DD/
    hour=HH/
      collector_id={collector_id}/
        assignment_version={assignment_version}/
          part-{sequence}.jsonl
```

Object metadata:

| metadata | 의미 |
|---|---|
| `collector_id` | 업로드한 Collector |
| `assignment_version` | 수집 당시 assignment |
| `universe_version` | 수집 당시 market universe |
| `line_count` | JSONL line 수 |
| `first_event_ts_ms` | object 내 첫 이벤트 timestamp |
| `last_event_ts_ms` | object 내 마지막 이벤트 timestamp |
| `schema_version` | 이벤트 schema version |

---

## 14. 병합과 처리 완료 객체 목록

### 14.1 왜 처리 완료 목록이 필요한가

Compactor는 GCS prefix polling과 notify queue를 모두 사용한다. 따라서 같은 object를 여러 번 볼 수 있다.

중복 발생 예:

1. Collector가 notify를 두 번 보냄
2. notify로 한 번 보고, prefix polling으로 다시 봄
3. Compactor가 재시작하면서 이전에 본 object를 다시 list함
4. GCS delete가 실패해 병합된 raw object가 계속 남아 있음

`Processed Object Manifest`는 “이 object generation은 이미 병합 완료했다”는 영속 기록이다.

### 14.2 처리 완료 key

최소 key:

```text
bucket + object_name + generation
```

GCS object는 같은 이름으로 다시 업로드될 수 있으므로 `generation`을 포함한다.

### 14.3 처리 완료 기록

```text
ProcessedObject
  bucket: string
  object_name: string
  generation: string
  processed_at_ms: i64
  input_line_count: usize
  valid_line_count: usize
  skipped_line_count: usize
  output_path: string
  output_line_range: optional
```

### 14.4 안전한 병합 순서

단순히 “병합 후 raw object 삭제”만으로는 충분하지 않다. 삭제가 실패할 수 있고, notify가 중복될 수 있고, audit도 필요하다.

권장 순서:

```text
1. object가 manifest에 이미 있는지 확인
2. GCS object 다운로드
3. JSONL validation
4. merged output에 쓰기
5. output write 성공 확인
6. processed manifest 기록
7. raw object delete/archive는 선택적으로 수행
```

raw object 삭제 정책:

| 정책 | 설명 |
|---|---|
| keep | 디버깅과 재처리 용이. 비용 증가 |
| delete_after_manifest | output + manifest 성공 후 삭제. 비용 감소 |
| archive_after_manifest | 더 저렴한 storage class/prefix로 이동 |

사용자 의견 반영:

- 비용을 줄이려면 병합 완료 후 raw object를 삭제하는 것이 맞다.
- 단, 삭제는 `output write`와 `processed manifest`가 성공한 이후여야 한다.
- manifest는 그래도 필요하다. 삭제 실패, 중복 notify, audit, 재처리 판단 때문이다.

### 14.5 Output layout — Silver 계층은 필수인가?

초기 버전에서 별도의 “Silver Output Layout”은 필수로 두지 않는다.

초기 출력은 단순히 `merged` dataset으로 둔다.

```text
gs://{bucket}/merged/orderbook/
  dt=YYYY-MM-DD/
    hour=HH/
      part-{sequence}.jsonl
```

향후 Parquet/DuckDB/warehouse로 넘어갈 때 `silver` 같은 용어를 다시 도입할 수 있다. 지금은 raw와 merged만 구분한다.

---

## 15. 뷰어 / API

Viewer/API는 Collector나 Orchestrator에 직접 붙은 live ingest component가 아니다. 병합된 output을 읽는 read-side component다.

초기 구현:

```text
merged JSONL read
  → in-memory index build
  → market list
  → asset별 book snapshot
  → trade list
  → tx detail endpoint
```

데이터가 커지면 다음 중 하나로 확장한다.

- SQLite
- DuckDB
- Parquet
- 별도 query service

---

## 16. 온체인 상세 조회

`last_trade` 이벤트에 transaction hash가 있으면 Polygon RPC에서 transaction receipt/log를 조회한다.

```text
Viewer/API /trade-detail/{tx_hash}
  → Polygon RPC getTransactionReceipt
  → CTF Exchange OrderFilled log 찾기
  → maker/taker/amount/side/token_id 파싱
  → 응답
```

요구사항:

- RPC URL은 config로 주입한다.
- rate limit을 고려해 cache를 둔다.
- receipt/log가 없으면 명확한 empty result를 반환한다.

---

## 17. 배포 형태

### 17.1 로컬 개발

fake Gamma API는 기본 개발 경로로 쓰지 않는다. 작은 live Gamma refresh를 사용한다.

```text
local machine
  ├─ small live Gamma refresh
  ├─ fake-gcs-server
  ├─ Orchestrator
  ├─ 1-2 Collectors
  ├─ Compactor
  └─ Viewer/API
```

이후 어느 정도 완성되면 실제 GCS bucket으로 작은 smoke를 수행한다.

```text
local or small cloud run
  ├─ small live Gamma refresh
  ├─ real GCS bucket
  ├─ strict token limit
  ├─ strict duration limit
  ├─ low budget warning/hard stop threshold
  └─ Discord webhook enabled
```

### 17.2 Google Cloud 후보

```text
Google Cloud project
  ├─ GCS bucket
  │   ├─ control/
  │   ├─ raw/orderbook/
  │   ├─ merged/orderbook/
  │   └─ manifests/
  │
  ├─ Orchestrator service
  │   ├─ private API
  │   ├─ metrics endpoint
  │   └─ Discord alert integration
  │
  ├─ Collector compute fleet
  │   ├─ GKE Deployment or MIG or selected runtime
  │   └─ service account with raw object create permission
  │
  ├─ Compactor worker
  │   └─ service account with raw read + merged write permission
  │
  └─ Viewer/API
      └─ read permission on merged output
```

---

## 18. 신뢰성 모델

| 상황 | 기대 동작 |
|---|---|
| WebSocket disconnect | Collector가 backoff 후 reconnect하고 reconnect count 보고 |
| Collector crash | heartbeat timeout 후 Orchestrator가 stale 처리하고 shard 재배정 |
| Collector overload | 원래 과할당하면 안 됨. 실제 부하가 예상보다 크면 status로 감지하되 v1에서는 기존 market 이동 없이 알림/신규 assignment 제한/운영자 판단 |
| notify 유실 | GCS prefix polling으로 object 재발견 |
| GCS upload 실패 | local closed file 유지 후 retry |
| Orchestrator restart | universe/assignment/budget/manifest checkpoint로 복구 |
| Compactor restart | processed object manifest를 읽어 이미 처리한 object skip |
| budget warning | Discord 알림 |
| budget hard stop | 모든 Collector가 WebSocket 수집 중단, local spool 보존 |

보장 수준:

- 초기 목표는 at-least-once 수집이다.
- object-level 병합 중복은 manifest로 막는다.
- event-level exactly-once는 초기 목표가 아니다.
- replication은 초기에는 사용하지 않는다.

---

## 19. 관측성

| 영역 | 로그/메트릭 |
|---|---|
| Market Universe | refresh 성공/실패, market 수, token 수, diff 수, universe version |
| Assignment | assignment version, collector별 assigned/subscribed count, capacity utilization |
| Rebalance | rebalance reason, moved shard 수, handoff 상태, overlap duration. v1에서는 거래량 폭증 기반 이동은 없음 |
| Collector | ws connection 수, events/sec, reconnect 수, local spool bytes, upload backlog |
| 비용 | estimated GCS cost, uploaded bytes, object count, warning/hard stop state |
| Discord | alert send success/failure |
| GCS Upload | upload latency, object bytes, line count, notify latency |
| Compactor | discovered/processed/skipped object 수, valid/skipped line 수, lag |
| Viewer/API | dataset load time, query latency |
| On-chain | RPC latency, error rate, cache hit rate |

---

## 20. 보안

1. Google Cloud credential은 코드나 문서에 하드코딩하지 않는다.
2. Discord webhook URL은 secret으로 관리한다.
3. Service account는 컴포넌트별 최소 권한으로 분리한다.
4. Orchestrator API는 private network 또는 인증 뒤에 둔다.
5. Collector status/notify는 collector identity와 연결한다.
6. GCS object path에 secret을 넣지 않는다.
7. hard stop 명령은 인증된 Orchestrator control state에서만 나온다.

---

## 21. 열려 있는 결정

| 결정 | 후보 | 기본 방향 |
|---|---|---|
| Collector runtime | GKE / MIG / Cloud Run | 장시간 WebSocket 안정성과 egress 제어를 보고 GKE 또는 MIG 우선 검토 |
| assignment watch 방식 | polling / long-poll / SSE / gRPC stream | 처음에는 polling 또는 long-poll |
| control state 저장 | GCS manifest / SQLite / Firestore / Cloud SQL | 처음에는 GCS manifest 또는 SQLite |
| raw object 삭제 | keep / delete_after_manifest / archive | 개발 중 keep, 비용 검증 후 delete_after_manifest |
| merged format | JSONL / Parquet | JSONL 먼저 |
| event-level dedup | 없음 / optional | 초기에는 object-level idempotency만 |
| hard stop upload policy | local spool only / flush to GCS | 비용 보호 우선이면 local spool only |

---

## 22. 세분화된 개발 순서

### 22.1 계약/타입 단계

1. `MarketUniverseSnapshot` 정의
2. `MarketLifecycleState` 정의
3. `CollectorCapacity` 정의
4. `CollectorStatus` 정의
5. `AssignmentPlan` 정의
6. `ObjectNotification` 정의
7. `ProcessedObject` manifest record 정의
8. `BudgetPolicy` / `BudgetState` 정의
9. `ControlState` 정의
10. `OrderbookEvent` 정의

### 22.2 순수 로직 단계

1. Gamma market metadata를 normalized market으로 변환
2. lifecycle policy 계산
3. universe diff 계산
4. capacity-aware assignment planner 구현
5. hard stop 시 empty/stop assignment 생성
6. rebalance 필요 조건 판단
7. 장애 복구/수동 이동용 make-before-break handoff plan 생성
8. estimated GCS cost 계산
9. processed object 중복 판단

### 22.3 로컬 infrastructure abstraction 단계

1. ObjectStore trait 정의
2. in-memory object store 구현
3. fake-gcs-server adapter 구현
4. local spool manager 구현
5. processed manifest store 구현
6. Discord notifier interface와 mock 구현

### 22.4 Orchestrator 단계

1. register endpoint
2. heartbeat endpoint
3. status endpoint
4. assignment endpoint
5. object notify endpoint
6. budget state endpoint
7. metrics endpoint
8. emergency stop state propagation

### 22.5 Market refresh 단계

1. small live Gamma refresh 구현
2. limit markets/tokens 옵션
3. refresh interval 설정
4. universe checkpoint 저장
5. refresh failure backoff

### 22.6 Collector 단계

1. register + assignment fetch
2. assignment polling/long-poll
3. WebSocket worker skeleton
4. memory buffer
5. local rotated JSONL writer
6. local spool recovery
7. fake object store upload
8. real/fake GCS upload
9. object notify
10. status report
11. stop command 처리

### 22.7 Compactor 단계

1. notify queue 소비
2. GCS prefix polling
3. processed manifest check
4. JSONL validation
5. merged output write
6. manifest write
7. delete_after_manifest 정책
8. restart recovery test

### 22.8 통합 단계

1. local in-memory integration
2. fake-gcs-server integration
3. small live Gamma + fake GCS
4. small live Gamma + real GCS
5. Discord budget warning test
6. hard stop test
7. 2-3 Collector 신규/미할당 shard 배정 및 장애 복구 test
8. Viewer/API smoke

---

## 23. 세분화된 인수 시나리오

### 23.1 Market Universe

1. Gamma refresh가 market 10개를 반환하면 universe version 1 생성
2. 다음 refresh에서 token이 추가되면 universe version 2 생성
3. closed market은 `DRAINING`으로 전환
4. drain window가 지나면 assignment에서 제거
5. archived market은 active universe에서 제외

### 23.2 할당 / 용량

1. Collector capacity 100 token이면 101 token을 할당하지 않는다.
2. Collector 2대가 있으면 token shard를 capacity 내에서 분산한다.
3. 새 Collector가 register하면 기존 healthy shard를 뺏지 않고 신규/미할당 shard를 우선 배정한다.
4. Collector status의 subscribed count가 assignment와 다르면 mismatch metric이 증가한다.
5. healthy capacity가 부족하면 `collector_capacity_needed > 0`이 된다. 단, 이 값은 신규/미할당 shard를 받을 capacity가 부족하다는 뜻이며, 거래량 폭증 market을 자동 이동한다는 뜻이 아니다.

### 23.3 리밸런싱 / 이관

1. 거래량 폭증만으로 기존 healthy shard를 이동하지 않는다.
2. 새 Collector 등록 시 신규/미할당 shard를 우선 배정한다.
3. 장애 복구 또는 운영자 수동 이동처럼 shard 이동이 불가피하면 새 Collector가 먼저 subscribe한다.
4. 새 Collector가 subscribed status를 보고하기 전에는 기존 Collector를 끊지 않는다.
5. overlap window 이후 기존 Collector가 unsubscribe한다.
6. hard stop 상태에서는 rebalance가 아니라 stop command가 우선한다.

### 23.4 로컬 회전 JSONL

1. buffer size 도달 시 local JSONL에 flush된다.
2. rotation interval 도달 시 파일이 close된다.
3. GCS upload 실패 시 closed file이 local spool에 남는다.
4. Collector 재시작 시 orphan closed file을 다시 업로드한다.
5. notify 실패 시 uploaded_pending_notify 상태가 보존된다.

### 23.5 GCS 알림 / 병합기

1. object notify를 받으면 pending manifest에 기록된다.
2. 같은 notify가 두 번 와도 한 번만 처리된다.
3. notify가 유실되어도 prefix polling으로 object를 찾는다.
4. malformed JSONL line은 skipped count에 기록된다.
5. output write 성공 후 processed manifest가 기록된다.
6. manifest 기록 후 raw object delete 정책이 실행된다.
7. Compactor 재시작 후 이미 처리한 object는 skip된다.

### 23.6 비용 가드

1. estimated cost가 warning threshold를 넘으면 Discord 알림이 전송된다.
2. Discord 전송 실패 시 retry/backoff하고 metric을 남긴다.
3. hard stop threshold를 넘으면 Orchestrator state가 `EMERGENCY_STOP_BY_BUDGET`이 된다.
4. Collector는 stop command 수신 후 WebSocket을 닫는다.
5. hard stop mode가 local spool이면 추가 GCS upload를 하지 않는다.
6. operator resume 전에는 새 assignment가 발행되지 않는다.

### 23.7 실제 GCS 스모크

1. strict token limit으로 실제 GCS에 object 1개를 업로드한다.
2. notify 후 Compactor가 해당 object를 merged output에 반영한다.
3. processed manifest가 GCS 또는 persistent store에 남는다.
4. delete_after_manifest가 켜져 있으면 raw object가 삭제된다.
5. 비용 추정 metric이 증가한다.

### 23.8 뷰어 / 온체인

1. merged JSONL이 있으면 Viewer/API가 market/asset 목록을 반환한다.
2. asset별 book/trade query가 동작한다.
3. transaction hash가 있으면 on-chain resolver가 Polygon RPC를 호출한다.
4. RPC 실패 시 명확한 empty/error response를 반환하고 cache/state를 오염시키지 않는다.

---

## 24. 한 줄 요약

이 아키텍처는 Gamma API에서 최신 market universe를 계속 갱신하고, Orchestrator가 Collector 용량과 비용 한도를 고려해 assignment를 발행하며, Collector는 WebSocket 이벤트를 로컬 회전 JSONL로 안전하게 모은 뒤 GCS에 업로드하고, Compactor는 processed manifest로 중복 병합을 막으며 merged dataset을 만든다.
