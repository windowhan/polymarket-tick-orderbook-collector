# 현재 코드 구조와 흐름
이 문서는 현재 repository에 실제로 존재하는 코드 기준의 검증용 설명이다.
`architecture-draft-kr.md`의 장기 목표가 아니라, 지금 구현된 `src/`와 `tests/` 흐름만 다룬다.

## 1. 한 줄 요약
현재 코드는 end-to-end 서비스가 아니라, 그린필드 v2를 위한 **계약 타입 + 순수 Orchestrator 로직 + 테스트 기반**이다.
- Rust: 공유 계약 타입과 최소 smoke binary 중심.
- Python: market universe 생성, generic rebalance core, Polymarket assignment adapter 중심.
- 아직 없음: 실제 Gamma HTTP 호출, WebSocket collector runtime, API server, GCS, Discord, Terraform 배포.

## 2. 현재 디렉터리 책임
```text
src/
  common/contracts.rs          # Rust 공유 계약 타입
  node/mod.rs                  # Rust Collector runtime 자리표시자
  lib.rs                       # Rust library entry + library_version()
  main.rs                      # smoke binary
  orchestrator/contracts.py    # Python 공유 계약 dataclass/enum
  orchestrator/market_manager/ # raw market -> MarketUniverseSnapshot
  orchestrator/rebalance_core/ # 도메인 중립 task/node assignment core
  orchestrator/assigner/       # Polymarket 계약 <-> rebalance_core adapter/wrapper
  orchestrator/autoscale/      # 자리표시자
  orchestrator/compactor/      # 자리표시자
  orchestrator/viewer/         # 자리표시자
tests/                         # Python unit tests
```

## 3. Rust 코드 현황

### 3.1 `src/common/contracts.rs`
Rust Collector와 Python Orchestrator가 공유해야 할 JSON 계약을 고정한다.
주요 타입:
- lifecycle/control/event enum: `MarketLifecycleState`, `ControlState`, `OrderbookEventType`, `HandoffMode`
- market/universe: `MarketInfo`, `MarketUniverseSnapshot`
- collector/assignment: `CollectorCapacity`, `CollectorStatus`, `AssignmentLimits`, `HandoffAction`, `CollectorAssignment`, `AssignmentPlan`
- storage/budget/event: `ObjectNotification`, `ProcessedObject`, `BudgetPolicy`, `BudgetState`, `OrderbookEvent`
현재 이 파일은 serde 직렬화와 helper 메서드 검증이 목적이다.
실제 WebSocket 수집, 파일 buffering, GCS 업로드는 구현되어 있지 않다.

### 3.2 `src/lib.rs`, `src/main.rs`, `src/node/mod.rs`
- `lib.rs`: `common`, `node` export와 `library_version()` 제공.
- `main.rs`: `polymarket-collector v0.1.0 greenfield contracts`를 출력하는 smoke binary.
- `node/mod.rs`: 앞으로 Rust Collector runtime이 들어갈 자리표시자.

## 4. Python 계약 계층
`src/orchestrator/contracts.py`는 Python Orchestrator에서 쓰는 dataclass/enum 계약이다.
Rust 계약과 같은 의미를 유지하는 것이 목적이다.
중요 helper:
- `CollectorCapacity.fits_subscription_counts()`
  - planned market/token/ws count가 declared capacity 안인지 확인한다.
- `CollectorStatus.assignment_matches_subscription()`
  - assigned count와 subscribed count가 일치하는지 확인한다.
- `ObjectNotification.idempotency_key()` / `ProcessedObject.idempotency_key()`
  - object generation 단위 idempotency key를 만든다.
- `BudgetState.warning_exceeded()` / `hard_stop_exceeded()`
  - 비용 임계값 도달 여부를 계산한다.
이 파일도 endpoint, persistence, webhook 호출을 수행하지 않는다.

## 5. Market Universe 생성 흐름
구현 위치: `src/orchestrator/market_manager/`
현재 이 계층은 Gamma API를 직접 호출하지 않는다.
호출자가 넘긴 raw market payload iterable을 순수 함수로 처리한다.
```text
raw market payloads
  -> build_market_universe_snapshot()
    -> extract_raw_market_flags()
    -> compute_lifecycle_state()
    -> should_include_in_active_universe()
    -> normalize_gamma_market()
    -> build_universe_diff()
  -> UniverseBuildResult(snapshot, diff, memory)
```

### 5.1 `normalizer.py`
역할:
- market id 후보 추출: `id`, `market_id`, `conditionId`, `condition_id`
- token id 추출: `clobTokenIds`, `clob_token_ids`, `tokens[].token_id`, `tokens[].tokenId`, `tokens[].id`
- camelCase/snake_case field 일부 허용
- `RawMarketFlags`와 `MarketInfo` 생성
현재 동작:
- market id가 없으면 `None` 반환.
- token id 중복과 빈 값 제거.
- orderbook flag가 없으면 `False`로 간주.

### 5.2 `policy.py`, `lifecycle.py`
`LifecyclePolicy`는 신규 market confirm 횟수, closed drain 시간, archived 제거 정책, token/orderbook 제외 정책을 담는다.
`LifecycleMemory`는 refresh 사이의 관측 횟수와 DRAINING 시작 시각을 담는다.
`compute_lifecycle_state()` 현재 판단:
- archived -> `ARCHIVED`
- closed 또는 accepting_orders=false -> `DRAINING` 또는 `CLOSED`
- token id 없음 -> 정책상 `EXCLUDED`
- orderbook disabled -> 정책상 `EXCLUDED`
- active=false -> `EXCLUDED`
- 신규 open market -> `DISCOVERED` 후 confirm 정책에 따라 `ACTIVE`
- 기존 open market -> `ACTIVE`

### 5.3 `diff.py`, `builder.py`
`build_universe_diff()`는 이전/현재 snapshot에서 다음 변화를 계산한다.
- added market
- removed market
- lifecycle changed market
- token changed market
`build_market_universe_snapshot()`은 raw payload를 snapshot/diff/memory로 묶는다.
현재 version 규칙:
- 첫 snapshot은 version 1.
- diff가 있으면 이전 version + 1.
- diff가 없으면 이전 version 유지.
- market map은 market id 기준 정렬.

## 6. Generic Rebalance Core
구현 위치: `src/orchestrator/rebalance_core/`
이 패키지는 Polymarket을 모르는 재사용 가능한 task/node assignment core다.
core 내부에는 특정 시장, GCS, Discord, Terraform 같은 도메인 지식을 넣지 않는 방향이다.

### 6.1 `models.py`
주요 타입:
- `ResourceVector`: generic resource vector. `plus`, `fits_within`, `utilization_against` 제공.
- `TaskSpec`: 배정 대상 task.
- `NodeSpec`: task를 받을 node와 capacity.
- `NodeRuntimeHint`: application/adapter가 계산한 runtime hint.
- `PlannerContext`: runtime hint와 metadata 묶음.
- `Assignment`: task_id -> node_id 관계.
- `AssignmentPlanCore`: generic planner 결과.

### 6.2 `adapter.py`
core가 도메인 제약을 질의하는 protocol이다.
- `ConstraintDecision`: allowed/code/reason/metadata.
- `AssignmentAdapter.can_assign()`: task-node pair 허용 여부.
- `AssignmentAdapter.score_assignment()`: safety/stability 이후 candidate 선호 점수.
- `DefaultAssignmentAdapter`: 모든 pair 허용, score 0.0.
중요: score는 최적화 목표가 아니라 후보 선호도다.
capacity, availability, callback reject, previous assignment stability가 먼저다.

### 6.3 `planner.py`
`build_assignment_plan()`이 generic assignment를 만든다.
```text
tasks/nodes/previous_assignments/context
  -> task_id/node_id 기준 정렬
  -> 유효한 previous assignment 먼저 유지
  -> 미배정 task를 stable order로 순회
  -> node 후보 검사
     - node.available
     - runtime hint available
     - 신규 배정이면 accepts_new_tasks
     - capacity fits
     - adapter.can_assign
     - adapter.score_assignment
  -> score desc, utilization asc, node_id asc로 후보 선택
  -> 후보 없음이면 unassigned_task_ids와 reasons 기록
```
현재 보장:
- capacity 초과 배정 금지.
- adapter reject pair 배정 금지.
- 유효한 기존 assignment 우선 유지.
- 입력 순서와 무관한 deterministic 결과.
- `accepts_new_tasks=False` node는 기존 assignment 유지 가능, 신규 task 차단.
- unavailable node는 기존/신규 모두 차단.

### 6.4 `rebalance.py`
`detect_rebalance_reasons()`는 이전/현재 task-node 입력 차이를 generic reason으로 만든다.
현재 reason code:
- `task_added`
- `task_removed`
- `node_added`
- `node_removed`
- `node_unavailable`
- `node_not_accepting_new_tasks`
- `no_changes`
이 함수는 assignment plan을 만들지 않는다.
planner를 다시 돌릴 이유를 설명하는 순수 계산이다.

## 7. Polymarket Assigner
구현 위치: `src/orchestrator/assigner/`
이 계층은 Polymarket 계약 타입과 generic core 사이를 변환한다.

### 7.1 `polymarket_adapter.py`
주요 타입:
- `PolymarketAdapterPolicy`
  - `max_tokens_per_ws_connection`
  - `block_new_tasks_on_subscription_mismatch`
- `ShardWeightEstimate`
  - optional `events_per_sec`
Market -> Task resource mapping:
```text
markets = 1
tokens = len(token_ids)
ws_connections = ceil(token_count / max_tokens_per_ws_connection)
events_per_sec = optional shard_weight_estimate.events_per_sec
```
CollectorCapacity -> Node capacity mapping:
```text
markets = max_market_subscriptions
tokens = max_token_subscriptions
ws_connections = max_ws_connections
events_per_sec = max_events_per_sec, 값이 있을 때만
```
CollectorStatus -> NodeRuntimeHint mapping:
- `upload_backlog_files >= max_upload_backlog_files` -> `accepts_new_tasks=False`, reason `upload_backlog_limit`
- subscription mismatch -> 정책에 따라 `accepts_new_tasks=False`, reason `subscription_mismatch`
- 현재 status adapter는 node를 unavailable로 만들지 않는다.
- heartbeat timeout은 아직 상위 application layer가 계산해 hint로 넣어야 한다.

### 7.2 `planner.py`
`build_polymarket_assignment_plan()`의 흐름:
```text
MarketUniverseSnapshot + capacities + optional previous/status/weights/control_state
  -> hard stop이면 build_stop_assignment_plan() 즉시 반환
  -> snapshot_to_task_specs()
  -> capacities_to_node_specs()
  -> statuses_to_runtime_hints()
  -> previous AssignmentPlan을 generic Assignment tuple로 변환
  -> rebalance_core.build_assignment_plan()
  -> core_plan_to_assignment_plan()
  -> AssignmentPlan
```
Hard stop 처리:
- `ControlState.EMERGENCY_STOP_BY_BUDGET`이면 core planner를 호출하지 않는다.
- collectors가 빈 `AssignmentPlan`을 반환한다.
Core plan -> AssignmentPlan 변환:
- collector별 market id 목록 생성.
- market id에서 token id를 모아 중복 제거.
- capacity가 있는 모든 collector를 output collectors에 포함.
- 배정 없는 collector도 빈 `market_ids`, 빈 `token_ids`로 포함.
- `handoff_actions`는 현재 항상 빈 list.
현재 주의점:
- `AssignmentPlan` 계약에는 core의 `unassigned_task_ids`와 `reasons` field가 없다.
- 따라서 wrapper 결과만 보면 미배정 reason은 사라진다.
- 미배정 reason을 API에 노출하려면 별도 report 타입 또는 contract 확장이 필요하다.

## 8. 현재 가능한 순수 end-to-end 흐름
현재 코드로 가능한 흐름:
```text
1. caller가 raw market payload 목록을 준비한다.
2. build_market_universe_snapshot()이 MarketUniverseSnapshot을 만든다.
3. caller가 collector capacity/status map을 준비한다.
4. build_polymarket_assignment_plan()이 generic core 입력으로 변환한다.
5. rebalance_core.build_assignment_plan()이 capacity-aware assignment를 계산한다.
6. wrapper가 AssignmentPlan 계약으로 변환한다.
7. caller가 반환된 AssignmentPlan을 저장하거나 API로 제공할 수 있다.
```
주의: 현재 repository에는 1번의 실제 Gamma HTTP client와 7번의 API/server/persistence가 없다.
즉, 지금 구현은 service runtime이 아니라 service를 만들기 위한 순수 로직 기반이다.

## 9. 테스트 구조
Python tests:
- `test_market_manager_normalizer.py`: raw payload parsing, token extraction, market id fallback, MarketInfo 생성.
- `test_market_manager_lifecycle.py`: DISCOVERED/ACTIVE/DRAINING/CLOSED/ARCHIVED/EXCLUDED 전환.
- `test_market_manager_builder.py`: snapshot version, diff, deterministic order, drain window.
- `test_rebalance_core_models.py`: resource vector와 generic model validation.
- `test_rebalance_core_adapter.py`: adapter decision helper와 default adapter.
- `test_rebalance_core_planner.py`: capacity guard, previous assignment stability, deterministic result, runtime hint.
- `test_rebalance_core_rebalance.py`: rebalance reason 계산.
- `test_assigner_polymarket_adapter.py`: Polymarket contract -> generic spec 변환.
- `test_assigner_planner.py`: generic core plan -> AssignmentPlan 변환, hard stop bypass.
- `test_orchestrator_package.py`: package import/export smoke test.
Rust tests:
- `src/common/contracts.rs` 계약 타입 serialization/helper.
- `src/lib.rs`의 `library_version()` doc test.

## 10. 명시적으로 아직 구현되지 않은 것
현재 코드에 없는 것:
- Gamma API live refresh client
- Orchestrator HTTP API endpoint
- Collector registration/assignment endpoint
- assignment polling/long-poll/SSE
- Collector WebSocket runtime
- subscribe/unsubscribe 실행
- make-before-break 상태머신
- heartbeat timeout 계산기
- GCS object notification intake
- local rotated JSONL buffering
- compactor merge/manifest 구현
- Discord webhook 발송
- GCS cost estimator runtime
- Terraform/GKE/MIG/Cloud Run 배포 코드
- viewer API/runtime
검증할 때는 “현재 순수 로직 설명”과 “미래 아키텍처 목표”를 분리해서 봐야 한다.
