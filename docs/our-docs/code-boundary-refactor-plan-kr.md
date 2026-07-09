# 코드 경계 리팩터링 계획

이 문서는 현재 섞여 있는 Orchestrator/Collector core 코드와 Polymarket 도메인 코드를 분리하기 위한 실행 계획이다.
목표는 기존 동작을 유지하면서 import 경계를 명확히 만드는 것이다.

## 1. 현재 문제

현재 코드에서 이미 잘 분리된 부분은 `src/orchestrator/rebalance_core/`다.
이 패키지는 generic `TaskSpec`, `NodeSpec`, `ResourceVector`, `AssignmentPlanCore`를 사용하며 Polymarket/Gamma/GCS/Discord/Terraform을 모른다.

반면 다음 영역은 아직 섞여 있다.

- `src/orchestrator/contracts.py`
  - generic control state, Polymarket market/assignment contract, GCS object/budget contract가 한 파일에 있다.
- `src/orchestrator/market_manager/`
  - 이름은 generic처럼 보이지만 실제로는 Gamma/Polymarket market universe logic이다.
- `src/orchestrator/assigner/`
  - `polymarket_adapter.py`와 `planner.py` 모두 Polymarket AssignmentPlan wrapper다.
- `src/common/contracts.rs`
  - Rust 공유 계약도 generic collector 계약과 Polymarket/CLOB 계약이 한 파일에 있다.

## 2. 목표 구조

2026-07-09 기준 Python 경계 분리는 구현되어 있다. 기존 import path는 compatibility facade로 유지하며, 새 구현과 테스트는 `core/`, `infra/gcs/`, `domains/polymarket/`를 우선 사용한다. Rust 계약 분리는 아직 장기 목표로 남아 있다.

Python 목표 구조:

```text
src/orchestrator/
  core/
    control.py
    budget.py
  infra/
    gcs/contracts.py
  domains/
    polymarket/
      contracts.py
      market_manager/
      assigner/
  rebalance_core/
  contracts.py          # compatibility re-export
  market_manager/       # compatibility re-export
  assigner/             # compatibility re-export
```

Rust 장기 목표 구조:

```text
src/
  common/
    control_contracts.rs
    storage_contracts.rs
    contracts.rs        # compatibility re-export
  domains/polymarket/
    contracts.rs
  collector_core/
```

## 3. 경계 원칙

1. `rebalance_core`는 계속 도메인 중립으로 둔다.
2. Gamma/Polymarket/CLOB 의미가 있는 코드는 `domains/polymarket` 아래로 이동한다.
3. GCS object notification과 processed manifest는 Polymarket 도메인이 아니라 infra/gcs로 이동한다.
4. 기존 import path는 compatibility facade로 유지해 한번에 깨지지 않게 한다.
5. 새 코드에서는 새 import path를 우선 사용한다.
6. 각 커밋은 300줄 미만으로 유지한다.
7. 이동 후 매 단계에서 Python unit test와 Rust test를 확인한다.

## 4. 단계별 계획

### Phase 1 — 문서화

- 이 문서를 추가한다.
- 현재 코드 흐름 문서와 함께 검증 기준으로 사용한다.

### Phase 2 — Polymarket domain skeleton 생성

추가 위치:

```text
src/orchestrator/domains/__init__.py
src/orchestrator/domains/polymarket/__init__.py
src/orchestrator/domains/polymarket/market_manager/__init__.py
src/orchestrator/domains/polymarket/assigner/__init__.py
```

테스트:

- `tests/test_orchestrator_package.py`에 domain package import smoke를 추가한다.

### Phase 3 — market_manager 이동

이동 전:

```text
src/orchestrator/market_manager/*.py
```

이동 후:

```text
src/orchestrator/domains/polymarket/market_manager/*.py
```

기존 `src/orchestrator/market_manager`는 compatibility re-export만 유지한다.
테스트는 새 import path를 우선 사용하고 compatibility smoke만 남긴다.

### Phase 4 — assigner 이동

이동 전:

```text
src/orchestrator/assigner/polymarket_adapter.py
src/orchestrator/assigner/planner.py
```

이동 후:

```text
src/orchestrator/domains/polymarket/assigner/adapter.py
src/orchestrator/domains/polymarket/assigner/planner.py
```

기존 `src/orchestrator/assigner`는 compatibility re-export만 유지한다.

### Phase 5 — Python contracts 분리

분리 목표:

```text
src/orchestrator/core/control.py
  ControlState
src/orchestrator/core/budget.py
  BudgetPolicy
  BudgetState
src/orchestrator/infra/gcs/contracts.py
  ObjectNotification
  ProcessedObject
src/orchestrator/domains/polymarket/contracts.py
  MarketLifecycleState
  OrderbookEventType
  HandoffMode
  MarketInfo
  MarketUniverseSnapshot
  CollectorCapacity
  CollectorStatus
  AssignmentLimits
  HandoffAction
  CollectorAssignment
  AssignmentPlan
  OrderbookEvent
```

기존 `src/orchestrator/contracts.py`는 re-export facade로 남긴다.

### Phase 6 — Rust contracts 분리

Python import 경계가 안정된 뒤 Rust도 같은 방향으로 나눈다.
Rust는 doctest와 public API 영향이 커서 별도 작업으로 진행한다.

### Phase 7 — import boundary 검증 추가

최종적으로 다음 검증을 추가한다.

```bash
grep -R "Polymarket\|Gamma\|MarketInfo\|Orderbook" src/orchestrator/rebalance_core
grep -R "Gamma\|clob\|MarketInfo" src/orchestrator/core
```

## 5. 완료 기준

- `rebalance_core`는 도메인 import 없이 유지된다.
- Polymarket/Gamma/CLOB 코드는 `domains/polymarket` 아래에 모인다.
- GCS/processed object 계약은 `infra/gcs` 아래에 모인다.
- 기존 import path는 compatibility facade로 동작한다.
- 전체 테스트와 smoke command가 통과한다.

## 6. 검증 명령

```bash
python3 -m py_compile $(find src/orchestrator tests -name '*.py' | sort)
python3 -m unittest discover -s tests
cargo test
cargo run --quiet
```
