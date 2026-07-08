# Assigner

이 패키지는 Polymarket 도메인 계약을 재사용 가능한 `rebalance_core` 입력/출력으로
변환하는 Orchestrator 계층입니다.

## 경계

- `rebalance_core`는 generic task/node/resource 배정만 계산합니다.
- `polymarket_adapter.py`는 `MarketUniverseSnapshot`, `CollectorCapacity`,
  `CollectorStatus`를 `TaskSpec`, `NodeSpec`, `NodeRuntimeHint`로 변환합니다.
- `planner.py`는 generic plan을 기존 `AssignmentPlan` 계약으로 되돌립니다.
- 예산 hard stop은 core planner를 호출하지 않고 즉시 stop plan으로 처리합니다.
- WebSocket subscribe/unsubscribe, make-before-break 상태머신, Terraform 배포는 이
  패키지의 책임이 아닙니다.

## v1 정책

- 정상 동작 중인 기존 market assignment는 가능한 유지합니다.
- upload backlog나 subscription mismatch는 신규 배정을 제한하는 hint로 먼저 표현합니다.
- 거래량 급증만으로 기존 healthy market을 다른 Collector로 옮기지 않습니다.
