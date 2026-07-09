"""그린필드 v2 Orchestrator 계약 dataclass 모음입니다.

이 dataclass들은 Python Orchestrator 쪽에서 아키텍처 22.1절을 구현합니다.
``src/common/contracts.rs``의 Rust 계약과 의도적으로 같은 의미와 JSON 값을 유지하여,
Orchestrator와 Collector가 숨은 변환 규칙 없이 payload를 교환할 수 있게 합니다.
"""

from __future__ import annotations

from src.orchestrator.domains.polymarket.contracts import (
    AssignmentLimits,
    AssignmentPlan,
    CollectorAssignment,
    CollectorCapacity,
    CollectorStatus,
    HandoffAction,
    HandoffMode,
    MarketInfo,
    MarketLifecycleState,
    MarketUniverseSnapshot,
    OrderbookEvent,
    OrderbookEventType,
)
from src.orchestrator.core.budget import BudgetPolicy, BudgetState
from src.orchestrator.core.control import ControlState
from src.orchestrator.infra.gcs.contracts import ObjectNotification, ProcessedObject
