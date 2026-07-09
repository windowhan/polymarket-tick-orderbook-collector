"""도메인 중립 Orchestrator 제어 상태 계약입니다."""

from __future__ import annotations

from enum import Enum


class ControlState(str, Enum):
    """배정과 함께 반환되는 전역 제어 상태입니다.

    Collector 프로세스는 ``EMERGENCY_STOP_BY_BUDGET``을 일반 배정 변경보다 더 높은
    우선순위로 처리해야 합니다.
    """

    RUNNING = "RUNNING"
    PAUSED_BY_OPERATOR = "PAUSED_BY_OPERATOR"
    PAUSED_BY_BUDGET_WARNING = "PAUSED_BY_BUDGET_WARNING"
    EMERGENCY_STOP_BY_BUDGET = "EMERGENCY_STOP_BY_BUDGET"
