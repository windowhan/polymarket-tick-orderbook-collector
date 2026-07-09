"""도메인 중립 예산 정책과 상태 계약입니다."""

from __future__ import annotations

from dataclasses import dataclass

from src.orchestrator.core.control import ControlState


@dataclass(frozen=True)
class BudgetPolicy:
    """비용 가드레일에 사용할 예산 임계값 설정입니다."""

    warning_threshold_usd: float
    hard_stop_threshold_usd: float
    discord_webhook_secret_name: str
    check_interval_secs: int


@dataclass(frozen=True)
class BudgetState:
    """Orchestrator가 유지하는 런타임 예산 상태입니다."""

    estimated_gcs_cost_usd: float
    uploaded_bytes: int
    object_create_count: int
    object_list_count: int
    object_get_count: int
    object_delete_count: int
    control_state: ControlState

    def warning_exceeded(self, policy: BudgetPolicy) -> bool:
        """경고 임계값을 넘었는지 반환합니다."""

        return self.estimated_gcs_cost_usd >= policy.warning_threshold_usd

    def hard_stop_exceeded(self, policy: BudgetPolicy) -> bool:
        """강제 중단 임계값을 넘었는지 반환합니다."""

        return self.estimated_gcs_cost_usd >= policy.hard_stop_threshold_usd
