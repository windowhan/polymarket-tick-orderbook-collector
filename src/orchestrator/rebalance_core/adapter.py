"""generic assignment planner가 호출하는 adapter protocol입니다.

Core는 이 protocol만 알고, 실제 도메인 제약과 선호도 계산은 adapter 구현체가 담당합니다.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Protocol

from src.orchestrator.rebalance_core.models import NodeSpec, PlannerContext, TaskSpec


@dataclass(frozen=True)
class ConstraintDecision:
    """task-node pair에 대한 adapter 제약 판단 결과입니다.

    인자:
        allowed: 배정 허용 여부입니다.
        code: machine-readable 판단 code입니다.
        reason: 사람이 읽을 수 있는 판단 이유입니다.
        metadata: adapter가 남기는 추가 generic metadata입니다.
    """

    allowed: bool
    code: str = "allowed"
    reason: Optional[str] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """판단 code와 metadata를 검증/정규화합니다."""

        # 빈 code는 reason 집계와 테스트 assertion을 어렵게 하므로 거절합니다.
        if not self.code:
            raise ValueError("ConstraintDecision.code는 비어 있을 수 없습니다")
        object.__setattr__(self, "metadata", dict(self.metadata))

    @classmethod
    def allow(cls, *, code: str = "allowed", reason: Optional[str] = None) -> "ConstraintDecision":
        """허용 decision을 생성합니다.

        인자:
            code: 허용 판단 code입니다.
            reason: 선택적인 설명입니다.

        반환값:
            ``allowed=True``인 ``ConstraintDecision``입니다.
        """

        return cls(allowed=True, code=code, reason=reason)

    @classmethod
    def reject(cls, code: str, reason: str) -> "ConstraintDecision":
        """거절 decision을 생성합니다.

        인자:
            code: 거절 판단 code입니다.
            reason: 거절 이유입니다.

        반환값:
            ``allowed=False``인 ``ConstraintDecision``입니다.
        """

        return cls(allowed=False, code=code, reason=reason)


class AssignmentAdapter(Protocol):
    """core planner가 도메인 제약과 선호도를 질의하는 protocol입니다."""

    def can_assign(
        self,
        task: TaskSpec,
        node: NodeSpec,
        context: PlannerContext,
    ) -> ConstraintDecision:
        """task를 node에 배정할 수 있는지 판단합니다."""

    def score_assignment(
        self,
        task: TaskSpec,
        node: NodeSpec,
        context: PlannerContext,
    ) -> float:
        """safety/stability 이후 candidate 선호 점수를 반환합니다."""


class DefaultAssignmentAdapter:
    """모든 task-node pair를 허용하고 동일 점수를 주는 기본 adapter입니다."""

    def can_assign(
        self,
        task: TaskSpec,
        node: NodeSpec,
        context: PlannerContext,
    ) -> ConstraintDecision:
        """기본 구현에서는 모든 pair를 허용합니다."""

        return ConstraintDecision.allow()

    def score_assignment(
        self,
        task: TaskSpec,
        node: NodeSpec,
        context: PlannerContext,
    ) -> float:
        """기본 구현에서는 모든 candidate에 같은 선호 점수를 부여합니다."""

        return 0.0
