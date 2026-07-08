"""재사용 가능한 task/node 리밸런싱 core의 순수 model 타입입니다.

이 모듈은 Polymarket, Gamma, cloud storage, notification 같은 도메인 지식을 갖지
않습니다. 모든 용량은 문자열 key를 가진 generic resource vector로 표현합니다.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from math import inf
from typing import Any, Mapping, Optional


@dataclass(frozen=True)
class ResourceVector:
    """task 사용량 또는 node 용량을 나타내는 generic resource vector입니다.

    인자:
        values: resource 이름에서 양수 또는 0 이상의 수치로 이어지는 mapping입니다.

    반환값:
        dataclass 자체를 반환하며, 음수 resource 값은 ``ValueError``를 발생시킵니다.
    """

    values: Mapping[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """resource key를 문자열로 정규화하고 0 값은 제거합니다.

        반환값:
            정상 값이면 아무것도 반환하지 않습니다. 음수 값이면 ``ValueError``를 발생시킵니다.
        """

        normalized: dict[str, float] = {}
        # 모든 resource 항목을 안정적인 내부 dict로 복사합니다.
        for key, value in self.values.items():
            amount = float(value)
            # 음수 사용량은 capacity 계산을 깨뜨리므로 생성 시점에 거절합니다.
            if amount < 0:
                raise ValueError(f"resource 값은 0 이상이어야 합니다: {key}")
            # 0 값은 missing key와 같은 의미로 취급해 vector를 작게 유지합니다.
            if amount != 0:
                normalized[str(key)] = amount
        object.__setattr__(self, "values", normalized)

    def get(self, key: str) -> float:
        """특정 resource 값을 반환합니다.

        인자:
            key: 조회할 resource 이름입니다.

        반환값:
            값이 없으면 0.0을 반환합니다.
        """

        return float(self.values.get(key, 0.0))

    def plus(self, other: "ResourceVector") -> "ResourceVector":
        """두 resource vector를 더한 새 vector를 반환합니다.

        인자:
            other: 더할 resource vector입니다.

        반환값:
            key별 합계를 담은 새 ``ResourceVector``입니다.
        """

        merged: dict[str, float] = dict(self.values)
        # 상대 vector의 key를 순회하며 같은 resource끼리 합산합니다.
        for key, value in other.values.items():
            merged[key] = merged.get(key, 0.0) + value
        return ResourceVector(merged)

    def fits_within(self, capacity: "ResourceVector") -> bool:
        """현재 사용량이 주어진 capacity 안에 들어가는지 반환합니다.

        인자:
            capacity: 비교 대상 node capacity입니다.

        반환값:
            모든 non-zero resource 사용량이 capacity 이하이면 ``True``입니다.
        """

        # 사용 중인 resource만 검사하면 missing capacity key는 0으로 비교됩니다.
        for key, amount in self.values.items():
            # 하나라도 capacity를 초과하면 즉시 실패를 반환합니다.
            if amount > capacity.get(key):
                return False
        return True

    def utilization_against(self, capacity: "ResourceVector") -> float:
        """capacity 대비 최대 사용률을 반환합니다.

        인자:
            capacity: 비교 대상 node capacity입니다.

        반환값:
            가장 높은 resource 사용률입니다. capacity가 0인데 사용량이 있으면 ``inf``입니다.
        """

        max_ratio = 0.0
        # 사용량이 존재하는 key만 순회해 최대 resource 사용률을 계산합니다.
        for key, amount in self.values.items():
            cap = capacity.get(key)
            # 사용량이 있는데 capacity가 0이면 배정 불가능한 무한 사용률입니다.
            if cap <= 0:
                return inf
            ratio = amount / cap
            # 현재 resource가 기존 최대보다 높으면 최대 사용률을 갱신합니다.
            if ratio > max_ratio:
                max_ratio = ratio
        return max_ratio


@dataclass(frozen=True)
class TaskSpec:
    """generic planner가 배정할 단일 task입니다."""

    task_id: str
    resources: ResourceVector
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """task id와 metadata를 검증/정규화합니다."""

        # 빈 task id는 deterministic sorting과 reason 기록을 깨뜨리므로 거절합니다.
        if not self.task_id:
            raise ValueError("task_id는 비어 있을 수 없습니다")
        object.__setattr__(self, "metadata", dict(self.metadata))


@dataclass(frozen=True)
class NodeSpec:
    """generic planner가 task를 배정할 단일 node입니다."""

    node_id: str
    capacity: ResourceVector
    available: bool = True
    accepts_new_tasks: bool = True
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """node id와 metadata를 검증/정규화합니다."""

        # 빈 node id는 deterministic sorting과 assignment key를 깨뜨리므로 거절합니다.
        if not self.node_id:
            raise ValueError("node_id는 비어 있을 수 없습니다")
        object.__setattr__(self, "metadata", dict(self.metadata))


@dataclass(frozen=True)
class NodeRuntimeHint:
    """application/adapter layer가 계산해 core에 전달하는 node runtime hint입니다."""

    node_id: str
    available: bool = True
    accepts_new_tasks: bool = True
    reason: Optional[str] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """runtime hint의 node id와 metadata를 검증/정규화합니다."""

        # 빈 node id는 context lookup을 불가능하게 하므로 거절합니다.
        if not self.node_id:
            raise ValueError("node_id는 비어 있을 수 없습니다")
        object.__setattr__(self, "metadata", dict(self.metadata))


@dataclass(frozen=True)
class PlannerContext:
    """planner 실행 중 adapter callback에 함께 전달되는 typed generic context입니다."""

    runtime_hints: Mapping[str, NodeRuntimeHint] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """runtime hint와 metadata를 안정적인 dict로 정규화합니다."""

        hints: dict[str, NodeRuntimeHint] = {}
        # hint 객체의 node_id를 기준으로 lookup table을 재구성합니다.
        for hint in self.runtime_hints.values():
            hints[hint.node_id] = hint
        object.__setattr__(self, "runtime_hints", hints)
        object.__setattr__(self, "metadata", dict(self.metadata))

    def hint_for(self, node_id: str) -> NodeRuntimeHint:
        """특정 node에 대한 runtime hint를 반환합니다.

        인자:
            node_id: 조회할 node 식별자입니다.

        반환값:
            명시적 hint가 없으면 기본 허용 hint를 반환합니다.
        """

        return self.runtime_hints.get(node_id, NodeRuntimeHint(node_id=node_id))


@dataclass(frozen=True)
class Assignment:
    """단일 task가 단일 node에 배정되었음을 나타내는 generic assignment입니다."""

    task_id: str
    node_id: str


@dataclass(frozen=True)
class AssignmentPlanCore:
    """generic planner가 반환하는 순수 assignment plan입니다."""

    assignments: tuple[Assignment, ...] = ()
    unassigned_task_ids: tuple[str, ...] = ()
    resource_usage_by_node: Mapping[str, ResourceVector] = field(default_factory=dict)
    reasons: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
