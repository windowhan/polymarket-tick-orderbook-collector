"""assignment 재계산이 필요한 generic reason을 계산합니다."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

from src.orchestrator.rebalance_core.models import NodeSpec, PlannerContext, TaskSpec


@dataclass(frozen=True)
class RebalanceReason:
    """planner 재실행 또는 운영 판단에 남길 generic rebalance reason입니다.

    인자:
        code: machine-readable reason code입니다.
        subject_type: reason 대상 종류입니다.
        subject_id: reason 대상 식별자입니다.
        detail: 사람이 읽을 수 있는 추가 설명입니다.
    """

    code: str
    subject_type: str
    subject_id: str
    detail: Optional[str] = None


def detect_rebalance_reasons(
    previous_tasks: Iterable[TaskSpec],
    current_tasks: Iterable[TaskSpec],
    previous_nodes: Iterable[NodeSpec],
    current_nodes: Iterable[NodeSpec],
    context: PlannerContext | None = None,
) -> tuple[RebalanceReason, ...]:
    """이전/현재 task-node 입력 차이에서 generic rebalance reason을 계산합니다.

    인자:
        previous_tasks: 이전 planner 입력 task 목록입니다.
        current_tasks: 현재 planner 입력 task 목록입니다.
        previous_nodes: 이전 planner 입력 node 목록입니다.
        current_nodes: 현재 planner 입력 node 목록입니다.
        context: runtime hint가 들어 있는 planner context입니다.

    반환값:
        stable order로 정렬된 ``RebalanceReason`` tuple입니다. 변화가 없으면 ``no_changes``를 반환합니다.
    """

    selected_context = context or PlannerContext()
    previous_task_ids = _task_ids(previous_tasks)
    current_task_ids = _task_ids(current_tasks)
    previous_node_ids = _node_ids(previous_nodes)
    current_node_map = _node_map(current_nodes)
    reasons: list[RebalanceReason] = []

    # 새 task는 배정 후보가 늘어난 것이므로 reason으로 기록합니다.
    for task_id in sorted(current_task_ids - previous_task_ids):
        reasons.append(RebalanceReason("task_added", "task", task_id))
    # 사라진 task는 기존 배정 제거가 필요할 수 있으므로 reason으로 기록합니다.
    for task_id in sorted(previous_task_ids - current_task_ids):
        reasons.append(RebalanceReason("task_removed", "task", task_id))
    # 새 node는 미배정 task를 받을 수 있으므로 reason으로 기록합니다.
    for node_id in sorted(set(current_node_map) - previous_node_ids):
        reasons.append(RebalanceReason("node_added", "node", node_id))
    # 사라진 node는 해당 node의 task 재배정이 필요하므로 reason으로 기록합니다.
    for node_id in sorted(previous_node_ids - set(current_node_map)):
        reasons.append(RebalanceReason("node_removed", "node", node_id))
    # 현재 node 상태와 runtime hint를 순회해 운영상 배정 제약 reason을 기록합니다.
    for node_id in sorted(current_node_map):
        node = current_node_map[node_id]
        hint = selected_context.hint_for(node_id)
        # unavailable node는 기존 배정을 유지할 수 없으므로 강한 rebalance reason입니다.
        if not node.available or not hint.available:
            reasons.append(RebalanceReason("node_unavailable", "node", node_id, hint.reason))
            continue
        # 신규 task 수용 차단은 새 task 배정 제한 reason으로 기록합니다.
        if not node.accepts_new_tasks or not hint.accepts_new_tasks:
            reasons.append(RebalanceReason("node_not_accepting_new_tasks", "node", node_id, hint.reason))

    # 어떤 변화도 없으면 caller가 no-op으로 판단할 수 있게 명시 reason을 남깁니다.
    if not reasons:
        reasons.append(RebalanceReason("no_changes", "planner", "inputs"))
    return tuple(reasons)


def _task_ids(tasks: Iterable[TaskSpec]) -> set[str]:
    """task iterable에서 task id set을 추출합니다."""

    ids: set[str] = set()
    # 모든 task를 순회해 id만 비교 집합에 넣습니다.
    for task in tasks:
        ids.add(task.task_id)
    return ids


def _node_ids(nodes: Iterable[NodeSpec]) -> set[str]:
    """node iterable에서 node id set을 추출합니다."""

    ids: set[str] = set()
    # 모든 node를 순회해 id만 비교 집합에 넣습니다.
    for node in nodes:
        ids.add(node.node_id)
    return ids


def _node_map(nodes: Iterable[NodeSpec]) -> dict[str, NodeSpec]:
    """node iterable에서 node id 기준 map을 생성합니다."""

    mapped: dict[str, NodeSpec] = {}
    # 현재 node 목록을 순회해 상태 검사에 사용할 lookup table을 만듭니다.
    for node in nodes:
        mapped[node.node_id] = node
    return mapped
