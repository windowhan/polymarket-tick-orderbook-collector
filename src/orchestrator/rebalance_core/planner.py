"""generic task/node assignment plan을 생성하는 순수 planner입니다."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

from src.orchestrator.rebalance_core.adapter import (
    AssignmentAdapter,
    ConstraintDecision,
    DefaultAssignmentAdapter,
)
from src.orchestrator.rebalance_core.models import (
    Assignment,
    AssignmentPlanCore,
    NodeSpec,
    PlannerContext,
    ResourceVector,
    TaskSpec,
)


@dataclass(frozen=True)
class _Candidate:
    """내부 정렬에 사용하는 node 후보입니다."""

    node: NodeSpec
    score: float
    utilization: float
    proposed_usage: ResourceVector


@dataclass(frozen=True)
class _CandidateResult:
    """단일 task에 대한 후보 선택 결과입니다."""

    candidate: Optional[_Candidate]
    reasons: tuple[str, ...]


def build_assignment_plan(
    tasks: Iterable[TaskSpec],
    nodes: Iterable[NodeSpec],
    previous_assignments: Iterable[Assignment] = (),
    adapter: AssignmentAdapter | None = None,
    context: PlannerContext | None = None,
) -> AssignmentPlanCore:
    """task 목록을 node capacity 안에서 deterministic하게 배정합니다.

    인자:
        tasks: 배정할 generic task iterable입니다.
        nodes: 배정 대상 generic node iterable입니다.
        previous_assignments: 가능한 한 유지할 이전 generic assignment iterable입니다.
        adapter: task-node 제약과 선호 점수를 제공하는 adapter입니다.
        context: adapter callback과 runtime hint에 전달할 typed context입니다.

    반환값:
        배정, 미배정 task, node별 resource usage, reason을 담은 ``AssignmentPlanCore``입니다.
    """

    selected_adapter = adapter or DefaultAssignmentAdapter()
    selected_context = context or PlannerContext()
    sorted_tasks = _sort_tasks(tasks)
    sorted_nodes = _sort_nodes(nodes)
    usage_by_node = _empty_usage_by_node(sorted_nodes)
    task_by_id = _task_map(sorted_tasks)
    node_by_id = _node_map(sorted_nodes)
    assignments, assigned_task_ids = _keep_previous_assignments(
        previous_assignments,
        task_by_id,
        node_by_id,
        usage_by_node,
        selected_adapter,
        selected_context,
    )
    unassigned: list[str] = []
    reasons: dict[str, tuple[str, ...]] = {}

    # task를 stable order로 순회해 입력 순서와 무관한 배정 결과를 만듭니다.
    for task in sorted_tasks:
        # 이전 배정으로 이미 유지된 task는 새 candidate 탐색에서 제외합니다.
        if task.task_id in assigned_task_ids:
            continue
        result = _choose_node_for_new_task(
            task,
            sorted_nodes,
            usage_by_node,
            selected_adapter,
            selected_context,
        )
        # 후보가 없으면 미배정으로 남기고 실패 이유를 task에 붙입니다.
        if result.candidate is None:
            unassigned.append(task.task_id)
            reasons[task.task_id] = result.reasons
        # 후보가 있으면 assignment와 node usage를 함께 갱신합니다.
        else:
            assignments.append(Assignment(task_id=task.task_id, node_id=result.candidate.node.node_id))
            usage_by_node[result.candidate.node.node_id] = result.candidate.proposed_usage

    return AssignmentPlanCore(
        assignments=tuple(assignments),
        unassigned_task_ids=tuple(unassigned),
        resource_usage_by_node=usage_by_node,
        reasons=reasons,
    )


def _sort_tasks(tasks: Iterable[TaskSpec]) -> tuple[TaskSpec, ...]:
    """task id 기준 stable order로 task를 정렬합니다."""

    return tuple(sorted(tasks, key=lambda task: task.task_id))


def _sort_nodes(nodes: Iterable[NodeSpec]) -> tuple[NodeSpec, ...]:
    """node id 기준 stable order로 node를 정렬합니다."""

    return tuple(sorted(nodes, key=lambda node: node.node_id))



def _task_map(tasks: tuple[TaskSpec, ...]) -> dict[str, TaskSpec]:
    """task id로 task를 조회하는 map을 생성합니다."""

    task_by_id: dict[str, TaskSpec] = {}
    # sorted task를 순회해 이전 배정 검증에 사용할 lookup table을 만듭니다.
    for task in tasks:
        task_by_id[task.task_id] = task
    return task_by_id


def _node_map(nodes: tuple[NodeSpec, ...]) -> dict[str, NodeSpec]:
    """node id로 node를 조회하는 map을 생성합니다."""

    node_by_id: dict[str, NodeSpec] = {}
    # sorted node를 순회해 이전 배정 검증에 사용할 lookup table을 만듭니다.
    for node in nodes:
        node_by_id[node.node_id] = node
    return node_by_id


def _keep_previous_assignments(
    previous_assignments: Iterable[Assignment],
    task_by_id: dict[str, TaskSpec],
    node_by_id: dict[str, NodeSpec],
    usage_by_node: dict[str, ResourceVector],
    adapter: AssignmentAdapter,
    context: PlannerContext,
) -> tuple[list[Assignment], set[str]]:
    """아직 유효한 이전 assignment를 먼저 유지합니다."""

    kept_assignments: list[Assignment] = []
    assigned_task_ids: set[str] = set()
    sorted_previous = sorted(previous_assignments, key=lambda item: (item.task_id, item.node_id))
    # 이전 assignment를 stable order로 확인해 가능한 배정을 선점합니다.
    for assignment in sorted_previous:
        # 같은 task가 중복으로 들어오면 첫 stable assignment만 유지합니다.
        if assignment.task_id in assigned_task_ids:
            continue
        task = task_by_id.get(assignment.task_id)
        node = node_by_id.get(assignment.node_id)
        # task나 node가 사라졌으면 이전 배정은 더 이상 유효하지 않습니다.
        if task is None or node is None:
            continue
        result = _candidate_for_node(
            task,
            node,
            usage_by_node[node.node_id],
            adapter,
            context,
            existing_assignment=True,
        )
        # capacity나 adapter 제약을 깨는 이전 배정은 유지하지 않습니다.
        if result.candidate is None:
            continue
        kept_assignments.append(assignment)
        assigned_task_ids.add(assignment.task_id)
        usage_by_node[node.node_id] = result.candidate.proposed_usage
    return kept_assignments, assigned_task_ids


def _empty_usage_by_node(nodes: tuple[NodeSpec, ...]) -> dict[str, ResourceVector]:
    """모든 node의 초기 resource usage map을 생성합니다."""

    usage_by_node: dict[str, ResourceVector] = {}
    # 모든 node가 결과 usage map에 나타나도록 0 사용량으로 초기화합니다.
    for node in nodes:
        usage_by_node[node.node_id] = ResourceVector()
    return usage_by_node


def _choose_node_for_new_task(
    task: TaskSpec,
    nodes: tuple[NodeSpec, ...],
    usage_by_node: dict[str, ResourceVector],
    adapter: AssignmentAdapter,
    context: PlannerContext,
) -> _CandidateResult:
    """단일 task를 새로 받을 수 있는 최선의 node 후보를 고릅니다."""

    candidates: list[_Candidate] = []
    reject_reasons: list[str] = []
    # 모든 node를 stable order로 검사해 candidate와 reject reason을 수집합니다.
    for node in nodes:
        result = _candidate_for_node(
            task,
            node,
            usage_by_node[node.node_id],
            adapter,
            context,
            existing_assignment=False,
        )
        # node가 후보가 아니면 이유만 누적하고 다음 node를 검사합니다.
        if result.candidate is None:
            reject_reasons.extend(result.reasons)
        # node가 후보이면 나중에 score/utilization으로 정렬합니다.
        else:
            candidates.append(result.candidate)

    # 하나 이상의 후보가 있으면 preference score와 사용률로 최종 node를 고릅니다.
    if candidates:
        candidates.sort(key=lambda candidate: (-candidate.score, candidate.utilization, candidate.node.node_id))
        return _CandidateResult(candidate=candidates[0], reasons=())
    return _CandidateResult(candidate=None, reasons=tuple(reject_reasons))


def _candidate_for_node(
    task: TaskSpec,
    node: NodeSpec,
    current_usage: ResourceVector,
    adapter: AssignmentAdapter,
    context: PlannerContext,
    existing_assignment: bool,
) -> _CandidateResult:
    """task-node pair 하나가 배정 후보인지 평가합니다."""

    hint = context.hint_for(node.node_id)
    # 정적 node 상태나 runtime hint가 unavailable이면 기존/신규 배정 모두 막습니다.
    if not node.available or not hint.available:
        return _CandidateResult(candidate=None, reasons=(f"{node.node_id}:node_unavailable",))
    # 신규 배정에서는 node와 runtime hint의 신규 수용 가능 상태를 모두 확인합니다.
    if not existing_assignment and (not node.accepts_new_tasks or not hint.accepts_new_tasks):
        return _CandidateResult(candidate=None, reasons=(f"{node.node_id}:not_accepting_new_tasks",))

    proposed_usage = current_usage.plus(task.resources)
    # proposed usage가 capacity를 넘으면 adapter score와 무관하게 거절합니다.
    if not proposed_usage.fits_within(node.capacity):
        return _CandidateResult(candidate=None, reasons=(f"{node.node_id}:capacity_exceeded",))

    decision = adapter.can_assign(task, node, context)
    # adapter가 거절하면 capacity가 남아도 배정하지 않습니다.
    if not decision.allowed:
        return _CandidateResult(candidate=None, reasons=(_format_decision_reason(node, decision),))

    return _CandidateResult(
        candidate=_Candidate(
            node=node,
            score=adapter.score_assignment(task, node, context),
            utilization=proposed_usage.utilization_against(node.capacity),
            proposed_usage=proposed_usage,
        ),
        reasons=(),
    )


def _format_decision_reason(node: NodeSpec, decision: ConstraintDecision) -> str:
    """adapter decision을 task reason 문자열로 변환합니다."""

    # 사람이 읽는 reason이 있으면 code와 함께 보존합니다.
    if decision.reason:
        return f"{node.node_id}:{decision.code}:{decision.reason}"
    return f"{node.node_id}:{decision.code}"
