"""Polymarket assignment plan을 generic rebalance_core로 생성하는 wrapper입니다."""

from __future__ import annotations

from typing import Mapping, Optional

from src.orchestrator.domains.polymarket.assigner.adapter import (
    PolymarketAdapterPolicy,
    ShardWeightEstimate,
    capacities_to_node_specs,
    snapshot_to_task_specs,
    statuses_to_runtime_hints,
)
from src.orchestrator.contracts import (
    AssignmentLimits,
    AssignmentPlan,
    CollectorAssignment,
    CollectorCapacity,
    CollectorStatus,
    ControlState,
    MarketInfo,
    MarketUniverseSnapshot,
)
from src.orchestrator.rebalance_core import (
    Assignment,
    AssignmentPlanCore,
    PlannerContext,
    build_assignment_plan as build_core_assignment_plan,
)


def build_polymarket_assignment_plan(
    universe: MarketUniverseSnapshot,
    capacities: Mapping[str, CollectorCapacity],
    version: int,
    generated_at_ms: int,
    previous_plan: AssignmentPlan | None = None,
    statuses: Mapping[str, CollectorStatus] | None = None,
    shard_weight_estimates: Mapping[str, ShardWeightEstimate] | None = None,
    control_state: ControlState = ControlState.RUNNING,
    policy: PolymarketAdapterPolicy | None = None,
) -> AssignmentPlan:
    """Polymarket 계약 입력으로 collector assignment plan을 생성합니다.

    인자:
        universe: assignment 대상 market universe snapshot입니다.
        capacities: collector id별 declared capacity입니다.
        version: 새 assignment plan version입니다.
        generated_at_ms: plan 생성 시각이며 Unix millisecond 단위입니다.
        previous_plan: 가능한 유지할 이전 assignment plan입니다.
        statuses: collector id별 마지막 runtime status입니다.
        shard_weight_estimates: market id별 선택적 처리량 추정치입니다.
        control_state: 생성할 plan의 제어 상태입니다.
        policy: adapter 변환 정책입니다.

    반환값:
        Collector가 읽을 수 있는 ``AssignmentPlan``입니다.
    """

    selected_policy = policy or PolymarketAdapterPolicy()
    # hard stop은 rebalance가 아니라 전체 중단이므로 core planner를 호출하지 않습니다.
    if control_state == ControlState.EMERGENCY_STOP_BY_BUDGET:
        return build_stop_assignment_plan(version, universe.version, generated_at_ms, control_state)

    tasks = snapshot_to_task_specs(universe, selected_policy, shard_weight_estimates)
    nodes = capacities_to_node_specs(capacities)
    hints = statuses_to_runtime_hints(statuses or {}, capacities, selected_policy)
    core_plan = build_core_assignment_plan(
        tasks,
        nodes,
        previous_assignments=_previous_plan_to_core(previous_plan),
        context=PlannerContext(runtime_hints=hints),
    )
    return core_plan_to_assignment_plan(
        core_plan,
        universe,
        capacities,
        selected_policy,
        version,
        generated_at_ms,
        control_state,
    )


def build_stop_assignment_plan(
    version: int,
    universe_version: int,
    generated_at_ms: int,
    control_state: ControlState = ControlState.EMERGENCY_STOP_BY_BUDGET,
) -> AssignmentPlan:
    """core planner를 우회해 전체 중단 assignment plan을 생성합니다."""

    return AssignmentPlan(
        version=version,
        universe_version=universe_version,
        generated_at_ms=generated_at_ms,
        control_state=control_state,
        collectors={},
    )


def core_plan_to_assignment_plan(
    core_plan: AssignmentPlanCore,
    universe: MarketUniverseSnapshot,
    capacities: Mapping[str, CollectorCapacity],
    policy: PolymarketAdapterPolicy,
    version: int,
    generated_at_ms: int,
    control_state: ControlState,
) -> AssignmentPlan:
    """generic AssignmentPlanCore를 Polymarket AssignmentPlan으로 변환합니다."""

    market_ids_by_collector = _market_ids_by_collector(core_plan)
    collectors: dict[str, CollectorAssignment] = {}
    # capacity가 있는 모든 collector를 stable order로 plan에 포함합니다.
    for collector_id in sorted(capacities):
        market_ids = market_ids_by_collector.get(collector_id, [])
        collectors[collector_id] = CollectorAssignment(
            collector_id=collector_id,
            market_ids=market_ids,
            token_ids=_token_ids_for_markets(market_ids, universe.markets),
            limits=AssignmentLimits(
                max_ws_connections=capacities[collector_id].max_ws_connections,
                max_tokens_per_ws_connection=policy.max_tokens_per_ws_connection,
            ),
            handoff_actions=[],
        )
    return AssignmentPlan(version, universe.version, generated_at_ms, control_state, collectors)


def _previous_plan_to_core(previous_plan: AssignmentPlan | None) -> tuple[Assignment, ...]:
    """이전 Polymarket AssignmentPlan을 generic Assignment tuple로 변환합니다."""

    assignments: list[Assignment] = []
    # 이전 plan이 없으면 유지할 assignment도 없습니다.
    if previous_plan is None:
        return ()
    # collector id 기준 stable order로 이전 market 배정을 generic assignment로 바꿉니다.
    for collector_id in sorted(previous_plan.collectors):
        collector = previous_plan.collectors[collector_id]
        # market id 기준 stable order로 이전 task-node 관계를 복원합니다.
        for market_id in sorted(collector.market_ids):
            assignments.append(Assignment(task_id=market_id, node_id=collector_id))
    return tuple(assignments)


def _market_ids_by_collector(core_plan: AssignmentPlanCore) -> dict[str, list[str]]:
    """generic assignment를 collector id별 market id 목록으로 묶습니다."""

    grouped: dict[str, list[str]] = {}
    # generic assignment를 순회하며 node_id를 collector_id로 사용합니다.
    for assignment in core_plan.assignments:
        grouped.setdefault(assignment.node_id, []).append(assignment.task_id)
    # collector별 market id를 stable order로 정렬합니다.
    for market_ids in grouped.values():
        market_ids.sort()
    return grouped


def _token_ids_for_markets(market_ids: list[str], markets: Mapping[str, MarketInfo]) -> list[str]:
    """market id 목록에서 중복 없는 token id 목록을 stable order로 추출합니다."""

    seen: set[str] = set()
    token_ids: list[str] = []
    # market id를 순회하며 각 market의 token id를 assignment payload로 확장합니다.
    for market_id in market_ids:
        market = markets.get(market_id)
        # universe에 없는 market은 stale assignment일 수 있으므로 token 확장에서 제외합니다.
        if market is None:
            continue
        # market 안의 token id 순서를 유지하되 collector 전체에서는 중복을 제거합니다.
        for token_id in market.token_ids:
            # 이미 추가한 token은 payload 중복을 피하기 위해 건너뜁니다.
            if token_id in seen:
                continue
            seen.add(token_id)
            token_ids.append(token_id)
    return token_ids
