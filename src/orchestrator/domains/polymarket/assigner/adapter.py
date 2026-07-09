"""Polymarket 계약 타입을 generic rebalance_core 타입으로 변환합니다."""

from __future__ import annotations

from dataclasses import dataclass
from math import ceil
from typing import Mapping, Optional

from src.orchestrator.domains.polymarket.contracts import (
    CollectorCapacity,
    CollectorStatus,
    MarketInfo,
    MarketUniverseSnapshot,
)
from src.orchestrator.rebalance_core import NodeRuntimeHint, NodeSpec, ResourceVector, TaskSpec


@dataclass(frozen=True)
class PolymarketAdapterPolicy:
    """Polymarket adapter가 generic resource를 만들 때 쓰는 정책 값입니다.

    인자:
        max_tokens_per_ws_connection: WebSocket 연결 하나가 담당한다고 가정하는 최대 token 수입니다.
        block_new_tasks_on_subscription_mismatch: 구독 수 불일치가 있을 때 신규 task를 막을지 여부입니다.
    """

    max_tokens_per_ws_connection: int = 500
    block_new_tasks_on_subscription_mismatch: bool = True

    def __post_init__(self) -> None:
        """정책 숫자 값이 안전한 범위인지 검증합니다."""

        # 0 이하이면 ws connection 추정에서 나눗셈이 불가능하므로 거절합니다.
        if self.max_tokens_per_ws_connection <= 0:
            raise ValueError("max_tokens_per_ws_connection은 1 이상이어야 합니다")


@dataclass(frozen=True)
class ShardWeightEstimate:
    """market별 선택적 처리량 추정치입니다.

    인자:
        events_per_sec: 해당 market이 초당 만들 것으로 예상되는 event 수입니다.
    """

    events_per_sec: Optional[float] = None


def snapshot_to_task_specs(
    snapshot: MarketUniverseSnapshot,
    policy: PolymarketAdapterPolicy,
    shard_weight_estimates: Mapping[str, ShardWeightEstimate] | None = None,
) -> tuple[TaskSpec, ...]:
    """MarketUniverseSnapshot을 stable order의 generic TaskSpec tuple로 변환합니다.

    인자:
        snapshot: market universe snapshot입니다.
        policy: WebSocket 연결 추정 정책입니다.
        shard_weight_estimates: market id별 선택적 처리량 추정치입니다.

    반환값:
        market id 기준으로 정렬된 ``TaskSpec`` tuple입니다.
    """

    estimates = shard_weight_estimates or {}
    tasks: list[TaskSpec] = []
    # market id 기준 순서로 변환해 입력 순서와 무관한 adapter 출력을 만듭니다.
    for market_id in sorted(snapshot.markets):
        tasks.append(market_to_task_spec(snapshot.markets[market_id], policy, estimates.get(market_id)))
    return tuple(tasks)


def market_to_task_spec(
    market: MarketInfo,
    policy: PolymarketAdapterPolicy,
    estimate: ShardWeightEstimate | None = None,
) -> TaskSpec:
    """MarketInfo 하나를 generic TaskSpec으로 변환합니다.

    인자:
        market: 정규화된 market metadata입니다.
        policy: WebSocket 연결 추정 정책입니다.
        estimate: 선택적 처리량 추정치입니다.

    반환값:
        market 하나를 표현하는 ``TaskSpec``입니다.
    """

    resources = {
        "markets": 1.0,
        "tokens": float(len(market.token_ids)),
        "ws_connections": float(estimate_ws_connections(len(market.token_ids), policy)),
    }
    # 처리량 추정치가 있을 때만 optional resource dimension을 추가합니다.
    if estimate is not None and estimate.events_per_sec is not None:
        resources["events_per_sec"] = float(estimate.events_per_sec)
    return TaskSpec(
        task_id=market.market_id,
        resources=ResourceVector(resources),
        metadata={"slug": market.slug, "token_ids": tuple(market.token_ids)},
    )


def capacities_to_node_specs(capacities: Mapping[str, CollectorCapacity]) -> tuple[NodeSpec, ...]:
    """collector capacity map을 stable order의 generic NodeSpec tuple로 변환합니다."""

    nodes: list[NodeSpec] = []
    # collector id 기준 순서로 변환해 입력 순서와 무관한 adapter 출력을 만듭니다.
    for collector_id in sorted(capacities):
        nodes.append(capacity_to_node_spec(collector_id, capacities[collector_id]))
    return tuple(nodes)


def capacity_to_node_spec(collector_id: str, capacity: CollectorCapacity) -> NodeSpec:
    """CollectorCapacity 하나를 generic NodeSpec으로 변환합니다."""

    resources = {
        "markets": float(capacity.max_market_subscriptions),
        "tokens": float(capacity.max_token_subscriptions),
        "ws_connections": float(capacity.max_ws_connections),
    }
    # collector가 처리량 상한을 선언했을 때만 optional resource dimension으로 사용합니다.
    if capacity.max_events_per_sec is not None:
        resources["events_per_sec"] = float(capacity.max_events_per_sec)
    return NodeSpec(node_id=collector_id, capacity=ResourceVector(resources))


def statuses_to_runtime_hints(
    statuses: Mapping[str, CollectorStatus],
    capacities: Mapping[str, CollectorCapacity],
    policy: PolymarketAdapterPolicy,
) -> dict[str, NodeRuntimeHint]:
    """collector status map을 generic runtime hint map으로 변환합니다."""

    hints: dict[str, NodeRuntimeHint] = {}
    # status가 있는 collector만 runtime hint로 변환합니다.
    for collector_id in sorted(statuses):
        capacity = capacities.get(collector_id)
        # capacity가 없으면 planner node도 없으므로 status hint를 만들지 않습니다.
        if capacity is None:
            continue
        hints[collector_id] = status_to_runtime_hint(statuses[collector_id], capacity, policy)
    return hints


def status_to_runtime_hint(
    status: CollectorStatus,
    capacity: CollectorCapacity,
    policy: PolymarketAdapterPolicy,
) -> NodeRuntimeHint:
    """CollectorStatus 하나를 generic NodeRuntimeHint로 변환합니다."""

    accepts_new_tasks = True
    reason: Optional[str] = None
    # upload backlog가 선언 용량 이상이면 기존 배정은 유지하되 신규 task를 막습니다.
    if status.upload_backlog_files >= capacity.max_upload_backlog_files:
        accepts_new_tasks = False
        reason = "upload_backlog_limit"
    # 구독 수 불일치 정책이 켜져 있으면 신규 task를 막아 추가 흔들림을 줄입니다.
    if policy.block_new_tasks_on_subscription_mismatch and not status.assignment_matches_subscription():
        accepts_new_tasks = False
        reason = "subscription_mismatch"
    return NodeRuntimeHint(
        node_id=status.collector_id,
        available=True,
        accepts_new_tasks=accepts_new_tasks,
        reason=reason,
    )


def estimate_ws_connections(token_count: int, policy: PolymarketAdapterPolicy) -> int:
    """token 수를 기반으로 보수적인 WebSocket 연결 수를 추정합니다."""

    # token이 없으면 구독할 WebSocket 연결도 없다고 봅니다.
    if token_count <= 0:
        return 0
    return ceil(token_count / policy.max_tokens_per_ws_connection)
