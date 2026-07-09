"""Gamma market flag와 이전 상태로 lifecycle state를 계산합니다.

이 모듈은 네트워크, 파일, clock을 직접 사용하지 않습니다. 호출자가 넘긴 ``now_ms``와
``LifecycleMemory``만으로 다음 상태와 갱신된 memory를 반환합니다.
"""

from __future__ import annotations

from src.orchestrator.contracts import MarketInfo, MarketLifecycleState
from src.orchestrator.market_manager.normalizer import RawMarketFlags
from src.orchestrator.market_manager.policy import LifecycleMemory, LifecyclePolicy

_ASSIGNABLE_STATES = frozenset(
    {
        MarketLifecycleState.DISCOVERED,
        MarketLifecycleState.ACTIVE,
        MarketLifecycleState.DRAINING,
    }
)
_DRAINABLE_PREVIOUS_STATES = frozenset(
    {
        MarketLifecycleState.DISCOVERED,
        MarketLifecycleState.ACTIVE,
        MarketLifecycleState.DRAINING,
    }
)


def compute_lifecycle_state(
    raw_flags: RawMarketFlags,
    previous: MarketInfo | None,
    now_ms: int,
    policy: LifecyclePolicy,
    memory: LifecycleMemory,
) -> tuple[MarketLifecycleState, LifecycleMemory]:
    """단일 마켓의 다음 lifecycle state와 memory를 계산합니다.

    인자:
        raw_flags: Gamma payload에서 추출한 lifecycle 판단용 flag입니다.
        previous: 이전 snapshot에 있던 동일 market 정보입니다.
        now_ms: 판단 기준 시각이며 Unix millisecond 단위입니다.
        policy: lifecycle 전환 정책입니다.
        memory: refresh 사이에 전달되는 보조 상태입니다.

    반환값:
        계산된 ``MarketLifecycleState``와 갱신된 ``LifecycleMemory``의 tuple입니다.
    """

    if raw_flags.archived:
        return MarketLifecycleState.ARCHIVED, _clear_draining(memory, raw_flags.market_id)

    if _is_closing(raw_flags):
        return _compute_closing_state(raw_flags, previous, now_ms, policy, memory)

    if policy.exclude_missing_token_ids and not raw_flags.token_ids:
        return MarketLifecycleState.EXCLUDED, _clear_draining(memory, raw_flags.market_id)

    if policy.exclude_orderbook_disabled and not raw_flags.enable_order_book:
        return MarketLifecycleState.EXCLUDED, _clear_draining(memory, raw_flags.market_id)

    if not raw_flags.active:
        return MarketLifecycleState.EXCLUDED, _clear_draining(memory, raw_flags.market_id)

    return _compute_open_state(raw_flags, previous, memory, policy)


def should_include_in_active_universe(
    state: MarketLifecycleState,
    policy: LifecyclePolicy,
) -> bool:
    """계산된 lifecycle state를 active universe snapshot에 포함할지 반환합니다.

    인자:
        state: 계산된 lifecycle state입니다.
        policy: archived 보존 여부 같은 포함 정책입니다.

    반환값:
        assignment 후보 또는 drain 유지 대상이면 ``True``입니다. ``ARCHIVED``는 정책상
        즉시 제거하지 않는 경우에만 ``True``입니다.
    """

    if state in _ASSIGNABLE_STATES:
        return True
    if state == MarketLifecycleState.ARCHIVED:
        return not policy.archived_remove_immediately
    return False


def _compute_open_state(
    raw_flags: RawMarketFlags,
    previous: MarketInfo | None,
    memory: LifecycleMemory,
    policy: LifecyclePolicy,
) -> tuple[MarketLifecycleState, LifecycleMemory]:
    """닫힘/제외 조건이 없는 마켓의 DISCOVERED 또는 ACTIVE 상태를 계산합니다."""

    next_memory = _clear_draining(memory, raw_flags.market_id)
    seen_count = next_memory.seen_refresh_counts.get(raw_flags.market_id, 0) + 1
    next_memory = next_memory.with_seen_count(raw_flags.market_id, seen_count)

    if previous is None or previous.lifecycle_state == MarketLifecycleState.DISCOVERED:
        if seen_count <= policy.new_market_confirm_refreshes:
            return MarketLifecycleState.DISCOVERED, next_memory
    return MarketLifecycleState.ACTIVE, next_memory


def _compute_closing_state(
    raw_flags: RawMarketFlags,
    previous: MarketInfo | None,
    now_ms: int,
    policy: LifecyclePolicy,
    memory: LifecycleMemory,
) -> tuple[MarketLifecycleState, LifecycleMemory]:
    """닫힘 신호가 들어온 마켓의 DRAINING 또는 CLOSED 상태를 계산합니다."""

    if not _can_enter_draining(raw_flags.market_id, previous, memory):
        return MarketLifecycleState.CLOSED, _clear_draining(memory, raw_flags.market_id)

    started_at_ms = memory.draining_since_ms.get(raw_flags.market_id, now_ms)
    next_memory = memory.with_draining_since(raw_flags.market_id, started_at_ms)
    drain_window_ms = policy.closed_market_drain_secs * 1_000

    if now_ms - started_at_ms < drain_window_ms:
        return MarketLifecycleState.DRAINING, next_memory
    return MarketLifecycleState.CLOSED, next_memory


def _is_closing(raw_flags: RawMarketFlags) -> bool:
    """Gamma flag가 마켓 종료 또는 주문 중단을 나타내는지 반환합니다."""

    return raw_flags.closed or not raw_flags.accepting_orders


def _can_enter_draining(
    market_id: str,
    previous: MarketInfo | None,
    memory: LifecycleMemory,
) -> bool:
    """이 마켓이 drain window를 유지할 수 있는 이전 상태를 가졌는지 반환합니다."""

    if market_id in memory.draining_since_ms:
        return True
    if previous is None:
        return False
    return previous.lifecycle_state in _DRAINABLE_PREVIOUS_STATES


def _clear_draining(memory: LifecycleMemory, market_id: str) -> LifecycleMemory:
    """마켓이 다시 열린 상태거나 제외 상태가 되었을 때 stale DRAINING memory를 제거합니다."""

    if market_id not in memory.draining_since_ms:
        return memory
    updated = dict(memory.draining_since_ms)
    del updated[market_id]
    return LifecycleMemory(dict(memory.seen_refresh_counts), updated)
