"""MarketUniverseSnapshot 사이의 assignment 영향 변경분을 계산합니다."""

from __future__ import annotations

from dataclasses import dataclass, field
from src.orchestrator.contracts import MarketInfo, MarketUniverseSnapshot


@dataclass(frozen=True)
class UniverseDiff:
    """두 마켓 유니버스 스냅샷 사이의 의미 있는 변경분입니다.

    인자:
        added_market_ids: 새 snapshot에 처음 등장한 market id 집합입니다.
        removed_market_ids: 이전 snapshot에는 있었지만 새 snapshot에서 사라진 market id 집합입니다.
        lifecycle_changed_market_ids: lifecycle 상태가 바뀐 market id 집합입니다.
        token_changed_market_ids: token id 목록이 바뀐 market id 집합입니다.
    """

    added_market_ids: frozenset[str] = field(default_factory=frozenset)
    removed_market_ids: frozenset[str] = field(default_factory=frozenset)
    lifecycle_changed_market_ids: frozenset[str] = field(default_factory=frozenset)
    token_changed_market_ids: frozenset[str] = field(default_factory=frozenset)

    def has_changes(self) -> bool:
        """assignment 재계산이 필요한 변경분이 있는지 반환합니다.

        반환값:
            추가, 제거, lifecycle 변경, token 변경 중 하나라도 있으면 ``True``입니다.
        """

        return bool(
            self.added_market_ids
            or self.removed_market_ids
            or self.lifecycle_changed_market_ids
            or self.token_changed_market_ids
        )

    def changed_market_ids(self) -> frozenset[str]:
        """변경이 발생한 모든 market id를 합쳐 반환합니다.

        반환값:
            모든 변경 집합의 합집합입니다.
        """

        return frozenset().union(
            self.added_market_ids,
            self.removed_market_ids,
            self.lifecycle_changed_market_ids,
            self.token_changed_market_ids,
        )


def build_universe_diff(
    previous: MarketUniverseSnapshot | None,
    current: MarketUniverseSnapshot,
) -> UniverseDiff:
    """이전 snapshot과 현재 snapshot 사이의 변경분을 계산합니다.

    인자:
        previous: 이전 마켓 유니버스 스냅샷입니다. 없으면 모든 현재 마켓이 추가로 간주됩니다.
        current: 새로 계산한 마켓 유니버스 스냅샷입니다.

    반환값:
        assignment에 영향을 줄 수 있는 추가/제거/lifecycle/token 변경분입니다.
    """

    if previous is None:
        return UniverseDiff(added_market_ids=frozenset(current.markets))

    previous_ids = set(previous.markets)
    current_ids = set(current.markets)
    shared_ids = previous_ids & current_ids

    lifecycle_changed = {
        market_id
        for market_id in shared_ids
        if previous.markets[market_id].lifecycle_state
        != current.markets[market_id].lifecycle_state
    }
    token_changed = {
        market_id
        for market_id in shared_ids
        if _token_tuple(previous.markets[market_id]) != _token_tuple(current.markets[market_id])
    }

    return UniverseDiff(
        added_market_ids=frozenset(current_ids - previous_ids),
        removed_market_ids=frozenset(previous_ids - current_ids),
        lifecycle_changed_market_ids=frozenset(lifecycle_changed),
        token_changed_market_ids=frozenset(token_changed),
    )


def _token_tuple(market: MarketInfo) -> tuple[str, ...]:
    """token id 목록을 비교 가능한 tuple로 변환합니다."""

    return tuple(market.token_ids)
