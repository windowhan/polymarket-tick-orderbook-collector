"""Gamma payload 목록에서 MarketUniverseSnapshot을 만드는 순수 builder입니다.

이 모듈은 HTTP 호출, 파일 저장, assignment 재계산을 수행하지 않습니다. 호출자가 전달한
raw payload, 이전 snapshot, lifecycle memory만 사용해 새 snapshot과 diff를 계산합니다.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from src.orchestrator.contracts import MarketUniverseSnapshot
from src.orchestrator.market_manager.diff import UniverseDiff, build_universe_diff
from src.orchestrator.market_manager.lifecycle import (
    compute_lifecycle_state,
    should_include_in_active_universe,
)
from src.orchestrator.market_manager.normalizer import (
    extract_raw_market_flags,
    normalize_gamma_market,
)
from src.orchestrator.market_manager.policy import LifecycleMemory, LifecyclePolicy


@dataclass(frozen=True)
class UniverseBuildResult:
    """마켓 유니버스 build 결과입니다.

    인자:
        snapshot: 새로 계산한 마켓 유니버스 스냅샷입니다.
        diff: 이전 snapshot과 새 snapshot 사이의 변경분입니다.
        memory: 다음 refresh에 넘길 lifecycle memory입니다.
    """

    snapshot: MarketUniverseSnapshot
    diff: UniverseDiff
    memory: LifecycleMemory


def build_market_universe_snapshot(
    raw_markets: Iterable[Mapping[str, Any]],
    previous_snapshot: MarketUniverseSnapshot | None,
    previous_memory: LifecycleMemory | None,
    now_ms: int,
    policy: LifecyclePolicy,
) -> UniverseBuildResult:
    """Gamma raw market 목록으로 새 universe snapshot, diff, memory를 계산합니다.

    인자:
        raw_markets: Gamma API가 반환한 market dict iterable입니다.
        previous_snapshot: 직전 마켓 유니버스 스냅샷입니다. 첫 refresh이면 ``None``입니다.
        previous_memory: 직전 lifecycle memory입니다. 첫 refresh이면 ``None``입니다.
        now_ms: build 기준 시각이며 Unix millisecond 단위입니다.
        policy: lifecycle 계산 정책입니다.

    반환값:
        새 snapshot, diff, 갱신된 lifecycle memory를 담은 ``UniverseBuildResult``입니다.
    """

    memory = previous_memory or LifecycleMemory()
    previous_markets = previous_snapshot.markets if previous_snapshot else {}
    included_markets = {}

    for raw in raw_markets:
        flags = extract_raw_market_flags(raw)
        if flags is None:
            continue

        previous_market = previous_markets.get(flags.market_id)
        lifecycle_state, memory = compute_lifecycle_state(
            flags,
            previous_market,
            now_ms,
            policy,
            memory,
        )
        if not should_include_in_active_universe(lifecycle_state, policy):
            continue

        market = normalize_gamma_market(raw, lifecycle_state)
        if market is not None:
            included_markets[market.market_id] = market

    candidate_snapshot = _make_snapshot(
        version=_candidate_version(previous_snapshot),
        generated_at_ms=now_ms,
        markets=included_markets,
    )
    diff = build_universe_diff(previous_snapshot, candidate_snapshot)
    final_snapshot = _make_snapshot(
        version=_next_version(previous_snapshot, diff),
        generated_at_ms=now_ms,
        markets=included_markets,
    )

    return UniverseBuildResult(snapshot=final_snapshot, diff=diff, memory=memory)


def _candidate_version(previous_snapshot: MarketUniverseSnapshot | None) -> int:
    """diff 계산용 임시 version을 반환합니다."""

    return previous_snapshot.version if previous_snapshot is not None else 1


def _next_version(previous_snapshot: MarketUniverseSnapshot | None, diff: UniverseDiff) -> int:
    """diff 결과를 기준으로 다음 snapshot version을 계산합니다."""

    if previous_snapshot is None:
        return 1
    if diff.has_changes():
        return previous_snapshot.version + 1
    return previous_snapshot.version


def _make_snapshot(
    version: int,
    generated_at_ms: int,
    markets: Mapping[str, Any],
) -> MarketUniverseSnapshot:
    """market id 기준 정렬을 적용한 snapshot을 생성합니다."""

    return MarketUniverseSnapshot(
        version=version,
        generated_at_ms=generated_at_ms,
        markets={market_id: markets[market_id] for market_id in sorted(markets)},
    )
