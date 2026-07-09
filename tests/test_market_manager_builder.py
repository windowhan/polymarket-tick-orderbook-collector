import unittest

from src.orchestrator.contracts import MarketInfo, MarketLifecycleState, MarketUniverseSnapshot
from src.orchestrator.domains.polymarket.market_manager.builder import build_market_universe_snapshot
from src.orchestrator.domains.polymarket.market_manager.policy import LifecycleMemory, LifecyclePolicy


class MarketManagerBuilderTests(unittest.TestCase):
    # 샘플 입력: 이전 snapshot 없음, 유효 raw market 10개
    # 기대 출력: snapshot.version=1, added_market_ids 10개
    def test_first_refresh_creates_version_one_with_ten_markets(self):
        """첫 Gamma refresh에서 모든 유효 market이 version 1 snapshot에 들어가는 상황을 검증합니다."""
        result = build_market_universe_snapshot(
            [_raw_market(f"market-{idx}") for idx in range(10)],
            previous_snapshot=None,
            previous_memory=None,
            now_ms=1_000,
            policy=LifecyclePolicy(new_market_confirm_refreshes=0),
        )

        self.assertEqual(result.snapshot.version, 1)
        self.assertEqual(len(result.snapshot.markets), 10)
        self.assertEqual(len(result.diff.added_market_ids), 10)
        self.assertTrue(result.diff.has_changes())

    # 샘플 입력: 이전 m1 tokens=[t1], 새 m1 tokens=[t1,t2]
    # 기대 출력: snapshot.version이 2로 증가하고 token_changed_market_ids={m1}
    def test_token_added_increments_version_and_marks_token_diff(self):
        """기존 market의 token 목록이 늘면 universe version과 token diff가 갱신되는 상황을 검증합니다."""
        previous = _snapshot(1, [_market("market-1", ["token-a"])])

        result = build_market_universe_snapshot(
            [_raw_market("market-1", token_ids=["token-a", "token-b"])],
            previous_snapshot=previous,
            previous_memory=LifecycleMemory({"market-1": 2}, {}),
            now_ms=2_000,
            policy=LifecyclePolicy(new_market_confirm_refreshes=0),
        )

        self.assertEqual(result.snapshot.version, 2)
        self.assertEqual(result.diff.token_changed_market_ids, frozenset({"market-1"}))
        self.assertEqual(result.snapshot.markets["market-1"].token_ids, ["token-a", "token-b"])

    # 샘플 입력: 이전 snapshot과 동일한 raw market refresh
    # 기대 출력: snapshot.version은 이전 version 유지, diff.has_changes()=False
    def test_no_diff_keeps_previous_version(self):
        """동일 payload refresh에서는 snapshot version을 올리지 않는 상황을 검증합니다."""
        previous = _snapshot(7, [_market("market-1", ["token-a"])])

        result = build_market_universe_snapshot(
            [_raw_market("market-1", token_ids=["token-a"])],
            previous_snapshot=previous,
            previous_memory=LifecycleMemory({"market-1": 3}, {}),
            now_ms=9_000,
            policy=LifecyclePolicy(new_market_confirm_refreshes=0),
        )

        self.assertEqual(result.snapshot.version, 7)
        self.assertFalse(result.diff.has_changes())
        self.assertEqual(result.snapshot.generated_at_ms, 9_000)

    # 샘플 입력: raw market 입력 순서 [m2,m1]
    # 기대 출력: snapshot.markets key 순서 [m1,m2]
    def test_payload_order_does_not_change_snapshot_order(self):
        """Gamma payload 순서가 바뀌어도 snapshot market order가 deterministic하게 유지되는 상황을 검증합니다."""
        result = build_market_universe_snapshot(
            [_raw_market("market-b"), _raw_market("market-a")],
            previous_snapshot=None,
            previous_memory=None,
            now_ms=1_000,
            policy=LifecyclePolicy(new_market_confirm_refreshes=0),
        )

        self.assertEqual(list(result.snapshot.markets), ["market-a", "market-b"])

    # 샘플 입력: 기존 ACTIVE market이 closed=True 후 drain window 전/후 refresh
    # 기대 출력: 처음엔 DRAINING으로 포함되고 window 이후 snapshot에서 제거
    def test_closed_market_drains_then_gets_removed_after_window(self):
        """닫힌 market이 drain window 동안 유지된 뒤 제거되는 lifecycle 흐름을 검증합니다."""
        policy = LifecyclePolicy(new_market_confirm_refreshes=0, closed_market_drain_secs=60)
        previous = _snapshot(1, [_market("market-1", ["token-a"])])

        draining = build_market_universe_snapshot(
            [_raw_market("market-1", token_ids=["token-a"], closed=True)],
            previous_snapshot=previous,
            previous_memory=LifecycleMemory({"market-1": 2}, {}),
            now_ms=100_000,
            policy=policy,
        )
        removed = build_market_universe_snapshot(
            [_raw_market("market-1", token_ids=["token-a"], closed=True)],
            previous_snapshot=draining.snapshot,
            previous_memory=draining.memory,
            now_ms=161_000,
            policy=policy,
        )

        self.assertEqual(
            draining.snapshot.markets["market-1"].lifecycle_state,
            MarketLifecycleState.DRAINING,
        )
        self.assertEqual(draining.snapshot.version, 2)
        self.assertEqual(removed.snapshot.version, 3)
        self.assertNotIn("market-1", removed.snapshot.markets)
        self.assertEqual(removed.diff.removed_market_ids, frozenset({"market-1"}))

    # 샘플 입력: archived=True raw market
    # 기대 출력: active universe snapshot에 market이 포함되지 않음
    def test_archived_market_is_removed_from_active_universe(self):
        """archived market을 active universe에서 즉시 제거하는 정책 상황을 검증합니다."""
        previous = _snapshot(1, [_market("market-1", ["token-a"])])

        result = build_market_universe_snapshot(
            [_raw_market("market-1", token_ids=["token-a"], archived=True)],
            previous_snapshot=previous,
            previous_memory=LifecycleMemory({"market-1": 2}, {}),
            now_ms=2_000,
            policy=LifecyclePolicy(new_market_confirm_refreshes=0),
        )

        self.assertEqual(result.snapshot.version, 2)
        self.assertEqual(result.snapshot.markets, {})
        self.assertEqual(result.diff.removed_market_ids, frozenset({"market-1"}))


def _raw_market(
    market_id: str,
    *,
    token_ids: list[str] | None = None,
    closed: bool = False,
    archived: bool = False,
) -> dict:
    return {
        "id": market_id,
        "slug": f"{market_id}-slug",
        "question": f"{market_id} 질문",
        "active": True,
        "closed": closed,
        "archived": archived,
        "accepting_orders": not closed,
        "enable_order_book": True,
        "clob_token_ids": token_ids if token_ids is not None else [f"{market_id}-token"],
    }


def _market(market_id: str, token_ids: list[str]) -> MarketInfo:
    return MarketInfo(
        market_id=market_id,
        slug=f"{market_id}-slug",
        question=f"{market_id} 질문",
        active=True,
        closed=False,
        archived=False,
        accepting_orders=True,
        enable_order_book=True,
        token_ids=token_ids,
        lifecycle_state=MarketLifecycleState.ACTIVE,
    )


def _snapshot(version: int, markets: list[MarketInfo]) -> MarketUniverseSnapshot:
    return MarketUniverseSnapshot(
        version=version,
        generated_at_ms=1_000,
        markets={market.market_id: market for market in markets},
    )


if __name__ == "__main__":
    unittest.main()
