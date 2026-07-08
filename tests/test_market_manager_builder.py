import unittest

from src.orchestrator.contracts import MarketInfo, MarketLifecycleState, MarketUniverseSnapshot
from src.orchestrator.market_manager.builder import build_market_universe_snapshot
from src.orchestrator.market_manager.policy import LifecycleMemory, LifecyclePolicy


class MarketManagerBuilderTests(unittest.TestCase):
    def test_first_refresh_creates_version_one_with_ten_markets(self):
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

    def test_token_added_increments_version_and_marks_token_diff(self):
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

    def test_no_diff_keeps_previous_version(self):
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

    def test_payload_order_does_not_change_snapshot_order(self):
        result = build_market_universe_snapshot(
            [_raw_market("market-b"), _raw_market("market-a")],
            previous_snapshot=None,
            previous_memory=None,
            now_ms=1_000,
            policy=LifecyclePolicy(new_market_confirm_refreshes=0),
        )

        self.assertEqual(list(result.snapshot.markets), ["market-a", "market-b"])

    def test_closed_market_drains_then_gets_removed_after_window(self):
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

    def test_archived_market_is_removed_from_active_universe(self):
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
