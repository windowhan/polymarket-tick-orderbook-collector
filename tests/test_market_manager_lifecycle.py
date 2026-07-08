import unittest

from src.orchestrator.contracts import MarketInfo, MarketLifecycleState
from src.orchestrator.market_manager.lifecycle import (
    compute_lifecycle_state,
    should_include_in_active_universe,
)
from src.orchestrator.market_manager.normalizer import RawMarketFlags
from src.orchestrator.market_manager.policy import LifecycleMemory, LifecyclePolicy


class MarketManagerLifecycleTests(unittest.TestCase):
    def test_new_market_starts_as_discovered(self):
        state, memory = compute_lifecycle_state(
            _flags("market-1"),
            previous=None,
            now_ms=1_000,
            policy=LifecyclePolicy(new_market_confirm_refreshes=1),
            memory=LifecycleMemory(),
        )

        self.assertEqual(state, MarketLifecycleState.DISCOVERED)
        self.assertEqual(memory.seen_refresh_counts["market-1"], 1)
        self.assertTrue(should_include_in_active_universe(state, LifecyclePolicy()))

    def test_confirmed_new_market_becomes_active(self):
        policy = LifecyclePolicy(new_market_confirm_refreshes=1)
        first_state, first_memory = compute_lifecycle_state(
            _flags("market-1"), None, 1_000, policy, LifecycleMemory()
        )

        second_state, second_memory = compute_lifecycle_state(
            _flags("market-1"),
            _market("market-1", first_state),
            2_000,
            policy,
            first_memory,
        )

        self.assertEqual(second_state, MarketLifecycleState.ACTIVE)
        self.assertEqual(second_memory.seen_refresh_counts["market-1"], 2)

    def test_open_existing_market_stays_active(self):
        state, _ = compute_lifecycle_state(
            _flags("market-1"),
            _market("market-1", MarketLifecycleState.ACTIVE),
            1_000,
            LifecyclePolicy(),
            LifecycleMemory(),
        )

        self.assertEqual(state, MarketLifecycleState.ACTIVE)

    def test_closed_market_enters_draining_before_removal(self):
        policy = LifecyclePolicy(closed_market_drain_secs=60)
        previous = _market("market-1", MarketLifecycleState.ACTIVE)

        draining_state, draining_memory = compute_lifecycle_state(
            _flags("market-1", closed=True), previous, 100_000, policy, LifecycleMemory()
        )
        closed_state, closed_memory = compute_lifecycle_state(
            _flags("market-1", closed=True),
            _market("market-1", draining_state),
            161_000,
            policy,
            draining_memory,
        )

        self.assertEqual(draining_state, MarketLifecycleState.DRAINING)
        self.assertEqual(draining_memory.draining_since_ms["market-1"], 100_000)
        self.assertEqual(closed_state, MarketLifecycleState.CLOSED)
        self.assertFalse(should_include_in_active_universe(closed_state, policy))
        self.assertEqual(closed_memory.draining_since_ms["market-1"], 100_000)

    def test_accepting_orders_false_enters_draining(self):
        state, _ = compute_lifecycle_state(
            _flags("market-1", accepting_orders=False),
            _market("market-1", MarketLifecycleState.ACTIVE),
            1_000,
            LifecyclePolicy(),
            LifecycleMemory(),
        )

        self.assertEqual(state, MarketLifecycleState.DRAINING)

    def test_archived_market_is_excluded_from_active_universe(self):
        policy = LifecyclePolicy(archived_remove_immediately=True)
        state, _ = compute_lifecycle_state(
            _flags("market-1", archived=True),
            _market("market-1", MarketLifecycleState.ACTIVE),
            1_000,
            policy,
            LifecycleMemory(),
        )

        self.assertEqual(state, MarketLifecycleState.ARCHIVED)
        self.assertFalse(should_include_in_active_universe(state, policy))

    def test_missing_token_ids_are_excluded(self):
        state, _ = compute_lifecycle_state(
            _flags("market-1", token_ids=[]),
            previous=None,
            now_ms=1_000,
            policy=LifecyclePolicy(exclude_missing_token_ids=True),
            memory=LifecycleMemory(),
        )

        self.assertEqual(state, MarketLifecycleState.EXCLUDED)
        self.assertFalse(should_include_in_active_universe(state, LifecyclePolicy()))

    def test_orderbook_disabled_is_excluded(self):
        state, _ = compute_lifecycle_state(
            _flags("market-1", enable_order_book=False),
            previous=None,
            now_ms=1_000,
            policy=LifecyclePolicy(exclude_orderbook_disabled=True),
            memory=LifecycleMemory(),
        )

        self.assertEqual(state, MarketLifecycleState.EXCLUDED)


def _flags(
    market_id: str,
    *,
    active: bool = True,
    closed: bool = False,
    archived: bool = False,
    accepting_orders: bool = True,
    enable_order_book: bool = True,
    token_ids: list[str] | None = None,
) -> RawMarketFlags:
    return RawMarketFlags(
        market_id=market_id,
        slug=f"{market_id}-slug",
        question=f"{market_id} 질문",
        active=active,
        closed=closed,
        archived=archived,
        accepting_orders=accepting_orders,
        enable_order_book=enable_order_book,
        token_ids=token_ids if token_ids is not None else [f"{market_id}-token"],
    )


def _market(market_id: str, state: MarketLifecycleState) -> MarketInfo:
    return MarketInfo(
        market_id=market_id,
        slug=f"{market_id}-slug",
        question=f"{market_id} 질문",
        active=True,
        closed=False,
        archived=False,
        accepting_orders=True,
        enable_order_book=True,
        token_ids=[f"{market_id}-token"],
        lifecycle_state=state,
    )


if __name__ == "__main__":
    unittest.main()
