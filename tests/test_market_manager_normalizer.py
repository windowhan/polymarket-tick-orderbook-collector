import unittest

from src.orchestrator.contracts import MarketLifecycleState
from src.orchestrator.market_manager.normalizer import (
    extract_market_id,
    extract_raw_market_flags,
    extract_token_ids,
    normalize_gamma_market,
)


class MarketManagerNormalizerTests(unittest.TestCase):
    def test_extracts_token_ids_from_json_string(self):
        raw = {"clobTokenIds": '["token-a", "token-b", "token-a"]'}

        self.assertEqual(extract_token_ids(raw), ["token-a", "token-b"])

    def test_extracts_token_ids_from_token_objects(self):
        raw = {
            "tokens": [
                {"token_id": "yes-token"},
                {"id": "no-token"},
                {"tokenId": "maybe-token"},
            ]
        }

        self.assertEqual(extract_token_ids(raw), ["yes-token", "no-token", "maybe-token"])

    def test_market_id_uses_condition_id_fallback(self):
        raw = {"conditionId": "condition-1"}

        self.assertEqual(extract_market_id(raw), "condition-1")

    def test_raw_flags_accept_camel_case_gamma_fields(self):
        raw = {
            "id": 123,
            "slug": "sample-market",
            "title": "샘플 마켓인가?",
            "active": "true",
            "closed": "false",
            "archived": 0,
            "acceptingOrders": "yes",
            "enableOrderBook": "1",
            "clobTokenIds": ["token-a", "token-b"],
        }

        flags = extract_raw_market_flags(raw)

        self.assertIsNotNone(flags)
        assert flags is not None
        self.assertEqual(flags.market_id, "123")
        self.assertEqual(flags.question, "샘플 마켓인가?")
        self.assertTrue(flags.active)
        self.assertFalse(flags.closed)
        self.assertFalse(flags.archived)
        self.assertTrue(flags.accepting_orders)
        self.assertTrue(flags.enable_order_book)
        self.assertEqual(flags.token_ids, ["token-a", "token-b"])

    def test_accepting_orders_defaults_to_active_and_not_closed(self):
        raw = {
            "id": "market-1",
            "active": True,
            "closed": False,
            "enableOrderBook": True,
            "clobTokenIds": ["token-a"],
        }

        flags = extract_raw_market_flags(raw)

        self.assertIsNotNone(flags)
        assert flags is not None
        self.assertTrue(flags.accepting_orders)

    def test_missing_orderbook_flag_defaults_to_false(self):
        raw = {"id": "market-1", "active": True, "clobTokenIds": ["token-a"]}

        flags = extract_raw_market_flags(raw)

        self.assertIsNotNone(flags)
        assert flags is not None
        self.assertFalse(flags.enable_order_book)

    def test_normalize_returns_none_without_market_id(self):
        raw = {"slug": "missing-id", "clobTokenIds": ["token-a"]}

        self.assertIsNone(normalize_gamma_market(raw, MarketLifecycleState.ACTIVE))

    def test_normalize_builds_market_info_with_given_lifecycle(self):
        raw = {
            "id": "market-1",
            "slug": "sample-market",
            "question": "샘플인가?",
            "active": True,
            "closed": False,
            "archived": False,
            "accepting_orders": True,
            "enable_order_book": True,
            "clob_token_ids": ["token-a", "token-b"],
        }

        market = normalize_gamma_market(raw, MarketLifecycleState.DISCOVERED)

        self.assertIsNotNone(market)
        assert market is not None
        self.assertEqual(market.market_id, "market-1")
        self.assertEqual(market.token_ids, ["token-a", "token-b"])
        self.assertEqual(market.lifecycle_state, MarketLifecycleState.DISCOVERED)

    def test_excluded_lifecycle_can_represent_missing_tokens(self):
        raw = {
            "id": "market-1",
            "active": True,
            "closed": False,
            "archived": False,
            "enable_order_book": False,
        }

        market = normalize_gamma_market(raw, MarketLifecycleState.EXCLUDED)

        self.assertIsNotNone(market)
        assert market is not None
        self.assertEqual(market.token_ids, [])
        self.assertEqual(market.lifecycle_state, MarketLifecycleState.EXCLUDED)


if __name__ == "__main__":
    unittest.main()
