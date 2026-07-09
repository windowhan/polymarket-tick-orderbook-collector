import unittest

from src.orchestrator.contracts import MarketLifecycleState
from src.orchestrator.domains.polymarket.market_manager.normalizer import (
    extract_market_id,
    extract_raw_market_flags,
    extract_token_ids,
    normalize_gamma_market,
)


class MarketManagerNormalizerTests(unittest.TestCase):
    # 샘플 입력: clobTokenIds='["1","2"]'
    # 기대 출력: token_ids=['1','2']
    def test_extracts_token_ids_from_json_string(self):
        """Gamma가 token id 목록을 JSON 문자열로 줄 때 정상 list로 파싱하는 상황을 검증합니다."""
        raw = {"clobTokenIds": '["token-a", "token-b", "token-a"]'}

        self.assertEqual(extract_token_ids(raw), ["token-a", "token-b"])

    # 샘플 입력: tokens=[{token_id:t1},{id:t2}]
    # 기대 출력: token_ids=['t1','t2']
    def test_extracts_token_ids_from_token_objects(self):
        """Gamma tokens 객체 배열에서 token id 후보를 추출하는 상황을 검증합니다."""
        raw = {
            "tokens": [
                {"token_id": "yes-token"},
                {"id": "no-token"},
                {"tokenId": "maybe-token"},
            ]
        }

        self.assertEqual(extract_token_ids(raw), ["yes-token", "no-token", "maybe-token"])

    # 샘플 입력: id/market_id 없음, conditionId='cond-1'
    # 기대 출력: market_id='cond-1'
    def test_market_id_uses_condition_id_fallback(self):
        """기본 id가 없을 때 conditionId를 market id fallback으로 사용하는 상황을 검증합니다."""
        raw = {"conditionId": "condition-1"}

        self.assertEqual(extract_market_id(raw), "condition-1")

    # 샘플 입력: active/closed/archived와 acceptingOrders/enableOrderBook camelCase payload
    # 기대 출력: RawMarketFlags가 camelCase 값을 bool과 token list로 보존
    def test_raw_flags_accept_camel_case_gamma_fields(self):
        """Gamma camelCase 필드를 lifecycle 판단용 raw flag로 받아들이는 상황을 검증합니다."""
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

    # 샘플 입력: accepting_orders 없음, active=True, closed=False
    # 기대 출력: accepting_orders=True
    def test_accepting_orders_defaults_to_active_and_not_closed(self):
        """accepting_orders 값이 없을 때 active/closed 조합으로 기본값을 계산하는 상황을 검증합니다."""
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

    # 샘플 입력: enable_order_book 계열 field 없음
    # 기대 출력: enable_order_book=False
    def test_missing_orderbook_flag_defaults_to_false(self):
        """orderbook flag가 없으면 안전하게 비활성으로 보는 상황을 검증합니다."""
        raw = {"id": "market-1", "active": True, "clobTokenIds": ["token-a"]}

        flags = extract_raw_market_flags(raw)

        self.assertIsNotNone(flags)
        assert flags is not None
        self.assertFalse(flags.enable_order_book)

    # 샘플 입력: market id 후보가 없는 raw payload
    # 기대 출력: normalize_gamma_market() is None
    def test_normalize_returns_none_without_market_id(self):
        """market id 후보가 전혀 없으면 정규화 결과를 만들지 않는 상황을 검증합니다."""
        raw = {"slug": "missing-id", "clobTokenIds": ["token-a"]}

        self.assertIsNone(normalize_gamma_market(raw, MarketLifecycleState.ACTIVE))

    # 샘플 입력: valid raw payload와 lifecycle_state=ACTIVE
    # 기대 출력: MarketInfo에 id/token/lifecycle이 채워짐
    def test_normalize_builds_market_info_with_given_lifecycle(self):
        """계산된 lifecycle state를 보존해 MarketInfo를 생성하는 상황을 검증합니다."""
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

    # 샘플 입력: token id 없는 raw payload와 lifecycle_state=EXCLUDED
    # 기대 출력: MarketInfo.lifecycle_state=EXCLUDED
    def test_excluded_lifecycle_can_represent_missing_tokens(self):
        """token 누락 같은 제외 사유를 EXCLUDED lifecycle로 표현할 수 있는 상황을 검증합니다."""
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
