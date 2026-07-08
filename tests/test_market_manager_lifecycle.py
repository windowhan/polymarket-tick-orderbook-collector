import unittest


@unittest.skip("lifecycle.py 구현 커밋에서 활성화할 테스트 골격입니다")
class MarketManagerLifecyclePendingTests(unittest.TestCase):
    def test_new_market_starts_as_discovered(self):
        pass

    def test_confirmed_new_market_becomes_active(self):
        pass

    def test_closed_market_enters_draining_before_removal(self):
        pass

    def test_archived_market_is_excluded_from_active_universe(self):
        pass

    def test_missing_token_ids_are_excluded(self):
        pass


if __name__ == "__main__":
    unittest.main()
