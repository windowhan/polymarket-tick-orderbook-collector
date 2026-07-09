import importlib
import unittest

from src.orchestrator import CollectorCapacity


class OrchestratorPackageImportTests(unittest.TestCase):
    # 샘플 입력: market_manager/assigner/autoscale/compactor/viewer import
    # 기대 출력: 모든 subpackage import 성공
    def test_subpackages_are_importable(self):
        """예약된 orchestrator 하위 패키지들이 import 가능한 상태를 유지하는지 검증합니다."""
        for module_name in (
            "src.orchestrator.assigner",
            "src.orchestrator.autoscale",
            "src.orchestrator.compactor",
            "src.orchestrator.market_manager",
            "src.orchestrator.viewer",
        ):
            with self.subTest(module_name=module_name):
                self.assertIsNotNone(importlib.import_module(module_name))

    # 샘플 입력: from src.orchestrator import MarketInfo
    # 기대 출력: MarketInfo import 성공
    def test_contract_export_still_works(self):
        """orchestrator package가 계약 타입 export를 깨뜨리지 않는 상황을 검증합니다."""
        capacity = CollectorCapacity(
            max_market_subscriptions=2,
            max_token_subscriptions=4,
            max_ws_connections=1,
            max_events_per_sec=None,
            max_upload_backlog_files=10,
        )

        self.assertTrue(capacity.fits_subscription_counts(2, 4, 1))
        self.assertFalse(capacity.fits_subscription_counts(3, 4, 1))


if __name__ == "__main__":
    unittest.main()
    # 샘플 입력: domains.polymarket과 하위 namespace package import
    # 기대 출력: 모든 domain package import 성공
    def test_polymarket_domain_packages_are_importable(self):
        """새 Polymarket domain package skeleton이 import 가능한 상황을 검증합니다."""
        import src.orchestrator.domains
        import src.orchestrator.domains.polymarket
        import src.orchestrator.domains.polymarket.assigner
        import src.orchestrator.domains.polymarket.market_manager

        self.assertIsNotNone(src.orchestrator.domains)
