import importlib
import unittest

from src.orchestrator import CollectorCapacity


class OrchestratorPackageImportTests(unittest.TestCase):
    def test_subpackages_are_importable(self):
        for module_name in (
            "src.orchestrator.assigner",
            "src.orchestrator.autoscale",
            "src.orchestrator.compactor",
            "src.orchestrator.market_manager",
            "src.orchestrator.viewer",
        ):
            with self.subTest(module_name=module_name):
                self.assertIsNotNone(importlib.import_module(module_name))

    def test_contract_export_still_works(self):
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
