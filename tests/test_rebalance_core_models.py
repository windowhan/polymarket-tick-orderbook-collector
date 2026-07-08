"""rebalance_core model 타입의 순수 동작을 검증합니다."""

import unittest
from math import inf
from src.orchestrator.rebalance_core import (
    NodeRuntimeHint,
    NodeSpec,
    PlannerContext,
    ResourceVector,
    TaskSpec,
)


class ResourceVectorTests(unittest.TestCase):
    def test_plus_and_missing_keys(self) -> None:
        base = ResourceVector({"markets": 1, "tokens": 2})
        extra = ResourceVector({"tokens": 3, "ws_connections": 1})
        result = base.plus(extra)
        self.assertEqual(result.get("markets"), 1.0)
        self.assertEqual(result.get("tokens"), 5.0)
        self.assertEqual(result.get("ws_connections"), 1.0)
        self.assertEqual(result.get("missing"), 0.0)

    def test_fits_within_checks_each_dimension(self) -> None:
        usage = ResourceVector({"markets": 2, "tokens": 5})
        self.assertTrue(usage.fits_within(ResourceVector({"markets": 2, "tokens": 5})))
        self.assertFalse(usage.fits_within(ResourceVector({"markets": 2, "tokens": 4})))
        self.assertFalse(usage.fits_within(ResourceVector({"markets": 2})))

    def test_utilization_uses_max_ratio(self) -> None:
        usage = ResourceVector({"markets": 2, "tokens": 5})
        capacity = ResourceVector({"markets": 4, "tokens": 10})
        self.assertEqual(usage.utilization_against(capacity), 0.5)
        self.assertEqual(usage.utilization_against(ResourceVector({"markets": 4})), inf)

    def test_negative_resource_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            ResourceVector({"tokens": -1})


class PlannerModelTests(unittest.TestCase):
    def test_task_and_node_reject_empty_ids(self) -> None:
        with self.assertRaises(ValueError):
            TaskSpec("", ResourceVector({"markets": 1}))
        with self.assertRaises(ValueError):
            NodeSpec("", ResourceVector({"markets": 1}))

    def test_context_normalizes_runtime_hints_by_node_id(self) -> None:
        context = PlannerContext(
            runtime_hints={
                "stale-key": NodeRuntimeHint(
                    node_id="collector-a",
                    available=False,
                    reason="heartbeat_timeout",
                )
            }
        )
        self.assertFalse(context.hint_for("collector-a").available)
        self.assertEqual(context.hint_for("collector-a").reason, "heartbeat_timeout")
        self.assertTrue(context.hint_for("collector-b").available)
