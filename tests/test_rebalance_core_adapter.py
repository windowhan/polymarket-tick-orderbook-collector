"""rebalance_core adapter protocol의 기본 동작을 검증합니다."""

import unittest

from src.orchestrator.rebalance_core import (
    ConstraintDecision,
    DefaultAssignmentAdapter,
    NodeSpec,
    PlannerContext,
    ResourceVector,
    TaskSpec,
)


class ConstraintDecisionTests(unittest.TestCase):
    def test_allow_and_reject_helpers(self) -> None:
        allowed = ConstraintDecision.allow(reason="ok")
        rejected = ConstraintDecision.reject("blocked", "테스트 거절")
        self.assertTrue(allowed.allowed)
        self.assertEqual(allowed.code, "allowed")
        self.assertFalse(rejected.allowed)
        self.assertEqual(rejected.code, "blocked")
        self.assertEqual(rejected.reason, "테스트 거절")

    def test_empty_code_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            ConstraintDecision(True, code="")


class DefaultAssignmentAdapterTests(unittest.TestCase):
    def test_default_adapter_allows_every_pair_with_zero_score(self) -> None:
        adapter = DefaultAssignmentAdapter()
        task = TaskSpec("task-a", ResourceVector({"markets": 1}))
        node = NodeSpec("node-a", ResourceVector({"markets": 10}))
        context = PlannerContext()

        decision = adapter.can_assign(task, node, context)
        score = adapter.score_assignment(task, node, context)

        self.assertTrue(decision.allowed)
        self.assertEqual(decision.code, "allowed")
        self.assertEqual(score, 0.0)
