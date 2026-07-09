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
        """adapter decision helper가 허용/거절 결과와 reason을 올바르게 만드는 상황을 검증합니다."""
        allowed = ConstraintDecision.allow(reason="ok")
        rejected = ConstraintDecision.reject("blocked", "테스트 거절")
        self.assertTrue(allowed.allowed)
        self.assertEqual(allowed.code, "allowed")
        self.assertFalse(rejected.allowed)
        self.assertEqual(rejected.code, "blocked")
        self.assertEqual(rejected.reason, "테스트 거절")

    def test_empty_code_is_rejected(self) -> None:
        """비어 있는 decision code를 거절해 reason 집계가 깨지지 않는 상황을 검증합니다."""
        with self.assertRaises(ValueError):
            ConstraintDecision(True, code="")


class DefaultAssignmentAdapterTests(unittest.TestCase):
    def test_default_adapter_allows_every_pair_with_zero_score(self) -> None:
        """기본 adapter가 모든 pair를 허용하고 동일 선호 점수를 주는 상황을 검증합니다."""
        adapter = DefaultAssignmentAdapter()
        task = TaskSpec("task-a", ResourceVector({"markets": 1}))
        node = NodeSpec("node-a", ResourceVector({"markets": 10}))
        context = PlannerContext()

        decision = adapter.can_assign(task, node, context)
        score = adapter.score_assignment(task, node, context)

        self.assertTrue(decision.allowed)
        self.assertEqual(decision.code, "allowed")
        self.assertEqual(score, 0.0)
