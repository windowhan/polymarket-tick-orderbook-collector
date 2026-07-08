"""rebalance_core의 generic rebalance reason 계산을 검증합니다."""

import unittest

from src.orchestrator.rebalance_core import (
    NodeRuntimeHint,
    NodeSpec,
    PlannerContext,
    RebalanceReason,
    ResourceVector,
    TaskSpec,
    detect_rebalance_reasons,
)


class RebalanceReasonTests(unittest.TestCase):
    def test_detects_task_and_node_add_remove(self) -> None:
        previous_tasks = [TaskSpec("task-old", ResourceVector({"markets": 1}))]
        current_tasks = [TaskSpec("task-new", ResourceVector({"markets": 1}))]
        previous_nodes = [NodeSpec("node-old", ResourceVector({"markets": 1}))]
        current_nodes = [NodeSpec("node-new", ResourceVector({"markets": 1}))]

        reasons = detect_rebalance_reasons(previous_tasks, current_tasks, previous_nodes, current_nodes)

        self.assertEqual(
            reasons,
            (
                RebalanceReason("task_added", "task", "task-new"),
                RebalanceReason("task_removed", "task", "task-old"),
                RebalanceReason("node_added", "node", "node-new"),
                RebalanceReason("node_removed", "node", "node-old"),
            ),
        )

    def test_detects_unavailable_and_not_accepting_nodes(self) -> None:
        tasks = [TaskSpec("task-a", ResourceVector({"markets": 1}))]
        nodes = [
            NodeSpec("node-a", ResourceVector({"markets": 1})),
            NodeSpec("node-b", ResourceVector({"markets": 1}), accepts_new_tasks=False),
        ]
        context = PlannerContext(
            runtime_hints={
                "node-a": NodeRuntimeHint(
                    node_id="node-a",
                    available=False,
                    reason="heartbeat_timeout",
                )
            }
        )

        reasons = detect_rebalance_reasons(tasks, tasks, nodes, nodes, context)

        self.assertEqual(
            reasons,
            (
                RebalanceReason("node_unavailable", "node", "node-a", "heartbeat_timeout"),
                RebalanceReason("node_not_accepting_new_tasks", "node", "node-b"),
            ),
        )

    def test_reports_no_changes(self) -> None:
        tasks = [TaskSpec("task-a", ResourceVector({"markets": 1}))]
        nodes = [NodeSpec("node-a", ResourceVector({"markets": 1}))]

        reasons = detect_rebalance_reasons(tasks, tasks, nodes, nodes)

        self.assertEqual(reasons, (RebalanceReason("no_changes", "planner", "inputs"),))
