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
    # 샘플 입력: previous task-old/node-old, current task-new/node-new
    # 기대 출력: task_added/task_removed/node_added/node_removed reason
    def test_detects_task_and_node_add_remove(self) -> None:
        """task와 node의 추가/삭제가 rebalance reason으로 기록되는 상황을 검증합니다."""
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

    # 샘플 입력: node-a runtime unavailable, node-b accepts_new_tasks=False
    # 기대 출력: node_unavailable과 node_not_accepting_new_tasks reason
    def test_detects_unavailable_and_not_accepting_nodes(self) -> None:
        """unavailable node와 신규 배정 차단 node가 각각 다른 reason으로 기록되는 상황을 검증합니다."""
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

    # 샘플 입력: previous/current task와 node가 동일
    # 기대 출력: RebalanceReason(no_changes, planner, inputs)
    def test_reports_no_changes(self) -> None:
        """입력 변화가 없을 때 no_changes reason으로 명시하는 상황을 검증합니다."""
        tasks = [TaskSpec("task-a", ResourceVector({"markets": 1}))]
        nodes = [NodeSpec("node-a", ResourceVector({"markets": 1}))]

        reasons = detect_rebalance_reasons(tasks, tasks, nodes, nodes)

        self.assertEqual(reasons, (RebalanceReason("no_changes", "planner", "inputs"),))
