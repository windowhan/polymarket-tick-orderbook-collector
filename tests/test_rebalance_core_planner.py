"""rebalance_core planner의 capacity 기반 배정을 검증합니다."""

import unittest

from src.orchestrator.rebalance_core import (
    NodeSpec,
    ResourceVector,
    TaskSpec,
    build_assignment_plan,
)


class AssignmentPlannerCapacityTests(unittest.TestCase):
    def test_assigns_tasks_without_exceeding_capacity(self) -> None:
        tasks = [
            TaskSpec("task-b", ResourceVector({"markets": 1, "tokens": 2})),
            TaskSpec("task-a", ResourceVector({"markets": 1, "tokens": 2})),
        ]
        nodes = [NodeSpec("node-a", ResourceVector({"markets": 2, "tokens": 4}))]

        plan = build_assignment_plan(tasks, nodes)

        self.assertEqual(
            plan.assignments,
            (
                self._assignment("task-a", "node-a"),
                self._assignment("task-b", "node-a"),
            ),
        )
        self.assertEqual(plan.resource_usage_by_node["node-a"].get("tokens"), 4.0)
        self.assertEqual(plan.unassigned_task_ids, ())

    def test_leaves_task_unassigned_when_capacity_is_missing(self) -> None:
        tasks = [TaskSpec("task-a", ResourceVector({"tokens": 1}))]
        nodes = [NodeSpec("node-a", ResourceVector({"markets": 1}))]

        plan = build_assignment_plan(tasks, nodes)

        self.assertEqual(plan.assignments, ())
        self.assertEqual(plan.unassigned_task_ids, ("task-a",))
        self.assertEqual(plan.reasons["task-a"], ("node-a:capacity_exceeded",))

    def test_uses_stable_node_tie_break(self) -> None:
        tasks = [TaskSpec("task-a", ResourceVector({"markets": 1}))]
        nodes = [
            NodeSpec("node-b", ResourceVector({"markets": 1})),
            NodeSpec("node-a", ResourceVector({"markets": 1})),
        ]

        plan = build_assignment_plan(tasks, nodes)

        self.assertEqual(plan.assignments, (self._assignment("task-a", "node-a"),))

    @staticmethod
    def _assignment(task_id: str, node_id: str):
        from src.orchestrator.rebalance_core import Assignment

        return Assignment(task_id=task_id, node_id=node_id)
