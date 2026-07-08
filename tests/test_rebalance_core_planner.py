"""rebalance_core planner의 capacity, stability, 결정성을 검증합니다."""

import unittest

from src.orchestrator.rebalance_core import (
    Assignment,
    ConstraintDecision,
    NodeSpec,
    PlannerContext,
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
            (Assignment("task-a", "node-a"), Assignment("task-b", "node-a")),
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

        self.assertEqual(plan.assignments, (Assignment("task-a", "node-a"),))


class AssignmentPlannerStabilityTests(unittest.TestCase):
    def test_keeps_valid_previous_assignment_before_score(self) -> None:
        tasks = [TaskSpec("task-a", ResourceVector({"markets": 1}))]
        nodes = [
            NodeSpec("node-a", ResourceVector({"markets": 1})),
            NodeSpec("node-b", ResourceVector({"markets": 1})),
        ]

        plan = build_assignment_plan(
            tasks,
            nodes,
            previous_assignments=[Assignment("task-a", "node-a")],
            adapter=_PreferNodeBAdapter(),
        )

        self.assertEqual(plan.assignments, (Assignment("task-a", "node-a"),))

    def test_input_order_does_not_change_plan(self) -> None:
        tasks = [
            TaskSpec("task-b", ResourceVector({"markets": 1})),
            TaskSpec("task-a", ResourceVector({"markets": 1})),
        ]
        nodes = [
            NodeSpec("node-b", ResourceVector({"markets": 2})),
            NodeSpec("node-a", ResourceVector({"markets": 2})),
        ]

        first = build_assignment_plan(tasks, nodes)
        second = build_assignment_plan(list(reversed(tasks)), list(reversed(nodes)))

        self.assertEqual(first.assignments, second.assignments)

    def test_callback_reject_takes_precedence(self) -> None:
        tasks = [TaskSpec("task-a", ResourceVector({"markets": 1}))]
        nodes = [
            NodeSpec("node-a", ResourceVector({"markets": 1})),
            NodeSpec("node-b", ResourceVector({"markets": 1})),
        ]

        plan = build_assignment_plan(tasks, nodes, adapter=_RejectNodeAAdapter())

        self.assertEqual(plan.assignments, (Assignment("task-a", "node-b"),))


class _PreferNodeBAdapter:
    def can_assign(self, task: TaskSpec, node: NodeSpec, context: PlannerContext):
        return ConstraintDecision.allow()

    def score_assignment(self, task: TaskSpec, node: NodeSpec, context: PlannerContext):
        return {"node-b": 10.0}.get(node.node_id, 0.0)


class _RejectNodeAAdapter:
    def can_assign(self, task: TaskSpec, node: NodeSpec, context: PlannerContext):
        # node-a는 adapter 거절 우선순위를 검증하기 위해 차단합니다.
        if node.node_id == "node-a":
            return ConstraintDecision.reject("blocked_node", "node-a 거절")
        return ConstraintDecision.allow()

    def score_assignment(self, task: TaskSpec, node: NodeSpec, context: PlannerContext):
        return 0.0
