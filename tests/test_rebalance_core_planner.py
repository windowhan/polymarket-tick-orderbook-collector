"""rebalance_core planner의 capacity, stability, 결정성을 검증합니다."""

import unittest

from src.orchestrator.rebalance_core import (
    Assignment,
    ConstraintDecision,
    NodeRuntimeHint,
    NodeSpec,
    PlannerContext,
    ResourceVector,
    TaskSpec,
    build_assignment_plan,
)


class AssignmentPlannerCapacityTests(unittest.TestCase):
    # 샘플 입력: task-a/b 각 markets=1,tokens=2, node-a capacity markets=2,tokens=4
    # 기대 출력: task-a,b 모두 node-a 배정, tokens usage=4
    def test_assigns_tasks_without_exceeding_capacity(self) -> None:
        """여러 task가 node capacity를 넘지 않을 때 stable order로 모두 배정되는 상황을 검증합니다."""
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

    # 샘플 입력: task-a requires tokens=1, node-a capacity has only markets=1
    # 기대 출력: assignments=(), unassigned=(task-a), reason=capacity_exceeded
    def test_leaves_task_unassigned_when_capacity_is_missing(self) -> None:
        """필요 resource capacity가 없는 task를 미배정으로 남기고 reason을 기록하는 상황을 검증합니다."""
        tasks = [TaskSpec("task-a", ResourceVector({"tokens": 1}))]
        nodes = [NodeSpec("node-a", ResourceVector({"markets": 1}))]

        plan = build_assignment_plan(tasks, nodes)

        self.assertEqual(plan.assignments, ())
        self.assertEqual(plan.unassigned_task_ids, ("task-a",))
        self.assertEqual(plan.reasons["task-a"], ("node-a:capacity_exceeded",))

    # 샘플 입력: 동일 capacity node-b/node-a와 task-a
    # 기대 출력: node id tie-break로 task-a -> node-a
    def test_uses_stable_node_tie_break(self) -> None:
        """동점 후보가 여러 node일 때 node id 기준 tie-break가 적용되는 상황을 검증합니다."""
        tasks = [TaskSpec("task-a", ResourceVector({"markets": 1}))]
        nodes = [
            NodeSpec("node-b", ResourceVector({"markets": 1})),
            NodeSpec("node-a", ResourceVector({"markets": 1})),
        ]

        plan = build_assignment_plan(tasks, nodes)

        self.assertEqual(plan.assignments, (Assignment("task-a", "node-a"),))


class AssignmentPlannerStabilityTests(unittest.TestCase):
    # 샘플 입력: previous task-a->node-a, adapter는 node-b 선호
    # 기대 출력: task-a는 node-a에 유지
    def test_keeps_valid_previous_assignment_before_score(self) -> None:
        """유효한 이전 배정이 score가 높은 새 후보보다 우선되는 상황을 검증합니다."""
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

    # 샘플 입력: tasks/nodes 순서를 뒤집어 두 번 planner 실행
    # 기대 출력: 두 plan.assignments가 동일
    def test_input_order_does_not_change_plan(self) -> None:
        """task/node 입력 순서가 달라도 동일한 assignment 결과가 나오는 상황을 검증합니다."""
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

    # 샘플 입력: node-a는 adapter reject, node-b는 allow
    # 기대 출력: task-a -> node-b
    def test_callback_reject_takes_precedence(self) -> None:
        """adapter reject가 capacity와 score보다 우선해 해당 node 배정을 막는 상황을 검증합니다."""
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


class AssignmentPlannerRuntimeHintTests(unittest.TestCase):
    # 샘플 입력: node-a runtime available=False, node-b available=True
    # 기대 출력: task-a -> node-b
    def test_runtime_unavailable_node_is_excluded(self) -> None:
        """runtime hint가 unavailable인 node를 신규 배정 후보에서 제외하는 상황을 검증합니다."""
        tasks = [TaskSpec("task-a", ResourceVector({"markets": 1}))]
        nodes = [
            NodeSpec("node-a", ResourceVector({"markets": 1})),
            NodeSpec("node-b", ResourceVector({"markets": 1})),
        ]
        context = PlannerContext(
            runtime_hints={"node-a": NodeRuntimeHint(node_id="node-a", available=False)}
        )

        plan = build_assignment_plan(tasks, nodes, context=context)

        self.assertEqual(plan.assignments, (Assignment("task-a", "node-b"),))

    # 샘플 입력: node-a accepts_new_tasks=False에 previous task-a, 새 task-b
    # 기대 출력: task-a는 node-a 유지, task-b는 node-b 배정
    def test_not_accepting_new_tasks_keeps_previous_but_blocks_new(self) -> None:
        """신규 task 차단 node가 기존 배정은 유지하고 새 task만 받지 않는 상황을 검증합니다."""
        tasks = [
            TaskSpec("task-a", ResourceVector({"markets": 1})),
            TaskSpec("task-b", ResourceVector({"markets": 1})),
        ]
        nodes = [
            NodeSpec("node-a", ResourceVector({"markets": 2}), accepts_new_tasks=False),
            NodeSpec("node-b", ResourceVector({"markets": 1})),
        ]

        plan = build_assignment_plan(
            tasks,
            nodes,
            previous_assignments=[Assignment("task-a", "node-a")],
        )

        self.assertEqual(
            plan.assignments,
            (Assignment("task-a", "node-a"), Assignment("task-b", "node-b")),
        )

    # 샘플 입력: node-a accepts_new_tasks=False, 신규 task-a
    # 기대 출력: task-a 미배정, reason=not_accepting_new_tasks
    def test_not_accepting_new_tasks_reason_is_reported(self) -> None:
        """신규 task 차단 때문에 미배정된 task에 reason이 남는 상황을 검증합니다."""
        tasks = [TaskSpec("task-a", ResourceVector({"markets": 1}))]
        nodes = [NodeSpec("node-a", ResourceVector({"markets": 1}), accepts_new_tasks=False)]

        plan = build_assignment_plan(tasks, nodes)

        self.assertEqual(plan.unassigned_task_ids, ("task-a",))
        self.assertEqual(plan.reasons["task-a"], ("node-a:not_accepting_new_tasks",))
