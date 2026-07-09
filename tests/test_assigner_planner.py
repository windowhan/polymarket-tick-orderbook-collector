"""Polymarket assignment planner wrapper를 검증합니다."""

import unittest

from src.orchestrator.assigner import PolymarketAdapterPolicy, build_polymarket_assignment_plan
from src.orchestrator.contracts import (
    AssignmentLimits,
    AssignmentPlan,
    CollectorAssignment,
    CollectorCapacity,
    ControlState,
    MarketInfo,
    MarketLifecycleState,
    MarketUniverseSnapshot,
)


class PolymarketAssignmentPlannerTests(unittest.TestCase):
    # 샘플 입력: universe m1(t1,t2), collector-a capacity(markets=2,tokens=4,ws=1), version=7
    # 기대 출력: collector-a에 market_ids=[m1], token_ids=[t1,t2], version=7인 AssignmentPlan
    def test_builds_assignment_plan_from_generic_core(self) -> None:
        """generic core 결과가 기존 Collector AssignmentPlan 계약으로 변환되는 정상 상황을 검증합니다."""
        universe = MarketUniverseSnapshot(1, 1000, {"m1": self._market("m1", ["t1", "t2"])})
        capacities = {"collector-a": CollectorCapacity(2, 4, 1, None, 10)}

        plan = build_polymarket_assignment_plan(universe, capacities, 7, 2000)

        assignment = plan.collectors["collector-a"]
        self.assertEqual(plan.version, 7)
        self.assertEqual(plan.universe_version, 1)
        self.assertEqual(assignment.market_ids, ["m1"])
        self.assertEqual(assignment.token_ids, ["t1", "t2"])
        self.assertEqual(assignment.limits.max_ws_connections, 1)

    # 샘플 입력: empty universe, control_state=EMERGENCY_STOP_BY_BUDGET
    # 기대 출력: collectors={}이고 control_state가 EMERGENCY_STOP_BY_BUDGET인 stop plan
    def test_hard_stop_bypasses_collectors(self) -> None:
        """예산 hard stop 상태에서는 core planner를 호출하지 않고 빈 중단 plan을 만드는 상황을 검증합니다."""
        universe = MarketUniverseSnapshot(3, 1000, {})

        plan = build_polymarket_assignment_plan(
            universe,
            {},
            8,
            2000,
            control_state=ControlState.EMERGENCY_STOP_BY_BUDGET,
        )

        self.assertEqual(plan.control_state, ControlState.EMERGENCY_STOP_BY_BUDGET)
        self.assertEqual(plan.collectors, {})

    # 샘플 입력: m1이 이전 plan에서 collector-b에 배정되어 있고 collector-a/b 모두 capacity 있음
    # 기대 출력: m1은 collector-b에 유지되고 collector-a는 빈 배정
    def test_previous_plan_keeps_existing_collector(self) -> None:
        """이전 배정이 유효하면 더 앞선 collector 후보가 있어도 기존 collector를 유지하는 상황을 검증합니다."""
        universe = MarketUniverseSnapshot(1, 1000, {"m1": self._market("m1", ["t1"])})
        capacities = {
            "collector-a": CollectorCapacity(1, 1, 1, None, 10),
            "collector-b": CollectorCapacity(1, 1, 1, None, 10),
        }
        previous = AssignmentPlan(
            1,
            1,
            900,
            ControlState.RUNNING,
            {
                "collector-b": CollectorAssignment(
                    "collector-b",
                    ["m1"],
                    ["t1"],
                    AssignmentLimits(1, PolymarketAdapterPolicy().max_tokens_per_ws_connection),
                )
            },
        )

        plan = build_polymarket_assignment_plan(universe, capacities, 2, 2000, previous_plan=previous)

        self.assertEqual(plan.collectors["collector-b"].market_ids, ["m1"])
        self.assertEqual(plan.collectors["collector-a"].market_ids, [])

    @staticmethod
    def _market(market_id: str, token_ids: list[str]) -> MarketInfo:
        return MarketInfo(market_id, market_id, market_id, True, False, False, True, True, token_ids, MarketLifecycleState.ACTIVE)
