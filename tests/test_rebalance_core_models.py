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
    # 샘플 입력: base markets=1/tokens=2 + extra tokens=3/ws=1
    # 기대 출력: markets=1,tokens=5,ws=1,missing=0
    def test_plus_and_missing_keys(self) -> None:
        """resource vector 합산에서 없는 key를 0으로 취급하는 상황을 검증합니다."""
        base = ResourceVector({"markets": 1, "tokens": 2})
        extra = ResourceVector({"tokens": 3, "ws_connections": 1})
        result = base.plus(extra)
        self.assertEqual(result.get("markets"), 1.0)
        self.assertEqual(result.get("tokens"), 5.0)
        self.assertEqual(result.get("ws_connections"), 1.0)
        self.assertEqual(result.get("missing"), 0.0)

    # 샘플 입력: usage markets=2/tokens=5, capacity tokens=5/4/missing
    # 기대 출력: 각각 True/False/False
    def test_fits_within_checks_each_dimension(self) -> None:
        """모든 resource dimension이 capacity 안에 들어와야 fit으로 판단하는 상황을 검증합니다."""
        usage = ResourceVector({"markets": 2, "tokens": 5})
        self.assertTrue(usage.fits_within(ResourceVector({"markets": 2, "tokens": 5})))
        self.assertFalse(usage.fits_within(ResourceVector({"markets": 2, "tokens": 4})))
        self.assertFalse(usage.fits_within(ResourceVector({"markets": 2})))

    # 샘플 입력: usage markets=2/tokens=5, capacity markets=4/tokens=10 또는 tokens missing
    # 기대 출력: 사용률 0.5 또는 inf
    def test_utilization_uses_max_ratio(self) -> None:
        """node 사용률을 여러 resource dimension 중 최대 비율로 계산하는 상황을 검증합니다."""
        usage = ResourceVector({"markets": 2, "tokens": 5})
        capacity = ResourceVector({"markets": 4, "tokens": 10})
        self.assertEqual(usage.utilization_against(capacity), 0.5)
        self.assertEqual(usage.utilization_against(ResourceVector({"markets": 4})), inf)

    # 샘플 입력: ResourceVector({'tokens': -1})
    # 기대 출력: ValueError 발생
    def test_negative_resource_is_rejected(self) -> None:
        """음수 resource가 capacity 계산을 깨뜨리지 않도록 생성 시점에 거절하는 상황을 검증합니다."""
        with self.assertRaises(ValueError):
            ResourceVector({"tokens": -1})


class PlannerModelTests(unittest.TestCase):
    # 샘플 입력: TaskSpec('', ...), NodeSpec('', ...)
    # 기대 출력: 각각 ValueError 발생
    def test_task_and_node_reject_empty_ids(self) -> None:
        """빈 task/node id가 deterministic key를 깨뜨리므로 거절되는 상황을 검증합니다."""
        with self.assertRaises(ValueError):
            TaskSpec("", ResourceVector({"markets": 1}))
        with self.assertRaises(ValueError):
            NodeSpec("", ResourceVector({"markets": 1}))

    # 샘플 입력: runtime_hints key는 stale-key, hint.node_id=collector-a
    # 기대 출력: collector-a hint 조회 성공, collector-b는 기본 available hint
    def test_context_normalizes_runtime_hints_by_node_id(self) -> None:
        """runtime hint 입력 key가 틀려도 hint 내부 node_id 기준으로 정규화되는 상황을 검증합니다."""
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
