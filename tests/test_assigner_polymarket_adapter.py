"""Polymarket adapter의 generic core 변환을 검증합니다."""

import unittest

from src.orchestrator.assigner import (
    PolymarketAdapterPolicy,
    ShardWeightEstimate,
    capacities_to_node_specs,
    estimate_ws_connections,
    snapshot_to_task_specs,
    status_to_runtime_hint,
)
from src.orchestrator.contracts import (
    CollectorCapacity,
    CollectorStatus,
    MarketInfo,
    MarketLifecycleState,
    MarketUniverseSnapshot,
)


class PolymarketAdapterTaskTests(unittest.TestCase):
    # 샘플 입력: snapshot m0(1 token), m1(3 tokens), m1 events_per_sec=12.5, ws chunk=2
    # 기대 출력: task 순서 [m0,m1], m1 tokens=3/ws=2/events=12.5, m0 events=0
    def test_snapshot_to_task_specs_adds_optional_event_weight(self) -> None:
        """market snapshot을 task로 바꾸면서 선택적 처리량 추정치가 있을 때만 resource를 추가하는 상황을 검증합니다."""
        snapshot = MarketUniverseSnapshot(
            version=1,
            generated_at_ms=1000,
            markets={"m1": self._market("m1", ["t1", "t2", "t3"]), "m0": self._market("m0", ["t0"])},
        )
        policy = PolymarketAdapterPolicy(max_tokens_per_ws_connection=2)

        tasks = snapshot_to_task_specs(
            snapshot,
            policy,
            {"m1": ShardWeightEstimate(events_per_sec=12.5)},
        )

        self.assertEqual([task.task_id for task in tasks], ["m0", "m1"])
        self.assertEqual(tasks[1].resources.get("tokens"), 3.0)
        self.assertEqual(tasks[1].resources.get("ws_connections"), 2.0)
        self.assertEqual(tasks[1].resources.get("events_per_sec"), 12.5)
        self.assertEqual(tasks[0].resources.get("events_per_sec"), 0.0)

    @staticmethod
    def _market(market_id: str, token_ids: list[str]) -> MarketInfo:
        return MarketInfo(market_id, market_id, market_id, True, False, False, True, True, token_ids, MarketLifecycleState.ACTIVE)


class PolymarketAdapterNodeTests(unittest.TestCase):
    # 샘플 입력: collector-a max_events_per_sec=30.0, collector-b max_events_per_sec=None
    # 기대 출력: node 순서 [collector-a,collector-b], a events=30.0, b events=0
    def test_capacity_to_node_specs_keeps_optional_events_capacity(self) -> None:
        """collector capacity를 node로 바꿀 때 선택적 events_per_sec capacity를 보존하는 상황을 검증합니다."""
        nodes = capacities_to_node_specs(
            {
                "collector-b": CollectorCapacity(10, 20, 2, None, 5),
                "collector-a": CollectorCapacity(1, 2, 1, 30.0, 3),
            }
        )

        self.assertEqual([node.node_id for node in nodes], ["collector-a", "collector-b"])
        self.assertEqual(nodes[0].capacity.get("events_per_sec"), 30.0)
        self.assertEqual(nodes[1].capacity.get("events_per_sec"), 0.0)

    # 샘플 입력: upload_backlog_files=3, max_upload_backlog_files=3
    # 기대 출력: available=True, accepts_new_tasks=False, reason=upload_backlog_limit
    def test_status_to_runtime_hint_blocks_new_tasks_on_backlog(self) -> None:
        """upload backlog가 한계에 도달한 collector가 기존 배정은 유지하되 신규 배정을 막는 상황을 검증합니다."""
        status = CollectorStatus("collector-a", 1, 1, 2, 1, 2, 1, 0.0, 0, 3, 0, None)
        capacity = CollectorCapacity(10, 20, 2, None, 3)

        hint = status_to_runtime_hint(status, capacity, PolymarketAdapterPolicy())

        self.assertTrue(hint.available)
        self.assertFalse(hint.accepts_new_tasks)
        self.assertEqual(hint.reason, "upload_backlog_limit")

    # 샘플 입력: token_count 0/1/3, max_tokens_per_ws_connection=2
    # 기대 출력: 예상 ws 연결 수 0/1/2
    def test_estimate_ws_connections_uses_policy_chunk_size(self) -> None:
        """token 수와 정책 chunk 크기로 보수적인 WebSocket 연결 수를 추정하는 상황을 검증합니다."""
        policy = PolymarketAdapterPolicy(max_tokens_per_ws_connection=2)

        self.assertEqual(estimate_ws_connections(0, policy), 0)
        self.assertEqual(estimate_ws_connections(1, policy), 1)
        self.assertEqual(estimate_ws_connections(3, policy), 2)
