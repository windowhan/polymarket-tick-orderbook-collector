"""Polymarket/Gamma/CLOB 도메인 계약 dataclass 모음입니다."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from src.orchestrator.core.control import ControlState


class MarketLifecycleState(str, Enum):
    """Market Universe Manager가 부여하는 마켓 생명주기 상태입니다."""

    DISCOVERED = "DISCOVERED"
    ACTIVE = "ACTIVE"
    DRAINING = "DRAINING"
    CLOSED = "CLOSED"
    ARCHIVED = "ARCHIVED"
    EXCLUDED = "EXCLUDED"


class OrderbookEventType(str, Enum):
    """Collector가 JSONL에 기록하는 정규화된 이벤트 종류입니다."""

    BOOK = "book"
    PRICE_CHANGE = "price_change"
    LAST_TRADE = "last_trade"


class HandoffMode(str, Enum):
    """구조적 shard handoff에 사용할 방식입니다."""

    NONE = "NONE"
    MAKE_BEFORE_BREAK = "MAKE_BEFORE_BREAK"
    BREAK_BEFORE_MAKE = "BREAK_BEFORE_MAKE"
    DRAIN_ONLY = "DRAIN_ONLY"


@dataclass(frozen=True)
class MarketInfo:
    """배정 planner가 사용하는 정규화된 마켓 메타데이터입니다."""

    market_id: str
    slug: str
    question: str
    active: bool
    closed: bool
    archived: bool
    accepting_orders: bool
    enable_order_book: bool
    token_ids: list[str]
    lifecycle_state: MarketLifecycleState


@dataclass(frozen=True)
class MarketUniverseSnapshot:
    """계획 판단 대상 마켓을 담은 버전 관리 스냅샷입니다."""

    version: int
    generated_at_ms: int
    markets: dict[str, MarketInfo] = field(default_factory=dict)


@dataclass(frozen=True)
class CollectorCapacity:
    """과배정을 막기 위해 Collector가 선언하는 처리 용량입니다."""

    max_market_subscriptions: int
    max_token_subscriptions: int
    max_ws_connections: int
    max_events_per_sec: Optional[float]
    max_upload_backlog_files: int

    def fits_subscription_counts(
        self,
        market_count: int,
        token_count: int,
        ws_connections: int,
    ) -> bool:
        """계획된 수치가 이 Collector의 용량 안에 들어오는지 반환합니다."""

        return (
            market_count <= self.max_market_subscriptions
            and token_count <= self.max_token_subscriptions
            and ws_connections <= self.max_ws_connections
        )


@dataclass(frozen=True)
class CollectorStatus:
    """Collector가 보고하는 런타임 상태입니다."""

    collector_id: str
    assignment_version: int
    assigned_market_count: int
    assigned_token_count: int
    subscribed_market_count: int
    subscribed_token_count: int
    active_ws_connections: int
    events_per_sec: float
    reconnects_last_minute: int
    upload_backlog_files: int
    local_spool_bytes: int
    last_successful_upload_at_ms: Optional[int]

    def assignment_matches_subscription(self) -> bool:
        """실제 구독 수가 배정 수와 일치하는지 반환합니다."""

        return (
            self.assigned_market_count == self.subscribed_market_count
            and self.assigned_token_count == self.subscribed_token_count
        )


@dataclass(frozen=True)
class AssignmentLimits:
    """단일 배정과 함께 Collector에 전달하는 worker 분할 제한입니다."""

    max_ws_connections: int
    max_tokens_per_ws_connection: int


@dataclass(frozen=True)
class HandoffAction:
    """token ID 묶음에 대해 계획된 구조적 handoff입니다."""

    token_ids: list[str]
    from_collector_id: Optional[str]
    to_collector_id: Optional[str]
    mode: HandoffMode
    overlap_window_ms: Optional[int]


@dataclass(frozen=True)
class CollectorAssignment:
    """단일 Collector에 대한 배정 payload입니다."""

    collector_id: str
    market_ids: list[str]
    token_ids: list[str]
    limits: AssignmentLimits
    handoff_actions: list[HandoffAction] = field(default_factory=list)


@dataclass(frozen=True)
class AssignmentPlan:
    """모든 Collector에 대한 버전 관리 배정 계획입니다."""

    version: int
    universe_version: int
    generated_at_ms: int
    control_state: ControlState
    collectors: dict[str, CollectorAssignment] = field(default_factory=dict)


@dataclass(frozen=True)
class OrderbookEvent:
    """Rust Collector가 기록하는 정규화된 JSONL 행입니다."""

    schema_version: int
    event_type: OrderbookEventType
    market_id: Optional[str]
    asset: str
    side: Optional[str]
    price: Optional[float]
    size: Optional[float]
    timestamp_ms: int
    received_at_ms: int
    collector_id: str
    assignment_version: int
    universe_version: int
    raw: str
