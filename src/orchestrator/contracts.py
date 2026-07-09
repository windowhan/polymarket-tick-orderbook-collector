"""그린필드 v2 Orchestrator 계약 dataclass 모음입니다.

이 dataclass들은 Python Orchestrator 쪽에서 아키텍처 22.1절을 구현합니다.
``src/common/contracts.rs``의 Rust 계약과 의도적으로 같은 의미와 JSON 값을 유지하여,
Orchestrator와 Collector가 숨은 변환 규칙 없이 payload를 교환할 수 있게 합니다.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from src.orchestrator.core.control import ControlState


class MarketLifecycleState(str, Enum):
    """Market Universe Manager가 부여하는 마켓 생명주기 상태입니다.

    입출력 예시:
        >>> MarketLifecycleState.ACTIVE.value
        'ACTIVE'
    """

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
    """구조적 shard handoff에 사용할 방식입니다.

    v1에서는 거래량 급증에 따른 hot-market 이동을 사용하지 않습니다. handoff는
    장애 복구나 운영자 지시 이동처럼 구조적인 이동에만 예약합니다.
    """

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
        """계획된 수치가 이 Collector의 용량 안에 들어오는지 반환합니다.

        인자:
            market_count: Collector에 계획된 마켓 수입니다.
            token_count: Collector에 계획된 CLOB 토큰 수입니다.
            ws_connections: 예상 WebSocket 연결 수입니다.

        반환값:
            모든 수치가 선언 용량 이내일 때만 ``True``를 반환합니다.

        입출력 예시:
            >>> c = CollectorCapacity(2, 4, 1, None, 10)
            >>> c.fits_subscription_counts(2, 4, 1)
            True
            >>> c.fits_subscription_counts(3, 4, 1)
            False
        """

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
        """실제 구독 수가 배정 수와 일치하는지 반환합니다.

        반환값:
            마켓 수와 토큰 수가 모두 일치하면 ``True``를 반환합니다.

        입출력 예시:
            >>> s = CollectorStatus('c', 1, 1, 2, 1, 2, 1, 0.0, 0, 0, 0, None)
            >>> s.assignment_matches_subscription()
            True
        """

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
class ObjectNotification:
    """Collector가 업로드한 GCS object의 메타데이터입니다."""

    collector_id: str
    assignment_version: int
    bucket: str
    object_name: str
    generation: str
    line_count: int
    first_event_ts_ms: Optional[int]
    last_event_ts_ms: Optional[int]
    checksum_crc32c: Optional[str]

    def idempotency_key(self) -> str:
        """object 단위 멱등성 key를 반환합니다.

        반환값:
            안정적인 ``bucket/object_name#generation`` key입니다.

        입출력 예시:
            >>> n = ObjectNotification('c', 1, 'b', 'o', 'g', 1, None, None, None)
            >>> n.idempotency_key()
            'b/o#g'
        """

        return f"{self.bucket}/{self.object_name}#{self.generation}"


@dataclass(frozen=True)
class ProcessedObject:
    """GCS object generation이 병합 완료되었음을 나타내는 영속 기록입니다."""

    bucket: str
    object_name: str
    generation: str
    processed_at_ms: int
    input_line_count: int
    valid_line_count: int
    skipped_line_count: int
    output_path: str

    def idempotency_key(self) -> str:
        """중복 skip 판단에 사용하는 object 단위 멱등성 key를 반환합니다."""

        return f"{self.bucket}/{self.object_name}#{self.generation}"


@dataclass(frozen=True)
class BudgetPolicy:
    """GCS 비용 가드레일에 사용할 예산 임계값 설정입니다."""

    warning_threshold_usd: float
    hard_stop_threshold_usd: float
    discord_webhook_secret_name: str
    check_interval_secs: int


@dataclass(frozen=True)
class BudgetState:
    """Orchestrator가 유지하는 런타임 예산 상태입니다."""

    estimated_gcs_cost_usd: float
    uploaded_bytes: int
    object_create_count: int
    object_list_count: int
    object_get_count: int
    object_delete_count: int
    control_state: ControlState

    def warning_exceeded(self, policy: BudgetPolicy) -> bool:
        """경고 임계값을 넘었는지 반환합니다."""

        return self.estimated_gcs_cost_usd >= policy.warning_threshold_usd

    def hard_stop_exceeded(self, policy: BudgetPolicy) -> bool:
        """강제 중단 임계값을 넘었는지 반환합니다."""

        return self.estimated_gcs_cost_usd >= policy.hard_stop_threshold_usd


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
