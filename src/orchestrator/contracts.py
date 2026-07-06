"""Greenfield v2 Orchestrator contract dataclasses.

These dataclasses implement architecture section 22.1 for the Python
Orchestrator side.  They intentionally mirror the Rust contracts in
``src/common/contracts.rs`` so that the Orchestrator and Collector can exchange
JSON payloads without hidden translation rules.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class MarketLifecycleState(str, Enum):
    """Lifecycle state assigned by the Market Universe Manager.

    Example input/output:
        >>> MarketLifecycleState.ACTIVE.value
        'ACTIVE'
    """

    DISCOVERED = "DISCOVERED"
    ACTIVE = "ACTIVE"
    DRAINING = "DRAINING"
    CLOSED = "CLOSED"
    ARCHIVED = "ARCHIVED"
    EXCLUDED = "EXCLUDED"


class ControlState(str, Enum):
    """Global control state returned with assignments.

    Collector processes must treat ``EMERGENCY_STOP_BY_BUDGET`` as higher
    priority than normal assignment changes.
    """

    RUNNING = "RUNNING"
    PAUSED_BY_OPERATOR = "PAUSED_BY_OPERATOR"
    PAUSED_BY_BUDGET_WARNING = "PAUSED_BY_BUDGET_WARNING"
    EMERGENCY_STOP_BY_BUDGET = "EMERGENCY_STOP_BY_BUDGET"


class OrderbookEventType(str, Enum):
    """Normalized event type written by Collectors to JSONL."""

    BOOK = "book"
    PRICE_CHANGE = "price_change"
    LAST_TRADE = "last_trade"


class HandoffMode(str, Enum):
    """Mode for structural shard handoff.

    v1 does not use traffic-spike based hot-market moves; handoff is reserved
    for structural movement such as failure recovery or operator-directed moves.
    """

    NONE = "NONE"
    MAKE_BEFORE_BREAK = "MAKE_BEFORE_BREAK"
    BREAK_BEFORE_MAKE = "BREAK_BEFORE_MAKE"
    DRAIN_ONLY = "DRAIN_ONLY"


@dataclass(frozen=True)
class MarketInfo:
    """Normalized market metadata used by the assignment planner."""

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
    """Versioned snapshot of markets eligible for planning decisions."""

    version: int
    generated_at_ms: int
    markets: dict[str, MarketInfo] = field(default_factory=dict)


@dataclass(frozen=True)
class CollectorCapacity:
    """Declared Collector capacity used to prevent over-assignment."""

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
        """Return whether planned counts fit this Collector's capacity.

        Args:
            market_count: Number of markets planned for the Collector.
            token_count: Number of CLOB tokens planned for the Collector.
            ws_connections: Number of WebSocket connections expected.

        Returns:
            ``True`` only when all counts are within declared capacity.

        Example input/output:
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
    """Runtime status reported by a Collector."""

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
        """Return whether actual subscriptions match assigned counts.

        Returns:
            ``True`` when market and token subscription counts match.

        Example input/output:
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
    """Per-assignment worker splitting limits sent to a Collector."""

    max_ws_connections: int
    max_tokens_per_ws_connection: int


@dataclass(frozen=True)
class HandoffAction:
    """Planned structural handoff for token IDs."""

    token_ids: list[str]
    from_collector_id: Optional[str]
    to_collector_id: Optional[str]
    mode: HandoffMode
    overlap_window_ms: Optional[int]


@dataclass(frozen=True)
class CollectorAssignment:
    """Assignment payload for one Collector."""

    collector_id: str
    market_ids: list[str]
    token_ids: list[str]
    limits: AssignmentLimits
    handoff_actions: list[HandoffAction] = field(default_factory=list)


@dataclass(frozen=True)
class AssignmentPlan:
    """Versioned assignment plan for all Collectors."""

    version: int
    universe_version: int
    generated_at_ms: int
    control_state: ControlState
    collectors: dict[str, CollectorAssignment] = field(default_factory=dict)


@dataclass(frozen=True)
class ObjectNotification:
    """Metadata for a GCS object uploaded by a Collector."""

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
        """Return the object-level idempotency key.

        Returns:
            A stable ``bucket/object_name#generation`` key.

        Example input/output:
            >>> n = ObjectNotification('c', 1, 'b', 'o', 'g', 1, None, None, None)
            >>> n.idempotency_key()
            'b/o#g'
        """

        return f"{self.bucket}/{self.object_name}#{self.generation}"


@dataclass(frozen=True)
class ProcessedObject:
    """Durable record that a GCS object generation has been merged."""

    bucket: str
    object_name: str
    generation: str
    processed_at_ms: int
    input_line_count: int
    valid_line_count: int
    skipped_line_count: int
    output_path: str

    def idempotency_key(self) -> str:
        """Return the object-level idempotency key used for duplicate skips."""

        return f"{self.bucket}/{self.object_name}#{self.generation}"


@dataclass(frozen=True)
class BudgetPolicy:
    """Configured budget thresholds for GCS cost guardrails."""

    warning_threshold_usd: float
    hard_stop_threshold_usd: float
    discord_webhook_secret_name: str
    check_interval_secs: int


@dataclass(frozen=True)
class BudgetState:
    """Runtime budget state maintained by the Orchestrator."""

    estimated_gcs_cost_usd: float
    uploaded_bytes: int
    object_create_count: int
    object_list_count: int
    object_get_count: int
    object_delete_count: int
    control_state: ControlState

    def warning_exceeded(self, policy: BudgetPolicy) -> bool:
        """Return whether the warning threshold has been crossed."""

        return self.estimated_gcs_cost_usd >= policy.warning_threshold_usd

    def hard_stop_exceeded(self, policy: BudgetPolicy) -> bool:
        """Return whether the hard stop threshold has been crossed."""

        return self.estimated_gcs_cost_usd >= policy.hard_stop_threshold_usd


@dataclass(frozen=True)
class OrderbookEvent:
    """Normalized JSONL row written by the Rust Collector."""

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
