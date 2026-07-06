//! Shared greenfield v2 architecture contracts.
//!
//! These types implement section 22.1 of `docs/our-docs/architecture-draft-kr.md`.
//! They are intentionally data-oriented so the Rust Collector and Python
//! Orchestrator can exchange JSON with the same field names and state values.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Lifecycle state assigned to a Polymarket market by the Market Universe Manager.
///
/// # Detailed Description
/// The Market Universe Manager periodically refreshes Gamma API market metadata
/// and maps raw flags such as `active`, `closed`, `archived`, and
/// `enable_order_book` into one of these explicit states.  Assignment planning
/// consumes this state instead of duplicating raw-field policy decisions.
///
/// # Example — Input / Output
/// ```rust
/// use polymarket_collector::common::contracts::MarketLifecycleState;
///
/// let state = MarketLifecycleState::Active;
/// let json = serde_json::to_string(&state).unwrap();
/// assert_eq!(json, "\"ACTIVE\"");
/// ```
///
/// # Related
/// - [`MarketUniverseSnapshot`]
/// - [`MarketInfo`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MarketLifecycleState {
    /// Market was newly seen and may need one more refresh before collection.
    Discovered,
    /// Market is eligible for assignment and WebSocket collection.
    Active,
    /// Market is leaving active collection but may keep a short drain window.
    Draining,
    /// Market is closed and should not receive new assignments.
    Closed,
    /// Market is archived and removed from the active universe.
    Archived,
    /// Market is intentionally excluded by policy.
    Excluded,
}

/// Global control state returned by the Orchestrator with assignments.
///
/// # Detailed Description
/// This state lets the Orchestrator pause or stop Collectors without requiring
/// a separate emergency endpoint.  Collectors must treat emergency stop as
/// higher priority than assignment changes or handoff actions.
///
/// # Example — Input / Output
/// ```rust
/// use polymarket_collector::common::contracts::ControlState;
///
/// let state = ControlState::EmergencyStopByBudget;
/// assert_eq!(serde_json::to_string(&state).unwrap(), "\"EMERGENCY_STOP_BY_BUDGET\"");
/// ```
///
/// # Related
/// - [`BudgetState`]
/// - [`AssignmentPlan`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ControlState {
    /// Normal collection is allowed.
    Running,
    /// Operator manually paused collection.
    PausedByOperator,
    /// Budget warning has been crossed; policy may restrict new assignments.
    PausedByBudgetWarning,
    /// Hard budget limit has been crossed; Collectors must stop WebSocket input.
    EmergencyStopByBudget,
}

/// Normalized event type emitted by the Collector.
///
/// # Detailed Description
/// Polymarket CLOB WebSocket messages use source-specific event names.  The
/// Collector maps those events into this stable enum before writing JSONL so
/// downstream compaction and viewer code do not depend on raw source naming.
///
/// # Example — Input / Output
/// ```rust
/// use polymarket_collector::common::contracts::OrderbookEventType;
///
/// let event_type = OrderbookEventType::PriceChange;
/// assert_eq!(serde_json::to_string(&event_type).unwrap(), "\"price_change\"");
/// ```
///
/// # Related
/// - [`OrderbookEvent`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderbookEventType {
    /// Full or partial book snapshot event.
    Book,
    /// Price/size level change event.
    PriceChange,
    /// Last trade event derived from `last_trade_price` source messages.
    LastTrade,
}

/// Handoff mode used when a shard must move between Collectors.
///
/// # Detailed Description
/// v1 avoids moving healthy shards for traffic spikes.  Handoff modes are kept
/// for structural changes such as Collector failure recovery or explicit
/// operator-directed movement.  `MakeBeforeBreak` minimizes subscription gaps by
/// subscribing the destination before draining the source.
///
/// # Example — Input / Output
/// ```rust
/// use polymarket_collector::common::contracts::HandoffMode;
///
/// let mode = HandoffMode::MakeBeforeBreak;
/// assert_eq!(serde_json::to_string(&mode).unwrap(), "\"MAKE_BEFORE_BREAK\"");
/// ```
///
/// # Related
/// - [`HandoffAction`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum HandoffMode {
    /// No handoff is required.
    None,
    /// Destination subscribes before source unsubscribes.
    MakeBeforeBreak,
    /// Source unsubscribes before destination subscribes.
    BreakBeforeMake,
    /// Source drains/removes tokens without a destination Collector.
    DrainOnly,
}

/// Metadata for a single market in the current universe snapshot.
///
/// # Detailed Description
/// This type stores the normalized subset of Gamma API metadata required for
/// assignment planning.  Raw Gamma payloads should not be passed through the
/// planner directly because lifecycle policy should be explicit and auditable.
///
/// # Example — Input / Output
/// ```rust
/// use polymarket_collector::common::contracts::{MarketInfo, MarketLifecycleState};
///
/// let market = MarketInfo {
///     market_id: "m1".into(),
///     slug: "example".into(),
///     question: "Will this compile?".into(),
///     active: true,
///     closed: false,
///     archived: false,
///     accepting_orders: true,
///     enable_order_book: true,
///     token_ids: vec!["t1".into(), "t2".into()],
///     lifecycle_state: MarketLifecycleState::Active,
/// };
/// assert_eq!(market.token_ids.len(), 2);
/// ```
///
/// # Related
/// - [`MarketUniverseSnapshot`]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketInfo {
    /// Stable market identifier from the source API.
    pub market_id: String,
    /// Human-readable URL slug.
    pub slug: String,
    /// Market question text.
    pub question: String,
    /// Raw active flag.
    pub active: bool,
    /// Raw closed flag.
    pub closed: bool,
    /// Raw archived flag.
    pub archived: bool,
    /// Whether orders are currently accepted.
    pub accepting_orders: bool,
    /// Whether CLOB orderbook is enabled.
    pub enable_order_book: bool,
    /// CLOB token IDs assigned to this market's outcomes.
    pub token_ids: Vec<String>,
    /// Policy-derived lifecycle state.
    pub lifecycle_state: MarketLifecycleState,
}

/// Versioned snapshot of all markets eligible for planning decisions.
///
/// # Detailed Description
/// Every Gamma refresh that changes the normalized universe creates a new
/// version.  Assignment plans reference this version so Collectors and operators
/// can understand which market universe produced a given assignment.
///
/// # Example — Input / Output
/// ```rust
/// use polymarket_collector::common::contracts::MarketUniverseSnapshot;
/// use std::collections::BTreeMap;
///
/// let snapshot = MarketUniverseSnapshot {
///     version: 1,
///     generated_at_ms: 1_781_051_970_000,
///     markets: BTreeMap::new(),
/// };
/// assert_eq!(snapshot.version, 1);
/// ```
///
/// # Related
/// - [`AssignmentPlan`]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketUniverseSnapshot {
    /// Monotonic universe version.
    pub version: u64,
    /// Snapshot creation time in Unix milliseconds.
    pub generated_at_ms: i64,
    /// Markets keyed by `market_id` for deterministic serialization.
    pub markets: BTreeMap<String, MarketInfo>,
}

/// Declared capacity for a Collector process.
///
/// # Detailed Description
/// The Orchestrator must not intentionally assign beyond this capacity.  Runtime
/// overload metrics are separate and are used for alerting/protection rather
/// than hot-market migration in v1.
///
/// # Example — Input / Output
/// ```rust
/// use polymarket_collector::common::contracts::CollectorCapacity;
///
/// let capacity = CollectorCapacity {
///     max_market_subscriptions: 10,
///     max_token_subscriptions: 20,
///     max_ws_connections: 2,
///     max_events_per_sec: Some(1_000.0),
///     max_upload_backlog_files: 50,
/// };
/// assert!(capacity.fits_subscription_counts(10, 20, 2));
/// assert!(!capacity.fits_subscription_counts(11, 20, 2));
/// ```
///
/// # Related
/// - [`CollectorStatus`]
/// - [`AssignmentPlan`]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollectorCapacity {
    /// Maximum markets this Collector should subscribe to.
    pub max_market_subscriptions: usize,
    /// Maximum CLOB tokens this Collector should subscribe to.
    pub max_token_subscriptions: usize,
    /// Maximum WebSocket connections this Collector should open.
    pub max_ws_connections: usize,
    /// Optional soft event-rate guardrail for alerting.
    pub max_events_per_sec: Option<f64>,
    /// Maximum local files waiting for upload/notify before alerting.
    pub max_upload_backlog_files: usize,
}

impl CollectorCapacity {
    /// Check whether planned subscription counts fit declared Collector capacity.
    ///
    /// # Detailed Description
    /// The assignment planner calls this before producing an assignment.  A
    /// `false` result means the plan is invalid for this Collector and must be
    /// split or assigned elsewhere.  This does not inspect runtime overload; it
    /// only validates planned counts.
    ///
    /// # Arguments
    /// * `market_count` — Number of markets planned for the Collector.
    /// * `token_count` — Number of CLOB tokens planned for the Collector.
    /// * `ws_connections` — Number of WebSocket connections expected.
    ///
    /// # Returns
    /// `true` when all planned counts are within capacity; otherwise `false`.
    ///
    /// # Example — Input / Output
    /// ```rust
    /// use polymarket_collector::common::contracts::CollectorCapacity;
    ///
    /// let capacity = CollectorCapacity {
    ///     max_market_subscriptions: 2,
    ///     max_token_subscriptions: 4,
    ///     max_ws_connections: 1,
    ///     max_events_per_sec: None,
    ///     max_upload_backlog_files: 10,
    /// };
    /// assert!(capacity.fits_subscription_counts(2, 4, 1));
    /// assert!(!capacity.fits_subscription_counts(3, 4, 1));
    /// ```
    ///
    /// # Related
    /// - [`AssignmentPlan`]
    pub fn fits_subscription_counts(
        &self,
        market_count: usize,
        token_count: usize,
        ws_connections: usize,
    ) -> bool {
        market_count <= self.max_market_subscriptions
            && token_count <= self.max_token_subscriptions
            && ws_connections <= self.max_ws_connections
    }
}

/// Runtime status reported by a Collector.
///
/// # Detailed Description
/// Status reports tell the Orchestrator what the Collector is actually doing.
/// The Orchestrator uses these reports to detect assignment mismatch, backlog,
/// reconnect churn, and capacity pressure.  In v1, traffic spikes do not cause
/// automatic hot-market reassignment; they are observed and alerted.
///
/// # Example — Input / Output
/// ```rust
/// use polymarket_collector::common::contracts::CollectorStatus;
///
/// let status = CollectorStatus {
///     collector_id: "collector-a".into(),
///     assignment_version: 7,
///     assigned_market_count: 10,
///     assigned_token_count: 20,
///     subscribed_market_count: 10,
///     subscribed_token_count: 20,
///     active_ws_connections: 1,
///     events_per_sec: 42.0,
///     reconnects_last_minute: 0,
///     upload_backlog_files: 0,
///     local_spool_bytes: 0,
///     last_successful_upload_at_ms: Some(1_781_051_970_000),
/// };
/// assert!(status.assignment_matches_subscription());
/// ```
///
/// # Related
/// - [`CollectorCapacity`]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollectorStatus {
    /// Collector ID issued by the Orchestrator.
    pub collector_id: String,
    /// Assignment version currently applied by this Collector.
    pub assignment_version: u64,
    /// Number of markets the Orchestrator assigned.
    pub assigned_market_count: usize,
    /// Number of tokens the Orchestrator assigned.
    pub assigned_token_count: usize,
    /// Number of markets actually subscribed.
    pub subscribed_market_count: usize,
    /// Number of tokens actually subscribed.
    pub subscribed_token_count: usize,
    /// Currently open WebSocket connections.
    pub active_ws_connections: usize,
    /// Observed event rate for alerting and dashboards.
    pub events_per_sec: f64,
    /// Reconnect count in the last minute.
    pub reconnects_last_minute: usize,
    /// Files waiting for upload or notify.
    pub upload_backlog_files: usize,
    /// Bytes currently retained in local spool.
    pub local_spool_bytes: u64,
    /// Last successful upload time in Unix milliseconds.
    pub last_successful_upload_at_ms: Option<i64>,
}

impl CollectorStatus {
    /// Check whether actual subscriptions match the current assignment counts.
    ///
    /// # Detailed Description
    /// This helps the Orchestrator distinguish "assigned" from "actually
    /// subscribed." A mismatch should not trigger traffic-based hot migration in
    /// v1, but it should be surfaced as an operational warning or repair item.
    ///
    /// # Arguments
    /// This method does not accept additional arguments.
    ///
    /// # Returns
    /// `true` when market and token subscription counts match assignment counts.
    ///
    /// # Example — Input / Output
    /// ```rust
    /// # use polymarket_collector::common::contracts::CollectorStatus;
    /// # let status = CollectorStatus { collector_id: "c".into(), assignment_version: 1,
    /// # assigned_market_count: 1, assigned_token_count: 2, subscribed_market_count: 1,
    /// # subscribed_token_count: 2, active_ws_connections: 1, events_per_sec: 0.0,
    /// # reconnects_last_minute: 0, upload_backlog_files: 0, local_spool_bytes: 0,
    /// # last_successful_upload_at_ms: None };
    /// assert!(status.assignment_matches_subscription());
    /// ```
    ///
    /// # Related
    /// - [`CollectorCapacity`]
    pub fn assignment_matches_subscription(&self) -> bool {
        self.assigned_market_count == self.subscribed_market_count
            && self.assigned_token_count == self.subscribed_token_count
    }
}

/// Per-assignment limits sent to a Collector.
///
/// # Detailed Description
/// These limits describe how the Collector should split assigned tokens into
/// WebSocket workers.  They are distinct from registration capacity, which is
/// the Collector's advertised maximum.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignmentLimits {
    /// Maximum WebSocket connections for this assignment.
    pub max_ws_connections: usize,
    /// Maximum tokens per WebSocket connection.
    pub max_tokens_per_ws_connection: usize,
}

/// Planned handoff for a set of token IDs.
///
/// # Detailed Description
/// Handoff actions are only for structural movement in v1, such as Collector
/// failure recovery or operator-directed moves.  They are not generated for
/// hot-market traffic spikes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandoffAction {
    /// Token IDs involved in the handoff.
    pub token_ids: Vec<String>,
    /// Source Collector, if any.
    pub from_collector_id: Option<String>,
    /// Destination Collector, if any.
    pub to_collector_id: Option<String>,
    /// Handoff mode.
    pub mode: HandoffMode,
    /// Optional overlap window in milliseconds.
    pub overlap_window_ms: Option<u64>,
}

/// Assignment for a single Collector.
///
/// # Detailed Description
/// The Orchestrator returns this payload to a Collector.  The `token_ids` and
/// `market_ids` vectors are the authoritative collection set for the referenced
/// `assignment_version`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectorAssignment {
    /// Collector ID receiving the assignment.
    pub collector_id: String,
    /// Market IDs assigned to the Collector.
    pub market_ids: Vec<String>,
    /// Token IDs assigned to the Collector.
    pub token_ids: Vec<String>,
    /// Runtime limits for worker splitting.
    pub limits: AssignmentLimits,
    /// Planned handoff actions, if any.
    pub handoff_actions: Vec<HandoffAction>,
}

/// Versioned assignment plan for all Collectors.
///
/// # Detailed Description
/// A plan references one market universe version and includes the global control
/// state.  Collectors should stop collection when `control_state` is
/// `EmergencyStopByBudget`, even if token IDs are present.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignmentPlan {
    /// Monotonic assignment version.
    pub version: u64,
    /// Market universe version used to build this assignment.
    pub universe_version: u64,
    /// Plan creation time in Unix milliseconds.
    pub generated_at_ms: i64,
    /// Global control state.
    pub control_state: ControlState,
    /// Collector assignments keyed by Collector ID.
    pub collectors: BTreeMap<String, CollectorAssignment>,
}

/// Metadata notification for a newly uploaded GCS object.
///
/// # Detailed Description
/// Collectors send this payload after uploading a closed local JSONL file.  The
/// Orchestrator stores the metadata and the Compactor later downloads the object
/// from GCS.  The JSONL body is never sent through the control plane.
///
/// # Example — Input / Output
/// ```rust
/// use polymarket_collector::common::contracts::ObjectNotification;
///
/// let notification = ObjectNotification {
///     collector_id: "collector-a".into(),
///     assignment_version: 1,
///     bucket: "bucket".into(),
///     object_name: "raw/object.jsonl".into(),
///     generation: "123".into(),
///     line_count: 10,
///     first_event_ts_ms: Some(1),
///     last_event_ts_ms: Some(2),
///     checksum_crc32c: None,
/// };
/// assert_eq!(notification.idempotency_key(), "bucket/raw/object.jsonl#123");
/// ```
///
/// # Related
/// - [`ProcessedObject`]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectNotification {
    /// Collector that uploaded the object.
    pub collector_id: String,
    /// Assignment version active when the object was produced.
    pub assignment_version: u64,
    /// GCS bucket name.
    pub bucket: String,
    /// GCS object name.
    pub object_name: String,
    /// GCS object generation.
    pub generation: String,
    /// Number of JSONL lines reported by the Collector.
    pub line_count: usize,
    /// First source event timestamp in Unix milliseconds, when known.
    pub first_event_ts_ms: Option<i64>,
    /// Last source event timestamp in Unix milliseconds, when known.
    pub last_event_ts_ms: Option<i64>,
    /// Optional CRC32C checksum from GCS metadata.
    pub checksum_crc32c: Option<String>,
}

impl ObjectNotification {
    /// Build the object-level idempotency key.
    ///
    /// # Detailed Description
    /// GCS can have multiple generations for the same object name.  Including
    /// generation prevents a later upload with the same name from being skipped
    /// incorrectly.
    ///
    /// # Arguments
    /// This method does not accept additional arguments.
    ///
    /// # Returns
    /// A stable `bucket/object_name#generation` key.
    ///
    /// # Example — Input / Output
    /// ```rust
    /// # use polymarket_collector::common::contracts::ObjectNotification;
    /// # let notification = ObjectNotification { collector_id: "c".into(), assignment_version: 1,
    /// # bucket: "b".into(), object_name: "o".into(), generation: "g".into(),
    /// # line_count: 1, first_event_ts_ms: None, last_event_ts_ms: None, checksum_crc32c: None };
    /// assert_eq!(notification.idempotency_key(), "b/o#g");
    /// ```
    ///
    /// # Related
    /// - [`ProcessedObject::idempotency_key`]
    pub fn idempotency_key(&self) -> String {
        format!("{}/{}#{}", self.bucket, self.object_name, self.generation)
    }
}

/// Durable record that a GCS object generation has been merged.
///
/// # Detailed Description
/// Compactor restarts, duplicate notifications, and prefix polling can reveal
/// the same object more than once.  This manifest record lets the Compactor skip
/// already merged object generations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessedObject {
    /// GCS bucket name.
    pub bucket: String,
    /// GCS object name.
    pub object_name: String,
    /// GCS object generation.
    pub generation: String,
    /// Processing completion time in Unix milliseconds.
    pub processed_at_ms: i64,
    /// Input JSONL line count.
    pub input_line_count: usize,
    /// Valid output line count.
    pub valid_line_count: usize,
    /// Skipped malformed line count.
    pub skipped_line_count: usize,
    /// Merged output path or object name.
    pub output_path: String,
}

impl ProcessedObject {
    /// Build the same idempotency key used by object notifications.
    ///
    /// # Detailed Description
    /// The Compactor can compare this value with [`ObjectNotification`] keys to
    /// determine whether an input object generation has already been processed.
    ///
    /// # Arguments
    /// This method does not accept additional arguments.
    ///
    /// # Returns
    /// A stable `bucket/object_name#generation` key.
    ///
    /// # Example — Input / Output
    /// ```rust
    /// # use polymarket_collector::common::contracts::ProcessedObject;
    /// # let processed = ProcessedObject { bucket: "b".into(), object_name: "o".into(),
    /// # generation: "g".into(), processed_at_ms: 0, input_line_count: 1,
    /// # valid_line_count: 1, skipped_line_count: 0, output_path: "merged".into() };
    /// assert_eq!(processed.idempotency_key(), "b/o#g");
    /// ```
    ///
    /// # Related
    /// - [`ObjectNotification::idempotency_key`]
    pub fn idempotency_key(&self) -> String {
        format!("{}/{}#{}", self.bucket, self.object_name, self.generation)
    }
}

/// Configured budget thresholds for GCS cost guardrails.
///
/// # Detailed Description
/// The Orchestrator uses this policy to decide when to send Discord warnings
/// and when to enter emergency stop.  The Discord webhook should be referenced
/// by secret name rather than stored directly in code or manifests.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BudgetPolicy {
    /// Warning threshold in estimated USD.
    pub warning_threshold_usd: f64,
    /// Hard stop threshold in estimated USD.
    pub hard_stop_threshold_usd: f64,
    /// Secret name or config key that resolves to the Discord webhook URL.
    pub discord_webhook_secret_name: String,
    /// Cost check interval in seconds.
    pub check_interval_secs: u64,
}

/// Runtime budget state maintained by the Orchestrator.
///
/// # Detailed Description
/// Google Cloud Billing data may lag, so this state tracks an internal estimate
/// from uploaded bytes and object operations.  Crossing the hard stop threshold
/// should move global control state to [`ControlState::EmergencyStopByBudget`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BudgetState {
    /// Current estimated GCS cost in USD.
    pub estimated_gcs_cost_usd: f64,
    /// Uploaded bytes counted by Collector notifications.
    pub uploaded_bytes: u64,
    /// GCS object create operation count.
    pub object_create_count: u64,
    /// GCS object list operation count.
    pub object_list_count: u64,
    /// GCS object get operation count.
    pub object_get_count: u64,
    /// GCS object delete operation count.
    pub object_delete_count: u64,
    /// Current global control state derived from budget policy.
    pub control_state: ControlState,
}

impl BudgetState {
    /// Determine whether the warning threshold has been crossed.
    ///
    /// # Detailed Description
    /// This is a pure comparison helper.  The caller is responsible for sending
    /// Discord notifications and recording alert state to avoid duplicate spam.
    ///
    /// # Arguments
    /// * `policy` — Budget thresholds configured by the operator.
    ///
    /// # Returns
    /// `true` when estimated cost is greater than or equal to warning threshold.
    ///
    /// # Example — Input / Output
    /// ```rust
    /// # use polymarket_collector::common::contracts::{BudgetPolicy, BudgetState, ControlState};
    /// # let policy = BudgetPolicy { warning_threshold_usd: 10.0, hard_stop_threshold_usd: 20.0,
    /// # discord_webhook_secret_name: "discord".into(), check_interval_secs: 60 };
    /// # let state = BudgetState { estimated_gcs_cost_usd: 10.0, uploaded_bytes: 0,
    /// # object_create_count: 0, object_list_count: 0, object_get_count: 0,
    /// # object_delete_count: 0, control_state: ControlState::Running };
    /// assert!(state.warning_exceeded(&policy));
    /// ```
    ///
    /// # Related
    /// - [`BudgetState::hard_stop_exceeded`]
    pub fn warning_exceeded(&self, policy: &BudgetPolicy) -> bool {
        self.estimated_gcs_cost_usd >= policy.warning_threshold_usd
    }

    /// Determine whether the hard stop threshold has been crossed.
    ///
    /// # Detailed Description
    /// When this returns `true`, the Orchestrator should publish emergency stop
    /// state and Collectors should close WebSocket subscriptions while retaining
    /// local spool according to policy.
    ///
    /// # Arguments
    /// * `policy` — Budget thresholds configured by the operator.
    ///
    /// # Returns
    /// `true` when estimated cost is greater than or equal to hard stop threshold.
    ///
    /// # Example — Input / Output
    /// ```rust
    /// # use polymarket_collector::common::contracts::{BudgetPolicy, BudgetState, ControlState};
    /// # let policy = BudgetPolicy { warning_threshold_usd: 10.0, hard_stop_threshold_usd: 20.0,
    /// # discord_webhook_secret_name: "discord".into(), check_interval_secs: 60 };
    /// # let state = BudgetState { estimated_gcs_cost_usd: 19.99, uploaded_bytes: 0,
    /// # object_create_count: 0, object_list_count: 0, object_get_count: 0,
    /// # object_delete_count: 0, control_state: ControlState::Running };
    /// assert!(!state.hard_stop_exceeded(&policy));
    /// ```
    ///
    /// # Related
    /// - [`ControlState::EmergencyStopByBudget`]
    pub fn hard_stop_exceeded(&self, policy: &BudgetPolicy) -> bool {
        self.estimated_gcs_cost_usd >= policy.hard_stop_threshold_usd
    }
}

/// Normalized event written by the Rust Collector.
///
/// # Detailed Description
/// This is the row-level JSONL contract for raw/merged orderbook output.  The
/// Collector keeps the original source payload in `raw` so parser bugs or schema
/// changes can be audited later.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderbookEvent {
    /// Event schema version.
    pub schema_version: u32,
    /// Normalized event type.
    pub event_type: OrderbookEventType,
    /// Optional market ID if known from the universe snapshot.
    pub market_id: Option<String>,
    /// CLOB token ID.
    pub asset: String,
    /// Side such as BUY, SELL, bid, or ask.
    pub side: Option<String>,
    /// Decimal price.
    pub price: Option<f64>,
    /// Decimal size.
    pub size: Option<f64>,
    /// Source event timestamp in Unix milliseconds.
    pub timestamp_ms: i64,
    /// Collector receive timestamp in Unix milliseconds.
    pub received_at_ms: i64,
    /// Collector that observed the event.
    pub collector_id: String,
    /// Assignment version active when observed.
    pub assignment_version: u64,
    /// Market universe version active when observed.
    pub universe_version: u64,
    /// Original source JSON payload.
    pub raw: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_state_serializes_as_contract_value() {
        let json = serde_json::to_string(&MarketLifecycleState::Draining).unwrap();
        assert_eq!(json, "\"DRAINING\"");
    }

    #[test]
    fn capacity_rejects_over_assignment() {
        let capacity = CollectorCapacity {
            max_market_subscriptions: 2,
            max_token_subscriptions: 4,
            max_ws_connections: 1,
            max_events_per_sec: None,
            max_upload_backlog_files: 10,
        };

        assert!(capacity.fits_subscription_counts(2, 4, 1));
        assert!(!capacity.fits_subscription_counts(3, 4, 1));
        assert!(!capacity.fits_subscription_counts(2, 5, 1));
        assert!(!capacity.fits_subscription_counts(2, 4, 2));
    }

    #[test]
    fn object_notification_key_includes_generation() {
        let notification = ObjectNotification {
            collector_id: "collector-a".into(),
            assignment_version: 1,
            bucket: "bucket".into(),
            object_name: "raw/object.jsonl".into(),
            generation: "123".into(),
            line_count: 10,
            first_event_ts_ms: None,
            last_event_ts_ms: None,
            checksum_crc32c: None,
        };

        assert_eq!(
            notification.idempotency_key(),
            "bucket/raw/object.jsonl#123"
        );
    }

    #[test]
    fn budget_threshold_helpers_are_inclusive() {
        let policy = BudgetPolicy {
            warning_threshold_usd: 10.0,
            hard_stop_threshold_usd: 20.0,
            discord_webhook_secret_name: "discord-webhook".into(),
            check_interval_secs: 60,
        };
        let state = BudgetState {
            estimated_gcs_cost_usd: 20.0,
            uploaded_bytes: 0,
            object_create_count: 0,
            object_list_count: 0,
            object_get_count: 0,
            object_delete_count: 0,
            control_state: ControlState::Running,
        };

        assert!(state.warning_exceeded(&policy));
        assert!(state.hard_stop_exceeded(&policy));
    }
}
