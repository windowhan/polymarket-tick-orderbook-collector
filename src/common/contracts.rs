//! 그린필드 v2 아키텍처의 공유 계약 타입입니다.
//!
//! 이 타입들은 `docs/our-docs/architecture-draft-kr.md`의 22.1절을 구현합니다.
//! Rust Collector와 Python Orchestrator가 숨은 변환 규칙 없이 같은 필드명과
//! 상태값으로 JSON을 주고받을 수 있도록 데이터 중심으로 설계했습니다.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Market Universe Manager가 Polymarket 마켓에 부여하는 생명주기 상태입니다.
///
/// # 상세 설명
/// Market Universe Manager는 Gamma API 마켓 메타데이터를 주기적으로 갱신하고,
/// `active`, `closed`, `archived`, `enable_order_book` 같은 원천 플래그를 명시적인
/// 상태 중 하나로 매핑합니다. 배정 계획은 원천 필드 정책을 중복 구현하지 않고
/// 이 상태값을 기준으로 판단합니다.
///
/// # 예시 — 입력 / 출력
/// ```rust
/// use polymarket_collector::common::contracts::MarketLifecycleState;
///
/// let state = MarketLifecycleState::Active;
/// let json = serde_json::to_string(&state).unwrap();
/// assert_eq!(json, "\"ACTIVE\"");
/// ```
///
/// # 관련
/// - [`MarketUniverseSnapshot`]
/// - [`MarketInfo`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MarketLifecycleState {
    /// 새로 발견되어 수집 전 한 번 더 갱신 확인이 필요할 수 있는 마켓입니다.
    Discovered,
    /// 배정과 WebSocket 수집 대상이 될 수 있는 활성 마켓입니다.
    Active,
    /// 활성 수집에서 빠지는 중이며 짧은 배수 구간을 둘 수 있는 마켓입니다.
    Draining,
    /// 종료되어 새 배정을 받으면 안 되는 마켓입니다.
    Closed,
    /// 보관 상태라 활성 유니버스에서 제거되는 마켓입니다.
    Archived,
    /// 정책상 의도적으로 제외된 마켓입니다.
    Excluded,
}

/// Orchestrator가 배정 응답과 함께 내려주는 전역 제어 상태입니다.
///
/// # 상세 설명
/// 이 상태를 통해 Orchestrator는 별도 긴급 엔드포인트 없이 Collector를 일시정지하거나
/// 중단시킬 수 있습니다. Collector는 긴급 중단 상태를 일반 배정 변경이나 handoff 동작보다
/// 더 높은 우선순위로 처리해야 합니다.
///
/// # 예시 — 입력 / 출력
/// ```rust
/// use polymarket_collector::common::contracts::ControlState;
///
/// let state = ControlState::EmergencyStopByBudget;
/// assert_eq!(serde_json::to_string(&state).unwrap(), "\"EMERGENCY_STOP_BY_BUDGET\"");
/// ```
///
/// # 관련
/// - [`BudgetState`]
/// - [`AssignmentPlan`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ControlState {
    /// 정상 수집을 허용합니다.
    Running,
    /// 운영자가 수동으로 수집을 일시정지했습니다.
    PausedByOperator,
    /// 예산 경고선에 도달했으며 정책상 새 배정을 제한할 수 있습니다.
    PausedByBudgetWarning,
    /// 예산 하드 리밋에 도달했으므로 Collector는 WebSocket 입력을 중단해야 합니다.
    EmergencyStopByBudget,
}

/// Collector가 내보내는 정규화된 이벤트 종류입니다.
///
/// # 상세 설명
/// Polymarket CLOB WebSocket 메시지는 원천별 이벤트 이름을 사용합니다. Collector는 JSONL을
/// 쓰기 전에 해당 이벤트를 안정적인 열거형으로 매핑하여, 이후 compaction이나 viewer 코드가
/// 원천 이벤트 이름에 직접 의존하지 않도록 합니다.
///
/// # 예시 — 입력 / 출력
/// ```rust
/// use polymarket_collector::common::contracts::OrderbookEventType;
///
/// let event_type = OrderbookEventType::PriceChange;
/// assert_eq!(serde_json::to_string(&event_type).unwrap(), "\"price_change\"");
/// ```
///
/// # 관련
/// - [`OrderbookEvent`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderbookEventType {
    /// 전체 또는 부분 오더북 스냅샷 이벤트입니다.
    Book,
    /// 가격/수량 레벨 변경 이벤트입니다.
    PriceChange,
    /// `last_trade_price` 원천 메시지에서 파생한 마지막 체결 이벤트입니다.
    LastTrade,
}

/// shard를 Collector 사이에서 옮겨야 할 때 사용하는 handoff 방식입니다.
///
/// # 상세 설명
/// v1에서는 거래량 급증만으로 정상 shard를 옮기지 않습니다. handoff 방식은 Collector 장애 복구나
/// 운영자가 명시한 이동처럼 구조적인 변경을 위해 유지합니다. `MakeBeforeBreak`는 목적지에서 먼저
/// 구독을 시작한 뒤 원본을 배수하여 구독 공백을 최소화합니다.
///
/// # 예시 — 입력 / 출력
/// ```rust
/// use polymarket_collector::common::contracts::HandoffMode;
///
/// let mode = HandoffMode::MakeBeforeBreak;
/// assert_eq!(serde_json::to_string(&mode).unwrap(), "\"MAKE_BEFORE_BREAK\"");
/// ```
///
/// # 관련
/// - [`HandoffAction`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum HandoffMode {
    /// handoff가 필요하지 않습니다.
    None,
    /// 목적지 Collector가 먼저 구독한 뒤 원본 Collector가 구독을 해제합니다.
    MakeBeforeBreak,
    /// 원본 Collector가 먼저 구독을 해제한 뒤 목적지 Collector가 구독합니다.
    BreakBeforeMake,
    /// 목적지 Collector 없이 원본 토큰만 배수하거나 제거합니다.
    DrainOnly,
}

/// 현재 유니버스 스냅샷에 포함된 단일 마켓의 메타데이터입니다.
///
/// # 상세 설명
/// 배정 계획에 필요한 Gamma API 메타데이터의 정규화된 부분집합을 저장합니다. 생명주기 정책은
/// 명시적이고 감사 가능해야 하므로 원시 Gamma payload를 planner에 직접 넘기지 않습니다.
///
/// # 예시 — 입력 / 출력
/// ```rust
/// use polymarket_collector::common::contracts::{MarketInfo, MarketLifecycleState};
///
/// let market = MarketInfo {
///     market_id: "m1".into(),
///     slug: "example".into(),
///     question: "컴파일 예시인가?".into(),
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
/// # 관련
/// - [`MarketUniverseSnapshot`]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketInfo {
    /// 원천 API가 제공하는 안정적인 마켓 식별자입니다.
    pub market_id: String,
    /// 사람이 읽을 수 있는 URL slug입니다.
    pub slug: String,
    /// 마켓 질문 문구입니다.
    pub question: String,
    /// 원천 `active` 플래그입니다.
    pub active: bool,
    /// 원천 `closed` 플래그입니다.
    pub closed: bool,
    /// 원천 `archived` 플래그입니다.
    pub archived: bool,
    /// 현재 주문 접수를 허용하는지 여부입니다.
    pub accepting_orders: bool,
    /// CLOB 오더북이 활성화되어 있는지 여부입니다.
    pub enable_order_book: bool,
    /// 이 마켓의 결과값에 배정된 CLOB token ID 목록입니다.
    pub token_ids: Vec<String>,
    /// 정책으로 도출한 생명주기 상태입니다.
    pub lifecycle_state: MarketLifecycleState,
}

/// 계획 판단에 사용할 수 있는 전체 마켓의 버전 관리 스냅샷입니다.
///
/// # 상세 설명
/// 정규화된 유니버스가 바뀌는 Gamma 갱신마다 새 버전을 만듭니다. 배정 계획은 이 버전을 참조하여
/// Collector와 운영자가 어떤 마켓 유니버스에서 해당 배정이 만들어졌는지 이해할 수 있게 합니다.
///
/// # 예시 — 입력 / 출력
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
/// # 관련
/// - [`AssignmentPlan`]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketUniverseSnapshot {
    /// 단조 증가하는 유니버스 버전입니다.
    pub version: u64,
    /// 스냅샷 생성 시각이며 Unix millisecond 단위입니다.
    pub generated_at_ms: i64,
    /// 결정적 직렬화를 위해 `market_id`로 정렬된 마켓 맵입니다.
    pub markets: BTreeMap<String, MarketInfo>,
}

/// Collector 프로세스가 선언한 처리 용량입니다.
///
/// # 상세 설명
/// Orchestrator는 의도적으로 이 용량을 넘는 배정을 만들면 안 됩니다. 런타임 overload 지표는
/// 별도로 취급하며, v1에서는 hot-market 이동이 아니라 알림과 보호 판단에 사용합니다.
///
/// # 예시 — 입력 / 출력
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
/// # 관련
/// - [`CollectorStatus`]
/// - [`AssignmentPlan`]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollectorCapacity {
    /// 이 Collector가 구독해야 하는 최대 마켓 수입니다.
    pub max_market_subscriptions: usize,
    /// 이 Collector가 구독해야 하는 최대 CLOB 토큰 수입니다.
    pub max_token_subscriptions: usize,
    /// 이 Collector가 열 수 있는 최대 WebSocket 연결 수입니다.
    pub max_ws_connections: usize,
    /// 알림에 사용할 선택적 soft 이벤트 속도 가드레일입니다.
    pub max_events_per_sec: Option<f64>,
    /// 업로드 또는 알림 대기 상태로 남아도 되는 로컬 파일 최대 수입니다.
    pub max_upload_backlog_files: usize,
}

impl CollectorCapacity {
    /// 계획된 구독 수가 선언된 Collector 용량 안에 들어오는지 확인합니다.
    ///
    /// # 상세 설명
    /// 배정 planner는 배정을 만들기 전에 이 함수를 호출합니다. `false`가 반환되면 해당 Collector에
    /// 대한 계획은 유효하지 않으므로 쪼개거나 다른 Collector에 배정해야 합니다. 이 함수는 런타임
    /// overload를 보지 않고 계획된 개수만 검증합니다.
    ///
    /// # 인자
    /// * `market_count` — Collector에 계획된 마켓 수입니다.
    /// * `token_count` — Collector에 계획된 CLOB 토큰 수입니다.
    /// * `ws_connections` — 예상 WebSocket 연결 수입니다.
    ///
    /// # 반환값
    /// 모든 계획 수치가 용량 이내이면 `true`, 하나라도 넘으면 `false`를 반환합니다.
    ///
    /// # 예시 — 입력 / 출력
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
    /// # 관련
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

/// Collector가 보고하는 런타임 상태입니다.
///
/// # 상세 설명
/// 상태 보고는 Collector가 실제로 무엇을 하고 있는지 Orchestrator에게 알려줍니다. Orchestrator는
/// 이 보고를 사용해 배정 불일치, backlog, reconnect 증가, 용량 압박을 감지합니다. v1에서는 거래량
/// 급증이 자동 hot-market 재배정으로 이어지지 않고 관찰 및 알림 대상으로 남습니다.
///
/// # 예시 — 입력 / 출력
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
/// # 관련
/// - [`CollectorCapacity`]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollectorStatus {
    /// Orchestrator가 발급한 Collector ID입니다.
    pub collector_id: String,
    /// 이 Collector가 현재 적용 중인 배정 버전입니다.
    pub assignment_version: u64,
    /// Orchestrator가 배정한 마켓 수입니다.
    pub assigned_market_count: usize,
    /// Orchestrator가 배정한 토큰 수입니다.
    pub assigned_token_count: usize,
    /// 실제로 구독 중인 마켓 수입니다.
    pub subscribed_market_count: usize,
    /// 실제로 구독 중인 토큰 수입니다.
    pub subscribed_token_count: usize,
    /// 현재 열려 있는 WebSocket 연결 수입니다.
    pub active_ws_connections: usize,
    /// 알림과 대시보드에 사용할 관측 이벤트 속도입니다.
    pub events_per_sec: f64,
    /// 최근 1분 동안의 reconnect 횟수입니다.
    pub reconnects_last_minute: usize,
    /// 업로드 또는 알림을 기다리는 파일 수입니다.
    pub upload_backlog_files: usize,
    /// 로컬 spool에 현재 남아 있는 바이트 수입니다.
    pub local_spool_bytes: u64,
    /// 마지막 업로드 성공 시각이며 Unix millisecond 단위입니다.
    pub last_successful_upload_at_ms: Option<i64>,
}

impl CollectorStatus {
    /// 실제 구독 수가 현재 배정 수와 일치하는지 확인합니다.
    ///
    /// # 상세 설명
    /// 이 함수는 Orchestrator가 “배정됨”과 “실제로 구독됨”을 구분하도록 돕습니다. 불일치가 있더라도
    /// v1에서는 거래량 기반 hot migration을 일으키지 않으며, 운영 경고나 복구 항목으로 노출해야 합니다.
    ///
    /// # 인자
    /// 이 메서드는 추가 인자를 받지 않습니다.
    ///
    /// # 반환값
    /// 마켓과 토큰 구독 수가 배정 수와 모두 일치하면 `true`를 반환합니다.
    ///
    /// # 예시 — 입력 / 출력
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
    /// # 관련
    /// - [`CollectorCapacity`]
    pub fn assignment_matches_subscription(&self) -> bool {
        self.assigned_market_count == self.subscribed_market_count
            && self.assigned_token_count == self.subscribed_token_count
    }
}

/// 단일 배정과 함께 Collector에 전달되는 실행 제한입니다.
///
/// # 상세 설명
/// 이 제한은 Collector가 배정된 토큰을 WebSocket worker로 어떻게 나눌지 설명합니다. 이는 Collector가
/// 등록 시 광고하는 최대치인 등록 용량과 구분됩니다.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignmentLimits {
    /// 이 배정에서 사용할 수 있는 최대 WebSocket 연결 수입니다.
    pub max_ws_connections: usize,
    /// WebSocket 연결 하나에 담을 수 있는 최대 토큰 수입니다.
    pub max_tokens_per_ws_connection: usize,
}

/// 토큰 ID 묶음에 대해 계획된 구조적 handoff입니다.
///
/// # 상세 설명
/// handoff action은 v1에서 Collector 장애 복구나 운영자 지시 이동 같은 구조적 이동에만 사용합니다.
/// 거래량 급증만으로는 생성하지 않습니다.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandoffAction {
    /// handoff 대상 token ID 목록입니다.
    pub token_ids: Vec<String>,
    /// 원본 Collector ID이며 없을 수도 있습니다.
    pub from_collector_id: Option<String>,
    /// 목적지 Collector ID이며 없을 수도 있습니다.
    pub to_collector_id: Option<String>,
    /// 적용할 handoff 방식입니다.
    pub mode: HandoffMode,
    /// 선택적 중첩 구간이며 millisecond 단위입니다.
    pub overlap_window_ms: Option<u64>,
}

/// 단일 Collector에 대한 배정 payload입니다.
///
/// # 상세 설명
/// Orchestrator는 이 payload를 Collector에 반환합니다. `token_ids`와 `market_ids` 벡터는 참조된
/// `assignment_version`에서 권위 있는 수집 대상 집합입니다.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectorAssignment {
    /// 배정을 받을 Collector ID입니다.
    pub collector_id: String,
    /// Collector에 배정된 market ID 목록입니다.
    pub market_ids: Vec<String>,
    /// Collector에 배정된 token ID 목록입니다.
    pub token_ids: Vec<String>,
    /// worker 분할에 사용할 런타임 제한입니다.
    pub limits: AssignmentLimits,
    /// 계획된 handoff action 목록이며 없을 수 있습니다.
    pub handoff_actions: Vec<HandoffAction>,
}

/// 모든 Collector에 대한 버전 관리 배정 계획입니다.
///
/// # 상세 설명
/// 계획은 하나의 마켓 유니버스 버전을 참조하고 전역 제어 상태를 포함합니다. `control_state`가
/// `EmergencyStopByBudget`이면 token ID가 들어 있더라도 Collector는 수집을 중단해야 합니다.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignmentPlan {
    /// 단조 증가하는 배정 버전입니다.
    pub version: u64,
    /// 이 배정을 만들 때 사용한 마켓 유니버스 버전입니다.
    pub universe_version: u64,
    /// 계획 생성 시각이며 Unix millisecond 단위입니다.
    pub generated_at_ms: i64,
    /// 전역 제어 상태입니다.
    pub control_state: ControlState,
    /// Collector ID로 정렬된 Collector별 배정입니다.
    pub collectors: BTreeMap<String, CollectorAssignment>,
}

/// 새로 업로드된 GCS object에 대한 메타데이터 알림입니다.
///
/// # 상세 설명
/// Collector는 닫힌 로컬 JSONL 파일을 업로드한 뒤 이 payload를 보냅니다. Orchestrator는 메타데이터를
/// 저장하고 Compactor는 나중에 GCS에서 object 본문을 내려받습니다. JSONL 본문은 제어면을 통해
/// 전송하지 않습니다.
///
/// # 예시 — 입력 / 출력
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
/// # 관련
/// - [`ProcessedObject`]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectNotification {
    /// object를 업로드한 Collector입니다.
    pub collector_id: String,
    /// object가 생성될 당시 적용 중이던 배정 버전입니다.
    pub assignment_version: u64,
    /// GCS bucket 이름입니다.
    pub bucket: String,
    /// GCS object 이름입니다.
    pub object_name: String,
    /// GCS object generation 값입니다.
    pub generation: String,
    /// Collector가 보고한 JSONL 라인 수입니다.
    pub line_count: usize,
    /// 알 수 있는 경우 첫 원천 이벤트 시각이며 Unix millisecond 단위입니다.
    pub first_event_ts_ms: Option<i64>,
    /// 알 수 있는 경우 마지막 원천 이벤트 시각이며 Unix millisecond 단위입니다.
    pub last_event_ts_ms: Option<i64>,
    /// GCS 메타데이터에서 온 선택적 CRC32C checksum입니다.
    pub checksum_crc32c: Option<String>,
}

impl ObjectNotification {
    /// object 단위 멱등성 key를 만듭니다.
    ///
    /// # 상세 설명
    /// GCS는 같은 object 이름에 대해 여러 generation을 가질 수 있습니다. generation을 key에 포함하면
    /// 같은 이름으로 나중에 다시 업로드된 object가 잘못 skip되는 일을 막을 수 있습니다.
    ///
    /// # 인자
    /// 이 메서드는 추가 인자를 받지 않습니다.
    ///
    /// # 반환값
    /// 안정적인 `bucket/object_name#generation` key를 반환합니다.
    ///
    /// # 예시 — 입력 / 출력
    /// ```rust
    /// # use polymarket_collector::common::contracts::ObjectNotification;
    /// # let notification = ObjectNotification { collector_id: "c".into(), assignment_version: 1,
    /// # bucket: "b".into(), object_name: "o".into(), generation: "g".into(),
    /// # line_count: 1, first_event_ts_ms: None, last_event_ts_ms: None, checksum_crc32c: None };
    /// assert_eq!(notification.idempotency_key(), "b/o#g");
    /// ```
    ///
    /// # 관련
    /// - [`ProcessedObject::idempotency_key`]
    pub fn idempotency_key(&self) -> String {
        format!("{}/{}#{}", self.bucket, self.object_name, self.generation)
    }
}

/// GCS object generation이 병합 완료되었음을 기록하는 영속 manifest 행입니다.
///
/// # 상세 설명
/// Compactor 재시작, 중복 알림, prefix polling은 같은 object를 두 번 이상 발견할 수 있습니다.
/// 이 manifest 행은 이미 병합한 object generation을 Compactor가 건너뛰도록 해줍니다.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessedObject {
    /// GCS bucket 이름입니다.
    pub bucket: String,
    /// GCS object 이름입니다.
    pub object_name: String,
    /// GCS object generation 값입니다.
    pub generation: String,
    /// 처리 완료 시각이며 Unix millisecond 단위입니다.
    pub processed_at_ms: i64,
    /// 입력 JSONL 라인 수입니다.
    pub input_line_count: usize,
    /// 유효한 출력 라인 수입니다.
    pub valid_line_count: usize,
    /// 형식 오류 등으로 건너뛴 라인 수입니다.
    pub skipped_line_count: usize,
    /// 병합 결과 경로 또는 object 이름입니다.
    pub output_path: String,
}

impl ProcessedObject {
    /// object 알림과 같은 멱등성 key를 만듭니다.
    ///
    /// # 상세 설명
    /// Compactor는 이 값을 [`ObjectNotification`] key와 비교하여 입력 object generation이 이미
    /// 처리되었는지 판단할 수 있습니다.
    ///
    /// # 인자
    /// 이 메서드는 추가 인자를 받지 않습니다.
    ///
    /// # 반환값
    /// 안정적인 `bucket/object_name#generation` key를 반환합니다.
    ///
    /// # 예시 — 입력 / 출력
    /// ```rust
    /// # use polymarket_collector::common::contracts::ProcessedObject;
    /// # let processed = ProcessedObject { bucket: "b".into(), object_name: "o".into(),
    /// # generation: "g".into(), processed_at_ms: 0, input_line_count: 1,
    /// # valid_line_count: 1, skipped_line_count: 0, output_path: "merged".into() };
    /// assert_eq!(processed.idempotency_key(), "b/o#g");
    /// ```
    ///
    /// # 관련
    /// - [`ObjectNotification::idempotency_key`]
    pub fn idempotency_key(&self) -> String {
        format!("{}/{}#{}", self.bucket, self.object_name, self.generation)
    }
}

/// GCS 비용 가드레일에 사용할 예산 임계값 설정입니다.
///
/// # 상세 설명
/// Orchestrator는 이 정책으로 Discord 경고를 보낼 시점과 긴급 중단에 들어갈 시점을 결정합니다.
/// Discord webhook URL은 코드나 manifest에 직접 저장하지 말고 secret 이름으로 참조해야 합니다.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BudgetPolicy {
    /// 추정 USD 비용 기준의 경고 임계값입니다.
    pub warning_threshold_usd: f64,
    /// 추정 USD 비용 기준의 강제 중단 임계값입니다.
    pub hard_stop_threshold_usd: f64,
    /// Discord webhook URL을 조회할 secret 이름 또는 config key입니다.
    pub discord_webhook_secret_name: String,
    /// 비용 확인 주기이며 second 단위입니다.
    pub check_interval_secs: u64,
}

/// Orchestrator가 유지하는 런타임 예산 상태입니다.
///
/// # 상세 설명
/// Google Cloud Billing 데이터는 지연될 수 있으므로, 이 상태는 업로드 바이트와 object 작업 수를
/// 바탕으로 내부 추정치를 추적합니다. 하드 리밋을 넘으면 전역 제어 상태를
/// [`ControlState::EmergencyStopByBudget`]로 바꿔야 합니다.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BudgetState {
    /// 현재 추정 GCS 비용이며 USD 단위입니다.
    pub estimated_gcs_cost_usd: f64,
    /// Collector 알림으로 집계한 업로드 바이트 수입니다.
    pub uploaded_bytes: u64,
    /// GCS object 생성 작업 수입니다.
    pub object_create_count: u64,
    /// GCS object 목록 조회 작업 수입니다.
    pub object_list_count: u64,
    /// GCS object 다운로드 작업 수입니다.
    pub object_get_count: u64,
    /// GCS object 삭제 작업 수입니다.
    pub object_delete_count: u64,
    /// 예산 정책에서 도출한 현재 전역 제어 상태입니다.
    pub control_state: ControlState,
}

impl BudgetState {
    /// 경고 임계값을 넘었는지 확인합니다.
    ///
    /// # 상세 설명
    /// 이 함수는 순수 비교 helper입니다. Discord 알림 전송과 중복 spam 방지를 위한 알림 상태 기록은
    /// 호출자가 책임집니다.
    ///
    /// # 인자
    /// * `policy` — 운영자가 설정한 예산 임계값입니다.
    ///
    /// # 반환값
    /// 추정 비용이 경고 임계값 이상이면 `true`를 반환합니다.
    ///
    /// # 예시 — 입력 / 출력
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
    /// # 관련
    /// - [`BudgetState::hard_stop_exceeded`]
    pub fn warning_exceeded(&self, policy: &BudgetPolicy) -> bool {
        self.estimated_gcs_cost_usd >= policy.warning_threshold_usd
    }

    /// 강제 중단 임계값을 넘었는지 확인합니다.
    ///
    /// # 상세 설명
    /// 이 함수가 `true`를 반환하면 Orchestrator는 긴급 중단 상태를 게시해야 하며, Collector는 정책에
    /// 따라 로컬 spool을 유지한 채 WebSocket 구독을 닫아야 합니다.
    ///
    /// # 인자
    /// * `policy` — 운영자가 설정한 예산 임계값입니다.
    ///
    /// # 반환값
    /// 추정 비용이 강제 중단 임계값 이상이면 `true`를 반환합니다.
    ///
    /// # 예시 — 입력 / 출력
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
    /// # 관련
    /// - [`ControlState::EmergencyStopByBudget`]
    pub fn hard_stop_exceeded(&self, policy: &BudgetPolicy) -> bool {
        self.estimated_gcs_cost_usd >= policy.hard_stop_threshold_usd
    }
}

/// Rust Collector가 기록하는 정규화된 이벤트 행입니다.
///
/// # 상세 설명
/// raw/merged 오더북 출력에 사용하는 행 단위 JSONL 계약입니다. Collector는 parser 버그나 schema 변경을
/// 나중에 감사할 수 있도록 원천 payload를 `raw`에 보존합니다.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderbookEvent {
    /// 이벤트 schema 버전입니다.
    pub schema_version: u32,
    /// 정규화된 이벤트 종류입니다.
    pub event_type: OrderbookEventType,
    /// 유니버스 스냅샷에서 알 수 있는 경우의 market ID입니다.
    pub market_id: Option<String>,
    /// CLOB token ID입니다.
    pub asset: String,
    /// BUY, SELL, bid, ask 같은 side 값입니다.
    pub side: Option<String>,
    /// 십진수 가격입니다.
    pub price: Option<f64>,
    /// 십진수 수량입니다.
    pub size: Option<f64>,
    /// 원천 이벤트 시각이며 Unix millisecond 단위입니다.
    pub timestamp_ms: i64,
    /// Collector가 이벤트를 받은 시각이며 Unix millisecond 단위입니다.
    pub received_at_ms: i64,
    /// 이벤트를 관측한 Collector입니다.
    pub collector_id: String,
    /// 이벤트 관측 당시 적용 중이던 배정 버전입니다.
    pub assignment_version: u64,
    /// 이벤트 관측 당시 적용 중이던 마켓 유니버스 버전입니다.
    pub universe_version: u64,
    /// 원본 원천 JSON payload입니다.
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
