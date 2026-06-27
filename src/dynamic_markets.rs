use crate::http_client::HttpClient;
use crate::market_discovery::{fetch_markets_with_client, Market};
use anyhow::{Context, Result};
use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Default runtime market refresh interval in seconds.
pub const DEFAULT_MARKET_REFRESH_INTERVAL_SECS: u64 = 600;
/// Default stale-market retention window in hours.
pub const DEFAULT_STALE_MARKET_TTL_HOURS: u64 = 12;
/// Number of seconds in one hour, used to convert the stale-market TTL.
const SECONDS_PER_HOUR: u64 = 60 * 60;

/// Runtime policy for live Polymarket market discovery refreshes.
///
/// # Detailed Description
/// This policy captures the two timing knobs that define the live-market
/// lifecycle. `refresh_interval` controls how often live paths should request a
/// fresh Gamma market snapshot. `stale_ttl` controls how long a market that is
/// missing from successful refresh responses remains in the runtime token set
/// before it is removed. Keeping both values together prevents the aggregator
/// and standalone collector from drifting into different lifecycle semantics.
///
/// # Arguments
/// This type is constructed directly or via [`Default`]. It has no positional
/// arguments.
///
/// # Returns
/// A policy value that can be passed to [`MarketRegistry::from_initial_markets`].
///
/// # Example — Input / Output
/// ```rust
/// use std::time::Duration;
/// use polymarket_collector::dynamic_markets::{
///     MarketRefreshPolicy,
///     DEFAULT_MARKET_REFRESH_INTERVAL_SECS,
///     DEFAULT_STALE_MARKET_TTL_HOURS,
/// };
///
/// let policy = MarketRefreshPolicy::default();
/// assert_eq!(policy.refresh_interval, Duration::from_secs(DEFAULT_MARKET_REFRESH_INTERVAL_SECS));
/// assert_eq!(policy.stale_ttl, Duration::from_secs(DEFAULT_STALE_MARKET_TTL_HOURS * 60 * 60));
/// ```
///
/// # Related
/// - [`MarketRegistry`]
/// - [`RefreshOutcome`]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketRefreshPolicy {
    /// How often runtime refresh loops should fetch a fresh market list.
    pub refresh_interval: Duration,
    /// How long missing markets remain subscribed before removal.
    pub stale_ttl: Duration,
}

impl Default for MarketRefreshPolicy {
    fn default() -> Self {
        Self {
            refresh_interval: Duration::from_secs(DEFAULT_MARKET_REFRESH_INTERVAL_SECS),
            stale_ttl: Duration::from_secs(DEFAULT_STALE_MARKET_TTL_HOURS * SECONDS_PER_HOUR),
        }
    }
}

/// Immutable token snapshot emitted by a [`MarketRegistry`].
///
/// # Detailed Description
/// A snapshot is the handoff format that runtime components can use without
/// knowing the registry's per-market bookkeeping. `token_ids` contains a stable,
/// deduplicated set of tokens from active markets plus stale markets still
/// inside their grace window. `version` changes only when that token output
/// changes, making it a cheap signal for assignment polling and collector
/// subscription diffs.
///
/// # Arguments
/// Snapshot values are returned by [`MarketRegistry::snapshot`]; callers do not
/// normally construct them manually.
///
/// # Returns
/// A stable, cloneable view of the live token universe.
///
/// # Example — Input / Output
/// ```rust,ignore
/// let snapshot = registry.snapshot();
/// assert_eq!(snapshot.token_ids, vec!["token-a".to_string(), "token-b".to_string()]);
/// assert_eq!(snapshot.version, 1);
/// ```
///
/// # Related
/// - [`MarketRegistry::snapshot`]
/// - [`RefreshOutcome`]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketSnapshot {
    /// Stable, deduplicated token IDs from active and retained stale markets.
    pub token_ids: Vec<String>,
    /// Version increments only when `token_ids` changes.
    pub version: u64,
    /// Number of markets currently present in the latest successful refresh.
    pub active_markets: usize,
    /// Number of missing markets retained inside the stale grace window.
    pub stale_markets: usize,
}

/// Warning classification for refresh attempts that intentionally keep state.
///
/// # Detailed Description
/// Runtime refresh after startup is fail-open: an API failure or an empty usable
/// response must not clear active subscriptions. This enum records why a refresh
/// was ignored so callers can log a precise warning while preserving the current
/// registry state.
///
/// # Arguments
/// Constructed by [`MarketRegistry::apply_refresh_result`] or
/// [`MarketRegistry::apply_successful_refresh`].
///
/// # Returns
/// A warning value embedded in [`RefreshOutcome::warning`].
///
/// # Example — Input / Output
/// ```rust
/// use polymarket_collector::dynamic_markets::RefreshWarning;
///
/// let warning = RefreshWarning::EmptyUsableRefresh;
/// assert_eq!(warning, RefreshWarning::EmptyUsableRefresh);
/// ```
///
/// # Related
/// - [`RefreshOutcome`]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefreshWarning {
    /// The market source returned an error and the registry kept old state.
    FetchFailed(String),
    /// The market source returned no usable orderbook markets after filtering.
    EmptyUsableRefresh,
}

/// Result of applying a live-market refresh to a registry.
///
/// # Detailed Description
/// The outcome reports both lifecycle counters and whether the emitted token
/// snapshot changed. It is intentionally small enough to log and to assert in
/// tests. A warning outcome with `changed == false` represents fail-open behavior
/// after startup.
///
/// # Arguments
/// Constructed by [`MarketRegistry`] methods.
///
/// # Returns
/// A structured refresh result including counters, current version, and token
/// count.
///
/// # Example — Input / Output
/// ```rust,ignore
/// let outcome = registry.apply_successful_refresh(now, refreshed_markets);
/// assert!(outcome.changed);
/// assert_eq!(outcome.added_markets, 1);
/// assert_eq!(outcome.warning, None);
/// ```
///
/// # Related
/// - [`MarketRegistry::apply_successful_refresh`]
/// - [`MarketRegistry::apply_refresh_result`]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshOutcome {
    /// True when the deduplicated token output changed and version incremented.
    pub changed: bool,
    /// Number of brand-new market entries inserted during this refresh.
    pub added_markets: usize,
    /// Number of previously active markets marked stale during this refresh.
    pub marked_stale: usize,
    /// Number of stale markets removed after exceeding the TTL.
    pub removed_markets: usize,
    /// Current registry snapshot version.
    pub version: u64,
    /// Current deduplicated token count.
    pub token_count: usize,
    /// Fail-open warning, if the refresh was ignored.
    pub warning: Option<RefreshWarning>,
}

/// Source abstraction for live market discovery.
///
/// # Detailed Description
/// This trait is the injection boundary used by later aggregator and collector
/// runtime loops. Production can use [`GammaMarketSource`], while tests can pass
/// a deterministic source that returns exact market snapshots or errors without
/// mutating global HTTP state.
///
/// # Arguments
/// Implementors choose their own construction arguments.
///
/// # Returns
/// [`fetch_usable_markets`](LiveMarketSource::fetch_usable_markets) returns only
/// markets that are usable for live orderbook collection.
///
/// # Example — Input / Output
/// ```rust,ignore
/// let markets = source.fetch_usable_markets().await?;
/// assert!(markets.iter().all(polymarket_collector::dynamic_markets::is_usable_orderbook_market));
/// ```
///
/// # Related
/// - [`GammaMarketSource`]
#[async_trait]
pub trait LiveMarketSource: Send + Sync {
    /// Fetch the current usable live-market snapshot.
    async fn fetch_usable_markets(&self) -> Result<Vec<Market>>;
}

/// Gamma-backed implementation of [`LiveMarketSource`].
///
/// # Detailed Description
/// `GammaMarketSource` reuses the repository's existing `HttpClient` abstraction
/// and `market_discovery::fetch_markets_with_client` pagination logic. It always
/// requests `active=true` and `closed=false`, then filters the returned markets
/// to orderbook-enabled, accepting, non-archived markets with at least one token
/// ID. This keeps production and tests on the same request/parsing path.
///
/// # Arguments
/// * `client` — Shared HTTP client used for Gamma API requests.
///
/// # Returns
/// A source object that can be used by live startup and refresh loops.
///
/// # Example — Input / Output
/// ```rust,ignore
/// use std::sync::Arc;
/// use polymarket_collector::dynamic_markets::{GammaMarketSource, LiveMarketSource};
/// use polymarket_collector::http_client::ReqwestHttpClient;
///
/// let source = GammaMarketSource::new(Arc::new(ReqwestHttpClient::new()));
/// let markets = source.fetch_usable_markets().await?;
/// assert!(markets.iter().all(|market| market.enable_order_book));
/// # anyhow::Ok(())
/// ```
///
/// # Related
/// - [`fetch_usable_markets_with_client`]
#[derive(Clone)]
pub struct GammaMarketSource {
    client: Arc<dyn HttpClient>,
}

impl GammaMarketSource {
    /// Build a Gamma market source from an injected HTTP client.
    ///
    /// # Detailed Description
    /// This constructor is intentionally small so tests and runtime entry points
    /// can pass either an in-memory client or the production reqwest-backed
    /// client without changing refresh logic.
    ///
    /// # Arguments
    /// * `client` — Shared HTTP client used for all Gamma requests.
    ///
    /// # Returns
    /// A [`GammaMarketSource`] that implements [`LiveMarketSource`].
    ///
    /// # Example — Input / Output
    /// ```rust,ignore
    /// let source = GammaMarketSource::new(client);
    /// assert_eq!(source.fetch_usable_markets().await?.len(), 2);
    /// # anyhow::Ok(())
    /// ```
    ///
    /// # Related
    /// - [`LiveMarketSource`]
    pub fn new(client: Arc<dyn HttpClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl LiveMarketSource for GammaMarketSource {
    async fn fetch_usable_markets(&self) -> Result<Vec<Market>> {
        fetch_usable_markets_with_client(self.client.as_ref()).await
    }
}

/// Registry of live markets and retained stale markets.
///
/// # Detailed Description
/// `MarketRegistry` is the shared lifecycle state for live orderbook paths. It
/// tracks markets keyed by stable market identifiers, remembers when a market
/// was last seen, marks missing markets as stale, and removes them only after
/// the configured stale TTL. Its snapshot output is deterministic and deduped so
/// aggregator assignment and collector subscription diffs can compare token
/// vectors directly.
///
/// # Arguments
/// Construct with [`MarketRegistry::from_initial_markets`] after a successful
/// startup fetch.
///
/// # Returns
/// A registry whose snapshot can drive assignments or WebSocket subscriptions.
///
/// # Example — Input / Output
/// ```rust,ignore
/// let mut registry = MarketRegistry::from_initial_markets(
///     now,
///     MarketRefreshPolicy::default(),
///     vec![market_with_tokens("m1", &["token-a"])],
/// )?;
/// assert_eq!(registry.snapshot().token_ids, vec!["token-a".to_string()]);
/// # anyhow::Ok(())
/// ```
///
/// # Related
/// - [`MarketSnapshot`]
/// - [`RefreshOutcome`]
#[derive(Debug, Clone)]
pub struct MarketRegistry {
    policy: MarketRefreshPolicy,
    entries: BTreeMap<String, MarketEntry>,
    version: u64,
}

#[derive(Debug, Clone)]
struct MarketEntry {
    _market: Market,
    token_ids: Vec<String>,
    _last_seen_at: Instant,
    stale_since: Option<Instant>,
}

impl MarketRegistry {
    /// Create a registry from the startup market snapshot.
    ///
    /// # Detailed Description
    /// Live startup is fail-fast: unlike runtime refresh, an initial API error
    /// or empty usable market set should prevent the process from starting. This
    /// constructor accepts the successful market list, filters unusable markets,
    /// initializes the registry, and errors if no usable token output remains.
    ///
    /// # Arguments
    /// * `now` — Logical timestamp for the initial snapshot.
    /// * `policy` — Refresh and stale-retention timing policy.
    /// * `markets` — Raw Gamma markets from startup discovery.
    ///
    /// # Returns
    /// A ready registry, or an error when the startup snapshot has no usable
    /// orderbook tokens.
    ///
    /// # Example — Input / Output
    /// ```rust,ignore
    /// let registry = MarketRegistry::from_initial_markets(now, policy, markets)?;
    /// assert!(!registry.snapshot().token_ids.is_empty());
    /// # anyhow::Ok(())
    /// ```
    ///
    /// # Related
    /// - [`filter_usable_markets`]
    pub fn from_initial_markets(
        now: Instant,
        policy: MarketRefreshPolicy,
        markets: Vec<Market>,
    ) -> Result<Self> {
        let mut registry = Self {
            policy,
            entries: BTreeMap::new(),
            version: 0,
        };
        let outcome = registry.apply_successful_refresh(now, markets);
        if outcome.token_count == 0 {
            anyhow::bail!("initial market refresh produced no usable orderbook tokens");
        }
        Ok(registry)
    }

    /// Return the policy used by this registry.
    ///
    /// # Detailed Description
    /// Runtime loops need policy access to schedule refresh intervals and to
    /// report stale-retention settings. Exposing the policy avoids duplicating
    /// constants outside the registry.
    ///
    /// # Arguments
    /// This method has no arguments.
    ///
    /// # Returns
    /// The immutable [`MarketRefreshPolicy`] configured at construction time.
    ///
    /// # Example — Input / Output
    /// ```rust,ignore
    /// assert_eq!(registry.policy().refresh_interval, Duration::from_secs(600));
    /// ```
    ///
    /// # Related
    /// - [`MarketRefreshPolicy`]
    pub fn policy(&self) -> MarketRefreshPolicy {
        self.policy
    }

    /// Return a deterministic snapshot of active plus retained stale tokens.
    ///
    /// # Detailed Description
    /// The snapshot includes tokens from active markets and markets that are
    /// stale but still inside the configured grace window. Tokens are sorted and
    /// deduplicated so downstream code can compute stable diffs.
    ///
    /// # Arguments
    /// This method has no arguments.
    ///
    /// # Returns
    /// A [`MarketSnapshot`] containing token IDs, version, and market counters.
    ///
    /// # Example — Input / Output
    /// ```rust,ignore
    /// let snapshot = registry.snapshot();
    /// assert_eq!(snapshot.version, 2);
    /// assert_eq!(snapshot.stale_markets, 1);
    /// ```
    ///
    /// # Related
    /// - [`MarketSnapshot`]
    pub fn snapshot(&self) -> MarketSnapshot {
        let mut tokens = BTreeSet::new();
        let mut active_markets = 0;
        let mut stale_markets = 0;

        for entry in self.entries.values() {
            if entry.stale_since.is_some() {
                stale_markets += 1;
            } else {
                active_markets += 1;
            }

            for token in &entry.token_ids {
                tokens.insert(token.clone());
            }
        }

        MarketSnapshot {
            token_ids: tokens.into_iter().collect(),
            version: self.version,
            active_markets,
            stale_markets,
        }
    }

    /// Apply a refresh result using fail-open runtime semantics.
    ///
    /// # Detailed Description
    /// This is the preferred method for post-startup refresh loops. A successful
    /// market list is reconciled into the registry. A fetch error is recorded as
    /// [`RefreshWarning::FetchFailed`] and leaves the registry unchanged.
    ///
    /// # Arguments
    /// * `now` — Logical refresh time.
    /// * `result` — Market-source result from a live API fetch.
    ///
    /// # Returns
    /// A [`RefreshOutcome`] describing either the applied lifecycle changes or
    /// the fail-open warning.
    ///
    /// # Example — Input / Output
    /// ```rust,ignore
    /// let outcome = registry.apply_refresh_result(now, Err(anyhow!("timeout")));
    /// assert!(!outcome.changed);
    /// assert!(matches!(outcome.warning, Some(RefreshWarning::FetchFailed(_))));
    /// ```
    ///
    /// # Related
    /// - [`MarketRegistry::apply_successful_refresh`]
    pub fn apply_refresh_result(
        &mut self,
        now: Instant,
        result: Result<Vec<Market>>,
    ) -> RefreshOutcome {
        match result {
            Ok(markets) => self.apply_successful_refresh(now, markets),
            Err(error) => self.warning_outcome(RefreshWarning::FetchFailed(error.to_string())),
        }
    }

    /// Apply a successful market refresh to the registry.
    ///
    /// # Detailed Description
    /// Incoming markets are filtered to usable orderbook markets. New markets are
    /// added immediately. Previously seen markets missing from the refresh are
    /// marked stale and retained until `policy.stale_ttl` elapses. Stale markets
    /// that reappear are restored to active. If the successful response contains
    /// no usable markets, the registry stays unchanged and reports a fail-open
    /// empty-refresh warning.
    ///
    /// # Arguments
    /// * `now` — Logical refresh time used for `last_seen_at` and `stale_since`.
    /// * `markets` — Raw markets from Gamma API or a test source.
    ///
    /// # Returns
    /// A [`RefreshOutcome`] with lifecycle counters and current token version.
    ///
    /// # Example — Input / Output
    /// ```rust,ignore
    /// let outcome = registry.apply_successful_refresh(now, vec![new_market]);
    /// assert_eq!(outcome.added_markets, 1);
    /// assert!(registry.snapshot().token_ids.contains(&"new-token".to_string()));
    /// ```
    ///
    /// # Related
    /// - [`filter_usable_markets`]
    pub fn apply_successful_refresh(
        &mut self,
        now: Instant,
        markets: Vec<Market>,
    ) -> RefreshOutcome {
        let incoming = usable_market_entries(now, markets);
        if incoming.is_empty() {
            return self.warning_outcome(RefreshWarning::EmptyUsableRefresh);
        }

        let before_tokens = self.snapshot().token_ids;
        let mut added_markets = 0;
        let mut marked_stale = 0;

        for (key, entry) in &incoming {
            if self.entries.contains_key(key) {
                self.entries.insert(key.clone(), entry.clone());
            } else {
                self.entries.insert(key.clone(), entry.clone());
                added_markets += 1;
            }
        }

        for (key, entry) in self.entries.iter_mut() {
            if incoming.contains_key(key) {
                continue;
            }
            if entry.stale_since.is_none() {
                entry.stale_since = Some(now);
                marked_stale += 1;
            }
        }

        let stale_ttl = self.policy.stale_ttl;
        let expired_keys: Vec<String> = self
            .entries
            .iter()
            .filter_map(|(key, entry)| {
                entry
                    .stale_since
                    .filter(|stale_since| now.duration_since(*stale_since) >= stale_ttl)
                    .map(|_| key.clone())
            })
            .collect();
        let removed_markets = expired_keys.len();
        for key in expired_keys {
            self.entries.remove(&key);
        }

        let after_snapshot = self.snapshot();
        let changed = before_tokens != after_snapshot.token_ids;
        if changed {
            self.version += 1;
        }
        let snapshot = self.snapshot();

        RefreshOutcome {
            changed,
            added_markets,
            marked_stale,
            removed_markets,
            version: snapshot.version,
            token_count: snapshot.token_ids.len(),
            warning: None,
        }
    }

    fn warning_outcome(&self, warning: RefreshWarning) -> RefreshOutcome {
        let snapshot = self.snapshot();
        RefreshOutcome {
            changed: false,
            added_markets: 0,
            marked_stale: 0,
            removed_markets: 0,
            version: snapshot.version,
            token_count: snapshot.token_ids.len(),
            warning: Some(warning),
        }
    }
}

/// Return whether a market can be used for live orderbook collection.
///
/// # Detailed Description
/// Polymarket Gamma responses include markets that may be closed, archived,
/// not accepting orders, not orderbook-enabled, or missing CLOB token IDs. Live
/// collectors only need active orderbook markets with at least one non-empty
/// token ID. This predicate encodes that filter in one place so aggregator and
/// standalone live paths stay aligned.
///
/// # Arguments
/// * `market` — Parsed Gamma market metadata.
///
/// # Returns
/// `true` when the market should contribute token IDs to live collection;
/// otherwise `false`.
///
/// # Example — Input / Output
/// ```rust,ignore
/// let market = market_with_tokens("m1", &["token-a"]);
/// assert!(is_usable_orderbook_market(&market));
/// ```
///
/// # Related
/// - [`filter_usable_markets`]
pub fn is_usable_orderbook_market(market: &Market) -> bool {
    market.active
        && !market.closed
        && !market.archived
        && market.enable_order_book
        && market.accepting_orders
        && !normalized_token_ids(&market.token_ids).is_empty()
        && market_key(market).is_some()
}

/// Filter raw Gamma markets down to live orderbook markets.
///
/// # Detailed Description
/// This helper keeps the raw parser separate from runtime eligibility rules.
/// It preserves market metadata but normalizes each retained market's `token_ids`
/// by trimming empty tokens, sorting, and deduplicating them.
///
/// # Arguments
/// * `markets` — Raw parsed Gamma market list.
///
/// # Returns
/// A list of markets that are usable for live orderbook collection.
///
/// # Example — Input / Output
/// ```rust,ignore
/// let usable = filter_usable_markets(vec![open_market, closed_market]);
/// assert_eq!(usable.len(), 1);
/// assert_eq!(usable[0].token_ids, vec!["token-a".to_string()]);
/// ```
///
/// # Related
/// - [`is_usable_orderbook_market`]
pub fn filter_usable_markets(markets: Vec<Market>) -> Vec<Market> {
    markets
        .into_iter()
        .filter_map(|mut market| {
            market.token_ids = normalized_token_ids(&market.token_ids);
            is_usable_orderbook_market(&market).then_some(market)
        })
        .collect()
}

/// Fetch usable orderbook markets from Gamma with an injected HTTP client.
///
/// # Detailed Description
/// This function is the API/client injection boundary for live startup and
/// refresh code. It reuses the existing paginated Gamma market fetch with
/// `active=true` and `closed=false`, then applies the live orderbook filter.
/// Tests can pass [`crate::http_client::InMemoryHttpClient`] to drive exact API
/// responses without network calls.
///
/// # Arguments
/// * `client` — HTTP client used to call Gamma API.
///
/// # Returns
/// A filtered market list, or an error if the Gamma request/parsing fails.
///
/// # Example — Input / Output
/// ```rust,ignore
/// let markets = fetch_usable_markets_with_client(&client).await?;
/// assert!(markets.iter().all(is_usable_orderbook_market));
/// # anyhow::Ok(())
/// ```
///
/// # Related
/// - [`GammaMarketSource`]
/// - [`filter_usable_markets`]
pub async fn fetch_usable_markets_with_client(client: &dyn HttpClient) -> Result<Vec<Market>> {
    let markets = fetch_markets_with_client(client, Some(true), Some(false))
        .await
        .context("failed to fetch active open markets from Gamma")?;
    Ok(filter_usable_markets(markets))
}

fn usable_market_entries(now: Instant, markets: Vec<Market>) -> BTreeMap<String, MarketEntry> {
    let mut entries = BTreeMap::new();
    for market in filter_usable_markets(markets) {
        let Some(key) = market_key(&market) else {
            continue;
        };
        let token_ids = normalized_token_ids(&market.token_ids);
        entries.insert(
            key,
            MarketEntry {
                _market: market,
                token_ids,
                _last_seen_at: now,
                stale_since: None,
            },
        );
    }
    entries
}

fn normalized_token_ids(token_ids: &[String]) -> Vec<String> {
    token_ids
        .iter()
        .map(|token| token.trim())
        .filter(|token| !token.is_empty())
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn market_key(market: &Market) -> Option<String> {
    let id = market.id.trim();
    if !id.is_empty() {
        return Some(id.to_string());
    }

    let condition_id = market.condition_id.trim();
    if !condition_id.is_empty() {
        return Some(condition_id.to_string());
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_client::{HttpResponse, InMemoryHttpClient};

    fn market(id: &str, token_ids: &[&str]) -> Market {
        Market {
            id: id.to_string(),
            condition_id: format!("condition-{id}"),
            question: format!("Question {id}?"),
            slug: format!("slug-{id}"),
            description: None,
            active: true,
            closed: false,
            archived: false,
            end_date: None,
            start_date: None,
            created_at: None,
            updated_at: None,
            volume: None,
            liquidity: None,
            volume_24h: None,
            outcomes: None,
            outcome_prices: None,
            token_ids: token_ids.iter().map(|token| token.to_string()).collect(),
            enable_order_book: true,
            order_min_size: None,
            order_price_min_tick_size: None,
            neg_risk: false,
            accepting_orders: true,
            clob_rewards: Vec::new(),
            rewards_min_size: None,
            rewards_max_spread: None,
            competitive: None,
        }
    }

    fn gamma_url() -> String {
        "https://gamma-api.polymarket.com/markets?limit=100&active=true&closed=false".to_string()
    }

    fn gamma_market_json(id: &str, token_ids: &[&str]) -> serde_json::Value {
        // Test token IDs are plain UTF-8 strings, so serializing this vector to
        // the Gamma `clobTokenIds` JSON-string field is expected to succeed.
        let clob_token_ids = serde_json::to_string(&token_ids).expect("token ids serialize");
        serde_json::json!({
            "id": id,
            "conditionId": format!("condition-{id}"),
            "question": format!("Question {id}?"),
            "slug": format!("slug-{id}"),
            "active": true,
            "closed": false,
            "archived": false,
            "clobTokenIds": clob_token_ids,
            "enableOrderBook": true,
            "negRisk": false,
            "acceptingOrders": true,
            "clobRewards": []
        })
    }

    fn ok_response(body: serde_json::Value) -> HttpResponse {
        // Test JSON values are generated by serde_json, so converting them to a
        // response body is expected to succeed.
        let body = serde_json::to_string(&body).expect("test JSON serializes");
        HttpResponse { status: 200, body }
    }

    #[test]
    fn default_policy_uses_confirmed_runtime_values() {
        let policy = MarketRefreshPolicy::default();

        assert_eq!(
            policy.refresh_interval,
            Duration::from_secs(DEFAULT_MARKET_REFRESH_INTERVAL_SECS)
        );
        assert_eq!(
            policy.stale_ttl,
            Duration::from_secs(DEFAULT_STALE_MARKET_TTL_HOURS * SECONDS_PER_HOUR)
        );
    }

    #[test]
    fn filters_only_usable_orderbook_markets_and_normalizes_tokens() {
        let mut closed = market("closed", &["closed-token"]);
        closed.closed = true;
        let mut archived = market("archived", &["archived-token"]);
        archived.archived = true;
        let mut not_accepting = market("not-accepting", &["not-accepting-token"]);
        not_accepting.accepting_orders = false;
        let mut no_orderbook = market("no-orderbook", &["no-orderbook-token"]);
        no_orderbook.enable_order_book = false;
        let empty_tokens = market("empty", &["", "   "]);
        let usable = market("usable", &[" token-b ", "token-a", "token-a"]);

        let filtered = filter_usable_markets(vec![
            closed,
            archived,
            not_accepting,
            no_orderbook,
            empty_tokens,
            usable,
        ]);

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].id, "usable");
        assert_eq!(filtered[0].token_ids, vec!["token-a", "token-b"]);
    }

    #[test]
    fn initial_registry_fails_without_usable_tokens() {
        let now = Instant::now();
        let mut closed = market("closed", &["closed-token"]);
        closed.closed = true;

        let error =
            MarketRegistry::from_initial_markets(now, MarketRefreshPolicy::default(), vec![closed])
                .expect_err("closed market should not seed live startup");

        assert!(error.to_string().contains("no usable orderbook tokens"));
    }

    #[test]
    fn new_markets_are_added_and_tokens_are_deduplicated() -> Result<()> {
        let now = Instant::now();
        let mut registry = MarketRegistry::from_initial_markets(
            now,
            MarketRefreshPolicy::default(),
            vec![market("m1", &["token-b", "token-a", "token-a"])],
        )?;

        assert_eq!(registry.snapshot().token_ids, vec!["token-a", "token-b"]);
        assert_eq!(registry.snapshot().version, 1);

        let outcome = registry.apply_successful_refresh(
            now + Duration::from_secs(60),
            vec![
                market("m1", &["token-b", "token-a"]),
                market("m2", &["token-c"]),
            ],
        );

        assert!(outcome.changed);
        assert_eq!(outcome.added_markets, 1);
        assert_eq!(outcome.version, 2);
        assert_eq!(
            registry.snapshot().token_ids,
            vec!["token-a", "token-b", "token-c"]
        );
        Ok(())
    }

    #[test]
    fn missing_markets_are_retained_until_stale_ttl_then_removed() -> Result<()> {
        let now = Instant::now();
        let policy = MarketRefreshPolicy::default();
        let mut registry = MarketRegistry::from_initial_markets(
            now,
            policy,
            vec![market("m1", &["token-a"]), market("m2", &["token-b"])],
        )?;

        let first_missing = now + policy.refresh_interval;
        let stale_outcome =
            registry.apply_successful_refresh(first_missing, vec![market("m1", &["token-a"])]);

        assert!(!stale_outcome.changed);
        assert_eq!(stale_outcome.marked_stale, 1);
        assert_eq!(registry.snapshot().stale_markets, 1);
        assert_eq!(registry.snapshot().token_ids, vec!["token-a", "token-b"]);

        let almost_expired = first_missing + policy.stale_ttl - Duration::from_secs(1);
        let retained_outcome =
            registry.apply_successful_refresh(almost_expired, vec![market("m1", &["token-a"])]);

        assert!(!retained_outcome.changed);
        assert_eq!(registry.snapshot().token_ids, vec!["token-a", "token-b"]);

        let expired = first_missing + policy.stale_ttl;
        let removed_outcome =
            registry.apply_successful_refresh(expired, vec![market("m1", &["token-a"])]);

        assert!(removed_outcome.changed);
        assert_eq!(removed_outcome.removed_markets, 1);
        assert_eq!(registry.snapshot().stale_markets, 0);
        assert_eq!(registry.snapshot().token_ids, vec!["token-a"]);
        Ok(())
    }

    #[test]
    fn stale_market_reappearing_clears_stale_marker() -> Result<()> {
        let now = Instant::now();
        let policy = MarketRefreshPolicy::default();
        let mut registry = MarketRegistry::from_initial_markets(
            now,
            policy,
            vec![market("m1", &["token-a"]), market("m2", &["token-b"])],
        )?;

        registry.apply_successful_refresh(
            now + policy.refresh_interval,
            vec![market("m1", &["token-a"])],
        );
        assert_eq!(registry.snapshot().stale_markets, 1);

        let reappeared = registry.apply_successful_refresh(
            now + policy.refresh_interval + Duration::from_secs(60),
            vec![market("m1", &["token-a"]), market("m2", &["token-b"])],
        );

        assert!(!reappeared.changed);
        assert_eq!(registry.snapshot().active_markets, 2);
        assert_eq!(registry.snapshot().stale_markets, 0);
        Ok(())
    }

    #[test]
    fn empty_successful_refresh_is_fail_open_after_startup() -> Result<()> {
        let now = Instant::now();
        let mut registry = MarketRegistry::from_initial_markets(
            now,
            MarketRefreshPolicy::default(),
            vec![market("m1", &["token-a"])],
        )?;

        let outcome = registry.apply_successful_refresh(now + Duration::from_secs(60), Vec::new());

        assert!(!outcome.changed);
        assert_eq!(outcome.warning, Some(RefreshWarning::EmptyUsableRefresh));
        assert_eq!(outcome.token_count, 1);
        assert_eq!(registry.snapshot().token_ids, vec!["token-a"]);
        Ok(())
    }

    #[test]
    fn refresh_error_is_fail_open_after_startup() -> Result<()> {
        let now = Instant::now();
        let mut registry = MarketRegistry::from_initial_markets(
            now,
            MarketRefreshPolicy::default(),
            vec![market("m1", &["token-a"])],
        )?;

        let outcome = registry.apply_refresh_result(
            now + Duration::from_secs(60),
            Err(anyhow::anyhow!("Gamma timeout")),
        );

        assert!(!outcome.changed);
        assert!(matches!(
            outcome.warning,
            Some(RefreshWarning::FetchFailed(_))
        ));
        assert_eq!(registry.snapshot().token_ids, vec!["token-a"]);
        Ok(())
    }

    #[tokio::test]
    async fn fetch_usable_markets_with_client_reuses_gamma_active_open_query() -> Result<()> {
        let client = InMemoryHttpClient::new();
        let mut closed = gamma_market_json("closed", &["closed-token"]);
        closed["closed"] = serde_json::json!(true);
        let mut not_accepting = gamma_market_json("not-accepting", &["not-accepting-token"]);
        not_accepting["acceptingOrders"] = serde_json::json!(false);
        let usable = gamma_market_json("usable", &["token-a"]);

        client.set_response(
            &gamma_url(),
            Ok(ok_response(serde_json::json!([
                closed,
                not_accepting,
                usable
            ]))),
        );

        let markets = fetch_usable_markets_with_client(&client).await?;

        assert_eq!(client.request_count(&gamma_url()), 1);
        assert_eq!(markets.len(), 1);
        assert_eq!(markets[0].id, "usable");
        assert_eq!(markets[0].token_ids, vec!["token-a"]);
        Ok(())
    }
}
