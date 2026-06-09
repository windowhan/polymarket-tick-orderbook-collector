use anyhow::Result;
use axum::{
    extract::{Path as AxumPath, State},
    response::Html,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

#[derive(Debug, Clone, Serialize)]
struct Level {
    price: f64,
    size: f64,
}

#[derive(Debug, Clone, Serialize, Default)]
struct BookSnapshot {
    bids: Vec<Level>,
    asks: Vec<Level>,
}

#[derive(Debug, Clone, Serialize)]
struct PricePoint {
    timestamp: i64,
    price: f64,
    side: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct TradePoint {
    timestamp: i64,
    price: f64,
    size: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Default)]
struct MarketView {
    asset: String,
    latest_book: BookSnapshot,
    price_history: Vec<PricePoint>,
    trades: Vec<TradePoint>,
}

#[derive(Default)]
struct ViewerData {
    markets: HashMap<String, MarketView>,
}

#[derive(Clone)]
struct AppState {
    data: Arc<RwLock<ViewerData>>,
}

#[derive(Debug, Deserialize)]
struct RawEvent {
    event_type: String,
    asset: String,
    side: Option<String>,
    price: Option<f64>,
    size: Option<f64>,
    timestamp: i64,
}

async fn load_data(path: &Path) -> Result<ViewerData> {
    info!(path = %path.display(), "Loading viewer data");
    let content = tokio::fs::read_to_string(path).await?;
    let mut data = ViewerData::default();

    for line in content.lines() {
        let ev: RawEvent = match serde_json::from_str(line) {
            Ok(e) => e,
            Err(e) => {
                warn!(error = %e, line, "Skipping malformed line");
                continue;
            }
        };

        if ev.asset.is_empty() {
            continue;
        }

        let entry = data
            .markets
            .entry(ev.asset.clone())
            .or_insert_with(|| MarketView {
                asset: ev.asset.clone(),
                ..Default::default()
            });

        match ev.event_type.as_str() {
            "book" => {
                // Book events are stored as raw JSON in the 'raw' field,
                // but we simplified to just track latest price.
                if let Some(price) = ev.price {
                    entry.price_history.push(PricePoint {
                        timestamp: ev.timestamp,
                        price,
                        side: ev.side.clone(),
                    });
                }
            }
            "price_change" => {
                if let Some(price) = ev.price {
                    entry.price_history.push(PricePoint {
                        timestamp: ev.timestamp,
                        price,
                        side: ev.side.clone(),
                    });
                }
            }
            "last_trade" => {
                entry.trades.push(TradePoint {
                    timestamp: ev.timestamp,
                    price: ev.price.unwrap_or(0.0),
                    size: ev.size,
                });
            }
            _ => {}
        }
    }

    for mv in data.markets.values_mut() {
        mv.price_history.sort_by_key(|p| p.timestamp);
        mv.trades.sort_by_key(|t| t.timestamp);
    }

    info!(markets = data.markets.len(), "Viewer data loaded");
    Ok(data)
}

async fn index_handler() -> Html<&'static str> {
    Html(include_str!("viewer.html"))
}

async fn markets_handler(State(state): State<AppState>) -> Json<Vec<String>> {
    let data = state.data.read().await;
    let mut markets: Vec<String> = data.markets.keys().cloned().collect();
    markets.sort();
    Json(markets)
}

async fn orderbook_handler(
    State(state): State<AppState>,
    AxumPath(asset): AxumPath<String>,
) -> Json<Option<BookSnapshot>> {
    let data = state.data.read().await;
    Json(data.markets.get(&asset).map(|m| m.latest_book.clone()))
}

async fn trades_handler(
    State(state): State<AppState>,
    AxumPath(asset): AxumPath<String>,
) -> Json<Vec<TradePoint>> {
    let data = state.data.read().await;
    Json(
        data.markets
            .get(&asset)
            .map(|m| m.trades.clone())
            .unwrap_or_default(),
    )
}

async fn price_history_handler(
    State(state): State<AppState>,
    AxumPath(asset): AxumPath<String>,
) -> Json<Vec<PricePoint>> {
    let data = state.data.read().await;
    Json(
        data.markets
            .get(&asset)
            .map(|m| m.price_history.clone())
            .unwrap_or_default(),
    )
}

pub async fn run(input_path: &Path, bind: &str) -> Result<()> {
    let data = load_data(input_path).await?;
    let state = AppState {
        data: Arc::new(RwLock::new(data)),
    };

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/api/markets", get(markets_handler))
        .route("/api/orderbook/:asset", get(orderbook_handler))
        .route("/api/trades/:asset", get(trades_handler))
        .route("/api/price_history/:asset", get(price_history_handler))
        .with_state(state);

    info!(bind = %bind, "Viewer listening");
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
