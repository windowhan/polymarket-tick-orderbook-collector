use anyhow::Result;
use axum::{
    extract::State,
    http::StatusCode,
    routing::post,
    Router,
};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{interval, Duration};
use tracing::{info, warn};

#[derive(Clone)]
struct AppState {
    output_dir: PathBuf,
    buffer: Arc<Mutex<Vec<String>>>,
}

async fn ingest(State(state): State<AppState>, body: String) -> StatusCode {
    let mut buf = state.buffer.lock().await;
    buf.push(body);
    // Flush every 100 messages or let the background task handle it
    if buf.len() >= 100 {
        if let Err(e) = flush(&state.output_dir, &buf).await {
            warn!(error = %e, "Failed to flush buffer");
        }
        buf.clear();
    }
    StatusCode::OK
}

async fn flush(output_dir: &PathBuf, lines: &[String]) -> Result<()> {
    if lines.is_empty() {
        return Ok(());
    }
    if let Some(parent) = output_dir.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    use tokio::io::AsyncWriteExt;
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(output_dir)
        .await?;
    for line in lines {
        file.write_all(line.as_bytes()).await?;
        file.write_all(b"\n").await?;
    }
    file.flush().await?;
    Ok(())
}

async fn flush_task(state: AppState) {
    let mut tick = interval(Duration::from_secs(5));
    loop {
        tick.tick().await;
        let mut buf = state.buffer.lock().await;
        if !buf.is_empty() {
            let count = buf.len();
            if let Err(e) = flush(&state.output_dir, &buf).await {
                warn!(error = %e, "Background flush failed");
            } else {
                info!(count, "Flushed buffered records");
            }
            buf.clear();
        }
    }
}

pub async fn run(bind: &str, output_path: PathBuf) -> Result<()> {
    info!(bind = %bind, path = %output_path.display(), "Starting aggregator");

    let state = AppState {
        output_dir: output_path,
        buffer: Arc::new(Mutex::new(Vec::with_capacity(128))),
    };

    let bg_state = state.clone();
    tokio::spawn(async move {
        flush_task(bg_state).await;
    });

    let app = Router::new()
        .route("/ingest", post(ingest))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind).await?;
    info!(bind = %bind, "Aggregator listening");
    axum::serve(listener, app).await?;
    Ok(())
}
