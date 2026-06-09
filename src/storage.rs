use std::path::{Path, PathBuf};

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::io::AsyncWriteExt;

/// Return the project root directory (works when run from cargo target dirs too).
pub fn project_root() -> PathBuf {
    let mut path = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    if path.ends_with("target/debug") || path.ends_with("target/release") {
        path.pop();
        path.pop();
    }
    path
}

/// `data/` directory under the project root.
pub fn data_dir() -> PathBuf {
    project_root().join("data")
}

/// Build a partitioned path like `{root}/2024-06-08/15_worker_3.jsonl`.
pub fn partitioned_path(root: &Path, ts: DateTime<Utc>, suffix: &str) -> PathBuf {
    let dir = root.join(ts.format("%Y-%m-%d").to_string());
    std::fs::create_dir_all(&dir).ok();
    dir.join(format!("{}{}", ts.format("%H"), suffix))
}

/// Append JSON Lines to a file asynchronously (creates if missing).
pub async fn append_jsonl<T: Serialize>(path: &Path, records: &[T]) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;
    for record in records {
        let line = serde_json::to_string(record)?;
        file.write_all(line.as_bytes()).await?;
        file.write_all(b"\n").await?;
    }
    file.flush().await?;
    Ok(())
}

/// Write JSON Lines to a file asynchronously (truncates existing).
pub async fn write_jsonl<T: Serialize>(path: &Path, records: &[T]) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .await?;
    for record in records {
        let line = serde_json::to_string(record)?;
        file.write_all(line.as_bytes()).await?;
        file.write_all(b"\n").await?;
    }
    file.flush().await?;
    Ok(())
}
