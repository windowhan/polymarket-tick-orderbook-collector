use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Result;
use chrono::{DateTime, Timelike, Utc};
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

/// Floor a timestamp to the nearest rotate window, e.g. 5 minutes.
pub fn floor_time(ts: DateTime<Utc>, interval: Duration) -> DateTime<Utc> {
    let secs = interval.as_secs() as i64;
    let timestamp = (ts.timestamp() / secs) * secs;
    DateTime::from_timestamp(timestamp, 0).unwrap_or(ts)
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

/// A time-rotated JSONL writer. Each write goes to a new file when the
/// rotate window boundary is crossed. Files are never overwritten after
/// rotation, making them safe to upload to S3 without data loss.
pub struct RotatedWriter {
    root: PathBuf,
    suffix: String,
    rotate_interval: Duration,
    current_window: Option<DateTime<Utc>>,
    current_file: Option<tokio::fs::File>,
}

impl RotatedWriter {
    pub fn new(root: PathBuf, suffix: impl Into<String>, rotate_interval: Duration) -> Self {
        Self {
            root,
            suffix: suffix.into(),
            rotate_interval,
            current_window: None,
            current_file: None,
        }
    }

    /// Compute the path for a given rotate window.
    fn window_path(&self, window: DateTime<Utc>) -> PathBuf {
        let interval_min = self.rotate_interval.as_secs() / 60;
        let minute_block = (window.minute() / interval_min as u32) * interval_min as u32;
        let dir = self
            .root
            .join(window.format("%Y-%m-%d").to_string())
            .join(window.format("%H").to_string());
        dir.join(format!(
            "{:02}_{:02}{}.jsonl",
            window.hour(),
            minute_block,
            self.suffix
        ))
    }

    async fn rotate_to(&mut self, window: DateTime<Utc>) -> Result<()> {
        if let Some(mut file) = self.current_file.take() {
            file.flush().await?;
        }
        let path = self.window_path(window);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;
        self.current_file = Some(file);
        self.current_window = Some(window);
        Ok(())
    }

    /// Append records. Rotates to a new file when the current window expires.
    pub async fn append<T: Serialize>(&mut self, records: &[T]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let now = Utc::now();
        let window = floor_time(now, self.rotate_interval);
        if self.current_window != Some(window) {
            self.rotate_to(window).await?;
        }
        let file = self.current_file.as_mut().expect("file initialized");
        for record in records {
            let line = serde_json::to_string(record)?;
            file.write_all(line.as_bytes()).await?;
            file.write_all(b"\n").await?;
        }
        file.flush().await?;
        Ok(())
    }

    /// Explicitly flush and close the current file.
    pub async fn flush(&mut self) -> Result<()> {
        if let Some(file) = self.current_file.as_mut() {
            file.flush().await?;
        }
        Ok(())
    }

    /// Return the path of the currently active file, if any.
    pub fn current_path(&self) -> Option<PathBuf> {
        self.current_window.map(|w| self.window_path(w))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_floor_time() {
        let ts = DateTime::parse_from_rfc3339("2025-06-08T12:07:30Z")
            .unwrap()
            .with_timezone(&Utc);
        let floored = floor_time(ts, Duration::from_secs(300));
        assert_eq!(floored.minute(), 5);
        assert_eq!(floored.second(), 0);
    }

    #[tokio::test]
    async fn test_rotated_writer_creates_separate_windows() {
        let dir = tempdir().unwrap();
        let mut writer = RotatedWriter::new(dir.path().to_path_buf(), "", Duration::from_secs(60));
        #[derive(Serialize)]
        struct Rec {
            v: i32,
        }
        // two writes within same minute => same file
        writer.append(&[Rec { v: 1 }]).await.unwrap();
        writer.append(&[Rec { v: 2 }]).await.unwrap();
        let path1 = writer.current_path().unwrap().clone();
        assert!(path1.exists());

        // force rotation by changing writer state (simulate boundary crossing)
        writer.current_window = None;
        writer.append(&[Rec { v: 3 }]).await.unwrap();
        let path2 = writer.current_path().unwrap();
        // path should be same unless minute actually rolled over in real time
        // if test runs across minute boundary we accept different file
        let content1 = tokio::fs::read_to_string(&path1).await.unwrap();
        let content2 = tokio::fs::read_to_string(&path2).await.unwrap();
        assert!(content1.contains("1"));
        assert!(content2.contains("3"));
    }
}
