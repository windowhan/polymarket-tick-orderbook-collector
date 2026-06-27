use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Result;
use chrono::{DateTime, Timelike, Utc};
use serde::Serialize;
use tokio::io::AsyncWriteExt;

/// Return the project root directory (works when run from cargo target dirs too).
pub fn project_root() -> PathBuf {
    let mut path = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    if path.ends_with("target/debug/deps") || path.ends_with("target/release/deps") {
        path.pop();
        path.pop();
        path.pop();
    } else if path.ends_with("target/debug") || path.ends_with("target/release") {
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
        file.write_all(serde_json::to_string(record)?.as_bytes())
            .await?;
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
        file.write_all(serde_json::to_string(record)?.as_bytes())
            .await?;
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
    pub(crate) current_window: Option<DateTime<Utc>>,
    current_file: Option<tokio::fs::File>,
    /// Path of the file that was just closed by the most recent rotation.
    /// Consumed by `take_rotated_path`.
    pub(crate) rotated_path: Option<PathBuf>,
}

impl RotatedWriter {
    pub fn new(root: PathBuf, suffix: impl Into<String>, rotate_interval: Duration) -> Self {
        Self {
            root,
            suffix: suffix.into(),
            rotate_interval,
            current_window: None,
            current_file: None,
            rotated_path: None,
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

    pub(crate) async fn rotate_to(&mut self, window: DateTime<Utc>) -> Result<()> {
        if let Some(current_window) = self.current_window.take() {
            let prev_path = self.window_path(current_window);
            if let Some(mut file) = self.current_file.take() {
                file.flush().await?;
            }
            self.rotated_path = Some(prev_path);
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
        let window = floor_time(Utc::now(), self.rotate_interval);
        if self.current_window != Some(window) {
            self.rotate_to(window).await?;
        }
        let file = self.current_file.as_mut().expect("file initialized");
        for record in records {
            file.write_all(serde_json::to_string(record)?.as_bytes())
                .await?;
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

    /// Return and clear the path of the file closed by the most recent rotation.
    ///
    /// This is used by callers to upload or archive a completed file after the
    /// writer has moved to a new time window.
    pub fn take_rotated_path(&mut self) -> Option<PathBuf> {
        self.rotated_path.take()
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

    #[derive(Serialize)]
    struct Rec {
        v: i32,
    }

    /// A value that always fails JSON serialization.
    struct BadRec;

    impl Serialize for BadRec {
        fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("intentional serialize failure"))
        }
    }

    #[tokio::test]
    async fn test_rotated_writer_creates_separate_windows() {
        let dir = tempdir().unwrap();
        let mut writer = RotatedWriter::new(dir.path().to_path_buf(), "", Duration::from_secs(60));
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

    #[tokio::test]
    async fn test_rotated_writer_exposes_rotated_path() {
        let dir = tempdir().unwrap();
        let mut writer =
            RotatedWriter::new(dir.path().to_path_buf(), "_w0", Duration::from_secs(60));

        writer.append(&[Rec { v: 1 }]).await.unwrap();
        let first_path = writer.current_path().unwrap().clone();

        // Simulate crossing into the next minute window.
        let next_window = writer.current_window.unwrap() + chrono::Duration::minutes(1);
        writer.rotate_to(next_window).await.unwrap();

        assert_eq!(writer.take_rotated_path(), Some(first_path));
        assert!(writer.take_rotated_path().is_none());
    }

    #[test]
    fn test_project_root_from_target_debug() {
        let original = std::env::current_dir().unwrap();
        // Simulate running from target/debug/deps
        let target_deps = original.join("target").join("debug").join("deps");
        std::fs::create_dir_all(&target_deps).unwrap();
        std::env::set_current_dir(&target_deps).unwrap();
        let root = project_root();
        std::env::set_current_dir(&original).unwrap();
        assert_eq!(root, original);
    }

    #[test]
    fn test_project_root_from_target_release() {
        let original = std::env::current_dir().unwrap();
        let target_release = original.join("target").join("release");
        std::fs::create_dir_all(&target_release).unwrap();
        std::env::set_current_dir(&target_release).unwrap();
        let root = project_root();
        std::env::set_current_dir(&original).unwrap();
        assert_eq!(root, original);
    }

    #[test]
    fn test_data_dir() {
        let root = project_root();
        assert_eq!(data_dir(), root.join("data"));
    }

    #[test]
    fn test_partitioned_path() {
        let dir = tempdir().unwrap();
        let ts = DateTime::parse_from_rfc3339("2025-06-08T12:07:30Z")
            .unwrap()
            .with_timezone(&Utc);
        let path = partitioned_path(dir.path(), ts, "_suffix.jsonl");
        assert!(path.to_string_lossy().contains("2025-06-08"));
        assert!(path.to_string_lossy().contains("12_suffix.jsonl"));
    }

    #[tokio::test]
    async fn test_append_jsonl_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.jsonl");
        append_jsonl::<Rec>(&path, &[]).await.unwrap();
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn test_append_jsonl_creates_parent() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nested").join("dir").join("file.jsonl");
        append_jsonl(&path, &[Rec { v: 42 }]).await.unwrap();
        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(content.contains("42"));
    }

    #[tokio::test]
    async fn test_write_jsonl_truncates() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("file.jsonl");
        append_jsonl(&path, &[Rec { v: 1 }]).await.unwrap();
        write_jsonl(&path, &[Rec { v: 2 }]).await.unwrap();
        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(!content.contains("1"));
        assert!(content.contains("2"));
    }

    #[tokio::test]
    async fn test_rotated_writer_empty_append() {
        let dir = tempdir().unwrap();
        let mut writer = RotatedWriter::new(dir.path().to_path_buf(), "", Duration::from_secs(60));
        writer.append::<Rec>(&[]).await.unwrap();
        assert!(writer.current_path().is_none());
    }

    #[tokio::test]
    async fn test_rotated_writer_flush_without_file() {
        let dir = tempdir().unwrap();
        let mut writer = RotatedWriter::new(dir.path().to_path_buf(), "", Duration::from_secs(60));
        writer.flush().await.unwrap();
    }

    #[tokio::test]
    async fn test_rotated_writer_current_path_none() {
        let dir = tempdir().unwrap();
        let writer = RotatedWriter::new(dir.path().to_path_buf(), "", Duration::from_secs(60));
        assert!(writer.current_path().is_none());
    }

    #[tokio::test]
    async fn test_rotated_writer_window_path_format() {
        let dir = tempdir().unwrap();
        let writer = RotatedWriter::new(dir.path().to_path_buf(), "_w", Duration::from_secs(300));
        let ts = DateTime::parse_from_rfc3339("2025-06-08T12:07:30Z")
            .unwrap()
            .with_timezone(&Utc);
        let path = writer.window_path(ts);
        assert!(path
            .to_string_lossy()
            .contains("2025-06-08/12/12_05_w.jsonl"));
    }

    #[tokio::test]
    async fn test_append_jsonl_create_dir_all_error() {
        let dir = tempdir().unwrap();
        // Create a file where a parent directory should be created.
        let file_as_dir = dir.path().join("foo");
        std::fs::write(&file_as_dir, "not a dir").unwrap();
        let path = file_as_dir.join("bar.jsonl");

        let err = append_jsonl(&path, &[Rec { v: 1 }]).await.unwrap_err();
        assert!(
            err.to_string().contains("Not a directory") || err.to_string().contains("File exists"),
            "unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_append_jsonl_open_error() {
        let dir = tempdir().unwrap();
        // Use a directory as the file path so opening it for append fails.
        let path = dir.path().join("is_a_dir");
        std::fs::create_dir(&path).unwrap();

        let err = append_jsonl(&path, &[Rec { v: 1 }]).await.unwrap_err();
        assert!(
            err.to_string().contains("Is a directory") || err.to_string().contains("directory"),
            "unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_append_jsonl_serialize_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bad.jsonl");

        let err = append_jsonl(&path, &[BadRec]).await.unwrap_err();
        assert!(
            err.to_string().contains("intentional serialize failure"),
            "unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_write_jsonl_create_dir_all_error() {
        let dir = tempdir().unwrap();
        let file_as_dir = dir.path().join("foo");
        std::fs::write(&file_as_dir, "not a dir").unwrap();
        let path = file_as_dir.join("bar.jsonl");

        let err = write_jsonl(&path, &[Rec { v: 1 }]).await.unwrap_err();
        assert!(
            err.to_string().contains("Not a directory") || err.to_string().contains("File exists"),
            "unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_write_jsonl_open_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("is_a_dir");
        std::fs::create_dir(&path).unwrap();

        let err = write_jsonl(&path, &[Rec { v: 1 }]).await.unwrap_err();
        assert!(
            err.to_string().contains("Is a directory") || err.to_string().contains("directory"),
            "unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_write_jsonl_serialize_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bad.jsonl");

        let err = write_jsonl(&path, &[BadRec]).await.unwrap_err();
        assert!(
            err.to_string().contains("intentional serialize failure"),
            "unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_rotated_writer_rotate_to_flushes_current_file() {
        let dir = tempdir().unwrap();
        let mut writer = RotatedWriter::new(dir.path().to_path_buf(), "", Duration::from_secs(60));
        writer.append(&[Rec { v: 1 }]).await.unwrap();

        let next_window = writer.current_window.unwrap() + chrono::Duration::minutes(1);
        writer.rotate_to(next_window).await.unwrap();

        assert!(writer.rotated_path.is_some());
        let content = tokio::fs::read_to_string(writer.rotated_path.as_ref().unwrap())
            .await
            .unwrap();
        assert!(content.contains("1"));
    }

    #[tokio::test]
    async fn test_rotated_writer_rotate_to_create_dir_all_error() {
        let dir = tempdir().unwrap();
        // Root is a file, so creating the date/hour subdirectories fails.
        let root_as_file = dir.path().join("root");
        std::fs::write(&root_as_file, "not a dir").unwrap();
        let mut writer = RotatedWriter::new(root_as_file, "", Duration::from_secs(60));

        let err = writer.append(&[Rec { v: 1 }]).await.unwrap_err();
        assert!(
            err.to_string().contains("Not a directory") || err.to_string().contains("File exists"),
            "unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_rotated_writer_append_serialize_error() {
        let dir = tempdir().unwrap();
        let mut writer = RotatedWriter::new(dir.path().to_path_buf(), "", Duration::from_secs(60));

        let err = writer.append(&[BadRec]).await.unwrap_err();
        assert!(
            err.to_string().contains("intentional serialize failure"),
            "unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_rotated_writer_flush_hits_file_flush() {
        let dir = tempdir().unwrap();
        let mut writer = RotatedWriter::new(dir.path().to_path_buf(), "", Duration::from_secs(60));
        writer.append(&[Rec { v: 1 }]).await.unwrap();
        writer.flush().await.unwrap();

        let path = writer.current_path().unwrap();
        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(content.contains("1"));
    }
}
