use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result};
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::{Client, Config};
use chrono::Utc;
use tokio::io::AsyncWriteExt;
use tracing::{info, warn};

/// A single S3 object reference.
#[derive(Debug, Clone)]
pub struct S3Object {
    pub key: String,
    pub etag: Option<String>,
    pub size: i64,
}

/// Async trait abstracting S3 operations. Enables unit tests with an
/// in-memory backend and integration tests against LocalStack or real S3.
#[async_trait]
pub trait S3Service: Send + Sync {
    async fn list_objects(&self, bucket: &str, prefix: &str) -> Result<Vec<S3Object>>;
    async fn get_object(&self, bucket: &str, key: &str) -> Result<Vec<u8>>;
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<()>;
}

/// Production S3 implementation using the AWS SDK.
pub struct AwsS3Service {
    client: Client,
}

impl AwsS3Service {
    pub async fn new(region: impl Into<String>) -> Self {
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_config::Region::new(region.into()))
            .load()
            .await;
        let client = Client::new(&config);
        Self { client }
    }

    /// Build a client from an explicit endpoint + credentials pair.
    /// Useful for LocalStack integration tests.
    pub fn from_endpoint(
        region: impl Into<String>,
        endpoint: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        let creds = Credentials::new(access_key, secret_key, None, None, "test");
        let config = Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(aws_config::Region::new(region.into()))
            .endpoint_url(endpoint.into())
            .credentials_provider(creds)
            .force_path_style(true)
            .build();
        Self {
            client: Client::from_conf(config),
        }
    }
}

#[async_trait]
impl S3Service for AwsS3Service {
    async fn list_objects(&self, bucket: &str, prefix: &str) -> Result<Vec<S3Object>> {
        let mut objects = Vec::new();
        let mut continuation_token = None;
        loop {
            let mut req = self.client.list_objects_v2().bucket(bucket).prefix(prefix);
            if let Some(token) = &continuation_token {
                req = req.continuation_token(token);
            }
            let resp = req.send().await.context("list_objects_v2 failed")?;
            if let Some(contents) = resp.contents {
                for obj in contents {
                    objects.push(S3Object {
                        key: obj.key.unwrap_or_default(),
                        etag: obj.e_tag,
                        size: obj.size.unwrap_or(0),
                    });
                }
            }
            continuation_token = resp.next_continuation_token;
            if continuation_token.is_none() {
                break;
            }
        }
        Ok(objects)
    }

    async fn get_object(&self, bucket: &str, key: &str) -> Result<Vec<u8>> {
        let resp = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .with_context(|| format!("get_object failed for {}", key))?;
        let body = resp.body.collect().await?;
        Ok(body.into_bytes().to_vec())
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .with_context(|| format!("delete_object failed for {}", key))?;
        Ok(())
    }
}

/// In-memory S3 backend for fast unit tests.
#[derive(Default)]
pub struct InMemoryS3Service {
    pub objects: std::sync::Mutex<BTreeMap<String, Vec<u8>>>,
}

#[async_trait]
impl S3Service for InMemoryS3Service {
    async fn list_objects(&self, _bucket: &str, prefix: &str) -> Result<Vec<S3Object>> {
        let guard = self.objects.lock().unwrap();
        let mut list: Vec<S3Object> = guard
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| S3Object {
                key: k.clone(),
                etag: None,
                size: v.len() as i64,
            })
            .collect();
        list.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(list)
    }

    async fn get_object(&self, _bucket: &str, key: &str) -> Result<Vec<u8>> {
        let guard = self.objects.lock().unwrap();
        guard
            .get(key)
            .cloned()
            .with_context(|| format!("object not found: {}", key))
    }

    async fn delete_object(&self, _bucket: &str, key: &str) -> Result<()> {
        let mut guard = self.objects.lock().unwrap();
        guard.remove(key);
        Ok(())
    }
}

/// Options controlling the aggregation run.
#[derive(Debug, Clone)]
pub struct AggregateOptions {
    pub bucket: String,
    pub prefix: String,
    pub output_path: std::path::PathBuf,
    pub delete_after_merge: bool,
}

/// Download all JSONL objects under `prefix`, merge them into a single
/// newline-delimited file at `output_path`, and optionally delete source
/// objects once merged.
pub async fn aggregate_s3(
    service: &dyn S3Service,
    opts: &AggregateOptions,
) -> Result<AggregateSummary> {
    info!(bucket = %opts.bucket, prefix = %opts.prefix, output = %opts.output_path.display(), "Starting S3 aggregation");

    let objects = service
        .list_objects(&opts.bucket, &opts.prefix)
        .await
        .context("Failed to list S3 objects")?;

    let jsonl_objects: Vec<_> = objects
        .into_iter()
        .filter(|o| o.key.ends_with(".jsonl"))
        .collect();

    info!(count = jsonl_objects.len(), "Found JSONL objects");

    if jsonl_objects.is_empty() {
        return Ok(AggregateSummary {
            objects_processed: 0,
            lines_merged: 0,
            bytes_downloaded: 0,
        });
    }

    // Ensure output directory exists
    if let Some(parent) = opts.output_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let mut output = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&opts.output_path)
        .await?;

    let mut lines_merged: u64 = 0;
    let mut bytes_downloaded: u64 = 0;
    let mut objects_processed: u64 = 0;

    for obj in &jsonl_objects {
        let data = service
            .get_object(&opts.bucket, &obj.key)
            .await
            .with_context(|| format!("Failed to download {}", obj.key))?;

        bytes_downloaded += data.len() as u64;

        // Validate that data looks like JSONL and count lines
        let text = String::from_utf8_lossy(&data);
        let mut valid_lines = 0;
        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            if serde_json::from_str::<serde_json::Value>(line).is_ok() {
                valid_lines += 1;
            } else {
                warn!(key = %obj.key, "Skipping malformed JSON line");
            }
            output.write_all(line.as_bytes()).await?;
            output.write_all(b"\n").await?;
        }
        lines_merged += valid_lines;
        objects_processed += 1;

        if opts.delete_after_merge {
            service
                .delete_object(&opts.bucket, &obj.key)
                .await
                .with_context(|| format!("Failed to delete {}", obj.key))?;
            info!(key = %obj.key, "Deleted merged object");
        }
    }

    output.flush().await?;

    info!(
        objects_processed,
        lines_merged,
        bytes_downloaded,
        output = %opts.output_path.display(),
        "Aggregation complete"
    );

    Ok(AggregateSummary {
        objects_processed,
        lines_merged,
        bytes_downloaded,
    })
}

#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct AggregateSummary {
    pub objects_processed: u64,
    pub lines_merged: u64,
    pub bytes_downloaded: u64,
}

/// Write a small manifest file documenting the aggregation run.
pub async fn write_manifest(
    path: &Path,
    summary: AggregateSummary,
    opts: &AggregateOptions,
) -> Result<()> {
    #[derive(serde::Serialize)]
    struct Manifest {
        generated_at: String,
        bucket: String,
        prefix: String,
        output_path: String,
        delete_after_merge: bool,
        summary: AggregateSummary,
    }
    let manifest = Manifest {
        generated_at: Utc::now().to_rfc3339(),
        bucket: opts.bucket.clone(),
        prefix: opts.prefix.clone(),
        output_path: opts.output_path.to_string_lossy().to_string(),
        delete_after_merge: opts.delete_after_merge,
        summary,
    };
    let json = serde_json::to_string_pretty(&manifest)?;
    tokio::fs::write(path, json).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_aggregate_s3_merges_and_deletes() {
        let s3 = InMemoryS3Service::default();
        s3.objects.lock().unwrap().insert(
            "orderbook/2025-06-08/12/12_00_worker_0.jsonl".to_string(),
            b"{\"v\":1}\n{\"v\":2}\n".to_vec(),
        );
        s3.objects.lock().unwrap().insert(
            "orderbook/2025-06-08/12/12_05_worker_0.jsonl".to_string(),
            b"{\"v\":3}\n".to_vec(),
        );
        s3.objects.lock().unwrap().insert(
            "orderbook/2025-06-08/12/_manifest.json".to_string(),
            b"skip me".to_vec(),
        );

        let dir = tempdir().unwrap();
        let output = dir.path().join("merged.jsonl");
        let opts = AggregateOptions {
            bucket: "test-bucket".to_string(),
            prefix: "orderbook/".to_string(),
            output_path: output.clone(),
            delete_after_merge: true,
        };

        let summary = aggregate_s3(&s3, &opts).await.unwrap();
        assert_eq!(summary.objects_processed, 2);
        assert_eq!(summary.lines_merged, 3);

        let merged = std::fs::read_to_string(&output).unwrap();
        assert!(merged.contains("\"v\":1"));
        assert!(merged.contains("\"v\":2"));
        assert!(merged.contains("\"v\":3"));

        let remaining = s3.objects.lock().unwrap();
        assert!(!remaining.contains_key("orderbook/2025-06-08/12/12_00_worker_0.jsonl"));
        assert!(remaining.contains_key("orderbook/2025-06-08/12/_manifest.json"));
    }

    #[tokio::test]
    async fn test_aggregate_s3_no_delete_keeps_sources() {
        let s3 = InMemoryS3Service::default();
        s3.objects.lock().unwrap().insert(
            "orderbook/2025-06-08/12/12_00_worker_0.jsonl".to_string(),
            b"{\"v\":1}\n".to_vec(),
        );

        let dir = tempdir().unwrap();
        let opts = AggregateOptions {
            bucket: "test-bucket".to_string(),
            prefix: "orderbook/".to_string(),
            output_path: dir.path().join("merged.jsonl"),
            delete_after_merge: false,
        };

        aggregate_s3(&s3, &opts).await.unwrap();
        assert!(s3
            .objects
            .lock()
            .unwrap()
            .contains_key("orderbook/2025-06-08/12/12_00_worker_0.jsonl"));
    }
}
