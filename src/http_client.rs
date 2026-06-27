use anyhow::{Context, Result};
use async_trait::async_trait;
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::Duration;

/// A generic HTTP response for testing purposes.
#[derive(Debug, Clone, PartialEq)]
pub struct HttpResponse {
    pub status: u16,
    pub body: String,
}

impl HttpResponse {
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }

    pub fn json<T: serde::de::DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_str(&self.body).context("Failed to parse JSON response")
    }
}

/// Abstract HTTP client to enable mocking in tests.
#[async_trait]
pub trait HttpClient: Send + Sync {
    async fn get(&self, url: &str) -> Result<HttpResponse>;
    async fn post(&self, url: &str, body: String) -> Result<HttpResponse>;
}

/// Production HTTP client backed by reqwest.
pub struct ReqwestHttpClient {
    client: reqwest::Client,
}

impl ReqwestHttpClient {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    pub fn with_timeout(timeout: Duration) -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(timeout)
                .build()
                .context("Failed to build reqwest client")?,
        })
    }
}

impl Default for ReqwestHttpClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl HttpClient for ReqwestHttpClient {
    async fn get(&self, url: &str) -> Result<HttpResponse> {
        let resp = self
            .client
            .get(url)
            .header("User-Agent", "polymarket-collector/0.1")
            .send()
            .await
            .with_context(|| format!("GET request failed: {}", url))?;
        let status = resp.status().as_u16();
        let body = resp.text().await?;
        Ok(HttpResponse { status, body })
    }

    async fn post(&self, url: &str, body: String) -> Result<HttpResponse> {
        let resp = self
            .client
            .post(url)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await
            .with_context(|| format!("POST request failed: {}", url))?;
        let status = resp.status().as_u16();
        let body = resp.text().await?;
        Ok(HttpResponse { status, body })
    }
}

/// Compatibility implementation so existing callers that pass a raw
/// `reqwest::Client` (e.g. `main.rs` and `viewer.rs`) continue to compile
/// while module internals are refactored against the `HttpClient` trait.
#[async_trait]
impl HttpClient for reqwest::Client {
    async fn get(&self, url: &str) -> Result<HttpResponse> {
        let resp = self
            .get(url)
            .send()
            .await
            .with_context(|| format!("GET request failed: {}", url))?;
        let status = resp.status().as_u16();
        let body = resp.text().await?;
        Ok(HttpResponse { status, body })
    }

    async fn post(&self, url: &str, body: String) -> Result<HttpResponse> {
        let resp = self
            .post(url)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await
            .with_context(|| format!("POST request failed: {}", url))?;
        let status = resp.status().as_u16();
        let body = resp.text().await?;
        Ok(HttpResponse { status, body })
    }
}

/// In-memory HTTP client for tests.
#[derive(Default)]
pub struct InMemoryHttpClient {
    responses: Mutex<HashMap<String, Result<HttpResponse, String>>>,
    sequences: Mutex<HashMap<String, VecDeque<Result<HttpResponse, String>>>>,
    counts: Mutex<HashMap<String, usize>>,
}

impl Clone for InMemoryHttpClient {
    fn clone(&self) -> Self {
        Self {
            responses: Mutex::new(self.responses.lock().unwrap().clone()),
            sequences: Mutex::new(self.sequences.lock().unwrap().clone()),
            counts: Mutex::new(self.counts.lock().unwrap().clone()),
        }
    }
}

impl InMemoryHttpClient {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_response(&self, url: &str, response: Result<HttpResponse, String>) {
        self.responses
            .lock()
            .unwrap()
            .insert(url.to_string(), response);
    }

    pub fn set_response_sequence(&self, url: &str, responses: Vec<Result<HttpResponse, String>>) {
        self.sequences
            .lock()
            .unwrap()
            .insert(url.to_string(), responses.into_iter().collect());
    }

    pub fn request_count(&self, url: &str) -> usize {
        self.counts.lock().unwrap().get(url).copied().unwrap_or(0)
    }

    pub fn total_request_count(&self) -> usize {
        self.counts.lock().unwrap().values().sum()
    }

    fn record_request(&self, url: &str) {
        let mut counts = self.counts.lock().unwrap();
        *counts.entry(url.to_string()).or_insert(0) += 1;
    }

    fn lookup(&self, url: &str) -> Result<HttpResponse> {
        let mut sequences = self.sequences.lock().unwrap();
        if let Some(queue) = sequences.get_mut(url) {
            if let Some(response) = queue.pop_front() {
                return response.map_err(|e| anyhow::anyhow!(e));
            }
        }
        drop(sequences);

        self.responses
            .lock()
            .unwrap()
            .get(url)
            .cloned()
            .unwrap_or_else(|| Err(format!("No mock response for {}", url)))
            .map_err(|e| anyhow::anyhow!(e))
    }
}

#[async_trait]
impl HttpClient for InMemoryHttpClient {
    async fn get(&self, url: &str) -> Result<HttpResponse> {
        self.record_request(url);
        self.lookup(url)
    }

    async fn post(&self, url: &str, _body: String) -> Result<HttpResponse> {
        self.record_request(url);
        self.lookup(url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_response_is_success() {
        assert!(HttpResponse {
            status: 200,
            body: "".to_string()
        }
        .is_success());
        assert!(HttpResponse {
            status: 299,
            body: "".to_string()
        }
        .is_success());
        assert!(!HttpResponse {
            status: 199,
            body: "".to_string()
        }
        .is_success());
        assert!(!HttpResponse {
            status: 300,
            body: "".to_string()
        }
        .is_success());
        assert!(!HttpResponse {
            status: 500,
            body: "".to_string()
        }
        .is_success());
    }

    #[test]
    fn test_http_response_json() {
        let resp = HttpResponse {
            status: 200,
            body: r#"{"v":1}"#.to_string(),
        };
        let value: serde_json::Value = resp.json().unwrap();
        assert_eq!(value["v"], 1);

        let bad = HttpResponse {
            status: 200,
            body: "not json".to_string(),
        };
        assert!(bad.json::<serde_json::Value>().is_err());
    }

    #[test]
    fn test_in_memory_http_client_counts() {
        let client = InMemoryHttpClient::new();
        client.set_response(
            "http://example.com",
            Ok(HttpResponse {
                status: 200,
                body: "ok".to_string(),
            }),
        );

        assert_eq!(client.total_request_count(), 0);
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let resp = client.get("http://example.com").await.unwrap();
            assert_eq!(resp.status, 200);
        });
        assert_eq!(client.request_count("http://example.com"), 1);
        assert_eq!(client.total_request_count(), 1);
    }

    #[test]
    fn test_in_memory_http_client_sequence() {
        let client = InMemoryHttpClient::new();
        client.set_response_sequence(
            "http://example.com",
            vec![
                Ok(HttpResponse {
                    status: 200,
                    body: "first".to_string(),
                }),
                Ok(HttpResponse {
                    status: 201,
                    body: "second".to_string(),
                }),
            ],
        );

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let r1 = client.get("http://example.com").await.unwrap();
            assert_eq!(r1.status, 200);
            let r2 = client.get("http://example.com").await.unwrap();
            assert_eq!(r2.status, 201);
        });
    }

    #[test]
    fn test_in_memory_http_client_missing_response() {
        let client = InMemoryHttpClient::new();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            assert!(client.get("http://missing").await.is_err());
        });
    }

    #[tokio::test]
    async fn test_reqwest_http_client_get() {
        use axum::{routing::get, Router};

        let app = Router::new().route("/", get(|| async { "hello\n" }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let client = ReqwestHttpClient::new();
        let resp = client
            .get(&format!("http://127.0.0.1:{}/", port))
            .await
            .unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, "hello\n");
    }

    #[tokio::test]
    async fn test_reqwest_http_client_post() {
        use axum::{routing::post, Router};

        let app = Router::new().route("/", post(|_body: String| async { "ok" }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let client = ReqwestHttpClient::new();
        let resp = client
            .post(&format!("http://127.0.0.1:{}/", port), "{}".to_string())
            .await
            .unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, "ok");
    }

    #[test]
    fn test_reqwest_http_client_with_timeout() {
        let client = ReqwestHttpClient::with_timeout(Duration::from_secs(1)).unwrap();
        let _ = client.client;
    }

    #[test]
    fn test_reqwest_http_client_default() {
        let _client = ReqwestHttpClient::default();
    }

    #[tokio::test]
    async fn test_reqwest_http_client_get_error() {
        let client = ReqwestHttpClient::new();
        let result = client.get("http://127.0.0.1:1/no-such-port").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_raw_reqwest_client_as_http_client() {
        use axum::{
            routing::{get, post},
            Router,
        };

        let app = Router::new()
            .route("/", get(|| async { "raw-get" }))
            .route("/", post(|_body: String| async { "raw-post" }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let raw = reqwest::Client::new();
        let resp = HttpClient::get(&raw, &format!("http://127.0.0.1:{}/", port))
            .await
            .unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, "raw-get");

        let resp = HttpClient::post(
            &raw,
            &format!("http://127.0.0.1:{}/", port),
            "{}".to_string(),
        )
        .await
        .unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, "raw-post");
    }

    #[tokio::test]
    async fn test_raw_reqwest_client_get_error_context() {
        let raw = reqwest::Client::new();
        let err = HttpClient::get(&raw, "http://127.0.0.1:1/no-such-port")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("GET request failed"));
    }

    #[tokio::test]
    async fn test_raw_reqwest_client_post_error_context() {
        let raw = reqwest::Client::new();
        let err = HttpClient::post(&raw, "http://127.0.0.1:1/no-such-port", "{}".to_string())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("POST request failed"));
    }

    #[test]
    fn test_in_memory_http_client_default_and_clone() {
        let client1 = InMemoryHttpClient::default();
        client1.set_response(
            "http://x",
            Ok(HttpResponse {
                status: 200,
                body: "x".to_string(),
            }),
        );
        let client2 = client1.clone();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let resp = client2.get("http://x").await.unwrap();
            assert_eq!(resp.status, 200);
        });
    }
}
