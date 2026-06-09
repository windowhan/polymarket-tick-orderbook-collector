use aws_sdk_s3::primitives::ByteStream;
use polymarket_collector::aggregate_s3::{
    aggregate_s3, AggregateOptions, AwsS3Service,
};
use tempfile::tempdir;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::localstack::LocalStack;

#[tokio::test]
async fn test_aggregate_s3_with_localstack() {
    let container = LocalStack::default().start().await.unwrap();
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(4566).await.unwrap();
    let endpoint = format!("http://{}:{}", host, port);

    // Build a one-off S3 client to seed the bucket
    let seed_config = aws_sdk_s3::Config::builder()
        .behavior_version(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-east-1"))
        .endpoint_url(&endpoint)
        .force_path_style(true)
        .credentials_provider(aws_credential_types::Credentials::new(
            "test", "test", None, None, "test",
        ))
        .build();
    let seed_client = aws_sdk_s3::Client::from_conf(seed_config);

    seed_client
        .create_bucket()
        .bucket("test-bucket")
        .send()
        .await
        .unwrap();

    let objects = vec![
        (
            "orderbook/2025-06-08/12/12_00_worker_0.jsonl",
            b"{\"asset\":\"A\",\"price\":1.0}\n{\"asset\":\"A\",\"price\":1.1}\n".to_vec(),
        ),
        (
            "orderbook/2025-06-08/12/12_05_worker_0.jsonl",
            b"{\"asset\":\"B\",\"price\":2.0}\n".to_vec(),
        ),
        (
            "orderbook/2025-06-08/12/12_05_worker_1.jsonl",
            b"{\"asset\":\"C\",\"price\":3.0}\n{\"asset\":\"C\",\"price\":3.1}\n{\"asset\":\"C\",\"price\":3.2}\n"
                .to_vec(),
        ),
    ];

    for (key, body) in objects {
        seed_client
            .put_object()
            .bucket("test-bucket")
            .key(key)
            .body(ByteStream::from(body))
            .send()
            .await
            .unwrap();
    }

    let service = AwsS3Service::from_endpoint("us-east-1", endpoint, "test", "test");

    let dir = tempdir().unwrap();
    let output = dir.path().join("merged.jsonl");
    let opts = AggregateOptions {
        bucket: "test-bucket".to_string(),
        prefix: "orderbook/".to_string(),
        output_path: output.clone(),
        delete_after_merge: true,
    };

    let summary = aggregate_s3(&service, &opts).await.unwrap();

    assert_eq!(summary.objects_processed, 3);
    assert_eq!(summary.lines_merged, 6);
    assert!(output.exists());

    let merged = tokio::fs::read_to_string(&output).await.unwrap();
    assert!(merged.contains("\"asset\":\"A\""));
    assert!(merged.contains("\"asset\":\"B\""));
    assert!(merged.contains("\"asset\":\"C\""));

    // Ensure source objects were deleted
    let remaining = seed_client
        .list_objects_v2()
        .bucket("test-bucket")
        .prefix("orderbook/")
        .send()
        .await
        .unwrap();
    assert!(remaining.contents.is_none() || remaining.contents.unwrap().is_empty());
}

#[tokio::test]
async fn test_aggregate_s3_empty_prefix() {
    let container = LocalStack::default().start().await.unwrap();
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(4566).await.unwrap();
    let endpoint = format!("http://{}:{}", host, port);

    let seed_config = aws_sdk_s3::Config::builder()
        .behavior_version(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-east-1"))
        .endpoint_url(&endpoint)
        .force_path_style(true)
        .credentials_provider(aws_credential_types::Credentials::new(
            "test", "test", None, None, "test",
        ))
        .build();
    let seed_client = aws_sdk_s3::Client::from_conf(seed_config);

    seed_client
        .create_bucket()
        .bucket("empty-bucket")
        .send()
        .await
        .unwrap();

    let service = AwsS3Service::from_endpoint("us-east-1", endpoint, "test", "test");

    let dir = tempdir().unwrap();
    let opts = AggregateOptions {
        bucket: "empty-bucket".to_string(),
        prefix: "orderbook/".to_string(),
        output_path: dir.path().join("merged.jsonl"),
        delete_after_merge: false,
    };

    let summary = aggregate_s3(&service, &opts).await.unwrap();
    assert_eq!(summary.objects_processed, 0);
    assert_eq!(summary.lines_merged, 0);
    assert_eq!(summary.bytes_downloaded, 0);
}
