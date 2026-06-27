#!/bin/bash
set -e

MODE=${mode}
S3_BUCKET=${s3_bucket}
S3_PREFIX=${s3_prefix}
CHUNK_SIZE=${chunk_size}
AGGREGATOR_URL=${aggregator_url}
REPLICATION_FACTOR=${replication_factor}
HEARTBEAT_TIMEOUT_SECS=${heartbeat_timeout_secs}
DELETE_AFTER_MERGE=${delete_after_merge}

# Install AWS CLI
apt-get update
apt-get install -y awscli

# Create directories
mkdir -p /opt/polymarket /data/orderbook

# Download collector binary from S3
aws s3 cp "s3://${S3_BUCKET}/polymarket-collector" /opt/polymarket/polymarket-collector
chmod +x /opt/polymarket/polymarket-collector

if [ "$MODE" = "aggregator" ]; then
    DELETE_FLAG=""
    if [ "$DELETE_AFTER_MERGE" = "true" ]; then
        DELETE_FLAG="--delete-after-merge"
    fi

    cat > /etc/systemd/system/polymarket-aggregator.service << EOF
[Unit]
Description=Polymarket Orderbook Aggregator
After=network.target

[Service]
Type=simple
Restart=always
RestartSec=5
ExecStart=/opt/polymarket/polymarket-collector aggregator \\
    --bind 0.0.0.0:8080 \\
    --output-path /data/aggregated_orderbook.jsonl \\
    --s3-bucket ${S3_BUCKET} \\
    --s3-prefix ${S3_PREFIX} \\
    --region ${region} \\
    --replication-factor ${REPLICATION_FACTOR} \\
    --heartbeat-timeout-secs ${HEARTBEAT_TIMEOUT_SECS} \\
    ${DELETE_FLAG}
WorkingDirectory=/data
StandardOutput=append:/data/aggregator.log
StandardError=append:/data/aggregator.log

[Install]
WantedBy=multi-user.target
EOF
    systemctl daemon-reload
    systemctl enable polymarket-aggregator
    systemctl start polymarket-aggregator

elif [ "$MODE" = "collector" ]; then
    cat > /etc/systemd/system/polymarket-collector.service << EOF
[Unit]
Description=Polymarket Orderbook Collector
After=network.target

[Service]
Type=simple
Restart=always
RestartSec=5
ExecStart=/opt/polymarket/polymarket-collector collect-orderbook \\
    --aggregator-url ${AGGREGATOR_URL} \\
    --output-dir /data/orderbook \\
    --s3-bucket ${S3_BUCKET} \\
    --s3-prefix ${S3_PREFIX} \\
    --aws-region ${region} \\
    --chunk-size ${CHUNK_SIZE} \\
    --rotate-interval-secs 300
WorkingDirectory=/data
StandardOutput=append:/data/collector.log
StandardError=append:/data/collector.log

[Install]
WantedBy=multi-user.target
EOF
    systemctl daemon-reload
    systemctl enable polymarket-collector
    systemctl start polymarket-collector
fi

# Optional: CloudWatch agent
apt-get install -y amazon-cloudwatch-agent
cat > /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json << 'EOF'
{
  "metrics": {
    "namespace": "PolymarketCollector",
    "metrics_collected": {
      "disk": {
        "measurement": ["used_percent"],
        "resources": ["*"]
      },
      "mem": {
        "measurement": ["used_percent"]
      }
    }
  }
}
EOF
/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -s -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
