#!/bin/bash
set -e

MODE=${mode}
SHARD_INDEX=${shard_index}
S3_BUCKET=${s3_bucket}
CHUNK_SIZE=${chunk_size}

# Install AWS CLI
apt-get update
apt-get install -y awscli

# Create directories
mkdir -p /opt/polymarket /data

# Download collector binary from S3
aws s3 cp "s3://${S3_BUCKET}/polymarket-collector" /opt/polymarket/polymarket-collector
chmod +x /opt/polymarket/polymarket-collector

if [ "$MODE" = "aggregator" ]; then
    # Aggregator mode: receive data from all collectors
    cat > /etc/systemd/system/polymarket-aggregator.service << 'EOF'
[Unit]
Description=Polymarket Orderbook Aggregator
After=network.target

[Service]
Type=simple
Restart=always
RestartSec=5
ExecStart=/opt/polymarket/polymarket-collector aggregator --bind 0.0.0.0:8080 --output-path /data/aggregated_orderbook.jsonl
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
    # Collector mode: download assigned shard and relay to aggregator
    aws s3 cp "s3://${S3_BUCKET}/markets_shard_${SHARD_INDEX}.jsonl" /data/markets_shard.jsonl

    cat > /etc/systemd/system/polymarket-collector.service << EOF
[Unit]
Description=Polymarket Orderbook Collector Relay
After=network.target

[Service]
Type=simple
Restart=always
RestartSec=5
ExecStart=/opt/polymarket/polymarket-collector collect-orderbook --markets-path /data/markets_shard.jsonl --output-dir /data/orderbook --chunk-size ${CHUNK_SIZE}
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
