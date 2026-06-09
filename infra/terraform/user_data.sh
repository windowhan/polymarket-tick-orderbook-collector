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
mkdir -p /opt/polymarket /data/orderbook

# Download collector binary from S3
aws s3 cp "s3://${S3_BUCKET}/polymarket-collector" /opt/polymarket/polymarket-collector
chmod +x /opt/polymarket/polymarket-collector

if [ "$MODE" = "aggregator" ]; then
    # Legacy HTTP aggregator mode. Not used in the cost-optimized S3 architecture.
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
    # Collector mode: download assigned shard and write locally with rotation
    aws s3 cp "s3://${S3_BUCKET}/markets_shard_${SHARD_INDEX}.jsonl" /data/markets_shard.jsonl

    cat > /etc/systemd/system/polymarket-collector.service << EOF
[Unit]
Description=Polymarket Orderbook Collector
After=network.target

[Service]
Type=simple
Restart=always
RestartSec=5
ExecStart=/opt/polymarket/polymarket-collector collect-orderbook --markets-path /data/markets_shard.jsonl --output-dir /data/orderbook --chunk-size ${CHUNK_SIZE} --rotate-interval-secs 300
WorkingDirectory=/data
StandardOutput=append:/data/collector.log
StandardError=append:/data/collector.log

[Install]
WantedBy=multi-user.target
EOF
    systemctl daemon-reload
    systemctl enable polymarket-collector
    systemctl start polymarket-collector

    # Upload closed rotation files to S3 every 5 minutes.
    # Files older than 5 minutes are guaranteed to be complete (rotated),
    # so we can safely upload and delete the local copy without data loss.
    cat > /opt/polymarket/upload-to-s3.sh << 'EOF'
#!/bin/bash
set -e
BUCKET="${S3_BUCKET}"
LOG="/data/s3_upload.log"
mkdir -p /data

echo "[$(date -Iseconds)] Starting upload scan" >> "$LOG"

find /data/orderbook -type f -mmin +5 -name "*.jsonl" -print0 | while IFS= read -r -d '' file; do
    rel="${file#/data/orderbook/}"
    key="orderbook/${rel}"
    echo "[$(date -Iseconds)] Uploading s3://${BUCKET}/${key}" >> "$LOG"
    if aws s3 cp "$file" "s3://${BUCKET}/${key}" >> "$LOG" 2>&1; then
        rm -f "$file"
        echo "[$(date -Iseconds)] Uploaded and removed ${file}" >> "$LOG"
    else
        echo "[$(date -Iseconds)] FAILED upload for ${file}" >> "$LOG"
    fi
done
EOF
    chmod +x /opt/polymarket/upload-to-s3.sh

    cat > /etc/cron.d/polymarket-s3-upload << EOF
S3_BUCKET=${S3_BUCKET}
*/5 * * * * root /opt/polymarket/upload-to-s3.sh
EOF
    chmod 644 /etc/cron.d/polymarket-s3-upload
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
