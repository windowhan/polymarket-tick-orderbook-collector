#!/bin/bash
set -e

cd "$(dirname "$0")/.."

export RUST_LOG=info

echo "=== Cleaning up port 8080 ==="
lsof -ti:8080 | xargs kill -9 2>/dev/null || true
sleep 1

echo "=== Building ==="
cargo build --quiet

OUTPUT="data/local_test_aggregated.jsonl"
rm -f "$OUTPUT"
mkdir -p data

echo "=== Starting Aggregator on :8080 ==="
./target/debug/polymarket-collector aggregator --bind 127.0.0.1:8080 --output-path "$OUTPUT" > data/aggregator.log 2>&1 &
AGG_PID=$!
sleep 2

echo "=== Starting Collector (60 seconds, 8 tokens, 1 worker) ==="
./target/debug/polymarket-collector collect-orderbook \
  --markets-path data/markets_sample.jsonl \
  --chunk-size 30 \
  --relay-url http://127.0.0.1:8080/ingest \
  --output-dir data/orderbook_local \
  --duration-secs 60 \
  > data/collector.log 2>&1 || true

echo "=== Waiting 5s for aggregator flush ==="
sleep 5

echo "=== Stopping Aggregator ==="
kill -TERM $AGG_PID 2>/dev/null || true
wait $AGG_PID 2>/dev/null || true
sleep 1

echo ""
echo "=== Results ==="
if [ -f "$OUTPUT" ]; then
    LINES=$(wc -l < "$OUTPUT")
    echo "Total events aggregated: $LINES lines"
    echo ""
    echo "First 3 events:"
    head -3 "$OUTPUT" | jq -c '{event_type: .event_type, asset: .asset, price: .price, side: .side}' 2>/dev/null || head -3 "$OUTPUT"
else
    echo "No output file generated"
fi

echo ""
echo "=== Collector log (last 30 lines) ==="
tail -30 data/collector.log || echo "No collector log"

echo ""
echo "=== Aggregator log (last 30 lines) ==="
tail -30 data/aggregator.log || echo "No aggregator log"
