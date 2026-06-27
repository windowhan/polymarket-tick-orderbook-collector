#!/bin/bash
set -e

# Legacy filename retained for convenience. The old relay ingestion mode was
# removed; this script now runs a local static/offline collector smoke test.

cd "$(dirname "$0")/.."

export RUST_LOG=info

echo "=== Building ==="
cargo build --quiet

OUTPUT_DIR="data/orderbook_local"
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

echo "=== Starting static collector smoke test (60 seconds) ==="
./target/debug/polymarket-collector collect-orderbook \
  --static-markets-path data/markets_sample.jsonl \
  --chunk-size 30 \
  --output-dir "$OUTPUT_DIR" \
  --duration-secs 60 \
  > data/collector.log 2>&1

echo ""
echo "=== Results ==="
LINES=$(find "$OUTPUT_DIR" -name '*.jsonl' -type f -exec cat {} + 2>/dev/null | wc -l | tr -d ' ')
echo "Total local orderbook events: $LINES lines"

echo ""
echo "=== Collector log (last 30 lines) ==="
tail -30 data/collector.log || echo "No collector log"
