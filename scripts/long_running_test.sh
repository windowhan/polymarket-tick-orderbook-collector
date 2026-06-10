#!/bin/bash
set -e

# Long-running stability test for Polymarket collector.
# Usage: ./long_running_test.sh [hours]
# Default: 8 hours

cd "$(dirname "$0")/.."

export RUST_LOG=info

DURATION_HOURS="${1:-8}"
DURATION_SECS=$((DURATION_HOURS * 3600))

echo "=========================================="
echo "Polymarket Collector Stability Test"
echo "Duration: ${DURATION_HOURS} hours (${DURATION_SECS} seconds)"
echo "Start: $(date)"
echo "=========================================="

# Cleanup previous test
echo "Cleaning up..."
lsof -ti:8080 | xargs kill -9 2>/dev/null || true
sleep 1

rm -f data/longrun_aggregated.jsonl data/longrun_metrics.csv
mkdir -p data

# Build if needed
cargo build --quiet

# Start aggregator
echo "Starting aggregator on :8080..."
./target/debug/polymarket-collector aggregator \
  --bind 127.0.0.1:8080 \
  --output-path data/longrun_aggregated.jsonl \
  > data/aggregator_longrun.log 2>&1 &
AGG_PID=$!
sleep 2

# Start collector
echo "Starting collector (PID will be shown)..."
./target/debug/polymarket-collector collect-orderbook \
  --markets-path data/markets_sample.jsonl \
  --chunk-size 30 \
  --relay-url http://127.0.0.1:8080/ingest \
  --output-dir data/orderbook_longrun \
  --duration-secs $DURATION_SECS \
  > data/collector_longrun.log 2>&1 &
COL_PID=$!

echo ""
echo "Aggregator PID: $AGG_PID"
echo "Collector PID:  $COL_PID"
echo ""
echo "To monitor: tail -f data/collector_longrun.log"
echo "To stop:    kill $COL_PID $AGG_PID"
echo ""

# Metrics logging
echo "timestamp,elapsed_min,total_lines,lines_last_min" > data/longrun_metrics.csv
PREV_LINES=0
START_EPOCH=$(date +%s)

while kill -0 $COL_PID 2>/dev/null; do
    sleep 60
    
    NOW_EPOCH=$(date +%s)
    ELAPSED_MIN=$(( (NOW_EPOCH - START_EPOCH) / 60 ))
    TOTAL_LINES=$(wc -l < data/longrun_aggregated.jsonl 2>/dev/null || echo 0)
    LINES_LAST_MIN=$(( TOTAL_LINES - PREV_LINES ))
    PREV_LINES=$TOTAL_LINES
    
    echo "$(date -Iseconds),$ELAPSED_MIN,$TOTAL_LINES,$LINES_LAST_MIN" >> data/longrun_metrics.csv
    
    # Memory check
    COL_MEM=$(ps -o rss= -p $COL_PID 2>/dev/null || echo "0")
    AGG_MEM=$(ps -o rss= -p $AGG_PID 2>/dev/null || echo "0")
    
    printf "[%s] %3d min | Events: %6d total (%4d/min) | MEM collector:%5sKB aggregator:%5sKB\n" \
        "$(date +%H:%M:%S)" "$ELAPSED_MIN" "$TOTAL_LINES" "$LINES_LAST_MIN" "$COL_MEM" "$AGG_MEM"
done

echo ""
echo "Collector finished at $(date). Flushing aggregator..."
sleep 10

kill -TERM $AGG_PID 2>/dev/null || true
wait $AGG_PID 2>/dev/null || true

echo ""
echo "=========================================="
echo "Test Complete!"
echo "End: $(date)"
echo "=========================================="
echo ""
echo "Results:"
echo "  Total events: $(wc -l < data/longrun_aggregated.jsonl)"
echo "  Aggregator log: data/aggregator_longrun.log"
echo "  Collector log:  data/collector_longrun.log"
echo "  Metrics CSV:    data/longrun_metrics.csv"
echo ""
echo "Quick summary:"
tail -20 data/longrun_metrics.csv
