#!/bin/bash
set -e

# Long-running stability test for standalone API-first Polymarket collection.
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
rm -f data/longrun_metrics.csv
rm -rf data/orderbook_longrun
mkdir -p data/orderbook_longrun

# Build if needed
cargo build --quiet

# Start collector in the current default live mode. Static/offline fixture runs
# can pass --static-markets-path manually, but this stability script exercises
# the API-first refresh path.
echo "Starting collector (PID will be shown)..."
./target/debug/polymarket-collector collect-orderbook \
  --chunk-size 30 \
  --output-dir data/orderbook_longrun \
  --duration-secs $DURATION_SECS \
  > data/collector_longrun.log 2>&1 &
COL_PID=$!

echo ""
echo "Collector PID:  $COL_PID"
echo ""
echo "To monitor: tail -f data/collector_longrun.log"
echo "To stop:    kill $COL_PID"
echo ""

# Metrics logging
echo "timestamp,elapsed_min,total_lines,lines_last_min" > data/longrun_metrics.csv
PREV_LINES=0
START_EPOCH=$(date +%s)

while kill -0 $COL_PID 2>/dev/null; do
    sleep 60
    
    NOW_EPOCH=$(date +%s)
    ELAPSED_MIN=$(( (NOW_EPOCH - START_EPOCH) / 60 ))
    TOTAL_LINES=$(find data/orderbook_longrun -name '*.jsonl' -type f -exec cat {} + 2>/dev/null \
        | wc -l \
        | tr -d ' ')
    LINES_LAST_MIN=$(( TOTAL_LINES - PREV_LINES ))
    PREV_LINES=$TOTAL_LINES
    
    echo "$(date -Iseconds),$ELAPSED_MIN,$TOTAL_LINES,$LINES_LAST_MIN" >> data/longrun_metrics.csv
    
    # Memory check
    COL_MEM=$(ps -o rss= -p $COL_PID 2>/dev/null || echo "0")
    printf "[%s] %3d min | Events: %6d total (%4d/min) | MEM collector:%5sKB\n" \
        "$(date +%H:%M:%S)" "$ELAPSED_MIN" "$TOTAL_LINES" "$LINES_LAST_MIN" "$COL_MEM"
done

set +e
wait "$COL_PID"
COL_STATUS=$?
set -e

echo ""
echo "Collector finished at $(date) with exit status $COL_STATUS."

echo ""
echo "=========================================="
echo "Test Complete!"
echo "End: $(date)"
echo "=========================================="
echo ""
echo "Results:"
echo "  Total events: $(find data/orderbook_longrun -name '*.jsonl' -type f -exec cat {} + 2>/dev/null | wc -l | tr -d ' ')"
echo "  Collector log:  data/collector_longrun.log"
echo "  Metrics CSV:    data/longrun_metrics.csv"
echo ""
echo "Quick summary:"
tail -20 data/longrun_metrics.csv

exit "$COL_STATUS"
