#!/bin/bash
# End-to-end test: build index from PBF, start server, query known coordinates,
# verify expected results.
#
# Usage: ./test_e2e_queries.sh <build-index-binary> <server-binary> [monaco.osm.pbf]
#
# Requires: curl, jq (or python3 for JSON parsing)

set -e

BUILD="${1:?Usage: $0 <build-index-binary> <server-binary> [test.osm.pbf]}"
SERVER="${2:?Usage: $0 <build-index-binary> <server-binary> [test.osm.pbf]}"
PBF="${3:-}"
TMPDIR=$(mktemp -d)
PORT=18080

cleanup() {
    kill $SERVER_PID 2>/dev/null || true
    rm -rf "$TMPDIR"
}
trap cleanup EXIT

# Download Monaco if needed
if [ -z "$PBF" ]; then
    echo "Downloading Monaco PBF..."
    wget -q -O "$TMPDIR/monaco.osm.pbf" "https://download.geofabrik.de/europe/monaco-latest.osm.pbf"
    PBF="$TMPDIR/monaco.osm.pbf"
fi

# Build index
echo "Building index..."
mkdir -p "$TMPDIR/index"
"$BUILD" "$TMPDIR/index" "$PBF" 2>&1 | tail -3

# Create API key
cat > "$TMPDIR/index/geocoder.json" << 'EOF'
{
  "users": {
    "test": {
      "password_hash": "$2b$12$dummy",
      "admin": true,
      "rate_per_second": 100,
      "rate_per_day": 100000,
      "rate_by_ip": false
    }
  },
  "tokens": {
    "testkey": "test"
  }
}
EOF

# Start server
echo "Starting server on port $PORT..."
"$SERVER" "$TMPDIR/index" "0.0.0.0:$PORT" &
SERVER_PID=$!
sleep 2

# Verify server is running
if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo "FAIL: Server failed to start"
    exit 1
fi

# Test queries
PASS=0
FAIL=0
KEY="testkey"

test_query() {
    local lat="$1" lon="$2" expected="$3" desc="$4"
    local result=$(curl -s "http://localhost:$PORT/reverse?lat=$lat&lon=$lon&key=$KEY")
    local display=$(echo "$result" | python3 -c "import sys,json; print(json.load(sys.stdin).get('display_name',''))" 2>/dev/null || echo "")

    if echo "$display" | grep -qi "$expected"; then
        echo "  PASS: $desc — got '$display'"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $desc — expected '$expected', got '$display'"
        FAIL=$((FAIL + 1))
    fi
}

echo ""
echo "=== E2E Query Tests ==="

# Monaco coordinates
test_query "43.7384" "7.4246" "Monaco" "Monaco city center"
test_query "43.7311" "7.4197" "Monaco" "Monte Carlo area"

# Test that server returns valid JSON
RESULT=$(curl -s "http://localhost:$PORT/reverse?lat=43.7384&lon=7.4246&key=$KEY")
if echo "$RESULT" | python3 -c "import sys,json; json.load(sys.stdin)" 2>/dev/null; then
    echo "  PASS: Valid JSON response"
    PASS=$((PASS + 1))
else
    echo "  FAIL: Invalid JSON response: $RESULT"
    FAIL=$((FAIL + 1))
fi

# Test missing API key
RESULT=$(curl -s "http://localhost:$PORT/reverse?lat=43.7384&lon=7.4246")
if echo "$RESULT" | grep -q "Missing API key"; then
    echo "  PASS: Missing API key returns error"
    PASS=$((PASS + 1))
else
    echo "  FAIL: Missing API key should return error"
    FAIL=$((FAIL + 1))
fi

# Test invalid API key
RESULT=$(curl -s "http://localhost:$PORT/reverse?lat=43.7384&lon=7.4246&key=badkey")
if echo "$RESULT" | grep -q "Invalid API key"; then
    echo "  PASS: Invalid API key returns error"
    PASS=$((PASS + 1))
else
    echo "  FAIL: Invalid API key should return error"
    FAIL=$((FAIL + 1))
fi

echo ""
echo "$PASS passed, $FAIL failed out of $((PASS + FAIL)) tests."
echo "=== E2E tests complete ==="

exit $FAIL
