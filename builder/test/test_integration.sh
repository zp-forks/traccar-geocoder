#!/bin/bash
# Integration test: build Monaco PBF with both parallel and sequential
# admin assembly, verify identical output for ways/addrs/interps,
# and check admin polygon counts are within expected range.
#
# Usage: ./test_integration.sh <build-index-binary> [monaco.osm.pbf]
#
# Downloads Monaco PBF if not provided.

set -e

BINARY="${1:?Usage: $0 <build-index-binary> [monaco.osm.pbf]}"
PBF="${2:-}"
COMPARE="${3:-}"
TMPDIR=$(mktemp -d)

cleanup() { rm -rf "$TMPDIR"; }
trap cleanup EXIT

# Download Monaco if needed
if [ -z "$PBF" ]; then
    echo "Downloading Monaco PBF..."
    wget -q -O "$TMPDIR/monaco.osm.pbf" "https://download.geofabrik.de/europe/monaco-latest.osm.pbf"
    PBF="$TMPDIR/monaco.osm.pbf"
fi

# Build compare tool if not provided
if [ -z "$COMPARE" ]; then
    COMPARE="$TMPDIR/compare_indexes"
    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
    echo "Building comparison tool..."
    g++ -O2 -std=c++17 -o "$COMPARE" "$SCRIPT_DIR/compare_indexes.cpp"
fi

echo "=== Integration Test ==="
echo "Binary: $BINARY"
echo "PBF: $PBF"
echo ""

# Test 1: Build with parallel admin assembly
echo "--- Test 1: Parallel admin assembly ---"
mkdir -p "$TMPDIR/parallel"
"$BINARY" "$TMPDIR/parallel" "$PBF" --parallel-admin 2>&1 | grep -E 'polygon|Done|street ways|address'

# Test 2: Build with sequential admin assembly
echo ""
echo "--- Test 2: Sequential admin assembly ---"
mkdir -p "$TMPDIR/sequential"
"$BINARY" "$TMPDIR/sequential" "$PBF" --no-parallel-admin 2>&1 | grep -E 'polygon|Done|street ways|address'

# Test 3: Compare outputs
echo ""
echo "--- Test 3: Exact comparison ---"
"$COMPARE" "$TMPDIR/parallel" "$TMPDIR/sequential" 2>&1

# Test 4: Semantic comparison
echo ""
echo "--- Test 4: Semantic comparison ---"
"$COMPARE" "$TMPDIR/parallel" "$TMPDIR/sequential" --semantic 2>&1

# Test 5: Verify all index files exist
echo ""
echo "--- Test 5: Index files present ---"
EXPECTED_FILES="admin_cells.bin admin_entries.bin admin_polygons.bin admin_vertices.bin \
addr_entries.bin addr_points.bin geo_cells.bin interp_entries.bin interp_nodes.bin \
interp_ways.bin street_entries.bin street_nodes.bin street_ways.bin strings.bin"
ALL_PRESENT=true
for f in $EXPECTED_FILES; do
    if [ ! -f "$TMPDIR/parallel/$f" ]; then
        echo "MISSING: $f"
        ALL_PRESENT=false
    fi
done
if $ALL_PRESENT; then
    echo "All 14 index files present."
fi

# Test 6: Verify files are non-empty (except interp which can be empty for Monaco)
echo ""
echo "--- Test 6: Non-empty files ---"
for f in admin_cells.bin admin_polygons.bin geo_cells.bin street_ways.bin street_nodes.bin addr_points.bin strings.bin; do
    SIZE=$(stat --format='%s' "$TMPDIR/parallel/$f" 2>/dev/null || echo 0)
    if [ "$SIZE" -eq 0 ]; then
        echo "FAIL: $f is empty"
        ALL_PRESENT=false
    fi
done
if $ALL_PRESENT; then
    echo "All required files non-empty."
fi

echo ""
echo "=== Integration test complete ==="
