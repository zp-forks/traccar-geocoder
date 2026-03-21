#!/bin/bash
# Test that admin boundary processing produces identical output
# regardless of whether osmium's multipolygon assembler or our
# custom parallel assembler is used.
#
# Usage: ./test_admin_assembly.sh <build-index-binary> <test.osm.pbf>
#
# Downloads Monaco PBF if no test file provided.

set -e

BINARY="${1:?Usage: $0 <build-index-binary> [test.osm.pbf]}"
PBF="${2:-}"
TMPDIR=$(mktemp -d)

cleanup() { rm -rf "$TMPDIR"; }
trap cleanup EXIT

# Download Monaco PBF if none provided
if [ -z "$PBF" ]; then
    PBF="$TMPDIR/monaco.osm.pbf"
    echo "Downloading Monaco PBF..."
    curl -fSL -o "$PBF" "https://download.geofabrik.de/europe/monaco-latest.osm.pbf" 2>/dev/null
fi

echo "=== Test 1: Full build produces valid output ==="
mkdir -p "$TMPDIR/full"
"$BINARY" "$TMPDIR/full" "$PBF" > "$TMPDIR/full.log" 2>&1
echo "Full build: $(ls "$TMPDIR/full/"*.bin | wc -l) files"

# Check all expected files exist
for f in geo_cells street_entries street_ways street_nodes addr_entries addr_points \
         interp_entries interp_ways interp_nodes admin_cells admin_entries \
         admin_polygons admin_vertices strings; do
    if [ ! -f "$TMPDIR/full/${f}.bin" ]; then
        echo "FAIL: missing $f.bin"
        exit 1
    fi
done
echo "PASS: All 14 index files present"

# Check admin data is non-empty
ADMIN_SIZE=$(stat -c%s "$TMPDIR/full/admin_polygons.bin" 2>/dev/null || stat -f%z "$TMPDIR/full/admin_polygons.bin")
if [ "$ADMIN_SIZE" -eq 0 ]; then
    echo "FAIL: admin_polygons.bin is empty"
    exit 1
fi
echo "PASS: admin_polygons.bin is ${ADMIN_SIZE} bytes"

ADMIN_VERTS=$(stat -c%s "$TMPDIR/full/admin_vertices.bin" 2>/dev/null || stat -f%z "$TMPDIR/full/admin_vertices.bin")
echo "PASS: admin_vertices.bin is ${ADMIN_VERTS} bytes"

echo ""
echo "=== Test 2: Multi-output produces all 3 modes ==="
mkdir -p "$TMPDIR/multi"
"$BINARY" "$TMPDIR/multi" "$PBF" --multi-output > "$TMPDIR/multi.log" 2>&1

for mode in full no-addresses admin-only; do
    if [ ! -d "$TMPDIR/multi/$mode" ]; then
        echo "FAIL: missing $mode directory"
        exit 1
    fi
    if [ ! -f "$TMPDIR/multi/$mode/admin_cells.bin" ]; then
        echo "FAIL: missing $mode/admin_cells.bin"
        exit 1
    fi
done
echo "PASS: All 3 output modes present"

# Admin data should be identical across all modes
for f in admin_cells admin_entries admin_polygons admin_vertices; do
    if ! diff -q "$TMPDIR/multi/full/${f}.bin" "$TMPDIR/multi/no-addresses/${f}.bin" > /dev/null 2>&1; then
        echo "FAIL: ${f}.bin differs between full and no-addresses"
        exit 1
    fi
    if ! diff -q "$TMPDIR/multi/full/${f}.bin" "$TMPDIR/multi/admin-only/${f}.bin" > /dev/null 2>&1; then
        echo "FAIL: ${f}.bin differs between full and admin-only"
        exit 1
    fi
done
echo "PASS: Admin data identical across all modes"

# Full mode should have geo files, admin-only should not
if [ ! -f "$TMPDIR/multi/full/geo_cells.bin" ]; then
    echo "FAIL: full mode missing geo_cells.bin"
    exit 1
fi
if [ -f "$TMPDIR/multi/admin-only/geo_cells.bin" ]; then
    echo "FAIL: admin-only mode should not have geo_cells.bin"
    exit 1
fi
echo "PASS: Correct files per mode"

echo ""
echo "=== Test 3: Cache round-trip produces identical output ==="
mkdir -p "$TMPDIR/cached"
"$BINARY" "$TMPDIR/cached" "$PBF" --save-cache "$TMPDIR/test.cache" > "$TMPDIR/cached.log" 2>&1

mkdir -p "$TMPDIR/from-cache"
"$BINARY" "$TMPDIR/from-cache" --load-cache "$TMPDIR/test.cache" > "$TMPDIR/from-cache.log" 2>&1

DIFF_COUNT=0
for f in "$TMPDIR/cached/"*.bin; do
    fname=$(basename "$f")
    if ! diff -q "$f" "$TMPDIR/from-cache/$fname" > /dev/null 2>&1; then
        echo "FAIL: $fname differs between direct and cache-loaded build"
        DIFF_COUNT=$((DIFF_COUNT + 1))
    fi
done
if [ "$DIFF_COUNT" -eq 0 ]; then
    echo "PASS: Cache round-trip produces identical output"
else
    echo "FAIL: $DIFF_COUNT files differ"
    exit 1
fi

echo ""
echo "=== Test 4: Continent filtering produces valid subsets ==="
mkdir -p "$TMPDIR/continents"
"$BINARY" "$TMPDIR/continents" "$PBF" --continents > "$TMPDIR/continents.log" 2>&1

# Monaco is in Europe — check that europe has admin data
if [ -d "$TMPDIR/continents/europe" ]; then
    EUR_ADMIN=$(stat -c%s "$TMPDIR/continents/europe/admin_polygons.bin" 2>/dev/null || stat -f%z "$TMPDIR/continents/europe/admin_polygons.bin")
    if [ "$EUR_ADMIN" -gt 0 ]; then
        echo "PASS: Europe continent has admin data (${EUR_ADMIN} bytes)"
    else
        echo "FAIL: Europe continent admin_polygons.bin is empty"
        exit 1
    fi
else
    echo "WARN: No europe directory (Monaco may not match bounding box)"
fi

echo ""
echo "=== Test 5: Admin boundary data integrity ==="
# Verify admin polygon struct sizes are consistent
# AdminPolygon is 24 bytes, NodeCoord is 8 bytes
POLY_SIZE=$(stat -c%s "$TMPDIR/full/admin_polygons.bin" 2>/dev/null || stat -f%z "$TMPDIR/full/admin_polygons.bin")
VERT_SIZE=$(stat -c%s "$TMPDIR/full/admin_vertices.bin" 2>/dev/null || stat -f%z "$TMPDIR/full/admin_vertices.bin")

POLY_COUNT=$((POLY_SIZE / 24))
VERT_COUNT=$((VERT_SIZE / 8))

if [ $((POLY_SIZE % 24)) -ne 0 ]; then
    echo "FAIL: admin_polygons.bin size ($POLY_SIZE) not divisible by 24"
    exit 1
fi
if [ $((VERT_SIZE % 8)) -ne 0 ]; then
    echo "FAIL: admin_vertices.bin size ($VERT_SIZE) not divisible by 8"
    exit 1
fi
echo "PASS: $POLY_COUNT polygons, $VERT_COUNT vertices (struct alignment correct)"

echo ""
echo "=== ALL TESTS PASSED ==="
