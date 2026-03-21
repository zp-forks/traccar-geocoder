#!/bin/bash
# Compare admin boundary output between two builder binaries.
# Used to verify that the parallel admin assembly produces
# identical output to the osmium-based assembly.
#
# Usage: ./test_admin_compare.sh <binary-A> <binary-B> [test.osm.pbf]
#
# Both binaries must accept the same CLI interface.
# Compares: admin_cells.bin, admin_entries.bin, admin_polygons.bin, admin_vertices.bin

set -e

BINARY_A="${1:?Usage: $0 <binary-A> <binary-B> [test.osm.pbf]}"
BINARY_B="${2:?Usage: $0 <binary-A> <binary-B> [test.osm.pbf]}"
PBF="${3:-}"
TMPDIR=$(mktemp -d)

cleanup() { rm -rf "$TMPDIR"; }
trap cleanup EXIT

if [ -z "$PBF" ]; then
    PBF="$TMPDIR/monaco.osm.pbf"
    echo "Downloading Monaco PBF..."
    curl -fSL -o "$PBF" "https://download.geofabrik.de/europe/monaco-latest.osm.pbf" 2>/dev/null
fi

echo "Binary A: $BINARY_A"
echo "Binary B: $BINARY_B"
echo "PBF: $PBF"
echo ""

echo "=== Running Binary A ==="
mkdir -p "$TMPDIR/a"
"$BINARY_A" "$TMPDIR/a" "$PBF" > "$TMPDIR/a.log" 2>&1
echo "Done. $(grep -c 'admin' "$TMPDIR/a.log" 2>/dev/null || echo 0) admin log lines"

echo ""
echo "=== Running Binary B ==="
mkdir -p "$TMPDIR/b"
"$BINARY_B" "$TMPDIR/b" "$PBF" > "$TMPDIR/b.log" 2>&1
echo "Done. $(grep -c 'admin' "$TMPDIR/b.log" 2>/dev/null || echo 0) admin log lines"

echo ""
echo "=== Comparing Admin Files ==="
PASS=true

for f in admin_cells.bin admin_entries.bin admin_polygons.bin admin_vertices.bin; do
    SIZE_A=$(stat -c%s "$TMPDIR/a/$f" 2>/dev/null || stat -f%z "$TMPDIR/a/$f" 2>/dev/null || echo 0)
    SIZE_B=$(stat -c%s "$TMPDIR/b/$f" 2>/dev/null || stat -f%z "$TMPDIR/b/$f" 2>/dev/null || echo 0)

    if [ ! -f "$TMPDIR/a/$f" ]; then
        echo "FAIL: $f missing from Binary A output"
        PASS=false
        continue
    fi
    if [ ! -f "$TMPDIR/b/$f" ]; then
        echo "FAIL: $f missing from Binary B output"
        PASS=false
        continue
    fi

    if diff -q "$TMPDIR/a/$f" "$TMPDIR/b/$f" > /dev/null 2>&1; then
        echo "PASS: $f identical ($SIZE_A bytes)"
    else
        echo "FAIL: $f differs (A=${SIZE_A} bytes, B=${SIZE_B} bytes)"
        # Show hex diff of first differences
        echo "  First difference:"
        diff <(xxd "$TMPDIR/a/$f" | head -100) <(xxd "$TMPDIR/b/$f" | head -100) | head -10
        PASS=false
    fi
done

echo ""
echo "=== Comparing String Pool ==="
# The string pool should contain the same admin-related strings
# (may differ in order due to parallel processing, but content should match)
SIZE_A=$(stat -c%s "$TMPDIR/a/strings.bin" 2>/dev/null || stat -f%z "$TMPDIR/a/strings.bin")
SIZE_B=$(stat -c%s "$TMPDIR/b/strings.bin" 2>/dev/null || stat -f%z "$TMPDIR/b/strings.bin")

if diff -q "$TMPDIR/a/strings.bin" "$TMPDIR/b/strings.bin" > /dev/null 2>&1; then
    echo "PASS: strings.bin identical ($SIZE_A bytes)"
else
    echo "INFO: strings.bin differs (A=${SIZE_A}, B=${SIZE_B}) — expected if processing order changed"
    # Extract and compare sorted strings
    strings "$TMPDIR/a/strings.bin" | sort > "$TMPDIR/strings_a.txt"
    strings "$TMPDIR/b/strings.bin" | sort > "$TMPDIR/strings_b.txt"
    if diff -q "$TMPDIR/strings_a.txt" "$TMPDIR/strings_b.txt" > /dev/null 2>&1; then
        echo "PASS: String content identical (different ordering)"
    else
        DIFF=$(diff "$TMPDIR/strings_a.txt" "$TMPDIR/strings_b.txt" | head -20)
        echo "WARN: String content differs:"
        echo "$DIFF"
    fi
fi

echo ""
echo "=== Comparing Non-Admin Files ==="
for f in geo_cells.bin street_entries.bin street_ways.bin street_nodes.bin \
         addr_entries.bin addr_points.bin interp_entries.bin interp_ways.bin interp_nodes.bin; do
    if [ -f "$TMPDIR/a/$f" ] && [ -f "$TMPDIR/b/$f" ]; then
        if diff -q "$TMPDIR/a/$f" "$TMPDIR/b/$f" > /dev/null 2>&1; then
            SIZE=$(stat -c%s "$TMPDIR/a/$f" 2>/dev/null || stat -f%z "$TMPDIR/a/$f")
            echo "PASS: $f identical ($SIZE bytes)"
        else
            SIZE_A=$(stat -c%s "$TMPDIR/a/$f" 2>/dev/null || stat -f%z "$TMPDIR/a/$f")
            SIZE_B=$(stat -c%s "$TMPDIR/b/$f" 2>/dev/null || stat -f%z "$TMPDIR/b/$f")
            echo "INFO: $f differs (A=${SIZE_A}, B=${SIZE_B})"
        fi
    fi
done

echo ""
if $PASS; then
    echo "=== ALL ADMIN TESTS PASSED ==="
    exit 0
else
    echo "=== SOME TESTS FAILED ==="
    exit 1
fi
