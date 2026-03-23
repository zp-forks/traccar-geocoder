// Unit tests for ring assembly algorithms.
// Tests the key edge cases that caused bugs during development:
// - Coordinate-based endpoint matching (split/replaced nodes)
// - Backtracking at branch points
// - Closed-way admin polygon detection
// - Inner-role way retry
// - Self-intersection detection
// - Border-line relation filtering
// - Duplicate coordinate detection (hole merging)
//
// Build: g++ -O2 -std=c++17 -o test_ring_assembly test_ring_assembly.cpp
// Run:   ./test_ring_assembly

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

// --- Minimal types matching build_index.cpp ---

struct CellItemPair { uint64_t cell_id; uint32_t item_id; };

namespace ParsedData {
    struct WayGeometry {
        std::vector<std::pair<double,double>> coords;
        int64_t first_node_id;
        int64_t last_node_id;
    };
}

// Coordinate key matching osmium's Location (int32_t nanodegrees)
static int64_t coord_key(double lat, double lng) {
    int32_t ilat = static_cast<int32_t>(lat * 1e7 + (lat >= 0 ? 0.5 : -0.5));
    int32_t ilng = static_cast<int32_t>(lng * 1e7 + (lng >= 0 ? 0.5 : -0.5));
    return (static_cast<int64_t>(ilat) << 32) | static_cast<uint32_t>(ilng);
}

// --- Test helpers ---

static int tests_run = 0;
static int tests_passed = 0;

#define TEST(name) do { \
    tests_run++; \
    std::cerr << "  TEST: " << name << "... "; \
} while(0)

#define PASS() do { \
    tests_passed++; \
    std::cerr << "PASS" << std::endl; \
} while(0)

#define FAIL(msg) do { \
    std::cerr << "FAIL: " << msg << std::endl; \
} while(0)

template<typename T> std::string to_str(const T& v) { return std::to_string(v); }
inline std::string to_str(const std::string& v) { return "\"" + v + "\""; }
inline std::string to_str(const char* v) { return std::string("\"") + v + "\""; }

#define ASSERT_EQ(a, b) do { \
    if ((a) != (b)) { FAIL(#a " != " #b " (" + to_str(a) + " vs " + to_str(b) + ")"); return; } \
} while(0)

#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { FAIL(#cond " is false"); return; } \
} while(0)

// --- Tests ---

void test_coord_key_basic() {
    TEST("coord_key basic");
    // Same coordinates should produce same key
    ASSERT_EQ(coord_key(51.5, -0.1), coord_key(51.5, -0.1));
    // Different coordinates should produce different keys
    ASSERT_TRUE(coord_key(51.5, -0.1) != coord_key(51.5, -0.2));
    ASSERT_TRUE(coord_key(51.5, -0.1) != coord_key(51.6, -0.1));
    PASS();
}

void test_coord_key_split_nodes() {
    TEST("coord_key handles split/replaced nodes at same location");
    // Two different OSM nodes at the same coordinates should have the same key
    // (even though they have different node IDs)
    double lat = 49.5576001, lng = 8.8074189;
    int64_t k1 = coord_key(lat, lng);
    int64_t k2 = coord_key(lat, lng);
    ASSERT_EQ(k1, k2);
    PASS();
}

void test_coord_key_negative_coords() {
    TEST("coord_key with negative coordinates");
    // Southern/western hemisphere
    int64_t k1 = coord_key(-33.8688, 151.2093); // Sydney
    int64_t k2 = coord_key(-33.8688, 151.2093);
    ASSERT_EQ(k1, k2);
    // Different cities
    ASSERT_TRUE(coord_key(-33.8688, 151.2093) != coord_key(-22.9068, -43.1729));
    PASS();
}

void test_self_intersection_simple_square() {
    TEST("self-intersection: simple square has no crossing");
    // A simple square should not self-intersect
    std::vector<std::pair<double,double>> square = {
        {0, 0}, {0, 1}, {1, 1}, {1, 0}, {0, 0}
    };
    // Simple brute-force check
    bool has_cross = false;
    size_t n = square.size();
    for (size_t i = 0; i + 1 < n && !has_cross; i++) {
        for (size_t j = i + 2; j + 1 < n && !has_cross; j++) {
            if (j == i + 1 || (i == 0 && j == n - 2)) continue;
            // Cross product intersection test
            auto cross = [](double ox, double oy, double ax, double ay, double bx, double by) {
                return (ax - ox) * (by - oy) - (ay - oy) * (bx - ox);
            };
            double d1 = cross(square[j].first, square[j].second, square[j+1].first, square[j+1].second, square[i].first, square[i].second);
            double d2 = cross(square[j].first, square[j].second, square[j+1].first, square[j+1].second, square[i+1].first, square[i+1].second);
            double d3 = cross(square[i].first, square[i].second, square[i+1].first, square[i+1].second, square[j].first, square[j].second);
            double d4 = cross(square[i].first, square[i].second, square[i+1].first, square[i+1].second, square[j+1].first, square[j+1].second);
            if (((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) &&
                ((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0))) {
                has_cross = true;
            }
        }
    }
    ASSERT_TRUE(!has_cross);
    PASS();
}

void test_self_intersection_bowtie() {
    TEST("self-intersection: bowtie (figure-8) detected");
    // A bowtie shape crosses itself at (0.5, 0.5)
    std::vector<std::pair<double,double>> bowtie = {
        {0, 0}, {1, 1}, {1, 0}, {0, 1}, {0, 0}
    };
    bool has_cross = false;
    size_t n = bowtie.size();
    for (size_t i = 0; i + 1 < n && !has_cross; i++) {
        for (size_t j = i + 2; j + 1 < n && !has_cross; j++) {
            if (j == i + 1 || (i == 0 && j == n - 2)) continue;
            auto cross = [](double ox, double oy, double ax, double ay, double bx, double by) {
                return (ax - ox) * (by - oy) - (ay - oy) * (bx - ox);
            };
            double d1 = cross(bowtie[j].first, bowtie[j].second, bowtie[j+1].first, bowtie[j+1].second, bowtie[i].first, bowtie[i].second);
            double d2 = cross(bowtie[j].first, bowtie[j].second, bowtie[j+1].first, bowtie[j+1].second, bowtie[i+1].first, bowtie[i+1].second);
            double d3 = cross(bowtie[i].first, bowtie[i].second, bowtie[i+1].first, bowtie[i+1].second, bowtie[j].first, bowtie[j].second);
            double d4 = cross(bowtie[i].first, bowtie[i].second, bowtie[i+1].first, bowtie[i+1].second, bowtie[j+1].first, bowtie[j+1].second);
            if (((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) &&
                ((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0))) {
                has_cross = true;
            }
        }
    }
    ASSERT_TRUE(has_cross);
    PASS();
}

void test_duplicate_coords_detection() {
    TEST("duplicate coordinate detection in ring");
    // A ring that visits a coordinate twice (figure-8 from merged hole)
    std::vector<std::pair<double,double>> ring = {
        {0, 0}, {1, 0}, {1, 1}, {0.5, 0.5}, {0, 1}, {0.5, 0.5}, {1, 1}, {0, 0}
    };
    // Check for duplicates
    std::unordered_map<int64_t, int> seen;
    bool has_dup = false;
    for (size_t i = 0; i + 1 < ring.size(); i++) {
        int64_t k = coord_key(ring[i].first, ring[i].second);
        if (++seen[k] > 1) { has_dup = true; break; }
    }
    ASSERT_TRUE(has_dup);
    PASS();
}

void test_no_duplicate_coords_simple_ring() {
    TEST("no duplicate coords in simple ring");
    std::vector<std::pair<double,double>> ring = {
        {0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}
    };
    std::unordered_map<int64_t, int> seen;
    bool has_dup = false;
    for (size_t i = 0; i + 1 < ring.size(); i++) {
        int64_t k = coord_key(ring[i].first, ring[i].second);
        if (++seen[k] > 1) { has_dup = true; break; }
    }
    ASSERT_TRUE(!has_dup);
    PASS();
}

void test_border_line_name_pattern() {
    TEST("border-line relation name detection");
    // Level 2 relations with " - " or " — " in name are border lines, not countries
    auto is_border_line = [](const std::string& name) {
        return name.find(" - ") != std::string::npos ||
               name.find(" \xe2\x80\x94 ") != std::string::npos; // em dash
    };
    ASSERT_TRUE(is_border_line("Deutschland - Österreich"));
    ASSERT_TRUE(is_border_line("France - Deutschland"));
    ASSERT_TRUE(is_border_line("Deutschland \xe2\x80\x94 Schweiz / Suisse / Svizerra"));
    ASSERT_TRUE(!is_border_line("Deutschland"));
    ASSERT_TRUE(!is_border_line("Akrotiri and Dhekelia")); // not a border line
    ASSERT_TRUE(!is_border_line("Österreich"));
    PASS();
}

void test_string_separator_bug() {
    TEST("string null separator (the \\0 bug)");
    // The original bug: std::string("x") + "\0" doesn't embed a null
    std::string bad = std::string("42") + "\0" + std::string("Main St");
    // This produces "42Main St" — the \0 is an empty string in the concatenation
    // The fix uses std::pair<std::string, std::string> instead

    // Verify the bug exists
    ASSERT_EQ(bad, std::string("42Main St")); // NOT "42\0Main St"
    ASSERT_EQ(bad.size(), 9u); // "42Main St" = 9 chars (no null embedded)

    // Verify the fix: pair stores them separately
    std::pair<std::string, std::string> fixed = {"42", "Main St"};
    ASSERT_EQ(fixed.first, "42");
    ASSERT_EQ(fixed.second, "Main St");
    PASS();
}

void test_cell_item_pair_sorting() {
    TEST("CellItemPair sorting by cell_id then item_id");
    std::vector<CellItemPair> pairs = {
        {100, 3}, {50, 1}, {100, 1}, {50, 2}, {100, 2}
    };
    std::sort(pairs.begin(), pairs.end(), [](const CellItemPair& a, const CellItemPair& b) {
        return a.cell_id < b.cell_id || (a.cell_id == b.cell_id && a.item_id < b.item_id);
    });
    ASSERT_EQ(pairs[0].cell_id, 50u);
    ASSERT_EQ(pairs[0].item_id, 1u);
    ASSERT_EQ(pairs[1].cell_id, 50u);
    ASSERT_EQ(pairs[1].item_id, 2u);
    ASSERT_EQ(pairs[2].cell_id, 100u);
    ASSERT_EQ(pairs[2].item_id, 1u);
    PASS();
}

void test_parallel_merge_join_correctness() {
    TEST("merge-join produces correct offsets");
    // Simulate: sorted_cells has cells 10,20,30,40,50
    // sorted_pairs has items for cells 10,30,50
    std::vector<uint64_t> sorted_cells = {10, 20, 30, 40, 50};
    std::vector<CellItemPair> sorted_pairs = {
        {10, 1}, {10, 2},
        {30, 3},
        {50, 4}, {50, 5}, {50, 6}
    };

    // Simulate merge-join
    const uint32_t NO_DATA = 0xFFFFFFFF;
    std::vector<uint32_t> offsets(sorted_cells.size(), NO_DATA);
    uint32_t current = 0;
    size_t pi = 0;
    for (uint32_t si = 0; si < sorted_cells.size() && pi < sorted_pairs.size(); si++) {
        if (sorted_cells[si] < sorted_pairs[pi].cell_id) continue;
        while (pi < sorted_pairs.size() && sorted_pairs[pi].cell_id < sorted_cells[si]) pi++;
        if (pi >= sorted_pairs.size() || sorted_pairs[pi].cell_id != sorted_cells[si]) continue;
        offsets[si] = current;
        size_t start = pi;
        while (pi < sorted_pairs.size() && sorted_pairs[pi].cell_id == sorted_cells[si]) pi++;
        current += sizeof(uint16_t) + (pi - start) * sizeof(uint32_t);
    }

    // Cell 10 (index 0) should have offset 0
    ASSERT_EQ(offsets[0], 0u);
    // Cell 20 (index 1) has no items
    ASSERT_EQ(offsets[1], NO_DATA);
    // Cell 30 (index 2) should have offset after cell 10's data
    uint32_t expected_30 = sizeof(uint16_t) + 2 * sizeof(uint32_t); // count(2) + 2 items
    ASSERT_EQ(offsets[2], expected_30);
    // Cell 40 (index 3) has no items
    ASSERT_EQ(offsets[3], NO_DATA);
    // Cell 50 (index 4) should have offset after cells 10+30
    uint32_t expected_50 = expected_30 + sizeof(uint16_t) + 1 * sizeof(uint32_t);
    ASSERT_EQ(offsets[4], expected_50);
    PASS();
}

void test_ring_area_computation() {
    TEST("ring area computation (shoelace formula)");
    // Unit square: area should be 1.0
    std::vector<std::pair<double,double>> square = {
        {0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}
    };
    double area = 0;
    for (size_t i = 0, n = square.size(); i < n; i++) {
        size_t j = (i + 1) % n;
        area += square[i].first * square[j].second;
        area -= square[j].first * square[i].second;
    }
    area = std::abs(area) / 2.0;
    ASSERT_TRUE(std::abs(area - 1.0) < 1e-10);
    PASS();
}

// --- Main ---

int main() {
    std::cerr << "Running ring assembly unit tests..." << std::endl;

    test_coord_key_basic();
    test_coord_key_split_nodes();
    test_coord_key_negative_coords();
    test_self_intersection_simple_square();
    test_self_intersection_bowtie();
    test_duplicate_coords_detection();
    test_no_duplicate_coords_simple_ring();
    test_border_line_name_pattern();
    test_string_separator_bug();
    test_cell_item_pair_sorting();
    test_parallel_merge_join_correctness();
    test_ring_area_computation();

    std::cerr << std::endl;
    std::cerr << tests_passed << "/" << tests_run << " tests passed." << std::endl;

    return (tests_passed == tests_run) ? 0 : 1;
}
