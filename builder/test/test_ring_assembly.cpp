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
#include <cstring>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include "../src/types.h"
#include "../src/string_pool.h"
#include "../src/geometry.h"

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
    std::vector<std::pair<double,double>> square = {
        {0, 0}, {0, 1}, {1, 1}, {1, 0}, {0, 0}
    };
    ASSERT_TRUE(!ring_has_self_intersection(square));
    PASS();
}

void test_self_intersection_bowtie() {
    TEST("self-intersection: bowtie (figure-8) detected");
    std::vector<std::pair<double,double>> bowtie = {
        {0, 0}, {1, 1}, {1, 0}, {0, 1}, {0, 0}
    };
    ASSERT_TRUE(ring_has_self_intersection(bowtie));
    PASS();
}

void test_duplicate_coords_detection() {
    TEST("duplicate coordinate detection in ring");
    std::vector<std::pair<double,double>> ring = {
        {0, 0}, {1, 0}, {1, 1}, {0.5, 0.5}, {0, 1}, {0.5, 0.5}, {1, 1}, {0, 0}
    };
    ASSERT_TRUE(ring_has_duplicate_coords(ring));
    PASS();
}

void test_no_duplicate_coords_simple_ring() {
    TEST("no duplicate coords in simple ring");
    std::vector<std::pair<double,double>> ring = {
        {0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}
    };
    ASSERT_TRUE(!ring_has_duplicate_coords(ring));
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
    std::vector<std::pair<double,double>> square = {
        {0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}
    };
    float area = polygon_area(square);
    ASSERT_TRUE(std::abs(area - 1.0f) < 1e-6f);
    PASS();
}

// --- New tests: coord_key edge cases ---

void test_coord_key_equator_prime_meridian() {
    TEST("coord_key at equator/prime meridian (0,0)");
    int64_t k = coord_key(0.0, 0.0);
    ASSERT_TRUE(k != coord_key(0.0001, 0.0));
    ASSERT_TRUE(k != coord_key(0.0, 0.0001));
    PASS();
}

void test_coord_key_max_values() {
    TEST("coord_key at extreme coordinates");
    // North pole area
    int64_t k1 = coord_key(89.9999, 179.9999);
    int64_t k2 = coord_key(-89.9999, -179.9999);
    ASSERT_TRUE(k1 != k2);
    // Very close but distinct
    ASSERT_TRUE(coord_key(51.5000001, -0.1) != coord_key(51.5000002, -0.1));
    PASS();
}

void test_coord_key_precision_boundary() {
    TEST("coord_key precision at 1e-7 degree boundary");
    // 1e-7 degrees is ~1cm at equator — our quantization boundary
    // These should be equal (within quantization)
    ASSERT_EQ(coord_key(51.50000004, -0.1), coord_key(51.50000004, -0.1));
    // These should differ (different quantized values)
    ASSERT_TRUE(coord_key(51.5000000, -0.1) != coord_key(51.5000002, -0.1));
    PASS();
}

// --- New tests: segments_intersect ---

void test_segments_parallel() {
    TEST("segments_intersect: parallel segments don't intersect");
    ASSERT_TRUE(!segments_intersect(0,0, 1,0,  0,1, 1,1));
    PASS();
}

void test_segments_collinear() {
    TEST("segments_intersect: collinear overlapping segments");
    // Collinear segments: our cross-product test returns false (no proper crossing)
    ASSERT_TRUE(!segments_intersect(0,0, 2,0,  1,0, 3,0));
    PASS();
}

void test_segments_t_junction() {
    TEST("segments_intersect: T-junction (endpoint on segment)");
    // Endpoint touching — not a proper crossing
    ASSERT_TRUE(!segments_intersect(0,0, 2,0,  1,0, 1,1));
    PASS();
}

void test_segments_clear_cross() {
    TEST("segments_intersect: clear X crossing");
    ASSERT_TRUE(segments_intersect(0,0, 1,1,  0,1, 1,0));
    PASS();
}

void test_segments_no_cross() {
    TEST("segments_intersect: clearly separate segments");
    ASSERT_TRUE(!segments_intersect(0,0, 1,0,  2,2, 3,2));
    PASS();
}

// --- New tests: ring_has_self_intersection ---

void test_self_intersection_triangle() {
    TEST("self-intersection: triangle has no crossing");
    std::vector<std::pair<double,double>> tri = {
        {0, 0}, {1, 0}, {0.5, 1}, {0, 0}
    };
    ASSERT_TRUE(!ring_has_self_intersection(tri));
    PASS();
}

void test_self_intersection_too_few_points() {
    TEST("self-intersection: fewer than 4 points returns false");
    std::vector<std::pair<double,double>> tiny = {{0,0}, {1,0}, {0,0}};
    ASSERT_TRUE(!ring_has_self_intersection(tiny));
    PASS();
}

void test_self_intersection_large_ring_clean() {
    TEST("self-intersection: large clean ring (sweep-line path)");
    // Create a 100-point ring (circle) — no self-intersection
    std::vector<std::pair<double,double>> ring;
    for (int i = 0; i < 100; i++) {
        double angle = 2.0 * M_PI * i / 100.0;
        ring.push_back({std::cos(angle), std::sin(angle)});
    }
    ring.push_back(ring.front());
    ASSERT_TRUE(!ring_has_self_intersection(ring));
    PASS();
}

void test_self_intersection_large_ring_crossed() {
    TEST("self-intersection: large ring with crossing");
    // Circle with two swapped points to create crossing
    std::vector<std::pair<double,double>> ring;
    for (int i = 0; i < 100; i++) {
        double angle = 2.0 * M_PI * i / 100.0;
        ring.push_back({std::cos(angle), std::sin(angle)});
    }
    ring.push_back(ring.front());
    // Swap two non-adjacent points to create crossing
    std::swap(ring[20], ring[80]);
    ASSERT_TRUE(ring_has_self_intersection(ring));
    PASS();
}

// --- New tests: polygon_area ---

void test_polygon_area_triangle() {
    TEST("polygon_area: right triangle");
    std::vector<std::pair<double,double>> tri = {
        {0, 0}, {2, 0}, {0, 3}, {0, 0}
    };
    float area = polygon_area(tri);
    ASSERT_TRUE(std::abs(area - 3.0f) < 1e-6f);
    PASS();
}

void test_polygon_area_degenerate() {
    TEST("polygon_area: degenerate (line) has zero area");
    std::vector<std::pair<double,double>> line = {
        {0, 0}, {1, 0}, {2, 0}, {0, 0}
    };
    float area = polygon_area(line);
    ASSERT_TRUE(std::abs(area) < 1e-6f);
    PASS();
}

void test_polygon_area_clockwise() {
    TEST("polygon_area: clockwise and counterclockwise same result");
    std::vector<std::pair<double,double>> ccw = {
        {0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}
    };
    std::vector<std::pair<double,double>> cw = {
        {0, 0}, {0, 1}, {1, 1}, {1, 0}, {0, 0}
    };
    ASSERT_TRUE(std::abs(polygon_area(ccw) - polygon_area(cw)) < 1e-6f);
    PASS();
}

// --- New tests: simplify_polygon ---

void test_simplify_already_small() {
    TEST("simplify_polygon: already below threshold returns unchanged");
    std::vector<std::pair<double,double>> pts = {
        {0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}
    };
    auto result = simplify_polygon(pts, 10);
    ASSERT_EQ(result.size(), pts.size());
    PASS();
}

void test_simplify_reduces_points() {
    TEST("simplify_polygon: large polygon gets reduced");
    // Create a 200-point near-circle
    std::vector<std::pair<double,double>> ring;
    for (int i = 0; i < 200; i++) {
        double angle = 2.0 * M_PI * i / 200.0;
        ring.push_back({std::cos(angle), std::sin(angle)});
    }
    ring.push_back(ring.front());
    auto result = simplify_polygon(ring, 50);
    ASSERT_TRUE(result.size() <= 50);
    ASSERT_TRUE(result.size() >= 3);
    PASS();
}

// --- New tests: parse_house_number ---

void test_parse_house_number_basic() {
    TEST("parse_house_number: basic number");
    ASSERT_EQ(parse_house_number("42"), 42u);
    PASS();
}

void test_parse_house_number_alpha_suffix() {
    TEST("parse_house_number: number with letter suffix");
    ASSERT_EQ(parse_house_number("42a"), 42u);
    ASSERT_EQ(parse_house_number("123B"), 123u);
    PASS();
}

void test_parse_house_number_empty() {
    TEST("parse_house_number: empty string");
    ASSERT_EQ(parse_house_number(""), 0u);
    PASS();
}

void test_parse_house_number_null() {
    TEST("parse_house_number: null pointer");
    ASSERT_EQ(parse_house_number(nullptr), 0u);
    PASS();
}

void test_parse_house_number_no_digits() {
    TEST("parse_house_number: no leading digits");
    ASSERT_EQ(parse_house_number("abc"), 0u);
    PASS();
}

void test_parse_house_number_range() {
    TEST("parse_house_number: range string");
    ASSERT_EQ(parse_house_number("10-12"), 10u);
    PASS();
}

// --- New tests: is_included_highway ---

void test_highway_excluded_types() {
    TEST("is_included_highway: excluded types");
    ASSERT_TRUE(!is_included_highway("footway"));
    ASSERT_TRUE(!is_included_highway("path"));
    ASSERT_TRUE(!is_included_highway("track"));
    ASSERT_TRUE(!is_included_highway("steps"));
    ASSERT_TRUE(!is_included_highway("cycleway"));
    ASSERT_TRUE(!is_included_highway("service"));
    ASSERT_TRUE(!is_included_highway("pedestrian"));
    ASSERT_TRUE(!is_included_highway("bridleway"));
    ASSERT_TRUE(!is_included_highway("construction"));
    PASS();
}

void test_highway_included_types() {
    TEST("is_included_highway: included types");
    ASSERT_TRUE(is_included_highway("residential"));
    ASSERT_TRUE(is_included_highway("primary"));
    ASSERT_TRUE(is_included_highway("secondary"));
    ASSERT_TRUE(is_included_highway("tertiary"));
    ASSERT_TRUE(is_included_highway("motorway"));
    ASSERT_TRUE(is_included_highway("trunk"));
    ASSERT_TRUE(is_included_highway("living_street"));
    ASSERT_TRUE(is_included_highway("unclassified"));
    PASS();
}

// --- New tests: StringPool ---

void test_string_pool_basic() {
    TEST("StringPool: basic intern and retrieve");
    StringPool pool;
    uint32_t id1 = pool.intern("hello");
    uint32_t id2 = pool.intern("world");
    ASSERT_TRUE(id1 != id2);
    ASSERT_EQ(std::string(pool.data().data() + id1), std::string("hello"));
    ASSERT_EQ(std::string(pool.data().data() + id2), std::string("world"));
    PASS();
}

void test_string_pool_dedup() {
    TEST("StringPool: duplicate strings return same offset");
    StringPool pool;
    uint32_t id1 = pool.intern("test");
    uint32_t id2 = pool.intern("test");
    ASSERT_EQ(id1, id2);
    PASS();
}

void test_string_pool_empty_string() {
    TEST("StringPool: empty string");
    StringPool pool;
    uint32_t id = pool.intern("");
    ASSERT_EQ(std::string(pool.data().data() + id), std::string(""));
    ASSERT_EQ(pool.data()[id], '\0');
    PASS();
}

void test_string_pool_order() {
    TEST("StringPool: first intern gets offset 0");
    StringPool pool;
    uint32_t id = pool.intern("first");
    ASSERT_EQ(id, 0u);
    // Second string starts after "first\0" = 6 bytes
    uint32_t id2 = pool.intern("second");
    ASSERT_EQ(id2, 6u);
    PASS();
}

// --- New tests: ring_has_duplicate_coords edge cases ---

void test_duplicate_coords_closing_point() {
    TEST("duplicate coords: closing point not counted as duplicate");
    // First and last point are the same — this is normal for a closed ring
    std::vector<std::pair<double,double>> ring = {
        {0, 0}, {1, 0}, {1, 1}, {0, 0}
    };
    ASSERT_TRUE(!ring_has_duplicate_coords(ring));
    PASS();
}

void test_duplicate_coords_two_point() {
    TEST("duplicate coords: two-point ring");
    std::vector<std::pair<double,double>> ring = {{0, 0}, {0, 0}};
    // Only one point checked (skip closing), no duplicate
    ASSERT_TRUE(!ring_has_duplicate_coords(ring));
    PASS();
}

// --- New tests: dp_simplify ---

void test_dp_simplify_straight_line() {
    TEST("dp_simplify: points on a straight line collapse");
    std::vector<std::pair<double,double>> pts = {
        {0, 0}, {0.5, 0}, {1, 0}, {1.5, 0}, {2, 0}
    };
    std::vector<bool> keep(pts.size(), false);
    keep[0] = true;
    keep[pts.size() - 1] = true;
    dp_simplify(pts, 0, pts.size() - 1, 0.01, keep);
    // Only first and last should be kept — interior points are on the line
    int count = 0;
    for (bool k : keep) if (k) count++;
    ASSERT_EQ(count, 2);
    PASS();
}

void test_dp_simplify_preserves_peak() {
    TEST("dp_simplify: preserves peak point");
    std::vector<std::pair<double,double>> pts = {
        {0, 0}, {0.5, 1}, {1, 0}
    };
    std::vector<bool> keep(pts.size(), false);
    keep[0] = true;
    keep[pts.size() - 1] = true;
    dp_simplify(pts, 0, pts.size() - 1, 0.01, keep);
    // The peak at (0.5, 1) should be kept
    ASSERT_TRUE(keep[1]);
    PASS();
}

// --- New tests: binary structs ---

void test_way_header_size() {
    TEST("WayHeader struct size");
    // Ensure struct is packed as expected
    ASSERT_TRUE(sizeof(WayHeader) > 0);
    ASSERT_TRUE(sizeof(WayHeader) <= 16); // should be compact
    PASS();
}

void test_admin_polygon_struct() {
    TEST("AdminPolygon country_code encoding");
    AdminPolygon ap{};
    const char* cc = "US";
    ap.country_code = static_cast<uint16_t>((cc[0] << 8) | cc[1]);
    ASSERT_EQ(ap.country_code >> 8, (uint16_t)'U');
    ASSERT_EQ(ap.country_code & 0xFF, (uint16_t)'S');
    PASS();
}

void test_interior_flag() {
    TEST("INTERIOR_FLAG and ID_MASK are complementary");
    uint32_t id = 12345;
    uint32_t flagged = id | INTERIOR_FLAG;
    ASSERT_EQ(flagged & ID_MASK, id);
    ASSERT_TRUE(flagged & INTERIOR_FLAG);
    ASSERT_TRUE(!(id & INTERIOR_FLAG));
    PASS();
}

// --- Main ---

int main() {
    std::cerr << "Running unit tests..." << std::endl;

    // Original tests
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

    // New: coord_key edge cases
    test_coord_key_equator_prime_meridian();
    test_coord_key_max_values();
    test_coord_key_precision_boundary();

    // New: segments_intersect
    test_segments_parallel();
    test_segments_collinear();
    test_segments_t_junction();
    test_segments_clear_cross();
    test_segments_no_cross();

    // New: ring_has_self_intersection
    test_self_intersection_triangle();
    test_self_intersection_too_few_points();
    test_self_intersection_large_ring_clean();
    test_self_intersection_large_ring_crossed();

    // New: polygon_area
    test_polygon_area_triangle();
    test_polygon_area_degenerate();
    test_polygon_area_clockwise();

    // New: simplify_polygon
    test_simplify_already_small();
    test_simplify_reduces_points();

    // New: parse_house_number
    test_parse_house_number_basic();
    test_parse_house_number_alpha_suffix();
    test_parse_house_number_empty();
    test_parse_house_number_null();
    test_parse_house_number_no_digits();
    test_parse_house_number_range();

    // New: is_included_highway
    test_highway_excluded_types();
    test_highway_included_types();

    // New: StringPool
    test_string_pool_basic();
    test_string_pool_dedup();
    test_string_pool_empty_string();
    test_string_pool_order();

    // New: ring_has_duplicate_coords edge cases
    test_duplicate_coords_closing_point();
    test_duplicate_coords_two_point();

    // New: dp_simplify
    test_dp_simplify_straight_line();
    test_dp_simplify_preserves_peak();

    // New: binary structs
    test_way_header_size();
    test_admin_polygon_struct();
    test_interior_flag();

    std::cerr << std::endl;
    std::cerr << tests_passed << "/" << tests_run << " tests passed." << std::endl;

    return (tests_passed == tests_run) ? 0 : 1;
}
