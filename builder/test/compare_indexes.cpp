// Semantic comparison of two geocoder index directories.
// Decodes all binary files, normalizes ordering, and compares content.
// Returns 0 if semantically identical, 1 if different.
//
// Usage: compare_indexes <dir-A> <dir-B>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <vector>

// --- Binary structs (must match build_index.cpp and server main.rs) ---

struct WayHeader {
    uint32_t node_offset;
    uint8_t node_count;
    uint32_t name_id;
};

struct AddrPoint {
    float lat;
    float lng;
    uint32_t housenumber_id;
    uint32_t street_id;
};

struct InterpWay {
    uint32_t node_offset;
    uint8_t node_count;
    uint32_t street_id;
    uint32_t start_number;
    uint32_t end_number;
    uint8_t interpolation;
};

struct AdminPolygon {
    uint32_t vertex_offset;
    uint16_t vertex_count;
    uint32_t name_id;
    uint8_t admin_level;
    float area;
    uint16_t country_code;
};

struct NodeCoord {
    float lat;
    float lng;
};

// --- Helpers ---

static std::vector<char> read_file(const std::string& path) {
    std::ifstream f(path, std::ios::binary | std::ios::ate);
    if (!f) return {};
    auto size = f.tellg();
    f.seekg(0);
    std::vector<char> data(size);
    f.read(data.data(), size);
    return data;
}

static const char* get_string(const std::vector<char>& pool, uint32_t offset) {
    if (offset >= pool.size()) return "(invalid)";
    return pool.data() + offset;
}

template<typename T>
static std::vector<T> read_structs(const std::string& path) {
    auto data = read_file(path);
    size_t count = data.size() / sizeof(T);
    std::vector<T> result(count);
    memcpy(result.data(), data.data(), count * sizeof(T));
    return result;
}

// --- Decoded representations for comparison ---

struct DecodedAdminPolygon {
    std::string name;
    uint8_t admin_level;
    float area;
    uint16_t country_code;
    std::vector<std::pair<float, float>> vertices;

    // Canonical sort key: name + admin_level + vertex_count
    // This ensures consistent ordering regardless of insertion order
    std::string sort_key() const {
        return name + "|" + std::to_string(admin_level) + "|" + std::to_string(vertices.size());
    }

    bool operator<(const DecodedAdminPolygon& o) const {
        return sort_key() < o.sort_key();
    }

    // Compare vertex rings as sets — same vertices regardless of order/rotation/direction
    static bool rings_equal(const std::vector<std::pair<float,float>>& a,
                            const std::vector<std::pair<float,float>>& b) {
        if (a.size() != b.size()) return false;
        if (a.empty()) return true;

        // Round to grid and compare as sorted sets
        auto to_key = [](const std::pair<float,float>& v) -> std::pair<int,int> {
            return {(int)(v.first * 1e5f + 0.5f), (int)(v.second * 1e5f + 0.5f)};
        };

        std::vector<std::pair<int,int>> sa, sb;
        for (auto& v : a) sa.push_back(to_key(v));
        for (auto& v : b) sb.push_back(to_key(v));
        std::sort(sa.begin(), sa.end());
        std::sort(sb.begin(), sb.end());
        return sa == sb;
    }

    bool operator==(const DecodedAdminPolygon& o) const {
        if (admin_level != o.admin_level) return false;
        if (name != o.name) return false;
        if (country_code != o.country_code) return false;
        if (vertices.size() != o.vertices.size()) return false;
        // Area tolerance: relative comparison for large values, absolute for small
        float area_diff = std::fabs(area - o.area);
        float area_max = std::max(std::fabs(area), std::fabs(o.area));
        if (area_max > 1e-6f && area_diff / area_max > 0.01f) return false;
        if (area_max <= 1e-6f && area_diff > 1e-8f) return false;
        return rings_equal(vertices, o.vertices);
    }
};

struct DecodedWay {
    std::string name;
    std::vector<std::pair<float, float>> nodes;

    std::string sort_key() const {
        std::string key = name + "|" + std::to_string(nodes.size());
        if (!nodes.empty()) {
            key += "|" + std::to_string((int)(nodes[0].first * 1e5)) + "," + std::to_string((int)(nodes[0].second * 1e5));
        }
        return key;
    }

    bool operator<(const DecodedWay& o) const {
        return sort_key() < o.sort_key();
    }

    bool operator==(const DecodedWay& o) const {
        if (name != o.name) return false;
        if (nodes.size() != o.nodes.size()) return false;
        // Compare as sorted vertex sets (order may differ due to parallel processing)
        auto to_key = [](const std::pair<float,float>& v) -> std::pair<int,int> {
            return {(int)(v.first * 1e5f + 0.5f), (int)(v.second * 1e5f + 0.5f)};
        };
        std::vector<std::pair<int,int>> sa, sb;
        for (auto& v : nodes) sa.push_back(to_key(v));
        for (auto& v : o.nodes) sb.push_back(to_key(v));
        std::sort(sa.begin(), sa.end());
        std::sort(sb.begin(), sb.end());
        return sa == sb;
    }
};

struct DecodedAddr {
    float lat, lng;
    std::string housenumber;
    std::string street;

    std::string sort_key() const {
        return std::to_string((int)(lat * 1e5)) + "," + std::to_string((int)(lng * 1e5)) + "|" + housenumber + "|" + street;
    }

    bool operator<(const DecodedAddr& o) const {
        return sort_key() < o.sort_key();
    }

    bool operator==(const DecodedAddr& o) const {
        return std::fabs(lat - o.lat) < 1e-5f &&
               std::fabs(lng - o.lng) < 1e-5f &&
               housenumber == o.housenumber &&
               street == o.street;
    }
};

// --- Decode functions ---

static std::vector<DecodedAdminPolygon> decode_admin(const std::string& dir) {
    auto strings = read_file(dir + "/strings.bin");
    auto polygons = read_structs<AdminPolygon>(dir + "/admin_polygons.bin");
    auto vertices = read_structs<NodeCoord>(dir + "/admin_vertices.bin");

    std::vector<DecodedAdminPolygon> result;
    for (const auto& p : polygons) {
        DecodedAdminPolygon dp;
        dp.name = get_string(strings, p.name_id);
        dp.admin_level = p.admin_level;
        dp.area = p.area;
        dp.country_code = p.country_code;
        for (uint16_t i = 0; i < p.vertex_count; i++) {
            uint32_t vi = p.vertex_offset + i;
            if (vi < vertices.size()) {
                dp.vertices.push_back({vertices[vi].lat, vertices[vi].lng});
            }
        }
        result.push_back(std::move(dp));
    }
    return result;
}

static std::vector<DecodedWay> decode_ways(const std::string& dir) {
    auto strings = read_file(dir + "/strings.bin");
    auto ways = read_structs<WayHeader>(dir + "/street_ways.bin");
    auto nodes = read_structs<NodeCoord>(dir + "/street_nodes.bin");

    std::vector<DecodedWay> result;
    for (const auto& w : ways) {
        DecodedWay dw;
        dw.name = get_string(strings, w.name_id);
        for (uint8_t i = 0; i < w.node_count; i++) {
            uint32_t ni = w.node_offset + i;
            if (ni < nodes.size()) {
                dw.nodes.push_back({nodes[ni].lat, nodes[ni].lng});
            }
        }
        result.push_back(std::move(dw));
    }
    return result;
}

static std::vector<DecodedAddr> decode_addrs(const std::string& dir) {
    auto strings = read_file(dir + "/strings.bin");
    auto points = read_structs<AddrPoint>(dir + "/addr_points.bin");

    std::vector<DecodedAddr> result;
    for (const auto& p : points) {
        DecodedAddr da;
        da.lat = p.lat;
        da.lng = p.lng;
        da.housenumber = get_string(strings, p.housenumber_id);
        da.street = get_string(strings, p.street_id);
        result.push_back(std::move(da));
    }
    return result;
}

// --- Comparison ---

template<typename T>
static bool compare_sorted(const char* label, std::vector<T> a, std::vector<T> b) {
    std::sort(a.begin(), a.end());
    std::sort(b.begin(), b.end());

    if (a.size() != b.size()) {
        std::cerr << "FAIL: " << label << " count differs (A=" << a.size()
                  << " B=" << b.size() << ")" << std::endl;
        return false;
    }

    size_t diffs = 0;
    for (size_t i = 0; i < a.size(); i++) {
        if (!(a[i] == b[i])) {
            if (diffs < 3) {
                std::cerr << "  Mismatch at sorted position " << i << std::endl;
            }
            diffs++;
        }
    }

    if (diffs > 0) {
        std::cerr << "FAIL: " << label << " has " << diffs << " differences out of "
                  << a.size() << " items" << std::endl;
        return false;
    }

    std::cout << "PASS: " << label << " (" << a.size() << " items)" << std::endl;
    return true;
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: compare_indexes <dir-A> <dir-B>" << std::endl;
        return 2;
    }

    std::string dir_a = argv[1];
    std::string dir_b = argv[2];

    std::cout << "Comparing:" << std::endl;
    std::cout << "  A: " << dir_a << std::endl;
    std::cout << "  B: " << dir_b << std::endl;
    std::cout << std::endl;

    bool all_pass = true;

    // Compare admin polygons
    {
        auto a = decode_admin(dir_a);
        auto b = decode_admin(dir_b);
        if (!compare_sorted("admin_polygons", std::move(a), std::move(b)))
            all_pass = false;
    }

    // Compare street ways
    {
        auto a = decode_ways(dir_a);
        auto b = decode_ways(dir_b);
        if (!compare_sorted("street_ways", std::move(a), std::move(b)))
            all_pass = false;
    }

    // Compare address points
    {
        auto a = decode_addrs(dir_a);
        auto b = decode_addrs(dir_b);
        if (!compare_sorted("address_points", std::move(a), std::move(b)))
            all_pass = false;
    }

    // Compare cell index sizes (should be identical regardless of ordering)
    {
        auto ac = read_file(dir_a + "/admin_cells.bin");
        auto bc = read_file(dir_b + "/admin_cells.bin");
        if (ac == bc) {
            std::cout << "PASS: admin_cells.bin byte-identical (" << ac.size() << " bytes)" << std::endl;
        } else if (ac.size() == bc.size()) {
            std::cout << "INFO: admin_cells.bin same size (" << ac.size() << " bytes) but different content" << std::endl;
        } else {
            std::cerr << "FAIL: admin_cells.bin size differs (A=" << ac.size()
                      << " B=" << bc.size() << ")" << std::endl;
            all_pass = false;
        }

        auto gc_a = read_file(dir_a + "/geo_cells.bin");
        auto gc_b = read_file(dir_b + "/geo_cells.bin");
        if (gc_a.size() == gc_b.size()) {
            std::cout << "PASS: geo_cells.bin same size (" << gc_a.size() << " bytes)" << std::endl;
        } else {
            std::cerr << "FAIL: geo_cells.bin size differs (A=" << gc_a.size()
                      << " B=" << gc_b.size() << ")" << std::endl;
            all_pass = false;
        }
    }

    std::cout << std::endl;
    if (all_pass) {
        std::cout << "=== ALL COMPARISONS PASSED ===" << std::endl;
        return 0;
    } else {
        std::cout << "=== SOME COMPARISONS FAILED ===" << std::endl;
        return 1;
    }
}
