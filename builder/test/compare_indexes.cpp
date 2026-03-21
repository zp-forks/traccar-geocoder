// Semantic comparison of two geocoder index directories.
// Decodes all binary files, creates canonical fingerprints for each item,
// and compares as multisets — handles any ordering differences between
// parallel and sequential builds.
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
#include <unordered_map>
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

// Round a float coordinate to an integer grid (1e5 ~ 1m precision)
static int to_grid(float v) {
    return (int)(v * 1e5f + (v >= 0 ? 0.5f : -0.5f));
}

// --- Canonical fingerprint builders ---
// Each function produces a string that uniquely identifies an item,
// independent of storage order.

// For a way: "name|sorted_node_coords"
// Nodes are kept in original order (path order matters for ways),
// but we also provide a sorted-set version for order-independent matching.
static std::string way_fingerprint(const std::string& name,
                                    const std::vector<std::pair<float,float>>& nodes) {
    // Use sorted coordinate set as the canonical form.
    // Two ways are "the same" if they have the same name and same set of nodes.
    std::vector<std::pair<int,int>> grid_nodes;
    grid_nodes.reserve(nodes.size());
    for (const auto& n : nodes) {
        grid_nodes.push_back({to_grid(n.first), to_grid(n.second)});
    }
    std::sort(grid_nodes.begin(), grid_nodes.end());

    std::string fp = name + "|" + std::to_string(nodes.size());
    for (const auto& g : grid_nodes) {
        fp += "|";
        fp += std::to_string(g.first);
        fp += ",";
        fp += std::to_string(g.second);
    }
    return fp;
}

// For an address: "grid_lat,grid_lng|housenumber|street"
static std::string addr_fingerprint(float lat, float lng,
                                     const std::string& housenumber,
                                     const std::string& street) {
    return std::to_string(to_grid(lat)) + "," + std::to_string(to_grid(lng)) +
           "|" + housenumber + "|" + street;
}

// For an admin polygon: "name|level|country|sorted_vertices"
static std::string admin_fingerprint(const std::string& name,
                                      uint8_t admin_level,
                                      uint16_t country_code,
                                      const std::vector<std::pair<float,float>>& vertices) {
    std::vector<std::pair<int,int>> grid_verts;
    grid_verts.reserve(vertices.size());
    for (const auto& v : vertices) {
        grid_verts.push_back({to_grid(v.first), to_grid(v.second)});
    }
    std::sort(grid_verts.begin(), grid_verts.end());

    std::string fp = name + "|" + std::to_string(admin_level) + "|" + std::to_string(country_code);
    for (const auto& g : grid_verts) {
        fp += "|";
        fp += std::to_string(g.first);
        fp += ",";
        fp += std::to_string(g.second);
    }
    return fp;
}

// For interpolation ways: "street|start|end|interp|sorted_nodes"
static std::string interp_fingerprint(const std::string& street,
                                       uint32_t start_number, uint32_t end_number,
                                       uint8_t interpolation,
                                       const std::vector<std::pair<float,float>>& nodes) {
    std::vector<std::pair<int,int>> grid_nodes;
    grid_nodes.reserve(nodes.size());
    for (const auto& n : nodes) {
        grid_nodes.push_back({to_grid(n.first), to_grid(n.second)});
    }
    std::sort(grid_nodes.begin(), grid_nodes.end());

    std::string fp = street + "|" + std::to_string(start_number) + "-" +
                     std::to_string(end_number) + "|" + std::to_string(interpolation);
    for (const auto& g : grid_nodes) {
        fp += "|";
        fp += std::to_string(g.first);
        fp += ",";
        fp += std::to_string(g.second);
    }
    return fp;
}

// --- Multiset comparison ---
// Compare two collections by their fingerprints. Reports missing/extra items.

static bool compare_multisets(const char* label,
                               const std::vector<std::string>& fps_a,
                               const std::vector<std::string>& fps_b) {
    // Build frequency maps
    std::unordered_map<std::string, int> freq_a, freq_b;
    for (const auto& fp : fps_a) freq_a[fp]++;
    for (const auto& fp : fps_b) freq_b[fp]++;

    // Find items in A but not in B (missing from B)
    size_t missing = 0;
    for (const auto& [fp, count] : freq_a) {
        int b_count = 0;
        auto it = freq_b.find(fp);
        if (it != freq_b.end()) b_count = it->second;
        if (count > b_count) missing += (count - b_count);
    }

    // Find items in B but not in A (extra in B)
    size_t extra = 0;
    for (const auto& [fp, count] : freq_b) {
        int a_count = 0;
        auto it = freq_a.find(fp);
        if (it != freq_a.end()) a_count = it->second;
        if (count > a_count) extra += (count - a_count);
    }

    size_t matched = fps_a.size() - missing;

    if (missing == 0 && extra == 0) {
        std::cout << "PASS: " << label << " (" << fps_a.size() << " items, all matched)" << std::endl;
        return true;
    } else {
        std::cerr << "FAIL: " << label << " — A=" << fps_a.size() << " B=" << fps_b.size()
                  << " matched=" << matched
                  << " only_in_A=" << missing
                  << " only_in_B=" << extra << std::endl;
        return false;
    }
}

// --- Decode + fingerprint functions ---

static std::vector<std::string> fingerprint_admin(const std::string& dir) {
    auto strings = read_file(dir + "/strings.bin");
    auto polygons = read_structs<AdminPolygon>(dir + "/admin_polygons.bin");
    auto vertices = read_structs<NodeCoord>(dir + "/admin_vertices.bin");

    std::vector<std::string> fps;
    fps.reserve(polygons.size());
    for (const auto& p : polygons) {
        std::string name = get_string(strings, p.name_id);
        std::vector<std::pair<float,float>> verts;
        for (uint16_t i = 0; i < p.vertex_count; i++) {
            uint32_t vi = p.vertex_offset + i;
            if (vi < vertices.size()) {
                verts.push_back({vertices[vi].lat, vertices[vi].lng});
            }
        }
        fps.push_back(admin_fingerprint(name, p.admin_level, p.country_code, verts));
    }
    return fps;
}

static std::vector<std::string> fingerprint_ways(const std::string& dir) {
    auto strings = read_file(dir + "/strings.bin");
    auto ways = read_structs<WayHeader>(dir + "/street_ways.bin");
    auto nodes = read_structs<NodeCoord>(dir + "/street_nodes.bin");

    std::vector<std::string> fps;
    fps.reserve(ways.size());
    for (const auto& w : ways) {
        std::string name = get_string(strings, w.name_id);
        std::vector<std::pair<float,float>> way_nodes;
        for (uint8_t i = 0; i < w.node_count; i++) {
            uint32_t ni = w.node_offset + i;
            if (ni < nodes.size()) {
                way_nodes.push_back({nodes[ni].lat, nodes[ni].lng});
            }
        }
        fps.push_back(way_fingerprint(name, way_nodes));
    }
    return fps;
}

static std::vector<std::string> fingerprint_addrs(const std::string& dir) {
    auto strings = read_file(dir + "/strings.bin");
    auto points = read_structs<AddrPoint>(dir + "/addr_points.bin");

    std::vector<std::string> fps;
    fps.reserve(points.size());
    for (const auto& p : points) {
        std::string hn = get_string(strings, p.housenumber_id);
        std::string st = get_string(strings, p.street_id);
        fps.push_back(addr_fingerprint(p.lat, p.lng, hn, st));
    }
    return fps;
}

static std::vector<std::string> fingerprint_interps(const std::string& dir) {
    auto strings = read_file(dir + "/strings.bin");
    auto interps = read_structs<InterpWay>(dir + "/interp_ways.bin");
    auto nodes = read_structs<NodeCoord>(dir + "/interp_nodes.bin");

    std::vector<std::string> fps;
    fps.reserve(interps.size());
    for (const auto& iw : interps) {
        std::string street = get_string(strings, iw.street_id);
        std::vector<std::pair<float,float>> way_nodes;
        for (uint8_t i = 0; i < iw.node_count; i++) {
            uint32_t ni = iw.node_offset + i;
            if (ni < nodes.size()) {
                way_nodes.push_back({nodes[ni].lat, nodes[ni].lng});
            }
        }
        fps.push_back(interp_fingerprint(street, iw.start_number, iw.end_number,
                                          iw.interpolation, way_nodes));
    }
    return fps;
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
        std::cout << "Loading admin polygons..." << std::flush;
        auto fps_a = fingerprint_admin(dir_a);
        auto fps_b = fingerprint_admin(dir_b);
        std::cout << " done." << std::endl;
        if (!compare_multisets("admin_polygons", fps_a, fps_b))
            all_pass = false;
    }

    // Compare street ways
    {
        std::cout << "Loading street ways..." << std::flush;
        auto fps_a = fingerprint_ways(dir_a);
        auto fps_b = fingerprint_ways(dir_b);
        std::cout << " done." << std::endl;
        if (!compare_multisets("street_ways", fps_a, fps_b))
            all_pass = false;
    }

    // Compare address points
    {
        std::cout << "Loading address points..." << std::flush;
        auto fps_a = fingerprint_addrs(dir_a);
        auto fps_b = fingerprint_addrs(dir_b);
        std::cout << " done." << std::endl;
        if (!compare_multisets("address_points", fps_a, fps_b))
            all_pass = false;
    }

    // Compare interpolation ways
    {
        std::cout << "Loading interpolation ways..." << std::flush;
        auto fps_a = fingerprint_interps(dir_a);
        auto fps_b = fingerprint_interps(dir_b);
        std::cout << " done." << std::endl;
        if (!compare_multisets("interp_ways", fps_a, fps_b))
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
