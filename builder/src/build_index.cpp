#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <osmium/handler.hpp>
#include <osmium/io/pbf_input.hpp>
#include <osmium/visitor.hpp>
#include <osmium/handler/node_locations_for_ways.hpp>
#include <fcntl.h>
#include <unistd.h>
#include <osmium/index/map/sparse_file_array.hpp>
#include <osmium/area/assembler.hpp>
#include <osmium/area/multipolygon_manager.hpp>

#include <s2/s2cell_id.h>
#include <s2/s2latlng.h>
#include <s2/s2region_coverer.h>
#include <s2/s2polyline.h>
#include <s2/s2polygon.h>
#include <s2/s2loop.h>
#include <s2/s2builder.h>

// --- Binary format structs ---

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

static const uint32_t INTERIOR_FLAG = 0x80000000u;
static const uint32_t ID_MASK = 0x7FFFFFFFu;

// --- String interning ---

class StringPool {
public:
    uint32_t intern(const std::string& s) {
        auto it = index_.find(s);
        if (it != index_.end()) {
            return it->second;
        }
        uint32_t offset = static_cast<uint32_t>(data_.size());
        index_[s] = offset;
        data_.insert(data_.end(), s.begin(), s.end());
        data_.push_back('\0');
        return offset;
    }

    const std::vector<char>& data() const { return data_; }

private:
    std::unordered_map<std::string, uint32_t> index_;
    std::vector<char> data_;
};

// --- Collected data ---

static StringPool string_pool;

// Streets
static std::vector<WayHeader> ways;
static std::vector<NodeCoord> street_nodes;
static std::unordered_map<uint64_t, std::vector<uint32_t>> cell_to_ways;

// Addresses
static std::vector<AddrPoint> addr_points;
static std::unordered_map<uint64_t, std::vector<uint32_t>> cell_to_addrs;

// Interpolation
static std::vector<InterpWay> interp_ways;
static std::vector<NodeCoord> interp_nodes;
static std::unordered_map<uint64_t, std::vector<uint32_t>> cell_to_interps;

// Admin boundaries
static std::vector<AdminPolygon> admin_polygons;
static std::vector<NodeCoord> admin_vertices;
static std::unordered_map<uint64_t, std::vector<uint32_t>> cell_to_admin;

// --- S2 helpers ---

static int kStreetCellLevel = 17;
static int kAdminCellLevel = 10;
static bool kAdminOnly = false;
static int kMaxAdminLevel = 0;  // 0 means no filtering
static bool kNoAddresses = false;

static std::vector<S2CellId> cover_edge(double lat1, double lng1, double lat2, double lng2) {
    S2Point p1 = S2LatLng::FromDegrees(lat1, lng1).ToPoint();
    S2Point p2 = S2LatLng::FromDegrees(lat2, lng2).ToPoint();

    if (p1 == p2) {
        return {S2CellId(p1).parent(kStreetCellLevel)};
    }

    std::vector<S2Point> points = {p1, p2};
    S2Polyline polyline(points);

    S2RegionCoverer::Options options;
    options.set_fixed_level(kStreetCellLevel);

    S2RegionCoverer coverer(options);
    S2CellUnion covering = coverer.GetCovering(polyline);
    return covering.cell_ids();
}

static S2CellId point_to_cell(double lat, double lng) {
    return S2CellId(S2LatLng::FromDegrees(lat, lng)).parent(kStreetCellLevel);
}

// Returns pairs of (cell_id, is_interior)
static std::vector<std::pair<S2CellId, bool>> cover_polygon(const std::vector<std::pair<double,double>>& vertices) {
    std::vector<S2Point> points;
    points.reserve(vertices.size());
    for (const auto& [lat, lng] : vertices) {
        S2Point p = S2LatLng::FromDegrees(lat, lng).ToPoint();
        if (!points.empty() && points.back() == p) continue;
        points.push_back(p);
    }
    // Remove closing duplicate if first == last
    if (points.size() > 1 && points.front() == points.back()) {
        points.pop_back();
    }
    if (points.size() < 3) return {};

    // Build S2Loop (must be CCW), skip invalid polygons
    S2Error error;
    auto loop = std::make_unique<S2Loop>(points, S2Debug::DISABLE);
    loop->Normalize();
    if (loop->FindValidationError(&error)) return {};

    S2Polygon polygon(std::move(loop));

    S2RegionCoverer::Options options;
    options.set_max_level(kAdminCellLevel);
    options.set_max_cells(200);

    S2RegionCoverer coverer(options);
    S2CellUnion covering = coverer.GetCovering(polygon);
    S2CellUnion interior = coverer.GetInteriorCovering(polygon);

    // Build set of interior cell IDs for fast lookup
    std::unordered_set<uint64_t> interior_set;
    for (const auto& cell : interior.cell_ids()) {
        if (cell.level() <= kAdminCellLevel) {
            auto begin = cell.range_min().parent(kAdminCellLevel);
            auto end = cell.range_max().parent(kAdminCellLevel);
            for (auto c = begin; c != end; c = c.next()) {
                interior_set.insert(c.id());
            }
            interior_set.insert(end.id());
        } else {
            interior_set.insert(cell.parent(kAdminCellLevel).id());
        }
    }

    // Normalize all covering cells to kAdminCellLevel
    std::vector<std::pair<S2CellId, bool>> result;
    for (const auto& cell : covering.cell_ids()) {
        if (cell.level() <= kAdminCellLevel) {
            auto begin = cell.range_min().parent(kAdminCellLevel);
            auto end = cell.range_max().parent(kAdminCellLevel);
            for (auto c = begin; c != end; c = c.next()) {
                result.emplace_back(c, interior_set.count(c.id()) > 0);
            }
            result.emplace_back(end, interior_set.count(end.id()) > 0);
        } else {
            auto parent = cell.parent(kAdminCellLevel);
            result.emplace_back(parent, interior_set.count(parent.id()) > 0);
        }
    }

    // Deduplicate by cell_id, keeping interior=true if any duplicate is interior
    std::sort(result.begin(), result.end(), [](const auto& a, const auto& b) {
        return a.first < b.first;
    });
    auto it = result.begin();
    for (auto curr = result.begin(); curr != result.end(); ) {
        auto next = curr + 1;
        bool is_interior = curr->second;
        while (next != result.end() && next->first == curr->first) {
            is_interior = is_interior || next->second;
            ++next;
        }
        *it = {curr->first, is_interior};
        ++it;
        curr = next;
    }
    result.erase(it, result.end());
    return result;
}

// Approximate polygon area in square degrees
static float polygon_area(const std::vector<std::pair<double,double>>& vertices) {
    double area = 0;
    size_t n = vertices.size();
    for (size_t i = 0; i < n; i++) {
        size_t j = (i + 1) % n;
        area += vertices[i].first * vertices[j].second;
        area -= vertices[j].first * vertices[i].second;
    }
    return static_cast<float>(std::fabs(area) / 2.0);
}

// Douglas-Peucker simplification
static void dp_simplify(const std::vector<std::pair<double,double>>& pts,
                        size_t start, size_t end, double epsilon,
                        std::vector<bool>& keep) {
    if (end <= start + 1) return;

    double max_dist = 0;
    size_t max_idx = start;

    double ax = pts[start].first, ay = pts[start].second;
    double bx = pts[end].first, by = pts[end].second;
    double dx = bx - ax, dy = by - ay;
    double len_sq = dx * dx + dy * dy;

    for (size_t i = start + 1; i < end; i++) {
        double px = pts[i].first - ax, py = pts[i].second - ay;
        double dist;
        if (len_sq == 0) {
            dist = std::sqrt(px * px + py * py);
        } else {
            double t = std::max(0.0, std::min(1.0, (px * dx + py * dy) / len_sq));
            double proj_x = t * dx - px, proj_y = t * dy - py;
            dist = std::sqrt(proj_x * proj_x + proj_y * proj_y);
        }
        if (dist > max_dist) {
            max_dist = dist;
            max_idx = i;
        }
    }

    if (max_dist > epsilon) {
        keep[max_idx] = true;
        dp_simplify(pts, start, max_idx, epsilon, keep);
        dp_simplify(pts, max_idx, end, epsilon, keep);
    }
}

static std::vector<std::pair<double,double>> simplify_polygon(
    const std::vector<std::pair<double,double>>& pts, size_t max_vertices) {
    if (pts.size() <= max_vertices) return pts;

    // Binary search for epsilon that gives ~max_vertices
    double lo = 0, hi = 1.0;
    std::vector<std::pair<double,double>> result;

    for (int iter = 0; iter < 20; iter++) {
        double epsilon = (lo + hi) / 2;
        std::vector<bool> keep(pts.size(), false);
        keep[0] = true;
        keep[pts.size() - 1] = true;
        dp_simplify(pts, 0, pts.size() - 1, epsilon, keep);

        size_t count = 0;
        for (bool k : keep) if (k) count++;

        if (count > max_vertices) {
            lo = epsilon;
        } else {
            hi = epsilon;
        }
    }

    std::vector<bool> keep(pts.size(), false);
    keep[0] = true;
    keep[pts.size() - 1] = true;
    dp_simplify(pts, 0, pts.size() - 1, hi, keep);

    result.clear();
    for (size_t i = 0; i < pts.size(); i++) {
        if (keep[i]) result.push_back(pts[i]);
    }
    return result;
}

// --- Highway filter ---

static const std::vector<std::string> kExcludedHighways = {
    "footway", "path", "track", "steps", "cycleway",
    "service", "pedestrian", "bridleway", "construction"
};

static bool is_included_highway(const char* value) {
    for (const auto& excluded : kExcludedHighways) {
        if (excluded == value) return false;
    }
    return true;
}

// --- Parse house number (leading digits) ---

static uint32_t parse_house_number(const char* s) {
    if (!s) return 0;
    uint32_t n = 0;
    while (*s >= '0' && *s <= '9') {
        n = n * 10 + (*s - '0');
        s++;
    }
    return n;
}

// --- Add an address point ---

static uint64_t addr_count_total = 0;

static void add_addr_point(double lat, double lng, const char* housenumber, const char* street) {
    uint32_t addr_id = static_cast<uint32_t>(addr_points.size());
    addr_points.push_back({
        static_cast<float>(lat),
        static_cast<float>(lng),
        string_pool.intern(housenumber),
        string_pool.intern(street)
    });

    S2CellId cell = point_to_cell(lat, lng);
    cell_to_addrs[cell.id()].push_back(addr_id);

    addr_count_total++;
    if (addr_count_total % 1000000 == 0) {
        std::cerr << "Collected " << addr_count_total / 1000000 << "M addresses..." << std::endl;
    }
}

// --- Add an admin polygon ---

static void add_admin_polygon(const std::vector<std::pair<double,double>>& vertices,
                               const char* name, uint8_t admin_level,
                               const char* country_code) {
    // Simplify large polygons
    auto simplified = simplify_polygon(vertices, 500);
    if (simplified.size() < 3) return;

    uint32_t poly_id = static_cast<uint32_t>(admin_polygons.size());
    uint32_t vertex_offset = static_cast<uint32_t>(admin_vertices.size());

    for (const auto& [lat, lng] : simplified) {
        admin_vertices.push_back({static_cast<float>(lat), static_cast<float>(lng)});
    }

    AdminPolygon poly{};
    poly.vertex_offset = vertex_offset;
    poly.vertex_count = static_cast<uint16_t>(std::min(simplified.size(), size_t(65535)));
    poly.name_id = string_pool.intern(name);
    poly.admin_level = admin_level;
    poly.area = polygon_area(simplified);
    poly.country_code = (country_code && country_code[0] && country_code[1])
        ? static_cast<uint16_t>((country_code[0] << 8) | country_code[1])
        : 0;
    admin_polygons.push_back(poly);

    // S2 cell coverage (high bit marks interior cells)
    auto cell_ids = cover_polygon(simplified);
    for (const auto& [cell_id, is_interior] : cell_ids) {
        uint32_t entry = is_interior ? (poly_id | INTERIOR_FLAG) : poly_id;
        cell_to_admin[cell_id.id()].push_back(entry);
    }
}

// --- OSM handler (pass 2) ---

class BuildHandler : public osmium::handler::Handler {
public:
    void node(const osmium::Node& node) {
        if (kAdminOnly || kNoAddresses) return;
        const char* housenumber = node.tags()["addr:housenumber"];
        if (!housenumber) return;
        const char* street = node.tags()["addr:street"];
        if (!street) return;
        if (!node.location().valid()) return;

        add_addr_point(node.location().lat(), node.location().lon(), housenumber, street);
    }

    void way(const osmium::Way& way) {
        if (kAdminOnly) return;

        if (!kNoAddresses) {
            // Address interpolation
            const char* interpolation = way.tags()["addr:interpolation"];
            if (interpolation) {
                process_interpolation_way(way, interpolation);
                return;
            }

            // Building addresses
            const char* housenumber = way.tags()["addr:housenumber"];
            if (housenumber) {
                const char* street = way.tags()["addr:street"];
                if (street) {
                    process_building_address(way, housenumber, street);
                }
            }
        }

        // Highway ways
        const char* highway = way.tags()["highway"];
        if (highway && is_included_highway(highway)) {
            const char* name = way.tags()["name"];
            if (name) {
                process_highway(way, name);
            }
        }
    }

    void area(const osmium::Area& area) {
        const char* boundary = area.tags()["boundary"];
        if (!boundary) return;

        bool is_admin = (std::strcmp(boundary, "administrative") == 0);
        bool is_postal = (std::strcmp(boundary, "postal_code") == 0);
        if (!is_admin && !is_postal) return;

        uint8_t admin_level = 0;
        if (is_admin) {
            const char* level_str = area.tags()["admin_level"];
            if (!level_str) return;
            admin_level = static_cast<uint8_t>(std::atoi(level_str));
            if (admin_level < 2 || admin_level > 10) return;
        } else {
            admin_level = 11; // use 11 for postal codes
        }
        if (kMaxAdminLevel > 0 && admin_level > kMaxAdminLevel) return;

        const char* name = area.tags()["name"];
        if (!name && is_admin) return;

        // For postal codes, use postal_code tag as name
        std::string name_str;
        if (is_postal) {
            const char* postal_code = area.tags()["postal_code"];
            if (!postal_code) postal_code = name;
            if (!postal_code) return;
            name_str = postal_code;
        } else {
            name_str = name;
        }

        // Extract country code for level 2 boundaries
        const char* country_code = (admin_level == 2)
            ? area.tags()["ISO3166-1:alpha2"]
            : nullptr;
        // Extract outer ring vertices
        for (const auto& outer_ring : area.outer_rings()) {
            std::vector<std::pair<double,double>> vertices;
            for (const auto& node_ref : outer_ring) {
                if (node_ref.location().valid()) {
                    vertices.emplace_back(node_ref.location().lat(), node_ref.location().lon());
                }
            }
            if (vertices.size() >= 3) {
                add_admin_polygon(vertices, name_str.c_str(), admin_level, country_code);
            }
        }

        admin_count_++;
        if (admin_count_ % 10000 == 0) {
            std::cerr << "Processed " << admin_count_ / 1000 << "K admin boundaries..." << std::endl;
        }
    }

    uint64_t way_count() const { return way_count_; }
    uint64_t building_addr_count() const { return building_addr_count_; }
    uint64_t interp_count() const { return interp_count_; }
    uint64_t admin_count() const { return admin_count_; }

private:
    uint64_t way_count_ = 0;
    uint64_t building_addr_count_ = 0;
    uint64_t interp_count_ = 0;
    uint64_t admin_count_ = 0;

    void process_building_address(const osmium::Way& way, const char* housenumber, const char* street) {
        const auto& wnodes = way.nodes();
        if (wnodes.empty()) return;

        double sum_lat = 0, sum_lng = 0;
        int valid = 0;
        for (const auto& nr : wnodes) {
            if (!nr.location().valid()) continue;
            sum_lat += nr.location().lat();
            sum_lng += nr.location().lon();
            valid++;
        }
        if (valid == 0) return;

        add_addr_point(sum_lat / valid, sum_lng / valid, housenumber, street);
        building_addr_count_++;
    }

    void process_interpolation_way(const osmium::Way& way, const char* interpolation) {
        const auto& wnodes = way.nodes();
        if (wnodes.size() < 2) return;

        for (const auto& nr : wnodes) {
            if (!nr.location().valid()) return;
        }

        const char* street = way.tags()["addr:street"];
        if (!street) return;

        uint8_t interp_type = 0;
        if (std::strcmp(interpolation, "even") == 0) interp_type = 1;
        else if (std::strcmp(interpolation, "odd") == 0) interp_type = 2;

        uint32_t interp_id = static_cast<uint32_t>(interp_ways.size());
        uint32_t node_offset = static_cast<uint32_t>(interp_nodes.size());

        for (const auto& nr : wnodes) {
            interp_nodes.push_back({
                static_cast<float>(nr.location().lat()),
                static_cast<float>(nr.location().lon())
            });
        }

        InterpWay iw{};
        iw.node_offset = node_offset;
        iw.node_count = static_cast<uint8_t>(std::min(wnodes.size(), size_t(255)));
        iw.street_id = string_pool.intern(street);
        iw.start_number = 0;
        iw.end_number = 0;
        iw.interpolation = interp_type;
        interp_ways.push_back(iw);

        std::unordered_set<uint64_t> interp_cells;
        for (size_t i = 0; i + 1 < wnodes.size(); i++) {
            double lat1 = wnodes[i].location().lat();
            double lng1 = wnodes[i].location().lon();
            double lat2 = wnodes[i + 1].location().lat();
            double lng2 = wnodes[i + 1].location().lon();

            auto cell_ids = cover_edge(lat1, lng1, lat2, lng2);
            for (const auto& cell_id : cell_ids) {
                interp_cells.insert(cell_id.id());
            }
        }
        for (uint64_t cell_id : interp_cells) {
            cell_to_interps[cell_id].push_back(interp_id);
        }

        interp_count_++;
    }

    void process_highway(const osmium::Way& way, const char* name) {
        const auto& wnodes = way.nodes();
        if (wnodes.size() < 2) return;

        for (const auto& nr : wnodes) {
            if (!nr.location().valid()) return;
        }

        uint32_t way_id = static_cast<uint32_t>(ways.size());
        uint32_t node_offset = static_cast<uint32_t>(street_nodes.size());

        for (const auto& nr : wnodes) {
            street_nodes.push_back({
                static_cast<float>(nr.location().lat()),
                static_cast<float>(nr.location().lon())
            });
        }

        WayHeader header{};
        header.node_offset = node_offset;
        header.node_count = static_cast<uint8_t>(std::min(wnodes.size(), size_t(255)));
        header.name_id = string_pool.intern(name);
        ways.push_back(header);

        std::unordered_set<uint64_t> way_cells;
        for (size_t i = 0; i + 1 < wnodes.size(); i++) {
            double lat1 = wnodes[i].location().lat();
            double lng1 = wnodes[i].location().lon();
            double lat2 = wnodes[i + 1].location().lat();
            double lng2 = wnodes[i + 1].location().lon();

            auto cell_ids = cover_edge(lat1, lng1, lat2, lng2);
            for (const auto& cell_id : cell_ids) {
                way_cells.insert(cell_id.id());
            }
        }
        for (uint64_t cell_id : way_cells) {
            cell_to_ways[cell_id].push_back(way_id);
        }

        way_count_++;
        if (way_count_ % 1000000 == 0) {
            std::cerr << "Processed " << way_count_ / 1000000 << "M street ways..." << std::endl;
        }
    }
};

// --- Resolve interpolation way endpoint house numbers ---

static void resolve_interpolation_endpoints() {
    struct CoordKey {
        int32_t lat;
        int32_t lng;
        bool operator==(const CoordKey& o) const { return lat == o.lat && lng == o.lng; }
    };
    struct CoordHash {
        size_t operator()(const CoordKey& k) const {
            return std::hash<int64_t>()(((int64_t)k.lat << 32) | (uint32_t)k.lng);
        }
    };

    std::unordered_map<CoordKey, uint32_t, CoordHash> addr_by_coord;
    for (uint32_t i = 0; i < addr_points.size(); i++) {
        CoordKey key{
            static_cast<int32_t>(addr_points[i].lat * 100000),
            static_cast<int32_t>(addr_points[i].lng * 100000)
        };
        addr_by_coord[key] = i;
    }

    uint32_t resolved = 0;
    for (auto& iw : interp_ways) {
        if (iw.node_count < 2) continue;

        const auto& start = interp_nodes[iw.node_offset];
        CoordKey start_key{
            static_cast<int32_t>(start.lat * 100000),
            static_cast<int32_t>(start.lng * 100000)
        };
        auto it_start = addr_by_coord.find(start_key);

        const auto& end = interp_nodes[iw.node_offset + iw.node_count - 1];
        CoordKey end_key{
            static_cast<int32_t>(end.lat * 100000),
            static_cast<int32_t>(end.lng * 100000)
        };
        auto it_end = addr_by_coord.find(end_key);

        if (it_start != addr_by_coord.end()) {
            const char* hn = string_pool.data().data() + addr_points[it_start->second].housenumber_id;
            iw.start_number = parse_house_number(hn);
        }
        if (it_end != addr_by_coord.end()) {
            const char* hn = string_pool.data().data() + addr_points[it_end->second].housenumber_id;
            iw.end_number = parse_house_number(hn);
        }

        if (iw.start_number > 0 && iw.end_number > 0) resolved++;
    }

    std::cerr << "Resolved " << resolved << "/" << interp_ways.size()
              << " interpolation ways" << std::endl;
}

// --- Deduplicate IDs per cell ---

template<typename Map>
static void deduplicate(Map& cell_map) {
    for (auto& [cell_id, ids] : cell_map) {
        std::sort(ids.begin(), ids.end());
        ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
    }
}

// --- Write cell index ---

static const uint32_t NO_DATA = 0xFFFFFFFFu;

// Write entries file and return offset map
static std::unordered_map<uint64_t, uint32_t> write_entries(
    const std::string& path,
    const std::vector<uint64_t>& sorted_cells,
    const std::unordered_map<uint64_t, std::vector<uint32_t>>& cell_map
) {
    std::unordered_map<uint64_t, uint32_t> offsets;
    std::ofstream f(path, std::ios::binary);
    uint32_t current = 0;
    for (uint64_t cell_id : sorted_cells) {
        auto it = cell_map.find(cell_id);
        if (it == cell_map.end()) continue;
        offsets[cell_id] = current;
        uint16_t count = static_cast<uint16_t>(std::min(it->second.size(), size_t(65535)));
        f.write(reinterpret_cast<const char*>(&count), sizeof(count));
        f.write(reinterpret_cast<const char*>(it->second.data()), it->second.size() * sizeof(uint32_t));
        current += sizeof(uint16_t) + it->second.size() * sizeof(uint32_t);
    }
    return offsets;
}

static void write_cell_index(
    const std::string& cells_path,
    const std::string& entries_path,
    const std::unordered_map<uint64_t, std::vector<uint32_t>>& cell_map
) {
    std::vector<std::pair<uint64_t, std::vector<uint32_t>>> sorted(
        cell_map.begin(), cell_map.end());
    std::sort(sorted.begin(), sorted.end());

    {
        std::ofstream f(cells_path, std::ios::binary);
        uint32_t current_offset = 0;
        for (const auto& [cell_id, ids] : sorted) {
            f.write(reinterpret_cast<const char*>(&cell_id), sizeof(cell_id));
            f.write(reinterpret_cast<const char*>(&current_offset), sizeof(current_offset));
            current_offset += sizeof(uint16_t) + ids.size() * sizeof(uint32_t);
        }
    }

    {
        std::ofstream f(entries_path, std::ios::binary);
        for (const auto& [cell_id, ids] : sorted) {
            uint16_t count = static_cast<uint16_t>(std::min(ids.size(), size_t(65535)));
            f.write(reinterpret_cast<const char*>(&count), sizeof(count));
            f.write(reinterpret_cast<const char*>(ids.data()), ids.size() * sizeof(uint32_t));
        }
    }
}

// --- Write all index files ---

static void write_index(const std::string& output_dir) {
    if (!kAdminOnly) {
        // Merged geo cell index for streets, addresses, and interpolation
        std::set<uint64_t> all_geo_cells;
        for (const auto& [id, _] : cell_to_ways) all_geo_cells.insert(id);
        if (!kNoAddresses) {
            for (const auto& [id, _] : cell_to_addrs) all_geo_cells.insert(id);
            for (const auto& [id, _] : cell_to_interps) all_geo_cells.insert(id);
        }
        std::vector<uint64_t> sorted_geo_cells(all_geo_cells.begin(), all_geo_cells.end());

        auto street_offsets = write_entries(output_dir + "/street_entries.bin", sorted_geo_cells, cell_to_ways);

        std::unordered_map<uint64_t, uint32_t> addr_offsets;
        std::unordered_map<uint64_t, uint32_t> interp_offsets;
        if (!kNoAddresses) {
            addr_offsets = write_entries(output_dir + "/addr_entries.bin", sorted_geo_cells, cell_to_addrs);
            interp_offsets = write_entries(output_dir + "/interp_entries.bin", sorted_geo_cells, cell_to_interps);
        }

        {
            std::ofstream f(output_dir + "/geo_cells.bin", std::ios::binary);
            for (uint64_t cell_id : sorted_geo_cells) {
                f.write(reinterpret_cast<const char*>(&cell_id), sizeof(cell_id));
                auto write_offset = [&](const std::unordered_map<uint64_t, uint32_t>& offsets) {
                    auto it = offsets.find(cell_id);
                    uint32_t offset = (it != offsets.end()) ? it->second : NO_DATA;
                    f.write(reinterpret_cast<const char*>(&offset), sizeof(offset));
                };
                write_offset(street_offsets);
                write_offset(addr_offsets);
                write_offset(interp_offsets);
            }
        }

        std::cerr << "geo index: " << sorted_geo_cells.size() << " cells ("
                  << ways.size() << " ways, "
                  << addr_points.size() << " addrs, "
                  << interp_ways.size() << " interps)" << std::endl;
    }

    write_cell_index(output_dir + "/admin_cells.bin", output_dir + "/admin_entries.bin", cell_to_admin);
    std::cerr << "admin index: " << cell_to_admin.size() << " cells, " << admin_polygons.size() << " polygons" << std::endl;

    if (!kAdminOnly) {
        // Street ways
        {
            std::ofstream f(output_dir + "/street_ways.bin", std::ios::binary);
            f.write(reinterpret_cast<const char*>(ways.data()), ways.size() * sizeof(WayHeader));
        }

        // Street nodes
        {
            std::ofstream f(output_dir + "/street_nodes.bin", std::ios::binary);
            f.write(reinterpret_cast<const char*>(street_nodes.data()), street_nodes.size() * sizeof(NodeCoord));
        }

        if (!kNoAddresses) {
            // Address points
            {
                std::ofstream f(output_dir + "/addr_points.bin", std::ios::binary);
                f.write(reinterpret_cast<const char*>(addr_points.data()), addr_points.size() * sizeof(AddrPoint));
            }

            // Interpolation ways
            {
                std::ofstream f(output_dir + "/interp_ways.bin", std::ios::binary);
                f.write(reinterpret_cast<const char*>(interp_ways.data()), interp_ways.size() * sizeof(InterpWay));
            }

            // Interpolation nodes
            {
                std::ofstream f(output_dir + "/interp_nodes.bin", std::ios::binary);
                f.write(reinterpret_cast<const char*>(interp_nodes.data()), interp_nodes.size() * sizeof(NodeCoord));
            }
        }
    }

    // Admin polygons
    {
        std::ofstream f(output_dir + "/admin_polygons.bin", std::ios::binary);
        f.write(reinterpret_cast<const char*>(admin_polygons.data()), admin_polygons.size() * sizeof(AdminPolygon));
    }

    // Admin vertices
    {
        std::ofstream f(output_dir + "/admin_vertices.bin", std::ios::binary);
        f.write(reinterpret_cast<const char*>(admin_vertices.data()), admin_vertices.size() * sizeof(NodeCoord));
    }

    // Strings
    {
        std::ofstream f(output_dir + "/strings.bin", std::ios::binary);
        f.write(string_pool.data().data(), string_pool.data().size());
        std::cerr << "strings.bin: " << string_pool.data().size() << " bytes" << std::endl;
    }


}

// --- Main ---

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: build-index <output-dir> <input.osm.pbf> [input2.osm.pbf ...] [--street-level N] [--admin-level N] [--admin-only] [--no-addresses] [--max-admin-level N]" << std::endl;
        return 1;
    }

    std::string output_dir = argv[1];
    std::vector<std::string> input_files;
    for (int i = 2; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--street-level" && i + 1 < argc) {
            kStreetCellLevel = std::atoi(argv[++i]);
        } else if (arg == "--admin-level" && i + 1 < argc) {
            kAdminCellLevel = std::atoi(argv[++i]);
        } else if (arg == "--admin-only") {
            kAdminOnly = true;
        } else if (arg == "--no-addresses") {
            kNoAddresses = true;
        } else if (arg == "--max-admin-level" && i + 1 < argc) {
            kMaxAdminLevel = std::atoi(argv[++i]);
        } else {
            input_files.push_back(arg);
        }
    }

    BuildHandler handler;

    for (const auto& input_file : input_files) {
        std::cerr << "Processing " << input_file << "..." << std::endl;

        // --- Pass 1: collect relation members for multipolygon assembly ---
        std::cerr << "  Pass 1: scanning relations..." << std::endl;

        osmium::area::Assembler::config_type assembler_config;
        osmium::area::MultipolygonManager<osmium::area::Assembler> mp_manager{assembler_config};

        {
            osmium::io::Reader reader1{input_file, osmium::osm_entity_bits::relation};
            osmium::apply(reader1, mp_manager);
            reader1.close();
            mp_manager.prepare_for_lookup();
        }

        // --- Pass 2: process all data ---
        std::cerr << "  Pass 2: processing nodes, ways, and areas..." << std::endl;

        using index_type = osmium::index::map::SparseFileArray<
            osmium::unsigned_object_id_type, osmium::Location>;
        using location_handler_type = osmium::handler::NodeLocationsForWays<index_type>;

        std::string tmp_path = output_dir + "/node_locations.tmp";
        int fd = open(tmp_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0600);
        index_type index{fd};
        location_handler_type location_handler{index};

        osmium::io::Reader reader2{input_file};

        osmium::apply(reader2, location_handler, handler, mp_manager.handler([&handler](osmium::memory::Buffer&& buffer) {
            osmium::apply(buffer, handler);
        }));
        reader2.close();
        close(fd);
        std::remove(tmp_path.c_str());
    }

    std::cerr << "Done reading:" << std::endl;
    std::cerr << "  " << handler.way_count() << " street ways" << std::endl;
    std::cerr << "  " << addr_count_total << " address points ("
              << handler.building_addr_count() << " from buildings)" << std::endl;
    std::cerr << "  " << handler.interp_count() << " interpolation ways" << std::endl;
    std::cerr << "  " << handler.admin_count() << " admin/postcode boundaries ("
              << admin_polygons.size() << " polygon rings)" << std::endl;

    if (kAdminOnly) {
        std::cerr << "Admin-only mode: skipped streets, addresses, and interpolation" << std::endl;
    }
    if (kNoAddresses) {
        std::cerr << "No-addresses mode: skipped address points and interpolation" << std::endl;
    }

    if (!kAdminOnly && !kNoAddresses) {
        std::cerr << "Resolving interpolation endpoints..." << std::endl;
        resolve_interpolation_endpoints();
    }

    std::cerr << "Deduplicating..." << std::endl;
    if (!kAdminOnly) {
        deduplicate(cell_to_ways);
        if (!kNoAddresses) {
            deduplicate(cell_to_addrs);
            deduplicate(cell_to_interps);
        }
    }
    deduplicate(cell_to_admin);

    std::cerr << "Writing index files to " << output_dir << "..." << std::endl;
    write_index(output_dir);

    std::cerr << "Done." << std::endl;
    return 0;
}
