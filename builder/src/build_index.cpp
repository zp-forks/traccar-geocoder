#include <algorithm>
#include <atomic>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <deque>
#include <fstream>
#include <future>
#include <iostream>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <osmium/handler.hpp>
#include <osmium/io/pbf_input.hpp>
#include <osmium/visitor.hpp>
#include <osmium/handler/node_locations_for_ways.hpp>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <osmium/index/map/flex_mem.hpp>
#include <osmium/area/assembler.hpp>
#include <osmium/area/multipolygon_manager.hpp>

#include <s2/s2cell_id.h>
#include <s2/s2latlng.h>
#include <s2/s2region_coverer.h>
#include <s2/s2polyline.h>
#include <s2/s2polygon.h>
#include <s2/s2loop.h>
#include <s2/s2builder.h>

// --- Directory creation ---

static void ensure_dir(const std::string& path) {
    mkdir(path.c_str(), 0755);
}

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

// --- Index mode ---

enum class IndexMode { Full, NoAddresses, AdminOnly };

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
    std::vector<char>& mutable_data() { return data_; }

private:
    std::unordered_map<std::string, uint32_t> index_;
    std::vector<char> data_;
};

// --- Deferred S2 work items (computed in parallel after PBF read) ---

struct DeferredWay {
    uint32_t way_id;
    uint32_t node_offset;
    uint8_t node_count;
};

struct DeferredInterp {
    uint32_t interp_id;
    uint32_t node_offset;
    uint8_t node_count;
};

// Forward declaration
static std::vector<std::pair<S2CellId, bool>> cover_polygon(const std::vector<std::pair<double,double>>& vertices);

// --- Thread pool for concurrent admin polygon S2 covering ---

class AdminCoverPool {
public:
    struct WorkItem {
        uint32_t poly_id;
        std::vector<std::pair<double,double>> vertices;
    };

    explicit AdminCoverPool(size_t num_threads) : stop_(false) {
        for (size_t i = 0; i < num_threads; i++) {
            workers_.emplace_back([this]() { worker_loop(); });
        }
    }

    ~AdminCoverPool() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stop_ = true;
        }
        cv_.notify_all();
        for (auto& t : workers_) t.join();
    }

    void submit(uint32_t poly_id, std::vector<std::pair<double,double>>&& vertices) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            queue_.push_back({poly_id, std::move(vertices)});
        }
        cv_.notify_one();
    }

    // Wait for all pending work to complete, then return merged results
    std::unordered_map<uint64_t, std::vector<uint32_t>> drain() {
        // Wait until queue is empty and all workers are idle
        {
            std::unique_lock<std::mutex> lock(mutex_);
            done_cv_.wait(lock, [this]() { return queue_.empty() && active_workers_ == 0; });
        }

        // Merge all thread-local results
        std::unordered_map<uint64_t, std::vector<uint32_t>> merged;
        for (auto& local : thread_results_) {
            for (auto& [cell_id, ids] : local) {
                auto& target = merged[cell_id];
                target.insert(target.end(), ids.begin(), ids.end());
            }
        }
        return merged;
    }

private:
    void worker_loop() {
        size_t my_idx;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            my_idx = thread_results_.size();
            thread_results_.emplace_back();
        }

        while (true) {
            WorkItem item;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait(lock, [this]() { return stop_ || !queue_.empty(); });
                if (stop_ && queue_.empty()) return;
                item = std::move(queue_.front());
                queue_.pop_front();
                active_workers_++;
            }

            // Compute S2 covering (no locks needed — pure computation)
            auto cell_ids = cover_polygon(item.vertices);
            auto& local = thread_results_[my_idx];
            for (const auto& [cell_id, is_interior] : cell_ids) {
                uint32_t entry = is_interior ? (item.poly_id | INTERIOR_FLAG) : item.poly_id;
                local[cell_id.id()].push_back(entry);
            }

            {
                std::lock_guard<std::mutex> lock(mutex_);
                active_workers_--;
            }
            done_cv_.notify_all();
        }
    }

    std::vector<std::thread> workers_;
    std::deque<WorkItem> queue_;
    std::vector<std::unordered_map<uint64_t, std::vector<uint32_t>>> thread_results_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::condition_variable done_cv_;
    size_t active_workers_ = 0;
    bool stop_;
};

// --- Parsed data container ---

struct ParsedData {
    StringPool string_pool;
    std::vector<WayHeader> ways;
    std::vector<NodeCoord> street_nodes;
    std::unordered_map<uint64_t, std::vector<uint32_t>> cell_to_ways;
    std::vector<AddrPoint> addr_points;
    std::unordered_map<uint64_t, std::vector<uint32_t>> cell_to_addrs;
    std::vector<InterpWay> interp_ways;
    std::vector<NodeCoord> interp_nodes;
    std::unordered_map<uint64_t, std::vector<uint32_t>> cell_to_interps;
    std::vector<AdminPolygon> admin_polygons;
    std::vector<NodeCoord> admin_vertices;
    std::unordered_map<uint64_t, std::vector<uint32_t>> cell_to_admin;

    // Deferred work for parallel S2 computation (ways + interps)
    std::vector<DeferredWay> deferred_ways;
    std::vector<DeferredInterp> deferred_interps;
};

// --- S2 helpers ---

static int kStreetCellLevel = 17;
static int kAdminCellLevel = 10;
static int kMaxAdminLevel = 0;  // 0 means no filtering

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

// --- Douglas-Peucker simplification ---

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

static void add_addr_point(ParsedData& data, double lat, double lng,
                           const char* housenumber, const char* street,
                           uint64_t& addr_count_total) {
    uint32_t addr_id = static_cast<uint32_t>(data.addr_points.size());
    data.addr_points.push_back({
        static_cast<float>(lat),
        static_cast<float>(lng),
        data.string_pool.intern(housenumber),
        data.string_pool.intern(street)
    });

    S2CellId cell = point_to_cell(lat, lng);
    data.cell_to_addrs[cell.id()].push_back(addr_id);

    addr_count_total++;
    if (addr_count_total % 1000000 == 0) {
        std::cerr << "Collected " << addr_count_total / 1000000 << "M addresses..." << std::endl;
    }
}

// --- Add an admin polygon ---

static void add_admin_polygon(ParsedData& data,
                               const std::vector<std::pair<double,double>>& vertices,
                               const char* name, uint8_t admin_level,
                               const char* country_code,
                               AdminCoverPool* admin_pool = nullptr) {
    // Simplify large polygons
    auto simplified = simplify_polygon(vertices, 500);
    if (simplified.size() < 3) return;

    uint32_t poly_id = static_cast<uint32_t>(data.admin_polygons.size());
    uint32_t vertex_offset = static_cast<uint32_t>(data.admin_vertices.size());

    for (const auto& [lat, lng] : simplified) {
        data.admin_vertices.push_back({static_cast<float>(lat), static_cast<float>(lng)});
    }

    AdminPolygon poly{};
    poly.vertex_offset = vertex_offset;
    poly.vertex_count = static_cast<uint16_t>(std::min(simplified.size(), size_t(65535)));
    poly.name_id = data.string_pool.intern(name);
    poly.admin_level = admin_level;
    poly.area = polygon_area(simplified);
    poly.country_code = (country_code && country_code[0] && country_code[1])
        ? static_cast<uint16_t>((country_code[0] << 8) | country_code[1])
        : 0;
    data.admin_polygons.push_back(poly);

    // Submit S2 cell coverage to thread pool (runs concurrently with PBF reading)
    if (admin_pool) {
        admin_pool->submit(poly_id, std::move(simplified));
    }
}

// --- OSM handler ---

class BuildHandler : public osmium::handler::Handler {
public:
    explicit BuildHandler(ParsedData& data, AdminCoverPool* admin_pool = nullptr)
        : data_(data), admin_pool_(admin_pool) {}

    void node(const osmium::Node& node) {
        const char* housenumber = node.tags()["addr:housenumber"];
        if (!housenumber) return;
        const char* street = node.tags()["addr:street"];
        if (!street) return;
        if (!node.location().valid()) return;

        add_addr_point(data_, node.location().lat(), node.location().lon(),
                       housenumber, street, addr_count_total_);
    }

    void way(const osmium::Way& way) {
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
                add_admin_polygon(data_, vertices, name_str.c_str(), admin_level, country_code, admin_pool_);
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
    uint64_t addr_count_total() const { return addr_count_total_; }

private:
    ParsedData& data_;
    AdminCoverPool* admin_pool_ = nullptr;
    uint64_t way_count_ = 0;
    uint64_t building_addr_count_ = 0;
    uint64_t interp_count_ = 0;
    uint64_t admin_count_ = 0;
    uint64_t addr_count_total_ = 0;

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

        add_addr_point(data_, sum_lat / valid, sum_lng / valid,
                       housenumber, street, addr_count_total_);
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

        uint32_t interp_id = static_cast<uint32_t>(data_.interp_ways.size());
        uint32_t node_offset = static_cast<uint32_t>(data_.interp_nodes.size());

        for (const auto& nr : wnodes) {
            data_.interp_nodes.push_back({
                static_cast<float>(nr.location().lat()),
                static_cast<float>(nr.location().lon())
            });
        }

        InterpWay iw{};
        iw.node_offset = node_offset;
        iw.node_count = static_cast<uint8_t>(std::min(wnodes.size(), size_t(255)));
        iw.street_id = data_.string_pool.intern(street);
        iw.start_number = 0;
        iw.end_number = 0;
        iw.interpolation = interp_type;
        data_.interp_ways.push_back(iw);

        // Defer S2 cell computation to parallel phase
        data_.deferred_interps.push_back({interp_id, node_offset,
            static_cast<uint8_t>(std::min(wnodes.size(), size_t(255)))});

        interp_count_++;
    }

    void process_highway(const osmium::Way& way, const char* name) {
        const auto& wnodes = way.nodes();
        if (wnodes.size() < 2) return;

        for (const auto& nr : wnodes) {
            if (!nr.location().valid()) return;
        }

        uint32_t way_id = static_cast<uint32_t>(data_.ways.size());
        uint32_t node_offset = static_cast<uint32_t>(data_.street_nodes.size());

        for (const auto& nr : wnodes) {
            data_.street_nodes.push_back({
                static_cast<float>(nr.location().lat()),
                static_cast<float>(nr.location().lon())
            });
        }

        WayHeader header{};
        header.node_offset = node_offset;
        header.node_count = static_cast<uint8_t>(std::min(wnodes.size(), size_t(255)));
        header.name_id = data_.string_pool.intern(name);
        data_.ways.push_back(header);

        // Defer S2 cell computation to parallel phase
        data_.deferred_ways.push_back({way_id, node_offset, header.node_count});

        way_count_++;
        if (way_count_ % 1000000 == 0) {
            std::cerr << "Collected " << way_count_ / 1000000 << "M street ways..." << std::endl;
        }
    }
};

// --- Resolve interpolation way endpoint house numbers ---

static void resolve_interpolation_endpoints(ParsedData& data) {
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
    for (uint32_t i = 0; i < data.addr_points.size(); i++) {
        CoordKey key{
            static_cast<int32_t>(data.addr_points[i].lat * 100000),
            static_cast<int32_t>(data.addr_points[i].lng * 100000)
        };
        addr_by_coord[key] = i;
    }

    uint32_t resolved = 0;
    for (auto& iw : data.interp_ways) {
        if (iw.node_count < 2) continue;

        const auto& start = data.interp_nodes[iw.node_offset];
        CoordKey start_key{
            static_cast<int32_t>(start.lat * 100000),
            static_cast<int32_t>(start.lng * 100000)
        };
        auto it_start = addr_by_coord.find(start_key);

        const auto& end = data.interp_nodes[iw.node_offset + iw.node_count - 1];
        CoordKey end_key{
            static_cast<int32_t>(end.lat * 100000),
            static_cast<int32_t>(end.lng * 100000)
        };
        auto it_end = addr_by_coord.find(end_key);

        if (it_start != addr_by_coord.end()) {
            const char* hn = data.string_pool.data().data() + data.addr_points[it_start->second].housenumber_id;
            iw.start_number = parse_house_number(hn);
        }
        if (it_end != addr_by_coord.end()) {
            const char* hn = data.string_pool.data().data() + data.addr_points[it_end->second].housenumber_id;
            iw.end_number = parse_house_number(hn);
        }

        if (iw.start_number > 0 && iw.end_number > 0) resolved++;
    }

    std::cerr << "Resolved " << resolved << "/" << data.interp_ways.size()
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

// --- Cache serialization ---

enum class SectionType : uint32_t {
    STRING_POOL = 0,
    WAYS = 1,
    STREET_NODES = 2,
    CELL_TO_WAYS = 3,
    ADDR_POINTS = 4,
    CELL_TO_ADDRS = 5,
    INTERP_WAYS = 6,
    INTERP_NODES = 7,
    CELL_TO_INTERPS = 8,
    ADMIN_POLYGONS = 9,
    ADMIN_VERTICES = 10,
    CELL_TO_ADMIN = 11,
};

static const char CACHE_MAGIC[8] = {'T','G','C','A','C','H','E','\0'};
static const uint32_t CACHE_VERSION = 1;
static const uint32_t CACHE_SECTION_COUNT = 12;

// Serialize a vector of structs to a binary blob
template<typename T>
static std::vector<char> serialize_vector(const std::vector<T>& vec) {
    std::vector<char> buf;
    uint64_t count = vec.size();
    buf.resize(sizeof(uint64_t) + count * sizeof(T));
    std::memcpy(buf.data(), &count, sizeof(uint64_t));
    if (count > 0) {
        std::memcpy(buf.data() + sizeof(uint64_t), vec.data(), count * sizeof(T));
    }
    return buf;
}

// Deserialize a vector of structs from a binary blob
template<typename T>
static bool deserialize_vector(const char* data, uint64_t length, std::vector<T>& vec) {
    if (length < sizeof(uint64_t)) return false;
    uint64_t count;
    std::memcpy(&count, data, sizeof(uint64_t));
    if (length < sizeof(uint64_t) + count * sizeof(T)) return false;
    vec.resize(count);
    if (count > 0) {
        std::memcpy(vec.data(), data + sizeof(uint64_t), count * sizeof(T));
    }
    return true;
}

// Serialize a cell map (uint64_t -> vector<uint32_t>)
static std::vector<char> serialize_cell_map(const std::unordered_map<uint64_t, std::vector<uint32_t>>& map) {
    // Calculate total size
    size_t total = sizeof(uint64_t); // entry count
    for (const auto& [cell_id, ids] : map) {
        total += sizeof(uint64_t) + sizeof(uint32_t) + ids.size() * sizeof(uint32_t);
    }
    std::vector<char> buf(total);
    char* ptr = buf.data();

    uint64_t entry_count = map.size();
    std::memcpy(ptr, &entry_count, sizeof(uint64_t));
    ptr += sizeof(uint64_t);

    for (const auto& [cell_id, ids] : map) {
        std::memcpy(ptr, &cell_id, sizeof(uint64_t));
        ptr += sizeof(uint64_t);
        uint32_t id_count = static_cast<uint32_t>(ids.size());
        std::memcpy(ptr, &id_count, sizeof(uint32_t));
        ptr += sizeof(uint32_t);
        if (id_count > 0) {
            std::memcpy(ptr, ids.data(), id_count * sizeof(uint32_t));
            ptr += id_count * sizeof(uint32_t);
        }
    }
    return buf;
}

// Deserialize a cell map
static bool deserialize_cell_map(const char* data, uint64_t length,
                                  std::unordered_map<uint64_t, std::vector<uint32_t>>& map) {
    if (length < sizeof(uint64_t)) return false;
    const char* ptr = data;
    const char* end = data + length;

    uint64_t entry_count;
    std::memcpy(&entry_count, ptr, sizeof(uint64_t));
    ptr += sizeof(uint64_t);

    map.reserve(entry_count);
    for (uint64_t i = 0; i < entry_count; i++) {
        if (ptr + sizeof(uint64_t) + sizeof(uint32_t) > end) return false;
        uint64_t cell_id;
        std::memcpy(&cell_id, ptr, sizeof(uint64_t));
        ptr += sizeof(uint64_t);
        uint32_t id_count;
        std::memcpy(&id_count, ptr, sizeof(uint32_t));
        ptr += sizeof(uint32_t);
        if (ptr + id_count * sizeof(uint32_t) > end) return false;
        std::vector<uint32_t> ids(id_count);
        if (id_count > 0) {
            std::memcpy(ids.data(), ptr, id_count * sizeof(uint32_t));
            ptr += id_count * sizeof(uint32_t);
        }
        map[cell_id] = std::move(ids);
    }
    return true;
}

static void serialize_cache(const ParsedData& data, const std::string& path) {
    std::cerr << "Saving cache to " << path << "..." << std::endl;
    std::ofstream f(path, std::ios::binary);
    if (!f.is_open()) {
        std::cerr << "Error: could not open cache file for writing: " << path << std::endl;
        return;
    }

    f.write(CACHE_MAGIC, 8);
    f.write(reinterpret_cast<const char*>(&CACHE_VERSION), sizeof(uint32_t));
    f.write(reinterpret_cast<const char*>(&CACHE_SECTION_COUNT), sizeof(uint32_t));

    // Helper to write a section
    auto write_section = [&](SectionType type, const std::vector<char>& blob) {
        uint32_t t = static_cast<uint32_t>(type);
        uint64_t len = blob.size();
        f.write(reinterpret_cast<const char*>(&t), sizeof(uint32_t));
        f.write(reinterpret_cast<const char*>(&len), sizeof(uint64_t));
        f.write(blob.data(), len);
    };

    // STRING_POOL: raw char vector
    {
        const auto& sp = data.string_pool.data();
        std::vector<char> blob(sizeof(uint64_t) + sp.size());
        uint64_t sz = sp.size();
        std::memcpy(blob.data(), &sz, sizeof(uint64_t));
        if (sz > 0) std::memcpy(blob.data() + sizeof(uint64_t), sp.data(), sz);
        write_section(SectionType::STRING_POOL, blob);
    }

    write_section(SectionType::WAYS, serialize_vector(data.ways));
    write_section(SectionType::STREET_NODES, serialize_vector(data.street_nodes));
    write_section(SectionType::CELL_TO_WAYS, serialize_cell_map(data.cell_to_ways));
    write_section(SectionType::ADDR_POINTS, serialize_vector(data.addr_points));
    write_section(SectionType::CELL_TO_ADDRS, serialize_cell_map(data.cell_to_addrs));
    write_section(SectionType::INTERP_WAYS, serialize_vector(data.interp_ways));
    write_section(SectionType::INTERP_NODES, serialize_vector(data.interp_nodes));
    write_section(SectionType::CELL_TO_INTERPS, serialize_cell_map(data.cell_to_interps));
    write_section(SectionType::ADMIN_POLYGONS, serialize_vector(data.admin_polygons));
    write_section(SectionType::ADMIN_VERTICES, serialize_vector(data.admin_vertices));
    write_section(SectionType::CELL_TO_ADMIN, serialize_cell_map(data.cell_to_admin));

    std::cerr << "Cache saved." << std::endl;
}

static bool deserialize_cache(ParsedData& data, const std::string& path) {
    std::cerr << "Loading cache from " << path << "..." << std::endl;
    std::ifstream f(path, std::ios::binary);
    if (!f.is_open()) {
        std::cerr << "Error: could not open cache file: " << path << std::endl;
        return false;
    }

    char magic[8];
    f.read(magic, 8);
    if (std::memcmp(magic, CACHE_MAGIC, 8) != 0) {
        std::cerr << "Error: invalid cache magic" << std::endl;
        return false;
    }

    uint32_t version;
    f.read(reinterpret_cast<char*>(&version), sizeof(uint32_t));
    if (version != CACHE_VERSION) {
        std::cerr << "Error: unsupported cache version " << version << std::endl;
        return false;
    }

    uint32_t section_count;
    f.read(reinterpret_cast<char*>(&section_count), sizeof(uint32_t));

    for (uint32_t s = 0; s < section_count; s++) {
        uint32_t type;
        uint64_t length;
        f.read(reinterpret_cast<char*>(&type), sizeof(uint32_t));
        f.read(reinterpret_cast<char*>(&length), sizeof(uint64_t));

        std::vector<char> blob(length);
        f.read(blob.data(), length);

        switch (static_cast<SectionType>(type)) {
        case SectionType::STRING_POOL: {
            if (length < sizeof(uint64_t)) return false;
            uint64_t sz;
            std::memcpy(&sz, blob.data(), sizeof(uint64_t));
            auto& sp = data.string_pool.mutable_data();
            sp.resize(sz);
            if (sz > 0) std::memcpy(sp.data(), blob.data() + sizeof(uint64_t), sz);
            break;
        }
        case SectionType::WAYS:
            if (!deserialize_vector(blob.data(), length, data.ways)) return false;
            break;
        case SectionType::STREET_NODES:
            if (!deserialize_vector(blob.data(), length, data.street_nodes)) return false;
            break;
        case SectionType::CELL_TO_WAYS:
            if (!deserialize_cell_map(blob.data(), length, data.cell_to_ways)) return false;
            break;
        case SectionType::ADDR_POINTS:
            if (!deserialize_vector(blob.data(), length, data.addr_points)) return false;
            break;
        case SectionType::CELL_TO_ADDRS:
            if (!deserialize_cell_map(blob.data(), length, data.cell_to_addrs)) return false;
            break;
        case SectionType::INTERP_WAYS:
            if (!deserialize_vector(blob.data(), length, data.interp_ways)) return false;
            break;
        case SectionType::INTERP_NODES:
            if (!deserialize_vector(blob.data(), length, data.interp_nodes)) return false;
            break;
        case SectionType::CELL_TO_INTERPS:
            if (!deserialize_cell_map(blob.data(), length, data.cell_to_interps)) return false;
            break;
        case SectionType::ADMIN_POLYGONS:
            if (!deserialize_vector(blob.data(), length, data.admin_polygons)) return false;
            break;
        case SectionType::ADMIN_VERTICES:
            if (!deserialize_vector(blob.data(), length, data.admin_vertices)) return false;
            break;
        case SectionType::CELL_TO_ADMIN:
            if (!deserialize_cell_map(blob.data(), length, data.cell_to_admin)) return false;
            break;
        default:
            std::cerr << "Warning: unknown cache section type " << type << ", skipping" << std::endl;
            break;
        }
    }

    std::cerr << "Cache loaded: " << data.ways.size() << " ways, "
              << data.addr_points.size() << " addrs, "
              << data.interp_ways.size() << " interps, "
              << data.admin_polygons.size() << " admin polygons" << std::endl;
    return true;
}

// --- Continent bounding box filtering ---

struct ContinentBBox {
    const char* name;
    double min_lat, max_lat, min_lng, max_lng;
};

static const ContinentBBox kContinents[] = {
    {"africa",            -35.0,  37.5,  -25.0,  55.0},
    {"asia",              -12.0,  82.0,   25.0, 180.0},
    {"europe",             35.0,  72.0,  -25.0,  45.0},
    {"north-america",       7.0,  84.0, -170.0, -50.0},
    {"south-america",     -56.0,  13.0,  -82.0, -34.0},
    {"oceania",           -50.0,   0.0,  110.0, 180.0},
    {"central-america",     7.0,  23.5, -120.0, -57.0},
    {"antarctica",        -90.0, -60.0, -180.0, 180.0},
};

static bool cell_in_bbox(uint64_t cell_id, const ContinentBBox& bbox) {
    S2CellId cell(cell_id);
    S2LatLng center = cell.ToLatLng();
    double lat = center.lat().degrees();
    double lng = center.lng().degrees();
    return lat >= bbox.min_lat && lat <= bbox.max_lat &&
           lng >= bbox.min_lng && lng <= bbox.max_lng;
}

static ParsedData filter_by_bbox(const ParsedData& full, const ContinentBBox& bbox) {
    ParsedData out;

    // --- Filter cell_to_ways and collect referenced way IDs ---
    std::unordered_set<uint32_t> used_way_ids;
    for (const auto& [cell_id, ids] : full.cell_to_ways) {
        if (cell_in_bbox(cell_id, bbox)) {
            for (uint32_t id : ids) {
                used_way_ids.insert(id);
            }
        }
    }

    // --- Filter cell_to_addrs and collect referenced addr IDs ---
    std::unordered_set<uint32_t> used_addr_ids;
    for (const auto& [cell_id, ids] : full.cell_to_addrs) {
        if (cell_in_bbox(cell_id, bbox)) {
            for (uint32_t id : ids) {
                used_addr_ids.insert(id);
            }
        }
    }

    // --- Filter cell_to_interps and collect referenced interp IDs ---
    std::unordered_set<uint32_t> used_interp_ids;
    for (const auto& [cell_id, ids] : full.cell_to_interps) {
        if (cell_in_bbox(cell_id, bbox)) {
            for (uint32_t id : ids) {
                used_interp_ids.insert(id);
            }
        }
    }

    // --- Filter cell_to_admin and collect referenced admin IDs ---
    std::unordered_set<uint32_t> used_admin_ids;
    for (const auto& [cell_id, ids] : full.cell_to_admin) {
        if (cell_in_bbox(cell_id, bbox)) {
            for (uint32_t id : ids) {
                used_admin_ids.insert(id & ID_MASK);
            }
        }
    }

    // --- Build remapped ways ---
    std::unordered_map<uint32_t, uint32_t> way_remap; // old_id -> new_id
    {
        std::vector<uint32_t> sorted_ids(used_way_ids.begin(), used_way_ids.end());
        std::sort(sorted_ids.begin(), sorted_ids.end());
        for (uint32_t old_id : sorted_ids) {
            uint32_t new_id = static_cast<uint32_t>(out.ways.size());
            way_remap[old_id] = new_id;
            const auto& w = full.ways[old_id];
            WayHeader nw = w;
            nw.node_offset = static_cast<uint32_t>(out.street_nodes.size());
            out.ways.push_back(nw);
            for (uint8_t n = 0; n < w.node_count; n++) {
                out.street_nodes.push_back(full.street_nodes[w.node_offset + n]);
            }
        }
    }

    // --- Build remapped addr points ---
    std::unordered_map<uint32_t, uint32_t> addr_remap;
    {
        std::vector<uint32_t> sorted_ids(used_addr_ids.begin(), used_addr_ids.end());
        std::sort(sorted_ids.begin(), sorted_ids.end());
        for (uint32_t old_id : sorted_ids) {
            uint32_t new_id = static_cast<uint32_t>(out.addr_points.size());
            addr_remap[old_id] = new_id;
            out.addr_points.push_back(full.addr_points[old_id]);
        }
    }

    // --- Build remapped interp ways ---
    std::unordered_map<uint32_t, uint32_t> interp_remap;
    {
        std::vector<uint32_t> sorted_ids(used_interp_ids.begin(), used_interp_ids.end());
        std::sort(sorted_ids.begin(), sorted_ids.end());
        for (uint32_t old_id : sorted_ids) {
            uint32_t new_id = static_cast<uint32_t>(out.interp_ways.size());
            interp_remap[old_id] = new_id;
            const auto& iw = full.interp_ways[old_id];
            InterpWay niw = iw;
            niw.node_offset = static_cast<uint32_t>(out.interp_nodes.size());
            out.interp_ways.push_back(niw);
            for (uint8_t n = 0; n < iw.node_count; n++) {
                out.interp_nodes.push_back(full.interp_nodes[iw.node_offset + n]);
            }
        }
    }

    // --- Build remapped admin polygons ---
    std::unordered_map<uint32_t, uint32_t> admin_remap;
    {
        std::vector<uint32_t> sorted_ids(used_admin_ids.begin(), used_admin_ids.end());
        std::sort(sorted_ids.begin(), sorted_ids.end());
        for (uint32_t old_id : sorted_ids) {
            uint32_t new_id = static_cast<uint32_t>(out.admin_polygons.size());
            admin_remap[old_id] = new_id;
            const auto& ap = full.admin_polygons[old_id];
            AdminPolygon nap = ap;
            nap.vertex_offset = static_cast<uint32_t>(out.admin_vertices.size());
            out.admin_polygons.push_back(nap);
            for (uint16_t v = 0; v < ap.vertex_count; v++) {
                out.admin_vertices.push_back(full.admin_vertices[ap.vertex_offset + v]);
            }
        }
    }

    // --- Build remapped cell maps ---
    for (const auto& [cell_id, ids] : full.cell_to_ways) {
        if (!cell_in_bbox(cell_id, bbox)) continue;
        std::vector<uint32_t> new_ids;
        for (uint32_t id : ids) {
            auto it = way_remap.find(id);
            if (it != way_remap.end()) {
                new_ids.push_back(it->second);
            }
        }
        if (!new_ids.empty()) {
            out.cell_to_ways[cell_id] = std::move(new_ids);
        }
    }

    for (const auto& [cell_id, ids] : full.cell_to_addrs) {
        if (!cell_in_bbox(cell_id, bbox)) continue;
        std::vector<uint32_t> new_ids;
        for (uint32_t id : ids) {
            auto it = addr_remap.find(id);
            if (it != addr_remap.end()) {
                new_ids.push_back(it->second);
            }
        }
        if (!new_ids.empty()) {
            out.cell_to_addrs[cell_id] = std::move(new_ids);
        }
    }

    for (const auto& [cell_id, ids] : full.cell_to_interps) {
        if (!cell_in_bbox(cell_id, bbox)) continue;
        std::vector<uint32_t> new_ids;
        for (uint32_t id : ids) {
            auto it = interp_remap.find(id);
            if (it != interp_remap.end()) {
                new_ids.push_back(it->second);
            }
        }
        if (!new_ids.empty()) {
            out.cell_to_interps[cell_id] = std::move(new_ids);
        }
    }

    for (const auto& [cell_id, ids] : full.cell_to_admin) {
        if (!cell_in_bbox(cell_id, bbox)) continue;
        std::vector<uint32_t> new_ids;
        for (uint32_t id : ids) {
            uint32_t raw_id = id & ID_MASK;
            uint32_t flags = id & INTERIOR_FLAG;
            auto it = admin_remap.find(raw_id);
            if (it != admin_remap.end()) {
                new_ids.push_back(it->second | flags);
            }
        }
        if (!new_ids.empty()) {
            out.cell_to_admin[cell_id] = std::move(new_ids);
        }
    }

    // --- Rebuild compact string pool with only referenced strings ---
    // Collect all referenced string offsets
    std::unordered_set<uint32_t> used_offsets;
    for (const auto& w : out.ways) {
        used_offsets.insert(w.name_id);
    }
    for (const auto& a : out.addr_points) {
        used_offsets.insert(a.housenumber_id);
        used_offsets.insert(a.street_id);
    }
    for (const auto& iw : out.interp_ways) {
        used_offsets.insert(iw.street_id);
    }
    for (const auto& ap : out.admin_polygons) {
        used_offsets.insert(ap.name_id);
    }

    // Build remap: old offset -> new offset
    const auto& old_sp = full.string_pool.data();
    std::unordered_map<uint32_t, uint32_t> string_remap;
    auto& new_sp = out.string_pool.mutable_data();
    new_sp.clear();

    std::vector<uint32_t> sorted_offsets(used_offsets.begin(), used_offsets.end());
    std::sort(sorted_offsets.begin(), sorted_offsets.end());

    for (uint32_t old_off : sorted_offsets) {
        uint32_t new_off = static_cast<uint32_t>(new_sp.size());
        string_remap[old_off] = new_off;
        // Copy the null-terminated string
        const char* str = old_sp.data() + old_off;
        size_t len = std::strlen(str);
        new_sp.insert(new_sp.end(), str, str + len + 1);
    }

    // Remap all string IDs in the output data
    for (auto& w : out.ways) {
        w.name_id = string_remap[w.name_id];
    }
    for (auto& a : out.addr_points) {
        a.housenumber_id = string_remap[a.housenumber_id];
        a.street_id = string_remap[a.street_id];
    }
    for (auto& iw : out.interp_ways) {
        iw.street_id = string_remap[iw.street_id];
    }
    for (auto& ap : out.admin_polygons) {
        ap.name_id = string_remap[ap.name_id];
    }

    return out;
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

static void write_index(const ParsedData& data, const std::string& output_dir, IndexMode mode) {
    ensure_dir(output_dir);

    bool write_streets = (mode != IndexMode::AdminOnly);
    bool write_addresses = (mode == IndexMode::Full);

    if (write_streets) {
        // Merged geo cell index for streets, addresses, and interpolation
        std::set<uint64_t> all_geo_cells;
        for (const auto& [id, _] : data.cell_to_ways) all_geo_cells.insert(id);
        if (write_addresses) {
            for (const auto& [id, _] : data.cell_to_addrs) all_geo_cells.insert(id);
            for (const auto& [id, _] : data.cell_to_interps) all_geo_cells.insert(id);
        }
        std::vector<uint64_t> sorted_geo_cells(all_geo_cells.begin(), all_geo_cells.end());

        auto street_offsets = write_entries(output_dir + "/street_entries.bin", sorted_geo_cells, data.cell_to_ways);

        std::unordered_map<uint64_t, uint32_t> addr_offsets;
        std::unordered_map<uint64_t, uint32_t> interp_offsets;
        if (write_addresses) {
            addr_offsets = write_entries(output_dir + "/addr_entries.bin", sorted_geo_cells, data.cell_to_addrs);
            interp_offsets = write_entries(output_dir + "/interp_entries.bin", sorted_geo_cells, data.cell_to_interps);
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
                  << data.ways.size() << " ways, "
                  << data.addr_points.size() << " addrs, "
                  << data.interp_ways.size() << " interps)" << std::endl;
    }

    write_cell_index(output_dir + "/admin_cells.bin", output_dir + "/admin_entries.bin", data.cell_to_admin);
    std::cerr << "admin index: " << data.cell_to_admin.size() << " cells, " << data.admin_polygons.size() << " polygons" << std::endl;

    if (write_streets) {
        // Street ways
        {
            std::ofstream f(output_dir + "/street_ways.bin", std::ios::binary);
            f.write(reinterpret_cast<const char*>(data.ways.data()), data.ways.size() * sizeof(WayHeader));
        }

        // Street nodes
        {
            std::ofstream f(output_dir + "/street_nodes.bin", std::ios::binary);
            f.write(reinterpret_cast<const char*>(data.street_nodes.data()), data.street_nodes.size() * sizeof(NodeCoord));
        }

        if (write_addresses) {
            // Address points
            {
                std::ofstream f(output_dir + "/addr_points.bin", std::ios::binary);
                f.write(reinterpret_cast<const char*>(data.addr_points.data()), data.addr_points.size() * sizeof(AddrPoint));
            }

            // Interpolation ways
            {
                std::ofstream f(output_dir + "/interp_ways.bin", std::ios::binary);
                f.write(reinterpret_cast<const char*>(data.interp_ways.data()), data.interp_ways.size() * sizeof(InterpWay));
            }

            // Interpolation nodes
            {
                std::ofstream f(output_dir + "/interp_nodes.bin", std::ios::binary);
                f.write(reinterpret_cast<const char*>(data.interp_nodes.data()), data.interp_nodes.size() * sizeof(NodeCoord));
            }
        }
    }

    // Admin polygons
    {
        std::ofstream f(output_dir + "/admin_polygons.bin", std::ios::binary);
        f.write(reinterpret_cast<const char*>(data.admin_polygons.data()), data.admin_polygons.size() * sizeof(AdminPolygon));
    }

    // Admin vertices
    {
        std::ofstream f(output_dir + "/admin_vertices.bin", std::ios::binary);
        f.write(reinterpret_cast<const char*>(data.admin_vertices.data()), data.admin_vertices.size() * sizeof(NodeCoord));
    }

    // Strings
    {
        std::ofstream f(output_dir + "/strings.bin", std::ios::binary);
        f.write(data.string_pool.data().data(), data.string_pool.data().size());
        std::cerr << "strings.bin: " << data.string_pool.data().size() << " bytes" << std::endl;
    }
}

// --- Main ---

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: build-index <output-dir> <input.osm.pbf> [options]" << std::endl;
        std::cerr << "       build-index <output-dir> --load-cache <path> [options]" << std::endl;
        std::cerr << std::endl;
        std::cerr << "Options:" << std::endl;
        std::cerr << "  --street-level N       S2 cell level for streets (default: 17)" << std::endl;
        std::cerr << "  --admin-level N        S2 cell level for admin (default: 10)" << std::endl;
        std::cerr << "  --max-admin-level N    Only include admin levels <= N" << std::endl;
        std::cerr << "  --save-cache <path>    Save parsed data to cache file" << std::endl;
        std::cerr << "  --load-cache <path>    Load from cache instead of PBF" << std::endl;
        std::cerr << "  --multi-output         Write full, no-addresses, admin-only indexes" << std::endl;
        std::cerr << "  --continents           Also generate per-continent indexes" << std::endl;
        std::cerr << "  --mode <mode>          Index mode: full, no-addresses, admin-only (default: full)" << std::endl;
        std::cerr << "  --admin-only           Shorthand for --mode admin-only" << std::endl;
        std::cerr << "  --no-addresses         Shorthand for --mode no-addresses" << std::endl;
        return 1;
    }

    std::string output_dir = argv[1];
    std::vector<std::string> input_files;
    std::string save_cache_path;
    std::string load_cache_path;
    bool multi_output = false;
    bool generate_continents = false;
    IndexMode mode = IndexMode::Full;

    for (int i = 2; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--street-level" && i + 1 < argc) {
            kStreetCellLevel = std::atoi(argv[++i]);
        } else if (arg == "--admin-level" && i + 1 < argc) {
            kAdminCellLevel = std::atoi(argv[++i]);
        } else if (arg == "--admin-only") {
            mode = IndexMode::AdminOnly;
        } else if (arg == "--no-addresses") {
            mode = IndexMode::NoAddresses;
        } else if (arg == "--max-admin-level" && i + 1 < argc) {
            kMaxAdminLevel = std::atoi(argv[++i]);
        } else if (arg == "--save-cache" && i + 1 < argc) {
            save_cache_path = argv[++i];
        } else if (arg == "--load-cache" && i + 1 < argc) {
            load_cache_path = argv[++i];
        } else if (arg == "--multi-output") {
            multi_output = true;
        } else if (arg == "--continents") {
            generate_continents = true;
        } else if (arg == "--mode" && i + 1 < argc) {
            std::string mode_str = argv[++i];
            if (mode_str == "full") {
                mode = IndexMode::Full;
            } else if (mode_str == "no-addresses") {
                mode = IndexMode::NoAddresses;
            } else if (mode_str == "admin-only") {
                mode = IndexMode::AdminOnly;
            } else {
                std::cerr << "Error: unknown mode '" << mode_str << "'" << std::endl;
                return 1;
            }
        } else {
            input_files.push_back(arg);
        }
    }

    ParsedData data;

    if (!load_cache_path.empty()) {
        // Load from cache
        if (!deserialize_cache(data, load_cache_path)) {
            std::cerr << "Error: failed to load cache" << std::endl;
            return 1;
        }
    } else {
        // Parse PBF files (always collect everything)
        if (input_files.empty()) {
            std::cerr << "Error: no input files specified and no --load-cache" << std::endl;
            return 1;
        }

        // Create thread pool for concurrent admin polygon S2 covering
        unsigned int num_threads = std::max(1u, std::thread::hardware_concurrency() > 4 ? std::thread::hardware_concurrency() - 4 : 1u);
        std::cerr << "Using " << num_threads << " worker threads." << std::endl;
        AdminCoverPool admin_pool(num_threads);
        BuildHandler handler(data, &admin_pool);

        using index_type = osmium::index::map::FlexMem<
            osmium::unsigned_object_id_type, osmium::Location>;

        // Sharded node location index for parallel writes
        static const int NUM_SHARDS = 64;
        struct ShardedIndex {
            index_type shards[NUM_SHARDS];
            std::mutex mutexes[NUM_SHARDS];

            void set(osmium::unsigned_object_id_type id, osmium::Location loc) {
                int shard = id % NUM_SHARDS;
                std::lock_guard<std::mutex> lock(mutexes[shard]);
                shards[shard].set(id, loc);
            }

            osmium::Location get(osmium::unsigned_object_id_type id) const {
                return shards[id % NUM_SHARDS].get(id);
            }

            // Batch set — caller holds no lock, we lock per-shard
            void set_batch(const std::vector<std::pair<osmium::unsigned_object_id_type, osmium::Location>>& batch) {
                // Sort by shard to minimize lock switches
                for (const auto& [id, loc] : batch) {
                    int shard = id % NUM_SHARDS;
                    std::lock_guard<std::mutex> lock(mutexes[shard]);
                    shards[shard].set(id, loc);
                }
            }
        } index;

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

            // --- Pass 2: nodes (streaming parallel block processing) ---
            std::cerr << "  Pass 2: processing nodes in parallel..." << std::endl;

            {
                // Producer-consumer: reader feeds blocks, workers process them
                // Bounded queue prevents memory blowup
                std::mutex queue_mutex;
                std::condition_variable queue_cv;
                std::deque<osmium::memory::Buffer> block_queue;
                bool reader_done = false;
                const size_t MAX_QUEUE = 64; // limit buffered blocks

                struct NodeThreadLocal {
                    std::vector<std::pair<double,double>> addr_coords;
                    std::vector<std::pair<std::string,std::string>> addr_strings;
                    uint64_t count = 0;
                };
                std::vector<NodeThreadLocal> ntld(num_threads);
                std::atomic<uint64_t> blocks_done{0};

                // Worker threads
                std::vector<std::thread> node_workers;
                for (unsigned int t = 0; t < num_threads; t++) {
                    node_workers.emplace_back([&, t]() {
                        auto& local = ntld[t];
                        std::vector<std::pair<osmium::unsigned_object_id_type, osmium::Location>> loc_batch;
                        loc_batch.reserve(8192);

                        while (true) {
                            osmium::memory::Buffer block;
                            {
                                std::unique_lock<std::mutex> lock(queue_mutex);
                                queue_cv.wait(lock, [&]{ return !block_queue.empty() || reader_done; });
                                if (block_queue.empty() && reader_done) break;
                                block = std::move(block_queue.front());
                                block_queue.pop_front();
                            }
                            queue_cv.notify_one(); // wake reader if it was waiting

                            for (const auto& item : block) {
                                if (item.type() == osmium::item_type::node) {
                                    const auto& node = static_cast<const osmium::Node&>(item);
                                    if (!node.location().valid()) continue;
                                    loc_batch.push_back({node.positive_id(), node.location()});

                                    const char* housenumber = node.tags()["addr:housenumber"];
                                    if (housenumber) {
                                        const char* street = node.tags()["addr:street"];
                                        if (street) {
                                            local.addr_coords.push_back({node.location().lat(), node.location().lon()});
                                            local.addr_strings.push_back({housenumber, street});
                                            local.count++;
                                        }
                                    }
                                }
                            }

                            // Flush location batch (sharded index handles its own locking)
                            if (!loc_batch.empty()) {
                                index.set_batch(loc_batch);
                                loc_batch.clear();
                            }

                            uint64_t n = blocks_done.fetch_add(1) + 1;
                            if (n % 1000 == 0) {
                                std::cerr << "  Processed " << n << " node blocks..." << std::endl;
                            }
                        }

                        // Flush remaining
                        if (!loc_batch.empty()) {
                            index.set_batch(loc_batch);
                        }
                    });
                }

                // Reader thread (producer)
                {
                    osmium::io::Reader reader_nodes{input_file, osmium::osm_entity_bits::node};
                    while (auto buf = reader_nodes.read()) {
                        {
                            std::unique_lock<std::mutex> lock(queue_mutex);
                            queue_cv.wait(lock, [&]{ return block_queue.size() < MAX_QUEUE; });
                            block_queue.push_back(std::move(buf));
                        }
                        queue_cv.notify_one();
                    }
                    reader_nodes.close();
                    {
                        std::lock_guard<std::mutex> lock(queue_mutex);
                        reader_done = true;
                    }
                    queue_cv.notify_all();
                }

                for (auto& w : node_workers) w.join();

                // Merge address points
                uint64_t total_addrs = 0;
                for (auto& local : ntld) {
                    for (size_t j = 0; j < local.addr_coords.size(); j++) {
                        uint64_t dummy = 0;
                        add_addr_point(data, local.addr_coords[j].first, local.addr_coords[j].second,
                                       local.addr_strings[j].first.c_str(),
                                       local.addr_strings[j].second.c_str(), dummy);
                    }
                    total_addrs += local.count;
                }
                std::cerr << "  Node processing complete: " << total_addrs
                          << " address points collected." << std::endl;
            }

            // --- Pass 3: ways + relations (parallel way processing) ---
            std::cerr << "  Pass 3: processing ways and areas with " << num_threads
                      << " threads..." << std::endl;

            {
                osmium::io::Reader reader_ways{input_file,
                    osmium::osm_entity_bits::way | osmium::osm_entity_bits::relation};

                // Thread-local data for parallel way processing
                struct ThreadLocalData {
                    std::vector<WayHeader> ways;
                    std::vector<NodeCoord> street_nodes;
                    std::vector<DeferredWay> deferred_ways;
                    std::vector<InterpWay> interp_ways;
                    std::vector<NodeCoord> interp_nodes;
                    std::vector<DeferredInterp> deferred_interps;
                    std::vector<AddrPoint> building_addrs;
                    std::vector<std::pair<double, double>> building_addr_coords; // lat,lng for S2 cell
                    std::vector<std::string> interned_strings; // strings to intern later
                    uint64_t way_count = 0;
                    uint64_t building_addr_count = 0;
                    uint64_t interp_count = 0;
                };

                // Read buffers and dispatch to thread pool
                std::vector<osmium::memory::Buffer> buffer_queue;
                osmium::memory::Buffer buf = reader_ways.read();
                while (buf) {
                    buffer_queue.push_back(std::move(buf));
                    buf = reader_ways.read();
                }
                reader_ways.close();

                std::cerr << "  Read " << buffer_queue.size() << " PBF blocks, processing in parallel..." << std::endl;

                // Process way buffers in parallel
                // Note: area/multipolygon processing must remain sequential due to mp_manager state
                std::vector<ThreadLocalData> tld(num_threads);
                std::atomic<size_t> block_idx{0};
                std::atomic<uint64_t> blocks_done{0};
                size_t total_blocks = buffer_queue.size();

                std::vector<std::thread> workers;
                for (unsigned int t = 0; t < num_threads; t++) {
                    workers.emplace_back([&, t]() {
                        auto& local = tld[t];
                        while (true) {
                            size_t i = block_idx.fetch_add(1);
                            if (i >= total_blocks) break;

                            auto& block = buffer_queue[i];
                            for (const auto& item : block) {
                                if (item.type() == osmium::item_type::way) {
                                    const auto& way = static_cast<const osmium::Way&>(item);

                                    // Resolve node locations (read-only from shared index)
                                    // We need to check if nodes have valid locations
                                    const auto& wnodes = way.nodes();

                                    // Address interpolation
                                    const char* interpolation = way.tags()["addr:interpolation"];
                                    if (interpolation) {
                                        if (wnodes.size() >= 2) {
                                            bool all_valid = true;
                                            for (const auto& nr : wnodes) {
                                                try {
                                                    auto loc = index.get(nr.positive_ref());
                                                    if (!loc.valid()) { all_valid = false; break; }
                                                } catch (...) { all_valid = false; break; }
                                            }
                                            if (all_valid) {
                                                const char* street = way.tags()["addr:street"];
                                                if (street) {
                                                    uint32_t interp_id = static_cast<uint32_t>(local.interp_ways.size());
                                                    uint32_t node_offset = static_cast<uint32_t>(local.interp_nodes.size());

                                                    for (const auto& nr : wnodes) {
                                                        auto loc = index.get(nr.positive_ref());
                                                        local.interp_nodes.push_back({
                                                            static_cast<float>(loc.lat()),
                                                            static_cast<float>(loc.lon())
                                                        });
                                                    }

                                                    uint8_t interp_type = 0;
                                                    if (std::strcmp(interpolation, "even") == 0) interp_type = 1;
                                                    else if (std::strcmp(interpolation, "odd") == 0) interp_type = 2;

                                                    InterpWay iw{};
                                                    iw.node_offset = node_offset;
                                                    iw.node_count = static_cast<uint8_t>(std::min(wnodes.size(), size_t(255)));
                                                    iw.street_id = 0; // placeholder, intern later
                                                    iw.start_number = 0;
                                                    iw.end_number = 0;
                                                    iw.interpolation = interp_type;
                                                    local.interp_ways.push_back(iw);
                                                    local.interned_strings.push_back(street);
                                                    local.deferred_interps.push_back({interp_id, node_offset, iw.node_count});
                                                    local.interp_count++;
                                                }
                                            }
                                        }
                                        continue;
                                    }

                                    // Building addresses
                                    const char* housenumber = way.tags()["addr:housenumber"];
                                    if (housenumber) {
                                        const char* street = way.tags()["addr:street"];
                                        if (street && !wnodes.empty()) {
                                            double sum_lat = 0, sum_lng = 0;
                                            int valid = 0;
                                            for (const auto& nr : wnodes) {
                                                try {
                                                    auto loc = index.get(nr.positive_ref());
                                                    if (loc.valid()) {
                                                        sum_lat += loc.lat();
                                                        sum_lng += loc.lon();
                                                        valid++;
                                                    }
                                                } catch (...) {}
                                            }
                                            if (valid > 0) {
                                                // Store coords for later S2 cell computation
                                                local.building_addr_coords.push_back({sum_lat / valid, sum_lng / valid});
                                                // Placeholder addr point — string IDs filled in during merge
                                                local.building_addrs.push_back({
                                                    static_cast<float>(sum_lat / valid),
                                                    static_cast<float>(sum_lng / valid),
                                                    0, 0 // placeholder string IDs
                                                });
                                                // Store strings for later interning
                                                local.interned_strings.push_back(std::string(housenumber) + "\0" + std::string(street));
                                                local.building_addr_count++;
                                            }
                                        }
                                    }

                                    // Highway ways
                                    const char* highway = way.tags()["highway"];
                                    if (highway && is_included_highway(highway)) {
                                        const char* name = way.tags()["name"];
                                        if (name && wnodes.size() >= 2) {
                                            bool all_valid = true;
                                            for (const auto& nr : wnodes) {
                                                try {
                                                    auto loc = index.get(nr.positive_ref());
                                                    if (!loc.valid()) { all_valid = false; break; }
                                                } catch (...) { all_valid = false; break; }
                                            }
                                            if (all_valid) {
                                                uint32_t way_id = static_cast<uint32_t>(local.ways.size());
                                                uint32_t node_offset = static_cast<uint32_t>(local.street_nodes.size());

                                                for (const auto& nr : wnodes) {
                                                    auto loc = index.get(nr.positive_ref());
                                                    local.street_nodes.push_back({
                                                        static_cast<float>(loc.lat()),
                                                        static_cast<float>(loc.lon())
                                                    });
                                                }

                                                WayHeader header{};
                                                header.node_offset = node_offset;
                                                header.node_count = static_cast<uint8_t>(std::min(wnodes.size(), size_t(255)));
                                                header.name_id = 0; // placeholder, intern later
                                                local.ways.push_back(header);
                                                local.interned_strings.push_back(name);
                                                local.deferred_ways.push_back({way_id, node_offset, header.node_count});
                                                local.way_count++;
                                            }
                                        }
                                    }
                                }
                            }

                            uint64_t done = blocks_done.fetch_add(1) + 1;
                            if (done % 1000 == 0) {
                                std::cerr << "  Processed " << done << "/" << total_blocks << " blocks..." << std::endl;
                            }
                        }
                    });
                }
                for (auto& w : workers) w.join();
                std::cerr << "  Parallel way processing complete." << std::endl;

                // Process areas/multipolygons sequentially (mp_manager requires it)
                // Use a custom location handler that reads from our sharded index
                std::cerr << "  Processing multipolygon areas..." << std::endl;
                {
                    // Create a wrapper handler that resolves locations from sharded index
                    struct ShardedLocationHandler : public osmium::handler::Handler {
                        ShardedIndex& idx;
                        explicit ShardedLocationHandler(ShardedIndex& i) : idx(i) {}
                        void node(osmium::Node& node) {
                            node.set_location(idx.get(node.positive_id()));
                        }
                        void way(osmium::Way& way) {
                            for (auto& nr : way.nodes()) {
                                nr.set_location(idx.get(nr.positive_ref()));
                            }
                        }
                    } sharded_loc_handler{index};

                    osmium::io::Reader reader_rels{input_file};
                    osmium::apply(reader_rels, sharded_loc_handler,
                        mp_manager.handler([&handler](osmium::memory::Buffer&& buffer) {
                            osmium::apply(buffer, handler);
                        }));
                    reader_rels.close();
                }

                // Merge thread-local way/interp data into main ParsedData
                std::cerr << "  Merging thread-local data..." << std::endl;
                uint64_t total_ways = 0, total_building_addrs = 0, total_interps = 0;
                for (auto& local : tld) {
                    uint32_t way_base = static_cast<uint32_t>(data.ways.size());
                    uint32_t node_base = static_cast<uint32_t>(data.street_nodes.size());
                    uint32_t interp_base = static_cast<uint32_t>(data.interp_ways.size());
                    uint32_t interp_node_base = static_cast<uint32_t>(data.interp_nodes.size());

                    // String interning index for this thread's data
                    size_t str_idx = 0;

                    // Merge street ways
                    for (size_t i = 0; i < local.ways.size(); i++) {
                        auto h = local.ways[i];
                        h.node_offset += node_base;
                        h.name_id = data.string_pool.intern(local.interned_strings[str_idx++]);
                        data.ways.push_back(h);
                    }
                    data.street_nodes.insert(data.street_nodes.end(),
                        local.street_nodes.begin(), local.street_nodes.end());

                    // Remap deferred ways
                    for (auto dw : local.deferred_ways) {
                        dw.way_id += way_base;
                        dw.node_offset += node_base;
                        data.deferred_ways.push_back(dw);
                    }

                    // Merge building addresses
                    for (size_t i = 0; i < local.building_addrs.size(); i++) {
                        auto& s = local.interned_strings[str_idx++];
                        auto sep = s.find('\0');
                        std::string hn = s.substr(0, sep);
                        std::string st = s.substr(sep + 1);
                        uint64_t dummy = 0;
                        add_addr_point(data, local.building_addrs[i].lat, local.building_addrs[i].lng,
                                       hn.c_str(), st.c_str(), dummy);
                    }

                    // Merge interpolation ways
                    for (size_t i = 0; i < local.interp_ways.size(); i++) {
                        auto iw = local.interp_ways[i];
                        iw.node_offset += interp_node_base;
                        iw.street_id = data.string_pool.intern(local.interned_strings[str_idx++]);
                        data.interp_ways.push_back(iw);
                    }
                    data.interp_nodes.insert(data.interp_nodes.end(),
                        local.interp_nodes.begin(), local.interp_nodes.end());

                    // Remap deferred interps
                    for (auto di : local.deferred_interps) {
                        di.interp_id += interp_base;
                        di.node_offset += interp_node_base;
                        data.deferred_interps.push_back(di);
                    }

                    total_ways += local.way_count;
                    total_building_addrs += local.building_addr_count;
                    total_interps += local.interp_count;
                }
                std::cerr << "  Merged: " << total_ways << " ways, "
                          << total_building_addrs << " building addrs, "
                          << total_interps << " interps from parallel processing." << std::endl;
            }
        }

        std::cerr << "Done reading:" << std::endl;
        std::cerr << "  " << data.ways.size() << " street ways" << std::endl;
        std::cerr << "  " << data.addr_points.size() << " address points" << std::endl;
        std::cerr << "  " << data.interp_ways.size() << " interpolation ways" << std::endl;
        std::cerr << "  " << data.admin_polygons.size() << " admin polygon rings" << std::endl;

        // Drain admin polygon thread pool (may still be processing)
        std::cerr << "Waiting for admin polygon S2 covering to complete..." << std::endl;
        auto admin_results = admin_pool.drain();
        for (auto& [cell_id, ids] : admin_results) {
            auto& target = data.cell_to_admin[cell_id];
            target.insert(target.end(), ids.begin(), ids.end());
        }
        std::cerr << "Admin polygon S2 covering complete (" << data.cell_to_admin.size() << " cells)." << std::endl;

        // --- Parallel S2 cell computation for ways and interpolation ---
        {
            std::cerr << "Computing S2 cells for ways with " << num_threads << " threads..." << std::endl;

            // Each thread builds its own local cell maps, then we merge
            struct ThreadLocalMaps {
                std::unordered_map<uint64_t, std::vector<uint32_t>> ways;
                std::unordered_map<uint64_t, std::vector<uint32_t>> interps;
            };
            std::vector<ThreadLocalMaps> thread_maps(num_threads);

            // Process streets in parallel
            std::cerr << "  Processing " << data.deferred_ways.size() << " street ways..." << std::endl;
            {
                std::atomic<size_t> way_idx{0};
                std::vector<std::thread> threads;
                for (unsigned int t = 0; t < num_threads; t++) {
                    threads.emplace_back([&, t]() {
                        auto& local = thread_maps[t].ways;
                        while (true) {
                            size_t i = way_idx.fetch_add(1);
                            if (i >= data.deferred_ways.size()) break;
                            const auto& dw = data.deferred_ways[i];
                            std::unordered_set<uint64_t> cells;
                            for (uint8_t j = 0; j + 1 < dw.node_count; j++) {
                                const auto& n1 = data.street_nodes[dw.node_offset + j];
                                const auto& n2 = data.street_nodes[dw.node_offset + j + 1];
                                auto edge_cells = cover_edge(n1.lat, n1.lng, n2.lat, n2.lng);
                                for (const auto& c : edge_cells) {
                                    cells.insert(c.id());
                                }
                            }
                            for (uint64_t cell_id : cells) {
                                local[cell_id].push_back(dw.way_id);
                            }
                        }
                    });
                }
                for (auto& t : threads) t.join();
            }
            std::cerr << "  Street ways done." << std::endl;

            // Process interpolations in parallel
            std::cerr << "  Processing " << data.deferred_interps.size() << " interpolation ways..." << std::endl;
            {
                std::atomic<size_t> interp_idx{0};
                std::vector<std::thread> threads;
                for (unsigned int t = 0; t < num_threads; t++) {
                    threads.emplace_back([&, t]() {
                        auto& local = thread_maps[t].interps;
                        while (true) {
                            size_t i = interp_idx.fetch_add(1);
                            if (i >= data.deferred_interps.size()) break;
                            const auto& di = data.deferred_interps[i];
                            std::unordered_set<uint64_t> cells;
                            for (uint8_t j = 0; j + 1 < di.node_count; j++) {
                                const auto& n1 = data.interp_nodes[di.node_offset + j];
                                const auto& n2 = data.interp_nodes[di.node_offset + j + 1];
                                auto edge_cells = cover_edge(n1.lat, n1.lng, n2.lat, n2.lng);
                                for (const auto& c : edge_cells) {
                                    cells.insert(c.id());
                                }
                            }
                            for (uint64_t cell_id : cells) {
                                local[cell_id].push_back(di.interp_id);
                            }
                        }
                    });
                }
                for (auto& t : threads) t.join();
            }
            std::cerr << "  Interpolation ways done." << std::endl;

            // Merge thread-local maps into data
            std::cerr << "  Merging thread-local results..." << std::endl;
            for (auto& tm : thread_maps) {
                for (auto& [cell_id, ids] : tm.ways) {
                    auto& target = data.cell_to_ways[cell_id];
                    target.insert(target.end(), ids.begin(), ids.end());
                }
                for (auto& [cell_id, ids] : tm.interps) {
                    auto& target = data.cell_to_interps[cell_id];
                    target.insert(target.end(), ids.begin(), ids.end());
                }
            }

            // Free deferred work items
            data.deferred_ways.clear();
            data.deferred_ways.shrink_to_fit();
            data.deferred_interps.clear();
            data.deferred_interps.shrink_to_fit();

            std::cerr << "S2 cell computation complete." << std::endl;
        }

        // Resolve interpolation endpoints
        std::cerr << "Resolving interpolation endpoints..." << std::endl;
        resolve_interpolation_endpoints(data);

        // Deduplicate all cell maps (in parallel)
        std::cerr << "Deduplicating..." << std::endl;
        {
            auto f1 = std::async(std::launch::async, [&]{ deduplicate(data.cell_to_ways); });
            auto f2 = std::async(std::launch::async, [&]{ deduplicate(data.cell_to_addrs); });
            auto f3 = std::async(std::launch::async, [&]{ deduplicate(data.cell_to_interps); });
            auto f4 = std::async(std::launch::async, [&]{ deduplicate(data.cell_to_admin); });
            f1.get(); f2.get(); f3.get(); f4.get();
        }

        // Save cache if requested
        if (!save_cache_path.empty()) {
            serialize_cache(data, save_cache_path);
        }
    }

    // --- Write index files ---
    std::cerr << "Writing index files to " << output_dir << "..." << std::endl;

    if (multi_output) {
        // Write all 3 index modes in parallel (they read shared data, write to separate dirs)
        auto wf1 = std::async(std::launch::async, [&]{ write_index(data, output_dir + "/full", IndexMode::Full); });
        auto wf2 = std::async(std::launch::async, [&]{ write_index(data, output_dir + "/no-addresses", IndexMode::NoAddresses); });
        auto wf3 = std::async(std::launch::async, [&]{ write_index(data, output_dir + "/admin-only", IndexMode::AdminOnly); });
        wf1.get(); wf2.get(); wf3.get();
    } else {
        write_index(data, output_dir, mode);
    }

    if (generate_continents) {
        // Process continents in parallel (each filters independently from shared data)
        std::vector<std::future<void>> continent_futures;
        std::mutex cerr_mutex;
        for (const auto& continent : kContinents) {
            continent_futures.push_back(std::async(std::launch::async, [&, continent]() {
                {
                    std::lock_guard<std::mutex> lock(cerr_mutex);
                    std::cerr << "Filtering for continent: " << continent.name << "..." << std::endl;
                }
                auto subset = filter_by_bbox(data, continent);
                std::string base = output_dir + "/" + continent.name;
                ensure_dir(base);
                if (multi_output) {
                    // Write modes in parallel within each continent
                    auto cf1 = std::async(std::launch::async, [&]{ write_index(subset, base + "/full", IndexMode::Full); });
                    auto cf2 = std::async(std::launch::async, [&]{ write_index(subset, base + "/no-addresses", IndexMode::NoAddresses); });
                    auto cf3 = std::async(std::launch::async, [&]{ write_index(subset, base + "/admin-only", IndexMode::AdminOnly); });
                    cf1.get(); cf2.get(); cf3.get();
                } else {
                    write_index(subset, base, mode);
                }
                {
                    std::lock_guard<std::mutex> lock(cerr_mutex);
                    std::cerr << "Done continent: " << continent.name << std::endl;
                }
            }));
        }
        for (auto& f : continent_futures) {
            f.get();
        }
    }

    std::cerr << "Done." << std::endl;
    return 0;
}
