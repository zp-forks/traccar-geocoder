#include <algorithm>
#include <chrono>
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
#include <sys/mman.h>
#include <sys/stat.h>
// FlexMem no longer used — replaced by DenseIndex mmap
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

// --- Phase timer ---
static void log_phase(const char* name, std::chrono::steady_clock::time_point& t) {
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t).count();
    std::cerr << "  [" << ms/1000 << "." << (ms%1000)/100 << "s] " << name << std::endl;
    t = std::chrono::steady_clock::now();
}

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

// Collected relation data for parallel admin assembly
struct CollectedRelation {
    int64_t id;
    uint8_t admin_level;
    std::string name;
    std::string country_code;
    bool is_postal;
    std::vector<std::pair<int64_t, std::string>> members; // (way_id, role)
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

// Sorted cell-item pair for direct entry writing (avoids hash map)
struct CellItemPair { uint64_t cell_id; uint32_t item_id; };

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

    // Sorted (cell_id, item_id) pairs — kept for direct entry writing
    std::vector<CellItemPair> sorted_way_cells;
    std::vector<CellItemPair> sorted_addr_cells;

    // Collected data for parallel admin assembly
    std::vector<CollectedRelation> collected_relations;
    struct WayGeometry {
        std::vector<std::pair<double,double>> coords;
        int64_t first_node_id;
        int64_t last_node_id;
    };
    std::unordered_map<int64_t, WayGeometry> way_geometries;
};

// --- S2 helpers ---

static int kStreetCellLevel = 17;
static int kAdminCellLevel = 10;
static int kMaxAdminLevel = 0;  // 0 means no filtering

// Fast cover_edge: compute cells covered by a line segment.
// Fast path for short edges (same cell or adjacent) avoids S2RegionCoverer entirely.
// ~80% of street edges are within 1-2 cells, so this eliminates most S2 library calls.
static void cover_edge(double lat1, double lng1, double lat2, double lng2,
                        std::vector<S2CellId>& out) {
    out.clear();

    // Fast: compute cell IDs directly from coordinates
    S2CellId c1 = S2CellId(S2LatLng::FromDegrees(lat1, lng1)).parent(kStreetCellLevel);
    S2CellId c2 = S2CellId(S2LatLng::FromDegrees(lat2, lng2)).parent(kStreetCellLevel);

    // Same cell — most common case for short edges
    if (c1 == c2) {
        out.push_back(c1);
        return;
    }

    // Adjacent cells — check midpoint
    S2CellId cm = S2CellId(S2LatLng::FromDegrees((lat1+lat2)/2, (lng1+lng2)/2)).parent(kStreetCellLevel);
    if (cm == c1 || cm == c2) {
        out.push_back(c1);
        out.push_back(c2);
        return;
    }

    // Longer edge — use S2RegionCoverer for correctness (handles geodesic curves)
    thread_local S2RegionCoverer coverer = []() {
        S2RegionCoverer::Options options;
        options.set_fixed_level(kStreetCellLevel);
        return S2RegionCoverer(options);
    }();

    S2Point p1 = S2LatLng::FromDegrees(lat1, lng1).ToPoint();
    S2Point p2 = S2LatLng::FromDegrees(lat2, lng2).ToPoint();
    if (p1 == p2) { out.push_back(c1); return; }

    std::vector<S2Point> points = {p1, p2};
    S2Polyline polyline(points);
    S2CellUnion covering = coverer.GetCovering(polyline);
    out = std::move(covering.cell_ids());
}

// Legacy overload for code that still uses return-by-value
static std::vector<S2CellId> cover_edge(double lat1, double lng1, double lat2, double lng2) {
    std::vector<S2CellId> result;
    cover_edge(lat1, lng1, lat2, lng2, result);
    return result;
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

// --- Ring self-intersection check (matches osmium's segment crossing detection) ---

static bool segments_intersect(double ax1, double ay1, double ax2, double ay2,
                                double bx1, double by1, double bx2, double by2) {
    // Check if segment (a1,a2) crosses segment (b1,b2)
    // Using cross product orientation test
    auto cross = [](double ox, double oy, double ax, double ay, double bx, double by) -> double {
        return (ax - ox) * (by - oy) - (ay - oy) * (bx - ox);
    };
    double d1 = cross(bx1, by1, bx2, by2, ax1, ay1);
    double d2 = cross(bx1, by1, bx2, by2, ax2, ay2);
    double d3 = cross(ax1, ay1, ax2, ay2, bx1, by1);
    double d4 = cross(ax1, ay1, ax2, ay2, bx2, by2);
    if (((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) &&
        ((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0))) {
        return true;
    }
    return false;
}

static bool ring_has_self_intersection(const std::vector<std::pair<double,double>>& ring) {
    // Sweep-line inspired check: sort segments by min-x, then only check
    // overlapping x-ranges. Much faster than O(n²) for large rings.
    size_t n = ring.size();
    if (n < 4) return false;

    // For small rings, brute force is fine
    if (n <= 32) {
        for (size_t i = 0; i + 1 < n; i++) {
            for (size_t j = i + 2; j + 1 < n; j++) {
                if (j == i + 1 || (i == 0 && j == n - 2)) continue;
                if (segments_intersect(ring[i].first, ring[i].second,
                                        ring[i+1].first, ring[i+1].second,
                                        ring[j].first, ring[j].second,
                                        ring[j+1].first, ring[j+1].second))
                    return true;
            }
        }
        return false;
    }

    // For larger rings, sort segments by min-x and prune non-overlapping
    struct Seg { double min_x, max_x; size_t idx; };
    std::vector<Seg> segs(n - 1);
    for (size_t i = 0; i + 1 < n; i++) {
        double x1 = ring[i].second, x2 = ring[i+1].second; // use lng as x
        segs[i] = {std::min(x1,x2), std::max(x1,x2), i};
    }
    std::sort(segs.begin(), segs.end(), [](const Seg& a, const Seg& b) {
        return a.min_x < b.min_x;
    });

    for (size_t a = 0; a + 1 < segs.size(); a++) {
        for (size_t b = a + 1; b < segs.size(); b++) {
            if (segs[b].min_x > segs[a].max_x) break; // no more x-overlap
            size_t i = segs[a].idx, j = segs[b].idx;
            // Skip adjacent segments
            if (j == i + 1 || i == j + 1) continue;
            if ((i == 0 && j == n - 2) || (j == 0 && i == n - 2)) continue;
            if (segments_intersect(ring[i].first, ring[i].second,
                                    ring[i+1].first, ring[i+1].second,
                                    ring[j].first, ring[j].second,
                                    ring[j+1].first, ring[j+1].second))
                return true;
        }
    }
    return false;
}

// Check if a ring has duplicate coordinates (spikes/figure-8 from merged holes)
// A valid simple polygon visits each coordinate exactly once (except closing point).
static bool ring_has_duplicate_coords(const std::vector<std::pair<double,double>>& ring,
                                       decltype(std::function<int64_t(double,double)>()) coord_key_fn) {
    std::unordered_set<int64_t> seen;
    for (size_t i = 0; i + 1 < ring.size(); i++) { // skip closing point
        int64_t k = coord_key_fn(ring[i].first, ring[i].second);
        if (!seen.insert(k).second) return true; // duplicate
    }
    return false;
}

// --- Parallel admin: assemble outer rings from collected way geometries ---

static std::vector<std::vector<std::pair<double,double>>> assemble_outer_rings(
    const std::vector<std::pair<int64_t, std::string>>& members,
    const std::unordered_map<int64_t, ParsedData::WayGeometry>& way_geoms,
    bool include_all_roles = false)
{
    // Coordinate-based ring assembly: split ways at shared internal nodes,
    // then stitch sub-ways at endpoints with backtracking.
    // This matches osmium's segment-level approach.

    // Coordinate key matching osmium's Location (int32_t nanodegrees)
    auto coord_key = [](double lat, double lng) -> int64_t {
        int32_t ilat = static_cast<int32_t>(lat * 1e7 + (lat >= 0 ? 0.5 : -0.5));
        int32_t ilng = static_cast<int32_t>(lng * 1e7 + (lng >= 0 ? 0.5 : -0.5));
        return (static_cast<int64_t>(ilat) << 32) | static_cast<uint32_t>(ilng);
    };

    // Collect coordinates from member ways.
    // First pass uses outer/empty-role ways only. If that produces 0 rings,
    // retry with ALL ways (matching osmium's check_roles=false fallback) —
    // inner ways sometimes bridge gaps between outer ways.
    std::unordered_map<int64_t, int> coord_count;
    std::vector<const std::vector<std::pair<double,double>>*> way_coords;
    for (const auto& [way_id, role] : members) {
        if (!include_all_roles && role != "outer" && !role.empty()) continue;
        auto it = way_geoms.find(way_id);
        if (it == way_geoms.end() || it->second.coords.empty()) continue;
        way_coords.push_back(&it->second.coords);
        for (const auto& [lat, lng] : it->second.coords) {
            coord_count[coord_key(lat, lng)]++;
        }
    }

    // A coordinate is a "split point" if it appears in 2+ ways' node lists.
    // Endpoints always count, internal nodes that appear in multiple ways need splitting.
    // Also, all way endpoints are natural split points.
    std::unordered_set<int64_t> split_points;
    for (auto* coords : way_coords) {
        split_points.insert(coord_key(coords->front().first, coords->front().second));
        split_points.insert(coord_key(coords->back().first, coords->back().second));
    }
    for (auto& [ck, cnt] : coord_count) {
        if (cnt > 1) split_points.insert(ck);
    }

    // Split each way at split points to create sub-ways
    struct SubWay {
        std::vector<std::pair<double,double>> coords;
    };
    std::vector<SubWay> sub_ways;
    for (auto* coords : way_coords) {
        size_t seg_start = 0;
        for (size_t i = 1; i < coords->size(); i++) {
            int64_t ck = coord_key((*coords)[i].first, (*coords)[i].second);
            if (split_points.count(ck) && i > seg_start) {
                // Create sub-way from seg_start to i (inclusive)
                SubWay sw;
                sw.coords.assign(coords->begin() + seg_start, coords->begin() + i + 1);
                if (sw.coords.size() >= 2) {
                    sub_ways.push_back(std::move(sw));
                }
                seg_start = i;
            }
        }
        // Handle remaining segment (if not already added)
        if (seg_start < coords->size() - 1) {
            SubWay sw;
            sw.coords.assign(coords->begin() + seg_start, coords->end());
            if (sw.coords.size() >= 2) {
                sub_ways.push_back(std::move(sw));
            }
        }
    }

    // Build adjacency by sub-way coordinate endpoints
    struct WayRef {
        int64_t way_id;
        const ParsedData::WayGeometry* geom; // unused for sub-ways
    };
    // We'll adapt the BacktrackState to work with sub_ways directly
    std::unordered_map<int64_t, std::vector<std::pair<size_t, bool>>> coord_adj;
    for (size_t i = 0; i < sub_ways.size(); i++) {
        auto& first = sub_ways[i].coords.front();
        auto& last = sub_ways[i].coords.back();
        coord_adj[coord_key(first.first, first.second)].push_back({i, false});
        coord_adj[coord_key(last.first, last.second)].push_back({i, true});
    }

    std::vector<bool> used(sub_ways.size(), false);
    std::vector<std::vector<std::pair<double,double>>> rings;

    // Ring closure: greedy first (fast), then full backtracking retry from
    // scratch if greedy left unused sub-ways. The second pass gets clean
    // used flags so backtracking makes optimal branch choices.

    // Backtracking with depth limit (for fallback only)
    struct BacktrackState {
        const std::vector<SubWay>& sways;
        const std::unordered_map<int64_t, std::vector<std::pair<size_t, bool>>>& adj;
        std::vector<bool>& used;
        decltype(coord_key)& key_fn;
        int calls;

        bool try_close(int64_t first_key, int64_t last_key,
                       std::vector<std::pair<size_t, bool>>& path, int depth) {
            if (++calls > 100000) return false; // limit total work, not just depth
            if (!path.empty() && first_key == last_key) return true;
            if (depth > 200) return false;

            auto it = adj.find(last_key);
            if (it == adj.end()) return false;

            for (const auto& [wi, is_last] : it->second) {
                if (used[wi]) continue;
                auto& endpoint = is_last ? sways[wi].coords.front() : sways[wi].coords.back();
                int64_t new_key = key_fn(endpoint.first, endpoint.second);

                used[wi] = true;
                path.push_back({wi, is_last});

                if (try_close(first_key, new_key, path, depth + 1)) return true;

                path.pop_back();
                used[wi] = false;
            }
            return false;
        }
    };

    BacktrackState bt{sub_ways, coord_adj, used, coord_key, 0};

    // Pass 1: Greedy (fast, O(n) per ring)
    for (size_t start_idx = 0; start_idx < sub_ways.size(); start_idx++) {
        if (used[start_idx]) continue;

        const auto& sg = sub_ways[start_idx];
        int64_t first_key = coord_key(sg.coords.front().first, sg.coords.front().second);
        int64_t last_key = coord_key(sg.coords.back().first, sg.coords.back().second);

        if (sg.coords.size() >= 4 && first_key == last_key) {
            used[start_idx] = true;
            rings.push_back(sg.coords);
            continue;
        }

        used[start_idx] = true;
        std::vector<size_t> attempt_used = {start_idx};
        std::vector<std::pair<double,double>> ring = sg.coords;
        int64_t current_last = last_key;
        bool extended = true;
        while (extended && current_last != first_key) {
            extended = false;
            auto it = coord_adj.find(current_last);
            if (it == coord_adj.end()) break;
            for (const auto& [wi, is_last] : it->second) {
                if (used[wi]) continue;
                const auto& sw = sub_ways[wi];
                bool reversed = is_last;
                auto& endpoint = reversed ? sw.coords.front() : sw.coords.back();
                used[wi] = true;
                attempt_used.push_back(wi);
                if (!reversed) ring.insert(ring.end(), sw.coords.begin() + 1, sw.coords.end());
                else for (auto rit = sw.coords.rbegin() + 1; rit != sw.coords.rend(); ++rit) ring.push_back(*rit);
                current_last = coord_key(endpoint.first, endpoint.second);
                extended = true;
                break;
            }
        }
        if (ring.size() >= 4 && current_last == first_key &&
            !ring_has_self_intersection(ring)) {
            rings.push_back(std::move(ring));
        } else {
            for (auto wi : attempt_used) used[wi] = false;
        }
    }

    // Pass 2: If greedy left unused sub-ways, retry from scratch with backtracking.
    // Backtracking makes correct branch choices but is slower.
    {
        bool any_unused = false;
        for (size_t i = 0; i < used.size(); i++) {
            if (!used[i]) { any_unused = true; break; }
        }
        if (any_unused && sub_ways.size() <= 30) {
            // Only retry for small relations — large ones stall the build
            std::vector<bool> bt_used(sub_ways.size(), false);
            std::vector<std::vector<std::pair<double,double>>> bt_rings;

            BacktrackState bt2{sub_ways, coord_adj, bt_used, coord_key, 0};
            int total_bt_calls = 0;
            constexpr int MAX_TOTAL_BT_CALLS = 500000; // total budget for entire relation

            for (size_t si = 0; si < sub_ways.size(); si++) {
                if (bt_used[si]) continue;
                if (total_bt_calls >= MAX_TOTAL_BT_CALLS) break; // budget exceeded
                const auto& sg = sub_ways[si];
                int64_t fk = coord_key(sg.coords.front().first, sg.coords.front().second);
                int64_t lk = coord_key(sg.coords.back().first, sg.coords.back().second);
                if (sg.coords.size() >= 4 && fk == lk) {
                    bt_used[si] = true;
                    bt_rings.push_back(sg.coords);
                    continue;
                }
                bt_used[si] = true;
                bt2.calls = 0;
                std::vector<std::pair<size_t, bool>> path;
                if (bt2.try_close(fk, lk, path, 0)) {
                    std::vector<std::pair<double,double>> r = sg.coords;
                    for (const auto& [wi, rev] : path) {
                        const auto& c = sub_ways[wi].coords;
                        if (!rev) r.insert(r.end(), c.begin() + 1, c.end());
                        else for (auto it2 = c.rbegin() + 1; it2 != c.rend(); ++it2) r.push_back(*it2);
                    }
                    if (r.size() >= 4 && !ring_has_self_intersection(r)) {
                        bt_rings.push_back(std::move(r));
                        continue;
                    }
                }
                bt_used[si] = false;
                total_bt_calls += bt2.calls;
            }

            // Keep whichever pass produced more rings
            if (bt_rings.size() > rings.size()) {
                rings = std::move(bt_rings);
            }
        }
    }

    return rings;
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
                               const std::vector<std::pair<double,double>>& vertices_in,
                               const char* name, uint8_t admin_level,
                               const char* country_code,
                               AdminCoverPool* admin_pool = nullptr) {
    // Normalize ring rotation: start at the vertex with smallest (lat, lng)
    // This ensures identical output regardless of ring assembly order.
    auto vertices = vertices_in;
    if (vertices.size() >= 4 &&
        std::fabs(vertices.front().first - vertices.back().first) < 1e-7 &&
        std::fabs(vertices.front().second - vertices.back().second) < 1e-7) {
        vertices.pop_back();
        auto min_it = std::min_element(vertices.begin(), vertices.end());
        std::rotate(vertices.begin(), min_it, vertices.end());
        vertices.push_back(vertices.front());
    }

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

// --- Relation collector for parallel admin assembly (runs during pass 1) ---

struct RelationCollector : public osmium::handler::Handler {
    std::vector<CollectedRelation>& relations;

    explicit RelationCollector(std::vector<CollectedRelation>& r) : relations(r) {}

    void relation(const osmium::Relation& rel) {
        const char* boundary = rel.tags()["boundary"];
        if (!boundary) return;

        bool is_admin = (std::strcmp(boundary, "administrative") == 0);
        bool is_postal = (std::strcmp(boundary, "postal_code") == 0);
        if (!is_admin && !is_postal) return;

        uint8_t admin_level = 0;
        if (is_admin) {
            const char* level_str = rel.tags()["admin_level"];
            if (!level_str) return;
            admin_level = static_cast<uint8_t>(std::atoi(level_str));
            if (admin_level < 2 || admin_level > 10) return;
        } else {
            admin_level = 11; // use 11 for postal codes
        }
        if (kMaxAdminLevel > 0 && admin_level > kMaxAdminLevel) return;

        const char* name = rel.tags()["name"];
        if (!name && is_admin) return;

        // For postal codes, use postal_code tag as name
        std::string name_str;
        if (is_postal) {
            const char* postal_code = rel.tags()["postal_code"];
            if (!postal_code) postal_code = name;
            if (!postal_code) return;
            name_str = postal_code;
        } else {
            name_str = name;
        }

        // Extract country code for level 2 boundaries
        std::string country_code;
        if (admin_level == 2) {
            const char* cc = rel.tags()["ISO3166-1:alpha2"];
            if (cc) country_code = cc;
        }

        // Collect member way IDs and roles
        CollectedRelation cr;
        cr.id = rel.id();
        cr.admin_level = admin_level;
        cr.name = std::move(name_str);
        cr.country_code = std::move(country_code);
        cr.is_postal = is_postal;

        for (const auto& member : rel.members()) {
            if (member.type() == osmium::item_type::way) {
                cr.members.emplace_back(member.ref(), member.role());
            }
        }

        if (!cr.members.empty()) {
            relations.push_back(std::move(cr));
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

// Write entries file from a hash map (used for addr and interp cell maps).
static std::vector<uint32_t> write_entries(
    const std::string& path,
    const std::vector<uint64_t>& sorted_cells,
    const std::unordered_map<uint64_t, std::vector<uint32_t>>& cell_map
) {
    struct CellRef { uint64_t cell_id; const std::vector<uint32_t>* ids; };
    std::vector<CellRef> sorted_refs;
    sorted_refs.reserve(cell_map.size());
    for (auto& [id, ids] : cell_map) {
        sorted_refs.push_back({id, &ids});
    }
    std::sort(sorted_refs.begin(), sorted_refs.end(),
        [](const CellRef& a, const CellRef& b) { return a.cell_id < b.cell_id; });

    std::vector<uint32_t> offsets(sorted_cells.size(), NO_DATA);
    size_t total_size = 0;
    for (auto& r : sorted_refs) total_size += sizeof(uint16_t) + r.ids->size() * sizeof(uint32_t);

    std::vector<char> buf;
    buf.reserve(total_size);
    uint32_t current = 0;
    size_t ri = 0;
    for (uint32_t si = 0; si < sorted_cells.size() && ri < sorted_refs.size(); si++) {
        if (sorted_cells[si] < sorted_refs[ri].cell_id) continue;
        if (sorted_cells[si] > sorted_refs[ri].cell_id) { si--; ri++; continue; }
        offsets[si] = current;
        const auto& ids = *sorted_refs[ri].ids;
        uint16_t count = static_cast<uint16_t>(std::min(ids.size(), size_t(65535)));
        buf.insert(buf.end(), reinterpret_cast<const char*>(&count),
                   reinterpret_cast<const char*>(&count) + sizeof(count));
        buf.insert(buf.end(), reinterpret_cast<const char*>(ids.data()),
                   reinterpret_cast<const char*>(ids.data()) + ids.size() * sizeof(uint32_t));
        current += sizeof(uint16_t) + ids.size() * sizeof(uint32_t);
        ri++;
    }
    std::ofstream f(path, std::ios::binary);
    f.write(buf.data(), buf.size());
    return offsets;
}

// Write entries file from sorted pairs — parallel chunked merge-join.
// Splits sorted_cells into chunks, each thread builds its chunk independently,
// then prefix-sum computes global offsets and buffers are concatenated.
static std::vector<uint32_t> write_entries_from_sorted(
    const std::string& path,
    const std::vector<uint64_t>& sorted_cells,
    const std::vector<CellItemPair>& sorted_pairs
) {
    std::vector<uint32_t> offsets(sorted_cells.size(), NO_DATA);
    if (sorted_pairs.empty()) {
        std::ofstream f(path, std::ios::binary);
        return offsets;
    }

    // Find where each chunk of sorted_cells starts in sorted_pairs
    // using binary search (both are sorted by cell_id)
    unsigned int nthreads = std::min(std::thread::hardware_concurrency(), 32u);
    if (nthreads == 0) nthreads = 4;
    size_t cells_per_chunk = (sorted_cells.size() + nthreads - 1) / nthreads;

    struct ChunkResult {
        std::vector<char> buf;
        size_t cell_start, cell_end; // range in sorted_cells
        uint32_t local_size; // total bytes in this chunk's buffer
    };
    std::vector<ChunkResult> chunks(nthreads);

    // Parallel: each thread processes its range of sorted_cells
    std::vector<std::thread> threads;
    for (unsigned int t = 0; t < nthreads; t++) {
        size_t cs = t * cells_per_chunk;
        size_t ce = std::min(cs + cells_per_chunk, sorted_cells.size());
        if (cs >= sorted_cells.size()) break;

        threads.emplace_back([&, t, cs, ce]() {
            auto& chunk = chunks[t];
            chunk.cell_start = cs;
            chunk.cell_end = ce;
            chunk.local_size = 0;

            // Binary search for where this chunk's first cell appears in sorted_pairs
            size_t pi = std::lower_bound(sorted_pairs.begin(), sorted_pairs.end(),
                sorted_cells[cs], [](const CellItemPair& p, uint64_t id) {
                    return p.cell_id < id;
                }) - sorted_pairs.begin();

            // Merge-join for this chunk
            for (size_t si = cs; si < ce && pi < sorted_pairs.size(); si++) {
                if (sorted_cells[si] < sorted_pairs[pi].cell_id) continue;
                while (pi < sorted_pairs.size() && sorted_pairs[pi].cell_id < sorted_cells[si]) pi++;
                if (pi >= sorted_pairs.size() || sorted_pairs[pi].cell_id != sorted_cells[si]) continue;

                offsets[si] = chunk.local_size; // local offset, adjusted later
                size_t start = pi;
                while (pi < sorted_pairs.size() && sorted_pairs[pi].cell_id == sorted_cells[si]) pi++;
                uint16_t count = static_cast<uint16_t>(std::min(pi - start, size_t(65535)));
                size_t entry_size = sizeof(uint16_t) + (pi - start) * sizeof(uint32_t);
                size_t buf_pos = chunk.buf.size();
                chunk.buf.resize(buf_pos + entry_size);
                memcpy(chunk.buf.data() + buf_pos, &count, sizeof(count));
                for (size_t k = start; k < pi; k++) {
                    memcpy(chunk.buf.data() + buf_pos + sizeof(uint16_t) + (k - start) * sizeof(uint32_t),
                           &sorted_pairs[k].item_id, sizeof(uint32_t));
                }
                chunk.local_size += entry_size;
            }
        });
    }
    for (auto& t : threads) t.join();

    // Prefix-sum: compute global offsets for each chunk
    uint32_t global_offset = 0;
    for (auto& chunk : chunks) {
        for (size_t si = chunk.cell_start; si < chunk.cell_end; si++) {
            if (offsets[si] != NO_DATA) {
                offsets[si] += global_offset;
            }
        }
        global_offset += chunk.local_size;
    }

    // Concatenate buffers and write
    std::vector<char> buf;
    buf.reserve(global_offset);
    for (auto& chunk : chunks) {
        buf.insert(buf.end(), chunk.buf.begin(), chunk.buf.end());
    }

    std::ofstream f(path, std::ios::binary);
    f.write(buf.data(), buf.size());
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
    auto _wt = std::chrono::steady_clock::now();

    bool write_streets = (mode != IndexMode::AdminOnly);
    bool write_addresses = (mode == IndexMode::Full);

    if (write_streets) {
        // Build sorted_geo_cells from sorted pairs where available (avoids hash map iteration).
        // Extract unique cell IDs from each sorted pair vector in parallel, then merge.
        std::vector<uint64_t> sorted_geo_cells;
        {
            auto extract_unique_cells = [](const std::vector<CellItemPair>& pairs) {
                std::vector<uint64_t> cells;
                cells.reserve(pairs.size() / 2);
                for (size_t i = 0; i < pairs.size(); ) {
                    cells.push_back(pairs[i].cell_id);
                    uint64_t cur = pairs[i].cell_id;
                    while (i < pairs.size() && pairs[i].cell_id == cur) i++;
                }
                return cells;
            };
            auto extract_from_map = [](const std::unordered_map<uint64_t, std::vector<uint32_t>>& m) {
                std::vector<uint64_t> cells;
                cells.reserve(m.size());
                for (auto& [id, _] : m) cells.push_back(id);
                std::sort(cells.begin(), cells.end());
                return cells;
            };

            // Extract unique cell IDs in parallel
            std::vector<uint64_t> way_cells, addr_cells, interp_cells;
            {
                auto f1 = std::async(std::launch::async, [&] {
                    return !data.sorted_way_cells.empty()
                        ? extract_unique_cells(data.sorted_way_cells)
                        : extract_from_map(data.cell_to_ways);
                });
                if (write_addresses) {
                    auto f2 = std::async(std::launch::async, [&] {
                        return !data.sorted_addr_cells.empty()
                            ? extract_unique_cells(data.sorted_addr_cells)
                            : extract_from_map(data.cell_to_addrs);
                    });
                    auto f3 = std::async(std::launch::async, [&] {
                        return extract_from_map(data.cell_to_interps);
                    });
                    addr_cells = f2.get();
                    interp_cells = f3.get();
                }
                way_cells = f1.get();
            }

            // Merge sorted unique cell ID vectors
            sorted_geo_cells.reserve(way_cells.size() + addr_cells.size() + interp_cells.size());
            std::merge(way_cells.begin(), way_cells.end(),
                       addr_cells.begin(), addr_cells.end(),
                       std::back_inserter(sorted_geo_cells));
            if (!interp_cells.empty()) {
                std::vector<uint64_t> tmp;
                tmp.reserve(sorted_geo_cells.size() + interp_cells.size());
                std::merge(sorted_geo_cells.begin(), sorted_geo_cells.end(),
                           interp_cells.begin(), interp_cells.end(),
                           std::back_inserter(tmp));
                sorted_geo_cells = std::move(tmp);
            }
            // Deduplicate (merge may have duplicates from overlapping cells)
            sorted_geo_cells.erase(std::unique(sorted_geo_cells.begin(), sorted_geo_cells.end()),
                                    sorted_geo_cells.end());
        }

        // Write entry files in parallel (returns positional offset vectors, not hash maps)
        std::vector<uint32_t> street_offsets, addr_offsets, interp_offsets;
        {
            auto f1 = std::async(std::launch::async, [&]() {
                // Use direct sorted-pair writing for ways (skips hash map extraction + sort)
                if (!data.sorted_way_cells.empty()) {
                    return write_entries_from_sorted(output_dir + "/street_entries.bin", sorted_geo_cells, data.sorted_way_cells);
                }
                return write_entries(output_dir + "/street_entries.bin", sorted_geo_cells, data.cell_to_ways);
            });
            if (write_addresses) {
                auto f2 = std::async(std::launch::async, [&]() {
                    if (!data.sorted_addr_cells.empty()) {
                        return write_entries_from_sorted(output_dir + "/addr_entries.bin", sorted_geo_cells, data.sorted_addr_cells);
                    }
                    return write_entries(output_dir + "/addr_entries.bin", sorted_geo_cells, data.cell_to_addrs);
                });
                auto f3 = std::async(std::launch::async, [&]() {
                    return write_entries(output_dir + "/interp_entries.bin", sorted_geo_cells, data.cell_to_interps);
                });
                addr_offsets = f2.get();
                interp_offsets = f3.get();
            }
            street_offsets = f1.get();
        }

        log_phase("  Write: entry files", _wt);
        // Build geo_cells.bin — parallel buffer fill + single write
        {
            size_t n = sorted_geo_cells.size();
            size_t row_size = sizeof(uint64_t) + 3 * sizeof(uint32_t);
            std::vector<char> buf(n * row_size);

            if (addr_offsets.empty()) addr_offsets.resize(n, NO_DATA);
            if (interp_offsets.empty()) interp_offsets.resize(n, NO_DATA);

            // Parallel buffer fill — split into chunks across threads
            unsigned int nthreads = std::thread::hardware_concurrency();
            if (nthreads == 0) nthreads = 4;
            size_t chunk = (n + nthreads - 1) / nthreads;
            std::vector<std::thread> fill_threads;
            for (unsigned int t = 0; t < nthreads; t++) {
                size_t start = t * chunk;
                size_t end = std::min(start + chunk, n);
                if (start >= n) break;
                fill_threads.emplace_back([&, start, end]() {
                    char* ptr = buf.data() + start * row_size;
                    for (size_t i = start; i < end; i++) {
                        memcpy(ptr, &sorted_geo_cells[i], sizeof(uint64_t)); ptr += sizeof(uint64_t);
                        memcpy(ptr, &street_offsets[i], sizeof(uint32_t)); ptr += sizeof(uint32_t);
                        memcpy(ptr, &addr_offsets[i], sizeof(uint32_t)); ptr += sizeof(uint32_t);
                        memcpy(ptr, &interp_offsets[i], sizeof(uint32_t)); ptr += sizeof(uint32_t);
                    }
                });
            }
            for (auto& t : fill_threads) t.join();

            std::ofstream f(output_dir + "/geo_cells.bin", std::ios::binary);
            f.write(buf.data(), buf.size());
        }

        std::cerr << "geo index: " << sorted_geo_cells.size() << " cells ("
                  << data.ways.size() << " ways, "
                  << data.addr_points.size() << " addrs, "
                  << data.interp_ways.size() << " interps)" << std::endl;
    }

    log_phase("  Write: geo_cells.bin", _wt);
    // Write admin cell index, raw data files, and admin polygons in parallel
    auto admin_future = std::async(std::launch::async, [&] {
        write_cell_index(output_dir + "/admin_cells.bin", output_dir + "/admin_entries.bin", data.cell_to_admin);
        std::cerr << "admin index: " << data.cell_to_admin.size() << " cells, " << data.admin_polygons.size() << " polygons" << std::endl;
    });

    std::vector<std::future<void>> write_futures;

    if (write_streets) {
        write_futures.push_back(std::async(std::launch::async, [&] {
            std::ofstream f(output_dir + "/street_ways.bin", std::ios::binary);
            f.write(reinterpret_cast<const char*>(data.ways.data()), data.ways.size() * sizeof(WayHeader));
        }));
        write_futures.push_back(std::async(std::launch::async, [&] {
            std::ofstream f(output_dir + "/street_nodes.bin", std::ios::binary);
            f.write(reinterpret_cast<const char*>(data.street_nodes.data()), data.street_nodes.size() * sizeof(NodeCoord));
        }));
        if (write_addresses) {
            write_futures.push_back(std::async(std::launch::async, [&] {
                std::ofstream f(output_dir + "/addr_points.bin", std::ios::binary);
                f.write(reinterpret_cast<const char*>(data.addr_points.data()), data.addr_points.size() * sizeof(AddrPoint));
            }));
            write_futures.push_back(std::async(std::launch::async, [&] {
                std::ofstream f(output_dir + "/interp_ways.bin", std::ios::binary);
                f.write(reinterpret_cast<const char*>(data.interp_ways.data()), data.interp_ways.size() * sizeof(InterpWay));
            }));
            write_futures.push_back(std::async(std::launch::async, [&] {
                std::ofstream f(output_dir + "/interp_nodes.bin", std::ios::binary);
                f.write(reinterpret_cast<const char*>(data.interp_nodes.data()), data.interp_nodes.size() * sizeof(NodeCoord));
            }));
        }
    }

    write_futures.push_back(std::async(std::launch::async, [&] {
        std::ofstream f(output_dir + "/admin_polygons.bin", std::ios::binary);
        f.write(reinterpret_cast<const char*>(data.admin_polygons.data()), data.admin_polygons.size() * sizeof(AdminPolygon));
    }));

    write_futures.push_back(std::async(std::launch::async, [&] {
        std::ofstream f(output_dir + "/admin_vertices.bin", std::ios::binary);
        f.write(reinterpret_cast<const char*>(data.admin_vertices.data()), data.admin_vertices.size() * sizeof(NodeCoord));
    }));

    write_futures.push_back(std::async(std::launch::async, [&] {
        std::ofstream f(output_dir + "/strings.bin", std::ios::binary);
        f.write(data.string_pool.data().data(), data.string_pool.data().size());
        std::cerr << "strings.bin: " << data.string_pool.data().size() << " bytes" << std::endl;
    }));

    // Wait for all parallel writes
    log_phase("  Write: parallel data files launched", _wt);
    admin_future.get();
    for (auto& f : write_futures) f.get();
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
        std::cerr << "  --parallel-admin       Use parallel admin boundary assembly (default: on)" << std::endl;
        std::cerr << "  --no-parallel-admin    Disable parallel admin assembly (use osmium fallback)" << std::endl;
        return 1;
    }

    std::string output_dir = argv[1];
    std::vector<std::string> input_files;
    std::string save_cache_path;
    std::string load_cache_path;
    bool multi_output = false;
    bool generate_continents = false;
    bool parallel_admin = true;
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
        } else if (arg == "--parallel-admin") {
            parallel_admin = true;
        } else if (arg == "--no-parallel-admin") {
            parallel_admin = false;
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
    auto _pt = std::chrono::steady_clock::now();

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

        // Dense array node location index — lockless parallel writes
        // Planet OSM node IDs max ~12.5 billion. 8 bytes per Location = 100GB virtual.
        // MAP_NORESERVE means OS only allocates pages on write (~80GB for 10B nodes).
        static const size_t MAX_NODE_ID = 15000000000ULL;
        struct DenseIndex {
            osmium::Location* data;
            size_t capacity;

            DenseIndex() : capacity(MAX_NODE_ID) {
                void* ptr = mmap(nullptr, capacity * sizeof(osmium::Location),
                    PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE,
                    -1, 0);
                if (ptr == MAP_FAILED) {
                    std::cerr << "Error: failed to mmap dense node index ("
                              << (capacity * sizeof(osmium::Location) / (1024*1024*1024)) << "GB virtual)" << std::endl;
                    std::exit(1);
                }
                data = static_cast<osmium::Location*>(ptr);
                std::cerr << "Allocated dense node index: " << (capacity * sizeof(osmium::Location) / (1024*1024*1024))
                          << "GB virtual address space" << std::endl;
            }

            ~DenseIndex() {
                munmap(data, capacity * sizeof(osmium::Location));
            }

            // Lockless — each node ID maps to a unique array slot
            void set(osmium::unsigned_object_id_type id, osmium::Location loc) {
                if (id >= capacity) {
                    std::cerr << "FATAL: node ID " << id << " exceeds dense index capacity " << capacity << std::endl;
                    std::exit(1);
                }
                data[id] = loc;
            }

            osmium::Location get(osmium::unsigned_object_id_type id) const {
                if (id >= capacity) return osmium::Location{};
                return data[id];
            }

            void set_batch(const std::vector<std::pair<osmium::unsigned_object_id_type, osmium::Location>>& batch) {
                for (const auto& [id, loc] : batch) {
                    if (id >= capacity) {
                        std::cerr << "FATAL: node ID " << id << " exceeds dense index capacity " << capacity << std::endl;
                        std::exit(1);
                    }
                    data[id] = loc;
                }
            }
        } index;

        for (const auto& input_file : input_files) {
            std::cerr << "Processing " << input_file << "..." << std::endl;

            // --- Pass 1: collect relation members for multipolygon assembly ---
            std::cerr << "  Pass 1: scanning relations..." << std::endl;

            osmium::area::Assembler::config_type assembler_config;
            osmium::area::MultipolygonManager<osmium::area::Assembler> mp_manager{assembler_config};

            // Also collect admin/postal relation metadata for parallel assembly
            RelationCollector rel_collector(data.collected_relations);

            {
                osmium::io::Reader reader1{input_file, osmium::osm_entity_bits::relation};
                if (parallel_admin) {
                    // Run both: mp_manager (for fallback compatibility) and rel_collector
                    osmium::apply(reader1, mp_manager, rel_collector);
                } else {
                    osmium::apply(reader1, mp_manager);
                }
                reader1.close();
                mp_manager.prepare_for_lookup();
            }
            if (parallel_admin) {
                std::cerr << "  Collected " << data.collected_relations.size()
                          << " admin/postal relations for parallel assembly." << std::endl;
            }

            // --- Combined Pass 2+3: nodes then ways in a single PBF read ---
            // PBF guarantees ordering: nodes → ways → relations.
            // We read everything in one pass, processing nodes first, then
            // transitioning to way processing when the first way block arrives.
            log_phase("Pass 1: relation scanning", _pt);

            // Build admin_way_ids BEFORE the combined pass (needed during way processing)
            std::unordered_set<int64_t> admin_way_ids;
            if (parallel_admin) {
                for (const auto& rel : data.collected_relations) {
                    for (const auto& [way_id, role] : rel.members) {
                        admin_way_ids.insert(way_id);
                    }
                }
                std::cerr << "  Admin assembly needs " << admin_way_ids.size() << " way geometries." << std::endl;
            }

            std::cerr << "  Pass 2+3: processing nodes and ways in single read..." << std::endl;

            {
                // Shared queue infrastructure for both phases
                std::mutex queue_mutex;
                std::condition_variable queue_cv;
                std::deque<osmium::memory::Buffer> block_queue;
                bool reader_done = false;
                const size_t MAX_QUEUE = 64;

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

                // Single reader for all entity types — processes nodes first,
                // then transitions to way processing when first way block arrives.
                osmium::io::Reader combined_reader{input_file};
                bool nodes_done = false;
                osmium::memory::Buffer first_way_buf; // stash the first way block

                // Phase 1: Feed node blocks to node workers
                while (auto buf = combined_reader.read()) {
                    // Check if this buffer contains nodes or ways/relations
                    bool has_node = false;
                    for (const auto& item : buf) {
                        if (item.type() == osmium::item_type::node) { has_node = true; break; }
                    }

                    if (has_node) {
                        std::unique_lock<std::mutex> lock(queue_mutex);
                        queue_cv.wait(lock, [&]{ return block_queue.size() < MAX_QUEUE; });
                        block_queue.push_back(std::move(buf));
                        queue_cv.notify_one();
                    } else {
                        // First non-node block — stash it and transition
                        first_way_buf = std::move(buf);
                        nodes_done = true;
                        break;
                    }
                }

                // Signal node workers done
                {
                    std::lock_guard<std::mutex> lock(queue_mutex);
                    reader_done = true;
                }
                queue_cv.notify_all();
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
                log_phase("Pass 2: node processing", _pt);
                std::cerr << "  Processing ways with " << num_threads << " threads..." << std::endl;

                // --- Phase 2: Way processing using same reader ---

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
                    std::vector<std::string> way_strings;      // way name strings
                    std::vector<std::pair<std::string,std::string>> addr_strings; // building addr {hn, street}
                    std::vector<std::string> interp_strings;   // interp street name strings
                    uint64_t way_count = 0;
                    uint64_t building_addr_count = 0;
                    uint64_t interp_count = 0;
                    // Way geometries for parallel admin assembly
                    struct WayGeomEntry {
                        int64_t way_id;
                        std::vector<std::pair<double,double>> coords;
                        int64_t first_node_id;
                        int64_t last_node_id;
                    };
                    std::vector<WayGeomEntry> way_geoms;
                    // Closed-way admin polygons (ways with boundary=administrative)
                    struct ClosedWayAdmin {
                        std::vector<std::pair<double,double>> vertices;
                        std::string name;
                        uint8_t admin_level;
                        std::string country_code;
                    };
                    std::vector<ClosedWayAdmin> closed_way_admins;
                };

                // Read buffers and dispatch to thread pool
                // Streaming producer-consumer for way blocks
                std::mutex way_queue_mutex;
                std::condition_variable way_queue_cv;
                std::deque<osmium::memory::Buffer> way_block_queue;
                bool way_reader_done = false;
                const size_t WAY_MAX_QUEUE = 64;

                std::vector<ThreadLocalData> tld(num_threads);
                std::atomic<uint64_t> way_blocks_done{0};

                std::vector<std::thread> workers;
                for (unsigned int t = 0; t < num_threads; t++) {
                    workers.emplace_back([&, t]() {
                        auto& local = tld[t];
                        while (true) {
                            osmium::memory::Buffer block;
                            {
                                std::unique_lock<std::mutex> lock(way_queue_mutex);
                                way_queue_cv.wait(lock, [&]{ return !way_block_queue.empty() || way_reader_done; });
                                if (way_block_queue.empty() && way_reader_done) break;
                                block = std::move(way_block_queue.front());
                                way_block_queue.pop_front();
                            }
                            way_queue_cv.notify_one();

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
                                                    local.interp_strings.push_back(street);
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
                                                local.addr_strings.push_back({housenumber, street});
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
                                                local.way_strings.push_back(name);
                                                local.deferred_ways.push_back({way_id, node_offset, header.node_count});
                                                local.way_count++;
                                            }
                                        }
                                    }

                                    // Store way geometry only for admin boundary member ways
                                    if (parallel_admin && !wnodes.empty() && admin_way_ids.count(way.id())) {
                                        std::vector<std::pair<double,double>> geom;
                                        for (const auto& nr : wnodes) {
                                            try {
                                                auto loc = index.get(nr.positive_ref());
                                                if (loc.valid()) geom.push_back({loc.lat(), loc.lon()});
                                            } catch (...) {}
                                        }
                                        if (!geom.empty()) {
                                            int64_t first_nid = wnodes.front().positive_ref();
                                            int64_t last_nid = wnodes.back().positive_ref();
                                            local.way_geoms.push_back({way.id(), std::move(geom), first_nid, last_nid});
                                        }
                                    }

                                    // Handle closed ways that are admin boundaries themselves
                                    // Osmium creates areas from closed ways independently of
                                    // multipolygon relations, even if the way is a relation member.
                                    if (parallel_admin) {
                                        const char* boundary = way.tags()["boundary"];
                                        if (boundary) {
                                            bool is_admin = (std::strcmp(boundary, "administrative") == 0);
                                            bool is_postal = (std::strcmp(boundary, "postal_code") == 0);
                                            if ((is_admin || is_postal) && wnodes.size() >= 4 &&
                                                wnodes.front().ref() == wnodes.back().ref()) {
                                                // Closed way forming an admin polygon
                                                uint8_t al = 0;
                                                if (is_admin) {
                                                    const char* level_str = way.tags()["admin_level"];
                                                    if (level_str) al = static_cast<uint8_t>(std::atoi(level_str));
                                                } else {
                                                    al = 11;
                                                }
                                                int max_al = is_postal ? 11 : 10;
                                                if (al >= 2 && al <= max_al && (kMaxAdminLevel == 0 || al <= kMaxAdminLevel)) {
                                                    const char* aname = way.tags()["name"];
                                                    if (!aname && is_admin) continue; // admin needs name
                                                    std::string name_str;
                                                    if (is_postal) {
                                                        const char* pc = way.tags()["postal_code"];
                                                        if (!pc) pc = aname;
                                                        if (!pc) continue; // postal needs postal_code or name
                                                        name_str = pc;
                                                    } else {
                                                        name_str = aname;
                                                    }
                                                    {
                                                        std::vector<std::pair<double,double>> verts;
                                                        bool all_valid = true;
                                                        for (const auto& nr : wnodes) {
                                                            try {
                                                                auto loc = index.get(nr.positive_ref());
                                                                if (loc.valid()) {
                                                                    verts.push_back({loc.lat(), loc.lon()});
                                                                } else { all_valid = false; break; }
                                                            } catch (...) { all_valid = false; break; }
                                                        }
                                                        if (all_valid && verts.size() >= 3) {
                                                            std::string cc;
                                                            if (al == 2) {
                                                                const char* iso = way.tags()["ISO3166-1:alpha2"];
                                                                if (iso) cc = iso;
                                                            }
                                                            local.closed_way_admins.push_back({
                                                                std::move(verts),
                                                                std::move(name_str),
                                                                al, std::move(cc)
                                                            });
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            uint64_t done = way_blocks_done.fetch_add(1) + 1;
                            if (done % 1000 == 0) {
                                std::cerr << "  Processed " << done << " way blocks..." << std::endl;
                            }
                        }
                    });
                }

                // Way reader: use the combined_reader (continuing from where nodes left off)
                {
                    // First, push the stashed way buffer
                    if (first_way_buf) {
                        std::unique_lock<std::mutex> lock(way_queue_mutex);
                        way_queue_cv.wait(lock, [&]{ return way_block_queue.size() < WAY_MAX_QUEUE; });
                        way_block_queue.push_back(std::move(first_way_buf));
                        way_queue_cv.notify_one();
                    }
                    // Continue reading remaining blocks from the same reader
                    while (auto buf = combined_reader.read()) {
                        {
                            std::unique_lock<std::mutex> lock(way_queue_mutex);
                            way_queue_cv.wait(lock, [&]{ return way_block_queue.size() < WAY_MAX_QUEUE; });
                            way_block_queue.push_back(std::move(buf));
                        }
                        way_queue_cv.notify_one();
                    }
                    combined_reader.close();
                    {
                        std::lock_guard<std::mutex> lock(way_queue_mutex);
                        way_reader_done = true;
                    }
                    way_queue_cv.notify_all();
                }

                for (auto& w : workers) w.join();
                std::cerr << "  Parallel way processing complete." << std::endl;

                // Process areas/multipolygons — sequential fallback path
                if (!parallel_admin) {
                    // Use a custom location handler that reads from our dense index
                    std::cerr << "  Processing multipolygon areas (sequential)..." << std::endl;
                    {
                        struct DenseLocationHandler : public osmium::handler::Handler {
                            DenseIndex& idx;
                            explicit DenseLocationHandler(DenseIndex& i) : idx(i) {}
                            void node(osmium::Node& node) {
                                node.set_location(idx.get(node.positive_id()));
                            }
                            void way(osmium::Way& way) {
                                for (auto& nr : way.nodes()) {
                                    nr.set_location(idx.get(nr.positive_ref()));
                                }
                            }
                        } dense_loc_handler{index};

                        osmium::io::Reader reader_rels{input_file};
                        osmium::apply(reader_rels, dense_loc_handler,
                            mp_manager.handler([&handler](osmium::memory::Buffer&& buffer) {
                                osmium::apply(buffer, handler);
                            }));
                        reader_rels.close();
                    }
                }

                // Merge thread-local way/interp data into main ParsedData
                std::cerr << "  Merging thread-local data..." << std::endl;
                uint64_t total_ways = 0, total_building_addrs = 0, total_interps = 0;
                for (auto& local : tld) {
                    uint32_t way_base = static_cast<uint32_t>(data.ways.size());
                    uint32_t node_base = static_cast<uint32_t>(data.street_nodes.size());
                    uint32_t interp_base = static_cast<uint32_t>(data.interp_ways.size());
                    uint32_t interp_node_base = static_cast<uint32_t>(data.interp_nodes.size());

                    // Merge street ways
                    for (size_t i = 0; i < local.ways.size(); i++) {
                        auto h = local.ways[i];
                        h.node_offset += node_base;
                        h.name_id = data.string_pool.intern(local.way_strings[i]);
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
                        uint64_t dummy = 0;
                        add_addr_point(data, local.building_addrs[i].lat, local.building_addrs[i].lng,
                                       local.addr_strings[i].first.c_str(),
                                       local.addr_strings[i].second.c_str(), dummy);
                    }

                    // Merge interpolation ways
                    for (size_t i = 0; i < local.interp_ways.size(); i++) {
                        auto iw = local.interp_ways[i];
                        iw.node_offset += interp_node_base;
                        iw.street_id = data.string_pool.intern(local.interp_strings[i]);
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

                    // Merge way geometries for parallel admin assembly
                    if (parallel_admin) {
                        for (auto& wg : local.way_geoms) {
                            ParsedData::WayGeometry g;
                            g.coords = std::move(wg.coords);
                            g.first_node_id = wg.first_node_id;
                            g.last_node_id = wg.last_node_id;
                            data.way_geometries[wg.way_id] = std::move(g);
                        }
                        local.way_geoms.clear();
                        local.way_geoms.shrink_to_fit();
                    }
                }
                std::cerr << "  Merged: " << total_ways << " ways, "
                          << total_building_addrs << " building addrs, "
                          << total_interps << " interps from parallel processing." << std::endl;

                // --- Merge closed-way admin polygons ---
                if (parallel_admin) {
                    uint64_t closed_way_admin_count = 0;
                    for (auto& local : tld) {
                        for (auto& cwa : local.closed_way_admins) {
                            const char* cc = cwa.country_code.empty() ? nullptr : cwa.country_code.c_str();
                            add_admin_polygon(data, cwa.vertices, cwa.name.c_str(),
                                              cwa.admin_level, cc, &admin_pool);
                            closed_way_admin_count++;
                        }
                        local.closed_way_admins.clear();
                    }
                    if (closed_way_admin_count > 0) {
                        std::cerr << "  Added " << closed_way_admin_count
                                  << " admin polygons from closed ways." << std::endl;
                    }
                }

                // --- Parallel admin boundary assembly ---
                if (parallel_admin) {
                    log_phase("Pass 2b: way processing", _pt);
                    std::cerr << "  Assembling admin polygons in parallel ("
                              << data.collected_relations.size() << " relations, "
                              << data.way_geometries.size() << " way geometries)..." << std::endl;

                    // Thread-local admin results (to avoid locking add_admin_polygon)
                    struct AdminResult {
                        std::vector<std::pair<double,double>> vertices;
                        std::string name;
                        uint8_t admin_level;
                        std::string country_code;
                    };

                    std::vector<std::vector<AdminResult>> thread_admin_results(num_threads);
                    std::atomic<size_t> rel_idx{0};
                    std::atomic<uint64_t> assembled_count{0};

                    std::vector<std::thread> admin_workers;
                    for (unsigned int t = 0; t < num_threads; t++) {
                        admin_workers.emplace_back([&, t]() {
                            auto& local_results = thread_admin_results[t];
                            while (true) {
                                size_t i = rel_idx.fetch_add(1);
                                if (i >= data.collected_relations.size()) break;

                                const auto& rel = data.collected_relations[i];

                                // Skip level 2 border-line relations — these have names like
                                // "Deutschland - Österreich" (two countries separated by
                                // " - " or " — "). They are boundary lines, not country
                                // polygons, and produce self-intersecting geometry.
                                if (rel.admin_level == 2 && rel.country_code.empty() &&
                                    (rel.name.find(" - ") != std::string::npos ||
                                     rel.name.find(" \xe2\x80\x94 ") != std::string::npos)) { // " — " (em dash)
                                    assembled_count.fetch_add(1);
                                    continue;
                                }

                                // Skip relations with missing member ways —
                                // these cross the extract boundary and would produce
                                // incorrect partial polygons
                                bool has_missing = false;
                                for (const auto& [way_id, role] : rel.members) {
                                    if (data.way_geometries.find(way_id) == data.way_geometries.end()) {
                                        has_missing = true;
                                        break;
                                    }
                                }
                                if (has_missing) {
                                    assembled_count.fetch_add(1);
                                    continue;
                                }

                                auto rings = assemble_outer_rings(rel.members, data.way_geometries);
                                // If outer-only assembly failed, retry with all ways.
                                // Osmium ignores roles during assembly (check_roles=false) —
                                // inner ways sometimes form the boundary or bridge gaps.
                                // Filter retry results: discard rings with duplicate coords
                                // (figure-8 shapes from merging holes with outer boundary).
                                if (rings.empty()) {
                                    auto retry = assemble_outer_rings(rel.members, data.way_geometries, true);
                                    auto ck = [](double lat, double lng) -> int64_t {
                                        int32_t ilat = static_cast<int32_t>(lat * 1e7 + (lat >= 0 ? 0.5 : -0.5));
                                        int32_t ilng = static_cast<int32_t>(lng * 1e7 + (lng >= 0 ? 0.5 : -0.5));
                                        return (static_cast<int64_t>(ilat) << 32) | static_cast<uint32_t>(ilng);
                                    };
                                    for (auto& ring : retry) {
                                        if (!ring_has_duplicate_coords(ring, ck)) {
                                            rings.push_back(std::move(ring));
                                        }
                                    }
                                }

                                // Diagnostic: log relations with 0 rings produced
                                if (rings.empty() && !rel.members.empty()) {
                                    int total_ways = 0, found = 0, missing = 0;
                                    for (const auto& [way_id, role] : rel.members) {
                                        total_ways++;
                                        auto it = data.way_geometries.find(way_id);
                                        if (it != data.way_geometries.end() && !it->second.coords.empty()) {
                                            found++;
                                        } else {
                                            missing++;
                                        }
                                    }
                                    if (missing == 0 && found > 0) {
                                        std::string detail = "  DEBUG '" + rel.name + "': ways:";
                                        for (const auto& [way_id, role] : rel.members) {
                                            auto it = data.way_geometries.find(way_id);
                                            if (it == data.way_geometries.end()) continue;
                                            detail += " [w" + std::to_string(way_id) +
                                                      " first=" + std::to_string(it->second.first_node_id) +
                                                      " last=" + std::to_string(it->second.last_node_id) +
                                                      " n=" + std::to_string(it->second.coords.size()) + "]";
                                        }
                                        std::cerr << detail << std::endl;
                                    }
                                    std::cerr << "  WARN: relation '" << rel.name
                                              << "' (level " << (int)rel.admin_level
                                              << ", " << rel.members.size() << " members): "
                                              << "0 rings from " << total_ways << " ways ("
                                              << found << " found, " << missing << " missing)" << std::endl;
                                }

                                for (auto& ring : rings) {
                                    if (ring.size() >= 3) {
                                        AdminResult ar;
                                        ar.vertices = std::move(ring);
                                        ar.name = rel.name;
                                        ar.admin_level = rel.admin_level;
                                        ar.country_code = rel.country_code;
                                        local_results.push_back(std::move(ar));
                                    }
                                }

                                uint64_t done = assembled_count.fetch_add(1) + 1;
                                if (done % 10000 == 0) {
                                    std::cerr << "  Assembled " << done / 1000
                                              << "K admin relations..." << std::endl;
                                }
                            }
                        });
                    }
                    for (auto& w : admin_workers) w.join();

                    // Merge thread-local admin results into ParsedData (sequential, thread-safe)
                    uint64_t total_admin_rings = 0;
                    for (auto& local_results : thread_admin_results) {
                        for (auto& ar : local_results) {
                            const char* cc = ar.country_code.empty() ? nullptr : ar.country_code.c_str();
                            add_admin_polygon(data, ar.vertices, ar.name.c_str(),
                                              ar.admin_level, cc, &admin_pool);
                            total_admin_rings++;
                        }
                    }
                    std::cerr << "  Parallel admin assembly complete: "
                              << total_admin_rings << " polygon rings from "
                              << data.collected_relations.size() << " relations." << std::endl;

                    // Free collected data
                    data.collected_relations.clear();
                    data.collected_relations.shrink_to_fit();
                    data.way_geometries.clear();
                    std::unordered_map<int64_t, ParsedData::WayGeometry>().swap(data.way_geometries);
                }
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
            log_phase("Admin assembly", _pt);
            std::cerr << "Computing S2 cells for ways with " << num_threads << " threads..." << std::endl;

            // Flat-array approach: threads emit (cell_id, item_id) pairs into
            // thread-local vectors, then we sort globally and group by cell_id.
            // This avoids hash maps entirely — sort is cache-friendly O(n log n).

            struct CellItem { uint64_t cell_id; uint32_t item_id; };

            auto _s2t = std::chrono::steady_clock::now();

            // Process streets: emit (cell_id, way_id) pairs
            std::cerr << "  Processing " << data.deferred_ways.size() << " street ways..." << std::endl;
            std::vector<std::vector<CellItem>> way_pairs(num_threads);
            {
                std::atomic<size_t> way_idx{0};
                std::vector<std::thread> threads;
                for (unsigned int t = 0; t < num_threads; t++) {
                    threads.emplace_back([&, t]() {
                        auto& local = way_pairs[t];
                        local.reserve(data.deferred_ways.size() / num_threads * 3);
                        std::vector<S2CellId> edge_cells;
                        std::vector<uint64_t> way_cells;
                        while (true) {
                            size_t i = way_idx.fetch_add(1);
                            if (i >= data.deferred_ways.size()) break;
                            const auto& dw = data.deferred_ways[i];
                            way_cells.clear();
                            for (uint8_t j = 0; j + 1 < dw.node_count; j++) {
                                const auto& n1 = data.street_nodes[dw.node_offset + j];
                                const auto& n2 = data.street_nodes[dw.node_offset + j + 1];
                                cover_edge(n1.lat, n1.lng, n2.lat, n2.lng, edge_cells);
                                for (const auto& c : edge_cells) way_cells.push_back(c.id());
                            }
                            std::sort(way_cells.begin(), way_cells.end());
                            way_cells.erase(std::unique(way_cells.begin(), way_cells.end()), way_cells.end());
                            for (uint64_t cell_id : way_cells) {
                                local.push_back({cell_id, dw.way_id});
                            }
                        }
                    });
                }
                for (auto& t : threads) t.join();
            }
            log_phase("  S2: street ways (parallel)", _s2t);

            // Process interpolations: emit (cell_id, interp_id) pairs
            std::cerr << "  Processing " << data.deferred_interps.size() << " interpolation ways..." << std::endl;
            std::vector<std::vector<CellItem>> interp_pairs(num_threads);
            {
                std::atomic<size_t> interp_idx{0};
                std::vector<std::thread> threads;
                for (unsigned int t = 0; t < num_threads; t++) {
                    threads.emplace_back([&, t]() {
                        auto& local = interp_pairs[t];
                        std::vector<S2CellId> edge_cells;
                        std::vector<uint64_t> way_cells;
                        while (true) {
                            size_t i = interp_idx.fetch_add(1);
                            if (i >= data.deferred_interps.size()) break;
                            const auto& di = data.deferred_interps[i];
                            way_cells.clear();
                            for (uint8_t j = 0; j + 1 < di.node_count; j++) {
                                const auto& n1 = data.interp_nodes[di.node_offset + j];
                                const auto& n2 = data.interp_nodes[di.node_offset + j + 1];
                                cover_edge(n1.lat, n1.lng, n2.lat, n2.lng, edge_cells);
                                for (const auto& c : edge_cells) way_cells.push_back(c.id());
                            }
                            std::sort(way_cells.begin(), way_cells.end());
                            way_cells.erase(std::unique(way_cells.begin(), way_cells.end()), way_cells.end());
                            for (uint64_t cell_id : way_cells) {
                                local.push_back({cell_id, di.interp_id});
                            }
                        }
                    });
                }
                for (auto& t : threads) t.join();
            }
            log_phase("  S2: interp ways (parallel)", _s2t);

            // Merge thread-local pairs into single vectors, sort, build cell maps
            std::cerr << "  Sorting and grouping cell pairs..." << std::endl;
            // Concatenate + sort helper for flat CellItem pairs
            // Parallel sort each thread's pairs, then k-way merge directly into
            // hash map + sorted output. No intermediate merged vector needed.
            auto parallel_sort_and_build = [](
                std::vector<std::vector<CellItem>>& thread_pairs,
                std::unordered_map<uint64_t, std::vector<uint32_t>>& cell_map,
                std::vector<CellItemPair>& sorted_out
            ) {
                // Step 1: Convert + sort each thread's data in parallel
                size_t total = 0;
                for (auto& v : thread_pairs) total += v.size();

                std::vector<std::vector<CellItemPair>> chunks(thread_pairs.size());
                {
                    std::vector<std::thread> sort_threads;
                    for (size_t t = 0; t < thread_pairs.size(); t++) {
                        sort_threads.emplace_back([&, t]() {
                            auto& src = thread_pairs[t];
                            auto& dst = chunks[t];
                            dst.reserve(src.size());
                            for (auto& ci : src) dst.push_back({ci.cell_id, ci.item_id});
                            src.clear(); src.shrink_to_fit();
                            std::sort(dst.begin(), dst.end(), [](const CellItemPair& a, const CellItemPair& b) {
                                return a.cell_id < b.cell_id || (a.cell_id == b.cell_id && a.item_id < b.item_id);
                            });
                        });
                    }
                    for (auto& t : sort_threads) t.join();
                }

                // Step 2: Concatenate sorted chunks and merge using parallel tree.
                // Each chunk is already sorted. Record run boundaries, concatenate,
                // then merge adjacent runs pairwise in parallel at each level.
                auto cmp = [](const CellItemPair& a, const CellItemPair& b) {
                    return a.cell_id < b.cell_id || (a.cell_id == b.cell_id && a.item_id < b.item_id);
                };

                // Record run boundaries before concatenation
                std::vector<size_t> run_bounds = {0};
                sorted_out.clear();
                sorted_out.reserve(total);
                for (auto& chunk : chunks) {
                    sorted_out.insert(sorted_out.end(), chunk.begin(), chunk.end());
                    run_bounds.push_back(sorted_out.size());
                    chunk.clear(); chunk.shrink_to_fit();
                }

                // Parallel tree merge: at each level, merge adjacent run pairs in parallel.
                // Level 0: merge runs (0,1), (2,3), ... → N/2 runs
                // Level 1: merge runs (01,23), (45,67), ... → N/4 runs
                // ... until one sorted run remains.
                while (run_bounds.size() > 2) {
                    std::vector<size_t> new_bounds = {0};
                    std::vector<std::thread> merge_threads;
                    for (size_t i = 0; i + 2 < run_bounds.size(); i += 2) {
                        size_t left = run_bounds[i];
                        size_t mid = run_bounds[i + 1];
                        size_t right = run_bounds[i + 2];
                        merge_threads.emplace_back([&, left, mid, right] {
                            std::inplace_merge(sorted_out.begin() + left,
                                               sorted_out.begin() + mid,
                                               sorted_out.begin() + right, cmp);
                        });
                        new_bounds.push_back(right);
                    }
                    // If odd number of runs, carry the last one forward
                    if (run_bounds.size() % 2 == 0) {
                        new_bounds.push_back(run_bounds.back());
                    }
                    for (auto& t : merge_threads) t.join();
                    run_bounds = std::move(new_bounds);
                }

                // Skip building hash map — sorted_out is used directly for writing.
                // cell_map stays empty (only needed for cache/continent modes).
            };


            // Sort each thread's pairs in parallel, then k-way merge directly
            // into hash map + sorted output. No intermediate merged vector.
            auto f_ways = std::async(std::launch::async, [&] {
                parallel_sort_and_build(way_pairs, data.cell_to_ways, data.sorted_way_cells);
            });
            auto f_interps = std::async(std::launch::async, [&] {
                std::vector<CellItemPair> interp_sorted; // not needed for writing
                parallel_sort_and_build(interp_pairs, data.cell_to_interps, interp_sorted);
            });
            f_ways.get();
            f_interps.get();
            log_phase("  S2: sort + group into cell maps", _s2t);

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

        // Deduplicate + convert to sorted pairs for fast writing
        log_phase("S2 cell computation", _pt);
        std::cerr << "Deduplicating + sorting for write..." << std::endl;
        {
            // Convert addr hash map to sorted pairs.
            // Sort cell IDs only (30M unique), then build pairs in sorted order.
            // Much faster than sorting all 160M pairs.
            auto f2 = std::async(std::launch::async, [&] {
                // Extract and sort unique cell IDs
                std::vector<uint64_t> sorted_cells;
                sorted_cells.reserve(data.cell_to_addrs.size());
                size_t total_pairs = 0;
                for (auto& [cell_id, ids] : data.cell_to_addrs) {
                    sorted_cells.push_back(cell_id);
                    total_pairs += ids.size();
                }
                std::sort(sorted_cells.begin(), sorted_cells.end());

                // Build sorted pairs by iterating sorted cell IDs
                std::vector<CellItemPair> pairs;
                pairs.reserve(total_pairs);
                for (uint64_t cell_id : sorted_cells) {
                    auto& ids = data.cell_to_addrs[cell_id];
                    std::sort(ids.begin(), ids.end()); // sort within cell
                    ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
                    for (auto id : ids) pairs.push_back({cell_id, id});
                }
                data.sorted_addr_cells = std::move(pairs);
            });
            auto f4 = std::async(std::launch::async, [&]{ deduplicate(data.cell_to_admin); });
            f2.get(); f4.get();
        }

        // Save cache if requested
        if (!save_cache_path.empty()) {
            serialize_cache(data, save_cache_path);
        }
    }

    // --- Write index files ---
    log_phase("Deduplication", _pt);
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

    log_phase("Index file writing", _pt);
    std::cerr << "Done." << std::endl;
    return 0;
}
