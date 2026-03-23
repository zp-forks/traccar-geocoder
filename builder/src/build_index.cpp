#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <future>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <osmium/io/pbf_input.hpp>
#include <osmium/visitor.hpp>
#include <sys/mman.h>

#include <s2/s2cell_id.h>

#include "types.h"
#include "string_pool.h"
#include "geometry.h"
#include "parsed_data.h"
#include "s2_helpers.h"
#include "ring_assembly.h"
#include "relation_collector.h"
#include "interpolation.h"
#include "cache.h"
#include "continent_filter.h"
#include "cell_index.h"


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
        // BuildHandler no longer used — parallel processing handles everything

        // Dense array node location index — lockless parallel writes
        // Planet OSM node IDs max ~12.5 billion. 8 bytes per Location = 100GB virtual.
        // MAP_NORESERVE means OS only allocates pages on write (~80GB for 10B nodes).
        static const size_t MAX_NODE_ID = MAX_NODE_ID_DEFAULT;
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

            // --- Pass 1: collect relation members for parallel admin assembly ---
            std::cerr << "  Pass 1: scanning relations..." << std::endl;
            RelationCollector rel_collector(data.collected_relations);

            {
                osmium::io::Reader reader1{input_file, osmium::osm_entity_bits::relation};
                osmium::apply(reader1, rel_collector);
                reader1.close();
            }
            std::cerr << "  Collected " << data.collected_relations.size()
                      << " admin/postal relations for parallel assembly." << std::endl;

            // --- Combined Pass 2+3: nodes then ways in a single PBF read ---
            // PBF guarantees ordering: nodes → ways → relations.
            // We read everything in one pass, processing nodes first, then
            // transitioning to way processing when the first way block arrives.
            log_phase("Pass 1: relation scanning", _pt);

            // Build admin_way_ids BEFORE the combined pass (needed during way processing)
            std::unordered_set<int64_t> admin_way_ids;
            for (const auto& rel : data.collected_relations) {
                for (const auto& [way_id, role] : rel.members) {
                    admin_way_ids.insert(way_id);
                }
            }
            std::cerr << "  Admin assembly needs " << admin_way_ids.size() << " way geometries." << std::endl;

            std::cerr << "  Pass 2+3: processing nodes and ways in single read..." << std::endl;

            {
                // Shared queue infrastructure for both phases
                std::mutex queue_mutex;
                std::condition_variable queue_cv;
                std::deque<osmium::memory::Buffer> block_queue;
                bool reader_done = false;
                const size_t MAX_QUEUE = MAX_BLOCK_QUEUE;

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
                const size_t WAY_MAX_QUEUE = MAX_BLOCK_QUEUE;

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
                                                auto loc = index.get(nr.positive_ref());
                                                if (!loc.valid()) { all_valid = false; break; }
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
                                                auto loc = index.get(nr.positive_ref());
                                                if (loc.valid()) {
                                                    sum_lat += loc.lat();
                                                    sum_lng += loc.lon();
                                                    valid++;
                                                }
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
                                                auto loc = index.get(nr.positive_ref());
                                                if (!loc.valid()) { all_valid = false; break; }
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
                                    if (!wnodes.empty() && admin_way_ids.count(way.id())) {
                                        std::vector<std::pair<double,double>> geom;
                                        for (const auto& nr : wnodes) {
                                            auto loc = index.get(nr.positive_ref());
                                            if (loc.valid()) geom.push_back({loc.lat(), loc.lon()});
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
                                    {
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
                                                            auto loc = index.get(nr.positive_ref());
                                                            if (loc.valid()) {
                                                                verts.push_back({loc.lat(), loc.lon()});
                                                            } else { all_valid = false; break; }
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

                    // Merge way geometries for admin assembly
                    {
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
                {
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
                {
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
                                    for (auto& ring : retry) {
                                        if (!ring_has_duplicate_coords(ring)) {
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
        for (size_t ci = 0; ci < kContinentCount; ci++) {
            const auto& continent = kContinents[ci];
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
