#pragma once

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <s2/s2cell_id.h>

#include "types.h"
#include "parsed_data.h"

// --- S2 cell level globals (set by main arg parsing) ---

extern int kStreetCellLevel;
extern int kAdminCellLevel;
extern int kMaxAdminLevel;

// --- S2 helpers ---

void cover_edge(double lat1, double lng1, double lat2, double lng2,
                std::vector<S2CellId>& out);

std::vector<S2CellId> cover_edge(double lat1, double lng1, double lat2, double lng2);

S2CellId point_to_cell(double lat, double lng);

std::vector<std::pair<S2CellId, bool>> cover_polygon(
    const std::vector<std::pair<double,double>>& vertices);

// --- Thread pool for concurrent admin polygon S2 covering ---

class AdminCoverPool {
public:
    struct WorkItem {
        uint32_t poly_id;
        std::vector<std::pair<double,double>> vertices;
    };

    explicit AdminCoverPool(size_t num_threads);
    ~AdminCoverPool();

    void submit(uint32_t poly_id, std::vector<std::pair<double,double>>&& vertices);
    std::unordered_map<uint64_t, std::vector<uint32_t>> drain();

private:
    void worker_loop();

    std::vector<std::thread> workers_;
    std::deque<WorkItem> queue_;
    std::vector<std::unordered_map<uint64_t, std::vector<uint32_t>>> thread_results_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::condition_variable done_cv_;
    size_t active_workers_ = 0;
    bool stop_;
};

// --- Helper functions that depend on S2 ---

void add_addr_point(ParsedData& data, double lat, double lng,
                    const char* housenumber, const char* street,
                    uint64_t& addr_count_total);

void add_admin_polygon(ParsedData& data,
                       const std::vector<std::pair<double,double>>& vertices_in,
                       const char* name, uint8_t admin_level,
                       const char* country_code,
                       AdminCoverPool* admin_pool = nullptr);
