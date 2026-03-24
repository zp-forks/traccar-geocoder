#pragma once

#include "parsed_data.h"

struct ContinentBBox {
    const char* name;
    double min_lat, max_lat, min_lng, max_lng;
};

extern const ContinentBBox kContinents[];
extern const size_t kContinentCount;

// Filter with optional pre-computed cell masks for faster continent lookups.
// If cell_mask is provided, uses bitmask lookup instead of cell_in_bbox.
ParsedData filter_by_bbox(const ParsedData& full, const ContinentBBox& bbox,
                          const std::unordered_map<uint64_t, uint8_t>* cell_mask = nullptr,
                          uint8_t continent_bit = 0);
