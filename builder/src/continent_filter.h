#pragma once

#include "parsed_data.h"

struct ContinentBBox {
    const char* name;
    double min_lat, max_lat, min_lng, max_lng;
};

extern const ContinentBBox kContinents[];
extern const size_t kContinentCount;

ParsedData filter_by_bbox(const ParsedData& full, const ContinentBBox& bbox);
