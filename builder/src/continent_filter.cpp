#include "continent_filter.h"

#include <algorithm>
#include <cstring>
#include <unordered_set>

#include <s2/s2cell_id.h>
#include <s2/s2latlng.h>

const ContinentBBox kContinents[] = {
    {"africa",            -35.0,  37.5,  -25.0,  55.0},
    {"asia",              -12.0,  82.0,   25.0, 180.0},
    {"europe",             35.0,  72.0,  -25.0,  45.0},
    {"north-america",       7.0,  84.0, -170.0, -50.0},
    {"south-america",     -56.0,  13.0,  -82.0, -34.0},
    {"oceania",           -50.0,   0.0,  110.0, 180.0},
    {"central-america",     7.0,  23.5, -120.0, -57.0},
    {"antarctica",        -90.0, -60.0, -180.0, 180.0},
};

const size_t kContinentCount = sizeof(kContinents) / sizeof(kContinents[0]);

static bool cell_in_bbox(uint64_t cell_id, const ContinentBBox& bbox) {
    S2CellId cell(cell_id);
    S2LatLng center = cell.ToLatLng();
    double lat = center.lat().degrees();
    double lng = center.lng().degrees();
    return lat >= bbox.min_lat && lat <= bbox.max_lat &&
           lng >= bbox.min_lng && lng <= bbox.max_lng;
}

ParsedData filter_by_bbox(const ParsedData& full, const ContinentBBox& bbox) {
    ParsedData out;

    std::unordered_set<uint32_t> used_way_ids;
    for (const auto& [cell_id, ids] : full.cell_to_ways) {
        if (cell_in_bbox(cell_id, bbox)) {
            for (uint32_t id : ids) used_way_ids.insert(id);
        }
    }

    std::unordered_set<uint32_t> used_addr_ids;
    for (const auto& [cell_id, ids] : full.cell_to_addrs) {
        if (cell_in_bbox(cell_id, bbox)) {
            for (uint32_t id : ids) used_addr_ids.insert(id);
        }
    }

    std::unordered_set<uint32_t> used_interp_ids;
    for (const auto& [cell_id, ids] : full.cell_to_interps) {
        if (cell_in_bbox(cell_id, bbox)) {
            for (uint32_t id : ids) used_interp_ids.insert(id);
        }
    }

    std::unordered_set<uint32_t> used_admin_ids;
    for (const auto& [cell_id, ids] : full.cell_to_admin) {
        if (cell_in_bbox(cell_id, bbox)) {
            for (uint32_t id : ids) used_admin_ids.insert(id & ID_MASK);
        }
    }

    // Remap ways
    std::unordered_map<uint32_t, uint32_t> way_remap;
    { std::vector<uint32_t> sorted_ids(used_way_ids.begin(), used_way_ids.end());
      std::sort(sorted_ids.begin(), sorted_ids.end());
      for (uint32_t old_id : sorted_ids) {
          uint32_t new_id = static_cast<uint32_t>(out.ways.size());
          way_remap[old_id] = new_id;
          const auto& w = full.ways[old_id];
          WayHeader nw = w;
          nw.node_offset = static_cast<uint32_t>(out.street_nodes.size());
          out.ways.push_back(nw);
          for (uint8_t n = 0; n < w.node_count; n++)
              out.street_nodes.push_back(full.street_nodes[w.node_offset + n]);
      } }

    // Remap addrs
    std::unordered_map<uint32_t, uint32_t> addr_remap;
    { std::vector<uint32_t> sorted_ids(used_addr_ids.begin(), used_addr_ids.end());
      std::sort(sorted_ids.begin(), sorted_ids.end());
      for (uint32_t old_id : sorted_ids) {
          uint32_t new_id = static_cast<uint32_t>(out.addr_points.size());
          addr_remap[old_id] = new_id;
          out.addr_points.push_back(full.addr_points[old_id]);
      } }

    // Remap interps
    std::unordered_map<uint32_t, uint32_t> interp_remap;
    { std::vector<uint32_t> sorted_ids(used_interp_ids.begin(), used_interp_ids.end());
      std::sort(sorted_ids.begin(), sorted_ids.end());
      for (uint32_t old_id : sorted_ids) {
          uint32_t new_id = static_cast<uint32_t>(out.interp_ways.size());
          interp_remap[old_id] = new_id;
          const auto& iw = full.interp_ways[old_id];
          InterpWay niw = iw;
          niw.node_offset = static_cast<uint32_t>(out.interp_nodes.size());
          out.interp_ways.push_back(niw);
          for (uint8_t n = 0; n < iw.node_count; n++)
              out.interp_nodes.push_back(full.interp_nodes[iw.node_offset + n]);
      } }

    // Remap admins
    std::unordered_map<uint32_t, uint32_t> admin_remap;
    { std::vector<uint32_t> sorted_ids(used_admin_ids.begin(), used_admin_ids.end());
      std::sort(sorted_ids.begin(), sorted_ids.end());
      for (uint32_t old_id : sorted_ids) {
          uint32_t new_id = static_cast<uint32_t>(out.admin_polygons.size());
          admin_remap[old_id] = new_id;
          const auto& ap = full.admin_polygons[old_id];
          AdminPolygon nap = ap;
          nap.vertex_offset = static_cast<uint32_t>(out.admin_vertices.size());
          out.admin_polygons.push_back(nap);
          for (uint16_t v = 0; v < ap.vertex_count; v++)
              out.admin_vertices.push_back(full.admin_vertices[ap.vertex_offset + v]);
      } }

    // Remap cell maps
    for (const auto& [cell_id, ids] : full.cell_to_ways) {
        if (!cell_in_bbox(cell_id, bbox)) continue;
        std::vector<uint32_t> new_ids;
        for (uint32_t id : ids) { auto it = way_remap.find(id); if (it != way_remap.end()) new_ids.push_back(it->second); }
        if (!new_ids.empty()) out.cell_to_ways[cell_id] = std::move(new_ids);
    }
    for (const auto& [cell_id, ids] : full.cell_to_addrs) {
        if (!cell_in_bbox(cell_id, bbox)) continue;
        std::vector<uint32_t> new_ids;
        for (uint32_t id : ids) { auto it = addr_remap.find(id); if (it != addr_remap.end()) new_ids.push_back(it->second); }
        if (!new_ids.empty()) out.cell_to_addrs[cell_id] = std::move(new_ids);
    }
    for (const auto& [cell_id, ids] : full.cell_to_interps) {
        if (!cell_in_bbox(cell_id, bbox)) continue;
        std::vector<uint32_t> new_ids;
        for (uint32_t id : ids) { auto it = interp_remap.find(id); if (it != interp_remap.end()) new_ids.push_back(it->second); }
        if (!new_ids.empty()) out.cell_to_interps[cell_id] = std::move(new_ids);
    }
    for (const auto& [cell_id, ids] : full.cell_to_admin) {
        if (!cell_in_bbox(cell_id, bbox)) continue;
        std::vector<uint32_t> new_ids;
        for (uint32_t id : ids) {
            uint32_t raw_id = id & ID_MASK;
            uint32_t flags = id & INTERIOR_FLAG;
            auto it = admin_remap.find(raw_id);
            if (it != admin_remap.end()) new_ids.push_back(it->second | flags);
        }
        if (!new_ids.empty()) out.cell_to_admin[cell_id] = std::move(new_ids);
    }

    // Rebuild compact string pool
    std::unordered_set<uint32_t> used_offsets;
    for (const auto& w : out.ways) used_offsets.insert(w.name_id);
    for (const auto& a : out.addr_points) { used_offsets.insert(a.housenumber_id); used_offsets.insert(a.street_id); }
    for (const auto& iw : out.interp_ways) used_offsets.insert(iw.street_id);
    for (const auto& ap : out.admin_polygons) used_offsets.insert(ap.name_id);

    const auto& old_sp = full.string_pool.data();
    std::unordered_map<uint32_t, uint32_t> string_remap;
    auto& new_sp = out.string_pool.mutable_data();
    new_sp.clear();

    std::vector<uint32_t> sorted_offsets(used_offsets.begin(), used_offsets.end());
    std::sort(sorted_offsets.begin(), sorted_offsets.end());
    for (uint32_t old_off : sorted_offsets) {
        uint32_t new_off = static_cast<uint32_t>(new_sp.size());
        string_remap[old_off] = new_off;
        const char* str = old_sp.data() + old_off;
        size_t len = std::strlen(str);
        new_sp.insert(new_sp.end(), str, str + len + 1);
    }

    for (auto& w : out.ways) w.name_id = string_remap[w.name_id];
    for (auto& a : out.addr_points) { a.housenumber_id = string_remap[a.housenumber_id]; a.street_id = string_remap[a.street_id]; }
    for (auto& iw : out.interp_ways) iw.street_id = string_remap[iw.street_id];
    for (auto& ap : out.admin_polygons) ap.name_id = string_remap[ap.name_id];

    return out;
}
