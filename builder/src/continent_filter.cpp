#include "continent_filter.h"

#include <algorithm>
#include <cstring>
#include <future>
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

    // Filter cell maps in parallel — each builds an independent ID set
    auto filter_cells = [&](const std::unordered_map<uint64_t, std::vector<uint32_t>>& cell_map,
                            bool mask_interior = false) {
        std::unordered_set<uint32_t> ids;
        for (const auto& [cell_id, cell_ids] : cell_map) {
            if (cell_in_bbox(cell_id, bbox)) {
                for (uint32_t id : cell_ids)
                    ids.insert(mask_interior ? (id & ID_MASK) : id);
            }
        }
        return ids;
    };

    auto f_ways = std::async(std::launch::async, [&]{ return filter_cells(full.cell_to_ways); });
    auto f_addrs = std::async(std::launch::async, [&]{ return filter_cells(full.cell_to_addrs); });
    auto f_interps = std::async(std::launch::async, [&]{ return filter_cells(full.cell_to_interps); });
    auto f_admin = std::async(std::launch::async, [&]{ return filter_cells(full.cell_to_admin, true); });

    auto used_way_ids = f_ways.get();
    auto used_addr_ids = f_addrs.get();
    auto used_interp_ids = f_interps.get();
    auto used_admin_ids = f_admin.get();

    // Remap all 4 data types in parallel — each builds its own vectors and remap table
    std::unordered_map<uint32_t, uint32_t> way_remap, addr_remap, interp_remap, admin_remap;

    // Ways
    auto f_remap_ways = std::async(std::launch::async, [&]() {
        std::vector<uint32_t> sorted_ids(used_way_ids.begin(), used_way_ids.end());
        std::sort(sorted_ids.begin(), sorted_ids.end());
        std::vector<WayHeader> ways;
        std::vector<NodeCoord> nodes;
        ways.reserve(sorted_ids.size());
        nodes.reserve(sorted_ids.size() * 5);
        std::unordered_map<uint32_t, uint32_t> remap;
        remap.reserve(sorted_ids.size());
        for (uint32_t old_id : sorted_ids) {
            remap[old_id] = static_cast<uint32_t>(ways.size());
            const auto& w = full.ways[old_id];
            WayHeader nw = w;
            nw.node_offset = static_cast<uint32_t>(nodes.size());
            ways.push_back(nw);
            for (uint8_t n = 0; n < w.node_count; n++)
                nodes.push_back(full.street_nodes[w.node_offset + n]);
        }
        return std::make_tuple(std::move(remap), std::move(ways), std::move(nodes));
    });

    // Addrs
    auto f_remap_addrs = std::async(std::launch::async, [&]() {
        std::vector<uint32_t> sorted_ids(used_addr_ids.begin(), used_addr_ids.end());
        std::sort(sorted_ids.begin(), sorted_ids.end());
        std::vector<AddrPoint> addrs;
        addrs.reserve(sorted_ids.size());
        std::unordered_map<uint32_t, uint32_t> remap;
        remap.reserve(sorted_ids.size());
        for (uint32_t old_id : sorted_ids) {
            remap[old_id] = static_cast<uint32_t>(addrs.size());
            addrs.push_back(full.addr_points[old_id]);
        }
        return std::make_tuple(std::move(remap), std::move(addrs));
    });

    // Interps
    auto f_remap_interps = std::async(std::launch::async, [&]() {
        std::vector<uint32_t> sorted_ids(used_interp_ids.begin(), used_interp_ids.end());
        std::sort(sorted_ids.begin(), sorted_ids.end());
        std::vector<InterpWay> iways;
        std::vector<NodeCoord> inodes;
        iways.reserve(sorted_ids.size());
        std::unordered_map<uint32_t, uint32_t> remap;
        remap.reserve(sorted_ids.size());
        for (uint32_t old_id : sorted_ids) {
            remap[old_id] = static_cast<uint32_t>(iways.size());
            const auto& iw = full.interp_ways[old_id];
            InterpWay niw = iw;
            niw.node_offset = static_cast<uint32_t>(inodes.size());
            iways.push_back(niw);
            for (uint8_t n = 0; n < iw.node_count; n++)
                inodes.push_back(full.interp_nodes[iw.node_offset + n]);
        }
        return std::make_tuple(std::move(remap), std::move(iways), std::move(inodes));
    });

    // Admins
    auto f_remap_admins = std::async(std::launch::async, [&]() {
        std::vector<uint32_t> sorted_ids(used_admin_ids.begin(), used_admin_ids.end());
        std::sort(sorted_ids.begin(), sorted_ids.end());
        std::vector<AdminPolygon> polys;
        std::vector<NodeCoord> verts;
        polys.reserve(sorted_ids.size());
        std::unordered_map<uint32_t, uint32_t> remap;
        remap.reserve(sorted_ids.size());
        for (uint32_t old_id : sorted_ids) {
            remap[old_id] = static_cast<uint32_t>(polys.size());
            const auto& ap = full.admin_polygons[old_id];
            AdminPolygon nap = ap;
            nap.vertex_offset = static_cast<uint32_t>(verts.size());
            polys.push_back(nap);
            for (uint32_t v = 0; v < ap.vertex_count; v++)
                verts.push_back(full.admin_vertices[ap.vertex_offset + v]);
        }
        return std::make_tuple(std::move(remap), std::move(polys), std::move(verts));
    });

    // Collect results
    {
        auto [wr, ways, nodes] = f_remap_ways.get();
        way_remap = std::move(wr);
        out.ways = std::move(ways);
        out.street_nodes = std::move(nodes);
    }
    {
        auto [ar, addrs] = f_remap_addrs.get();
        addr_remap = std::move(ar);
        out.addr_points = std::move(addrs);
    }
    {
        auto [ir, iways, inodes] = f_remap_interps.get();
        interp_remap = std::move(ir);
        out.interp_ways = std::move(iways);
        out.interp_nodes = std::move(inodes);
    }
    {
        auto [ar, polys, verts] = f_remap_admins.get();
        admin_remap = std::move(ar);
        out.admin_polygons = std::move(polys);
        out.admin_vertices = std::move(verts);
    }

    // Remap cell maps in parallel (each builds an independent output map)
    auto remap_cells = [&](const std::unordered_map<uint64_t, std::vector<uint32_t>>& src,
                           const std::unordered_map<uint32_t, uint32_t>& remap,
                           std::unordered_map<uint64_t, std::vector<uint32_t>>& dst,
                           bool handle_flags = false) {
        for (const auto& [cell_id, ids] : src) {
            if (!cell_in_bbox(cell_id, bbox)) continue;
            std::vector<uint32_t> new_ids;
            for (uint32_t id : ids) {
                uint32_t raw_id = handle_flags ? (id & ID_MASK) : id;
                uint32_t flags = handle_flags ? (id & INTERIOR_FLAG) : 0;
                auto it = remap.find(raw_id);
                if (it != remap.end()) new_ids.push_back(it->second | flags);
            }
            if (!new_ids.empty()) dst[cell_id] = std::move(new_ids);
        }
    };

    {
        auto f1 = std::async(std::launch::async, [&]{ remap_cells(full.cell_to_ways, way_remap, out.cell_to_ways); });
        auto f2 = std::async(std::launch::async, [&]{ remap_cells(full.cell_to_addrs, addr_remap, out.cell_to_addrs); });
        auto f3 = std::async(std::launch::async, [&]{ remap_cells(full.cell_to_interps, interp_remap, out.cell_to_interps); });
        auto f4 = std::async(std::launch::async, [&]{ remap_cells(full.cell_to_admin, admin_remap, out.cell_to_admin, true); });
        f1.get(); f2.get(); f3.get(); f4.get();
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
