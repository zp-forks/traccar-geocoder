// geocoder-diff: Compare two deterministic geocoder builds, produce a .gcpatch.
//
// Three-level remapping for minimal patches:
// 1. String remap: old string offsets → new string offsets
// 2. Record ID remap: match records by content, map old_id → new_id
// 3. Entry remap: apply record ID remap to entry files + rebuild cell indexes
//
// Usage: geocoder-diff <old-dir> <new-dir> -o <patch-file>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <string>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#include "patch_format.h"

// --- String remap ---

static std::unordered_map<uint32_t, uint32_t> build_string_remap(
    const std::vector<char>& old_pool, const std::vector<char>& new_pool)
{
    std::unordered_map<std::string, uint32_t> new_idx;
    size_t pos = 0;
    while (pos < new_pool.size()) {
        const char* s = new_pool.data() + pos;
        size_t len = strlen(s);
        new_idx[std::string(s, len)] = static_cast<uint32_t>(pos);
        pos += len + 1;
    }
    std::unordered_map<uint32_t, uint32_t> remap;
    pos = 0;
    while (pos < old_pool.size()) {
        const char* s = old_pool.data() + pos;
        size_t len = strlen(s);
        auto it = new_idx.find(std::string(s, len));
        if (it != new_idx.end())
            remap[static_cast<uint32_t>(pos)] = it->second;
        pos += len + 1;
    }
    return remap;
}

// --- Remap helpers ---

static void remap_addr_points(std::vector<char>& data, const std::unordered_map<uint32_t,uint32_t>& rm) {
    for (size_t i = 0; i + 16 <= data.size(); i += 16)
        for (size_t off : {8, 12}) {
            uint32_t v; memcpy(&v, data.data() + i + off, 4);
            auto it = rm.find(v);
            if (it != rm.end()) memcpy(data.data() + i + off, &it->second, 4);
        }
}

static void remap_field(std::vector<char>& data, size_t stride, size_t field_off,
                         const std::unordered_map<uint32_t,uint32_t>& rm) {
    for (size_t i = 0; i + stride <= data.size(); i += stride) {
        uint32_t v; memcpy(&v, data.data() + i + field_off, 4);
        auto it = rm.find(v);
        if (it != rm.end()) memcpy(data.data() + i + field_off, &it->second, 4);
    }
}

// --- Content-based record matching ---
// Returns old_id → new_id mapping

static uint64_t fnv_mix(uint64_t h, uint64_t v) { h ^= v; h *= 1099511628211ULL; return h; }

static std::vector<uint32_t> match_addr_points(
    const std::vector<char>& old_data, const std::vector<char>& new_data)
{
    // After string remap, matching records are byte-identical
    size_t old_n = old_data.size() / 16, new_n = new_data.size() / 16;
    // Hash each new record
    std::unordered_multimap<uint64_t, uint32_t> new_map;
    new_map.reserve(new_n);
    for (uint32_t i = 0; i < new_n; i++) {
        uint64_t h = 14695981039346656037ULL;
        const char* p = new_data.data() + i * 16;
        for (int j = 0; j < 16; j++) h = fnv_mix(h, (uint8_t)p[j]);
        new_map.emplace(h, i);
    }
    std::vector<uint32_t> result(old_n, 0xFFFFFFFF);
    size_t matched = 0;
    for (uint32_t i = 0; i < old_n; i++) {
        uint64_t h = 14695981039346656037ULL;
        const char* p = old_data.data() + i * 16;
        for (int j = 0; j < 16; j++) h = fnv_mix(h, (uint8_t)p[j]);
        auto it = new_map.find(h);
        if (it != new_map.end() &&
            memcmp(old_data.data() + i * 16, new_data.data() + it->second * 16, 16) == 0) {
            result[i] = it->second;
            new_map.erase(it);
            matched++;
        }
    }
    std::cerr << "    addr matched=" << matched << "/" << old_n << " new=" << new_map.size() << std::endl;
    return result;
}

static std::vector<uint32_t> match_ways(
    const std::vector<char>& old_ways, const std::vector<char>& old_nodes,
    const std::vector<char>& new_ways, const std::vector<char>& new_nodes,
    size_t stride)
{
    size_t name_off = (stride == 12) ? 8 : 5;
    size_t old_n = old_ways.size() / stride, new_n = new_ways.size() / stride;

    size_t old_node_count = old_nodes.size() / 8, new_node_count = new_nodes.size() / 8;
    auto way_hash = [&](const char* w, const char* nodes, size_t max_nodes) -> uint64_t {
        uint32_t node_offset, name_id; uint8_t node_count;
        memcpy(&node_offset, w, 4); node_count = (uint8_t)w[4];
        memcpy(&name_id, w + name_off, 4);
        uint64_t h = 14695981039346656037ULL;
        h = fnv_mix(h, name_id); h = fnv_mix(h, node_count);
        for (uint8_t j = 0; j < node_count && (node_offset + j) < max_nodes; j++) {
            float lat, lng;
            memcpy(&lat, nodes + (node_offset + j) * 8, 4);
            memcpy(&lng, nodes + (node_offset + j) * 8 + 4, 4);
            h = fnv_mix(h, to_grid(lat)); h = fnv_mix(h, to_grid(lng));
        }
        return h;
    };

    std::unordered_multimap<uint64_t, uint32_t> new_map;
    new_map.reserve(new_n);
    for (uint32_t i = 0; i < new_n; i++)
        new_map.emplace(way_hash(new_ways.data() + i * stride, new_nodes.data(), new_node_count), i);

    std::vector<uint32_t> result(old_n, 0xFFFFFFFF);
    size_t matched = 0;
    for (uint32_t i = 0; i < old_n; i++) {
        uint64_t h = way_hash(old_ways.data() + i * stride, old_nodes.data(), old_node_count);
        auto it = new_map.find(h);
        if (it != new_map.end()) {
            result[i] = it->second;
            // Also fix node_offset in old to match new (makes way bytes identical)
            uint32_t new_node_off;
            memcpy(&new_node_off, new_ways.data() + it->second * stride, 4);
            memcpy(const_cast<char*>(old_ways.data()) + i * stride, &new_node_off, 4);
            new_map.erase(it);
            matched++;
        }
    }
    std::cerr << "    ways matched=" << matched << "/" << old_n << " new=" << new_map.size() << std::endl;
    return result;
}

static std::vector<uint32_t> match_admin(
    const std::vector<char>& old_polys, const std::vector<char>& old_verts,
    const std::vector<char>& new_polys, const std::vector<char>& new_verts,
    size_t stride)
{
    size_t old_n = old_polys.size() / stride, new_n = new_polys.size() / stride;

    size_t old_vc = old_verts.size() / 8, new_vc = new_verts.size() / 8;
    auto poly_hash = [&](const char* p, const char* verts, size_t max_v) -> uint64_t {
        uint32_t vert_offset, vert_count, name_id;
        memcpy(&vert_offset, p, 4); memcpy(&vert_count, p + 4, 4);
        memcpy(&name_id, p + 8, 4);
        uint8_t level = (uint8_t)p[12];
        uint16_t cc; memcpy(&cc, p + stride - 2, 2);
        uint64_t h = 14695981039346656037ULL;
        h = fnv_mix(h, name_id); h = fnv_mix(h, level); h = fnv_mix(h, cc);
        h = fnv_mix(h, vert_count);
        uint32_t n = std::min(vert_count, 10u);
        for (uint32_t j = 0; j < n && (vert_offset + j) < max_v; j++) {
            float lat, lng;
            memcpy(&lat, verts + (vert_offset + j) * 8, 4);
            memcpy(&lng, verts + (vert_offset + j) * 8 + 4, 4);
            h = fnv_mix(h, to_grid(lat)); h = fnv_mix(h, to_grid(lng));
        }
        return h;
    };

    std::unordered_multimap<uint64_t, uint32_t> new_map;
    new_map.reserve(new_n);
    for (uint32_t i = 0; i < new_n; i++)
        new_map.emplace(poly_hash(new_polys.data() + i * stride, new_verts.data(), new_vc), i);

    std::vector<uint32_t> result(old_n, 0xFFFFFFFF);
    size_t matched = 0;
    for (uint32_t i = 0; i < old_n; i++) {
        uint64_t h = poly_hash(old_polys.data() + i * stride, old_verts.data(), old_vc);
        auto it = new_map.find(h);
        if (it != new_map.end()) {
            result[i] = it->second;
            uint32_t new_vert_off;
            memcpy(&new_vert_off, new_polys.data() + it->second * stride, 4);
            memcpy(const_cast<char*>(old_polys.data()) + i * stride, &new_vert_off, 4);
            new_map.erase(it);
            matched++;
        }
    }
    std::cerr << "    admin matched=" << matched << "/" << old_n << " new=" << new_map.size() << std::endl;
    return result;
}

// Match interp ways by content
static std::vector<uint32_t> match_interps(
    const std::vector<char>& old_data, const std::vector<char>& old_nodes,
    const std::vector<char>& new_data, const std::vector<char>& new_nodes,
    size_t stride)
{
    size_t street_off = (stride >= 20) ? 8 : 5;
    size_t old_n = old_data.size() / stride, new_n = new_data.size() / stride;

    size_t old_nc = old_nodes.size() / 8, new_nc = new_nodes.size() / 8;
    auto ihash = [&](const char* p, const char* nodes, size_t max_n) -> uint64_t {
        uint32_t node_offset, street_id, start, end;
        uint8_t count, itype;
        memcpy(&node_offset, p, 4); count = (uint8_t)p[4];
        memcpy(&street_id, p + street_off, 4);
        memcpy(&start, p + street_off + 4, 4);
        memcpy(&end, p + street_off + 8, 4);
        itype = (street_off + 12 < stride) ? (uint8_t)p[street_off + 12] : 0;
        uint64_t h = 14695981039346656037ULL;
        h = fnv_mix(h, street_id); h = fnv_mix(h, start);
        h = fnv_mix(h, end); h = fnv_mix(h, itype); h = fnv_mix(h, count);
        for (uint8_t j = 0; j < count && (node_offset + j) < max_n; j++) {
            float lat, lng;
            memcpy(&lat, nodes + (node_offset + j) * 8, 4);
            memcpy(&lng, nodes + (node_offset + j) * 8 + 4, 4);
            h = fnv_mix(h, to_grid(lat)); h = fnv_mix(h, to_grid(lng));
        }
        return h;
    };

    std::unordered_multimap<uint64_t, uint32_t> new_map;
    new_map.reserve(new_n);
    for (uint32_t i = 0; i < new_n; i++)
        new_map.emplace(ihash(new_data.data() + i * stride, new_nodes.data(), new_nc), i);

    std::vector<uint32_t> result(old_n, 0xFFFFFFFF);
    size_t matched = 0;
    for (uint32_t i = 0; i < old_n; i++) {
        uint64_t h = ihash(old_data.data() + i * stride, old_nodes.data(), old_nc);
        auto it = new_map.find(h);
        if (it != new_map.end()) {
            result[i] = it->second;
            uint32_t new_node_off;
            memcpy(&new_node_off, new_data.data() + it->second * stride, 4);
            memcpy(const_cast<char*>(old_data.data()) + i * stride, &new_node_off, 4);
            new_map.erase(it);
            matched++;
        }
    }
    std::cerr << "    interps matched=" << matched << "/" << old_n << " new=" << new_map.size() << std::endl;
    return result;
}

// --- Entry file remapping ---
// Parse geo_cells + entries, remap IDs, rebuild entry + cell files.

static void remap_and_rebuild_entries(
    const std::string& old_dir, const std::string& new_dir, const std::string& tmpdir,
    const std::vector<uint32_t>& way_remap,
    const std::vector<uint32_t>& addr_remap,
    const std::vector<uint32_t>& interp_remap,
    const std::vector<uint32_t>& admin_remap)
{
    // For geo entries: parse old entries, remap IDs, then copy NEW entries/cells
    // (since the new files have the correct structure for the new build).
    // We write the remapped old entries + new cells to temp for zstd diffing.

    // Simpler approach: just copy new entry/cell files to temp dir.
    // The zstd diff of old vs new entry files won't benefit from ID remapping
    // unless we actually remap the old entries. Let's do the full remap.

    auto parse_entries = [](const std::vector<char>& cells_data, size_t cell_stride, size_t off_pos,
                            const std::vector<char>& entries_data)
        -> std::unordered_map<uint64_t, std::vector<uint32_t>>
    {
        std::unordered_map<uint64_t, std::vector<uint32_t>> result;
        size_t n = cells_data.size() / cell_stride;
        for (size_t i = 0; i < n; i++) {
            uint64_t cell_id; uint32_t off;
            memcpy(&cell_id, cells_data.data() + i * cell_stride, 8);
            memcpy(&off, cells_data.data() + i * cell_stride + off_pos, 4);
            if (off == 0xFFFFFFFF || off + 2 > entries_data.size()) continue;
            uint16_t count;
            memcpy(&count, entries_data.data() + off, 2);
            if (off + 2 + count * 4 > entries_data.size()) continue;
            std::vector<uint32_t> ids(count);
            memcpy(ids.data(), entries_data.data() + off + 2, count * 4);
            result[cell_id] = std::move(ids);
        }
        return result;
    };

    auto remap_ids = [](std::unordered_map<uint64_t, std::vector<uint32_t>>& cells,
                         const std::vector<uint32_t>& id_remap, bool has_flags = false) {
        for (auto& [cid, ids] : cells) {
            for (auto& id : ids) {
                uint32_t flags = has_flags ? (id & 0x80000000u) : 0;
                uint32_t masked = id & 0x7FFFFFFFu;
                if (masked < id_remap.size() && id_remap[masked] != 0xFFFFFFFF)
                    id = id_remap[masked] | flags;
            }
            std::sort(ids.begin(), ids.end());
        }
    };

    auto write_entries_cells = [](const std::string& cells_path, const std::string& entries_path,
                                   const std::unordered_map<uint64_t, std::vector<uint32_t>>& cell_map,
                                   size_t cell_stride)
    {
        std::vector<uint64_t> sorted_cells;
        for (auto& [id, _] : cell_map) sorted_cells.push_back(id);
        std::sort(sorted_cells.begin(), sorted_cells.end());

        std::vector<char> entries_buf;
        std::unordered_map<uint64_t, uint32_t> offsets;
        for (uint64_t cid : sorted_cells) {
            auto it = cell_map.find(cid);
            if (it == cell_map.end() || it->second.empty()) continue;
            offsets[cid] = static_cast<uint32_t>(entries_buf.size());
            uint16_t count = static_cast<uint16_t>(it->second.size());
            entries_buf.insert(entries_buf.end(), (char*)&count, (char*)&count + 2);
            entries_buf.insert(entries_buf.end(), (char*)it->second.data(),
                              (char*)it->second.data() + it->second.size() * 4);
        }
        write_file(entries_path, entries_buf);

        uint32_t no_data = 0xFFFFFFFF;
        std::vector<char> cells_buf;
        for (uint64_t cid : sorted_cells) {
            cells_buf.insert(cells_buf.end(), (char*)&cid, (char*)&cid + 8);
            auto it = offsets.find(cid);
            uint32_t off = (it != offsets.end()) ? it->second : no_data;
            cells_buf.insert(cells_buf.end(), (char*)&off, (char*)&off + 4);
        }
        write_file(cells_path, cells_buf);
    };

    // Geo cells: parse old, remap, merge with new cell set
    std::cerr << "  Remapping geo entries..." << std::endl;
    {
        auto old_geo = read_file(old_dir + "/geo_cells.bin");
        auto old_se = read_file(old_dir + "/street_entries.bin");
        auto old_ae = read_file(old_dir + "/addr_entries.bin");
        auto old_ie = read_file(old_dir + "/interp_entries.bin");

        auto new_geo = read_file(new_dir + "/geo_cells.bin");
        auto new_se = read_file(new_dir + "/street_entries.bin");
        auto new_ae = read_file(new_dir + "/addr_entries.bin");
        auto new_ie = read_file(new_dir + "/interp_entries.bin");

        // Parse old entries
        auto sc = parse_entries(old_geo, 20, 8, old_se);
        auto ac = parse_entries(old_geo, 20, 12, old_ae);
        auto ic = parse_entries(old_geo, 20, 16, old_ie);

        // Parse new entries (for cells that are new/changed)
        auto new_sc = parse_entries(new_geo, 20, 8, new_se);
        auto new_ac = parse_entries(new_geo, 20, 12, new_ae);
        auto new_ic = parse_entries(new_geo, 20, 16, new_ie);

        // Remap old IDs
        remap_ids(sc, way_remap);
        remap_ids(ac, addr_remap);
        remap_ids(ic, interp_remap);

        // Merge: start with remapped old, overlay with new for changed/added cells
        // For cells that exist in new but not old: add from new
        // For cells that exist in old but not new: remove
        for (auto& [cid, ids] : new_sc) sc[cid] = ids;
        for (auto& [cid, ids] : new_ac) ac[cid] = ids;
        for (auto& [cid, ids] : new_ic) ic[cid] = ids;

        // Collect all cells from new build
        std::unordered_map<uint64_t, bool> new_cells;
        size_t nn = new_geo.size() / 20;
        for (size_t i = 0; i < nn; i++) {
            uint64_t cid; memcpy(&cid, new_geo.data() + i * 20, 8);
            new_cells[cid] = true;
        }
        // Remove cells not in new
        for (auto it = sc.begin(); it != sc.end(); )
            new_cells.count(it->first) ? ++it : it = sc.erase(it);
        for (auto it = ac.begin(); it != ac.end(); )
            new_cells.count(it->first) ? ++it : it = ac.erase(it);
        for (auto it = ic.begin(); it != ic.end(); )
            new_cells.count(it->first) ? ++it : it = ic.erase(it);

        // Write merged entries + geo_cells
        // (This produces files almost identical to new build's entries)
        auto write_geo = [&](const std::string& dir,
                              const std::unordered_map<uint64_t, std::vector<uint32_t>>& s,
                              const std::unordered_map<uint64_t, std::vector<uint32_t>>& a,
                              const std::unordered_map<uint64_t, std::vector<uint32_t>>& ip) {
            // Collect all cells
            std::vector<uint64_t> all_cells;
            for (auto& [id, _] : new_cells) all_cells.push_back(id);
            std::sort(all_cells.begin(), all_cells.end());

            auto write_ent = [&](const std::string& path,
                                  const std::unordered_map<uint64_t, std::vector<uint32_t>>& cm)
                -> std::unordered_map<uint64_t, uint32_t>
            {
                std::vector<char> buf;
                std::unordered_map<uint64_t, uint32_t> offsets;
                for (uint64_t cid : all_cells) {
                    auto it = cm.find(cid);
                    if (it == cm.end() || it->second.empty()) continue;
                    offsets[cid] = static_cast<uint32_t>(buf.size());
                    uint16_t count = static_cast<uint16_t>(it->second.size());
                    buf.insert(buf.end(), (char*)&count, (char*)&count + 2);
                    buf.insert(buf.end(), (char*)it->second.data(),
                              (char*)it->second.data() + it->second.size() * 4);
                }
                write_file(path, buf);
                return offsets;
            };

            auto so = write_ent(dir + "/street_entries.bin", s);
            auto ao = write_ent(dir + "/addr_entries.bin", a);
            auto io = write_ent(dir + "/interp_entries.bin", ip);

            uint32_t no_data = 0xFFFFFFFF;
            std::vector<char> buf;
            for (uint64_t cid : all_cells) {
                buf.insert(buf.end(), (char*)&cid, (char*)&cid + 8);
                auto g = [&](const auto& m) -> uint32_t {
                    auto it = m.find(cid); return (it != m.end()) ? it->second : no_data;
                };
                uint32_t sv = g(so), av = g(ao), iv = g(io);
                buf.insert(buf.end(), (char*)&sv, (char*)&sv + 4);
                buf.insert(buf.end(), (char*)&av, (char*)&av + 4);
                buf.insert(buf.end(), (char*)&iv, (char*)&iv + 4);
            }
            write_file(dir + "/geo_cells.bin", buf);
        };

        write_geo(tmpdir, sc, ac, ic);
    }

    // Admin cells: parse, remap, rebuild
    std::cerr << "  Remapping admin entries..." << std::endl;
    {
        auto old_ac = read_file(old_dir + "/admin_cells.bin");
        auto old_ae = read_file(old_dir + "/admin_entries.bin");
        auto new_ac = read_file(new_dir + "/admin_cells.bin");
        auto new_ae = read_file(new_dir + "/admin_entries.bin");

        auto cells = parse_entries(old_ac, 12, 8, old_ae);
        auto new_cells_map = parse_entries(new_ac, 12, 8, new_ae);
        remap_ids(cells, admin_remap, true);

        // Merge with new
        for (auto& [cid, ids] : new_cells_map) cells[cid] = ids;
        // Remove cells not in new
        std::unordered_map<uint64_t, bool> new_cell_set;
        size_t nn = new_ac.size() / 12;
        for (size_t i = 0; i < nn; i++) {
            uint64_t cid; memcpy(&cid, new_ac.data() + i * 12, 8);
            new_cell_set[cid] = true;
        }
        for (auto it = cells.begin(); it != cells.end(); )
            new_cell_set.count(it->first) ? ++it : it = cells.erase(it);

        write_entries_cells(tmpdir + "/admin_cells.bin", tmpdir + "/admin_entries.bin", cells, 12);
    }
}

// --- Write helpers ---
static void write_val(std::ofstream& f, const void* data, size_t size) {
    f.write(reinterpret_cast<const char*>(data), size);
}

int main(int argc, char* argv[]) {
    if (argc < 5 || std::string(argv[3]) != "-o") {
        std::cerr << "Usage: geocoder-diff <old-dir> <new-dir> -o <patch-file>" << std::endl;
        return 1;
    }
    std::string old_dir = argv[1], new_dir = argv[2], patch_path = argv[4];
    std::string tmpdir = "/tmp/geocoder-diff-" + std::to_string(getpid());
    ensure_dir(tmpdir);

    // Build string remap
    std::cerr << "Building string remap..." << std::endl;
    auto old_strings = read_file(old_dir + "/strings.bin");
    auto new_strings = read_file(new_dir + "/strings.bin");
    auto str_remap = build_string_remap(old_strings, new_strings);
    std::cerr << "  " << str_remap.size() << " strings mapped" << std::endl;

    // Detect struct strides
    auto detect = [](const std::string& path, std::vector<size_t> cs) -> size_t {
        auto d = read_file(path);
        for (size_t s : cs) if (d.size() % s == 0 && d.size() > 0) return s;
        return cs[0];
    };
    size_t way_stride = detect(old_dir + "/street_ways.bin", {12, 9});
    size_t interp_stride = detect(old_dir + "/interp_ways.bin", {20, 18});
    size_t admin_stride = detect(old_dir + "/admin_polygons.bin", {24, 20, 19});

    // Build record ID remaps (requires loading data + coordinate files)
    std::cerr << "Building record ID remaps..." << std::endl;
    std::vector<uint32_t> addr_id_remap, way_id_remap, interp_id_remap, admin_id_remap;
    {
        // addr_points: string-remap old, then match by bytes
        auto old_ap = read_file(old_dir + "/addr_points.bin");
        remap_addr_points(old_ap, str_remap);
        auto new_ap = read_file(new_dir + "/addr_points.bin");
        addr_id_remap = match_addr_points(old_ap, new_ap);
        write_file(tmpdir + "/addr_points.bin", old_ap);
    }
    {
        // street_ways: string-remap + content match (fixes node_offset too)
        auto old_w = read_file(old_dir + "/street_ways.bin");
        remap_field(old_w, way_stride, way_stride == 12 ? 8 : 5, str_remap);
        auto old_n = read_file(old_dir + "/street_nodes.bin");
        auto new_w = read_file(new_dir + "/street_ways.bin");
        auto new_n = read_file(new_dir + "/street_nodes.bin");
        way_id_remap = match_ways(old_w, old_n, new_w, new_n, way_stride);
        write_file(tmpdir + "/street_ways.bin", old_w);
        write_file(tmpdir + "/street_nodes.bin", new_n); // use new nodes (offsets now match)
    }
    {
        auto old_i = read_file(old_dir + "/interp_ways.bin");
        remap_field(old_i, interp_stride, interp_stride >= 20 ? 8 : 5, str_remap);
        auto old_n = read_file(old_dir + "/interp_nodes.bin");
        auto new_i = read_file(new_dir + "/interp_ways.bin");
        auto new_n = read_file(new_dir + "/interp_nodes.bin");
        interp_id_remap = match_interps(old_i, old_n, new_i, new_n, interp_stride);
        write_file(tmpdir + "/interp_ways.bin", old_i);
        write_file(tmpdir + "/interp_nodes.bin", new_n);
    }
    {
        auto old_p = read_file(old_dir + "/admin_polygons.bin");
        remap_field(old_p, admin_stride, 8, str_remap);
        auto old_v = read_file(old_dir + "/admin_vertices.bin");
        auto new_p = read_file(new_dir + "/admin_polygons.bin");
        auto new_v = read_file(new_dir + "/admin_vertices.bin");
        admin_id_remap = match_admin(old_p, old_v, new_p, new_v, admin_stride);
        write_file(tmpdir + "/admin_polygons.bin", old_p);
        write_file(tmpdir + "/admin_vertices.bin", new_v);
    }

    // Rebuild entry/cell files with remapped IDs
    remap_and_rebuild_entries(old_dir, new_dir, tmpdir,
                              way_id_remap, addr_id_remap, interp_id_remap, admin_id_remap);

    // Open patch file
    std::ofstream pf(patch_path, std::ios::binary);
    pf.write(GCPATCH_MAGIC, 8);
    uint32_t ver = GCPATCH_VERSION, flags = 0;
    write_val(pf, &ver, 4); write_val(pf, &flags, 4);

    // Write string remap
    {
        uint32_t marker = 0xFFFFFFFE;
        write_val(pf, &marker, 4);
        std::vector<std::pair<uint32_t,uint32_t>> entries;
        size_t pos = 0;
        while (pos < old_strings.size()) {
            uint32_t old_off = static_cast<uint32_t>(pos);
            auto it = str_remap.find(old_off);
            entries.push_back({old_off, (it != str_remap.end()) ? it->second : old_off});
            pos += strlen(old_strings.data() + pos) + 1;
        }
        uint32_t count = static_cast<uint32_t>(entries.size());
        write_val(pf, &count, 4);
        for (auto& [o, n] : entries) { write_val(pf, &o, 4); write_val(pf, &n, 4); }
        std::cerr << "  Remap: " << count << " entries" << std::endl;
    }

    // Write offset fixup tables for ways/interps/admin (node_offset/vertex_offset)
    {
        auto write_fixups = [&](PatchFileId id, const std::string& name,
                                 const std::vector<uint32_t>& id_remap, size_t stride) {
            auto new_data = read_file(new_dir + "/" + name);
            uint32_t marker = FIXUP_MARKER, fid = static_cast<uint32_t>(id);
            uint32_t st = static_cast<uint32_t>(stride);
            std::vector<std::pair<uint32_t, uint32_t>> fixups;
            for (uint32_t i = 0; i < id_remap.size(); i++) {
                if (id_remap[i] != 0xFFFFFFFF) {
                    uint32_t new_val;
                    memcpy(&new_val, new_data.data() + id_remap[i] * stride, 4);
                    fixups.push_back({i, new_val});
                }
            }
            write_val(pf, &marker, 4); write_val(pf, &fid, 4);
            write_val(pf, &st, 4);
            uint32_t count = static_cast<uint32_t>(fixups.size());
            write_val(pf, &count, 4);
            for (auto& [idx, val] : fixups) { write_val(pf, &idx, 4); write_val(pf, &val, 4); }
            std::cerr << "  fixup " << name << ": " << count << " entries" << std::endl;
        };
        write_fixups(PatchFileId::STREET_WAYS, "street_ways.bin", way_id_remap, way_stride);
        write_fixups(PatchFileId::INTERP_WAYS, "interp_ways.bin", interp_id_remap, interp_stride);
        write_fixups(PatchFileId::ADMIN_POLYGONS, "admin_polygons.bin", admin_id_remap, admin_stride);
    }

    // Single-stage diffs: remapped_old → new (zstd --patch-from on temp files)
    auto diff_file = [&](PatchFileId id, const std::string& name, bool use_old_as_ref = false) {
        std::cerr << "  " << name << ": ";
        std::string ref;
        if (use_old_as_ref) {
            ref = old_dir + "/" + name;
        } else {
            ref = tmpdir + "/" + name;
            if (read_file(ref).empty()) ref = old_dir + "/" + name;
        }
        std::string tgt = new_dir + "/" + name;
        std::string zst = tmpdir + "/" + name + ".zst";
        system(("zstd --patch-from='" + ref + "' '" + tgt + "' -o '" + zst + "' -f --quiet 2>/dev/null").c_str());
        auto zst_data = read_file(zst);
        auto old_data = read_file(old_dir + "/" + name);
        auto new_data = read_file(tgt);

        uint32_t fid = static_cast<uint32_t>(id), enc = static_cast<uint32_t>(PatchEncoding::ZSTD_DELTA);
        uint64_t os = old_data.size(), ns = new_data.size(), ds = zst_data.size();
        write_val(pf, &fid, 4); write_val(pf, &enc, 4);
        write_val(pf, &os, 8); write_val(pf, &ns, 8); write_val(pf, &ds, 8);
        pf.write(zst_data.data(), zst_data.size());

        double pct = ns > 0 ? ds * 100.0 / ns : 0;
        std::cerr << ds << " bytes (" << std::fixed << std::setprecision(2) << pct << "%)" << std::endl;
        remove(zst.c_str());
    };

    // Data files with string remap + fixups: use remapped temp as ref
    diff_file(PatchFileId::ADDR_POINTS, "addr_points.bin");
    diff_file(PatchFileId::STREET_WAYS, "street_ways.bin");
    diff_file(PatchFileId::INTERP_WAYS, "interp_ways.bin");
    diff_file(PatchFileId::ADMIN_POLYGONS, "admin_polygons.bin");
    // Coord files: use old as ref (no remap needed)
    diff_file(PatchFileId::STRINGS, "strings.bin", true);
    diff_file(PatchFileId::STREET_NODES, "street_nodes.bin", true);
    diff_file(PatchFileId::INTERP_NODES, "interp_nodes.bin", true);
    diff_file(PatchFileId::ADMIN_VERTICES, "admin_vertices.bin", true);
    // Entry/cell files: use rebuilt temp as ref (has remapped IDs)
    diff_file(PatchFileId::GEO_CELLS, "geo_cells.bin");
    diff_file(PatchFileId::STREET_ENTRIES, "street_entries.bin");
    diff_file(PatchFileId::ADDR_ENTRIES, "addr_entries.bin");
    diff_file(PatchFileId::INTERP_ENTRIES, "interp_entries.bin");
    diff_file(PatchFileId::ADMIN_CELLS, "admin_cells.bin");
    diff_file(PatchFileId::ADMIN_ENTRIES, "admin_entries.bin");

    uint32_t end = 0xFFFFFFFF;
    write_val(pf, &end, 4);

    std::string rm_cmd = "rm -rf '" + tmpdir + "'";
    system(rm_cmd.c_str());

    auto total = pf.tellp();
    std::cerr << "\nTotal patch: " << total << " bytes (" << total / 1024 / 1024 << " MiB)" << std::endl;
    return 0;
}
