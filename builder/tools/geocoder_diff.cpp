// geocoder-diff v3: Fully custom patch format using merge-sequence encoding.
//
// For each data file: walk old (string-remapped) and new in parallel,
// match records by content, emit MATCH/INSERT/DELETE operations.
// For entry/cell files: include changed cell entry data directly.
// geo_cells rebuilt by patch tool from entries (not delta-patched).
//
// Patch format: custom binary, zstd-compressed as a whole for transport.
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
#include <unordered_set>
#include <vector>

#include "patch_format.h"

// --- Merge sequence encoding ---
// Walk two sorted arrays (old remapped, new), matching by record bytes.
// Emit: MATCH(n) = copy n records from old, INSERT(n,data) = add n new records,
//       DELETE(n) = skip n old records.

enum MergeOp : uint8_t { OP_MATCH_RUN = 0, OP_INSERT_RUN = 1, OP_DELETE_RUN = 2 };

struct MergeSequence {
    std::vector<char> data; // serialized ops

    void add_match(uint32_t count) {
        uint8_t op = OP_MATCH_RUN;
        data.insert(data.end(), (char*)&op, (char*)&op + 1);
        data.insert(data.end(), (char*)&count, (char*)&count + 4);
    }
    void add_delete(uint32_t count) {
        uint8_t op = OP_DELETE_RUN;
        data.insert(data.end(), (char*)&op, (char*)&op + 1);
        data.insert(data.end(), (char*)&count, (char*)&count + 4);
    }
    void add_insert(const char* records, uint32_t count, size_t stride) {
        uint8_t op = OP_INSERT_RUN;
        data.insert(data.end(), (char*)&op, (char*)&op + 1);
        data.insert(data.end(), (char*)&count, (char*)&count + 4);
        data.insert(data.end(), records, records + count * stride);
    }
};

// Build merge sequence for a data file.
// old_data has string remap already applied.
// Records are compared by byte equality (stride bytes).
static MergeSequence build_merge_seq(
    const std::vector<char>& old_data, const std::vector<char>& new_data,
    size_t stride)
{
    size_t old_n = old_data.size() / stride;
    size_t new_n = new_data.size() / stride;
    size_t oi = 0, ni = 0;
    MergeSequence seq;
    uint32_t match_run = 0, del_run = 0;

    auto flush_match = [&]() { if (match_run > 0) { seq.add_match(match_run); match_run = 0; } };
    auto flush_del = [&]() { if (del_run > 0) { seq.add_delete(del_run); del_run = 0; } };

    // For small strides (<=8), records aren't unique enough for hash matching.
    // Use simple sequential scan instead.
    bool use_hash = (stride > 8);

    // Pre-build hash index of new records for fast mismatch resolution
    auto record_hash = [&](const char* p, size_t s) -> uint64_t {
        uint64_t h = 14695981039346656037ULL;
        for (size_t i = 0; i < s; i++) { h ^= (uint8_t)p[i]; h *= 1099511628211ULL; }
        return h;
    };
    std::unordered_multimap<uint64_t, uint32_t> new_hash;
    if (use_hash) {
        new_hash.reserve(new_n);
        for (uint32_t i = 0; i < new_n; i++)
            new_hash.emplace(record_hash(new_data.data() + i * stride, stride), i);
    }

    while (oi < old_n && ni < new_n) {
        const char* op = old_data.data() + oi * stride;
        const char* np = new_data.data() + ni * stride;

        if (memcmp(op, np, stride) == 0) {
            flush_del();
            match_run++;
            oi++; ni++;
        } else if (use_hash) {
            // Hash-based mismatch resolution (for records with unique content)
            uint64_t oh = record_hash(op, stride);
            auto range = new_hash.equal_range(oh);
            uint32_t best_ni = UINT32_MAX;
            for (auto it = range.first; it != range.second; ++it) {
                if (it->second >= ni && it->second < ni + 10000 &&
                    memcmp(op, new_data.data() + it->second * stride, stride) == 0) {
                    if (it->second < best_ni) best_ni = it->second;
                }
            }
            if (best_ni != UINT32_MAX && best_ni > ni) {
                flush_match(); flush_del();
                seq.add_insert(new_data.data() + ni * stride, best_ni - ni, stride);
                ni = best_ni;
            } else if (best_ni == ni) {
                flush_del(); match_run++; oi++; ni++;
            } else {
                flush_match(); del_run++; oi++;
            }
        } else {
            // Sequential scan (for coordinate files where records aren't unique)
            size_t lookahead = std::min((size_t)200, std::min(old_n - oi, new_n - ni));
            bool found = false;
            for (size_t k = 1; k <= lookahead; k++) {
                if (ni + k < new_n && memcmp(op, new_data.data() + (ni + k) * stride, stride) == 0) {
                    flush_match(); flush_del();
                    seq.add_insert(new_data.data() + ni * stride, k, stride);
                    ni += k; found = true; break;
                }
                if (oi + k < old_n && memcmp(np, old_data.data() + (oi + k) * stride, stride) == 0) {
                    flush_match(); del_run += k; oi += k; found = true; break;
                }
            }
            if (!found) { flush_match(); del_run++; oi++; flush_del(); seq.add_insert(np, 1, stride); ni++; }
        }
    }

    flush_match(); flush_del();
    // Remaining old records: deletions
    if (oi < old_n) seq.add_delete(old_n - oi);
    // Remaining new records: insertions
    if (ni < new_n) seq.add_insert(new_data.data() + ni * stride, new_n - ni, stride);

    return seq;
}

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

// --- Remap + fixup helpers ---

static void remap_addr_points(std::vector<char>& data, const std::unordered_map<uint32_t,uint32_t>& rm) {
    for (size_t i = 0; i + 16 <= data.size(); i += 16)
        for (size_t off : {8, 12}) {
            uint32_t v; memcpy(&v, data.data() + i + off, 4);
            auto it = rm.find(v); if (it != rm.end()) memcpy(data.data() + i + off, &it->second, 4);
        }
}
static void remap_field(std::vector<char>& data, size_t stride, size_t field_off,
                         const std::unordered_map<uint32_t,uint32_t>& rm) {
    for (size_t i = 0; i + stride <= data.size(); i += stride) {
        uint32_t v; memcpy(&v, data.data() + i + field_off, 4);
        auto it = rm.find(v); if (it != rm.end()) memcpy(data.data() + i + field_off, &it->second, 4);
    }
}

// Content matching for ways (by name + nodes, ignoring node_offset)
static uint64_t fnv_mix(uint64_t h, uint64_t v) { h ^= v; h *= 1099511628211ULL; return h; }

static void fixup_way_offsets(std::vector<char>& old_ways, const std::vector<char>& old_nodes,
                                const std::vector<char>& new_ways, const std::vector<char>& new_nodes,
                                size_t stride) {
    size_t name_off = (stride == 12) ? 8 : 5;
    size_t old_n = old_ways.size() / stride, new_n = new_ways.size() / stride;
    size_t old_nc = old_nodes.size() / 8, new_nc = new_nodes.size() / 8;

    auto way_hash = [&](const char* w, const char* nodes, size_t max_n) -> uint64_t {
        uint32_t node_offset, name_id; uint8_t node_count;
        memcpy(&node_offset, w, 4); node_count = (uint8_t)w[4]; memcpy(&name_id, w + name_off, 4);
        uint64_t h = 14695981039346656037ULL;
        h = fnv_mix(h, name_id); h = fnv_mix(h, node_count);
        for (uint8_t j = 0; j < node_count && (node_offset + j) < max_n; j++) {
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
        new_map.emplace(way_hash(new_ways.data() + i * stride, new_nodes.data(), new_nc), i);

    for (uint32_t i = 0; i < old_n; i++) {
        uint64_t h = way_hash(old_ways.data() + i * stride, old_nodes.data(), old_nc);
        auto it = new_map.find(h);
        if (it != new_map.end()) {
            // Fix node_offset to match new way
            uint32_t new_node_off;
            memcpy(&new_node_off, new_ways.data() + it->second * stride, 4);
            memcpy(old_ways.data() + i * stride, &new_node_off, 4);
            new_map.erase(it);
        }
    }
}

// Same for admin polygons (vertex_offset)
static void fixup_admin_offsets(std::vector<char>& old_polys, const std::vector<char>& old_verts,
                                  const std::vector<char>& new_polys, const std::vector<char>& new_verts,
                                  size_t stride) {
    size_t old_n = old_polys.size() / stride, new_n = new_polys.size() / stride;
    size_t old_vc = old_verts.size() / 8, new_vc = new_verts.size() / 8;

    auto poly_hash = [&](const char* p, const char* verts, size_t max_v) -> uint64_t {
        uint32_t vert_offset, vert_count, name_id;
        memcpy(&vert_offset, p, 4); memcpy(&vert_count, p + 4, 4); memcpy(&name_id, p + 8, 4);
        uint8_t level = (uint8_t)p[12]; uint16_t cc; memcpy(&cc, p + stride - 2, 2);
        uint64_t h = 14695981039346656037ULL;
        h = fnv_mix(h, name_id); h = fnv_mix(h, level); h = fnv_mix(h, cc); h = fnv_mix(h, vert_count);
        for (uint32_t j = 0; j < std::min(vert_count, 10u) && (vert_offset + j) < max_v; j++) {
            float lat, lng;
            memcpy(&lat, verts + (vert_offset + j) * 8, 4); memcpy(&lng, verts + (vert_offset + j) * 8 + 4, 4);
            h = fnv_mix(h, to_grid(lat)); h = fnv_mix(h, to_grid(lng));
        }
        return h;
    };

    std::unordered_multimap<uint64_t, uint32_t> new_map;
    new_map.reserve(new_n);
    for (uint32_t i = 0; i < new_n; i++)
        new_map.emplace(poly_hash(new_polys.data() + i * stride, new_verts.data(), new_vc), i);

    for (uint32_t i = 0; i < old_n; i++) {
        uint64_t h = poly_hash(old_polys.data() + i * stride, old_verts.data(), old_vc);
        auto it = new_map.find(h);
        if (it != new_map.end()) {
            uint32_t new_vert_off;
            memcpy(&new_vert_off, new_polys.data() + it->second * stride, 4);
            memcpy(old_polys.data() + i * stride, &new_vert_off, 4);
            new_map.erase(it);
        }
    }
}

// Same for interp ways
static void fixup_interp_offsets(std::vector<char>& old_data, const std::vector<char>& old_nodes,
                                   const std::vector<char>& new_data, const std::vector<char>& new_nodes,
                                   size_t stride) {
    size_t street_off = (stride >= 20) ? 8 : 5;
    size_t old_n = old_data.size() / stride, new_n = new_data.size() / stride;
    size_t old_nc = old_nodes.size() / 8, new_nc = new_nodes.size() / 8;

    auto ihash = [&](const char* p, const char* nodes, size_t max_n) -> uint64_t {
        uint32_t node_offset, street_id, start, end; uint8_t count, itype;
        memcpy(&node_offset, p, 4); count = (uint8_t)p[4];
        memcpy(&street_id, p + street_off, 4); memcpy(&start, p + street_off + 4, 4);
        memcpy(&end, p + street_off + 8, 4);
        itype = (street_off + 12 < stride) ? (uint8_t)p[street_off + 12] : 0;
        uint64_t h = 14695981039346656037ULL;
        h = fnv_mix(h, street_id); h = fnv_mix(h, start); h = fnv_mix(h, end);
        h = fnv_mix(h, itype); h = fnv_mix(h, count);
        for (uint8_t j = 0; j < count && (node_offset + j) < max_n; j++) {
            float lat, lng;
            memcpy(&lat, nodes + (node_offset + j) * 8, 4); memcpy(&lng, nodes + (node_offset + j) * 8 + 4, 4);
            h = fnv_mix(h, to_grid(lat)); h = fnv_mix(h, to_grid(lng));
        }
        return h;
    };

    std::unordered_multimap<uint64_t, uint32_t> new_map;
    new_map.reserve(new_n);
    for (uint32_t i = 0; i < new_n; i++)
        new_map.emplace(ihash(new_data.data() + i * stride, new_nodes.data(), new_nc), i);
    for (uint32_t i = 0; i < old_n; i++) {
        uint64_t h = ihash(old_data.data() + i * stride, old_nodes.data(), old_nc);
        auto it = new_map.find(h);
        if (it != new_map.end()) {
            uint32_t new_off; memcpy(&new_off, new_data.data() + it->second * stride, 4);
            memcpy(old_data.data() + i * stride, &new_off, 4);
            new_map.erase(it);
        }
    }
}

// --- Derive ID remap from merge sequence (same logic as patch tool) ---
static std::vector<uint32_t> derive_id_remap_from_merge(
    const MergeSequence& seq, size_t old_count, size_t stride)
{
    std::vector<uint32_t> id_map(old_count, 0xFFFFFFFF);
    size_t pos = 0, old_rec = 0, new_rec = 0;
    while (pos < seq.data.size()) {
        uint8_t op = static_cast<uint8_t>(seq.data[pos]); pos++;
        uint32_t count; memcpy(&count, seq.data.data() + pos, 4); pos += 4;
        if (op == OP_MATCH_RUN) {
            for (uint32_t k = 0; k < count; k++)
                if (old_rec + k < id_map.size())
                    id_map[old_rec + k] = static_cast<uint32_t>(new_rec + k);
            old_rec += count; new_rec += count;
        } else if (op == OP_INSERT_RUN) {
            pos += count * stride;
            new_rec += count;
        } else if (op == OP_DELETE_RUN) {
            old_rec += count;
        }
    }
    return id_map;
}

// --- Write helpers ---
static void wval(std::vector<char>& buf, const void* data, size_t size) {
    buf.insert(buf.end(), (const char*)data, (const char*)data + size);
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

    // Detect strides
    auto detect = [](const std::string& path, std::vector<size_t> cs) -> size_t {
        auto d = read_file(path);
        for (size_t s : cs) if (d.size() % s == 0 && d.size() > 0) return s;
        return cs[0];
    };
    size_t way_stride = detect(old_dir + "/street_ways.bin", {12, 9});
    size_t interp_stride = detect(old_dir + "/interp_ways.bin", {24, 20, 18});
    size_t admin_stride = detect(old_dir + "/admin_polygons.bin", {24, 20, 19});

    // Build patch data (uncompressed, will be zstd-compressed at the end)
    std::vector<char> patch;
    patch.insert(patch.end(), GCPATCH_MAGIC, GCPATCH_MAGIC + 8);
    uint32_t ver = 2, flags = 0; // version 2 = custom format
    wval(patch, &ver, 4); wval(patch, &flags, 4);

    // --- Section: String remap ---
    {
        std::vector<std::pair<uint32_t,uint32_t>> entries;
        size_t pos = 0;
        while (pos < old_strings.size()) {
            uint32_t old_off = static_cast<uint32_t>(pos);
            auto it = str_remap.find(old_off);
            entries.push_back({old_off, (it != str_remap.end()) ? it->second : old_off});
            pos += strlen(old_strings.data() + pos) + 1;
        }
        uint32_t marker = 0xFFFFFFFE, count = static_cast<uint32_t>(entries.size());
        wval(patch, &marker, 4); wval(patch, &count, 4);
        for (auto& [o, n] : entries) { wval(patch, &o, 4); wval(patch, &n, 4); }
        std::cerr << "  String remap: " << count << " entries (" << count * 8 << " bytes)" << std::endl;
    }

    // --- Section: Per-file merge sequences ---
    // Track fixups per file (applied before merge, included in patch for patch tool)
    std::unordered_map<uint32_t, std::vector<std::pair<uint32_t,uint32_t>>> file_fixups;

    // Stored merge sequences for ID remap derivation
    std::unordered_map<uint32_t, MergeSequence> stored_merges;

    auto write_merge = [&](PatchFileId id, const std::string& name,
                             const std::vector<char>& old_data, const std::vector<char>& new_data,
                             size_t stride) {
        auto seq = build_merge_seq(old_data, new_data, stride);
        stored_merges[static_cast<uint32_t>(id)] = seq; // store for entry rebuild
        uint32_t fid = static_cast<uint32_t>(id);
        uint64_t os = old_data.size(), ns = new_data.size(), ss = seq.data.size();

        // Check for fixups
        auto fit = file_fixups.find(fid);
        uint32_t n_fixups = fit != file_fixups.end() ? static_cast<uint32_t>(fit->second.size()) : 0;

        wval(patch, &fid, 4);
        uint32_t st = static_cast<uint32_t>(stride);
        wval(patch, &st, 4);
        wval(patch, &os, 8); wval(patch, &ns, 8);
        // Fixup count + data before merge sequence
        wval(patch, &n_fixups, 4);
        if (n_fixups > 0) {
            for (auto& [idx, val] : fit->second) { wval(patch, &idx, 4); wval(patch, &val, 4); }
        }
        wval(patch, &ss, 8);
        patch.insert(patch.end(), seq.data.begin(), seq.data.end());
        std::cerr << "  " << name << ": seq=" << ss << " fixups=" << n_fixups
                  << " (" << std::fixed << std::setprecision(2)
                  << (ns > 0 ? ss * 100.0 / ns : 0) << "%)" << std::endl;
    };

    // strings.bin: full replacement (byte-level merge is inefficient for string pools)
    {
        uint32_t fid = static_cast<uint32_t>(PatchFileId::STRINGS);
        uint32_t st = 0; // full replacement
        uint64_t os = 0, ns = new_strings.size();
        uint32_t nfix = 0;
        uint64_t ss = new_strings.size();
        wval(patch, &fid, 4); wval(patch, &st, 4);
        wval(patch, &os, 8); wval(patch, &ns, 8);
        wval(patch, &nfix, 4); wval(patch, &ss, 8);
        patch.insert(patch.end(), new_strings.begin(), new_strings.end());
        std::cerr << "  strings.bin: full " << new_strings.size() << " bytes" << std::endl;
    }

    // addr_points.bin
    {
        auto old_data = read_file(old_dir + "/addr_points.bin");
        remap_addr_points(old_data, str_remap);
        auto new_data = read_file(new_dir + "/addr_points.bin");
        write_merge(PatchFileId::ADDR_POINTS, "addr_points.bin", old_data, new_data, 16);
    }

    // street_ways.bin (string remap + offset fixup for node_offset)
    {
        auto old_w = read_file(old_dir + "/street_ways.bin");
        remap_field(old_w, way_stride, way_stride == 12 ? 8 : 5, str_remap);
        auto new_w = read_file(new_dir + "/street_ways.bin");
        auto old_n = read_file(old_dir + "/street_nodes.bin");
        auto new_n = read_file(new_dir + "/street_nodes.bin");
        // Record fixups: (record_index, new_node_offset)
        auto& fixups = file_fixups[static_cast<uint32_t>(PatchFileId::STREET_WAYS)];
        // Save old offsets, apply fixups, then record changes
        size_t wn = old_w.size() / way_stride;
        std::vector<uint32_t> old_offsets(wn);
        for (size_t i = 0; i < wn; i++) memcpy(&old_offsets[i], old_w.data() + i * way_stride, 4);
        fixup_way_offsets(old_w, old_n, new_w, new_n, way_stride);
        for (size_t i = 0; i < wn; i++) {
            uint32_t new_off; memcpy(&new_off, old_w.data() + i * way_stride, 4);
            if (new_off != old_offsets[i]) fixups.push_back({static_cast<uint32_t>(i), new_off});
        }
        write_merge(PatchFileId::STREET_WAYS, "street_ways.bin", old_w, new_w, way_stride);
    }

    // street_nodes.bin (no remap)
    {
        auto old_data = read_file(old_dir + "/street_nodes.bin");
        auto new_data = read_file(new_dir + "/street_nodes.bin");
        write_merge(PatchFileId::STREET_NODES, "street_nodes.bin", old_data, new_data, 8);
    }

    // interp_ways.bin
    {
        auto old_data = read_file(old_dir + "/interp_ways.bin");
        remap_field(old_data, interp_stride, interp_stride >= 20 ? 8 : 5, str_remap);
        auto old_n = read_file(old_dir + "/interp_nodes.bin");
        auto new_data = read_file(new_dir + "/interp_ways.bin");
        auto new_n = read_file(new_dir + "/interp_nodes.bin");
        auto& fixups = file_fixups[static_cast<uint32_t>(PatchFileId::INTERP_WAYS)];
        size_t in_count = old_data.size() / interp_stride;
        std::vector<uint32_t> old_offsets(in_count);
        for (size_t i = 0; i < in_count; i++) memcpy(&old_offsets[i], old_data.data() + i * interp_stride, 4);
        fixup_interp_offsets(old_data, old_n, new_data, new_n, interp_stride);
        for (size_t i = 0; i < in_count; i++) {
            uint32_t new_off; memcpy(&new_off, old_data.data() + i * interp_stride, 4);
            if (new_off != old_offsets[i]) fixups.push_back({static_cast<uint32_t>(i), new_off});
        }
        write_merge(PatchFileId::INTERP_WAYS, "interp_ways.bin", old_data, new_data, interp_stride);
    }

    // interp_nodes.bin
    {
        auto old_data = read_file(old_dir + "/interp_nodes.bin");
        auto new_data = read_file(new_dir + "/interp_nodes.bin");
        write_merge(PatchFileId::INTERP_NODES, "interp_nodes.bin", old_data, new_data, 8);
    }

    // admin_polygons.bin
    {
        auto old_data = read_file(old_dir + "/admin_polygons.bin");
        auto new_data = read_file(new_dir + "/admin_polygons.bin");
        // Zero padding bytes (offset 13-15 between admin_level and area) for deterministic comparison
        if (admin_stride == 24) {
            for (size_t i = 0; i + admin_stride <= old_data.size(); i += admin_stride)
                memset(old_data.data() + i + 13, 0, 3);
            for (size_t i = 0; i + admin_stride <= new_data.size(); i += admin_stride)
                memset(new_data.data() + i + 13, 0, 3);
        }
        remap_field(old_data, admin_stride, 8, str_remap);
        auto old_v = read_file(old_dir + "/admin_vertices.bin");
        auto new_v = read_file(new_dir + "/admin_vertices.bin");
        auto& fixups = file_fixups[static_cast<uint32_t>(PatchFileId::ADMIN_POLYGONS)];
        size_t an = old_data.size() / admin_stride;
        std::vector<uint32_t> old_offsets(an);
        for (size_t i = 0; i < an; i++) memcpy(&old_offsets[i], old_data.data() + i * admin_stride, 4);
        fixup_admin_offsets(old_data, old_v, new_data, new_v, admin_stride);
        for (size_t i = 0; i < an; i++) {
            uint32_t new_off; memcpy(&new_off, old_data.data() + i * admin_stride, 4);
            if (new_off != old_offsets[i]) fixups.push_back({static_cast<uint32_t>(i), new_off});
        }
        write_merge(PatchFileId::ADMIN_POLYGONS, "admin_polygons.bin", old_data, new_data, admin_stride);
    }

    // admin_vertices.bin
    {
        auto old_data = read_file(old_dir + "/admin_vertices.bin");
        auto new_data = read_file(new_dir + "/admin_vertices.bin");
        write_merge(PatchFileId::ADMIN_VERTICES, "admin_vertices.bin", old_data, new_data, 8);
    }

    // --- Section: Entry/cell files as full new data ---
    // These are small relative to data files and rebuilt by patch tool isn't reliable.
    // Include them as full replacement. zstd transport compression handles the rest.
    // Entry/cell files: NOT included.
    // The patch tool reconstructs them from the derived ID remaps
    // (extracted implicitly from the merge sequences above).
    // geo_cells.bin, admin_cells.bin, and all entry files are rebuilt.
    std::cerr << "  (entry/cell files: omitted, rebuilt by patch tool)" << std::endl;

    // Include cell_id changes for entry reconstruction
    {
        // Geo cell changes
        auto new_geo = read_file(new_dir + "/geo_cells.bin");
        auto old_geo = read_file(old_dir + "/geo_cells.bin");
        std::unordered_set<uint64_t> old_set, new_set;
        for (size_t i = 0; i < old_geo.size() / 20; i++) {
            uint64_t c; memcpy(&c, old_geo.data() + i * 20, 8); old_set.insert(c);
        }
        for (size_t i = 0; i < new_geo.size() / 20; i++) {
            uint64_t c; memcpy(&c, new_geo.data() + i * 20, 8); new_set.insert(c);
        }
        std::vector<uint64_t> g_added, g_removed;
        for (auto c : new_set) if (!old_set.count(c)) g_added.push_back(c);
        for (auto c : old_set) if (!new_set.count(c)) g_removed.push_back(c);
        std::sort(g_added.begin(), g_added.end());
        std::sort(g_removed.begin(), g_removed.end());

        uint32_t marker = CELL_CHANGES_GEO_MARKER;
        uint32_t na = g_added.size(), nr = g_removed.size();
        wval(patch, &marker, 4); wval(patch, &na, 4); wval(patch, &nr, 4);
        for (auto c : g_added) wval(patch, &c, 8);
        for (auto c : g_removed) wval(patch, &c, 8);
        std::cerr << "  Geo cell changes: +" << na << " -" << nr << " ("
                  << (na + nr) * 8 << " bytes)" << std::endl;

        // Admin cell changes
        auto new_ac = read_file(new_dir + "/admin_cells.bin");
        auto old_ac = read_file(old_dir + "/admin_cells.bin");
        old_set.clear(); new_set.clear();
        for (size_t i = 0; i < old_ac.size() / 12; i++) {
            uint64_t c; memcpy(&c, old_ac.data() + i * 12, 8); old_set.insert(c);
        }
        for (size_t i = 0; i < new_ac.size() / 12; i++) {
            uint64_t c; memcpy(&c, new_ac.data() + i * 12, 8); new_set.insert(c);
        }
        std::vector<uint64_t> a_added, a_removed;
        for (auto c : new_set) if (!old_set.count(c)) a_added.push_back(c);
        for (auto c : old_set) if (!new_set.count(c)) a_removed.push_back(c);
        std::sort(a_added.begin(), a_added.end());
        std::sort(a_removed.begin(), a_removed.end());

        marker = CELL_CHANGES_ADMIN_MARKER;
        na = a_added.size(); nr = a_removed.size();
        wval(patch, &marker, 4); wval(patch, &na, 4); wval(patch, &nr, 4);
        for (auto c : a_added) wval(patch, &c, 8);
        for (auto c : a_removed) wval(patch, &c, 8);
        std::cerr << "  Admin cell changes: +" << na << " -" << nr << std::endl;

        // Derive ID remaps from stored merge sequences (same as patch tool does)
        auto derive_remap = [&](PatchFileId id, size_t old_size, size_t stride) -> std::unordered_map<uint32_t,uint32_t> {
            auto it = stored_merges.find(static_cast<uint32_t>(id));
            if (it == stored_merges.end()) return {};
            auto vec = derive_id_remap_from_merge(it->second, old_size / stride, stride);
            std::unordered_map<uint32_t,uint32_t> m;
            for (uint32_t i = 0; i < vec.size(); i++)
                if (vec[i] != 0xFFFFFFFF) m[i] = vec[i];
            return m;
        };

        auto w_rm_d = derive_remap(PatchFileId::STREET_WAYS,
            read_file(old_dir + "/street_ways.bin").size(), way_stride);
        auto a_rm_d = derive_remap(PatchFileId::ADDR_POINTS,
            read_file(old_dir + "/addr_points.bin").size(), 16);
        auto i_rm_d = derive_remap(PatchFileId::INTERP_WAYS,
            read_file(old_dir + "/interp_ways.bin").size(), interp_stride);
        auto ad_rm_d = derive_remap(PatchFileId::ADMIN_POLYGONS,
            read_file(old_dir + "/admin_polygons.bin").size(), admin_stride);

        // Rebuild entries using derived remaps (same as patch tool)
        auto old_se2 = read_file(old_dir + "/street_entries.bin");
        auto old_ae2 = read_file(old_dir + "/addr_entries.bin");
        auto old_ie2 = read_file(old_dir + "/interp_entries.bin");
        auto derived = rebuild_geo_from_remap(old_geo, old_se2, old_ae2, old_ie2,
                                               w_rm_d, a_rm_d, i_rm_d, g_added, g_removed);
        auto old_admc = read_file(old_dir + "/admin_cells.bin");
        auto old_adme = read_file(old_dir + "/admin_entries.bin");
        auto admin_derived = rebuild_admin_from_remap(old_admc, old_adme, ad_rm_d);

        // Write correction deltas: derived → new (using zstd --patch-from)
        struct CorrEntry { PatchFileId fid; std::string label; std::vector<char>* data; std::string fname; };
        CorrEntry corr_entries[] = {
            // SKIP: geo_cells and admin_cells are included as full replacement below
            {PatchFileId::STREET_ENTRIES, "street_entries", &derived.street_entries_data, "street_entries.bin"},
            {PatchFileId::ADDR_ENTRIES, "addr_entries", &derived.addr_entries_data, "addr_entries.bin"},
            {PatchFileId::INTERP_ENTRIES, "interp_entries", &derived.interp_entries_data, "interp_entries.bin"},
            // admin_cells included as full replacement above
            {PatchFileId::ADMIN_ENTRIES, "admin_entries", &admin_derived.admin_entries_data, "admin_entries.bin"},
        };
        for (auto& ce : corr_entries) {
            auto& derived_data = ce.data;
            auto new_fname = ce.fname;
            auto fname = ce.label;
            auto fid = ce.fid;
            auto new_data = read_file(new_dir + "/" + new_fname);

            // Write derived → new as zstd --patch-from via temp files
            std::string ref_path = tmpdir + "/corr_" + fname + ".ref";
            std::string zst_path = tmpdir + "/corr_" + fname + ".zst";
            write_file(ref_path, *derived_data);
            system(("zstd --patch-from='" + ref_path + "' '" + new_dir + "/" + new_fname +
                    "' -o '" + zst_path + "' -f --quiet 2>/dev/null").c_str());
            auto zst_data = read_file(zst_path);

            // Write as correction section: stride=0xFE (correction marker)
            uint32_t f = static_cast<uint32_t>(fid), stride_corr = 0xFE;
            uint64_t rs = derived_data->size(), ns = new_data.size();
            uint32_t nfix = 0; uint64_t ds = zst_data.size();
            wval(patch, &f, 4); wval(patch, &stride_corr, 4);
            wval(patch, &rs, 8); wval(patch, &ns, 8);
            wval(patch, &nfix, 4); wval(patch, &ds, 8);
            patch.insert(patch.end(), zst_data.begin(), zst_data.end());
            std::cerr << "  " << new_fname << ": correction " << ds << " bytes ("
                      << std::fixed << std::setprecision(2)
                      << (ns > 0 ? ds * 100.0 / ns : 0) << "%)" << std::endl;
            remove(ref_path.c_str()); remove(zst_path.c_str());
        }

        // Cell flag corrections: identify cells whose has_street/has_addr/has_interp
        // flags differ between old and new. Only ~3K cells typically.
        // The patch tool uses these to correctly assign entry offsets during geo_cells rebuild.
        {
            auto new_geo_data = read_file(new_dir + "/geo_cells.bin");
            std::unordered_map<uint64_t, uint8_t> old_cell_flags;
            for (size_t i = 0; i < old_geo.size() / 20; i++) {
                uint64_t cid; memcpy(&cid, old_geo.data() + i * 20, 8);
                uint32_t s, a, ip;
                memcpy(&s, old_geo.data()+i*20+8, 4); memcpy(&a, old_geo.data()+i*20+12, 4);
                memcpy(&ip, old_geo.data()+i*20+16, 4);
                uint8_t flags = (s != 0xFFFFFFFF ? 1 : 0) | (a != 0xFFFFFFFF ? 2 : 0) | (ip != 0xFFFFFFFF ? 4 : 0);
                old_cell_flags[cid] = flags;
            }
            std::vector<std::pair<uint64_t, uint8_t>> flag_corrections;
            for (size_t i = 0; i < new_geo_data.size() / 20; i++) {
                uint64_t cid; memcpy(&cid, new_geo_data.data() + i * 20, 8);
                uint32_t s, a, ip;
                memcpy(&s, new_geo_data.data()+i*20+8, 4); memcpy(&a, new_geo_data.data()+i*20+12, 4);
                memcpy(&ip, new_geo_data.data()+i*20+16, 4);
                uint8_t new_flags = (s != 0xFFFFFFFF ? 1 : 0) | (a != 0xFFFFFFFF ? 2 : 0) | (ip != 0xFFFFFFFF ? 4 : 0);
                auto it = old_cell_flags.find(cid);
                uint8_t old_f = (it != old_cell_flags.end()) ? it->second : 0;
                if (new_flags != old_f)
                    flag_corrections.push_back({cid, new_flags});
            }
            uint32_t fm = CELL_FLAGS_MARKER;
            uint32_t fc = static_cast<uint32_t>(flag_corrections.size());
            wval(patch, &fm, 4); wval(patch, &fc, 4);
            for (auto& [cid, flags] : flag_corrections) {
                wval(patch, &cid, 8); wval(patch, &flags, 1);
            }
            std::cerr << "  Cell flag corrections: " << fc << " cells ("
                      << fc * 9 << " bytes)" << std::endl;
        }

        if (false) { // disabled: correction approach doesn't produce matching rebuilds
        // Rebuild geo_cells from NEW entry files (which the patch tool will have after corrections)
        // using the derived cell structure. This produces a nearly-correct geo_cells.
        auto rebuild_geo_from_entries = [&](const std::vector<char>& derived_geo,
                                             const std::string& se_path, const std::string& ae_path,
                                             const std::string& ie_path) -> std::vector<char> {
            auto se = read_file(se_path), ae = read_file(ae_path), ie = read_file(ie_path);
            auto build_offsets = [](const std::vector<char>& ef) -> std::vector<uint32_t> {
                std::vector<uint32_t> offs; uint32_t p = 0;
                while (p + 2 <= ef.size()) { offs.push_back(p); uint16_t c; memcpy(&c, ef.data()+p, 2); p += 2+c*4; }
                return offs;
            };
            auto so = build_offsets(se), ao = build_offsets(ae), io = build_offsets(ie);
            size_t n = derived_geo.size() / 20, si = 0, ai = 0, ii = 0;
            uint32_t no_data = 0xFFFFFFFF;
            std::vector<char> result(derived_geo.size());
            for (size_t c = 0; c < n; c++) {
                memcpy(result.data() + c*20, derived_geo.data() + c*20, 8);
                uint32_t os, oa, oi;
                memcpy(&os, derived_geo.data()+c*20+8, 4); memcpy(&oa, derived_geo.data()+c*20+12, 4);
                memcpy(&oi, derived_geo.data()+c*20+16, 4);
                uint32_t ns = (os != no_data && si < so.size()) ? so[si++] : no_data;
                uint32_t na = (oa != no_data && ai < ao.size()) ? ao[ai++] : no_data;
                uint32_t ni = (oi != no_data && ii < io.size()) ? io[ii++] : no_data;
                memcpy(result.data()+c*20+8, &ns, 4); memcpy(result.data()+c*20+12, &na, 4);
                memcpy(result.data()+c*20+16, &ni, 4);
            }
            return result;
        };

        // For geo_cells: rebuild from new entries + derived cell structure
        auto geo_from_new_entries = rebuild_geo_from_entries(derived.geo_cells_data,
            new_dir + "/street_entries.bin", new_dir + "/addr_entries.bin", new_dir + "/interp_entries.bin");

        for (auto& [c_fid, c_label, c_data, c_fname] : std::vector<std::tuple<PatchFileId, std::string, std::vector<char>*, std::string>>{
            {PatchFileId::GEO_CELLS, "geo_cells", &geo_from_new_entries, "geo_cells.bin"},
            {PatchFileId::ADMIN_CELLS, "admin_cells", &admin_derived.admin_cells_data, "admin_cells.bin"}})
        {
            auto c_new_data = read_file(new_dir + "/" + c_fname);
            std::string c_ref = tmpdir + "/cell_" + c_label + ".ref";
            std::string c_zst = tmpdir + "/cell_" + c_label + ".zst";
            write_file(c_ref, *c_data);
            system(("zstd --patch-from='" + c_ref + "' '" + new_dir + "/" + c_fname +
                    "' -o '" + c_zst + "' -f --quiet 2>/dev/null").c_str());
            auto c_zst_data = read_file(c_zst);
            uint32_t cf = static_cast<uint32_t>(c_fid), c_st = 0xFE;
            uint64_t c_rs = c_data->size(), c_ns = c_new_data.size();
            uint32_t c_nfix = 0; uint64_t c_ds = c_zst_data.size();
            wval(patch, &cf, 4); wval(patch, &c_st, 4);
            wval(patch, &c_rs, 8); wval(patch, &c_ns, 8);
            wval(patch, &c_nfix, 4); wval(patch, &c_ds, 8);
            patch.insert(patch.end(), c_zst_data.begin(), c_zst_data.end());
            std::cerr << "  " << c_fname << ": correction " << c_ds << " bytes ("
                      << std::fixed << std::setprecision(2)
                      << (c_ns > 0 ? c_ds * 100.0 / c_ns : 0) << "%)" << std::endl;
            remove(c_ref.c_str()); remove(c_zst.c_str());
        }
    } } // end disabled correction block

    if (false) // disabled — entry files not included
    for (auto& [id, name] : std::vector<std::pair<PatchFileId, std::string>>{
        {PatchFileId::GEO_CELLS, "geo_cells.bin"}}) {
        auto data = read_file(new_dir + "/" + name);
        uint32_t fid = static_cast<uint32_t>(id);
        uint32_t st = 0; // stride 0 = full replacement
        uint64_t os = 0, ns = data.size();
        uint32_t nfix = 0;
        uint64_t ss = data.size();
        wval(patch, &fid, 4); wval(patch, &st, 4);
        wval(patch, &os, 8); wval(patch, &ns, 8);
        wval(patch, &nfix, 4); // no fixups
        wval(patch, &ss, 8);   // data size
        patch.insert(patch.end(), data.begin(), data.end());
        std::cerr << "  " << name << ": full " << data.size() << " bytes" << std::endl;
    }

    // End marker
    uint32_t end_marker = 0xFFFFFFFF;
    wval(patch, &end_marker, 4);

    std::cerr << "\nUncompressed patch: " << patch.size() << " bytes ("
              << patch.size() / 1024 / 1024 << " MiB)" << std::endl;

    // Compress whole patch with zstd for transport
    {
        std::string raw_path = tmpdir + "/patch.raw";
        write_file(raw_path, patch);
        std::string cmd = "zstd -19 '" + raw_path + "' -o '" + patch_path + "' -f --quiet 2>/dev/null";
        system(cmd.c_str());
        auto compressed = read_file(patch_path);
        std::cerr << "Compressed patch: " << compressed.size() << " bytes ("
                  << compressed.size() / 1024 / 1024 << " MiB)" << std::endl;
        remove(raw_path.c_str());
    }

    std::string rm_cmd = "rm -rf '" + tmpdir + "'";
    system(rm_cmd.c_str());
    return 0;
}
