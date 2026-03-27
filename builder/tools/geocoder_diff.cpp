// geocoder-diff: Compare two deterministic geocoder builds, produce a .gcpatch.
//
// For each file: apply string remap to old, write temp, run zstd --patch-from.
// Package all per-file zstd patches + remap table into .gcpatch.
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
#include <unordered_map>
#include <vector>

#include <unistd.h>
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

// --- Remap functions ---

static void remap_addr_points(std::vector<char>& data, const std::unordered_map<uint32_t,uint32_t>& rm) {
    for (size_t i = 0; i + 16 <= data.size(); i += 16) {
        for (size_t off : {8, 12}) {
            uint32_t v; memcpy(&v, data.data() + i + off, 4);
            auto it = rm.find(v);
            if (it != rm.end()) memcpy(data.data() + i + off, &it->second, 4);
        }
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

    // Create temp directory
    std::string tmpdir = "/tmp/geocoder-diff-" + std::to_string(getpid());
    ensure_dir(tmpdir);

    // Load string pools, build remap
    std::cerr << "Building string remap..." << std::endl;
    auto old_strings = read_file(old_dir + "/strings.bin");
    auto new_strings = read_file(new_dir + "/strings.bin");
    auto remap = build_string_remap(old_strings, new_strings);
    std::cerr << "  " << remap.size() << " strings mapped" << std::endl;

    // Detect struct strides
    auto detect_stride = [](const std::string& path, std::vector<size_t> candidates) -> size_t {
        auto data = read_file(path);
        for (size_t s : candidates)
            if (data.size() % s == 0 && data.size() > 0) return s;
        return candidates[0];
    };
    size_t way_stride = detect_stride(old_dir + "/street_ways.bin", {12, 9});
    size_t interp_stride = detect_stride(old_dir + "/interp_ways.bin", {20, 18});
    size_t admin_stride = detect_stride(old_dir + "/admin_polygons.bin", {24, 20, 19});

    // Open patch file
    std::ofstream pf(patch_path, std::ios::binary);
    pf.write(GCPATCH_MAGIC, 8);
    uint32_t ver = GCPATCH_VERSION, flags = 0;
    write_val(pf, &ver, 4);
    write_val(pf, &flags, 4);

    // Section: string remap
    {
        uint32_t marker = 0xFFFFFFFE;
        write_val(pf, &marker, 4);
        // Collect all remap entries (at string start positions)
        std::vector<std::pair<uint32_t,uint32_t>> entries;
        size_t pos = 0;
        while (pos < old_strings.size()) {
            uint32_t old_off = static_cast<uint32_t>(pos);
            auto it = remap.find(old_off);
            uint32_t new_off = (it != remap.end()) ? it->second : old_off; // identity for deleted strings
            entries.push_back({old_off, new_off});
            pos += strlen(old_strings.data() + pos) + 1;
        }
        uint32_t count = static_cast<uint32_t>(entries.size());
        write_val(pf, &count, 4);
        for (auto& [o, n] : entries) { write_val(pf, &o, 4); write_val(pf, &n, 4); }
        std::cerr << "  Remap: " << count << " entries" << std::endl;
    }

    // Helper: remap old file, write temp, run zstd --patch-from, package result
    auto diff_file = [&](PatchFileId id, const std::string& name,
                          std::function<void(std::vector<char>&)> remap_fn) {
        std::cerr << "  " << name << ": ";

        std::string old_path = old_dir + "/" + name;
        std::string new_path = new_dir + "/" + name;
        std::string ref_path;

        if (remap_fn) {
            // Remap old file, write to temp
            auto old_data = read_file(old_path);
            remap_fn(old_data);
            ref_path = tmpdir + "/" + name;
            write_file(ref_path, old_data);
        } else {
            ref_path = old_path;
        }

        // Run zstd --patch-from
        std::string zst_path = tmpdir + "/" + name + ".zst";
        std::string cmd = "zstd --patch-from='" + ref_path + "' '" + new_path +
                          "' -o '" + zst_path + "' -f --quiet 2>/dev/null";
        system(cmd.c_str());

        auto zst_data = read_file(zst_path);
        auto new_data = read_file(new_path);
        auto old_data = read_file(old_path);

        // Write section: file_id, encoding, old_size, new_size, delta_size, delta_data
        uint32_t fid = static_cast<uint32_t>(id);
        uint32_t enc = static_cast<uint32_t>(PatchEncoding::COPY_INSERT);
        uint64_t os = old_data.size(), ns = new_data.size(), ds = zst_data.size();
        write_val(pf, &fid, 4);
        write_val(pf, &enc, 4);
        write_val(pf, &os, 8);
        write_val(pf, &ns, 8);
        write_val(pf, &ds, 8);
        pf.write(zst_data.data(), zst_data.size());

        double pct = ns > 0 ? ds * 100.0 / ns : 0;
        std::cerr << ds << " bytes (" << std::fixed << std::setprecision(1)
                  << pct << "% of " << ns / 1024 / 1024 << " MiB)" << std::endl;

        // Clean temp files
        if (remap_fn) remove(ref_path.c_str());
        remove(zst_path.c_str());
    };

    // Diff all files
    size_t ws = way_stride, is = interp_stride, as = admin_stride;
    diff_file(PatchFileId::STRINGS, "strings.bin", {});
    diff_file(PatchFileId::ADDR_POINTS, "addr_points.bin",
        [&](std::vector<char>& d) { remap_addr_points(d, remap); });
    diff_file(PatchFileId::STREET_WAYS, "street_ways.bin",
        [&, ws](std::vector<char>& d) { remap_field(d, ws, ws == 12 ? 8 : 5, remap); });
    diff_file(PatchFileId::INTERP_WAYS, "interp_ways.bin",
        [&, is](std::vector<char>& d) { remap_field(d, is, is >= 20 ? 8 : 5, remap); });
    diff_file(PatchFileId::ADMIN_POLYGONS, "admin_polygons.bin",
        [&, as](std::vector<char>& d) { remap_field(d, as, 8, remap); });

    std::function<void(std::vector<char>&)> no_remap;
    diff_file(PatchFileId::STREET_NODES, "street_nodes.bin", no_remap);
    diff_file(PatchFileId::INTERP_NODES, "interp_nodes.bin", no_remap);
    diff_file(PatchFileId::ADMIN_VERTICES, "admin_vertices.bin", no_remap);
    diff_file(PatchFileId::GEO_CELLS, "geo_cells.bin", no_remap);
    diff_file(PatchFileId::STREET_ENTRIES, "street_entries.bin", no_remap);
    diff_file(PatchFileId::ADDR_ENTRIES, "addr_entries.bin", no_remap);
    diff_file(PatchFileId::INTERP_ENTRIES, "interp_entries.bin", no_remap);
    diff_file(PatchFileId::ADMIN_CELLS, "admin_cells.bin", no_remap);
    diff_file(PatchFileId::ADMIN_ENTRIES, "admin_entries.bin", no_remap);

    uint32_t end = 0xFFFFFFFF;
    write_val(pf, &end, 4);

    // Cleanup
    std::string rm_cmd = "rm -rf '" + tmpdir + "'";
    system(rm_cmd.c_str());

    auto total = pf.tellp();
    std::cerr << "\nTotal patch: " << total << " bytes (" << total / 1024 / 1024 << " MiB)" << std::endl;
    return 0;
}
