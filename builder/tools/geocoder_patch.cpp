// geocoder-patch: Apply a .gcpatch to a geocoder index directory.
//
// For each file: apply string remap to old, write temp, run zstd -d --patch-from.
//
// Usage: geocoder-patch <current-dir> <patch-file> -o <output-dir>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include <unistd.h>
#include "patch_format.h"

// --- String remap application ---

static void remap_addr_points(std::vector<char>& data,
                                const std::unordered_map<uint32_t,uint32_t>& rm) {
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

int main(int argc, char* argv[]) {
    if (argc < 5 || std::string(argv[3]) != "-o") {
        std::cerr << "Usage: geocoder-patch <current-dir> <patch-file> -o <output-dir>" << std::endl;
        return 1;
    }
    std::string cur_dir = argv[1], patch_path = argv[2], out_dir = argv[4];
    ensure_dir(out_dir);

    std::string tmpdir = "/tmp/geocoder-patch-" + std::to_string(getpid());
    ensure_dir(tmpdir);

    std::ifstream pf(patch_path, std::ios::binary);
    if (!pf) { std::cerr << "Cannot open " << patch_path << std::endl; return 1; }

    // Read header
    char magic[8]; pf.read(magic, 8);
    if (memcmp(magic, GCPATCH_MAGIC, 8) != 0) {
        std::cerr << "Invalid patch file" << std::endl; return 1;
    }
    uint32_t version, flags;
    pf.read(reinterpret_cast<char*>(&version), 4);
    pf.read(reinterpret_cast<char*>(&flags), 4);
    if (version != GCPATCH_VERSION) {
        std::cerr << "Unsupported version " << version << std::endl; return 1;
    }

    // Read string remap
    std::unordered_map<uint32_t, uint32_t> str_remap;
    {
        uint32_t section_id; pf.read(reinterpret_cast<char*>(&section_id), 4);
        if (section_id != 0xFFFFFFFE) {
            std::cerr << "Expected remap section" << std::endl; return 1;
        }
        uint32_t count; pf.read(reinterpret_cast<char*>(&count), 4);
        for (uint32_t i = 0; i < count; i++) {
            uint32_t old_off, new_off;
            pf.read(reinterpret_cast<char*>(&old_off), 4);
            pf.read(reinterpret_cast<char*>(&new_off), 4);
            str_remap[old_off] = new_off;
        }
        std::cerr << "Loaded " << count << " string remap entries" << std::endl;
    }

    // Detect struct strides
    auto detect_stride = [&](const std::string& name, std::vector<size_t> candidates) -> size_t {
        auto data = read_file(cur_dir + "/" + name);
        for (size_t s : candidates)
            if (data.size() % s == 0 && data.size() > 0) return s;
        return candidates[0];
    };
    size_t way_stride = detect_stride("street_ways.bin", {12, 9});
    size_t interp_stride = detect_stride("interp_ways.bin", {20, 18});
    size_t admin_stride = detect_stride("admin_polygons.bin", {24, 20, 19});

    // Which files need string remap?
    auto needs_remap = [](uint32_t fid) -> bool {
        return fid == (uint32_t)PatchFileId::ADDR_POINTS ||
               fid == (uint32_t)PatchFileId::STREET_WAYS ||
               fid == (uint32_t)PatchFileId::INTERP_WAYS ||
               fid == (uint32_t)PatchFileId::ADMIN_POLYGONS;
    };

    // Process file sections
    while (pf) {
        uint32_t file_id; pf.read(reinterpret_cast<char*>(&file_id), 4);
        if (!pf || file_id == 0xFFFFFFFF) break;

        uint32_t encoding; pf.read(reinterpret_cast<char*>(&encoding), 4);
        uint64_t old_size, new_size, delta_size;
        pf.read(reinterpret_cast<char*>(&old_size), 8);
        pf.read(reinterpret_cast<char*>(&new_size), 8);
        pf.read(reinterpret_cast<char*>(&delta_size), 8);

        if (file_id >= (uint32_t)PatchFileId::COUNT) {
            pf.seekg(delta_size, std::ios::cur);
            continue;
        }
        const char* filename = patch_file_names[file_id];

        // Write delta data to temp file
        std::string zst_path = tmpdir + "/" + std::string(filename) + ".zst";
        {
            std::ofstream zf(zst_path, std::ios::binary);
            std::vector<char> buf(std::min(delta_size, (uint64_t)1024*1024));
            uint64_t remaining = delta_size;
            while (remaining > 0) {
                size_t chunk = std::min(remaining, (uint64_t)buf.size());
                pf.read(buf.data(), chunk);
                zf.write(buf.data(), chunk);
                remaining -= chunk;
            }
        }

        // Prepare reference file (old file, with remap if needed)
        std::string ref_path;
        if (needs_remap(file_id)) {
            auto old_data = read_file(cur_dir + "/" + std::string(filename));
            if (file_id == (uint32_t)PatchFileId::ADDR_POINTS)
                remap_addr_points(old_data, str_remap);
            else if (file_id == (uint32_t)PatchFileId::STREET_WAYS)
                remap_field(old_data, way_stride, way_stride == 12 ? 8 : 5, str_remap);
            else if (file_id == (uint32_t)PatchFileId::INTERP_WAYS)
                remap_field(old_data, interp_stride, interp_stride >= 20 ? 8 : 5, str_remap);
            else if (file_id == (uint32_t)PatchFileId::ADMIN_POLYGONS)
                remap_field(old_data, admin_stride, 8, str_remap);
            ref_path = tmpdir + "/" + std::string(filename) + ".ref";
            write_file(ref_path, old_data);
        } else {
            ref_path = cur_dir + "/" + std::string(filename);
        }

        // Apply delta: zstd -d --patch-from=ref delta -o output
        std::string out_path = out_dir + "/" + std::string(filename);
        std::string cmd = "zstd --patch-from='" + ref_path + "' -d '" + zst_path +
                          "' -o '" + out_path + "' -f --long=31";
        int ret = system(cmd.c_str());
        if (ret != 0) {
            std::cerr << "FAILED: " << filename << " (zstd exit " << ret << ")" << std::endl;
            return 1;
        }

        auto out_data = read_file(out_path);
        std::cerr << "  " << filename << ": " << out_data.size() << " bytes" << std::endl;

        // Cleanup temp files
        remove(zst_path.c_str());
        if (needs_remap(file_id)) remove(ref_path.c_str());
    }

    // Cleanup
    std::string rm_cmd = "rm -rf '" + tmpdir + "'";
    system(rm_cmd.c_str());

    std::cerr << "Patch applied. Output in " << out_dir << std::endl;
    return 0;
}
