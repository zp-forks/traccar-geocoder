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

    // Skip string remap section (two-stage encoding handles remapping internally)
    {
        uint32_t section_id; pf.read(reinterpret_cast<char*>(&section_id), 4);
        if (section_id == 0xFFFFFFFE) {
            uint32_t count; pf.read(reinterpret_cast<char*>(&count), 4);
            pf.seekg(count * 8, std::ios::cur);
            std::cerr << "Skipped string remap (" << count << " entries, handled by two-stage)" << std::endl;
        } else {
            std::cerr << "Warning: expected remap section, got " << section_id << std::endl;
            // Seek back so section processing can handle it
            pf.seekg(-4, std::ios::cur);
        }
    }

    // Process file sections
    while (pf) {
        uint32_t file_id; pf.read(reinterpret_cast<char*>(&file_id), 4);
        if (!pf || file_id == 0xFFFFFFFF) break;
        uint32_t encoding; pf.read(reinterpret_cast<char*>(&encoding), 4);

        if (file_id >= (uint32_t)PatchFileId::COUNT) {
            std::cerr << "  Unknown section " << file_id << std::endl;
            break;
        }
        const char* filename = patch_file_names[file_id];

        auto write_temp = [&](const std::string& path, uint64_t size) {
            std::ofstream zf(path, std::ios::binary);
            std::vector<char> buf(std::min(size, (uint64_t)1024*1024));
            uint64_t remaining = size;
            while (remaining > 0) {
                size_t chunk = std::min(remaining, (uint64_t)buf.size());
                pf.read(buf.data(), chunk);
                zf.write(buf.data(), chunk);
                remaining -= chunk;
            }
        };

        auto zstd_apply = [&](const std::string& ref, const std::string& zst, const std::string& out) -> bool {
            std::string cmd = "zstd --patch-from='" + ref + "' -d '" + zst + "' -o '" + out + "' -f --long=31 --quiet 2>/dev/null";
            return system(cmd.c_str()) == 0;
        };

        if (encoding == (uint32_t)PatchEncoding::COPY_INSERT) {
            // Single-stage: old → new
            uint64_t old_size, new_size, delta_size;
            pf.read(reinterpret_cast<char*>(&old_size), 8);
            pf.read(reinterpret_cast<char*>(&new_size), 8);
            pf.read(reinterpret_cast<char*>(&delta_size), 8);

            std::string zst = tmpdir + "/" + std::string(filename) + ".zst";
            write_temp(zst, delta_size);

            std::string ref = cur_dir + "/" + std::string(filename);
            std::string out = out_dir + "/" + std::string(filename);
            if (!zstd_apply(ref, zst, out)) {
                std::cerr << "FAILED: " << filename << std::endl; return 1;
            }
            std::cerr << "  " << filename << ": " << read_file(out).size() << " bytes" << std::endl;
            remove(zst.c_str());

        } else if (encoding == (uint32_t)PatchEncoding::TWO_STAGE) {
            // Two-stage: old → remapped → new
            uint64_t old_size, remap_size, new_size, s1_size, s2_size;
            pf.read(reinterpret_cast<char*>(&old_size), 8);
            pf.read(reinterpret_cast<char*>(&remap_size), 8);
            pf.read(reinterpret_cast<char*>(&new_size), 8);
            pf.read(reinterpret_cast<char*>(&s1_size), 8);
            pf.read(reinterpret_cast<char*>(&s2_size), 8);

            std::string zst1 = tmpdir + "/" + std::string(filename) + ".s1.zst";
            std::string zst2 = tmpdir + "/" + std::string(filename) + ".s2.zst";
            std::string mid = tmpdir + "/" + std::string(filename) + ".mid";
            std::string out = out_dir + "/" + std::string(filename);

            write_temp(zst1, s1_size);
            write_temp(zst2, s2_size);

            // Stage 1: old → remapped
            std::string ref = cur_dir + "/" + std::string(filename);
            if (!zstd_apply(ref, zst1, mid)) {
                std::cerr << "FAILED stage1: " << filename << std::endl; return 1;
            }
            // Stage 2: remapped → new
            if (!zstd_apply(mid, zst2, out)) {
                std::cerr << "FAILED stage2: " << filename << std::endl; return 1;
            }
            std::cerr << "  " << filename << ": " << read_file(out).size() << " bytes" << std::endl;
            remove(zst1.c_str()); remove(zst2.c_str()); remove(mid.c_str());

        } else {
            std::cerr << "  Unknown encoding " << encoding << " for " << filename << std::endl;
            break;
        }
    }

    // Cleanup
    std::string rm_cmd = "rm -rf '" + tmpdir + "'";
    system(rm_cmd.c_str());

    std::cerr << "Patch applied. Output in " << out_dir << std::endl;
    return 0;
}
