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
        if (section_id == 0xFFFFFFFE) {
            uint32_t count; pf.read(reinterpret_cast<char*>(&count), 4);
            for (uint32_t i = 0; i < count; i++) {
                uint32_t old_off, new_off;
                pf.read(reinterpret_cast<char*>(&old_off), 4);
                pf.read(reinterpret_cast<char*>(&new_off), 4);
                if (old_off != new_off) str_remap[old_off] = new_off;
            }
            std::cerr << "Loaded " << str_remap.size() << " string remap entries" << std::endl;
        }
    }

    // Read offset fixup tables
    struct Fixup { uint32_t record_idx; uint32_t new_value; };
    std::unordered_map<uint32_t, std::vector<Fixup>> fixup_tables; // file_id → fixups
    std::unordered_map<uint32_t, uint32_t> fixup_strides; // file_id → stride
    while (pf) {
        uint32_t marker; pf.read(reinterpret_cast<char*>(&marker), 4);
        if (marker != FIXUP_MARKER) {
            pf.seekg(-4, std::ios::cur);
            break;
        }
        uint32_t fid, stride, count;
        pf.read(reinterpret_cast<char*>(&fid), 4);
        pf.read(reinterpret_cast<char*>(&stride), 4);
        pf.read(reinterpret_cast<char*>(&count), 4);
        auto& vec = fixup_tables[fid];
        fixup_strides[fid] = stride;
        vec.resize(count);
        for (uint32_t i = 0; i < count; i++) {
            pf.read(reinterpret_cast<char*>(&vec[i].record_idx), 4);
            pf.read(reinterpret_cast<char*>(&vec[i].new_value), 4);
        }
        std::cerr << "Loaded " << count << " fixups for file " << fid << std::endl;
    }

    // Detect struct strides from current files
    auto detect_stride = [&](const std::string& name, std::vector<size_t> cs) -> size_t {
        auto d = read_file(cur_dir + "/" + name);
        for (size_t s : cs) if (d.size() % s == 0 && d.size() > 0) return s;
        return cs[0];
    };
    size_t way_stride = detect_stride("street_ways.bin", {12, 9});
    size_t interp_stride = detect_stride("interp_ways.bin", {20, 18});
    size_t admin_stride = detect_stride("admin_polygons.bin", {24, 20, 19});

    // Helper: apply string remap to old data file
    auto apply_str_remap = [&](std::vector<char>& data, uint32_t file_id) {
        if (str_remap.empty()) return;
        if (file_id == (uint32_t)PatchFileId::ADDR_POINTS) {
            for (size_t i = 0; i + 16 <= data.size(); i += 16)
                for (size_t off : {8, 12}) {
                    uint32_t v; memcpy(&v, data.data() + i + off, 4);
                    auto it = str_remap.find(v);
                    if (it != str_remap.end()) memcpy(data.data() + i + off, &it->second, 4);
                }
        } else if (file_id == (uint32_t)PatchFileId::STREET_WAYS) {
            size_t name_off = (way_stride == 12) ? 8 : 5;
            for (size_t i = 0; i + way_stride <= data.size(); i += way_stride) {
                uint32_t v; memcpy(&v, data.data() + i + name_off, 4);
                auto it = str_remap.find(v);
                if (it != str_remap.end()) memcpy(data.data() + i + name_off, &it->second, 4);
            }
        } else if (file_id == (uint32_t)PatchFileId::INTERP_WAYS) {
            size_t soff = (interp_stride >= 20) ? 8 : 5;
            for (size_t i = 0; i + interp_stride <= data.size(); i += interp_stride) {
                uint32_t v; memcpy(&v, data.data() + i + soff, 4);
                auto it = str_remap.find(v);
                if (it != str_remap.end()) memcpy(data.data() + i + soff, &it->second, 4);
            }
        } else if (file_id == (uint32_t)PatchFileId::ADMIN_POLYGONS) {
            for (size_t i = 0; i + admin_stride <= data.size(); i += admin_stride) {
                uint32_t v; memcpy(&v, data.data() + i + 8, 4);
                auto it = str_remap.find(v);
                if (it != str_remap.end()) memcpy(data.data() + i + 8, &it->second, 4);
            }
        }
    };

    // Helper: apply offset fixups
    auto apply_fixups = [&](std::vector<char>& data, uint32_t file_id) {
        auto it = fixup_tables.find(file_id);
        if (it == fixup_tables.end()) return;
        uint32_t stride = fixup_strides[file_id];
        for (auto& f : it->second) {
            size_t pos = (size_t)f.record_idx * stride;
            if (pos + 4 <= data.size())
                memcpy(data.data() + pos, &f.new_value, 4);
        }
    };

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

        if (encoding == (uint32_t)PatchEncoding::ZSTD_DELTA) {
            uint64_t old_size, new_size, delta_size;
            pf.read(reinterpret_cast<char*>(&old_size), 8);
            pf.read(reinterpret_cast<char*>(&new_size), 8);
            pf.read(reinterpret_cast<char*>(&delta_size), 8);

            // Write zstd frame to temp
            std::string zst = tmpdir + "/" + std::string(filename) + ".zst";
            write_temp(zst, delta_size);

            // Load old file, apply string remap + fixups → write remapped ref
            auto old_data = read_file(cur_dir + "/" + std::string(filename));
            apply_str_remap(old_data, file_id);
            apply_fixups(old_data, file_id);
            std::string ref = tmpdir + "/" + std::string(filename) + ".ref";
            write_file(ref, old_data);

            // Apply zstd delta
            std::string out = out_dir + "/" + std::string(filename);
            if (!zstd_apply(ref, zst, out)) {
                std::cerr << "FAILED: " << filename << std::endl; return 1;
            }
            std::cerr << "  " << filename << ": " << read_file(out).size() << " bytes" << std::endl;
            remove(zst.c_str()); remove(ref.c_str());

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
