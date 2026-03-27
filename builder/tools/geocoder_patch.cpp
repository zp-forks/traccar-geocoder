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

    // Helper: decompress zstd blob via CLI
    auto zstd_decompress_blob = [&](const std::vector<char>& compressed, size_t expected_size) -> std::vector<char> {
        std::string zst_path = tmpdir + "/blob.zst";
        std::string raw_path = tmpdir + "/blob.raw";
        write_file(zst_path, compressed);
        system(("zstd -d '" + zst_path + "' -o '" + raw_path + "' -f --quiet 2>/dev/null").c_str());
        auto result = read_file(raw_path);
        remove(zst_path.c_str()); remove(raw_path.c_str());
        return result;
    };

    // Read string remap (compressed)
    std::unordered_map<uint32_t, uint32_t> str_remap;
    {
        uint32_t section_id; pf.read(reinterpret_cast<char*>(&section_id), 4);
        if (section_id == 0xFFFFFFFE) {
            uint32_t count; pf.read(reinterpret_cast<char*>(&count), 4);
            uint64_t comp_size; pf.read(reinterpret_cast<char*>(&comp_size), 8);
            std::vector<char> compressed(comp_size);
            pf.read(compressed.data(), comp_size);
            auto blob = zstd_decompress_blob(compressed, count * 8);
            for (uint32_t i = 0; i < count; i++) {
                uint32_t old_off, new_off;
                memcpy(&old_off, blob.data() + i * 8, 4);
                memcpy(&new_off, blob.data() + i * 8 + 4, 4);
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
        uint64_t comp_size; pf.read(reinterpret_cast<char*>(&comp_size), 8);
        std::vector<char> compressed(comp_size);
        pf.read(compressed.data(), comp_size);
        auto blob = zstd_decompress_blob(compressed, count * 8);
        auto& vec = fixup_tables[fid];
        fixup_strides[fid] = stride;
        vec.resize(count);
        for (uint32_t i = 0; i < count; i++) {
            memcpy(&vec[i].record_idx, blob.data() + i * 8, 4);
            memcpy(&vec[i].new_value, blob.data() + i * 8 + 4, 4);
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

    // Read ID remap tables
    std::unordered_map<uint32_t, std::unordered_map<uint32_t,uint32_t>> id_remaps; // file_id → (old→new)
    while (pf) {
        uint32_t marker; pf.read(reinterpret_cast<char*>(&marker), 4);
        if (marker != 0xFFFFFFFC) {
            pf.seekg(-4, std::ios::cur);
            break;
        }
        uint32_t fid, count;
        pf.read(reinterpret_cast<char*>(&fid), 4);
        pf.read(reinterpret_cast<char*>(&count), 4);
        uint64_t comp_size; pf.read(reinterpret_cast<char*>(&comp_size), 8);
        std::vector<char> compressed(comp_size);
        pf.read(compressed.data(), comp_size);
        auto blob = zstd_decompress_blob(compressed, count * 8);
        auto& rm = id_remaps[fid];
        for (uint32_t i = 0; i < count; i++) {
            uint32_t old_id, new_id;
            memcpy(&old_id, blob.data() + i * 8, 4);
            memcpy(&new_id, blob.data() + i * 8 + 4, 4);
            rm[old_id] = new_id;
        }
        std::cerr << "Loaded " << count << " ID remaps for file " << fid << std::endl;
    }

    // Helper: rebuild entry + cell files using ID remap
    auto rebuild_entries = [&](const std::string& cells_file, const std::string& entries_file,
                                size_t cell_stride, size_t offset_pos,
                                const std::unordered_map<uint32_t,uint32_t>& id_rm, bool has_flags)
        -> std::pair<std::vector<char>, std::vector<char>> // (entries_data, cells_data)
    {
        auto old_cells = read_file(cur_dir + "/" + cells_file);
        auto old_entries = read_file(cur_dir + "/" + entries_file);
        size_t n_cells = old_cells.size() / cell_stride;

        // Parse old entries, remap IDs
        struct CellEntry { uint64_t cell_id; std::vector<uint32_t> ids; };
        std::vector<CellEntry> parsed;
        for (size_t i = 0; i < n_cells; i++) {
            uint64_t cid; uint32_t off;
            memcpy(&cid, old_cells.data() + i * cell_stride, 8);
            memcpy(&off, old_cells.data() + i * cell_stride + offset_pos, 4);
            CellEntry ce; ce.cell_id = cid;
            if (off != 0xFFFFFFFF && off + 2 <= old_entries.size()) {
                uint16_t count; memcpy(&count, old_entries.data() + off, 2);
                if (off + 2 + count * 4 <= old_entries.size()) {
                    for (uint16_t j = 0; j < count; j++) {
                        uint32_t id; memcpy(&id, old_entries.data() + off + 2 + j * 4, 4);
                        uint32_t flags = has_flags ? (id & 0x80000000u) : 0;
                        uint32_t masked = id & 0x7FFFFFFFu;
                        auto it = id_rm.find(masked);
                        if (it != id_rm.end())
                            ce.ids.push_back(it->second | flags);
                        else
                            ce.ids.push_back(id); // keep unmapped IDs as-is
                    }
                }
            }
            std::sort(ce.ids.begin(), ce.ids.end());
            parsed.push_back(std::move(ce));
        }

        // Rebuild entries + cells
        std::vector<char> new_entries_buf, new_cells_buf;
        uint32_t no_data = 0xFFFFFFFF;
        for (auto& ce : parsed) {
            new_cells_buf.insert(new_cells_buf.end(), (char*)&ce.cell_id, (char*)&ce.cell_id + 8);
            if (ce.ids.empty()) {
                new_cells_buf.insert(new_cells_buf.end(), (char*)&no_data, (char*)&no_data + 4);
            } else {
                uint32_t off = static_cast<uint32_t>(new_entries_buf.size());
                new_cells_buf.insert(new_cells_buf.end(), (char*)&off, (char*)&off + 4);
                uint16_t count = static_cast<uint16_t>(ce.ids.size());
                new_entries_buf.insert(new_entries_buf.end(), (char*)&count, (char*)&count + 2);
                new_entries_buf.insert(new_entries_buf.end(), (char*)ce.ids.data(),
                                      (char*)ce.ids.data() + ce.ids.size() * 4);
            }
        }
        return {new_entries_buf, new_cells_buf};
    };

    // Pre-rebuild entry/cell files using ID remaps (for use as zstd reference)
    std::unordered_map<std::string, std::string> rebuilt_refs; // filename → temp path
    if (!id_remaps.empty()) {
        // Rebuild geo entries (street, addr, interp)
        auto way_rm = id_remaps.count((uint32_t)PatchFileId::STREET_WAYS) ?
                       id_remaps[(uint32_t)PatchFileId::STREET_WAYS] : std::unordered_map<uint32_t,uint32_t>{};
        auto addr_rm = id_remaps.count((uint32_t)PatchFileId::ADDR_POINTS) ?
                        id_remaps[(uint32_t)PatchFileId::ADDR_POINTS] : std::unordered_map<uint32_t,uint32_t>{};
        auto interp_rm = id_remaps.count((uint32_t)PatchFileId::INTERP_WAYS) ?
                          id_remaps[(uint32_t)PatchFileId::INTERP_WAYS] : std::unordered_map<uint32_t,uint32_t>{};
        auto admin_rm = id_remaps.count((uint32_t)PatchFileId::ADMIN_POLYGONS) ?
                         id_remaps[(uint32_t)PatchFileId::ADMIN_POLYGONS] : std::unordered_map<uint32_t,uint32_t>{};

        // Rebuild geo: street_entries (off 8), addr_entries (off 12), interp_entries (off 16)
        auto old_geo = read_file(cur_dir + "/geo_cells.bin");
        auto old_se = read_file(cur_dir + "/street_entries.bin");
        auto old_ae = read_file(cur_dir + "/addr_entries.bin");
        auto old_ie = read_file(cur_dir + "/interp_entries.bin");

        // Parse + remap each entry type
        auto parse_and_remap = [&](const std::vector<char>& geo, size_t off_pos,
                                     const std::vector<char>& entries,
                                     const std::unordered_map<uint32_t,uint32_t>& rm) {
            size_t n = geo.size() / 20;
            std::vector<std::pair<uint64_t, std::vector<uint32_t>>> result;
            for (size_t i = 0; i < n; i++) {
                uint64_t cid; uint32_t off;
                memcpy(&cid, geo.data() + i * 20, 8);
                memcpy(&off, geo.data() + i * 20 + off_pos, 4);
                std::vector<uint32_t> ids;
                if (off != 0xFFFFFFFF && off + 2 <= entries.size()) {
                    uint16_t count; memcpy(&count, entries.data() + off, 2);
                    if (off + 2 + count * 4 <= entries.size()) {
                        for (uint16_t j = 0; j < count; j++) {
                            uint32_t id; memcpy(&id, entries.data() + off + 2 + j * 4, 4);
                            auto it = rm.find(id);
                            ids.push_back(it != rm.end() ? it->second : id);
                        }
                    }
                }
                std::sort(ids.begin(), ids.end());
                result.push_back({cid, std::move(ids)});
            }
            return result;
        };

        auto sc = parse_and_remap(old_geo, 8, old_se, way_rm);
        auto ac = parse_and_remap(old_geo, 12, old_ae, addr_rm);
        auto ic = parse_and_remap(old_geo, 16, old_ie, interp_rm);

        // Write rebuilt entries + geo_cells
        auto write_rebuilt = [&](const std::string& name,
                                  const std::vector<std::pair<uint64_t, std::vector<uint32_t>>>& entries) -> std::string {
            std::string path = tmpdir + "/" + name;
            std::vector<char> buf;
            std::unordered_map<uint64_t, uint32_t> offsets;
            for (auto& [cid, ids] : entries) {
                if (ids.empty()) continue;
                offsets[cid] = static_cast<uint32_t>(buf.size());
                uint16_t count = static_cast<uint16_t>(ids.size());
                buf.insert(buf.end(), (char*)&count, (char*)&count + 2);
                buf.insert(buf.end(), (char*)ids.data(), (char*)ids.data() + ids.size() * 4);
            }
            write_file(path, buf);
            return path;
        };

        // Entry files: don't use rebuilt refs (diff uses old→new directly)
        // Only geo_cells uses rebuilt ref (proven to match byte-identical)
        write_rebuilt("street_entries.bin", sc); // write for geo_cells offset calculation
        write_rebuilt("addr_entries.bin", ac);
        write_rebuilt("interp_entries.bin", ic);

        // Rebuild geo_cells from the remapped entries
        {
            uint32_t no_data = 0xFFFFFFFF;
            std::vector<char> buf;
            // Build offset maps for each entry type
            std::unordered_map<uint64_t, uint32_t> s_off, a_off, i_off;
            auto entries_data = [](const std::string& path) {
                auto d = read_file(path);
                std::unordered_map<uint64_t, uint32_t> offsets;
                return d;
            };
            // Re-parse the rebuilt entries to get offsets
            auto get_offsets = [](const std::vector<std::pair<uint64_t, std::vector<uint32_t>>>& entries) {
                std::unordered_map<uint64_t, uint32_t> offsets;
                uint32_t pos = 0;
                for (auto& [cid, ids] : entries) {
                    if (ids.empty()) continue;
                    offsets[cid] = pos;
                    pos += 2 + ids.size() * 4;
                }
                return offsets;
            };
            auto so = get_offsets(sc), ao = get_offsets(ac), io = get_offsets(ic);

            size_t n = old_geo.size() / 20;
            for (size_t i = 0; i < n; i++) {
                uint64_t cid; memcpy(&cid, old_geo.data() + i * 20, 8);
                buf.insert(buf.end(), (char*)&cid, (char*)&cid + 8);
                auto get = [&](const auto& m) -> uint32_t {
                    auto it = m.find(cid); return it != m.end() ? it->second : no_data;
                };
                uint32_t sv = get(so), av = get(ao), iv = get(io);
                buf.insert(buf.end(), (char*)&sv, (char*)&sv + 4);
                buf.insert(buf.end(), (char*)&av, (char*)&av + 4);
                buf.insert(buf.end(), (char*)&iv, (char*)&iv + 4);
            }
            std::string path = tmpdir + "/geo_cells.bin";
            write_file(path, buf);
            rebuilt_refs["geo_cells.bin"] = path;
        }

        // Rebuild admin entries
        {
            auto [ae_data, ac_data] = rebuild_entries("admin_cells.bin", "admin_entries.bin",
                                                        12, 8, admin_rm, true);
            std::string ae_path = tmpdir + "/admin_entries.bin";
            std::string ac_path = tmpdir + "/admin_cells.bin";
            write_file(ae_path, ae_data);
            write_file(ac_path, ac_data);
            // Don't add admin entries/cells to rebuilt_refs (diff uses old→new directly)
        }

        std::cerr << "Rebuilt " << rebuilt_refs.size() << " entry/cell files from ID remaps" << std::endl;
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

        if (encoding == (uint32_t)PatchEncoding::ZSTD_DELTA) {
            uint64_t old_size, new_size, delta_size;
            pf.read(reinterpret_cast<char*>(&old_size), 8);
            pf.read(reinterpret_cast<char*>(&new_size), 8);
            pf.read(reinterpret_cast<char*>(&delta_size), 8);

            // Write zstd frame to temp
            std::string zst = tmpdir + "/" + std::string(filename) + ".zst";
            write_temp(zst, delta_size);

            // Check if we have a rebuilt ref (entry/cell files with ID remap)
            std::string ref;
            auto rit = rebuilt_refs.find(std::string(filename));
            if (rit != rebuilt_refs.end()) {
                ref = rit->second;
            } else {
                // Load old file, apply string remap + fixups → write remapped ref
                auto old_data = read_file(cur_dir + "/" + std::string(filename));
                apply_str_remap(old_data, file_id);
                apply_fixups(old_data, file_id);
                ref = tmpdir + "/" + std::string(filename) + ".ref";
                write_file(ref, old_data);
            }

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
