// geocoder-patch v3: Apply custom patch format.
//
// Decompresses zstd transport layer, then:
// 1. Reads string remap, applies to old data files
// 2. Replays merge sequences to reconstruct new data files
// 3. Writes full-replacement entry/cell files
//
// Usage: geocoder-patch <current-dir> <patch-file> -o <output-dir>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <unistd.h>
#include <vector>

#include "patch_format.h"

// Merge ops (must match diff tool)
enum MergeOp : uint8_t { OP_MATCH_RUN = 0, OP_INSERT_RUN = 1, OP_DELETE_RUN = 2 };

int main(int argc, char* argv[]) {
    if (argc < 5 || std::string(argv[3]) != "-o") {
        std::cerr << "Usage: geocoder-patch <current-dir> <patch-file> -o <output-dir>" << std::endl;
        return 1;
    }
    std::string cur_dir = argv[1], patch_path = argv[2], out_dir = argv[4];
    ensure_dir(out_dir);
    std::string tmpdir = "/tmp/geocoder-patch-" + std::to_string(getpid());
    ensure_dir(tmpdir);

    // Decompress zstd transport layer
    std::string raw_path = tmpdir + "/patch.raw";
    {
        std::string cmd = "zstd -d '" + patch_path + "' -o '" + raw_path + "' -f --quiet 2>/dev/null";
        if (system(cmd.c_str()) != 0) {
            std::cerr << "Failed to decompress patch" << std::endl; return 1;
        }
    }
    auto patch = read_file(raw_path);
    remove(raw_path.c_str());
    std::cerr << "Decompressed patch: " << patch.size() << " bytes" << std::endl;

    size_t pos = 0;
    auto read_u32 = [&]() -> uint32_t { uint32_t v; memcpy(&v, patch.data() + pos, 4); pos += 4; return v; };
    auto read_u64 = [&]() -> uint64_t { uint64_t v; memcpy(&v, patch.data() + pos, 8); pos += 8; return v; };

    // Check header
    if (memcmp(patch.data(), GCPATCH_MAGIC, 8) != 0) {
        std::cerr << "Invalid patch magic" << std::endl; return 1;
    }
    pos = 8;
    uint32_t version = read_u32();
    if (version != 2) { std::cerr << "Expected patch v2, got " << version << std::endl; return 1; }
    uint32_t flags = read_u32();
    (void)flags;

    // Read string remap
    std::unordered_map<uint32_t, uint32_t> str_remap;
    {
        uint32_t marker = read_u32();
        if (marker != 0xFFFFFFFE) { std::cerr << "Expected remap section" << std::endl; return 1; }
        uint32_t count = read_u32();
        for (uint32_t i = 0; i < count; i++) {
            uint32_t old_off = read_u32(), new_off = read_u32();
            if (old_off != new_off) str_remap[old_off] = new_off;
        }
        std::cerr << "Loaded " << str_remap.size() << " string remap entries (pos=" << pos << ")" << std::endl;
    }

    // Detect strides
    auto detect = [&](const std::string& name, std::vector<size_t> cs) -> size_t {
        auto d = read_file(cur_dir + "/" + name);
        for (size_t s : cs) if (d.size() % s == 0 && d.size() > 0) return s;
        return cs[0];
    };
    size_t way_stride = detect("street_ways.bin", {12, 9});
    size_t interp_stride = detect("interp_ways.bin", {24, 20, 18});
    size_t admin_stride = detect("admin_polygons.bin", {24, 20, 19});

    // String remap functions
    auto apply_str_remap = [&](std::vector<char>& data, uint32_t file_id, size_t stride) {
        if (str_remap.empty()) return;
        std::vector<size_t> field_offsets;
        if (file_id == (uint32_t)PatchFileId::ADDR_POINTS) field_offsets = {8, 12};
        else if (file_id == (uint32_t)PatchFileId::STREET_WAYS) field_offsets = {(stride == 12) ? (size_t)8 : (size_t)5};
        else if (file_id == (uint32_t)PatchFileId::INTERP_WAYS) field_offsets = {(stride >= 20) ? (size_t)8 : (size_t)5};
        else if (file_id == (uint32_t)PatchFileId::ADMIN_POLYGONS) field_offsets = {8};
        else return;

        for (size_t i = 0; i + stride <= data.size(); i += stride)
            for (size_t off : field_offsets) {
                uint32_t v; memcpy(&v, data.data() + i + off, 4);
                auto it = str_remap.find(v);
                if (it != str_remap.end()) memcpy(data.data() + i + off, &it->second, 4);
            }
    };

    // ID remaps derived from merge sequences (populated during merge replay)
    std::unordered_map<uint32_t, std::vector<uint32_t>> derived_id_remaps;

    // Buffered correction sections (applied after entry reconstruction)
    struct PendingCorrection { uint32_t file_id; std::string filename; std::vector<char> delta; };
    std::vector<PendingCorrection> pending_corrections;

    // Read cell changes (may appear before end marker)
    std::vector<uint64_t> geo_added, geo_removed, admin_added, admin_removed;

    // Process sections
    while (pos < patch.size()) {
        uint32_t file_id = read_u32();
        if (file_id == 0xFFFFFFFF) break;

        // Cell change sections
        if (file_id == CELL_CHANGES_GEO_MARKER) {
            uint32_t na = read_u32(), nr = read_u32();
            geo_added.resize(na); geo_removed.resize(nr);
            for (uint32_t i = 0; i < na; i++) { uint64_t c; memcpy(&c, patch.data()+pos, 8); pos+=8; geo_added[i]=c; }
            for (uint32_t i = 0; i < nr; i++) { uint64_t c; memcpy(&c, patch.data()+pos, 8); pos+=8; geo_removed[i]=c; }
            std::cerr << "  Geo cell changes: +" << na << " -" << nr << std::endl;
            continue;
        }
        if (file_id == CELL_CHANGES_ADMIN_MARKER) {
            uint32_t na = read_u32(), nr = read_u32();
            admin_added.resize(na); admin_removed.resize(nr);
            for (uint32_t i = 0; i < na; i++) { uint64_t c; memcpy(&c, patch.data()+pos, 8); pos+=8; admin_added[i]=c; }
            for (uint32_t i = 0; i < nr; i++) { uint64_t c; memcpy(&c, patch.data()+pos, 8); pos+=8; admin_removed[i]=c; }
            std::cerr << "  Admin cell changes: +" << na << " -" << nr << std::endl;
            continue;
        }

        uint32_t stride = read_u32();
        uint64_t old_size = read_u64(), new_size = read_u64();

        if (file_id >= (uint32_t)PatchFileId::COUNT) {
            std::cerr << "  Unknown file " << file_id << std::endl;
            break;
        }
        const char* filename = patch_file_names[file_id];

        if (stride == 0) {
            // Full replacement
            uint32_t n_fix = read_u32(); (void)n_fix;
            uint64_t data_size = read_u64();
            std::vector<char> data(patch.data() + pos, patch.data() + pos + data_size);
            write_file(out_dir + "/" + std::string(filename), data);
            std::cerr << "  " << filename << ": full replacement " << data.size() << " bytes" << std::endl;
            pos += data_size;
            continue;
        }
        if (stride == 0xFE) {
            // Correction section: buffer for processing AFTER entry reconstruction
            uint32_t n_fix = read_u32(); (void)n_fix;
            uint64_t delta_size = read_u64();
            pending_corrections.push_back({file_id, std::string(filename),
                std::vector<char>(patch.data() + pos, patch.data() + pos + delta_size)});
            pos += delta_size;
            std::cerr << "  " << filename << ": correction buffered (" << delta_size << " bytes)" << std::endl;
            continue;
        }

        // Merge sequence: load old file, apply remap + fixups, replay sequence
        auto old_data = read_file(cur_dir + "/" + std::string(filename));
        size_t actual_stride = stride;
        if (file_id == (uint32_t)PatchFileId::STREET_WAYS) actual_stride = way_stride;
        else if (file_id == (uint32_t)PatchFileId::INTERP_WAYS) actual_stride = interp_stride;
        else if (file_id == (uint32_t)PatchFileId::ADMIN_POLYGONS) actual_stride = admin_stride;

        // Apply string remap to old data
        apply_str_remap(old_data, file_id, actual_stride);

        // Read and apply fixups (node_offset/vertex_offset fixes)
        // Read and apply fixups (offset field patches for matched records)
        uint32_t n_fixups = read_u32();
        for (uint32_t i = 0; i < n_fixups; i++) {
            uint32_t idx = read_u32(), val = read_u32();
            size_t byte_pos = (size_t)idx * actual_stride;
            if (byte_pos + 4 <= old_data.size())
                memcpy(old_data.data() + byte_pos, &val, 4);
        }
        if (n_fixups > 0) std::cerr << "    Applied " << n_fixups << " fixups" << std::endl;

        // Read merge sequence size and replay, tracking old→new ID mapping
        uint64_t seq_size = read_u64();
        std::vector<char> output;
        output.reserve(new_size);
        size_t old_rec = 0, new_rec = 0; // record indices
        size_t old_pos_bytes = 0;
        size_t seq_end = pos + seq_size;

        // ID remap for this file (old_record_index → new_record_index)
        std::vector<uint32_t> id_map;
        bool track_ids = (file_id == (uint32_t)PatchFileId::ADDR_POINTS ||
                          file_id == (uint32_t)PatchFileId::STREET_WAYS ||
                          file_id == (uint32_t)PatchFileId::INTERP_WAYS ||
                          file_id == (uint32_t)PatchFileId::ADMIN_POLYGONS);
        if (track_ids) id_map.assign(old_data.size() / actual_stride, 0xFFFFFFFF);

        while (pos < seq_end) {
            uint8_t op = static_cast<uint8_t>(patch[pos]); pos++;
            uint32_t count; memcpy(&count, patch.data() + pos, 4); pos += 4;

            if (op == OP_MATCH_RUN) {
                size_t bytes = count * actual_stride;
                if (old_pos_bytes + bytes <= old_data.size())
                    output.insert(output.end(), old_data.data() + old_pos_bytes,
                                 old_data.data() + old_pos_bytes + bytes);
                if (track_ids)
                    for (uint32_t k = 0; k < count; k++)
                        if (old_rec + k < id_map.size())
                            id_map[old_rec + k] = static_cast<uint32_t>(new_rec + k);
                old_rec += count; new_rec += count;
                old_pos_bytes += bytes;
            } else if (op == OP_INSERT_RUN) {
                size_t bytes = count * actual_stride;
                output.insert(output.end(), patch.data() + pos, patch.data() + pos + bytes);
                pos += bytes;
                new_rec += count;
            } else if (op == OP_DELETE_RUN) {
                old_rec += count;
                old_pos_bytes += count * actual_stride;
            }
        }

        write_file(out_dir + "/" + std::string(filename), output);

        // Store ID remap for entry reconstruction
        if (track_ids) {
            derived_id_remaps[file_id] = std::move(id_map);
            size_t matched = 0;
            for (auto v : derived_id_remaps[file_id]) if (v != 0xFFFFFFFF) matched++;
            std::cerr << "  " << filename << ": " << output.size() << " bytes (mapped "
                      << matched << "/" << derived_id_remaps[file_id].size() << " IDs)" << std::endl;
        } else {
            std::cerr << "  " << filename << ": " << output.size() << " bytes" << std::endl;
        }
    }

    // Reconstruct entry/cell files from derived ID remaps (if not provided as full replacement)
    {
        bool have_geo = false, have_admin = false;
        // Check if geo_cells was already written (full replacement)
        auto check = [&](const std::string& name) -> bool {
            auto d = read_file(out_dir + "/" + name);
            return !d.empty();
        };
        have_geo = check("geo_cells.bin");
        have_admin = check("admin_cells.bin");

        if (!derived_id_remaps.empty()) { // always reconstruct entries (needed for corrections)
            std::cerr << "Reconstructing entry/cell files from derived ID remaps..." << std::endl;
            auto convert = [](const std::vector<uint32_t>& vec) {
                std::unordered_map<uint32_t,uint32_t> m;
                for (uint32_t i = 0; i < vec.size(); i++)
                    if (vec[i] != 0xFFFFFFFF) m[i] = vec[i];
                return m;
            };
            std::unordered_map<uint32_t,uint32_t> w_rm, a_rm, i_rm;
            if (derived_id_remaps.count((uint32_t)PatchFileId::STREET_WAYS))
                w_rm = convert(derived_id_remaps[(uint32_t)PatchFileId::STREET_WAYS]);
            if (derived_id_remaps.count((uint32_t)PatchFileId::ADDR_POINTS))
                a_rm = convert(derived_id_remaps[(uint32_t)PatchFileId::ADDR_POINTS]);
            if (derived_id_remaps.count((uint32_t)PatchFileId::INTERP_WAYS))
                i_rm = convert(derived_id_remaps[(uint32_t)PatchFileId::INTERP_WAYS]);

            auto old_geo = read_file(cur_dir + "/geo_cells.bin");
            auto old_se = read_file(cur_dir + "/street_entries.bin");
            auto old_ae = read_file(cur_dir + "/addr_entries.bin");
            auto old_ie = read_file(cur_dir + "/interp_entries.bin");

            auto geo = rebuild_geo_from_remap(old_geo, old_se, old_ae, old_ie, w_rm, a_rm, i_rm,
                                               geo_added, geo_removed);
            // Always write geo_cells (needed as correction prefix)
            write_file(out_dir + "/geo_cells.bin", geo.geo_cells_data);
            write_file(out_dir + "/street_entries.bin", geo.street_entries_data);
            write_file(out_dir + "/addr_entries.bin", geo.addr_entries_data);
            write_file(out_dir + "/interp_entries.bin", geo.interp_entries_data);
            std::cerr << "  Rebuilt 3 entry files" << (have_geo ? "" : " + geo_cells") << std::endl;
        }

        if (!derived_id_remaps.empty()) { // always reconstruct admin entries for corrections
            std::unordered_map<uint32_t,uint32_t> ad_rm;
            if (derived_id_remaps.count((uint32_t)PatchFileId::ADMIN_POLYGONS)) {
                auto& vec = derived_id_remaps[(uint32_t)PatchFileId::ADMIN_POLYGONS];
                for (uint32_t i = 0; i < vec.size(); i++)
                    if (vec[i] != 0xFFFFFFFF) ad_rm[i] = vec[i];
            }
            auto old_ac = read_file(cur_dir + "/admin_cells.bin");
            auto old_ae = read_file(cur_dir + "/admin_entries.bin");
            auto admin = rebuild_admin_from_remap(old_ac, old_ae, ad_rm);
            write_file(out_dir + "/admin_cells.bin", admin.admin_cells_data);
            write_file(out_dir + "/admin_entries.bin", admin.admin_entries_data);
            std::cerr << "  Rebuilt admin_entries" << (have_admin ? "" : " + admin_cells") << std::endl;
        }
    }

    // Apply buffered corrections: entry files first, then cell files
    // Entry corrections must be applied before geo_cells rebuild.
    std::vector<PendingCorrection> cell_corrections;
    for (auto& corr : pending_corrections) {
        if (corr.file_id == (uint32_t)PatchFileId::GEO_CELLS ||
            corr.file_id == (uint32_t)PatchFileId::ADMIN_CELLS) {
            cell_corrections.push_back(std::move(corr));
            continue;
        }
        // Apply entry correction
        std::string derived_path = out_dir + "/" + corr.filename;
        std::string zst_path = tmpdir + "/corr_" + corr.filename + ".zst";
        std::string corrected_path = tmpdir + "/corr_" + corr.filename + ".out";
        write_file(zst_path, corr.delta);
        std::string cmd = "zstd --patch-from='" + derived_path + "' -d '" + zst_path +
                          "' -o '" + corrected_path + "' -f --long=31 --quiet 2>/dev/null";
        if (system(cmd.c_str()) != 0)
            std::cerr << "  " << corr.filename << ": entry correction FAILED" << std::endl;
        else {
            auto corrected = read_file(corrected_path);
            write_file(derived_path, corrected);
            std::cerr << "  " << corr.filename << ": corrected → " << corrected.size() << " bytes" << std::endl;
        }
        remove(zst_path.c_str()); remove(corrected_path.c_str());
    }
    pending_corrections.clear(); // entry corrections done

    // Now apply cell corrections (after geo_cells rebuild from corrected entries)
    for (auto& corr : cell_corrections) {
        std::string derived_path = out_dir + "/" + corr.filename;
        std::string zst_path = tmpdir + "/corr_" + corr.filename + ".zst";
        std::string corrected_path = tmpdir + "/corr_" + corr.filename + ".out";
        write_file(zst_path, corr.delta);
        std::string cmd = "zstd --patch-from='" + derived_path + "' -d '" + zst_path +
                          "' -o '" + corrected_path + "' -f --long=31 --quiet 2>/dev/null";
        if (system(cmd.c_str()) != 0) {
            std::cerr << "  " << corr.filename << ": correction FAILED" << std::endl;
        } else {
            auto corrected = read_file(corrected_path);
            write_file(derived_path, corrected);
            std::cerr << "  " << corr.filename << ": corrected → " << corrected.size() << " bytes" << std::endl;
        }
        remove(zst_path.c_str()); remove(corrected_path.c_str());
    }

    // Post-correction geo_cells rebuild from corrected entries
    // This produces a nearly-correct geo_cells (>99.99% accurate)
    // which is then fixed by the tiny geo_cells correction delta.
    {
        auto corr_se = read_file(out_dir + "/street_entries.bin");
        auto corr_ae = read_file(out_dir + "/addr_entries.bin");
        auto corr_ie = read_file(out_dir + "/interp_entries.bin");
        auto derived_geo = read_file(out_dir + "/geo_cells.bin");

        if (!derived_geo.empty() && !cell_corrections.empty()) {
            size_t n = derived_geo.size() / 20;
            uint32_t no_data = 0xFFFFFFFF;

            // Count entries in each corrected file
            auto count_entries = [](const std::vector<char>& ef) -> size_t {
                size_t count = 0, pos = 0;
                while (pos + 2 <= ef.size()) {
                    uint16_t c; memcpy(&c, ef.data() + pos, 2);
                    pos += 2 + c * 4;
                    count++;
                }
                return count;
            };

            // Build offset arrays by scanning corrected entries
            auto build_offsets = [](const std::vector<char>& ef) -> std::vector<uint32_t> {
                std::vector<uint32_t> offsets;
                uint32_t pos = 0;
                while (pos + 2 <= ef.size()) {
                    offsets.push_back(pos);
                    uint16_t c; memcpy(&c, ef.data() + pos, 2);
                    pos += 2 + c * 4;
                }
                return offsets;
            };

            auto s_offsets = build_offsets(corr_se);
            auto a_offsets = build_offsets(corr_ae);
            auto i_offsets = build_offsets(corr_ie);

            // Walk derived geo_cells, replace offsets with corrected values
            size_t si = 0, ai = 0, ii = 0;
            std::vector<char> new_geo(derived_geo.size());
            for (size_t c = 0; c < n; c++) {
                // Copy cell_id
                memcpy(new_geo.data() + c * 20, derived_geo.data() + c * 20, 8);

                uint32_t old_s, old_a, old_i;
                memcpy(&old_s, derived_geo.data() + c * 20 + 8, 4);
                memcpy(&old_a, derived_geo.data() + c * 20 + 12, 4);
                memcpy(&old_i, derived_geo.data() + c * 20 + 16, 4);

                uint32_t new_s = (old_s != no_data && si < s_offsets.size()) ? s_offsets[si++] : no_data;
                uint32_t new_a = (old_a != no_data && ai < a_offsets.size()) ? a_offsets[ai++] : no_data;
                uint32_t new_i = (old_i != no_data && ii < i_offsets.size()) ? i_offsets[ii++] : no_data;

                memcpy(new_geo.data() + c * 20 + 8, &new_s, 4);
                memcpy(new_geo.data() + c * 20 + 12, &new_a, 4);
                memcpy(new_geo.data() + c * 20 + 16, &new_i, 4);
            }
            write_file(out_dir + "/geo_cells.bin", new_geo);
            std::cerr << "  Rebuilt geo_cells from corrected entries" << std::endl;
        }
    }
    if (false) { // legacy rebuild code
        auto se = read_file(out_dir + "/street_entries.bin");
        auto ae = read_file(out_dir + "/addr_entries.bin");
        auto ie = read_file(out_dir + "/interp_entries.bin");
        auto existing_geo = read_file(out_dir + "/geo_cells.bin");

        if (!existing_geo.empty()) {
            // Parse the existing (derived) geo_cells for cell IDs, rebuild offsets from corrected entries
            size_t n = existing_geo.size() / 20;

            // Parse each entry file to build cell→offset maps
            // Walk entries sequentially, matching against geo_cells cell order
            auto build_offsets_from_geo = [](const std::vector<char>& geo, size_t off_pos,
                                              const std::vector<char>& old_entries,
                                              const std::vector<char>& new_entries) -> std::vector<char> {
                // The corrected entries file has the right content.
                // We need to map cell_ids to byte offsets in the corrected entries.
                // The cell order is the same as in the derived geo_cells.
                // We just need to recompute offsets.
                return new_entries; // entries are already correct, just need geo_cells offsets
            };

            // Actually: re-walk the derived geo_cells, find each cell's entries in the corrected files
            // Since the cell IDs are the same and entries are in the same order, we just recompute offsets.
            // The corrected entry files have entries in cell_id order (from the rebuild).
            // We can walk them to compute offsets.

            // For each entry type, build cell_id → offset by scanning the entry file
            auto scan_entries = [](const std::vector<char>& entries) -> std::vector<uint32_t> {
                // Returns a vector of entry start positions
                std::vector<uint32_t> positions;
                uint32_t pos = 0;
                while (pos + 2 <= entries.size()) {
                    positions.push_back(pos);
                    uint16_t count; memcpy(&count, entries.data() + pos, 2);
                    pos += 2 + count * 4;
                }
                return positions;
            };

            auto s_pos = scan_entries(se);
            auto a_pos = scan_entries(ae);
            auto i_pos = scan_entries(ie);

            // The derived geo_cells has cell_ids + offsets into derived entries.
            // The corrected entries have the same cell order but potentially different byte offsets.
            // Rebuild geo_cells with corrected offsets.
            uint32_t no_data = 0xFFFFFFFF;
            size_t si = 0, ai = 0, ii = 0;
            std::vector<char> new_geo;
            for (size_t c = 0; c < n; c++) {
                uint64_t cid; memcpy(&cid, existing_geo.data() + c * 20, 8);
                new_geo.insert(new_geo.end(), (char*)&cid, (char*)&cid + 8);

                // For each entry type, check if this cell has data
                uint32_t old_s, old_a, old_i;
                memcpy(&old_s, existing_geo.data() + c * 20 + 8, 4);
                memcpy(&old_a, existing_geo.data() + c * 20 + 12, 4);
                memcpy(&old_i, existing_geo.data() + c * 20 + 16, 4);

                uint32_t new_s = (old_s != no_data && si < s_pos.size()) ? s_pos[si++] : no_data;
                uint32_t new_a = (old_a != no_data && ai < a_pos.size()) ? a_pos[ai++] : no_data;
                uint32_t new_i = (old_i != no_data && ii < i_pos.size()) ? i_pos[ii++] : no_data;

                new_geo.insert(new_geo.end(), (char*)&new_s, (char*)&new_s + 4);
                new_geo.insert(new_geo.end(), (char*)&new_a, (char*)&new_a + 4);
                new_geo.insert(new_geo.end(), (char*)&new_i, (char*)&new_i + 4);
            }
            write_file(out_dir + "/geo_cells.bin", new_geo);
            std::cerr << "  Rebuilt geo_cells.bin from corrected entries (" << new_geo.size() << " bytes)" << std::endl;
        }
    }

    // Similarly for admin_cells from admin_entries
    {
        auto ae = read_file(out_dir + "/admin_entries.bin");
        auto existing_ac = read_file(out_dir + "/admin_cells.bin");
        if (!existing_ac.empty()) {
            size_t n = existing_ac.size() / 12;
            uint32_t no_data = 0xFFFFFFFF;
            std::vector<uint32_t> a_pos;
            uint32_t p = 0;
            while (p + 2 <= ae.size()) {
                a_pos.push_back(p);
                uint16_t count; memcpy(&count, ae.data() + p, 2);
                p += 2 + count * 4;
            }
            size_t ai = 0;
            std::vector<char> new_ac;
            for (size_t c = 0; c < n; c++) {
                uint64_t cid; memcpy(&cid, existing_ac.data() + c * 12, 8);
                uint32_t old_off; memcpy(&old_off, existing_ac.data() + c * 12 + 8, 4);
                new_ac.insert(new_ac.end(), (char*)&cid, (char*)&cid + 8);
                uint32_t new_off = (old_off != no_data && ai < a_pos.size()) ? a_pos[ai++] : no_data;
                new_ac.insert(new_ac.end(), (char*)&new_off, (char*)&new_off + 4);
            }
            write_file(out_dir + "/admin_cells.bin", new_ac);
            std::cerr << "  Rebuilt admin_cells.bin from corrected entries (" << new_ac.size() << " bytes)" << std::endl;
        }
    }

    std::string rm_cmd = "rm -rf '" + tmpdir + "'";
    system(rm_cmd.c_str());
    std::cerr << "Patch applied. Output in " << out_dir << std::endl;
    return 0;
}
