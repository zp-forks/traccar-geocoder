// geocoder-patch v4: Low-memory streaming patch application.
//
// Key design: mmap old files (zero copy), stream output via fwrite (never accumulate),
// process entry pipeline cell-by-cell. Target: <1 GiB peak RSS for planet.
//
// Usage: geocoder-patch <current-dir> <patch-file> -o <output-dir>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <malloc.h>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unordered_map>
#include <unordered_set>
#include <unistd.h>
#include <vector>

#include "patch_format.h"

enum MergeOp : uint8_t { OP_MATCH_RUN = 0, OP_INSERT_RUN = 1, OP_DELETE_RUN = 2 };

// --- Helpers ---
static double now_ms() {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}
static size_t get_rss_mb() {
    FILE* f = fopen("/proc/self/statm", "r");
    if (!f) return 0;
    size_t dummy, rss; fscanf(f, "%zu %zu", &dummy, &rss); fclose(f);
    return rss * 4096 / (1024*1024);
}
static void log_phase(const char* label, double start) {
    std::cerr << "  [" << std::fixed << std::setprecision(1) << (now_ms() - start) / 1000.0
              << "s, " << get_rss_mb() << " MiB] " << label << std::endl;
}

// mmap a file read-only. Returns {pointer, size}. Caller must munmap.
struct MappedFile { const char* data; size_t size; };
static MappedFile mmap_file(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) return {nullptr, 0};
    struct stat st; fstat(fd, &st);
    size_t sz = st.st_size;
    if (sz == 0) { close(fd); return {nullptr, 0}; }
    void* p = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (p == MAP_FAILED) return {nullptr, 0};
    return {static_cast<const char*>(p), sz};
}
static void unmap_file(MappedFile& f) {
    if (f.data) { munmap(const_cast<char*>(f.data), f.size); f.data = nullptr; f.size = 0; }
}

// Detect stride from file size
static size_t detect_stride(const std::string& path, std::initializer_list<size_t> candidates) {
    struct stat st; if (stat(path.c_str(), &st) != 0) return *candidates.begin();
    for (size_t s : candidates) if (st.st_size % s == 0 && st.st_size > 0) return s;
    return *candidates.begin();
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
    double t_start = now_ms();

    // --- Phase 1: Decompress + mmap patch ---
    std::string raw_path = tmpdir + "/patch.raw";
    {
        std::string cmd = "zstd -d '" + patch_path + "' -o '" + raw_path + "' -f --quiet 2>/dev/null";
        if (system(cmd.c_str()) != 0) { std::cerr << "Failed to decompress" << std::endl; return 1; }
    }
    MappedFile patch_map = mmap_file(raw_path);
    if (!patch_map.data) { std::cerr << "Failed to mmap patch" << std::endl; return 1; }
    remove(raw_path.c_str());
    const char* P = patch_map.data;
    size_t patch_size = patch_map.size;
    std::cerr << "Patch: " << patch_size << " bytes" << std::endl;
    log_phase("Decompress", t_start);

    size_t pos = 0;
    auto ru32 = [&]() -> uint32_t { uint32_t v; memcpy(&v, P+pos, 4); pos += 4; return v; };
    auto ru64 = [&]() -> uint64_t { uint64_t v; memcpy(&v, P+pos, 8); pos += 8; return v; };

    // Header
    if (memcmp(P, GCPATCH_MAGIC, 8) != 0) { std::cerr << "Bad magic" << std::endl; return 1; }
    pos = 8;
    uint32_t ver = ru32(); if (ver != 2) { std::cerr << "Bad version" << std::endl; return 1; }
    ru32(); // flags

    // --- Phase 2: String rebuild ---
    std::unordered_map<uint32_t, uint32_t> str_remap;
    {
        uint32_t marker = ru32();
        if (marker == 0xFFFFFFF7) {
            uint32_t n_added = ru32(), n_deleted = ru32();
            std::vector<std::string> added;
            for (uint32_t i = 0; i < n_added; i++) { const char* s = P+pos; added.push_back(s); pos += strlen(s)+1; }
            std::vector<uint32_t> del_idx(n_deleted);
            for (uint32_t i = 0; i < n_deleted; i++) del_idx[i] = ru32();
            std::unordered_set<uint32_t> del_set(del_idx.begin(), del_idx.end());

            MappedFile old_pool = mmap_file(cur_dir + "/strings.bin");
            std::vector<std::pair<uint32_t, std::string>> old_strs;
            size_t sp = 0;
            while (sp < old_pool.size) {
                old_strs.push_back({(uint32_t)sp, std::string(old_pool.data + sp)});
                sp += strlen(old_pool.data + sp) + 1;
            }
            std::vector<std::string> merged;
            for (size_t i = 0; i < old_strs.size(); i++)
                if (!del_set.count(i)) merged.push_back(old_strs[i].second);
            for (auto& s : added) merged.push_back(s);
            std::sort(merged.begin(), merged.end());

            // Write new pool and build remap
            FILE* fp = fopen((out_dir + "/strings.bin").c_str(), "wb");
            std::unordered_map<std::string, uint32_t> new_offs;
            uint32_t wpos = 0;
            for (auto& s : merged) {
                new_offs[s] = wpos;
                fwrite(s.c_str(), 1, s.size() + 1, fp);
                wpos += s.size() + 1;
            }
            fclose(fp);
            for (auto& [old_off, s] : old_strs) {
                auto it = new_offs.find(s);
                if (it != new_offs.end() && it->second != old_off) str_remap[old_off] = it->second;
            }
            unmap_file(old_pool);
            std::cerr << "  Strings: +" << n_added << " -" << n_deleted << " → " << merged.size()
                      << ", " << str_remap.size() << " remapped" << std::endl;
            marker = ru32();
        }
        if (marker == 0xFFFFFFFE) { uint32_t c = ru32(); for (uint32_t i = 0; i < c; i++) { uint32_t a = ru32(), b = ru32(); str_remap[a] = b; } }
        else pos -= 4;
    }
    log_phase("Strings", t_start);

    // Detect strides
    size_t way_stride = detect_stride(cur_dir + "/street_ways.bin", {12, 9});
    size_t interp_stride = detect_stride(cur_dir + "/interp_ways.bin", {24, 20, 18});
    size_t admin_stride = detect_stride(cur_dir + "/admin_polygons.bin", {24, 20, 19});

    // String remap applier (works on mutable copy)
    auto apply_str_remap = [&](std::vector<char>& data, uint32_t fid, size_t stride) {
        if (str_remap.empty()) return;
        std::vector<size_t> offs;
        if (fid == (uint32_t)PatchFileId::ADDR_POINTS) offs = {8, 12};
        else if (fid == (uint32_t)PatchFileId::STREET_WAYS) offs = {(stride == 12) ? 8ul : 5ul};
        else if (fid == (uint32_t)PatchFileId::INTERP_WAYS) offs = {(stride >= 20) ? 8ul : 5ul};
        else if (fid == (uint32_t)PatchFileId::ADMIN_POLYGONS) offs = {8};
        else return;
        for (size_t i = 0; i + stride <= data.size(); i += stride)
            for (size_t o : offs) { uint32_t v; memcpy(&v, data.data()+i+o, 4); auto it = str_remap.find(v); if (it != str_remap.end()) memcpy(data.data()+i+o, &it->second, 4); }
    };

    // --- Phase 3: Merge replays (streaming output) ---
    // ID remaps: file_id → vector<uint32_t> where remap[old_idx] = new_idx
    std::unordered_map<uint32_t, std::vector<uint32_t>> id_remaps;
    std::vector<uint64_t> geo_added, geo_removed, admin_added, admin_removed;
    std::unordered_map<uint64_t, uint8_t> flag_corrections;
    struct CellCorr { uint64_t cell_id; std::vector<uint32_t> ids; };
    std::unordered_map<uint32_t, std::vector<CellCorr>> entry_corrections;

    while (pos < patch_size) {
        uint32_t file_id = ru32();
        if (file_id == 0xFFFFFFFF) break;

        // --- Metadata sections ---
        if (file_id == 0xFFFFFFF7) {
            // String diff in main loop — process it (may be the only occurrence)
            uint32_t n_added = ru32(), n_deleted = ru32();
            std::vector<std::string> added;
            for (uint32_t i = 0; i < n_added; i++) { const char* s = P+pos; added.push_back(s); pos += strlen(s)+1; }
            std::vector<uint32_t> del_idx(n_deleted);
            for (uint32_t i = 0; i < n_deleted; i++) del_idx[i] = ru32();
            std::unordered_set<uint32_t> del_set(del_idx.begin(), del_idx.end());
            MappedFile old_pool = mmap_file(cur_dir + "/strings.bin");
            std::vector<std::pair<uint32_t, std::string>> old_strs;
            size_t sp = 0;
            while (sp < old_pool.size) { old_strs.push_back({(uint32_t)sp, std::string(old_pool.data+sp)}); sp += strlen(old_pool.data+sp)+1; }
            std::vector<std::string> merged;
            for (size_t i = 0; i < old_strs.size(); i++) if (!del_set.count(i)) merged.push_back(old_strs[i].second);
            for (auto& s : added) merged.push_back(s);
            std::sort(merged.begin(), merged.end());
            FILE* fp = fopen((out_dir + "/strings.bin").c_str(), "wb");
            std::unordered_map<std::string, uint32_t> new_offs;
            uint32_t wpos = 0;
            for (auto& s : merged) { new_offs[s] = wpos; fwrite(s.c_str(), 1, s.size()+1, fp); wpos += s.size()+1; }
            fclose(fp);
            for (auto& [old_off, s] : old_strs) { auto it = new_offs.find(s); if (it != new_offs.end() && it->second != old_off) str_remap[old_off] = it->second; }
            unmap_file(old_pool);
            std::cerr << "  Strings (loop): +" << n_added << " -" << n_deleted << " → " << merged.size() << std::endl;
            continue;
        }
        if (file_id == CELL_CHANGES_GEO_MARKER) {
            uint32_t na = ru32(), nr = ru32();
            geo_added.resize(na); geo_removed.resize(nr);
            for (uint32_t i = 0; i < na; i++) { memcpy(&geo_added[i], P+pos, 8); pos += 8; }
            for (uint32_t i = 0; i < nr; i++) { memcpy(&geo_removed[i], P+pos, 8); pos += 8; }
            std::cerr << "  Geo cells: +" << na << " -" << nr << std::endl;
            continue;
        }
        if (file_id == CELL_CHANGES_ADMIN_MARKER) {
            uint32_t na = ru32(), nr = ru32();
            admin_added.resize(na); admin_removed.resize(nr);
            for (uint32_t i = 0; i < na; i++) { memcpy(&admin_added[i], P+pos, 8); pos += 8; }
            for (uint32_t i = 0; i < nr; i++) { memcpy(&admin_removed[i], P+pos, 8); pos += 8; }
            std::cerr << "  Admin cells: +" << na << " -" << nr << std::endl;
            continue;
        }
        if (file_id == CELL_FLAGS_MARKER) {
            uint32_t c = ru32();
            for (uint32_t i = 0; i < c; i++) { uint64_t cid; memcpy(&cid, P+pos, 8); pos += 8; flag_corrections[cid] = P[pos++]; }
            std::cerr << "  Flag corrections: " << c << std::endl;
            continue;
        }
        if (file_id == SECONDARY_REMAP_MARKER) {
            // Apply secondary remaps directly to the temp remap files on disk
            uint32_t nf = ru32();
            for (uint32_t f = 0; f < nf; f++) {
                uint32_t fid = ru32(), np = ru32();
                std::string remap_path = tmpdir + "/remap_" + std::to_string(fid) + ".bin";
                // Open remap file for read-write
                int rfd = open(remap_path.c_str(), O_RDWR);
                if (rfd >= 0) {
                    struct stat st; fstat(rfd, &st);
                    size_t remap_size = st.st_size;
                    uint32_t* remap_data = static_cast<uint32_t*>(
                        mmap(nullptr, remap_size, PROT_READ | PROT_WRITE, MAP_SHARED, rfd, 0));
                    size_t remap_count = remap_size / 4;
                    for (uint32_t i = 0; i < np; i++) {
                        uint32_t o = ru32(), n = ru32();
                        if (o < remap_count) remap_data[o] = n;
                    }
                    munmap(remap_data, remap_size);
                    close(rfd);
                } else {
                    // Skip data if file doesn't exist
                    for (uint32_t i = 0; i < np; i++) { ru32(); ru32(); }
                }
                std::cerr << "  Secondary remap " << fid << ": " << np << " pairs" << std::endl;
            }
            continue;
        }
        if (file_id == ENTRY_CORRECTION_MARKER) {
            uint32_t fid = ru32(), c = ru32();
            auto& list = entry_corrections[fid];
            for (uint32_t i = 0; i < c; i++) {
                uint64_t cid; memcpy(&cid, P+pos, 8); pos += 8;
                uint16_t ec; memcpy(&ec, P+pos, 2); pos += 2;
                std::vector<uint32_t> ids(ec);
                if (ec > 0) { memcpy(ids.data(), P+pos, ec*4); pos += ec*4; }
                list.push_back({cid, std::move(ids)});
            }
            std::cerr << "  Entry corrections " << fid << ": " << c << std::endl;
            continue;
        }

        // --- Merge sequence replay (streaming) ---
        uint32_t stride = ru32();
        uint64_t old_size = ru64(), new_size = ru64();
        if (file_id >= (uint32_t)PatchFileId::COUNT) { std::cerr << "Unknown file " << file_id << std::endl; break; }
        const char* fname = patch_file_names[file_id];

        if (stride == 0) {
            // Full replacement — write directly from patch mmap
            uint32_t nf = ru32(); (void)nf; uint64_t ds = ru64();
            FILE* fp = fopen((out_dir + "/" + fname).c_str(), "wb");
            fwrite(P+pos, 1, ds, fp); fclose(fp);
            pos += ds;
            std::cerr << "  " << fname << ": full replace " << ds << " bytes" << std::endl;
            continue;
        }
        if (stride == 0xFE) { uint32_t nf = ru32(); (void)nf; uint64_t ds = ru64(); pos += ds; continue; }

        size_t actual_stride = stride;
        if (file_id == (uint32_t)PatchFileId::STREET_WAYS) actual_stride = way_stride;
        else if (file_id == (uint32_t)PatchFileId::INTERP_WAYS) actual_stride = interp_stride;
        else if (file_id == (uint32_t)PatchFileId::ADMIN_POLYGONS) actual_stride = admin_stride;

        // Determine if this file needs in-memory modifications
        bool needs_remap = (file_id == (uint32_t)PatchFileId::ADDR_POINTS ||
                            file_id == (uint32_t)PatchFileId::STREET_WAYS ||
                            file_id == (uint32_t)PatchFileId::INTERP_WAYS ||
                            file_id == (uint32_t)PatchFileId::ADMIN_POLYGONS);
        bool needs_padding = (file_id == (uint32_t)PatchFileId::ADMIN_POLYGONS && actual_stride == 24) ||
                             (file_id == (uint32_t)PatchFileId::INTERP_WAYS && actual_stride == 24);

        // Read fixup data position (applied inline during merge replay)
        uint32_t n_fixups = ru32();
        size_t fixup_data_pos = pos; // position of delta-encoded fixup data in patch
        if (n_fixups > 0) {
            uint32_t delta_size = ru32();
            fixup_data_pos = pos;
            pos += delta_size; // skip past fixup data
        }
        // Build sorted fixup array (decode delta-encoded, ~2 bytes/entry instead of ~40 bytes/entry in hash map)
        std::vector<std::pair<uint32_t,uint32_t>> fixups_sorted;
        if (n_fixups > 0) {
            fixups_sorted.reserve(n_fixups);
            uint32_t pi = 0, pv = 0; size_t fp = fixup_data_pos;
            for (uint32_t i = 0; i < n_fixups; i++) {
                uint32_t idx = pi + read_varint(P, fp);
                uint32_t val = pv + read_varint(P, fp);
                fixups_sorted.push_back({idx, val});
                pi = idx; pv = val;
            }
        }

        // Get string remap field offsets for this file type
        std::vector<size_t> remap_offs;
        if (!str_remap.empty() && needs_remap) {
            if (file_id == (uint32_t)PatchFileId::ADDR_POINTS) remap_offs = {8, 12};
            else if (file_id == (uint32_t)PatchFileId::STREET_WAYS) remap_offs = {(actual_stride == 12) ? 8ul : 5ul};
            else if (file_id == (uint32_t)PatchFileId::INTERP_WAYS) remap_offs = {(actual_stride >= 20) ? 8ul : 5ul};
            else if (file_id == (uint32_t)PatchFileId::ADMIN_POLYGONS) remap_offs = {8};
        }

        // mmap old file read-only (zero allocation)
        MappedFile old_mmap = mmap_file(cur_dir + "/" + std::string(fname));
        madvise(const_cast<char*>(old_mmap.data), old_mmap.size, MADV_SEQUENTIAL);
        size_t n_old_records = old_mmap.size / actual_stride;

        // Replay merge sequence — stream output, apply remap/fixups per-record inline
        uint64_t seq_size = ru64();
        size_t seq_end = pos + seq_size;
        FILE* outf = fopen((out_dir + "/" + fname).c_str(), "wb");

        bool track = needs_remap; // track ID remap only for data files
        std::vector<uint32_t> id_map;
        if (track) id_map.assign(n_old_records, 0xFFFFFFFF);

        // Per-record buffer for applying remap/fixups inline (avoid copying entire file)
        std::vector<char> rec_buf(actual_stride);

        size_t old_rec = 0, new_rec = 0, old_bytes = 0, written = 0;
        size_t fixup_cursor = 0; // cursor into fixups_sorted for sequential access
        while (pos < seq_end) {
            uint8_t op = P[pos++];
            uint32_t count; memcpy(&count, P+pos, 4); pos += 4;
            if (op == OP_MATCH_RUN) {
                for (uint32_t k = 0; k < count; k++) {
                    size_t rec_off = old_bytes + k * actual_stride;
                    if (rec_off + actual_stride > old_mmap.size) break;

                    bool modified = false;
                    // Check if this record needs any modification
                    if (!remap_offs.empty() || needs_padding) modified = true;
                    // Check fixup using sequential cursor (fixups are sorted by record index)
                    uint32_t fixup_val = 0;
                    bool has_fixup = false;
                    while (fixup_cursor < fixups_sorted.size() && fixups_sorted[fixup_cursor].first < old_rec + k)
                        fixup_cursor++;
                    if (fixup_cursor < fixups_sorted.size() && fixups_sorted[fixup_cursor].first == old_rec + k) {
                        has_fixup = true; fixup_val = fixups_sorted[fixup_cursor].second;
                        modified = true; fixup_cursor++;
                    }

                    if (modified) {
                        memcpy(rec_buf.data(), old_mmap.data + rec_off, actual_stride);
                        // Apply padding zeroing
                        if (file_id == (uint32_t)PatchFileId::ADMIN_POLYGONS && actual_stride == 24)
                            memset(rec_buf.data() + 13, 0, 3);
                        if (file_id == (uint32_t)PatchFileId::INTERP_WAYS && actual_stride == 24) {
                            memset(rec_buf.data() + 5, 0, 3);
                            memset(rec_buf.data() + 21, 0, 3);
                        }
                        // Apply string remap
                        for (size_t off : remap_offs) {
                            uint32_t v; memcpy(&v, rec_buf.data() + off, 4);
                            auto it = str_remap.find(v);
                            if (it != str_remap.end()) memcpy(rec_buf.data() + off, &it->second, 4);
                        }
                        // Apply fixup (node_offset/vertex_offset at byte 0)
                        if (has_fixup)
                            memcpy(rec_buf.data(), &fixup_val, 4);
                        fwrite(rec_buf.data(), 1, actual_stride, outf);
                    } else {
                        // No modification needed — write directly from mmap
                        fwrite(old_mmap.data + rec_off, 1, actual_stride, outf);
                    }
                    if (track && old_rec + k < id_map.size()) id_map[old_rec + k] = new_rec + k;
                }
                written += count * actual_stride;
                old_rec += count; new_rec += count; old_bytes += count * actual_stride;
            } else if (op == OP_INSERT_RUN) {
                size_t bytes = count * actual_stride;
                fwrite(P+pos, 1, bytes, outf);
                written += bytes; pos += bytes; new_rec += count;
            } else if (op == OP_DELETE_RUN) {
                old_rec += count; old_bytes += count * actual_stride;
            }
        }
        fclose(outf);
        unmap_file(old_mmap);
        fixups_sorted.clear(); fixups_sorted.shrink_to_fit();

        if (track) {
            // Write id_map to temp file to avoid keeping all remaps in memory simultaneously
            std::string remap_path = tmpdir + "/remap_" + std::to_string(file_id) + ".bin";
            FILE* rf = fopen(remap_path.c_str(), "wb");
            fwrite(id_map.data(), 4, id_map.size(), rf);
            fclose(rf);
            id_remaps[file_id] = {}; // store empty placeholder (will mmap from disk later)
            size_t matched = 0;
            for (auto v : id_map) if (v != 0xFFFFFFFF) matched++;
            std::cerr << "  " << fname << ": " << written << " bytes (mapped " << matched << "/" << id_map.size()
                      << ", remap saved to disk)" << std::endl;
        } else {
            std::cerr << "  " << fname << ": " << written << " bytes" << std::endl;
        }
        malloc_trim(0);
    }
    log_phase("Merge replays", t_start);

    // Free string remap (no longer needed)
    { std::unordered_map<uint32_t,uint32_t>().swap(str_remap); }
    malloc_trim(0);

    // --- Phase 4: Streaming entry pipeline ---
    // Walk old geo_cells, remap IDs, apply corrections, write output directly to files.
    {
        double t_entry = now_ms();
        std::cerr << "Entry pipeline (lean streaming)..." << std::endl;

        // mmap remap files from disk (only pages accessed are in RSS)
        auto mmap_remap = [&](PatchFileId fid) -> MappedFile {
            std::string path = tmpdir + "/remap_" + std::to_string((uint32_t)fid) + ".bin";
            auto m = mmap_file(path);
            if (m.data) madvise(const_cast<char*>(m.data), m.size, MADV_SEQUENTIAL);
            return m;
        };
        // Helper: look up remap value from mmap'd file
        struct RemapRef {
            const uint32_t* data;
            size_t count;
            uint32_t operator[](size_t i) const { return i < count ? data[i] : 0xFFFFFFFF; }
            size_t size() const { return count; }
        };
        MappedFile m_w_rm = mmap_remap(PatchFileId::STREET_WAYS);
        MappedFile m_a_rm = mmap_remap(PatchFileId::ADDR_POINTS);
        MappedFile m_i_rm = mmap_remap(PatchFileId::INTERP_WAYS);
        RemapRef w_rm = {(const uint32_t*)m_w_rm.data, m_w_rm.size / 4};
        RemapRef a_rm = {(const uint32_t*)m_a_rm.data, m_a_rm.size / 4};
        RemapRef i_rm = {(const uint32_t*)m_i_rm.data, m_i_rm.size / 4};

        // mmap old files (zero RSS until pages are accessed, then only working set)
        MappedFile m_geo = mmap_file(cur_dir + "/geo_cells.bin");
        MappedFile m_se = mmap_file(cur_dir + "/street_entries.bin");
        MappedFile m_ae = mmap_file(cur_dir + "/addr_entries.bin");
        MappedFile m_ie = mmap_file(cur_dir + "/interp_entries.bin");
        madvise(const_cast<char*>(m_se.data), m_se.size, MADV_SEQUENTIAL);
        madvise(const_cast<char*>(m_ae.data), m_ae.size, MADV_SEQUENTIAL);
        madvise(const_cast<char*>(m_ie.data), m_ie.size, MADV_SEQUENTIAL);
        size_t n_old = m_geo.size / 20;

        // Build correction maps + removed set (small memory)
        std::unordered_set<uint64_t> rm_set(geo_removed.begin(), geo_removed.end());
        std::unordered_map<uint64_t, const std::vector<uint32_t>*> cs, ca, ci_map;
        for (auto& [fid, list] : entry_corrections) {
            auto* m = (fid == (uint32_t)PatchFileId::STREET_ENTRIES) ? &cs :
                      (fid == (uint32_t)PatchFileId::ADDR_ENTRIES) ? &ca :
                      (fid == (uint32_t)PatchFileId::INTERP_ENTRIES) ? &ci_map : nullptr;
            if (m) for (auto& c : list) (*m)[c.cell_id] = &c.ids;
        }
        // Added cells sorted
        std::sort(geo_added.begin(), geo_added.end());
        log_phase("  Setup", t_entry);

        // Open 4 output files
        FILE* f_geo = fopen((out_dir + "/geo_cells.bin").c_str(), "wb");
        FILE* f_se = fopen((out_dir + "/street_entries.bin").c_str(), "wb");
        FILE* f_ae = fopen((out_dir + "/addr_entries.bin").c_str(), "wb");
        FILE* f_ie = fopen((out_dir + "/interp_entries.bin").c_str(), "wb");
        constexpr uint32_t NO = 0xFFFFFFFF;

        // Reusable buffer (one per entry type to avoid aliasing issues)
        std::vector<uint32_t> buf;
        buf.reserve(4096);

        // Helper: parse IDs from mmap'd entry file
        auto parse = [&buf](const MappedFile& f, uint32_t off) {
            buf.clear();
            if (off == NO || off + 2 > f.size) return;
            uint16_t c; memcpy(&c, f.data + off, 2);
            if (off + 2 + (size_t)c * 4 > f.size) return;
            buf.resize(c);
            memcpy(buf.data(), f.data + off + 2, c * 4);
        };
        auto remap = [](std::vector<uint32_t>& ids, const RemapRef& rm) {
            constexpr uint32_t NO2 = 0xFFFFFFFF;
            for (auto& id : ids) if (id < rm.size() && rm[id] != NO2) id = rm[id];
            std::sort(ids.begin(), ids.end());
        };
        // Write entry and return offset, or NO if empty
        auto emit = [&NO](FILE* f, const uint32_t* ids, size_t n) -> uint32_t {
            if (n == 0) return NO;
            uint32_t off = (uint32_t)ftell(f);
            uint16_t c = (uint16_t)n;
            fwrite(&c, 2, 1, f); fwrite(ids, 4, n, f);
            return off;
        };

        // Merge-walk: old cells + added cells in sorted order
        // Both are sorted by cell_id. Merge them, skip removed.
        size_t old_i = 0, add_i = 0;
        size_t cells_written = 0;

        auto process_cell = [&](uint64_t cid, int32_t oi) {
            // For each entry type: check correction → remap old → write
            auto do_entry = [&](const MappedFile& old_e, size_t geo_off, const RemapRef& rm,
                                const std::unordered_map<uint64_t, const std::vector<uint32_t>*>& corr,
                                FILE* outf, uint8_t flag_bit) -> uint32_t {
                // Check flag
                bool has = false;
                if (oi >= 0 && (size_t)oi * 20 + geo_off + 4 <= m_geo.size) {
                    uint32_t off; memcpy(&off, m_geo.data + oi * 20 + geo_off, 4);
                    has = (off != NO);
                } else if (oi >= 0) {
                    std::cerr << "OOB: oi=" << oi << " geo_off=" << geo_off << " m_geo.size=" << m_geo.size << std::endl;
                    return NO;
                }
                auto fc = flag_corrections.find(cid);
                if (fc != flag_corrections.end()) has = (fc->second & flag_bit) != 0;
                if (corr.count(cid)) has = true;
                if (!has) return NO;

                auto cit = corr.find(cid);
                if (cit != corr.end()) return emit(outf, cit->second->data(), cit->second->size());

                if (oi < 0) return NO;
                if ((size_t)oi * 20 + geo_off + 4 > m_geo.size) return NO;
                uint32_t off; memcpy(&off, m_geo.data + oi * 20 + geo_off, 4);
                parse(old_e, off);
                if (buf.empty()) return NO;
                remap(buf, rm);
                return emit(outf, buf.data(), buf.size());
            };

            uint32_t so = do_entry(m_se, 8, w_rm, cs, f_se, 1);
            uint32_t ao = do_entry(m_ae, 12, a_rm, ca, f_ae, 2);
            uint32_t io = do_entry(m_ie, 16, i_rm, ci_map, f_ie, 4);
            fwrite(&cid, 8, 1, f_geo); fwrite(&so, 4, 1, f_geo); fwrite(&ao, 4, 1, f_geo); fwrite(&io, 4, 1, f_geo);
            cells_written++;
        };

        while (old_i < n_old || add_i < geo_added.size()) {
            uint64_t old_cid = UINT64_MAX, add_cid = UINT64_MAX;
            if (old_i < n_old) memcpy(&old_cid, m_geo.data + old_i * 20, 8);
            if (add_i < geo_added.size()) add_cid = geo_added[add_i];

            if (old_cid <= add_cid) {
                if (!rm_set.count(old_cid))
                    process_cell(old_cid, (int32_t)old_i);
                old_i++;
                if (old_cid == add_cid) add_i++; // skip duplicate add
            } else {
                process_cell(add_cid, -1);
                add_i++;
            }

            if (cells_written % 10000000 == 0 && cells_written > 0) {
                std::cerr << "    " << cells_written << " cells, se=" << ftell(f_se)/1024/1024
                          << "M ae=" << ftell(f_ae)/1024/1024 << "M rss=" << get_rss_mb() << "M" << std::endl;
            }
        }

        fclose(f_geo); fclose(f_se); fclose(f_ae); fclose(f_ie);
        unmap_file(m_geo); unmap_file(m_se); unmap_file(m_ae); unmap_file(m_ie);
        unmap_file(m_w_rm); unmap_file(m_a_rm); unmap_file(m_i_rm);
        std::cerr << "  Geo: " << cells_written << " cells written" << std::endl;
        malloc_trim(0);
        log_phase("  Geo entries complete", t_entry);

        // Admin: small, use existing rebuild + corrections
        {
            uint32_t no_data = 0xFFFFFFFF;
            std::unordered_map<uint32_t,uint32_t> ad_rm;
            MappedFile m_ad_rm = mmap_remap(PatchFileId::ADMIN_POLYGONS);
            if (m_ad_rm.data) {
                const uint32_t* ad_vec = (const uint32_t*)m_ad_rm.data;
                size_t ad_count = m_ad_rm.size / 4;
                for (uint32_t i = 0; i < ad_count; i++)
                    if (ad_vec[i] != 0xFFFFFFFF) ad_rm[i] = ad_vec[i];
                unmap_file(m_ad_rm);
            }
            auto old_ac = read_file(cur_dir + "/admin_cells.bin");
            auto old_adme = read_file(cur_dir + "/admin_entries.bin");
            auto admin = rebuild_admin_from_remap(old_ac, old_adme, ad_rm, admin_added, admin_removed);

            auto ecit = entry_corrections.find((uint32_t)PatchFileId::ADMIN_ENTRIES);
            if (ecit != entry_corrections.end()) {
                std::unordered_map<uint64_t, const std::vector<uint32_t>*> ac_corr;
                for (auto& c : ecit->second) ac_corr[c.cell_id] = &c.ids;
                size_t n = admin.admin_cells_data.size() / 12;
                FILE* fac = fopen((out_dir + "/admin_cells.bin").c_str(), "wb");
                FILE* fae = fopen((out_dir + "/admin_entries.bin").c_str(), "wb");
                uint32_t ae_wpos = 0;
                for (size_t i = 0; i < n; i++) {
                    uint64_t cid; memcpy(&cid, admin.admin_cells_data.data()+i*12, 8);
                    fwrite(&cid, 8, 1, fac);
                    auto cit = ac_corr.find(cid);
                    if (cit != ac_corr.end()) {
                        uint32_t off = cit->second->empty() ? no_data : ae_wpos;
                        fwrite(&off, 4, 1, fac);
                        if (!cit->second->empty()) {
                            uint16_t c = cit->second->size();
                            fwrite(&c, 2, 1, fae); fwrite(cit->second->data(), 4, c, fae);
                            ae_wpos += 2 + c*4;
                        }
                    } else {
                        uint32_t old_off; memcpy(&old_off, admin.admin_cells_data.data()+i*12+8, 4);
                        if (old_off != no_data && old_off+2 <= admin.admin_entries_data.size()) {
                            uint32_t new_off = ae_wpos; fwrite(&new_off, 4, 1, fac);
                            uint16_t c; memcpy(&c, admin.admin_entries_data.data()+old_off, 2);
                            fwrite(admin.admin_entries_data.data()+old_off, 1, 2+c*4, fae);
                            ae_wpos += 2+c*4;
                        } else {
                            fwrite(&no_data, 4, 1, fac);
                        }
                    }
                }
                fclose(fac); fclose(fae);
                std::cerr << "  Admin: " << n << " cells, " << ac_corr.size() << " corrections" << std::endl;
            } else {
                write_file(out_dir + "/admin_cells.bin", admin.admin_cells_data);
                write_file(out_dir + "/admin_entries.bin", admin.admin_entries_data);
            }
            log_phase("  Admin", t_entry);
        }
    }

    // Cleanup
    unmap_file(patch_map);
    { std::string cmd = "rm -rf '" + tmpdir + "'"; system(cmd.c_str()); }
    log_phase("Total", t_start);
    std::cerr << "Patch applied. Output in " << out_dir << std::endl;
    return 0;
}
