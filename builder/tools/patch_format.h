#pragma once
// Shared definitions for the geocoder patch system.
// Used by geocoder-canonicalize, geocoder-diff, and geocoder-patch.

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

// --- Binary record structs (must match types.h and server) ---

#pragma pack(push, 1)
struct PatchWayHeader {
    uint32_t node_offset;
    uint8_t node_count;
    uint32_t name_id;
};
static_assert(sizeof(PatchWayHeader) == 9, "WayHeader must be 9 bytes packed");

struct PatchAddrPoint {
    float lat;
    float lng;
    uint32_t housenumber_id;
    uint32_t street_id;
};
static_assert(sizeof(PatchAddrPoint) == 16, "AddrPoint must be 16 bytes");

struct PatchNodeCoord {
    float lat;
    float lng;
};
static_assert(sizeof(PatchNodeCoord) == 8, "NodeCoord must be 8 bytes");
#pragma pack(pop)

// InterpWay and AdminPolygon have compiler-dependent padding.
// Read them field-by-field instead of struct-casting.

struct PatchInterpWay {
    uint32_t node_offset;
    uint8_t node_count;
    uint32_t street_id;
    uint32_t start_number;
    uint32_t end_number;
    uint8_t interpolation;
};

struct PatchAdminPolygon {
    uint32_t vertex_offset;
    uint32_t vertex_count;
    uint32_t name_id;
    uint8_t admin_level;
    float area;
    uint16_t country_code;
};

// --- File I/O helpers ---

inline std::vector<char> read_file(const std::string& path) {
    std::ifstream f(path, std::ios::binary | std::ios::ate);
    if (!f) return {};
    auto size = f.tellg();
    f.seekg(0);
    std::vector<char> data(size);
    f.read(data.data(), size);
    return data;
}

inline bool write_file(const std::string& path, const char* data, size_t size) {
    std::ofstream f(path, std::ios::binary);
    if (!f) return false;
    f.write(data, size);
    return f.good();
}

inline bool write_file(const std::string& path, const std::vector<char>& data) {
    return write_file(path, data.data(), data.size());
}

inline const char* get_string(const std::vector<char>& pool, uint32_t offset) {
    if (offset >= pool.size()) return "";
    return pool.data() + offset;
}

// Read WayHeader array — detect stride from file size vs known struct sizes
inline std::vector<PatchWayHeader> read_ways(const std::string& path) {
    auto data = read_file(path);
    // Determine stride: builder writes sizeof(WayHeader) which may be 9 or 12
    // depending on compiler padding. Try 12 first (common), then 9.
    size_t stride = 12;
    if (data.size() % 12 != 0 || data.size() / 12 == 0) {
        stride = 9;
    }
    size_t count = data.size() / stride;
    std::vector<PatchWayHeader> result(count);
    for (size_t i = 0; i < count; i++) {
        const char* p = data.data() + i * stride;
        memcpy(&result[i].node_offset, p, 4);
        result[i].node_count = static_cast<uint8_t>(p[4]);
        // name_id is at offset 5 (packed) or 8 (padded to 4-byte alignment)
        size_t name_off = (stride == 12) ? 8 : 5;
        memcpy(&result[i].name_id, p + name_off, 4);
    }
    return result;
}

inline size_t detect_way_stride(const std::string& path) {
    auto data = read_file(path);
    return (data.size() % 12 == 0 && data.size() / 12 > 0) ? 12 : 9;
}

template<typename T>
inline std::vector<T> read_structs(const std::string& path) {
    auto data = read_file(path);
    size_t count = data.size() / sizeof(T);
    std::vector<T> result(count);
    if (count > 0) memcpy(result.data(), data.data(), count * sizeof(T));
    return result;
}

// Read InterpWay with field-by-field parsing to handle padding
inline std::vector<PatchInterpWay> read_interps(const std::string& path) {
    auto data = read_file(path);
    // Determine record size from file: try sizeof(InterpWay) as compiled by the builder
    // The builder uses the same struct, so sizeof should match.
    size_t rec_size = sizeof(PatchInterpWay);
    // But the builder's InterpWay has padding. Read with the builder's size.
    // From types.h the struct is: u32 + u8 + u32 + u32 + u32 + u8 = 18 bytes raw
    // but compiler pads to 20 bytes typically. Check by trying both.
    size_t count_20 = data.size() / 20;
    size_t count_18 = data.size() / 18;
    // Use whichever divides evenly
    size_t stride = (data.size() % 20 == 0 && count_20 > 0) ? 20 :
                    (data.size() % 18 == 0 && count_18 > 0) ? 18 : 20;
    size_t count = data.size() / stride;
    std::vector<PatchInterpWay> result(count);
    for (size_t i = 0; i < count; i++) {
        const char* p = data.data() + i * stride;
        memcpy(&result[i].node_offset, p, 4); p += 4;
        result[i].node_count = *reinterpret_cast<const uint8_t*>(p); p += 1;
        // Skip padding to align street_id
        if (stride == 20) p += 3; // 3 bytes padding after node_count
        memcpy(&result[i].street_id, p, 4); p += 4;
        memcpy(&result[i].start_number, p, 4); p += 4;
        memcpy(&result[i].end_number, p, 4); p += 4;
        result[i].interpolation = *reinterpret_cast<const uint8_t*>(p);
    }
    return result;
}

// Read AdminPolygon with field-by-field parsing
inline std::vector<PatchAdminPolygon> read_admin_polygons(const std::string& path) {
    auto data = read_file(path);
    // AdminPolygon: u32 + u32 + u32 + u8 + f32 + u16 = 19 raw bytes
    // Compiler likely pads to 20 or 24 bytes. Try common sizes.
    size_t stride = 0;
    for (size_t s : {24, 20, 19}) {
        if (data.size() % s == 0 && data.size() / s > 0) { stride = s; break; }
    }
    if (stride == 0) stride = 24; // fallback
    size_t count = data.size() / stride;
    std::vector<PatchAdminPolygon> result(count);
    for (size_t i = 0; i < count; i++) {
        const char* p = data.data() + i * stride;
        memcpy(&result[i].vertex_offset, p, 4); p += 4;
        memcpy(&result[i].vertex_count, p, 4); p += 4;
        memcpy(&result[i].name_id, p, 4); p += 4;
        result[i].admin_level = *reinterpret_cast<const uint8_t*>(p); p += 1;
        // padding for area alignment
        if (stride >= 20) {
            size_t skip = (stride == 24) ? 3 : (stride == 20 ? 3 : 0);
            p += skip;
        }
        memcpy(&result[i].area, p, 4); p += 4;
        memcpy(&result[i].country_code, p, 2);
    }
    return result;
}

// --- Patch file format ---

static constexpr char GCPATCH_MAGIC[8] = {'G','C','P','A','T','C','H','\0'};
static constexpr uint32_t GCPATCH_VERSION = 1;

enum class PatchFileId : uint32_t {
    STRINGS = 0,
    STREET_WAYS = 1,
    STREET_NODES = 2,
    ADDR_POINTS = 3,
    INTERP_WAYS = 4,
    INTERP_NODES = 5,
    ADMIN_POLYGONS = 6,
    ADMIN_VERTICES = 7,
    GEO_CELLS = 8,
    STREET_ENTRIES = 9,
    ADDR_ENTRIES = 10,
    INTERP_ENTRIES = 11,
    ADMIN_CELLS = 12,
    ADMIN_ENTRIES = 13,
    COUNT = 14
};

static const char* patch_file_names[] = {
    "strings.bin", "street_ways.bin", "street_nodes.bin", "addr_points.bin",
    "interp_ways.bin", "interp_nodes.bin", "admin_polygons.bin", "admin_vertices.bin",
    "geo_cells.bin", "street_entries.bin", "addr_entries.bin", "interp_entries.bin",
    "admin_cells.bin", "admin_entries.bin"
};

// Encoding types for each section
enum class PatchEncoding : uint32_t {
    RAW_REPLACE = 0,  // Full replacement (zlib compressed)
    COPY_INSERT = 1,  // Single zstd delta frame (zstd --patch-from)
    TWO_STAGE = 2,    // Two zstd delta frames: old→remapped, remapped→new
};

// Patch file header
struct PatchHeader {
    char magic[8];
    uint32_t version;
    uint32_t flags;
    // Per-file sections follow
};

// Section header (one per file)
struct PatchSection {
    uint32_t file_id;       // PatchFileId
    uint32_t encoding;      // PatchEncoding
    uint64_t old_size;      // Expected size of old file
    uint64_t new_size;      // Size of reconstructed file
    uint64_t data_size;     // Size of compressed section data
};

// COPY_INSERT opcodes
enum : uint8_t {
    OP_COPY = 0,    // Copy from old file: [u8 tag][u64 offset][u32 length]
    OP_INSERT = 1,  // Insert new data:    [u8 tag][u32 length][data...]
};

// Offset fixup section marker: 0xFFFFFFFD
// Format: uint32_t marker, uint32_t file_id, uint32_t stride,
//         uint32_t count, [(uint32_t record_index, uint32_t new_offset_value)] * count
// Applied to byte offset 0 of each record (node_offset/vertex_offset field).

static constexpr uint32_t FIXUP_MARKER = 0xFFFFFFFD;

// --- Grid coordinate for fingerprinting ---

inline int to_grid(float v) {
    return (int)(v * 1e5f + (v >= 0 ? 0.5f : -0.5f));
}

// --- Directory creation ---
inline void ensure_dir(const std::string& path) {
    std::string cmd = "mkdir -p '" + path + "'";
    system(cmd.c_str());
}
