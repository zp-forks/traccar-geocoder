#pragma once

// Parallel PBF reader — replaces osmium for PBF file reading.
// Reads all blob offsets upfront, then dispatches decompression + parsing
// to a thread pool with zero sequential bottleneck.

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

// --- Raw PBF data structures ---

struct PbfNode {
    int64_t id;
    double lat;
    double lng;
    // Tags stored as indices into block-local string table
    std::vector<std::pair<std::string, std::string>> tags;
};

struct PbfWay {
    int64_t id;
    std::vector<int64_t> node_refs;
    std::vector<std::pair<std::string, std::string>> tags;
};

struct PbfRelation {
    struct Member {
        char type; // 'n', 'w', 'r'
        int64_t ref;
        std::string role;
    };
    int64_t id;
    std::vector<Member> members;
    std::vector<std::pair<std::string, std::string>> tags;
};

// A decoded PBF block containing nodes, ways, and/or relations
struct PbfBlock {
    std::vector<PbfNode> nodes;
    std::vector<PbfWay> ways;
    std::vector<PbfRelation> relations;
};

// Blob location within the file
struct BlobInfo {
    size_t offset;       // file offset to start of BlobHeader length prefix
    size_t header_size;  // BlobHeader size
    size_t data_size;    // Blob data size
    std::string type;    // "OSMHeader" or "OSMData"
};

// --- PBF Reader ---

// Scan the file to find all blob offsets
std::vector<BlobInfo> scan_pbf_blobs(const std::string& filename);

// Decode a single blob from raw file data into a PbfBlock.
// `blob_data` is the raw bytes of the Blob message (after the BlobHeader).
PbfBlock decode_pbf_blob(const char* blob_data, size_t blob_size);

// Read and decompress a single blob from file.
// Returns the decompressed PrimitiveBlock data.
std::string read_and_decompress_blob(int fd, const BlobInfo& info);

// High-level parallel reader: scans file, decodes all blocks in parallel,
// calls the callback for each decoded block (in arbitrary order).
// `num_threads` controls parallelism (0 = hardware_concurrency).
// `entity_filter` controls which entity types to parse: 'n' = nodes, 'w' = ways, 'r' = relations
void read_pbf_parallel(const std::string& filename,
                       std::function<void(PbfBlock&&, size_t block_index)> callback,
                       unsigned num_threads = 0,
                       const std::string& entity_filter = "nwr");
