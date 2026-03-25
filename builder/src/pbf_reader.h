#pragma once

// Parallel PBF reader — replaces osmium for PBF file reading.
// Reads all blob offsets upfront, then dispatches decompression + parsing
// to a thread pool with zero sequential bottleneck.

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

// --- Raw PBF data structures ---

// Tag lookup helper
inline const char* find_tag(const std::vector<std::pair<std::string, std::string>>& tags,
                             const char* key) {
    for (const auto& [k, v] : tags) {
        if (k == key) return v.c_str();
    }
    return nullptr;
}

struct PbfNode {
    int64_t id;
    double lat;
    double lng;
    std::vector<std::pair<std::string, std::string>> tags;

    const char* tag(const char* key) const { return find_tag(tags, key); }
};

struct PbfWay {
    int64_t id;
    std::vector<int64_t> node_refs;
    std::vector<std::pair<std::string, std::string>> tags;

    const char* tag(const char* key) const { return find_tag(tags, key); }
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

    const char* tag(const char* key) const { return find_tag(tags, key); }
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

// Managed PBF file — opens once, provides phased parallel reading.
class PbfFile {
public:
    explicit PbfFile(const std::string& filename, unsigned num_threads = 0);
    ~PbfFile();

    // Read all blocks containing the specified entity types in parallel.
    // Callback is called from worker threads with (block, thread_index).
    // The callback must be thread-safe OR use the thread_index for thread-local storage.
    // If ordered=true: decompresses in parallel but calls callback in file order
    // (useful for nodes where sequential ID locality matters for mmap performance).
    void read_blocks(std::function<void(PbfBlock&&, unsigned thread_idx)> callback,
                     const std::string& entity_filter = "nwr",
                     bool ordered = false);

    // Convenience: read only relations, only nodes, only ways
    unsigned thread_count() const { return num_threads_; }

    const std::vector<BlobInfo>& blobs() const { return blobs_; }

private:
    std::string filename_;
    unsigned num_threads_;
    std::vector<BlobInfo> blobs_;
    // Pre-classified blob indices by content type
    std::vector<size_t> node_blobs_;
    std::vector<size_t> way_blobs_;
    std::vector<size_t> relation_blobs_;
    bool classified_ = false;
    void classify_blobs();
};
