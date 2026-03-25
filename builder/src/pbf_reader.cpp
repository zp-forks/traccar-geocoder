#include "pbf_reader.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <unistd.h>
#include <zlib.h>

#include <protozero/pbf_reader.hpp>

// --- PBF file format constants ---
// BlobHeader max size: 32 MiB (per spec), but typically <100 bytes
// Blob max size: 32 MiB (per spec)
static constexpr size_t MAX_BLOB_HEADER_SIZE = 64 * 1024;
static constexpr size_t MAX_BLOB_SIZE = 64 * 1024 * 1024;

// Protobuf field tags (from OSM PBF spec)
namespace BlobHeaderTag {
    constexpr int TYPE = 1;      // string
    constexpr int DATASIZE = 3;  // int32
}
namespace BlobTag {
    constexpr int RAW = 1;       // bytes
    constexpr int RAW_SIZE = 2;  // int32
    constexpr int ZLIB = 3;      // bytes
}
namespace PrimitiveBlockTag {
    constexpr int STRINGTABLE = 1;   // StringTable
    constexpr int PRIMITIVEGROUP = 2; // repeated PrimitiveGroup
    constexpr int GRANULARITY = 17;  // int32 (default 100)
    constexpr int LAT_OFFSET = 19;   // int64 (default 0)
    constexpr int LON_OFFSET = 20;   // int64 (default 0)
}
namespace PrimitiveGroupTag {
    constexpr int NODES = 1;
    constexpr int DENSE = 2;
    constexpr int WAYS = 3;
    constexpr int RELATIONS = 4;
}
namespace StringTableTag {
    constexpr int S = 1; // repeated bytes
}
namespace DenseNodesTag {
    constexpr int ID = 1;
    constexpr int LAT = 8;
    constexpr int LON = 9;
    constexpr int KEYS_VALS = 10;
}
namespace WayTag {
    constexpr int ID = 1;
    constexpr int KEYS = 2;
    constexpr int VALS = 3;
    constexpr int REFS = 8;
}
namespace RelationTag {
    constexpr int ID = 1;
    constexpr int KEYS = 2;
    constexpr int VALS = 3;
    constexpr int ROLES_SID = 8;
    constexpr int MEMIDS = 9;
    constexpr int TYPES = 10;
}

// --- File I/O helpers ---

static std::string read_bytes(int fd, size_t offset, size_t count) {
    std::string buf(count, '\0');
    ssize_t n = pread(fd, buf.data(), count, offset);
    if (n != (ssize_t)count) {
        throw std::runtime_error("pread failed at offset " + std::to_string(offset) +
                                 ": expected " + std::to_string(count) +
                                 " got " + std::to_string(n));
    }
    return buf;
}

static uint32_t read_u32_be(int fd, size_t offset) {
    uint8_t buf[4];
    if (pread(fd, buf, 4, offset) != 4) {
        throw std::runtime_error("failed to read 4 bytes at offset " + std::to_string(offset));
    }
    return (uint32_t(buf[0]) << 24) | (uint32_t(buf[1]) << 16) |
           (uint32_t(buf[2]) << 8) | uint32_t(buf[3]);
}

// --- Blob scanning ---

std::vector<BlobInfo> scan_pbf_blobs(const std::string& filename) {
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) throw std::runtime_error("cannot open " + filename + ": " + strerror(errno));

    // Get file size
    off_t file_size = lseek(fd, 0, SEEK_END);

    std::vector<BlobInfo> blobs;
    size_t offset = 0;

    while (offset < (size_t)file_size) {
        // Read 4-byte big-endian BlobHeader size
        uint32_t header_size = read_u32_be(fd, offset);
        if (header_size > MAX_BLOB_HEADER_SIZE) {
            throw std::runtime_error("BlobHeader too large: " + std::to_string(header_size));
        }

        // Read BlobHeader
        std::string header_data = read_bytes(fd, offset + 4, header_size);
        protozero::pbf_reader header_pbf(header_data);

        std::string type;
        int32_t data_size = 0;
        while (header_pbf.next()) {
            switch (header_pbf.tag()) {
                case BlobHeaderTag::TYPE:
                    type = header_pbf.get_string();
                    break;
                case BlobHeaderTag::DATASIZE:
                    data_size = header_pbf.get_int32();
                    break;
                default:
                    header_pbf.skip();
            }
        }

        BlobInfo info;
        info.offset = offset;
        info.header_size = header_size;
        info.data_size = data_size;
        info.type = type;
        blobs.push_back(info);

        // Advance past: 4 bytes length + header + blob data
        offset += 4 + header_size + data_size;
    }

    close(fd);
    return blobs;
}

// --- Blob decompression ---

std::string read_and_decompress_blob(int fd, const BlobInfo& info) {
    // Read the blob data (after 4-byte header length + header)
    size_t blob_offset = info.offset + 4 + info.header_size;
    std::string blob_data = read_bytes(fd, blob_offset, info.data_size);

    protozero::pbf_reader blob_pbf(blob_data);
    std::string raw_data;
    std::string zlib_data;
    int32_t raw_size = 0;

    while (blob_pbf.next()) {
        switch (blob_pbf.tag()) {
            case BlobTag::RAW: {
                auto view = blob_pbf.get_view();
                raw_data.assign(view.data(), view.size());
                break;
            }
            case BlobTag::RAW_SIZE:
                raw_size = blob_pbf.get_int32();
                break;
            case BlobTag::ZLIB: {
                auto view = blob_pbf.get_view();
                zlib_data.assign(view.data(), view.size());
                break;
            }
            default:
                blob_pbf.skip();
        }
    }

    if (!raw_data.empty()) {
        return raw_data;
    }

    if (!zlib_data.empty()) {
        std::string decompressed(raw_size, '\0');
        z_stream strm{};
        strm.next_in = reinterpret_cast<Bytef*>(zlib_data.data());
        strm.avail_in = zlib_data.size();
        strm.next_out = reinterpret_cast<Bytef*>(decompressed.data());
        strm.avail_out = decompressed.size();

        if (inflateInit(&strm) != Z_OK) {
            throw std::runtime_error("inflateInit failed");
        }
        int ret = inflate(&strm, Z_FINISH);
        inflateEnd(&strm);
        if (ret != Z_STREAM_END) {
            throw std::runtime_error("zlib inflate failed: " + std::to_string(ret));
        }
        decompressed.resize(strm.total_out);
        return decompressed;
    }

    throw std::runtime_error("blob has neither raw nor zlib data");
}

// --- PrimitiveBlock decoding ---

PbfBlock decode_pbf_blob(const char* data, size_t size) {
    PbfBlock block;

    protozero::pbf_reader pb(data, size);
    auto& string_table = block.string_table;
    int32_t granularity = 100;
    int64_t lat_offset = 0;
    int64_t lon_offset = 0;

    // First pass: extract string table and granularity
    while (pb.next()) {
        switch (pb.tag()) {
            case PrimitiveBlockTag::STRINGTABLE: {
                protozero::pbf_reader st = pb.get_message();
                while (st.next()) {
                    if (st.tag() == StringTableTag::S) {
                        auto view = st.get_view();
                        string_table.emplace_back(view.data(), view.size());
                    } else {
                        st.skip();
                    }
                }
                break;
            }
            case PrimitiveBlockTag::GRANULARITY:
                granularity = pb.get_int32();
                break;
            case PrimitiveBlockTag::LAT_OFFSET:
                lat_offset = pb.get_int64();
                break;
            case PrimitiveBlockTag::LON_OFFSET:
                lon_offset = pb.get_int64();
                break;
            default:
                pb.skip();
        }
    }

    auto* st_ptr = &block.string_table; // for tag construction

    // Second pass: decode primitive groups
    protozero::pbf_reader pb2(data, size);
    while (pb2.next()) {
        if (pb2.tag() != PrimitiveBlockTag::PRIMITIVEGROUP) {
            pb2.skip();
            continue;
        }

        protozero::pbf_reader group = pb2.get_message();
        while (group.next()) {
            switch (group.tag()) {
                case PrimitiveGroupTag::DENSE: {
                    protozero::pbf_reader dense = group.get_message();
                    std::vector<int64_t> ids_vec, lats_vec, lons_vec;
                    std::vector<int32_t> kv_vec;

                    while (dense.next()) {
                        switch (dense.tag()) {
                            case DenseNodesTag::ID:
                                for (auto v : dense.get_packed_sint64()) ids_vec.push_back(v);
                                break;
                            case DenseNodesTag::LAT:
                                for (auto v : dense.get_packed_sint64()) lats_vec.push_back(v);
                                break;
                            case DenseNodesTag::LON:
                                for (auto v : dense.get_packed_sint64()) lons_vec.push_back(v);
                                break;
                            case DenseNodesTag::KEYS_VALS:
                                for (auto v : dense.get_packed_int32()) kv_vec.push_back(v);
                                break;
                            default:
                                dense.skip();
                        }
                    }

                    int64_t id = 0, lat = 0, lon = 0;
                    size_t kv_pos = 0;

                    for (size_t i = 0; i < ids_vec.size(); i++) {
                        id += ids_vec[i];
                        lat += (i < lats_vec.size()) ? lats_vec[i] : 0;
                        lon += (i < lons_vec.size()) ? lons_vec[i] : 0;

                        PbfNode node;
                        node.id = id;
                        node.lat = 0.000000001 * (lat_offset + (int64_t)granularity * lat);
                        node.lng = 0.000000001 * (lon_offset + (int64_t)granularity * lon);

                        // Only allocate tags if this node has any
                        if (kv_pos < kv_vec.size() && kv_vec[kv_pos] != 0) {
                            node.tags.string_table = st_ptr;
                            while (kv_pos < kv_vec.size()) {
                                int32_t key_idx = kv_vec[kv_pos++];
                                if (key_idx == 0) break;
                                int32_t val_idx = (kv_pos < kv_vec.size()) ? kv_vec[kv_pos++] : 0;
                                node.tags.indices.emplace_back(key_idx, val_idx);
                            }
                        } else {
                            if (kv_pos < kv_vec.size()) kv_pos++; // skip the 0 delimiter
                        }

                        block.nodes.push_back(std::move(node));
                    }
                    break;
                }

                case PrimitiveGroupTag::WAYS: {
                    protozero::pbf_reader way_msg = group.get_message();
                    PbfWay way;
                    std::vector<uint32_t> keys, vals;

                    while (way_msg.next()) {
                        switch (way_msg.tag()) {
                            case WayTag::ID:
                                way.id = way_msg.get_int64();
                                break;
                            case WayTag::KEYS: {
                                auto packed = way_msg.get_packed_uint32();
                                for (auto v : packed) keys.push_back(v);
                                break;
                            }
                            case WayTag::VALS: {
                                auto packed = way_msg.get_packed_uint32();
                                for (auto v : packed) vals.push_back(v);
                                break;
                            }
                            case WayTag::REFS: {
                                auto packed = way_msg.get_packed_sint64();
                                int64_t ref = 0;
                                for (auto delta : packed) {
                                    ref += delta;
                                    way.node_refs.push_back(ref);
                                }
                                break;
                            }
                            default:
                                way_msg.skip();
                        }
                    }

                    way.tags.string_table = st_ptr;
                    for (size_t i = 0; i < keys.size() && i < vals.size(); i++) {
                        way.tags.indices.emplace_back(keys[i], vals[i]);
                    }
                    block.ways.push_back(std::move(way));
                    break;
                }

                case PrimitiveGroupTag::RELATIONS: {
                    protozero::pbf_reader rel_msg = group.get_message();
                    PbfRelation rel;
                    std::vector<uint32_t> keys, vals;
                    std::vector<int32_t> roles_sid;
                    std::vector<int64_t> memids_delta;
                    std::vector<int32_t> types;

                    while (rel_msg.next()) {
                        switch (rel_msg.tag()) {
                            case RelationTag::ID:
                                rel.id = rel_msg.get_int64();
                                break;
                            case RelationTag::KEYS: {
                                auto packed = rel_msg.get_packed_uint32();
                                for (auto v : packed) keys.push_back(v);
                                break;
                            }
                            case RelationTag::VALS: {
                                auto packed = rel_msg.get_packed_uint32();
                                for (auto v : packed) vals.push_back(v);
                                break;
                            }
                            case RelationTag::ROLES_SID: {
                                auto packed = rel_msg.get_packed_int32();
                                for (auto v : packed) roles_sid.push_back(v);
                                break;
                            }
                            case RelationTag::MEMIDS: {
                                auto packed = rel_msg.get_packed_sint64();
                                for (auto v : packed) memids_delta.push_back(v);
                                break;
                            }
                            case RelationTag::TYPES: {
                                auto packed = rel_msg.get_packed_int32();
                                for (auto v : packed) types.push_back(v);
                                break;
                            }
                            default:
                                rel_msg.skip();
                        }
                    }

                    rel.tags.string_table = st_ptr;
                    for (size_t i = 0; i < keys.size() && i < vals.size(); i++) {
                        rel.tags.indices.emplace_back(keys[i], vals[i]);
                    }

                    int64_t memid = 0;
                    for (size_t i = 0; i < memids_delta.size(); i++) {
                        memid += memids_delta[i];
                        char type = '?';
                        if (i < types.size()) {
                            switch (types[i]) {
                                case 0: type = 'n'; break;
                                case 1: type = 'w'; break;
                                case 2: type = 'r'; break;
                            }
                        }
                        uint32_t role_sid = (i < roles_sid.size()) ? roles_sid[i] : 0;
                        rel.members.push_back({type, memid, role_sid});
                    }
                    block.relations.push_back(std::move(rel));
                    break;
                }

                default:
                    group.skip();
            }
        }
    }

    return block;
}

// --- Parallel reader ---

void read_pbf_parallel(const std::string& filename,
                       std::function<void(PbfBlock&&, size_t block_index)> callback,
                       unsigned num_threads,
                       const std::string& entity_filter) {
    if (num_threads == 0) num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 4;

    // Scan blob offsets
    auto blobs = scan_pbf_blobs(filename);

    // Filter to OSMData blobs only
    std::vector<size_t> data_blob_indices;
    for (size_t i = 0; i < blobs.size(); i++) {
        if (blobs[i].type == "OSMData") {
            data_blob_indices.push_back(i);
        }
    }

    std::cerr << "  PBF: " << blobs.size() << " blobs, "
              << data_blob_indices.size() << " data blocks, "
              << num_threads << " threads" << std::endl;

    // Open file descriptor for each thread (pread is thread-safe with separate fds)
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) throw std::runtime_error("cannot open " + filename);

    // Parallel decode
    std::atomic<size_t> next_block{0};
    std::mutex callback_mutex;
    std::vector<std::thread> threads;

    for (unsigned t = 0; t < num_threads; t++) {
        threads.emplace_back([&]() {
            // Each thread opens its own fd for independent pread
            int local_fd = open(filename.c_str(), O_RDONLY);
            if (local_fd < 0) return;

            while (true) {
                size_t idx = next_block.fetch_add(1);
                if (idx >= data_blob_indices.size()) break;

                size_t blob_idx = data_blob_indices[idx];
                const auto& info = blobs[blob_idx];

                std::string decompressed = read_and_decompress_blob(local_fd, info);
                PbfBlock block = decode_pbf_blob(decompressed.data(), decompressed.size());

                // Filter entities if requested
                if (entity_filter.find('n') == std::string::npos) block.nodes.clear();
                if (entity_filter.find('w') == std::string::npos) block.ways.clear();
                if (entity_filter.find('r') == std::string::npos) block.relations.clear();

                std::lock_guard<std::mutex> lock(callback_mutex);
                callback(std::move(block), idx);
            }

            close(local_fd);
        });
    }

    for (auto& t : threads) t.join();
    close(fd);
}

// --- PbfFile implementation ---

PbfFile::PbfFile(const std::string& filename, unsigned num_threads)
    : filename_(filename), num_threads_(num_threads) {
    if (num_threads_ == 0) num_threads_ = std::thread::hardware_concurrency();
    if (num_threads_ == 0) num_threads_ = 4;
    blobs_ = scan_pbf_blobs(filename_);
}

PbfFile::~PbfFile() = default;

void PbfFile::classify_blobs() {
    if (classified_) return;

    // PBF files are ordered: node blocks → way blocks → relation blocks.
    // Use parallel classification: each thread peeks at assigned blobs.
    std::vector<size_t> data_indices;
    for (size_t i = 0; i < blobs_.size(); i++) {
        if (blobs_[i].type == "OSMData") data_indices.push_back(i);
    }

    // Classify in parallel
    struct BlobType { bool nodes = false, ways = false, relations = false; };
    std::vector<BlobType> types(data_indices.size());
    std::atomic<size_t> next_idx{0};
    std::vector<std::thread> threads;

    for (unsigned t = 0; t < num_threads_; t++) {
        threads.emplace_back([&]() {
            int local_fd = open(filename_.c_str(), O_RDONLY);
            if (local_fd < 0) return;
            while (true) {
                size_t j = next_idx.fetch_add(1);
                if (j >= data_indices.size()) break;
                size_t blob_idx = data_indices[j];
                std::string data = read_and_decompress_blob(local_fd, blobs_[blob_idx]);
                protozero::pbf_reader pb(data);
                auto& bt = types[j];
                while (pb.next()) {
                    if (pb.tag() == PrimitiveBlockTag::PRIMITIVEGROUP) {
                        protozero::pbf_reader group = pb.get_message();
                        while (group.next()) {
                            switch (group.tag()) {
                                case PrimitiveGroupTag::NODES:
                                case PrimitiveGroupTag::DENSE:
                                    bt.nodes = true; group.skip(); break;
                                case PrimitiveGroupTag::WAYS:
                                    bt.ways = true; group.skip(); break;
                                case PrimitiveGroupTag::RELATIONS:
                                    bt.relations = true; group.skip(); break;
                                default: group.skip();
                            }
                        }
                    } else {
                        pb.skip();
                    }
                }
            }
            close(local_fd);
        });
    }
    for (auto& t : threads) t.join();

    for (size_t j = 0; j < data_indices.size(); j++) {
        if (types[j].nodes) node_blobs_.push_back(data_indices[j]);
        if (types[j].ways) way_blobs_.push_back(data_indices[j]);
        if (types[j].relations) relation_blobs_.push_back(data_indices[j]);
    }

    classified_ = true;

    std::cerr << "  PBF classified: " << node_blobs_.size() << " node blocks, "
              << way_blobs_.size() << " way blocks, "
              << relation_blobs_.size() << " relation blocks" << std::endl;
}

void PbfFile::read_blocks(std::function<void(PbfBlock&&, unsigned thread_idx)> callback,
                           const std::string& entity_filter,
                           bool ordered) {
    classify_blobs();

    // Collect which blob indices to process
    std::vector<size_t> indices;
    if (entity_filter.find('n') != std::string::npos)
        indices.insert(indices.end(), node_blobs_.begin(), node_blobs_.end());
    if (entity_filter.find('w') != std::string::npos)
        indices.insert(indices.end(), way_blobs_.begin(), way_blobs_.end());
    if (entity_filter.find('r') != std::string::npos)
        indices.insert(indices.end(), relation_blobs_.begin(), relation_blobs_.end());

    if (indices.empty()) return;

    if (ordered) {
        // Ordered mode: decompress + decode in parallel, callback in file order.
        // Ring buffer of decoded PbfBlocks — workers do all heavy work,
        // consumer thread just runs the lightweight callback.
        const size_t WINDOW = num_threads_ * 4;
        std::vector<PbfBlock> ring(WINDOW);
        std::vector<std::atomic<bool>> ring_ready(WINDOW);
        for (auto& r : ring_ready) r.store(false);

        std::atomic<size_t> next_decompress{0};
        std::vector<std::thread> decomp_threads;

        for (unsigned t = 0; t < num_threads_; t++) {
            decomp_threads.emplace_back([&]() {
                int local_fd = open(filename_.c_str(), O_RDONLY);
                if (local_fd < 0) return;
                while (true) {
                    size_t j = next_decompress.fetch_add(1);
                    if (j >= indices.size()) break;
                    size_t slot = j % WINDOW;
                    // Wait until this slot has been consumed
                    while (ring_ready[slot].load(std::memory_order_acquire)) {
                        std::this_thread::yield();
                    }
                    // Decompress + decode (heavy work done in parallel)
                    std::string data = read_and_decompress_blob(local_fd, blobs_[indices[j]]);
                    ring[slot] = decode_pbf_blob(data.data(), data.size());
                    // Filter entities
                    if (entity_filter.find('n') == std::string::npos) ring[slot].nodes.clear();
                    if (entity_filter.find('w') == std::string::npos) ring[slot].ways.clear();
                    if (entity_filter.find('r') == std::string::npos) ring[slot].relations.clear();
                    ring_ready[slot].store(true, std::memory_order_release);
                }
                close(local_fd);
            });
        }

        // Consume in order — callback only (lightweight)
        for (size_t j = 0; j < indices.size(); j++) {
            size_t slot = j % WINDOW;
            while (!ring_ready[slot].load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            PbfBlock block = std::move(ring[slot]);
            ring_ready[slot].store(false, std::memory_order_release);

            if (entity_filter.find('n') == std::string::npos) block.nodes.clear();
            if (entity_filter.find('w') == std::string::npos) block.ways.clear();
            if (entity_filter.find('r') == std::string::npos) block.relations.clear();

            callback(std::move(block), 0); // thread_idx=0, single consumer

            if ((j + 1) % 1000 == 0) {
                std::cerr << "  Processed " << (j + 1) << "/" << indices.size() << " blocks..." << std::endl;
            }
        }

        for (auto& t : decomp_threads) t.join();
        return;
    }

    // Unordered mode: full parallel decode + callback
    std::atomic<size_t> next_idx{0};
    std::atomic<size_t> blocks_done{0};
    std::vector<std::thread> threads;

    for (unsigned t = 0; t < num_threads_; t++) {
        threads.emplace_back([&, t]() {
            int local_fd = open(filename_.c_str(), O_RDONLY);
            if (local_fd < 0) return;

            while (true) {
                size_t j = next_idx.fetch_add(1);
                if (j >= indices.size()) break;

                size_t blob_idx = indices[j];
                std::string decompressed = read_and_decompress_blob(local_fd, blobs_[blob_idx]);
                PbfBlock block = decode_pbf_blob(decompressed.data(), decompressed.size());

                // Filter entities
                if (entity_filter.find('n') == std::string::npos) block.nodes.clear();
                if (entity_filter.find('w') == std::string::npos) block.ways.clear();
                if (entity_filter.find('r') == std::string::npos) block.relations.clear();

                callback(std::move(block), t);

                size_t done = blocks_done.fetch_add(1) + 1;
                if (done % 1000 == 0) {
                    std::cerr << "  Processed " << done << "/" << indices.size() << " blocks..." << std::endl;
                }
            }

            close(local_fd);
        });
    }

    for (auto& t : threads) t.join();
}
