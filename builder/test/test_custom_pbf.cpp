// Test custom PBF reader against osmium reference output.
// Usage: test_custom_pbf <file.osm.pbf> [--all|--summary]
// Output format matches test_pbf_parsing.cpp for diff comparison.

#include "../src/pbf_reader.h"

#include <algorithm>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: test_custom_pbf <file.osm.pbf> [--all|--summary]" << std::endl;
        return 1;
    }

    std::string input_file = argv[1];
    bool summary_only = true;
    bool dump_all = false;
    for (int i = 2; i < argc; i++) {
        if (std::strcmp(argv[i], "--all") == 0) { dump_all = true; summary_only = false; }
        else if (std::strcmp(argv[i], "--summary") == 0) summary_only = true;
    }

    std::vector<PbfNode> all_nodes;
    std::vector<PbfWay> all_ways;
    std::vector<PbfRelation> all_relations;

    uint64_t addr_nodes = 0, highway_ways = 0, interp_ways = 0;
    uint64_t building_addr_ways = 0, admin_relations = 0;

    read_pbf_parallel(input_file, [&](PbfBlock&& block, size_t) {
        for (auto& n : block.nodes) {
            if (n.tag("addr:housenumber") && n.tag("addr:street")) addr_nodes++;
            if (dump_all) all_nodes.push_back(std::move(n));
        }
        for (auto& w : block.ways) {
            if (w.tag("highway")) highway_ways++;
            if (w.tag("addr:interpolation")) interp_ways++;
            if (w.tag("addr:housenumber")) building_addr_ways++;
            if (dump_all) all_ways.push_back(std::move(w));
        }
        for (auto& r : block.relations) {
            const char* boundary = r.tag("boundary");
            if (boundary && std::strcmp(boundary, "administrative") == 0) admin_relations++;

            if (dump_all) all_relations.push_back(std::move(r));
        }
    }, 4); // Use 4 threads for small test files

    if (summary_only) {
        std::cout << "NODES=" << (dump_all ? all_nodes.size() : 0) << std::endl;
        std::cout << "WAYS=" << (dump_all ? all_ways.size() : 0) << std::endl;
        std::cout << "RELATIONS=" << (dump_all ? all_relations.size() : 0) << std::endl;
        std::cout << "ADDR_NODES=" << addr_nodes << std::endl;
        std::cout << "HIGHWAY_WAYS=" << highway_ways << std::endl;
        std::cout << "INTERP_WAYS=" << interp_ways << std::endl;
        std::cout << "BUILDING_ADDR_WAYS=" << building_addr_ways << std::endl;
        std::cout << "ADMIN_RELATIONS=" << admin_relations << std::endl;
        return 0;
    }

    // Sort for deterministic output
    std::sort(all_nodes.begin(), all_nodes.end(), [](const PbfNode& a, const PbfNode& b) { return a.id < b.id; });
    std::sort(all_ways.begin(), all_ways.end(), [](const PbfWay& a, const PbfWay& b) { return a.id < b.id; });
    std::sort(all_relations.begin(), all_relations.end(), [](const PbfRelation& a, const PbfRelation& b) { return a.id < b.id; });

    // Output in same format as test_pbf_parsing (osmium version)
    // Note: osmium outputs lat/lon as int32 nanodegrees via location.x()/y()
    // Our reader outputs double lat/lng. We need to convert to match.
    for (const auto& n : all_nodes) {
        // Match osmium's output: location.x() = lon, location.y() = lat (fixed-point * 10^7)
        int32_t lon_fixed = static_cast<int32_t>(n.lng * 10000000.0 + (n.lng >= 0 ? 0.5 : -0.5));
        int32_t lat_fixed = static_cast<int32_t>(n.lat * 10000000.0 + (n.lat >= 0 ? 0.5 : -0.5));
        std::cout << "NODE " << n.id << " " << lon_fixed << " " << lat_fixed;
        // Resolve + sort tags for deterministic output
        std::vector<std::pair<std::string,std::string>> tags;
        if (n.tags.string_table) {
            for (auto& [ki, vi] : n.tags.indices) {
                auto& st = *n.tags.string_table;
                tags.emplace_back(ki < st.size() ? st[ki] : "", vi < st.size() ? st[vi] : "");
            }
        }
        std::sort(tags.begin(), tags.end());
        for (const auto& [k, v] : tags) std::cout << " " << k << "=" << v;
        std::cout << "\n";
    }
    for (const auto& w : all_ways) {
        std::cout << "WAY " << w.id;
        for (size_t i = 0; i < w.node_refs.size(); i++) {
            std::cout << (i == 0 ? " " : ",") << w.node_refs[i];
        }
        {
            std::vector<std::pair<std::string,std::string>> tags;
            if (w.tags.string_table) {
                for (auto& [ki, vi] : w.tags.indices) {
                    auto& st = *w.tags.string_table;
                    tags.emplace_back(ki < st.size() ? st[ki] : "", vi < st.size() ? st[vi] : "");
                }
            }
            std::sort(tags.begin(), tags.end());
            for (const auto& [k, v] : tags) std::cout << " " << k << "=" << v;
        }
        std::cout << "\n";
    }
    for (const auto& r : all_relations) {
        std::cout << "REL " << r.id;
        for (size_t i = 0; i < r.members.size(); i++) {
            std::cout << (i == 0 ? " " : ",") << r.members[i].type << "/" << r.members[i].ref << "/" << r.member_role(i);
        }
        {
            std::vector<std::pair<std::string,std::string>> tags;
            if (r.tags.string_table) {
                for (auto& [ki, vi] : r.tags.indices) {
                    auto& st = *r.tags.string_table;
                    tags.emplace_back(ki < st.size() ? st[ki] : "", vi < st.size() ? st[vi] : "");
                }
            }
            std::sort(tags.begin(), tags.end());
            for (const auto& [k, v] : tags) std::cout << " " << k << "=" << v;
        }
        std::cout << "\n";
    }

    return 0;
}
