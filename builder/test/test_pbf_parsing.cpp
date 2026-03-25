// Test PBF parsing: extracts all data using osmium and dumps to a canonical
// text format. Used to verify our custom PBF reader produces identical results.
//
// Usage: test_pbf_parsing <file.osm.pbf> [--nodes|--ways|--relations|--all]
//
// Output format (sorted for deterministic comparison):
// NODE <id> <lat_nanodeg> <lon_nanodeg> [<key>=<value> ...]
// WAY <id> <node_ref>,<node_ref>,... [<key>=<value> ...]
// REL <id> <type>/<ref>/<role>,... [<key>=<value> ...]

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include <osmium/io/pbf_input.hpp>
#include <osmium/visitor.hpp>

struct NodeRecord {
    int64_t id;
    int32_t lat_nanodeg;
    int32_t lon_nanodeg;
    std::vector<std::pair<std::string,std::string>> tags;

    bool operator<(const NodeRecord& o) const { return id < o.id; }
};

struct WayRecord {
    int64_t id;
    std::vector<int64_t> node_refs;
    std::vector<std::pair<std::string,std::string>> tags;

    bool operator<(const WayRecord& o) const { return id < o.id; }
};

struct RelationRecord {
    int64_t id;
    struct Member {
        char type; // 'n', 'w', 'r'
        int64_t ref;
        std::string role;
    };
    std::vector<Member> members;
    std::vector<std::pair<std::string,std::string>> tags;

    bool operator<(const RelationRecord& o) const { return id < o.id; }
};

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: test_pbf_parsing <file.osm.pbf> [--nodes|--ways|--relations|--all|--summary]" << std::endl;
        return 1;
    }

    std::string input_file = argv[1];
    bool want_nodes = false, want_ways = false, want_relations = false, summary_only = false;
    for (int i = 2; i < argc; i++) {
        if (std::strcmp(argv[i], "--nodes") == 0) want_nodes = true;
        else if (std::strcmp(argv[i], "--ways") == 0) want_ways = true;
        else if (std::strcmp(argv[i], "--relations") == 0) want_relations = true;
        else if (std::strcmp(argv[i], "--all") == 0) { want_nodes = want_ways = want_relations = true; }
        else if (std::strcmp(argv[i], "--summary") == 0) summary_only = true;
    }
    if (!want_nodes && !want_ways && !want_relations && !summary_only) {
        summary_only = true;
    }

    std::vector<NodeRecord> nodes;
    std::vector<WayRecord> ways;
    std::vector<RelationRecord> relations;

    // Count tags of interest for geocoding
    uint64_t addr_nodes = 0, highway_ways = 0, interp_ways = 0;
    uint64_t building_addr_ways = 0, admin_relations = 0;

    {
        osmium::io::Reader reader{input_file};
        while (auto buf = reader.read()) {
            for (const auto& item : buf) {
                if (item.type() == osmium::item_type::node) {
                    const auto& node = static_cast<const osmium::Node&>(item);
                    if (!node.location().valid()) continue;

                    NodeRecord nr;
                    nr.id = node.id();
                    nr.lat_nanodeg = node.location().x();
                    nr.lon_nanodeg = node.location().y();
                    for (const auto& tag : node.tags()) {
                        nr.tags.push_back({tag.key(), tag.value()});
                    }
                    std::sort(nr.tags.begin(), nr.tags.end());

                    if (node.tags()["addr:housenumber"] && node.tags()["addr:street"]) {
                        addr_nodes++;
                    }

                    if (want_nodes) nodes.push_back(std::move(nr));

                } else if (item.type() == osmium::item_type::way) {
                    const auto& way = static_cast<const osmium::Way&>(item);

                    WayRecord wr;
                    wr.id = way.id();
                    for (const auto& nr : way.nodes()) {
                        wr.node_refs.push_back(nr.ref());
                    }
                    for (const auto& tag : way.tags()) {
                        wr.tags.push_back({tag.key(), tag.value()});
                    }
                    std::sort(wr.tags.begin(), wr.tags.end());

                    if (way.tags()["highway"]) highway_ways++;
                    if (way.tags()["addr:interpolation"]) interp_ways++;
                    if (way.tags()["addr:housenumber"]) building_addr_ways++;

                    if (want_ways) ways.push_back(std::move(wr));

                } else if (item.type() == osmium::item_type::relation) {
                    const auto& rel = static_cast<const osmium::Relation&>(item);

                    RelationRecord rr;
                    rr.id = rel.id();
                    for (const auto& member : rel.members()) {
                        char type = '?';
                        switch (member.type()) {
                            case osmium::item_type::node: type = 'n'; break;
                            case osmium::item_type::way: type = 'w'; break;
                            case osmium::item_type::relation: type = 'r'; break;
                            default: break;
                        }
                        rr.members.push_back({type, member.ref(), member.role()});
                    }
                    for (const auto& tag : rel.tags()) {
                        rr.tags.push_back({tag.key(), tag.value()});
                    }
                    std::sort(rr.tags.begin(), rr.tags.end());

                    const char* boundary = rel.tags()["boundary"];
                    if (boundary && std::strcmp(boundary, "administrative") == 0) {
                        admin_relations++;
                    }

                    if (want_relations) relations.push_back(std::move(rr));
                }
            }
        }
    }

    if (summary_only) {
        std::cout << "NODES=" << nodes.size() << std::endl;
        std::cout << "WAYS=" << ways.size() << std::endl;
        std::cout << "RELATIONS=" << relations.size() << std::endl;
        // Re-count from the file if we didn't collect
        if (nodes.empty() && ways.empty() && relations.empty()) {
            // Re-read just for counts
            uint64_t n_nodes = 0, n_ways = 0, n_rels = 0;
            osmium::io::Reader reader2{input_file};
            while (auto buf = reader2.read()) {
                for (const auto& item : buf) {
                    if (item.type() == osmium::item_type::node) {
                        const auto& node = static_cast<const osmium::Node&>(item);
                        if (node.location().valid()) n_nodes++;
                    } else if (item.type() == osmium::item_type::way) n_ways++;
                    else if (item.type() == osmium::item_type::relation) n_rels++;
                }
            }
            std::cout << "NODES=" << n_nodes << std::endl;
            std::cout << "WAYS=" << n_ways << std::endl;
            std::cout << "RELATIONS=" << n_rels << std::endl;
        }
        std::cout << "ADDR_NODES=" << addr_nodes << std::endl;
        std::cout << "HIGHWAY_WAYS=" << highway_ways << std::endl;
        std::cout << "INTERP_WAYS=" << interp_ways << std::endl;
        std::cout << "BUILDING_ADDR_WAYS=" << building_addr_ways << std::endl;
        std::cout << "ADMIN_RELATIONS=" << admin_relations << std::endl;
        return 0;
    }

    // Sort for deterministic output
    std::sort(nodes.begin(), nodes.end());
    std::sort(ways.begin(), ways.end());
    std::sort(relations.begin(), relations.end());

    // Output
    for (const auto& n : nodes) {
        std::cout << "NODE " << n.id << " " << n.lat_nanodeg << " " << n.lon_nanodeg;
        for (const auto& [k, v] : n.tags) std::cout << " " << k << "=" << v;
        std::cout << "\n";
    }
    for (const auto& w : ways) {
        std::cout << "WAY " << w.id;
        for (size_t i = 0; i < w.node_refs.size(); i++) {
            std::cout << (i == 0 ? " " : ",") << w.node_refs[i];
        }
        for (const auto& [k, v] : w.tags) std::cout << " " << k << "=" << v;
        std::cout << "\n";
    }
    for (const auto& r : relations) {
        std::cout << "REL " << r.id;
        for (size_t i = 0; i < r.members.size(); i++) {
            std::cout << (i == 0 ? " " : ",") << r.members[i].type << "/" << r.members[i].ref << "/" << r.members[i].role;
        }
        for (const auto& [k, v] : r.tags) std::cout << " " << k << "=" << v;
        std::cout << "\n";
    }

    return 0;
}
