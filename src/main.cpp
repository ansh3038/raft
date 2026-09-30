// Usage:
//   raft_node <my_id> <cluster_config_file>
//
// Cluster config file format (one line per node, including self):
//   <id> <host> <port>
#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>

#include "raft_node.h"

namespace {

struct ClusterEntry {
    uint32_t id;
    std::string host;
    int port;
};

std::vector<ClusterEntry> parseConfig(const std::string& path) {
    std::vector<ClusterEntry> entries;
    std::ifstream in(path);
    if (!in) {
        std::cerr << "cannot open config file: " << path << "\n";
        std::exit(1);
    }
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty() || line[0] == '#') continue;
        std::istringstream iss(line);
        ClusterEntry e;
        if (!(iss >> e.id >> e.host >> e.port)) continue;
        entries.push_back(e);
    }
    return entries;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "usage: " << argv[0] << " <my_id> <cluster_config_file>\n";
        return 1;
    }

    uint32_t myId = static_cast<uint32_t>(std::stoul(argv[1]));
    auto entries = parseConfig(argv[2]);

    int myPort = -1;
    std::vector<raft::PeerInfo> peers;
    for (const auto& e : entries) {
        if (e.id == myId) {
            myPort = e.port;
        } else {
            peers.push_back(raft::PeerInfo{e.id, e.host, e.port});
        }
    }

    if (myPort < 0) {
        std::cerr << "id " << myId << " not found in config file\n";
        return 1;
    }

    raft::RaftNode node(myId, myPort, std::move(peers),
                         [](uint32_t index, const std::string& command) {
                             std::cout << "[applied] index=" << index
                                       << " command=\"" << command << "\""
                                       << std::endl;
                         });
    node.run();
    return 0;
}
