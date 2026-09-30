// Usage:
//   raft_client <cluster_config_file> <command...>
//
// Sends a command to the cluster for replication. Tries nodes in the
// config file in order; if a node isn't the leader it replies with a
// redirect hint (leaderId), which the client follows. Falls back to
// trying the next node in the list if the leader is unknown or
// unreachable.
#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>

#include "raft_rpc.h"

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

const ClusterEntry* findEntry(const std::vector<ClusterEntry>& entries,
                               uint32_t id) {
    for (const auto& e : entries) {
        if (e.id == id) return &e;
    }
    return nullptr;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "usage: " << argv[0]
                   << " <cluster_config_file> <command...>\n";
        return 1;
    }

    auto entries = parseConfig(argv[1]);
    std::ostringstream cmd;
    for (int i = 2; i < argc; ++i) {
        if (i > 2) cmd << ' ';
        cmd << argv[i];
    }
    raft::ClientRequestMsg req{cmd.str()};

    constexpr int kTimeoutMs = 500;
    constexpr int kMaxAttempts = 10;

    int nextIdx = 0;             // fallback: round-robin through entries
    std::optional<uint32_t> targetId;  // leader hint from last response

    for (int attempt = 0; attempt < kMaxAttempts && !entries.empty();
         ++attempt) {
        const ClusterEntry* target = nullptr;
        if (targetId.has_value()) target = findEntry(entries, *targetId);
        if (!target) {
            target = &entries[nextIdx % entries.size()];
            nextIdx++;
        }

        std::cerr << "trying node " << target->id << " (" << target->host
                   << ":" << target->port << ")...\n";
        auto resp = raft::sendClientRequest(target->host, target->port, req,
                                             kTimeoutMs);
        if (!resp.has_value()) {
            targetId.reset();
            continue;
        }
        if (resp->success) {
            std::cout << "OK: command applied by node " << target->id
                       << "\n";
            return 0;
        }
        if (resp->leaderId != 0) {
            targetId = resp->leaderId;
        } else {
            targetId.reset();
        }
    }

    std::cerr << "failed: could not reach leader after " << kMaxAttempts
               << " attempts\n";
    return 1;
}
