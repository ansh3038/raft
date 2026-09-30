// Integration test: spins up a real 3-node cluster (in-process, real TCP
// loopback sockets via the custom RPC layer) and checks that exactly one
// leader is elected and the rest remain followers in the same term.
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include "raft_node.h"
#include "test_util.h"

using namespace std::chrono_literals;
using raft::ClientRequestMsg;
using raft::PeerInfo;
using raft::RaftNode;
using raft::Role;

namespace {

std::vector<PeerInfo> allPeers() {
    return {{1, "127.0.0.1", 19201},
            {2, "127.0.0.1", 19202},
            {3, "127.0.0.1", 19203}};
}

std::vector<PeerInfo> peersExcept(uint32_t id) {
    std::vector<PeerInfo> result;
    for (const auto& p : allPeers()) {
        if (p.id != id) result.push_back(p);
    }
    return result;
}

}  // namespace

int main() {
    std::vector<std::unique_ptr<RaftNode>> nodes;
    for (const auto& self : allPeers()) {
        nodes.push_back(std::make_unique<RaftNode>(self.id, self.port,
                                                     peersExcept(self.id)));
    }
    for (auto& n : nodes) n->start();

    // Election timeout is up to 3s; give the cluster time to converge.
    std::this_thread::sleep_for(4s);

    int leaders = 0;
    uint32_t leaderTerm = 0;
    for (auto& n : nodes) {
        if (n->role() == Role::Leader) {
            leaders++;
            leaderTerm = n->term();
        }
    }
    CHECK(leaders == 1);

    int followers = 0;
    for (auto& n : nodes) {
        if (n->role() == Role::Follower) {
            followers++;
            CHECK(n->term() == leaderTerm);
        }
    }
    CHECK(followers == 2);

    // Log replication: submit a couple of client commands directly to the
    // leader and verify they get replicated and applied (commitIndex
    // advances) consistently on every node.
    RaftNode* leader = nullptr;
    for (auto& n : nodes) {
        if (n->role() == Role::Leader) leader = n.get();
    }
    CHECK(leader != nullptr);
    if (leader != nullptr) {
        auto r1 = leader->handleClientRequest(ClientRequestMsg{"cmd-1"});
        CHECK(r1.success);
        auto r2 = leader->handleClientRequest(ClientRequestMsg{"cmd-2"});
        CHECK(r2.success);

        // Heartbeats fire every 500ms; give replication a couple of rounds.
        std::this_thread::sleep_for(2s);

        for (auto& n : nodes) {
            CHECK(n->commitIndex() == 2);
            auto log = n->logCopy();
            CHECK(log.size() == 2);
            CHECK(log[0].command == "cmd-1");
            CHECK(log[1].command == "cmd-2");
        }
    }

    nodes.clear();  // destructors stop servers/threads cleanly
    TEST_MAIN_RETURN();
}
