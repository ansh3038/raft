// Raft leader election (RequestVote + heartbeat AppendEntries only;
// no log replication).
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "raft_rpc.h"

namespace raft {

struct PeerInfo {
    uint32_t id;
    std::string host;
    int port;
};

enum class Role { Follower, Candidate, Leader };

class RaftNode {
public:
    RaftNode(uint32_t id, int port, std::vector<PeerInfo> peers);
    ~RaftNode();

    // Starts the RPC server and election timer in the background and
    // returns immediately. Used by tests that want to inspect state.
    void start();

    // Starts the node and blocks the calling thread forever.
    void run();

    // Thread-safe accessors, mainly for tests.
    Role role() const;
    uint32_t term() const;
    uint32_t id() const { return id_; }

    // Exposed for unit testing of the pure decision logic without going
    // through the network.
    RequestVoteResponse handleRequestVote(const RequestVoteRequest& req);
    AppendEntriesResponse handleAppendEntries(const AppendEntriesRequest& req);

private:
    const uint32_t id_;
    const int port_;
    const std::vector<PeerInfo> peers_;

    mutable std::mutex mu_;
    std::condition_variable cv_;
    uint32_t currentTerm_ = 0;
    std::optional<uint32_t> votedFor_;
    Role role_ = Role::Follower;
    std::chrono::steady_clock::time_point lastHeartbeat_;
    // The random election timeout for the *current* waiting period. Drawn
    // once whenever lastHeartbeat_ is reset (construction, vote granted,
    // heartbeat received, election started) rather than on every poll
    // tick, so each follower gets one genuine random draw per round as
    // the Raft paper intends.
    int electionTimeoutMs_ = 0;

    std::unique_ptr<RpcServer> server_;
    std::thread electionThread_;
    std::thread heartbeatThread_;
    std::atomic<bool> stopping_{false};

    void electionTimerLoop();
    void startElection();
    void becomeLeader();
    void becomeFollower(uint32_t newTerm);
    void leaderHeartbeatLoop();

    int randomElectionTimeoutMs() const;
    void resetElectionDeadline();  // caller must hold mu_
    void log(const std::string& msg) const;
};

}  // namespace raft
