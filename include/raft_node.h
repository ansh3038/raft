// Raft leader election (RequestVote + heartbeat AppendEntries only;
// no log replication).
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "raft_rpc.h"
#include "raft_storage.h"

namespace raft {

struct PeerInfo {
    uint32_t id;
    std::string host;
    int port;
};

enum class Role { Follower, Candidate, Leader };

class RaftNode {
public:
    // Called once per committed entry, in order, with the entry's log index
    // (1-based) and command. Invoked without mu_ held.
    using ApplyCallback = std::function<void(uint32_t index, const std::string& command)>;

    // `dataDir`, if non-empty, enables persistence: currentTerm/votedFor/
    // log entries are durably written to `dataDir` (WAL + metadata file),
    // and reloaded from there on construction (crash recovery). Leave
    // empty for a purely in-memory node (e.g. in unit tests).
    // `snapshotThreshold`, if non-zero, triggers a compacting snapshot
    // (discarding applied log entries from the WAL) once that many
    // entries have been applied since the last snapshot.
    RaftNode(uint32_t id, int port, std::vector<PeerInfo> peers,
             ApplyCallback onApply = nullptr, std::string dataDir = "",
             uint32_t snapshotThreshold = 0);
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
    uint32_t commitIndex() const;
    // Snapshot of the current log (1-based index == vector index + 1).
    std::vector<LogEntry> logCopy() const;

    // Exposed for unit testing of the pure decision logic without going
    // through the network.
    RequestVoteResponse handleRequestVote(const RequestVoteRequest& req);
    AppendEntriesResponse handleAppendEntries(const AppendEntriesRequest& req);
    // Only meaningful on the leader; appends `command` to the log at the
    // current term. Returns {false, leaderId} if this node isn't the
    // leader (leaderId is 0 if unknown).
    ClientResponseMsg handleClientRequest(const ClientRequestMsg& req);

private:
    const uint32_t id_;
    const int port_;
    const std::vector<PeerInfo> peers_;
    const ApplyCallback onApply_;

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

    // Replicated log; log_[i] is the entry at absolute index
    // lastIncludedIndex_ + i + 1 (normally lastIncludedIndex_ is 0, so
    // log_[i] is at 1-based index i+1, unless entries before that point
    // have been compacted away by a snapshot).
    std::vector<LogEntry> log_;
    uint32_t commitIndex_ = 0;
    uint32_t lastApplied_ = 0;
    // Snapshot boundary: entries at or before lastIncludedIndex_ have been
    // compacted out of log_ and are no longer stored individually.
    uint32_t lastIncludedIndex_ = 0;
    uint32_t lastIncludedTerm_ = 0;
    std::optional<uint32_t> leaderId_;  // last known leader, for redirects

    // Leader-only volatile state, reinitialized on becoming leader.
    std::map<uint32_t, uint32_t> nextIndex_;   // peer id -> next log index to send
    std::map<uint32_t, uint32_t> matchIndex_;  // peer id -> highest replicated index

    std::unique_ptr<RpcServer> server_;
    std::unique_ptr<Storage> storage_;  // null if persistence is disabled
    const uint32_t snapshotThreshold_ = 0;
    std::thread electionThread_;
    std::thread heartbeatThread_;
    std::atomic<bool> stopping_{false};

    void electionTimerLoop();
    void startElection();
    void becomeLeader();
    void becomeFollower(uint32_t newTerm);
    void leaderHeartbeatLoop();

    // Log helpers. Caller must hold mu_.
    uint32_t lastLogIndex() const;   // 0 if log is empty
    uint32_t lastLogTerm() const;    // 0 if log is empty
    uint32_t termAt(uint32_t index) const;  // 0 for index 0
    void advanceCommitIndex();  // leader-only; caller must hold mu_
    void applyCommitted();      // caller must hold mu_; invokes onApply_ unlocked

    // Persistence helpers. Caller must hold mu_.
    void persistMeta();   // durably save currentTerm_/votedFor_
    void maybeSnapshot(); // compact log_ into a snapshot if threshold reached

    int randomElectionTimeoutMs() const;
    void resetElectionDeadline();  // caller must hold mu_
    void log(const std::string& msg) const;
};

}  // namespace raft
