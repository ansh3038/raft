// Tests for WAL + snapshot persistence: state must survive destroying and
// recreating a RaftNode against the same data directory (simulating a
// crash/restart), and the log must compact once a snapshot threshold is
// crossed.
#include <cstdio>
#include <filesystem>

#include "raft_node.h"
#include "test_util.h"

using raft::AppendEntriesRequest;
using raft::ClientRequestMsg;
using raft::LogEntry;
using raft::PeerInfo;
using raft::RaftNode;
using raft::RequestVoteRequest;

namespace {
std::string freshDir(const std::string& name) {
    std::string dir = "test_data_" + name;
    std::filesystem::remove_all(dir);
    return dir;
}
}  // namespace

int main() {
    // currentTerm_/votedFor_ survive a restart, preventing a double vote
    // in a term the node already voted in.
    {
        std::string dir = freshDir("vote");
        {
            RaftNode node(1, 19101, std::vector<PeerInfo>{}, nullptr, dir);
            auto resp = node.handleRequestVote(RequestVoteRequest{1, 42});
            CHECK(resp.voteGranted);
        }
        {
            // Fresh RaftNode object, same data dir: simulates a restart.
            RaftNode restarted(1, 19102, std::vector<PeerInfo>{}, nullptr, dir);
            CHECK(restarted.term() == 1);
            auto second =
                restarted.handleRequestVote(RequestVoteRequest{1, 43});
            CHECK(!second.voteGranted);
        }
        std::filesystem::remove_all(dir);
    }

    // Replicated log entries survive a restart.
    {
        std::string dir = freshDir("log");
        {
            RaftNode node(1, 19103, std::vector<PeerInfo>{}, nullptr, dir);
            std::vector<LogEntry> entries{{1, "cmd-a"}, {1, "cmd-b"}};
            node.handleAppendEntries(
                AppendEntriesRequest{1, 99, 0, 0, 2, entries});
        }
        {
            RaftNode restarted(1, 19104, std::vector<PeerInfo>{}, nullptr, dir);
            auto log = restarted.logCopy();
            CHECK(log.size() == 2);
            CHECK(log[0].command == "cmd-a");
            CHECK(log[1].command == "cmd-b");
        }
        std::filesystem::remove_all(dir);
    }

    // A conflicting truncation is reflected on disk too (not just memory).
    {
        std::string dir = freshDir("truncate");
        {
            RaftNode node(1, 19105, std::vector<PeerInfo>{}, nullptr, dir);
            std::vector<LogEntry> first{{1, "a"}, {1, "b"}, {1, "c"}};
            node.handleAppendEntries(
                AppendEntriesRequest{1, 99, 0, 0, 0, first});
            std::vector<LogEntry> conflict{{2, "b2"}};
            node.handleAppendEntries(
                AppendEntriesRequest{2, 99, 1, 1, 0, conflict});
        }
        {
            RaftNode restarted(1, 19106, std::vector<PeerInfo>{}, nullptr, dir);
            auto log = restarted.logCopy();
            CHECK(log.size() == 2);
            CHECK(log[0].command == "a");
            CHECK(log[1].command == "b2");
            CHECK(log[1].term == 2);
        }
        std::filesystem::remove_all(dir);
    }

    // Once applied entries exceed snapshotThreshold, the log is compacted
    // (both in memory and, after a restart, via the snapshot file).
    {
        std::string dir = freshDir("snapshot");
        {
            RaftNode node(1, 19107, std::vector<PeerInfo>{}, nullptr, dir,
                           /*snapshotThreshold=*/2);
            std::vector<LogEntry> entries{{1, "a"}, {1, "b"}, {1, "c"}};
            node.handleAppendEntries(
                AppendEntriesRequest{1, 99, 0, 0, 3, entries});
            // 3 entries applied >= threshold of 2: log_ should have been
            // compacted down to just the tail beyond the snapshot point.
            CHECK(node.logCopy().size() < 3);
        }
        {
            // After restart, the snapshot boundary plus remaining WAL
            // entries reconstruct exactly the entries after the snapshot.
            RaftNode restarted(1, 19108, std::vector<PeerInfo>{}, nullptr,
                                dir, /*snapshotThreshold=*/2);
            CHECK(restarted.commitIndex() == 3);
        }
        std::filesystem::remove_all(dir);
    }

    TEST_MAIN_RETURN();
}
