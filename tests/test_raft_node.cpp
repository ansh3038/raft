// Unit tests for RaftNode's vote/heartbeat decision logic. These call the
// handler methods directly (no networking, no started server) so they run
// fast and deterministically.
#include "raft_node.h"
#include "test_util.h"

using raft::AppendEntriesRequest;
using raft::ClientRequestMsg;
using raft::LogEntry;
using raft::PeerInfo;
using raft::RaftNode;
using raft::RequestVoteRequest;
using raft::Role;

int main() {
    // A node grants its vote to the first candidate that asks in a term it
    // hasn't voted in yet, and adopts that term.
    {
        RaftNode node(1, 19001, std::vector<PeerInfo>{});
        auto resp = node.handleRequestVote(RequestVoteRequest{1, 42});
        CHECK(resp.voteGranted);
        CHECK(resp.term == 1);
        CHECK(node.term() == 1);
    }

    // A node does not grant a second, different vote within the same term.
    {
        RaftNode node(1, 19002, std::vector<PeerInfo>{});
        auto first = node.handleRequestVote(RequestVoteRequest{1, 42});
        CHECK(first.voteGranted);
        auto second = node.handleRequestVote(RequestVoteRequest{1, 43});
        CHECK(!second.voteGranted);
    }

    // A repeat vote request from the same candidate in the same term is
    // granted again (idempotent, e.g. due to a retried RPC).
    {
        RaftNode node(1, 19003, std::vector<PeerInfo>{});
        node.handleRequestVote(RequestVoteRequest{1, 42});
        auto again = node.handleRequestVote(RequestVoteRequest{1, 42});
        CHECK(again.voteGranted);
    }

    // A vote request for a strictly higher term always resets votedFor and
    // is granted, even if the node already voted in an earlier term.
    {
        RaftNode node(1, 19004, std::vector<PeerInfo>{});
        node.handleRequestVote(RequestVoteRequest{1, 42});
        auto higherTerm = node.handleRequestVote(RequestVoteRequest{2, 43});
        CHECK(higherTerm.voteGranted);
        CHECK(node.term() == 2);
    }

    // AppendEntries (heartbeat) at or above the current term succeeds and
    // moves the node to Follower.
    {
        RaftNode node(1, 19005, std::vector<PeerInfo>{});
        auto resp = node.handleAppendEntries(AppendEntriesRequest{5, 99});
        CHECK(resp.success);
        CHECK(resp.term == 5);
        CHECK(node.term() == 5);
        CHECK(node.role() == Role::Follower);
    }

    // AppendEntries with a stale (lower) term is rejected and does not
    // change the node's current term.
    {
        RaftNode node(1, 19006, std::vector<PeerInfo>{});
        node.handleAppendEntries(AppendEntriesRequest{5, 99});
        auto stale = node.handleAppendEntries(AppendEntriesRequest{3, 100});
        CHECK(!stale.success);
        CHECK(node.term() == 5);
    }

    // A follower with an empty log accepts entries appended right at the
    // start of the log (prevLogIndex == 0) and applies them once told
    // they're committed via leaderCommit.
    {
        RaftNode node(1, 19007, std::vector<PeerInfo>{});
        std::vector<LogEntry> entries{{1, "cmd-a"}, {1, "cmd-b"}};
        auto resp = node.handleAppendEntries(
            AppendEntriesRequest{1, 99, 0, 0, 2, entries});
        CHECK(resp.success);
        CHECK(resp.matchIndex == 2);
        CHECK(node.commitIndex() == 2);
        auto log = node.logCopy();
        CHECK(log.size() == 2);
        CHECK(log[0].command == "cmd-a");
        CHECK(log[1].command == "cmd-b");
    }

    // AppendEntries is rejected when the follower's log doesn't have a
    // matching entry at prevLogIndex/prevLogTerm (log matching property).
    {
        RaftNode node(1, 19008, std::vector<PeerInfo>{});
        auto resp = node.handleAppendEntries(
            AppendEntriesRequest{1, 99, 5, 1, 0, {}});
        CHECK(!resp.success);
        CHECK(node.logCopy().empty());
    }

    // A conflicting entry at an existing index causes the follower to
    // truncate its log from that point and adopt the leader's entries.
    {
        RaftNode node(1, 19009, std::vector<PeerInfo>{});
        std::vector<LogEntry> first{{1, "a"}, {1, "b"}, {1, "c"}};
        node.handleAppendEntries(AppendEntriesRequest{1, 99, 0, 0, 0, first});
        CHECK(node.logCopy().size() == 3);

        // Leader (now in term 2) overwrites index 2 onward.
        std::vector<LogEntry> conflict{{2, "b2"}};
        auto resp = node.handleAppendEntries(
            AppendEntriesRequest{2, 99, 1, 1, 0, conflict});
        CHECK(resp.success);
        auto log = node.logCopy();
        CHECK(log.size() == 2);
        CHECK(log[0].command == "a");
        CHECK(log[1].command == "b2");
        CHECK(log[1].term == 2);
    }

    // handleClientRequest only succeeds on the leader; followers/candidates
    // reject and report the last known leader (if any).
    {
        RaftNode node(1, 19010, std::vector<PeerInfo>{});
        auto resp = node.handleClientRequest(ClientRequestMsg{"noop"});
        CHECK(!resp.success);
        CHECK(node.logCopy().empty());
    }

    TEST_MAIN_RETURN();
}
