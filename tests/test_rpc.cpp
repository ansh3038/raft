// Unit tests for the custom minimal RPC layer (real TCP over loopback).
#include <chrono>
#include <thread>

#include "raft_rpc.h"
#include "test_util.h"

using namespace std::chrono_literals;

int main() {
    // RequestVote round-trips correctly.
    {
        raft::RpcServer server(
            19101,
            [](const raft::RequestVoteRequest& req) {
                return raft::RequestVoteResponse{req.term, req.candidateId == 7};
            },
            [](const raft::AppendEntriesRequest&) {
                return raft::AppendEntriesResponse{0, false};
            });
        server.start();
        std::this_thread::sleep_for(50ms);

        auto granted = raft::sendRequestVote("127.0.0.1", 19101,
                                              raft::RequestVoteRequest{3, 7}, 500);
        CHECK(granted.has_value());
        CHECK(granted->term == 3);
        CHECK(granted->voteGranted);

        auto denied = raft::sendRequestVote("127.0.0.1", 19101,
                                             raft::RequestVoteRequest{3, 8}, 500);
        CHECK(denied.has_value());
        CHECK(!denied->voteGranted);
    }

    // AppendEntries (heartbeat) round-trips correctly.
    {
        raft::RpcServer server(
            19102,
            [](const raft::RequestVoteRequest&) {
                return raft::RequestVoteResponse{0, false};
            },
            [](const raft::AppendEntriesRequest& req) {
                return raft::AppendEntriesResponse{req.term, true};
            });
        server.start();
        std::this_thread::sleep_for(50ms);

        auto resp = raft::sendAppendEntries(
            "127.0.0.1", 19102, raft::AppendEntriesRequest{9, 1}, 500);
        CHECK(resp.has_value());
        CHECK(resp->term == 9);
        CHECK(resp->success);
    }

    // Connecting to a port with nothing listening returns nullopt rather
    // than hanging or crashing.
    {
        auto resp = raft::sendRequestVote("127.0.0.1", 19199,
                                           raft::RequestVoteRequest{1, 1}, 300);
        CHECK(!resp.has_value());
    }

    TEST_MAIN_RETURN();
}
