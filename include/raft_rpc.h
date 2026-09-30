// Minimal custom binary RPC protocol for Raft leader election.
//
// Wire format (all multi-byte integers in network byte order):
//   [1 byte  MessageType]
//   [payload fields specific to the message type]
//
// A single RPC = one short-lived TCP connection: the client connects,
// writes the request message, reads the response message, then closes.
#pragma once

#include <cstdint>
#include <string>
#include <optional>
#include <functional>
#include <thread>
#include <vector>

namespace raft {

enum class MessageType : uint8_t {
    RequestVoteRequest = 1,
    RequestVoteResponse = 2,
    AppendEntriesRequest = 3,   // heartbeat when entries is empty
    AppendEntriesResponse = 4,
    ClientRequest = 5,
    ClientResponse = 6,
};

struct RequestVoteRequest {
    uint32_t term;
    uint32_t candidateId;
};

struct RequestVoteResponse {
    uint32_t term;
    bool voteGranted;
};

// One entry in the replicated log.
struct LogEntry {
    uint32_t term;
    std::string command;
};

struct AppendEntriesRequest {
    uint32_t term;
    uint32_t leaderId;
    uint32_t prevLogIndex;   // index of the log entry right before `entries`
    uint32_t prevLogTerm;    // term of that entry (0 if prevLogIndex == 0)
    uint32_t leaderCommit;   // leader's commitIndex
    std::vector<LogEntry> entries;  // empty for a pure heartbeat
};

struct AppendEntriesResponse {
    uint32_t term;
    bool success;
    // Index of the last log entry the follower now has that matches the
    // leader's log, if success. Lets the leader update matchIndex/nextIndex
    // without guessing.
    uint32_t matchIndex;
};

// A client submits a command to be replicated. Only the leader can accept
// it; other nodes reject with a hint pointing at the current leader (if
// known) so the client can retry there.
struct ClientRequestMsg {
    std::string command;
};

struct ClientResponseMsg {
    bool success;
    uint32_t leaderId;  // 0 if unknown; meaningful when success == false
};

// Starts a background TCP server listening on `port` that accepts single
// shot RPC connections and dispatches them to the provided handlers.
// Runs until the process exits (daemon thread).
class RpcServer {
public:
    using RequestVoteHandler =
        std::function<RequestVoteResponse(const RequestVoteRequest&)>;
    using AppendEntriesHandler =
        std::function<AppendEntriesResponse(const AppendEntriesRequest&)>;
    using ClientRequestHandler =
        std::function<ClientResponseMsg(const ClientRequestMsg&)>;

    RpcServer(int port, RequestVoteHandler onRequestVote,
              AppendEntriesHandler onAppendEntries,
              ClientRequestHandler onClientRequest = nullptr);
    ~RpcServer();

    void start();
    void stop();

private:
    int port_;
    int listenFd_ = -1;
    RequestVoteHandler onRequestVote_;
    AppendEntriesHandler onAppendEntries_;
    ClientRequestHandler onClientRequest_;
    std::thread acceptThread_;
    bool running_ = false;

    void acceptLoop();
    void handleConnection(int clientFd);
};

// Simple blocking client. Returns std::nullopt on any connection/IO error
// or timeout, which callers should treat as "no response" (e.g. peer down).
std::optional<RequestVoteResponse> sendRequestVote(
    const std::string& host, int port, const RequestVoteRequest& req,
    int timeoutMs);

std::optional<AppendEntriesResponse> sendAppendEntries(
    const std::string& host, int port, const AppendEntriesRequest& req,
    int timeoutMs);

std::optional<ClientResponseMsg> sendClientRequest(
    const std::string& host, int port, const ClientRequestMsg& req,
    int timeoutMs);

}  // namespace raft
