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

namespace raft {

enum class MessageType : uint8_t {
    RequestVoteRequest = 1,
    RequestVoteResponse = 2,
    AppendEntriesRequest = 3,   // used only as a heartbeat (no log entries)
    AppendEntriesResponse = 4,
};

struct RequestVoteRequest {
    uint32_t term;
    uint32_t candidateId;
};

struct RequestVoteResponse {
    uint32_t term;
    bool voteGranted;
};

struct AppendEntriesRequest {
    uint32_t term;
    uint32_t leaderId;
};

struct AppendEntriesResponse {
    uint32_t term;
    bool success;
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

    RpcServer(int port, RequestVoteHandler onRequestVote,
              AppendEntriesHandler onAppendEntries);
    ~RpcServer();

    void start();
    void stop();

private:
    int port_;
    int listenFd_ = -1;
    RequestVoteHandler onRequestVote_;
    AppendEntriesHandler onAppendEntries_;
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

}  // namespace raft
