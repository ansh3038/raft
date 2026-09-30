#include "raft_rpc.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <iostream>

namespace raft {

namespace {

bool readAll(int fd, void* buf, size_t len) {
    auto* p = static_cast<uint8_t*>(buf);
    size_t got = 0;
    while (got < len) {
        ssize_t n = ::read(fd, p + got, len - got);
        if (n <= 0) return false;
        got += static_cast<size_t>(n);
    }
    return true;
}

bool writeAll(int fd, const void* buf, size_t len) {
    const auto* p = static_cast<const uint8_t*>(buf);
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = ::write(fd, p + sent, len - sent);
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

int connectTo(const std::string& host, int port, int timeoutMs) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    struct timeval tv{};
    tv.tv_sec = timeoutMs / 1000;
    tv.tv_usec = (timeoutMs % 1000) * 1000;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        ::close(fd);
        return -1;
    }
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

}  // namespace

RpcServer::RpcServer(int port, RequestVoteHandler onRequestVote,
                      AppendEntriesHandler onAppendEntries)
    : port_(port),
      onRequestVote_(std::move(onRequestVote)),
      onAppendEntries_(std::move(onAppendEntries)) {}

RpcServer::~RpcServer() { stop(); }

void RpcServer::start() {
    listenFd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(listenFd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(static_cast<uint16_t>(port_));

    if (::bind(listenFd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::cerr << "raft_rpc: bind failed on port " << port_ << "\n";
        std::exit(1);
    }
    ::listen(listenFd_, 16);

    running_ = true;
    acceptThread_ = std::thread(&RpcServer::acceptLoop, this);
}

void RpcServer::stop() {
    if (!running_) return;
    running_ = false;
    if (listenFd_ >= 0) ::shutdown(listenFd_, SHUT_RDWR);
    if (acceptThread_.joinable()) acceptThread_.join();
    if (listenFd_ >= 0) ::close(listenFd_);
    listenFd_ = -1;
}

void RpcServer::acceptLoop() {
    // Poll with a short timeout instead of blocking in accept() forever:
    // on some platforms (e.g. macOS/BSD) shutdown()/close() on a listening
    // socket does not reliably unblock a thread stuck in accept().
    while (running_) {
        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(listenFd_, &readfds);
        struct timeval tv{0, 200 * 1000};  // 200ms

        int ready = ::select(listenFd_ + 1, &readfds, nullptr, nullptr, &tv);
        if (ready <= 0) continue;

        int clientFd = ::accept(listenFd_, nullptr, nullptr);
        if (clientFd < 0) continue;
        std::thread(&RpcServer::handleConnection, this, clientFd).detach();
    }
}

void RpcServer::handleConnection(int clientFd) {
    uint8_t type;
    if (!readAll(clientFd, &type, 1)) {
        ::close(clientFd);
        return;
    }

    if (type == static_cast<uint8_t>(MessageType::RequestVoteRequest)) {
        uint32_t netTerm, netCandidateId;
        if (!readAll(clientFd, &netTerm, 4) ||
            !readAll(clientFd, &netCandidateId, 4)) {
            ::close(clientFd);
            return;
        }
        RequestVoteRequest req{ntohl(netTerm), ntohl(netCandidateId)};
        RequestVoteResponse resp = onRequestVote_(req);

        uint8_t respType = static_cast<uint8_t>(MessageType::RequestVoteResponse);
        uint32_t respTerm = htonl(resp.term);
        uint8_t granted = resp.voteGranted ? 1 : 0;
        writeAll(clientFd, &respType, 1);
        writeAll(clientFd, &respTerm, 4);
        writeAll(clientFd, &granted, 1);
    } else if (type == static_cast<uint8_t>(MessageType::AppendEntriesRequest)) {
        uint32_t netTerm, netLeaderId;
        if (!readAll(clientFd, &netTerm, 4) ||
            !readAll(clientFd, &netLeaderId, 4)) {
            ::close(clientFd);
            return;
        }
        AppendEntriesRequest req{ntohl(netTerm), ntohl(netLeaderId)};
        AppendEntriesResponse resp = onAppendEntries_(req);

        uint8_t respType = static_cast<uint8_t>(MessageType::AppendEntriesResponse);
        uint32_t respTerm = htonl(resp.term);
        uint8_t success = resp.success ? 1 : 0;
        writeAll(clientFd, &respType, 1);
        writeAll(clientFd, &respTerm, 4);
        writeAll(clientFd, &success, 1);
    }

    ::close(clientFd);
}

std::optional<RequestVoteResponse> sendRequestVote(
    const std::string& host, int port, const RequestVoteRequest& req,
    int timeoutMs) {
    int fd = connectTo(host, port, timeoutMs);
    if (fd < 0) return std::nullopt;

    uint8_t type = static_cast<uint8_t>(MessageType::RequestVoteRequest);
    uint32_t netTerm = htonl(req.term);
    uint32_t netCandidateId = htonl(req.candidateId);
    bool ok = writeAll(fd, &type, 1) && writeAll(fd, &netTerm, 4) &&
              writeAll(fd, &netCandidateId, 4);

    RequestVoteResponse resp{};
    if (ok) {
        uint8_t respType;
        uint32_t respTerm;
        uint8_t granted;
        ok = readAll(fd, &respType, 1) && readAll(fd, &respTerm, 4) &&
             readAll(fd, &granted, 1);
        resp.term = ntohl(respTerm);
        resp.voteGranted = granted != 0;
    }
    ::close(fd);
    return ok ? std::optional(resp) : std::nullopt;
}

std::optional<AppendEntriesResponse> sendAppendEntries(
    const std::string& host, int port, const AppendEntriesRequest& req,
    int timeoutMs) {
    int fd = connectTo(host, port, timeoutMs);
    if (fd < 0) return std::nullopt;

    uint8_t type = static_cast<uint8_t>(MessageType::AppendEntriesRequest);
    uint32_t netTerm = htonl(req.term);
    uint32_t netLeaderId = htonl(req.leaderId);
    bool ok = writeAll(fd, &type, 1) && writeAll(fd, &netTerm, 4) &&
              writeAll(fd, &netLeaderId, 4);

    AppendEntriesResponse resp{};
    if (ok) {
        uint8_t respType;
        uint32_t respTerm;
        uint8_t success;
        ok = readAll(fd, &respType, 1) && readAll(fd, &respTerm, 4) &&
             readAll(fd, &success, 1);
        resp.term = ntohl(respTerm);
        resp.success = success != 0;
    }
    ::close(fd);
    return ok ? std::optional(resp) : std::nullopt;
}

}  // namespace raft
