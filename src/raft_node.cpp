#include "raft_node.h"

#include <chrono>
#include <cstdio>
#include <iostream>
#include <random>

namespace raft {

using namespace std::chrono_literals;

namespace {
constexpr int kElectionTimeoutMinMs = 1500;
constexpr int kElectionTimeoutMaxMs = 3000;
constexpr int kHeartbeatIntervalMs = 500;
constexpr int kRpcTimeoutMs = 300;
}  // namespace

RaftNode::RaftNode(uint32_t id, int port, std::vector<PeerInfo> peers)
    : id_(id), port_(port), peers_(std::move(peers)) {
    resetElectionDeadline();
    server_ = std::make_unique<RpcServer>(
        port_,
        [this](const RequestVoteRequest& r) { return handleRequestVote(r); },
        [this](const AppendEntriesRequest& r) { return handleAppendEntries(r); });
}

RaftNode::~RaftNode() {
    stopping_ = true;
    cv_.notify_all();
    if (server_) server_->stop();
    if (electionThread_.joinable()) electionThread_.join();
    if (heartbeatThread_.joinable()) heartbeatThread_.join();
}

void RaftNode::log(const std::string& msg) const {
    std::cout << "[node " << id_ << "] " << msg << std::endl;
}

int RaftNode::randomElectionTimeoutMs() const {
    static thread_local std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> dist(kElectionTimeoutMinMs,
                                             kElectionTimeoutMaxMs);
    return dist(rng);
}

// Resets the "haven't heard from a leader" clock and draws a fresh random
// timeout for the new waiting period. Caller must hold mu_.
void RaftNode::resetElectionDeadline() {
    lastHeartbeat_ = std::chrono::steady_clock::now();
    electionTimeoutMs_ = randomElectionTimeoutMs();
}

void RaftNode::start() {
    server_->start();
    log("listening on port " + std::to_string(port_));
    electionThread_ = std::thread(&RaftNode::electionTimerLoop, this);
}

void RaftNode::run() {
    start();
    electionThread_.join();
}

Role RaftNode::role() const {
    std::lock_guard<std::mutex> lock(mu_);
    return role_;
}

uint32_t RaftNode::term() const {
    std::lock_guard<std::mutex> lock(mu_);
    return currentTerm_;
}

RequestVoteResponse RaftNode::handleRequestVote(const RequestVoteRequest& req) {
    std::lock_guard<std::mutex> lock(mu_);
    if (req.term > currentTerm_) becomeFollower(req.term);

    bool grant = false;
    if (req.term == currentTerm_ &&
        (!votedFor_.has_value() || votedFor_ == req.candidateId)) {
        grant = true;
        votedFor_ = req.candidateId;
        resetElectionDeadline();
        log("voted for " + std::to_string(req.candidateId) + " in term " +
            std::to_string(req.term));
    }
    return RequestVoteResponse{currentTerm_, grant};
}

AppendEntriesResponse RaftNode::handleAppendEntries(
    const AppendEntriesRequest& req) {
    std::lock_guard<std::mutex> lock(mu_);
    if (req.term < currentTerm_) {
        return AppendEntriesResponse{currentTerm_, false};
    }
    if (req.term >= currentTerm_) {
        becomeFollower(req.term);
    }
    resetElectionDeadline();
    return AppendEntriesResponse{currentTerm_, true};
}

void RaftNode::becomeFollower(uint32_t newTerm) {
    if (newTerm > currentTerm_) {
        currentTerm_ = newTerm;
        votedFor_.reset();
    }
    if (role_ != Role::Follower) log("stepping down to follower");
    role_ = Role::Follower;
}

void RaftNode::electionTimerLoop() {
    while (!stopping_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        if (stopping_) return;

        std::unique_lock<std::mutex> lock(mu_);
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::steady_clock::now() - lastHeartbeat_)
                           .count();
        bool shouldElect = role_ != Role::Leader && elapsed >= electionTimeoutMs_;
        lock.unlock();

        if (shouldElect) startElection();
    }
}

void RaftNode::startElection() {
    uint32_t term;
    {
        std::lock_guard<std::mutex> lock(mu_);
        currentTerm_++;
        role_ = Role::Candidate;
        votedFor_ = id_;
        resetElectionDeadline();
        term = currentTerm_;
        log("starting election for term " + std::to_string(term));
    }

    // Fan out RequestVote RPCs to all peers concurrently (one thread each)
    // instead of sequentially, so a single slow/unreachable peer can't
    // delay hearing back from the rest by up to kRpcTimeoutMs.
    std::vector<std::optional<RequestVoteResponse>> responses(peers_.size());
    {
        std::vector<std::thread> workers;
        workers.reserve(peers_.size());
        for (size_t i = 0; i < peers_.size(); ++i) {
            const auto& peer = peers_[i];
            workers.emplace_back([&, i]() {
                responses[i] = sendRequestVote(
                    peer.host, peer.port, RequestVoteRequest{term, id_},
                    kRpcTimeoutMs);
            });
        }
        for (auto& t : workers) t.join();
    }

    int votes = 1;  // vote for self
    std::lock_guard<std::mutex> lock(mu_);
    if (currentTerm_ != term || role_ != Role::Candidate) return;
    for (const auto& resp : responses) {
        if (!resp.has_value()) continue;
        if (resp->term > currentTerm_) {
            becomeFollower(resp->term);
            return;
        }
        if (resp->voteGranted) votes++;
    }

    int majority = static_cast<int>(peers_.size() + 1) / 2 + 1;
    if (votes >= majority) {
        role_ = Role::Leader;
        log("won election for term " + std::to_string(term) + " with " +
            std::to_string(votes) + " votes -> becoming LEADER");
        if (heartbeatThread_.joinable()) heartbeatThread_.join();
        heartbeatThread_ = std::thread(&RaftNode::leaderHeartbeatLoop, this);
    } else {
        log("lost election for term " + std::to_string(term) + " (" +
            std::to_string(votes) + " votes)");
    }
}

void RaftNode::leaderHeartbeatLoop() {
    while (!stopping_) {
        uint32_t term;
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (role_ != Role::Leader) return;
            term = currentTerm_;
        }
        // Fan out heartbeats to all peers concurrently so one
        // slow/unreachable follower can't delay heartbeats to the rest,
        // which could otherwise cause them to spuriously start elections.
        std::vector<std::optional<AppendEntriesResponse>> responses(
            peers_.size());
        {
            std::vector<std::thread> workers;
            workers.reserve(peers_.size());
            for (size_t i = 0; i < peers_.size(); ++i) {
                const auto& peer = peers_[i];
                workers.emplace_back([&, i]() {
                    responses[i] = sendAppendEntries(
                        peer.host, peer.port, AppendEntriesRequest{term, id_},
                        kRpcTimeoutMs);
                });
            }
            for (auto& t : workers) t.join();
        }
        for (const auto& resp : responses) {
            if (resp.has_value() && resp->term > term) {
                std::lock_guard<std::mutex> lock(mu_);
                becomeFollower(resp->term);
                return;
            }
        }
        std::this_thread::sleep_for(
            std::chrono::milliseconds(kHeartbeatIntervalMs));
    }
}

}  // namespace raft
