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
    lastHeartbeat_ = std::chrono::steady_clock::now();
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
        lastHeartbeat_ = std::chrono::steady_clock::now();
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
    lastHeartbeat_ = std::chrono::steady_clock::now();
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
        int timeoutMs = randomElectionTimeoutMs();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        if (stopping_) return;

        std::unique_lock<std::mutex> lock(mu_);
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::steady_clock::now() - lastHeartbeat_)
                           .count();
        bool shouldElect = role_ != Role::Leader && elapsed >= timeoutMs;
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
        lastHeartbeat_ = std::chrono::steady_clock::now();
        term = currentTerm_;
        log("starting election for term " + std::to_string(term));
    }

    int votes = 1;  // vote for self
    for (const auto& peer : peers_) {
        auto resp = sendRequestVote(peer.host, peer.port,
                                     RequestVoteRequest{term, id_},
                                     kRpcTimeoutMs);
        std::lock_guard<std::mutex> lock(mu_);
        if (currentTerm_ != term || role_ != Role::Candidate) return;
        if (resp.has_value()) {
            if (resp->term > currentTerm_) {
                becomeFollower(resp->term);
                return;
            }
            if (resp->voteGranted) votes++;
        }
    }

    std::lock_guard<std::mutex> lock(mu_);
    if (currentTerm_ != term || role_ != Role::Candidate) return;

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
        for (const auto& peer : peers_) {
            auto resp = sendAppendEntries(peer.host, peer.port,
                                           AppendEntriesRequest{term, id_},
                                           kRpcTimeoutMs);
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
