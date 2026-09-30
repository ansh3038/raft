#include "raft_node.h"

#include <algorithm>
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

RaftNode::RaftNode(uint32_t id, int port, std::vector<PeerInfo> peers,
                    ApplyCallback onApply, std::string dataDir,
                    uint32_t snapshotThreshold)
    : id_(id), port_(port), peers_(std::move(peers)),
      onApply_(std::move(onApply)), snapshotThreshold_(snapshotThreshold) {
    if (!dataDir.empty()) {
        storage_ = std::make_unique<Storage>(std::move(dataDir));
        Storage::LoadedState loaded = storage_->load();
        currentTerm_ = loaded.currentTerm;
        votedFor_ = loaded.votedFor;
        lastIncludedIndex_ = loaded.lastIncludedIndex;
        lastIncludedTerm_ = loaded.lastIncludedTerm;
        log_ = std::move(loaded.log);
        // Entries up to the snapshot boundary were already applied before
        // the last shutdown/crash; commitIndex_ is conservatively set to
        // the same point and will be re-advanced once a leader emerges.
        lastApplied_ = lastIncludedIndex_;
        commitIndex_ = lastIncludedIndex_;
    }
    resetElectionDeadline();
    server_ = std::make_unique<RpcServer>(
        port_,
        [this](const RequestVoteRequest& r) { return handleRequestVote(r); },
        [this](const AppendEntriesRequest& r) { return handleAppendEntries(r); },
        [this](const ClientRequestMsg& r) { return handleClientRequest(r); });
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

uint32_t RaftNode::commitIndex() const {
    std::lock_guard<std::mutex> lock(mu_);
    return commitIndex_;
}

std::vector<LogEntry> RaftNode::logCopy() const {
    std::lock_guard<std::mutex> lock(mu_);
    return log_;
}

uint32_t RaftNode::lastLogIndex() const {
    return lastIncludedIndex_ + static_cast<uint32_t>(log_.size());
}

uint32_t RaftNode::lastLogTerm() const {
    return log_.empty() ? lastIncludedTerm_ : log_.back().term;
}

uint32_t RaftNode::termAt(uint32_t index) const {
    if (index == 0) return 0;
    if (index == lastIncludedIndex_) return lastIncludedTerm_;
    if (index < lastIncludedIndex_ || index > lastLogIndex()) return 0;
    return log_[index - lastIncludedIndex_ - 1].term;
}

// Durably persists currentTerm_/votedFor_. Caller must hold mu_ and call
// this before responding to any RPC that changed either value (Raft
// §5.1/Figure 2: "(Updated on stable storage before responding to RPCs)").
void RaftNode::persistMeta() {
    if (storage_) storage_->saveMeta(currentTerm_, votedFor_);
}

// Compacts log_ into a snapshot once enough entries have been applied
// since the last snapshot. This only shrinks disk usage (and the
// in-memory log_ vector); it does not touch commitIndex_/lastApplied_.
// Caller must hold mu_.
//
// Known limitation: there is no InstallSnapshot RPC yet, so a follower
// that falls behind past lastIncludedIndex_ cannot currently be caught
// up by the leader (its AppendEntries consistency check will keep
// failing). Fine for this project's scope; a real deployment would add
// InstallSnapshot to handle that case.
void RaftNode::maybeSnapshot() {
    if (snapshotThreshold_ == 0) return;
    if (lastApplied_ - lastIncludedIndex_ < snapshotThreshold_) return;

    uint32_t newLastIncludedIndex = lastApplied_;
    uint32_t newLastIncludedTerm = termAt(newLastIncludedIndex);
    log_.erase(log_.begin(),
               log_.begin() + (newLastIncludedIndex - lastIncludedIndex_));
    lastIncludedIndex_ = newLastIncludedIndex;
    lastIncludedTerm_ = newLastIncludedTerm;
    if (storage_) {
        storage_->writeSnapshot(lastIncludedIndex_, lastIncludedTerm_, log_);
    }
    log("compacted log via snapshot at index " +
        std::to_string(lastIncludedIndex_));
}

// Applies newly committed entries [lastApplied_+1, commitIndex_] via
// onApply_. Caller must hold mu_; the callback itself is invoked without
// the lock held to avoid re-entrancy issues in user code.
void RaftNode::applyCommitted() {
    std::vector<std::pair<uint32_t, std::string>> toApply;
    while (lastApplied_ < commitIndex_) {
        lastApplied_++;
        toApply.emplace_back(
            lastApplied_, log_[lastApplied_ - lastIncludedIndex_ - 1].command);
    }
    if (!toApply.empty() && onApply_) {
        mu_.unlock();
        for (auto& [idx, cmd] : toApply) onApply_(idx, cmd);
        mu_.lock();
    }
    if (!toApply.empty()) maybeSnapshot();
}

// Leader-only: advances commitIndex_ to the highest index replicated on a
// majority of servers, provided that entry's term matches currentTerm_
// (the Raft safety rule preventing committing entries from prior terms
// purely by count). Caller must hold mu_.
void RaftNode::advanceCommitIndex() {
    if (role_ != Role::Leader) return;
    std::vector<uint32_t> matches;
    matches.push_back(lastLogIndex());  // leader's own log
    for (const auto& [peerId, idx] : matchIndex_) matches.push_back(idx);
    std::sort(matches.begin(), matches.end());
    // With N servers total, the majority-committed index is the median
    // when sorted descending, i.e. matches[size/2] when sorted ascending
    // (matches has one entry per server including self).
    uint32_t majorityIndex = matches[matches.size() / 2];
    if (majorityIndex > commitIndex_ && termAt(majorityIndex) == currentTerm_) {
        commitIndex_ = majorityIndex;
        applyCommitted();
    }
}

RequestVoteResponse RaftNode::handleRequestVote(const RequestVoteRequest& req) {
    std::lock_guard<std::mutex> lock(mu_);
    if (req.term > currentTerm_) becomeFollower(req.term);

    bool grant = false;
    if (req.term == currentTerm_ &&
        (!votedFor_.has_value() || votedFor_ == req.candidateId)) {
        grant = true;
        votedFor_ = req.candidateId;
        persistMeta();
        resetElectionDeadline();
        log("voted for " + std::to_string(req.candidateId) + " in term " +
            std::to_string(req.term));
    }
    return RequestVoteResponse{currentTerm_, grant};
}

AppendEntriesResponse RaftNode::handleAppendEntries(
    const AppendEntriesRequest& req) {
    std::unique_lock<std::mutex> lock(mu_);
    if (req.term < currentTerm_) {
        return AppendEntriesResponse{currentTerm_, false, 0};
    }
    if (req.term >= currentTerm_) {
        becomeFollower(req.term);
    }
    resetElectionDeadline();
    leaderId_ = req.leaderId;

    // Log consistency check (Raft's log matching property): reject unless
    // we have an entry at prevLogIndex with term == prevLogTerm (or
    // prevLogIndex == 0, meaning "start of log").
    if (req.prevLogIndex > 0) {
        if (req.prevLogIndex > lastLogIndex() ||
            termAt(req.prevLogIndex) != req.prevLogTerm) {
            return AppendEntriesResponse{currentTerm_, false, 0};
        }
    }

    // Append new entries, truncating any conflicting suffix first.
    uint32_t index = req.prevLogIndex;
    for (const auto& entry : req.entries) {
        index++;
        if (index <= lastIncludedIndex_) continue;  // already snapshotted
        if (index <= lastLogIndex()) {
            if (termAt(index) == entry.term) continue;  // already matches
            log_.resize(index - 1 - lastIncludedIndex_);  // truncate suffix
            if (storage_) storage_->rewriteLog(log_, lastIncludedIndex_);
        }
        log_.push_back(entry);
        if (storage_) storage_->appendEntry(index, entry);
    }

    if (req.leaderCommit > commitIndex_) {
        commitIndex_ = std::min(req.leaderCommit, lastLogIndex());
        applyCommitted();
    }

    return AppendEntriesResponse{currentTerm_, true, lastLogIndex()};
}

ClientResponseMsg RaftNode::handleClientRequest(const ClientRequestMsg& req) {
    std::lock_guard<std::mutex> lock(mu_);
    if (role_ != Role::Leader) {
        return ClientResponseMsg{false, leaderId_.value_or(0)};
    }
    log_.push_back(LogEntry{currentTerm_, req.command});
    if (storage_) storage_->appendEntry(lastLogIndex(), log_.back());
    matchIndex_[id_] = lastLogIndex();
    return ClientResponseMsg{true, id_};
}

void RaftNode::becomeFollower(uint32_t newTerm) {
    if (newTerm > currentTerm_) {
        currentTerm_ = newTerm;
        votedFor_.reset();
        persistMeta();
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
        persistMeta();
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
        leaderId_ = id_;
        // Reinitialize leader-only volatile state (Raft §5.3): assume
        // every peer's log matches ours until proven otherwise.
        nextIndex_.clear();
        matchIndex_.clear();
        for (const auto& peer : peers_) {
            nextIndex_[peer.id] = lastLogIndex() + 1;
            matchIndex_[peer.id] = 0;
        }
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
        uint32_t leaderCommit;
        // Per-peer request built under the lock (nextIndex_ may have
        // changed since the last round), sent without holding it.
        std::vector<AppendEntriesRequest> reqs;
        reqs.reserve(peers_.size());
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (role_ != Role::Leader) return;
            term = currentTerm_;
            leaderCommit = commitIndex_;
            for (const auto& peer : peers_) {
                uint32_t next = nextIndex_[peer.id];
                uint32_t prevIndex = next > 0 ? next - 1 : 0;
                uint32_t prevTerm = termAt(prevIndex);
                size_t offset = prevIndex > lastIncludedIndex_
                                    ? prevIndex - lastIncludedIndex_
                                    : 0;
                std::vector<LogEntry> entries(
                    log_.begin() + std::min(offset, log_.size()),
                    log_.end());
                reqs.push_back(AppendEntriesRequest{
                    term, id_, prevIndex, prevTerm, leaderCommit,
                    std::move(entries)});
            }
        }

        // Fan out AppendEntries to all peers concurrently so one
        // slow/unreachable follower can't delay replication/heartbeats to
        // the rest, which could otherwise cause spurious elections.
        std::vector<std::optional<AppendEntriesResponse>> responses(
            peers_.size());
        {
            std::vector<std::thread> workers;
            workers.reserve(peers_.size());
            for (size_t i = 0; i < peers_.size(); ++i) {
                const auto& peer = peers_[i];
                workers.emplace_back([&, i]() {
                    responses[i] = sendAppendEntries(peer.host, peer.port,
                                                       reqs[i], kRpcTimeoutMs);
                });
            }
            for (auto& t : workers) t.join();
        }

        {
            std::unique_lock<std::mutex> lock(mu_);
            if (role_ != Role::Leader || currentTerm_ != term) return;
            for (size_t i = 0; i < peers_.size(); ++i) {
                const auto& resp = responses[i];
                if (!resp.has_value()) continue;
                if (resp->term > currentTerm_) {
                    becomeFollower(resp->term);
                    return;
                }
                uint32_t peerId = peers_[i].id;
                if (resp->success) {
                    nextIndex_[peerId] = resp->matchIndex + 1;
                    matchIndex_[peerId] = resp->matchIndex;
                } else {
                    // Log inconsistency: back off nextIndex_ and retry with
                    // an earlier prevLogIndex next round.
                    if (nextIndex_[peerId] > 1) nextIndex_[peerId]--;
                }
            }
            advanceCommitIndex();
        }
        std::this_thread::sleep_for(
            std::chrono::milliseconds(kHeartbeatIntervalMs));
    }
}

}  // namespace raft
