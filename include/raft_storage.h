// Persistent storage for RaftNode: an append-only write-ahead log (WAL)
// for log entries, a small metadata file for currentTerm/votedFor, and a
// periodic snapshot file that lets the WAL be compacted.
//
// Layout inside `dataDir`:
//   meta.dat      currentTerm(4) + hasVotedFor(1) + votedFor(4)
//   snapshot.dat  lastIncludedIndex(4) + lastIncludedTerm(4)   (may not exist)
//   log.dat       sequence of records: term(4) + cmdLen(4) + cmd(bytes),
//                 one per log entry with absolute index
//                 lastIncludedIndex+1, lastIncludedIndex+2, ...
//
// All writes that must be durable before an RPC response is sent
// (Raft §5.1/Figure 2) go through fopen+fwrite+fflush+fsync, and file
// replacement (meta/snapshot) is done via write-to-temp + atomic rename
// so a crash mid-write never corrupts the previous durable copy.
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "raft_rpc.h"

namespace raft {

class Storage {
public:
    struct LoadedState {
        uint32_t currentTerm = 0;
        std::optional<uint32_t> votedFor;
        uint32_t lastIncludedIndex = 0;
        uint32_t lastIncludedTerm = 0;
        std::vector<LogEntry> log;  // entries after lastIncludedIndex
    };

    explicit Storage(std::string dataDir);

    // Creates dataDir if missing and reads back whatever state was
    // previously persisted (all-default LoadedState if this is a brand
    // new node). Call once at startup before serving any RPCs.
    LoadedState load();

    // Durably persists currentTerm/votedFor. Caller must do this before
    // responding to any RPC that changed either value.
    void saveMeta(uint32_t currentTerm, std::optional<uint32_t> votedFor);

    // Durably appends one entry at absolute log index `index` to the WAL.
    void appendEntry(uint32_t index, const LogEntry& entry);

    // Rewrites the WAL to contain exactly `log` (entries immediately
    // after `lastIncludedIndex`). Used when a follower truncates a
    // conflicting suffix, and as part of snapshotting.
    void rewriteLog(const std::vector<LogEntry>& log,
                     uint32_t lastIncludedIndex);

    // Durably writes a new snapshot boundary and compacts the WAL down to
    // just the entries after lastIncludedIndex.
    void writeSnapshot(uint32_t lastIncludedIndex, uint32_t lastIncludedTerm,
                        const std::vector<LogEntry>& remainingLog);

private:
    std::string dataDir_;
    std::string metaPath_;
    std::string snapshotPath_;
    std::string logPath_;
};

}  // namespace raft
