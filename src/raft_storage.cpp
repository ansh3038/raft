#include "raft_storage.h"

#include <sys/stat.h>
#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <functional>

namespace raft {

namespace {

bool fileExists(const std::string& path) {
    struct stat st{};
    return ::stat(path.c_str(), &st) == 0;
}

void writeU32(FILE* f, uint32_t v) { std::fwrite(&v, sizeof(v), 1, f); }

bool readU32(FILE* f, uint32_t* out) {
    return std::fread(out, sizeof(*out), 1, f) == 1;
}

void writeString(FILE* f, const std::string& s) {
    writeU32(f, static_cast<uint32_t>(s.size()));
    if (!s.empty()) std::fwrite(s.data(), 1, s.size(), f);
}

bool readString(FILE* f, std::string* out) {
    uint32_t len;
    if (!readU32(f, &len)) return false;
    out->resize(len);
    if (len == 0) return true;
    return std::fread(out->data(), 1, len, f) == len;
}

// Writes `writer(f)` to `path`.tmp, fsyncs it, then atomically renames it
// over `path`. Guarantees `path` is never left partially written.
void atomicReplace(const std::string& path,
                    const std::function<void(FILE*)>& writer) {
    std::string tmp = path + ".tmp";
    FILE* f = std::fopen(tmp.c_str(), "wb");
    if (!f) return;
    writer(f);
    std::fflush(f);
    ::fsync(fileno(f));
    std::fclose(f);
    std::rename(tmp.c_str(), path.c_str());
}

}  // namespace

Storage::Storage(std::string dataDir) : dataDir_(std::move(dataDir)) {
    ::mkdir(dataDir_.c_str(), 0755);  // ignore EEXIST
    metaPath_ = dataDir_ + "/meta.dat";
    snapshotPath_ = dataDir_ + "/snapshot.dat";
    logPath_ = dataDir_ + "/log.dat";
}

Storage::LoadedState Storage::load() {
    LoadedState state;

    if (fileExists(metaPath_)) {
        FILE* f = std::fopen(metaPath_.c_str(), "rb");
        if (f) {
            uint8_t hasVoted = 0;
            uint32_t voted = 0;
            if (readU32(f, &state.currentTerm) &&
                std::fread(&hasVoted, 1, 1, f) == 1 && readU32(f, &voted)) {
                if (hasVoted) state.votedFor = voted;
            }
            std::fclose(f);
        }
    }

    if (fileExists(snapshotPath_)) {
        FILE* f = std::fopen(snapshotPath_.c_str(), "rb");
        if (f) {
            readU32(f, &state.lastIncludedIndex);
            readU32(f, &state.lastIncludedTerm);
            std::fclose(f);
        }
    }

    if (fileExists(logPath_)) {
        FILE* f = std::fopen(logPath_.c_str(), "rb");
        if (f) {
            LogEntry entry;
            while (readU32(f, &entry.term) && readString(f, &entry.command)) {
                state.log.push_back(entry);
            }
            std::fclose(f);
        }
    }

    return state;
}

void Storage::saveMeta(uint32_t currentTerm, std::optional<uint32_t> votedFor) {
    atomicReplace(metaPath_, [&](FILE* f) {
        writeU32(f, currentTerm);
        uint8_t hasVoted = votedFor.has_value() ? 1 : 0;
        std::fwrite(&hasVoted, 1, 1, f);
        writeU32(f, votedFor.value_or(0));
    });
}

void Storage::appendEntry(uint32_t /*index*/, const LogEntry& entry) {
    FILE* f = std::fopen(logPath_.c_str(), "ab");
    if (!f) return;
    writeU32(f, entry.term);
    writeString(f, entry.command);
    std::fflush(f);
    ::fsync(fileno(f));
    std::fclose(f);
}

void Storage::rewriteLog(const std::vector<LogEntry>& log,
                          uint32_t /*lastIncludedIndex*/) {
    atomicReplace(logPath_, [&](FILE* f) {
        for (const auto& entry : log) {
            writeU32(f, entry.term);
            writeString(f, entry.command);
        }
    });
}

void Storage::writeSnapshot(uint32_t lastIncludedIndex,
                             uint32_t lastIncludedTerm,
                             const std::vector<LogEntry>& remainingLog) {
    atomicReplace(snapshotPath_, [&](FILE* f) {
        writeU32(f, lastIncludedIndex);
        writeU32(f, lastIncludedTerm);
    });
    rewriteLog(remainingLog, lastIncludedIndex);
}

}  // namespace raft
