# Raft Leader Election + Log Replication (C++)

A minimal, from-scratch implementation of **leader election and log
replication** from the [Raft consensus algorithm](https://raft.github.io/raft.pdf)
(Ongaro & Ousterhout, "In Search of an Understandable Consensus Algorithm").

Snapshots and cluster membership changes are **out of scope** for this
project. It covers `RequestVote`, `AppendEntries` (heartbeats + log
replication), client command submission, and the
Follower → Candidate → Leader state machine.

Nodes communicate over a **custom, minimal binary RPC protocol** built
directly on raw TCP sockets (no gRPC/HTTP/serialization libraries).

## How it works

**Leader election:**
- Each node is a separate OS process listening on its own TCP port.
- Time is divided into **terms**. Every node starts as a **Follower**.
- If a follower doesn't hear a heartbeat within a randomized election
  timeout (1.5–3s), it becomes a **Candidate**, increments its term, votes
  for itself, and sends `RequestVote` RPCs to all peers.
- A candidate that receives votes from a majority becomes **Leader** and
  starts sending periodic `AppendEntries` RPCs (every 500ms) to maintain
  authority and reset followers' election timers.
- Any node that sees a higher term in an RPC steps down to Follower.

**Log replication:**
- A client submits a command to any node via `ClientRequest`. Only the
  leader accepts it; other nodes reject with a `leaderId` hint so the
  client can retry against the actual leader.
- The leader appends the command to its own log, then replicates it to
  followers via `AppendEntries`, which also carries `prevLogIndex`/
  `prevLogTerm` for the **log matching property** consistency check.
- If a follower's log conflicts with the leader's at some index, it
  truncates the conflicting suffix and adopts the leader's entries.
- The leader tracks `nextIndex`/`matchIndex` per peer, decrementing
  `nextIndex` and retrying on a failed consistency check.
- Once an entry is replicated to a majority (and is from the leader's
  current term), the leader advances `commitIndex` and applies the entry;
  `leaderCommit` propagates this to followers, which apply it locally too.

**Persistence (write-ahead log + periodic snapshot):**
- Each node writes `currentTerm`/`votedFor` (`meta.dat`) and every log
  entry (append-only `log.dat`) to its own data directory, and reloads
  them on startup — this is what lets a restarted node avoid violating
  Raft's safety rules (e.g. voting twice in a term it already voted in,
  or forgetting a log entry it had acknowledged).
- Per the paper (§5.1, Figure 2), these writes happen **before**
  responding to the RPC that caused them: a vote grant persists
  `votedFor` first, an `AppendEntries` append persists each new entry
  first, and a term bump persists `currentTerm` first.
- Once `lastApplied` has advanced by `snapshotThreshold` entries past the
  last snapshot, the node writes a snapshot boundary (`snapshot.dat`) and
  compacts `log.dat`/the in-memory log down to just the entries after it,
  bounding on-disk and in-memory log growth.

## Project layout

```
include/raft_rpc.h    RPC message types, server, client (declarations)
src/raft_rpc.cpp       TCP socket-based RPC implementation
include/raft_node.h    RaftNode state machine (declarations)
src/raft_node.cpp      Election timer, voting, log replication, commit/apply logic
include/raft_storage.h WAL + metadata + snapshot persistence (declarations)
src/raft_storage.cpp   File-based WAL/metadata/snapshot implementation
src/main.cpp           CLI entry point: reads cluster config, runs a node
src/raft_client.cpp    CLI to submit a command to the cluster (follows leader redirects)
tests/                 Unit + integration tests (see below)
cluster.conf           Sample 5-node local cluster config
run_cluster.sh         Launches all nodes from cluster.conf as background processes
```

## Wire protocol

Each RPC is a single short-lived TCP connection. The client connects,
writes a request, reads a response, and closes the connection. All
multi-byte integers are sent in network byte order; strings and entry
lists are framed as `[uint32 length][bytes]`.

| Message                 | Bytes                                                                  |
|--------------------------|-------------------------------------------------------------------------|
| `RequestVoteRequest`     | `type(1) + term(4) + candidateId(4)`                                    |
| `RequestVoteResponse`    | `type(1) + term(4) + voteGranted(1)`                                    |
| `AppendEntriesRequest`   | `type(1) + term(4) + leaderId(4) + prevLogIndex(4) + prevLogTerm(4) + leaderCommit(4) + entries` |
| `AppendEntriesResponse`  | `type(1) + term(4) + success(1) + matchIndex(4)`                        |
| `ClientRequest`          | `type(1) + command(string)`                                             |
| `ClientResponse`         | `type(1) + success(1) + leaderId(4)`                                    |

Each `LogEntry` (used in `entries` above) is `term(4) + command(string)`.

## Building

Requires CMake >= 3.10 and a C++17 compiler. POSIX sockets are used, so this
builds on macOS/Linux.

```bash
cmake -S . -B build
cmake --build build
```

This produces:
- `build/raft_node` — the node binary
- `build/raft_client` — CLI to submit a command to the cluster
- `build/test_rpc`, `build/test_raft_node`, `build/test_election` — test binaries

## Running a local cluster

`cluster.conf` defines a 5-node cluster (`id host port` per line). Start it:

```bash
./run_cluster.sh
```

This launches all 5 nodes as background processes and writes each node's
log to `logs/node<id>.log`. Watch the election happen:

```bash
tail -f logs/*.log
```

Press `Ctrl-C` to stop the cluster. To run a single node manually:

```bash
./build/raft_node <id> cluster.conf
```

To test failover, kill the current leader's process (its id/pid is printed
by `run_cluster.sh`) and watch the remaining nodes elect a new leader in the
logs.

## Submitting client commands

Once a cluster is running, submit a command with `raft_client`:

```bash
./build/raft_client cluster.conf set x=42
```

The client tries nodes from `cluster.conf` in order; a non-leader node
replies with the current leader's id, and the client automatically
retries against it. Applied commands are logged by each node as:

```
[applied] index=1 command="set x=42"
```

## Running the tests

Tests use [CTest](https://cmake.org/cmake/help/latest/manual/ctest.1.html)
with a tiny header-only assertion helper (`tests/test_util.h`) — no external
test framework dependency.

```bash
cmake --build build
cd build && ctest --output-on-failure
```

Test suites:
- **`test_rpc`** — exercises the RPC layer directly over real loopback TCP
  sockets (`RequestVote`, `AppendEntries`, and the "nothing listening"
  error case).
- **`test_raft_node`** — unit tests for `RaftNode`'s vote-granting and
  heartbeat-handling decision logic, called directly without starting a
  server or touching the network.
- **`test_election`** — integration test that spins up a real 3-node
  cluster in-process (real TCP loopback), asserts exactly one leader is
  elected and the other two converge to Follower in the same term, then
  submits client commands to the leader and verifies every node
  replicates and applies them at matching log indices.
- **`test_persistence`** — destroys and recreates `RaftNode`s against the
  same data directory (simulating a crash/restart) and checks that
  `currentTerm`/`votedFor`/the log survive, that a conflicting-suffix
  truncation is reflected on disk, and that snapshotting compacts the log
  once the threshold is crossed.

## Known limitations

- No InstallSnapshot RPC: a follower that falls behind past a leader's
  snapshot boundary can't currently be caught up automatically.
- No cluster membership changes.
- RPC has no authentication/encryption — intended for local/trusted
  networks only.
