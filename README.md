# Raft Leader Election (C++)

A minimal, from-scratch implementation of the **leader election** portion of
the [Raft consensus algorithm](https://raft.github.io/raft.pdf)
(Ongaro & Ousterhout, "In Search of an Understandable Consensus Algorithm").

Log replication, snapshots, and membership changes are **out of scope** for
this project — it focuses purely on `RequestVote` / heartbeat `AppendEntries`
and the Follower → Candidate → Leader state machine.

Nodes communicate over a **custom, minimal binary RPC protocol** built
directly on raw TCP sockets (no gRPC/HTTP/serialization libraries).

## How it works

- Each node is a separate OS process listening on its own TCP port.
- Time is divided into **terms**. Every node starts as a **Follower**.
- If a follower doesn't hear a heartbeat within a randomized election
  timeout (1.5–3s), it becomes a **Candidate**, increments its term, votes
  for itself, and sends `RequestVote` RPCs to all peers.
- A candidate that receives votes from a majority becomes **Leader** and
  starts sending periodic heartbeat `AppendEntries` RPCs (every 500ms) to
  maintain authority and reset followers' election timers.
- Any node that sees a higher term in an RPC steps down to Follower.

## Project layout

```
include/raft_rpc.h    RPC message types, server, client (declarations)
src/raft_rpc.cpp       TCP socket-based RPC implementation
include/raft_node.h    RaftNode state machine (declarations)
src/raft_node.cpp      Election timer, voting, heartbeat logic
src/main.cpp           CLI entry point: reads cluster config, runs a node
tests/                 Unit + integration tests (see below)
cluster.conf           Sample 5-node local cluster config
run_cluster.sh         Launches all nodes from cluster.conf as background processes
```

## Wire protocol

Each RPC is a single short-lived TCP connection. The client connects,
writes a request, reads a response, and closes the connection. All
multi-byte integers are sent in network byte order.

| Message                 | Bytes                                      |
|--------------------------|---------------------------------------------|
| `RequestVoteRequest`     | `type(1) + term(4) + candidateId(4)`        |
| `RequestVoteResponse`    | `type(1) + term(4) + voteGranted(1)`        |
| `AppendEntriesRequest`   | `type(1) + term(4) + leaderId(4)`           |
| `AppendEntriesResponse`  | `type(1) + term(4) + success(1)`            |

## Building

Requires CMake >= 3.10 and a C++17 compiler. POSIX sockets are used, so this
builds on macOS/Linux.

```bash
cmake -S . -B build
cmake --build build
```

This produces:
- `build/raft_node` — the node binary
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
  cluster in-process (real TCP loopback) and asserts exactly one leader is
  elected and the other two converge to Follower in the same term.

## Known limitations

- No persistent storage: `currentTerm`/`votedFor` are in memory only, so a
  restarted node forgets its term (fine for a leader-election demo, not
  safe for production use).
- No log replication, snapshotting, or cluster membership changes.
- RPC has no authentication/encryption — intended for local/trusted
  networks only.
