#!/usr/bin/env bash
# Launches a local 5-node Raft cluster (leader election only) using
# cluster.conf. Logs for each node go to logs/node<id>.log.
# Ctrl-C stops all nodes.
set -e
cd "$(dirname "$0")"

BIN=./build/raft_node
CONF=./cluster.conf
mkdir -p logs

if [ ! -x "$BIN" ]; then
    echo "Binary not found at $BIN. Build first:"
    echo "  cmake -S . -B build && cmake --build build"
    exit 1
fi

pids=()
cleanup() {
    echo "stopping cluster..."
    for pid in "${pids[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
}
trap cleanup EXIT INT TERM

while read -r id host port; do
    [[ "$id" =~ ^#.*$ || -z "$id" ]] && continue
    "$BIN" "$id" "$CONF" > "logs/node${id}.log" 2>&1 &
    pids+=("$!")
    echo "started node $id (pid $!) -> logs/node${id}.log"
done < "$CONF"

echo "cluster running. tail -f logs/*.log to watch elections. Ctrl-C to stop."
wait
