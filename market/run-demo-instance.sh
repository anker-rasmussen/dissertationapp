#!/bin/bash
# Helper script to run a market app instance for demo
# Usage: ./run-demo-instance.sh <node_offset|cluster>
# Example: ./run-demo-instance.sh 5       (runs on port 5165, IP 1.2.3.6)
# Example: ./run-demo-instance.sh cluster (runs all 3 nodes: 5, 6, 7)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

run_single_node() {
    local NODE_OFFSET=$1
    local PORT=$((5160 + NODE_OFFSET))
    local IP_SUFFIX=$((NODE_OFFSET + 1))

    echo "========================================="
    echo "Starting Market App Instance"
    echo "========================================="
    echo "Node Offset: $NODE_OFFSET"
    echo "Port: $PORT"
    echo "IP: 1.2.3.$IP_SUFFIX"
    echo "Data Dir: ~/.local/share/smpc-auction-node-$NODE_OFFSET"
    echo "========================================="
    echo ""

    export MARKET_NODE_OFFSET=$NODE_OFFSET
    VEILID_DIR="${VEILID_REPO_PATH:-$(dirname "$(dirname "$SCRIPT_DIR")")/veilid}"
    export LD_PRELOAD="$VEILID_DIR/.devcontainer/scripts/libipspoof.so"
    export RUST_LOG=info,veilid_core=info

    cargo run
}

run_cluster() {
    echo "========================================="
    echo "Starting 3-Node Demo Cluster"
    echo "========================================="
    echo "  Node 5 -> port 5165, IP 1.2.3.6 (Bidder 1)"
    echo "  Node 6 -> port 5166, IP 1.2.3.7 (Bidder 2)"
    echo "  Node 7 -> port 5167, IP 1.2.3.8 (Auctioneer)"
    echo "========================================="
    echo ""

    # Build first to avoid concurrent compilation issues
    echo "Building project..."
    cargo build
    echo ""

    # Array to track child PIDs
    PIDS=()

    # Cleanup function to kill all child processes on exit
    cleanup() {
        echo ""
        echo "Shutting down cluster..."
        for pid in "${PIDS[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                kill "$pid" 2>/dev/null
            fi
        done
        wait
        echo "Cluster stopped."
    }
    trap cleanup EXIT INT TERM

    # Start each node in its own terminal or background
    for NODE_OFFSET in 5 6 7; do
        local PORT=$((5160 + NODE_OFFSET))
        local IP_SUFFIX=$((NODE_OFFSET + 1))

        echo "Starting node $NODE_OFFSET (port $PORT, IP 1.2.3.$IP_SUFFIX)..."

        (
            export MARKET_NODE_OFFSET=$NODE_OFFSET
            VEILID_DIR="${VEILID_REPO_PATH:-$(dirname "$(dirname "$SCRIPT_DIR")")/veilid}"
            export LD_PRELOAD="$VEILID_DIR/.devcontainer/scripts/libipspoof.so"
            export RUST_LOG=info,veilid_core=info
            cd "$SCRIPT_DIR"
            cargo run 2>&1 | sed "s/^/[Node $NODE_OFFSET] /"
        ) &
        PIDS+=($!)

        # Small delay between starts to avoid port conflicts during init
        sleep 2
    done

    echo ""
    echo "All 3 nodes started. Press Ctrl+C to stop the cluster."
    echo ""

    # Wait for all background processes
    wait
}

if [ $# -ne 1 ]; then
    echo "Usage: $0 <node_offset|cluster>"
    echo "  node_offset 5 -> port 5165, IP 1.2.3.6 (Bidder 1)"
    echo "  node_offset 6 -> port 5166, IP 1.2.3.7 (Bidder 2)"
    echo "  node_offset 7 -> port 5167, IP 1.2.3.8 (Auctioneer)"
    echo "  cluster      -> start all 3 nodes at once"
    exit 1
fi

if [ "$1" = "cluster" ]; then
    run_cluster
else
    run_single_node "$1"
fi
