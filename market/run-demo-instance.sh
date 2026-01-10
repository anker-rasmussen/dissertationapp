#!/bin/bash
# Helper script to run a market app instance for demo
# Usage: ./run-demo-instance.sh <node_offset>
# Example: ./run-demo-instance.sh 5   (runs on port 5165, IP 1.2.3.6)

set -e

if [ $# -ne 1 ]; then
    echo "Usage: $0 <node_offset>"
    echo "  node_offset 5 -> port 5165, IP 1.2.3.6 (Bidder 1)"
    echo "  node_offset 6 -> port 5166, IP 1.2.3.7 (Bidder 2)"
    echo "  node_offset 7 -> port 5167, IP 1.2.3.8 (Auctioneer)"
    exit 1
fi

NODE_OFFSET=$1
PORT=$((5160 + NODE_OFFSET))
IP_SUFFIX=$((NODE_OFFSET + 1))

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
export LD_PRELOAD=/home/broadcom/Repos/Dissertation/Repos/veilid/.devcontainer/scripts/libipspoof.so
export RUST_LOG=info,veilid_core=info

# Run the market app
cargo run
