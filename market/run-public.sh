#!/bin/bash
# Run market app connected to the PUBLIC Veilid network (internet)
# This connects to real bootstrap nodes on the public internet
# No LD_PRELOAD needed - uses real network interfaces

set -e

echo "========================================="
echo "Starting Market App - PUBLIC NETWORK"
echo "========================================="
echo "This will connect to the public Veilid network"
echo "WARNING: Your node will be discoverable on the internet"
echo "Data Dir: ~/.local/share/smpc-auction-public"
echo "========================================="
echo ""

# Set environment for public network mode
export MARKET_MODE=public
export RUST_LOG=info,veilid_core=info

# No LD_PRELOAD for public network
cargo run
