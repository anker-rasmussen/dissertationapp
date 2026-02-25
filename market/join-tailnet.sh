#!/bin/bash
# join-tailnet.sh — Launch a market node that connects to a remote devnet over Tailscale.
#
# Usage:
#   ./join-tailnet.sh <server-tailscale-ip> [offset]
#
# The server must be running `make devnet-up-tailnet` (or `make demo-tailnet`).
# This script requires:
#   - Tailscale running on both hosts
#   - libipspoof.so built (run `make build-ipspoof` on the server, then copy it)
#
# How it works:
#   Veilid classifies Tailscale IPs (100.64.0.0/10 CGNAT) as non-global, so nodes
#   can't get valid public dial info with real Tailscale IPs. Instead, we keep the
#   fake 1.2.3.X IP scheme and use LD_PRELOAD to translate:
#     - Outgoing: 1.2.3.X → <server-tailscale-ip> (via IPSPOOF_TARGET_IP)
#     - Incoming: <server-tailscale-ip> → 1.2.3.X (automatic)
#   This lets Veilid think it's talking to global IPs while traffic routes over tailnet.
#
# Arguments:
#   server-tailscale-ip  Tailscale IPv4 of the host running the devnet
#   offset               Node offset (default: 25). Must not collide with
#                        devnet nodes (0-19) or server market nodes (20-22).
#
# Example:
#   ./join-tailnet.sh 100.64.0.1        # offset 25, port 5185
#   ./join-tailnet.sh 100.64.0.1 30     # offset 30, port 5190

set -euo pipefail

SERVER_IP="${1:?Usage: $0 <server-tailscale-ip> [offset]}"
OFFSET="${2:-25}"

# Find libipspoof.so
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
IPSPOOF_SO="${SCRIPT_DIR}/../../veilid/.devcontainer/scripts/libipspoof.so"
if [ ! -f "$IPSPOOF_SO" ]; then
    echo "ERROR: libipspoof.so not found at $IPSPOOF_SO"
    echo "Build it with: make build-ipspoof"
    exit 1
fi

PORT=$((5160 + OFFSET))

echo "Joining devnet via Tailscale (LD_PRELOAD mode)"
echo "  Server:         ${SERVER_IP}:5160 (bootstrap)"
echo "  Node offset:    ${OFFSET} (port ${PORT})"
echo "  IP translation: 1.2.3.X → ${SERVER_IP}"
echo ""

exec env \
    LD_PRELOAD="${IPSPOOF_SO}" \
    IPSPOOF_TARGET_IP="${SERVER_IP}" \
    VEILID_BASE_PORT=5160 \
    MARKET_NODE_OFFSET="${OFFSET}" \
    MARKET_LISTEN_ADDR="0.0.0.0" \
    MARKET_NETWORK_KEY="development-network-2025" \
    MARKET_INSECURE_STORAGE=true \
    RUST_LOG=info,veilid_core=info \
    cargo run --release
