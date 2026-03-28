#!/bin/bash
# Nextest setup script for E2E tests
# Starts veilid-playground devnet once, sets LD_PRELOAD for IP spoofing

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MARKET_DIR="$(dirname "$SCRIPT_DIR")"
VEILID_DIR="${VEILID_REPO_PATH:-$(dirname "$(dirname "$MARKET_DIR")")/veilid}"

# Platform-specific library extension and preload variable
if [ "$(uname -s)" = "Darwin" ]; then
    IPSPOOF_LIB="libveilid_ipspoof.dylib"
    PRELOAD_VAR="DYLD_INSERT_LIBRARIES"
else
    IPSPOOF_LIB="libveilid_ipspoof.so"
    PRELOAD_VAR="LD_PRELOAD"
fi

# Find libipspoof — prefer cargo-built, fall back to legacy location
LIBIPSPOOF=""
if [ -f "$VEILID_DIR/target/release/$IPSPOOF_LIB" ]; then
    LIBIPSPOOF="$VEILID_DIR/target/release/$IPSPOOF_LIB"
elif [ -f "$VEILID_DIR/.devcontainer/scripts/libipspoof.so" ]; then
    LIBIPSPOOF="$VEILID_DIR/.devcontainer/scripts/libipspoof.so"
else
    echo "[setup-e2e] Error: ipspoof library not found in $VEILID_DIR" >&2
    echo "[setup-e2e] Build with: cd $VEILID_DIR && cargo build --release -p veilid-ipspoof" >&2
    exit 1
fi

# Exit if NEXTEST_ENV isn't defined (required by nextest)
if [ -z "${NEXTEST_ENV:-}" ]; then
    echo "[setup-e2e] NEXTEST_ENV not set, skipping env export" >&2
    exit 0
fi

# Write preload var to NEXTEST_ENV so tests receive it
echo "$PRELOAD_VAR=$LIBIPSPOOF" >> "$NEXTEST_ENV"
echo "[setup-e2e] Exported $PRELOAD_VAR=$LIBIPSPOOF"

# MASCOT (mascot-party.x) is the default MPC protocol — dishonest majority,
# malicious security.  Override via MPC_PROTOCOL env var if needed.
echo "[setup-e2e] Using MPC protocol: ${MPC_PROTOCOL:-mascot-party.x (default)}"

# ── Devnet management ──────────────────────────────────────────────
# Start playground devnet once per nextest invocation.  All tests reuse it.

if [ "${E2E_FAST_MODE:-}" = "1" ]; then
    echo "[setup-e2e] Fast mode: skipping devnet management"
    exit 0
fi

# Kill orphaned processes from previous runs
pkill -f "veilid-playground.*start" 2>/dev/null || true
pkill -f "veilid-server.*subnode-index" 2>/dev/null || true
pkill -f market-headless 2>/dev/null || true
pkill -f mascot-party.x 2>/dev/null || true
pkill -f shamir-party.x 2>/dev/null || true
sleep 2

# Clean playground data for fresh routing tables
rm -rf /tmp/veilid-playground

# Find veilid-playground binary
PLAYGROUND=""
if [ -x "$VEILID_DIR/target/release/veilid-playground" ]; then
    PLAYGROUND="$VEILID_DIR/target/release/veilid-playground"
elif command -v veilid-playground &>/dev/null; then
    PLAYGROUND="$(command -v veilid-playground)"
fi

if [ -z "$PLAYGROUND" ]; then
    echo "[setup-e2e] Warning: veilid-playground not found, tests will manage devnet themselves" >&2
    exit 0
fi

echo "[setup-e2e] Starting devnet via $PLAYGROUND..."
$PLAYGROUND start 20 --ipspoof "$LIBIPSPOOF" >/dev/null 2>/tmp/veilid-playground-setup.log &
disown

# Wait for all 20 nodes to be reachable
echo "[setup-e2e] Waiting for devnet health..."
for i in $(seq 1 90); do
    all_up=true
    for port in $(seq 5150 5169); do
        if ! timeout 1 bash -c "echo >/dev/tcp/127.0.0.1/$port" 2>/dev/null; then
            all_up=false
            break
        fi
    done
    if [ "$all_up" = true ]; then
        echo "[setup-e2e] Devnet ready: 20/20 nodes reachable"
        exit 0
    fi
    sleep 1
done

echo "[setup-e2e] Warning: devnet not fully healthy after 90s, proceeding anyway" >&2
