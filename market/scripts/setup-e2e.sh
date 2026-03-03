#!/bin/bash
# Nextest setup script for E2E tests
# Manages devnet lifecycle and sets LD_PRELOAD for IP spoofing

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MARKET_DIR="$(dirname "$SCRIPT_DIR")"
VEILID_DIR="$(dirname "$(dirname "$MARKET_DIR")")/veilid"
LIBIPSPOOF="$VEILID_DIR/.devcontainer/scripts/libipspoof.so"
COMPOSE_FILE="$VEILID_DIR/.devcontainer/compose/docker-compose.dev.yml"

if [ ! -f "$LIBIPSPOOF" ]; then
    echo "[setup-e2e] Error: libipspoof.so not found at $LIBIPSPOOF" >&2
    echo "[setup-e2e] Please build it with: cd $VEILID_DIR/.devcontainer/scripts && make libipspoof.so" >&2
    exit 1
fi

# Exit if NEXTEST_ENV isn't defined (required by nextest)
if [ -z "${NEXTEST_ENV:-}" ]; then
    echo "[setup-e2e] NEXTEST_ENV not set, skipping env export" >&2
    exit 0
fi

# Write LD_PRELOAD to NEXTEST_ENV so tests receive it
echo "LD_PRELOAD=$LIBIPSPOOF" >> "$NEXTEST_ENV"
echo "[setup-e2e] Exported LD_PRELOAD=$LIBIPSPOOF"

# MASCOT (mascot-party.x) is the default MPC protocol — dishonest majority,
# malicious security.  Override via MPC_PROTOCOL env var if needed.
echo "[setup-e2e] Using MPC protocol: ${MPC_PROTOCOL:-mascot-party.x (default)}"

# ── Devnet management ──────────────────────────────────────────────
# Start a fresh devnet once per nextest invocation.  Individual tests
# reuse this devnet instead of tearing down/rebuilding each time.

if [ "${E2E_FAST_MODE:-}" = "1" ]; then
    echo "[setup-e2e] Fast mode: skipping devnet management"
    exit 0
fi

if [ ! -f "$COMPOSE_FILE" ]; then
    echo "[setup-e2e] Warning: docker-compose file not found at $COMPOSE_FILE, skipping devnet" >&2
    exit 0
fi

# Kill orphaned processes from previous runs
pkill -f market-headless 2>/dev/null || true
pkill -f mascot-party.x 2>/dev/null || true

echo "[setup-e2e] Stopping old devnet (if any)..."
docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true

echo "[setup-e2e] Starting fresh devnet..."
docker compose -f "$COMPOSE_FILE" up -d 2>/dev/null

echo "[setup-e2e] Waiting for devnet health..."
for i in $(seq 1 60); do
    healthy=$(docker compose -f "$COMPOSE_FILE" ps --format "{{.Health}}" 2>/dev/null | grep -c healthy || true)
    if [ "$healthy" -ge 20 ]; then
        echo "[setup-e2e] Devnet ready: $healthy/20 nodes healthy"
        # Tell tests not to restart the devnet
        echo "E2E_DEVNET_MANAGED=1" >> "$NEXTEST_ENV"
        exit 0
    fi
    sleep 1
done

echo "[setup-e2e] Warning: devnet not fully healthy after 60s, proceeding anyway" >&2
echo "E2E_DEVNET_MANAGED=1" >> "$NEXTEST_ENV"
