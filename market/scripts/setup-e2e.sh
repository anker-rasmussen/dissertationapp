#!/bin/bash
# Nextest setup script for E2E tests
# Sets LD_PRELOAD for IP spoofing required by devnet

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MARKET_DIR="$(dirname "$SCRIPT_DIR")"
VEILID_DIR="$(dirname "$(dirname "$MARKET_DIR")")/veilid"
LIBIPSPOOF="$VEILID_DIR/.devcontainer/scripts/libipspoof.so"

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
