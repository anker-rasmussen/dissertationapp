#!/bin/bash
# Nextest setup script for fast E2E tests (persistent devnet)
# Sets LD_PRELOAD and E2E_FAST_MODE for reusing an existing devnet

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MARKET_DIR="$(dirname "$SCRIPT_DIR")"
VEILID_DIR="$(dirname "$(dirname "$MARKET_DIR")")/veilid"
LIBIPSPOOF="$VEILID_DIR/.devcontainer/scripts/libipspoof.so"

if [ ! -f "$LIBIPSPOOF" ]; then
    echo "[setup-e2e-fast] Error: libipspoof.so not found at $LIBIPSPOOF" >&2
    echo "[setup-e2e-fast] Please build it with: cd $VEILID_DIR/.devcontainer/scripts && make libipspoof.so" >&2
    exit 1
fi

# Exit if NEXTEST_ENV isn't defined (required by nextest)
if [ -z "${NEXTEST_ENV:-}" ]; then
    echo "[setup-e2e-fast] NEXTEST_ENV not set, skipping env export" >&2
    exit 0
fi

# Write environment variables to NEXTEST_ENV
echo "LD_PRELOAD=$LIBIPSPOOF" >> "$NEXTEST_ENV"
echo "E2E_FAST_MODE=1" >> "$NEXTEST_ENV"
echo "[setup-e2e-fast] Exported LD_PRELOAD=$LIBIPSPOOF"
echo "[setup-e2e-fast] Exported E2E_FAST_MODE=1 (tests will reuse existing devnet)"
