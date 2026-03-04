# Demo Setup

How to run multiple market nodes for a live auction demo.

## Prerequisites

- Docker (for the 20-node Veilid devnet)
- Rust toolchain
- MP-SPDZ built (`make build-mpspdz` from repo root)
- `libipspoof.so` built (`make build-ipspoof` from repo root)

## One-Command Demo

From the repo root:

```bash
make demo
```

This handles everything: builds the binary + dependencies, starts a fresh devnet, and launches 3 market nodes with interleaved output. Skip the rest of this doc if that's all you need.

## Manual Setup

### 1. Start the devnet

```bash
make devnet-up
# Wait for "healthy" status — takes ~30s
```

### 2. Launch market nodes

Each node needs a unique `VEILID_NODE_OFFSET` (20+), `LD_PRELOAD` pointing at `libipspoof.so`, and the `MP_SPDZ_DIR` env var. Open 3 terminals:

**Terminal 1 — Bidder 1 (offset 20, port 5180)**
```bash
cd Repos/dissertationapp/market
VEILID_NODE_OFFSET=20 \
  LD_PRELOAD=../../veilid/.devcontainer/scripts/libipspoof.so \
  MP_SPDZ_DIR=../../MP-SPDZ \
  RUST_LOG=info \
  cargo run --release
```

**Terminal 2 — Bidder 2 (offset 21, port 5181)**
```bash
cd Repos/dissertationapp/market
VEILID_NODE_OFFSET=21 \
  LD_PRELOAD=../../veilid/.devcontainer/scripts/libipspoof.so \
  MP_SPDZ_DIR=../../MP-SPDZ \
  RUST_LOG=info \
  cargo run --release
```

**Terminal 3 — Auctioneer (offset 22, port 5182)**
```bash
cd Repos/dissertationapp/market
VEILID_NODE_OFFSET=22 \
  LD_PRELOAD=../../veilid/.devcontainer/scripts/libipspoof.so \
  MP_SPDZ_DIR=../../MP-SPDZ \
  RUST_LOG=info \
  cargo run --release
```

Or use the helper script if available:

```bash
./run-demo-instance.sh 20       # Single node
./run-demo-instance.sh cluster  # All 3 in one terminal
```

### 3. Verify it's working

Each node should show:
- **Status**: "Connected" (FullyAttached)
- **Peers**: Non-zero count (devnet nodes + other market instances)
- **Node ID**: Different `VLD0:` identifier per instance

The warning "This node has no valid public dial info" is expected — you're on the local devnet.

## Network Topology

```
Docker devnet (offsets 0–19):
  Bootstrap:  1.2.3.1:5160  (offset 0)
  Nodes 1–19: 1.2.3.2:5161 ... 1.2.3.20:5179

Market nodes (offsets 20+):
  Node 20:    1.2.3.21:5180  (Bidder 1)
  Node 21:    1.2.3.22:5181  (Bidder 2)
  Node 22:    1.2.3.23:5182  (Auctioneer)
  ...up to offset ~39 (libipspoof translates 40 IPs total)
```

All nodes share the network key `development-network-2025`.

The `LD_PRELOAD` shim (`libipspoof.so`) intercepts socket calls to translate fake global IPs (1.2.3.x) to localhost, so all 20+ nodes can run on a single machine.

## Troubleshooting

**Stuck on "Connecting..."** — Check that devnet is running (`docker ps`). If you just restarted the devnet, clear stale data dirs first: `make clean-data`.

**Port already in use** — Kill leftover instances: `make devnet-down` (also kills market processes).

**Zero peers after 30s** — Verify `LD_PRELOAD` is set and points to a valid `libipspoof.so`. Without it, the fake IPs don't resolve.

**Stale data after devnet restart** — The devnet generates new node identities on restart. Always clean data dirs: `rm -rf ~/.local/share/smpc-auction-node-*`

## Teardown

```bash
# Ctrl+C in each terminal, then:
make devnet-down    # Stops docker + kills market processes
make clean-data     # Optional: wipe node data dirs + docker volumes
```
