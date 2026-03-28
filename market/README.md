# market — P2P Sealed-Bid Auction Marketplace

A peer-to-peer sealed-bid auction system built on [Veilid](https://veilid.com/) with winner determination via [MP-SPDZ](https://github.com/data61/MP-SPDZ) MASCOT multi-party computation. Bids are SHA256-committed, resolved privately by MPC (only the seller learns the winner), and listing content is AES-256-GCM encrypted until post-MPC verification completes.

## Build & Test

```bash
cargo build                          # Debug build
cargo build --release                # Release build
cargo test                           # Unit + integration tests (~227 tests, <1s)
cargo clippy -- -D warnings          # Lint (warnings = errors)
cargo fmt --check                    # Format check
```

### Running E2E Tests

E2E tests need a 20-node Docker devnet and the LD_PRELOAD IP spoofing shim. From the repo root:

```bash
make devnet-up                       # Start devnet
make test-e2e                        # Run E2E smoke tests
```

Or with [cargo-nextest](https://nexte.st/):

```bash
cargo integration                    # Runs the e2e nextest profile (3 tests, sequential)
```

### Running a Single Test

```bash
cargo test test_bid_commitment                          # By name
cargo test --lib marketplace::bid::tests                # By module path
cargo test --test integration_tests test_auction_flow   # Specific integration test
cargo test -- --ignored                                 # E2E only (needs devnet)
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   Dioxus Desktop UI                      │
│                 (src/app/, src/main.rs)                   │
├─────────────────────────────────────────────────────────┤
│            AuctionCoordinator (real Veilid)               │
│         src/veilid/auction_coordinator/                   │
│     ┌──────────────┬──────────────┬─────────────┐        │
│     │  DHT Ops     │  MPC Orch.   │  Registry   │        │
│     │  (dht.rs)    │  (mpc_orch.) │  (registry) │        │
│     └──────────────┴──────────────┴─────────────┘        │
├─────────────────────────────────────────────────────────┤
│       AuctionLogic<D, C>  (generic, testable)            │
│         src/veilid/auction_logic.rs                       │
│       D = DhtStore, C = TimeProvider                     │
├──────────────────────┬──────────────────────────────────┤
│   MPC Tunnel Proxy   │       Veilid P2P Network          │
│   (mpc/)             │   DHT storage, private routes,    │
│   TCP ↔ app_call     │   broadcast messaging             │
│   Pipeline depth: 8  │                                    │
└──────────────────────┴──────────────────────────────────┘
```

The two-tier split keeps the auction state machine testable — `AuctionLogic` runs full N-party simulations in <1s with mocks, while `AuctionCoordinator` wires in real Veilid DHT, routes, and MPC.

## Module Map

| Directory          | What's in it                                            |
|--------------------|---------------------------------------------------------|
| `src/veilid/`      | Veilid integration: coordinator, MPC tunnel, routes, DHT |
| `src/marketplace/`  | Domain types: listings, bids, bid records               |
| `src/traits/`       | DI traits (`DhtStore`, `TimeProvider`, `RandomSource`)   |
| `src/mocks/`        | Mock impls for fast deterministic testing               |
| `src/crypto/`       | AES-256-GCM content encryption                         |
| `src/app/`          | Dioxus desktop UI                                      |
| `src/config.rs`     | Constants + env var parsing                             |
| `src/error.rs`      | `MarketError` enum                                     |
| `tests/`            | Integration tests (mock-based)                         |
| `tests/e2e/`        | End-to-end tests (`#[ignore]`, needs devnet)           |

## Binaries

| Binary           | Purpose                                               |
|------------------|-------------------------------------------------------|
| `market`         | Main desktop app (Dioxus UI + Veilid node)            |
| `market-headless`| Headless mode for E2E testing (no UI)                 |
| `devnet-ctl`     | Devnet node management CLI (WIP)                      |

## Demo

From the repo root:

```bash
make demo    # Build everything, start devnet, launch 3 market nodes
```

This gives you 3 nodes (2 bidders + 1 auctioneer) with interleaved terminal output.
