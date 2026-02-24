# market — P2P Sealed-Bid Auction Marketplace

A peer-to-peer sealed-bid auction marketplace built on [Veilid](https://veilid.com/) with winner determination via [MP-SPDZ](https://github.com/data61/MP-SPDZ) MASCOT multi-party computation. Bids are committed via SHA256, resolved privately by MPC (only the seller learns the winner), and listing content is AES-256-GCM encrypted until post-MPC verification completes.

## Build

```bash
cargo build                          # Debug build
cargo build --release                # Release build
cargo clippy -- -D warnings          # Lint (warnings = errors)
cargo fmt --check                    # Format check
```

## Test

```bash
cargo test                           # Unit + integration tests (mock-based, <5s)
cargo test -- --ignored              # E2E tests (requires devnet + LD_PRELOAD)
```

E2E tests run against a 20-node Docker devnet. Use the top-level Makefile for full setup:

```bash
make devnet-up                       # Start 20-node devnet
make test-e2e                        # Run E2E smoke tests
```

Or with [cargo-nextest](https://nexte.st/):

```bash
cargo integration                    # Runs the `e2e` nextest profile (3 tests, sequential)
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Dioxus Desktop UI                       │
│                   (src/app/, src/main.rs)                    │
├─────────────────────────────────────────────────────────────┤
│              AuctionCoordinator (real Veilid)                │
│           src/veilid/auction_coordinator.rs                  │
│     ┌──────────────┬──────────────┬─────────────┐           │
│     │  DHT Ops     │  MPC Orch.   │  Registry   │           │
│     │  (dht.rs)    │  (mpc_orch.) │  (registry) │           │
│     └──────────────┴──────────────┴─────────────┘           │
├─────────────────────────────────────────────────────────────┤
│     AuctionLogic<D, T, M, C>  (generic, testable)           │
│           src/veilid/auction_logic.rs                        │
│     Parameterized by: DhtStore, TimeProvider,                │
│                        MessageTransport, RandomSource        │
├──────────────────────┬──────────────────────────────────────┤
│   MPC Tunnel Proxy   │       Veilid P2P Network             │
│   (mpc.rs)           │   DHT storage, private routes,       │
│   TCP ↔ app_call     │   broadcast messaging                │
│   Pipeline depth: 8  │                                      │
└──────────────────────┴──────────────────────────────────────┘
```

The two-tier pattern separates testable auction logic (runs in <1s with mocks) from the real Veilid network layer (minutes with devnet). `AuctionLogic` is parameterized by trait bounds; `AuctionCoordinator` instantiates it with real implementations.

## Module Map

| Directory        | Purpose                                                    |
|------------------|------------------------------------------------------------|
| `src/veilid/`    | Veilid integration: coordinator, MPC tunnel, routes, DHT   |
| `src/marketplace/` | Domain types: listings, bids, bid records                |
| `src/traits/`    | Dependency injection traits (DhtStore, Transport, etc.)    |
| `src/mocks/`     | Mock implementations for fast deterministic testing        |
| `src/crypto/`    | AES-256-GCM content encryption                            |
| `src/app/`       | Dioxus desktop UI components and state management          |
| `src/config.rs`  | Configuration constants and environment variable parsing   |
| `src/error.rs`   | `MarketError` enum and `MarketResult` type alias           |
| `tests/`         | Integration tests (mock-based)                             |
| `tests/e2e/`     | End-to-end tests (Docker devnet, `#[ignore]`)              |

## Demo

See [DEMO_SETUP.md](DEMO_SETUP.md) for full instructions on running a 3-node demo cluster.

Quick start (from repo root):

```bash
make demo    # Build everything, start devnet, launch 3 market nodes
```
