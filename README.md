# dissertationapp

Veilid-based P2P sealed-bid auction marketplace. The actual code lives in [`market/`](market/).

## Structure

```
market/           # Rust crate — the auction app
├── src/          # Library + binaries (market, market-headless, devnet-ctl)
└── tests/        # Integration + E2E tests
```

## Quick Build

```bash
cd market
cargo build
cargo test            # ~227 tests, all mock-based, finishes in under a second
cargo clippy -- -D warnings
```

See [`market/README.md`](market/README.md) for architecture details and the full command reference.
