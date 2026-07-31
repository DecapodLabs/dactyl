<!-- decapod-release: 0.88.0 -->
<!-- decapod-fingerprint: 846eb547626a4b19779938fd48e5f33d1ad94c43948e0f2550f28dfcc5685a3a -->
# AGENTS.md

Dactyl is the governed datastore boundary for Decapod. This file describes the
agent-facing contract for working inside this repository.

## Scope

- Public surface: `init`, `active_datastore`, `read`, `write`, `query!`. No other items
  at the crate root.
- SQLite types live behind `feature = "sqlite"`. Re-export only the Adapter impl + handle.
- Neon types live behind `feature = "neon"`. The bearer token is opaque.

## Build & test

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --test conformance --features "sqlite neon"
```

## PR workflow

Open PRs against `main`. Title: `feat(dactyl): <summary>`. Reference DecapodLabs/dactyl#1.
