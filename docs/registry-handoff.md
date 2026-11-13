# Registry capability handoff

Issue 94 separates an SDK release from deployed-service qualification. The
accepted 0.16.0 source includes logical local/cloud connectors, managed Auth,
native Supabase RPC and explicit SQLite/Neon/service HTTP routes. Rust and Cargo
1.85 are required. Decapod's currently inspected master pins registry Dactyl
`=0.14.0`; its security adoption in issue 1638 is complete. Adopting later SDK
capabilities requires a separate consumer dependency and validation change.

## Observed published artifact

The official sparse registry index at `https://index.crates.io/da/ct/dactyl-db`
lists unyanked 0.16.0 with Rust 1.85. The archive downloaded from
`https://static.crates.io/crates/dactyl-db/dactyl-db-0.16.0.crate` independently
matched its index SHA-256:

```text
af724e4a561da93c4cf9bf8ca63f25c6d227bee6a49318fdbd5dbf7bc93083ca
```

The packaged `.cargo_vcs_info.json` identifies source
`7d539730f9f3218f58f244c284c529c5b19967fb`. Inspection confirmed the archive
contains `src/adapter/supabase.rs`, `src/connector.rs`, `src/cloud.rs`,
`docs/cloud-connectors.md`, `docs/supabase-rpc-v1.md`, README and Cargo metadata.
This archive inspection establishes observed availability, checksum and those
contents. The separate clean-consumer execution below establishes compilation;
neither observation establishes hosted execution. The archive and machine
observations stay outside Git.

## Executed clean registry consumer

A fresh consumer with the exact dependency below passed `cargo check` on
Rust/Cargo 1.85.0 in headless Linux, with SQLite, Neon and Supabase enabled.
It compiled the public logical connector, managed session, cloud connector and
native RPC route types without a Git/path dependency or patch. Its lock records
the crates.io registry source, the SDK checksum above and `quinn-proto 0.11.19`.
An explicit GCC 12 linker path corrected the initial environment-only failure
before the successful check; no SDK source or dependency constraint changed.

```text
exit code: 0
consumer lock SHA-256: 726f3a5862de224bf279574e3e2b2e0d837e8ff97a30fe9f06d808175c49d5ac
sanitized compiler output SHA-256: fbb457a4b32d0009b615579364d7f59d844205cd4fcd35a3321abdb4e01ee745
```

This is an all-feature registry compilation, separate from the repository's
full optional-feature test matrix. No hosted data, managed OAuth, database
credentials or production service was used.

## Consumer acceptance

Use an otherwise empty consumer with an exact registry dependency and no
`[patch]`, path or Git source:

```toml
[package]
name = "dactyl-registry-consumer"
version = "0.0.0"
edition = "2021"
rust-version = "1.85"

[dependencies]
dactyl-db = { version = "=0.16.0", features = ["sqlite", "neon", "supabase"] }
```

Resolve it with Cargo 1.85 and inspect its independently generated lock: SDK
source is crates.io, checksum matches the observation above, and any resolved
`quinn-proto` is at least 0.11.19. Compile the public APIs the consumer needs;
enabling all features is not evidence that actual cloud calls were executed.
Retain exact toolchain, command, exit code and sanitized output digest. The
repository's `tools/proof/check.py` path-based clean consumer remains necessary
candidate proof but is a different artifact from this registry test.

## Delivery boundary

Normal `.github/workflows/release.yml` automation creates a release only when
its reviewed release conditions are met. Inspect actual publication steps;
a skipped upload or a tag alone cannot establish registry publication. This
document authorizes no release dispatch, merge, tag, deployment or credential
change. Registry adoption belongs to the consuming repository's own review and
full validation, including its Cargo/Bazel/Nix metadata where applicable.

Hosted native RPC, managed OAuth/refresh, physical read-only enforcement,
authorization and database connection modes remain separate issue 70/85/91
proof obligations. Their absence must never be converted into a registry or
production-readiness pass.
