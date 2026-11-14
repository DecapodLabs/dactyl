# Operations

## Current registry and consumer handoff (issue 94)

Native proof disables incremental caches and dev/test debug information through
the reviewed `CARGO_INCREMENTAL=0`, `CARGO_PROFILE_DEV_DEBUG=0` and
`CARGO_PROFILE_TEST_DEBUG=0` execution environment. Repeated source snapshots
otherwise exhausted the shared host disk with disposable build artifacts.
These [Cargo profile settings](https://doc.rust-lang.org/cargo/reference/profiles.html)
control caches and debugging information; debug assertions, overflow checks,
optimization levels, toolchain paths and all 17 required checks retain their
existing settings. No test is filtered or converted into a skipped check, and
the published crate manifest and consumers' build profiles are unchanged.
The command-contract digest is explicitly re-reviewed for this environment
change; complete exact-candidate validation remains required.

The official 0.16.0 archive matches sparse-index SHA-256 `af724e4a561da93c4cf9bf8ca63f25c6d227bee6a49318fdbd5dbf7bc93083ca` and source `7d539730f9f3218f58f244c284c529c5b19967fb`. `docs/registry-handoff.md` records an executed clean crates.io consumer on Rust/Cargo 1.85.0 with SQLite, Neon and Supabase enabled, no Git/path patch, public connector/session/RPC types compiled and Quinn 0.11.19 resolved. This establishes registry compilation, not hosted runtime acceptance; complete candidate feature tests remain independently required.

README.md documents the accepted 0.16.0 API, Rust/Cargo 1.85 and the distinction between logical connectors, explicit ambient service routes and native RPC. DATASTORE_TOKEN is remote-only and required by the Supabase service. Package metadata already names SQLite, Neon and Supabase; exact registry/checksum/package and clean registry-consumer observations are recorded above, while hosted provider qualification and subsequent consumer rollout retain their own evidence requirements. Decapod has adopted registry =0.14.0, completing its security handoff in issue 1638; a future SDK upgrade requires separate consumer validation. A GitHub release or path-based package proof cannot establish registry-consumer execution or hosted readiness. Normal release automation retains publication authority; this documentation work neither publishes nor deploys.

## Capacity, dogfooding inventory, and recovery guardrails (issue 101)

`docs/datastore/dogfooding-inventory.json` records read-only physical snapshot metrics from the repository's `.decapod/data/decapod.db` (`13,344,768` apparent bytes across `3,258` pages; `events` holds `18,941` rows and `94.3%` of live allocated B-tree bytes). Operators and callers use `ResourceBudget` (`docs/datastore/resource-budgets.md`) and `Connection::visit_rows` to prevent unbounded single-call memory or HTTP payload growth while applying caller-level concurrency limits across multiple connections. When `DactylError::mutation_outcome()` returns `MutationOutcome::Uncertain` after a remote transport, timeout, or post-send response failure, callers must reconcile persisted state or supply an idempotency key rather than blindly replaying non-idempotent writes. Explicit SQLite `recover_from_dump_reload` now streams table data through one staging transaction and one reused C-ABI prepared `INSERT` per table, avoiding `32x` JSON BLOB memory amplification during incident recovery.

## Operational Readiness Checklist
- [ ] On-call ownership defined.
- [ ] SLOs and alert thresholds defined.
- [ ] Dashboards for latency/errors/throughput are live.
- [ ] Runbooks linked for all Sev1/Sev2 alerts.
- [ ] Rollback plan validated.
- [ ] Capacity guardrails documented.

## Deployment Model
Describe the operational runtime model, scheduling, and system deployment architecture.

## Service Level Objectives
| SLI | SLO Target | Measurement Window | Owner |
|---|---|---|---|
| Availability | 99.9% | 30d | TBD |
| P95 latency | TBD | 7d | TBD |
| Error rate | < 1% | 7d | TBD |

## Monitoring
| Signal | Metric | Threshold | Alert |
|---|---|---|---|
| Traffic | requests/sec | baseline drift | warn |
| Latency | p95/p99 | threshold breach | page |
| Reliability | error ratio | threshold breach | page |
| Saturation | cpu/memory/queue depth | sustained high | page |

## Health Checks
- Liveness:
- Readiness:
- Dependency health:
- Synthetic transaction:

## SQLite maintenance runbook

1. Call `Connection::verify_integrity()` and record the typed result. Treat
   `Corrupt` as a storage incident; do not retry `REINDEX` or mutate the live
   file as an automatic response. Busy/locked/unavailable outcomes require
   contention or filesystem triage instead.
2. For a live snapshot, call `Connection::backup(destination)`. The online
   backup includes committed WAL state through SQLite and does not require
   copying the source main file, `-wal`, or `-shm` independently. Verify the
   returned destination before handing it to another process.
3. For explicit corruption recovery, stop/quiesce cooperating writers, close
   all other Dactyl connections, choose an unused archive path in the database
   directory, and call `recover_from_dump_reload(RecoveryOptions::new(...,
   RecoveryJournalMode::Delete))`. The result records the active path, archive
   path, preserved metadata, and DELETE journal mode.
4. Reopen through the normal `Connection` API and run
   `verify_integrity()` again. The original database and sidecars remain at
   the reported archive path for forensic recovery. If activation or rollback
   reports a failure, preserve all named paths and escalate rather than
   deleting or recreating the store.

This runbook is online for backup but quiesced for replacement. Dactyl uses a
bounded SQLite exclusive transaction and same-process handle tracking, but it
cannot coordinate arbitrary external writers or guarantee advisory locks over
unreliable bind mounts/filesystems. Re-enabling WAL after recovery is a
separate explicit operation and is outside replacement atomicity.

## Incident Response
- Detection:
- Triage:
- Mitigation:
- Communication:
- Post-mortem:

## Rollout Strategy
- Blue/green deployment:
- Canary release:
- Rolling update:
- Feature flags:

## Release Synchronization
Release preparation starts from fetched `origin/main` in an isolated managed
worktree. `CHANGELOG.md` distinguishes unpublished compatibility guidance from
published versions. The accepted manifest requires Rust/Cargo 1.85 and patched
inactive Quinn resolution; downstream applications must refresh their own
lockfiles and run their own audits. This metadata requirement does not activate
HTTP/3 or alter Dactyl's Rust API, wire contract or backend authorization.

`.github/workflows/release.yml` runs release-please on main-branch pushes.
Maintainers review and merge its version/changelog PR; only a run reporting
`release_created` proceeds to `cargo publish`. A successful workflow with
publication steps skipped is not evidence that a crate was published. Verify
both the GitHub release and crates.io version before upgrading consumers.

When a release-worthy compatibility change was merged without a recognized
release unit, a meaningful conventional `fix!` preparation commit can carry the
documented `Release-As: 0.14.0` footer to request the next release PR. It leaves
`Cargo.toml` version changes to release-please and does not authorize a direct
release, privileged workflow dispatch or downstream dependency adoption.
This path follows the [release-please version override contract](https://github.com/googleapis/release-please#how-do-i-change-the-version-number).
Merged-PR body commit overrides are unsupported for plain merges.

**CI/CD Notes**:
- `tools/proof/Containerfile` and the accepted validation entrypoints pin the independent published Decapod controller to `0.143.0`. Full `decapod validate` runs the unchanged reviewed Rust proof through `tools/proof/check.py`, including locked and lock-free consumer checks on Rust 1.85; preparation only fetches dependencies.
- The `Pages` workflow publishes the `docs/` directory as GitHub Pages. Live github.io hosting still requires the repository Pages source to be GitHub Actions. The connector report is also readable from the tree at `docs/whitepapers/dactyl-sqlite-connector.md`.

## Local SQLite operations

The local route path is an ordinary SQLite database. Operators may use standard SQLite backup, integrity-check, and journal/WAL tooling appropriate to the configured SQLite mode; Dactyl does not add sidecar storage.

| Artifact | Meaning | Recovery |
|---|---|---|
| `$ROUTE` | SQLite database file | use a SQLite-compatible backup or restore procedure |
| SQLite journal/WAL files | SQLite-managed durability state | let SQLite recover them; do not treat them as Dactyl artifacts |
| `OpenOptions::lock_timeout` | SQLite busy timeout | retry or surface the typed busy/locked error; Dactyl does not retry |

Read-only opens never create or mutate a database. Existing SQLite files are opened directly; there is no migration/import command in Dactyl. Decapod remains responsible for explicit schema and migration policy.

## Logical connector and managed-session operations

Per-workspace `LogicalConnector` resolution (`src/connector.rs`) holds no process-global cache, background worker, or durable token file. Callers checkpoint pending one-time auth exchanges, token rotations, and mutating account actions (`Register`, `Checkout`, `Manage`) before dispatch; if Dactyl returns `code = Some("outcome_unknown")` after sending a mutating or one-time request, operators/callers must reconcile account or session status before re-attempting rather than replaying blindly. Failed cloud profile discovery never degrades to local SQLite. Offline synthetic HTTP and local SQLite proofs do not attest hosted Supabase Auth, live Propodus Edge Functions, or PostgreSQL pooler availability (`unavailable` until exercised in deployment).

## Capacity Planning
- Traffic patterns:
- Resource utilization:
- Scaling triggers:

## Logging
Use `tracing` + `tracing-subscriber` with structured JSON output and request correlation ids.

## Secrets Management
| Secret | Source | Rotation | Consumer |
|---|---|---|---|
| External service auth material | managed runtime configuration | periodic | runtime services |
| Artifact signing material | managed signing service/local secure store | periodic | release pipeline |

## Security Testing
| Test Type | Cadence | Tooling |
|---|---|---|
| SAST | each PR | language linters/scanners |
| Dependency scan | each PR + weekly | supply-chain tools |
| DAST/pentest | scheduled | external/internal |

## Compliance and Audit
- Regulatory scope:
- Audit evidence location:
- Exception process:

## Pre-Promotion Security Checklist
- [ ] Threat model updated for changed surfaces.
- [ ] Auth/authz tests pass.
- [ ] Dependency vulnerability scan reviewed.
- [ ] No unresolved critical/high security findings.

<!-- decapod:capability-overlay:background-processing:start -->

Generated capability obligations: `background-processing` is declared for this project. These are decisions and proof requirements, not claims of implemented behavior. Resolve each against project source and tests; record unsupported or non-applicable mechanisms explicitly.

## Background Processing Operations Overlay

### Queue Visibility
- Queue depth, processing rate, and latency MUST be monitored
- If a dead-letter queue exists, define visibility and alerting obligations for it
- Worker health and processing rate metrics required

### Shutdown Behavior
- Graceful shutdown: stop accepting new work, finish current job
- Drain behavior and timeout MUST be selected for the deployment
- Termination and requeue behavior MUST be selected and proven for the deployment

### Worker Health
- Worker liveness and readiness probes
- Queue depth alerts for backpressure detection
- Processing latency percentiles (p50, p95, p99)
<!-- decapod:capability-overlay:background-processing:end -->

<!-- decapod:capability-overlay:persistent-state:start -->

Generated capability obligations: `persistent-state` is declared for this project. These are decisions and proof requirements, not claims of implemented behavior. Resolve each against project source and tests; record unsupported or non-applicable mechanisms explicitly.

## Persistent State Operations Overlay

### Backup & Recovery
- Backup scope, schedule, retention, and restore evidence MUST be selected for the project
- Recovery point objectives MUST be explicit project decisions, not assumed values
- Recovery time objectives MUST be explicit project decisions, not assumed values
- Restore verification cadence MUST be recorded with the operational proof plan

### Migration Operations
- All schema changes via migration files
- Migration rollback procedures documented
- Migration downtime and availability effects MUST be declared; zero downtime requires project-specific proof
- Migration health checks and rollback triggers
<!-- decapod:capability-overlay:persistent-state:end -->

<!-- decapod:codebase-attestation:start -->

## Codebase Attestation

- Repository signal fingerprint: `c31e1bc9ffdfc10d9a7d2f4116aa7fba2967366521a7cd58a84d4793310211e2`
- Significant implementation surfaces: `.github/` (4 files), `Cargo.lock/` (1 files), `Cargo.toml/` (1 files), `README.md/` (1 files), `examples/` (1 files), `src/` (18 files), `tests/` (24 files), `tools/` (2 files)
- Refreshed from the current codebase by `decapod rpc --op specs.refresh`
<!-- decapod:codebase-attestation:end -->
