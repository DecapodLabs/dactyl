# Operations

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
- `tools/proof/Containerfile` and the accepted validation entrypoints pin the independent published Decapod controller to `0.138.3`. Full `decapod validate` runs the unchanged reviewed Rust proof through `tools/proof/check.py`, including locked and lock-free consumer checks on Rust 1.85; preparation only fetches dependencies.
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

- Repository signal fingerprint: `f90ae1b129223e76e9f10f47d5d425676af0e7cc4ede2eb510ca3542d837a468`
- Significant implementation surfaces: `.github/` (4 files), `Cargo.lock/` (1 files), `Cargo.toml/` (1 files), `README.md/` (1 files), `examples/` (1 files), `src/` (17 files), `tests/` (17 files), `tools/` (2 files)
- Refreshed from the current codebase by `decapod rpc --op specs.refresh`
<!-- decapod:codebase-attestation:end -->
