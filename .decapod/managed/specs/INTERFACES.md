# Interfaces

## Backend capability inventory (issue 79)

docs/backend-capabilities.md maps the accepted SQLite, Neon HTTP, Supabase service HTTP and native RPC surfaces to source and test obligations. Connection::from_env / DatastoreRoute::from_env remain explicit ambient compatibility APIs; logical per-workspace connectors do not mutate that environment. DATASTORE=supabase retains service HTTP meaning. Remote catalog predicates, snapshots and maintenance return capability errors without network I/O; bounded remote visit_rows decodes a materialized response rather than providing server streaming. The private SQLite FFI loads the host runtime. Arbitrary SQL, domain migrations, retry/idempotency and effective grants remain caller/service-owned; unavailable service proof is distinct from an unsupported capability.

## Resource budgets, streaming scans, and catalog predicates (issue 101)

`dactyl_db` re-exports `RESOURCE_BUDGET_VERSION` (`1`), `ResourceBudget`, `FailureOrigin`, and `MutationOutcome` alongside `Connection::open_with_budget`, `Connection::budget`, `Connection::visit_rows`, `Connection::has_table`, `Connection::has_column`, `DactylError::failure_origin`, and `DactylError::mutation_outcome`. `OpenOptions` retains its two public fields (`access_mode`, `lock_timeout`) so struct-literal callers compile unchanged. `ResourceBudget::v1()` defaults to 256 KiB SQL, 64 KiB serialized `StorageContext`, 4 MiB parameter/cell/wire limits, 256 columns, 100,000 rows, 64 MiB decoded result bytes, 1,024 batch operations, 5s connect timeout, and 15s request timeout; bounds apply per call or per `atomic` batch on a single `Connection`. `Connection::visit_rows` returns `Ok(ScanOutcome { rows, complete: false })` on visitor `Ok(false)` and `Err(DactylError)` (`AdapterErrorKind::Quota`) on budget exhaustion. `has_table` and `has_column` match `StoreSchema::table` / `table_info` visibility on SQLite and return `AdapterErrorKind::Capability` (`unsupported_schema_inspection`, `MutationOutcome::NotAttempted`) locally on Neon and Supabase.

## Compiler and consumer contract

`Cargo.toml` declares Rust 1.85. This raises the consumer toolchain requirement
and therefore needs a breaking next-0.x-minor release, while preserving every
existing public Rust method, type, feature key and wire contract. Rust 1.75 is
not supported by this proposal. Locked all-feature checks and a separately
resolved consumer are both exercised with Rust 1.85; the consumer does not
inherit Dactyl's Cargo.lock. The inactive quinn-proto dependency must resolve at
0.11.19 or later, with no HTTP/3 feature enabled.

## Bounded diagnostic snapshots

`Connection::inspect_snapshot(SnapshotLimits, callback)` requires a dedicated
read-only SQLite connection. SELECT/WITH queries share a deferred read snapshot
and budgets for VM work, duration, queries, rows, decoded bytes, cell/row bytes,
columns and SQL. `SnapshotReader::visit` reports deliberate early stop through
`ScanOutcome.complete=false`; exhaustion returns
`Quota/inspection_budget_exceeded`, never successful truncation. All exits
release statements, remove callbacks, restore native limits and roll back the
read transaction. Other adapters return `unsupported_snapshot_inspection`
without network I/O or fallback. Budgets exclude unbounded caller callback work
and cannot interrupt arbitrary blocking filesystem calls; they are not a
process-wide RSS guarantee. Existing owned `Row`/`Rows` contracts are unchanged.

## Logical cloud connector and managed Auth

`Backend::{Local,Cloud}`, `LogicalTarget`, `LogicalConnector`, and
`DatastoreRoute::for_backend` define per-workspace application selection without
process-global state or ambient environment variables. `LogicalTarget`,
`LogicalConnector`, `DEFAULT_CLOUD_ORIGIN`, `AccountOperation`, `ScmProvider`,
`ScmCredential`, `AuthenticationSecrets`, `AuthenticationStart`, and
`UserSession` are exported unconditionally across Cargo feature combinations.
`LogicalConnector` resolves either a local SQLite route or a discovered
`CloudConnector`, opens `Connection` handles via `LogicalConnector::open`, and
exposes unified start/status/code/session exchange, completion, refresh,
revocation, and account readiness methods (`Capability` error on local handles;
zero SQLite fallback on cloud failure). `CloudConnector` discovers profile v1
(`version`, `project_origin`, `publishable_key`, `schema`).
`AuthenticationSecrets` and `UserSession` serialize only for caller-controlled
private custody and redact `Debug`. Data access uses the additive native
Supabase RPC v1 route; existing service constructors preserve `/query` and
`/batch`. See `docs/cloud-connectors.md` and `docs/supabase-rpc-v1.md` for
bounded execution, `outcome_unknown` post-send write handling, and the boundary
between offline and hosted proof.

`LogicalConnector::account_request` and `CloudConnector::account_request(AccountOperation, &Value, bearer,
Option<&ScmCredential>)` select fixed `account-register`, `account-status`,
`billing-quote`, `billing-checkout`, `billing-manage` (`Manage`), or
`billing-portal` (`Portal`) POST paths. Ordinary account access passes no
SCM credential. Explicit linking accepts only validated `ScmCredential::github`
or `::gitlab`, paired with `github.com` or `gitlab.com` respectively. These opaque
credentials and optional `UserSession::provider_token` redact `Debug`; a provider
token is never automatically forwarded. HTTP `409` maps to
`AdapterErrorKind::Conflict`, untrusted JSON responses enforce
`MAX_JSON_DEPTH = 64`, and public error-code allowlisting across Propodus
(`error.code`), Supabase GoTrue v2 (`error_code`), and OAuth2 (`error`)
preserves actionable session/seat/billing/custody denials (including
`organization_seat_required`, `payment_pending`, `repository_not_authorized`,
`session_expired`, `session_revoked`, `session_not_found`, `bad_jwt`,
`refresh_token_already_used`, `custody_conflict`, and `billing_not_configured`)
while omitting remote messages and unknown codes. Post-send network or response
truncation failures on mutating/one-time calls surface `code = Some("outcome_unknown")`.

Auth completion confirms only session handoff, including an unpaid/unlinked
session. The caller persists it before setup and uses the account result's
explicit `backend_authorized` field for readiness. Dactyl forwards server-issued
project references and workspace capacity fields opaquely; billing catalog
decisions stay backend-owned. `billing_not_configured` is an allowed public code.

## Secure shared-directory SQLite recovery

RecoveryOptions remains unchanged. Linux/Android secure recovery supports plain mode/owner/group access and trusted-group/setgid directories; unsupported platforms, ACL/MAC metadata, ownership reproduction, symlinks/hardlinks, path replacement and cross-filesystem staging return typed errors before destructive mutation where applicable. Recovery archive names cannot alias active sidecars. See docs/sqlite-recovery.md for exact failure/rollback outcomes.

## Contract Principles
- Prefer explicit schemas over implicit behavior.
- Every mutating interface defines idempotency semantics.
- Every failure path maps to a typed, documented error code.

## Generated Contract Depth
Generated interface specs should include:
- API/CLI contracts with request/response schemas.
- Read/write ownership for each storage path.
- Idempotency and retry behavior for mutations.
- Typed failure classes and recovery instructions.

## API / RPC Contracts
| Interface | Method | Request Schema | Response Schema | Errors | Idempotency |
|---|---|---|---|---|---|
| `TODO` | `TODO` | `TODO` | `TODO` | `TODO` | `TODO` |

## Event Consumers
| Consumer | Event | Ordering Requirement | Retry Policy | DLQ Policy |
|---|---|---|---|---|
| `TODO` | `TODO` | `TODO` | `TODO` | `TODO` |

## Outbound Dependencies
| Dependency | Purpose | SLA | Timeout | Circuit-Breaker |
|---|---|---|---|---|
| `TODO` | `TODO` | `TODO` | `TODO` | `TODO` |

## Inbound Contracts
- API / RPC entrypoints:
- CLI surfaces:
- Event/webhook consumers:
- Repository-detected surfaces: not detected yet

## Data Ownership
- Source-of-truth tables/collections:
- Cross-boundary read models:
- Consistency expectations:

## Error Taxonomy Example (not classified yet)
```rust
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("validation failed: {0}")]
    Validation(String),
    #[error("upstream timeout")]
    UpstreamTimeout,
    #[error("conflict: {0}")]
    Conflict(String),
}
```

## Failure Semantics
| Failure Class | Retry/Backoff | Client Contract | Observability |
|---|---|---|---|
| Validation | No retry | 4xx typed error | warn log + metric |
| Dependency timeout | Exponential backoff | 503 with retryable code | error log + alert |
| Conflict | Conditional retry | 409 with conflict detail | info log + metric |

## Timeout Budget
| Hop | Budget (ms) | Notes |
|---|---|---|
| Client -> Edge/API | 500 | Includes auth + routing |
| API -> Domain | 300 | Includes validation |
| Domain -> Store/Dependency | 200 | Includes retry overhead |

## Interface Versioning
- Version strategy (`v1`, date-based, semver):
- Backward-compatibility guarantees:
- Deprecation window and removal policy:

## Dactyl — API Contract (design-time note)

- Public application surface: `read`, `write_result`, `atomic(&[Operation])`, `OpenOptions { access_mode, lock_timeout }`, and `Connection::open` for an explicit route. `write` remains the affected-row compatibility wrapper.
- Adapter selection is ambient-env-driven for free functions: `DATASTORE` is `sqlite` or `neon`, `DATASTORE_ROUTE` is the SQLite path or Neon endpoint, and `DATASTORE_TOKEN` is the optional Neon authorization credential.
- `StorageContext` is a versioned opaque `{ version: u16, payload: JSON object }` envelope supplied through `Connection::open_with_context` or `Connection::open_with_options_and_context`. Dactyl validates only a non-zero version and object payload shape; Decapod/Propodus own the payload meaning.
- Parameters are always adapter-bound, never interpolated. `Parameter` covers `Null`, `Bool`, `Integer`, `Real`, `Text`, and `Blob`. Neon JSON transport encodes `Blob` as an array of bytes so remote writes can bind the same values as local SQLite.
- `Rows` owns normalized `Row` values. SQLite and Neon use the same column/value representation and typed row accessors, including explicit NULL and conversion failures.
- SQL is never interpolated or rewritten for domain meaning. The local adapter binds parameters and delegates SQL to the real SQLite connection, while Neon forwards the SQL transport request. Dactyl has no schema bootstrap, migration API, retry policy, idempotency policy, analytics, or business-intelligence behavior.
- The local route file is an ordinary SQLite database. Read/write opens create missing paths; read-only opens require an existing path. SQLite owns file format compatibility, locking, journaling, and SQL execution. Dactyl configures the busy timeout and maps native failures to stable `AdapterErrorKind` categories.
- `Connection::inspect_schema()` is the backend-neutral catalog for local SQLite. It reports tables, columns, defaults, primary/unique keys, indexes, foreign keys, delete actions, and row counts. `Row::get_blob` reads the canonical JSON byte-array row shape. Neon inspection fails as `unsupported_schema_inspection`.
- `atomic` is an opaque all-or-nothing batch with ordered results, empty-batch no-op semantics, and no nested transaction handles. Operational adapter errors expose typed categories and preserve stable remote error codes so application code does not parse backend messages. The Neon adapter maps `constraint_failed` / unique / not-null / foreign-key violation codes to `AdapterErrorKind::Constraint` and busy/locked/timeout codes to the matching contention kinds.

### Explicit SQLite maintenance contract

- `Connection::verify_integrity()` is read-only and returns `IntegrityReport`
  (`journal_mode`, `user_version`, `application_id`) only for a healthy local
  store. Malformed files and damaged indexes use the additive
  `AdapterErrorKind::Corrupt` category with stable codes; lock, unavailable,
  and filesystem failures remain distinguishable.
- `Connection::backup(destination)` is a local-only online snapshot. It
  accounts for WAL/SHM through SQLite's backup API, validates and syncs a
  temporary sibling, then atomically publishes a standalone destination.
  Existing destination paths are conflicts, so an operator never loses a
  prior backup implicitly.
- `Connection::recover_from_dump_reload(RecoveryOptions)` is an explicit
  mutating operation. The caller names a same-directory archive path and the
  journal-mode choice exposed by the typed options/result. The current
  supported mode is DELETE: logical dump/reload does not preserve source WAL,
  and Dactyl does not re-enable WAL implicitly.
- Recovery validates schema/data/metadata in a new file before moving the
  original main file and any `-wal`/`-shm` sidecars to the archive. Same-process
  sibling connections return `Conflict/open_connections`; cross-process
  quiescence remains the caller/coordinator boundary. Rename, fsync, and
  reopen failures attempt rollback and surface `recovery_rollback_failed` if
  rollback itself cannot be proven.
- Ordinary open, validation, reads, and writes never invoke recovery. The
  public surface contains no SQLite handle, raw dump text, Decapod event model,
  workspace model, or automatic repair policy. Neon returns typed capability
  errors without issuing network requests.

#### Ambient route and batch result hardening

`DATASTORE` is the sole ambient selector; `DATASTORE_ROUTE` is mandatory and
must be non-empty for both `sqlite` and `neon`. Dactyl does not silently fall
back to local SQLite when the selected route is incomplete. `DATASTORE_TOKEN`
is read only for Neon, and a blank value is treated as absent. The explicit
`Connection::open` path remains available for callers that do not want ambient
selection.

Neon `/batch` results are normalized using the corresponding caller-supplied
`OperationKind`, not by inspecting whether the response happens to contain
columns or rows. This preserves an empty remote read as `OperationResult::Rows`
and keeps write/generated-key results in `OperationResult::Write` even when a
provider includes auxiliary row data. HTTP 408/502/503/504 fallbacks map to
typed timeout/unavailable outcomes; provider error codes still take precedence.

### Storage-context transport contract

| Consumer | Request field | Local behavior | Neon behavior | Typed failures |
|---|---|---|---|---|
| Dactyl adapter | `context: { version, payload }` on `/query` and `/batch` | Ignore context; keep the local route/database boundary authoritative | Forward the envelope unchanged alongside SQL, parameters, access mode, or operations | Missing context: `authentication_required`; invalid envelope: `invalid_context` / `Protocol`; service authz codes remain typed without interpretation |

The context is optional at the connection API so existing local callers remain
compatible. A Neon operation requires a valid context and fails before network
I/O when it is absent. The public contract contains no organization, user,
repository, membership, or schema types; those semantics belong to the
Decapod and Propodus contracts. The same context is attached once to an atomic
batch, so all operations in that physical transaction share one forwarding
boundary.

Local fixtures must keep succeeding when a caller supplies unused
`org_id` / `user_id` / `repository_id` payload fields, and they must produce
the same rows, affected counts, and typed constraint errors when context is
absent. Dactyl still does not read those fields. Remote fixtures prove
forwarding plus typed `authentication_required`, `invalid_context`, and
`repository_not_authorized` outcomes. Live Propodus membership lookup is
outside this contract.

### Backend-neutral fixture suite

`tests/storage_fixtures.rs` is the reusable operation matrix. Each case is a
schema-generic driver behavior, not a Decapod TODO/lease/governance policy.
Local SQLite is the required offline backend. The Neon executing mock is the
offline remote backend. `DACTYL_LIVE_PROPODUS_ROUTE` may exist later; an unset
or unused live target must be reported as `unavailable`, never as a green pass.

### Multi-backend boundary

SQLite and Neon are the current supported application stores. Any future
backend must preserve the same read/write, access-mode, atomic-batch, result,
and typed-error contract; adding database administration or business-
intelligence features is out of scope. Propodus resource-route translation and
live cloud deployment proof remain service-side concerns.

<!-- decapod:capability-overlay:public-api:start -->

Generated capability obligations: `public-api` is declared for this project. These are decisions and proof requirements, not claims of implemented behavior. Resolve each against project source and tests; record unsupported or non-applicable mechanisms explicitly.

## Public API Capability Overlay

### API Contract Requirements
- All public endpoints MUST define explicit request/response schemas
- Versioning strategy MUST be documented (URL path or header-based)
- All public endpoints MUST implement idempotency for mutating operations
- Rate limiting and pagination MUST be implemented for list endpoints

### Compatibility Guarantees
- Backward-compatible changes ONLY within a version
- Breaking changes require new version (v1, v2, etc.)
- Deprecation and removal policy MUST be selected for this project and proven against its consumers

### Security Requirements
- All public endpoints MUST implement authentication
- Abuse-control enforcement point MUST be a documented project decision
- Input validation MUST reject malformed requests with typed errors
<!-- decapod:capability-overlay:public-api:end -->

<!-- decapod:codebase-attestation:start -->

## Codebase Attestation

- Repository signal fingerprint: `a7a057f783efbf536549d413204a9e09cae9e963fb7a89f823cb25e6e7f56cf1`
- Significant implementation surfaces: `.github/` (4 files), `Cargo.lock/` (1 files), `Cargo.toml/` (1 files), `README.md/` (1 files), `examples/` (1 files), `src/` (18 files), `tests/` (23 files), `tools/` (2 files)
- Refreshed from the current codebase by `decapod rpc --op specs.refresh`
<!-- decapod:codebase-attestation:end -->

## Supabase HTTP contract (issue 91)

The `supabase` Cargo feature adds `Datastore::Supabase` and
`DatastoreRoute::supabase(endpoint, bearer)`. `DATASTORE=supabase` selects the
same route with `DATASTORE_ROUTE` and `DATASTORE_TOKEN`. `DatastoreTransport`
reports Local or Http without specifying an HTTP hosting company. No SQL call
accepts a provider override. The existing versioned StorageContext is forwarded
unchanged; it does not grant tenant authority. The service owns authorization.

The endpoint exposes `/query` and `/batch` with the existing JSON scalar and
byte-array parameter profile, ordered results, explicit affected counts and
explicit generated keys. SQLite-only catalog/maintenance operations fail with
capability errors. SQL compatibility and physical commit/rollback require actual
PostgreSQL service evidence, not an HTTP mock executing SQLite.

## Native read-only batch boundary

The additive native dactyl_read_batch_v1 endpoint preserves the version1 ordered batch envelope and explicit read_only mode. Write batches keep dactyl_batch_v1 and legacy routes are unchanged. No fallback is introduced for an unavailable receiver.
