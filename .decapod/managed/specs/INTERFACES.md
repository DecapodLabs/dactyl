# Interfaces

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
<!-- decapod:codebase-attestation:start -->

## Codebase Attestation

- Repository signal fingerprint: `d577d6f04f4dc668f2833f953cdd4c3854c28b689bbdff85e5a9b2343e46641c`
- Significant implementation surfaces: `.github/` (4 files), `Cargo.lock/` (1 files), `Cargo.toml/` (1 files), `README.md/` (1 files), `src/` (9 files), `tests/` (1 files)
- Refreshed from the current codebase by `decapod specs.refresh`
<!-- decapod:codebase-attestation:end -->
