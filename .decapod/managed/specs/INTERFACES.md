# Interfaces

<!-- decapod:capability-overlay:public-api:start -->

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

- Public surface (crate root): `query(sql: &str, params: &[Parameter]) -> Result<Rows, DactylError>`, `execute(sql: &str, params: &[Parameter]) -> Result<u64, DactylError>`, and `transaction(statements: &[Statement]) -> Result<Vec<Rows>, DactylError>`, plus the `query!("...")` macro and the `Parameter` / `Statement` / `Rows` / `Row` / `DactylError` result types.
- There is no `init` / `active_datastore` and no process-global connection cache. Each call builds a short-lived adapter for the ambient selection and drops it on return, so session isolation is automatic.
- Adapter selection is ambient-env-driven only:
  - `DATASTORE` set to `"sqlite"` or `"neon"` (any other value is a typed error).
  - `DATASTORE_ROUTE` specifies the database path (SQLite) or the endpoint URL (Neon).
  - `DATASTORE_TOKEN` is the optional auth token for Neon.
  - No legacy `DACTYL_*` variables are honored.
- Parameters are always adapter-bound, never interpolated into SQL. `Parameter` enumerates `Null` / `Bool` / `Integer` / `Real` / `Text`.
- `execute` is the caller-owned schema surface: dactyl never silently creates tables.
- `Row` provides strict `get` / `try_get<T: DeserializeOwned>`, lenient `get_bool` / `get_int` / `get_real` / `get_str` / `get_json`, borrowed `get_str_ref` / `get_json_ref`, and `is_null`, with explicit `ColumnNotFound` / `Conversion` errors. Named lookup is left-to-right first-match for duplicate aliases. SQL NULL maps to `Option<T>` or a `Conversion` that mentions NULL for non-Option targets. Rows own their cells; borrowed accessors are tied to `&Row` only (dactyl #25 / #2; DecapodLabs/decapod#1111).
- `transaction` is atomic: any per-statement failure rolls back the whole unit on SQLite and is rejected by the Neon `/batch` endpoint (dactyl #24). Nesting is not supported (no SAVEPOINT). dactyl does not retry and exposes no deadline parameter; callers own retry/idempotency after ambiguous transport failures. Empty batch → `Ok([])`. Conformance proves failure-injection on SQLite and Neon mock plus an event-plus-state fixture.
- `query!("sql")` lexically analyzes the literal at compile time and returns the rewritten SQL as a `String` for the caller to pass to `query`.

### Multi-backend vision

dactyl-db is the single SQL-vendor-agnostic persistence framework. The `Adapter` trait is backend-neutral and SQL-focused; new backends (Redis, MySQL, Cassandra, and other structured query languages) slot in as one module plus one `DATASTORE` match arm. The public `query` / `execute` / `transaction` surface never changes per backend.

<!-- decapod:codebase-attestation:start -->
## Codebase Attestation

- Repository signal fingerprint: `b3e97603d56159f5f37a8856b93961904220dd5b18190b52f3f7896f1bf3e65f`
- Significant implementation surfaces: `.github/` (2 files), `Cargo.lock/` (1 files), `Cargo.toml/` (1 files), `README.md/` (1 files), `dactyl-db-macros/` (1 files), `src/` (10 files)
- Refreshed from the current codebase by `decapod specs.refresh`
<!-- decapod:codebase-attestation:end -->
