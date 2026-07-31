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

- Public surface (crate root): `read(query: &str, optimize: bool) -> Result<Rows, DactylError>` and `write(query: &str, optimize: bool) -> Result<Rows, DactylError>` plus the `query!("...")` macro and the `Rows` / `Row` / `DactylError` result types.
- There is no `init` / `active_datastore` at the crate root. The first `read` or `write` call lazily establishes the connection.
- Adapter selection is env-driven:
  - `DATASTORE` set to `"sqlite"` or `"neon"`.
  - `DATASTORE_ROUTE` specifies the database path (SQLite) or the endpoint URL (Neon).
  - `DATASTORE_TOKEN` is the optional auth token for Neon.
  - Legacy variables (`DACTYL_NEON_ENDPOINT`, `DACTYL_NEON_BEARER`, `DACTYL_SQLITE_PATH`, `DACTYL_SQLITE_ROOT`) are supported as fallbacks.
- `optimize = true` allows the analyzer to rewrite the query; `optimize = false` rejects the call with `DactylError::Unsupported { construct }` when any construct is not native to the inferred adapter.
- `query!("sql")` lexically analyzes the literal at compile time and returns the rewritten SQL as a `String` for the caller to pass to `read` / `write`.

<!-- decapod:codebase-attestation:start -->
## Codebase Attestation

- Repository signal fingerprint: `470a7a990e6e7e4903cd694b2c38fc58717d81290eed1cfb531172a335f088c4`
- Significant implementation surfaces: `.github/` (2 files), `Cargo.lock/` (1 files), `Cargo.toml/` (1 files), `README.md/` (1 files), `dactyl_macros/` (1 files), `src/` (11 files)
- Refreshed from the current codebase by `decapod specs.refresh`
<!-- decapod:codebase-attestation:end -->
