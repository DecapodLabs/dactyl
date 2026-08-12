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
- Adapter selection is ambient-env-driven for free functions: `DATASTORE` is `sqlite` or `neon`, `DATASTORE_ROUTE` is the SQLite path or Neon endpoint, and `DATASTORE_TOKEN` is the optional Neon bearer token.
- Parameters are always adapter-bound, never interpolated. `Parameter` covers `Null`, `Bool`, `Integer`, `Real`, `Text`, and `Blob`.
- `Rows` owns normalized `Row` values. SQLite and Neon use the same column/value representation and typed row accessors, including explicit NULL and conversion failures.
- SQL is never interpolated or rewritten for domain meaning. The local adapter parses only its bounded storage subset, including caller-owned DDL, while Neon forwards the SQL transport request. Dactyl has no schema bootstrap, migration API, retry policy, idempotency policy, analytics, or business-intelligence behavior.
- `atomic` is an opaque all-or-nothing batch with ordered results, empty-batch no-op semantics, and no nested transaction handles. Operational adapter errors expose typed categories and preserve stable remote error codes so application code does not parse backend messages.

### Multi-backend boundary

SQLite and Neon are the current supported application stores. Any future
backend must preserve the same read/write, access-mode, atomic-batch, result,
and typed-error contract; adding database administration or business-
intelligence features is out of scope. Propodus resource-route translation and
live cloud deployment proof remain service-side concerns.

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

<!-- decapod:codebase-attestation:start -->

## Codebase Attestation

- Repository signal fingerprint: `37f13361e4f6bac90d4cf2c135899189f5f0b1c4333fbda2d7f8431481c507ed`
- Significant implementation surfaces: `.github/` (2 files), `Cargo.lock/` (1 files), `Cargo.toml/` (1 files), `README.md/` (1 files), `src/` (7 files)
- Refreshed from the current codebase by `decapod specs.refresh`
<!-- decapod:codebase-attestation:end -->
