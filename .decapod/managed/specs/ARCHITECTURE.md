# Architecture

## Direction
library

## What This Project Is
dactyl-db is a small Rust application driver: one normalized operation surface over a real local SQLite file and Vercel Neon. It is not a database-administration layer and does not reimplement SQLite.

Architectural principles:
- **Simplicity**: Keep components focused and reusable.
- **Modularity**: Clearly defined interface boundaries and dependency separation.
- **Reliability**: Graceful failure handling and thorough verification.
- **Backend-neutrality**: local and Neon adapters receive the same SQL, parameters, operation kinds, opaque atomic batches, and optional caller-owned context; backend-specific handles remain private.
- **Context separation**: the physical `DatastoreRoute` remains distinct from the versioned opaque `StorageContext`. Local storage ignores cloud-only payloads, while Neon forwards the validated envelope without interpreting its fields.

## Current Facts
- Runtime/languages: Rust
- Detected surfaces/framework hints: to be confirmed
- Product type: to be confirmed

## Architecture Map
This project's architecture consists of the following key layers/directories:
- `src/`: Main source directory containing primary logic.
- `tests/`: Integration and unit test suite.

## Data Flows
- The application supplies SQL, bound values, and caller-owned schema operations.
- Dactyl selects the local or Neon adapter and enforces the operation/access boundary.
- For Neon, Dactyl validates the context envelope, attaches it to `/query` and
  `/batch`, and fails closed before transport when it is absent or malformed.
- The adapter returns normalized rows, explicit write results, typed errors, or an ordered atomic result.

## Strongest Existing Primitives
- Define the strongest existing primitives in the codebase (e.g., helper utilities, base controllers, data access layers).

## Topology
```mermaid
flowchart LR
  C[Client] --> G[API Gateway]
  G --> S[Service Core]
  S --> W[Workers]
  S --> DB[(Primary Datastore)]
  W --> Q[(Queue)]
```

## Store Boundaries
```mermaid
flowchart LR
  I[Inbound Requests] --> C[Core Logic]
  C --> W[(Write Store)]
  C --> R[(Read Store)]
```

## Happy Path Sequence
```mermaid
sequenceDiagram
  participant C as Client
  participant G as API
  participant D as Domain
  participant DB as Datastore
  C->>G: Request
  G->>D: Validate + execute
  D->>DB: Read or write SQL
  DB-->>D: Rows or affected count
  D-->>G: Domain result
  G-->>C: Response + trace_id
```

## Error Path
```mermaid
sequenceDiagram
  participant Client
  participant Service
  participant Store
  Client->>Service: Request
  Service->>Store: Database Query
  Store--xService: Error/Timeout
  Service-->>Client: Typed Error / Recovery Instructions
```

## Execution Path
- Ingress parse + validation:
- Policy/interlock checks:
- Core execution + persistence:
- Verification and artifact emission:

## Concurrency and Runtime Model
- Execution model: each free `read` / `write` call constructs a short-lived adapter for the ambient route and drops it on return; explicit `Connection` scopes several application operations to one route.
- Isolation boundaries: Dactyl keeps no process-global cache and exposes no backend handle. Each local connection owns a private mutex-protected SQLite handle; SQLite's pager, lock, journal, busy timeout, and transaction machinery provide file isolation. `Connection::atomic` commits only after every operation succeeds.
- Backpressure strategy: owned by the backend or Neon service; Dactyl does not retry or schedule work.
- Shared state synchronization: none at the Dactyl layer.

## Deployment Topology
- Runtime units: none (library).
- Region/zone model: n/a.
- Rollout strategy: library releases via release-plz.
- Rollback trigger and blast-radius scope: callers pin a dactyl version; breaking changes are recorded in CHANGELOG.md.

## Data and Contracts
- Inbound contracts (application calls): `read`, `write_result`, `atomic`, `OpenOptions`, `StorageContext`, and owned row/result values.
- Outbound dependencies (datastores/queues/external APIs): optional runtime-loaded host SQLite and Neon/Propodus HTTP transport. The local C-ABI loader is isolated behind the private adapter module; no SQLite wrapper or bundled SQLite implementation is compiled into consumers.
- Data ownership boundaries: callers own schema definitions and migration policy; Decapod owns context meaning; Propodus owns cloud authorization; Dactyl executes only the documented caller-supplied schema subset and physical atomicity.
- Schema evolution + migration policy: outside Dactyl's scope.

## ADR Register
| ADR | Title | Status | Rationale | Date |
|---|---|---|---|---|
| ADR-001 | Ambient-env routing contract (DATASTORE/DATASTORE_ROUTE/DATASTORE_TOKEN) | Accepted | Single authoritative selector; no init(); per-call adapters for session isolation (dactyl #26) | 2026-08-01 |
| ADR-002 | Thin application API: read(sql, params) + write(sql, params) | Accepted | One uniform read/write surface for SQLite and Neon; administration remains outside the crate (dactyl #47) | 2026-08-01 |
| ADR-003 | Backend-neutral Adapter trait | Proposed | New backends (redis, mysql, cassandra) add one module + one DATASTORE arm; public surface unchanged | 2026-08-01 |
| ADR-004 | Pure-Rust local engine with opaque atomic batches | Superseded | The handwritten snapshot engine was removed when Issue #77 required existing SQLite file compatibility. Opaque batches remain part of the public contract, but SQLite owns execution and durability. | 2026-08-11 |
| ADR-005 | Opaque storage-context forwarding | Accepted | Keep physical route configuration and cloud tenancy separate: Dactyl validates only a versioned envelope, ignores it locally, forwards it to Neon, and leaves authorization semantics to Propodus (#64) | 2026-08-11 |
| ADR-006 | Local fixture completeness before live Propodus | Accepted | Issues #57 and #64 are complete for the local store and the offline Neon mock: CAS is a zero-row observation, concurrent writers share a file lock, cleanup is `DROP`, and live Vercel Neon/Propodus proof is a separate deployment issue | 2026-08-12 |
| ADR-007 | Dactyl-owned snapshot format, not a SQLite file header | Superseded | The snapshot and its sidecars were removed; the local route now opens the requested SQLite file directly. | 2026-08-12 |
| ADR-008 | Explicit pure-Rust SQLite-to-Dactyl import for issue #77 | Superseded | The revised Issue #77 scope requires direct compatibility, so the importer and custom reader were removed. | 2026-08-12 |
| ADR-009 | Real SQLite local connector for issue #77 | Accepted | Use the smallest suitable SQLite binding behind the private adapter, preserve the existing backend-neutral API, and leave schema/migration policy outside Dactyl. | 2026-08-13 |

## Delivery Plan (first 3 slices)
- Slice 1 (ship first):
- Slice 2:
- Slice 3:

## Risks and Mitigations
| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Contract drift across components | Medium | High | Spec + schema checks in CI |
| Runtime saturation under peak load | Medium | High | Capacity model + load tests |

<!-- decapod:capability-overlay:persistent-state:start -->

## Persistent State Architecture Overlay

### State Ownership
- Each entity type MUST have a designated state owner
- State ownership boundaries MUST be explicitly documented
- Cross-boundary state access MUST go through defined interfaces

### Transaction Boundaries
- All multi-entity mutations MUST occur within explicit transactions
- Transaction boundaries MUST be documented in ARCHITECTURE.md
- Compensating transactions for distributed operations

### Storage Abstraction
- Storage ownership, consistency behavior, and access boundaries MUST be explicit
- Portability or swappable implementations are project decisions, not universal requirements
- Migration and rollback treatment MUST match the selected storage technology
<!-- decapod:capability-overlay:persistent-state:end -->

<!-- decapod:codebase-attestation:start -->

## Codebase Attestation

- Repository signal fingerprint: `b71bd1cffaed13db8a75a4ebc3a4ea93a9a6eb088f3249bc9b4c76bc2f888d0e`
- Significant implementation surfaces: `.github/` (3 files), `Cargo.lock/` (1 files), `Cargo.toml/` (1 files), `README.md/` (1 files), `src/` (8 files), `tests/` (1 files)
- Refreshed from the current codebase by `decapod specs.refresh`
<!-- decapod:codebase-attestation:end -->
