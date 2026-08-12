# Semantics

## State Machines
```mermaid
stateDiagram-v2
  [*] --> Draft
  Draft --> InProgress
  InProgress --> Verified
  InProgress --> Blocked
  Blocked --> InProgress
  Verified --> [*]
```

## Invariants
| Invariant | Type | Validation |
|---|---|---|
| No promoted change without proof | System | validation gate |
| Canonical source-of-truth per entity | Data | interface/spec review |
| Mutation events are replayable | Data | deterministic replay |

## Event Sourcing Schema
| Field | Type | Description |
|---|---|---|
| event_id | string | globally unique event id |
| aggregate_id | string | entity/workflow id |
| event_type | string | semantic transition |
| payload | object | transition data |
| recorded_at | timestamp | append time |

## Replay Semantics
- Replay order:
- Conflict resolution:
- Snapshot cadence:
- Determinism proof strategy:

## Error Code Semantics
- Namespace:
- Stable compatibility window:
- Mapping to retry/degrade behavior:

## Domain Rules
- Local conditional writes report stale CAS as `affected_rows = 0`. Dactyl does not promote a local zero-row update into `VersionConflict`; that code is a remote/provider outcome.
- Local storage ignores `StorageContext`. Cloud-only tenancy fields are not required, stored, or interpreted. The same local SQL has the same result with or without a context envelope.
- Concurrent local writers serialize on the route lock file. Cleanup is a caller-owned `DROP TABLE` / `DROP INDEX`; Dactyl does not retain scoped test schemas.
- A local snapshot is Dactyl JSON. `format_version` 1 and 2 load; other versions are a capability miss. After load, the in-memory store is raised to version 2. A SQLite magic prefix is never treated as a Dactyl snapshot. Journal recovery is a write: read-only opens refuse a leftover `.wal`.

## Idempotency Contracts
| Operation | Idempotency Key | Duplicate Behavior |
|---|---|---|
| create/update mutation | request_id | return original result |
| async enqueue | event_id | ignore duplicate enqueue |

## Language Note
- Primary language inferred: Rust

<!-- decapod:capability-overlay:background-processing:start -->

## Background Processing Semantics Overlay

### Retry Semantics
- Retry and backoff behavior MUST be selected and documented for each work class
- Poison-work handling MUST be selected and documented for each work class
- Retry MUST preserve the declared side-effect and idempotency semantics

### Idempotency
- Each job MUST declare whether it is idempotent, transactional, compensating, or otherwise duplicate-safe
- Deduplication or compensation mechanisms are project decisions and require proof
- Duplicate execution MUST follow the job's declared duplicate-handling semantics

### Poison Message Handling
- Messages failing after max retries go to dead letter queue
- DLQ MUST be monitored and alerted
- Manual replay capability for DLQ messages
<!-- decapod:capability-overlay:background-processing:end -->

<!-- decapod:capability-overlay:persistent-state:start -->

## Persistent State Semantics Overlay

### Transaction Semantics
- All multi-entity operations MUST be atomic
- Read-after-write consistency within transaction boundaries
- Eventual consistency windows MUST be documented

### Migration Semantics
- Schema migrations MUST be backward-compatible
- Migration rollback procedures MUST be documented
- Data integrity checks post-migration

### Recovery Semantics
- Point-in-time recovery capability
- Recovery objectives MUST be selected for the project and recorded as proof obligations
- Recovery test cadence MUST be selected for the project and recorded as a proof obligation
<!-- decapod:capability-overlay:persistent-state:end -->

<!-- decapod:codebase-attestation:start -->

## Codebase Attestation

- Repository signal fingerprint: `cf7a521e61f99ad1452855e5decc7daacf95fd68f9b5d0c516ca4f127bf5ae74`
- Significant implementation surfaces: `.github/` (3 files), `Cargo.lock/` (1 files), `Cargo.toml/` (1 files), `README.md/` (1 files), `src/` (7 files)
- Refreshed from the current codebase by `decapod specs.refresh`
<!-- decapod:codebase-attestation:end -->
