# Interfaces

## Trust, endpoint validation, and dependency bounds (issue #1944)

- **`DatastoreRoute::validate` (`src/lib.rs`) & `NeonAdapter` (`src/adapter/neon/mod.rs`)**: `DatastoreRoute::validate` and `NeonAdapter::new_with_options_and_budget` now enforce the same `validate_endpoint` HTTPS/literal-loopback URL policy as `Datastore::Supabase`, rejecting `http://` non-loopback hosts, userinfo, queries, and fragments before network dispatch (`DactylError::Config`). `NeonAdapter` rejects `3xx` redirects and cross-origin responses with `AdapterErrorKind::Protocol` (`provenance_origin_mismatch`).
- **Patched HTTP/URL dependency floors (`Cargo.toml`, `Cargo.lock`)**: `neon` and `supabase` feature lists include `dep:quinn-udp` (`>=0.5.16, <0.6` under `[target.'cfg(any())'.dependencies]`, inactive HTTP/3) and `dep:url` (`>=2.5.9, <3`), preserving `default = []`, `sqlite` zero-HTTP isolation, and `rust-version = "1.85"`.

## Transport deadline coverage, fairness guarantees, and regression baseline (issue #144)

- **Unified queue + execution deadline (`src/http_pool.rs`, `src/adapter/neon/mod.rs`, `src/adapter/supabase.rs`, `src/cloud.rs`)**: `ResourceBudget::request_timeout()` bounds both pool-slot queueing (`acquire_permit*`) and HTTP execution/body reading (including a single `401 use_dpop_nonce` retry). Timeouts occurring in queue or after permit acquisition prior to `request.send()` consistently return `AdapterErrorKind::Timeout` + `MutationOutcome::NotAttempted` (`EffectStatus::NotAttempted` / `ObservationSource::LocalPreflight` on Supabase/CloudConnector), whereas post-send mutating timeouts return `MutationOutcome::Uncertain`.
- **Pool limits & Propodus separation (`docs/transport-load-baseline.md`)**: `PoolSettings` (`connect_timeout`, `http_max_in_flight`, `http_max_idle_per_host`, `http_idle_timeout`) shares `SharedHttpClient` pools across origins, capped at `MAX_LIVE_POOLS = 64` (`AdapterErrorKind::Unavailable`, `NotAttempted` on overflow), `MAX_PROCESS_IN_FLIGHT = 64`, and `MAX_ASYNC_RUNTIMES_PER_POOL = 16`, with FIFO slot fairness and no per-tenant admission policy in Dactyl.

## Transport fault reconciliation & origin cutover guarantees (issues #143, #138)

- **Pre-effect DPoP challenge & non-2xx uncertain outcome invariants (`src/http_pool.rs`, `src/adapter/supabase.rs`, `src/cloud.rs`)**: `401 use_dpop_nonce` retries at most once and only prior to any `Uncertain`/`CommittedObserved` effect; non-2xx mutating responses with `EffectStatus::Uncertain` provenance always yield `MutationOutcome::Uncertain` (`outcome_unknown` on control/onboarding channels); and `error_kind_for_cloud_status_and_code` (`src/cloud.rs`) maps all allowlisted DPoP/token/device revocation codes (including `dpop_htu_mismatch`) to `AdapterErrorKind::Authentication` across `400`/`401`/`403` statuses.
- **Pre-send vs. post-send outcome separation**: Pool-slot exhaustion timeouts (`acquire_permit*`), transport initialization failures, and pre-dispatch worker spawn failures report `MutationOutcome::NotAttempted` + `ObservationSource::LocalPreflight`; unadvertised mutating `2xx` decode/budget failures (`validate_rpc_result`, `decode_write_response`, `decode_atomic_response`) emit `EffectStatus::Uncertain` (`ObservationSource::TransportFault`).
- **Staged `#138` origin cutover boundary**: `DEFAULT_CLOUD_ORIGIN` stays `"https://goyxpcmxetztjlrrcwew.supabase.co"` while `FIRST_PARTY_CLOUD_ORIGIN` (`"https://api.decapodlabs.ai"`) coexists per workspace with origin-pinned discovery, DPoP `htu` host/path enforcement, non-redirecting transport, and no-local-fallback reconciliation.

## Compatibility matrix and hardened parameter/read boundaries (issues #145, #146)

- **Compatibility matrix (`docs/wire-compatibility-matrix.md`, `#146`)**: Formalizes required fields, additive extensions, minimum version floors (`1`), downgrade refusal (`0`), unknown enum refusal, strict `DeploymentProfile` schema enforcement, and `#138` `https://api.decapodlabs.ai` DPoP `htu` binding across `PROVENANCE_CONTRACT_VERSION` (`1`), `STORAGE_CONTEXT_VERSION` (`1`), `RESOURCE_BUDGET_VERSION` (`1`), and `SUPABASE_RPC_CONTRACT_VERSION` (`1`), verified by `tests/wire_compatibility.rs` and `tests/fixtures/wire_compat/*.json`.
- **Boundary enforcement (`src/rows.rs`, `src/adapter/sqlite/mod.rs`, `#145`)**: `Parameter` JSON deserialization refuses integers above `i64::MAX`; `SqliteAdapter::read`, `visit_rows`, and `OperationKind::Read` reject non-read-only statements (`AdapterErrorKind::ReadOnly`, `"read_only"`); `SqliteAdapter` rejects non-finite `Parameter::Real` values (`AdapterErrorKind::Value`, `"unsupported_value"`, `MutationOutcome::NotAttempted`); and 0-row `INSERT` writes return empty `generated_keys`.

## Staged first-party custom origin and canonical route helpers (issue #138)

- **Unconditional exports (`src/connector.rs`, `src/lib.rs`)**: `FIRST_PARTY_CLOUD_ORIGIN` (`"https://api.decapodlabs.ai"`), `FIRST_PARTY_AUTH_PATH` (`"/auth"`), and `FIRST_PARTY_REPO_PATH_PREFIX` (`"/decapod"`) are exported across all Cargo feature combinations alongside unchanged `DEFAULT_CLOUD_ORIGIN` (`"https://goyxpcmxetztjlrrcwew.supabase.co"`).
- **Opt-in constructors & route builders (`src/connector.rs`, `src/cloud.rs`)**: `LogicalTarget::first_party_cloud()`, `LogicalConnector::first_party_cloud()`, and `LogicalConnector::first_party_cloud_async()` target `FIRST_PARTY_CLOUD_ORIGIN` explicitly without mutating ambient env or falling back to SQLite when `supabase` is disabled. `LogicalConnector` and `CloudConnector` expose `canonical_auth_url(&self) -> Result<String, DactylError>` (`<origin>/auth`) and `canonical_repository_url(&self, org: &str, repo: &str) -> Result<String, DactylError>` (`<origin>/decapod/<org>/<repo>`), failing closed with `AdapterErrorKind::Capability` (`cloud_connector_required`) on local connectors and `AdapterErrorKind::Protocol` on malformed/traversal path segments.

## HTTP pool budgets, native async operations, effect provenance, and onboarding/admin accounts (issues #134, #140)

- **`ResourceBudget` & `CloudConnector`/`LogicalConnector` pooling (`#134`)**: `ResourceBudget` adds `http_max_in_flight()`, `http_max_idle_per_host()`, `http_idle_timeout()`, and `with_http_pool_limits(max_in_flight, max_idle_per_host, idle_timeout)` (`1..=64`, `0..=64`, `> Duration::ZERO`). `CloudConnector::with_http_budget` / `LogicalConnector::with_http_budget` configure pooled connector concurrency alongside `Connection::open_with_budget`.
- **Native async driver & connector surface (`#134`)**: `Connection` exposes `read_async`, `visit_rows_async`, `write_result_async`, `write_async`, `write_with_attempt_async`, `atomic_async`, and `atomic_with_attempt_async`. `CloudConnector` and `LogicalConnector` expose `*_async` equivalents for discovery/resolution, auth start/status/exchange/refresh/revoke, `control_request*`, `account_request*`, `reconcile_operation_async`, and `reconcile_control_attempt_async`.
- **Unconditional provenance types (`#140`)**: `src/lib.rs` re-exports `PROVENANCE_CONTRACT_VERSION` (`1`), `EffectStatus`, `DecisionOutcome`, `ObservationSource`, `OperationAttempt` (`.new`, `.with_request_digest`, `.with_scope_ref`), `EffectProvenance`, and `ReconciliationObservation` across all feature combinations; `CloudResponseMetadata::provenance()` returns `Option<&EffectProvenance>`.
- **Attempt & reconciliation methods (`#140`)**: `Connection::{write_with_attempt, write_with_attempt_async, atomic_with_attempt, atomic_with_attempt_async}`, `CloudConnector` / `LogicalConnector` `account_request_with_attempt*`, `control_request_with_attempt*`, `reconcile_operation*` (`operation-status`), and `reconcile_control_attempt*`. Local SQLite and Neon reject `*_with_attempt` and `reconcile_*` locally with `AdapterErrorKind::Capability` (`cloud_connector_required`, `LocalPreflight` / `NotAttempted`) while existing `read`, `visit_rows`, `write`, `write_result`, and `atomic` contracts remain unchanged.
- **Additional `AccountOperation` onboarding/admin variants (`src/connector.rs`, `src/cloud.rs`)**: Adds `RepositoryEnroll` (`"repository-enroll"`, mutating), `ResourceLimits` (`"resource-limits"`, read), `OrgAdmins` (`"org-admins"`, mutating), `OrgRecovery` (`"org-recovery"`, mutating), `PolicyTemplates` (`"policy-templates"`, mutating), and `OrgUpgrades` (`"org-upgrades"`, mutating), plus `pub fn requires_human_bearer(self) -> bool` (false only for `DeviceRefresh` and `DeviceRevoke`). Passing `dpc_at_` device tokens or empty/missing bearers to human-required calls returns `AdapterErrorKind::Authentication` (`"authentication_required"`) or `Protocol` (`MutationOutcome::NotAttempted`, `EffectStatus::NotAttempted`, `LocalPreflight`) without dispatching network traffic.
- **Additional allowlisted codes (`src/cloud.rs`, `src/adapter/supabase.rs`, `src/http_pool.rs`)**: `dpop_aud_mismatch`, `expired_credential`, `grant_revoked`, `grant_expired`, `scope_denied`, `idempotency_conflict`, `idempotency_in_progress`, `transaction_aborted`, `accepted_not_committed`.

## Wave 2 gateway routing, account operations, runtime selector, retry, and quota interfaces (issues #132, #133, #136, #137, #138, #139)

- `Backend::datastore(self) -> Datastore` and `Datastore::as_str(self) -> &'static str` (`src/lib.rs`) expose canonical `"sqlite"`, `"neon"`, and `"supabase"` runtime identifiers independent of process environment overrides.
- `SupabaseRpcConfig`, `CloudConnector`, and `LogicalConnector` expose `.with_device_gateway(self) -> Self` and `.uses_device_gateway(&self) -> bool`. Existing legacy `/rest/v1/rpc/{dactyl_query_v1,dactyl_read_batch_v1,dactyl_batch_v1,<function>}` paths and `DEFAULT_CLOUD_ORIGIN = "https://goyxpcmxetztjlrrcwew.supabase.co"` remain unchanged when `with_device_gateway()` is not enabled.
- `AccountOperation` (`src/connector.rs`) adds `BillingOveragePolicy` (`"billing-overage-policy"`, mutating), `DeviceKeys` (`"device-keys"`, read), `DeviceSessions` (`"device-sessions"`, read), `DeviceSession` (`"device-session"`, mutating), and `DeviceRefresh` (`"device-refresh"`, mutating).
- `CloudResponseMetadata` (`src/connector.rs`) exposes `pub fn retry_after(&self) -> Option<&str>` (redacted in `Debug` via `has_retry_after`) and reflects the final HTTP attempt's headers after any single `use_dpop_nonce` retry.
- Gateway wire shapes (`with_device_gateway()`):
  - Data RPCs POST `VersionedRequest` JSON directly (`version`, flattened query/batch fields) to `/functions/v1/decapod-onboarding/cloud-query` and `/functions/v1/decapod-onboarding/cloud-batch` with `Authorization: DPoP <token>` and `DPoP: <proof>`.
  - Control RPCs POST `{"version": 1, "function": "<id>", "request": <json>}` to `/functions/v1/decapod-onboarding/cloud-control` (falling back once on HTTP `404` missing-function responses to `/functions/v1/propodus-onboarding/cloud-control`) with `Authorization: DPoP <token>` and `DPoP: <proof>`.
  - Missing or `AuthScheme::Bearer`-downgraded signers on DPoP-required gateway surfaces fail locally before network dispatch with `AdapterErrorKind::Authentication`, code `Some("device_proof_required")`, and `MutationOutcome::NotAttempted`.
- Error codes (`src/cloud.rs` and `src/adapter/supabase.rs`): HTTP `429` with `quota_exceeded` or `overage_cap_exhausted` returns `AdapterErrorKind::Quota`; `429` with `rate_limited` returns `RateLimited`; `403` adds `provider_disabled`, `mfa_step_up_required`, and `repo_access_denied` (`Authorization`); `401` adds `device_key_expired` and `refresh_reuse_detected` (`Authentication`).
- `tests/fixtures/supabase-service-schema.sql` adds SQL-portable `tasks` and `events` (`UNIQUE (stream, seq)`) tables alongside `dactyl_service_conformance` for `dactyl_event_v1` baseline sequence and post-state `WITH event_params(...)` conformance (`task.add`, `task.claim`, `task.release`, `task.done`).

## DPoP proof signer, Decapod Cloud onboarding, and metadata surfaces (issues #127, #128, #129)

- `src/connector.rs` / `src/lib.rs` export `AuthScheme`, `RequestProofInput`, `RequestProof`, `RequestProofSigner`, and `CloudResponseMetadata` unconditionally across all Cargo feature combinations. `DatastoreRoute`, `CloudConnector`, and `LogicalConnector` accept `.with_proof_signer(Arc<dyn RequestProofSigner>)` and `.with_dpop_nonce(...)`, and `LogicalConnector` / `CloudConnector` expose `resolve_with_metadata`, `discover_with_metadata`, `control_request_with_metadata`, and `account_request_with_metadata` so callers observe `DPoP-Nonce` and `Decapod-Cloud-*` headers even on `401` challenges while keeping `Debug` and `DactylError` formatting strictly redacted.
- `AccountOperation` adds `DeviceChallenge` (`"device-challenge"`, `is_mutating() == false`), `DeviceEnroll` (`"device-enroll"`, `is_mutating() == true`), `DeviceStepUp` (`"device-step-up"`, `is_mutating() == true`), and `DeviceRevoke` (`"device-revoke"`, `is_mutating() == true`).
- `src/cloud.rs` uses `/functions/v1/decapod-onboarding` as the canonical discovery, handoff, auth, and account path with a single bounded HTTP `404` fallback (absent domain error codes) to `/functions/v1/propodus-onboarding`, maps HTTP `413` to `AdapterErrorKind::Quota`, `404` to `AdapterErrorKind::NotFound`, and marks post-dispatch `5xx` / redirect / truncated failures on mutating operations with `code = Some("outcome_unknown")` and `MutationOutcome::Uncertain`.
- `src/adapter/supabase.rs` and `src/cloud.rs` allowlist RFC 9449 / DPoP and device error codes (`use_dpop_nonce`, `invalid_dpop_proof`, `dpop_proof_required`, `missing_dpop_proof`, `dpop_replay`, `dpop_nonce_mismatch`, `dpop_clock_skew`, `dpop_ath_mismatch`, `dpop_htu_mismatch`, `dpop_htm_mismatch`, `dpop_jkt_mismatch`, `stolen_bearer`, `device_revoked`, `device_not_enrolled`) under `AdapterErrorKind::Authentication`.

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

<!-- decapod:codebase-contract:start -->

## Codebase Contract Snapshot

- CLI/RPC/protocol surfaces (1): `src/lib.rs`
- Manifest authority path: `.decapod/managed/.manifest.json`
<!-- decapod:codebase-contract:end -->

<!-- decapod:codebase-attestation:start -->

## Codebase Attestation

- Repository signal fingerprint: `da78aa2f53ca6d657f21644802d437244efee1c302b9bc62da65c54e6ab81f0c`
- Significant implementation surfaces: `.github/` (4 files), `Cargo.lock/` (1 files), `Cargo.toml/` (1 files), `README.md/` (1 files), `examples/` (1 files), `src/` (19 files), `tests/` (31 files), `tools/` (3 files)
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

## Authenticated opaque control calls (issue 123)

`LogicalConnector::control_request(function, &Value, Option<&str>, OperationKind,
ResourceBudget)` is available across feature combinations; a local connector
returns Capability without opening a database. With `supabase`,
`CloudConnector::control_request` exposes the same call. It POSTs a single
`{request: value}` argument to `/rest/v1/rpc/<identifier>` using the trusted
discovered profile, current managed bearer, native schema headers and contract
version. There is no caller-supplied per-call origin/schema or StorageContext.

The operation kind explicitly declares side effects for loss classification.
Read does not enforce a PostgreSQL read-only transaction. Unknown effects and
Propodus evaluation audit writes require Write. Returned application JSON is
opaque, including successful HTTP responses carrying denial decisions.
Missing/invalid bearers fail before dispatch; remote Auth/permission/schema/rate
errors preserve typed semantics and known public codes. Unusable mutation
acknowledgements preserve their error kind with outcome_unknown and Uncertain.
Wire limits and deadlines apply; SQL and datastore-row budgets do not.
`docs/control-rpc.md` specifies consumer migration from published 0.16.1, which
lacks this seam. The eventual registry version and native/hosted authorization
proof require independent release/deployment evidence.
